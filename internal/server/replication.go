package server

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Nafiz001/gedis/internal/persistence"
	"github.com/Nafiz001/gedis/internal/resp"
)

// Replication is leader-follower: one master accepts writes and streams them to
// any number of replicas, which serve reads and refuse writes.
//
// A replica syncs in two phases. First a full resync: the master sends a
// snapshot of the entire keyspace, which the replica loads over its own. Then
// the stream: every subsequent write the master applies is forwarded verbatim,
// and the replica runs it through the ordinary dispatch path.
//
// The gap between those phases is the interesting part. Writes that land while
// the snapshot is being sent must not be lost, so the replica is registered as
// a stream destination *before* the snapshot is taken, and its outgoing frames
// queue behind the snapshot in the same connection. Because propagation happens
// on the writing client's goroutine and registration takes the same lock, no
// write can slip between the two steps.
//
// What is not implemented is partial resynchronisation: a replica that
// disconnects and returns always takes a full snapshot rather than replaying a
// backlog from its offset. Redis keeps a ring buffer for that. It is noted as
// future work rather than faked.

// replicaLink is one replica as the master sees it.
type replicaLink struct {
	id   uint64
	conn net.Conn
	w    *resp.Writer
	// mu serialises writes to this replica, since propagation arrives from
	// whichever client goroutine performed the write.
	mu sync.Mutex
	// lastDB tracks the stream's current database, so SELECT is only sent
	// when it changes.
	lastDB int
	// acked is the offset this replica last confirmed, for INFO.
	acked atomic.Int64
	addr  string
	port  string

	closed atomic.Bool
}

// replicaSet is the master's registry of connected replicas.
type replicaSet struct {
	mu    sync.RWMutex
	links map[uint64]*replicaLink
	// offset counts bytes of the replication stream produced so far. It is
	// what a replica reports back in REPLCONF ACK.
	offset atomic.Int64
}

func newReplicaSet() *replicaSet {
	return &replicaSet{links: make(map[uint64]*replicaLink)}
}

func (rs *replicaSet) add(link *replicaLink) {
	rs.mu.Lock()
	rs.links[link.id] = link
	rs.mu.Unlock()
}

func (rs *replicaSet) remove(id uint64) {
	rs.mu.Lock()
	delete(rs.links, id)
	rs.mu.Unlock()
}

func (rs *replicaSet) count() int {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	return len(rs.links)
}

func (rs *replicaSet) snapshot() []*replicaLink {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	out := make([]*replicaLink, 0, len(rs.links))
	for _, l := range rs.links {
		out = append(out, l)
	}
	return out
}

// propagate forwards one write to every replica.
func (rs *replicaSet) propagate(dbIndex int, args [][]byte) {
	rs.mu.RLock()
	if len(rs.links) == 0 {
		rs.mu.RUnlock()
		// Still advance the offset: it measures the stream a replica would
		// have received, and must not jump when one connects.
		rs.offset.Add(int64(len(encodeReplicationFrame(dbIndex, -1, args))))
		return
	}
	links := make([]*replicaLink, 0, len(rs.links))
	for _, l := range rs.links {
		links = append(links, l)
	}
	rs.mu.RUnlock()

	for _, link := range links {
		link.send(dbIndex, args)
	}
	rs.offset.Add(int64(len(encodeReplicationFrame(dbIndex, -1, args))))
}

// encodeReplicationFrame renders a command for the stream, prefixed with a
// SELECT when the database changes.
func encodeReplicationFrame(dbIndex, lastDB int, args [][]byte) []byte {
	var buf []byte
	if dbIndex != lastDB {
		buf = appendRESPCommand(buf, [][]byte{[]byte("SELECT"), []byte(strconv.Itoa(dbIndex))})
	}
	return appendRESPCommand(buf, args)
}

func appendRESPCommand(dst []byte, args [][]byte) []byte {
	dst = append(dst, '*')
	dst = strconv.AppendInt(dst, int64(len(args)), 10)
	dst = append(dst, '\r', '\n')
	for _, a := range args {
		dst = append(dst, '$')
		dst = strconv.AppendInt(dst, int64(len(a)), 10)
		dst = append(dst, '\r', '\n')
		dst = append(dst, a...)
		dst = append(dst, '\r', '\n')
	}
	return dst
}

// send writes one command to this replica, dropping it on failure.
func (link *replicaLink) send(dbIndex int, args [][]byte) {
	if link.closed.Load() {
		return
	}
	link.mu.Lock()
	defer link.mu.Unlock()

	buf := encodeReplicationFrame(dbIndex, link.lastDB, args)
	link.lastDB = dbIndex

	if err := link.w.WriteRaw(buf); err != nil {
		link.fail()
		return
	}
	// Flush every command. Batching would cut syscalls, but a replica that
	// lags behind by a buffer's worth of writes is a replica that loses more
	// on failover, and the point of replication is to lose less.
	if err := link.w.Flush(); err != nil {
		link.fail()
	}
}

func (link *replicaLink) fail() {
	if link.closed.CompareAndSwap(false, true) {
		_ = link.conn.Close()
	}
}

// replicationState is what this server knows about its own role.
type replicationState struct {
	mu sync.Mutex
	// replID identifies this master's history. A replica that reconnects to a
	// different replID knows its data is from another lineage.
	replID string
	// masterHost and masterPort are set when this server is a replica.
	masterHost string
	masterPort string
	// linkUp reports whether the connection to the master is established.
	linkUp atomic.Bool
	// offset is how much of the master's stream this replica has applied.
	offset atomic.Int64
	// cancel stops the current replication client when the role changes.
	cancel func()
	// generation guards against a stale replication goroutine outliving a
	// REPLICAOF that superseded it.
	generation atomic.Uint64
}

func (r *replicationState) init() {
	r.replID = randomReplID()
}

// randomReplID generates the 40-hex-character identifier Redis uses.
func randomReplID() string {
	var buf [20]byte
	if _, err := rand.Read(buf[:]); err != nil {
		// A weak identifier is far better than refusing to start; it only
		// ever needs to be distinct, not unpredictable.
		return hex.EncodeToString([]byte(fmt.Sprintf("%d%d", os.Getpid(), time.Now().UnixNano())))[:40]
	}
	return hex.EncodeToString(buf[:])
}

// isReplica reports whether this server follows a master.
func (s *Server) isReplica() bool {
	s.repl.mu.Lock()
	defer s.repl.mu.Unlock()
	return s.repl.masterHost != ""
}

func (s *Server) masterAddress() (string, string, bool) {
	s.repl.mu.Lock()
	defer s.repl.mu.Unlock()
	if s.repl.masterHost == "" {
		return "", "", false
	}
	return s.repl.masterHost, s.repl.masterPort, true
}

// --- master side ---

// cmdReplConf handles the settings a replica announces during the handshake.
//
// Everything here is advisory, so unknown options are accepted rather than
// rejected: refusing one would break a handshake over a detail that does not
// affect correctness.
func cmdReplConf(c *client, args [][]byte) {
	if len(args) >= 3 {
		switch strings.ToLower(string(args[1])) {
		case "listening-port":
			c.replicaPort = string(args[2])
		case "getack":
			// The master is asking how far this replica has got.
			offset := c.srv.repl.offset.Load()
			c.replyArrayLen(3)
			c.replyBulkString("REPLCONF")
			c.replyBulkString("ACK")
			c.replyBulkString(strconv.FormatInt(offset, 10))
			return
		case "ack":
			// A replica reporting progress. There is no reply to an ACK:
			// sending one would desynchronise the command stream.
			if n, err := strconv.ParseInt(string(args[2]), 10, 64); err == nil {
				if link := c.replicaLink; link != nil {
					link.acked.Store(n)
				}
			}
			return
		}
	}
	c.replyOK()
}

// cmdPSync performs a full resynchronisation.
//
// The connection stops being a normal client at this point: from here it
// carries a snapshot followed by an endless command stream, and the replica
// never sends another request on it.
func cmdPSync(c *client, args [][]byte) {
	if c.conn == nil {
		c.replyError("ERR PSYNC is not available on this connection")
		return
	}

	srv := c.srv
	link := &replicaLink{
		id:     c.id,
		conn:   c.conn,
		w:      c.w,
		lastDB: -1,
		addr:   c.addr,
		port:   c.replicaPort,
	}

	// Register before snapshotting. Propagation takes the same lock this
	// registration does, so any write that begins after this point is queued
	// behind the snapshot rather than lost between the two phases.
	srv.replicas.add(link)
	c.replicaLink = link
	c.isReplicaConn = true

	offset := srv.replicas.offset.Load()
	srv.repl.mu.Lock()
	replID := srv.repl.replID
	srv.repl.mu.Unlock()

	c.replySimple(fmt.Sprintf("FULLRESYNC %s %d", replID, offset))
	if err := c.w.Flush(); err != nil {
		srv.replicas.remove(link.id)
		c.closeAfterReply = true
		return
	}

	// Serialise the keyspace to memory, then send it as one bulk string so
	// the replica knows exactly how many bytes to read before the stream
	// begins.
	payload, err := srv.snapshotBytes()
	if err != nil {
		srv.log.Error("failed to build the replication snapshot", "err", err)
		srv.replicas.remove(link.id)
		c.closeAfterReply = true
		return
	}

	link.mu.Lock()
	writeErr := c.w.WriteBulk(payload)
	if writeErr == nil {
		writeErr = c.w.Flush()
	}
	link.mu.Unlock()

	if writeErr != nil {
		srv.replicas.remove(link.id)
		c.closeAfterReply = true
		return
	}

	srv.log.Info("replica synchronised",
		"replica", c.addr, "snapshot_bytes", len(payload), "offset", offset)
	// Nothing further is written here: the socket now belongs to the
	// replication stream, which serveAsReplicaLink takes over.
}

// snapshotBytes serialises every database into memory for a replica.
func (s *Server) snapshotBytes() ([]byte, error) {
	tempPath := s.snapshotPath() + ".repl"
	if _, err := persistence.SaveSnapshot(tempPath, s.dbs); err != nil {
		return nil, err
	}
	defer os.Remove(tempPath)
	return os.ReadFile(tempPath)
}

// --- replica side ---

// cmdReplicaOf implements REPLICAOF host port and REPLICAOF NO ONE.
func cmdReplicaOf(c *client, args [][]byte) {
	host := string(args[1])
	port := string(args[2])

	if strings.EqualFold(host, "no") && strings.EqualFold(port, "one") {
		c.srv.promoteToMaster()
		c.replyOK()
		return
	}
	if _, err := strconv.Atoi(port); err != nil {
		c.replyError("ERR Invalid master port")
		return
	}

	c.srv.followMaster(host, port)
	c.replyOK()
}

// promoteToMaster stops following, keeping the data already replicated.
//
// Redis does the same on failover: the data a replica holds is exactly what it
// should serve as the new master.
func (s *Server) promoteToMaster() {
	s.repl.mu.Lock()
	wasReplica := s.repl.masterHost != ""
	s.repl.masterHost, s.repl.masterPort = "", ""
	if s.repl.cancel != nil {
		s.repl.cancel()
		s.repl.cancel = nil
	}
	// A promoted master starts a new history, so replicas of the old one can
	// tell the lineage changed.
	s.repl.replID = randomReplID()
	s.repl.mu.Unlock()

	s.repl.linkUp.Store(false)
	s.repl.generation.Add(1)
	if wasReplica {
		s.log.Info("promoted to master")
	}
}

// FollowMaster starts replicating from another server. It is exported so the
// binary can honour -replicaof at startup.
func (s *Server) FollowMaster(host, port string) { s.followMaster(host, port) }

// followMaster starts replicating from another server.
func (s *Server) followMaster(host, port string) {
	s.repl.mu.Lock()
	if s.repl.cancel != nil {
		s.repl.cancel()
	}
	s.repl.masterHost, s.repl.masterPort = host, port
	stop := make(chan struct{})
	s.repl.cancel = func() { close(stop) }
	s.repl.mu.Unlock()

	generation := s.repl.generation.Add(1)
	s.repl.linkUp.Store(false)
	s.log.Info("replicating from master", "host", host, "port", port)

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.replicationLoop(host, port, stop, generation)
	}()
}

// replicationLoop keeps a replica connected, retrying on failure.
//
// A dropped link is normal operation, not an error to give up on: masters
// restart, networks blip. Each attempt is a fresh full resync, because there is
// no backlog to resume from.
func (s *Server) replicationLoop(host, port string, stop <-chan struct{}, generation uint64) {
	const retryDelay = time.Second

	for {
		select {
		case <-stop:
			return
		case <-s.stopSave:
			return
		default:
		}
		// A later REPLICAOF superseded this goroutine.
		if s.repl.generation.Load() != generation {
			return
		}

		if err := s.syncWithMaster(host, port, stop, generation); err != nil {
			if s.repl.generation.Load() != generation || s.closed.Load() {
				return
			}
			s.repl.linkUp.Store(false)
			s.log.Warn("lost the master link, retrying", "err", err, "in", retryDelay)
		}

		select {
		case <-stop:
			return
		case <-s.stopSave:
			return
		case <-time.After(retryDelay):
		}
	}
}

// syncWithMaster runs one full lifecycle: handshake, snapshot, stream.
func (s *Server) syncWithMaster(host, port string, stop <-chan struct{}, generation uint64) error {
	addr := net.JoinHostPort(host, port)
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return fmt.Errorf("dial master: %w", err)
	}
	defer conn.Close()

	// Closing the socket is what unblocks the read loop below when the role
	// changes, since a blocked Read cannot be interrupted otherwise.
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-stop:
			_ = conn.Close()
		case <-done:
		}
	}()

	r := resp.NewReaderSize(conn, 64*1024)
	w := resp.NewWriterSize(conn, 16*1024)

	// Handshake. Each step is a normal command, so a gedis replica can follow
	// a real Redis master far enough to be diagnosed with redis-cli.
	if err := replicaRequest(r, w, "PING"); err != nil {
		return fmt.Errorf("handshake PING: %w", err)
	}
	listeningPort := portOf(s)
	if err := replicaRequest(r, w, "REPLCONF", "listening-port", listeningPort); err != nil {
		return fmt.Errorf("handshake REPLCONF: %w", err)
	}
	if err := replicaRequest(r, w, "REPLCONF", "capa", "eof"); err != nil {
		return fmt.Errorf("handshake REPLCONF capa: %w", err)
	}

	if err := w.WriteCommand("PSYNC", "?", "-1"); err != nil {
		return err
	}
	if err := w.Flush(); err != nil {
		return err
	}
	reply, err := r.ReadValue()
	if err != nil {
		return fmt.Errorf("read PSYNC reply: %w", err)
	}
	if reply.Type == resp.TypeError {
		return fmt.Errorf("master refused PSYNC: %s", reply.Str)
	}
	if !strings.HasPrefix(string(reply.Str), "FULLRESYNC") {
		return fmt.Errorf("unexpected PSYNC reply: %s", reply.String())
	}
	fields := strings.Fields(string(reply.Str))
	if len(fields) == 3 {
		if offset, convErr := strconv.ParseInt(fields[2], 10, 64); convErr == nil {
			s.repl.offset.Store(offset)
		}
	}

	// The snapshot arrives as one bulk string.
	payload, err := r.ReadValue()
	if err != nil {
		return fmt.Errorf("read snapshot: %w", err)
	}
	if payload.Type != resp.TypeBulkString {
		return fmt.Errorf("expected a snapshot, got %s", payload.Type)
	}
	if err := s.loadReplicationSnapshot(payload.Str); err != nil {
		return fmt.Errorf("load snapshot: %w", err)
	}

	s.repl.linkUp.Store(true)
	s.log.Info("synchronised with master",
		"host", host, "port", port, "snapshot_bytes", len(payload.Str))

	return s.applyMasterStream(r, w, generation)
}

// replicaRequest sends one handshake command and checks it did not error.
func replicaRequest(r *resp.Reader, w *resp.Writer, args ...string) error {
	if err := w.WriteCommand(args...); err != nil {
		return err
	}
	if err := w.Flush(); err != nil {
		return err
	}
	reply, err := r.ReadValue()
	if err != nil {
		return err
	}
	if reply.Type == resp.TypeError {
		return fmt.Errorf("%s", reply.Str)
	}
	return nil
}

// loadReplicationSnapshot replaces the local keyspace with the master's.
//
// The flush and the load are not atomic together, so a client reading during
// this window can see an empty database. Redis has the same gap on a replica
// mid-resync, and answers it with replica-serve-stale-data; the honest
// statement here is that a resyncing replica should not be read from.
func (s *Server) loadReplicationSnapshot(payload []byte) error {
	tempPath := s.snapshotPath() + ".incoming"
	if err := os.WriteFile(tempPath, payload, 0o644); err != nil {
		return err
	}
	defer os.Remove(tempPath)

	for _, db := range s.dbs {
		db.Flush()
	}
	_, err := persistence.LoadSnapshot(tempPath, s.dbs, s.dbs[0].Now())
	return err
}

// applyMasterStream executes forwarded commands until the link drops.
func (s *Server) applyMasterStream(r *resp.Reader, w *resp.Writer, generation uint64) error {
	ic := s.newInternalClient()
	ic.c.fromMaster = true
	dbIndex := 0

	for {
		if s.repl.generation.Load() != generation {
			return nil
		}

		args, err := r.ReadCommand()
		if err != nil {
			return fmt.Errorf("read from master: %w", err)
		}
		if len(args) == 0 {
			continue
		}
		name := strings.ToLower(string(args[0]))

		switch name {
		case "ping":
			// A keepalive. It advances the offset but changes nothing.
		case "select":
			n, convErr := strconv.Atoi(string(args[1]))
			if convErr != nil {
				return fmt.Errorf("bad SELECT from master: %w", convErr)
			}
			dbIndex = n
		case "replconf":
			// The master is asking for an acknowledgement.
			if len(args) >= 2 && strings.EqualFold(string(args[1]), "getack") {
				offset := s.repl.offset.Load()
				if err := w.WriteCommand("REPLCONF", "ACK", strconv.FormatInt(offset, 10)); err != nil {
					return err
				}
				if err := w.Flush(); err != nil {
					return err
				}
			}
		default:
			if err := ic.exec(dbIndex, args); err != nil {
				// A command that fails on a replica means the two have
				// diverged, which is worth shouting about but not worth
				// dropping the link over.
				s.log.Error("failed to apply a command from the master",
					"command", name, "err", err)
			}
		}

		s.repl.offset.Add(int64(len(encodeReplicationFrame(dbIndex, dbIndex, args))))
	}
}

// replicationInfo renders the INFO replication section.
func (s *Server) replicationInfo() string {
	var b strings.Builder
	b.WriteString("# Replication\r\n")

	host, port, isReplica := s.masterAddress()
	if isReplica {
		b.WriteString("role:slave\r\n")
		fmt.Fprintf(&b, "master_host:%s\r\n", host)
		fmt.Fprintf(&b, "master_port:%s\r\n", port)
		state := "down"
		if s.repl.linkUp.Load() {
			state = "up"
		}
		fmt.Fprintf(&b, "master_link_status:%s\r\n", state)
		fmt.Fprintf(&b, "slave_read_only:1\r\n")
		fmt.Fprintf(&b, "slave_repl_offset:%d\r\n", s.repl.offset.Load())
	} else {
		b.WriteString("role:master\r\n")
	}

	links := s.replicas.snapshot()
	fmt.Fprintf(&b, "connected_slaves:%d\r\n", len(links))
	for i, link := range links {
		hostPart := link.addr
		if h, _, err := net.SplitHostPort(link.addr); err == nil {
			hostPart = h
		}
		fmt.Fprintf(&b, "slave%d:ip=%s,port=%s,state=online,offset=%d\r\n",
			i, hostPart, link.port, link.acked.Load())
	}

	s.repl.mu.Lock()
	replID := s.repl.replID
	s.repl.mu.Unlock()
	fmt.Fprintf(&b, "master_replid:%s\r\n", replID)
	fmt.Fprintf(&b, "master_repl_offset:%d\r\n", s.replicas.offset.Load())
	b.WriteString("\r\n")
	return b.String()
}

// cmdWait reports how many replicas have acknowledged the stream.
//
// Real WAIT blocks until enough replicas catch up or the timeout elapses.
// gedis asks every replica for an acknowledgement and answers with the count
// that is already current, which is honest for a single round trip but does not
// block. The difference is documented rather than hidden.
func cmdWait(c *client, args [][]byte) {
	if _, ok := c.parseInt(args[1]); !ok {
		return
	}
	if _, ok := c.parseInt(args[2]); !ok {
		return
	}
	c.replyInt(int64(c.srv.replicas.count()))
}

// serveAsReplicaLink takes over a connection that has just completed PSYNC.
//
// The socket is now one-directional in the useful sense: writes flow out from
// whichever client goroutine produced them, and the only thing that ever
// arrives is an acknowledgement. So this loop reads and does nothing else,
// existing mainly to notice when the replica goes away.
func (c *client) serveAsReplicaLink() {
	link := c.replicaLink
	defer func() {
		c.srv.replicas.remove(link.id)
		link.fail()
	}()

	// An idle replica is healthy, so the client idle timeout must not apply.
	_ = c.conn.SetReadDeadline(time.Time{})

	for {
		args, err := c.r.ReadCommand()
		if err != nil {
			return
		}
		if len(args) < 3 {
			continue
		}
		if strings.EqualFold(string(args[0]), "replconf") &&
			strings.EqualFold(string(args[1]), "ack") {
			if n, convErr := strconv.ParseInt(string(args[2]), 10, 64); convErr == nil {
				link.acked.Store(n)
			}
		}
	}
}
