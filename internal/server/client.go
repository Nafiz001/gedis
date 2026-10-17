package server

import (
	"errors"
	"fmt"
	"io"
	"net"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Nafiz001/gedis/internal/resp"
	"github.com/Nafiz001/gedis/internal/store"
)

// flushThreshold bounds how much reply data accumulates before it is pushed to
// the socket. Without it, a client pipelining a million GETs would make the
// server buffer every reply in memory before sending any.
const flushThreshold = 64 * 1024

// client is one connection and the per-connection state that goes with it.
//
// Everything here is touched only by that connection's own goroutine, with two
// exceptions read by CLIENT LIST and by shutdown: conn and the immutable
// identity fields. That is why no mutex is needed on the hot path.
type client struct {
	srv  *Server
	conn net.Conn
	r    *resp.Reader
	w    *resp.Writer

	id      uint64
	name    string
	addr    string
	created time.Time

	dbIndex int
	db      *store.Store

	authenticated bool
	// closeAfterReply ends the connection once the pending reply is flushed,
	// which is how QUIT and fatal protocol errors say goodbye politely.
	closeAfterReply bool
	lastCommand     string

	// writeMu guards this connection's writer. The connection loop holds it
	// around each command's reply; a publisher holds it while pushing a
	// message. It exists because pub/sub is the one place where a command run
	// by one client writes to another client's socket.
	writeMu sync.Mutex
	closing bool

	// internal marks a client with no socket behind it, used to replay the
	// log at startup and to apply a master's stream.
	internal bool
	// fromMaster marks the internal client applying replicated writes, which
	// is allowed to write even on a read-only replica.
	fromMaster bool

	// --- propagation, reset before each command ---
	propagateOverride [][]byte
	noPropagate       bool

	// --- pub/sub ---
	channels map[string]struct{}
	patterns map[string]struct{}

	// --- transactions ---
	inMulti    bool
	inExec     bool
	queued     [][][]byte
	queueError bool
	watched    map[watchKey]struct{}
	// dirtyWatch is set by whichever client writes a key this one watches, so
	// it crosses goroutines and must be atomic.
	dirtyWatch atomic.Bool

	// --- replication ---
	isReplicaConn bool
	replicaLink   *replicaLink
	replicaPort   string
}

func (s *Server) newClient(conn net.Conn) *client {
	// Nagle batches small writes, which is exactly wrong for a request/response
	// protocol: it would add latency to every reply. Go disables it by default
	// on TCP, and this makes that dependency explicit rather than assumed.
	if tc, ok := conn.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
	}

	return &client{
		srv:           s,
		conn:          conn,
		r:             resp.NewReader(conn),
		w:             resp.NewWriter(conn),
		id:            s.nextID.Add(1),
		addr:          conn.RemoteAddr().String(),
		created:       time.Now(),
		dbIndex:       0,
		db:            s.dbs[0],
		authenticated: s.cfg.Password == "",
		channels:      make(map[string]struct{}),
		patterns:      make(map[string]struct{}),
		watched:       make(map[watchKey]struct{}),
	}
}

// serve reads and executes commands until the connection ends.
func (c *client) serve() {
	c.srv.registerClient(c)
	defer func() {
		// Leave every registry holding a pointer to this client before the
		// socket closes, or a publisher could still be handed a dead one.
		c.srv.pubsub.removeClient(c)
		c.srv.watch.unwatchAll(c)
		if c.replicaLink != nil {
			c.srv.replicas.remove(c.replicaLink.id)
		}
		c.srv.unregisterClient(c)
		_ = c.conn.Close()
	}()

	// A bug in one command handler must cost one connection, not the process.
	defer func() {
		if r := recover(); r != nil {
			c.srv.log.Error("panic while handling command",
				"client", c.id,
				"command", c.lastCommand,
				"panic", r,
				"stack", string(debug.Stack()),
			)
			_ = c.w.WriteError("ERR internal server error")
			_ = c.w.Flush()
		}
	}()

	for {
		if c.srv.cfg.IdleTimeout > 0 {
			_ = c.conn.SetReadDeadline(time.Now().Add(c.srv.cfg.IdleTimeout))
		}

		args, err := c.r.ReadCommand()
		if err != nil {
			c.handleReadError(err)
			return
		}
		// An empty multibulk is a legal no-op.
		if len(args) == 0 {
			continue
		}

		// Held across the handler and the flush together, so a published
		// message can never land in the middle of a reply.
		c.writeMu.Lock()
		c.srv.dispatch(c, args)

		if c.closeAfterReply {
			_ = c.w.Flush()
			c.closing = true
			c.writeMu.Unlock()
			return
		}
		// Flush once per batch rather than once per command. When a client
		// pipelines, the next command is already buffered, so we keep executing
		// and pay a single write syscall for the whole batch -- this is where
		// most of the pipelined throughput comes from.
		var flushErr error
		if c.r.Buffered() == 0 || c.w.Buffered() >= flushThreshold {
			flushErr = c.w.Flush()
		}
		c.writeMu.Unlock()

		if flushErr != nil {
			return
		}
		// A connection that just became a replication stream stops being a
		// client: from here it only carries forwarded writes.
		if c.isReplicaConn {
			c.serveAsReplicaLink()
			return
		}
	}
}

func (c *client) handleReadError(err error) {
	var protoErr *resp.ErrProtocol
	switch {
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		// Ordinary disconnect.
	case errors.As(err, &protoErr):
		// Once framing is lost the stream cannot be resynchronised, so report
		// the problem and hang up rather than misinterpreting later bytes.
		_ = c.w.WriteError("ERR " + protoErr.Error())
		_ = c.w.Flush()
		c.srv.log.Debug("protocol error", "client", c.id, "err", err)
	default:
		var ne net.Error
		if errors.As(err, &ne) && ne.Timeout() {
			c.srv.log.Debug("client idle timeout", "client", c.id)
			return
		}
		if !c.srv.closed.Load() {
			c.srv.log.Debug("connection error", "client", c.id, "err", err)
		}
	}
}

// selectDB switches the client to another keyspace.
func (c *client) selectDB(index int) error {
	if index < 0 || index >= len(c.srv.dbs) {
		return fmt.Errorf("DB index is out of range")
	}
	c.dbIndex = index
	c.db = c.srv.dbs[index]
	return nil
}

// --- reply helpers -----------------------------------------------------
//
// Write errors are deliberately ignored here. A failed write means the socket
// is gone, which the next read reports immediately and the connection loop acts
// on; checking at every call site would add noise to every handler for no
// change in behaviour.

func (c *client) replyOK()                 { _ = c.w.WriteSimpleString("OK") }
func (c *client) replySimple(s string)     { _ = c.w.WriteSimpleString(s) }
func (c *client) replyInt(n int64)         { _ = c.w.WriteInt(n) }
func (c *client) replyBulk(b []byte)       { _ = c.w.WriteBulk(b) }
func (c *client) replyBulkString(s string) { _ = c.w.WriteBulkString(s) }
func (c *client) replyNull()               { _ = c.w.WriteNullBulk() }
func (c *client) replyNullArray()          { _ = c.w.WriteNullArray() }
func (c *client) replyArrayLen(n int)      { _ = c.w.WriteArrayLen(n) }
func (c *client) replyError(msg string)    { _ = c.w.WriteError(msg) }

func (c *client) replyErrorf(format string, args ...any) {
	_ = c.w.WriteError(fmt.Sprintf(format, args...))
}

// replyEmptyArray is the reply for a command that matched nothing.
func (c *client) replyEmptyArray() { _ = c.w.WriteArrayLen(0) }

// replyStrings writes a flat array of bulk strings.
func (c *client) replyStrings(items []string) {
	_ = c.w.WriteArrayLen(len(items))
	for _, s := range items {
		_ = c.w.WriteBulkString(s)
	}
}

// --- common error replies ---
//
// The first word of a Redis error is a machine-readable code that clients
// switch on, so these are centralised to keep the codes exactly right.

func (c *client) replyWrongType() {
	c.replyError("WRONGTYPE Operation against a key holding the wrong kind of value")
}

func (c *client) replyNotInteger() {
	c.replyError("ERR value is not an integer or out of range")
}

func (c *client) replyNotFloat() {
	c.replyError("ERR value is not a valid float")
}

func (c *client) replySyntaxError() {
	c.replyError("ERR syntax error")
}

func (c *client) replyWrongArity(cmd string) {
	c.replyErrorf("ERR wrong number of arguments for '%s' command", cmd)
}
