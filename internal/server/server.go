// Package server wires the protocol layer to the keyspace: it accepts TCP
// connections, decodes commands, dispatches them, and writes replies.
//
// The concurrency model is one goroutine per connection. That is the natural
// shape in Go -- blocking reads stay readable, and the runtime multiplexes
// thousands of goroutines onto a handful of OS threads via epoll/IOCP under the
// hood, so this scales the way an event loop does without the callback
// structure. Shared state lives in the sharded store, not in the connection.
package server

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nafizahmed/gedis/internal/persistence"
	"github.com/nafizahmed/gedis/internal/store"
)

// SavePoint triggers an automatic snapshot once Changes writes have
// accumulated within After. Redis calls these "save" directives.
type SavePoint struct {
	After   time.Duration
	Changes int
}

// Version is reported by INFO and HELLO.
const Version = "0.1.0"

// Config holds the server's tunable settings.
type Config struct {
	// Addr is the listen address, e.g. ":6380".
	Addr string
	// Databases is how many SELECT-able keyspaces to create.
	Databases int
	// Password, when non-empty, requires clients to AUTH before anything else.
	Password string
	// MaxClients caps concurrent connections; 0 means unlimited.
	MaxClients int
	// Shards is the per-database shard count. 0 uses the store default.
	Shards int
	// IdleTimeout closes connections idle for this long. 0 disables it.
	IdleTimeout time.Duration
	// Clock overrides the time source used for key expiration. Tests inject a
	// controllable clock so expiry can be driven deterministically instead of
	// by sleeping.
	Clock store.Clock

	// Dir is the directory persistence files live in.
	Dir string
	// AppendOnly enables the append-only log.
	AppendOnly bool
	// AppendFilename names the log inside Dir.
	AppendFilename string
	// AppendFsync sets the log's durability policy.
	AppendFsync persistence.FsyncPolicy
	// SnapshotFilename names the snapshot inside Dir.
	SnapshotFilename string
	// SavePoints schedule automatic snapshots. Empty disables them.
	SavePoints []SavePoint

	// MaxMemory caps the process heap in bytes. 0 means no limit.
	MaxMemory int64
	// MaxMemoryPolicy decides what to evict once MaxMemory is reached.
	MaxMemoryPolicy EvictionPolicy
	// Logger receives server events. Defaults to a text logger on stderr.
	Logger *slog.Logger
}

// DefaultConfig returns the settings the binary starts with.
//
// Port 6380 rather than 6379 so gedis can run next to a real Redis during
// side-by-side comparison, which is exactly how it gets tested.
func DefaultConfig() Config {
	return Config{
		Addr:             ":6380",
		Databases:        16,
		Shards:           store.DefaultShards,
		Dir:              ".",
		AppendFilename:   "gedis.aof",
		SnapshotFilename: "gedis.rdb",
		AppendFsync:      persistence.FsyncEverySecond,
		// The same defaults Redis ships with: snapshot after 1 change in
		// 15 minutes, 10 in 5, or 10000 in one.
		SavePoints: []SavePoint{
			{After: 15 * time.Minute, Changes: 1},
			{After: 5 * time.Minute, Changes: 10},
			{After: 1 * time.Minute, Changes: 10000},
		},
	}
}

// Server is a running gedis instance.
type Server struct {
	cfg Config
	log *slog.Logger

	dbs []*store.Store

	ln     net.Listener
	lnMu   sync.Mutex
	closed atomic.Bool

	clientsMu sync.Mutex
	clients   map[uint64]*client
	nextID    atomic.Uint64

	startedAt time.Time
	stats     serverStats

	// --- persistence ---
	aof *persistence.AOF
	// dirty counts writes since the last successful snapshot, which is what
	// the save points are measured against.
	dirty    atomic.Int64
	lastSave atomic.Int64 // unix seconds
	// saving and rewriting guard against two background jobs of the same kind
	// running at once.
	saving    atomic.Bool
	rewriting atomic.Bool
	// loading is set while replaying from disk, so the write stream is not
	// re-fed with what it just produced.
	loading atomic.Bool

	// --- replication ---
	replicas *replicaSet
	repl     replicationState

	// --- pub/sub ---
	pubsub *pubsubRegistry

	// --- eviction ---
	memory *memoryMonitor

	// --- transactions ---
	watch *watchRegistry
	// execMu gives EXEC exclusive access to the keyspace. Ordinary commands
	// hold it shared, so they never block each other.
	execMu sync.RWMutex

	// stopSave shuts down the save-point and replication goroutines.
	stopSave     chan struct{}
	stopSaveOnce sync.Once

	wg sync.WaitGroup
}

type serverStats struct {
	connectionsReceived atomic.Int64
	commandsProcessed   atomic.Int64
	rejectedConnections atomic.Int64
	evictedKeys         atomic.Int64
}

// New creates a Server with its databases initialised but nothing listening.
func New(cfg Config) *Server {
	if cfg.Addr == "" {
		cfg.Addr = DefaultConfig().Addr
	}
	if cfg.Databases <= 0 {
		cfg.Databases = 16
	}
	if cfg.Shards <= 0 {
		cfg.Shards = store.DefaultShards
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
			Level: slog.LevelInfo,
		}))
	}

	s := &Server{
		cfg:       cfg,
		log:       cfg.Logger,
		dbs:       make([]*store.Store, cfg.Databases),
		clients:   make(map[uint64]*client),
		startedAt: time.Now(),
		replicas:  newReplicaSet(),
		pubsub:    newPubsubRegistry(),
		watch:     newWatchRegistry(),
		memory:    newMemoryMonitor(),
		stopSave:  make(chan struct{}),
	}
	s.lastSave.Store(time.Now().Unix())
	s.repl.init()
	for i := range s.dbs {
		opts := []store.Option{store.WithShards(cfg.Shards)}
		if cfg.Clock != nil {
			opts = append(opts, store.WithClock(cfg.Clock))
		}
		s.dbs[i] = store.New(opts...)
		s.dbs[i].EnableAccessTracking(cfg.MaxMemoryPolicy.needsAccessTracking())
		// Every key change has to reach the watch registry, which is what
		// lets EXEC know a WATCHed key moved. The index is captured per
		// database so the registry can tell them apart.
		dbIndex := i
		s.dbs[i].SetModifyHook(func(key string) { s.watch.touch(dbIndex, key) })
		s.dbs[i].StartExpiryCycle()
	}
	return s
}

// Addr returns the address actually being listened on. It is only meaningful
// after Listen or Serve has bound the socket, and is how tests discover the
// port when they ask for ":0".
func (s *Server) Addr() net.Addr {
	s.lnMu.Lock()
	defer s.lnMu.Unlock()
	if s.ln == nil {
		return nil
	}
	return s.ln.Addr()
}

// Listen binds the configured address without accepting yet.
//
// Binding is separated from accepting so a test can start the server, learn its
// port, and connect without racing against the listener coming up.
func (s *Server) Listen() error {
	ln, err := net.Listen("tcp", s.cfg.Addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", s.cfg.Addr, err)
	}
	s.lnMu.Lock()
	s.ln = ln
	s.lnMu.Unlock()
	return nil
}

// Serve accepts connections until the context is cancelled or Close is called.
// It binds first if Listen has not already been called.
func (s *Server) Serve(ctx context.Context) error {
	s.lnMu.Lock()
	bound := s.ln != nil
	s.lnMu.Unlock()
	if !bound {
		if err := s.Listen(); err != nil {
			return err
		}
	}

	// Restore before accepting, so no client ever sees a half-loaded keyspace.
	if err := s.loadFromDisk(); err != nil {
		return err
	}
	if err := s.openAOF(); err != nil {
		return err
	}
	s.startSavePoints()

	s.log.Info("gedis ready to accept connections",
		"addr", s.Addr().String(),
		"version", Version,
		"databases", s.cfg.Databases,
		"shards_per_db", s.cfg.Shards,
		"persistence", s.persistenceMode(),
	)

	// Cancelling the context unblocks Accept by closing the listener; there is
	// no portable way to interrupt a blocked Accept otherwise.
	go func() {
		<-ctx.Done()
		s.Close()
	}()

	for {
		conn, err := s.ln.Accept()
		if err != nil {
			if s.closed.Load() {
				break
			}
			// A transient accept failure (fd exhaustion, for instance) should
			// not bring the whole server down.
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			s.log.Error("accept failed", "err", err)
			continue
		}

		if s.cfg.MaxClients > 0 && s.clientCount() >= s.cfg.MaxClients {
			s.stats.rejectedConnections.Add(1)
			// Tell the client why before hanging up, rather than dropping it
			// silently and leaving it to guess.
			_, _ = conn.Write([]byte("-ERR max number of clients reached\r\n"))
			_ = conn.Close()
			continue
		}

		s.stats.connectionsReceived.Add(1)
		c := s.newClient(conn)
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			c.serve()
		}()
	}

	// Wait for in-flight connections so shutdown does not cut a reply in half.
	s.wg.Wait()
	for _, db := range s.dbs {
		db.Close()
	}
	// Flush the log last, once nothing else can append to it.
	if s.aof != nil {
		if err := s.aof.Close(); err != nil {
			s.log.Error("failed to flush the append-only file on shutdown", "err", err)
		}
	}
	s.log.Info("gedis shut down")
	return nil
}

// persistenceMode names the durability strategy in effect, for the startup log.
func (s *Server) persistenceMode() string {
	if s.cfg.AppendOnly {
		return "aof:" + s.cfg.AppendFsync.String()
	}
	if len(s.cfg.SavePoints) > 0 {
		return "snapshot"
	}
	return "none"
}

// ListenAndServe is Serve with a context that is never cancelled by the caller.
func (s *Server) ListenAndServe() error {
	return s.Serve(context.Background())
}

// Close stops accepting and disconnects every client. It is safe to call more
// than once and from any goroutine.
func (s *Server) Close() {
	if !s.closed.CompareAndSwap(false, true) {
		return
	}
	s.stopSaveOnce.Do(func() { close(s.stopSave) })

	// Stop following a master. The replication goroutine is parked on a
	// blocking socket read, and closing the stop channel is what makes its
	// watchdog close that socket -- without this, shutdown waits forever on a
	// read that will never return.
	s.repl.mu.Lock()
	if s.repl.cancel != nil {
		s.repl.cancel()
		s.repl.cancel = nil
	}
	s.repl.mu.Unlock()
	s.repl.generation.Add(1)

	// Drop the replicas this server is feeding, so their link goroutines end
	// too.
	for _, link := range s.replicas.snapshot() {
		link.fail()
	}

	s.lnMu.Lock()
	if s.ln != nil {
		_ = s.ln.Close()
	}
	s.lnMu.Unlock()

	// Closing each connection makes its blocked Read return, which is what ends
	// the per-client goroutines.
	s.clientsMu.Lock()
	for _, c := range s.clients {
		_ = c.conn.Close()
	}
	s.clientsMu.Unlock()
}

func (s *Server) registerClient(c *client) {
	s.clientsMu.Lock()
	s.clients[c.id] = c
	s.clientsMu.Unlock()
}

func (s *Server) unregisterClient(c *client) {
	s.clientsMu.Lock()
	delete(s.clients, c.id)
	s.clientsMu.Unlock()
}

func (s *Server) clientCount() int {
	s.clientsMu.Lock()
	defer s.clientsMu.Unlock()
	return len(s.clients)
}

// snapshotClients returns the current clients, for CLIENT LIST.
func (s *Server) snapshotClients() []*client {
	s.clientsMu.Lock()
	defer s.clientsMu.Unlock()
	out := make([]*client, 0, len(s.clients))
	for _, c := range s.clients {
		out = append(out, c)
	}
	return out
}

// DB returns the keyspace at index, or nil if out of range. Tests use it to
// assert on server state directly.
func (s *Server) DB(index int) *store.Store {
	if index < 0 || index >= len(s.dbs) {
		return nil
	}
	return s.dbs[index]
}
