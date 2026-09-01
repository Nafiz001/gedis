package server

import (
	"strings"
	"sync"
	"sync/atomic"
)

// Transactions are MULTI, a queue of commands, and EXEC to run them as a unit,
// with WATCH providing optimistic concurrency control on top.
//
// Two things have to be true for EXEC to mean anything. The queued commands
// must run without another client interleaving, and a WATCHed key that changed
// since WATCH must abort the whole thing. Redis gets the first for free by
// being single-threaded. gedis has to take it explicitly: every command holds
// a server-wide read lock, and EXEC holds the write lock, so a transaction runs
// with the keyspace to itself.
//
// That read lock is the price of honest semantics on a threaded server. It is
// uncontended in the normal case -- readers never block readers -- but it is
// not free, and the alternative was to run EXEC as an ordinary batch and call
// it a transaction, which it would not be.

// watchKey identifies a watched key within a database.
type watchKey struct {
	db  int
	key string
}

// watchRegistry tracks which clients are watching which keys.
//
// Invalidation is push-based, like Redis: a write marks every watcher dirty
// immediately, rather than EXEC comparing version numbers later. That keeps the
// check at EXEC time to a single boolean, and costs nothing at all when nobody
// is watching -- which is almost always.
type watchRegistry struct {
	// watching is the number of live watches. It is atomic and checked before
	// the mutex so that the write path -- which calls touch on every single
	// key change -- pays one relaxed load and nothing else when no client is
	// watching, which is nearly always.
	watching atomic.Int64
	mu       sync.Mutex
	keys     map[watchKey]map[uint64]*client
}

func newWatchRegistry() *watchRegistry {
	return &watchRegistry{keys: make(map[watchKey]map[uint64]*client)}
}

func (w *watchRegistry) watch(c *client, db int, key string) {
	w.mu.Lock()
	defer w.mu.Unlock()

	k := watchKey{db: db, key: key}
	if _, already := c.watched[k]; already {
		return
	}
	watchers, ok := w.keys[k]
	if !ok {
		watchers = make(map[uint64]*client)
		w.keys[k] = watchers
	}
	watchers[c.id] = c
	c.watched[k] = struct{}{}
	w.watching.Add(1)
}

// unwatchAll drops every watch a client holds, which EXEC, DISCARD, UNWATCH and
// disconnection all do.
func (w *watchRegistry) unwatchAll(c *client) {
	if len(c.watched) == 0 {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()

	for k := range c.watched {
		if watchers, ok := w.keys[k]; ok {
			delete(watchers, c.id)
			if len(watchers) == 0 {
				delete(w.keys, k)
			}
		}
		w.watching.Add(-1)
	}
	c.watched = make(map[watchKey]struct{})
}

// touch marks every client watching this key as dirty.
func (w *watchRegistry) touch(db int, key string) {
	if w.watching.Load() == 0 {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, c := range w.keys[watchKey{db: db, key: key}] {
		c.dirtyWatch.Store(true)
	}
}

// touchDatabase marks every watcher of a database dirty, for FLUSHDB.
func (w *watchRegistry) touchDatabase(db int) {
	if w.watching.Load() == 0 {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	for k, watchers := range w.keys {
		if k.db != db {
			continue
		}
		for _, c := range watchers {
			c.dirtyWatch.Store(true)
		}
	}
}

// touchAll marks every watcher dirty, for FLUSHALL.
func (w *watchRegistry) touchAll() {
	if w.watching.Load() == 0 {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, watchers := range w.keys {
		for _, c := range watchers {
			c.dirtyWatch.Store(true)
		}
	}
}

// hasWatchers reports whether any watch is active anywhere.
func (w *watchRegistry) hasWatchers() bool {
	return w.watching.Load() > 0
}

// --- commands ---

func cmdMulti(c *client, args [][]byte) {
	if c.inMulti {
		c.replyError("ERR MULTI calls can not be nested")
		return
	}
	c.inMulti = true
	c.queued = nil
	c.queueError = false
	c.replyOK()
}

func cmdDiscard(c *client, args [][]byte) {
	if !c.inMulti {
		c.replyError("ERR DISCARD without MULTI")
		return
	}
	c.resetTransaction()
	c.replyOK()
}

func cmdWatch(c *client, args [][]byte) {
	// Watching inside a transaction is meaningless: the queue is already
	// fixed, so there is nothing left for the watch to protect.
	if c.inMulti {
		c.replyError("ERR WATCH inside MULTI is not allowed")
		return
	}
	for _, raw := range args[1:] {
		c.srv.watch.watch(c, c.dbIndex, string(raw))
	}
	c.replyOK()
}

func cmdUnwatch(c *client, args [][]byte) {
	c.srv.watch.unwatchAll(c)
	c.dirtyWatch.Store(false)
	c.replyOK()
}

// cmdExec runs the queued commands as one unit.
func cmdExec(c *client, args [][]byte) {
	if !c.inMulti {
		c.replyError("ERR EXEC without MULTI")
		return
	}

	// A command that failed to queue poisons the whole transaction: running
	// the rest would apply a partial version of what the client asked for.
	if c.queueError {
		c.resetTransaction()
		c.replyError("EXECABORT Transaction discarded because of previous errors.")
		return
	}

	// Hold the exclusive lock for the whole batch so no other client can
	// interleave. This is the guarantee MULTI/EXEC actually sells.
	//
	// The lock is taken *before* the watch check, not after. Checking first
	// would leave a window between the check and the lock in which another
	// client could still write a watched key, and the transaction would then
	// commit on data it had already been told was stale -- exactly the lost
	// update WATCH exists to prevent.
	c.srv.execMu.Lock()
	defer c.srv.execMu.Unlock()

	// A watched key changed under us. The reply is a null array, which is how
	// a client distinguishes "aborted, retry" from "ran and produced nothing".
	if c.dirtyWatch.Load() {
		c.resetTransaction()
		c.replyNullArray()
		return
	}

	queued := c.queued
	c.resetTransaction()

	// The queued commands run through the same execute path as any other, and
	// that path takes the shared lock. Taking it again underneath the
	// exclusive one would deadlock, since a Go RWMutex is not reentrant, so
	// this flag tells execute the lock is already held.
	c.inExec = true
	defer func() { c.inExec = false }()

	c.replyArrayLen(len(queued))
	for _, cmd := range queued {
		// Errors inside a transaction are reported per command and do not
		// stop the rest: by the time EXEC runs, the client has already been
		// told each command was accepted.
		c.srv.execute(c, cmd)
	}
}

// resetTransaction clears queued state and releases watches.
func (c *client) resetTransaction() {
	c.inMulti = false
	c.queued = nil
	c.queueError = false
	c.srv.watch.unwatchAll(c)
	c.dirtyWatch.Store(false)
}

// commandsNotQueued run immediately even inside MULTI, because they control
// the transaction itself.
var commandsNotQueued = map[string]bool{
	"multi":   true,
	"exec":    true,
	"discard": true,
	"watch":   true,
	"reset":   true,
	"quit":    true,
}

// queueCommand adds a command to the pending transaction, validating it now so
// a typo is reported at queue time rather than silently at EXEC.
func (c *client) queueCommand(name string, args [][]byte) {
	spec, ok := commands[name]
	if !ok {
		c.queueError = true
		c.replyErrorf("ERR unknown command '%s', with args beginning with: %s",
			string(args[0]), quotedArgs(args[1:]))
		return
	}
	if !spec.matchesArity(len(args)) {
		c.queueError = true
		c.replyWrongArity(name)
		return
	}

	// Copy the arguments: they point into the connection's read buffer, which
	// is reused by the time EXEC runs.
	copied := make([][]byte, len(args))
	for i, a := range args {
		copied[i] = append([]byte(nil), a...)
	}
	c.queued = append(c.queued, copied)
	c.replySimple("QUEUED")
}

// transactionInfo renders the INFO clients fields about open transactions.
func (s *Server) blockedTransactionCount() int {
	count := 0
	for _, c := range s.snapshotClients() {
		if c.inMulti {
			count++
		}
	}
	return count
}

// isTransactionCommand reports whether a name belongs to the transaction
// family, for the subscribe-mode check.
func isTransactionCommand(name string) bool {
	switch strings.ToLower(name) {
	case "multi", "exec", "discard", "watch", "unwatch":
		return true
	}
	return false
}
