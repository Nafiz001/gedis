package server

import (
	"errors"
	"strconv"
	"strings"

	"github.com/Nafiz001/gedis/internal/store"
)

// handlerFunc executes one command. args includes the command name at index 0,
// exactly as Redis passes it, so handlers that accept subcommands can look at
// args[1] without an off-by-one.
type handlerFunc func(c *client, args [][]byte)

// commandSpec describes a command in the dispatch table.
type commandSpec struct {
	name    string
	handler handlerFunc

	// arity follows the Redis convention: a positive value is the exact number
	// of arguments including the command name, a negative value is the minimum.
	// So GET is 2 and MGET is -2.
	arity int

	// write marks commands that mutate the keyspace. Nothing branches on it
	// yet; it is what AOF persistence and replication will filter on, and
	// recording it at definition time is cheaper than retrofitting it later.
	write bool

	// noAuth marks the few commands allowed before AUTH succeeds.
	noAuth bool

	// denyOOM marks commands that may allocate, and so are the ones refused
	// once memory is full. It is deliberately narrower than write: DEL,
	// EXPIRE and LPOP are writes that only free memory, and refusing those
	// would leave a server that hit its limit with no way back under it.
	denyOOM bool
}

// matchesArity reports whether n arguments satisfy the spec.
func (spec commandSpec) matchesArity(n int) bool {
	if spec.arity >= 0 {
		return n == spec.arity
	}
	return n >= -spec.arity
}

// commands is the dispatch table, keyed by lowercase command name.
//
// It is built once at init and never written afterwards, so concurrent
// dispatch from every client goroutine needs no lock.
var commands = map[string]commandSpec{}

func register(specs ...commandSpec) {
	for _, spec := range specs {
		if _, dup := commands[spec.name]; dup {
			panic("gedis: duplicate command registration: " + spec.name)
		}
		commands[spec.name] = spec
	}
}

func init() {
	register(
		// --- connection ---
		commandSpec{name: "ping", handler: cmdPing, arity: -1, noAuth: false},
		commandSpec{name: "echo", handler: cmdEcho, arity: 2},
		commandSpec{name: "quit", handler: cmdQuit, arity: -1, noAuth: true},
		commandSpec{name: "select", handler: cmdSelect, arity: 2},
		commandSpec{name: "auth", handler: cmdAuth, arity: -2, noAuth: true},
		commandSpec{name: "hello", handler: cmdHello, arity: -1, noAuth: true},
		commandSpec{name: "reset", handler: cmdReset, arity: 1, noAuth: true},
		commandSpec{name: "client", handler: cmdClient, arity: -2},
		commandSpec{name: "command", handler: cmdCommand, arity: -1, noAuth: true},

		// --- strings ---
		commandSpec{name: "set", handler: cmdSet, arity: -3, write: true},
		commandSpec{name: "setnx", handler: cmdSetNX, arity: 3, write: true},
		commandSpec{name: "setex", handler: cmdSetEX, arity: 4, write: true},
		commandSpec{name: "psetex", handler: cmdPSetEX, arity: 4, write: true},
		commandSpec{name: "get", handler: cmdGet, arity: 2},
		commandSpec{name: "getset", handler: cmdGetSet, arity: 3, write: true},
		commandSpec{name: "getdel", handler: cmdGetDel, arity: 2, write: true},
		commandSpec{name: "getex", handler: cmdGetEx, arity: -2, write: true},
		commandSpec{name: "mget", handler: cmdMGet, arity: -2},
		commandSpec{name: "mset", handler: cmdMSet, arity: -3, write: true},
		commandSpec{name: "msetnx", handler: cmdMSetNX, arity: -3, write: true},
		commandSpec{name: "append", handler: cmdAppend, arity: 3, write: true},
		commandSpec{name: "strlen", handler: cmdStrlen, arity: 2},
		commandSpec{name: "setrange", handler: cmdSetRange, arity: 4, write: true},
		commandSpec{name: "getrange", handler: cmdGetRange, arity: 4},
		commandSpec{name: "substr", handler: cmdGetRange, arity: 4},
		commandSpec{name: "incr", handler: cmdIncr, arity: 2, write: true},
		commandSpec{name: "decr", handler: cmdDecr, arity: 2, write: true},
		commandSpec{name: "incrby", handler: cmdIncrBy, arity: 3, write: true},
		commandSpec{name: "decrby", handler: cmdDecrBy, arity: 3, write: true},
		commandSpec{name: "incrbyfloat", handler: cmdIncrByFloat, arity: 3, write: true},

		// --- lists ---
		commandSpec{name: "lpush", handler: cmdLPush, arity: -3, write: true},
		commandSpec{name: "rpush", handler: cmdRPush, arity: -3, write: true},
		commandSpec{name: "lpushx", handler: cmdLPushX, arity: -3, write: true},
		commandSpec{name: "rpushx", handler: cmdRPushX, arity: -3, write: true},
		commandSpec{name: "lpop", handler: cmdLPop, arity: -2, write: true},
		commandSpec{name: "rpop", handler: cmdRPop, arity: -2, write: true},
		commandSpec{name: "llen", handler: cmdLLen, arity: 2},
		commandSpec{name: "lrange", handler: cmdLRange, arity: 4},
		commandSpec{name: "lindex", handler: cmdLIndex, arity: 3},
		commandSpec{name: "lset", handler: cmdLSet, arity: 4, write: true},
		commandSpec{name: "linsert", handler: cmdLInsert, arity: 5, write: true},
		commandSpec{name: "lrem", handler: cmdLRem, arity: 4, write: true},
		commandSpec{name: "ltrim", handler: cmdLTrim, arity: 4, write: true},
		commandSpec{name: "lpos", handler: cmdLPos, arity: -3},
		commandSpec{name: "lmove", handler: cmdLMove, arity: 5, write: true},
		commandSpec{name: "rpoplpush", handler: cmdLMove, arity: 3, write: true},

		// --- hashes ---
		commandSpec{name: "hset", handler: cmdHSet, arity: -4, write: true},
		commandSpec{name: "hmset", handler: cmdHSet, arity: -4, write: true},
		commandSpec{name: "hsetnx", handler: cmdHSetNX, arity: 4, write: true},
		commandSpec{name: "hget", handler: cmdHGet, arity: 3},
		commandSpec{name: "hmget", handler: cmdHMGet, arity: -3},
		commandSpec{name: "hdel", handler: cmdHDel, arity: -3, write: true},
		commandSpec{name: "hlen", handler: cmdHLen, arity: 2},
		commandSpec{name: "hexists", handler: cmdHExists, arity: 3},
		commandSpec{name: "hstrlen", handler: cmdHStrlen, arity: 3},
		commandSpec{name: "hkeys", handler: cmdHKeys, arity: 2},
		commandSpec{name: "hvals", handler: cmdHVals, arity: 2},
		commandSpec{name: "hgetall", handler: cmdHGetAll, arity: 2},
		commandSpec{name: "hincrby", handler: cmdHIncrBy, arity: 4, write: true},
		commandSpec{name: "hincrbyfloat", handler: cmdHIncrByFloat, arity: 4, write: true},
		commandSpec{name: "hrandfield", handler: cmdHRandField, arity: -2},
		commandSpec{name: "hscan", handler: cmdHScan, arity: -3},

		// --- sets ---
		commandSpec{name: "sadd", handler: cmdSAdd, arity: -3, write: true},
		commandSpec{name: "srem", handler: cmdSRem, arity: -3, write: true},
		commandSpec{name: "scard", handler: cmdSCard, arity: 2},
		commandSpec{name: "sismember", handler: cmdSIsMember, arity: 3},
		commandSpec{name: "smismember", handler: cmdSMIsMember, arity: -3},
		commandSpec{name: "smembers", handler: cmdSMembers, arity: 2},
		commandSpec{name: "spop", handler: cmdSPop, arity: -2, write: true},
		commandSpec{name: "srandmember", handler: cmdSRandMember, arity: -2},
		commandSpec{name: "smove", handler: cmdSMove, arity: 4, write: true},
		commandSpec{name: "sinter", handler: cmdSInter, arity: -2},
		commandSpec{name: "sunion", handler: cmdSUnion, arity: -2},
		commandSpec{name: "sdiff", handler: cmdSDiff, arity: -2},
		commandSpec{name: "sinterstore", handler: cmdSInterStore, arity: -3, write: true},
		commandSpec{name: "sunionstore", handler: cmdSUnionStore, arity: -3, write: true},
		commandSpec{name: "sdiffstore", handler: cmdSDiffStore, arity: -3, write: true},
		commandSpec{name: "sintercard", handler: cmdSInterCard, arity: -3},
		commandSpec{name: "sscan", handler: cmdSScan, arity: -3},

		// --- sorted sets ---
		commandSpec{name: "zadd", handler: cmdZAdd, arity: -4, write: true},
		commandSpec{name: "zscore", handler: cmdZScore, arity: 3},
		commandSpec{name: "zmscore", handler: cmdZMScore, arity: -3},
		commandSpec{name: "zcard", handler: cmdZCard, arity: 2},
		commandSpec{name: "zcount", handler: cmdZCount, arity: 4},
		commandSpec{name: "zlexcount", handler: cmdZLexCount, arity: 4},
		commandSpec{name: "zincrby", handler: cmdZIncrBy, arity: 4, write: true},
		commandSpec{name: "zrank", handler: cmdZRank, arity: 3},
		commandSpec{name: "zrevrank", handler: cmdZRevRank, arity: 3},
		commandSpec{name: "zrange", handler: cmdZRange, arity: -4},
		commandSpec{name: "zrevrange", handler: cmdZRange, arity: -4},
		commandSpec{name: "zrangebyscore", handler: cmdZRange, arity: -4},
		commandSpec{name: "zrevrangebyscore", handler: cmdZRange, arity: -4},
		commandSpec{name: "zrangebylex", handler: cmdZRange, arity: -4},
		commandSpec{name: "zrevrangebylex", handler: cmdZRange, arity: -4},
		commandSpec{name: "zrem", handler: cmdZRem, arity: -3, write: true},
		commandSpec{name: "zremrangebyrank", handler: cmdZRemRangeByRank, arity: 4, write: true},
		commandSpec{name: "zremrangebyscore", handler: cmdZRemRangeByScore, arity: 4, write: true},
		commandSpec{name: "zremrangebylex", handler: cmdZRemRangeByLex, arity: 4, write: true},
		commandSpec{name: "zpopmin", handler: cmdZPopMin, arity: -2, write: true},
		commandSpec{name: "zpopmax", handler: cmdZPopMax, arity: -2, write: true},
		commandSpec{name: "zrandmember", handler: cmdZRandMember, arity: -2},
		commandSpec{name: "zscan", handler: cmdZScan, arity: -3},
		commandSpec{name: "zunion", handler: cmdZSetOp, arity: -3},
		commandSpec{name: "zinter", handler: cmdZSetOp, arity: -3},
		commandSpec{name: "zdiff", handler: cmdZSetOp, arity: -3},
		commandSpec{name: "zunionstore", handler: cmdZSetOp, arity: -4, write: true},
		commandSpec{name: "zinterstore", handler: cmdZSetOp, arity: -4, write: true},
		commandSpec{name: "zdiffstore", handler: cmdZSetOp, arity: -4, write: true},

		// --- keyspace ---
		commandSpec{name: "del", handler: cmdDel, arity: -2, write: true},
		commandSpec{name: "unlink", handler: cmdDel, arity: -2, write: true},
		commandSpec{name: "exists", handler: cmdExists, arity: -2},
		commandSpec{name: "type", handler: cmdType, arity: 2},
		commandSpec{name: "keys", handler: cmdKeys, arity: 2},
		commandSpec{name: "scan", handler: cmdScan, arity: -2},
		commandSpec{name: "randomkey", handler: cmdRandomKey, arity: 1},
		commandSpec{name: "rename", handler: cmdRename, arity: 3, write: true},
		commandSpec{name: "renamenx", handler: cmdRenameNX, arity: 3, write: true},
		commandSpec{name: "copy", handler: cmdCopy, arity: -3, write: true},
		commandSpec{name: "expire", handler: cmdExpire, arity: -3, write: true},
		commandSpec{name: "pexpire", handler: cmdPExpire, arity: -3, write: true},
		commandSpec{name: "expireat", handler: cmdExpireAt, arity: -3, write: true},
		commandSpec{name: "pexpireat", handler: cmdPExpireAt, arity: -3, write: true},
		commandSpec{name: "ttl", handler: cmdTTL, arity: 2},
		commandSpec{name: "pttl", handler: cmdPTTL, arity: 2},
		commandSpec{name: "expiretime", handler: cmdExpireTime, arity: 2},
		commandSpec{name: "pexpiretime", handler: cmdPExpireTime, arity: 2},
		commandSpec{name: "persist", handler: cmdPersist, arity: 2, write: true},

		// --- pub/sub ---
		commandSpec{name: "subscribe", handler: cmdSubscribe, arity: -2},
		commandSpec{name: "unsubscribe", handler: cmdUnsubscribe, arity: -1},
		commandSpec{name: "psubscribe", handler: cmdPSubscribe, arity: -2},
		commandSpec{name: "punsubscribe", handler: cmdPUnsubscribe, arity: -1},
		commandSpec{name: "publish", handler: cmdPublish, arity: 3},
		commandSpec{name: "pubsub", handler: cmdPubSub, arity: -2},

		// --- transactions ---
		commandSpec{name: "multi", handler: cmdMulti, arity: 1},
		commandSpec{name: "exec", handler: cmdExec, arity: 1},
		commandSpec{name: "discard", handler: cmdDiscard, arity: 1},
		commandSpec{name: "watch", handler: cmdWatch, arity: -2},
		commandSpec{name: "unwatch", handler: cmdUnwatch, arity: 1},

		// --- persistence ---
		commandSpec{name: "save", handler: cmdSave, arity: 1},
		commandSpec{name: "bgsave", handler: cmdBGSave, arity: -1},
		commandSpec{name: "bgrewriteaof", handler: cmdBGRewriteAOF, arity: 1},
		commandSpec{name: "lastsave", handler: cmdLastSave, arity: 1},

		// --- replication ---
		commandSpec{name: "replicaof", handler: cmdReplicaOf, arity: 3},
		commandSpec{name: "slaveof", handler: cmdReplicaOf, arity: 3},
		commandSpec{name: "replconf", handler: cmdReplConf, arity: -1},
		commandSpec{name: "psync", handler: cmdPSync, arity: -1},
		commandSpec{name: "sync", handler: cmdPSync, arity: -1},
		commandSpec{name: "wait", handler: cmdWait, arity: 3},

		// --- server ---
		commandSpec{name: "dbsize", handler: cmdDBSize, arity: 1},
		commandSpec{name: "flushdb", handler: cmdFlushDB, arity: -1, write: true},
		commandSpec{name: "flushall", handler: cmdFlushAll, arity: -1, write: true},
		commandSpec{name: "info", handler: cmdInfo, arity: -1},
		commandSpec{name: "time", handler: cmdTime, arity: 1},
		commandSpec{name: "config", handler: cmdConfig, arity: -2},
		commandSpec{name: "debug", handler: cmdDebug, arity: -2},
		commandSpec{name: "shutdown", handler: cmdShutdown, arity: -1},
	)

	markDenyOOM()
}

// denyOOMCommands are the commands that can grow the keyspace.
//
// Everything absent from this list still runs when memory is full, which is
// what makes an OOM condition recoverable: a client can DEL, EXPIRE, LPOP or
// FLUSHDB its way back under the limit.
var denyOOMCommands = []string{
	// strings and counters
	"set", "setnx", "setex", "psetex", "getset", "mset", "msetnx",
	"append", "setrange", "incr", "decr", "incrby", "decrby", "incrbyfloat",
	// lists
	"lpush", "rpush", "lpushx", "rpushx", "linsert", "lset",
	"lmove", "rpoplpush",
	// hashes
	"hset", "hmset", "hsetnx", "hincrby", "hincrbyfloat",
	// sets
	"sadd", "smove", "sinterstore", "sunionstore", "sdiffstore",
	// sorted sets
	"zadd", "zincrby", "zunionstore", "zinterstore", "zdiffstore",
	// keyspace
	"copy", "rename", "renamenx",
}

func markDenyOOM() {
	for _, name := range denyOOMCommands {
		spec, ok := commands[name]
		if !ok {
			panic("gedis: denyOOM names an unregistered command: " + name)
		}
		if !spec.write {
			panic("gedis: denyOOM names a non-write command: " + name)
		}
		spec.denyOOM = true
		commands[name] = spec
	}
}

// dispatch is the entry point for one command from a client.
//
// It handles the two states that change what a command means -- an open
// transaction queues instead of running -- and otherwise hands off to execute.
func (s *Server) dispatch(c *client, args [][]byte) {
	name := strings.ToLower(string(args[0]))
	c.lastCommand = name

	// Inside MULTI everything is queued except the commands that steer the
	// transaction itself.
	if c.inMulti && !commandsNotQueued[name] {
		c.queueCommand(name, args)
		return
	}
	s.execute(c, args)
}

// execute validates and runs one command, then feeds it to the write stream.
func (s *Server) execute(c *client, args [][]byte) {
	name := strings.ToLower(string(args[0]))
	c.lastCommand = name

	spec, ok := commands[name]
	if !ok {
		// Redis echoes the arguments back so a typo is easy to spot in a log.
		c.replyErrorf("ERR unknown command '%s', with args beginning with: %s",
			string(args[0]), quotedArgs(args[1:]))
		return
	}
	if !spec.matchesArity(len(args)) {
		c.replyWrongArity(name)
		return
	}
	if !c.authenticated && !spec.noAuth {
		c.replyError("NOAUTH Authentication required.")
		return
	}

	// A RESP2 connection with subscriptions is a one-way message stream, so
	// only a small set of commands still makes sense on it.
	if c.inSubscribeMode() && !commandsAllowedWhileSubscribed[name] {
		c.replyErrorf("ERR Can't execute '%s': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context", name)
		return
	}

	// A replica serves reads and refuses writes, so the master stays the one
	// place data changes. Writes arriving from the master itself are exempt.
	if spec.write && !c.fromMaster && !c.internal && s.isReplica() {
		c.replyError("READONLY You can't write against a read only replica.")
		return
	}

	// Commands that may allocate have to fit. Under a maxmemory policy this
	// evicts to make room; under noeviction it is where the write is refused.
	// Commands that only free memory are exempt, so a full server can still be
	// emptied.
	if spec.denyOOM && s.cfg.MaxMemory > 0 && !s.enforceMemoryLimit() {
		c.replyError("OOM command not allowed when used memory > 'maxmemory'.")
		return
	}

	c.propagateOverride = nil
	c.noPropagate = false

	// Ordinary commands take the shared lock; EXEC takes it exclusively so a
	// transaction runs with the keyspace to itself. Commands running inside an
	// EXEC already hold it and must not take it again.
	if !c.inExec && name != "exec" {
		s.execMu.RLock()
		spec.handler(c, args)
		s.execMu.RUnlock()
	} else {
		spec.handler(c, args)
	}
	s.stats.commandsProcessed.Add(1)

	// Writes replayed from disk at startup are already in the log; feeding
	// them back would double every command on the next restart.
	if spec.write && !s.loading.Load() {
		cmd := args
		if c.propagateOverride != nil {
			cmd = c.propagateOverride
		}
		if !c.noPropagate {
			s.propagate(c.dbIndex, cmd)
			s.dirty.Add(1)
		}
	}
}

// quotedArgs renders arguments the way Redis does in its unknown-command error.
func quotedArgs(args [][]byte) string {
	if len(args) == 0 {
		return ""
	}
	var b strings.Builder
	for i, a := range args {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(strconv.Quote(string(a)))
	}
	return b.String()
}

// --- argument parsing helpers ---

// parseInt parses an integer argument, replying with the standard error and
// returning false if it is malformed.
func (c *client) parseInt(arg []byte) (int64, bool) {
	n, err := strconv.ParseInt(string(arg), 10, 64)
	if err != nil {
		c.replyNotInteger()
		return 0, false
	}
	return n, true
}

// stringValue fetches key as a string, distinguishing "absent" from "present
// but holding another type". It writes the WRONGTYPE error itself and returns
// ok=false in that case, since every caller handles it identically.
func (c *client) stringValue(key string) (val []byte, exists bool, ok bool) {
	v, found := c.db.Get(key)
	if !found {
		return nil, false, true
	}
	b, isString := v.([]byte)
	if !isString {
		c.replyWrongType()
		return nil, true, false
	}
	return b, true, true
}

// entryString extracts the string payload of an entry held under a store lock,
// for use inside store.Update callbacks.
func entryString(e *store.Entry) ([]byte, error) {
	if e == nil {
		return nil, nil
	}
	b, ok := e.Value.([]byte)
	if !ok {
		return nil, store.ErrWrongType
	}
	return b, nil
}

// replyStoreError turns an error raised inside a store.Update callback into
// the right RESP error reply.
func (c *client) replyStoreError(err error) {
	var re respError
	if errors.As(err, &re) {
		c.replyError(string(re))
		return
	}
	if errors.Is(err, store.ErrWrongType) {
		c.replyWrongType()
		return
	}
	c.replyErrorf("ERR %s", err.Error())
}

// --- collection scanning ---
//
// HSCAN, SSCAN and ZSCAN all page through one key's contents. They share the
// argument grammar and the cursor scheme below.

// parseCollectionScan reads the common "cursor [MATCH p] [COUNT n]" arguments.
// It writes the error reply itself and returns false when the syntax is wrong.
func (c *client) parseCollectionScan(args [][]byte) (cursor uint64, pattern string, count int, ok bool) {
	n, err := strconv.ParseUint(string(args[2]), 10, 64)
	if err != nil {
		c.replyError("ERR invalid cursor")
		return 0, "", 0, false
	}
	cursor, pattern, count = n, "*", 10

	for i := 3; i < len(args); i++ {
		if i+1 >= len(args) {
			c.replySyntaxError()
			return 0, "", 0, false
		}
		option := strings.ToLower(string(args[i]))
		i++
		switch option {
		case "match":
			pattern = string(args[i])
		case "count":
			v, valid := c.parseInt(args[i])
			if !valid {
				return 0, "", 0, false
			}
			if v < 1 {
				c.replySyntaxError()
				return 0, "", 0, false
			}
			count = int(v)
		default:
			c.replySyntaxError()
			return 0, "", 0, false
		}
	}
	return cursor, pattern, count, true
}

// pageCollection returns one page of a sorted name list and the cursor for the
// next call, with 0 meaning iteration is complete.
//
// The cursor is simply an offset into the sorted order. Sorting on every call
// costs O(n log n), which is the price of a stable position: Go maps randomise
// iteration, so an offset into raw map order would skip and repeat elements
// unpredictably. Redis avoids both by giving its hash table reverse-binary
// cursors, which is the proper fix and is noted as future work.
func pageCollection(sorted []string, cursor uint64, count int) (uint64, []string) {
	if cursor >= uint64(len(sorted)) {
		return 0, nil
	}
	start := int(cursor)
	end := start + count
	if end >= len(sorted) {
		return 0, sorted[start:]
	}
	return uint64(end), sorted[start:end]
}
