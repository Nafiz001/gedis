package server

import (
	"errors"
	"strconv"
	"strings"

	"github.com/nafizahmed/gedis/internal/store"
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
}

// dispatch validates and runs one command.
func (s *Server) dispatch(c *client, args [][]byte) {
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

	spec.handler(c, args)
	s.stats.commandsProcessed.Add(1)
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
