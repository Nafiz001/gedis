package server

import (
	"math"
	"strconv"
	"strings"

	"github.com/nafizahmed/gedis/internal/store"
)

func cmdDel(c *client, args [][]byte) {
	keys := make([]string, 0, len(args)-1)
	for _, a := range args[1:] {
		keys = append(keys, string(a))
	}
	c.replyInt(int64(c.db.Delete(keys...)))
}

func cmdExists(c *client, args [][]byte) {
	// EXISTS counts repeats: "EXISTS k k" on a live key returns 2.
	var n int64
	for _, a := range args[1:] {
		if c.db.Exists(string(a)) {
			n++
		}
	}
	c.replyInt(n)
}

func cmdType(c *client, args [][]byte) {
	v, ok := c.db.Get(string(args[1]))
	if !ok {
		c.replySimple("none")
		return
	}
	c.replySimple(store.TypeName(v))
}

func cmdKeys(c *client, args [][]byte) {
	c.replyStrings(c.db.Keys(string(args[1])))
}

// cmdScan implements SCAN cursor [MATCH pattern] [COUNT n] [TYPE t].
func cmdScan(c *client, args [][]byte) {
	cursor, err := strconv.ParseUint(string(args[1]), 10, 64)
	if err != nil {
		c.replyError("ERR invalid cursor")
		return
	}

	pattern := "*"
	count := 10
	typeFilter := ""

	for i := 2; i < len(args); i++ {
		switch strings.ToLower(string(args[i])) {
		case "match":
			if i+1 >= len(args) {
				c.replySyntaxError()
				return
			}
			i++
			pattern = string(args[i])
		case "count":
			if i+1 >= len(args) {
				c.replySyntaxError()
				return
			}
			i++
			n, ok := c.parseInt(args[i])
			if !ok {
				return
			}
			if n < 1 {
				c.replySyntaxError()
				return
			}
			count = int(n)
		case "type":
			if i+1 >= len(args) {
				c.replySyntaxError()
				return
			}
			i++
			typeFilter = strings.ToLower(string(args[i]))
		default:
			c.replySyntaxError()
			return
		}
	}

	next, keys := c.db.Scan(cursor, count, pattern)
	if typeFilter != "" {
		filtered := keys[:0]
		for _, k := range keys {
			if v, ok := c.db.Get(k); ok && store.TypeName(v) == typeFilter {
				filtered = append(filtered, k)
			}
		}
		keys = filtered
	}

	// SCAN replies with a two-element array: the next cursor as a string, then
	// the batch of keys.
	c.replyArrayLen(2)
	c.replyBulkString(strconv.FormatUint(next, 10))
	c.replyStrings(keys)
}

func cmdRandomKey(c *client, args [][]byte) {
	key, ok := c.db.RandomKey()
	if !ok {
		c.replyNull()
		return
	}
	c.replyBulkString(key)
}

func cmdRename(c *client, args [][]byte) {
	if !c.db.Rename(string(args[1]), string(args[2])) {
		c.replyError("ERR no such key")
		return
	}
	c.replyOK()
}

func cmdRenameNX(c *client, args [][]byte) {
	src, dst := string(args[1]), string(args[2])
	if !c.db.Exists(src) {
		c.replyError("ERR no such key")
		return
	}
	if c.db.Exists(dst) {
		c.replyInt(0)
		return
	}
	c.replyInt(boolToInt(c.db.Rename(src, dst)))
}

// cmdCopy implements COPY source destination [DB n] [REPLACE].
func cmdCopy(c *client, args [][]byte) {
	src, dst := string(args[1]), string(args[2])
	replace := false
	destDB := c.db

	for i := 3; i < len(args); i++ {
		switch strings.ToLower(string(args[i])) {
		case "replace":
			replace = true
		case "db":
			if i+1 >= len(args) {
				c.replySyntaxError()
				return
			}
			i++
			n, ok := c.parseInt(args[i])
			if !ok {
				return
			}
			target := c.srv.DB(int(n))
			if target == nil {
				c.replyError("ERR DB index is out of range")
				return
			}
			destDB = target
		default:
			c.replySyntaxError()
			return
		}
	}

	if destDB == c.db && src == dst {
		c.replyError("ERR source and destination objects are the same")
		return
	}
	v, ok := c.db.Get(src)
	if !ok {
		c.replyInt(0)
		return
	}
	if !replace && destDB.Exists(dst) {
		c.replyInt(0)
		return
	}

	ttl, _ := c.db.TTL(src)
	expireAt := store.NoExpiry
	if ttl > 0 {
		expireAt = destDB.Now() + ttl
	}
	// Copy the payload so the two keys do not share a backing array; otherwise
	// an APPEND to one would be visible through the other.
	if b, isString := v.([]byte); isString {
		v = append([]byte(nil), b...)
	}
	destDB.Set(dst, v, expireAt)
	c.replyInt(1)
}

// --- expiration ---

// expireHandler builds the handler for one of the four EXPIRE variants. They
// differ only in the unit and whether the argument is relative or absolute, so
// the flag parsing and the reply logic are written once.
func expireHandler(unit, cmdName string) handlerFunc {
	return func(c *client, args [][]byte) {
		n, ok := c.parseInt(args[2])
		if !ok {
			return
		}

		// NX/XX/GT/LT condition the change on the key's current deadline.
		var nx, xx, gt, lt bool
		for i := 3; i < len(args); i++ {
			switch strings.ToLower(string(args[i])) {
			case "nx":
				nx = true
			case "xx":
				xx = true
			case "gt":
				gt = true
			case "lt":
				lt = true
			default:
				c.replyErrorf("ERR Unsupported option %s", string(args[i]))
				return
			}
		}
		// NX means "only if no TTL", so it cannot be combined with the others.
		if (nx && (xx || gt || lt)) || (gt && lt) {
			c.replyError("ERR NX and XX, GT or LT options at the same time are not compatible")
			return
		}

		now := c.db.Now()
		var expireAt int64
		switch unit {
		case "ex":
			// Unlike SET, a non-positive EXPIRE is legal and deletes the key,
			// so the value is not range-checked the same way.
			if n > math.MaxInt64/1000 || n < math.MinInt64/1000 {
				c.replyErrorf("ERR invalid expire time in '%s' command", cmdName)
				return
			}
			expireAt = now + n*1000
		case "px":
			expireAt = now + n
		case "exat":
			if n > math.MaxInt64/1000 || n < math.MinInt64/1000 {
				c.replyErrorf("ERR invalid expire time in '%s' command", cmdName)
				return
			}
			expireAt = n * 1000
		case "pxat":
			expireAt = n
		}

		key := string(args[1])
		current, exists := c.db.TTL(key)
		if !exists {
			c.replyInt(0)
			return
		}
		hasTTL := current != -1

		switch {
		case nx && hasTTL:
			c.replyInt(0)
			return
		case xx && !hasTTL:
			c.replyInt(0)
			return
		case gt && (!hasTTL || expireAt <= now+current):
			// GT against a key with no TTL is always false: an absent deadline
			// counts as infinity.
			c.replyInt(0)
			return
		case lt && hasTTL && expireAt >= now+current:
			c.replyInt(0)
			return
		}

		c.replyInt(boolToInt(c.db.Expire(key, expireAt)))
	}
}

var (
	cmdExpire    = expireHandler("ex", "expire")
	cmdPExpire   = expireHandler("px", "pexpire")
	cmdExpireAt  = expireHandler("exat", "expireat")
	cmdPExpireAt = expireHandler("pxat", "pexpireat")
)

// ttlHandler builds TTL and PTTL, which differ only in unit.
//
// The reply codes are part of the protocol: -2 means the key does not exist,
// -1 means it exists with no deadline.
func ttlHandler(inSeconds bool) handlerFunc {
	return func(c *client, args [][]byte) {
		ms, exists := c.db.TTL(string(args[1]))
		if !exists {
			c.replyInt(-2)
			return
		}
		if ms == -1 {
			c.replyInt(-1)
			return
		}
		if inSeconds {
			// Round up, so a key with 1ms left reports 1 second rather than 0
			// and looks alive until it actually is not.
			c.replyInt((ms + 999) / 1000)
			return
		}
		c.replyInt(ms)
	}
}

var (
	cmdTTL  = ttlHandler(true)
	cmdPTTL = ttlHandler(false)
)

// expireTimeHandler builds EXPIRETIME and PEXPIRETIME, which report the
// absolute deadline rather than the remaining time.
func expireTimeHandler(inSeconds bool) handlerFunc {
	return func(c *client, args [][]byte) {
		key := string(args[1])
		ms, exists := c.db.TTL(key)
		if !exists {
			c.replyInt(-2)
			return
		}
		if ms == -1 {
			c.replyInt(-1)
			return
		}
		at := c.db.Now() + ms
		if inSeconds {
			c.replyInt(at / 1000)
			return
		}
		c.replyInt(at)
	}
}

var (
	cmdExpireTime  = expireTimeHandler(true)
	cmdPExpireTime = expireTimeHandler(false)
)

func cmdPersist(c *client, args [][]byte) {
	c.replyInt(boolToInt(c.db.Persist(string(args[1]))))
}
