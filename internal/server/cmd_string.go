package server

import (
	"math"
	"strconv"
	"strings"

	"github.com/Nafiz001/gedis/internal/store"
)

// maxStringLength matches the Redis limit on a single string value.
const maxStringLength = 512 * 1024 * 1024

// setOptions holds the parsed modifiers of SET and GETEX.
type setOptions struct {
	nx       bool // only set if the key does not exist
	xx       bool // only set if the key already exists
	get      bool // return the old value
	keepTTL  bool // leave any existing deadline alone
	persist  bool // GETEX only: drop the deadline
	expireAt int64
	hasTTL   bool
}

// parseExpiry converts one expiry modifier into an absolute deadline in
// milliseconds.
//
// EX/PX are relative to now; EXAT/PXAT are already absolute. The overflow check
// matters: without it, "SET k v EX 9223372036854775807" would wrap to a
// deadline in the past and delete the key it was meant to keep alive.
func parseExpiry(unit string, n int64, now int64) (int64, bool) {
	const maxMillis = math.MaxInt64 / 1000

	switch unit {
	case "ex":
		if n <= 0 || n > maxMillis {
			return 0, false
		}
		ms := n * 1000
		if now > math.MaxInt64-ms {
			return 0, false
		}
		return now + ms, true
	case "px":
		if n <= 0 {
			return 0, false
		}
		if now > math.MaxInt64-n {
			return 0, false
		}
		return now + n, true
	case "exat":
		if n <= 0 || n > maxMillis {
			return 0, false
		}
		return n * 1000, true
	case "pxat":
		if n <= 0 {
			return 0, false
		}
		return n, true
	default:
		return 0, false
	}
}

// parseSetOptions reads the modifier arguments of SET.
// It writes the error reply itself and returns false when the syntax is wrong.
func (c *client) parseSetOptions(args [][]byte, cmdName string) (setOptions, bool) {
	var opts setOptions
	now := c.db.Now()

	for i := 0; i < len(args); i++ {
		switch flag := strings.ToLower(string(args[i])); flag {
		case "nx":
			if opts.xx {
				c.replySyntaxError()
				return opts, false
			}
			opts.nx = true
		case "xx":
			if opts.nx {
				c.replySyntaxError()
				return opts, false
			}
			opts.xx = true
		case "get":
			opts.get = true
		case "keepttl":
			if opts.hasTTL {
				c.replySyntaxError()
				return opts, false
			}
			opts.keepTTL = true
		case "persist":
			if opts.hasTTL {
				c.replySyntaxError()
				return opts, false
			}
			opts.persist = true
		case "ex", "px", "exat", "pxat":
			// Only one expiry modifier is allowed, and it needs a value.
			if opts.hasTTL || opts.keepTTL || opts.persist || i+1 >= len(args) {
				c.replySyntaxError()
				return opts, false
			}
			i++
			n, err := strconv.ParseInt(string(args[i]), 10, 64)
			if err != nil {
				c.replyNotInteger()
				return opts, false
			}
			at, ok := parseExpiry(flag, n, now)
			if !ok {
				c.replyErrorf("ERR invalid expire time in '%s' command", cmdName)
				return opts, false
			}
			opts.expireAt = at
			opts.hasTTL = true
		default:
			c.replySyntaxError()
			return opts, false
		}
	}
	return opts, true
}

// cmdSet implements SET key value [NX|XX] [GET] [EX s|PX ms|EXAT ts|PXAT ts|KEEPTTL].
//
// The whole thing runs inside a single store.Update so that the conditional
// (NX/XX), the read for GET, and the write are one atomic step. Doing the check
// with a separate Get would let another client slip in between and turn SET NX
// into a lost-update race -- the exact race SET NX exists to prevent.
func cmdSet(c *client, args [][]byte) {
	key := string(args[1])
	value := args[2]

	opts, ok := c.parseSetOptions(args[3:], "set")
	if !ok {
		return
	}
	if opts.persist {
		c.replySyntaxError()
		return
	}

	var (
		oldValue   []byte
		oldExisted bool
		didSet     bool
	)

	err := c.db.Update(key, func(cur *store.Entry) (*store.Entry, error) {
		// The GET modifier must fail on a non-string just like GET would.
		if cur != nil && opts.get {
			b, err := entryString(cur)
			if err != nil {
				return nil, err
			}
			oldValue, oldExisted = b, true
		} else if cur != nil {
			oldExisted = true
		}

		if (opts.nx && cur != nil) || (opts.xx && cur == nil) {
			return cur, nil // condition not met: leave the key untouched
		}

		expireAt := store.NoExpiry
		switch {
		case opts.hasTTL:
			expireAt = opts.expireAt
		case opts.keepTTL && cur != nil:
			expireAt = cur.ExpireAt
		}

		didSet = true
		return &store.Entry{Value: append([]byte(nil), value...), ExpireAt: expireAt}, nil
	})
	if err != nil {
		c.replyStoreError(err)
		return
	}

	// Fix up what reaches the log and the replicas. "SET k v EX 60" replayed
	// an hour from now would give the key another minute of life, so it is
	// rewritten to the absolute form; a SET that did not happen is dropped.
	switch {
	case !didSet:
		c.suppressPropagation()
	case opts.hasTTL:
		c.rewriteAs(buildCmd("SET").str(key).bytes(value).str("PXAT").int(opts.expireAt))
	}

	if opts.get {
		if oldExisted {
			c.replyBulk(oldValue)
		} else {
			c.replyNull()
		}
		return
	}
	if !didSet {
		// A rejected NX/XX replies with the null string, not an error.
		c.replyNull()
		return
	}
	c.replyOK()
}

func cmdGet(c *client, args [][]byte) {
	val, exists, ok := c.stringValue(string(args[1]))
	if !ok {
		return
	}
	if !exists {
		c.replyNull()
		return
	}
	c.replyBulk(val)
}

func cmdSetNX(c *client, args [][]byte) {
	key, value := string(args[1]), args[2]
	created := false
	err := c.db.Update(key, func(cur *store.Entry) (*store.Entry, error) {
		if cur != nil {
			return cur, nil
		}
		created = true
		return &store.Entry{Value: append([]byte(nil), value...)}, nil
	})
	if err != nil {
		c.replyStoreError(err)
		return
	}
	c.replyInt(boolToInt(created))
}

// cmdSetEX implements SETEX key seconds value.
func cmdSetEX(c *client, args [][]byte) { c.setWithTTL(args, "ex", "setex") }

// cmdPSetEX implements PSETEX key milliseconds value.
func cmdPSetEX(c *client, args [][]byte) { c.setWithTTL(args, "px", "psetex") }

func (c *client) setWithTTL(args [][]byte, unit, cmdName string) {
	n, ok := c.parseInt(args[2])
	if !ok {
		return
	}
	at, ok := parseExpiry(unit, n, c.db.Now())
	if !ok {
		c.replyErrorf("ERR invalid expire time in '%s' command", cmdName)
		return
	}
	c.db.Set(string(args[1]), append([]byte(nil), args[3]...), at)
	c.rewriteAs(buildCmd("SET").bytes(args[1]).bytes(args[3]).str("PXAT").int(at))
	c.replyOK()
}

func cmdGetSet(c *client, args [][]byte) {
	key, value := string(args[1]), args[2]
	var old []byte
	var existed bool

	err := c.db.Update(key, func(cur *store.Entry) (*store.Entry, error) {
		if cur != nil {
			b, err := entryString(cur)
			if err != nil {
				return nil, err
			}
			old, existed = b, true
		}
		// GETSET clears any TTL, which is what makes it different from
		// SET ... KEEPTTL.
		return &store.Entry{Value: append([]byte(nil), value...)}, nil
	})
	if err != nil {
		c.replyStoreError(err)
		return
	}
	if !existed {
		c.replyNull()
		return
	}
	c.replyBulk(old)
}

func cmdGetDel(c *client, args [][]byte) {
	key := string(args[1])
	var old []byte
	var existed bool

	err := c.db.Update(key, func(cur *store.Entry) (*store.Entry, error) {
		if cur == nil {
			return nil, nil
		}
		b, err := entryString(cur)
		if err != nil {
			return nil, err
		}
		old, existed = b, true
		return nil, nil // returning nil deletes
	})
	if err != nil {
		c.replyStoreError(err)
		return
	}
	if !existed {
		c.replyNull()
		return
	}
	c.replyBulk(old)
}

// cmdGetEx implements GETEX key [EX s|PX ms|EXAT ts|PXAT ts|PERSIST]:
// read the value and adjust its TTL in one atomic step.
func cmdGetEx(c *client, args [][]byte) {
	key := string(args[1])
	opts, ok := c.parseSetOptions(args[2:], "getex")
	if !ok {
		return
	}
	if opts.nx || opts.xx || opts.get || opts.keepTTL {
		c.replySyntaxError()
		return
	}

	var val []byte
	var existed bool
	err := c.db.Update(key, func(cur *store.Entry) (*store.Entry, error) {
		if cur == nil {
			return nil, nil
		}
		b, err := entryString(cur)
		if err != nil {
			return nil, err
		}
		val, existed = b, true

		switch {
		case opts.persist:
			cur.ExpireAt = store.NoExpiry
		case opts.hasTTL:
			cur.ExpireAt = opts.expireAt
		}
		return cur, nil
	})
	if err != nil {
		c.replyStoreError(err)
		return
	}
	switch {
	case !existed:
		c.suppressPropagation()
	case opts.persist:
		c.rewriteAs(buildCmd("PERSIST").str(key))
	case opts.hasTTL:
		c.rewriteAs(buildCmd("PEXPIREAT").str(key).int(opts.expireAt))
	default:
		// GETEX with no options is a plain read.
		c.suppressPropagation()
	}

	if !existed {
		c.replyNull()
		return
	}
	c.replyBulk(val)
}

func cmdMGet(c *client, args [][]byte) {
	c.replyArrayLen(len(args) - 1)
	for _, raw := range args[1:] {
		v, found := c.db.Get(string(raw))
		if !found {
			c.replyNull()
			continue
		}
		// MGET reports a wrong-typed key as nil rather than failing the whole
		// command, so one list in the batch does not hide every string.
		b, isString := v.([]byte)
		if !isString {
			c.replyNull()
			continue
		}
		c.replyBulk(b)
	}
}

func cmdMSet(c *client, args [][]byte) {
	pairs := args[1:]
	if len(pairs)%2 != 0 {
		c.replyWrongArity("mset")
		return
	}
	for i := 0; i < len(pairs); i += 2 {
		c.db.Set(string(pairs[i]), append([]byte(nil), pairs[i+1]...), store.NoExpiry)
	}
	c.replyOK()
}

// cmdMSetNX sets several keys only if none of them exist.
//
// True atomicity here would need every key locked at once, which invites
// deadlock across shards. Instead it checks all keys, then writes -- so a
// concurrent writer could create one of them in between. Redis gets this free
// by being single-threaded; the honest fix here is to route the command through
// a global lock, which is deferred until MULTI/EXEC needs the same machinery.
func cmdMSetNX(c *client, args [][]byte) {
	pairs := args[1:]
	if len(pairs)%2 != 0 {
		c.replyWrongArity("msetnx")
		return
	}
	for i := 0; i < len(pairs); i += 2 {
		if c.db.Exists(string(pairs[i])) {
			c.replyInt(0)
			return
		}
	}
	for i := 0; i < len(pairs); i += 2 {
		c.db.Set(string(pairs[i]), append([]byte(nil), pairs[i+1]...), store.NoExpiry)
	}
	c.replyInt(1)
}

func cmdAppend(c *client, args [][]byte) {
	key, suffix := string(args[1]), args[2]
	var newLen int

	err := c.db.Update(key, func(cur *store.Entry) (*store.Entry, error) {
		existing, err := entryString(cur)
		if err != nil {
			return nil, err
		}
		if len(existing)+len(suffix) > maxStringLength {
			return nil, errStringTooLong
		}
		if cur == nil {
			combined := append([]byte(nil), suffix...)
			newLen = len(combined)
			return &store.Entry{Value: combined}, nil
		}
		combined := append(existing, suffix...)
		cur.Value = combined
		newLen = len(combined)
		return cur, nil
	})
	if err != nil {
		c.replyStoreError(err)
		return
	}
	c.replyInt(int64(newLen))
}

func cmdStrlen(c *client, args [][]byte) {
	val, exists, ok := c.stringValue(string(args[1]))
	if !ok {
		return
	}
	if !exists {
		c.replyInt(0)
		return
	}
	c.replyInt(int64(len(val)))
}

// cmdSetRange overwrites part of a string, zero-padding if the offset is past
// the current end.
func cmdSetRange(c *client, args [][]byte) {
	key := string(args[1])
	offset, ok := c.parseInt(args[2])
	if !ok {
		return
	}
	if offset < 0 {
		c.replyError("ERR offset is out of range")
		return
	}
	value := args[3]

	if int64(len(value))+offset > maxStringLength {
		c.replyError("ERR string exceeds maximum allowed size (proto-max-bulk-len)")
		return
	}

	var newLen int
	err := c.db.Update(key, func(cur *store.Entry) (*store.Entry, error) {
		existing, err := entryString(cur)
		if err != nil {
			return nil, err
		}
		// Writing nothing to a missing key must not create it.
		if len(value) == 0 {
			newLen = len(existing)
			return cur, nil
		}

		need := int(offset) + len(value)
		buf := existing
		if len(buf) < need {
			grown := make([]byte, need)
			copy(grown, buf)
			buf = grown
		}
		copy(buf[offset:], value)
		newLen = len(buf)

		if cur == nil {
			return &store.Entry{Value: buf}, nil
		}
		cur.Value = buf
		return cur, nil
	})
	if err != nil {
		c.replyStoreError(err)
		return
	}
	c.replyInt(int64(newLen))
}

// cmdGetRange returns a substring by inclusive start and end offsets, either of
// which may be negative to count back from the end.
func cmdGetRange(c *client, args [][]byte) {
	start, ok := c.parseInt(args[2])
	if !ok {
		return
	}
	end, ok := c.parseInt(args[3])
	if !ok {
		return
	}
	val, exists, ok := c.stringValue(string(args[1]))
	if !ok {
		return
	}
	if !exists || len(val) == 0 {
		c.replyBulkString("")
		return
	}

	from, to, empty := resolveRange(start, end, len(val))
	if empty {
		c.replyBulkString("")
		return
	}
	c.replyBulk(val[from : to+1])
}

// resolveRange turns possibly-negative inclusive offsets into a concrete slice
// range, reporting whether the result is empty.
func resolveRange(start, end int64, length int) (from, to int, empty bool) {
	n := int64(length)
	if start < 0 {
		start += n
	}
	if end < 0 {
		end += n
	}
	if start < 0 {
		start = 0
	}
	if end < 0 {
		end = 0
	}
	if end >= n {
		end = n - 1
	}
	if start > end || start >= n {
		return 0, 0, true
	}
	return int(start), int(end), false
}

// --- counters ---

func cmdIncr(c *client, args [][]byte) { c.incrementBy(string(args[1]), 1) }
func cmdDecr(c *client, args [][]byte) { c.incrementBy(string(args[1]), -1) }

func cmdIncrBy(c *client, args [][]byte) {
	delta, ok := c.parseInt(args[2])
	if !ok {
		return
	}
	c.incrementBy(string(args[1]), delta)
}

func cmdDecrBy(c *client, args [][]byte) {
	delta, ok := c.parseInt(args[2])
	if !ok {
		return
	}
	// Negating the most negative int64 overflows, so reject it explicitly
	// rather than silently decrementing by a positive number.
	if delta == math.MinInt64 {
		c.replyError("ERR decrement would overflow")
		return
	}
	c.incrementBy(string(args[1]), -delta)
}

// incrementBy performs the whole read-parse-add-write cycle under one shard
// lock, which is what makes INCR usable as a distributed counter.
func (c *client) incrementBy(key string, delta int64) {
	var result int64

	err := c.db.Update(key, func(cur *store.Entry) (*store.Entry, error) {
		existing, err := entryString(cur)
		if err != nil {
			return nil, err
		}

		var n int64
		if cur != nil {
			// Redis is strict here: a value with leading spaces or a float is
			// not an integer, so ParseInt is exactly the right check.
			n, err = strconv.ParseInt(string(existing), 10, 64)
			if err != nil {
				return nil, errNotInteger
			}
		}

		// Signed overflow is undefined-ish territory in most languages and
		// wraps silently in Go, so check before adding.
		if (delta > 0 && n > math.MaxInt64-delta) || (delta < 0 && n < math.MinInt64-delta) {
			return nil, errIncrOverflow
		}
		result = n + delta

		encoded := strconv.AppendInt(make([]byte, 0, 20), result, 10)
		if cur == nil {
			return &store.Entry{Value: encoded}, nil
		}
		cur.Value = encoded
		return cur, nil
	})
	if err != nil {
		c.replyStoreError(err)
		return
	}
	c.replyInt(result)
}

func cmdIncrByFloat(c *client, args [][]byte) {
	delta, err := strconv.ParseFloat(string(args[2]), 64)
	if err != nil || math.IsNaN(delta) || math.IsInf(delta, 0) {
		c.replyNotFloat()
		return
	}
	key := string(args[1])
	var result float64

	updateErr := c.db.Update(key, func(cur *store.Entry) (*store.Entry, error) {
		existing, err := entryString(cur)
		if err != nil {
			return nil, err
		}
		var n float64
		if cur != nil {
			n, err = strconv.ParseFloat(string(existing), 64)
			if err != nil {
				return nil, errNotFloat
			}
		}
		result = n + delta
		if math.IsNaN(result) || math.IsInf(result, 0) {
			return nil, errNaNOrInf
		}

		// 'f' with precision -1 gives the shortest representation that round
		// trips, and never switches to exponent form -- which matters because
		// the result is stored as a string and must parse back identically.
		encoded := []byte(strconv.FormatFloat(result, 'f', -1, 64))
		if cur == nil {
			return &store.Entry{Value: encoded}, nil
		}
		cur.Value = encoded
		return cur, nil
	})
	if updateErr != nil {
		c.replyStoreError(updateErr)
		return
	}
	encoded := strconv.FormatFloat(result, 'f', -1, 64)
	// Propagate the outcome, not the operation: float arithmetic is not
	// guaranteed bit-identical across builds, so replaying the addition could
	// drift from replaying the answer. Redis rewrites this the same way.
	c.rewriteAs(buildCmd("SET").str(key).str(encoded))
	c.replyBulkString(encoded)
}

func boolToInt(b bool) int64 {
	if b {
		return 1
	}
	return 0
}
