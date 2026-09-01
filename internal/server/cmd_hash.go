package server

import (
	"math"
	"math/rand"
	"strconv"
	"strings"

	"github.com/nafizahmed/gedis/internal/store"
)

func entryHash(e *store.Entry) (*store.Hash, error) {
	if e == nil {
		return nil, nil
	}
	h, ok := e.Value.(*store.Hash)
	if !ok {
		return nil, store.ErrWrongType
	}
	return h, nil
}

// withHash mirrors withList: a missing key looks empty, and a hash that ends up
// empty is deleted so EXISTS reports 0.
func (c *client) withHash(key string, create bool, fn func(h *store.Hash) error) error {
	return c.db.Update(key, func(cur *store.Entry) (*store.Entry, error) {
		h, err := entryHash(cur)
		if err != nil {
			return nil, err
		}
		if h == nil {
			if !create {
				return cur, fn(nil)
			}
			h = store.NewHash()
		}
		if err := fn(h); err != nil {
			return nil, err
		}
		if h.Len() == 0 {
			return nil, nil
		}
		if cur == nil {
			return &store.Entry{Value: h}, nil
		}
		cur.Value = h
		return cur, nil
	})
}

// readHash fetches a hash for a read-only command, writing the WRONGTYPE reply
// itself. A missing key yields a nil hash, which every reader treats as empty.
func (c *client) readHash(key string) (*store.Hash, bool) {
	v, found := c.db.Get(key)
	if !found {
		return nil, true
	}
	h, ok := v.(*store.Hash)
	if !ok {
		c.replyWrongType()
		return nil, false
	}
	return h, true
}

// cmdHSet implements HSET and HMSET, which differ only in their reply: HSET
// counts the fields it created, HMSET just says OK.
func cmdHSet(c *client, args [][]byte) {
	pairs := args[2:]
	if len(pairs)%2 != 0 {
		c.replyWrongArity(strings.ToLower(string(args[0])))
		return
	}

	added := 0
	err := c.withHash(string(args[1]), true, func(h *store.Hash) error {
		for i := 0; i < len(pairs); i += 2 {
			if h.Set(string(pairs[i]), append([]byte(nil), pairs[i+1]...)) {
				added++
			}
		}
		return nil
	})
	if err != nil {
		c.replyStoreError(err)
		return
	}
	if strings.EqualFold(string(args[0]), "hmset") {
		c.replyOK()
		return
	}
	c.replyInt(int64(added))
}

func cmdHSetNX(c *client, args [][]byte) {
	created := false
	err := c.withHash(string(args[1]), true, func(h *store.Hash) error {
		created = h.SetNX(string(args[2]), append([]byte(nil), args[3]...))
		return nil
	})
	if err != nil {
		c.replyStoreError(err)
		return
	}
	c.replyInt(boolToInt(created))
}

func cmdHGet(c *client, args [][]byte) {
	h, ok := c.readHash(string(args[1]))
	if !ok {
		return
	}
	if h == nil {
		c.replyNull()
		return
	}
	v, found := h.Get(string(args[2]))
	if !found {
		c.replyNull()
		return
	}
	c.replyBulk(v)
}

func cmdHMGet(c *client, args [][]byte) {
	h, ok := c.readHash(string(args[1]))
	if !ok {
		return
	}
	c.replyArrayLen(len(args) - 2)
	for _, field := range args[2:] {
		if h == nil {
			c.replyNull()
			continue
		}
		v, found := h.Get(string(field))
		if !found {
			c.replyNull()
			continue
		}
		c.replyBulk(v)
	}
}

func cmdHDel(c *client, args [][]byte) {
	fields := make([]string, 0, len(args)-2)
	for _, f := range args[2:] {
		fields = append(fields, string(f))
	}

	removed := 0
	err := c.withHash(string(args[1]), false, func(h *store.Hash) error {
		if h != nil {
			removed = h.Delete(fields...)
		}
		return nil
	})
	if err != nil {
		c.replyStoreError(err)
		return
	}
	c.replyInt(int64(removed))
}

func cmdHLen(c *client, args [][]byte) {
	h, ok := c.readHash(string(args[1]))
	if !ok {
		return
	}
	if h == nil {
		c.replyInt(0)
		return
	}
	c.replyInt(int64(h.Len()))
}

func cmdHExists(c *client, args [][]byte) {
	h, ok := c.readHash(string(args[1]))
	if !ok {
		return
	}
	c.replyInt(boolToInt(h != nil && h.Exists(string(args[2]))))
}

func cmdHStrlen(c *client, args [][]byte) {
	h, ok := c.readHash(string(args[1]))
	if !ok {
		return
	}
	if h == nil {
		c.replyInt(0)
		return
	}
	v, _ := h.Get(string(args[2]))
	c.replyInt(int64(len(v)))
}

// hashReadHandler builds HKEYS, HVALS and HGETALL, which differ only in which
// halves of each pair they emit.
func hashReadHandler(wantFields, wantValues bool) handlerFunc {
	return func(c *client, args [][]byte) {
		h, ok := c.readHash(string(args[1]))
		if !ok {
			return
		}
		if h == nil {
			c.replyEmptyArray()
			return
		}
		pairs := h.All()

		width := 1
		if wantFields && wantValues {
			width = 2
		}
		c.replyArrayLen(len(pairs) * width)
		for _, p := range pairs {
			if wantFields {
				c.replyBulkString(p.Field)
			}
			if wantValues {
				c.replyBulk(p.Value)
			}
		}
	}
}

var (
	cmdHKeys   = hashReadHandler(true, false)
	cmdHVals   = hashReadHandler(false, true)
	cmdHGetAll = hashReadHandler(true, true)
)

func cmdHIncrBy(c *client, args [][]byte) {
	delta, ok := c.parseInt(args[3])
	if !ok {
		return
	}
	field := string(args[2])

	var result int64
	err := c.withHash(string(args[1]), true, func(h *store.Hash) error {
		var n int64
		if cur, found := h.Get(field); found {
			parsed, perr := strconv.ParseInt(string(cur), 10, 64)
			if perr != nil {
				return errHashNotInteger
			}
			n = parsed
		}
		if (delta > 0 && n > math.MaxInt64-delta) || (delta < 0 && n < math.MinInt64-delta) {
			return errIncrOverflow
		}
		result = n + delta
		h.Set(field, strconv.AppendInt(make([]byte, 0, 20), result, 10))
		return nil
	})
	if err != nil {
		c.replyStoreError(err)
		return
	}
	c.replyInt(result)
}

func cmdHIncrByFloat(c *client, args [][]byte) {
	delta, perr := strconv.ParseFloat(string(args[3]), 64)
	if perr != nil || math.IsNaN(delta) || math.IsInf(delta, 0) {
		c.replyNotFloat()
		return
	}
	field := string(args[2])

	var result float64
	err := c.withHash(string(args[1]), true, func(h *store.Hash) error {
		var n float64
		if cur, found := h.Get(field); found {
			parsed, e := strconv.ParseFloat(string(cur), 64)
			if e != nil {
				return errHashNotFloat
			}
			n = parsed
		}
		result = n + delta
		if math.IsNaN(result) || math.IsInf(result, 0) {
			return errNaNOrInf
		}
		h.Set(field, []byte(strconv.FormatFloat(result, 'f', -1, 64)))
		return nil
	})
	if err != nil {
		c.replyStoreError(err)
		return
	}
	encoded := strconv.FormatFloat(result, 'f', -1, 64)
	// Same reasoning as INCRBYFLOAT: propagate the result, not the sum.
	c.rewriteAs(buildCmd("HSET").bytes(args[1]).str(field).str(encoded))
	c.replyBulkString(encoded)
}

// cmdHRandField implements HRANDFIELD key [count [WITHVALUES]].
func cmdHRandField(c *client, args [][]byte) {
	h, ok := c.readHash(string(args[1]))
	if !ok {
		return
	}

	// Without a count the reply is a single bulk string.
	if len(args) == 2 {
		if h == nil || h.Len() == 0 {
			c.replyNull()
			return
		}
		fields := h.Fields()
		c.replyBulkString(fields[rand.Intn(len(fields))])
		return
	}

	count, valid := c.parseInt(args[2])
	if !valid {
		return
	}
	withValues := false
	if len(args) == 4 {
		if !strings.EqualFold(string(args[3]), "withvalues") {
			c.replySyntaxError()
			return
		}
		withValues = true
	} else if len(args) > 4 {
		c.replySyntaxError()
		return
	}

	if h == nil || h.Len() == 0 {
		c.replyEmptyArray()
		return
	}
	chosen := randomPick(h.Fields(), count)

	width := 1
	if withValues {
		width = 2
	}
	c.replyArrayLen(len(chosen) * width)
	for _, f := range chosen {
		c.replyBulkString(f)
		if withValues {
			v, _ := h.Get(f)
			c.replyBulk(v)
		}
	}
}

func cmdHScan(c *client, args [][]byte) {
	h, ok := c.readHash(string(args[1]))
	if !ok {
		return
	}
	var fields []string
	if h != nil {
		fields = h.SortedFields()
	}

	cursor, pattern, count, valid := c.parseCollectionScan(args)
	if !valid {
		return
	}
	next, page := pageCollection(fields, cursor, count)

	// HSCAN emits field and value alternately.
	var flat []string
	for _, f := range page {
		if pattern != "*" && !store.Match(pattern, f) {
			continue
		}
		v, _ := h.Get(f)
		flat = append(flat, f, string(v))
	}

	c.replyArrayLen(2)
	c.replyBulkString(strconv.FormatUint(next, 10))
	c.replyStrings(flat)
}

// randomPick selects members according to the count convention shared by
// HRANDFIELD, SRANDMEMBER and ZRANDMEMBER: a positive count returns up to that
// many distinct items, a negative count returns exactly that many and may
// repeat.
func randomPick(items []string, count int64) []string {
	if len(items) == 0 || count == 0 {
		return nil
	}

	if count < 0 {
		n := int(-count)
		out := make([]string, 0, n)
		for i := 0; i < n; i++ {
			out = append(out, items[rand.Intn(len(items))])
		}
		return out
	}

	n := int(count)
	if n >= len(items) {
		// Asking for at least everything returns everything, in random order.
		out := append([]string(nil), items...)
		rand.Shuffle(len(out), func(i, j int) { out[i], out[j] = out[j], out[i] })
		return out
	}
	// Partial Fisher-Yates on a copy: O(n) and never picks the same index
	// twice, unlike sampling with rejection.
	pool := append([]string(nil), items...)
	for i := 0; i < n; i++ {
		j := i + rand.Intn(len(pool)-i)
		pool[i], pool[j] = pool[j], pool[i]
	}
	return pool[:n]
}
