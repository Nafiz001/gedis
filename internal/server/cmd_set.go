package server

import (
	"math/rand"
	"strconv"
	"strings"

	"github.com/nafizahmed/gedis/internal/store"
)

func entrySet(e *store.Entry) (*store.Set, error) {
	if e == nil {
		return nil, nil
	}
	s, ok := e.Value.(*store.Set)
	if !ok {
		return nil, store.ErrWrongType
	}
	return s, nil
}

func (c *client) withSet(key string, create bool, fn func(s *store.Set) error) error {
	return c.db.Update(key, func(cur *store.Entry) (*store.Entry, error) {
		s, err := entrySet(cur)
		if err != nil {
			return nil, err
		}
		if s == nil {
			if !create {
				return cur, fn(nil)
			}
			s = store.NewSet()
		}
		if err := fn(s); err != nil {
			return nil, err
		}
		if s.Len() == 0 {
			return nil, nil
		}
		if cur == nil {
			return &store.Entry{Value: s}, nil
		}
		cur.Value = s
		return cur, nil
	})
}

// readSet fetches a set for a read-only command. A missing key yields nil,
// which readers treat as the empty set.
func (c *client) readSet(key string) (*store.Set, bool) {
	v, found := c.db.Get(key)
	if !found {
		return nil, true
	}
	s, ok := v.(*store.Set)
	if !ok {
		c.replyWrongType()
		return nil, false
	}
	return s, true
}

// readSets fetches several sets at once, for the SINTER/SUNION/SDIFF family.
func (c *client) readSets(keys [][]byte) ([]*store.Set, bool) {
	out := make([]*store.Set, 0, len(keys))
	for _, k := range keys {
		s, ok := c.readSet(string(k))
		if !ok {
			return nil, false
		}
		out = append(out, s)
	}
	return out, true
}

func cmdSAdd(c *client, args [][]byte) {
	added := 0
	err := c.withSet(string(args[1]), true, func(s *store.Set) error {
		for _, m := range args[2:] {
			if s.Add(string(m)) {
				added++
			}
		}
		return nil
	})
	if err != nil {
		c.replyStoreError(err)
		return
	}
	c.replyInt(int64(added))
}

func cmdSRem(c *client, args [][]byte) {
	removed := 0
	err := c.withSet(string(args[1]), false, func(s *store.Set) error {
		if s == nil {
			return nil
		}
		for _, m := range args[2:] {
			if s.Remove(string(m)) {
				removed++
			}
		}
		return nil
	})
	if err != nil {
		c.replyStoreError(err)
		return
	}
	c.replyInt(int64(removed))
}

func cmdSCard(c *client, args [][]byte) {
	s, ok := c.readSet(string(args[1]))
	if !ok {
		return
	}
	if s == nil {
		c.replyInt(0)
		return
	}
	c.replyInt(int64(s.Len()))
}

func cmdSIsMember(c *client, args [][]byte) {
	s, ok := c.readSet(string(args[1]))
	if !ok {
		return
	}
	c.replyInt(boolToInt(s != nil && s.Contains(string(args[2]))))
}

func cmdSMIsMember(c *client, args [][]byte) {
	s, ok := c.readSet(string(args[1]))
	if !ok {
		return
	}
	c.replyArrayLen(len(args) - 2)
	for _, m := range args[2:] {
		c.replyInt(boolToInt(s != nil && s.Contains(string(m))))
	}
}

func cmdSMembers(c *client, args [][]byte) {
	s, ok := c.readSet(string(args[1]))
	if !ok {
		return
	}
	if s == nil {
		c.replyEmptyArray()
		return
	}
	c.replyStrings(s.Members())
}

// cmdSPop removes and returns random members.
func cmdSPop(c *client, args [][]byte) {
	hasCount := len(args) == 3
	count := int64(1)
	if hasCount {
		n, ok := c.parseInt(args[2])
		if !ok {
			return
		}
		if n < 0 {
			c.replyError("ERR value is out of range, must be positive")
			return
		}
		count = n
	} else if len(args) > 3 {
		c.replyWrongArity("spop")
		return
	}

	var popped []string
	keyExists := false
	err := c.withSet(string(args[1]), false, func(s *store.Set) error {
		if s == nil {
			return nil
		}
		keyExists = true
		// A positive count never repeats, so this is a distinct sample.
		popped = randomPick(s.Members(), count)
		for _, m := range popped {
			s.Remove(m)
		}
		return nil
	})
	if err != nil {
		c.replyStoreError(err)
		return
	}

	// SPOP chooses at random, so the command itself is not replayable: the
	// log records the removal it turned out to be.
	if len(popped) == 0 {
		c.suppressPropagation()
	} else {
		rewrite := buildCmd("SREM").bytes(args[1])
		for _, m := range popped {
			rewrite = rewrite.str(m)
		}
		c.rewriteAs(rewrite)
	}

	if !hasCount {
		if len(popped) == 0 {
			c.replyNull()
			return
		}
		c.replyBulkString(popped[0])
		return
	}
	if !keyExists {
		c.replyEmptyArray()
		return
	}
	c.replyStrings(popped)
}

func cmdSRandMember(c *client, args [][]byte) {
	s, ok := c.readSet(string(args[1]))
	if !ok {
		return
	}

	if len(args) == 2 {
		if s == nil || s.Len() == 0 {
			c.replyNull()
			return
		}
		members := s.Members()
		c.replyBulkString(members[rand.Intn(len(members))])
		return
	}
	if len(args) > 3 {
		c.replyWrongArity("srandmember")
		return
	}

	count, valid := c.parseInt(args[2])
	if !valid {
		return
	}
	if s == nil {
		c.replyEmptyArray()
		return
	}
	c.replyStrings(randomPick(s.Members(), count))
}

// cmdSMove moves one member between sets atomically.
func cmdSMove(c *client, args [][]byte) {
	src, dst := string(args[1]), string(args[2])
	member := string(args[3])
	moved := false

	err := c.db.UpdateTwo(src, dst, func(e1, e2 *store.Entry) (*store.Entry, *store.Entry, error) {
		srcSet, err := entrySet(e1)
		if err != nil {
			return nil, nil, err
		}
		// The destination is type-checked even when the move will not happen,
		// so SMOVE onto a string is an error rather than a silent 0.
		dstSet, err := entrySet(e2)
		if err != nil {
			return nil, nil, err
		}
		if srcSet == nil || !srcSet.Contains(member) {
			return e1, e2, nil
		}
		moved = true

		if src == dst {
			return e1, e1, nil // moving within one set is a no-op
		}
		srcSet.Remove(member)
		if dstSet == nil {
			dstSet = store.NewSet()
			e2 = &store.Entry{Value: dstSet}
		}
		dstSet.Add(member)

		if srcSet.Len() == 0 {
			return nil, e2, nil
		}
		return e1, e2, nil
	})
	if err != nil {
		c.replyStoreError(err)
		return
	}
	c.replyInt(boolToInt(moved))
}

// setOpHandler builds SINTER, SUNION and SDIFF along with their STORE variants,
// which share everything but the combining function and the reply.
func setOpHandler(op func(...*store.Set) *store.Set, storeResult bool) handlerFunc {
	return func(c *client, args [][]byte) {
		keyArgs := args[1:]
		destination := ""
		if storeResult {
			destination = string(args[1])
			keyArgs = args[2:]
			if len(keyArgs) == 0 {
				c.replyWrongArity(strings.ToLower(string(args[0])))
				return
			}
		}

		sets, ok := c.readSets(keyArgs)
		if !ok {
			return
		}
		result := op(sets...)

		if !storeResult {
			c.replyStrings(result.Members())
			return
		}
		// STORE overwrites the destination, and an empty result deletes it
		// rather than leaving an empty set behind.
		if result.Len() == 0 {
			c.db.Delete(destination)
			c.replyInt(0)
			return
		}
		c.db.Set(destination, result, store.NoExpiry)
		c.replyInt(int64(result.Len()))
	}
}

var (
	cmdSInter      = setOpHandler(store.SetInter, false)
	cmdSUnion      = setOpHandler(store.SetUnion, false)
	cmdSDiff       = setOpHandler(store.SetDiff, false)
	cmdSInterStore = setOpHandler(store.SetInter, true)
	cmdSUnionStore = setOpHandler(store.SetUnion, true)
	cmdSDiffStore  = setOpHandler(store.SetDiff, true)
)

// cmdSInterCard implements SINTERCARD numkeys key [key ...] [LIMIT n], which
// reports the size of an intersection without building the reply for it.
func cmdSInterCard(c *client, args [][]byte) {
	numKeys, ok := c.parseInt(args[1])
	if !ok {
		return
	}
	if numKeys <= 0 {
		c.replyError("ERR numkeys should be greater than 0")
		return
	}
	if int(numKeys) > len(args)-2 {
		c.replyError("ERR Number of keys can't be greater than number of args")
		return
	}

	keyArgs := args[2 : 2+numKeys]
	limit := 0
	if rest := args[2+numKeys:]; len(rest) > 0 {
		if len(rest) != 2 || !strings.EqualFold(string(rest[0]), "limit") {
			c.replySyntaxError()
			return
		}
		n, valid := c.parseInt(rest[1])
		if !valid {
			return
		}
		if n < 0 {
			c.replyError("ERR LIMIT can't be negative")
			return
		}
		limit = int(n)
	}

	sets, ok := c.readSets(keyArgs)
	if !ok {
		return
	}
	n := store.SetInter(sets...).Len()
	if limit > 0 && n > limit {
		n = limit
	}
	c.replyInt(int64(n))
}

func cmdSScan(c *client, args [][]byte) {
	s, ok := c.readSet(string(args[1]))
	if !ok {
		return
	}
	var members []string
	if s != nil {
		members = s.SortedMembers()
	}

	cursor, pattern, count, valid := c.parseCollectionScan(args)
	if !valid {
		return
	}
	next, page := pageCollection(members, cursor, count)

	var matched []string
	for _, m := range page {
		if pattern == "*" || store.Match(pattern, m) {
			matched = append(matched, m)
		}
	}

	c.replyArrayLen(2)
	c.replyBulkString(strconv.FormatUint(next, 10))
	c.replyStrings(matched)
}
