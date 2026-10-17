package server

import (
	"strings"

	"github.com/Nafiz001/gedis/internal/store"
)

// entryList extracts the list held by an entry, for use inside a store update.
// A nil entry yields a nil list, which callers treat as "empty, create on
// write" -- the behaviour that makes LPUSH work on a key that does not exist.
func entryList(e *store.Entry) (*store.List, error) {
	if e == nil {
		return nil, nil
	}
	l, ok := e.Value.(*store.List)
	if !ok {
		return nil, store.ErrWrongType
	}
	return l, nil
}

// withList runs fn against the list at key.
//
// It centralises the two rules every list command shares: a missing key behaves
// like an empty list, and a list that ends up empty is deleted rather than left
// behind. Redis guarantees that an empty aggregate key does not exist, and code
// elsewhere relies on it -- EXISTS after popping the last element must be 0.
//
// create controls whether a missing key is materialised; read-only and
// pop-style commands pass false so they never resurrect a deleted key.
func (c *client) withList(key string, create bool, fn func(l *store.List) error) error {
	return c.db.Update(key, func(cur *store.Entry) (*store.Entry, error) {
		l, err := entryList(cur)
		if err != nil {
			return nil, err
		}
		if l == nil {
			if !create {
				return cur, fn(nil)
			}
			l = store.NewList()
		}
		if err := fn(l); err != nil {
			return nil, err
		}
		if l.Len() == 0 {
			return nil, nil // deletes the key
		}
		if cur == nil {
			return &store.Entry{Value: l}, nil
		}
		cur.Value = l
		return cur, nil
	})
}

// pushHandler builds LPUSH/RPUSH and their conditional LPUSHX/RPUSHX forms.
func pushHandler(front, requireExisting bool) handlerFunc {
	return func(c *client, args [][]byte) {
		key := string(args[1])
		length := 0

		err := c.withList(key, !requireExisting, func(l *store.List) error {
			if l == nil {
				return nil // PUSHX on a missing key does nothing
			}
			for _, v := range args[2:] {
				// Copy: args points into the connection's read buffer, which is
				// reused for the next command.
				value := append([]byte(nil), v...)
				if front {
					l.PushFront(value)
				} else {
					l.PushBack(value)
				}
			}
			length = l.Len()
			return nil
		})
		if err != nil {
			c.replyStoreError(err)
			return
		}
		c.replyInt(int64(length))
	}
}

var (
	cmdLPush  = pushHandler(true, false)
	cmdRPush  = pushHandler(false, false)
	cmdLPushX = pushHandler(true, true)
	cmdRPushX = pushHandler(false, true)
)

// popHandler builds LPOP and RPOP, which take an optional count.
func popHandler(front bool) handlerFunc {
	return func(c *client, args [][]byte) {
		// Without a count the reply is a single bulk string; with one it is an
		// array, even if only one element comes back.
		count := 1
		hasCount := len(args) == 3
		if hasCount {
			n, ok := c.parseInt(args[2])
			if !ok {
				return
			}
			if n < 0 {
				c.replyError("ERR value is out of range, must be positive")
				return
			}
			count = int(n)
		} else if len(args) > 3 {
			c.replyWrongArity(strings.ToLower(string(args[0])))
			return
		}

		// "Key is missing" and "popped nothing" are different replies: LPOP k 0
		// on a live list is an empty array, while LPOP missing 2 is null.
		// Verified against Redis 7.
		var popped [][]byte
		keyExists := false

		err := c.withList(string(args[1]), false, func(l *store.List) error {
			if l == nil {
				return nil
			}
			keyExists = true
			for i := 0; i < count; i++ {
				var v []byte
				var ok bool
				if front {
					v, ok = l.PopFront()
				} else {
					v, ok = l.PopBack()
				}
				if !ok {
					break
				}
				popped = append(popped, v)
			}
			return nil
		})
		if err != nil {
			c.replyStoreError(err)
			return
		}

		if !hasCount {
			if len(popped) == 0 {
				c.replyNull()
				return
			}
			c.replyBulk(popped[0])
			return
		}
		if !keyExists {
			c.replyNullArray()
			return
		}
		c.replyArrayLen(len(popped))
		for _, v := range popped {
			c.replyBulk(v)
		}
	}
}

var (
	cmdLPop = popHandler(true)
	cmdRPop = popHandler(false)
)

func cmdLLen(c *client, args [][]byte) {
	v, ok := c.db.Get(string(args[1]))
	if !ok {
		c.replyInt(0)
		return
	}
	l, isList := v.(*store.List)
	if !isList {
		c.replyWrongType()
		return
	}
	c.replyInt(int64(l.Len()))
}

func cmdLRange(c *client, args [][]byte) {
	start, ok := c.parseInt(args[2])
	if !ok {
		return
	}
	stop, ok := c.parseInt(args[3])
	if !ok {
		return
	}

	v, found := c.db.Get(string(args[1]))
	if !found {
		c.replyEmptyArray()
		return
	}
	l, isList := v.(*store.List)
	if !isList {
		c.replyWrongType()
		return
	}

	items := l.Range(clampIndex(start), clampIndex(stop))
	c.replyArrayLen(len(items))
	for _, item := range items {
		c.replyBulk(item)
	}
}

func cmdLIndex(c *client, args [][]byte) {
	idx, ok := c.parseInt(args[2])
	if !ok {
		return
	}
	v, found := c.db.Get(string(args[1]))
	if !found {
		c.replyNull()
		return
	}
	l, isList := v.(*store.List)
	if !isList {
		c.replyWrongType()
		return
	}
	item, inRange := l.Index(clampIndex(idx))
	if !inRange {
		c.replyNull()
		return
	}
	c.replyBulk(item)
}

func cmdLSet(c *client, args [][]byte) {
	idx, ok := c.parseInt(args[2])
	if !ok {
		return
	}
	value := append([]byte(nil), args[3]...)

	var missingKey, outOfRange bool
	err := c.withList(string(args[1]), false, func(l *store.List) error {
		if l == nil {
			missingKey = true
			return nil
		}
		if !l.Set(clampIndex(idx), value) {
			outOfRange = true
		}
		return nil
	})
	if err != nil {
		c.replyStoreError(err)
		return
	}
	switch {
	case missingKey:
		c.replyError("ERR no such key")
	case outOfRange:
		c.replyError("ERR index out of range")
	default:
		c.replyOK()
	}
}

func cmdLInsert(c *client, args [][]byte) {
	var before bool
	switch strings.ToLower(string(args[2])) {
	case "before":
		before = true
	case "after":
		before = false
	default:
		c.replySyntaxError()
		return
	}
	pivot := args[3]
	value := append([]byte(nil), args[4]...)

	length := 0
	err := c.withList(string(args[1]), false, func(l *store.List) error {
		if l == nil {
			length = 0 // missing key: LINSERT reports 0, not -1
			return nil
		}
		length = l.Insert(pivot, value, before)
		return nil
	})
	if err != nil {
		c.replyStoreError(err)
		return
	}
	c.replyInt(int64(length))
}

func cmdLRem(c *client, args [][]byte) {
	count, ok := c.parseInt(args[2])
	if !ok {
		return
	}
	value := args[3]

	removed := 0
	err := c.withList(string(args[1]), false, func(l *store.List) error {
		if l == nil {
			return nil
		}
		removed = l.Remove(clampIndex(count), value)
		return nil
	})
	if err != nil {
		c.replyStoreError(err)
		return
	}
	c.replyInt(int64(removed))
}

func cmdLTrim(c *client, args [][]byte) {
	start, ok := c.parseInt(args[2])
	if !ok {
		return
	}
	stop, ok := c.parseInt(args[3])
	if !ok {
		return
	}

	err := c.withList(string(args[1]), false, func(l *store.List) error {
		if l != nil {
			l.Trim(clampIndex(start), clampIndex(stop))
		}
		return nil
	})
	if err != nil {
		c.replyStoreError(err)
		return
	}
	// LTRIM always reports success, including when it emptied the key.
	c.replyOK()
}

// cmdLPos implements LPOS key element [RANK n] [COUNT n] [MAXLEN n].
func cmdLPos(c *client, args [][]byte) {
	element := args[2]
	rank, count, maxlen := 1, 0, 0
	hasCount := false

	for i := 3; i < len(args); i++ {
		if i+1 >= len(args) {
			c.replySyntaxError()
			return
		}
		option := strings.ToLower(string(args[i]))
		i++
		n, ok := c.parseInt(args[i])
		if !ok {
			return
		}
		switch option {
		case "rank":
			if n == 0 {
				c.replyError("ERR RANK can't be zero. Use 1 to start searching from the first matching element in the head of the list or a negative rank to start searching from the tail. A RANK of zero is invalid.")
				return
			}
			rank = int(n)
		case "count":
			if n < 0 {
				c.replyError("ERR COUNT can't be negative")
				return
			}
			hasCount = true
			count = int(n)
		case "maxlen":
			if n < 0 {
				c.replyError("ERR MAXLEN can't be negative")
				return
			}
			maxlen = int(n)
		default:
			c.replySyntaxError()
			return
		}
	}

	var positions []int
	v, found := c.db.Get(string(args[1]))
	if found {
		l, isList := v.(*store.List)
		if !isList {
			c.replyWrongType()
			return
		}
		// COUNT 0 means "every match"; without COUNT at all, only the first.
		want := count
		if hasCount && count == 0 {
			want = -1
		}
		positions = l.Pos(element, rank, want, maxlen)
	}

	if !hasCount {
		if len(positions) == 0 {
			c.replyNull()
			return
		}
		c.replyInt(int64(positions[0]))
		return
	}
	c.replyArrayLen(len(positions))
	for _, p := range positions {
		c.replyInt(int64(p))
	}
}

// cmdLMove implements LMOVE src dst LEFT|RIGHT LEFT|RIGHT, and RPOPLPUSH,
// which is the LMOVE RIGHT LEFT special case kept for compatibility.
func cmdLMove(c *client, args [][]byte) {
	var fromLeft, toLeft bool
	switch strings.ToLower(string(args[0])) {
	case "rpoplpush":
		fromLeft, toLeft = false, true
	default:
		if len(args) != 5 {
			c.replyWrongArity("lmove")
			return
		}
		var ok bool
		if fromLeft, ok = parseSide(args[3]); !ok {
			c.replySyntaxError()
			return
		}
		if toLeft, ok = parseSide(args[4]); !ok {
			c.replySyntaxError()
			return
		}
	}

	src, dst := string(args[1]), string(args[2])
	var moved []byte

	err := c.db.UpdateTwo(src, dst, func(e1, e2 *store.Entry) (*store.Entry, *store.Entry, error) {
		srcList, err := entryList(e1)
		if err != nil {
			return nil, nil, err
		}
		if srcList == nil || srcList.Len() == 0 {
			return e1, e2, nil
		}
		dstList, err := entryList(e2)
		if err != nil {
			return nil, nil, err
		}

		var ok bool
		if fromLeft {
			moved, ok = srcList.PopFront()
		} else {
			moved, ok = srcList.PopBack()
		}
		if !ok {
			return e1, e2, nil
		}

		// Rotating a list onto itself: the pop and push touch one list, so the
		// destination entry must be the source entry, not a second empty list.
		if src == dst {
			if toLeft {
				srcList.PushFront(moved)
			} else {
				srcList.PushBack(moved)
			}
			return e1, e1, nil
		}

		if dstList == nil {
			dstList = store.NewList()
			e2 = &store.Entry{Value: dstList}
		}
		if toLeft {
			dstList.PushFront(moved)
		} else {
			dstList.PushBack(moved)
		}

		// The source may now be empty, in which case its key must go.
		if srcList.Len() == 0 {
			return nil, e2, nil
		}
		return e1, e2, nil
	})
	if err != nil {
		c.replyStoreError(err)
		return
	}
	if moved == nil {
		c.replyNull()
		return
	}
	c.replyBulk(moved)
}

func parseSide(arg []byte) (left bool, ok bool) {
	switch strings.ToLower(string(arg)) {
	case "left":
		return true, true
	case "right":
		return false, true
	default:
		return false, false
	}
}

// clampIndex narrows a 64-bit argument to int without wrapping on 32-bit
// builds. Indexes far outside the list are clamped to values that still sort
// correctly against any real length.
func clampIndex(n int64) int {
	const maxInt = int64(^uint(0) >> 1)
	const minInt = -maxInt - 1
	if n > maxInt {
		return int(maxInt)
	}
	if n < minInt {
		return int(minInt)
	}
	return int(n)
}
