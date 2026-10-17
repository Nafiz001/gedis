package server

import (
	"math"
	"strconv"
	"strings"

	"github.com/Nafiz001/gedis/internal/store"
)

func entryZSet(e *store.Entry) (*store.ZSet, error) {
	if e == nil {
		return nil, nil
	}
	z, ok := e.Value.(*store.ZSet)
	if !ok {
		return nil, store.ErrWrongType
	}
	return z, nil
}

func (c *client) withZSet(key string, create bool, fn func(z *store.ZSet) error) error {
	return c.db.Update(key, func(cur *store.Entry) (*store.Entry, error) {
		z, err := entryZSet(cur)
		if err != nil {
			return nil, err
		}
		if z == nil {
			if !create {
				return cur, fn(nil)
			}
			z = store.NewZSet()
		}
		if err := fn(z); err != nil {
			return nil, err
		}
		if z.Len() == 0 {
			return nil, nil
		}
		if cur == nil {
			return &store.Entry{Value: z}, nil
		}
		cur.Value = z
		return cur, nil
	})
}

func (c *client) readZSet(key string) (*store.ZSet, bool) {
	v, found := c.db.Get(key)
	if !found {
		return nil, true
	}
	z, ok := v.(*store.ZSet)
	if !ok {
		c.replyWrongType()
		return nil, false
	}
	return z, true
}

// --- score parsing and formatting ---

// parseScore reads a score argument, accepting the infinity spellings Redis
// does. NaN is rejected: a score with no ordering would corrupt the skiplist.
func parseScore(b []byte) (float64, bool) {
	s := strings.ToLower(strings.TrimSpace(string(b)))
	switch s {
	case "inf", "+inf", "infinity", "+infinity":
		return math.Inf(1), true
	case "-inf", "-infinity":
		return math.Inf(-1), true
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil || math.IsNaN(v) {
		return 0, false
	}
	return v, true
}

// formatScore renders a score the way Redis does: the shortest representation
// that round-trips, so a whole number comes back as "3" rather than "3.0".
func formatScore(v float64) string {
	if math.IsInf(v, 1) {
		return "inf"
	}
	if math.IsInf(v, -1) {
		return "-inf"
	}
	return strconv.FormatFloat(v, 'f', -1, 64)
}

// parseScoreBound reads one end of a ZRANGEBYSCORE window. A leading "("
// makes the bound exclusive.
func parseScoreBound(b []byte) (value float64, exclusive bool, ok bool) {
	if len(b) > 0 && b[0] == '(' {
		v, valid := parseScore(b[1:])
		return v, true, valid
	}
	v, valid := parseScore(b)
	return v, false, valid
}

func (c *client) parseScoreRange(minArg, maxArg []byte) (store.ScoreRange, bool) {
	minVal, minExcl, ok := parseScoreBound(minArg)
	if !ok {
		c.replyError("ERR min or max is not a float")
		return store.ScoreRange{}, false
	}
	maxVal, maxExcl, ok := parseScoreBound(maxArg)
	if !ok {
		c.replyError("ERR min or max is not a float")
		return store.ScoreRange{}, false
	}
	return store.ScoreRange{
		Min: minVal, Max: maxVal, MinExcl: minExcl, MaxExcl: maxExcl,
	}, true
}

// parseLexBound reads one end of a ZRANGEBYLEX window: "[x" inclusive,
// "(x" exclusive, "-" and "+" for the extremes.
func parseLexBound(b []byte) (value string, exclusive, infinite, ok bool) {
	if len(b) == 0 {
		return "", false, false, false
	}
	switch b[0] {
	case '-', '+':
		if len(b) == 1 {
			return "", false, true, true
		}
		return "", false, false, false
	case '[':
		return string(b[1:]), false, false, true
	case '(':
		return string(b[1:]), true, false, true
	default:
		return "", false, false, false
	}
}

func (c *client) parseLexRange(minArg, maxArg []byte) (store.LexRange, bool) {
	minVal, minExcl, minInf, ok := parseLexBound(minArg)
	if !ok {
		c.replyError("ERR min or max not valid string range item")
		return store.LexRange{}, false
	}
	maxVal, maxExcl, maxInf, ok := parseLexBound(maxArg)
	if !ok {
		c.replyError("ERR min or max not valid string range item")
		return store.LexRange{}, false
	}
	// "-" as a max or "+" as a min is an empty window, which the flags below
	// express by leaving both infinite bounds on the wrong sides.
	r := store.LexRange{Min: minVal, Max: maxVal, MinExcl: minExcl, MaxExcl: maxExcl}
	if minInf {
		if minArg[0] == '+' {
			// A min of "+" excludes everything.
			r.Min, r.MinExcl, r.MinInf = "", false, false
			r.Max, r.MaxExcl, r.MaxInf = "", true, false
			return r, true
		}
		r.MinInf = true
	}
	if maxInf {
		if maxArg[0] == '-' {
			r.Min, r.MinExcl, r.MinInf = "", false, false
			r.Max, r.MaxExcl, r.MaxInf = "", true, false
			return r, true
		}
		r.MaxInf = true
	}
	return r, true
}

// replyZMembers writes a member list, optionally interleaved with scores.
func (c *client) replyZMembers(members []store.ZMember, withScores bool) {
	width := 1
	if withScores {
		width = 2
	}
	c.replyArrayLen(len(members) * width)
	for _, m := range members {
		c.replyBulkString(m.Member)
		if withScores {
			c.replyBulkString(formatScore(m.Score))
		}
	}
}

// --- commands ---

// cmdZAdd implements ZADD key [NX|XX] [GT|LT] [CH] [INCR] score member ...
func cmdZAdd(c *client, args [][]byte) {
	var nx, xx, gt, lt, ch, incr bool

	i := 2
	for ; i < len(args); i++ {
		switch strings.ToLower(string(args[i])) {
		case "nx":
			nx = true
		case "xx":
			xx = true
		case "gt":
			gt = true
		case "lt":
			lt = true
		case "ch":
			ch = true
		case "incr":
			incr = true
		default:
			goto parsed
		}
	}
parsed:

	if nx && xx {
		c.replyError("ERR XX and NX options at the same time are not compatible")
		return
	}
	// NX means "only create", so it cannot be combined with a condition that
	// only makes sense when updating.
	if nx && (gt || lt) {
		c.replyError("ERR GT, LT, and/or NX options at the same time are not compatible")
		return
	}
	if gt && lt {
		c.replyError("ERR GT, LT, and/or NX options at the same time are not compatible")
		return
	}

	pairs := args[i:]
	if len(pairs) == 0 || len(pairs)%2 != 0 {
		c.replySyntaxError()
		return
	}
	if incr && len(pairs) != 2 {
		c.replyError("ERR INCR option supports a single increment-element pair")
		return
	}

	// Validate every score before touching the keyspace, so a malformed pair
	// late in the list does not leave a half-applied command behind.
	type pair struct {
		score  float64
		member string
	}
	parsedPairs := make([]pair, 0, len(pairs)/2)
	for j := 0; j < len(pairs); j += 2 {
		score, ok := parseScore(pairs[j])
		if !ok {
			c.replyNotFloat()
			return
		}
		parsedPairs = append(parsedPairs, pair{score: score, member: string(pairs[j+1])})
	}

	var (
		added, changed int
		incrResult     float64
		incrApplied    bool
	)

	err := c.withZSet(string(args[1]), !xx, func(z *store.ZSet) error {
		if z == nil {
			return nil // XX on a missing key does nothing
		}
		for _, p := range parsedPairs {
			old, exists := z.Score(p.member)

			if (nx && exists) || (xx && !exists) {
				continue
			}
			newScore := p.score
			if incr && exists {
				newScore = old + p.score
				if math.IsNaN(newScore) {
					return errNaNScore
				}
			}
			// GT and LT only apply to members that already exist; a new member
			// is always inserted.
			if exists {
				if gt && newScore <= old {
					continue
				}
				if lt && newScore >= old {
					continue
				}
			}

			wasAdded, wasUpdated := z.Add(p.member, newScore)
			if wasAdded {
				added++
			}
			if wasAdded || wasUpdated {
				changed++
			}
			incrResult, incrApplied = newScore, true
		}
		return nil
	})
	if err != nil {
		c.replyStoreError(err)
		return
	}

	if incr {
		// A condition that rejected the update replies with null, not a score.
		if !incrApplied {
			c.replyNull()
			return
		}
		c.replyBulkString(formatScore(incrResult))
		return
	}
	if ch {
		c.replyInt(int64(changed))
		return
	}
	c.replyInt(int64(added))
}

func cmdZScore(c *client, args [][]byte) {
	z, ok := c.readZSet(string(args[1]))
	if !ok {
		return
	}
	if z == nil {
		c.replyNull()
		return
	}
	score, found := z.Score(string(args[2]))
	if !found {
		c.replyNull()
		return
	}
	c.replyBulkString(formatScore(score))
}

func cmdZMScore(c *client, args [][]byte) {
	z, ok := c.readZSet(string(args[1]))
	if !ok {
		return
	}
	c.replyArrayLen(len(args) - 2)
	for _, m := range args[2:] {
		if z == nil {
			c.replyNull()
			continue
		}
		score, found := z.Score(string(m))
		if !found {
			c.replyNull()
			continue
		}
		c.replyBulkString(formatScore(score))
	}
}

func cmdZCard(c *client, args [][]byte) {
	z, ok := c.readZSet(string(args[1]))
	if !ok {
		return
	}
	if z == nil {
		c.replyInt(0)
		return
	}
	c.replyInt(int64(z.Len()))
}

func cmdZCount(c *client, args [][]byte) {
	r, ok := c.parseScoreRange(args[2], args[3])
	if !ok {
		return
	}
	z, valid := c.readZSet(string(args[1]))
	if !valid {
		return
	}
	if z == nil {
		c.replyInt(0)
		return
	}
	c.replyInt(int64(z.Count(r)))
}

func cmdZLexCount(c *client, args [][]byte) {
	r, ok := c.parseLexRange(args[2], args[3])
	if !ok {
		return
	}
	z, valid := c.readZSet(string(args[1]))
	if !valid {
		return
	}
	if z == nil {
		c.replyInt(0)
		return
	}
	c.replyInt(int64(z.CountByLex(r)))
}

func cmdZIncrBy(c *client, args [][]byte) {
	delta, ok := parseScore(args[2])
	if !ok {
		c.replyNotFloat()
		return
	}
	member := string(args[3])

	var result float64
	err := c.withZSet(string(args[1]), true, func(z *store.ZSet) error {
		old, _ := z.Score(member)
		result = old + delta
		// inf + -inf is NaN, which has no place in an ordered structure.
		if math.IsNaN(result) {
			return errNaNScore
		}
		z.Add(member, result)
		return nil
	})
	if err != nil {
		c.replyStoreError(err)
		return
	}
	c.replyBulkString(formatScore(result))
}

// rankHandler builds ZRANK and ZREVRANK.
func rankHandler(reverse bool) handlerFunc {
	return func(c *client, args [][]byte) {
		z, ok := c.readZSet(string(args[1]))
		if !ok {
			return
		}
		if z == nil {
			c.replyNull()
			return
		}
		rank, found := z.Rank(string(args[2]), reverse)
		if !found {
			c.replyNull()
			return
		}
		c.replyInt(int64(rank))
	}
}

var (
	cmdZRank    = rankHandler(false)
	cmdZRevRank = rankHandler(true)
)

// rangeMode selects how ZRANGE interprets its start/stop arguments.
type rangeMode int

const (
	rangeByRank rangeMode = iota
	rangeByScore
	rangeByLex
)

// zrangeOptions is the parsed form of the ZRANGE family, which the legacy
// commands (ZREVRANGE, ZRANGEBYSCORE, ...) also funnel through.
type zrangeOptions struct {
	mode       rangeMode
	reverse    bool
	withScores bool
	offset     int
	count      int
	hasLimit   bool
}

// cmdZRange implements the unified ZRANGE plus every legacy variant. They share
// all their logic and differ only in defaults, so one handler covers them all.
func cmdZRange(c *client, args [][]byte) {
	name := strings.ToLower(string(args[0]))

	opts := zrangeOptions{count: -1}
	firstOption := 4

	switch name {
	case "zrange":
		// Modifiers are parsed below.
	case "zrevrange":
		opts.reverse = true
	case "zrangebyscore":
		opts.mode = rangeByScore
	case "zrevrangebyscore":
		opts.mode, opts.reverse = rangeByScore, true
	case "zrangebylex":
		opts.mode = rangeByLex
	case "zrevrangebylex":
		opts.mode, opts.reverse = rangeByLex, true
	}

	for i := firstOption; i < len(args); i++ {
		switch strings.ToLower(string(args[i])) {
		case "withscores":
			if opts.mode == rangeByLex {
				c.replySyntaxError() // BYLEX has no scores to show
				return
			}
			opts.withScores = true
		case "rev":
			if name != "zrange" {
				c.replySyntaxError()
				return
			}
			opts.reverse = true
		case "byscore":
			if name != "zrange" {
				c.replySyntaxError()
				return
			}
			opts.mode = rangeByScore
		case "bylex":
			if name != "zrange" {
				c.replySyntaxError()
				return
			}
			opts.mode = rangeByLex
		case "limit":
			if i+2 >= len(args) {
				c.replySyntaxError()
				return
			}
			offset, ok := c.parseInt(args[i+1])
			if !ok {
				return
			}
			count, ok := c.parseInt(args[i+2])
			if !ok {
				return
			}
			opts.offset, opts.count, opts.hasLimit = clampIndex(offset), clampIndex(count), true
			i += 2
		default:
			c.replySyntaxError()
			return
		}
	}

	// LIMIT is only meaningful for the score and lex forms.
	if opts.hasLimit && opts.mode == rangeByRank {
		c.replyError("ERR syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX")
		return
	}

	// The legacy reverse commands take their bounds in max/min order.
	minArg, maxArg := args[2], args[3]
	if opts.reverse && opts.mode != rangeByRank {
		minArg, maxArg = args[3], args[2]
	}

	// Parse the range before reading the key, so a malformed bound is reported
	// even when the key does not exist.
	var (
		scoreRange  store.ScoreRange
		lexRange    store.LexRange
		start, stop int
	)
	switch opts.mode {
	case rangeByScore:
		var ok bool
		if scoreRange, ok = c.parseScoreRange(minArg, maxArg); !ok {
			return
		}
	case rangeByLex:
		var ok bool
		if lexRange, ok = c.parseLexRange(minArg, maxArg); !ok {
			return
		}
	default:
		a, ok := c.parseInt(args[2])
		if !ok {
			return
		}
		b, ok := c.parseInt(args[3])
		if !ok {
			return
		}
		start, stop = clampIndex(a), clampIndex(b)
	}

	z, ok := c.readZSet(string(args[1]))
	if !ok {
		return
	}
	if z == nil {
		c.replyEmptyArray()
		return
	}

	var members []store.ZMember
	switch opts.mode {
	case rangeByScore:
		members = z.RangeByScore(scoreRange, opts.reverse, opts.offset, opts.count)
	case rangeByLex:
		members = z.RangeByLex(lexRange, opts.reverse, opts.offset, opts.count)
	default:
		members = z.RangeByRank(start, stop, opts.reverse)
	}
	c.replyZMembers(members, opts.withScores)
}

func cmdZRem(c *client, args [][]byte) {
	removed := 0
	err := c.withZSet(string(args[1]), false, func(z *store.ZSet) error {
		if z == nil {
			return nil
		}
		for _, m := range args[2:] {
			if z.Remove(string(m)) {
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

// remRangeHandler builds ZREMRANGEBYRANK, ZREMRANGEBYSCORE and ZREMRANGEBYLEX.
func remRangeHandler(mode rangeMode) handlerFunc {
	return func(c *client, args [][]byte) {
		var (
			scoreRange  store.ScoreRange
			lexRange    store.LexRange
			start, stop int
		)
		switch mode {
		case rangeByScore:
			var ok bool
			if scoreRange, ok = c.parseScoreRange(args[2], args[3]); !ok {
				return
			}
		case rangeByLex:
			var ok bool
			if lexRange, ok = c.parseLexRange(args[2], args[3]); !ok {
				return
			}
		default:
			a, ok := c.parseInt(args[2])
			if !ok {
				return
			}
			b, ok := c.parseInt(args[3])
			if !ok {
				return
			}
			start, stop = clampIndex(a), clampIndex(b)
		}

		removed := 0
		err := c.withZSet(string(args[1]), false, func(z *store.ZSet) error {
			if z == nil {
				return nil
			}
			// Collect first, then delete: removing while walking the skiplist
			// would invalidate the traversal.
			var doomed []store.ZMember
			switch mode {
			case rangeByScore:
				doomed = z.RangeByScore(scoreRange, false, 0, -1)
			case rangeByLex:
				doomed = z.RangeByLex(lexRange, false, 0, -1)
			default:
				doomed = z.RangeByRank(start, stop, false)
			}
			for _, m := range doomed {
				if z.Remove(m.Member) {
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
}

var (
	cmdZRemRangeByRank  = remRangeHandler(rangeByRank)
	cmdZRemRangeByScore = remRangeHandler(rangeByScore)
	cmdZRemRangeByLex   = remRangeHandler(rangeByLex)
)

// popHandlerZ builds ZPOPMIN and ZPOPMAX.
func popHandlerZ(fromMax bool) handlerFunc {
	return func(c *client, args [][]byte) {
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
			count = clampIndex(n)
		} else if len(args) > 3 {
			c.replyWrongArity(strings.ToLower(string(args[0])))
			return
		}

		var popped []store.ZMember
		err := c.withZSet(string(args[1]), false, func(z *store.ZSet) error {
			if z == nil {
				return nil
			}
			if fromMax {
				popped = z.PopMax(count)
			} else {
				popped = z.PopMin(count)
			}
			return nil
		})
		if err != nil {
			c.replyStoreError(err)
			return
		}
		// Both forms reply with a flat member/score array.
		c.replyZMembers(popped, true)
	}
}

var (
	cmdZPopMin = popHandlerZ(false)
	cmdZPopMax = popHandlerZ(true)
)

func cmdZRandMember(c *client, args [][]byte) {
	z, ok := c.readZSet(string(args[1]))
	if !ok {
		return
	}

	if len(args) == 2 {
		if z == nil || z.Len() == 0 {
			c.replyNull()
			return
		}
		picked := randomPick(z.Members(), 1)
		c.replyBulkString(picked[0])
		return
	}

	count, valid := c.parseInt(args[2])
	if !valid {
		return
	}
	withScores := false
	if len(args) == 4 {
		if !strings.EqualFold(string(args[3]), "withscores") {
			c.replySyntaxError()
			return
		}
		withScores = true
	} else if len(args) > 4 {
		c.replySyntaxError()
		return
	}

	if z == nil {
		c.replyEmptyArray()
		return
	}
	chosen := randomPick(z.Members(), count)

	width := 1
	if withScores {
		width = 2
	}
	c.replyArrayLen(len(chosen) * width)
	for _, m := range chosen {
		c.replyBulkString(m)
		if withScores {
			score, _ := z.Score(m)
			c.replyBulkString(formatScore(score))
		}
	}
}

func cmdZScan(c *client, args [][]byte) {
	z, ok := c.readZSet(string(args[1]))
	if !ok {
		return
	}
	var members []string
	if z != nil {
		members = z.SortedMembers()
	}

	cursor, pattern, count, valid := c.parseCollectionScan(args)
	if !valid {
		return
	}
	next, page := pageCollection(members, cursor, count)

	var flat []string
	for _, m := range page {
		if pattern != "*" && !store.Match(pattern, m) {
			continue
		}
		score, _ := z.Score(m)
		flat = append(flat, m, formatScore(score))
	}

	c.replyArrayLen(2)
	c.replyBulkString(strconv.FormatUint(next, 10))
	c.replyStrings(flat)
}

// cmdZSetOp implements ZUNIONSTORE, ZINTERSTORE, ZDIFFSTORE and their
// non-storing counterparts, which share their argument grammar.
func cmdZSetOp(c *client, args [][]byte) {
	name := strings.ToLower(string(args[0]))
	storeResult := strings.HasSuffix(name, "store")

	idx := 1
	destination := ""
	if storeResult {
		destination = string(args[1])
		idx = 2
	}
	if idx >= len(args) {
		c.replyWrongArity(name)
		return
	}

	numKeys, ok := c.parseInt(args[idx])
	if !ok {
		return
	}
	if numKeys <= 0 {
		c.replyError("ERR at least 1 input key is needed for the given command")
		return
	}
	idx++
	if int(numKeys) > len(args)-idx {
		c.replySyntaxError()
		return
	}
	keyArgs := args[idx : idx+int(numKeys)]
	idx += int(numKeys)

	weights := make([]float64, 0, numKeys)
	aggregate := store.AggregateSum
	withScores := false

	for ; idx < len(args); idx++ {
		switch strings.ToLower(string(args[idx])) {
		case "weights":
			if idx+int(numKeys) >= len(args) {
				c.replySyntaxError()
				return
			}
			for j := 0; j < int(numKeys); j++ {
				idx++
				w, valid := parseScore(args[idx])
				if !valid {
					c.replyError("ERR weight value is not a float")
					return
				}
				weights = append(weights, w)
			}
		case "aggregate":
			if idx+1 >= len(args) {
				c.replySyntaxError()
				return
			}
			idx++
			switch strings.ToLower(string(args[idx])) {
			case "sum":
				aggregate = store.AggregateSum
			case "min":
				aggregate = store.AggregateMin
			case "max":
				aggregate = store.AggregateMax
			default:
				c.replySyntaxError()
				return
			}
		case "withscores":
			if storeResult {
				c.replySyntaxError()
				return
			}
			withScores = true
		default:
			c.replySyntaxError()
			return
		}
	}

	// Inputs may be sorted sets or plain sets; a plain set behaves as though
	// every member scored 1, which is what makes SADD-built data usable here.
	sets := make([]*store.ZSet, 0, len(keyArgs))
	for _, k := range keyArgs {
		z, valid := c.asZSet(string(k))
		if !valid {
			return
		}
		sets = append(sets, z)
	}

	var result *store.ZSet
	switch {
	case strings.HasPrefix(name, "zunion"):
		result = store.ZSetUnion(sets, weights, aggregate)
	case strings.HasPrefix(name, "zinter"):
		result = store.ZSetInter(sets, weights, aggregate)
	default: // zdiff
		result = zsetDiff(sets)
	}

	if !storeResult {
		c.replyZMembers(result.RangeByRank(0, -1, false), withScores)
		return
	}
	if result.Len() == 0 {
		c.db.Delete(destination)
		c.replyInt(0)
		return
	}
	c.db.Set(destination, result, store.NoExpiry)
	c.replyInt(int64(result.Len()))
}

// asZSet reads a key as a sorted set, promoting a plain set by giving every
// member a score of 1 -- the conversion Redis applies in ZUNIONSTORE.
func (c *client) asZSet(key string) (*store.ZSet, bool) {
	v, found := c.db.Get(key)
	if !found {
		return nil, true
	}
	switch typed := v.(type) {
	case *store.ZSet:
		return typed, true
	case *store.Set:
		promoted := store.NewZSet()
		for _, m := range typed.Members() {
			promoted.Add(m, 1)
		}
		return promoted, true
	default:
		c.replyWrongType()
		return nil, false
	}
}

// zsetDiff keeps members of the first set that appear in none of the rest,
// carrying their original scores.
func zsetDiff(sets []*store.ZSet) *store.ZSet {
	out := store.NewZSet()
	if len(sets) == 0 || sets[0] == nil {
		return out
	}
member:
	for _, m := range sets[0].RangeByRank(0, -1, false) {
		for _, other := range sets[1:] {
			if other == nil {
				continue
			}
			if _, found := other.Score(m.Member); found {
				continue member
			}
		}
		out.Add(m.Member, m.Score)
	}
	return out
}
