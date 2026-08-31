package store

import "sort"

// ZSet is the value type behind ZADD: a set of members, each with a score,
// kept in score order.
//
// It is two structures over the same data, which is what lets every sorted-set
// command be fast:
//
//	dict      member -> score, so ZSCORE and membership are O(1)
//	skiplist  ordered by (score, member), so ZRANK, ZRANGE and the
//	          by-score windows are O(log n)
//
// Keeping both in step is the cost. A score update is a delete plus an insert
// in the skiplist, because the score is part of its sort key, while the map
// entry is simply overwritten.
//
// Not safe for concurrent use; callers hold the owning shard's write lock.
type ZSet struct {
	dict map[string]float64
	sl   *skiplist
}

// ZMember is a member together with its score.
type ZMember struct {
	Member string
	Score  float64
}

func NewZSet() *ZSet {
	return &ZSet{dict: make(map[string]float64), sl: newSkiplist()}
}

func (z *ZSet) Len() int { return len(z.dict) }

// Score returns a member's score.
func (z *ZSet) Score(member string) (float64, bool) {
	s, ok := z.dict[member]
	return s, ok
}

// Add inserts or rescores a member, reporting whether it was newly added and
// whether an existing score changed.
func (z *ZSet) Add(member string, score float64) (added, updated bool) {
	old, exists := z.dict[member]
	if !exists {
		z.dict[member] = score
		z.sl.Insert(score, member)
		return true, false
	}
	if old == score {
		return false, false
	}
	// The skiplist is ordered by score, so a rescore has to move the node.
	z.sl.Delete(old, member)
	z.sl.Insert(score, member)
	z.dict[member] = score
	return false, true
}

// Remove deletes a member.
func (z *ZSet) Remove(member string) bool {
	score, ok := z.dict[member]
	if !ok {
		return false
	}
	delete(z.dict, member)
	z.sl.Delete(score, member)
	return true
}

// Rank returns a member's 0-based position, counting from the low score end or,
// when reverse is set, from the high end.
func (z *ZSet) Rank(member string, reverse bool) (int, bool) {
	score, ok := z.dict[member]
	if !ok {
		return 0, false
	}
	rank := z.sl.Rank(score, member) // 1-based
	if reverse {
		return z.Len() - rank, true
	}
	return rank - 1, true
}

// RangeByRank returns members between two inclusive 0-based indexes, which may
// be negative to count from the end.
func (z *ZSet) RangeByRank(start, stop int, reverse bool) []ZMember {
	n := z.Len()
	if start < 0 {
		start += n
	}
	if stop < 0 {
		stop += n
	}
	if start < 0 {
		start = 0
	}
	if stop >= n {
		stop = n - 1
	}
	if start > stop || start >= n || n == 0 {
		return nil
	}

	out := make([]ZMember, 0, stop-start+1)
	if !reverse {
		// Jump straight to the first element by rank, then walk forward.
		node := z.sl.NodeByRank(start + 1)
		for i := start; i <= stop && node != nil; i++ {
			out = append(out, ZMember{Member: node.member, Score: node.score})
			node = node.level[0].forward
		}
		return out
	}
	// Reverse: index 0 is the highest score, so walk backward from the tail.
	node := z.sl.NodeByRank(n - start)
	for i := start; i <= stop && node != nil; i++ {
		out = append(out, ZMember{Member: node.member, Score: node.score})
		node = node.backward
	}
	return out
}

// RangeByScore returns members whose scores fall in the window, ordered by
// score. offset and count page the result; count < 0 means no limit.
func (z *ZSet) RangeByScore(r ScoreRange, reverse bool, offset, count int) []ZMember {
	if offset < 0 {
		return nil
	}
	var out []ZMember

	if !reverse {
		node := z.sl.FirstInScoreRange(r)
		for node != nil && offset > 0 {
			node = node.level[0].forward
			offset--
		}
		for node != nil && r.belowMax(node.score) {
			if count >= 0 && len(out) >= count {
				break
			}
			out = append(out, ZMember{Member: node.member, Score: node.score})
			node = node.level[0].forward
		}
		return out
	}

	node := z.sl.LastInScoreRange(r)
	for node != nil && offset > 0 {
		node = node.backward
		offset--
	}
	for node != nil && node != z.sl.header && r.aboveMin(node.score) {
		if count >= 0 && len(out) >= count {
			break
		}
		out = append(out, ZMember{Member: node.member, Score: node.score})
		node = node.backward
	}
	return out
}

// Count returns how many members fall inside a score window.
func (z *ZSet) Count(r ScoreRange) int { return z.sl.CountInScoreRange(r) }

// LexRange describes a ZRANGEBYLEX window.
//
// Lexicographic ranges are only meaningful when every member shares one score,
// because the skiplist orders by score first. Redis documents that condition
// rather than enforcing it, and so does this.
type LexRange struct {
	Min, Max         string
	MinExcl, MaxExcl bool
	// MinInf and MaxInf represent the "-" and "+" bounds, which sort before
	// and after every possible member.
	MinInf, MaxInf bool
}

func (r LexRange) above(member string) bool {
	if r.MinInf {
		return true
	}
	if r.MinExcl {
		return member > r.Min
	}
	return member >= r.Min
}

func (r LexRange) below(member string) bool {
	if r.MaxInf {
		return true
	}
	if r.MaxExcl {
		return member < r.Max
	}
	return member <= r.Max
}

// RangeByLex returns members inside a lexicographic window.
func (z *ZSet) RangeByLex(r LexRange, reverse bool, offset, count int) []ZMember {
	if offset < 0 {
		return nil
	}
	// Walk in member order and filter. A skiplist search could find the first
	// match in O(log n), but only by assuming the uniform-score precondition
	// actually holds; scanning stays correct either way, and these ranges are
	// used on small sets in practice.
	all := z.RangeByRank(0, -1, false)
	var matched []ZMember
	for _, m := range all {
		if r.above(m.Member) && r.below(m.Member) {
			matched = append(matched, m)
		}
	}
	if reverse {
		for i, j := 0, len(matched)-1; i < j; i, j = i+1, j-1 {
			matched[i], matched[j] = matched[j], matched[i]
		}
	}

	if offset >= len(matched) {
		return nil
	}
	matched = matched[offset:]
	if count >= 0 && count < len(matched) {
		matched = matched[:count]
	}
	return matched
}

// CountByLex returns how many members fall inside a lexicographic window.
func (z *ZSet) CountByLex(r LexRange) int {
	n := 0
	for member := range z.dict {
		if r.above(member) && r.below(member) {
			n++
		}
	}
	return n
}

// PopMin removes and returns up to count lowest-scored members.
func (z *ZSet) PopMin(count int) []ZMember { return z.pop(count, false) }

// PopMax removes and returns up to count highest-scored members.
func (z *ZSet) PopMax(count int) []ZMember { return z.pop(count, true) }

func (z *ZSet) pop(count int, fromMax bool) []ZMember {
	if count <= 0 {
		return nil
	}
	out := make([]ZMember, 0, count)
	for i := 0; i < count && z.Len() > 0; i++ {
		var node *skiplistNode
		if fromMax {
			node = z.sl.tail
		} else {
			node = z.sl.header.level[0].forward
		}
		if node == nil {
			break
		}
		out = append(out, ZMember{Member: node.member, Score: node.score})
		z.Remove(node.member)
	}
	return out
}

// Members returns every member name, in score order.
func (z *ZSet) Members() []string {
	out := make([]string, 0, z.Len())
	for node := z.sl.header.level[0].forward; node != nil; node = node.level[0].forward {
		out = append(out, node.member)
	}
	return out
}

// SortedMembers returns member names in byte order, for ZSCAN paging.
func (z *ZSet) SortedMembers() []string {
	out := make([]string, 0, len(z.dict))
	for m := range z.dict {
		out = append(out, m)
	}
	sort.Strings(out)
	return out
}

// ZSetAggregate selects how ZUNIONSTORE and ZINTERSTORE combine the scores of
// a member that appears in several inputs.
type ZSetAggregate int

const (
	AggregateSum ZSetAggregate = iota
	AggregateMin
	AggregateMax
)

func (a ZSetAggregate) apply(existing, incoming float64) float64 {
	switch a {
	case AggregateMin:
		if incoming < existing {
			return incoming
		}
		return existing
	case AggregateMax:
		if incoming > existing {
			return incoming
		}
		return existing
	default:
		return existing + incoming
	}
}

// ZSetUnion merges sets, scaling each by its weight and combining duplicate
// members with the aggregate function.
func ZSetUnion(sets []*ZSet, weights []float64, agg ZSetAggregate) *ZSet {
	out := NewZSet()
	seen := make(map[string]float64)

	for i, z := range sets {
		if z == nil {
			continue
		}
		w := 1.0
		if i < len(weights) {
			w = weights[i]
		}
		for member, score := range z.dict {
			weighted := score * w
			// Multiplying an infinite score by zero yields NaN, which has no
			// ordering. Redis defines the result as zero; match that.
			if weighted != weighted {
				weighted = 0
			}
			if prev, ok := seen[member]; ok {
				seen[member] = agg.apply(prev, weighted)
			} else {
				seen[member] = weighted
			}
		}
	}
	for member, score := range seen {
		out.Add(member, score)
	}
	return out
}

// ZSetInter keeps only members present in every input, combining their scores.
func ZSetInter(sets []*ZSet, weights []float64, agg ZSetAggregate) *ZSet {
	out := NewZSet()
	if len(sets) == 0 {
		return out
	}
	// Drive the walk from the smallest input, as SINTER does.
	smallest := 0
	for i, z := range sets {
		if z == nil || z.Len() == 0 {
			return out
		}
		if z.Len() < sets[smallest].Len() {
			smallest = i
		}
	}

	weightAt := func(i int) float64 {
		if i < len(weights) {
			return weights[i]
		}
		return 1.0
	}
	scale := func(score, w float64) float64 {
		v := score * w
		if v != v {
			return 0
		}
		return v
	}

member:
	for member, score := range sets[smallest].dict {
		total := scale(score, weightAt(smallest))
		for i, z := range sets {
			if i == smallest {
				continue
			}
			other, ok := z.dict[member]
			if !ok {
				continue member
			}
			total = agg.apply(total, scale(other, weightAt(i)))
		}
		out.Add(member, total)
	}
	return out
}
