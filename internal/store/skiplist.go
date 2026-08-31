package store

import "math/rand"

// A skiplist is the ordered index behind sorted sets.
//
// It is a linked list whose nodes carry a random number of forward pointers.
// Level 0 links every node; each higher level links roughly a quarter as many,
// so a search starts at the top, skips far ahead, and drops down as it closes
// in -- O(log n) expected, the same as a balanced tree but with no rotations
// and far less code.
//
// The reason Redis chose it over a red-black tree, and the reason this
// implementation matters, is the span field: each forward pointer records how
// many nodes it jumps over. Summing spans along a search path yields an
// element's rank for free, which is what makes ZRANK and ZRANGE-by-index
// O(log n) instead of O(n). Keeping spans correct through insert and delete is
// the whole difficulty of the structure.
//
// Nodes are ordered by score ascending, ties broken by member in byte order --
// the total order sorted sets promise.
const (
	// skiplistMaxLevel caps the tower height. 32 levels at p=0.25 indexes
	// about 4^32 elements, far more than memory allows.
	skiplistMaxLevel = 32
	// skiplistP is the probability a node is promoted to the next level.
	// 0.25 is Redis's choice: it costs less memory than 0.5 and searches
	// only marginally slower.
	skiplistP = 0.25
)

type skiplistLevel struct {
	forward *skiplistNode
	// span is how many nodes this pointer jumps over, counting the
	// destination. A level-0 pointer always has span 1.
	span int
}

type skiplistNode struct {
	member   string
	score    float64
	backward *skiplistNode
	level    []skiplistLevel
}

type skiplist struct {
	// header is a sentinel with the maximum tower height. It holds no data,
	// which removes the special cases around inserting at the front.
	header *skiplistNode
	tail   *skiplistNode
	length int
	// level is the height currently in use, so searches skip empty towers.
	level int
}

func newSkiplist() *skiplist {
	return &skiplist{
		header: &skiplistNode{level: make([]skiplistLevel, skiplistMaxLevel)},
		level:  1,
	}
}

// randomLevel picks a tower height, geometrically distributed.
func randomLevel() int {
	level := 1
	for level < skiplistMaxLevel && rand.Float64() < skiplistP {
		level++
	}
	return level
}

// lessThan reports whether (score, member) sorts before the given node.
func nodeBefore(n *skiplistNode, score float64, member string) bool {
	return n.score < score || (n.score == score && n.member < member)
}

// Insert adds a new node. The caller must have established that the member is
// not already present -- sorted sets update a score by deleting and reinserting,
// because the score is part of the sort key.
func (sl *skiplist) Insert(score float64, member string) *skiplistNode {
	var update [skiplistMaxLevel]*skiplistNode
	// rank[i] is how far along the list the search had travelled when it left
	// level i. The differences between these are what the new spans are
	// computed from.
	var rank [skiplistMaxLevel]int

	x := sl.header
	for i := sl.level - 1; i >= 0; i-- {
		if i == sl.level-1 {
			rank[i] = 0
		} else {
			rank[i] = rank[i+1]
		}
		for x.level[i].forward != nil && nodeBefore(x.level[i].forward, score, member) {
			rank[i] += x.level[i].span
			x = x.level[i].forward
		}
		update[i] = x
	}

	level := randomLevel()
	if level > sl.level {
		// New levels start at the header and span the entire list, since
		// nothing else has been promoted that high yet.
		for i := sl.level; i < level; i++ {
			rank[i] = 0
			update[i] = sl.header
			update[i].level[i].span = sl.length
		}
		sl.level = level
	}

	node := &skiplistNode{
		member: member,
		score:  score,
		level:  make([]skiplistLevel, level),
	}
	for i := 0; i < level; i++ {
		node.level[i].forward = update[i].level[i].forward
		update[i].level[i].forward = node

		// The predecessor's old span covered the gap it now shares with the
		// new node; split it at the insertion point.
		node.level[i].span = update[i].level[i].span - (rank[0] - rank[i])
		update[i].level[i].span = (rank[0] - rank[i]) + 1
	}
	// Towers taller than the new node simply jump over one more element.
	for i := level; i < sl.level; i++ {
		update[i].level[i].span++
	}

	if update[0] != sl.header {
		node.backward = update[0]
	}
	if node.level[0].forward != nil {
		node.level[0].forward.backward = node
	} else {
		sl.tail = node
	}
	sl.length++
	return node
}

// unlink removes x, whose predecessors at every level are given by update.
func (sl *skiplist) unlink(x *skiplistNode, update *[skiplistMaxLevel]*skiplistNode) {
	for i := 0; i < sl.level; i++ {
		if update[i].level[i].forward == x {
			// Absorb the removed node's span into its predecessor.
			update[i].level[i].span += x.level[i].span - 1
			update[i].level[i].forward = x.level[i].forward
		} else {
			// This tower passed over x, so it now jumps one fewer node.
			update[i].level[i].span--
		}
	}
	if x.level[0].forward != nil {
		x.level[0].forward.backward = x.backward
	} else {
		sl.tail = x.backward
	}
	// Drop levels that no longer hold anything, so searches do not walk
	// empty towers.
	for sl.level > 1 && sl.header.level[sl.level-1].forward == nil {
		sl.level--
	}
	sl.length--
}

// Delete removes the node with this exact score and member.
func (sl *skiplist) Delete(score float64, member string) bool {
	var update [skiplistMaxLevel]*skiplistNode

	x := sl.header
	for i := sl.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil && nodeBefore(x.level[i].forward, score, member) {
			x = x.level[i].forward
		}
		update[i] = x
	}

	x = x.level[0].forward
	if x == nil || x.score != score || x.member != member {
		return false
	}
	sl.unlink(x, &update)
	return true
}

// Rank returns the 1-based position of a member, or 0 if it is absent.
//
// The rank falls out of the search itself: every time the walk follows a
// pointer it adds that pointer's span, so arriving at the node means the
// running total is its position.
func (sl *skiplist) Rank(score float64, member string) int {
	rank := 0
	x := sl.header
	for i := sl.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil &&
			(x.level[i].forward.score < score ||
				(x.level[i].forward.score == score && x.level[i].forward.member <= member)) {
			rank += x.level[i].span
			x = x.level[i].forward
		}
		if x != sl.header && x.member == member {
			return rank
		}
	}
	return 0
}

// NodeByRank returns the node at a 1-based position, or nil.
func (sl *skiplist) NodeByRank(rank int) *skiplistNode {
	if rank <= 0 || rank > sl.length {
		return nil
	}
	traversed := 0
	x := sl.header
	for i := sl.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil && traversed+x.level[i].span <= rank {
			traversed += x.level[i].span
			x = x.level[i].forward
		}
		if traversed == rank {
			return x
		}
	}
	return nil
}

// ScoreRange describes a ZRANGEBYSCORE window. Infinite bounds are expressed
// as math.Inf, so no separate "unbounded" flag is needed.
type ScoreRange struct {
	Min, Max         float64
	MinExcl, MaxExcl bool
}

func (r ScoreRange) aboveMin(score float64) bool {
	if r.MinExcl {
		return score > r.Min
	}
	return score >= r.Min
}

func (r ScoreRange) belowMax(score float64) bool {
	if r.MaxExcl {
		return score < r.Max
	}
	return score <= r.Max
}

// IsEmpty reports ranges that can contain nothing, so callers can skip the
// search entirely.
func (r ScoreRange) IsEmpty() bool {
	if r.Min > r.Max {
		return true
	}
	return r.Min == r.Max && (r.MinExcl || r.MaxExcl)
}

// FirstInScoreRange returns the lowest-ranked node inside the range, in
// O(log n): the search descends to the last node below the window, then steps
// forward once.
func (sl *skiplist) FirstInScoreRange(r ScoreRange) *skiplistNode {
	if r.IsEmpty() {
		return nil
	}
	x := sl.header
	for i := sl.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil && !r.aboveMin(x.level[i].forward.score) {
			x = x.level[i].forward
		}
	}
	x = x.level[0].forward
	if x == nil || !r.belowMax(x.score) {
		return nil
	}
	return x
}

// LastInScoreRange returns the highest-ranked node inside the range.
func (sl *skiplist) LastInScoreRange(r ScoreRange) *skiplistNode {
	if r.IsEmpty() {
		return nil
	}
	x := sl.header
	for i := sl.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil && r.belowMax(x.level[i].forward.score) {
			x = x.level[i].forward
		}
	}
	if x == sl.header || !r.aboveMin(x.score) {
		return nil
	}
	return x
}

// CountInScoreRange returns how many nodes fall inside the range, without
// materialising them.
//
// It ranks the first and last nodes in the window and subtracts, so the cost is
// two O(log n) searches rather than a walk over the results -- which is what
// makes ZCOUNT cheap on a window holding millions of elements.
func (sl *skiplist) CountInScoreRange(r ScoreRange) int {
	first := sl.FirstInScoreRange(r)
	if first == nil {
		return 0
	}
	last := sl.LastInScoreRange(r)
	if last == nil {
		return 0
	}
	return sl.Rank(last.score, last.member) - sl.Rank(first.score, first.member) + 1
}
