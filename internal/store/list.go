package store

// List is the value type behind LPUSH/RPUSH and friends: an ordered sequence of
// binary-safe strings with cheap access at both ends.
//
// It is a ring buffer rather than a linked list. Redis uses a quicklist -- a
// linked list of compressed listpacks -- which optimises for memory over speed.
// A ring buffer makes a different trade that suits this server: pushes and pops
// at either end are O(1) amortised, LINDEX and LRANGE are O(1) and O(range)
// instead of O(n), and the elements sit in one contiguous slice, so walking a
// range touches far fewer cache lines than chasing node pointers. The cost is
// per-element overhead in the slice header and O(n) inserts in the middle,
// which LINSERT and LREM pay in Redis too.
//
// A List is not safe for concurrent use. Callers mutate it while holding the
// owning shard's write lock, which store.Update provides.
type List struct {
	// buf holds elements in ring order; only n of its slots are live.
	buf  [][]byte
	head int // slot of the first element
	n    int
}

// minListCap is the smallest backing array a non-empty list keeps. Below this,
// resizing costs more than the memory it saves.
const minListCap = 8

// NewList returns an empty list.
func NewList() *List { return &List{} }

// Len returns the number of elements.
func (l *List) Len() int { return l.n }

// slot maps a logical index to its position in the ring.
func (l *List) slot(i int) int {
	return (l.head + i) % len(l.buf)
}

// resize reallocates the backing array to exactly capacity slots, unrolling the
// ring so the elements start at slot 0. capacity must be at least l.n.
//
// Growth policy lives in grow and shrink, not here. Keeping the mechanism
// separate from the policy is what stops a shrink from accidentally asking for
// a bigger array than it started with.
func (l *List) resize(capacity int) {
	if capacity < minListCap {
		capacity = minListCap
	}
	buf := make([][]byte, capacity)
	for i := 0; i < l.n; i++ {
		buf[i] = l.buf[l.slot(i)]
	}
	l.buf = buf
	l.head = 0
}

// grow expands the backing array to hold at least want elements, doubling so
// that a run of pushes costs amortised O(1) each.
func (l *List) grow(want int) {
	capacity := len(l.buf) * 2
	if capacity < minListCap {
		capacity = minListCap
	}
	for capacity < want {
		capacity *= 2
	}
	l.resize(capacity)
}

// shrink releases memory after a list has drained.
//
// Without this, a list that grew to a million elements and was then popped
// empty would hold the million-slot array forever. The quarter-full threshold
// (rather than half) leaves hysteresis, so a list hovering near a power of two
// does not resize on every operation.
func (l *List) shrink() {
	if len(l.buf) <= minListCap || l.n > len(l.buf)/4 {
		return
	}
	l.resize(len(l.buf) / 2)
}

// PushFront prepends a value.
func (l *List) PushFront(v []byte) {
	if l.n == len(l.buf) {
		l.grow(l.n + 1)
	}
	l.head = (l.head - 1 + len(l.buf)) % len(l.buf)
	l.buf[l.head] = v
	l.n++
}

// PushBack appends a value.
func (l *List) PushBack(v []byte) {
	if l.n == len(l.buf) {
		l.grow(l.n + 1)
	}
	l.buf[l.slot(l.n)] = v
	l.n++
}

// PopFront removes and returns the first element.
func (l *List) PopFront() ([]byte, bool) {
	if l.n == 0 {
		return nil, false
	}
	v := l.buf[l.head]
	// Clear the slot so the element can be collected; a stale reference in a
	// large backing array is a real leak.
	l.buf[l.head] = nil
	l.head = (l.head + 1) % len(l.buf)
	l.n--
	l.shrink()
	return v, true
}

// PopBack removes and returns the last element.
func (l *List) PopBack() ([]byte, bool) {
	if l.n == 0 {
		return nil, false
	}
	i := l.slot(l.n - 1)
	v := l.buf[i]
	l.buf[i] = nil
	l.n--
	l.shrink()
	return v, true
}

// normalizeIndex converts a possibly-negative index into a real one, reporting
// whether it lands inside the list.
func (l *List) normalizeIndex(i int) (int, bool) {
	if i < 0 {
		i += l.n
	}
	if i < 0 || i >= l.n {
		return 0, false
	}
	return i, true
}

// Index returns the element at i, where -1 is the last element.
func (l *List) Index(i int) ([]byte, bool) {
	idx, ok := l.normalizeIndex(i)
	if !ok {
		return nil, false
	}
	return l.buf[l.slot(idx)], true
}

// Set replaces the element at i.
func (l *List) Set(i int, v []byte) bool {
	idx, ok := l.normalizeIndex(i)
	if !ok {
		return false
	}
	l.buf[l.slot(idx)] = v
	return true
}

// resolveRange clamps an inclusive start/stop pair to the list, the way LRANGE
// and LTRIM define it: negative counts from the end, out-of-range ends are
// pulled in, and an inverted range is empty.
func (l *List) resolveRange(start, stop int) (int, int, bool) {
	if start < 0 {
		start += l.n
	}
	if stop < 0 {
		stop += l.n
	}
	if start < 0 {
		start = 0
	}
	if stop >= l.n {
		stop = l.n - 1
	}
	if start > stop || start >= l.n || l.n == 0 {
		return 0, 0, false
	}
	return start, stop, true
}

// Range returns the elements from start to stop inclusive.
func (l *List) Range(start, stop int) [][]byte {
	from, to, ok := l.resolveRange(start, stop)
	if !ok {
		return nil
	}
	out := make([][]byte, 0, to-from+1)
	for i := from; i <= to; i++ {
		out = append(out, l.buf[l.slot(i)])
	}
	return out
}

// Trim keeps only the elements from start to stop inclusive, discarding the
// rest. An empty result leaves a list of length zero, which the caller deletes.
func (l *List) Trim(start, stop int) {
	from, to, ok := l.resolveRange(start, stop)
	if !ok {
		l.buf = nil
		l.head = 0
		l.n = 0
		return
	}
	if from == 0 && to == l.n-1 {
		return
	}

	keep := to - from + 1
	buf := make([][]byte, max(keep, minListCap))
	for i := 0; i < keep; i++ {
		buf[i] = l.buf[l.slot(from+i)]
	}
	l.buf = buf
	l.head = 0
	l.n = keep
}

// removeAt deletes the element at logical index i.
//
// It shifts whichever side is shorter, so removing near either end stays cheap
// even in a long list -- which is what makes LREM on a queue's head affordable.
func (l *List) removeAt(i int) {
	if i < l.n-1-i {
		// Closer to the front: slide the prefix forward.
		for j := i; j > 0; j-- {
			l.buf[l.slot(j)] = l.buf[l.slot(j-1)]
		}
		l.buf[l.head] = nil
		l.head = (l.head + 1) % len(l.buf)
	} else {
		// Closer to the back: slide the suffix back.
		for j := i; j < l.n-1; j++ {
			l.buf[l.slot(j)] = l.buf[l.slot(j+1)]
		}
		l.buf[l.slot(l.n-1)] = nil
	}
	l.n--
	l.shrink()
}

// insertAt places v at logical index i, shifting the shorter side.
func (l *List) insertAt(i int, v []byte) {
	if l.n == len(l.buf) {
		l.grow(l.n + 1)
	}
	if i < l.n-i {
		// Extend at the front and slide the prefix backward.
		l.head = (l.head - 1 + len(l.buf)) % len(l.buf)
		l.n++
		for j := 0; j < i; j++ {
			l.buf[l.slot(j)] = l.buf[l.slot(j+1)]
		}
	} else {
		l.n++
		for j := l.n - 1; j > i; j-- {
			l.buf[l.slot(j)] = l.buf[l.slot(j-1)]
		}
	}
	l.buf[l.slot(i)] = v
}

// Insert places value before or after the first element equal to pivot. It
// returns the new length, or -1 if pivot is not present.
func (l *List) Insert(pivot, value []byte, before bool) int {
	for i := 0; i < l.n; i++ {
		if bytesEqual(l.buf[l.slot(i)], pivot) {
			if before {
				l.insertAt(i, value)
			} else {
				l.insertAt(i+1, value)
			}
			return l.n
		}
	}
	return -1
}

// Remove deletes elements equal to value and returns how many went.
//
// count > 0 removes that many starting from the head, count < 0 that many from
// the tail, and count == 0 removes every match.
func (l *List) Remove(count int, value []byte) int {
	removed := 0

	switch {
	case count >= 0:
		limit := count
		for i := 0; i < l.n; {
			if (limit == 0 || removed < limit) && bytesEqual(l.buf[l.slot(i)], value) {
				l.removeAt(i)
				removed++
				if limit > 0 && removed == limit {
					break
				}
				continue // the element at i is now the next one
			}
			i++
		}
	default:
		limit := -count
		for i := l.n - 1; i >= 0; i-- {
			if bytesEqual(l.buf[l.slot(i)], value) {
				l.removeAt(i)
				removed++
				if removed == limit {
					break
				}
			}
		}
	}
	return removed
}

// Pos returns the logical indexes of elements equal to value.
//
// rank selects which match to start from (1 is the first, -1 the last, scanning
// backward); count caps how many indexes come back, with 0 meaning all;
// maxlen caps how many elements are compared, with 0 meaning no limit.
func (l *List) Pos(value []byte, rank, count, maxlen int) []int {
	if rank == 0 || l.n == 0 {
		return nil
	}

	var out []int
	compared, matches := 0, 0
	backward := rank < 0
	skip := rank - 1
	if backward {
		skip = -rank - 1
	}

	for step := 0; step < l.n; step++ {
		i := step
		if backward {
			i = l.n - 1 - step
		}
		compared++
		if maxlen > 0 && compared > maxlen {
			break
		}
		if !bytesEqual(l.buf[l.slot(i)], value) {
			continue
		}
		if matches < skip {
			matches++
			continue
		}
		out = append(out, i)
		if count > 0 && len(out) == count {
			break
		}
		if count == 0 && len(out) == 1 {
			break // COUNT omitted: a single index is requested
		}
	}
	return out
}

// All returns every element in order. Used by persistence and by tests.
func (l *List) All() [][]byte {
	out := make([][]byte, 0, l.n)
	for i := 0; i < l.n; i++ {
		out = append(out, l.buf[l.slot(i)])
	}
	return out
}

// bytesEqual compares two binary-safe strings.
func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
