package store

import "sort"

// Hash is the value type behind HSET/HGET: a map from field names to
// binary-safe values.
//
// A plain Go map is the right structure here. Redis switches between a listpack
// (a flat array, scanned linearly) for small hashes and a real hash table for
// large ones, purely to save memory on the many-small-hashes case. That
// optimisation buys nothing without Redis's memory layout, and it would put a
// size check on every access, so gedis uses one representation throughout.
//
// Not safe for concurrent use; callers hold the owning shard's write lock.
type Hash struct {
	m map[string][]byte
}

func NewHash() *Hash { return &Hash{m: make(map[string][]byte)} }

func (h *Hash) Len() int { return len(h.m) }

func (h *Hash) Get(field string) ([]byte, bool) {
	v, ok := h.m[field]
	return v, ok
}

// Set stores a field, reporting whether it was newly created rather than
// overwritten -- the count HSET replies with.
func (h *Hash) Set(field string, value []byte) bool {
	_, existed := h.m[field]
	h.m[field] = value
	return !existed
}

// SetNX stores a field only if it is absent.
func (h *Hash) SetNX(field string, value []byte) bool {
	if _, existed := h.m[field]; existed {
		return false
	}
	h.m[field] = value
	return true
}

// Delete removes fields and returns how many were present.
func (h *Hash) Delete(fields ...string) int {
	n := 0
	for _, f := range fields {
		if _, ok := h.m[f]; ok {
			delete(h.m, f)
			n++
		}
	}
	return n
}

func (h *Hash) Exists(field string) bool {
	_, ok := h.m[field]
	return ok
}

// Fields returns the field names, in no particular order.
func (h *Hash) Fields() []string {
	out := make([]string, 0, len(h.m))
	for f := range h.m {
		out = append(out, f)
	}
	return out
}

// Values returns the values, in no particular order.
func (h *Hash) Values() [][]byte {
	out := make([][]byte, 0, len(h.m))
	for _, v := range h.m {
		out = append(out, v)
	}
	return out
}

// HashPair is one field/value pair.
type HashPair struct {
	Field string
	Value []byte
}

// All returns every pair, in no particular order.
func (h *Hash) All() []HashPair {
	out := make([]HashPair, 0, len(h.m))
	for f, v := range h.m {
		out = append(out, HashPair{Field: f, Value: v})
	}
	return out
}

// SortedFields returns the field names in byte order. Used by HSCAN, which
// needs a stable order to page through.
func (h *Hash) SortedFields() []string {
	out := h.Fields()
	sort.Strings(out)
	return out
}

// Set is the value type behind SADD: an unordered collection of unique
// binary-safe strings.
//
// Members are map keys, so uniqueness and membership come for free. Redis
// additionally keeps an "intset" encoding for sets that hold only integers,
// which halves memory for that case; gedis does not, for the same reason it
// skips listpacks.
type Set struct {
	m map[string]struct{}
}

func NewSet() *Set { return &Set{m: make(map[string]struct{})} }

func (s *Set) Len() int { return len(s.m) }

// Add inserts a member, reporting whether it was new.
func (s *Set) Add(member string) bool {
	if _, ok := s.m[member]; ok {
		return false
	}
	s.m[member] = struct{}{}
	return true
}

// Remove deletes a member, reporting whether it was present.
func (s *Set) Remove(member string) bool {
	if _, ok := s.m[member]; !ok {
		return false
	}
	delete(s.m, member)
	return true
}

func (s *Set) Contains(member string) bool {
	_, ok := s.m[member]
	return ok
}

// Members returns every member, in no particular order.
func (s *Set) Members() []string {
	out := make([]string, 0, len(s.m))
	for m := range s.m {
		out = append(out, m)
	}
	return out
}

// SortedMembers returns members in byte order, for SSCAN paging.
func (s *Set) SortedMembers() []string {
	out := s.Members()
	sort.Strings(out)
	return out
}

// Clone returns an independent copy, used as the accumulator for the
// multi-set operations so the stored sets are never mutated.
func (s *Set) Clone() *Set {
	out := &Set{m: make(map[string]struct{}, len(s.m))}
	for m := range s.m {
		out.m[m] = struct{}{}
	}
	return out
}

// SetUnion returns every member of every input set.
func SetUnion(sets ...*Set) *Set {
	out := NewSet()
	for _, s := range sets {
		if s == nil {
			continue
		}
		for m := range s.m {
			out.m[m] = struct{}{}
		}
	}
	return out
}

// SetInter returns the members present in every input set.
//
// It walks the smallest set and probes the others, so the cost is
// O(smallest * count) rather than O(total). With one small set and several
// large ones -- the common shape of a tag intersection -- that is the
// difference between scanning hundreds and scanning millions.
func SetInter(sets ...*Set) *Set {
	if len(sets) == 0 {
		return NewSet()
	}
	smallest := 0
	for i, s := range sets {
		if s == nil || s.Len() == 0 {
			return NewSet() // intersecting with an empty set is empty
		}
		if s.Len() < sets[smallest].Len() {
			smallest = i
		}
	}

	out := NewSet()
member:
	for m := range sets[smallest].m {
		for i, s := range sets {
			if i == smallest {
				continue
			}
			if _, ok := s.m[m]; !ok {
				continue member
			}
		}
		out.m[m] = struct{}{}
	}
	return out
}

// SetDiff returns the members of the first set that appear in none of the rest.
func SetDiff(sets ...*Set) *Set {
	if len(sets) == 0 || sets[0] == nil {
		return NewSet()
	}
	out := NewSet()
member:
	for m := range sets[0].m {
		for _, s := range sets[1:] {
			if s == nil {
				continue
			}
			if _, ok := s.m[m]; ok {
				continue member
			}
		}
		out.m[m] = struct{}{}
	}
	return out
}
