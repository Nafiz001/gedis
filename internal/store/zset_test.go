package store

import (
	"fmt"
	"math"
	"math/rand"
	"sort"
	"testing"
)

// checkSkiplistInvariants verifies the properties the structure depends on.
//
// Spans are the thing worth checking hard: they are redundant bookkeeping that
// nothing else validates, an off-by-one in insert or delete corrupts them
// silently, and every rank query then returns a plausible but wrong answer.
func checkSkiplistInvariants(t *testing.T, sl *skiplist) {
	t.Helper()

	// Level 0 is the full list: it must be sorted, and its length must match.
	var members []string
	prev := sl.header.level[0].forward
	count := 0
	for node := prev; node != nil; node = node.level[0].forward {
		count++
		members = append(members, node.member)
		if next := node.level[0].forward; next != nil {
			if !(node.score < next.score || (node.score == next.score && node.member < next.member)) {
				t.Fatalf("level 0 out of order: (%v,%s) before (%v,%s)",
					node.score, node.member, next.score, next.member)
			}
		}
		// A level-0 pointer jumps exactly one node. The last node has no
		// forward pointer, and its span is 0 by convention -- the same rule
		// Redis uses, and what unlink relies on when the tail is removed.
		wantSpan := 1
		if node.level[0].forward == nil {
			wantSpan = 0
		}
		if node.level[0].span != wantSpan {
			t.Fatalf("level 0 span of %q is %d, want %d", node.member, node.level[0].span, wantSpan)
		}
	}
	if count != sl.length {
		t.Fatalf("walked %d nodes but length is %d", count, sl.length)
	}

	// Backward pointers must mirror level 0.
	var back []string
	for node := sl.tail; node != nil; node = node.backward {
		back = append(back, node.member)
	}
	if len(back) != len(members) {
		t.Fatalf("backward walk saw %d nodes, forward saw %d", len(back), len(members))
	}
	for i := range back {
		if back[i] != members[len(members)-1-i] {
			t.Fatalf("backward chain disagrees with forward at %d", i)
		}
	}

	// Every span must equal the real distance along level 0.
	position := map[*skiplistNode]int{sl.header: 0}
	pos := 0
	for node := sl.header.level[0].forward; node != nil; node = node.level[0].forward {
		pos++
		position[node] = pos
	}
	for lvl := 0; lvl < sl.level; lvl++ {
		for node := sl.header; node != nil; node = node.level[lvl].forward {
			if lvl >= len(node.level) {
				break
			}
			next := node.level[lvl].forward
			if next == nil {
				continue
			}
			want := position[next] - position[node]
			if node.level[lvl].span != want {
				t.Fatalf("level %d span from %q is %d, want %d",
					lvl, node.member, node.level[lvl].span, want)
			}
		}
	}

	// Rank must agree with the element's actual position.
	for i, m := range members {
		node := sl.NodeByRank(i + 1)
		if node == nil || node.member != m {
			t.Fatalf("NodeByRank(%d) = %v, want %q", i+1, node, m)
		}
		if got := sl.Rank(node.score, node.member); got != i+1 {
			t.Fatalf("Rank(%q) = %d, want %d", m, got, i+1)
		}
	}
}

func TestSkiplistInsertDelete(t *testing.T) {
	sl := newSkiplist()
	checkSkiplistInvariants(t, sl)

	for i := 0; i < 100; i++ {
		sl.Insert(float64(i), fmt.Sprintf("m%03d", i))
	}
	if sl.length != 100 {
		t.Fatalf("length %d, want 100", sl.length)
	}
	checkSkiplistInvariants(t, sl)

	// Delete from the middle, the front and the back.
	for _, i := range []int{50, 0, 99, 25, 75} {
		if !sl.Delete(float64(i), fmt.Sprintf("m%03d", i)) {
			t.Errorf("Delete(%d) reported failure", i)
		}
	}
	if sl.length != 95 {
		t.Errorf("length %d, want 95", sl.length)
	}
	checkSkiplistInvariants(t, sl)

	if sl.Delete(1000, "nosuchmember") {
		t.Error("deleting a missing member reported success")
	}
}

// Equal scores must be ordered by member, which is what makes ZRANGEBYLEX
// meaningful.
func TestSkiplistTiesOrderedByMember(t *testing.T) {
	sl := newSkiplist()
	for _, m := range []string{"delta", "alpha", "charlie", "bravo"} {
		sl.Insert(1.0, m)
	}
	checkSkiplistInvariants(t, sl)

	var got []string
	for node := sl.header.level[0].forward; node != nil; node = node.level[0].forward {
		got = append(got, node.member)
	}
	want := []string{"alpha", "bravo", "charlie", "delta"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

// Randomised insert/delete churn is what actually catches span bugs: the
// promotion heights are random, so a fixed sequence exercises only some of the
// level-adjustment paths.
func TestSkiplistRandomizedInvariants(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	sl := newSkiplist()
	present := map[string]float64{}

	for step := 0; step < 3000; step++ {
		if len(present) > 0 && rng.Float64() < 0.35 {
			// Delete an arbitrary existing member.
			var member string
			for m := range present {
				member = m
				break
			}
			if !sl.Delete(present[member], member) {
				t.Fatalf("step %d: failed to delete %q", step, member)
			}
			delete(present, member)
		} else {
			member := fmt.Sprintf("m%d", rng.Intn(400))
			score := float64(rng.Intn(50))
			if old, exists := present[member]; exists {
				sl.Delete(old, member)
			}
			sl.Insert(score, member)
			present[member] = score
		}

		if step%250 == 0 {
			checkSkiplistInvariants(t, sl)
		}
	}
	checkSkiplistInvariants(t, sl)

	if sl.length != len(present) {
		t.Errorf("length %d, want %d", sl.length, len(present))
	}
}

func TestSkiplistScoreRanges(t *testing.T) {
	sl := newSkiplist()
	for i := 1; i <= 10; i++ {
		sl.Insert(float64(i), fmt.Sprintf("m%02d", i))
	}

	tests := []struct {
		name  string
		r     ScoreRange
		first string
		last  string
		count int
	}{
		{"inclusive", ScoreRange{Min: 3, Max: 6}, "m03", "m06", 4},
		{"exclusive min", ScoreRange{Min: 3, Max: 6, MinExcl: true}, "m04", "m06", 3},
		{"exclusive max", ScoreRange{Min: 3, Max: 6, MaxExcl: true}, "m03", "m05", 3},
		{"both exclusive", ScoreRange{Min: 3, Max: 6, MinExcl: true, MaxExcl: true}, "m04", "m05", 2},
		{"unbounded", ScoreRange{Min: math.Inf(-1), Max: math.Inf(1)}, "m01", "m10", 10},
		{"single point", ScoreRange{Min: 5, Max: 5}, "m05", "m05", 1},
		{"below everything", ScoreRange{Min: -100, Max: 0}, "", "", 0},
		{"above everything", ScoreRange{Min: 100, Max: 200}, "", "", 0},
		{"inverted", ScoreRange{Min: 6, Max: 3}, "", "", 0},
		{"empty point", ScoreRange{Min: 5, Max: 5, MinExcl: true}, "", "", 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			first := sl.FirstInScoreRange(tc.r)
			last := sl.LastInScoreRange(tc.r)

			gotFirst, gotLast := "", ""
			if first != nil {
				gotFirst = first.member
			}
			if last != nil {
				gotLast = last.member
			}
			if gotFirst != tc.first || gotLast != tc.last {
				t.Errorf("first/last = %q/%q, want %q/%q", gotFirst, gotLast, tc.first, tc.last)
			}
			if got := sl.CountInScoreRange(tc.r); got != tc.count {
				t.Errorf("count = %d, want %d", got, tc.count)
			}
		})
	}
}

func TestZSetAddAndScore(t *testing.T) {
	z := NewZSet()

	if added, updated := z.Add("a", 1); !added || updated {
		t.Errorf("first add: added=%v updated=%v", added, updated)
	}
	if added, updated := z.Add("a", 1); added || updated {
		t.Errorf("re-add with the same score: added=%v updated=%v", added, updated)
	}
	if added, updated := z.Add("a", 2); added || !updated {
		t.Errorf("rescore: added=%v updated=%v", added, updated)
	}
	if s, ok := z.Score("a"); !ok || s != 2 {
		t.Errorf("Score = %v, %v; want 2, true", s, ok)
	}
	if _, ok := z.Score("missing"); ok {
		t.Error("Score of a missing member reported success")
	}

	if !z.Remove("a") {
		t.Error("Remove failed")
	}
	if z.Remove("a") {
		t.Error("removing twice reported success")
	}
	if z.Len() != 0 {
		t.Errorf("length %d after removal", z.Len())
	}
	checkSkiplistInvariants(t, z.sl)
}

// A rescore must move the member in the ordering, not just change the map.
func TestZSetRescoreReorders(t *testing.T) {
	z := NewZSet()
	z.Add("a", 1)
	z.Add("b", 2)
	z.Add("c", 3)

	z.Add("a", 100) // 'a' should move to last
	checkSkiplistInvariants(t, z.sl)

	got := z.Members()
	want := []string{"b", "c", "a"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("order after rescore: got %v, want %v", got, want)
	}
	if rank, _ := z.Rank("a", false); rank != 2 {
		t.Errorf("rank of a after rescore: got %d, want 2", rank)
	}
}

func TestZSetRank(t *testing.T) {
	z := NewZSet()
	for i, m := range []string{"a", "b", "c", "d", "e"} {
		z.Add(m, float64(i))
	}

	for i, m := range []string{"a", "b", "c", "d", "e"} {
		rank, ok := z.Rank(m, false)
		if !ok || rank != i {
			t.Errorf("Rank(%q) = %d, %v; want %d", m, rank, ok, i)
		}
		revRank, _ := z.Rank(m, true)
		if want := 4 - i; revRank != want {
			t.Errorf("reverse Rank(%q) = %d, want %d", m, revRank, want)
		}
	}
	if _, ok := z.Rank("missing", false); ok {
		t.Error("rank of a missing member reported success")
	}
}

func TestZSetRangeByRank(t *testing.T) {
	z := NewZSet()
	for i, m := range []string{"a", "b", "c", "d", "e"} {
		z.Add(m, float64(i))
	}

	names := func(ms []ZMember) string {
		out := make([]string, len(ms))
		for i, m := range ms {
			out[i] = m.Member
		}
		return fmt.Sprint(out)
	}

	tests := []struct {
		start, stop int
		reverse     bool
		want        string
	}{
		{0, -1, false, "[a b c d e]"},
		{0, 2, false, "[a b c]"},
		{-2, -1, false, "[d e]"},
		{1, 1, false, "[b]"},
		{0, 100, false, "[a b c d e]"},
		{3, 1, false, "[]"},
		{10, 20, false, "[]"},
		{0, -1, true, "[e d c b a]"},
		{0, 1, true, "[e d]"},
		{-2, -1, true, "[b a]"},
	}
	for _, tc := range tests {
		got := names(z.RangeByRank(tc.start, tc.stop, tc.reverse))
		if got != tc.want {
			t.Errorf("RangeByRank(%d,%d,rev=%v) = %s, want %s",
				tc.start, tc.stop, tc.reverse, got, tc.want)
		}
	}
}

func TestZSetRangeByScore(t *testing.T) {
	z := NewZSet()
	for i := 1; i <= 5; i++ {
		z.Add(fmt.Sprintf("m%d", i), float64(i*10))
	}

	names := func(ms []ZMember) string {
		out := make([]string, len(ms))
		for i, m := range ms {
			out[i] = m.Member
		}
		return fmt.Sprint(out)
	}

	all := ScoreRange{Min: math.Inf(-1), Max: math.Inf(1)}
	if got := names(z.RangeByScore(all, false, 0, -1)); got != "[m1 m2 m3 m4 m5]" {
		t.Errorf("unbounded: %s", got)
	}
	if got := names(z.RangeByScore(all, true, 0, -1)); got != "[m5 m4 m3 m2 m1]" {
		t.Errorf("reversed: %s", got)
	}
	if got := names(z.RangeByScore(ScoreRange{Min: 20, Max: 40}, false, 0, -1)); got != "[m2 m3 m4]" {
		t.Errorf("bounded: %s", got)
	}
	if got := names(z.RangeByScore(all, false, 1, 2)); got != "[m2 m3]" {
		t.Errorf("offset+count: %s", got)
	}
	if got := names(z.RangeByScore(all, true, 1, 2)); got != "[m4 m3]" {
		t.Errorf("reversed offset+count: %s", got)
	}
	if got := names(z.RangeByScore(ScoreRange{Min: 100, Max: 200}, false, 0, -1)); got != "[]" {
		t.Errorf("empty window: %s", got)
	}
}

func TestZSetPop(t *testing.T) {
	build := func() *ZSet {
		z := NewZSet()
		for i := 1; i <= 5; i++ {
			z.Add(fmt.Sprintf("m%d", i), float64(i))
		}
		return z
	}

	z := build()
	got := z.PopMin(2)
	if len(got) != 2 || got[0].Member != "m1" || got[1].Member != "m2" {
		t.Errorf("PopMin(2) = %v", got)
	}
	if z.Len() != 3 {
		t.Errorf("length %d after PopMin", z.Len())
	}
	checkSkiplistInvariants(t, z.sl)

	z = build()
	got = z.PopMax(2)
	if len(got) != 2 || got[0].Member != "m5" || got[1].Member != "m4" {
		t.Errorf("PopMax(2) = %v", got)
	}
	checkSkiplistInvariants(t, z.sl)

	// Popping more than exist drains the set.
	z = build()
	if got := z.PopMin(100); len(got) != 5 {
		t.Errorf("PopMin(100) returned %d", len(got))
	}
	if z.Len() != 0 {
		t.Errorf("length %d after draining", z.Len())
	}
	checkSkiplistInvariants(t, z.sl)
}

func TestZSetUnionInter(t *testing.T) {
	a, b := NewZSet(), NewZSet()
	a.Add("x", 1)
	a.Add("y", 2)
	b.Add("y", 10)
	b.Add("z", 20)

	scoresOf := func(z *ZSet) map[string]float64 {
		out := map[string]float64{}
		for _, m := range z.RangeByRank(0, -1, false) {
			out[m.Member] = m.Score
		}
		return out
	}

	got := scoresOf(ZSetUnion([]*ZSet{a, b}, nil, AggregateSum))
	want := map[string]float64{"x": 1, "y": 12, "z": 20}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("union sum: got %v, want %v", got, want)
	}

	got = scoresOf(ZSetUnion([]*ZSet{a, b}, nil, AggregateMax))
	want = map[string]float64{"x": 1, "y": 10, "z": 20}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("union max: got %v, want %v", got, want)
	}

	got = scoresOf(ZSetUnion([]*ZSet{a, b}, []float64{2, 3}, AggregateSum))
	want = map[string]float64{"x": 2, "y": 34, "z": 60}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("weighted union: got %v, want %v", got, want)
	}

	got = scoresOf(ZSetInter([]*ZSet{a, b}, nil, AggregateSum))
	want = map[string]float64{"y": 12}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("intersection: got %v, want %v", got, want)
	}

	// Intersecting with an empty set yields nothing.
	if n := ZSetInter([]*ZSet{a, NewZSet()}, nil, AggregateSum).Len(); n != 0 {
		t.Errorf("intersection with an empty set has %d members", n)
	}
}

// A ZSet with many members must agree with a plain sorted slice.
func TestZSetMatchesSortedSlice(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	z := NewZSet()
	type entry struct {
		member string
		score  float64
	}
	var expected []entry

	for i := 0; i < 500; i++ {
		m := fmt.Sprintf("m%03d", i)
		s := float64(rng.Intn(100))
		z.Add(m, s)
		expected = append(expected, entry{m, s})
	}
	sort.Slice(expected, func(i, j int) bool {
		if expected[i].score != expected[j].score {
			return expected[i].score < expected[j].score
		}
		return expected[i].member < expected[j].member
	})

	got := z.RangeByRank(0, -1, false)
	if len(got) != len(expected) {
		t.Fatalf("got %d members, want %d", len(got), len(expected))
	}
	for i := range got {
		if got[i].Member != expected[i].member || got[i].Score != expected[i].score {
			t.Fatalf("position %d: got (%s,%v), want (%s,%v)",
				i, got[i].Member, got[i].Score, expected[i].member, expected[i].score)
		}
	}
	for i, e := range expected {
		if rank, _ := z.Rank(e.member, false); rank != i {
			t.Fatalf("Rank(%q) = %d, want %d", e.member, rank, i)
		}
	}
	checkSkiplistInvariants(t, z.sl)
}

func BenchmarkZSetAdd(b *testing.B) {
	z := NewZSet()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		z.Add(fmt.Sprintf("m%d", i), float64(i%10000))
	}
}

func BenchmarkZSetRank(b *testing.B) {
	z := NewZSet()
	for i := 0; i < 100000; i++ {
		z.Add(fmt.Sprintf("m%06d", i), float64(i))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		z.Rank(fmt.Sprintf("m%06d", i%100000), false)
	}
}

func BenchmarkZSetRangeByRank(b *testing.B) {
	z := NewZSet()
	for i := 0; i < 100000; i++ {
		z.Add(fmt.Sprintf("m%06d", i), float64(i))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		z.RangeByRank(50000, 50009, false)
	}
}
