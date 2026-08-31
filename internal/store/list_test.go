package store

import (
	"fmt"
	"strings"
	"testing"
)

// contents renders a list as a comma-joined string, which makes the
// expectations below readable.
func contents(l *List) string {
	parts := make([]string, 0, l.Len())
	for _, b := range l.All() {
		parts = append(parts, string(b))
	}
	return strings.Join(parts, ",")
}

func listOf(items ...string) *List {
	l := NewList()
	for _, s := range items {
		l.PushBack([]byte(s))
	}
	return l
}

func TestListPushPop(t *testing.T) {
	l := NewList()
	if l.Len() != 0 {
		t.Fatalf("new list has length %d", l.Len())
	}
	if _, ok := l.PopFront(); ok {
		t.Error("PopFront on an empty list returned a value")
	}
	if _, ok := l.PopBack(); ok {
		t.Error("PopBack on an empty list returned a value")
	}

	l.PushBack([]byte("b"))
	l.PushBack([]byte("c"))
	l.PushFront([]byte("a"))
	if got := contents(l); got != "a,b,c" {
		t.Errorf("got %q, want a,b,c", got)
	}

	v, _ := l.PopFront()
	if string(v) != "a" {
		t.Errorf("PopFront: got %q", v)
	}
	v, _ = l.PopBack()
	if string(v) != "c" {
		t.Errorf("PopBack: got %q", v)
	}
	if got := contents(l); got != "b" {
		t.Errorf("got %q, want b", got)
	}
}

// The ring buffer must stay correct as it grows, wraps, and shrinks.
func TestListRingBufferWrapAndGrow(t *testing.T) {
	l := NewList()
	// Alternate ends so head walks backwards through the ring while the tail
	// walks forwards -- the pattern that exposes modulo mistakes.
	for i := 0; i < 500; i++ {
		if i%2 == 0 {
			l.PushBack([]byte(fmt.Sprintf("b%d", i)))
		} else {
			l.PushFront([]byte(fmt.Sprintf("f%d", i)))
		}
	}
	if l.Len() != 500 {
		t.Fatalf("length %d, want 500", l.Len())
	}

	// Every element must still be reachable by index, in order.
	all := l.All()
	for i := range all {
		got, ok := l.Index(i)
		if !ok || string(got) != string(all[i]) {
			t.Fatalf("Index(%d) disagrees with All()", i)
		}
	}
	// Negative indexes count from the end.
	if v, _ := l.Index(-1); string(v) != string(all[499]) {
		t.Error("Index(-1) is not the last element")
	}

	// Drain from alternating ends; the list must empty cleanly.
	for i := 0; l.Len() > 0; i++ {
		var ok bool
		if i%2 == 0 {
			_, ok = l.PopFront()
		} else {
			_, ok = l.PopBack()
		}
		if !ok {
			t.Fatal("pop failed while draining")
		}
	}
	if l.Len() != 0 {
		t.Errorf("length %d after draining", l.Len())
	}
	// The backing array must have been released, not left at 512 slots.
	if len(l.buf) > minListCap {
		t.Errorf("backing array is %d slots after draining, want <= %d", len(l.buf), minListCap)
	}

	// It must still be usable afterwards.
	l.PushBack([]byte("again"))
	if got := contents(l); got != "again" {
		t.Errorf("reuse after drain: got %q", got)
	}
}

// Popping must clear the slot, or the backing array pins dead elements.
func TestListPopClearsSlot(t *testing.T) {
	l := listOf("a", "b", "c")
	l.PopFront()
	for i, slot := range l.buf {
		if slot != nil && string(slot) == "a" {
			t.Errorf("slot %d still references the popped element", i)
		}
	}
}

func TestListIndexAndSet(t *testing.T) {
	l := listOf("a", "b", "c")

	tests := []struct {
		i    int
		want string
		ok   bool
	}{
		{0, "a", true}, {1, "b", true}, {2, "c", true},
		{-1, "c", true}, {-2, "b", true}, {-3, "a", true},
		{3, "", false}, {-4, "", false}, {100, "", false},
	}
	for _, tc := range tests {
		got, ok := l.Index(tc.i)
		if ok != tc.ok || (ok && string(got) != tc.want) {
			t.Errorf("Index(%d) = %q,%v; want %q,%v", tc.i, got, ok, tc.want, tc.ok)
		}
	}

	if !l.Set(1, []byte("B")) {
		t.Error("Set(1) failed")
	}
	if !l.Set(-1, []byte("C")) {
		t.Error("Set(-1) failed")
	}
	if l.Set(99, []byte("x")) {
		t.Error("Set(99) reported success")
	}
	if got := contents(l); got != "a,B,C" {
		t.Errorf("got %q, want a,B,C", got)
	}
}

func TestListRange(t *testing.T) {
	l := listOf("a", "b", "c", "d", "e")

	tests := []struct {
		start, stop int
		want        string
	}{
		{0, -1, "a,b,c,d,e"},
		{0, 2, "a,b,c"},
		{-3, -1, "c,d,e"},
		{1, 1, "b"},
		{0, 100, "a,b,c,d,e"}, // stop past the end is clamped
		{-100, 100, "a,b,c,d,e"},
		{3, 1, ""},   // inverted range is empty
		{10, 20, ""}, // entirely past the end
	}
	for _, tc := range tests {
		got := make([]string, 0)
		for _, b := range l.Range(tc.start, tc.stop) {
			got = append(got, string(b))
		}
		if strings.Join(got, ",") != tc.want {
			t.Errorf("Range(%d,%d) = %v, want %q", tc.start, tc.stop, got, tc.want)
		}
	}
}

func TestListTrim(t *testing.T) {
	tests := []struct {
		start, stop int
		want        string
	}{
		{0, -1, "a,b,c,d,e"},
		{1, 3, "b,c,d"},
		{-2, -1, "d,e"},
		{0, 0, "a"},
		{3, 1, ""},   // inverted: empties the list
		{10, 20, ""}, // out of range: empties the list
	}
	for _, tc := range tests {
		l := listOf("a", "b", "c", "d", "e")
		l.Trim(tc.start, tc.stop)
		if got := contents(l); got != tc.want {
			t.Errorf("Trim(%d,%d) = %q, want %q", tc.start, tc.stop, got, tc.want)
		}
		if l.Len() != len(l.All()) {
			t.Errorf("Trim(%d,%d): Len is %d but All returned %d", tc.start, tc.stop, l.Len(), len(l.All()))
		}
	}
}

func TestListInsert(t *testing.T) {
	l := listOf("a", "c")
	if n := l.Insert([]byte("c"), []byte("b"), true); n != 3 {
		t.Errorf("Insert before: got %d, want 3", n)
	}
	if got := contents(l); got != "a,b,c" {
		t.Errorf("got %q, want a,b,c", got)
	}

	if n := l.Insert([]byte("c"), []byte("d"), false); n != 4 {
		t.Errorf("Insert after: got %d, want 4", n)
	}
	if got := contents(l); got != "a,b,c,d" {
		t.Errorf("got %q, want a,b,c,d", got)
	}

	// Inserting before the head and after the tail both work.
	l.Insert([]byte("a"), []byte("start"), true)
	l.Insert([]byte("d"), []byte("end"), false)
	if got := contents(l); got != "start,a,b,c,d,end" {
		t.Errorf("got %q", got)
	}

	if n := l.Insert([]byte("nosuchpivot"), []byte("x"), true); n != -1 {
		t.Errorf("Insert with a missing pivot: got %d, want -1", n)
	}
}

// Inserting into a long list must shift the shorter side and stay correct.
func TestListInsertLarge(t *testing.T) {
	l := NewList()
	for i := 0; i < 200; i++ {
		l.PushBack([]byte(fmt.Sprintf("%d", i)))
	}
	// Near the front, near the back, and in the middle.
	l.Insert([]byte("5"), []byte("front"), true)
	l.Insert([]byte("195"), []byte("back"), false)
	l.Insert([]byte("100"), []byte("mid"), true)

	if l.Len() != 203 {
		t.Fatalf("length %d, want 203", l.Len())
	}
	all := l.All()
	find := func(want string) int {
		for i, b := range all {
			if string(b) == want {
				return i
			}
		}
		return -1
	}
	if i := find("front"); i < 0 || string(all[i+1]) != "5" {
		t.Errorf("front insert landed wrong (index %d)", i)
	}
	if i := find("back"); i < 0 || string(all[i-1]) != "195" {
		t.Errorf("back insert landed wrong (index %d)", i)
	}
	if i := find("mid"); i < 0 || string(all[i+1]) != "100" {
		t.Errorf("mid insert landed wrong (index %d)", i)
	}
}

func TestListRemove(t *testing.T) {
	tests := []struct {
		name    string
		count   int
		want    string
		removed int
	}{
		{"all", 0, "b,c", 3},
		// Verified against real Redis: LREM key 2 a on [a b a c a] leaves
		// [b c a] -- it removes the first two matches, not the first and last.
		{"first two from head", 2, "b,c,a", 2},
		{"one from head", 1, "b,a,c,a", 1},
		{"one from tail", -1, "a,b,a,c", 1},
		{"two from tail", -2, "a,b,c", 2},
		{"more than present", 10, "b,c", 3},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			l := listOf("a", "b", "a", "c", "a")
			got := l.Remove(tc.count, []byte("a"))
			if got != tc.removed {
				t.Errorf("removed %d, want %d", got, tc.removed)
			}
			if c := contents(l); c != tc.want {
				t.Errorf("contents %q, want %q", c, tc.want)
			}
		})
	}
}

func TestListPos(t *testing.T) {
	l := listOf("a", "b", "c", "b", "d", "b")

	tests := []struct {
		name                string
		rank, count, maxlen int
		want                []int
	}{
		{"first match", 1, 0, 0, []int{1}},
		{"second match", 2, 0, 0, []int{3}},
		{"last match", -1, 0, 0, []int{5}},
		{"second from tail", -2, 0, 0, []int{3}},
		{"all matches", 1, -1, 0, []int{1, 3, 5}},
		{"first two", 1, 2, 0, []int{1, 3}},
		{"limited comparisons", 1, -1, 2, []int{1}},
		{"rank past the end", 9, 0, 0, nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := l.Pos([]byte("b"), tc.rank, tc.count, tc.maxlen)
			if fmt.Sprint(got) != fmt.Sprint(tc.want) {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}

	if got := l.Pos([]byte("nothere"), 1, 0, 0); got != nil {
		t.Errorf("missing element: got %v", got)
	}
}

func BenchmarkListPushPopFront(b *testing.B) {
	l := NewList()
	v := []byte("value")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l.PushFront(v)
		l.PopFront()
	}
}

func BenchmarkListIndex(b *testing.B) {
	l := NewList()
	for i := 0; i < 10000; i++ {
		l.PushBack([]byte("value"))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l.Index(i % 10000)
	}
}
