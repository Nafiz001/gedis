package store

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

// fakeClock lets expiration tests advance time explicitly instead of sleeping,
// which makes them fast and free of flakes.
type fakeClock struct{ ms atomic.Int64 }

func (c *fakeClock) now() int64       { return c.ms.Load() }
func (c *fakeClock) advance(ms int64) { c.ms.Add(ms) }

func newTestStore(t *testing.T) (*Store, *fakeClock) {
	t.Helper()
	clk := &fakeClock{}
	clk.ms.Store(1_700_000_000_000)
	s := New(WithClock(clk.now), WithShards(8))
	t.Cleanup(s.Close)
	return s, clk
}

func str(v any) string {
	b, ok := v.([]byte)
	if !ok {
		return fmt.Sprintf("<not a string: %T>", v)
	}
	return string(b)
}

func TestSetGetDelete(t *testing.T) {
	s, _ := newTestStore(t)

	if _, ok := s.Get("missing"); ok {
		t.Error("Get on an empty store returned a value")
	}

	s.Set("k", []byte("v"), NoExpiry)
	v, ok := s.Get("k")
	if !ok || str(v) != "v" {
		t.Errorf("Get after Set: got %q, %v", str(v), ok)
	}

	s.Set("k", []byte("v2"), NoExpiry)
	if v, _ := s.Get("k"); str(v) != "v2" {
		t.Errorf("Set did not overwrite: got %q", str(v))
	}

	if n := s.Delete("k", "missing"); n != 1 {
		t.Errorf("Delete returned %d, want 1", n)
	}
	if s.Exists("k") {
		t.Error("key still present after Delete")
	}
}

func TestExpiration(t *testing.T) {
	s, clk := newTestStore(t)
	now := clk.now()

	s.Set("soon", []byte("v"), now+1000)
	s.Set("forever", []byte("v"), NoExpiry)

	if !s.Exists("soon") {
		t.Fatal("key expired before its deadline")
	}
	if ttl, ok := s.TTL("soon"); !ok || ttl != 1000 {
		t.Errorf("TTL: got %d, %v; want 1000, true", ttl, ok)
	}
	if ttl, ok := s.TTL("forever"); !ok || ttl != -1 {
		t.Errorf("TTL of persistent key: got %d, %v; want -1, true", ttl, ok)
	}

	// A key expires exactly at its deadline, not one tick later.
	clk.advance(1000)
	if _, ok := s.Get("soon"); ok {
		t.Error("key survived past its deadline")
	}
	if _, ok := s.TTL("soon"); ok {
		t.Error("TTL reported an expired key as present")
	}
	if !s.Exists("forever") {
		t.Error("persistent key was expired")
	}
}

// A GET of an expired key must not just hide it -- it must free the memory.
func TestLazyExpirationReclaims(t *testing.T) {
	s, clk := newTestStore(t)
	s.Set("k", []byte("v"), clk.now()+100)
	clk.advance(200)

	if _, ok := s.Get("k"); ok {
		t.Fatal("expired key was returned")
	}
	sh := s.shardFor("k")
	sh.mu.RLock()
	_, stillThere := sh.data["k"]
	sh.mu.RUnlock()
	if stillThere {
		t.Error("expired key was not reclaimed by the read path")
	}
}

// The sweeper must reclaim keys that are never touched again.
func TestActiveExpiration(t *testing.T) {
	s, clk := newTestStore(t)
	for i := 0; i < 200; i++ {
		s.Set(fmt.Sprintf("k%d", i), []byte("v"), clk.now()+100)
	}
	if got := s.Len(); got != 200 {
		t.Fatalf("Len before expiry: got %d, want 200", got)
	}

	clk.advance(200)
	// Enough rounds to sample every shard several times over.
	for i := 0; i < 20; i++ {
		s.RunExpiryCycle()
	}

	remaining := 0
	for _, sh := range s.shards {
		sh.mu.RLock()
		remaining += len(sh.data)
		sh.mu.RUnlock()
	}
	if remaining != 0 {
		t.Errorf("%d expired keys were never reclaimed", remaining)
	}
	if s.Stats().ExpiredKeys != 200 {
		t.Errorf("expired_keys stat: got %d, want 200", s.Stats().ExpiredKeys)
	}
}

// Len must not count keys that are expired but not yet reclaimed.
func TestLenIgnoresExpiredKeys(t *testing.T) {
	s, clk := newTestStore(t)
	s.Set("live", []byte("v"), NoExpiry)
	s.Set("dead", []byte("v"), clk.now()+100)
	clk.advance(200)

	if got := s.Len(); got != 1 {
		t.Errorf("Len: got %d, want 1", got)
	}
}

func TestExpireAndPersist(t *testing.T) {
	s, clk := newTestStore(t)

	if s.Expire("missing", clk.now()+1000) {
		t.Error("Expire on a missing key reported success")
	}

	s.Set("k", []byte("v"), NoExpiry)
	if !s.Expire("k", clk.now()+1000) {
		t.Fatal("Expire on an existing key failed")
	}
	if ttl, _ := s.TTL("k"); ttl != 1000 {
		t.Errorf("TTL after Expire: got %d, want 1000", ttl)
	}

	if !s.Persist("k") {
		t.Fatal("Persist failed")
	}
	if ttl, _ := s.TTL("k"); ttl != -1 {
		t.Errorf("TTL after Persist: got %d, want -1", ttl)
	}
	if s.Persist("k") {
		t.Error("Persist on a key with no TTL reported success")
	}

	// A deadline in the past deletes immediately.
	s.Set("doomed", []byte("v"), NoExpiry)
	if !s.Expire("doomed", clk.now()-1) {
		t.Error("Expire with a past deadline reported failure")
	}
	if s.Exists("doomed") {
		t.Error("key with a past deadline was not deleted")
	}
}

func TestUpdate(t *testing.T) {
	s, _ := newTestStore(t)

	// Create.
	err := s.Update("counter", func(cur *Entry) (*Entry, error) {
		if cur != nil {
			t.Error("cur should be nil for a new key")
		}
		return &Entry{Value: []byte("1")}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if v, _ := s.Get("counter"); str(v) != "1" {
		t.Errorf("after create: got %q", str(v))
	}

	// Modify in place.
	if err := s.Update("counter", func(cur *Entry) (*Entry, error) {
		cur.Value = []byte("2")
		return cur, nil
	}); err != nil {
		t.Fatal(err)
	}
	if v, _ := s.Get("counter"); str(v) != "2" {
		t.Errorf("after modify: got %q", str(v))
	}

	// Returning nil deletes.
	if err := s.Update("counter", func(cur *Entry) (*Entry, error) {
		return nil, nil
	}); err != nil {
		t.Fatal(err)
	}
	if s.Exists("counter") {
		t.Error("key survived a nil return from Update")
	}

	// An error leaves the key untouched.
	s.Set("k", []byte("original"), NoExpiry)
	wantErr := ErrWrongType
	if err := s.Update("k", func(cur *Entry) (*Entry, error) {
		return &Entry{Value: []byte("changed")}, wantErr
	}); err != wantErr {
		t.Errorf("Update error: got %v, want %v", err, wantErr)
	}
	if v, _ := s.Get("k"); str(v) != "original" {
		t.Errorf("failed Update mutated the key: got %q", str(v))
	}
}

// Update must observe an expired key as absent, not as a stale value.
func TestUpdateSeesExpiredAsAbsent(t *testing.T) {
	s, clk := newTestStore(t)
	s.Set("k", []byte("old"), clk.now()+100)
	clk.advance(200)

	if err := s.Update("k", func(cur *Entry) (*Entry, error) {
		if cur != nil {
			t.Errorf("Update saw an expired entry: %q", str(cur.Value))
		}
		return &Entry{Value: []byte("new")}, nil
	}); err != nil {
		t.Fatal(err)
	}
	if v, _ := s.Get("k"); str(v) != "new" {
		t.Errorf("got %q, want new", str(v))
	}
	// The replacement carries no TTL from the dead entry.
	if ttl, _ := s.TTL("k"); ttl != -1 {
		t.Errorf("TTL leaked from the expired entry: %d", ttl)
	}
}

func TestRename(t *testing.T) {
	s, clk := newTestStore(t)

	if s.Rename("missing", "dst") {
		t.Error("Rename of a missing key reported success")
	}

	s.Set("src", []byte("v"), clk.now()+5000)
	if !s.Rename("src", "dst") {
		t.Fatal("Rename failed")
	}
	if s.Exists("src") {
		t.Error("source key survived the rename")
	}
	if v, _ := s.Get("dst"); str(v) != "v" {
		t.Errorf("dst: got %q, want v", str(v))
	}
	if ttl, _ := s.TTL("dst"); ttl != 5000 {
		t.Errorf("TTL did not survive the rename: got %d", ttl)
	}

	// Renaming onto an existing key replaces it.
	s.Set("other", []byte("replaced"), NoExpiry)
	if !s.Rename("dst", "other") {
		t.Fatal("Rename over an existing key failed")
	}
	if v, _ := s.Get("other"); str(v) != "v" {
		t.Errorf("overwrite target: got %q, want v", str(v))
	}
}

// Renaming keys that land in the same shard takes a single lock and must not
// deadlock on the double-acquire path.
func TestRenameWithinOneShard(t *testing.T) {
	s, _ := newTestStore(t)
	// Find two distinct keys that hash to the same shard.
	var a, b string
	for i := 0; i < 10000 && b == ""; i++ {
		k := fmt.Sprintf("key%d", i)
		if a == "" {
			a = k
			continue
		}
		if s.indexFor(k) == s.indexFor(a) {
			b = k
		}
	}
	if b == "" {
		t.Skip("could not find a same-shard key pair")
	}

	s.Set(a, []byte("v"), NoExpiry)
	if !s.Rename(a, b) {
		t.Fatal("same-shard rename failed")
	}
	if v, _ := s.Get(b); str(v) != "v" {
		t.Errorf("got %q, want v", str(v))
	}
}

func TestKeysAndScan(t *testing.T) {
	s, _ := newTestStore(t)
	for i := 0; i < 100; i++ {
		s.Set(fmt.Sprintf("user:%d", i), []byte("v"), NoExpiry)
	}
	for i := 0; i < 50; i++ {
		s.Set(fmt.Sprintf("session:%d", i), []byte("v"), NoExpiry)
	}

	if got := len(s.Keys("*")); got != 150 {
		t.Errorf("KEYS *: got %d, want 150", got)
	}
	if got := len(s.Keys("user:*")); got != 100 {
		t.Errorf("KEYS user:*: got %d, want 100", got)
	}
	if got := len(s.Keys("nomatch*")); got != 0 {
		t.Errorf("KEYS nomatch*: got %d, want 0", got)
	}

	// A full SCAN must visit every key exactly once and terminate.
	seen := map[string]int{}
	cursor := uint64(0)
	for i := 0; ; i++ {
		if i > 1000 {
			t.Fatal("SCAN did not terminate")
		}
		next, keys := s.Scan(cursor, 10, "*")
		for _, k := range keys {
			seen[k]++
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	if len(seen) != 150 {
		t.Errorf("SCAN returned %d distinct keys, want 150", len(seen))
	}
	for k, n := range seen {
		if n != 1 {
			t.Errorf("SCAN returned %q %d times", k, n)
		}
	}
}

func TestScanWithPattern(t *testing.T) {
	s, _ := newTestStore(t)
	for i := 0; i < 40; i++ {
		s.Set(fmt.Sprintf("user:%d", i), []byte("v"), NoExpiry)
		s.Set(fmt.Sprintf("tmp:%d", i), []byte("v"), NoExpiry)
	}

	seen := map[string]bool{}
	cursor := uint64(0)
	for {
		next, keys := s.Scan(cursor, 10, "user:*")
		for _, k := range keys {
			seen[k] = true
		}
		if cursor = next; cursor == 0 {
			break
		}
	}
	if len(seen) != 40 {
		t.Errorf("filtered SCAN: got %d keys, want 40", len(seen))
	}
}

func TestFlushAndRandomKey(t *testing.T) {
	s, _ := newTestStore(t)
	if _, ok := s.RandomKey(); ok {
		t.Error("RandomKey on an empty store returned a key")
	}

	for i := 0; i < 20; i++ {
		s.Set(fmt.Sprintf("k%d", i), []byte("v"), NoExpiry)
	}
	if k, ok := s.RandomKey(); !ok || !s.Exists(k) {
		t.Errorf("RandomKey returned %q, ok=%v", k, ok)
	}

	s.Flush()
	if s.Len() != 0 {
		t.Errorf("Len after Flush: got %d, want 0", s.Len())
	}
}

// Run with -race: concurrent readers and writers across shards must not race,
// and every increment must be accounted for.
func TestConcurrentAccess(t *testing.T) {
	s, _ := newTestStore(t)
	const (
		writers      = 8
		perWriter    = 500
		sharedKeyOps = 200
	)

	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				key := fmt.Sprintf("w%d:k%d", w, i)
				s.Set(key, []byte("v"), NoExpiry)
				s.Get(key)
				s.TTL(key)
				if i%10 == 0 {
					s.Delete(key)
				}
			}
		}(w)
	}

	// Hammer one key from every goroutine through Update, which is the path
	// that must make read-modify-write atomic.
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < sharedKeyOps; i++ {
				_ = s.Update("shared", func(cur *Entry) (*Entry, error) {
					if cur == nil {
						// Eight bytes so the counter cannot wrap and hide a
						// lost update behind a plausible-looking total.
						cur = &Entry{Value: make([]byte, 8)}
					}
					b := cur.Value.([]byte)
					// Increment a little-endian counter held as bytes.
					for j := 0; j < len(b); j++ {
						b[j]++
						if b[j] != 0 {
							break
						}
					}
					return cur, nil
				})
			}
		}()
	}

	// Concurrent whole-keyspace reads must not race with the writers.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			s.Len()
			s.Keys("w1:*")
			s.Scan(0, 10, "*")
			s.RandomKey()
			s.RunExpiryCycle()
		}
	}()

	wg.Wait()

	v, ok := s.Get("shared")
	if !ok {
		t.Fatal("shared key is missing")
	}
	b := v.([]byte)
	total := 0
	for i := len(b) - 1; i >= 0; i-- {
		total = total*256 + int(b[i])
	}
	if want := writers * sharedKeyOps; total != want {
		t.Errorf("lost updates: counter is %d, want %d", total, want)
	}
}

func TestMatch(t *testing.T) {
	tests := []struct {
		pattern, s string
		want       bool
	}{
		{"*", "anything", true},
		{"*", "", true},
		{"", "", true},
		{"", "x", false},
		{"hello", "hello", true},
		{"hello", "hell", false},
		{"h?llo", "hello", true},
		{"h?llo", "hllo", false},
		{"h*llo", "hello", true},
		{"h*llo", "heeeello", true},
		{"h*llo", "hllo", true},
		{"user:*", "user:123", true},
		{"user:*", "session:123", false},
		{"*:123", "user:123", true},
		{"*o*", "foo", true},
		{"[abc]at", "cat", true},
		{"[abc]at", "dat", false},
		{"[^abc]at", "dat", true},
		{"[^abc]at", "cat", false},
		{"[a-z]at", "qat", true},
		{"[a-z]at", "Qat", false},
		{"[a-cx-z]at", "yat", true},
		{"[a-cx-z]at", "mat", false},
		// A backslash escapes the next byte.
		{string(rune(0x5c)) + "*", "*", true},
		{string(rune(0x5c)) + "*", "x", false},
		// Pathological pattern the backtracking matcher must handle quickly.
		{"a*a*a*a*a*a*a*b", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaab", true},
		{"a*a*a*a*a*a*a*b", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaac", false},
		// Keys are binary, so matching is byte-wise.
		{"?", "\xff", true},
	}
	for _, tc := range tests {
		t.Run(tc.pattern+"_vs_"+tc.s, func(t *testing.T) {
			if got := Match(tc.pattern, tc.s); got != tc.want {
				t.Errorf("Match(%q, %q) = %v, want %v", tc.pattern, tc.s, got, tc.want)
			}
		})
	}
}

func BenchmarkStoreGet(b *testing.B) {
	s := New()
	defer s.Close()
	for i := 0; i < 10000; i++ {
		s.Set(fmt.Sprintf("key:%d", i), []byte("value"), NoExpiry)
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			s.Get(fmt.Sprintf("key:%d", i%10000))
			i++
		}
	})
}

func BenchmarkStoreSet(b *testing.B) {
	s := New()
	defer s.Close()
	val := []byte("value")
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			s.Set(fmt.Sprintf("key:%d", i), val, NoExpiry)
			i++
		}
	})
}
