package server

import (
	"fmt"
	"strings"
	"testing"

	"github.com/Nafiz001/gedis/internal/store"
)

func TestParseEvictionPolicy(t *testing.T) {
	tests := []struct {
		in   string
		want EvictionPolicy
		ok   bool
	}{
		{"", EvictNone, true},
		{"noeviction", EvictNone, true},
		{"allkeys-lru", EvictAllKeysLRU, true},
		{"allkeys-random", EvictAllKeysRandom, true},
		{"volatile-lru", EvictVolatileLRU, true},
		{"volatile-random", EvictVolatileRandom, true},
		{"volatile-ttl", EvictVolatileTTL, true},
		{"ALLKEYS-LRU", EvictAllKeysLRU, true},
		{"nonsense", EvictNone, false},
	}
	for _, tc := range tests {
		got, err := ParseEvictionPolicy(tc.in)
		if (err == nil) != tc.ok {
			t.Errorf("ParseEvictionPolicy(%q): err=%v, want ok=%v", tc.in, err, tc.ok)
			continue
		}
		if tc.ok && got != tc.want {
			t.Errorf("ParseEvictionPolicy(%q) = %v, want %v", tc.in, got, tc.want)
		}
		if tc.ok && got.String() != strings.ToLower(orDefault(tc.in, "noeviction")) {
			t.Errorf("round trip of %q gave %q", tc.in, got.String())
		}
	}
}

func orDefault(s, fallback string) string {
	if s == "" {
		return fallback
	}
	return s
}

// pickVictim is where the policy actually decides, so it is tested directly
// rather than through the memory machinery.
func TestPickVictim(t *testing.T) {
	candidates := []store.EvictionCandidate{
		{Key: "recent", Accessed: 3000, ExpireAt: 9000, HasTTL: true},
		{Key: "oldest", Accessed: 1000, ExpireAt: 8000, HasTTL: true},
		{Key: "middle", Accessed: 2000, ExpireAt: 5000, HasTTL: true},
	}

	if got := pickVictim(candidates, EvictAllKeysLRU); got != "oldest" {
		t.Errorf("LRU picked %q, want the least recently accessed", got)
	}
	if got := pickVictim(candidates, EvictVolatileLRU); got != "oldest" {
		t.Errorf("volatile-lru picked %q", got)
	}
	// TTL order is about which expires soonest, not which was read longest ago.
	if got := pickVictim(candidates, EvictVolatileTTL); got != "middle" {
		t.Errorf("volatile-ttl picked %q, want the soonest to expire", got)
	}
	// Random takes whatever the sample offered first.
	if got := pickVictim(candidates, EvictAllKeysRandom); got == "" {
		t.Error("random policy returned no victim")
	}
}

func TestEvictionPolicyFlags(t *testing.T) {
	volatile := []EvictionPolicy{EvictVolatileLRU, EvictVolatileRandom, EvictVolatileTTL}
	for _, p := range volatile {
		if !p.onlyVolatile() {
			t.Errorf("%v should only evict keys with a TTL", p)
		}
	}
	for _, p := range []EvictionPolicy{EvictNone, EvictAllKeysLRU, EvictAllKeysRandom} {
		if p.onlyVolatile() {
			t.Errorf("%v should be allowed to evict any key", p)
		}
	}
	// Only the LRU policies need the read path to record access times.
	for _, p := range []EvictionPolicy{EvictAllKeysLRU, EvictVolatileLRU} {
		if !p.needsAccessTracking() {
			t.Errorf("%v needs access tracking", p)
		}
	}
	for _, p := range []EvictionPolicy{EvictNone, EvictAllKeysRandom, EvictVolatileRandom, EvictVolatileTTL} {
		if p.needsAccessTracking() {
			t.Errorf("%v should not pay for access tracking", p)
		}
	}
}

// startEvictionServer wires the memory figure to the key count, so the tests
// can move "memory" by adding and removing keys.
func startEvictionServer(t *testing.T, policy EvictionPolicy, bytesPerKey, limit int64) *harness {
	t.Helper()
	h := startServer(t, func(cfg *Config) {
		cfg.MaxMemory = limit
		cfg.MaxMemoryPolicy = policy
	})
	h.srv.memory.sampleFn = func() int64 {
		return int64(h.srv.dbs[0].Len()) * bytesPerKey
	}
	h.srv.memory.sample()
	return h
}

// Under noeviction, a write past the limit is refused rather than silently
// discarding someone else's data.
func TestNoEvictionRefusesWrites(t *testing.T) {
	h := startEvictionServer(t, EvictNone, 100, 500)
	c := h.dial(t)

	// The limit is checked before each write, not after, so the keyspace
	// crosses it by exactly one key: six writes fit under a 500-byte limit at
	// 100 bytes each, and the seventh is the one that finds it already over.
	for i := 0; i < 6; i++ {
		if got := c.do("SET", fmt.Sprintf("k%d", i), "v"); got != "OK" {
			t.Fatalf("write %d below the limit: %s", i, got)
		}
	}
	got := c.do("SET", "overflow", "v")
	if !strings.HasPrefix(got, "(error) OOM") {
		t.Errorf("write past maxmemory: got %s, want an OOM error", got)
	}

	// Reads keep working, which is what makes the error survivable.
	if got := c.do("GET", "k0"); got != "v" {
		t.Errorf("read while over the limit: %s", got)
	}
	// And freeing space lets writes through again.
	c.do("DEL", "k0")
	c.do("DEL", "k1")
	if got := c.do("SET", "after-free", "v"); got != "OK" {
		t.Errorf("write after freeing space: %s", got)
	}
}

// An allkeys policy makes room instead of refusing.
func TestAllKeysEvictionMakesRoom(t *testing.T) {
	h := startEvictionServer(t, EvictAllKeysRandom, 100, 1000)
	c := h.dial(t)

	for i := 0; i < 30; i++ {
		if got := c.do("SET", fmt.Sprintf("k%d", i), "v"); got != "OK" {
			t.Fatalf("write %d: got %s -- eviction should have made room", i, got)
		}
	}

	// The keyspace is held at the limit rather than growing past it.
	c.send("DBSIZE")
	size := c.read().Int
	// A 1000-byte limit at 100 bytes per key holds about ten, plus the one
	// that crosses the line before eviction runs.
	if size > 11 {
		t.Errorf("DBSIZE is %d, want at most 11 (a 1000-byte limit at 100 per key)", size)
	}
	if size == 0 {
		t.Error("eviction emptied the keyspace entirely")
	}
	if h.srv.stats.evictedKeys.Load() == 0 {
		t.Error("nothing was recorded as evicted")
	}
}

// A volatile policy may only remove keys that carry a TTL.
func TestVolatileEvictionSparesPersistentKeys(t *testing.T) {
	h := startEvictionServer(t, EvictVolatileRandom, 100, 1000)
	c := h.dial(t)

	// Five keys that must never be evicted.
	for i := 0; i < 5; i++ {
		c.do("SET", fmt.Sprintf("permanent%d", i), "v")
	}
	// Plus a stream of expiring ones that push past the limit.
	for i := 0; i < 30; i++ {
		c.do("SET", fmt.Sprintf("volatile%d", i), "v", "EX", "1000")
	}

	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("permanent%d", i)
		if got := c.do("EXISTS", key); got != "(integer) 1" {
			t.Errorf("%s was evicted despite having no TTL", key)
		}
	}
	if h.srv.stats.evictedKeys.Load() == 0 {
		t.Error("no volatile keys were evicted")
	}
}

// With only persistent keys and a volatile policy, there is nothing eligible,
// so the write has to be refused rather than evicting something it may not.
func TestVolatileEvictionWithNothingEligible(t *testing.T) {
	h := startEvictionServer(t, EvictVolatileRandom, 100, 500)
	c := h.dial(t)

	// Six keys at 100 bytes puts the store over the 500-byte limit, so the
	// next write finds it over and has nothing it is permitted to evict.
	for i := 0; i < 6; i++ {
		c.do("SET", fmt.Sprintf("permanent%d", i), "v")
	}
	got := c.do("SET", "overflow", "v")
	if !strings.HasPrefix(got, "(error) OOM") {
		t.Errorf("got %s, want OOM when no key may be evicted", got)
	}
	if got := c.do("DBSIZE"); got != "(integer) 6" {
		t.Errorf("a persistent key was evicted anyway: %s", got)
	}
}

// LRU should prefer keys that have not been read recently.
func TestLRUEvictionPrefersColdKeys(t *testing.T) {
	h := startEvictionServer(t, EvictAllKeysLRU, 100, 2000)
	c := h.dial(t)

	// Twenty keys, written at increasing timestamps.
	for i := 0; i < 20; i++ {
		h.clk.advance(1000)
		c.do("SET", fmt.Sprintf("k%d", i), "v")
	}

	// Keep the first five hot by reading them last.
	h.clk.advance(1000)
	hot := []string{"k0", "k1", "k2", "k3", "k4"}
	for _, key := range hot {
		h.clk.advance(100)
		c.do("GET", key)
	}

	// Now push past the limit, forcing eviction.
	for i := 20; i < 30; i++ {
		h.clk.advance(100)
		c.do("SET", fmt.Sprintf("k%d", i), "v")
	}

	// Sampling is approximate, so this checks the tendency rather than an
	// exact set: most of the recently-read keys should have survived.
	survivors := 0
	for _, key := range hot {
		if c.do("EXISTS", key) == "(integer) 1" {
			survivors++
		}
	}
	if survivors < 3 {
		t.Errorf("only %d of 5 recently-read keys survived; LRU is not favouring hot keys", survivors)
	}
}

func TestEvictionInfoAndConfig(t *testing.T) {
	h := startEvictionServer(t, EvictAllKeysLRU, 100, 5000)
	c := h.dial(t)

	c.send("INFO", "memory")
	info := string(c.read().Str)
	for _, want := range []string{"maxmemory:5000", "maxmemory_policy:allkeys-lru", "evicted_keys:"} {
		if !strings.Contains(info, want) {
			t.Errorf("INFO memory is missing %q\n%s", want, info)
		}
	}

	// CONFIG GET must report what is actually configured, not a stub.
	if got := c.do("CONFIG", "GET", "maxmemory-policy"); got != "[maxmemory-policy allkeys-lru]" {
		t.Errorf("CONFIG GET maxmemory-policy: %s", got)
	}
	if got := c.do("CONFIG", "GET", "maxmemory"); got != "[maxmemory 5000]" {
		t.Errorf("CONFIG GET maxmemory: %s", got)
	}
}

// With no limit set, nothing is evicted and nothing is refused.
func TestNoMemoryLimitNeverEvicts(t *testing.T) {
	h := startServer(t, nil)
	c := h.dial(t)

	for i := 0; i < 500; i++ {
		if got := c.do("SET", fmt.Sprintf("k%d", i), "value"); got != "OK" {
			t.Fatalf("write %d: %s", i, got)
		}
	}
	if got := c.do("DBSIZE"); got != "(integer) 500" {
		t.Errorf("DBSIZE: got %s, want 500", got)
	}
	if n := h.srv.stats.evictedKeys.Load(); n != 0 {
		t.Errorf("%d keys evicted with no limit configured", n)
	}
}
