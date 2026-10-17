package server

import (
	"fmt"
	"runtime/metrics"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/Nafiz001/gedis/internal/store"
)

// Eviction is what a cache does when it runs out of room: choose something to
// throw away rather than refuse the write.
//
// Two things make this hard here, and both are answered by approximation.
//
// Measuring memory. Redis tracks allocations itself and knows to the byte what
// each key costs. A Go program cannot: the heap holds garbage the collector has
// not swept, and per-key accounting would mean estimating the size of every
// value on every mutation. So gedis reads the runtime's own live-heap figure,
// sampled on a timer rather than per command, and treats it as the number to
// stay under. It measures the process, not the keyspace, which is the honest
// thing to measure anyway -- the process is what gets killed.
//
// Choosing a victim. A true LRU needs every read to move a node to the head of
// a shared list: a write on the read path, and a single point of contention
// across all shards. Redis samples a handful of keys and evicts the oldest of
// them instead, and the approximation is close enough that it has never been
// worth fixing. gedis does the same.

// EvictionPolicy decides which key goes when memory runs short.
type EvictionPolicy int

const (
	// EvictNone refuses writes once the limit is reached. The default,
	// because silently discarding a user's data is worse than an error unless
	// they asked for a cache.
	EvictNone EvictionPolicy = iota
	EvictAllKeysLRU
	EvictAllKeysRandom
	EvictVolatileLRU
	EvictVolatileRandom
	EvictVolatileTTL
)

func (p EvictionPolicy) String() string {
	switch p {
	case EvictAllKeysLRU:
		return "allkeys-lru"
	case EvictAllKeysRandom:
		return "allkeys-random"
	case EvictVolatileLRU:
		return "volatile-lru"
	case EvictVolatileRandom:
		return "volatile-random"
	case EvictVolatileTTL:
		return "volatile-ttl"
	default:
		return "noeviction"
	}
}

// ParseEvictionPolicy reads the maxmemory-policy setting.
func ParseEvictionPolicy(s string) (EvictionPolicy, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "noeviction":
		return EvictNone, nil
	case "allkeys-lru":
		return EvictAllKeysLRU, nil
	case "allkeys-random":
		return EvictAllKeysRandom, nil
	case "volatile-lru":
		return EvictVolatileLRU, nil
	case "volatile-random":
		return EvictVolatileRandom, nil
	case "volatile-ttl":
		return EvictVolatileTTL, nil
	default:
		return 0, fmt.Errorf("unknown maxmemory-policy %q", s)
	}
}

// needsAccessTracking reports whether the policy reads last-access times, so
// the store only pays for that bookkeeping when something uses it.
func (p EvictionPolicy) needsAccessTracking() bool {
	return p == EvictAllKeysLRU || p == EvictVolatileLRU
}

// onlyVolatile reports whether the policy may only evict keys with a TTL.
func (p EvictionPolicy) onlyVolatile() bool {
	switch p {
	case EvictVolatileLRU, EvictVolatileRandom, EvictVolatileTTL:
		return true
	}
	return false
}

// Eviction tuning.
const (
	// evictionSampleSize is how many keys are examined per victim chosen.
	// Redis defaults to 5; the accuracy gain above that flattens quickly.
	evictionSampleSize = 5
	// evictionMaxRounds bounds one eviction pass, so a client's command
	// cannot turn into an unbounded loop when memory will not come down.
	evictionMaxRounds = 200
)

// memoryMonitor caches the live-heap figure.
type memoryMonitor struct {
	bytes atomic.Int64
	// mu guards samples, which metrics.Read writes into. Sampling happens on
	// whichever client goroutine is about to perform a write, so the buffer
	// cannot be shared unguarded.
	mu      sync.Mutex
	samples []metrics.Sample
	// sampleFn overrides where the figure comes from. Eviction is otherwise
	// driven by the Go heap, which a test cannot move on demand, so this seam
	// lets one supply a number that responds to the keys being evicted.
	sampleFn func() int64
}

func newMemoryMonitor() *memoryMonitor {
	m := &memoryMonitor{
		// This metric is the runtime's own count of live heap objects. Unlike
		// runtime.ReadMemStats it does not stop the world, which matters for
		// something read on a timer while clients are being served.
		samples: []metrics.Sample{{Name: "/memory/classes/heap/objects:bytes"}},
	}
	m.sample()
	return m
}

func (m *memoryMonitor) sample() {
	if m.sampleFn != nil {
		m.bytes.Store(m.sampleFn())
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	metrics.Read(m.samples)
	if m.samples[0].Value.Kind() == metrics.KindUint64 {
		m.bytes.Store(int64(m.samples[0].Value.Uint64()))
	}
}

func (m *memoryMonitor) used() int64 { return m.bytes.Load() }

// overMemoryLimit reports whether the process is above maxmemory.
func (s *Server) overMemoryLimit() bool {
	if s.cfg.MaxMemory <= 0 {
		return false
	}
	return s.memory.used() > s.cfg.MaxMemory
}

// enforceMemoryLimit evicts until the process is back under the limit.
//
// It returns false when the limit still cannot be met, which is what turns a
// write into an OOM error under the noeviction policy.
func (s *Server) enforceMemoryLimit() bool {
	if s.cfg.MaxMemory <= 0 {
		return true
	}
	// Sample now rather than trusting a periodic refresh. A timer-driven
	// figure lets a burst of writes run far past the limit before anything
	// notices, which defeats the point of having one. Reading the runtime
	// metric costs a few hundred nanoseconds and is only paid when a limit is
	// configured at all.
	s.memory.sample()
	if !s.overMemoryLimit() {
		return true
	}
	if s.cfg.MaxMemoryPolicy == EvictNone {
		return false
	}

	for round := 0; round < evictionMaxRounds; round++ {
		if !s.evictOne() {
			// Nothing left that this policy is allowed to remove.
			break
		}
		s.stats.evictedKeys.Add(1)

		// Re-sample rather than assume: the runtime only returns memory as
		// the collector runs, so the figure moves on its own schedule.
		s.memory.sample()
		if !s.overMemoryLimit() {
			return true
		}
	}
	return !s.overMemoryLimit()
}

// evictOne removes a single victim, reporting whether it found one.
func (s *Server) evictOne() bool {
	policy := s.cfg.MaxMemoryPolicy
	volatileOnly := policy.onlyVolatile()

	for _, db := range s.dbs {
		candidates := db.SampleForEviction(evictionSampleSize, volatileOnly)
		if len(candidates) == 0 {
			continue
		}
		victim := pickVictim(candidates, policy)
		if db.Delete(victim) > 0 {
			return true
		}
	}
	return false
}

// pickVictim chooses the worst key in a sample under the given policy.
func pickVictim(candidates []store.EvictionCandidate, policy EvictionPolicy) string {
	best := candidates[0]

	switch policy {
	case EvictAllKeysLRU, EvictVolatileLRU:
		// Oldest access wins.
		for _, c := range candidates[1:] {
			if c.Accessed < best.Accessed {
				best = c
			}
		}
	case EvictVolatileTTL:
		// Soonest to expire wins: it was going to go anyway.
		for _, c := range candidates[1:] {
			if c.ExpireAt < best.ExpireAt {
				best = c
			}
		}
	default:
		// Random: the sample is already unordered, so the first will do.
	}
	return best.Key
}

// memoryInfo renders the INFO memory fields that describe the limit.
func (s *Server) memoryLimitInfo() string {
	var b strings.Builder
	fmt.Fprintf(&b, "maxmemory:%d\r\n", s.cfg.MaxMemory)
	fmt.Fprintf(&b, "maxmemory_policy:%s\r\n", s.cfg.MaxMemoryPolicy.String())
	fmt.Fprintf(&b, "evicted_keys:%d\r\n", s.stats.evictedKeys.Load())
	return b.String()
}
