// Package store implements the gedis keyspace: an expiring key/value map built
// for concurrent access.
//
// Real Redis is single-threaded, so it needs no locking at all. gedis instead
// runs one goroutine per connection, which means the keyspace is shared. Rather
// than put a single mutex in front of everything -- which would make the server
// no more parallel than Redis while adding lock overhead -- the keyspace is
// split into independently locked shards. Two clients touching different keys
// almost always touch different shards and never wait on each other.
package store

import (
	"errors"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// DefaultShards is the number of independently locked partitions.
//
// The number is a trade-off. More shards means less lock contention but more
// fixed overhead: each shard is a separate map, and whole-keyspace operations
// such as DBSIZE and the expiry cycle have to visit every one. 64 keeps
// contention negligible at realistic client counts while staying cheap to
// sweep. It must be a power of two so the shard index is a mask, not a modulo.
const DefaultShards = 64

// NoExpiry is the ExpireAt value for a key that never expires.
const NoExpiry int64 = 0

// ErrWrongType is returned when a command is applied to a key holding the wrong
// kind of value, e.g. INCR on a list.
var ErrWrongType = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")

// Entry is one value in the keyspace together with its expiration deadline.
type Entry struct {
	Value any
	// ExpireAt is a Unix timestamp in milliseconds, or NoExpiry.
	ExpireAt int64
}

// Clock returns the current time as Unix milliseconds. It is injectable so
// tests can drive expiration deterministically instead of sleeping.
type Clock func() int64

func systemClock() int64 { return time.Now().UnixMilli() }

type shard struct {
	mu   sync.RWMutex
	data map[string]*Entry

	// expires indexes exactly the keys in data that carry a TTL.
	//
	// The deadline itself lives on the Entry so the read path needs only one
	// map lookup. This second index exists purely so the active expiry cycle
	// can sample keys that actually have TTLs; without it, a keyspace where
	// only 1% of keys expire would make the sampler scan mostly non-expiring
	// keys. Both are updated together, and only by the helpers below.
	expires map[string]struct{}
}

// Store is a single gedis database (one SELECT slot).
type Store struct {
	shards []*shard
	mask   uint64
	now    Clock

	// hits and misses back the INFO keyspace statistics.
	hits   atomic.Int64
	misses atomic.Int64
	// expiredKeys counts keys removed by either expiry path.
	expiredKeys atomic.Int64

	stop     chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
}

// Option configures a Store.
type Option func(*config)

type config struct {
	shards int
	clock  Clock
}

// WithShards overrides the shard count. It is rounded up to a power of two.
func WithShards(n int) Option {
	return func(c *config) { c.shards = n }
}

// WithClock injects a time source, for deterministic expiration tests.
func WithClock(fn Clock) Option {
	return func(c *config) { c.clock = fn }
}

// New creates an empty Store.
func New(opts ...Option) *Store {
	cfg := config{shards: DefaultShards, clock: systemClock}
	for _, o := range opts {
		o(&cfg)
	}

	n := 1
	for n < cfg.shards {
		n <<= 1
	}

	s := &Store{
		shards: make([]*shard, n),
		mask:   uint64(n - 1),
		now:    cfg.clock,
		stop:   make(chan struct{}),
	}
	for i := range s.shards {
		s.shards[i] = &shard{
			data:    make(map[string]*Entry),
			expires: make(map[string]struct{}),
		}
	}
	return s
}

// Now returns the store's current time in Unix milliseconds.
func (s *Store) Now() int64 { return s.now() }

// hashKey hashes a key with FNV-1a.
//
// FNV-1a is chosen over a cryptographic hash because it is a handful of
// instructions per byte and needs no allocation: hashing a Go string here does
// not convert it to []byte. Collision quality only has to be good enough to
// spread keys evenly across the shards.
func hashKey(key string) uint64 {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	h := uint64(offset64)
	for i := 0; i < len(key); i++ {
		h ^= uint64(key[i])
		h *= prime64
	}
	return h
}

func (s *Store) indexFor(key string) int { return int(hashKey(key) & s.mask) }

func (s *Store) shardFor(key string) *shard { return s.shards[s.indexFor(key)] }

// --- shard helpers. All assume the caller holds the appropriate lock. ---

// setLocked inserts or replaces an entry, keeping the expires index in sync.
func (sh *shard) setLocked(key string, e *Entry) {
	sh.data[key] = e
	if e.ExpireAt == NoExpiry {
		delete(sh.expires, key)
	} else {
		sh.expires[key] = struct{}{}
	}
}

// deleteLocked removes an entry from both maps.
func (sh *shard) deleteLocked(key string) {
	delete(sh.data, key)
	delete(sh.expires, key)
}

// liveLocked returns the entry for key if it exists and has not expired.
//
// It does not delete an expired entry, because callers may only hold a read
// lock. Reclaiming is left to the write paths and the active expiry cycle; an
// expired entry is invisible to every reader in the meantime, so the only cost
// of the delay is memory.
func (sh *shard) liveLocked(key string, now int64) (*Entry, bool) {
	e, ok := sh.data[key]
	if !ok {
		return nil, false
	}
	if e.ExpireAt != NoExpiry && e.ExpireAt <= now {
		return nil, false
	}
	return e, true
}

// --- Read operations ---

// Get returns the value stored at key. The second result is false if the key is
// absent or has expired.
func (s *Store) Get(key string) (any, bool) {
	sh := s.shardFor(key)
	now := s.now()

	sh.mu.RLock()
	e, ok := sh.liveLocked(key, now)
	var v any
	if ok {
		v = e.Value
	}
	sh.mu.RUnlock()

	if ok {
		s.hits.Add(1)
		return v, true
	}
	s.misses.Add(1)
	// Reclaim lazily: if the key was present but dead, drop it now that we can
	// take the write lock. This is Redis lazy expiration, and it is what keeps
	// a key that is read often but never written from leaking.
	s.reclaimIfExpired(sh, key, now)
	return nil, false
}

// reclaimIfExpired deletes key if it is present and past its deadline.
func (s *Store) reclaimIfExpired(sh *shard, key string, now int64) {
	sh.mu.Lock()
	if e, ok := sh.data[key]; ok && e.ExpireAt != NoExpiry && e.ExpireAt <= now {
		sh.deleteLocked(key)
		s.expiredKeys.Add(1)
	}
	sh.mu.Unlock()
}

// Exists reports whether key is present and unexpired.
func (s *Store) Exists(key string) bool {
	sh := s.shardFor(key)
	now := s.now()
	sh.mu.RLock()
	_, ok := sh.liveLocked(key, now)
	sh.mu.RUnlock()
	return ok
}

// Len returns the number of live keys across all shards.
//
// It is a snapshot, not a serialized count: shards are locked one at a time, so
// concurrent writes to a shard already visited are not reflected. Nothing
// depends on it being exact.
func (s *Store) Len() int {
	now := s.now()
	total := 0
	for _, sh := range s.shards {
		sh.mu.RLock()
		total += len(sh.data)
		// Subtract entries that are expired but not yet reclaimed, so DBSIZE
		// does not report keys that GET would refuse to return.
		for key := range sh.expires {
			if e, ok := sh.data[key]; ok && e.ExpireAt <= now {
				total--
			}
		}
		sh.mu.RUnlock()
	}
	return total
}

// TTL returns the milliseconds remaining before key expires.
//
// The second result is false when the key does not exist. A live key with no
// deadline returns -1, mirroring the value Redis reports.
func (s *Store) TTL(key string) (int64, bool) {
	sh := s.shardFor(key)
	now := s.now()
	sh.mu.RLock()
	defer sh.mu.RUnlock()

	e, ok := sh.liveLocked(key, now)
	if !ok {
		return 0, false
	}
	if e.ExpireAt == NoExpiry {
		return -1, true
	}
	return e.ExpireAt - now, true
}

// --- Write operations ---

// Set stores value at key with the given deadline (NoExpiry for none),
// replacing whatever was there.
func (s *Store) Set(key string, value any, expireAt int64) {
	sh := s.shardFor(key)
	sh.mu.Lock()
	sh.setLocked(key, &Entry{Value: value, ExpireAt: expireAt})
	sh.mu.Unlock()
}

// Update runs fn against key while holding its shard write lock, which is how
// every read-modify-write command (INCR, APPEND, SETRANGE, and later the
// container types) stays atomic without a global lock.
//
// cur is nil when the key is absent or expired. Returning nil deletes the key;
// returning an entry stores it. fn must not block or call back into the store,
// since it runs under the lock.
func (s *Store) Update(key string, fn func(cur *Entry) (*Entry, error)) error {
	sh := s.shardFor(key)
	now := s.now()

	sh.mu.Lock()
	defer sh.mu.Unlock()

	cur, ok := sh.data[key]
	if ok && cur.ExpireAt != NoExpiry && cur.ExpireAt <= now {
		// Expired under our own lock: treat as absent and reclaim.
		sh.deleteLocked(key)
		s.expiredKeys.Add(1)
		cur, ok = nil, false
	}
	if !ok {
		cur = nil
	}

	next, err := fn(cur)
	if err != nil {
		return err
	}
	if next == nil {
		if ok {
			sh.deleteLocked(key)
		}
		return nil
	}
	sh.setLocked(key, next)
	return nil
}

// Delete removes keys and returns how many were actually present.
func (s *Store) Delete(keys ...string) int {
	now := s.now()
	removed := 0
	for _, key := range keys {
		sh := s.shardFor(key)
		sh.mu.Lock()
		if e, ok := sh.data[key]; ok {
			sh.deleteLocked(key)
			// An already-expired key was logically gone, so it does not count
			// toward the reply.
			if e.ExpireAt == NoExpiry || e.ExpireAt > now {
				removed++
			}
		}
		sh.mu.Unlock()
	}
	return removed
}

// Expire sets an absolute deadline on an existing key. It reports false if the
// key does not exist.
func (s *Store) Expire(key string, expireAt int64) bool {
	sh := s.shardFor(key)
	now := s.now()
	sh.mu.Lock()
	defer sh.mu.Unlock()

	e, ok := sh.liveLocked(key, now)
	if !ok {
		return false
	}
	// A deadline in the past deletes the key immediately, which is what Redis
	// does for EXPIRE with a non-positive TTL.
	if expireAt <= now {
		sh.deleteLocked(key)
		s.expiredKeys.Add(1)
		return true
	}
	e.ExpireAt = expireAt
	sh.expires[key] = struct{}{}
	return true
}

// Persist clears a key deadline, reporting whether one was removed.
func (s *Store) Persist(key string) bool {
	sh := s.shardFor(key)
	now := s.now()
	sh.mu.Lock()
	defer sh.mu.Unlock()

	e, ok := sh.liveLocked(key, now)
	if !ok || e.ExpireAt == NoExpiry {
		return false
	}
	e.ExpireAt = NoExpiry
	delete(sh.expires, key)
	return true
}

// Rename moves src to dst, carrying its TTL. It reports false if src is absent.
//
// The two keys usually hash to different shards, so both locks are taken. They
// are acquired in a fixed order -- lower shard index first -- because two
// concurrent renames in opposite directions would otherwise deadlock.
func (s *Store) Rename(src, dst string) bool {
	if src == dst {
		return s.Exists(src)
	}
	now := s.now()
	srcIdx, dstIdx := s.indexFor(src), s.indexFor(dst)

	if srcIdx == dstIdx {
		sh := s.shards[srcIdx]
		sh.mu.Lock()
		defer sh.mu.Unlock()
		e, ok := sh.liveLocked(src, now)
		if !ok {
			return false
		}
		sh.deleteLocked(src)
		sh.setLocked(dst, e)
		return true
	}

	first, second := srcIdx, dstIdx
	if first > second {
		first, second = second, first
	}
	s.shards[first].mu.Lock()
	defer s.shards[first].mu.Unlock()
	s.shards[second].mu.Lock()
	defer s.shards[second].mu.Unlock()

	srcSh, dstSh := s.shards[srcIdx], s.shards[dstIdx]
	e, ok := srcSh.liveLocked(src, now)
	if !ok {
		return false
	}
	srcSh.deleteLocked(src)
	dstSh.setLocked(dst, e)
	return true
}

// Flush empties the keyspace.
func (s *Store) Flush() {
	for _, sh := range s.shards {
		sh.mu.Lock()
		sh.data = make(map[string]*Entry)
		sh.expires = make(map[string]struct{})
		sh.mu.Unlock()
	}
}

// --- Iteration ---

// Keys returns every live key matching a glob pattern.
//
// This is O(n) over the whole keyspace and materialises the whole result, which
// is exactly why Redis warns against KEYS in production. SCAN is the incremental
// alternative.
func (s *Store) Keys(pattern string) []string {
	now := s.now()
	var out []string
	for _, sh := range s.shards {
		sh.mu.RLock()
		for key, e := range sh.data {
			if e.ExpireAt != NoExpiry && e.ExpireAt <= now {
				continue
			}
			if Match(pattern, key) {
				out = append(out, key)
			}
		}
		sh.mu.RUnlock()
	}
	return out
}

// RandomKey returns an arbitrary live key.
//
// Go randomises map iteration order, so taking the first key of a randomly
// chosen shard is already unbiased enough for what RANDOMKEY promises.
func (s *Store) RandomKey() (string, bool) {
	now := s.now()
	start := rand.Intn(len(s.shards))
	for i := range s.shards {
		sh := s.shards[(start+i)%len(s.shards)]
		sh.mu.RLock()
		for key, e := range sh.data {
			if e.ExpireAt == NoExpiry || e.ExpireAt > now {
				sh.mu.RUnlock()
				return key, true
			}
		}
		sh.mu.RUnlock()
	}
	return "", false
}

// Scan walks the keyspace incrementally, returning a batch of keys and the
// cursor to pass to the next call. A returned cursor of 0 means iteration is
// complete.
//
// The cursor is a shard index: each call snapshots one shard and moves on, so
// memory per call is bounded by the largest shard rather than by the keyspace.
// Keys present for the whole scan are returned at least once; keys added or
// removed mid-scan may or may not appear, which is the same weak guarantee
// Redis gives. Unlike Redis it cannot resume mid-shard, because Go maps expose
// no stable iteration cursor -- doing that properly needs a custom hash table
// with reverse-binary cursors, which is noted as future work in the README.
//
// count is a lower bound: shards are consumed until at least count keys have
// been examined.
func (s *Store) Scan(cursor uint64, count int, pattern string) (uint64, []string) {
	if count <= 0 {
		count = 10
	}
	now := s.now()
	if cursor >= uint64(len(s.shards)) {
		return 0, nil
	}
	idx := int(cursor)

	var out []string
	scanned := 0
	for idx < len(s.shards) {
		sh := s.shards[idx]
		sh.mu.RLock()
		for key, e := range sh.data {
			if e.ExpireAt != NoExpiry && e.ExpireAt <= now {
				continue
			}
			scanned++
			if Match(pattern, key) {
				out = append(out, key)
			}
		}
		sh.mu.RUnlock()
		idx++

		if scanned >= count {
			break
		}
	}

	if idx >= len(s.shards) {
		return 0, out
	}
	return uint64(idx), out
}

// --- Active expiration ---

// Expiry cycle tuning, mirroring the activeExpireCycle in Redis.
const (
	expiryInterval    = 100 * time.Millisecond
	expirySampleSize  = 20
	expiryRepeatRatio = 0.25
	expiryMaxRounds   = 16
)

// StartExpiryCycle begins background reclamation of expired keys and returns
// immediately. Call Close to stop it.
//
// Lazy expiration alone is not enough: a key that is set with a TTL and then
// never touched again would hold its memory forever. So a sweeper samples keys
// that carry deadlines and drops the dead ones.
//
// Sampling rather than scanning is the important part. A full sweep of a large
// keyspace would hold shard locks long enough to stall clients. Instead each
// round looks at a few keys per shard and, if a high proportion of them turned
// out to be expired, immediately goes round again -- so the sweeper does almost
// no work when few keys are expiring and ramps up automatically when many are.
func (s *Store) StartExpiryCycle() {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		ticker := time.NewTicker(expiryInterval)
		defer ticker.Stop()
		for {
			select {
			case <-s.stop:
				return
			case <-ticker.C:
				s.RunExpiryCycle()
			}
		}
	}()
}

// RunExpiryCycle performs one adaptive sweep. It is exported so tests can drive
// expiration without waiting on a ticker.
func (s *Store) RunExpiryCycle() {
	for round := 0; round < expiryMaxRounds; round++ {
		sampled, expired := 0, 0
		for _, sh := range s.shards {
			n, e := s.expireSampleShard(sh)
			sampled += n
			expired += e
		}
		// Nothing left with a TTL, or few enough dead keys that another pass
		// would be wasted work.
		if sampled == 0 || float64(expired)/float64(sampled) < expiryRepeatRatio {
			return
		}
	}
}

// expireSampleShard checks up to expirySampleSize keys that carry deadlines and
// deletes the dead ones. It returns how many it looked at and how many it
// removed.
func (s *Store) expireSampleShard(sh *shard) (sampled, expired int) {
	now := s.now()

	sh.mu.Lock()
	defer sh.mu.Unlock()

	if len(sh.expires) == 0 {
		return 0, 0
	}
	// Map iteration starts at a random bucket, so this is a random sample
	// without needing to build a key list first.
	for key := range sh.expires {
		if sampled >= expirySampleSize {
			break
		}
		sampled++
		if e, ok := sh.data[key]; ok {
			if e.ExpireAt != NoExpiry && e.ExpireAt <= now {
				sh.deleteLocked(key)
				expired++
			}
		} else {
			// Defensive: an index entry with no data entry should not happen,
			// but drop it rather than leak it.
			delete(sh.expires, key)
		}
	}
	s.expiredKeys.Add(int64(expired))
	return sampled, expired
}

// Close stops the background expiry cycle. It is safe to call more than once.
func (s *Store) Close() {
	s.stopOnce.Do(func() { close(s.stop) })
	s.wg.Wait()
}

// --- Statistics ---

// Stats is a snapshot of counters reported by INFO.
type Stats struct {
	Keys        int
	Expires     int
	Hits        int64
	Misses      int64
	ExpiredKeys int64
}

func (s *Store) Stats() Stats {
	withTTL := 0
	for _, sh := range s.shards {
		sh.mu.RLock()
		withTTL += len(sh.expires)
		sh.mu.RUnlock()
	}
	return Stats{
		Keys:        s.Len(),
		Expires:     withTTL,
		Hits:        s.hits.Load(),
		Misses:      s.misses.Load(),
		ExpiredKeys: s.expiredKeys.Load(),
	}
}

// TypeName reports the Redis type name of a stored value, for the TYPE command.
// It gains cases as container types are added.
func TypeName(v any) string {
	switch v.(type) {
	case []byte:
		return "string"
	default:
		return "none"
	}
}
