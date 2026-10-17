# gedis

A Redis-compatible in-memory data store, written from scratch in Go.

No Redis library is involved. gedis implements the RESP wire protocol itself, so
the official `redis-cli`, `redis-benchmark`, and any standard Redis client
library connect to it unmodified:

```console
$ gedis-server --addr :6380
$ redis-cli -p 6380
127.0.0.1:6380> SET user:1 "Nafiz Ahmed"
OK
127.0.0.1:6380> APPEND user:1 " (BD)"
(integer) 16
127.0.0.1:6380> GET user:1
"Nafiz Ahmed (BD)"
127.0.0.1:6380> ZADD leaderboard 1520 nafiz 1890 rahim 2100 sadia
(integer) 3
127.0.0.1:6380> ZINCRBY leaderboard 300 nafiz
"1820"
127.0.0.1:6380> ZREVRANGE leaderboard 0 2 WITHSCORES
1) "sadia"
2) "2100"
3) "rahim"
4) "1890"
5) "nafiz"
6) "1820"
127.0.0.1:6380> LMOVE jobs processing LEFT RIGHT
"email:1"
127.0.0.1:6380> HINCRBY session:abc logins 1
(integer) 42
127.0.0.1:6380> SINTER post:1 post:2
1) "go"
2) "backend"
```

Transactions, with optimistic locking:

```console
127.0.0.1:6380> WATCH acct:a
OK
127.0.0.1:6380> MULTI
OK
127.0.0.1:6380> DECRBY acct:a 30
QUEUED
127.0.0.1:6380> INCRBY acct:b 30
QUEUED
127.0.0.1:6380> EXEC          # (nil) instead, if another client touched acct:a
1) (integer) 70
2) (integer) 80
```

Pub/Sub, including patterns:

```console
127.0.0.1:6380> PSUBSCRIBE news.*
1) "psubscribe"  2) "news.*"  3) (integer) 1
# elsewhere: PUBLISH news.tech "go 1.27"
1) "pmessage"  2) "news.*"  3) "news.tech"  4) "go 1.27"
```

Replication, in two commands:

```console
127.0.0.1:6391> REPLICAOF 127.0.0.1 6390
OK
127.0.0.1:6391> DBSIZE          # the master's keyspace, synced
(integer) 501
127.0.0.1:6391> SET k v
(error) READONLY You can't write against a read only replica.
```

## Why build this

Redis is one of the few pieces of infrastructure almost every backend touches,
and almost nobody has opened. Reimplementing it forces you through the parts
that are usually hidden: framing bytes off a socket, keeping a shared keyspace
correct under concurrency, expiring keys without stalling the server, and
matching an error string exactly because real clients branch on it.

## What works

**156 commands**, matching Redis semantics including the modifier combinations
that actually carry meaning.

| Family | Commands |
|---|---|
| Strings | `SET` (`NX` `XX` `GET` `EX` `PX` `EXAT` `PXAT` `KEEPTTL`), `GET`, `GETSET`, `GETDEL`, `GETEX`, `SETNX`, `SETEX`, `PSETEX`, `MGET`, `MSET`, `MSETNX`, `APPEND`, `STRLEN`, `SETRANGE`, `GETRANGE`, `SUBSTR` |
| Counters | `INCR`, `DECR`, `INCRBY`, `DECRBY`, `INCRBYFLOAT` |
| Keyspace | `DEL`, `UNLINK`, `EXISTS`, `TYPE`, `KEYS`, `SCAN` (`MATCH` `COUNT` `TYPE`), `RANDOMKEY`, `RENAME`, `RENAMENX`, `COPY` |
| Expiration | `EXPIRE` / `PEXPIRE` / `EXPIREAT` / `PEXPIREAT` (`NX` `XX` `GT` `LT`), `TTL`, `PTTL`, `EXPIRETIME`, `PEXPIRETIME`, `PERSIST` |
| Connection | `PING`, `ECHO`, `SELECT`, `AUTH`, `HELLO`, `RESET`, `QUIT`, `CLIENT`, `COMMAND` |
| Lists | `LPUSH`, `RPUSH`, `LPUSHX`, `RPUSHX`, `LPOP`, `RPOP`, `LLEN`, `LRANGE`, `LINDEX`, `LSET`, `LINSERT`, `LREM`, `LTRIM`, `LPOS`, `LMOVE`, `RPOPLPUSH` |
| Hashes | `HSET`, `HMSET`, `HSETNX`, `HGET`, `HMGET`, `HDEL`, `HLEN`, `HEXISTS`, `HSTRLEN`, `HKEYS`, `HVALS`, `HGETALL`, `HINCRBY`, `HINCRBYFLOAT`, `HRANDFIELD`, `HSCAN` |
| Sets | `SADD`, `SREM`, `SCARD`, `SISMEMBER`, `SMISMEMBER`, `SMEMBERS`, `SPOP`, `SRANDMEMBER`, `SMOVE`, `SINTER`, `SUNION`, `SDIFF`, `SINTERSTORE`, `SUNIONSTORE`, `SDIFFSTORE`, `SINTERCARD`, `SSCAN` |
| Sorted sets | `ZADD` (`NX` `XX` `GT` `LT` `CH` `INCR`), `ZSCORE`, `ZMSCORE`, `ZCARD`, `ZCOUNT`, `ZLEXCOUNT`, `ZINCRBY`, `ZRANK`, `ZREVRANK`, `ZRANGE` (`BYSCORE` `BYLEX` `REV` `LIMIT` `WITHSCORES`), `ZREVRANGE`, `ZRANGEBYSCORE`, `ZREVRANGEBYSCORE`, `ZRANGEBYLEX`, `ZREVRANGEBYLEX`, `ZREM`, `ZREMRANGEBYRANK`, `ZREMRANGEBYSCORE`, `ZREMRANGEBYLEX`, `ZPOPMIN`, `ZPOPMAX`, `ZRANDMEMBER`, `ZSCAN`, `ZUNION`, `ZINTER`, `ZDIFF`, `ZUNIONSTORE`, `ZINTERSTORE`, `ZDIFFSTORE` |
| Pub/Sub | `SUBSCRIBE`, `UNSUBSCRIBE`, `PSUBSCRIBE`, `PUNSUBSCRIBE`, `PUBLISH`, `PUBSUB` |
| Transactions | `MULTI`, `EXEC`, `DISCARD`, `WATCH`, `UNWATCH` |
| Persistence | `SAVE`, `BGSAVE`, `BGREWRITEAOF`, `LASTSAVE` |
| Replication | `REPLICAOF`, `SLAVEOF`, `REPLCONF`, `PSYNC`, `WAIT` |
| Server | `DBSIZE`, `FLUSHDB`, `FLUSHALL`, `INFO`, `TIME`, `CONFIG`, `DEBUG`, `SHUTDOWN` |

Also: 16 independent `SELECT`-able databases, `requirepass` authentication,
client pipelining, the inline (telnet) protocol, glob patterns with character
classes, `maxmemory` with five eviction policies, and graceful shutdown on
`SIGINT`/`SIGTERM`.

## Architecture

```
                  ┌──────────────┐
   TCP client ───▶ │ goroutine    │  one per connection
                  │  resp.Reader │  decode command
                  │  dispatch    │  table lookup + arity check
                  │  resp.Writer │  encode reply, flush per batch
                  └──────┬───────┘
                         │
              ┌──────────▼──────────┐
              │  store.Store        │
              │  ┌────┬────┬─────┐  │  64 independently locked shards
              │  │ sh │ sh │ ... │  │  key → FNV-1a → shard
              │  └────┴────┴─────┘  │
              │  + active expiry    │  background sampler
              └──────────┬──────────┘
                         │  values
              ┌──────────▼─────────────────────────────┐
              │ []byte   string                        │
              │ List     ring-buffer deque             │
              │ Hash     map                           │
              │ Set      map                           │
              │ ZSet     map + skiplist (span-indexed) │
              └────────────────────────────────────────┘

   Every write also enters one propagation stream:

              write ──▶ ┌─────────────────┐
                        │ append-only log │──▶ disk (fsync policy)
                        ├─────────────────┤
                        │ replica links   │──▶ followers
                        └─────────────────┘
```

### Decisions worth explaining

**One goroutine per connection, not an event loop.** Redis is single-threaded
and uses an event loop because it must. Go's runtime already multiplexes
goroutines onto a few OS threads with epoll/IOCP underneath, so blocking reads
give the same scalability with straight-line, readable code — and, unlike
Redis, it uses every core.

**A sharded keyspace, because concurrency is the whole point.** Using more than
one core only helps if clients do not queue behind a single lock. The keyspace
is split into 64 independently locked shards, so two clients touching different
keys never wait on each other. Read-modify-write commands (`INCR`, `APPEND`,
`SET NX`) run their whole cycle inside one shard's write lock, so they stay
atomic without a global lock — the check-then-write race that would otherwise
make `SET NX` useless as a lock primitive simply cannot happen.

**Both expiration strategies, like Redis.** Lazy expiration alone leaks: a key
set with a TTL and never read again holds its memory forever. So reads reclaim
dead keys on access, *and* a background sweeper samples keys that carry TTLs and
repeats only while the hit rate stays above 25%. It costs nothing when little is
expiring and ramps up automatically when a lot is. A separate per-shard index of
keys-with-deadlines keeps the sampler from wasting time on keys that cannot
expire.

**Flush per batch, not per command.** The connection loop only flushes when the
read buffer is drained. A client pipelining 500 commands gets one write syscall
instead of 500 — worth roughly 10× on throughput.

**A skiplist for sorted sets, and spans are the reason.** Each forward pointer
records how many nodes it jumps over, so summing spans along a search path
yields an element's rank for free. That is what makes `ZRANK` O(log n) instead
of a linear count, and lets `ZRANGE` jump straight to an index rather than
walking there. A `ZSet` pairs that skiplist with a map, so `ZSCORE` stays O(1)
while ordered queries stay O(log n); the cost is that a rescore must delete and
reinsert, because the score is part of the skiplist's sort key.

**Lists are a ring buffer, not a linked list.** Redis uses a quicklist, which
optimises for memory. A ring buffer trades a little space for speed that suits
this server: O(1) at both ends, O(1) `LINDEX`, and contiguous storage so
walking a range touches far fewer cache lines than chasing node pointers.
Inserts and removals shift whichever side is shorter.

**Hashes and sets are plain maps.** Redis keeps a second compact encoding for
each -- listpacks for small hashes, intsets for all-integer sets -- purely to
save memory in its own allocator layout. That buys nothing here and would put a
size check on every access, so gedis uses one representation and says so.

**One write stream, two consumers.** Durability and replication ask the same
question — what changed, in what order — so they read the same stream rather
than each deriving it. A command that is not replayable is rewritten before it
enters: `SET k v EX 60` becomes the absolute `PXAT` form, or every restart would
silently renew the key; `SPOP` becomes the `SREM` it turned out to be, or a
replay would remove different members; `INCRBYFLOAT` becomes `SET`, because
float arithmetic is not guaranteed identical across builds.

**A log that tolerates damage, and a snapshot that refuses it.** The append-only
log accepts a truncated tail: that is the expected shape of a crash mid-write,
and refusing to start would turn a recoverable incident into an outage. The
snapshot does the opposite — a CRC-64 covers the whole file and a mismatch stops
the load, because a silently partial restore is worse than no restore. Both are
installed by rename, so a crash never leaves a half-written file where a good
one used to be.

**Pub/Sub needed one explicit rule.** It is the only place a command run by one
client writes to a different client's socket. Every client owns a write lock:
the connection loop holds it around each reply, a publisher holds it around each
delivery, so a message can never land in the middle of a reply. Deliveries carry
a write deadline, so one subscriber that stopped reading cannot hold a publisher
hostage. The alternative — an outbox channel and a writer goroutine per client —
doubles the goroutine count to solve what one uncontended mutex already does.

**Transactions are real, and they cost something.** Redis gets `EXEC` isolation
free by being single-threaded. Here every command holds a shared lock and `EXEC`
holds it exclusively, so a transaction genuinely runs with the keyspace to
itself. `WATCH` invalidation is push-based — a write marks watchers dirty at the
moment it happens — and the store hook behind it costs one atomic load when
nobody is watching, which is almost always.

**Replication has no gap between the snapshot and the stream.** A replica is
registered as a stream destination *before* its snapshot is taken, so writes
arriving mid-sync queue behind the snapshot on the same connection rather than
falling between the two phases. A test hammers a master with 200 writes during a
2,000-key sync and checks that every one of them lands.

**Error strings are copied exactly.** The first word of a Redis error
(`WRONGTYPE`, `NOAUTH`, `WRONGPASS`, `ERR`) is a machine-readable code that real
clients switch on. Getting it approximately right means client libraries
misbehave in ways that are painful to debug.

## Benchmarks

Measured with the included `gedis-bench` over loopback. 11th-gen i5-1155G7,
4 cores / 8 threads, Windows 11.

```
100000 requests, 50 clients, pipeline 1, 16-byte values

TEST          REQ/SEC     AVG ms     P50 ms     P99 ms
PING            97230      0.506      0.526      1.521
SET             79048      0.622      0.533      3.453
GET             96079      0.514      0.524      1.623
INCR            97742      0.505      0.522      1.535

400000 requests, 50 clients, pipeline 16

SET            780002      0.998      0.928      4.479
GET           1091156      0.710      0.635      2.528
```

Two honest caveats. Run-to-run variance on a laptop is wide -- an earlier run of
the same binary gave 54k for SET where this one gives 79k -- so treat these as a
range, not a measurement. And that variance is precisely why no claim is made
that the transaction lock added in the last milestone is free: every command now
takes a shared lock so `EXEC` can take it exclusively, and the cost of that is
somewhere below the noise floor of this machine rather than demonstrably zero.

Durability is close to free on this workload, which is the point of the everysec
policy -- the fsync happens on a background goroutine, not in the request path:

```
60000 requests, 50 clients, --appendonly --appendfsync everysec

SET            100705      0.486      0.527      1.580
INCR            95809      0.512      0.530      1.613
```

### Side by side with real Redis

Same machine, same benchmark client, same parameters — gedis on 6380, Redis on
6379:

| | Redis 3.0.504 | gedis | |
|---|---|---|---|
| PING | 57,595 | **72,450** | 1.26× |
| SET | 35,262 | **69,560** | 1.97× |
| GET | 27,796 | **70,763** | 2.55× |
| INCR | 30,135 | **72,248** | 2.40× |

**Read that comparison with care.** The Redis here is 3.0.504 — Microsoft's
Windows fork, abandoned in 2016. Redis is a Linux program; on Windows it runs
without the event-loop backend it was tuned for, while Go's runtime is native
here. Most of that gap is the platform, not the design. The honest claim is that
gedis is *competitive* on this hardware and genuinely uses all 8 threads where
Redis uses one; a rematch against Redis 7 on Linux is the test that would settle
it, and it is not the test run above.

Component-level, from `go test -bench`:

| | |
|---|---|
| RESP command decode | 228 ns/op, 4 allocs |
| RESP bulk reply encode | 19 ns/op, **0 allocs** |
| Store `Get` (parallel) | 62 ns/op |
| Store `Set` (parallel) | 138 ns/op |
| `ZRANK` on 100k members | 320 ns/op |
| `ZRANGE` of 10 from the middle of 100k | 237 ns/op |
| List push+pop at the front | 10 ns/op, **0 allocs** |
| List `LINDEX` | 2.4 ns/op, **0 allocs** |

## Testing

```console
$ go test -race ./...
ok  github.com/Nafiz001/gedis/internal/resp    2.180s
ok  github.com/Nafiz001/gedis/internal/server  2.064s
ok  github.com/Nafiz001/gedis/internal/store   1.767s
```

239 tests, race-clean. The server tests are end-to-end over a real TCP socket
rather than direct handler calls, because the mistakes that matter here are
reply framing, exact error text, and pipelined ordering — none of which a unit
test on a handler would catch. Expiration is driven by an injected clock, so
those tests are deterministic and fast instead of sleeping.

The skiplist gets its own treatment. Spans are redundant bookkeeping that
nothing else validates, and an off-by-one in insert or delete corrupts them
silently -- every rank query then returns a plausible but wrong answer. So the
tests check spans directly against real node positions, across 3000 randomised
insert/delete operations: promotion heights are random, so a fixed sequence
would exercise only some of the level-adjustment paths.

Where Redis behaviour was not obvious it was measured rather than guessed, by
running the same commands against `redis:7-alpine` in Docker. That caught two
wrong assumptions: `LREM key 2 a` on `[a b a c a]` leaves `[b c a]`, and
`LPOP key <count>` returns an empty array when the key exists but a null when
it does not.

The subsystems added last are tested against the thing they actually promise.
Persistence tests write, stop the server, start a new one over the same
directory, and check what came back — including a deliberately truncated log and
a deliberately corrupted snapshot. Replication tests stand up a real master and
a real replica on separate ports and let them talk over TCP. Transaction tests
run the optimistic-locking retry loop from four clients at once and assert no
update is lost.

That last one earned its keep. Four genuine defects turned up this way, none of
them typos:

- `EXEC` deadlocked against its own lock, because the reentrancy flag was
  declared and never set.
- `EXEC` checked the `WATCH` flag *before* taking the exclusive lock, leaving a
  window in which another client could still write a watched key — the exact
  lost update `WATCH` exists to prevent.
- `RENAME` never notified watchers, so a transaction watching a renamed key
  committed on data that had moved.
- Shutdown hung, because the replication goroutine parked on a socket read that
  nothing closed.

Verified against the official tooling:

```console
$ docker run --rm redis:7-alpine redis-cli -h host.docker.internal -p 6380 PING
PONG
$ docker run --rm redis:7-alpine redis-benchmark -h host.docker.internal -p 6380 -t set,get -q
```

## Build and run

```console
$ go build -o bin/ ./cmd/...
$ ./bin/gedis-server --addr :6380
```

| Flag | Default | |
|---|---|---|
| `--addr` | `:6380` | listen address |
| `--databases` | `16` | number of `SELECT`-able databases |
| `--requirepass` | — | require `AUTH` |
| `--maxclients` | `0` | connection cap, 0 = unlimited |
| `--shards` | `64` | lock shards per database |
| `--timeout` | `0` | idle connection timeout |
| `--loglevel` | `info` | `debug`, `info`, `warn`, `error` |
| `--appendonly` | off | log every write to an append-only file |
| `--appendfsync` | `everysec` | `always`, `everysec`, or `no` |
| `--dir` | `.` | directory for persistence files |
| `--save` | `900:1,300:10,60:10000` | snapshot points as `seconds:changes` |
| `--maxmemory` | `0` | memory limit, e.g. `512mb` |
| `--maxmemory-policy` | `noeviction` | plus four LRU/random/TTL policies |
| `--replicaof` | — | follow a master, as `host:port` |
| `--disable-commands` | — | switch commands off, comma separated |

Port 6380, not 6379, so gedis can run alongside a real Redis for comparison.

Durability is a real trade, so it is a flag rather than a default:

```console
$ gedis-server --appendonly --appendfsync always    # nothing acknowledged is lost
$ gedis-server --appendonly                         # at most one second lost
$ gedis-server --save 60:1000                       # snapshot only
```

Replication is two commands:

```console
$ gedis-server --addr :6380 --dir ./master
$ gedis-server --addr :6381 --dir ./replica --replicaof 127.0.0.1:6380
$ redis-cli -p 6381 INFO replication
role:slave
master_link_status:up
```

A server open to the internet should not let strangers run `CONFIG SET dir`
plus `SAVE` (which writes files wherever the process may), `REPLICAOF` (which
makes it connect out to any address) or `SHUTDOWN`. `--disable-commands`
switches them off the way Redis's `rename-command NAME ""` does: they answer
as unknown commands, `COMMAND` stops listing them, and a misspelt name stops
the server at startup rather than leaving the command on.

```console
$ gedis-server --disable-commands config,debug,replicaof,slaveof,shutdown,save,bgsave
```

## Roadmap

- [x] RESP2 protocol, TCP server, pipelining
- [x] Strings, counters, keyspace, expiration
- [x] Lists, hashes, sets, sorted sets
- [x] AOF persistence, rewrite, and replay on restart
- [x] Binary point-in-time snapshots with checksums
- [x] Pub/Sub, including pattern subscriptions
- [x] Transactions (`MULTI` / `EXEC` / `WATCH`)
- [x] Leader-follower replication
- [x] `maxmemory` with LRU / random / TTL eviction policies
- [ ] Blocking commands (`BLPOP`, `BRPOP`, `BLMOVE`)
- [ ] Partial resynchronisation from a replication backlog
- [ ] Redis Cluster hash slots

### Known limitations

Stated plainly, because an interviewer will ask.

- **`SCAN` cannot resume mid-shard.** Its cursor is a shard index, so a call
  returns a whole shard at a time and `COUNT` is a lower bound. Keys present for
  the entire scan are still returned at least once. Doing it properly needs a
  custom hash table with reverse-binary cursors, the way Redis does it, instead
  of Go's built-in map — which exposes no stable iteration cursor.
- **`HSCAN`, `SSCAN` and `ZSCAN` sort on every call.** Their cursor is an offset
  into byte order, which costs O(n log n) per call but is the price of a stable
  position -- Go randomises map iteration, so an offset into raw map order would
  skip and repeat elements. Redis avoids both with reverse-binary cursors.
- **`ZRANGEBYLEX` scans rather than seeks.** A skiplist search could find the
  first match in O(log n), but only by assuming the uniform-score precondition
  actually holds; scanning stays correct either way.
- **`MSETNX` is not fully atomic.** It checks every key and then writes. Locking
  all the keys at once invites cross-shard deadlock; the real fix arrives with
  the `MULTI`/`EXEC` machinery.
- **Replication has no partial resync.** A replica that reconnects always takes
  a full snapshot, where Redis would replay a backlog from the replica's offset.
  For a large keyspace on a flaky link, that is a real cost.
- **`WAIT` does not block.** It reports how many replicas are currently
  connected rather than waiting for acknowledgements, so it is a health check
  rather than a durability barrier. Better to say so than to fake the guarantee.
- **Eviction approximates twice.** Memory is the Go runtime's live-heap figure,
  not per-key accounting — it measures the process rather than the keyspace,
  which is what actually gets OOM-killed, but it is coarse. And victims come
  from sampling five keys rather than a true LRU list, which is the same
  approximation Redis makes and for the same reason.
- **A resyncing replica should not be read from.** The flush and the snapshot
  load are not atomic together, so there is a window where it looks empty.
  Redis has the same gap and answers it with `replica-serve-stale-data`.
- **RESP2 only.** `HELLO 3` is refused with `NOPROTO` rather than answered
  incorrectly.

## Layout

```
cmd/gedis-server     server binary
cmd/gedis-bench      throughput and latency benchmark client
internal/resp        RESP2 reader and writer
internal/store       sharded expiring keyspace and value types
internal/persistence append-only log and binary snapshots
internal/server      TCP server, dispatch, commands, pub/sub,
                     transactions, replication, eviction
```
