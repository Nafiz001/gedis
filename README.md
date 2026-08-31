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
127.0.0.1:6380> SET session:abc token EX 60
OK
127.0.0.1:6380> TTL session:abc
(integer) 60
127.0.0.1:6380> INFO keyspace
# Keyspace
db0:keys=5,expires=1,avg_ttl=0
```

## Why build this

Redis is one of the few pieces of infrastructure almost every backend touches,
and almost nobody has opened. Reimplementing it forces you through the parts
that are usually hidden: framing bytes off a socket, keeping a shared keyspace
correct under concurrency, expiring keys without stalling the server, and
matching an error string exactly because real clients branch on it.

## What works

**60 commands**, matching Redis semantics including the modifier combinations
that actually carry meaning.

| Family | Commands |
|---|---|
| Strings | `SET` (`NX` `XX` `GET` `EX` `PX` `EXAT` `PXAT` `KEEPTTL`), `GET`, `GETSET`, `GETDEL`, `GETEX`, `SETNX`, `SETEX`, `PSETEX`, `MGET`, `MSET`, `MSETNX`, `APPEND`, `STRLEN`, `SETRANGE`, `GETRANGE`, `SUBSTR` |
| Counters | `INCR`, `DECR`, `INCRBY`, `DECRBY`, `INCRBYFLOAT` |
| Keyspace | `DEL`, `UNLINK`, `EXISTS`, `TYPE`, `KEYS`, `SCAN` (`MATCH` `COUNT` `TYPE`), `RANDOMKEY`, `RENAME`, `RENAMENX`, `COPY` |
| Expiration | `EXPIRE` / `PEXPIRE` / `EXPIREAT` / `PEXPIREAT` (`NX` `XX` `GT` `LT`), `TTL`, `PTTL`, `EXPIRETIME`, `PEXPIRETIME`, `PERSIST` |
| Connection | `PING`, `ECHO`, `SELECT`, `AUTH`, `HELLO`, `RESET`, `QUIT`, `CLIENT`, `COMMAND` |
| Server | `DBSIZE`, `FLUSHDB`, `FLUSHALL`, `INFO`, `TIME`, `CONFIG`, `DEBUG`, `SHUTDOWN` |

Also: 16 independent `SELECT`-able databases, `requirepass` authentication,
client pipelining, the inline (telnet) protocol, glob patterns with character
classes, and graceful shutdown on `SIGINT`/`SIGTERM`.

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
              └─────────────────────┘
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
PING            71406      0.689      0.637      2.141
SET             54296      0.911      0.876      3.045
GET             63245      0.780      0.743      2.083
INCR            65836      0.751      0.662      2.094

400000 requests, 50 clients, pipeline 16

SET            598538      1.287      1.138      4.823
GET            675571      1.138      1.053      3.973
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

## Testing

```console
$ go test -race ./...
ok  github.com/nafizahmed/gedis/internal/resp    2.180s
ok  github.com/nafizahmed/gedis/internal/server  2.064s
ok  github.com/nafizahmed/gedis/internal/store   1.767s
```

112 tests, race-clean. The server tests are end-to-end over a real TCP socket
rather than direct handler calls, because the mistakes that matter here are
reply framing, exact error text, and pipelined ordering — none of which a unit
test on a handler would catch. Expiration is driven by an injected clock, so
those tests are deterministic and fast instead of sleeping.

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

Port 6380, not 6379, so gedis can run alongside a real Redis for comparison.

## Roadmap

- [x] RESP2 protocol, TCP server, pipelining
- [x] Strings, counters, keyspace, expiration
- [ ] Lists, hashes, sets, sorted sets
- [ ] AOF persistence and replay on restart
- [ ] RDB-style point-in-time snapshots
- [ ] Pub/Sub
- [ ] Transactions (`MULTI` / `EXEC` / `WATCH`)
- [ ] Leader-follower replication
- [ ] Keyspace eviction policies (LRU / LFU)

### Known limitations

Stated plainly, because an interviewer will ask.

- **`SCAN` cannot resume mid-shard.** Its cursor is a shard index, so a call
  returns a whole shard at a time and `COUNT` is a lower bound. Keys present for
  the entire scan are still returned at least once. Doing it properly needs a
  custom hash table with reverse-binary cursors, the way Redis does it, instead
  of Go's built-in map — which exposes no stable iteration cursor.
- **`MSETNX` is not fully atomic.** It checks every key and then writes. Locking
  all the keys at once invites cross-shard deadlock; the real fix arrives with
  the `MULTI`/`EXEC` machinery.
- **No persistence yet**, so all data is lost on restart.
- **RESP2 only.** `HELLO 3` is refused with `NOPROTO` rather than answered
  incorrectly.

## Layout

```
cmd/gedis-server   server binary
cmd/gedis-bench    throughput and latency benchmark client
internal/resp      RESP2 reader and writer
internal/store     sharded expiring keyspace
internal/server    TCP server, dispatch, command handlers
```
