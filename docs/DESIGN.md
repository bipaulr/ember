# Ember design notes

This document explains why Ember is built the way it is, from first principles,
and where the design is weaker than the alternatives. Every claim about speed
points at a measurement in this repository (`bench/results/`) or says that it is
reasoning rather than measurement.

Contents

1. [What Ember is, and is not](#1-what-ember-is-and-is-not)
2. [Life of a request](#2-life-of-a-request)
3. [Pipelining and why we flush once per batch](#3-pipelining-and-why-we-flush-once-per-batch)
4. [The keyspace: why a sharded RWMutex](#4-the-keyspace-why-a-sharded-rwmutex)
5. [Expiry: lazy and active](#5-expiry-lazy-and-active)
6. [Persistence: the append-only file](#6-persistence-the-append-only-file)
7. [Why EXPIRE is rewritten to PEXPIREAT](#7-why-expire-is-rewritten-to-pexpireat)
8. [AOF rewrite and consistency under concurrent writes](#8-aof-rewrite-and-consistency-under-concurrent-writes)
9. [Pub/sub and the slow-subscriber policy](#9-pubsub-and-the-slow-subscriber-policy)
10. [Locks, ordering and deadlock freedom](#10-locks-ordering-and-deadlock-freedom)
11. [How it is tested](#11-how-it-is-tested)
12. [Known weaknesses](#12-known-weaknesses)
13. [What I would build next](#13-what-i-would-build-next)

## 1. What Ember is, and is not

Ember is a Redis-compatible in-memory key-value server. It speaks RESP2, so the
real `redis-cli`, `redis-benchmark` and `go-redis` work against it unchanged.
It supports strings, lists, key expiry, an append-only file and pub/sub.

It is a learning project, not a Redis replacement. The README lists what is
missing (replication, clustering, RESP3, transactions, Lua, RDB snapshots,
eviction). Where behaviour is easy to get subtly wrong (error message texts,
TTL rounding, `SET` option rules) I copied Redis's observable behaviour rather
than inventing my own.

The server uses only the Go standard library. Tests use `go-redis` as an
independent client to prove that the protocol implementation is not just
consistent with itself.

## 2. Life of a request

```
socket -> flushReader -> resp.Reader -> command table -> handler -> store (shard lock)
                                                           |            |
                                                      resp.Writer       +--> AOF.Append (under the shard lock)
                                                           |
socket <- connWriter (AOF.Commit first, if this client wrote) <- bufio flush
```

One goroutine per connection owns the reader, the writer and the client state,
so no per-connection locking is needed for ordinary commands. A goroutine is
cheap (a few KB of stack) and the Go scheduler multiplexes thousands of them
onto the CPU cores, which is why Ember uses blocking reads in a plain loop
instead of an event loop with explicit state machines.

The only shared mutable state is the keyspace (sharded), the AOF (one mutex),
the pub/sub hub (one RWMutex) and a few counters.

## 3. Pipelining and why we flush once per batch

A client may send many commands without waiting for replies. That is
pipelining. The server must answer them in order. The parser reads from a
`bufio.Reader`, so one `read` system call can pull in dozens of commands, and
Ember simply loops: parse, execute, append the reply to a buffer.

The decision that matters is **when to send the replies**. Sending after every
command costs one `write` system call per command. A system call costs on the
order of a microsecond or more, which is more than executing a `GET` (about 90 ns
in `BenchmarkExecuteGET`). So Ember does not flush after each command. Instead
the `flushReader` wrapper flushes right before the connection would block
waiting for more input:

* `bufio.Reader` only calls the underlying `Read` when its buffer is empty,
* so "about to call Read on the socket" means "every command already received
  has been executed and answered",
* so all those replies leave in a single `write`.

This also handles a partly received command correctly. If a client sends
`PING\r\n*2\r\n$4\r\nEC` and waits for the first reply before sending the
rest, the reply to `PING` is flushed before we block waiting for `HO`. Flushing
"after the buffer is empty" at the command boundary would deadlock that client;
flushing "before the socket read" does not. `TestPartialCommandAfterReplies`
covers it.

Measured effect (loopback, 50 connections, `internal/server` benchmarks,
`bench/results/after.txt`): a `SET` costs about 4.7 microseconds at pipeline depth
1 and about 0.41 microseconds at depth 16, roughly 11 times cheaper per command,
because the per-batch system calls are shared by 16 commands.

Large replies are fine too: the writer is a 16 KB `bufio.Writer`, so a bigger
reply is written in chunks as it is produced.

## 4. The keyspace: why a sharded RWMutex

A map is not safe for concurrent use, and Ember handles each connection on its
own goroutine, so something must protect it. Three options:

| Design | How it works | Weakness |
|---|---|---|
| One `sync.Mutex` (or RWMutex) around one map | Simplest. Obviously correct. | Every command from every core queues on one lock. |
| `sync.Map` | Lock-free reads of stable keys, sharded internally. | Only single-step atomics (load, store, compare-and-swap); no multi-step read-modify-write. |
| N maps, each with its own `sync.RWMutex`, key hashed to a shard | Commands on different shards run in parallel. | More code; operations spanning shards need care. |

Ember uses the third, 256 shards by default (`--shards`), key hashed with FNV-1a
(32-bit, written inline so hashing does not allocate).

### What I measured

`BenchmarkContention` in `internal/store`, a 90% read / 10% write mix on 100,000
keys from all 16 hardware threads of the development machine (median of 5 runs,
`bench/results/after.txt`):

| Design | ns per operation |
|---|---:|
| plain `map` + one `RWMutex` | 187 |
| Ember store with 1 shard | 376 |
| Ember store with 16 shards | 69 |
| Ember store with 256 shards | 22 |
| `sync.Map` | 16 |

Honest reading of these numbers:

* Sharding is what removes the lock bottleneck: 256 shards is about 17 times
  faster than one shard (one lock) on the same code. Past the point where
  collisions are rare, more shards stop helping; the limit is then the core count.
* The store with 1 shard is slower than the bare map because it also does expiry
  checks and TTL bookkeeping on every access.
* **`sync.Map` is faster than my sharded store on this read-heavy micro-benchmark**
  (16 ns versus 22 ns). I still chose shards because Ember needs operations that
  `sync.Map` cannot express atomically:
  * `INCR`, `APPEND`, `SET ... NX GET`, `LPOP`: read, decide, write, as one step.
    With `sync.Map` that needs a compare-and-swap retry loop per command type, or
    a second lock, which brings back the problem.
  * The TTL index for active expiry must stay consistent with the map.
  * The AOF must be written in exactly the order writes were applied to each key
    (section 6). Holding the shard lock while appending gives that ordering for free.
* For a server whose real cost is parsing and system calls (a `GET` through the
  whole stack is hundreds of nanoseconds, the map is tens), the 5 ns difference
  does not decide anything. Correctness of compound operations does.

### Why 256 shards, why FNV

More shards than cores keeps the collision probability low. 256 shards cost
about 256 small maps and locks, which is negligible. FNV-1a is a few
multiply-xor steps per byte, has no allocation, and spreads keys well enough
(`TestHashSpreadsKeys` checks the spread). It is not a cryptographic hash, so a
malicious client could craft keys that all land in one shard and degrade
performance, though not correctness. Redis avoids this class of problem with a
randomly seeded hash; adding a per-process random seed to the hash is a cheap
future change.

### Limits of the design

* `KEYS` and `DBSIZE` visit shards one at a time, so they are not a point-in-time
  snapshot. Redis's single thread makes them atomic; Ember trades that for not
  stopping the world.
* `MSET` and `DEL a b c` apply per key, each under its own shard lock, so they
  are not atomic across keys. A concurrent reader can see half an `MSET`. This
  is written down rather than hidden; it matters only for applications that
  rely on multi-key atomicity, which Redis users normally get from transactions
  (not supported here).
* `FLUSHALL` is the one operation that takes every shard lock (in index order,
  the only multi-lock path, so it cannot deadlock with itself).

## 5. Expiry: lazy and active

A key with a TTL must stop being visible when the deadline passes. There are two
mechanisms, and Ember needs both.

**Lazy expiry** is the cheap one: every access first checks `ExpireAt` against the
clock. If the key is past its deadline, the access deletes it and behaves as if
it never existed. This is exact (a client never sees an expired value) and costs
one integer comparison. Its weakness: a key that nobody touches again would sit
in memory forever.

**Active expiry** fixes that. Scanning every key with a TTL periodically would
be too slow for a big keyspace, so Ember copies the Redis idea: sample a few
and extrapolate.

1. Every 100 ms, take 20 random keys that have a TTL.
2. Delete those that have expired.
3. If more than 25% of the sample was expired, the keyspace probably has many
   expired keys, so repeat immediately. Otherwise stop.
4. A time budget (25 ms) caps one round so expiry cannot starve clients.

The 25% rule is what makes it self-tuning. If 1% of TTL keys are stale, the
first sample finds about 0.2 expired keys and the cycle ends after 20 cheap
lookups. If a million keys all expire at the same instant, every sample is 100%
expired and the cycle keeps going until the budget ends, then continues on the
next tick. Memory held by stale keys is therefore bounded without ever scanning
everything.

How sampling is implemented (`internal/store/store.go`): each shard keeps a slice
`ttlKeys` of the keys that have a TTL plus a map from key to slice position, so
that a random element can be picked in O(1) and a key can be removed in O(1) by
swapping with the last element. A per-shard atomic counter says whether a shard
has any TTL keys, so the sampler can skip empty shards without locking them.

Two deliberate simplifications versus Redis:

* The shard is picked by starting at a random shard and moving forward to the
  first one with TTL keys. That slightly favours shards that follow empty
  shards. It is not uniform sampling, but with a good hash it is close, and it
  finds a single expiring key quickly even when 255 shards are empty.
* Redis samples buckets of its hash table; Ember samples from the explicit TTL
  index. Same statistical idea, simpler code.

Expired keys are also removed from the AOF: both lazy and active deletion
propagate a `DEL`, so replaying the log never resurrects them.

**Time during replay.** While the AOF is being replayed the store clock is
frozen (`SetLoading`). Without that, replaying `SET k 1 PXAT <deadline already
past>` followed by `INCRBY k 1` would expire `k` on the first access, create a
new immortal `k` with value 1, and silently turn a temporary key into a permanent
one. `TestExpiryAcrossDowntime` is the regression test. Real Redis does the same
(no expiry while loading).

## 6. Persistence: the append-only file

Ember logs every successful write command to a file in RESP format, the same
bytes a client would send. On startup it replays the file through the normal
command table. RESP is already a framing format, so the log needs no new
parser, and a human can read it with `less`.

### Ordering: log while holding the shard lock

If two clients run `SET k a` and `SET k b` at the same time, the final value is
whichever the shard applied last. The log must record them in the same order, or
replay would produce a different value than the live server had. Ember therefore
calls `AOF.Append` **inside** the shard-lock critical section, right after the
change. Append only copies bytes into a memory buffer (no I/O), so the lock is
held for nanoseconds extra. Writes to *different* keys can be logged in any
relative order, because they commute.

### Commands are logged in canonical form

The log stores what the command *did*, not what the client typed:

* `SET k v EX 10` is logged as `SET k v PXAT <absolute ms>` (section 7),
* `SET k v NX GET` is logged as plain `SET k v`, because the condition has been
  decided and replay must not re-decide it,
* `INCR k` is logged as `INCRBY k 1`,
* `MSET a 1 b 2` is logged as two `SET`s,
* `GETDEL k` is logged as `DEL k`.

### Writing: when does data reach the disk?

Between a `write` call and the platform's storage there are two stages. After
`write` returns, the bytes are in the operating system's page cache: they survive
if only Ember dies, but not if the machine loses power. `fsync` forces them to
the device. `fsync` is slow (it waits for the disk), so the policy decides how
often to pay for it:

| `--appendfsync` | When the reply is sent | Survives `kill -9` of Ember | Survives power loss |
|---|---|---|---|
| `always` | after `write` and `fsync` | all acknowledged writes | all acknowledged writes |
| `everysec` (default) | after `write` | all acknowledged writes | all but roughly the last second |
| `no` | after `write` | all acknowledged writes | whatever the OS had flushed (often 30 s) |

The server code that implements it: a client that has run a write command
carries a `Dirty` flag. Its reply buffer's underlying writer (`connWriter`)
calls `AOF.Commit()` before any bytes go to the socket. So with `always`, a
client never receives "OK" for a write that is not on disk. Under `everysec`
and `no`, `Commit` just hands the pending bytes to the OS, and a background
goroutine calls `fsync` once per second.

**Group commit.** Under `always`, 50 clients each wanting an `fsync` would
queue up and pay 50 disk round trips. `Commit` therefore elects a leader: the
first caller writes everything pending and calls `fsync` once, while the others
wait; when it finishes, all callers whose records were included return.
Throughput under `always` grows with concurrency instead of staying at
`1 / fsync latency`.

**What the policies cost, measured** (`bench/fsync.ps1`, 2 CPUs per server, 50
clients, `SET` only, median of 3; the same fsync cost applies to Redis, shown
for scale):

| appendfsync | -P | Redis SET rps | Ember SET rps | Ember / Redis | Redis p99 ms | Ember p99 ms |
|---|---:|---:|---:|---:|---:|---:|
| always | 1 | 20,469 | 15,359 | 0.75x | 4.599 | 7.615 |
| always | 16 | 133,067 | 160,000 | 1.20x | 8.647 | 7.183 |
| everysec | 1 | 66,622 | 114,220 | 1.71x | 1.295 | 1.287 |
| everysec | 16 | 266,312 | 399,202 | 1.50x | 4.071 | 4.223 |
| no | 1 | 66,622 | 114,220 | 1.71x | 1.335 | 1.183 |
| no | 16 | 266,312 | 399,202 | 1.50x | 3.983 | 4.503 |

* `everysec` and `no` are indistinguishable: a background fsync once per
  second costs nothing visible.
* `always` is where durability is paid for. At depth 1 Ember drops from
  114,220 to 15,359 SET/s (7.4x) and Redis from 66,622 to 20,469 (3.3x), because
  every acknowledged write waits for a disk flush; with 50 clients that is about
  3 ms per round. With the group commit above, those 50 writes share one flush: if
  each paid for its own, throughput would be bounded by `1 / fsync latency`
  (a few hundred per second if a flush takes a few milliseconds), not the 15,000
  measured. I did not measure the flush latency of this virtual disk directly;
  the 3 ms is the average round time per client, which is an upper bound on it.
* Under `always` at depth 1 Ember is 25% slower than Redis, although it is 71%
  faster under `everysec`. I have not profiled that; see BENCHMARKS.md for the
  suspected cause.
* The disk here is the Docker Desktop VM's virtual disk. A real NVMe drive with a
  power-loss-protected cache will have a very different `always` cost.

**What the crash tests prove, and what they cannot.** `TestCrashRecoveryAlways`
writes 10,000 keys (half with TTLs), kills the process with `Process.Kill()` (no
shutdown code runs), restarts it and checks every key and TTL: 0 lost.
`TestCrashRecoveryEverysec` kills the process during a stream of writes; it
asserts that at most about a second of writes is lost, and in practice it loses
zero. That zero is **expected and not a proof of the one-second claim**: killing a
process does not lose what is already in the OS page cache. Only pulling the
power (or a VM-level kill) would show the `everysec` loss window, and I have not
done that. The one-second figure is the design contract, not something this
test measured.

### Replay and damaged files

On startup, before the listener opens, `aof.Replay` reads commands one by one:

* **A partial final entry** (the process died in the middle of a `write`): the
  parser reports an unexpected end of file. Ember logs a warning, truncates the
  file back to the last complete command, and continues. This is the normal
  signature of a crash and losing the partial command is correct, because it was
  never acknowledged under `always`, and is within the loss window otherwise.
  The test cuts the file at *every* byte offset inside the last record
  (`TestTruncatedTail`).
* **Anything else** (a malformed header in the middle, or a command that fails
  when replayed) stops startup with `aof ... is corrupt at byte offset N`, exit
  status non-zero, and the file left untouched. Guessing past real corruption
  could silently restore the wrong data.

## 7. Why EXPIRE is rewritten to PEXPIREAT

`EXPIRE k 60` means "60 seconds from now". Suppose Ember logged it literally and
crashed. An hour later it restarts and replays the log. The replayed `EXPIRE k
60` would give `k` a fresh 60 seconds starting *at replay time*, so a key that
should have died 59 minutes ago comes back to life, and a key that was meant to
expire at 10:01 expires at 11:01.

`PEXPIREAT k 1760000000000` means "at this wall-clock instant". It is idempotent
and independent of when it is replayed. So `EXPIRE`, `PEXPIRE`, `EXPIREAT` and
the `EX`/`PX` options of `SET` are all converted to an absolute millisecond
timestamp before they are logged. On replay the deadline is the same as before
the crash, and a key whose deadline passed during downtime is simply expired
(lazily, once loading ends).

The rewrite output (section 8) uses the same representation: a key with a TTL is
emitted as `SET k v` followed by `PEXPIREAT k <ms>`.

The conversion trusts the machine's wall clock. If the clock jumps, TTLs jump
with it. Redis has the same property.

## 8. AOF rewrite and consistency under concurrent writes

The log only grows. `INCR counter` a million times writes a million records,
though the final state is one `SET`. A rewrite replaces the file with the
shortest set of commands that rebuilds the current keyspace. It starts on
`BGREWRITEAOF`, or automatically when the file is at least 64 MB and at least
twice its size after the previous rewrite.

Redis solves "the data keeps changing during the rewrite" with `fork`: the child
process has a frozen copy-on-write view of memory. Go cannot fork a running
runtime safely, and the server must also run on Windows, so Ember uses a
different technique. The difficulty is real: if a shard is copied at time t and
a write at time t - epsilon is *also* recorded in a side buffer, the write is
applied twice on replay. That is harmless for `SET` and wrong for `INCR`,
`APPEND` and `LPUSH`.

Ember's solution:

1. When the rewrite starts, every `Append` also copies its record into a
   **per-shard** buffer `rbuf[shard]`.
2. The rewriter visits the shards one at a time. For shard i it takes the
   shard's **read lock** and, while holding it, *clears* `rbuf[i]`, then renders
   the shard's contents into a memory chunk, then releases the lock and writes the
   chunk to a temporary file.
3. Why clearing at that moment is exact: appends for shard i happen under shard
   i's write lock. Holding the read lock excludes them. So everything in
   `rbuf[i]` so far was applied *before* the snapshot and is contained in it;
   everything appended later is *not* in the snapshot. After the snapshot,
   `rbuf[i]` holds exactly "the writes the snapshot lacks".
4. At the end, with appends briefly blocked (the AOF mutex), the rewriter
   appends every `rbuf[i]` to the temporary file, fsyncs it, closes the old
   file, **renames the temporary file over the old one**, and reopens it for
   appending. For each shard the new file is "snapshot, then later writes in
   order", which replays to the same state as the old file.

The test that proves this (`TestRewriteUnderConcurrentWrites`) runs 8 writers
using `INCR`, `RPUSH` and `APPEND` on shared keys, while a goroutine rewrites in
a tight loop (302 rewrites completed during 48,000 writes in the recorded run). Afterwards the file is replayed into a fresh store and
compared with the live store byte for byte, and the `INCR` totals must be exact.
A single lost or doubled write would fail it.

Other properties:

* **Crash safety.** The new file is complete and fsynced before the rename. If
  the process dies before the rename the old file is intact; after it, the new
  one is. A leftover temporary file is deleted at startup. On Windows a file that
  is open cannot be replaced, so the old file is closed first; the crash window
  between close and rename still leaves the old file in place.
* **Pause.** Appends (and therefore writes to every shard) wait only for the
  final tail copy and the rename. The expensive part, rendering the keyspace,
  holds one shard's read lock at a time.
* **`FLUSHALL` during a rewrite aborts it.** A single `FLUSHALL` record cannot be
  ordered correctly against per-shard snapshots taken at different moments, so
  the rewrite is abandoned, the old file stays valid, and the next trigger
  retries. This is rare and safe.
* **Memory.** The side buffers hold the writes that arrive during the rewrite,
  so a write-heavy rewrite of a large dataset needs extra memory proportional to
  the write volume during that time.

## 9. Pub/sub and the slow-subscriber policy

`SUBSCRIBE` registers the connection with the hub (channel to subscribers, and
pattern to subscribers). `PUBLISH` finds the subscribers and delivers to each.

The design problem is **what to do when one subscriber cannot keep up**. A
publisher cannot wait for it: that would let one stuck client stall every
`PUBLISH` and, indirectly, every other subscriber. There are three honest
options: block the publisher (wrong), buffer without limit (memory grows until
the process dies), or bound the buffer and cut off the client that overflows it.

Ember bounds it. Each subscriber has a queue limited by message count (1024) and
by bytes (8 MB). `Publish` does a non-blocking send:

* queue has room: the message is queued; the receiver counts toward the reply,
* queue is full or over its byte budget: the subscriber is marked overflowed,
  its connection is closed, and a warning is logged.

The disconnected client sees a closed connection and can reconnect and
resubscribe. That is the same contract as Redis's `client-output-buffer-limit
pubsub`, which also disconnects. Messages are not replayed: pub/sub is
at-most-once delivery by design.

Implementation notes:

* The message is encoded **once** per channel and the same immutable bytes are
  queued for every subscriber, so fan-out to 1,000 subscribers is 1,000 channel
  sends, not 1,000 encodings.
* A subscribed connection needs two writers: the command loop (confirmations,
  `PING` replies) and a **pump goroutine** that pushes queued messages. They share
  a per-connection mutex. The command loop holds it while it executes a batch and
  releases it only while blocked in `read`, so a pushed message is never written
  in the middle of a reply, and a subscription confirmation is always flushed
  before the first message that follows it.
* When the connection ends, cleanup removes every subscription from the hub,
  signals the pump and **waits for it**, so nothing leaks. `TestNoGoroutineLeaks`
  measures `runtime.NumGoroutine()` before and after 60 subscribers that
  disconnect abruptly, politely or with `QUIT` (3 before, 3 after).
* `TestSlowSubscriberIsDisconnected`: a client subscribes and never reads while
  80 half-megabyte messages are published. The worst `PUBLISH` took 18 ms, the
  silent client was disconnected, and a healthy subscriber on the same channel
  received all 80 messages.

Subscribed clients may only run `SUBSCRIBE`, `UNSUBSCRIBE`, `PSUBSCRIBE`,
`PUNSUBSCRIBE`, `PING` and `QUIT`; anything else returns the same error text as
Redis.

## 10. Locks, ordering and deadlock freedom

There are four kinds of lock. The only nesting is:

```
shard lock  ->  AOF mutex          (Append, called under the shard lock)
shard RLock ->  AOF mutex          (the rewrite's mark callback)
client write mutex (per connection), alone
hub RWMutex, alone
```

* Nothing takes a shard lock while holding the AOF mutex, so the two cannot
  deadlock. The rewrite's final phase holds the AOF mutex and touches no shard.
* Only `FLUSHALL` takes several shard locks, always in ascending index order.
* `Commit` waits on a condition variable that releases the AOF mutex while
  waiting; group-commit leaders fsync *outside* the mutex.
* The overflow callback of a slow subscriber only closes a connection. It does
  not take the hub lock (publish holds the read side), so there is no
  lock-order problem there either.
* `go test -race` is clean over the whole suite, including the stress, rewrite
  and pub/sub tests.

## 11. How it is tested

* **Unit tests** per package, table-driven where there are many cases (RESP,
  command semantics, glob, store, AOF, hub).
* **Fuzzing**: `FuzzParse` feeds arbitrary bytes to both parsers; the CI runs it
  for 30 s (6.5 million executions locally, no panics). A second fuzz target
  covers the glob matcher.
* **Integration tests** drive the server with `go-redis`, including the exact
  error strings (`WRONGTYPE`, arity, integer errors) and every command.
* **Crash tests** build the real binary, run it as a subprocess and kill it.
* **Concurrency**: 100 goroutines doing mixed `INCR`/`SET`/`GET`/`DEL` on
  overlapping keys, with exact `INCR` totals; run under the race detector.
* **Coverage** is above 80% in every package under `internal/` and is enforced in CI.

## 12. Known weaknesses

* Multi-key commands are not atomic (section 4).
* The AOF grows until a rewrite, and a rewrite needs memory for the writes that
  arrive meanwhile.
* The hash is not randomly seeded (section 4).
* Memory is not limited or accounted: there is no `maxmemory` and no eviction.
* `fsync` errors under `everysec` are logged but do not stop the server.
* Only database 0 exists.
* The `everysec` loss window under power failure is the design contract; it was
  not measured by cutting power (section 6).
* Compatibility was checked with `redis-cli`, `redis-benchmark` and `go-redis`.
  Other clients may use commands Ember does not implement.

## 13. What I would build next

### RDB-style snapshots

Goal: faster restarts and smaller files than replaying a log. Plan: reuse the
rewrite machinery. Per-shard snapshots under the read lock already produce a
consistent per-shard image. Write them in a compact binary format (type, key,
expiry, value, with a checksum per shard and a header recording the AOF offset at
which the snapshot started). Startup would load the snapshot, then replay only
the AOF tail after that offset. The consistency argument is the same as the AOF
rewrite. The cost is a second persistent format to keep compatible across versions.

### LRU eviction

Goal: a `maxmemory` limit. Plan: approximate LRU as Redis does. Track an access
clock in each entry (a coarse 24-bit seconds counter is enough), account the
approximate size of keys and values on every write, and when over the limit
sample five random keys from a shard and evict the least recently used, repeating
until under the limit. Sampling avoids the pointer-heavy linked list of exact
LRU, which would also need a global lock. The TTL index already shows how to
sample random keys cheaply. Evictions must be propagated to the AOF as `DEL`s.
The hard part is honest memory accounting in Go (headers, map overhead, GC
slack), and tests that show eviction keeps the process under its limit.

### Replication

Goal: a read replica. Plan, in stages:

1. A replica connects and sends `PSYNC`. The master takes a snapshot using the
   per-shard method, streams it, and meanwhile records the write stream that
   follows, just like `rbuf` in the rewrite.
2. After the snapshot, the master streams the same canonical commands it writes
   to the AOF (they are already deterministic: absolute expiry, no `NX`/`GET`
   conditions). Because they are canonical, the replica can apply them with the
   normal command table.
3. A replication offset and a bounded backlog (a ring buffer of recent commands)
   allow a briefly disconnected replica to resume without a full sync.
4. Replicas refuse writes and a master tracks replica lag in `INFO`.

The correctness issue to design for is ordering: the master's stream is
per-shard ordered, not globally ordered, which is fine for a replica that applies
commands per key, as long as `FLUSHALL` (the one global command) is a barrier.
