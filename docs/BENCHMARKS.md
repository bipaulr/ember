# Benchmarks

Ember against Redis 7.4, measured with the real `redis-benchmark`, plus Go
micro-benchmarks, a profile-driven optimisation, and the mistakes I made on the
way (two of them changed the results a lot, so they are written down).

Nothing here says "Ember is faster than Redis" without a qualifier, because the
honest answer depends on how many CPU cores each server gets. Read
[the summary](#summary) first.

## Summary

* **Same limits (2 CPUs each), default Redis:** Ember's throughput is 1.3x to
  2.5x Redis's on `SET`, `GET`, `INCR`, `LPUSH`, at pipeline depth 1 and 16.
  The main reason is that Ember runs commands on both cores and Redis runs
  them on one thread. Ember's median latency is lower; its 99th percentile is
  higher at depth 16.
* **One CPU each:** this removes the core-count advantage. Ember is then
  **slower** than Redis at depth 1 (0.75x to 0.90x) and roughly level at depth 16
  (0.83x to 1.0x, with one outlier where it is faster). Per core, Redis is the more efficient
  server.
* **Redis with 2 I/O threads (2 CPUs):** level at depth 1 (0.88x to 1.09x),
  Ember still ahead at depth 16 (1.5x to 2.3x).
* **Memory:** Ember used **2.1 times** the memory of Redis for the same keys
  (216 MiB against 101 MiB).
* **Durability:** `appendfsync always` costs Ember 7.4x of its depth-1
  throughput and Redis 3.3x. `everysec` costs nothing measurable.
* **The most useful finding was about Go in containers:** with a CPU limit,
  `GOMAXPROCS` has to match the limit or the kernel throttles the process
  (p99 of 67 ms and a 4x throughput loss in my first run). [Details below](#the-first-run-measured-a-bug-not-ember).

Absolute numbers are specific to this machine and to Docker Desktop's virtual
network; the ratios and the reasoning are what to take away.

## Method

**Machine.** Windows 11 Home, Intel Core i7-12650H (10 cores, 16 logical CPUs),
15.7 GB RAM, Docker Desktop 29.7.2 with its WSL2 VM given all 16 CPUs and 8 GB.
Recorded by the scripts in `bench/results/environment*.txt`.

**Topology.** One user-defined Docker bridge network, `docker compose` file
`bench/docker-compose.yml`:

| Container | Image | Limits | Persistence |
|---|---|---|---|
| `redis` | `redis:7-alpine` (7.4.11), pinned by digest | `cpus` 2 (or 1), 1 GB | `--appendonly yes --appendfsync everysec --save ""` |
| `ember` | built from the repo `Dockerfile` (Go 1.25, static, distroless, non-root) | same | `--appendfsync everysec` |
| `bench` | same `redis:7-alpine` image: `redis-benchmark` and `redis-cli` | none | |

Both servers use a named volume for their AOF, so the disk path is the same
kind for each. Only one server is under load at a time.

**Load.** `redis-benchmark -c 50 --threads 4 -r 100000 -P <1|16> -n <N> -t <test>`
for `set`, `get`, `incr`, `lpush`: 50 client connections, 4 benchmark threads,
random keys from a space of 100,000, values of 3 bytes (the default).

**Procedure.** For each server and depth: `FLUSHALL`; a warm-up (100,000 `SET`/`GET`);
`FLUSHALL`; then each test in turn. Before `GET`, 500,000 `SET`s populate the
keyspace so `GET` mostly hits. The two servers alternate which goes first in
each repeated run. The tables show the **median** of the repeats, plus the
min-to-max range.

**Exact commands.**

```powershell
.\bench\run.ps1                                    # the specified run: 100,000 requests, 5 repeats, 2 CPUs
.\bench\run.ps1 -Requests 1000000 -Repeat 3 -Tag -1m                       # long run, 2 CPUs
.\bench\run.ps1 -Cpus 1 -Requests 1000000 -Repeat 3 -Tag -1m               # 1 CPU each
.\bench\run.ps1 -RedisIoThreads 2 -Requests 1000000 -Repeat 3 -Tag -1m-redis-io2
.\bench\fsync.ps1 -Requests 200000 -Repeat 3       # fsync policies
.\bench\memory.ps1                                 # memory after loading keys
```

`bench/run.sh` does the same on Linux and macOS. Raw per-run rows are in
`bench/results/*.csv`; `go run ./bench/summarize <csv>` regenerates a table.

### A measurement limit you need to know about

`redis-benchmark` only notices that a run has finished on a 250 ms timer, so a
reported rate is `N / (k x 0.25 s)` for a whole `k`. At 100,000 requests, any
run that takes under a quarter of a second is reported as exactly 398,406
requests per second, whatever the server could really do. At 1,000,000 requests
and about 400,000 requests per second the resolution is roughly plus or minus 9%.
So:

* the specified 100,000-request run is shown, but **ties at 398,406 mean "fast
  enough to hit the timer", not "equal"**;
* the 1,000,000-request runs are the ones to read for fast configurations;
* differences under about 10% at depth 16 are not resolved by any table here.

## Results

### 1. As specified: 2 CPUs each, 100,000 requests, median of 5

Median of 5 runs. rps is requests per second; latencies are milliseconds.

| Test | -P | Redis rps | Ember rps | Ember / Redis | Redis p50 | Ember p50 | Redis p99 | Ember p99 | rps range (Redis; Ember) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| SET | 1 | 66622 | 99900 | 1.50x | 0.679 | 0.335 | 1.311 | 1.175 | 57110 to 66622; 99900 to 100000 |
| GET | 1 | 66622 | 133156 | 2.00x | 0.631 | 0.255 | 1.223 | 0.887 | 66578 to 66622; 133156 to 133333 |
| INCR | 1 | 66622 | 99900 | 1.50x | 0.687 | 0.343 | 1.391 | 1.287 | 66622 to 66622; 99900 to 100000 |
| LPUSH | 1 | 57078 | 99900 | 1.75x | 0.735 | 0.335 | 1.367 | 1.335 | 57078 to 57110; 99800 to 99900 |
| SET | 16 | 398406 | 398406 | 1.00x | 1.927 | 0.815 | 3.263 | 4.367 | 199601 to 400000; 398406 to 401606 |
| GET | 16 | 398406 | 398406 | 1.00x | 1.183 | 0.319 | 2.095 | 1.095 | 398406 to 400000; 398406 to 400000 |
| INCR | 16 | 199601 | 398406 | 2.00x | 1.911 | 0.871 | 4.567 | 4.807 | 199203 to 398406; 398406 to 400000 |
| LPUSH | 16 | 200000 | 398406 | 1.99x | 2.855 | 0.799 | 5.303 | 4.775 | 199203 to 200000; 398406 to 400000 |

Limited by the timer effect above: every `398,406` is a ceiling, and the
depth-16 `GET` tie says nothing.

### 2. 2 CPUs each, 1,000,000 requests, median of 3

Median of 3 runs. rps is requests per second; latencies are milliseconds.

| Test | -P | Redis rps | Ember rps | Ember / Redis | Redis p50 | Ember p50 | Redis p99 | Ember p99 | rps range (Redis; Ember) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| SET | 1 | 68942 | 88834 | 1.29x | 0.687 | 0.391 | 1.279 | 1.863 | 68932 to 68942; 88786 to 121197 |
| GET | 1 | 74069 | 128999 | 1.74x | 0.631 | 0.287 | 1.183 | 1.343 | 70102 to 75472; 111099 to 153870 |
| INCR | 1 | 70151 | 105219 | 1.50x | 0.671 | 0.367 | 1.247 | 1.607 | 53276 to 71398; 99980 to 117619 |
| LPUSH | 1 | 65535 | 121183 | 1.85x | 0.719 | 0.335 | 1.359 | 1.239 | 54010 to 65544; 90868 to 125000 |
| SET | 16 | 362976 | 665779 | 1.83x | 2.007 | 0.847 | 3.839 | 4.679 | 332668 to 362976; 499002 to 666222 |
| GET | 16 | 570451 | 1333333 | 2.34x | 1.143 | 0.311 | 2.007 | 1.255 | 570451 to 666222; 999001 to 1333333 |
| INCR | 16 | 399361 | 666222 | 1.67x | 1.679 | 0.903 | 3.383 | 5.263 | 399202 to 399680; 570776 to 666222 |
| LPUSH | 16 | 266099 | 665779 | 2.50x | 2.815 | 0.623 | 5.399 | 4.903 | 249377 to 266170; 570776 to 666222 |

### 3. 1 CPU each, 1,000,000 requests, median of 3 (Ember with `GOMAXPROCS=1`)

Median of 3 runs. rps is requests per second; latencies are milliseconds.

| Test | -P | Redis rps | Ember rps | Ember / Redis | Redis p50 | Ember p50 | Redis p99 | Ember p99 | rps range (Redis; Ember) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| SET | 1 | 68946 | 53308 | 0.77x | 0.687 | 0.887 | 1.295 | 2.127 | 68937 to 68946; 50607 to 53325 |
| GET | 1 | 76894 | 63464 | 0.83x | 0.615 | 0.751 | 1.175 | 1.687 | 75420 to 76905; 62477 to 63480 |
| INCR | 1 | 70161 | 52607 | 0.75x | 0.663 | 0.895 | 1.263 | 2.151 | 70151 to 77706; 51594 to 53308 |
| LPUSH | 1 | 64491 | 57941 | 0.90x | 0.719 | 0.823 | 1.423 | 1.871 | 63436 to 65548; 57113 to 57954 |
| SET | 16 | 362976 | 363240 | 1.00x | 2.031 | 1.959 | 3.879 | 8.023 | 362976 to 363108; 363108 to 363240 |
| GET | 16 | 570776 | 570776 | 1.00x | 1.135 | 1.287 | 1.975 | 2.735 | 570776 to 571102; 570776 to 571102 |
| INCR | 16 | 399202 | 332890 | 0.83x | 1.703 | 2.055 | 3.559 | 9.599 | 399202 to 399361; 307409 to 333000 |
| LPUSH | 16 | 266028 | 499500 | 1.88x | 2.775 | 1.303 | 5.319 | 7.399 | 249439 to 266170; 499251 to 499750 |

At depth 16, `SET` and `GET` landed in the same 250 ms tick for both servers,
so "1.00x" there means "within about 9%".

### 4. 2 CPUs each, Redis with `--io-threads 2`, 1,000,000 requests, median of 3

Median of 3 runs. rps is requests per second; latencies are milliseconds.

| Test | -P | Redis rps | Ember rps | Ember / Redis | Redis p50 | Ember p50 | Redis p99 | Ember p99 | rps range (Redis; Ember) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| SET | 1 | 121183 | 121212 | 1.00x | 0.319 | 0.335 | 0.895 | 1.239 | 121153 to 124969; 117633 to 121227 |
| GET | 1 | 148104 | 159949 | 1.08x | 0.263 | 0.247 | 0.767 | 0.863 | 148104 to 148192; 153822 to 159974 |
| INCR | 1 | 133280 | 117675 | 0.88x | 0.295 | 0.343 | 0.871 | 1.255 | 128982 to 133298; 117619 to 121197 |
| LPUSH | 1 | 111099 | 121197 | 1.09x | 0.351 | 0.327 | 1.039 | 1.239 | 108050 to 114273; 121183 to 124984 |
| SET | 16 | 399042 | 666222 | 1.67x | 1.727 | 0.855 | 3.279 | 4.735 | 398883 to 399202; 666222 to 667111 |
| GET | 16 | 665779 | 1331558 | 2.00x | 0.943 | 0.311 | 1.927 | 1.263 | 665779 to 665779; 999001 to 1331558 |
| INCR | 16 | 444050 | 665779 | 1.50x | 1.543 | 0.887 | 3.103 | 5.407 | 443459 to 499251; 665779 to 666222 |
| LPUSH | 16 | 284981 | 665779 | 2.34x | 2.671 | 0.791 | 4.559 | 4.967 | 266099 to 285144; 665779 to 666222 |

### 5. Memory

One million `SET`s with random keys from a space of one million (so about 632,000
distinct keys), 64-byte values, measured with `docker stats` after the load settled
(`bench/results/memory.txt`):

| | Idle | After load | Per key (approx.) |
|---|---:|---:|---:|
| Redis 7.4 | 3.6 MiB | **100.6 MiB** | 167 bytes |
| Ember | 1.6 MiB | **216.1 MiB** | 358 bytes |

Ember uses about **2.1 times** the memory.

### 6. What durability costs (`appendfsync`), `SET` only, 2 CPUs, median of 3, 200,000 requests

| appendfsync | -P | Redis SET rps | Ember SET rps | Ember / Redis | Redis p99 ms | Ember p99 ms |
|---|---:|---:|---:|---:|---:|---:|
| always | 1 | 20,469 | 15,359 | 0.75x | 4.599 | 7.615 |
| always | 16 | 133,067 | 160,000 | 1.20x | 8.647 | 7.183 |
| everysec | 1 | 66,622 | 114,220 | 1.71x | 1.295 | 1.287 |
| everysec | 16 | 266,312 | 399,202 | 1.50x | 4.071 | 4.223 |
| no | 1 | 66,622 | 114,220 | 1.71x | 1.335 | 1.183 |
| no | 16 | 266,312 | 399,202 | 1.50x | 3.983 | 4.503 |

Reading it:

* `everysec` and `no` are indistinguishable. Fsyncing once a second in the
  background costs nothing visible.
* `always` is the expensive one: at depth 1, Ember falls from 114k to 15k
  (7.4x), Redis from 67k to 20k (3.3x). Every acknowledged write waits for a disk
  flush; with 50 concurrent clients, about 3 ms per round.
* At depth 1 under `always` Ember is **25% slower than Redis** (it is 71% faster
  under `everysec`). I have not profiled this. My suspicion: Redis does one
  `fsync` per event-loop pass for every client served in it, while Ember's
  leader/follower group commit makes a write that arrives just after a leader
  started wait for that fsync *and* then pay for the next one, and wakes all 50
  waiters with a condition-variable broadcast. A candidate for future work, not a
  conclusion.
* Rates above about 250,000 per second in this table are at the timer limit.

## Why the numbers differ

**1. Cores, not cleverness.** Redis executes commands on one thread (this is the
default and what ran). Ember runs a goroutine per connection and the Go
scheduler spreads them over `GOMAXPROCS` threads, so given 2 CPUs it uses both.
Table 2 versus table 3 is the evidence: with 2 CPUs Ember leads by 1.3x to 2.5x;
pinned to 1 CPU it trails at depth 1 and ties at depth 16. Table 4 shows that
giving Redis 2 I/O threads closes the depth-1 gap.

**2. At depth 1, the kernel dominates.** Each request is one `read` and one
`write` system call plus a trip through Docker's virtual network. Both servers
spend most of their per-request time there, which is why the per-core numbers
are within 10 to 25% of each other (Redis about 69,000 and Ember about 53,000 for
`SET` on one CPU). Redis's loop is a tight `epoll` and a single thread; Ember
pays extra for the Go runtime's network poller, goroutine wake-ups and the
mutex/condition work around the AOF. That is the 10 to 25% per-core gap.

**3. At depth 16, system calls are shared by 16 commands**, so what is left is
parsing and executing. Ember's loopback numbers (about 120 to 260 ns per command
through the whole stack on Linux) are small, and on one CPU the two servers
finish in the same time tick. Ahead of Redis at 2 CPUs again because of
point 1.

**4. Ember does much less work per command.** There is no ACL check, client
tracking, slow log, latency monitor, replication feed, keyspace notification,
LRU/LFU clock update, or the Redis object model with its encodings. Some of
Ember's apparent speed is the absence of features. This benchmark does not show
what Ember would cost with them.

**5. Latency shape.** Ember's median is lower in the 2-CPU runs (it answers each
connection from its own goroutine without queueing behind other clients), but its
99th percentile at depth 16 is worse than Redis's for SET and INCR (by 1.2x to
1.6x in table 2, for example INCR 5.3 ms against 3.4 ms, and up to 2.7x on one
CPU in table 3), while for GET and LPUSH it is better. The likely causes are Go's garbage collector and the
scheduler moving work between threads; Redis has no collector and one thread. I
did not isolate which.

**6. Memory.** Per key, Ember keeps a map slot, the key string (header plus bytes),
an `Entry` struct (about 48 bytes), and the value in its own allocation rounded up
to an allocator size class. Redis packs key, value and metadata into compact
encodings in jemalloc. On top of that Go's collector lets the heap grow to roughly
twice the live data before collecting (`GOGC=100`), which shows up directly in the
container's memory.

**7. A single list key flatters `LPUSH`.** The benchmark pushes to one list.
Ember's list is a plain ring buffer; Redis's is a quicklist with its own
overhead. The large `LPUSH` ratio (up to 2.5x) is about that structure, not about
the server loop.

**8. The environment is not bare metal.** Everything runs through Docker
Desktop's WSL2 VM and a virtual bridge. Absolute rates are far below what
`redis-benchmark` reports on a quiet Linux host, and a network hop that costs
tens of microseconds hides CPU differences at depth 1.

## The first run measured a bug, not Ember

My first full run (`bench/results/run1-go124-gomaxprocs-unset.csv`, Go 1.24)
showed Ember with a *better* median than Redis but a **99th percentile of about 66 ms**
and `SET` at depth 1 at only 28,500 requests per second against Redis's 66,600.
A tail that regular looked like a cause, not noise.

Median of 5 runs. rps is requests per second; latencies are milliseconds.

| Test | -P | Redis rps | Ember rps | Ember / Redis | Redis p50 | Ember p50 | Redis p99 | Ember p99 | rps range (Redis; Ember) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| SET | 1 | 66578 | 28506 | 0.43x | 0.695 | 0.311 | 1.335 | 66.751 | 57078 to 66622; 21008 to 30713 |
| GET | 1 | 66622 | 30722 | 0.46x | 0.631 | 0.375 | 1.175 | 58.527 | 66578 to 66667; 26631 to 44385 |
| INCR | 1 | 66622 | 23469 | 0.35x | 0.679 | 0.399 | 1.327 | 68.031 | 66622 to 66622; 22158 to 39952 |
| LPUSH | 1 | 57110 | 19952 | 0.35x | 0.735 | 0.527 | 1.527 | 66.431 | 57078 to 66622; 18149 to 24944 |
| SET | 16 | 199601 | 132979 | 0.67x | 1.991 | 1.111 | 3.479 | 69.375 | 199203 to 398406; 132802 to 199601 |
| GET | 16 | 398406 | 398406 | 1.00x | 1.159 | 0.431 | 1.943 | 2.207 | 398406 to 400000; 199601 to 398406 |
| INCR | 16 | 199601 | 132979 | 0.67x | 1.847 | 1.103 | 3.847 | 69.695 | 199203 to 398406; 99701 to 199203 |
| LPUSH | 16 | 199601 | 198807 | 1.00x | 2.831 | 0.895 | 5.335 | 58.527 | 199203 to 199601; 131926 to 199203 |

Hypothesis: Go 1.24 sizes `GOMAXPROCS` from the host's CPU count, so a container
limited to 2 CPUs ran 16 busy threads. Docker enforces the limit with a CPU
quota per 100 ms period; once the threads together used 200 ms of CPU, the
kernel froze the whole container until the next period. Experiment (Ember alone,
2-CPU container, median of 3, `bench/results/gomaxprocs-experiment.txt`):

| Variant | `SET` depth 1 | p99 | max |
|---|---:|---:|---:|
| Go 1.24, default | 23,485 rps | **67.5 ms** | 75 ms |
| Go 1.24, `GOMAXPROCS=2` | 100,000 rps | 1.2 ms | 12 ms |
| Go 1.25, default | 99,900 rps | 1.1 ms | 6.5 ms |

Confirmed, so the project moved to Go 1.25, whose runtime reads the container's
CPU limit.

**It was not fully fixed.** My first 1-CPU run (kept as
`ember-vs-redis-1cpu-1m-gomaxprocs-floor2.csv`) had Ember at mostly 0.4x to 0.5x of Redis
with a p99 of 35 to 57 ms, the same signature again:

Median of 3 runs. rps is requests per second; latencies are milliseconds.

| Test | -P | Redis rps | Ember rps | Ember / Redis | Redis p50 | Ember p50 | Redis p99 | Ember p99 | rps range (Redis; Ember) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| SET | 1 | 68942 | 26989 | 0.39x | 0.687 | 0.703 | 1.279 | 39.903 | 68937 to 70151; 26097 to 27547 |
| GET | 1 | 76899 | 36645 | 0.48x | 0.623 | 0.495 | 1.159 | 35.551 | 75443 to 76911; 32738 to 39156 |
| INCR | 1 | 70161 | 26447 | 0.38x | 0.671 | 0.719 | 1.255 | 40.095 | 70151 to 71408; 26101 to 27167 |
| LPUSH | 1 | 64487 | 26802 | 0.42x | 0.735 | 0.703 | 1.359 | 40.095 | 63460 to 65552; 26626 to 28325 |
| SET | 16 | 362976 | 181488 | 0.50x | 1.991 | 1.527 | 3.647 | 56.927 | 362582 to 363108; 181455 to 210128 |
| GET | 16 | 571429 | 234797 | 0.41x | 1.143 | 0.983 | 1.959 | 47.519 | 570451 to 665779; 234742 to 285388 |
| INCR | 16 | 399042 | 210172 | 0.53x | 1.695 | 1.311 | 3.487 | 57.503 | 399042 to 399521; 190150 to 210261 |
| LPUSH | 16 | 266028 | 266383 | 1.00x | 2.783 | 1.127 | 5.343 | 48.575 | 249439 to 266170; 221828 to 285225 |

Reading the container's `cpu.stat` showed why: Go 1.25 never sets `GOMAXPROCS`
below 2, so with a 1-CPU quota Ember ran 2 busy threads and was throttled in
1,425 of 1,547 periods (265 seconds of throttle time). Setting `GOMAXPROCS=1`
cut that to 85 of 826 periods, p99 from 37 ms to 2 ms, and `SET` at depth 1 from
28k to 55k requests per second.

| 1-CPU container, 300,000 requests | `SET` d=1 rps | p99 | throttled periods |
|---|---:|---:|---:|
| Go 1.25 default (`GOMAXPROCS`=2) | 27,860 | 37 ms | 1,425 of 1,547 |
| `GOMAXPROCS=1` | 54,526 | 2.0 ms | 85 of 826 |
| `GOMAXPROCS=1`, `GOGC=400` | 54,516 | 2.0 ms | 126 of 812 |

The compose file now sets `GOMAXPROCS` to the CPU limit for Ember, and the
README tells anyone running it with a CPU limit to do the same. A Redis
container needs no such setting because it is single-threaded.

The lesson I would carry to any Go service in a container: **set `GOMAXPROCS` to
the CPU limit and look at `cpu.stat`'s `nr_throttled` before trusting a tail
latency.**

## Go micro-benchmarks

`go test -run '^$' -bench . -benchmem -count 5 ./internal/...` on the development
machine (Windows, 16 threads), median of 5 (`bench/results/after.txt`, table made
by `bench/microtable.ps1`). Ordinary (non-pipelined) benchmarks measure one
goroutine; the `Server` ones drive 50 connections over loopback TCP.

| Package | Benchmark | ns/op | B/op | allocs/op |
|---|---|---:|---:|---:|
| resp | ReadCommandGET | 100.2 | 0 | 0 |
| resp | ReadCommandSET | 132.9 | 0 | 0 |
| resp | ReadCommandPipeline16 | 125.9 | 0 | 0 |
| resp | ReadCommandInline | 214.0 | 176 | 5 |
| resp | ReadValueArray | 286.5 | 307 | 4 |
| resp | WriteReplies | 62.0 | 0 | 0 |
| store | StoreGet | 56.5 | 0 | 0 |
| store | StoreSet | 155.2 | 96 | 2 |
| store | Contention/shards=1 | 375.6 | 9 | 0 |
| store | Contention/shards=16 | 68.5 | 9 | 0 |
| store | Contention/shards=256 | 21.9 | 9 | 0 |
| store | SyncMapContention | 16.1 | 11 | 0 |
| store | MutexMapContention | 186.5 | 4 | 0 |
| command | ExecuteSET | 217.5 | 176 | 4 |
| command | ExecuteGET | 85.9 | 0 | 0 |
| command | ExecuteINCR | 179.9 | 80 | 3 |
| command | ExecuteLPUSH | 170.0 | 119 | 3 |
| command | ExecutePING | 42.8 | 0 | 0 |
| server | ServerSET_P1 | 4,680.0 | 176 | 4 |
| server | ServerSET_P16 | 412.4 | 176 | 4 |
| server | ServerGET_P16 | 303.4 | 0 | 0 |

Notes: `Contention/shards=N` is a parallel 90% read / 10% write mix (see
[DESIGN.md](DESIGN.md#4-the-keyspace-why-a-sharded-rwmutex) for what it shows);
`ServerSET_P1` is dominated by Windows system calls, which is why the same
benchmark is 2.7x faster on Linux.

## Profile-driven optimisation: the parser

**Measure first.** The pipelined `SET` benchmark (50 connections, depth 16) did
11 allocations and 346 bytes per command. A CPU profile on Linux
(`bench/profile.sh`, Go 1.24 toolchain) put `runtime.mallocgc` at **17%** of CPU
time and `resp.(*Reader).ReadCommand` at **42%**, of which `readBulkBody`, which
allocates a new byte slice for every argument, was 11%. An exact allocation
profile (`-memprofilerate=1`, taken on Windows) attributed these allocations to
Ember's code, per `SET` (the benchmark reports 11 per command; I did not
reconcile the last two or three, which the profile does not pin to a line):

| Allocations per `SET` before | Where |
|---|---|
| 3 | `readBulkBody`: one slice per argument (`SET`, key, value) |
| 1 | `ReadCommand`: the argument list |
| 1 | `bytes.Clone`: the value stored in the keyspace (needed) |
| 1 | `store.Update`: the per-call `Tx` handle |
| 1 | `cmdSet`: the key as a string (needed for the map lookup) |
| 1 | `cmdSet.func1`: the new `Entry` (needed) |

**Change.** `ReadCommand` now keeps one reusable argument arena and one reusable
argument slice per connection and parses lengths with an allocation-free integer
parser. The contract changed accordingly: arguments are valid only until the
next `ReadCommand`. I audited every handler (anything stored is already cloned;
the AOF copies bytes into its buffer immediately), and added a test that pins the
result: `TestReadCommandDoesNotAllocate` fails if steady-state parsing allocates.
A first version still allocated three times per command: a two-byte array used
to check the CRLF terminator escaped to the heap through `io.ReadFull`, and that
test caught it.

**Result.** Same benchmarks, same Go 1.25 toolchain, Linux, 5 runs each,
before = commit `a1dcb95`, after = commit `7fb5c03` (plus the Go 1.25 version bump) (`bench/results/linux-benchstat.txt`):

| Benchmark | Before | After | Change |
|---|---:|---:|---:|
| Parse `GET` | 215 ns, 5 allocs, 72 B | 101 ns, 0 allocs, 0 B | -53% |
| Parse `SET` | 303 ns, 7 allocs, 170 B | 132 ns, 0 allocs, 0 B | -57% |
| Parse 16 pipelined `SET`s | 296 ns, 7 allocs | 122 ns, 0 allocs | -59% |
| Server `SET`, depth 16 (50 conns) | 346 ns, 11 allocs, 346 B | 263 ns, 4 allocs, 176 B | -24% time, **+31% cmds/s** |
| Server `GET`, depth 16 (50 conns) | 177 ns, 5 allocs, 72 B | 120 ns, 0 allocs, 0 B | -32% time, **+48% cmds/s** |
| Server `SET`, depth 1 | 1.85 us | 1.72 us | -7% |
| Inline command parse (telnet) | 245 ns | 249 ns | no change (not optimised) |

On Windows the same change gave -10.6% (`SET`) and -11.8% (`GET`) at depth 16
(`bench/results/benchstat.txt`): there system calls take a larger share, so
removing allocations helps less. Depth 1 barely moves anywhere because a round
trip costs microseconds in the kernel and the saving is tens of nanoseconds.

**What is left.** `SET` still allocates 4 times: the `Tx`, the value clone, the
key string and the `Entry`. The `Tx` could live in the shard, and the key
lookup could use `m[string(b)]` without allocating. Each is a small, separate,
measurable change; I stopped at one, as planned.

## Reproducing

```powershell
go test -run '^$' -bench . -benchmem -count 5 ./internal/... > new.txt
go run golang.org/x/perf/cmd/benchstat@latest bench\results\after.txt new.txt
docker run --rm -v "${PWD}:/src" -v ember-gocache:/go -w /src golang:1.25 sh bench/profile.sh
```

Caveats worth repeating: one machine, one Docker network, three to five repeats
per cell (ranges are in the CSVs and the table), 250 ms timer resolution in the
load generator, and Redis left at its defaults except for the AOF setting.
