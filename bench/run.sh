#!/usr/bin/env bash
# Benchmarks Ember against Redis 7 with redis-benchmark, from a container on the
# same Docker network. Writes bench/results/ember-vs-redis.csv and prints a
# markdown summary.
#
#   REPEAT=5 REQUESTS=100000 CLIENTS=50 THREADS=4 ./bench/run.sh
set -euo pipefail
cd "$(dirname "$0")"

REPEAT=${REPEAT:-5}
REQUESTS=${REQUESTS:-100000}
CLIENTS=${CLIENTS:-50}
THREADS=${THREADS:-4}
export CPUS=${CPUS:-2}
export REDIS_IO_THREADS=${REDIS_IO_THREADS:-1}
if [ "$REDIS_IO_THREADS" -gt 1 ]; then export REDIS_IO_READS=yes; else export REDIS_IO_READS=no; fi
suffix=$([ "$CPUS" = 2 ] && echo "" || echo "-${CPUS}cpu")${TAG:-}

dc="docker compose -f docker-compose.yml"
client() { $dc run --rm --no-deps -T bench "$@"; }
cleanup() { $dc down -v --remove-orphans >/dev/null 2>&1 || true; }
trap cleanup EXIT

mkdir -p results
cleanup
$dc up -d --build redis ember

for target in redis:6379 ember:6380; do
  host=${target%%:*}; port=${target##*:}
  for _ in $(seq 1 60); do
    if client redis-cli -h "$host" -p "$port" ping 2>/dev/null | grep -q PONG; then break; fi
    sleep 1
  done
  client redis-cli -h "$host" -p "$port" ping | grep -q PONG || { echo "$host did not come up" >&2; exit 1; }
done

{
  echo "date: $(date -u +%FT%TZ)"
  echo "uname: $(uname -a)"
  command -v lscpu >/dev/null && lscpu | grep -E 'Model name|^CPU\(s\)' || true
  docker version --format 'docker: {{.Server.Version}}'
  docker info --format 'docker cpus: {{.NCPU}}  memory: {{.MemTotal}}  os: {{.OperatingSystem}}'
  client redis-server --version
  echo "limits: cpus=$CPUS mem=1g per server; redis io-threads=$REDIS_IO_THREADS; REPEAT=$REPEAT REQUESTS=$REQUESTS CLIENTS=$CLIENTS THREADS=$THREADS"
} > "results/environment$suffix.txt"

out="results/ember-vs-redis$suffix.csv"
echo "run,server,pipeline,test,rps,avg_ms,min_ms,p50_ms,p95_ms,p99_ms,max_ms" > "$out"
for run in $(seq 1 "$REPEAT"); do
  for pipeline in 1 16; do
    # Alternate which server goes first so neither always benefits from a warm host.
    if (( run % 2 )); then order="redis:6379 ember:6380"; else order="ember:6380 redis:6379"; fi
    for target in $order; do
      host=${target%%:*}; port=${target##*:}
      echo "run $run/$REPEAT  -P $pipeline  $host" >&2
      client sh /bench/inner.sh "$host" "$host" "$port" "$pipeline" "$REQUESTS" "$CLIENTS" "$THREADS" \
        | sed "s/^/$run,/" >> "$out"
    done
  done
done

if command -v go >/dev/null; then
  (cd .. && go run ./bench/summarize bench/results/ember-vs-redis$suffix.csv)
else
  echo "Install Go to print the summary table: go run ./bench/summarize bench/results/ember-vs-redis$suffix.csv"
fi
