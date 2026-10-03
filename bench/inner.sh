#!/bin/sh
# Runs inside the client container (see docker-compose.yml). For one server and
# one pipeline depth it runs SET, GET, INCR and LPUSH and prints CSV rows:
#   server,pipeline,test,rps,avg_ms,min_ms,p50_ms,p95_ms,p99_ms,max_ms
#
# usage: [TESTS="set get"] inner.sh NAME HOST PORT PIPELINE REQUESTS CLIENTS THREADS
set -eu
name=$1 host=$2 port=$3 pipeline=$4 requests=$5 clients=$6 threads=$7
keyspace=100000

bench() { redis-benchmark -h "$host" -p "$port" -c "$clients" --threads "$threads" -P "$pipeline" -r "$keyspace" "$@"; }

redis-cli -h "$host" -p "$port" flushall >/dev/null

# Warm up (heap growth, page faults, connection setup) identically for both servers.
bench -n 100000 -t set,get -q >/dev/null
redis-cli -h "$host" -p "$port" flushall >/dev/null

# TESTS can restrict the run, e.g. TESTS=set (used by fsync.ps1).
for t in ${TESTS:-set get incr lpush}; do
  if [ "$t" = get ]; then
    # Fill the keyspace first so GET mostly hits (about 99% of 100k keys).
    bench -n 500000 -t set -q >/dev/null
  fi
  # --csv prints a header line, then "TEST","rps","avg","min","p50","p95","p99","max".
  bench -n "$requests" -t "$t" --csv | tail -n 1 | tr -d '"' | sed "s/^[^,]*,/$name,$pipeline,$t,/"
done
