#!/bin/sh
# CPU and allocation profile of the pipelined SET benchmark (run in a Linux
# container, where the profile is not dominated by Windows syscall waits).
#
#   docker run --rm -v "$PWD:/src" -v ember-gocache:/go -w /src golang:1.25 sh bench/profile.sh
set -e
out=/tmp/prof
mkdir -p "$out"
go test -run '^$' -bench 'ServerSET_P16' -benchtime 6s -cpuprofile "$out/cpu.prof" -o "$out/server.test" ./internal/server | grep Benchmark
echo
echo "== CPU: top by cumulative time =="
go tool pprof -top -cum -nodecount=18 "$out/server.test" "$out/cpu.prof" 2>/dev/null | sed -n '4,30p'
echo
echo "== CPU: allocation share =="
go tool pprof -top -cum -nodecount=400 "$out/server.test" "$out/cpu.prof" 2>/dev/null | grep -E 'runtime.mallocgc |ReadCommand|readBulkBody|Store..Update|Table..Execute' | head
echo
echo "== allocations by function (objects), exact =="
go test -run '^$' -bench 'ServerSET_P16' -benchtime 2s -memprofile "$out/mem.prof" -memprofilerate 1 -o "$out/server.test" ./internal/server | grep Benchmark
go tool pprof -sample_index=alloc_objects -top -nodecount=8 "$out/server.test" "$out/mem.prof" 2>/dev/null | sed -n '5,20p'
