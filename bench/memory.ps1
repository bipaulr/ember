# Compares memory use after loading one million keys into Ember and Redis, each
# in a container limited to 2 CPUs and 1 GB. Reads container memory from
# "docker stats". Writes bench\results\memory.txt.
#
#   .\bench\memory.ps1
param([int]$Keys = 1000000, [int]$ValueBytes = 64)
$ErrorActionPreference = 'Continue'
Set-Location $PSScriptRoot
$redisImage = 'redis:7-alpine@sha256:858f009f9709ce576febc734aa78b8f6d624b82571f9ddb6bda4377c833b3499'
$net = 'ember-mem'
function Remove-All { docker rm -f mem-redis mem-ember *> $null; docker network rm $net *> $null }

Remove-All
docker build -t ember:bench .. *> $null
if ($LASTEXITCODE -ne 0) { throw 'image build failed' }
docker network create $net *> $null
$lines = @()
try {
    docker run -d --name mem-redis --network $net --cpus 2 --memory 1g $redisImage `
        redis-server --port 6379 --appendonly yes --appendfsync everysec --save '""' *> $null
    docker run -d --name mem-ember --network $net --cpus 2 --memory 1g -e GOMAXPROCS=2 ember:bench `
        --host 0.0.0.0 --port 6380 --aof-path /data/ember.aof --appendfsync everysec *> $null
    Start-Sleep -Seconds 3
    $idle = docker stats --no-stream --format '{{.Name}} {{.MemUsage}}' mem-redis mem-ember
    foreach ($t in @(@('mem-redis', 6379), @('mem-ember', 6380))) {
        docker run --rm --network $net $redisImage redis-benchmark -h $t[0] -p $t[1] -c 50 -P 16 -r $Keys -d $ValueBytes -t set -n $Keys -q *> $null
    }
    Start-Sleep -Seconds 8   # let background work and the garbage collector settle
    $size = @{}
    foreach ($t in @(@('mem-redis', 6379), @('mem-ember', 6380))) {
        $size[$t[0]] = (docker run --rm --network $net $redisImage redis-cli -h $t[0] -p $t[1] dbsize)
    }
    $loaded = docker stats --no-stream --format '{{.Name}} {{.MemUsage}}' mem-redis mem-ember
    $lines += "keys loaded: redis=$($size['mem-redis']) ember=$($size['mem-ember']) (value size $ValueBytes bytes, random key space $Keys)"
    $lines += 'idle:   ' + ($idle -join ' | ')
    $lines += 'loaded: ' + ($loaded -join ' | ')
}
finally { Remove-All }
$lines | Set-Content -Encoding ascii results\memory.txt
$lines | ForEach-Object { Write-Host $_ }
