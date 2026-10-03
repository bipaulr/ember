# Measures what durability costs: SET throughput for Ember and Redis under each
# AOF fsync policy (always, everysec, no), at pipeline depths 1 and 16.
# Writes bench\results\fsync.csv and prints a markdown table of medians.
#
#   .\bench\fsync.ps1                 (3 runs, 100000 requests, 50 clients)
param(
    [int]$Repeat = 3,
    [int]$Requests = 100000,
    [int]$Clients = 50,
    [int]$Threads = 4
)
# See run.ps1: native stderr must not become a terminating error in PowerShell 5.1.
$ErrorActionPreference = 'Continue'
Set-Location $PSScriptRoot
$redisImage = 'redis:7-alpine@sha256:858f009f9709ce576febc734aa78b8f6d624b82571f9ddb6bda4377c833b3499'
$net = 'ember-fsync'
$benchDir = (Get-Location).Path

function Remove-All {
    docker rm -f fs-redis fs-ember *> $null
    docker network rm $net *> $null
}

function Invoke-Client {
    docker run --rm --network $net -e TESTS=set -v "${benchDir}:/bench:ro" $redisImage @args
}

function Wait-Ready([string]$h, [string]$p) {
    for ($i = 0; $i -lt 30; $i++) {
        $r = Invoke-Client redis-cli -h $h -p $p ping 2>$null
        if ($r -match 'PONG') { return }
        Start-Sleep -Seconds 1
    }
    throw "$h did not come up"
}

New-Item -ItemType Directory -Force results | Out-Null
Remove-All
docker build -t ember:bench .. *> $null
if ($LASTEXITCODE -ne 0) { throw 'image build failed' }
docker network create $net *> $null

$out = 'results\fsync.csv'
'policy,run,server,pipeline,test,rps,avg_ms,min_ms,p50_ms,p95_ms,p99_ms,max_ms' | Set-Content -Encoding ascii $out
try {
    foreach ($policy in 'always', 'everysec', 'no') {
        Write-Host "== appendfsync $policy"
        docker run -d --name fs-redis --network $net --cpus 2 --memory 1g $redisImage `
            redis-server --port 6379 --appendonly yes --appendfsync $policy --save '""' *> $null
        docker run -d --name fs-ember --network $net --cpus 2 --memory 1g ember:bench `
            --host 0.0.0.0 --port 6380 --aof-path /data/ember.aof --appendfsync $policy *> $null
        Wait-Ready 'fs-redis' '6379'
        Wait-Ready 'fs-ember' '6380'
        for ($run = 1; $run -le $Repeat; $run++) {
            foreach ($pipeline in 1, 16) {
                foreach ($t in @(@('fs-redis', 'redis', 6379), @('fs-ember', 'ember', 6380))) {
                    Write-Host "   run $run/$Repeat -P $pipeline $($t[1])"
                    $rows = Invoke-Client sh /bench/inner.sh $t[1] $t[0] $t[2] $pipeline $Requests $Clients $Threads
                    $rows | Where-Object { $_ -match ',' } | ForEach-Object { "$policy,$run,$_" } | Add-Content -Encoding ascii $out
                }
            }
        }
        docker rm -f fs-redis fs-ember *> $null
    }
}
finally {
    Remove-All
}

$data = Import-Csv $out
function Get-Median($values) {
    $s = @($values | ForEach-Object { [double]$_ } | Sort-Object)
    if ($s.Count -eq 0) { return 0 }
    if ($s.Count % 2) { $s[[int](($s.Count - 1) / 2)] } else { ($s[$s.Count / 2 - 1] + $s[$s.Count / 2]) / 2 }
}
Write-Host ""
Write-Host '| appendfsync | -P | Redis SET rps | Ember SET rps | Ember / Redis | Redis p99 ms | Ember p99 ms |'
Write-Host '|---|---:|---:|---:|---:|---:|---:|'
foreach ($policy in 'always', 'everysec', 'no') {
    foreach ($p in '1', '16') {
        $r = $data | Where-Object { $_.policy -eq $policy -and $_.pipeline -eq $p -and $_.server -eq 'redis' }
        $e = $data | Where-Object { $_.policy -eq $policy -and $_.pipeline -eq $p -and $_.server -eq 'ember' }
        $rr = Get-Median ($r.rps); $er = Get-Median ($e.rps)
        '| {0} | {1} | {2:N0} | {3:N0} | {4:N2}x | {5:N3} | {6:N3} |' -f $policy, $p, $rr, $er, ($er / $rr), (Get-Median ($r.p99_ms)), (Get-Median ($e.p99_ms))
    }
}
