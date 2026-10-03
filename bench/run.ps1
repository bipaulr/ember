# Benchmarks Ember against Redis 7 with redis-benchmark, from a container on the
# same Docker network. Writes bench\results\ember-vs-redis.csv and prints a
# markdown summary.
#
#   .\bench\run.ps1                       (defaults: 5 runs, 100000 requests, 50 clients)
#   .\bench\run.ps1 -Repeat 1 -Requests 20000
param(
    [int]$Repeat = 5,
    [int]$Requests = 100000,
    [int]$Clients = 50,
    [int]$Threads = 4,
    [int]$Cpus = 2,
    [int]$RedisIoThreads = 1,   # >1 enables Redis threaded I/O for the comparison
    [string]$Tag = ''      # appended to result file names, e.g. -1m for a long run
)
$env:CPUS = "$Cpus"
$env:REDIS_IO_THREADS = "$RedisIoThreads"
$env:REDIS_IO_READS = if ($RedisIoThreads -gt 1) { 'yes' } else { 'no' }
$suffix = $(if ($Cpus -eq 2) { '' } else { "-${Cpus}cpu" }) + $Tag
# Windows PowerShell 5.1 turns anything a native command writes to stderr (Docker
# prints build progress there) into an error under 'Stop', so exit codes are
# checked explicitly instead.
$ErrorActionPreference = 'Continue'
Set-Location $PSScriptRoot
$compose = @('compose', '-f', 'docker-compose.yml')

function Invoke-Client {
    docker @compose run --rm --no-deps -T bench @args
}
function Stop-Stack {
    docker @compose down -v --remove-orphans *> $null
}

New-Item -ItemType Directory -Force results | Out-Null
Stop-Stack
try {
    docker @compose up -d --build redis ember
    if ($LASTEXITCODE -ne 0) { throw 'docker compose up failed' }

    foreach ($target in 'redis:6379', 'ember:6380') {
        $h, $p = $target.Split(':')
        $ok = $false
        for ($i = 0; $i -lt 60 -and -not $ok; $i++) {
            $reply = Invoke-Client redis-cli -h $h -p $p ping 2>$null
            if ($reply -match 'PONG') { $ok = $true } else { Start-Sleep -Seconds 1 }
        }
        if (-not $ok) { throw "$h did not come up" }
    }

    $cpu = (Get-CimInstance Win32_Processor | Select-Object -First 1).Name
    $logical = (Get-CimInstance Win32_Processor | Measure-Object NumberOfLogicalProcessors -Sum).Sum
    $ramGb = [math]::Round((Get-CimInstance Win32_ComputerSystem).TotalPhysicalMemory / 1GB, 1)
    $env = @(
        "date: $((Get-Date).ToUniversalTime().ToString('s'))Z",
        "host: $((Get-CimInstance Win32_OperatingSystem).Caption) ($cpu, $logical logical CPUs, $ramGb GB RAM)",
        "docker: $(docker version --format '{{.Server.Version}}')",
        "docker vm: $(docker info --format 'cpus: {{.NCPU}}  memory: {{.MemTotal}}  os: {{.OperatingSystem}}')",
        "$((Invoke-Client redis-server --version) -join ' ')",
        "limits: cpus=$Cpus mem=1g per server; redis io-threads=$RedisIoThreads; Repeat=$Repeat Requests=$Requests Clients=$Clients Threads=$Threads"
    )
    $env | Set-Content -Encoding ascii "results\environment$suffix.txt"

    $out = "results\ember-vs-redis$suffix.csv"
    'run,server,pipeline,test,rps,avg_ms,min_ms,p50_ms,p95_ms,p99_ms,max_ms' | Set-Content -Encoding ascii $out
    for ($run = 1; $run -le $Repeat; $run++) {
        foreach ($pipeline in 1, 16) {
            # Alternate which server goes first so neither always benefits from a warm host.
            $order = if ($run % 2 -eq 1) { 'redis:6379', 'ember:6380' } else { 'ember:6380', 'redis:6379' }
            foreach ($target in $order) {
                $h, $p = $target.Split(':')
                Write-Host "run $run/$Repeat  -P $pipeline  $h"
                $rows = Invoke-Client sh /bench/inner.sh $h $h $p $pipeline $Requests $Clients $Threads
                if ($LASTEXITCODE -ne 0) { throw "benchmark failed for $h" }
                $rows | Where-Object { $_ -match ',' } | ForEach-Object { "$run,$_" } | Add-Content -Encoding ascii $out
            }
        }
    }
}
finally {
    Stop-Stack
}

if (Get-Command go -ErrorAction SilentlyContinue) {
    Push-Location ..
    go run ./bench/summarize "bench/results/ember-vs-redis$suffix.csv"
    Pop-Location
} else {
    Write-Host "Install Go to print the summary table: go run ./bench/summarize bench/results/ember-vs-redis$suffix.csv"
}
