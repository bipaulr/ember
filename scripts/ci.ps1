# Runs the same checks as .github/workflows/ci.yml, locally, before you push.
#
#   .\scripts\ci.ps1                 everything
#   .\scripts\ci.ps1 -SkipDocker     skip the image build and smoke test
#   .\scripts\ci.ps1 -FuzzTime 10s   shorter fuzz run
#
# Requirements: Go 1.24+ and Docker. Lint and the race-detector tests run inside
# Linux containers, so no C compiler or golangci-lint install is needed on
# Windows. Keep the versions below in step with ci.yml.
param(
    [string]$FuzzTime = '30s',
    [switch]$SkipFuzz,
    [switch]$SkipDocker,
    [switch]$SkipLint
)

# Native commands write progress to stderr; exit codes are checked explicitly.
$ErrorActionPreference = 'Continue'
Set-Location (Split-Path -Parent $PSScriptRoot)

$GolangciLint = 'golangci/golangci-lint:v2.14.0'
$GoImage = 'golang:1.25'
$Repo = (Get-Location).Path
$results = [ordered]@{}

function Step([string]$name, [scriptblock]$body) {
    Write-Host ""
    Write-Host "==> $name" -ForegroundColor Cyan
    $global:LASTEXITCODE = 0
    $ok = $false
    try { $ok = [bool](& $body) } catch { Write-Host $_.Exception.Message -ForegroundColor Red }
    $results[$name] = $ok
    if ($ok) { Write-Host "    PASS" -ForegroundColor Green } else { Write-Host "    FAIL" -ForegroundColor Red }
}

Step 'gofmt' {
    $bad = gofmt -l .
    if ($bad) { Write-Host "needs formatting:`n$bad"; return $false }
    $true
}

Step 'go vet' {
    go vet ./...
    $LASTEXITCODE -eq 0
}

if (-not $SkipLint) {
    Step 'golangci-lint' {
        docker run --rm -v "${Repo}:/src" -v ember-gocache:/go -w /src $GolangciLint golangci-lint run --max-same-issues 0 ./...
        $LASTEXITCODE -eq 0
    }
}

Step 'go test -race (Linux container) with coverage' {
    $out = docker run --rm -v "${Repo}:/src" -v ember-gocache:/go -w /src $GoImage go test -race -count=1 -cover ./...
    $out | ForEach-Object { Write-Host $_ }
    if ($LASTEXITCODE -ne 0) { return $false }
    $ok = $true
    foreach ($line in $out) {
        if ($line -match 'ember/(internal/\S+)\s.*coverage: ([0-9.]+)%') {
            if ([double]$Matches[2] -lt 80) { Write-Host "coverage of $($Matches[1]) is $($Matches[2])%, below 80%" -ForegroundColor Red; $ok = $false }
        }
    }
    $ok
}

Step 'go test (native Windows)' {
    go test -count=1 ./...
    $LASTEXITCODE -eq 0
}

if (-not $SkipFuzz) {
    Step "fuzz FuzzParse ($FuzzTime)" {
        go test ./internal/resp -run '^$' -fuzz '^FuzzParse$' -fuzztime $FuzzTime
        $LASTEXITCODE -eq 0
    }
}

Step 'actionlint' {
    docker run --rm -v "${Repo}:/repo" -w /repo rhysd/actionlint:latest -color
    $LASTEXITCODE -eq 0
}

if (-not $SkipDocker) {
    Step 'docker build and smoke test' {
        docker build -t ember:ci .
        if ($LASTEXITCODE -ne 0) { return $false }
        $size = docker image inspect ember:ci --format '{{.Size}}'
        $user = docker image inspect ember:ci --format '{{.Config.User}}'
        Write-Host ("image size: {0:N1} MB, user: {1}" -f ([double]$size / 1MB), $user)
        if ($user -ne '65532:65532') { Write-Host 'image does not run as the non-root user'; return $false }

        docker network create ember-ci *> $null
        try {
            docker run -d --name ember-ci --network ember-ci ember:ci *> $null
            $health = ''
            for ($i = 0; $i -lt 30 -and $health -ne 'healthy'; $i++) {
                $health = docker inspect --format '{{.State.Health.Status}}' ember-ci
                if ($health -ne 'healthy') { Start-Sleep -Seconds 1 }
            }
            Write-Host "container health: $health"
            $cli = @('run', '--rm', '--network', 'ember-ci', 'redis:7-alpine', 'redis-cli', '-h', 'ember-ci', '-p', '6380')
            $pong = docker @cli ping
            docker @cli set smoke ok *> $null
            $got = docker @cli get smoke
            Write-Host "PING -> $pong   GET smoke -> $got"
            ($health -eq 'healthy') -and ($pong -eq 'PONG') -and ($got -eq 'ok')
        }
        finally {
            docker rm -f ember-ci *> $null
            docker network rm ember-ci *> $null
        }
    }
}

Write-Host ""
Write-Host "Summary" -ForegroundColor Cyan
$failed = 0
foreach ($k in $results.Keys) {
    if ($results[$k]) { Write-Host ("  PASS  {0}" -f $k) -ForegroundColor Green }
    else { Write-Host ("  FAIL  {0}" -f $k) -ForegroundColor Red; $failed++ }
}
exit $failed
