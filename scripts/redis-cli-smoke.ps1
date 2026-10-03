# Starts Ember in Docker and drives it with the real redis-cli: every supported
# command (test/cli/commands.txt), then a SUBSCRIBE/PUBLISH round trip. The
# transcript is written to docs/transcript.txt. Fails if any command is reported
# as unknown.
#
#   .\scripts\redis-cli-smoke.ps1
$ErrorActionPreference = 'Continue'
Set-Location (Split-Path -Parent $PSScriptRoot)
$repo = (Get-Location).Path
$net = 'ember-smoke'
$name = 'ember-smoke'

docker rm -f $name *> $null
docker network rm $net *> $null
docker build -t ember:smoke . *> $null
if ($LASTEXITCODE -ne 0) { throw 'image build failed' }
docker network create $net *> $null
try {
    docker run -d --name $name --network $net ember:smoke *> $null
    for ($i = 0; $i -lt 30; $i++) {
        if ((docker inspect --format '{{.State.Health.Status}}' $name) -eq 'healthy') { break }
        Start-Sleep -Seconds 1
    }

    # The container-side script is test/cli/smoke.sh (one redis-cli call per command,
    # so the transcript shows each command next to its reply).
    $out = docker run --rm --network $net -v "${repo}\test\cli:/cli:ro" redis:7-alpine sh /cli/smoke.sh
    $out | Set-Content -Encoding utf8 docs\transcript.txt
    $out | ForEach-Object { Write-Host $_ }

    $unknown = $out | Where-Object { $_ -match "unknown command" -and $_ -notmatch "FROBNICATE" }
    $count = ($out | Where-Object { $_ -match '^127\.0\.0\.1:6380> ' }).Count
    Write-Host ""
    Write-Host "commands run: $count"
    if ($unknown) { Write-Host "UNEXPECTED unknown-command replies:"; $unknown; exit 1 }
    Write-Host 'PASS: no supported command was rejected as unknown'
}
finally {
    docker rm -f $name *> $null
    docker network rm $net *> $null
}
