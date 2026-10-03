# Differential test: sends the same error-provoking and edge-case commands to a
# real Redis 7 and to Ember (both in Docker) and diffs the replies.
#
#   .\scripts\compat-diff.ps1
$ErrorActionPreference = 'Continue'
Set-Location (Split-Path -Parent $PSScriptRoot)
$repo = (Get-Location).Path
$net = 'ember-compat'
$redisImage = 'redis:7-alpine@sha256:858f009f9709ce576febc734aa78b8f6d624b82571f9ddb6bda4377c833b3499'

docker rm -f compat-redis compat-ember *> $null
docker network rm $net *> $null
docker build -t ember:compat . *> $null
if ($LASTEXITCODE -ne 0) { throw 'image build failed' }
docker network create $net *> $null
try {
    docker run -d --name compat-redis --network $net $redisImage redis-server --port 6379 --save '""' *> $null
    docker run -d --name compat-ember --network $net ember:compat *> $null
    Start-Sleep -Seconds 3
    docker run --rm --network $net -v "${repo}\test\cli:/cli:ro" $redisImage sh /cli/compat.sh compat-redis 6379 compat-ember 6380
}
finally {
    docker rm -f compat-redis compat-ember *> $null
    docker network rm $net *> $null
}
