# Start LaminarDB against the live Bluesky firehose (Windows / PowerShell).
#
#   .\run.ps1
#
# Uses an existing release binary if present, otherwise builds one.
# Ctrl-C to stop. pgwire listens on 127.0.0.1:5432, HTTP on 127.0.0.1:7777.

$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

$repo = (Resolve-Path "$PSScriptRoot\..\..").Path
$bin  = Join-Path $repo "target\release\laminardb.exe"

if (-not (Test-Path $bin)) {
    Write-Host "Building laminar-server (release)..." -ForegroundColor Cyan
    & cargo build --release -p laminar-server
}

Write-Host "Starting LaminarDB -> Bluesky Jetstream firehose" -ForegroundColor Green
Write-Host "  pgwire : 127.0.0.1:5432   (psql / libpq SUBSCRIBE)"
Write-Host "  http   : 127.0.0.1:7777   (/api/v1/checkpoint, /healthz)"
Write-Host ""
& $bin --config (Join-Path $PSScriptRoot "laminar.toml") --log-level info
