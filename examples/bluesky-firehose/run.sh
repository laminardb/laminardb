#!/usr/bin/env bash
# Start LaminarDB against the live Bluesky firehose (Linux / macOS).
#
#   ./run.sh
#
# Uses an existing release binary if present, otherwise builds one.
# Ctrl-C to stop. pgwire listens on 127.0.0.1:5432, HTTP on 127.0.0.1:7777.
set -euo pipefail
cd "$(dirname "$0")"

repo="$(cd ../.. && pwd)"
bin="$repo/target/release/laminardb"

if [[ ! -x "$bin" ]]; then
  echo "Building laminar-server (release)..."
  cargo build --release -p laminar-server
fi

echo "Starting LaminarDB -> Bluesky Jetstream firehose"
echo "  pgwire : 127.0.0.1:5432   (psql / libpq SUBSCRIBE)"
echo "  http   : 127.0.0.1:7777   (/api/v1/checkpoint, /healthz)"
echo
exec "$bin" --config "$PWD/laminar.toml" --log-level info
