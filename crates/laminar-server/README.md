# laminar-server

Standalone server binary for LaminarDB. Reads a TOML configuration file, constructs streaming pipelines, and serves a REST API.

## Features

- **TOML configuration** with `${VAR}` and `${VAR:-default}` environment variable substitution
- **REST API** (Axum) for health checks, pipeline introspection, ad-hoc SQL, and manual checkpoints
- **Prometheus metrics** at `/metrics`
- **Hot reload** — edit the TOML file and changes are applied automatically (file watcher with debounce), or `POST /api/v1/reload`
- **Checkpoint validation** — `--validate-checkpoints` flag validates all stored checkpoints and exits
- **Platform allocators** — jemalloc on Linux, mimalloc on Windows MSVC
- **Docker and Helm** deployment with multi-arch images

## CLI

```
laminardb [OPTIONS]

Options:
  --config <FILE>         Configuration file [default: laminardb.toml]
  --log-level <LEVEL>     Logging level: trace, debug, info, warn, error [default: info]
  --admin-bind <ADDR>     Override HTTP bind address from config
  --validate-checkpoints  Validate stored checkpoints and exit
  -h, --help              Print help
  -V, --version           Print version
```

## Quick Start

```bash
# Build from source
cargo build --release --bin laminardb

# Run with a config file
./target/release/laminardb --config laminardb.toml

# Or install and run
cargo install laminar-server
laminardb --config laminardb.toml

# Docker
docker run -d -p 8080:8080 -v laminardb-data:/var/lib/laminardb \
  ghcr.io/laminardb/laminardb-server:latest

# Check health
curl http://localhost:8080/health
```

## Configuration

See the [Configuration Reference](https://laminardb.io/docs/) for every field, or the example below:

```toml
[server]
mode = "embedded"           # "embedded" (single-node) or "delta" (multi-node scaffolding, not production-hardened)
bind = "0.0.0.0:8080"       # HTTP API bind address
workers = 0                 # 0 = auto-detect CPU count
log_level = "info"

[state]
backend = "memory"          # "memory" or "mmap"
path = "./data/state"       # mmap state directory

[checkpoint]
url = "file:///tmp/laminardb/checkpoints"  # file://, s3://, gs://
interval = "30s"

[[source]]
name = "trades"
connector = "kafka"
format = "json"
[source.properties]
bootstrap.servers = "localhost:9092"
topic = "market-trades"
group.id = "laminar"
[[source.schema]]
name = "symbol"
type = "VARCHAR"
nullable = false
[[source.schema]]
name = "price"
type = "DOUBLE"
[[source.schema]]
name = "ts"
type = "TIMESTAMP"
[source.watermark]
column = "ts"
max_out_of_orderness = "5s"

[[pipeline]]
name = "avg_price"
sql = """
SELECT symbol, AVG(price) AS avg_price
FROM trades
GROUP BY symbol, TUMBLE(ts, INTERVAL '1' MINUTE)
EMIT ON WINDOW CLOSE
"""

[[sink]]
name = "output"
pipeline = "avg_price"
connector = "kafka"
delivery = "at_least_once"
[sink.properties]
bootstrap.servers = "localhost:9092"
topic = "avg-prices"
format = "json"
```

## REST API

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Liveness probe |
| GET | `/ready` | Readiness probe (pipelines started) |
| GET | `/metrics` | Prometheus text metrics |
| GET | `/api/v1/sources` | List configured sources |
| GET | `/api/v1/sinks` | List configured sinks |
| GET | `/api/v1/streams` | List running streams |
| GET | `/api/v1/streams/{name}` | Stream detail by name |
| POST | `/api/v1/checkpoint` | Trigger immediate checkpoint |
| POST | `/api/v1/sql` | Execute ad-hoc SQL (`{"sql": "..."}`) |
| POST | `/api/v1/reload` | Hot-reload configuration |
| GET | `/api/v1/cluster` | Cluster status (delta mode only) |
| GET | `/ws/{name}` | WebSocket upgrade for push-based subscriptions to a stream |

## Hot Reload

Edit the TOML file while the server is running. The file watcher detects changes (500ms debounce), diffs the configuration, and applies incremental DDL:

1. Removes sinks, pipelines, lookups, sources that were deleted or changed
2. Recreates sources, lookups, pipelines, sinks that were added or changed

Changes to `[server]`, `[state]`, and `[checkpoint]` sections require a restart. Disable the file watcher with `LAMINAR_DISABLE_FILE_WATCH=1`.

## Deployment

See [deploy/README.md](../../deploy/README.md) for binary downloads, Docker, and Helm chart instructions.

## Related Crates

- [`laminar-db`](../laminar-db) -- Database facade
- [`laminar-connectors`](../laminar-connectors) -- External system connectors
- [`laminar-core`](../laminar-core) -- Streaming engine
- [`laminar-storage`](../laminar-storage) -- Checkpoint storage
