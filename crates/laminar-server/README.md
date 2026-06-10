# laminar-server

Standalone server binary for LaminarDB. Reads a TOML configuration file, constructs streaming pipelines, and serves a REST API.

## Features

- **TOML configuration** with `${VAR}` and `${VAR:-default}` environment variable substitution
- **REST API** (Axum) for health checks, pipeline introspection, ad-hoc SQL, and manual checkpoints
- **Postgres wire protocol** (optional) for `SUBSCRIBE` streaming via `psql` and any libpq client
- **Prometheus metrics** at `/metrics`
- **Hot reload**: edit the TOML file and changes are applied automatically (file watcher with debounce), or `POST /api/v1/reload`
- **Checkpoint validation**: `--validate-checkpoints` flag validates all stored checkpoints and exits
- **Platform allocators**: jemalloc on Linux, mimalloc on Windows MSVC (see [Tuning the Allocator](#tuning-the-allocator-malloc_conf) for `MALLOC_CONF` recommendations)
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
mode = "embedded"           # "embedded" (single-node) or "cluster" (multi-node scaffolding, not production-hardened)
bind = "0.0.0.0:8080"       # HTTP API bind address
pgwire_bind = "127.0.0.1:5433"  # optional; enables Postgres wire protocol for SUBSCRIBE
log_level = "info"
# Optional MD5 password auth for the pgwire listener. When this map is set,
# the listener requires MD5 auth and is allowed to bind to non-localhost
# interfaces. When empty, auth is "trust" and the bind must be localhost.
# [server.pgwire_users]
# alice = "${ALICE_PASSWORD}"
# bob   = "${BOB_PASSWORD}"
# Worker thread count is taken from $TOKIO_WORKER_THREADS. Defaults to logical CPUs.

[state]
backend = "local"           # "in_process", "local", or "object_store"
path = "./data/state"       # required when backend = "local"

[checkpoint]
# Local file://, or an object store: s3://, gs://, az://, abfs(s):// (the
# AWS/GCS/Azure backends are in the default build). Credentials come from the
# standard provider env vars, or set them under [checkpoint.storage].
url = "file:///tmp/laminardb/checkpoints"
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

## AI Functions

SQL functions (`ai_classify`, `ai_sentiment`, `ai_embed`, `ai_complete`, …) run a
named model resolved from a registry. A model is either **remote** (an LLM over
HTTP) or **local** (an ONNX encoder run in-process). Configure providers, models,
and per-task defaults:

```toml
[ai.providers.openai]              # kind inferred from the name (openai/anthropic/local)
api_key_env = "OPENAI_API_KEY"     # env var holding the key — the key is never stored in config

[ai.providers.local]
kind = "local"
cache_dir = "./models"             # where local models are cached / downloaded

[models.sentiment]
kind = "local"
task = "sentiment"
# downloaded from the Hugging Face CDN on first use; labels come from its config.json
source = "hf:onnx-community/distilbert-base-uncased-finetuned-sst-2-english-ONNX"

[models.writer]
kind = "remote"
task = ["complete", "summarize"]
provider = "openai"
model = "gpt-4o-mini"

[ai.defaults]                      # task → default model when a call omits `model => '…'`
sentiment = "sentiment"
complete = "writer"
```

**Local models require ONNX Runtime at runtime.** The `local` backend loads the
ONNX Runtime shared library dynamically rather than bundling it, so the build
stays independent of the host toolchain. Install ONNX Runtime **1.24 or newer**
and make the library loadable — on the system search path, or via `ORT_DYLIB_PATH`:

```bash
export ORT_DYLIB_PATH=/opt/onnxruntime/lib/libonnxruntime.so   # onnxruntime.dll on Windows
```

Local models are encoder-only (BERT / DistilBERT / MiniLM family) for `classify`,
`sentiment`, and `embed`; generative tasks require a remote provider. If the
library is absent, local inference returns an error while the rest of the server —
including remote AI — runs normally.

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
| GET | `/api/v1/cluster` | Cluster status (only available when `server.mode = "cluster"`) |
| GET | `/ws/{name}` | WebSocket upgrade for push-based subscriptions to a stream |

`POST /api/v1/sql` returns at most 1000 result rows (and stops after a 5s collection budget). When the result is larger, the JSON response sets `"truncated": true` and `data` holds the first 1000 rows; the field is omitted when the result is complete. Use SUBSCRIBE (pgwire/WebSocket) to stream unbounded results.

## Postgres Wire Protocol

When `[server].pgwire_bind` is set, the server also listens for Postgres clients and serves a small subset of the SimpleQuery protocol:

- `SUBSCRIBE <name> [WHERE <predicate>]`: streams rows as they're produced. `<name>` may be a materialized view, a source, or a named stream. The query stays open until the client disconnects.
- `SHOW`, `SET <key> = <value>`, and a handful of driver builtins (`SELECT version()`, `current_database()`, etc.) are accepted so standard psql / libpq clients can connect.
- `INSERT`, `UPDATE`, `DELETE`, and DDL are rejected with a clear error pointing to `POST /api/v1/sql`.

`WHERE` is compiled with DataFusion against the target's schema and works on materialized views and sources. It is rejected on named streams because their output schema isn't introspectable.

### Authentication

By default the listener uses **trust** auth and rejects non-localhost binds. The assumption is that any caller who can reach the loopback interface is already trusted. To bind to a routable address, configure MD5 password auth and explicitly opt in to remote binds:

```toml
pgwire_bind = "0.0.0.0:5433"
pgwire_allow_remote = true        # required even with auth on, two-key rule

[server.pgwire_users]
alice = "${ALICE_PASSWORD}"       # min 12 chars, validated at config load
```

Clients then connect with the password using the standard Postgres MD5 challenge flow:

```bash
PGPASSWORD=$ALICE_PASSWORD psql "host=db.internal port=5433 dbname=laminardb user=alice" -c "SUBSCRIBE avg_price"
```

> **MD5 is provided for libpq compatibility, not as a recommended production stance.** Postgres itself deprecated it in PG 14 in favor of SCRAM-SHA-256. Use it for development and short-lived deployments; for production, wait for the SCRAM work in the FIR follow-ups before exposing this listener beyond a trusted network segment.

Plaintext passwords sit in the TOML file. Use `${VAR}` substitution to pull them from environment variables or a secret manager rather than committing them. To avoid plaintext at rest entirely, supply the `pg_authid`-style pre-hashed form: `md5` followed by `md5(password ‖ username)` as 32 lowercase hex characters. The wire protocol is unchanged; clients still send the same plaintext password.

```bash
# bash, where pw and user are the plaintext password and username:
printf '%s' "${pw}${user}" | md5sum | awk '{print "md5"$1}'
```

The listener emits `target: "audit"` events on every connection accepted/closed, including auth-failed outcomes. Wire these into your SIEM.

### TLS

Optional. Setting both `pgwire_tls_cert` and `pgwire_tls_key` enables TLS via [`tokio-rustls`](https://crates.io/crates/tokio-rustls) (aws-lc-rs backend). Both must be PEM-encoded; the key may be PKCS#8 or RSA.

```toml
pgwire_tls_cert = "/etc/laminar/pgwire.crt"
pgwire_tls_key  = "/etc/laminar/pgwire.key"
# Optional. Default "1.2"; set "1.3" to refuse TLS 1.2 handshakes.
pgwire_tls_min_version = "1.2"
# Optional. Enable mTLS: every client must present a cert chained to
# one of the roots in this PEM bundle. No revocation (CRL/OCSP) yet.
pgwire_tls_client_ca = "/etc/laminar/clients-ca.pem"
```

Postgres clients negotiate TLS via `sslmode=require` (or `verify-ca` / `verify-full` if your cert chain is trusted by the client). The handshake follows the standard `SSLRequest` flow, so `psql`, JDBC, asyncpg, and friends all just work.

The server watches `pgwire_tls_cert`, `pgwire_tls_key`, and `pgwire_tls_client_ca` and reloads the TLS acceptor in place after a 500ms debounce, so cert rotation does not require a restart. In-flight handshakes finish with the cert that was current when the socket was accepted; new accepts pick up the rotated cert. A bad rotation (truncated file, expired cert) is logged as `pgwire.tls_reload outcome=failed` and the previous acceptor is kept. Set `LAMINAR_DISABLE_FILE_WATCH=1` to disable.

```bash
psql "host=127.0.0.1 port=5433 dbname=laminardb user=any" -c "SUBSCRIBE avg_price WHERE symbol = 'AAPL'"
```

## Failure-Domain Placement (rack/zone awareness)

In `mode = "cluster"`, advertise each node's physical topology with `failure_domain` (a flat label, or hierarchical coarsest-first `;`-separated tiers). It is gossiped to peers and exported as blast-radius metrics so you can see how vnode ownership is spread across racks/zones.

```toml
[discovery]
strategy = "gossip"
seeds = ["node-1:7946", "node-2:7946"]
failure_domain = "region=us-east-1;zone=us-east-1a;rack=r17"
placement_isolation_tier = 1     # 0=region, 1=zone, 2=rack — what counts as a "domain"
```

`placement_vnodes_per_domain{domain}` and `placement_blast_radius_ratio` (the largest domain's share of vnodes — the state that goes into recovery if that domain fails at once) are exported; see the "Failure-Domain Placement" row in the cluster Grafana dashboard. Unlabeled nodes collapse into one shared `unknown` domain. Placement itself is plain rendezvous hashing — this surfaces the blast radius, it does not yet act on it.

## Cluster Control-Plane TLS (mTLS)

In `mode = "cluster"`, the inter-node control plane (barrier sync, the distributed-query `RemoteScan` service, and the row shuffle) is plaintext and unauthenticated by default — run it on a trusted/isolated network. To require mutual TLS between nodes, set all four `[discovery]` keys together (omit them for plaintext):

```toml
[discovery]
strategy = "gossip"
seeds = ["node-1:7946", "node-2:7946"]
cluster_tls_cert = "/etc/laminar/node.crt"        # this node's cert (PEM)
cluster_tls_key  = "/etc/laminar/node.key"        # its key (PEM, PKCS#8 or RSA)
cluster_tls_client_ca = "/etc/laminar/cluster-ca.pem"  # CA that signed every node cert
cluster_tls_server_name = "laminar-cluster"       # DNS SAN present in every node cert
```

Every node both serves and dials, so the CA verifies **both** directions. Because peers connect by IP, issue all node certs with one shared DNS SAN and set `cluster_tls_server_name` to it (rather than per-node IP SANs). Enabling mTLS is a **coordinated cutover**: a TLS node cannot talk to a plaintext peer, so roll it out to all nodes at once. Cert rotation currently requires a restart (no hot reload on the control plane).

## Hot Reload

Edit the TOML file while the server is running. The file watcher detects changes (500ms debounce), diffs the configuration, and applies incremental DDL:

1. Removes sinks, pipelines, lookups, sources that were deleted or changed
2. Recreates sources, lookups, pipelines, sinks that were added or changed

Changes to `[server]`, `[state]`, and `[checkpoint]` sections require a restart. Disable the file watcher with `LAMINAR_DISABLE_FILE_WATCH=1`.

## Tuning the Allocator (`MALLOC_CONF`)

The server ships with [`jemalloc`](https://jemalloc.net/) on Linux / non-MSVC (via `tikv-jemallocator`) and `mimalloc` on Windows MSVC. These replace the system allocator and materially reduce fragmentation under bursty sink workloads (Delta Lake, Iceberg, Parquet writers). Both are enabled by default; no action needed to turn them on.

For long-running deployments, jemalloc's behavior can be tuned at process start via the `MALLOC_CONF` environment variable. The recommended baseline is:

```bash
MALLOC_CONF=background_thread:true,metadata_thp:auto
```

- `background_thread:true` spawns an auxiliary thread that purges freed pages back to the OS asynchronously. Without this, decay only fires on alloc/free events; a server that settles into a steady rhythm can end up holding pages indefinitely and RSS drifts upward. **This is the single most impactful setting for a long-running streaming process.**
- `metadata_thp:auto` backs jemalloc's internal metadata with transparent huge pages where available, reducing TLB pressure.

### When you're specifically trying to minimize RSS

Streaming sinks (Delta, Iceberg, Parquet) allocate in bursts per commit and free everything shortly after. The default decay intervals (`dirty_decay_ms=10000`, `muzzy_decay_ms=0`) favour *reusing* those freed pages over returning them to the kernel. If RSS growth is the primary operational concern and you're willing to trade syscall count for lower peak memory, shorten the decays:

```bash
MALLOC_CONF=background_thread:true,metadata_thp:auto,dirty_decay_ms:3000,muzzy_decay_ms:0
```

### Settings to avoid

- **Do not set `narenas:N` to a small value.** The jemalloc default is `ncpus * 4`, which gives each reactor / sink thread its own arena and eliminates cross-thread contention on `malloc`/`free`. LaminarDB is thread-per-core. Forcing `narenas:4` on an 8-core box means multiple reactors share an arena, and the lock contention shows up directly in the sink commit path. Leave `narenas` unset.
- **Do not set `tcache:false`.** The per-thread small-allocation cache is load-bearing on any sink that churns Arrow/Parquet buffers.

### How to set it

- **systemd**: `Environment=MALLOC_CONF=background_thread:true,metadata_thp:auto` in the unit file.
- **Docker**: `-e MALLOC_CONF=background_thread:true,metadata_thp:auto` on `docker run`, or the `environment:` block in Compose.
- **Kubernetes / Helm**: add to `env:` on the container, or set via the Helm chart's `env` values.
- **Shell**: `MALLOC_CONF=... laminardb --config laminardb.toml`.

The setting is read by jemalloc at process start; changing it requires a restart. It has no effect on the Windows MSVC build (mimalloc has its own env vars; see the [mimalloc options](https://microsoft.github.io/mimalloc/environment.html) if tuning is needed).

### Verifying it took effect

Dump jemalloc stats via SIGUSR1 (if enabled) or by configuring `stats_print:true` at startup:

```bash
MALLOC_CONF=background_thread:true,metadata_thp:auto,stats_print:true laminardb --config laminardb.toml
```

The stats are written to stderr on process exit and confirm the active configuration.

## Deployment

See [deploy/README.md](../../deploy/README.md) for binary downloads, Docker, and Helm chart instructions.

## Related Crates

- [`laminar-db`](../laminar-db) -- Database facade
- [`laminar-connectors`](../laminar-connectors) -- External system connectors
- [`laminar-core`](../laminar-core) -- Streaming engine and checkpoint storage
