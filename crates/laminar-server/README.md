# laminar-server

Standalone server binary for LaminarDB. Reads a TOML configuration file, constructs streaming pipelines, and serves a REST API.

## Features

- **TOML configuration** with `${VAR}` and `${VAR:-default}` environment variable substitution
- **REST API** (Axum) for health checks, pipeline introspection, ad-hoc SQL, and manual checkpoints
- **Postgres wire protocol** (optional) for `SUBSCRIBE` streaming via `psql` and any libpq client
- **Prometheus metrics** at `/metrics`
- **Hot reload**: edit the TOML file and changes are applied automatically (file watcher with debounce), or `POST /api/v1/reload`
- **Platform allocators**: jemalloc on Linux, mimalloc on Windows MSVC (see [Tuning the Allocator](#tuning-the-allocator-malloc_conf) for `MALLOC_CONF` recommendations)
- **Docker and Helm** deployment with multi-arch images

## CLI

```
laminardb [OPTIONS]

Options:
  -c, --config <CONFIG>            [default: laminardb.toml]
      --log-level <LOG_LEVEL>      [default: info]
      --admin-bind <ADMIN_BIND>
      --pgwire-bind <PGWIRE_BIND>  Postgres wire bind address (e.g. `127.0.0.1:5433`). Wildcard binds rejected
  -h, --help                       Print help
  -V, --version                    Print version
```

## Quick Start

```bash
# Build from source
cargo build --release --bin laminardb

# Run with a config file
./target/release/laminardb --config laminardb.toml

# Docker
export LAMINAR_CONSOLE_TOKEN="$(openssl rand -hex 32)"
docker run -d -p 8080:8080 -e LAMINAR_CONSOLE_TOKEN \
  -v laminardb-data:/var/lib/laminardb \
  laminardb/laminardb-server:0.33.0

# Check health
curl http://localhost:8080/health
```

## Configuration

See the [Configuration Reference](https://laminardb.io/docs/) for every field, or the example below:

`[[process_function]]` configures a supervised Python worker and its source. Single-node
best-effort execution supports direct input. Replay-safe Linux packages also support
at-least-once execution in single-node and cluster mode with a qualifying fixed-batch source and
durable checkpoints. Shipped sources currently lack the required replay-order contract, so
at-least-once process compositions using them fail before intake. See the
[Python deployment profile](../../python/laminardb_process/README.md#replay-safe-linux-deployment)
for package, source and worker-loss requirements.

Set `LAMINAR_CONSOLE_TOKEN` before starting with this configuration. Non-loopback HTTP binds
require a console token; loopback development can omit it. Use `Authorization: Bearer <token>`
for protected routes. HTTP TLS terminates at a trusted proxy; pgwire and cluster TLS settings
do not protect HTTP. Restrict network access to the public health and metrics endpoints.

```toml
[server]
mode = "single"             # "single" (standalone) or "cluster" (multi-node)
bind = "0.0.0.0:8080"       # HTTP API bind address
console_token = "${LAMINAR_CONSOLE_TOKEN}"
delivery = "at_least_once"  # pipeline-wide; cluster EO is connector-capability gated
datafusion_memory_limit_bytes = 268435456 # shared per DB/node; 256 MiB default, must be > 0
source_queue_max_bytes = 67108864 # shared connector FIFO per DB/node; 64 MiB default
reference_table_max_rows = 1000000 # independently per local reference table; must be > 0
reference_table_max_bytes = 268435456 # retained-memory charge per local table; 256 MiB default
materialized_view_max_rows = 1000000 # per local MV; distinct rows for multiset storage
materialized_view_max_bytes = 268435456 # retained-memory charge per local MV; must be > 0
materialized_view_read_cache_bytes = 268435456 # decoded committed view state cached for reads; 0 disables
# pipeline_max_managed_state_bytes = 268435456 # managed operator state charge; 256 MiB default
# pipeline_max_input_buf_batches = 256 # per graph input port; 0 disables count limit
# pipeline_max_input_buf_bytes = 33554432 # optional per-port Arrow bytes; unset disables
# drain_timeout = "150s" # cluster SIGTERM handoff allowance; default checkpoint.timeout + 30s
pgwire_bind = "127.0.0.1:5433"  # optional; enables Postgres wire protocol for SELECT and SUBSCRIBE
# Optional MD5 password auth for the pgwire listener. When this map is set,
# the listener requires MD5 auth and is allowed to bind to non-localhost
# interfaces. When empty, auth is "trust" and the bind must be localhost.
# [server.pgwire_users]
# alice = "${ALICE_PASSWORD}"
# bob   = "${BOB_PASSWORD}"
# Main I/O runtime worker count uses $TOKIO_WORKER_THREADS (default: logical CPUs).
# Streaming compute uses one separate single-threaded laminar-compute runtime.

[checkpoint]
# Provider-neutral object_store URL: absolute file://, s3[a]://, gs/gcs://,
# az://, abfs[s]://, or wasb[s]://.
# R2 and MinIO use s3:// with their endpoint option. Credentials come from the
# standard provider environment or [checkpoint.storage]. Cluster URLs must be
# visible to every node. Replay-capable single-node delivery currently requires
# file:// until remote writer fencing lands. Startup verifies conditional puts.
url = "file:///tmp/laminardb/checkpoints"
interval = "30s"
# Optional: while a `recent` view read waits for a newer checkpoint, the next one starts this
# long after the previous one ended instead of `interval` (shorter than `interval`; at least
# 100ms in cluster mode). Each early checkpoint also commits exactly-once sinks.
# recent_read_interval = "1s"
timeout = "120s" # checkpoint deadline and per-phase cluster assignment-recovery bound

# Provider features, accepted aliases, ambient identity, and native evidence:
# ../../docs/cloud-object-store-support.md

[[source]]
name = "trades"
connector = "kafka"
format = "json"
[source.properties]
"bootstrap.servers" = "localhost:9092"
topic = "market-trades"
"group.id" = "laminar"
"startup.mode" = "earliest" # at-least-once delivery cannot start from committed group offsets
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
format = "json"
[sink.properties]
"bootstrap.servers" = "localhost:9092"
topic = "avg-prices"
```

Materialized views have no structured section. Declare them, and any other DDL, in the top-level
`sql` key, which must come before the first `[table]`. It runs after the structured sections, and
in a cluster it is part of the startup catalog every node must agree on:

```toml
sql = """
CREATE MATERIALIZED VIEW trade_counts AS
SELECT symbol, COUNT(*) AS trades FROM trades GROUP BY symbol;
"""

[server]
# ...
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
| POST | `/api/v1/checkpoint/expedite` | Internal: a node forwards a waiting `recent` read's request to the cluster leader, which starts the next periodic checkpoint early under `recent_read_interval` (202) |
| POST | `/api/v1/sql` | Execute ad-hoc SQL (`{"sql": "...", "read_level": "committed", "min_epoch": 42}`; both read fields optional; `read_level` is `latest`, `committed` or `recent`) |
| POST | `/api/v1/reload` | Hot-reload configuration |
| GET | `/api/v1/cluster` | Cluster status (only available when `server.mode = "cluster"`) |
| GET | `/api/v1/cluster/topology` | Durable catalog version and this process's activation evidence (console authorization) |
| POST | `/api/v1/cluster/topology/adopt` | Explicitly adopt an existing legacy catalog after a coordinated upgrade |
| POST | `/api/v1/cluster/topology/validate` | Validate a topology candidate without applying it |
| POST | `/api/v1/cluster/topology/operations` | Submit one atomic migration with an operation UUID and expected parent version |
| GET | `/api/v1/cluster/topology/operations/{operation_id}` | Audited migration status (console authorization) |
| GET | `/ws/{name}` | WebSocket upgrade for push-based subscriptions to a stream |

`POST /api/v1/sql` returns at most 1000 result rows (and stops after a 5s collection budget). When the result is larger, the JSON response sets `"truncated": true` and `data` holds the first 1000 rows; the field is omitted when the result is complete. Use SUBSCRIBE (pgwire/WebSocket) to stream unbounded results.

A query reads materialized views at `read_level`: `latest` (the latest completed cycle, which
recovery may roll back), `committed` (one committed checkpoint for every view in the query) or
`recent` (a committed checkpoint holding every record written to the views' Kafka sources before
the request arrived, waited for up to 30 seconds before the 5s collection budget starts; with
`[checkpoint] recent_read_interval` the wait brings the next checkpoint forward).
Omitted, it is `latest` locally and `committed` in cluster mode, where a node holds only part of
each view. A committed or recent response adds `epoch`, `checkpoint_id` and `taken_at_ms`; pass
the epoch back as `min_epoch` to read no earlier checkpoint. `min_epoch` without a committed or
recent level is rejected. A read that passes its 30 second deadline (a `min_epoch` that never
commits, or no checkpoint yet holding a recent read's records) answers 504; a recent read whose
source cannot be asked how far it has been written answers 502; other read errors, including a
recent read of a view over a source that cannot report it, answer 400. See
[Delivery guarantees](../../docs/DELIVERY_GUARANTEES.md#materialized-view-reads). While a cluster node completes a coordinated recovery round, it keeps answering reads and
keeps `/ws/{name}` subscriptions open; other statements and routes answer 503 until it ends.

Supported cluster DDL returns an asynchronous migration receipt. See the main
README's [topology support and limits](../../README.md#change-a-running-clusters-topology).

## Postgres Wire Protocol

When `[server].pgwire_bind` is set, the server also listens for Postgres clients over the simple and extended query protocols. A cluster node opens the listener when it starts serving and closes it, with every session, when it stops. Like HTTP SQL reads and WebSocket subscriptions, it keeps answering committed reads while the node completes a coordinated recovery round (see [Delivery guarantees](../../docs/DELIVERY_GUARANTEES.md#materialized-view-reads)):

- `SELECT` reads the database, with the same read levels as `POST /api/v1/sql`, chosen per connection: `SET laminar.read_level = 'latest' | 'committed' | 'recent' | 'default'` and `SET laminar.min_epoch = <n>`. `SHOW laminar.read_epoch` returns the epoch of the connection's last committed or recent read, and later committed and recent reads on the connection read no earlier epoch. A recent read fails with `0A000` when a source behind its views cannot report how far it has been written, `58000` when one cannot be reached, and `57014` when no checkpoint holds every source within 30 seconds. Rows stream as the client reads them; statements with bind parameters are rejected (`0A000`).

- `SUBSCRIBE <name> [AS OF EPOCH n] [WHERE <predicate>]`: local mode streams live output from a materialized view, source, or named stream. Cluster mode exposes committed output only for materialized views and certified non-windowed keyed aggregate streams. The query stays open until the client disconnects or a terminal error occurs.
- `SHOW`, `SET <key> = <value>`, and a handful of driver builtins (`SELECT version()`, `current_database()`, etc.) are accepted so standard psql / libpq clients can connect. `SET` of names outside `laminar.*` changes database-wide session properties, not the connection's.
- `INSERT`, `UPDATE`, `DELETE`, and DDL are rejected with a clear error pointing to `POST /api/v1/sql`.

`WHERE` is compiled with DataFusion against the resolved output schema; an unresolved named-stream
schema is rejected. Local epoch replay uses byte-bounded in-memory history. Cluster replay uses
verified segments in the checkpoint store and is partition-ordered. There is no atomic
snapshot-plus-tail attachment or durable named-consumer cursor. See the
[subscription boundaries](../../docs/SQL_REFERENCE.md#subscribe-over-the-postgres-wire-protocol).

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

## Cluster rescaling, drain and recovery

Three layers are independent. **Source partitions** (for example Kafka partitions) bound how
input is read. **Vnodes** (`server.key_groups`, fixed per deployment) are the unit of keyed state,
routing, checkpoint framing and ownership; changing their count requires a new deployment.
**Owners** are stable `node_id` processes; the leader assigns vnodes to owners with unweighted
rendezvous hashing, which suits homogeneous workers. `GET /api/v1/cluster/vnodes` returns the
current owner of every vnode, `placement_vnodes_per_domain` summarises it by failure domain, and
each process exports its own count as `placement_owned_vnodes`.

Ownership changes reuse one protocol: the leader takes a checkpoint, publishes the next
assignment, and the new owners range-restore the committed donor frames before intake resumes.

- **Scale out:** start another node with a new stable `node_id`. The leader adds it after the
  rebalance debounce and a handoff checkpoint.
- **Graceful scale in:** send SIGTERM. The node announces `Draining` and waits up to
  `server.drain_timeout` (default `checkpoint.timeout` + 30 s) for the leader to move its vnodes.
  The value must be at least `checkpoint.timeout`. If the allowance runs out, the node stops
  fenced and exits with `graceful drain incomplete`. Its process lease then expires and survivors
  recover its vnodes from the last committed checkpoint, the same as a failed owner. Lease loss
  during the drain takes precedence.
- **Failed owner:** when a process lease expires, survivors take its vnodes through a coordinated
  recovery round from the last committed checkpoint. At-least-once output may replay.

A node restarted with the same `node_id` and a new boot is a new process incarnation; stale
incarnations are fenced. Before you remove a node permanently, confirm that the assignment no
longer names it. More owners do not split a hot key, add source partitions or speed up a slow
sink.

After a [topology migration](../../docs/architecture/DISTRIBUTED_STATE.md) commits, the
complete owner map stays frozen until the new topology is Active **and** has committed its own
checkpoint. Until then, only same-slot replacement of a failed process is admitted. Do not scale
in or out while `GET /api/v1/cluster/topology` reports `live_migration`. Afterwards all three ownership changes work normally, and a later
migration freezes whatever owner map is current when it is admitted. Upgrade every node before
rescaling a migrated deployment: older binaries reject a changed owner map after a migration
and stay fenced. A joining or restarted node that owns no vnodes yet starts its sources from
the last committed checkpoint, so sources a migration created with a non-replayable start
policy (such as Kafka `latest`) remain startable.

A topology request is admitted between two checkpoints. While the leader admits one, it delays
its next periodic checkpoint (manual checkpoints are not delayed) until the request is admitted
or fails, for at most the 45 s submission deadline.

The topology request journal keeps every admitted request identity, including finished ones, so
a retried request returns its original outcome. It holds 64 requests and does not compact yet.
`GET /api/v1/cluster/topology` reports `journal.retained` and `journal.capacity`. Once it is full,
new topology requests are rejected; retries of existing identities still work.

## Cluster Control-Plane TLS (mTLS)

In `mode = "cluster"`, the inter-node control plane (barrier sync and row shuffle) is plaintext and unauthenticated by default — run it on a trusted/isolated network. To require mutual TLS between nodes, set all four `[discovery]` keys together (omit them for plaintext):

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

Gossip discovery uses separate UDP traffic and is not covered by cluster mTLS. With `strategy = "gossip"`, keep the gossip network trusted or isolated.

## Hot Reload

Edit the TOML file while the server is running. The file watcher detects changes (500ms debounce), diffs the configuration, and applies incremental DDL:

1. Removes sinks, pipelines, lookups, sources that were deleted or changed
2. Recreates sources, lookups, pipelines, sinks that were added or changed

Changes to `[server]` and `[checkpoint]` require a restart. Disable the file watcher with `LAMINAR_DISABLE_FILE_WATCH=1`.

## Memory limits and production tuning

These defaults apply **per DB/node** unless the scope says per table, view, or graph port.
They are independent admission budgets, not a process memory limit. Set byte values as integer
bytes in `laminardb.toml`; the example under [Configuration](#configuration) shows the syntax.

| Setting | Default | Scope |
|---|---:|---|
| `server.datafusion_memory_limit_bytes` | 256 MiB | Shared participating DataFusion reservations; DB-owned contexts do not spill to disk. |
| `server.source_queue_max_bytes` | 64 MiB | Shared connector-to-coordinator Arrow queue, including parked input; each source's waiting batch must also fit. Must be positive and at most `MAX_SOURCE_QUEUE_BYTES`. |
| `server.pipeline_max_managed_state_bytes` | 256 MiB | Charged bytes of managed operator state (keyed aggregates, windows, joins) per pipeline; must be positive. Lower bound, not RSS. |
| `server.pipeline_max_input_buf_batches` | 256 | Each graph input port; `0` disables the count limit. |
| `server.pipeline_max_input_buf_bytes` | unset | Each graph input port; a configured value must be positive. |
| `server.reference_table_max_rows` / `server.reference_table_max_bytes` | 1,000,000 / 256 MiB | Each local reference table; both values must be positive. |
| `server.materialized_view_max_rows` / `server.materialized_view_max_bytes` | 1,000,000 / 256 MiB | Each local materialized view; both values must be positive. |
| `server.materialized_view_read_cache_bytes` | 256 MiB | Decoded committed view state, cached by digest for committed reads and view subscriptions; `0` disables the cache. |
| `checkpoint.max_node_data_bytes` | 512 MiB | Maximum participant checkpoint data object and in-flight captured-state admission, separate from live-state limits. |

`server.pipeline_max_managed_state_bytes` is the same budget embedded users set through
`LaminarConfig` or the builder. Exceeding it fails with a typed managed-state budget error at
startup, restore or during a cycle; its accounting is a lower bound and is not RSS. In a cluster,
each node charges only the vnodes it owns, so rescaling changes per-node state but a single hot
key still lands on one owner. Recovery and reassignment stage restored frames against the same
budget, so leave headroom for restore. Shuffle queues have fixed per-peer and per-node byte
budgets that are not configurable. Reference tables are local only; cluster plans requiring
them remain rejected.

For a production deployment, start with a representative peak workload and a durable checkpoint
location. Record **peak process/container RSS**, source lag, cycle backpressure, graph input bytes,
managed-state charge, checkpoint size and checkpoint duration during normal load, bursts, and
recovery. The `/metrics` endpoint exposes `laminardb_cycles_backpressured_total`,
`laminardb_input_buf_bytes`, `laminardb_managed_state_accounted_bytes`,
`laminardb_checkpoint_size_bytes`, and checkpoint duration/failure metrics. The managed-state
metric is a lower-bound charge, so use OS/container RSS for memory sizing. Where input waits
is exported too: `laminardb_source_queue_wait_seconds_total`,
`laminardb_intake_paused_seconds_total`, `laminardb_sink_epoch_gate_wait_seconds`,
`laminardb_managed_state_budget_ratio`, the per-operator `laminardb_vnode_*` skew gauges, and in
a cluster the per-peer `laminardb_shuffle_send_wait_seconds_total` and
`laminardb_shuffle_budget_used_bytes`.
The [flow-control table](../laminar-db/README.md#flow-control-and-backpressure) lists every
bounded buffer, what happens when it is full, and its metrics.

Set the container memory limit above the measured peak with room for connector decoding, Arrow
buffers, operator scratch, query results, simultaneous old/new state during restore, checkpoint
capture/encoding, and allocator overhead. The Helm chart leaves `resources` unset; supply
workload-specific requests and limits. Reducing one budget does not reduce all other owners.
If a queue or port saturates, inspect source batch size and downstream capacity before raising
its cap. An oversized graph result after execution faults the pipeline; an oversized checkpoint
or restored state can fail recovery. Set `checkpoint.max_node_data_bytes` high enough for the
largest expected participant artifact, then validate with fault/restart testing. Choose
`checkpoint.interval` for acceptable replay work and storage traffic, and keep
`checkpoint.timeout` above observed checkpoint duration (defaults: 10s and 120s).

`server.datafusion_memory_limit_bytes` bounds participating fallible DataFusion reservations in both
server modes and requires a restart to change. DB-owned contexts share the limit and disable
disk spilling. It does not cap process RSS, queues, managed state or connector I/O allocations;
see the [memory scope](../laminar-db/README.md#datafusion-memory-limit).

`server.source_queue_max_bytes` bounds queued connector Arrow storage in both server modes
and requires a restart to change. It also caps each individual source batch; oversized input
faults before its cursor is settled. See the [queue ownership scope](../laminar-db/README.md#connector-source-queue-limit)
for producer scratch, parked messages and downstream retention.

`server.pipeline_max_input_buf_batches` and `server.pipeline_max_input_buf_bytes` configure
prospective graph-port limits in both server modes. The count default is 256; the byte cap is
unset by default and must be greater than zero when supplied. Fan-out charges each port
independently. Output that cannot fit after execution halts before routing; see the
[graph input contract](../laminar-db/README.md#graph-input-limits). These startup settings are
not hot-reloaded and do not bound total process RSS.

`server.reference_table_max_rows` and `server.reference_table_max_bytes` bound each local
reference table's final state. Both must be greater than zero and require a restart to change.
Over-limit updates, multi-table refreshes and checkpoint restores fail before live installation.
Cluster reference-table admission remains restricted. See the
[table accounting and staging scope](../laminar-db/README.md#reference-table-memory-limits)
for shared Arrow buffers, external ownership, checkpoint captures and memory headroom.

`server.materialized_view_max_rows` and `server.materialized_view_max_bytes` apply to every
local MV storage mode, and on a cluster node to its part of each view. Both must be nonzero.
Aggregate, upsert and multiset growth fails before any affected MV changes or publishes cycle
output. Append views count the rows waiting for the next checkpoint and fail instead of dropping
rows. Restore fails instead of truncating committed state. See the
[MV accounting and publication scope](../laminar-db/README.md#materialized-view-memory-limits).

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

- **Leave `narenas` unset unless measurements justify changing it.** LaminarDB runs one coordinator on the dedicated single-threaded `laminar-compute` runtime; connector I/O, checkpoint persistence and sink publication use the main work-stealing runtime. Too few allocator arenas can increase contention among those threads. Profile the actual workload before tuning the arena count.
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

## Iceberg cluster certification soak

The ignored `iceberg_cluster_soak` test runs three LaminarDB processes and proves that a leader
failure after Iceberg publication does not create a second snapshot or duplicate rows. Its default
profile uses the repository's Kafka, Iceberg REST, and MinIO containers.

Set `ICEBERG_CLUSTER_PROFILE=aws` to run the same fault sequence against an external HTTPS REST
catalog and real S3. The external profile requires:

- `ICEBERG_CLUSTER_CATALOG_URI`
- `ICEBERG_CLUSTER_S3_BUCKET`
- `ICEBERG_CLUSTER_S3_REGION`
- `ICEBERG_CLUSTER_VISIBILITY_SLO_MS`

`ICEBERG_CLUSTER_S3_PREFIX`, `ICEBERG_CLUSTER_KAFKA_BROKERS`, and
`ICEBERG_CLUSTER_RECOVERY_TIMEOUT_MS` are optional. For bearer authentication, also set
`ICEBERG_CLUSTER_CATALOG_AUTH_TYPE=bearer` and
`ICEBERG_CLUSTER_CATALOG_BEARER_TOKEN`. AWS credentials come from the standard provider
environment, including temporary, web-identity, and container credentials; do not put them in the
test configuration. The catalog must be allowed to create a namespace and table beneath
`s3://$ICEBERG_CLUSTER_S3_BUCKET/$ICEBERG_CLUSTER_S3_PREFIX/wh`; the prefix defaults to
`laminardb-iceberg-certification`. Its configured base URI must directly expose the standard
`/v1` routes; this independent test oracle does not apply a server-advertised prefix override.

```bash
cargo test --profile soak -p laminar-server --no-default-features \
  --features "cluster,aws,kafka,iceberg" \
  --test iceberg_cluster_soak \
  leader_restart_reconciles_one_iceberg_snapshot_per_checkpoint \
  -- --ignored --exact --test-threads=1 --nocapture
```

The AWS profile fails closed when the catalog is not HTTPS, the resource scope is malformed, the
visibility bound is absent, either configured bound exceeds ten minutes, or an AWS endpoint
override would redirect the run away from real S3. It prints operation visibility in milliseconds
and retains its uniquely named namespace and objects for failure investigation and operator-managed
cleanup.

## Deployment

See [deploy/README.md](../../deploy/README.md) for binary downloads, Docker, and Helm chart instructions.

## Related Crates

- [`laminar-db`](../laminar-db) -- Database facade
- [`laminar-connectors`](../laminar-connectors) -- External system connectors
- [`laminar-core`](../laminar-core) -- Streaming engine and checkpoint storage
