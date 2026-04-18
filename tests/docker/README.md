# Docker-backed integration tests

Compose file for external systems used by LaminarDB integration tests.

## Bring up

```
docker compose -f tests/docker/compose.yml up -d
```

Wait ~8s for the Redpanda broker to pass its health check, or let the
`wait_for_broker` helper in `crates/laminar-db/tests/common/mod.rs`
block until `127.0.0.1:19092` is reachable.

## Run the scenarios

```
# Happy path + exactly-once restart (runs in the default `cargo test`):
cargo test -p laminar-db --features kafka --test kafka_docker_scenarios

# Including the disruption tests (broker kill, consumer rebalance):
cargo test -p laminar-db --features kafka --test kafka_docker_scenarios \
  -- --include-ignored --test-threads=1
```

Tests skip gracefully when the broker is unreachable, so the same command
is safe on CI environments without Docker.

### Windows build note

`rdkafka` pulls in `openssl-sys`. Install OpenSSL (e.g. from the
[Shining Light Productions](https://slproweb.com/products/Win32OpenSSL.html)
binaries into `C:\Program Files\OpenSSL-Win64`) and set:

```
export OPENSSL_DIR="C:\\Program Files\\OpenSSL-Win64"
export OPENSSL_NO_VENDOR=1
export OPENSSL_LIB_DIR="C:\\Program Files\\OpenSSL-Win64\\lib\\VC\\x64\\MD"
export OPENSSL_INCLUDE_DIR="C:\\Program Files\\OpenSSL-Win64\\include"
```

before `cargo test`. Without these the `ssl-vendored` feature from
`laminar-connectors` tries to build OpenSSL from source via Perl, which
fails on the stock Windows toolchain.

## Tear down

```
docker compose -f tests/docker/compose.yml down
```

## MinIO (S3-compatible object store)

Host port 19000 (API) / 19001 (console). Login at
http://localhost:19001 with `laminar` / `laminar-test-secret`.

Used by `crates/laminar-db/tests/cluster_minio_flow.rs` to exercise
`ObjectStoreBackend`'s cross-instance durability check with real
shared storage rather than a shared `Arc<InProcessBackend>` shortcut.
The test creates a unique bucket per run via `mc` exec, so repeated
runs don't collide.

## Scenarios covered

1. **`scenario_1_kafka_roundtrip`** — 50-record round-trip through a
   Kafka source → SQL projection → Kafka sink pipeline. Baseline proof
   that the rig works.
2. **`scenario_2_broker_kill_midstream`** (ignored) — kills the broker
   mid-stream via `docker compose kill redpanda`, restarts it, produces
   a second half of input, verifies at-least-once delivery to the sink.
3. **`scenario_3_exactly_once_survives_db_restart`** — runs the
   pipeline, forces a checkpoint, shuts down cleanly, reopens against
   the same storage dir, verifies no duplicates on the output topic.
4. **`scenario_4_consumer_rebalance_midstream`** (ignored) — runs two
   DB instances on the same consumer group against a 2-partition
   topic; joining the group triggers a rebalance mid-flight. Verifies
   no data loss.
5. **`cluster_minio_flow::two_node_minio_leader_commits_follower_mirrors`**
   — 2-node cluster sharing one MinIO bucket via `ObjectStoreBackend`.
   Verifies the leader's full-registry gate reads follower markers off
   shared storage, and the `_COMMIT` marker lands after the 2PC ack.
   Run with `cargo test -p laminar-db --test cluster_minio_flow --features cluster-unstable`.
