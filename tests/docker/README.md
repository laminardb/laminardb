# Docker-backed integration tests

Compose file for external systems used by LaminarDB integration tests.

## Bring up

```
docker compose -f tests/docker/compose.yml up -d
```

Wait for the Redpanda health check, or let the test helper wait until
`127.0.0.1:19092` answers a Kafka metadata request.

## Run the scenarios

```
# Happy path + checkpointed at-least-once restart (runs in the default `cargo test`):
cargo test -p laminar-db --features kafka --test kafka_docker_scenarios

# Including the broker-outage test:
cargo test -p laminar-db --features kafka --test kafka_docker_scenarios \
  -- --include-ignored --test-threads=1
```

Tests skip gracefully when the broker is unreachable, so the same command is safe on developer
machines without Docker. Set `LAMINAR_REQUIRE_REDPANDA=1` in release validation to fail instead.

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

before `cargo test`. Without these the `ssl` feature from
`laminar-connectors` (or `laminar-db` dev-dependencies) tries to build OpenSSL from source via Perl, which
fails on the stock Windows toolchain.

## Tear down

```
docker compose -f tests/docker/compose.yml down
```

## MinIO (S3-compatible object store)

Host port 19000 (API) / 19001 (console). Login at
http://localhost:19001 with `laminar` / `laminar-test-secret`.

Used by the `minio` cases in `crates/laminar-db/tests/cluster_integration.rs` to
exercise shared state, control-plane CAS, and restart recovery through fresh
object-store clients. Each test creates a unique bucket.

```
LAMINAR_REQUIRE_MINIO=1 cargo test -p laminar-db --no-default-features \
  --features cluster --test cluster_integration minio:: -- --test-threads=1
```

Without `LAMINAR_REQUIRE_MINIO=1`, these cases skip when MinIO is unavailable.

## Scenarios covered

1. **`scenario_1_kafka_roundtrip`** — 50-record round-trip through a
   Kafka source → SQL projection → Kafka sink pipeline. Consumes through a
   captured broker cut and requires each exact ID/value once.
2. **`scenario_2_broker_outage_between_batches_reconnect_smoke`** (ignored) — produces one
   input batch, kills the broker and proves metadata is unavailable, restarts
   it, then produces a disjoint batch and verifies every expected ID and value.
   This covers idle reconnect, not an in-flight checkpoint fault.
3. **`scenario_3_at_least_once_has_no_loss_after_db_restart`** — runs the
   first input batch, forces a checkpoint, shuts down cleanly, produces
   a second batch while stopped, reopens against the same storage dir,
   stops the writer, then verifies the final stable snapshot has exactly the
   expected IDs and values and no growth in pre-checkpoint per-ID counts.
4. **`minio::two_node_minio_leader_commits_follower_mirrors`**
   — 2-node cluster sharing one MinIO bucket via `ObjectStoreBackend`.
   Verifies the leader's full-registry gate reads follower markers off
   shared storage and seals the exact checkpoint after the 2PC ack.
5. **`minio::cluster_control_state_survives_fresh_minio_client_restart`**
   — restarts both cluster processes with a newly constructed MinIO client,
   then verifies assignment recovery and a durable post-restart commit.
6. **`minio::two_node_coordinated_descriptors_aggregate_on_leader`**
   — verifies the designated committer seals only after both nodes publish
   the required sink descriptors.
