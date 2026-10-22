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

## Delta Lake cross-engine oracle

`delta-spark-compose.yml` runs Spark 4.2.0 with delta-spark 4.4.0 as an independent reader and
writer. Spark writes a change-data-feed table with deletion vectors (INSERT, UPDATE, DELETE, MERGE,
a key change, OPTIMIZE) and a column-mapped table; the LaminarDB source must reproduce Spark's
tables from the change feed and from snapshots, and Spark must read LaminarDB's upsert mirror and
partitioned append tables back exactly. Tables live under `target/delta-spark-interop`; create it
before starting the container, because Docker otherwise creates the bind source owned by root and
the image's non-root user cannot write to it.

```
mkdir -p target/delta-spark-interop
docker compose -f tests/docker/delta-spark-compose.yml up -d
LAMINAR_DELTA_SPARK_ORACLE=1 cargo test -p laminar-connectors --features delta-lake \
  --test delta_spark_interop -- --nocapture
```

The first oracle call downloads the Delta jars from Maven. Without the variable the test skips.

## MongoDB CDC

`mongodb-cdc-compose.yml` starts a single-member replica set (`127.0.0.1:27117`), a
three-member set for election tests (`127.0.0.1:27201-27203`), and PostgreSQL
(`127.0.0.1:15433`, `laminar` / `laminar-test-secret`, database `mirror`). Every member
advertises the host-published address, so the driver discovers the same topology from the
host.

The history → Iceberg case also needs `iceberg-compose.yml` (SeaweedFS S3 on 9000, REST catalog on 8181).

```
docker compose -f tests/docker/mongodb-cdc-compose.yml up -d --wait
docker compose -f tests/docker/iceberg-compose.yml up -d --wait
cargo test -p laminar-db --features mongodb-cdc,postgres-sink,delta-lake,files,iceberg \
  --test mongodb_cdc_e2e -- --test-threads=1
LAMINAR_SCHEMA_TEST_MONGO="mongodb://127.0.0.1:27117/?directConnection=true&tls=false" \
  cargo test -p laminar-connectors --features mongodb-cdc --test schema_mongodb_integration \
  -- --ignored --test-threads=1
docker compose -f tests/docker/mongodb-cdc-compose.yml down -v
docker compose -f tests/docker/iceberg-compose.yml down -v
```

The end-to-end tests skip when MongoDB is unreachable unless `LAMINAR_REQUIRE_MONGODB_CDC=1`.

Two ignored suites measure rather than gate. Run each `perf_` scenario in its own process, in
release, so its peak memory is its own; the soak drives the standalone server with mixed writes
and transactions, hard-kills it every `LAMINAR_MONGODB_SOAK_KILL_EVERY_SECONDS` (default 20),
and requires exact PostgreSQL and MongoDB mirrors at the end:

```
cargo test --release -p laminar-db --test mongodb_cdc_e2e -- --ignored --nocapture --exact \
  perf::perf_history_fetch_decode_and_batch
LAMINAR_MONGODB_SOAK_SECONDS=300 cargo test --release -p laminar-server \
  --test mongodb_cdc_soak -- --ignored --nocapture
```

## Tear down

```
docker compose -f tests/docker/compose.yml down
```

## SeaweedFS (S3-compatible object store)

Host port 19000 serves the S3 API with access key `laminar` / `laminar-test-secret`
(`seaweedfs-s3.json`). SeaweedFS honours the conditional create and update requests that
checkpoint, lease, and exactly-once Delta commits depend on. The fault soaks create their bucket
with:

```
docker compose -f tests/docker/compose.yml exec -T seaweedfs /bin/sh -c \
  'echo "s3.bucket.create -name laminardb-soak" | weed shell'
```

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
