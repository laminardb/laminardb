# Durable schema resolution validation

Status: in progress, 2026-10-07. This report records executed results; pending
checks are not passes. Implementation and migration rules are in
[SCHEMA_RESOLUTION.md](../SCHEMA_RESOLUTION.md), including the complete registered
connector/direction/format matrix and SQL examples.

## Baseline and scope

Starting commit: `009d8d5848cc380b1b125ac716afc5cada24dbfe`, initially clean `main`.
Work is on `codex/durable-schema-resolution`. Dependency versions remain those
recorded in the [worklog](durable-schema-resolution-worklog.md); there is no lockfile
upgrade. Arrow schema serialization uses its existing supported serde feature.

The change applies to embedded, single-node and cluster creation/replay. It
retains each mode's existing delivery and SQL admission boundaries. Metadata is
resolved before activation; durable deployments first publish the immutable
contract through their existing authority. Explicit external preparation remains
separate from publication and can leave unused authorized artifacts after failure.

| Files / families changed | Purpose |
| --- | --- |
| `laminar-core/src/schema_binding/`, catalog manifest, durable local store | Versioned lossless Arrow/native contracts, deterministic SHA-256, optional legacy-compatible manifest field, exclusive local lease ownership |
| Connector registry, source/sink contracts, schema resolution and serde contracts | Direction/format declarations, typed failures, pure compatibility/mapping validation, bounded control-plane work |
| Kafka registry, Avro source/sink, startup/checkpoint and metrics | Concrete selections/references, committed readers with historical writers, controlled registration, prepared batch writer, bounded caches and backpressure |
| PostgreSQL catalog, CDC, sink, reference/lookup | Native relation identities/layouts, pre-consumption discovery, named transactional writes and relation drift rejection |
| Delta/Iceberg source, sink, reference/lookup and catalog adapters | Table identities, native schemas/protocol/field IDs, explicit creation, pinned logical readers with advancing data positions |
| MongoDB metadata, CDC, lookup and sink | Deployment/collection UUIDs, fixed envelopes, closed validator inference, explicit collection preparation and target checks |
| Files discovery, source/sink, output lease; NATS, WebSocket, OTLP, generator | Deterministic native metadata, opt-in bounded sampling, codec/built-in/query policies and native-format validation |
| DB DDL, connector manager, schema journal, bootstrap, planning, recovery identity and DESCRIBE | One fenced resolution/publication lifecycle, original intent, generation checks, replay and diagnostics |
| SQL lookup parser/planner | Authoritative lookup fields and separately validated declared keys |
| Core/DB/server test fixtures, connector conformance/native tests, SQL examples | Persistence, races, crash boundaries, native writes, unavailable writers, no skipped progress and unchanged admission |
| Connector schema benchmark; README, SQL reference, connector README and guides | Baseline comparisons, tested syntax, capabilities, configuration, migration and limitations |
| Readability function baseline | Remove only exceptions whose extracted functions no longer exist |

No coordinator-cycle or core-operator production code has been changed. The
existing graph input budget and terminal recovery policy remain enforced.

## Commands and environment

Linux checks use Rust `1.99.0` in the cached `laminar-process-pr-ci:20261007`
Docker image, the repository bind at `/repo`, `CARGO_HOME=/cache/cargo` and
`CARGO_TARGET_DIR=/cache/target`. Native dependencies are cached offline.
Build concurrency is two; final tests run without concurrent compilation.
Workspace tests use `RUST_TEST_THREADS=8` and `RUST_MIN_STACK=4194304` without
changing assertions, workloads or timeouts.

Windows feature checks use Rust 1.99.0/MSVC, offline dependencies and
`--target-dir target/schema-kafka`. Bundled native build tools require the
approved sandbox escalation. Docker access likewise requires escalation.

| Exact Cargo command (plus stated environment) | Executed outcome |
| --- | --- |
| `cargo +1.99.0 test --workspace --lib --offline -- --color never` | PASS: 6,302 tests (2,009 connectors, 1,140 core, 2,283 DB, 870 SQL), five existing ignored; log `linux-workspace-tests-14.txt`. |
| `cargo +1.99.0 clippy --workspace --all-features --all-targets --offline -- -D warnings` | PASS: log `linux-clippy-all-7.txt`. Earlier benchmark mutability/unit-value and test-module ordering errors were corrected. |
| `cargo +1.99.0 clippy --workspace --no-default-features --offline -- -D warnings` | PASS: log `linux-clippy-minimal-3.txt`. |
| `cargo +nightly fmt --all -- --check` | PASS after the final schema edits. |
| `cargo run --quiet --manifest-path tools/readability-check/Cargo.toml -- .` | PASS: 18 module and 208 function exceptions; no exception grows. |
| `cargo clippy -p laminar-connectors --no-default-features --features FEATURE --lib --offline --target-dir target/schema-kafka -- -D warnings` | All 13 executed isolated feature sets passed: `iceberg-core`, `iceberg-catalog-rest`, `iceberg-storage-fs`, `iceberg-gcs`, `iceberg-azure`, `delta-lake`, `delta-lake-s3`, `delta-lake-azure`, `delta-lake-gcs`, `kafka`, `postgres-cdc`, `postgres-sink`, `mongodb-cdc`. |

Logs are in `target/schema-resolution/` and `target/schema-kafka/`.
The published SQL examples passed all six cases in 7.95 s using:

```text
cargo +1.99.0 test -p laminar-db --no-default-features --features kafka,postgres-cdc,postgres-sink,files,delta-lake,websocket --test schema_sql_examples --offline -- --include-ignored --test-threads=1 --nocapture --color never
```

The example environment supplies `SCHEMA_PG_CONNECTION`, `SCHEMA_PG_PASSWORD`,
`SCHEMA_PG_PORT=55439`, `LAMINAR_SCHEMA_TEST_PG_DATABASE=laminar_schema` and
`LAMINAR_SCHEMA_TEST_KAFKA=127.0.0.1:19092`. Credentials are task fixture values,
not deployed credentials. Docker uses host networking for these service tests.

The native suite commands use `cargo test -p laminar-connectors
--no-default-features --features FEATURES --test TARGET --offline`, plus
`--target-dir target/schema-kafka` on Windows and `--profile soak` on Linux.
The test arguments are `--ignored --test-threads=1 --nocapture --color never`;
conformance uses ordinary, non-ignored tests.

| Target / feature set / environment | Executed result |
| --- | --- |
| `schema_conformance`; `kafka,files,delta-lake,iceberg,postgres-cdc,postgres-sink,mongodb-cdc,nats,otel,websocket`; Windows | PASS 10/10, 1.82 s; `conformance-5.txt`. Includes real file discovery and Delta identity, writer mapping and advancing data cursors. |
| `schema_postgres_integration`; same Windows features; `LAMINAR_SCHEMA_TEST_PG` points to task PostgreSQL 17 at port 55439 with `wal_level=logical` | PASS 5/5, 72.86 s; `postgres-native-3.txt`. Named writes, defaults/generated columns, relation replacement and publication/slot cursor checks. |
| `schema_mongodb_integration`; same Windows features; `LAMINAR_SCHEMA_TEST_MONGO=mongodb://127.0.0.1:57019/?directConnection=true&tls=false` | PASS 4/4, 12.66 s; `mongodb-native-2.txt`. Validator mapping, UUID replacement, preparation permission and restart. |
| `schema_kafka_integration`; `kafka`; Linux; `LAMINAR_SCHEMA_TEST_KAFKA=127.0.0.1:19092` | PASS 3/3, 15.17 s; `linux-kafka-native-2.txt`. Empty topic, historical/defaulted/reordered writers, independent Avro decoding, sustained outage with no skipped progress and recovery. |
| `iceberg_append`; `iceberg`; Linux REST/MinIO stack at 8181/9000 | PASS 6/6, 12.55 s; `linux-iceberg-integration-2.txt`. UUID/field identity, native mapping, snapshots, concurrent appends and checkpoint replay. |

## Codec and resolution benchmarks

Quiet timing command:

```text
cargo +1.99.0 bench --profile soak -p laminar-connectors --no-default-features --features kafka --bench schema_contract --offline -- --sample-size 30 --measurement-time 2 --warm-up-time 1
```

The allocation run adds the existing `testing` feature and is separate from the
timing run. Logs: `linux-schema-benchmark-2.txt`, `linux-schema-allocations-1.txt`.
All warm cases use the same independently checked 512-row, three-field fixture
(Int64, Utf8, Float64). Baseline source code reproduces starting commit `009d8d5`'s
decoder, mutex, batching and flush loop. The sink baseline reproduces its per-row
writer construction. Twenty warmups precede 2,000 percentile samples; Criterion
uses 30 samples. Cold HTTP cases use 200 percentile samples and 20 Criterion samples.

| Batch/stage | Mean | p50 / p95 / p99 | Allocation requests / cumulative bytes |
| --- | --- | --- | --- |
| Baseline Avro encode | 1.5600 ms | 1.565631 / 1.722215 / 2.216506 ms | 39,937 / 5,618,688 |
| Prepared Avro encode | 27.852 us | 23.571 / 25.042 / 32.013 us | 515 / 27,064 |
| Baseline Avro decode | 9.5822 us | 9.071 / 17.411 / 17.791 us | 15 / 28,460 |
| Committed-reader decode | 10.882 us | 10.891 / 11.251 / 18.521 us | 15 / 28,460 |
| Metadata-only HTTP control | 5.8692 ms | 5.843863 / 6.706201 / 7.025049 ms | 3,369 / 4,311,738 |
| Resolve binding and canonicalize | 5.9858 ms | 5.930061 / 6.455928 / 6.743874 ms | 4,258 / 4,397,746 |
| Unknown writer, mock 5 ms response | 12.075 ms | 12.010947 / 12.722020 / 13.234091 ms | 3,363 / 4,310,349 |
| Sixteen concurrent misses, single-flight | 12.350 ms | 12.166247 / 12.822658 / 13.916598 ms | 3,426 / 4,342,299 |

Warm encoding throughput is 328.20 K rows/s versus 18.383 M rows/s. Reader
resolution costs about 13.6% on this warm fixture: 53.433 M versus 47.048 M
rows/s, with unchanged measured allocation requests. This is not zero overhead.
The metadata-only cold comparator uses the current bounded HTTP client, rather
than the original HTTP client. These stage measurements exclude filesystem/CAS
publication; whole-catalog cold creation and graph benchmarks remain pending.

Warm process RSS observations were 10,692-10,800 KiB; no growth occurred during
either decode percentile loop. Cold-run HWM reached 60,488 KiB. These are cumulative,
order-dependent process observations, not isolated peak memory per operation.
Allocation bytes count requests, including reallocations, and are not live memory.
Criterion intervals and percentile samples differ; cold mocks and a development
WSL2 machine do not establish production latency or an external-service SLO.

## Recovery and failed runs

The default local exact four-kill soak passed in 79.07 s. The default three-node
Kafka-output at-least-once soak passed in 540.85 s with four kills, 96 input
partitions, 400 rows/s, a 500 ms checkpoint cadence and 90 s of steady work.
All temporal/window output oracles and hot/checkpoint latency gates remained on.

The default three-node Iceberg leader-restart test passed in 68.23 s against
the repository REST/MinIO stack. It verified one snapshot per checkpoint,
reconciliation and exact output after restart. This does not qualify an external
AWS catalog/storage deployment.

The full default Delta exact four-kill soak is currently failing. Two executions
recovered from the first two kill/rejoin rounds, then halted during the third
round when replay emitted 259 and 264 batches against the existing 256-batch
graph input budget. The terminal fault remained durable and intake stayed shut.
The optimized server digest for both runs is
`6d9f9d9abc00bed05b998e28e0c30264d497124eb15fabf114d95c660af4430b`.
The untouched starting commit reproduced the same terminal budget failure:
273 batches/148512 bytes during the first node rejoin, then intake stayed shut.
Its optimized server digest is
`6862362fbb885a89a2bd222563fcb6c0a46c4a08f5e7baf0738483db82c56fdd`.
The archived baseline production sources were unchanged; the current existing
soak harness selected that prebuilt server through its supported verified override.
This establishes a pre-existing failure, but does not turn the failed gate into a pass.

Earlier workspace runs overlapped heavy optimized compilation. One filesystem
listing/delete test, six conditional-store probes and one timed AI test exceeded
their unchanged bounds. Isolated runs with the same binaries passed all 19
filesystem tests (0.63 s), all nine checkpoint namespace tests (2.67 s), and the
AI case (0.73 s). A separate new dependency-generation test had a faulty fixture:
its manually inserted source was absent from the connector manager. It now
registers a normal generator and explicitly asserts the generation change.
The subsequent complete workspace run passed. A checkpoint-pruning assertion
failed once; its exact workspace binary passed the isolated case, and the next
complete run passed all 1,140 core tests without changing the test bounds.

Public SQL execution exposed actual configuration/parser handoff issues: native
file formats, quoted dotted keys, environment references, native key identifier
case, WebSocket binary, and PostgreSQL sink `hostname`/`table.name` plus its
runtime-owned delivery property. Fixes use the existing parser/configuration
semantics. The relational example now uses the supported on-demand lookup join,
and tests reference snapshot discovery separately. Existing SQL admission was
not widened to admit an unsupported snapshot-stream join.

Broker setup failures occurred before test workloads because retained task topics
exhausted capacity. One attempt also used the wrong broker environment variable
and stopped before setup. Automatic approval review rejected deleting six
completed-soak topics as irreversible without topic-specific authorization. The
old broker and its data are retained, stopped; an isolated fresh broker supplies
the repeated workload. No data deletion bypass was used.

## Limits to report on completion

- Registry-backed key codecs, Protobuf and JSON Schema are not implemented in this
  checkout. Precise errors preserve the supported Avro value/raw key contract.
- Historical writer availability remains a registry retention requirement.
  Committed readers do not persist every past/future writer; unresolved records
  apply bounded backpressure without acknowledged progress.
- Sampling is opt-in and cannot prove future homogeneity, keys or event time.
  Local file identity uses path/size/mtime, not an atomic content-version token.
- MongoDB name-based writes cannot atomically precondition on collection UUID;
  replacement between the final check and write remains a documented limitation.
- Legacy records missing the native identity needed for safe replay require a
  controlled migration. Rollback requires a compatible binary and saved cut;
  neither migration nor recovery silently fetches latest or deletes durable state.
- External cloud-provider qualification, Unity/Glue catalog service tests, and
  deployment-specific TLS/auth infrastructure are not represented by local mocks
  or MinIO and must not be described as executed.
