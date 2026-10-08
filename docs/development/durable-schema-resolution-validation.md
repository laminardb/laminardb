# Durable schema resolution validation

Status: implementation checks complete; Delta temporal visibility gate remains
a merge blocker, 2026-10-08. This report records executed results; failed checks
are not passes. Implementation and migration rules are in
[SCHEMA_RESOLUTION.md](../SCHEMA_RESOLUTION.md), including the complete registered
connector/direction/format matrix and SQL examples.

## Baseline and scope

Starting commit: `009d8d5848cc380b1b125ac716afc5cada24dbfe`, initially clean `main`.
Work is on `codex/durable-schema-resolution`. The release bump updates owned
Cargo packages and path requirements to `0.32.0`; the lockfile changes only the
six workspace packages and two maintained pgwire forks. External dependency
versions remain those recorded in the [worklog](durable-schema-resolution-worklog.md).
Arrow schema serialization uses its existing supported serde feature.

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

Graph output admission now reuses the bounded aggregate coalescer under interval-join
batch-count pressure, conditional on every live downstream plan certifying raw
batch concatenation. The 256-batch limit, retained-byte limit, atomic fanout and
terminal recovery policy remain enforced. Core operator algorithms are unchanged.

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
| `cargo +1.99.0 test --workspace --lib --offline -- --color never` | PASS at 0.32.0: 6,307 tests (2,009 connectors, 1,140 core, 2,288 DB, 870 SQL), five existing ignored; log `linux-workspace-tests-15.txt`. Includes all five replay regressions and durable terminal recovery coverage. |
| `cargo +1.99.0 clippy --workspace --all-features --all-targets --offline -- -D warnings` | PASS at 0.32.0: log `linux-clippy-all-9.txt`. Earlier benchmark mutability/unit-value and test-module ordering errors were corrected. |
| `cargo +1.99.0 clippy --workspace --no-default-features --offline -- -D warnings` | PASS at 0.32.0: log `linux-clippy-minimal-4.txt`. |
| `cargo +nightly fmt --all -- --check` | PASS after the final implementation and 0.32.0 bump. |
| `cargo run --quiet --manifest-path tools/readability-check/Cargo.toml -- .` | PASS: 18 module and 208 function exceptions; no exception grows. |
| `cargo +1.99.0 test --workspace --lib --offline interval_output_admission -- --color never --nocapture` | PASS: all five real-operator replay admission regressions; log `linux-graph-focused-2.txt`. The first run exposed an incomplete event-time fixture, corrected without changing production validation. |
| Current DB library test binary with filter `terminal --test-threads=1 --nocapture --color never` | PASS 76/76 in 1.58 s, including durable terminal authority and reopen prevention; log `linux-terminal-regressions-1.txt`. |
| `cargo clippy -p laminar-connectors --no-default-features --features FEATURE --lib --offline --target-dir target/schema-kafka -- -D warnings` | All 13 executed isolated feature sets passed: `iceberg-core`, `iceberg-catalog-rest`, `iceberg-storage-fs`, `iceberg-gcs`, `iceberg-azure`, `delta-lake`, `delta-lake-s3`, `delta-lake-azure`, `delta-lake-gcs`, `kafka`, `postgres-cdc`, `postgres-sink`, `mongodb-cdc`. |

Logs are in `target/schema-resolution/` and `target/schema-kafka/`.
The final workspace, Clippy, formatting and readability gates ran after the
version bump. Native integrations, examples, soaks and performance measurements
preceded that package-version-only change; they were not rerun at 0.32.0.
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
publication; the whole-catalog comparison below measures that additional phase.

Warm process RSS observations were 10,692-10,800 KiB; no growth occurred during
either decode percentile loop. Cold-run HWM reached 60,488 KiB. These are cumulative,
order-dependent process observations, not isolated peak memory per operation.
Allocation bytes count requests, including reallocations, and are not live memory.
Criterion intervals and percentile samples differ; cold mocks and a development
WSL2 machine do not establish production latency or an external-service SLO.

## Full creation and graph performance

The new `catalog_creation` fixture executes an explicit generator source, a
projection stream and a JSON file sink. Runtime/context/directory construction
and cleanup are outside the timer. The same fixture ran against archived
`009d8d5` production sources and the implementation. The first attempted Parquet
control failed because the starting commit rejected the native format during
creation; JSON provides a supported common workload. The published Parquet example
is independently exercised by `schema_sql_examples`.

| Whole creation, 200 samples | Original p50 / p95 / p99 | Current p50 / p95 / p99 |
| --- | --- | --- |
| Ephemeral | 0.364384 / 0.479782 / 0.609330 ms | 0.441509 / 0.547994 / 0.611570 ms |
| Local checkpoint configuration | 0.499822 / 0.603727 / 0.924368 ms | 40.792474 / 43.422456 / 46.146588 ms |

Criterion means were 464.73 / 458.44 us for ephemeral creation and 0.52042 /
65.973 ms with local checkpoint configuration. The original does not durably
publish resolved contracts at creation; current creation pays for publication
before activation. The current durable mean's interval was 54.187-78.941 ms,
showing filesystem variability distinct from the separate percentile sample.
Fixtures use the container's `/tmp` filesystem. These whole-creation measurements
do not isolate per-operation allocation or peak memory.

Both trees ran `cargo +1.99.0 bench --profile soak -p laminar-db
--no-default-features --features files,cluster --bench stream_executor_bench
--offline -- catalog_creation --sample-size 40 --measurement-time 3
--warm-up-time 1 --save-baseline NAME`, with `NAME=original-catalog` and
`schema-json-after`. Logs: `linux-original-catalog-bench-2.txt` and
`linux-catalog-bench-after-1.txt`.

Graph measurements bracket the replay change, after schema commit `31071aaf`.
They use that same Cargo prefix with filter `graph_admission|agg_group_by`,
40 samples, three-second measurements and one-second warmup. The before run
saves `schema-before`; the after run selects `--baseline schema-before`.

| Graph / aggregate benchmark | Before mean | After mean | Criterion conclusion |
| --- | --- | --- | --- |
| Aggregation, 1,024 rows / four groups | 159.03 us | 156.21 us | No significant change |
| Single admission path | 167.39 us | 167.79 us | No significant change |
| Four-way fanout | 177.22 us | 175.05 us | No significant change |
| Wide four-way fanout | 1.5217 ms | 1.4226 ms | No significant change |
| Two-input union | 184.59 us | 182.33 us | No significant change |

Relevant core checks use `cargo +1.99.0 bench --profile soak -p laminar-core
--features cluster --bench latency_bench --bench streaming_bench --offline --
--sample-size 40 --measurement-time 3 --warm-up-time 1`, saving/selecting
`schema-before`. Window assignment measured 1.3926 / 1.4019 ns. One unchanged
core case, `accepted_push/typed_4096`, reported +11.159%, then +6.8728% on a longer
isolated repeat. Consecutive 60-sample five-second controls on the identical binary
measured 15.715 / 17.709 us (+11.081%). Its SHA-256 is
`0c872de640cbe9fba70e89236860ad94e0ef956005a2c5c93bf771eca7224672`, with
modification time 21:57:09 UTC, preceding both measurements. Neither core code nor
that binary changed. This explains the timing outlier as demonstrated runner
variation; it does not turn it into a claimed throughput improvement. Logs include
`linux-core-bench-before-1.txt`, `linux-core-bench-after-1.txt`,
`linux-core-typed-repeat-1.txt`, `linux-core-control-before.txt` and
`linux-core-control-after.txt`. The original baseline is retained.

Linux `perf stat -e cycles:u,instructions:u` measured window-kernel IPC 5.18 / 5.20.
The broader DB profiles measured 0.86 / 0.88 and include runtime/subscription and
untimed DDL distribution work, so they do not establish record-loop IPC.
User-space 99-Hz sampling succeeded with data stored on the Linux filesystem;
the before run captured 1,122 samples without loss, with aggregate application at
13.17% of sampled CPU time. DWARF and Windows bind-mount recording attempts failed;
frame-pointer sampling provides function symbols with limited unwinding depth.
The graph change adds no ordinary-path allocations, locks, hashing or async
boundaries. Pressure-driven concatenation reuses the existing bounded coalescer.

## Recovery and failed runs

The default local exact four-kill soak passed in 79.07 s. The default three-node
Kafka-output at-least-once soak passed in 540.85 s with four kills, 96 input
partitions, 400 rows/s, a 500 ms checkpoint cadence and 90 s of steady work.
All temporal/window output oracles and hot/checkpoint latency gates remained on.

The default three-node Iceberg leader-restart test passed in 68.23 s against
the repository REST/MinIO stack. It verified one snapshot per checkpoint,
reconciliation and exact output after restart. This does not qualify an external
AWS catalog/storage deployment.

Before the replay fix, two full default Delta exact four-kill soak executions
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

The first full replay-fix run completed all four kill/rejoin rounds (two leaders
and two followers), without a graph budget failure. It failed at 624.01 s on the
unchanged ten-second temporal ASOF Delta visibility boundary: version 129 exposed
229,998 of 236,237 expected pairs; the frozen input prefix later became durable
through checkpoint 136. The bounded join boundary was exact with 847,052 rows;
nullable temporal/probe canaries and all four CoreWindow rows passed. All 331
pipeline-stall observations were within 1,024 ms, with zero exact timing SLO
violations. The default zero retained-state floor makes state-capture timing
observational. This execution is a failed full gate. Log:
`linux-cluster-eo-replay-1.txt`; server SHA-256:
`456254a5f0fd945630bb5a873756da9dc763ca2ec28cddc871ec8c48f9403c6d`.

The identical quiet repeat on a fresh broker also completed all four recovery
rounds without graph budget failure, then failed at 681.61 s on the same output
boundary: version 125 contained 253,886 of 258,223 temporal pairs. Its bounded
join boundary was exact with 927,689 rows. Temporal/probe and CoreWindow canaries
passed, all 321 pipeline-stall observations were within 1,024 ms, and exact timing
reported zero SLO violations. Log: `linux-cluster-eo-replay-3.txt`; the binary
digest was unchanged. The intervening second attempt stopped before the workload
on the retained broker's partition limit (`linux-cluster-eo-replay-2.txt`).

An independent read with installed PyArrow 20.0.0 reconstructed the retained
append logs and read every referenced Parquet file. The latest temporal snapshots
at runner termination were versions 132 and 128, with 234,377 and 257,330 unique
pairs, zero duplicates and zero unexpected pairs. They still lacked 1,860 and
893 expected pairs. The failed harness had stopped its nodes, so this inspection
does not establish eventual completion or classify the remaining probes as lost.
Log: `temporal-output-independent.txt`. The full Delta output visibility gate
remains failed and is a release/merge limitation; it is not downgraded to an
observational pass.

Read-only inspection of the repeat's committed checkpoint 132 found both temporal
source watermarks at `1791416558921`. The retained right-topic closing sentinel's
temporal timestamp was `1791416560921`, exactly the configured two-second
out-of-order allowance ahead. The durable frontier therefore includes that
sentinel's progress. This does not identify the cause of the visibility delay
or justify publishing a speculative temporal frontier. No ASOF runtime change
was made. Logs: `checkpoint-frontier-inspection.txt` and `closing-sentinel.txt`.

The exact replay-fix soak command is:

```text
cargo +1.99.0 test --profile soak -p laminar-server --no-default-features --features cluster,aws,kafka,delta-lake-s3,iceberg --test cluster_soak --offline three_node_eo_join_kill9_soak -- --ignored --exact --test-threads=1 --nocapture --color never
```

Its environment supplies `LAMINAR_SOAK_KAFKA_SOURCE_BROKERS=127.0.0.1:19092`,
`LAMINAR_SOAK_CHECKPOINT_URL=s3://laminardb-soak/schema-resolution-replay`,
`LAMINAR_SOAK_S3_ENDPOINT=http://127.0.0.1:19000`, task fixture access/secret keys,
`LAMINAR_SOAK_S3_REGION=us-east-1` and `LAMINAR_SOAK_DELTA_BUCKET=laminardb-soak`.
Checkpoint, hot-path and output visibility SLO modes retain their default
`certify` settings, including the default workload and unchanged timeouts.

The first optimized replay rebuild reused incompatible internal artifacts after
the archived baseline shared the same Cargo target cache. It failed on APIs that
exist in the current source. Updating current crate-root modification times,
without changing contents, forced the current internal crates to rebuild. The
second build passed in 16 m 19 s. No feature admission or dependency was changed
to resolve this cache issue, and all earlier binaries/logs are preserved.

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
