# Durable connector schema resolution

Status: implementation in progress, 2026-10-07.

## Baseline

- Clean `main`, commit `009d8d5848cc380b1b125ac716afc5cada24dbfe`.
- Feature branch: `codex/durable-schema-resolution`.
- LaminarDB 0.31.0; Arrow/Parquet/arrow-avro 58.4.0; DataFusion 53.1.0;
  apache-avro 0.21.0; rdkafka 0.39.0; reqwest 0.13.5 (existing lakehouse
  dependency also uses 0.12.28); tokio-postgres 0.7.18; Delta Lake 0.32.4;
  Iceberg 0.10.1; MongoDB 3.9.1; SHA-256 0.10.9.
- Rust 1.99.0, Windows MSVC; stable and nightly toolchains available.
- Read root AGENTS.md. Referenced private architecture/memory files are absent
  from this checkout; source, rustdoc, public documentation, and tests are the
  authority here.

## Findings

- Kafka Avro and OTLP implement the existing pre-open source discovery hook.
- Cluster catalog manifests currently store defining DDL only. Schema-less
  connector sources are rejected before discovery; removing that guard alone
  would permit nondeterministic catalog replay.
- Registered families include generator, Kafka, PostgreSQL CDC/sink, MongoDB
  CDC/sink, NATS, Delta, Iceberg, files, WebSocket, OTLP, and testing extensions.

## Phases

1. Trace all creation/replay paths and inventory direction/format capabilities.
2. Implement shared typed contracts, deterministic persistence, and fenced
   control-plane resolution/publication.
3. Integrate Kafka reader/writer contracts and existing registry/codec paths.
4. Integrate remaining metadata-capable connectors and precise policies elsewhere.
5. Harden recovery, resource bounds, drift checks, and feature coverage.
6. Update configuration/SQL documentation and run validation/benchmarks.

## Implementation status

- Shared versioned Arrow/native contracts, pure mapping validation, canonical SHA-256
  fingerprints, required factory capabilities, and bounded control-plane resolution.
- Cluster manifests retain original DDL plus concrete bindings; legacy explicit
  records reconstruct deterministically. New bootstrap resolves an isolated catalog,
  runs existing admission, prepares authorized targets, seals, then installs.
- Local durable creation uses conditional writes in the checkpoint namespace and
  the existing exclusive filesystem lease. Ephemeral embedded creation stays in memory.
- Kafka Avro creation resolves concrete subjects/IDs and bounded references. Readers
  use actual message writers with a committed reader; sinks register only during
  explicit preparation and use a prepared batch encoder.
- PostgreSQL sink metadata maps named columns and validates defaults/generated
  fields/keys. Writes validate native identity and layout under a transaction lock.
- Delta/Iceberg native contracts preserve table identity, schema/protocol/field IDs.
  Files resolve deterministic Parquet/IPC metadata or explicit bounded sampling.
- PostgreSQL CDC and MongoDB change streams bind their native identity and fixed
  envelope; relation/collection changes fail before acknowledged progress. Reference
  tables now resolve native PostgreSQL/Delta/Iceberg metadata before hydration.
- MongoDB lookup discovery now uses closed collection validators and native UUIDs;
  sink creation is explicitly controlled by `auto.create`, with read-back validation.
- Every built-in factory has a direction/format policy. Portable conformance tests
  exercise codecs, metadata and authorized preparation; native service tests are running.
- Public configuration, recovery and migration guidance is in `docs/SCHEMA_RESOLUTION.md`.
- Remaining work: finish service-backed coverage, test the published SQL examples,
  run full feature gates and existing failure/soak suites, and repeat benchmarks.
- Final review removed obsolete Delta deferred-initialization comments/attributes.
  An initializing sink now rejects even an empty write; successful open is required.

## Validation (executed)

- PASS: minimal connectors baseline check (before changes).
- PASS: all nine schema-binding tests, including nested metadata, native defaults/
  union ordering, resource identity, control columns and bounded untrusted input.
- PASS: cargo check -p laminar-db --no-default-features --features cluster,files
  --offline --target-dir target/schema-resolution before bootstrap extraction.
- PASS: cargo check -p laminar-connectors --no-default-features
  --features postgres-sink,postgres-cdc,delta-lake,iceberg,files,kafka --offline
  --target-dir target/schema-kafka, including prepared codec and transaction validation.
- PASS: broad connector Clippy with all targets and Kafka, PostgreSQL CDC/sink,
  MongoDB CDC, Delta, Iceberg, files, NATS, WebSocket and OTLP features.
- PASS: readability gate: 18 module exceptions, 208 function exceptions. Removed
  exceptions were deleted from the baseline; no production exception was enlarged.
- PASS: Linux workspace library gate: 6,281 passed, zero failures, five ignored.
  Connector/core/DB/SQL totals were 2,000 / 1,140 / 2,271 / 870 respectively.
- PASS: strict workspace Clippy, all features/all targets and no default features.
  Both are being repeated after the latest format/key validation fixes.
- PASS: five real PostgreSQL native metadata, named sink, reference/lookup, replacement
  and publication tests; four MongoDB validator/UUID/query-writer tests.
- PASS: three real Kafka broker tests: empty-topic metadata, historical writer/default
  projection and independent sink decoding, two restarts without registry writes,
  sustained unknown-writer outage/backpressure with unchanged accepted offsets.
- PASS: all six existing/extended Iceberg REST/MinIO integration tests, including
  native field IDs, named query mappings, resource replacement and checkpoint replay.
- PASS: existing default local exactly-once four-kill recovery soak (79.07 seconds).
  All default workload, time and correctness assertions were retained.
- PASS: all ten final registered-factory conformance tests, including native format
  rejection, WebSocket binary readers, file inference bounds, replaced Delta identity,
  and actual reordered writes while the frozen reader follows new data versions.
- PASS: the existing default three-node at-least-once four-kill join recovery soak
  (540.85 seconds), with its full temporal/window workload and latency gates.
- Expanded workspace run: all 2000 connector tests passed; one filesystem list/delete
  test exceeded its unchanged five-second bound during concurrent builds. All 19
  filesystem-store tests then passed using the same test binary in 0.63 seconds.
  The final workspace run bounds test-worker concurrency to eight.
- SQL examples exposed native file-format handoff, quoted option keys, environment
  substitution and primary-key identifier matching. These now use shared existing
  parser/configuration semantics. The relational example's regular snapshot join was
  rejected by the existing planner; it now uses the supported on-demand lookup join
  and separately tests reference-snapshot metadata discovery.
- Exactly-once cluster soak setup hit the task broker's partition limit after prior
  runs. The first fully executed Delta run failed during the third kill round when
  replay produced 259 batches against the unchanged 256-batch graph budget. Its
  terminal recovery fence remained closed. No budget, workload, assertion, or timeout
  has been weakened. A quiet repeat on an isolated fresh broker passed the first
  two kill/rejoin rounds, then hit the same budget at 264 batches on the third
  round. An untouched baseline build supplied the differential diagnosis below.
- The untouched baseline build and the identical default soak also failed: 273
  replay batches against the same 256-batch graph budget during the first node
  rejoin. Baseline server SHA-256:
  `6862362fbb885a89a2bd222563fcb6c0a46c4a08f5e7baf0738483db82c56fdd`.
  This establishes a pre-existing failure; the Delta exact soak remains a failed
  gate, not a pass or a new delivery certification.
- PASS: the existing three-node Iceberg leader restart test (68.23 s), with one
  native snapshot per checkpoint and exact output after reconciliation/restart.
  This is local REST/MinIO protocol coverage, not external AWS qualification.
- The expanded final workspace run passed 2008 connector and 1140 core tests.
  A new dependency-generation fixture did not register its source with the manager;
  it now uses an ordinary registered generator and asserts the generation change.
  Six conditional-store probes and one timed AI test also failed during concurrent
  optimized compilation. The same binaries passed all nine checkpoint namespace
  tests (2.67 s) and the timed AI case (0.73 s) in quiet isolated runs. The full
  workspace gate will be rerun sequentially.
- Published PostgreSQL SQL reached native sink configuration and exposed a missing
  runtime-owned delivery property in its strict allowlist. It now accepts that
  existing property without changing write mode, and the guide uses the actual
  `hostname` and `table.name` options. All six SQL examples will be repeated.
- The latest all-feature Clippy run found a unit-valued `black_box` in the new
  benchmark; that call was removed. Final strict gates remain pending.
- First optimized same-workload codec benchmark (512 rows, 30 Criterion samples):
  baseline Avro encoding mean 1.6039 ms; prepared encoding 27.262 us. Unpinned
  decoding mean 11.222 us; committed-reader decoding 11.079 us. Batch p50/p95/p99,
  bounded registry misses and concurrent single-flight latency were also measured.
  Source percentile uncertainty requires a quiet repeat; allocation instrumentation,
  final full-workspace Clippy and remaining process-failure soaks are in progress. These figures are
  codec batch results, not end-to-end pipeline latency or delivery certification.
- Final warm source comparisons now use the starting commit's original decoder
  loop, mutex, batching and flush algorithm, rather than the modified decoder in
  unpinned mode. Both decode the identical independently checked 512-row fixture.
- Final lifecycle review found reference-table `IF NOT EXISTS` could re-enter
  resolution after preparation recognized the existing table. The existing-object
  check now precedes handler resolution. The durable retry/outage regression passes.
- PASS: final schema workspace run, 6,302 tests and five existing ignored tests;
  strict all-feature/all-target and no-default-feature Clippy; formatting and readability.
  All six published SQL examples passed against ordinary SQL execution, including PostgreSQL.
- Final quiet same-workload codec timing and separate allocation runs passed.
  Prepared encoding mean 27.852 us versus baseline 1.5600 ms; committed-reader
  decoding 10.882 us versus the faithful original baseline 9.5822 us (13.6% cost).
  Warm decoding allocation requests/bytes are unchanged. Exact percentiles, memory
  limitations and cold-stage results are in the validation report.

## Environment and next action

Native builds need sandbox escalation to launch bundled protoc/native tools on
Windows; approved builds work. Dependencies are cached offline. Task-owned PostgreSQL,
MongoDB, Redpanda and two MinIO stacks are available. Linux Docker uses the repository's
cached Rust 1.99 build image. Next: complete final workspace/SQL gates, cluster Delta/Iceberg recovery
soaks, cold/warm/allocation benchmarks and final feature/gate validation. Logs are under
target/schema-kafka and target/schema-resolution.

Automatic approval review rejected deleting six completed-soak Kafka topics as
irreversible without topic-specific authorization. That broker and all its data
are retained in a stopped container; a new task broker supplies the next full run.
