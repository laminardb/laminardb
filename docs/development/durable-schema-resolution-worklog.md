# Durable connector schema resolution

Status: schema committed; replay validation complete with a remaining output gate, 2026-10-08.

## Baseline

- Clean `main`, commit `009d8d5848cc380b1b125ac716afc5cada24dbfe`.
- Feature branch: `codex/durable-schema-resolution`.
- LaminarDB 0.31.0; Arrow/Parquet/arrow-avro 58.4.0; DataFusion 53.1.0;
  apache-avro 0.21.0; rdkafka 0.39.0; reqwest 0.13.5 (existing lakehouse
  dependency also uses 0.12.28); tokio-postgres 0.7.18; Delta Lake 0.32.4;
  Iceberg 0.10.1; MongoDB 3.9.1; SHA-256 0.10.9.
- Rust 1.99.0, Windows MSVC and cached Linux Docker; nightly formatting.
- Read root AGENTS.md. Referenced private architecture/memory files are absent
  from this checkout; source, rustdoc, public documentation and tests are authoritative.

## Completed implementation

Schema implementation is committed as `31071aaf`.

- Inventory every registered direction/format and require factory capabilities.
  Reuse existing metadata APIs; distinguish explicit, built-in, metadata, query
  and opt-in bounded sampling policies.
- Persist versioned lossless Arrow/native contracts and deterministic SHA-256
  fingerprints. Keep original DDL, reader/query schemas, native writers and
  mappings distinct. Revalidate authority/dependencies and publish before activation.
- Integrate Kafka Avro concrete identities/references, actual historical writers,
  bounded single-flight/backpressure and prepared sink encoding.
- Integrate PostgreSQL CDC/reference/lookup/sink metadata and named transactional
  writes; Delta/Iceberg identity/native field mappings; MongoDB validators/UUIDs;
  deterministic file metadata/sampling; built-in/query policies for other factories.
- Preserve deployment/delivery/SQL admission, external preparation permission,
  terminal recovery authority and advancing data cursors under frozen logical schemas.
- Publish tested configuration, SQL, migration and rollback guidance in
  [SCHEMA_RESOLUTION.md](../SCHEMA_RESOLUTION.md).

The replay follow-up retains the 256-batch and byte caps. Only interval-join
output to initialized, unfiltered COUNT/MIN/MAX plans with column/literal
projections can reuse the existing bounded coalescer. Weighted/oversized input
retains its boundaries; every fanout destination passes admission before publication.
Unsafe or still-over-budget output remains terminal. No new queue or recovery
protocol is introduced.

## Executed validation

- Schema workspace gate: 6,302 passed, zero failed, five existing ignored;
  strict all-feature/all-target and no-default-feature Clippy passed.
- Replay regressions: five real-operator tests pass, including 273 batches,
  result equivalence, fanout atomicity, byte limits and terminal retry/drain fencing.
  Replay all-feature/all-target Clippy, nightly formatting and readability pass.
  No frozen readability exception grows (18 modules, 208 functions).
- All 13 isolated connector feature checks pass. Native conformance 10/10,
  PostgreSQL 5/5, MongoDB 4/4, Kafka 3/3 and Iceberg REST/MinIO 6/6 pass.
  All six published SQL examples pass through ordinary SQL execution.
- Default local exact four-kill soak passes (79.07 s); three-node Kafka-output
  at-least-once four-kill soak passes (540.85 s); Iceberg leader restart and
  one-snapshot-per-checkpoint test passes (68.23 s).
- Untouched baseline reproduces 273 replay batches against the 256 cap. The
  first replay-fix full Delta run completes all four recovery rounds without
  that failure, then misses the unchanged ten-second temporal output boundary.
  An identical quiet repeat fails the same visibility boundary after all four
  recoveries. The full Delta gate remains failed; exact output was not certified.
- Before/after graph benchmarks show no significant change. Whole creation,
  cold resolution, bounded misses, warm codec, allocation and CPU measurements
  are complete. Prepared Avro encoding is 27.852 us versus 1.5600 ms per baseline
  batch; committed-reader decoding costs 13.6% with unchanged measured allocations.
  Durable creation pays for publication; measurements do not claim zero overhead.

The [validation report](durable-schema-resolution-validation.md) records exact
commands, logs, historical failures, benchmark controls and remaining limits.
Local MinIO and mocks do not qualify external cloud services or production SLOs.

## Next action and environment

Commit the replay fix, bump owned Cargo packages to 0.32.0 and run final gates.
The user has an optional scope question pending about the remaining temporal
output investigation versus a draft PR with the failed gate documented.
Dependencies are cached offline; native Windows/Docker access uses approved
sandbox escalation. Test services are isolated task fixtures. Logs are in
`target/schema-resolution/` and `target/schema-kafka/`.

Automatic approval review rejected deleting completed-soak Kafka topics without
topic-specific authorization. Old task brokers and their data are retained,
stopped; fresh isolated brokers supply repeats. No deletion bypass is used.
