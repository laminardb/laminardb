# Changelog

## [Unreleased]

### Added

- Event time flows through interval joins and closed windows, in every mode. An interval join's
  output carries the time column of a side every output row has, when it projects that column
  unchanged: the left one for `INNER` (else the right), the left one for `LEFT`, `LEFT SEMI` and
  `LEFT ANTI`, the right one for the `RIGHT` kinds, and none for `FULL`. A closed `TUMBLE` or
  `HOP` window carries its projected end marker, or else its start marker. `EXPLAIN` shows the
  column.
- An interval join can read another interval join's stream on that stream's event-time column,
  in every mode.
- Cluster mode admits a window over an interval join stream and a window over a closed window
  stream, on the event-time column the input carries.
- An append-only view over an interval join keeps an interval on the join's event-time column, so
  it no longer has to declare `retention = '<n> rows'`.
- A chain of interval joins over three or more named inputs (`a JOIN b ON .. JOIN c ON ..`, each
  `ON` with a `BETWEEN` bound) runs in every mode, in a stream or a materialized view. It is split
  into one internal stage per join, each admitted as a two-way join over the stage before it, so
  each later join must bound its time by the event-time column the earlier joins carry; `CREATE`
  names the column otherwise. In cluster mode the chain counts toward the three-exchange depth
  limit like named stages.

- `MIN` and `MAX` work over changelog inputs: an `EMIT CHANGES` aggregate, a changelog
  materialized view, a CDC or change-feed source, and the second stage of a salted aggregate.
  Each group keeps every value it holds with its net weight, so retracting the current extreme
  falls back to the next one; the values count toward `managed_state_bytes` and the memory
  budgets. Embedded and single-node admit them with or without `GROUP BY`. Cluster mode admits
  them with a `GROUP BY` and still rejects a global (ungrouped) one at `CREATE`.

### Changed

- A multi-way join that is not a chain of interval joins over named inputs (with a lookup or
  temporal table, `USING`, a subquery input, or no interval bound) is still rejected, with a
  message that says which multi-way joins split. Cluster mode reports a window over a join that
  cannot be split with the one-input window message instead of the windowed-join message.
- `CREATE` rejects a window over a window stream that groups by any column other than the one the
  upstream carries (for example its start when it projects its end), and a window over a session
  window or over a window that projects no boundary. Such a window could close before the
  upstream emitted into it. The check runs whenever the DDL runs, so after upgrading, an embedded
  or single-node pipeline with such a window fails to start until it windows on the carried
  column. Cluster mode never admitted these windows.
- `TUMBLE`, `HOP` and `SESSION` windows keep their `COUNT`, `SUM`, `AVG`, `MIN` and `MAX` state
  in the same form as non-windowed aggregates, in every mode. A windowed `SUM` or `AVG` whose
  state would overflow now fails the operator's cycle with an error, as a non-windowed one does,
  instead of wrapping. A floating-point `SUM` or `AVG` adds its values in row order, so a result
  can differ in its last bits from earlier releases.

### Fixed

- A window over another window's output no longer drops the upstream's rows. A window stream's
  watermark now trails the watermark its windows close at: by the allowed lateness when it
  projects `tumble_end` or `hop_end`, and by the lateness plus the window size otherwise.
  Downstream windows, append-view retention and `stream_watermark_ms` see the held watermark.
  Session windows are not held. This applies in every mode.
- An append-only view that projects a window's end but not its start (`tumble_end` or `hop_end`
  grouped with `tumble` or `hop`) keeps an interval on the end column. `CREATE` used to fail for
  lack of an event-time column. A `TUMBLE` or `HOP` view now keeps a window while the window's end
  is within the interval of the watermark its windows close at, less the allowed lateness,
  whichever boundary it projects.
- An append-only session view keeps a session while its start is within the interval plus the gap
  and the allowed lateness. A session view whose `ALLOW LATENESS` exceeded its retention used to
  drop every session as it closed.
- `ALLOW LATENESS` is applied. It used to be parsed after the `EMIT` clause and then ignored. It
  now keeps windows open in embedded and single-node mode. It must follow `EMIT ON WINDOW CLOSE`,
  and `CREATE` rejects it with `EMIT FINAL`, with other `EMIT` modes, on a query without a
  window, and in cluster mode. Any other text after the `EMIT` clause is now a parse error.

### Upgrade

- A stream or view whose DDL already carried `ALLOW LATENESS` gets a new pipeline identity, so its
  existing checkpoints fail closed; it restarts from its sources.
- The state frame ABI is now 9, for the window state change above. Every checkpoint written by an
  earlier release fails closed at startup, in every pipeline and mode, and the pipeline restarts
  from its sources.

## [0.33.0]

### Added

- RabbitMQ source and sink (`rabbitmq` feature, off by default) over AMQP 0-9-1. The source
  acknowledges deliveries only after the checkpoint holding them is durable and can dead-letter
  undecodable messages; the sink publishes with confirms and `mandatory` and drains every pending
  confirm before a checkpoint commits. See `docs/RABBITMQ.md`.
- IBM MQ source and sink (`ibmmq` feature, off by default; IBM's client library is loaded at run
  time and never bundled). The source commits its unit of work only when a durable checkpoint
  covers it and can move poison messages to the backout queue in the same unit of work; the sink
  commits bounded units of work per write. See `docs/IBMMQ.md`.
- Both message sources run embedded and single-node; cluster mode rejects them. Both sinks also
  run in cluster mode as at-least-once writers.
- `FORMAT RAW` with a single Binary `value` column keeps payload bytes exactly.
- FIX TagValue source (`fix` feature, off by default) on the QuickFIX C++ engine: one initiator
  session per source (FIX.4.2, FIX.4.4, or FIXT.1.1 with one FIX 5.0 application version) over
  verified TLS or loopback TCP, a group-committed receive journal written inside the engine
  callback, a source-owned session store that never persists the inbound sequence number past a
  record that is not yet durable, dictionary-validated column mappings for header, body, group,
  and metadata fields, deterministic replay, and at-least-once or exactly-once delivery in
  embedded and single-node deployments. Cluster placement is rejected. See `docs/FIX_SOURCE.md`.
- FpML documents (`fpml` feature, off by default) on the files, Kafka, RabbitMQ, and IBM MQ
  sources (`FORMAT FPML`), and in a FIX data field such as SecurityXML (`fpml.field`, an appended
  `fpml` struct column). Each document is validated against the bundled FpML 5.13 confirmation
  schema with a vendored, network-free libxml2, its `id`/`href` references are checked, and a
  profile turns it into typed rows with exact decimals and dates. Fourteen bundled profiles cover
  every product, loan representation, and document type in the schema; your own schemas and
  profiles extend them without code. Embedded and single-node only. See `docs/FPML_SOURCE.md`.
- Certified commit-coupled sources are admitted for exactly-once delivery; uncertified ones are
  still rejected.
- FIX source `logon.field.<tag>` for static venue-specific Logon fields, and
  `counterparty.resends = 'false'` for counterparties that do not retransmit: gap fills and
  new sequence series then flag the next row `gap_before`.
- FIX source `venue = 'binance'` profile for Binance Spot drop copy: an Ed25519-signed Logon, a
  sequence reset at every logon, and `gap_before` on the first row of every later series,
  because Binance does not retransmit.
- `examples/fix-desk`: a trading desk over a FIX drop copy in SQL only (positions, per-minute
  flow, rejects, large fills, order bursts, drop-copy delay, feed health, and an exactly-once
  Delta Lake audit trail reconciled in DuckDB), with one connection file per counterparty and a
  local broker simulator (`laminar-connectors` example `fix_venue`).

### Changed

- The prebuilt server binaries for Linux (glibc), macOS, and Windows include the `fix` and `fpml`
  features, and their archives carry the third-party notices in `licenses/`. The Linux musl
  binary does not include them yet.

### Fixed

- Kafka sources with `metadata.columns` restart on persistent storage: declared columns are
  checked as the prefix of the committed schema binding.
- Managed aggregates coerce mixed operand types in group keys, aggregate arguments, and `WHERE`
  (`SUM(CASE WHEN ... THEN decimal ELSE 0 END)`, `double * 1000`, `decimal >= 1`) instead of
  failing at runtime with `LDB-8051`. A `CASE` whose branches cannot be narrowed exactly to its
  first branch's type is rejected at `CREATE` with a request to `CAST` them.

## [0.32.0]

### Added

- Kafka exactly-once output in embedded and single-node deployments: each checkpoint's records are
  published by one fenced transactional committer together with a cursor marker in a compacted
  `commit.topic`, so `read_committed` readers see every committed record once across crashes.
  Cluster Kafka output remains at-least-once.
- Kafka `latency.profile` (`low_latency`, `balanced`, `throughput`), `group.protocol`
  (`classic`, `consumer`), sink `key.format` and `partition.count`, and source
  `metadata.columns` (`key`, `topic`, `partition`, `offset`, `timestamp`, `timestamp_type`,
  `headers`).

### Changed

- librdkafka is 2.16.0 through a vendored `rdkafka-sys`; zstd compression needs the `kafka-zstd`
  feature.
- `kafka.*` passthrough properties that override a connector-owned or protected setting are
  rejected instead of ignored, and SASL mechanisms, TLS and codecs are checked against the linked
  librdkafka before a client is created. OAUTHBEARER is rejected.
- Kafka sinks route keyed records to `murmur2(key) % partitions` explicitly; upsert sinks require
  `partition.count`. Kafka sources preserve NULL versus empty keys and values, keep binary,
  duplicate and NULL headers in order, and advance their cursor over records that produce no row.
- Guaranteed Kafka append sources reject compacted input topics when the broker reports the
  topic's `cleanup.policy`; without DescribeConfigs permission the source starts with a warning.
  The Avro writer-schema cache evicts least recently used schemas instead of failing after 64,
  and a Schema Registry client certificate applies without a private CA.

### Fixed

- A sink reading an incremental materialized view directly binds the view's changelog, including
  its weight column, so a keyed Kafka upsert sink over a `GROUP BY` view no longer fails with a
  schema mismatch on its first write.

### Removed

- Kafka source `include.metadata` and `include.headers`; use `metadata.columns`.

## [0.28.0]

### Changed

- Stateful joins and non-windowed keyed aggregates now use one managed vnode ownership,
  checkpoint, restore, and rebalance lifecycle in single-node and cluster execution.
- The bounded append-only event-time join supports `INNER`, left/right/full outer, left/right semi,
  and left/right anti joins with ordered `BIGINT`/`VARCHAR` composite keys. A named join stream may
  feed a separate non-`DISTINCT` `COUNT`/`SUM`/`AVG`/`MIN`/`MAX` aggregate stage. Fused
  `JOIN ... GROUP BY`, cluster event-time window aggregates, and unsupported join shapes fail
  admission.
- Join output is capped at 262,144 rows and 64 MiB per cycle. Exceeding either cap is a terminal
  controlled failure; the operator does not spill or continue a partial fanout.
- Cluster exactly-once admission is connector-gated. The admitted composition is exact-certified
  Kafka input with coordinated append-mode Delta Lake on direct S3/S3A. Azure and GCS Delta targets
  remain cluster at-least-once pending provider-native EO soaks; Kafka and Iceberg sinks remain
  at-least-once.
- Checkpoint and recovery records now bind the exact deployment, pipeline, assignment, process
  roster, vnode inventory, source cut, state seal, and external publication cursor. Production
  certification remains pending the four-mode real-connector fault, validity, and latency soak
  matrix.
- The public `[state]` backend selector is removed; `[checkpoint]` is the sole durability
  configuration. Aggregate and window hot maps now use `FxHashMap`. The final one-node-object
  checkpoint cutover and production soak certification remain pending.
- Embedded, standalone, and cluster runtimes now share one configurable key-group topology with a
  default of 256. Local keyed aggregates and interval joins route across all locally owned vnodes;
  the unified v7 checkpoint image and remaining stateful operators are still pending.
- The v0.28 checkpoint and state format is intentionally incompatible with older persisted formats,
  which are rejected and require a fresh deployment namespace.

### Removed

- Removed the experimental tiered-state feature and configuration.
- Removed the public `VnodeRehydrator`, `VnodeRehydration`, and `RehydratedVnode` APIs and
  `LaminarDB::rehydrated_vnode_state()`. Restore is now an internal authority-pinned lifecycle.
- Removed obsolete WebSocket source-server, replay, connector-owned checkpoint, and connector-owned
  event-time options. WebSocket sources are client-only and best-effort; event time is declared by
  SQL watermarks.
- Removed superseded distributed-state and recovery plans from the active documentation set.

## [0.22.0]

### Added

- **Postgres wire protocol, production hardening**: full TLS path on
  the SUBSCRIBE listener: `pgwire_tls_min_version` to pin TLS 1.3
  (`bcab5471`, #12), optional mTLS via `pgwire_tls_client_ca` using
  `WebPkiClientVerifier` (`88e2af46`, #10), in-place hot reload of the
  `TlsAcceptor` via a `notify` watcher with debounce (`2cad3d54`, #11),
  and `pg_authid`-style pre-hashed `md5{32-hex}` passwords in
  `pgwire_users` so plaintext never has to live in `laminardb.toml`
  (`a3376408`, #7).
- **Extended-query + binary format** on the pgwire listener (`95fa8a74`).
- **SQL-level cursors**: `DECLARE c CURSOR FOR SUBSCRIBE …` +
  `FETCH n FROM c` for libpq clients with `\set FETCH_COUNT n`
  (`ab49408e`).
- **Explicit window-edge columns**: `tumble_end`, `hop_end`,
  `cumulate_end` UDFs return the window-end timestamp directly
  (`0a5b862e`).

### Changed

- **`DbState` collapse** (#381): removed the dual representation that
  carried both the in-flight `LaminarDB` handle and a snapshot of its
  contents. Trims docs/restated state.

## [0.21.0]

### Added

- **SUBSCRIBE over the Postgres wire protocol** (#369): `psql`, JDBC,
  asyncpg, and any libpq client can tail a stream or materialized view.
  Includes the listener itself, MD5 password auth, TLS, and a
  stream-side schema layer for `WHERE` push-down.
- **Pgwire hardening** (#371): connection caps, per-client throttling,
  certificate expiry warnings.
- **`RETAIN HISTORY` + `AS OF EPOCH`** (#373): `CREATE STREAM … WITH
  ('retain_history' = '64mb')` keeps a bounded ring of recent committed
  epochs in memory; `SUBSCRIBE … AS OF EPOCH n` resumes from epoch `n`
  so a reconnecting client doesn't miss rows produced during the
  disconnect.
- **Materialized view result storage and queryability** (#319): MV
  output is persisted via `mv_store` and exposed through a DataFusion
  `TableProvider`, so `SELECT * FROM mv_name` works after the pipeline
  has produced results.
- **Push-based WebSocket stream subscriptions** (#329): replaces the
  earlier poll loop on `/ws/{name}`.
- **Self-join pre-filtering** (#321): predicate analysis on
  `FROM a JOIN a …` cuts N² buffer growth before the join executes.
- **OTel demo + connector cleanup** (#317): schema auto-discovery,
  cleaner connector registration surface.
- **Production-grade Prometheus metrics** (#339): pipeline counters,
  cycle duration percentiles, checkpoint telemetry, sink error/timeout
  counters, per-connector gauges.
- **Throttled checkpoint barrier retries** (#327): back off rather
  than spin when a sink times out; clears `sink_timed_out` on success.

### Removed (dead-code waves)

- **#315**: Window operator unification deleted ≈13K LOC of DAG-only
  window operators.
- **#316**: Removed ≈23K LOC of dead DAG / checkpoint / storage paths
  (`core/dag/`, `core/state/`, `storage/checkpoint/`, `incremental/`,
  `wal.rs`). The single `StreamingCoordinator` tokio task is now the
  only execution path.
- **#320 / #326**: Removed ≈5K LOC from `laminar-db`, including the
  old `StreamExecutor` (4,652 LOC).

These deletions are not backwards-compatible. There were no public
APIs touched, but checkpoints from 0.20.x cannot be restored on 0.21+.

## [0.20.2]

### Added

- **Production-grade Prometheus metrics** (#339): expanded `/metrics`
  with pipeline counters, cycle duration percentiles, checkpoint
  telemetry, sink error/timeout counters, and per-connector gauges.
- **Temporal probe join DDL round-trip**: `CREATE STREAM` and
  `CREATE MATERIALIZED VIEW` now preserve the raw query text between
  `AS` and `EMIT` via a `query_sql` field on the AST, so custom
  streaming syntax (e.g. `TEMPORAL PROBE JOIN ... LIST (...)`,
  `RANGE FROM ... TO ... STEP ...`) survives the DDL parse / replay
  cycle used by `SHOW CREATE` and hot reload.

### Changed / Removed

- **Removed `WatermarkDynamicFilter`** (#338): the dead dynamic
  filter module was deleted. Watermark-based row filtering now runs
  through the standard streaming scan path.
- **Moved `examples/microstructure/pipeline.sql`** to use `TIMESTAMP`
  for the event-time column `"T"` (matches the v0.20.1 timestamp
  migration).
- **Deleted stale planning docs**: `docs/features/INDEX.md` and the
  `docs/plans/` directory were removed. `docs/ROADMAP.md` is the
  canonical feature tracker.

### Fixed

- **Watermark fallback wiring** (#337): `max.out.of.orderness.ms` is
  now respected when event-time extraction falls back to default
  generators. The attestation gate on the fallback path was removed.
- **Session window cardinality + CDC / sink shutdown** (#335):
  session windows cap per-key sessions to prevent unbounded state
  growth; sinks and CDC sources shut down cleanly when the pipeline
  stops.

## [0.20.1]

### Breaking: Timestamp column migration

Event-time columns throughout LaminarDB now use Arrow `Timestamp(_)`
instead of `Int64`-as-epoch-millis. The `interval_rewriter` is gone;
DataFusion handles `Timestamp ± INTERVAL` arithmetic natively.

**Schema changes in connector-produced columns:**
- OTel `_laminar_received_at`: `Int64` → `Timestamp(Nanosecond)`
- Postgres CDC `_ts_ms`: `Int64` → `Timestamp(Millisecond)`
- Kafka metadata `_timestamp`: `Int64` → `Timestamp(Millisecond)`
- MongoDB CDC `_wall_time_ms`: `Int64` → `Timestamp(Millisecond)`
- Files `_metadata.file_modification_time`: `Int64` → `Timestamp(Millisecond)`
- WebSocket `event.time.field` extraction: now reads any `Timestamp(_)`

**User-facing SQL changes:**
- Event-time DDL must declare `TIMESTAMP`, not `BIGINT`. Hand-written
  sources that used `ts BIGINT, WATERMARK FOR ts` need to switch to
  `ts TIMESTAMP, WATERMARK FOR ts AS ts - INTERVAL 'N' SECOND`.
- `INTERVAL` arithmetic is now native on `TIMESTAMP` columns.
  `ts BETWEEN t - INTERVAL '10' SECOND AND t + INTERVAL '10' SECOND`
  works. The old `t - 10000` numeric-arithmetic trick is no longer
  required.
- `tumble()` / `hop()` / `session()` UDFs accept any `Timestamp(_)`
  precision (Second / Millisecond / Microsecond / Nanosecond).
  Return type remains `Timestamp(Millisecond)`.

**Removed APIs:**
- `laminar_sql::parser::interval_rewriter` module
- `laminar_core::time::TimestampFormat` enum
- `EventTimeExtractor::from_column(name, format)`: now takes only `name`
- `laminar_db::db::infer_timestamp_format`
- `laminar_db::sql_analysis::infer_ts_format_from_batch`

**New APIs:**
- `laminar_core::time::cast_to_millis_array`: shared helper that
  normalises any `Timestamp(_)` array to `TimestampMillisecondArray`
  via Arrow's cast kernel. Used by the window UDFs, event-time
  extractor, interval-join key helper, and the WebSocket parser.

**Fixed:**
- `TUMBLE`/`HOP`/`SESSION` over `Timestamp(Nanosecond)` no longer
  errors with "Unsupported timestamp type" at runtime.
- Interval joins over `Timestamp(Nanosecond)` columns no longer
  error when extracting join keys.
- OTel `_laminar_received_at` unit-inference bug. Windows that
  used to be silently 1,000,000× smaller than declared (nanos
  treated as millis) now use real wall-clock durations.
- `WATERMARK FOR` in TOML config composes with connector schema
  auto-discovery. Columnless sources with a watermark clause are
  validated against the discovered schema instead of bailing out.

**Migration notes:**
- Existing checkpoints from v0.20.0 are **not** compatible. They
  reference Int64 timestamp columns in serialized operator state.
  Wipe `./data/checkpoints` before starting the server on v0.20.1.
- Hand-written SQL with `ts BIGINT` needs to change to `ts TIMESTAMP`
  if the column is used in a watermark or window. Plain `BIGINT`
  columns unrelated to event time are unchanged.

## [0.20.0] and earlier: notable additions

### Added: MongoDB CDC Source & Sink (`mongodb-cdc` feature, PR #255)

Landed in the 0.20.x line via PR #255. The feature provides MongoDB
change-stream source and write-sink connectors, CDC envelope types,
full-document modes, time-series sink configuration, and CDC replay writes.

The experimental `ResumeTokenStore` and `LargeEventReassembler` APIs later
proved unsafe or unwired and were removed. Resume tokens are no longer
persisted independently of the engine checkpoint, and split-large-event
handling is not advertised by the connector.
