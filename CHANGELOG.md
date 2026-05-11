# Changelog

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
- Postgres / MySQL CDC `_ts_ms`: `Int64` → `Timestamp(Millisecond)`
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

Landed in the 0.20.x line via PR #255; the API surface below is what
the `mongodb-cdc` feature exposes today.

New public API symbols in `laminar_connectors::mongodb`:

**Source Connector**
- `MongoDbCdcSource`: `SourceConnector` impl for MongoDB change streams
- `MongoDbSourceConfig`: connection, pipeline, full document mode config
- `FullDocumentMode`: Delta / UpdateLookup / RequirePostImage / WhenAvailable
- `mongodb_cdc_envelope_schema()`: Arrow schema for CDC envelope records

**Sink Connector**
- `MongoDbSink`: `SinkConnector` impl for MongoDB writes
- `MongoDbSinkConfig`: connection, batching, write concern config
- `WriteMode`: Insert / Upsert / Replace / CdcReplay
- `WriteConcernConfig`, `WriteConcernLevel`: write durability settings

**Change Events**
- `MongoDbChangeEvent`: typed change stream event
- `OperationType`: Insert / Update / Replace / Delete / Drop / Rename / Invalidate
- `UpdateDescription`: delta fields for update events
- `Namespace`: database + collection pair
- `TruncatedArray`: truncated array metadata (MongoDB 6.0+)

**Resume Token Persistence**
- `ResumeTokenStore` trait: pluggable async persistence
- `ResumeToken`: opaque resume token wrapper
- `FileResumeTokenStore`: file-based persistence (embedded/test)
- `MongoResumeTokenStore`: MongoDB-backed persistence (production, feature-gated)
- `InMemoryResumeTokenStore`: in-memory persistence (testing)
- `ResumeTokenStoreConfig`: configuration enum (File / Mongo / Memory)

**Time Series Support**
- `CollectionKind`: Standard / TimeSeries
- `TimeSeriesConfig`: time field, meta field, granularity, TTL
- `TimeSeriesGranularity`: Seconds / Minutes / Hours / Custom

**Large Event Handling**
- `LargeEventReassembler`: fragment reassembly for split events (MongoDB ≥ 6.0.9)
- `EventFragment`, `SplitEventInfo`: fragment types

**Write Model**
- `MongoWriteOp`: InsertOne / ReplaceOne / UpdateOne / DeleteOne / Lifecycle
- `build_update_document()`: $set/$unset update document builder
- `cdc_replay_op()`: operation type to write op translation

**Metrics**
- `MongoSourceMetrics`: lock-free source connector metrics
- `MongoSinkMetrics`: lock-free sink connector metrics

**Registry**
- `register_mongodb_cdc()`: registers source with ConnectorRegistry
- `register_mongodb_sink()`: registers sink with ConnectorRegistry
