# Changelog

## [0.20.1]

### Breaking — Timestamp column migration

Event-time columns throughout LaminarDB now use Arrow `Timestamp(_)`
instead of `Int64`-as-epoch-millis. The `interval_rewriter` is gone;
DataFusion handles `Timestamp ± INTERVAL` arithmetic natively.

**Schema changes in connector-produced columns:**
- OTel `_laminar_received_at` — `Int64` → `Timestamp(Nanosecond)`
- Postgres / MySQL CDC `_ts_ms` — `Int64` → `Timestamp(Millisecond)`
- Kafka metadata `_timestamp` — `Int64` → `Timestamp(Millisecond)`
- MongoDB CDC `_wall_time_ms` — `Int64` → `Timestamp(Millisecond)`
- Files `_metadata.file_modification_time` — `Int64` → `Timestamp(Millisecond)`
- WebSocket `event.time.field` extraction — now reads any `Timestamp(_)`

**User-facing SQL changes:**
- Event-time DDL must declare `TIMESTAMP`, not `BIGINT`. Hand-written
  sources that used `ts BIGINT, WATERMARK FOR ts` need to switch to
  `ts TIMESTAMP, WATERMARK FOR ts AS ts - INTERVAL 'N' SECOND`.
- `INTERVAL` arithmetic is now native on `TIMESTAMP` columns —
  `ts BETWEEN t - INTERVAL '10' SECOND AND t + INTERVAL '10' SECOND`
  works. The old `t - 10000` numeric-arithmetic trick is no longer
  required.
- `tumble()` / `hop()` / `session()` UDFs accept any `Timestamp(_)`
  precision (Second / Millisecond / Microsecond / Nanosecond).
  Return type remains `Timestamp(Millisecond)`.

**Removed APIs:**
- `laminar_sql::parser::interval_rewriter` module
- `laminar_core::time::TimestampFormat` enum
- `EventTimeExtractor::from_column(name, format)` — now takes only `name`
- `laminar_db::db::infer_timestamp_format`
- `laminar_db::sql_analysis::infer_ts_format_from_batch`

**New APIs:**
- `laminar_core::time::cast_to_millis_array` — shared helper that
  normalises any `Timestamp(_)` array to `TimestampMillisecondArray`
  via Arrow's cast kernel. Used by the window UDFs, event-time
  extractor, interval-join key helper, and the WebSocket parser.

**Fixed:**
- `TUMBLE`/`HOP`/`SESSION` over `Timestamp(Nanosecond)` no longer
  errors with "Unsupported timestamp type" at runtime.
- Interval joins over `Timestamp(Nanosecond)` columns no longer
  error when extracting join keys.
- OTel `_laminar_received_at` unit-inference bug — windows that
  used to be silently 1,000,000× smaller than declared (nanos
  treated as millis) now use real wall-clock durations.
- `WATERMARK FOR` in TOML config composes with connector schema
  auto-discovery. Columnless sources with a watermark clause are
  validated against the discovered schema instead of bailing out.

**Migration notes:**
- Existing checkpoints from v0.20.0 are **not** compatible — they
  reference Int64 timestamp columns in serialized operator state.
  Wipe `./data/checkpoints` before starting the server on v0.20.1.
- Hand-written SQL with `ts BIGINT` needs to change to `ts TIMESTAMP`
  if the column is used in a watermark or window. Plain `BIGINT`
  columns unrelated to event time are unchanged.

## [Unreleased]

### Added — MongoDB CDC Source & Sink (`mongodb-cdc` feature)

New public API symbols in `laminar_connectors::mongodb`:

**Source Connector**
- `MongoDbCdcSource` — `SourceConnector` impl for MongoDB change streams
- `MongoDbSourceConfig` — connection, pipeline, full document mode config
- `FullDocumentMode` — Delta / UpdateLookup / RequirePostImage / WhenAvailable
- `mongodb_cdc_envelope_schema()` — Arrow schema for CDC envelope records

**Sink Connector**
- `MongoDbSink` — `SinkConnector` impl for MongoDB writes
- `MongoDbSinkConfig` — connection, batching, write concern config
- `WriteMode` — Insert / Upsert / Replace / CdcReplay
- `WriteConcernConfig`, `WriteConcernLevel` — write durability settings

**Change Events**
- `MongoDbChangeEvent` — typed change stream event
- `OperationType` — Insert / Update / Replace / Delete / Drop / Rename / Invalidate
- `UpdateDescription` — delta fields for update events
- `Namespace` — database + collection pair
- `TruncatedArray` — truncated array metadata (MongoDB 6.0+)

**Resume Token Persistence**
- `ResumeTokenStore` trait — pluggable async persistence
- `ResumeToken` — opaque resume token wrapper
- `FileResumeTokenStore` — file-based persistence (embedded/test)
- `MongoResumeTokenStore` — MongoDB-backed persistence (production, feature-gated)
- `InMemoryResumeTokenStore` — in-memory persistence (testing)
- `ResumeTokenStoreConfig` — configuration enum (File / Mongo / Memory)

**Time Series Support**
- `CollectionKind` — Standard / TimeSeries
- `TimeSeriesConfig` — time field, meta field, granularity, TTL
- `TimeSeriesGranularity` — Seconds / Minutes / Hours / Custom

**Large Event Handling**
- `LargeEventReassembler` — fragment reassembly for split events (MongoDB ≥ 6.0.9)
- `EventFragment`, `SplitEventInfo` — fragment types

**Write Model**
- `MongoWriteOp` — InsertOne / ReplaceOne / UpdateOne / DeleteOne / Lifecycle
- `build_update_document()` — $set/$unset update document builder
- `cdc_replay_op()` — operation type to write op translation

**Metrics**
- `MongoSourceMetrics` — lock-free source connector metrics
- `MongoSinkMetrics` — lock-free sink connector metrics

**Registry**
- `register_mongodb_cdc()` — registers source with ConnectorRegistry
- `register_mongodb_sink()` — registers sink with ConnectorRegistry
