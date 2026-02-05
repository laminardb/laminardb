# Session Context

> This file tracks session continuity. Update at the end of each session.
> For historical details, see [CONTEXT_ARCHIVE.md](./CONTEXT_ARCHIVE.md)

## Last Session

**Date**: 2026-02-05

### What Was Accomplished
- **F031A: Delta Lake I/O Integration** - IMPLEMENTATION COMPLETE (13 new integration tests)
  - `lakehouse/delta_io.rs` (NEW): All deltalake crate integration functions
    - `path_to_url()`: Convert local/S3/Azure/GCS paths to URL
    - `open_or_create_table()`: Open existing or create new Delta table via `deltalake` crate
    - `write_batches()`: Write RecordBatches with exactly-once txn metadata
    - `get_last_committed_epoch()`: Recovery support via txn action in Delta log
    - `get_table_schema()`: Schema discovery from existing table
  - `lakehouse/delta.rs`: Added `Option<DeltaTable>` field, cfg-gated real I/O methods
    - `open()` now calls `delta_io::open_or_create_table()` and recovers last committed epoch
    - `flush_buffer_to_delta()`: NEW async method for real Parquet writes
    - `commit_epoch()`: Flushes buffer to Delta Lake when feature enabled
  - `lakehouse/mod.rs`: Added `#[cfg(feature = "delta-lake")] pub mod delta_io`
  - `Cargo.toml`: Added `url` dependency for delta-lake feature
  - 8 business-logic-only tests gated with `#[cfg(not(feature = "delta-lake"))]`
  - All clippy clean with `-D warnings`
  - Tests: 86 pass with feature, 81 pass without feature

Previous session (2026-02-05):
- **F025/F026 Kafka Connector Enhancement** - CRITICAL GAPS COMPLETE (140 tests passing)
  - `config.rs`: Added SecurityProtocol enum (plaintext/ssl/sasl_plaintext/sasl_ssl) with uses_ssl(), uses_sasl() helpers
  - `config.rs`: Added SaslMechanism enum (PLAIN/SCRAM-SHA-256/SCRAM-SHA-512/GSSAPI/OAUTHBEARER) with requires_credentials()
  - `config.rs`: Added IsolationLevel enum (read_uncommitted/read_committed) for transactional pipelines
  - `config.rs`: Added TopicSubscription enum (Topics/Pattern) for regex-based topic subscription
  - `config.rs`: Added explicit security fields (sasl_username, sasl_password, ssl_ca_location, ssl_certificate_location, ssl_key_location, ssl_key_password)
  - `config.rs`: Added fetch tuning fields (fetch_min_bytes, fetch_max_bytes, fetch_max_wait_ms, max_partition_fetch_bytes)
  - `config.rs`: Added include_headers and schema_registry_ssl_ca_location
  - `sink_config.rs`: Added security fields and batch_num_messages
  - `source.rs`: Updated to use TopicSubscription with regex pattern support
  - `mod.rs`: Added 45+ ConfigKeySpec entries for documentation/discovery
  - Validation: SASL requires mechanism, credential-based mechanisms require username/password
  - All clippy clean with `-D warnings`, 140 Kafka tests pass
  - Gap analysis document: `docs/features/phase-3/F025-F026-kafka-connector-gaps.md`

Previous session (2026-02-03):
- **F-STREAM-010: Broadcast Channel** - IMPLEMENTATION COMPLETE (42 new tests)
  - `streaming/broadcast.rs`: BroadcastChannel<T> (shared ring buffer SPMC), BroadcastConfig, BroadcastConfigBuilder, SlowSubscriberPolicy (Block/DropSlow/SkipForSlow), SubscriberInfo, BroadcastError (31 tests)
  - `planner/channel_derivation.rs`: DerivedChannelType (Spsc/Broadcast), derive_channel_types(), SourceDefinition, MvDefinition, analyze_mv_sources(), derive_channel_types_detailed() (11 tests)
  - Key design: Users never specify broadcast mode - derived from query plan analysis when multiple MVs read from same source
  - Features: per-subscriber cursors, lag tracking, configurable slow subscriber policies, dynamic subscribe/unsubscribe
  - All clippy clean with `-D warnings`, 2350 total tests pass

Previous session (2026-02-03):
- **F034: Connector SDK** - IMPLEMENTATION COMPLETE (68 new tests, 427 total connector-base tests)
  - `sdk/retry.rs`: RetryPolicy (exponential backoff, jitter), with_retry async helper, with_retry_and_handler, CircuitBreaker (Closed/Open/HalfOpen state machine), with_circuit_breaker helper (18 tests)
  - `sdk/rate_limit.rs`: RateLimiter trait, TokenBucket (tokens/sec + burst), LeakyBucket (requests/sec + queue), NoopRateLimiter (13 tests)
  - `sdk/harness.rs`: ConnectorTestHarness with test_source, test_sink, test_checkpoint_recovery, test_exactly_once methods; TestSourceConnector and TestSinkConnector with configurable batches/errors/delays (12 tests)
  - `sdk/builder.rs`: SourceConnectorBuilder and SinkConnectorBuilder for fluent connector configuration with retry policies, rate limiters, and circuit breakers; ConfiguredSource and ConfiguredSink wrappers (8 tests)
  - `sdk/defaults.rs`: register_all_connectors(), register_mock_connectors(), default_registry() with auto-registration of mock/delta-lake/iceberg/kafka/postgres/mysql connectors based on features (7 tests)
  - `sdk/schema.rs`: SchemaDiscoveryHints (type_hints, nullable_fields, field_names, prefer_larger_types, empty_as_null), infer_schema_from_samples() for JSON/CSV formats (10 tests)
  - `sdk/mod.rs`: Re-exports all SDK types
  - `laminar-derive/connector_config.rs`: #[derive(ConnectorConfig)] macro generates from_config(), validate(), config_keys() from struct fields with #[config(key, required, default, env, description, duration_ms)] attributes
  - All clippy clean with `-D warnings`, 2308 total tests pass

Previous session (2026-02-03):
- **F032: Apache Iceberg Sink** - IMPLEMENTATION COMPLETE (103 new tests, 359 total connector-base tests)
  - `lakehouse/iceberg_config.rs`: IcebergSinkConfig with IcebergCatalogType (REST/Glue/Hive/Memory), IcebergWriteMode, IcebergPartitionField, IcebergTransform (Identity/Bucket/Truncate/Year/Month/Day/Hour), MaintenanceConfig, SortField, IcebergFileFormat enums, from_config() parsing with parentheses-aware partition spec splitting, validation (49 tests)
  - `lakehouse/iceberg_metrics.rs`: IcebergSinkMetrics with 11 atomic counters (rows, bytes, flushes, commits, errors, rollbacks, data_files, delete_files, changelog_deletes, snapshot_id, table_version), MetricsSnapshot (9 tests)
  - `lakehouse/iceberg.rs`: IcebergSink implementing SinkConnector — buffering with size/time/rows flush triggers, epoch management with exactly-once skip, Z-set changelog splitting with Iceberg v2 equality delete file support, upsert mode, health checks, capabilities (45 tests)
  - `lakehouse/mod.rs`: Re-exports, register_iceberg_sink() and register_lakehouse_sinks() factories, 33 config key specs including catalog, partition, maintenance, and cloud storage options (6 tests)
  - Follows Delta Lake Sink pattern: all business logic without actual iceberg-rust crate dependency
  - Supports REST, Glue, Hive, and Memory catalogs
  - Iceberg-native partition transforms: IDENTITY, BUCKET(n), TRUNCATE(w), YEAR, MONTH, DAY, HOUR
  - All clippy clean with `-D warnings`, 2240 total tests pass

Previous session (2026-02-03):
- **F028: MySQL CDC Source** - IMPLEMENTATION COMPLETE (119 new tests, 359 total connector-base tests)
  - `cdc/mysql/config.rs`: MySqlCdcConfig with SslMode, SnapshotMode enums, from_config() parsing, validation (15 tests)
  - `cdc/mysql/gtid.rs`: Gtid, GtidRange, GtidSet types with parsing and ordering (21 tests)
  - `cdc/mysql/types.rs`: MySqlColumn, mysql_type_to_arrow mapping for 20+ MySQL types (14 tests)
  - `cdc/mysql/decoder.rs`: BinlogMessage enum, BinlogPosition, TableMapMessage, row event types (14 tests)
  - `cdc/mysql/schema.rs`: TableInfo, TableCache for binlog TABLE_MAP events (12 tests)
  - `cdc/mysql/changelog.rs`: CdcOperation, ChangeEvent, Z-set changelog format, events_to_record_batch (17 tests)
  - `cdc/mysql/metrics.rs`: MySqlCdcMetrics with 11 AtomicU64 counters, MetricsSnapshot (12 tests)
  - `cdc/mysql/source.rs`: MySqlCdcSource implementing SourceConnector — open/poll_batch/checkpoint/restore/health_check/close (14 tests)
  - `cdc/mysql/mod.rs`: Re-exports, register_mysql_cdc_source factory, 19 config key specs (4 tests)
  - Feature-gated behind `mysql-cdc` Cargo feature (business logic only, no mysql_async dependency)
  - Follows Delta Lake pattern: all business logic without actual I/O (F028A will add binlog I/O)
  - All clippy clean with `-D warnings`

Previous session (2026-02-02):
- **F-CLOUD-001, F-CLOUD-002, F-CLOUD-003: Cloud Storage Infrastructure** - ALL 3 COMPLETE (82 new tests, 256 total connector base tests)
  - `storage/provider.rs`: StorageProvider enum (AwsS3, AzureAdls, Gcs, Local), detect() from URI scheme, 18 tests
  - `storage/resolver.rs`: StorageCredentialResolver with resolve() and resolve_with_env(), env var fallback for AWS (7 vars), Azure (6), GCS (2), 22 tests
  - `storage/validation.rs`: CloudConfigValidator with per-provider rules — S3 requires region, Azure requires account_name, GCS warning-only, 20 tests
  - `storage/masking.rs`: SecretMasker with is_secret_key(), redact_map(), display_map(), 8 secret patterns, 22 tests
  - `storage/mod.rs`: Re-exports all primary types
  - **Integration with Delta Lake Sink**: DeltaLakeSinkConfig.from_config() now auto-resolves env vars via StorageCredentialResolver, validate() checks cloud credentials via CloudConfigValidator, display_storage_options() redacts secrets
  - `config.rs`: Added display_redacted() to ConnectorConfig
  - `lakehouse/mod.rs`: Added 12 cloud-specific ConfigKeySpec entries for discoverability
  - `lakehouse/delta_config.rs`: 10 new integration tests (S3/Azure/GCS path validation, secret redaction)
  - All clippy clean with `-D warnings`, 2137 total tests pass across workspace

Previous session (2026-02-02):
- **Delta Lake Deferred Work Breakdown** - Created 7 new feature specs for cloud storage infrastructure and Delta Lake I/O completion
  - `docs/features/phase-3/cloud/INDEX.md`: Cloud Storage Infrastructure sub-group
  - `F031A`: Delta Lake I/O Integration (blocked by deltalake crate)
  - `F031B`: Delta Lake Recovery & Exactly-Once I/O (blocked by F031A)
  - `F031C`: Delta Lake Compaction & Maintenance (blocked by F031A)
  - `F031D`: Delta Lake Schema Evolution (blocked by F031A)

Previous session (2026-02-02):
- **F031: Delta Lake Sink** - IMPLEMENTATION COMPLETE (73 new tests, 169 total connector base tests)
  - `lakehouse/delta_config.rs`: DeltaLakeSinkConfig with DeltaWriteMode, DeliveryGuarantee, CompactionConfig enums, from_config() parsing, validation (20 tests)
  - `lakehouse/delta_metrics.rs`: DeltaLakeSinkMetrics with 9 AtomicU64 counters, to_connector_metrics() (7 tests)
  - `lakehouse/delta.rs`: DeltaLakeSink implementing SinkConnector — buffering with size/time/rows flush triggers, epoch management with exactly-once skip, Z-set changelog splitting, health checks, capabilities (42 tests)
  - `lakehouse/mod.rs`: Re-exports, register_delta_lake_sink factory, 15 config key specs (4 tests)
  - Not feature-gated (compiles without deltalake crate dependency); deltalake crate deferred until version compatible with workspace datafusion
  - Follows PostgresSink pattern: all business logic without actual I/O
  - All clippy clean with `-D warnings`, 2050 total tests pass

Previous session (2026-02-01):
- **F-SUB-001 to F-SUB-008: Reactive Subscription System** - ALL COMPLETE (8/8 features)
  - F-SUB-001: ChangeEvent types (EventType, ChangeEvent, ChangeEventBatch, NotificationRef)
  - F-SUB-002: Notification Slot (NotificationSlot 64-byte cache-aligned, NotificationRing SPSC, NotificationHub)
  - F-SUB-003: Subscription Registry (SubscriptionRegistry, SubscriptionEntry, SubscriptionConfig, SubscriptionMetrics)
  - F-SUB-004: Subscription Dispatcher (SubscriptionDispatcher, Ring 1 broadcast, NotificationDataSource trait)
  - F-SUB-005: Push Subscription API (PushSubscription channel-based handle)
  - F-SUB-006: Callback Subscriptions (subscribe_callback, subscribe_fn, CallbackSubscriptionHandle)
  - F-SUB-007: Stream Subscriptions (subscribe_stream, ChangeEventStream, ChangeEventResultStream)
  - F-SUB-008: Backpressure & Filtering (BackpressureController, DemandBackpressure, NotificationBatcher, Ring0Predicate, compile_filter via sqlparser)
  - New files: event.rs, notification.rs, registry.rs, dispatcher.rs, handle.rs, callback.rs, stream.rs, backpressure.rs, batcher.rs, filter.rs
  - Added sqlparser dependency to laminar-core for filter compilation (SQL → Ring 0/Ring 1 predicate classification)
  - 42 new tests from F-SUB-008 alone (10 backpressure + 6 batcher + 26 filter)
  - All clippy clean with `-D warnings`, 1240 total laminar-core tests pass

Previous session (2026-01-31):
- **Performance Optimization: Event.data → Arc<RecordBatch>** - COMPLETE
  - Changed `Event.data` from owned `RecordBatch` to `Arc<RecordBatch>` for zero-copy multicast
  - Multicast fan-out clone cost: O(columns) → O(1) (~2ns atomic increment)
  - All clippy clean with `-D warnings`, all 1709 tests pass

Previous session (2026-01-31):
- **F-DAG-007: Performance Validation** - IMPLEMENTATION COMPLETE
  - `benches/dag_bench.rs`: 11 Criterion benchmarks in 5 groups (routing, multicast, executor latency, throughput, checkpoint/recovery)
  - `benches/dag_stress.rs`: 5 stress test scenarios (20-node mixed, 8-way fan-out, deep linear 10, diamond stateful, checkpoint under load with p99 latency)
  - Cargo.toml: Added `[[bench]]` entries for `dag_bench` and `dag_stress`
  - Performance audit: lock-free hot path confirmed, zero-lock architecture, proper cache-line alignment
  - Optimizations applied:
    - R1: Pre-allocated event pools in benchmarks (eliminates RecordBatch allocation from timed region)
    - R2: Pre-sized VecDeque input queues with `with_capacity(16)` in DagExecutor (avoids Ring 0 allocation on first push)
  - Benchmark results (post-optimization):
    - Routing lookup: ~4ns (target <50ns) — 12x headroom
    - Linear 3-node latency: **325ns** (target <500ns) — 35% headroom (was 542ns before pre-alloc fix)
    - Linear throughput: **2.24M events/sec** (target 500K) — 4.5x headroom
    - Deep 10-op throughput: **660K events/sec** (target 500K) — 32% headroom (with event alloc overhead)
  - All clippy clean with `-D warnings`

Previous session (2026-01-31):
- **F-DAG-006: Connector Bridge** - IMPLEMENTATION COMPLETE (25 new tests, 96 total connector base tests)

Previous session (2026-01-31):
- **F-DAG-005: SQL & MV Integration** - IMPLEMENTATION COMPLETE (18 new tests, 100 total DAG tests + 2 SQL tests)

Previous session (2026-01-31):
- **F027B: PostgreSQL Sink** - IMPLEMENTATION COMPLETE (84 new tests, 155 total connector tests with postgres-sink)
  - `postgres/types.rs`: Arrow→PG type mapping — DDL types, UNNEST array casts, SQL types (12 tests)
  - `postgres/sink_config.rs`: PostgresSinkConfig with WriteMode, DeliveryGuarantee, SslMode enums, validation, ConnectorConfig parsing (20 tests)
  - `postgres/sink_metrics.rs`: 9 AtomicU64 counters — records, bytes, errors, batches, copy/upsert ops, epochs, changelog deletes (7 tests)
  - `postgres/sink.rs`: PostgresSink implementing SinkConnector — COPY BINARY SQL, UNNEST upsert SQL, DELETE SQL, DDL generation, co-transactional epoch commit/recover SQL, Z-set changelog splitting, batch buffering with size/time flush triggers (41 tests)
  - `postgres/mod.rs`: Re-exports, register_postgres_sink factory, 18 config key specs (4 tests)
  - Feature-gated behind `postgres-sink` Cargo feature
  - Two write strategies: Append (COPY BINARY >500K rows/sec) and Upsert (INSERT...ON CONFLICT DO UPDATE with UNNEST)
  - Exactly-once via co-transactional offset storage (_laminardb_sink_offsets table)
  - Z-set changelog support: splits batches by _op column into inserts (I/U/r) and deletes (D)
  - All clippy clean with `-D warnings`

Previous session (2026-01-31):
- **F027: PostgreSQL CDC Source** - IMPLEMENTATION COMPLETE (107 new tests, 178 total connector tests)
  - Full pgoutput binary protocol parser (9 message types)
  - CDC envelope schema: _table, _op, _lsn, _ts_ms, _before, _after columns
  - Feature-gated behind `postgres-cdc` Cargo feature

Previous session (2026-01-31):
- **F027: PostgreSQL CDC Source** - SPEC UPDATED to v2.0
- **F027B: PostgreSQL Sink Connector** - NEW SPEC CREATED (v1.0)

Previous session (2026-01-30):
- F-DAG-004: DAG Checkpointing - COMPLETE (18 new tests, 84 total DAG tests)
- F026: Kafka Sink Connector - COMPLETE (51 new sink tests)
- F025: Kafka Source Connector - COMPLETE (67 tests)
- F-DAG-003: DAG Executor - COMPLETE (21 tests)
- F-DAG-002: Multicast & Routing - COMPLETE (16 tests)
- F-DAG-001: Core DAG Topology - COMPLETE (29 tests)

Previous session (2026-01-28):
- Developer API Overhaul - 3 new crates, SQL parser extensions, 5 examples
- F-STREAM-001 to F-STREAM-007: Streaming API - ALL COMPLETE (99 tests)
- Performance Audit: ALL 10 issues fixed
- F074-F077: Aggregation Semantics Enhancement - COMPLETE (219 tests)

**Total tests**: 1272 core + 412 sql + 115 storage + 120 laminar-db + 440 connectors + 4 demo = 2363 (base), +84 postgres-sink-only + 107 postgres-cdc-only + 118 kafka-only + 13 delta-lake-only = 2685 (with feature flags)

### Where We Left Off
**Phase 3 Connectors & Integration: 37/49 features COMPLETE (76%)**
- Streaming API core complete (F-STREAM-001 to F-STREAM-007, F-STREAM-013)
- Developer API overhaul complete (laminar-derive, laminar-db crates)
- DAG pipeline complete (F-DAG-001 to F-DAG-007)
- Kafka Source Connector complete (F025)
- Kafka Sink Connector complete (F026)
- PostgreSQL CDC Source complete (F027) — 107 tests, full pgoutput decoder
- PostgreSQL Sink complete (F027B) — 84 tests, COPY BINARY + upsert + exactly-once
- MySQL CDC Source complete (F028) — 119 tests, binlog decoder + GTID + Z-set changelog
- Delta Lake Sink business logic complete (F031) — 73 tests, buffering + epoch management + changelog splitting
- **Delta Lake I/O Integration complete (F031A)** — 13 new integration tests, real deltalake crate writes with exactly-once txn metadata
- Apache Iceberg Sink business logic complete (F032) — 103 tests, REST/Glue/Hive catalogs + partition transforms + equality deletes
- **Connector SDK complete (F034)** — 68 tests, retry/circuit breaker + rate limiting + test harness + builders + defaults + schema discovery
- **Broadcast Channel complete (F-STREAM-010)** — 42 tests, shared ring buffer SPMC + query plan derivation + slow subscriber policies
- SQL & MV Integration complete (F-DAG-005) — 18 new tests, DAG from MvRegistry, watermarks, changelog
- Connector Bridge complete (F-DAG-006) — 25 new tests, source/sink bridge + runtime orchestration
- Performance Validation complete (F-DAG-007) — 16 benchmarks, performance audit + optimizations
- Reactive Subscription System complete (F-SUB-001 to F-SUB-008) — 8 features, 10 new modules
- Cloud Storage Infrastructure complete (F-CLOUD-001/002/003) — 82 tests, integrated with Delta Lake Sink
- Next: F028A MySQL CDC I/O, F029 MongoDB CDC Source, or F033 Parquet File Source

### Immediate Next Steps
1. F028A: MySQL CDC binlog I/O (mysql_async now ready with rustls)
2. F031B/C/D: Delta Lake Recovery, Compaction, Schema Evolution
3. F029: MongoDB CDC Source
4. F033: Parquet File Source
5. F032A: Iceberg I/O (when iceberg-rust crate can be added)

### Open Issues
- **deltalake crate version**: ✅ RESOLVED - Using git main branch with DataFusion 52.x. F031A complete.
- **mysql_async crate**: ✅ RESOLVED - Now using rustls TLS backend (no OpenSSL required). F028A ready for implementation.
- **iceberg-rust crate**: Deferred until version compatible with workspace. Business logic complete in F032.
- None currently blocking.

---

## Phase 2 Progress

| Feature | Status | Notes |
|---------|--------|-------|
| F013: Thread-Per-Core | Done | SPSC queues, key routing, CPU pinning |
| F014: SPSC Queues | Done | Part of F013 |
| F015: CPU Pinning | Done | Part of F013 |
| F016: Sliding Windows | Done | Multi-window assignment |
| F017: Session Windows | Done | Gap-based sessions |
| F018: Hopping Windows | Done | Alias for sliding |
| F019: Stream-Stream Joins | Done | Inner/Left/Right/Full |
| F020: Lookup Joins | Done | Cached with TTL |
| F011B: EMIT Extension | Done | OnWindowClose, Changelog, Final |
| F023: Exactly-Once Sinks | Done | TransactionalSink, ExactlyOnceSinkAdapter, 28 tests |
| F059: FIRST/LAST | Done | Essential for OHLC |
| F063: Changelog/Retraction | Done | Z-set foundation |
| F067: io_uring Advanced | Done | SQPOLL, IOPOLL |
| F068: NUMA-Aware Memory | Done | NumaAllocator |
| F071: Zero-Alloc Enforcement | Done | HotPathGuard |
| F022: Incremental Checkpoint | Done | RocksDB backend |
| F062: Per-Core WAL | Done | Lock-free per-core |
| F069: Three-Ring I/O | Done | Latency/Main/Poll |
| F070: Task Budget | Done | BudgetMonitor |
| F073: Zero-Alloc Polling | Done | Callback APIs |
| F060: Cascading MVs | Done | MvRegistry |
| F056: ASOF Joins | Done | Backward/Forward/Nearest, tolerance |
| F064: Per-Partition Watermarks | Done | PartitionedWatermarkTracker, CoreWatermarkState |
| F065: Keyed Watermarks | Done | KeyedWatermarkTracker, per-key 99%+ accuracy |
| F057: Stream Join Optimizations | Done | CPU-friendly encoding, asymmetric compaction, per-key tracking |
| F024: Two-Phase Commit | Done | Presumed abort, crash recovery, 20 tests |
| F021: Temporal Joins | Done | Event-time/process-time, append-only/non-append-only, 22 tests |
| F066: Watermark Alignment Groups | Done | Pause/WarnOnly/DropExcess, coordinator, 25 tests |
| F072: XDP/eBPF | Done | Packet header, CPU steering, Linux loader, 34 tests |
| F005B: Advanced DataFusion | Done | Window/Watermark UDFs, async LogicalPlan, execute_streaming_sql, 31 tests |
| F074: Composite Aggregator | Done | ScalarResult, DynAccumulator, CompositeAggregator, f64 aggregators, 122 tests |
| F075: DataFusion Aggregate Bridge | Done | DataFusionAccumulatorAdapter, DataFusionAggregateFactory, 40 tests |
| F076: Retractable FIRST/LAST | Done | RetractableFirst/LastValue, f64 variants, 26 tests |
| F077: Extended Aggregation Parser | Done | 20+ new AggregateType variants, FILTER, WITHIN GROUP, 57 tests |

---

## Phase 1.5 Progress (SQL Parser)

| Feature | Status | Notes |
|---------|--------|-------|
| F006B: Production SQL Parser | Done | 129 tests, all 6 phases complete |

---

## Quick Reference

### Key Modules
```
laminar-core/src/
  dag/          # F-DAG-001/002/003/004/007: DAG pipeline topology + multicast/routing + executor + checkpointing + benchmarks
    topology      # StreamingDag, DagNode, DagEdge, DagChannelType
    builder       # DagBuilder, FanOutBuilder
    error         # DagError (+ checkpoint error variants)
    multicast     # F-DAG-002: MulticastBuffer<T> (SPMC, refcounted slots)
    routing       # F-DAG-002: RoutingTable, RoutingEntry (64-byte aligned)
    executor      # F-DAG-003: DagExecutor, DagExecutorMetrics (Ring 0 processing)
    checkpoint    # F-DAG-004: CheckpointBarrier, BarrierAligner, DagCheckpointCoordinator
    recovery      # F-DAG-004: DagCheckpointSnapshot, DagRecoveryManager, RecoveredDagState
  streaming/    # F-STREAM-001 to F-STREAM-006: In-memory streaming API
    ring_buffer   # Lock-free SPSC ring buffer
    channel       # SPSC/MPSC channel with auto-upgrade
    source        # Source<T> with push/watermark
    sink          # Sink<T> with broadcast mode
    subscription  # poll/recv/Iterator APIs
  time/         # F010, F064, F065, F066: Watermarks, partitioned + keyed + alignment
    alignment_group # F066: Watermark alignment groups
  mv/           # F060: Cascading MVs
  budget/       # F070: Task budgets
  sink/         # F023: Exactly-once sinks
    transactional  # F023: TransactionalSink<S> with buffer+commit
    adapter        # F023: ExactlyOnceSinkAdapter epoch-based bridge
  io_uring/     # F067, F069: io_uring + three-ring
  xdp/          # F072: XDP/eBPF network optimization
  alloc/        # F071: Zero-allocation
  numa/         # F068: NUMA awareness
  tpc/          # F013/F014: Thread-per-core
  subscription/ # F-SUB-001 to F-SUB-008: Reactive push-based subscription system
    event         # F-SUB-001: ChangeEvent, ChangeEventBatch, EventType, NotificationRef
    notification  # F-SUB-002: NotificationSlot (64-byte), NotificationRing (SPSC), NotificationHub
    registry      # F-SUB-003: SubscriptionRegistry, SubscriptionEntry, SubscriptionConfig, SubscriptionMetrics
    dispatcher    # F-SUB-004: SubscriptionDispatcher (Ring 1 broadcast), NotificationDataSource
    handle        # F-SUB-005: PushSubscription (channel-based)
    callback      # F-SUB-006: subscribe_callback, subscribe_fn, CallbackSubscriptionHandle
    stream        # F-SUB-007: subscribe_stream, ChangeEventStream, ChangeEventResultStream
    backpressure  # F-SUB-008: BackpressureController, DemandBackpressure, DemandHandle
    batcher       # F-SUB-008: BatchConfig, NotificationBatcher (size/time triggers)
    filter        # F-SUB-008: Ring0Predicate, ScalarValue, StringInternTable, compile_filter
  operator/     # Windows, joins, changelog
    window      # F074: CompositeAggregator, DynAccumulator, f64 aggregators
    changelog   # F076: RetractableFirst/LastValueAccumulator
    asof_join   # F056: ASOF joins
    temporal_join # F021: Temporal joins

laminar-sql/src/       # F006B: Production SQL Parser
  parser/              # SQL parsing with streaming extensions
    streaming_parser   # CREATE SOURCE/SINK
    window_rewriter    # TUMBLE/HOP/SESSION extraction
    emit_parser        # EMIT clause parsing
    late_data_parser   # Late data handling
    join_parser        # Stream-stream/lookup join analysis
    aggregation_parser # F077: 30+ aggregates, FILTER, WITHIN GROUP, datafusion_name()
  planner/             # StreamingPlanner, QueryPlan
  translator/          # Operator configuration builders
    window_translator  # WindowOperatorConfig
    join_translator    # JoinOperatorConfig (stream/lookup)
    streaming_ddl      # F-STREAM-007: CREATE SOURCE/SINK → SourceDefinition/SinkDefinition
  datafusion/          # F005/F005B: DataFusion integration
    window_udf         # F005B: TUMBLE/HOP/SESSION scalar UDFs
    watermark_udf      # F005B: watermark() UDF via Arc<AtomicI64>
    execute            # F005B: execute_streaming_sql end-to-end
    aggregate_bridge   # F075: DataFusion Accumulator ↔ DynAccumulator bridge

laminar-connectors/src/
  kafka/              # F025/F026: Kafka Source & Sink Connectors
    config            # KafkaSourceConfig, OffsetReset, AssignmentStrategy, CompatibilityLevel
    source            # KafkaSource (SourceConnector impl)
    offsets           # OffsetTracker (per-partition offset tracking)
    backpressure      # BackpressureController (high/low watermark)
    metrics           # KafkaSourceMetrics (AtomicU64 counters)
    rebalance         # RebalanceState (partition assignment)
    schema_registry   # SchemaRegistryClient (Confluent SR REST API, caching, registration)
    avro              # AvroDeserializer (arrow-avro Decoder, Confluent wire format)
    sink_config       # KafkaSinkConfig, DeliveryGuarantee, PartitionStrategy, CompressionType
    sink              # KafkaSink (SinkConnector impl, transactional/idempotent)
    avro_serializer   # AvroSerializer (arrow-avro Writer, Confluent wire format)
    partitioner       # KafkaPartitioner trait, KeyHash/RoundRobin/Sticky
    sink_metrics      # KafkaSinkMetrics (AtomicU64 counters)
  postgres/           # F027B: PostgreSQL Sink Connector
    types             # Arrow→PG type mapping (DDL, UNNEST casts, SQL types)
    sink_config       # PostgresSinkConfig, WriteMode, DeliveryGuarantee, SslMode
    sink_metrics      # PostgresSinkMetrics (9 AtomicU64 counters)
    sink              # PostgresSink (SinkConnector impl, COPY BINARY + upsert + changelog)
  cdc/postgres/       # F027: PostgreSQL CDC Source Connector
    lsn               # LSN type (X/Y hex format, ordering, arithmetic)
    types             # 28 PG OID constants, PgColumn, pg_type_to_arrow mapping
    config            # PostgresCdcConfig, SslMode, SnapshotMode
    decoder           # pgoutput binary protocol parser (9 message types)
    schema            # RelationInfo, RelationCache, cdc_envelope_schema
    changelog         # Z-set ChangeEvent, tuple_to_json, events_to_record_batch
    metrics           # CdcMetrics (11 atomic counters)
    source            # PostgresCdcSource (SourceConnector impl, transaction buffering)
  cdc/mysql/          # F028: MySQL CDC Source Connector
    config            # MySqlCdcConfig, SslMode, SnapshotMode
    gtid              # Gtid, GtidRange, GtidSet (GTID-based replication)
    types             # MySqlColumn, mysql_type_to_arrow mapping (20+ types)
    decoder           # BinlogMessage enum, BinlogPosition, row event types
    schema            # TableInfo, TableCache (TABLE_MAP event handling)
    changelog         # CdcOperation, ChangeEvent, Z-set format, events_to_record_batch
    metrics           # MySqlCdcMetrics (11 atomic counters)
    source            # MySqlCdcSource (SourceConnector impl)
  lakehouse/          # F031, F032: Lakehouse connectors (Delta Lake, Iceberg)
    delta             # DeltaLakeSink (SinkConnector impl, buffering + epoch + changelog)
    delta_config      # DeltaLakeSinkConfig, DeltaWriteMode, DeliveryGuarantee, CompactionConfig
    delta_metrics     # DeltaLakeSinkMetrics (9 AtomicU64 counters)
    iceberg           # F032: IcebergSink (SinkConnector impl, buffering + epoch + equality deletes)
    iceberg_config    # IcebergSinkConfig, IcebergCatalogType, IcebergWriteMode, IcebergTransform, IcebergPartitionField
    iceberg_metrics   # IcebergSinkMetrics (11 AtomicU64 counters)
  storage/            # F-CLOUD-001/002/003: Cloud Storage Infrastructure
    provider          # StorageProvider enum (AwsS3, AzureAdls, Gcs, Local), detect() from URI
    resolver          # StorageCredentialResolver, resolve() + resolve_with_env(), env var fallback
    validation        # CloudConfigValidator, per-provider required fields, warnings vs errors
    masking           # SecretMasker, is_secret_key(), redact_map(), display_map()
  bridge/             # F-DAG-006: Connector Bridge (DAG ↔ external connectors)
    source_bridge     # DagSourceBridge (SourceConnector → DagExecutor)
    sink_bridge       # DagSinkBridge (DagExecutor → SinkConnector)
    runtime           # ConnectorBridgeRuntime (orchestration + checkpoint/recovery)
    metrics           # SourceBridgeMetrics, SinkBridgeMetrics, BridgeRuntimeMetrics
    config            # BridgeRuntimeConfig
  serde/              # RecordDeserializer/RecordSerializer traits
    json, csv, raw, debezium  # Format implementations
  connector           # SourceConnector/SinkConnector traits
  registry            # ConnectorRegistry factory pattern

laminar-storage/src/
  incremental/  # F022: Checkpointing
  per_core_wal/ # F062: Per-core WAL
```

### Useful Commands
```bash
cargo test --all --lib          # Run all tests
cargo bench --bench tpc_bench   # TPC benchmarks
cargo bench --bench dag_bench   # DAG pipeline benchmarks (F-DAG-007)
cargo bench --bench dag_stress  # DAG stress tests with p99 latency
cargo clippy --all -- -D warnings
```
