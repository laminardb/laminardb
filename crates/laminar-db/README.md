# laminar-db

Unified database facade for LaminarDB. The main entry point that wires the SQL parser, query planner, DataFusion context, streaming infrastructure, and connector registry.

## Key Types

- **`LaminarDB`** -- Main database handle. Manages sources, streams, sinks, and the streaming pipeline lifecycle.
- **`LaminarDbBuilder`** -- Fluent builder for constructing `LaminarDB` with custom configuration, connectors, UDFs, and deployment profiles.
- **`ExecuteResult`** -- Result of executing a SQL statement (DDL, query, rows affected, metadata).
- **`QueryHandle`** -- Handle to a running streaming query with schema and subscription access.
- **`SourceHandle<T>`** / **`UntypedSourceHandle`** -- Typed and untyped handles for pushing data into sources.
- **`TypedSubscription<T>`** -- Subscription to a named stream with automatic RecordBatch-to-struct conversion.
- **`SubscriptionRegistry`** / **`SubscriptionPortal`** -- Broadcast fan-out and per-consumer pump.
- **`CheckpointCoordinator`** -- Seals source/operator state, records the exact durable decision, and hands coordinated external publication to the designated committer.
- **`RecoveryManager`** -- Restores operator state, connector offsets, and watermarks from the latest checkpoint.
- **`Profile`** -- Deployment profile (`BareMetal`, `Embedded`, `Durable`, `Cluster`).
- **`PipelineMetrics`** / **`PipelineCounters`** -- Real-time pipeline observability.
- **`DbError`** -- Structured error type with stable `LDB-NNNN` codes.

## Architecture

This crate sits at the top of the dependency graph, integrating other LaminarDB crates:

```
laminar-db
  |-- laminar-core        (operators, streaming channels, checkpoint barriers, storage)
  |-- laminar-sql         (SQL parsing + DataFusion)
  |-- laminar-connectors  (external connectors)
```

One `StreamingCoordinator` task executes on the dedicated single-threaded `laminar-compute`
runtime. Connector I/O, checkpoint persistence and sink publication run on the main runtime.
This model applies to embedded, single-node and cluster execution.

See the [cluster SQL boundary](../../docs/SQL_REFERENCE.md#cluster-sql-boundary) for
mode-specific admission. Cluster plans deliberately reject local materialized views and
reference-table enrichment; managed direct-source final windows and
certified interval/temporal joins have their own admitted paths. Feature flags and checkpoints
alone do not imply exactly-once delivery.

Local subscriptions use in-memory replay history; cluster subscriptions expose committed,
partition-ordered output only for certified non-windowed keyed aggregates. Neither a separate
snapshot query followed by a subscription nor a client cursor establishes an atomic
snapshot-plus-tail or transactional external-consumer guarantee. See the
[subscription boundaries](../../docs/SQL_REFERENCE.md#subscribe-over-the-postgres-wire-protocol)
before designing consumers.

## DataFusion memory limit

Every `LaminarDB` has a shared 256 MiB limit for participating fallible DataFusion reservations.
Set `LaminarConfig::datafusion_memory_limit_bytes` or
`LaminarDB::builder().datafusion_memory_limit_bytes(bytes)` to change it; zero is rejected.
The limit applies in embedded, single-node and cluster modes, per DB instance (per node in
a cluster). Main queries, connector operator graphs, sink-filter contexts and local-table
diagnostics share the budget, including concurrent queries and restarted graph generations.
DB-owned contexts disable disk spilling. Exhaustion returns an allocation/query error;
streaming delivery and recovery use their existing failure handling.

This is a reservation limit, not a process RSS cap. Direct Arrow/expression allocations,
managed operator state, queues, tables/MVs, checkpoint scratch and connector-owned I/O
contexts have separate ownership. Standalone `laminar-sql` factories and its thread-local
lambda evaluation context retain upstream defaults and do not join a DB's pool. The default
is an execution policy, not a qualified production memory envelope; size it for the workload
and leave headroom for allocations outside DataFusion reservations.

## Connector source queue limit

The connector-to-coordinator FIFO has a shared **64 MiB** Arrow-byte limit in all modes,
in addition to its default 64-message capacity. Set `LaminarConfig::source_queue_max_bytes`
or `LaminarDB::builder().source_queue_max_bytes(bytes)` to change it. Direct coordinator
users set `PipelineConfig::source_queue_max_bytes`. Zero and values above
`MAX_SOURCE_QUEUE_BYTES` are rejected before connector startup.

Admission charges retained Arrow array capacity and batch/column descriptors, including
backing buffers of slices, nested arrays and views. Shared buffers are charged independently
for each queued batch/column. A message retains its charge while parked by an intake fence;
staging or discard releases it. A single oversized batch faults the source without settling
its cursor. Full queues backpressure producers; shutdown-tail sends use the same byte limit.
Barriers share the ordered FIFO but do not consume data-byte capacity.

Each source may additionally hold one validated batch while waiting for capacity or a cursor,
up to the queue limit per source. Connector decoding happens before this validation and has
its own memory ownership. Schemas/cursor metadata, embedded push rings, staged cycles, graph
ports, replay buffers, sinks and checkpoints are outside this queue budget. Dequeue/staging
does not prove that the Arrow buffers have been freed. Size these owners separately; this
limit does not establish a whole-process RSS envelope.

## Embedded push source limits

Each registered in-process source has a **64 MiB** Arrow-byte cap shared by its input ring
and queued broadcast references. Set `LaminarConfig::push_source_max_bytes` or
`LaminarDB::builder().push_source_max_bytes(bytes)` to change it. Zero and values above
`laminar_core::streaming::MAX_SOURCE_QUEUED_BYTES` fail DB construction. This covers typed
`SourceHandle` pushes after Arrow conversion, raw pushes, SQL inserts and API writers.
Multiple handles for the same source share the cap; separate sources have separate caps.
The setting does not change cluster SQL or delivery admission.

Pushes reject immediately on count or byte saturation (`StreamingError::ChannelFull`),
or reject a batch larger than the entire budget (`BatchTooLarge`). Rejected batches do not
advance the source sequence or change snapshot history. Typed `push_batch` reports the number
of records admitted before its first failed conversion chunk. `is_backpressured` includes
byte saturation. Writer `flush` remains a no-op; it does not drain or release queued input.

Each source's snapshot history has an **independent cap of the same size** plus its existing
batch-count limit. Successful pushes evict the oldest history until both bounds fit; this
does not discard queued input. Arrow reservations follow broadcast retention until delivery,
eviction or subscriber drop. Snapshot readers take ownership of their returned batch references,
which can outlive history eviction. Existing broadcast lag semantics are unchanged.

The two caps measure retained Arrow storage using the
[core admission contract](../laminar-core/README.md#in-process-source-admission). Each concurrent
caller may additionally hold its input and typed-conversion scratch before admission. Caller-held
batches, active query snapshots, query-result queues, graph/staged state and other downstream
owners are outside these caps. Query-result queues retain their existing count bound. The
connector-to-coordinator `source_queue_max_bytes` setting governs a separate owner; neither
setting is a whole-process memory guarantee.


## Graph input limits

Graph ports enforce `pipeline_max_input_buf_batches` (256 by default; zero disables it)
and the optional `pipeline_max_input_buf_bytes` limit. Configure them through `LaminarConfig`
or the same-named builder methods. The byte cap is disabled by default; a configured cap must
be greater than zero. Single-node and cluster servers expose both settings under `[server]`.

Admission includes source priming and checks the complete incoming batch set against current
port ownership before retaining it. Arrow slices, nested arrays and views charge their retained
backing storage, plus fixed batch/column charges. Each fan-out edge charges independently,
including aliases. These conservative per-port charges are not a process-wide memory bound.
Caller/staged inputs, operator execution scratch, result/subscription ownership, schema metadata,
allocator overhead and vector spare capacity remain separate owners.

`Backpressure` retains input and withholds source progress when a producer can be deferred before
execution. Cached SQL retries read that retained input. Multiple streaming inputs have distinct
ports and provider bindings, with at most 256 dynamic inputs per query; reference/lookup tables
remain separate providers. An oversized source admission, or an already-executed result that
cannot fit, raises `GraphBufferBudgetExceeded`. The pipeline halts before publishing the rejected
admission to graph destinations, and its possibly mutated graph cannot run or checkpoint again.
Reduce batch size or raise
the configured limit and resolve the terminal fault before restoring a committed cut. Ordinary
retry/stop/start cannot reuse that graph; cluster terminal fault authority survives process restarts.
`Fail` also halts at the existing
pre-execution capacity gate. Best-effort `ShedOldest` evicts the oldest existing/incoming batches
before admitting the retained suffix, including dropping a batch that cannot fit by itself;
`shed_records_total` records discarded rows. Durable delivery still rejects `ShedOldest`.

## Reference-table memory limits

Each embedded or single-node reference table defaults to **1,000,000 live rows** and a
**256 MiB retained-memory charge**. Set `LaminarConfig::reference_table_max_rows` and
`reference_table_max_bytes`, or the corresponding builder methods, before opening the DB.
Both limits must be nonzero. They apply independently to every table; cluster reference-table
enrichment and mutation remain rejected.

An upsert checks its final distinct keys, with the last occurrence winning within a batch.
`DbError::ReferenceTableQuotaExceeded` rejects growth before changing rows or readiness.
Snapshot keys must be unique. Startup reads one snapshot batch at a time into a bounded
candidate and closes all sources on failure. Every candidate is validated before any table
is replaced; recovery applies the current limits to the selected checkpoint without falling
back to an older cut. A rejected multi-table refresh or restore leaves the previous complete
installation intact.

The byte charge includes encoded key bytes, conservative per-row/column/array descriptors,
and the full capacity of retained Arrow allocations. An allocation shared by columns, rows
or successive batches counts once within a table, and independently in other tables.
Nested children, dictionaries, views and validity buffers participate. One surviving row
can retain a large old buffer: replacement releases that charge only when its last live
reference is gone. Complete snapshot replacement removes absent keys; an empty replacement
or `DROP TABLE` releases all live charges. No live keys are evicted to satisfy a quota.
Buffers keep their sharing. For custom/external allocations the charge uses the original
allocation extent reported by Arrow, even for a zero-length slice. Memory retained by an
opaque owner beyond the region exposed to Arrow is outside this charge.

This is a per-table live-state limit. Hash-map spare capacity, allocator and schema overhead,
caller-held inputs, query snapshots and checkpoint scratch are separate. An update may also
hold one incoming batch and its key/delta scratch. Refresh or restore can hold the old live
state plus one quota-checked candidate per table, and one decoded incoming batch; checkpoint
decoding retains its existing 256 MiB encoded-image limit. Captured checkpoints can pin old
buffers after live charges are released and retain their separate capture/encoding budgets.
These owners must be included when sizing process memory; the table quota does not bound RSS.

## Materialized-view memory limits

Embedded and single-node databases default to **1,000,000 live rows / 256 MiB per MV**.
Set `materialized_view_max_rows` and `materialized_view_max_bytes` through `LaminarConfig`
or the builder. Both must be nonzero. Cluster materialized views remain rejected.

Aggregate snapshots and keyed upserts reject a final state above either limit. Multisets
count distinct stored rows; their charge includes the complete encoded row and count.
Append views retain the newest complete batches within their batch, row and byte limits,
evicting oldest batches as before. A single oversized append batch fails admission.
Checkpoint restore rejects over-limit images without dropping committed rows to make them fit.

All affected views pass quota and multiplicity validation before a cycle changes any MV or
publishes MV subscription output. Failure faults the pipeline and leaves source cursors
uncommitted. Multiset cycle publication also preflights the existing snapshot expansion
guards (1,000,000 expanded rows and a 256 MiB encoded-row estimate), regardless of whether
a subscriber is currently attached. Counted checkpoints retain multiplicities without
expanding them; reads continue to enforce those snapshot guards.

Arrow batches charge Arrow-reported backing capacity and batch/column metadata, including nested,
dictionary and view arrays. Upserts charge owned encoded keys, scalar allocation sizes and
row metadata. Shared backing buffers are conservatively counted per stored batch or scalar.
Replacements, deletes and append eviction release the corresponding live charge.

These are per-view live-state limits, not an RSS limit. Hash-map spare capacity,
schema/converter/allocator overhead, unreported external ownership and caller-held inputs are
separate. Keyed cycle staging is bounded to twice the configured row and byte limits, plus
fixed metadata for at most that many staged entries. This permits full replacement and old-row
retractions before final live-state admission. Replaced values and cancelled multiset deltas
release their staging charge. Each input batch must also fit a conservative Arrow row-encoding
estimate under those staging limits before conversion. String/binary views charge their logical
lengths; dictionaries reserve the largest value per row as well as conversion scratch. Large
net-neutral cycles, complex dictionary values or heavily aliased inputs can therefore
be rejected even if their final live state would fit. All modes validate input column names,
types and nullability before staging; keyed input requires exactly one Int64 weight column.

Cycle preflight retains staged deltas for every affected MV at once, plus existing output
batches, one candidate scalar row and conversion scratch. These separate per-view charges do not
establish a combined process bound; configure source and graph admission and reserve headroom as
well. It does not clone live row maps or old append batches. Recovery may hold old stores, decoded
checkpoint batches and private quota-checked replacement stores together. Query/subscriber
materialization and pinned checkpoint captures
need additional headroom and retain their existing separate guards.

## Feature Flags

| Flag | Purpose |
|------|---------|
| `api` | FFI-friendly API module with `Connection`, `Writer`, `QueryStream` |
| `ffi` | C FFI layer with `extern "C"` functions and Arrow C Data Interface (implies `api`) |
| `kafka` | Kafka source/sink connector |
| `postgres-cdc` | PostgreSQL CDC implementation (source admission rejected); also builds the supported `postgres` lookup connector |
| `postgres-sink` | PostgreSQL sink |
| `mongodb-cdc` | MongoDB sink/lookup and CDC implementation (CDC source admission rejected) |
| `delta-lake` | Delta Lake sink and source |
| `delta-lake-s3` / `delta-lake-azure` / `delta-lake-gcs` | Cloud storage backends for Delta Lake |
| `delta-lake-unity` / `delta-lake-glue` | Databricks Unity / AWS Glue catalogs for Delta Lake |
| `delta-lake-all` | All Delta Lake storage backends and catalogs |
| `iceberg` | Apache Iceberg source and sink |
| `websocket` | WebSocket source and sink connectors |
| `files` | File source (AutoLoader) and sink (rolling files) |
| `parquet-lookup` | Parquet schema and codec helpers; no standalone connector |
| `otel` | OpenTelemetry OTLP/gRPC source |
| `cluster` | Distributed mode with gRPC control plane, vnode state, and gossip/static discovery; ALO plus capability-gated EO. |
| `aws` / `gcs` / `azure` | Object-store checkpoint backends (forwards to laminar-core) |

## Related Crates

- [`laminar-core`](../laminar-core) -- Operators, streaming channels, window assigners, checkpoint barriers, storage
- [`laminar-sql`](../laminar-sql) -- SQL parser and DataFusion integration
- [`laminar-connectors`](../laminar-connectors) -- External system connectors
- [`laminar-derive`](../laminar-derive) -- Derive macros for typed data handling

