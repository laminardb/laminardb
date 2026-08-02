# Distributed state

- **Status date:** 2026-08-02
- **Working state target:** one worker-local TidesDB instance
- **Recovery authority:** committed checkpoint artifacts
- **Production validation:** pending the full soak matrix

## Current boundary

LaminarDB has one managed vnode-state lifecycle: capture, restore, prepare, publish, revoke,
account, and recover. The bounded interval join, aggregates, and windows use operator-specific
payloads within that lifecycle; they are not alternate state engines or compatibility paths.

Source connectors declare append-only, keyed-upsert, or full-changelog input explicitly. Source
primary keys are ordered catalog and pipeline-identity data. Mutation and raw CDC-envelope inputs
remain fail-closed until the canonical signed-weight row schema is installed end to end.

The current distributed join is an in-memory, append-only bounded event-time equi-join over direct
watermarked sources while the common mutable relation state is built. It implements `INNER`,
`LEFT`, `RIGHT`, `FULL`, `LEFT SEMI`, `RIGHT SEMI`, `LEFT ANTI`, and `RIGHT ANTI`. Keys may be
ordered composites of Arrow `Utf8`/`Int64` columns, with the type matching at each position. Every
projected expression needs an explicit alias. The join uses canonical tuple routing, per-vnode
state, aligned checkpoint capture, bounded restore, and fenced rebalance.

The same operator and keyed-aggregate machinery runs in single-node and cluster mode under
at-least-once or exactly-once delivery; the delivery choice changes the connector/checkpoint
contract, not join or aggregation semantics. Output from any join kind can feed a supported keyed
aggregate only through a separate named stream. Fused `JOIN ... GROUP BY`, joins over intermediate
streams, unbounded/cross/general non-equality joins, and multi-way joins are rejected. Event-time
windowed aggregation remains unsupported in cluster mode.

Cluster exactly-once is connector-gated. The current admitted composition is exact-certified Kafka
input with direct S3/S3A coordinated append-mode Delta Lake. Delta also recognizes standard Azure
Blob/ADLS and GCS targets for shared at-least-once operation and valid local contracts, but they are
not cluster-EO certified until native provider fault soaks pass. Custom cloud endpoints and
emulators are not EO evidence. A Kafka sink is durable at-least-once, not exactly-once. Iceberg
append is durable at-least-once and is rejected for EO because it has no LaminarDB checkpoint-bound
catalog commit cursor. Cluster materialized views and temporal/lookup joins remain fail-closed.

## State and latency

The selected working-state shape is one TidesDB database with one retained column family per
worker. Logical key prefixes separate pipelines, operators, tables, and vnodes. TidesDB owns the
memtable, block cache, WAL, flush, compaction, and local-disk capacity. LaminarDB does not add a
second full-state cache or implement spilling. Writes enter memtables and flush to local SSTables at
bounded thresholds; frequently read SSTable blocks are retained in the engine's block cache.

The engine memory limit must be an explicit byte value derived from the process/container budget;
TidesDB auto-sizing is not accepted in production containers. The block cache, write buffer, flush
pressure, file-descriptor ceiling, and disk reserve are validated at startup and measured under
checkpoint and compaction load. Local NVMe is the production profile. One admitted Arrow batch is
applied as one atomic state transaction; an error with an uncertain outcome poisons that worker
attempt before output or checkpoint publication.

`StateBackend` remains the durable checkpoint-artifact boundary in process, on local disk, or in
shared object storage. It is not queried per row and is not a selectable hot-state engine. TidesDB
directories are disposable local working state, so cluster recovery still uses provider-neutral
committed checkpoint artifacts to restore vnodes on a new owner. TidesDB object-store mode is not
part of this path.

The record path uses bounded batches, cached logical byte accounting, and no checkpoint or object
store I/O. Join output is hard-capped at 262,144 rows and 64 MiB per cycle. The current operator
fails when one Zipfian fanout exceeds either cap and is therefore not production-ready for the
required skew workload. The relation-state join must instead resume that fanout across bounded
cycles, apply downstream backpressure, and checkpoint its output cursor with the state that proves
which pairs were emitted. Checkpoint encoding, storage, and pruning remain off the record path.

TidesDB is the only selected embedded engine; there is no RocksDB alternative or runtime backend
selector. Native TidesDB 9.3.15 contains the required incomplete-transaction-batch rejection fix,
but the matching official Rust wrapper and source crate are not yet published. Wrapper 0.11.1,
which packages native 9.3.6, is not an integration target. LaminarDB will not use a git dependency,
fork, or vendored C source. Runtime integration starts with the official 0.11.2-or-later package and
passes the focused atomicity, lifecycle, memory-limit, and tail-latency gates. Until then, the
existing in-memory layouts are transitional implementation, not a second product option. The
wrapper declares Rust edition 2024, which the workspace's Rust 1.95 baseline already supports; do
not raise the workspace toolchain requirement solely for this dependency.

## Implementation sequence

1. Define one ingress contract for append, keyed-upsert, and full before/after changelogs. Normalize
   all mutations to signed `__weight` rows and reject missing keys or before-images when correctness
   cannot be proved.
2. Add one private worker-local relation-state service backed only by TidesDB. Keep native types out
   of operator APIs, use one database and column family per worker, and atomically apply each
   admitted batch across all logical indexes.
3. Move materialized views and grouped aggregates onto that relation state. Support group deletion,
   checked retractions, checkpoint tombstones, and update output as old `-1` plus new `+1`.
4. Move all eight symmetric join kinds onto signed left/right arrangements. Add correct outer,
   semi, and anti transitions, resumable bounded fanout for skewed keys, then allow join output to
   feed aggregation and materialized-view joins in local and cluster execution.
5. Implement backward `ASOF`, event-time `FOR SYSTEM_TIME AS OF`, and `TEMPORAL PROBE JOIN`
   `LIST`/`RANGE` as selection policies over one signed versioned right-side arrangement. Route
   versions and probes by the equality key's vnode; checkpoint pending target-time probes,
   selected-match ledgers, timers, and frontiers together. Emit `-1/+1` corrections when a mutation
   changes the selected version, retain the per-key predecessor when compacting history, and
   finalize only when `right_watermark > probe_time`. A temporal probe produces one nullable-right
   result per requested offset. Forward/nearest forms require a finite tolerance; processing-time,
   indefinitely non-final forms, and silent pending-probe eviction remain unsupported.
6. Move windows, event-time indexes, watermarks, and timers onto the same state path. Bounded joins
   evict only after their watermark/lateness contract; regular unbounded joins retain state without
   a silent correctness-changing TTL.
7. Admit sinks by their actual append, keyed-upsert, or full-changelog capability. Keep ALO and EO on
   the same operator state; implement checkpoint-coupled Kafka, Delta, and Iceberg publication as
   separate connector commit protocols.
8. Remove superseded maps, codecs, compatibility paths, and append-only filters after each
   replacement is live. Finish with deterministic correction oracles and the single-node/cluster ×
   ALO/EO Zipfian fault-and-latency soak matrix.

## Evidence gate

The implementation is not production-certified until all four real-connector validity soaks pass:

- single-node at-least-once;
- single-node exactly-once;
- three-node cluster at-least-once; and
- three-node cluster exactly-once.

Each run must use two Kafka inputs, Zipfian keys, process kills and recovery, an independent output
oracle, checkpoint-progress checks, and explicit latency gates. At-least-once may duplicate output
for replayed input after recovery but may not lose admitted records or invent join pairs.
Exactly-once may neither lose nor duplicate externally visible output. A shortened or emulator-
backed run is engineering evidence, not cloud-provider or production certification.

ADR-008 defines the decision. Historical backend studies, cycle notes, and superseded designs have
been removed from the active documentation set.
