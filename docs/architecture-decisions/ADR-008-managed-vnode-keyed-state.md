# ADR-008: Managed vnode-keyed working state for distributed operators

- **Status:** Proposed; implementation requires the Phase 0 review gate
- **Date:** 2026-07-22
- **Decision scope:** Cluster `CREATE STREAM` aggregates, windows, and joins
- **Related:** [validation report](../reports/cluster-keyed-state-validation-2026-07-22.md),
  [implementation plan](../plans/distributed-keyed-stateful-operators.md)

## Decision

LaminarDB will add a common, byte-governed, spillable **working-state service** for keyed
operators. The first production backend will be an embedded RocksDB-family local LSM, accessed in
batches on the worker. An in-memory implementation will remain a semantic reference and a small
local-mode option. Cluster-shared object storage and the existing `StateBackend` remain the
authoritative checkpoint/recovery layer; they will not be used for per-row state access.

The existing fixed vnode ABI, bounded shuffle, assignment/process fencing, aligned barriers,
per-vnode checkpoint artifacts, and exact-attempt seal are retained. Cluster admission will move
from SQL-shape exclusions plus permissive operator hooks to a planner-certified distribution/state
descriptor. Each stateful operator must declare its partition key, stable state tables, timers,
retention, output mode, checkpoint schema, and acquire/revoke behavior.

The implementation order is grouped aggregates, fixed event-time windows, then bounded interval
joins. Stateful streams may be enabled before cluster materialized views. Cluster exactly-once is
a separate connector/commit problem and remains rejected by `[LDB-0013]`; the first keyed-state
release targets the currently advertised cluster at-least-once contract.

## Context

Cluster SQL admission is correctly fail-closed today. Stateless streams and one direct ungrouped
aggregate stage are admitted; keyed aggregates, all windowed aggregates, all joins, and all
materialized views are rejected with nested `[LDB-4007]`. Embedded mode has local implementations
for these operators.

The error description is not a complete diagnosis for aggregates. `SqlQueryOperator` already
shuffles aggregate keys and captures, restores, rebases, and revokes aggregate state by vnode. The
live groups nevertheless remain in operator-owned maps behind a group-count guard that cannot
bound variable-width keys, accumulator state, changelog output, dirty generations, or allocator
retention. Window-close and join operators additionally lack per-vnode timers/state and input
co-partitioning. See the validation report for the code and empirical evidence.

`laminar_core::state::StateBackend` is deliberately an artifact backend. It writes immutable
checkpoint-attempt vnode payloads, inventories them, and seals an exact attempt. It has no hot
get, scan, atomic batch, timer, spill, or generation-freeze API. Expanding that interface would
mix remote recovery authority with latency-sensitive working state and obscure both contracts.

## Decision drivers

1. Preserve LaminarDB's low-latency Arrow-batch execution; no remote call or async operation per
   record.
2. Bound memory and local disk before admitting previously unbounded SQL.
3. Reuse the checkpoint and ownership invariants already present rather than building a second
   coordinator.
4. Make operator distribution explicit and mechanically auditable.
5. Restore from portable, versioned per-vnode artifacts even after local disk loss or backend
   upgrades.
6. Keep checkpoint barrier interruption independent of total state size.
7. Enable one narrow operator vertical at a time, with fail-closed admission for everything else.

## Architecture

### 1. Planner-certified operator capability

Every physical streaming operator will expose one mandatory capability descriptor:

| Class | Meaning | Cluster rule |
|---|---|---|
| `Stateless` | No retained data influences a later batch | Admit when every input distribution requirement is satisfied |
| `GlobalSingleton` | Retained state is intentionally owned by vnode 0 | Admit only with singleton routing and the managed lifecycle or the existing certified global path |
| `VnodeKeyed` | State and output are owned by a canonical key's vnode | Admit only with certified exchanges, tables, timers, checkpoint, and ownership hooks |
| `RebuildableReplicated` | Read-only state can be reconstructed from a versioned source snapshot | Local/lookup-specific path; not a substitute for mutable keyed state |
| `LocalOnly` | Operator has local state but no distributed contract | Reject with `[LDB-4007]` and its declared reason |

A `VnodeKeyed` descriptor contains:

- the partition-key expression fingerprint and partitioning ABI version;
- stable operator and state-table identifiers;
- state key/value schema versions and compatibility readers;
- input exchange requirements and the expected vnode count;
- event-time or processing-time timer tables and watermark inputs;
- logical retention/cleanup rules;
- append, update, retraction, or unmatched-output semantics; and
- capture, restore, acquire, revoke, and local-rebuild capabilities.

Operator IDs must derive from a canonical, persisted topology identity, not allocation order or a
process-local pointer. Admission compares the planned descriptor with the runtime's exchange,
backend, and ownership configuration. Runtime assertions reject a row for a vnode outside the
operator's active assignment. A stateful operator can no longer inherit no-op vnode hooks and
silently appear cluster-capable.

SQL pre-analysis remains a defensive early rejection, but the physical descriptor becomes the
positive proof. A DataFusion fallback is local-only until it produces the same descriptor and uses
the managed state service.

### 2. Stable partition and state ABI

The resolved vnode count remains fixed for a deployment/pipeline namespace. The following are one
versioned ABI and are persisted in catalog identity, shuffle handshakes, checkpoint descriptors,
and local-state metadata:

- canonical typed key encoding, including null, decimal, timestamp, collation, and NaN semantics;
- key hash algorithm/seed and vnode mapping;
- the vnode-0 convention for singleton global state;
- operator/table ID derivation;
- state-key ordering and value encoding; and
- timer-key ordering.

Golden vectors cover supported Arrow types. Any incompatible change requires an ABI bump and
explicit migration or rejection; it must never silently reinterpret a checkpoint. Assignment
generation and worker identity fence access but are not part of logical state keys, so ownership
can change without rewriting every key.

The physical key prefix is ordered by pipeline, operator/table, and vnode before the logical key.
This permits bounded vnode scans, bulk restore, range deletion, and quota attribution. A physical
database is created per running pipeline graph per worker, not per vnode. Logical tables share the
instance without creating an unbounded number of databases or column families.

### 3. Batched local working-state service

The service contract is synchronous/local from the operator's perspective and batch-oriented:

- multi-get and existence checks;
- ordered prefix/range scan with explicit result byte/row limits;
- one atomic write/delete/timer batch per processed Arrow batch;
- bulk ingest for restore;
- snapshot/freeze at a storage sequence;
- install, validate, and drop vnode ranges; and
- metrics plus resource reservations.

There is no `await` or object-store request per row. Operators collect encoded keys and mutations
while evaluating an Arrow batch, reserve the required resources, read in groups, and commit one
atomic state batch. State changes that must agree—such as an accumulator and its last-emitted value,
a window result and timer deletion, or both sides of a changelog join—share that batch.

The production backend is embedded RocksDB because it provides a mature local LSM, snapshots,
atomic batches, ordered iteration, bulk SST ingestion, block caching, and bounded write-buffer
controls, and it is the common production choice in Flink, Spark, and Kafka Streams. Phase 1 must
pin and qualify the Rust/native dependency on every supported platform, including crash behavior,
native-memory accounting, corruption handling, and compaction stalls. A failed qualification
reopens the backend choice without changing the service or checkpoint contracts.

The in-memory backend is required for model/differential tests and may serve explicitly bounded
local workloads. It is not the cluster production fallback: inability to open or govern the LSM
keeps keyed cluster admission closed.

### 4. Resource governance

One worker-level governor owns reservations across:

- RocksDB block cache, memtables/write buffers, pinned iterators, and native overhead;
- operator scratch data, decoded keys/values, Arrow input/output, and retained output;
- active, frozen, and not-yet-committed mutation generations;
- timer indexes and window/join side metadata;
- shuffle queues and bounded acquire/replay buffers; and
- local state bytes, temporary checkpoint files, compaction debt, and restore staging.

Cardinality counters remain metrics, not safety limits. Reservations happen before mutation with a
documented maximum one-batch slack. Pressure first triggers safe flush/compaction and bounded
backpressure. If capacity is not recovered within the configured deadline, the pipeline faults in
a controlled way and recovers from the last committed cut. It must not OOM, silently drop state, or
invent eviction. TTL/retention deletes state only when it is part of the SQL/operator semantics.

Memory and disk have separate hard limits. Compaction debt and write amplification are explicit
admission/health signals because free disk alone does not prove that the state path can sustain its
write rate. Native allocations are included; Kafka Streams' 2026 RocksDB leak fix is a useful
warning against relying on Rust heap metrics alone.

### 5. Checkpoint bridge

The working copy is local and rebuildable. Cluster recovery authority remains the exact sealed
checkpoint in shared storage.

Each state write batch atomically updates the logical tables and a coalescing mutation journal for
the owning vnode. At an already-aligned barrier:

1. stop admitting a new operator batch;
2. finish the bounded in-flight batch;
3. freeze a backend sequence and rotate the active journal generation;
4. capture watermarks, timers, and operator metadata at the same logical cut; and
5. resume post-cut processing once the immutable handles are safe.

Encoding, checksumming, and uploading per-vnode full/delta artifacts then occur asynchronously
before the existing exact-attempt seal is allowed. The synchronous barrier work is proportional to
the number of participating tables/generations and the bounded in-flight batch, not total state.

Checkpoint artifacts use LaminarDB's versioned portable logical encoding rather than raw RocksDB
directories. Delta artifacts contain latest values/tombstones from the frozen journal; scheduled
full bases range-scan a snapshot asynchronously. Restore may bulk-build SSTs as a backend-specific
optimization, but correctness is defined by the portable records and descriptor digests.

Frozen generations remain referenced until a later committed base/delta chain contains them. An
aborted or failed capture cannot clear its changes; the next attempt includes their union or emits
a full rebase. Limits on concurrent attempts, frozen bytes, and delta-chain length apply
backpressure. A mutable-capture or encoding error faults the pipeline rather than retrying against
partially consumed dirty state.

Local WAL/fsync policy is a recovery-time optimization, not cluster authority. Correctness must
hold after complete local-disk loss by restoring the sealed cut and replaying source input. A local
cache may accelerate restart only after its pipeline identity, ABI, decided checkpoint, and
assignment are validated.

### 6. Ownership and rebalance state machine

Initial rescaling uses checkpoint-cut transfer, not dual writes or record-by-record migration:

```text
owner:      Active -> Frozen/Draining -> Revoked -> local range eligible for deletion
successor:  Unowned -> Acquiring -> Restoring -> Validated -> Active
```

The old assignment token is fenced before a successor publishes output. The successor validates
the decided checkpoint, assignment/process provenance, ABI, schema, and full-plus-delta chain;
installs the vnode; then opens its input/output gate. Rows targeting a restoring vnode are held in
a byte/time-bounded replay buffer or kept at the upstream barrier. They are never processed against
partial state. Other vnodes should continue where the graph can isolate their gates.

After acquisition, the successor's next checkpoint emits a full base for that vnode, bounding
cross-owner delta dependencies. Local range deletion is asynchronous and only follows durable
revocation; stale local data can never authorize ownership.

If cut-over measurements miss the agreed pause/RTO objective, a later ADR may add Megaphone-style
fine-grained logical-time migration or standby replicas. Neither complexity is on the initial
correctness path.

## Operator state models and rollout order

### Grouped aggregate

Managed tables hold the encoded group key, versioned accumulator state, last-update metadata, and
last-emitted changelog value. Accumulator and emission mutations are atomic. Dirty tracking belongs
to the state service, not a second operator map.

The first admitted functions are `COUNT`, `SUM`, `AVG`, and append-only `MIN`/`MAX` with reviewed,
portable accumulator encodings. Changelog `MIN`/`MAX`, `DISTINCT`, and arbitrary UDAFs remain
closed until their multiset/set state is managed and their growth contract is classified. A UDAF
must declare a stable serializer, merge/restore compatibility, and resource behavior before it can
be cluster-capable.

The existing global aggregate remains vnode 0. Distributed partial/global aggregation is a
separate optimization; it is not required to admit grouped state.

### Event-time window

Fixed tumbling windows are first, hopping windows second, and merging session windows last. State
is keyed by vnode, logical group, and window identity. A vnode-owned ordered timer table is keyed by
fire/cleanup time and window key. The checkpoint includes input watermarks/frontiers, allowed
lateness, trigger/accumulation mode, emitted/retraction state, and pending timers.

Timer firing atomically updates or deletes state, records output/retraction bookkeeping, and removes
or advances the timer. Recovery may re-fire an uncommitted output under at-least-once delivery, but
cannot lose a window, fire before its persisted frontier, or retain it indefinitely after cleanup.
Late-data and allowed-lateness behavior are explicit SQL semantics, not an implementation TTL.

Processing-time timers and custom triggers remain local-only until their restart semantics are
specified. Session merging requires atomic range lookup, merge, timer replacement, and retraction,
so it follows fixed windows rather than sharing their initial admission flag.

### Stateful join

The first distributed join is an append-only bounded inner equi-interval join. Both inputs exchange
on the same canonical encoded join key. Each vnode owns two ordered multiset tables indexed by join
key, event time, and stable row identity, plus side watermarks and eviction timers. A match probes a
bounded time range and commits buffered rows and required output bookkeeping atomically.

Watermarks and interval bounds determine when each side can no longer match and may be deleted.
Unbounded stream joins remain rejected unless the user selects a documented finite retention
contract whose semantic effect is visible; an internal cache TTL is not correctness.

Outer/semi/anti joins follow only after unmatched-row timers and emitted/retraction identity are
checkpointed. Changelog joins additionally require signed multiplicities and deterministic
retraction. ASOF, temporal, and session-like joins need ordered-history/version rules and arrive
later. Lookup enrichment stays a distinct versioned replicated/read-through design.

### Materialized output

Enabling a stateful named stream does not enable `CREATE MATERIALIZED VIEW`. Cluster MVs need their
own planner-certified output partitioning, assignment-fenced writes, restore, routed or distributed
reads, and subscription ordering. MV work is a separate phase and keeps the blanket `[LDB-4007]`
guard until all of those pieces pass.

## Correctness and low-latency gates

Before Phase 1, maintainers must check in a reproducible workload profile with hardware, state
size relative to RAM, key/value distributions, hot-key skew, batch size, ingress rate, window/join
bounds, checkpoint cadence, and numerical p99/p99.9 latency, throughput, pause, and recovery goals.
Targets are chosen before optimization results are known; “fast on a laptop” is not a release gate.

Regardless of the profile, these architecture invariants are mandatory:

- no network or object-store I/O per state access;
- no per-row future, fsync, or database transaction;
- no total-state scan in the synchronous checkpoint barrier section;
- no uncharged unbounded map, timer heap, acquire buffer, or checkpoint generation;
- no successor output before state validation and ownership activation;
- no admission based only on a SQL string or a default no-op hook;
- no silent state eviction, partial restore, checkpoint downgrade, or guarantee widening; and
- no claimed production support when the fault/latency suite is skipped.

Release evidence reports p50/p95/p99/p99.9 end-to-end latency and event-loop stall, throughput,
checkpoint align/freeze/upload/seal times, RSS/native/cache/memtable bytes, local bytes and
compaction debt, write amplification, state size/cardinality/timer count per vnode, restore rate,
and vnode unavailability during rebalance. Results include steady state, checkpointing,
backpressure, spill, hot skew, node loss, object-store delay, disk pressure, and `1 -> 3 -> 2`
ownership changes.

Correctness uses differential output/state comparison with embedded/reference execution plus
deterministic crash points before and after state batch, timer fire, freeze, upload, seal,
assignment publish, revoke, install, and activation. A PGVal-style matrix varies data rate,
partitions, topology, parallelism, skew, and fault timing; one happy-path recovery test is not a
guarantee.

## Alternatives considered

### Keep operator maps and improve the group-count limit — rejected

Entry counts do not bound variable-sized state, allocator retention, timers, output history, or
checkpoint copies. Per-operator accounting would also duplicate spill, snapshot, restore, and
pressure policy and leave joins/windows inconsistent.

### Turn `StateBackend` into the live-state API — rejected

Its remote artifact/seal semantics are intentionally attempt-scoped and asynchronous. Combining
them with hot point/range operations would put remote latency on the data path and weaken the
meaning of checkpoint authority.

### Object-store-primary LSM now — rejected

RisingWave Hummock, Materialize Persist, and Flink ForSt demonstrate that this is a storage system:
it needs version metadata/consensus, pinned snapshots, async execution, tiered caches, compaction
ownership, garbage collection, and failure isolation. LaminarDB does not currently have that
system, and ForSt is still documented as experimental. Reconsider only if local-disk operations or
recovery objectives justify the cost.

### One database or checkpoint file per vnode — rejected

Hundreds of databases/files multiply caches, background threads, descriptors, compactions, and
small checkpoint objects. Vnodes are logical ordered ranges inside a bounded number of worker
databases and portable artifacts.

### Kafka changelog topics as the generic authority — rejected

Kafka Streams is coherent because tasks, input partitions, changelogs, and transactions all live
in Kafka. LaminarDB supports non-Kafka sources and already has vnodes and object-store recovery.
Kafka consumer groups or changelogs would create a second assignment/commit authority.

### Remote shared state to avoid migration — deferred

It exchanges restore pauses for steady-state network/tail latency and the remote-LSM system above.
Checkpoint-cut restore is simpler and must be measured first.

### Unaligned checkpoints or fine-grained live migration first — deferred

Flink's unaligned checkpoints capture in-flight buffers and help when measured alignment dominates;
they add artifact I/O and watermark/recovery complexity. CheckMate shows workload-dependent tradeoffs.
Megaphone reduces migration latency but adds routing/time-frontier machinery. Instrument the aligned
cut and initial restore before adopting either.

### Block keyed state on cluster exactly-once — rejected

Correct restore/rebalance is required for at-least-once too. External exactly-once additionally
needs term-fenced source handoff and sink cursor commits. Conflating the programs delays state
correctness and risks falsely advertising end-to-end exactly-once.

## Consequences and risks

Positive consequences:

- one bounded state lifecycle replaces operator-specific maps and snapshot rules;
- checkpoints retain their existing durable authority and portable artifacts;
- each operator can be admitted independently by a positive planner/runtime proof;
- local NVMe preserves the low-latency path while shared storage tolerates node/disk loss; and
- window timers and join cleanup become explicit, testable state rather than side structures.

Costs and risks:

- RocksDB adds native build, memory, corruption, tuning, and compaction operational risk;
- portable state encodings and stable operator IDs become long-lived compatibility contracts;
- asynchronous full/delta materialization needs strict generation retention and pressure control;
- hot keys can still serialize one vnode/operator even when storage is bounded;
- rebalance RTO grows with restored bytes until standby or incremental migration is justified; and
- embedded execution must not regress while maps are replaced incrementally.

Mitigations are the backend qualification spike, shared native-memory governor, version/golden
tests, bounded frozen generations, per-vnode skew metrics, checkpoint-cut rollout, and admission
flags that remain disabled until each vertical passes its evidence gate.

## State of the art considered (facts and LaminarDB inference)

| System/research | Relevant fact | Decision taken here |
|---|---|---|
| [Flink 2.3 key groups](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/datastream/execution/parallel/) and [state backends](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/ops/state/state_backends/) | Fixed key groups are rescale units; heap is low-latency but bounded by memory; EmbeddedRocksDB supports large/incremental state | Keep fixed vnodes; start with a local LSM and portable checkpoints |
| [Flink ForSt](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/ops/state/disaggregated_state/) | Remote/disaggregated primary state uses async access/caching and remains experimental | Do not put object storage on LaminarDB's initial hot path |
| [Flink checkpoint backpressure guidance](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/ops/state/checkpointing_under_backpressure/) | Unaligned capture is useful when measured alignment delay is the problem and captures in-flight data | Keep aligned barriers; instrument before adding unaligned state |
| [RisingWave architecture](https://docs.risingwave.com/get-started/architecture), [consistent hashing](https://risingwavelabs.github.io/risingwave/design/consistent-hash.html), and [checkpoint design](https://risingwavelabs.github.io/risingwave/design/checkpoint.html) | Stable vnodes and barriers combine with Hummock, Meta, cache, version, and compaction services | Reuse vnodes/barriers; do not imitate Hummock without its control/storage services |
| [Materialize views](https://materialize.com/docs/concepts/views/) and [arrangements](https://materialize.com/docs/get-started/arrangements/) | Durable collections and cluster-local indexes have distinct persistence/hydration roles; indexed state may be shared by operators | Keep materialized output separate; consider shared arrangements only after the common store works |
| [Spark Structured Streaming state](https://spark.apache.org/docs/latest/streaming/apis-on-dataframes-and-datasets.html) | Stable shuffle partitions, RocksDB, locality, and changelog checkpointing govern state; stateful exact processing is primarily micro-batch | Borrow the local store/journal pattern, not Spark's latency/execution model |
| [Kafka Streams runtime](https://kafka.apache.org/43/streams/developer-guide/running-app/) and [rebalance protocol](https://kafka.apache.org/43/streams/developer-guide/streams-rebalance-protocol/) | Partition tasks use local stores/changelogs and standby/warmup mechanisms; Kafka is assignment authority | Learn from local restore/standby, but keep LaminarDB's connector-independent vnode authority |
| [Asynchronous Barrier Snapshotting](https://arxiv.org/abs/1506.08603) and [Flink state management](https://www.vldb.org/pvldb/vol10/p1718-carbone.pdf) | A short consistent cut can be separated from asynchronous state materialization | Freeze state at the existing aligned cut and upload outside the pause |
| [Dataflow Model](https://research.google/pubs/the-dataflow-model-a-practical-approach-to-balancing-correctness-latency-and-cost-in-massive-scale-unbounded-out-of-order-data-processing/) and [MillWheel](https://research.google/pubs/millwheel-fault-tolerant-stream-processing-at-internet-scale/) | Event time, watermarks, triggers, persistent per-key state, and timers are correctness concepts | Make timers/frontiers first-class managed state |
| [Differential Dataflow](https://www.microsoft.com/en-us/research/publication/differential-dataflow/) | Incremental collections carry changes/differences | Preserve explicit weights/multiplicity for changelog joins rather than overwriting rows |
| [Megaphone](https://www.vldb.org/pvldb/vol12/p1002-hoffmann.pdf) | Fine-grained migration can reduce latency spikes | Defer until checkpoint-cut migration is measured |
| [CheckMate](https://arxiv.org/abs/2403.13629) | Coordinated versus uncoordinated checkpoint results vary with topology/skew | Treat checkpoint mode as an empirical decision |
| [PGVal](https://www.vldb.org/pvldb/vol18/p585-tahir.pdf) | Claimed guarantees vary across topology, rate, partitioning, and injected faults | Require a multidimensional output-oracle fault matrix |

Flink 2.3.0 (2026-06-25), Spark 4.2.0 (2026-07-14), Kafka 4.3.1 (2026-06-25),
RisingWave 3.0 documentation, and Materialize's current 2026 documentation were checked for this
decision. Vendor documentation establishes what those systems do; the LaminarDB choices above are
explicit design inferences, not claims that another system proves this implementation correct.

## Revisit conditions

Reopen this ADR if backend qualification cannot meet the committed latency/resource profile, local
restore cannot meet the production RTO at the target state size, the vnode/partition ABI is found
incompatible with required SQL key semantics, or a shared-state design becomes justified by
measured operations rather than analogy. Any replacement must preserve positive admission proofs,
portable sealed recovery, byte governance, and fenced ownership.
