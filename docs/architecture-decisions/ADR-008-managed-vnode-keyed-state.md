# ADR-008: Managed vnode-keyed working state for distributed operators

- **Status:** Proposed; implementation requires the Phase 0 review gate
- **Date:** 2026-07-22
- **Decision scope:** Cluster `CREATE STREAM` aggregates, windows, and joins
- **Related:** [validation report](../reports/cluster-keyed-state-validation-2026-07-22.md),
  [implementation plan](../plans/distributed-keyed-stateful-operators.md)

## Decision

LaminarDB will add a common, byte-governed, spillable **working-state service** for keyed
operators. Phase 0 will qualify current Fjall 3.x and RocksDB behind the same narrow service and
workload/fault harness, then record one production backend; it will not ship and maintain both by
default. Fjall is the incumbent project candidate, not an accepted dependency—the current tree no
longer contains it. An in-memory implementation remains a semantic reference and a small
local-mode option. Cluster-shared object storage and the existing `StateBackend` remain the
authoritative checkpoint/recovery layer; neither local LSM is remote recovery authority.

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

LaminarDB has prior Fjall experience, but it is important not to overstate it. The v0.26-era
`state-tier` path introduced at `7b6ad7aa` stored demoted `(operator, vnode)` checkpoint slices and
later individual cold groups behind an asynchronous promotion worker. The current baseline
`1e2f8429` removed that feature and has no Fjall dependency in its workspace or lockfile. That cold
tier was rebuildable capacity, not the always-current managed working state designed here. Its
benchmarks and failure lessons are useful; restoring the deleted implementation would not close
the present lifecycle gap.

## Decision drivers

1. Preserve LaminarDB's low-latency Arrow-batch execution: no remote call, blocking LSM operation,
   future, transaction, or fsync per record. Cold I/O is coalesced at Arrow-batch granularity and
   runs off the compute/event-loop thread.
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

The resolved vnode count remains fixed for a deployment/pipeline namespace. ABI v1 freezes the
existing Arrow `RowConverter` bytes using default sort fields, xxh3-64 with its existing seed, and
modulo vnode mapping; the new codec is a single authority around those semantics, not a second
encoder. The following are one versioned ABI and are persisted in catalog identity, shuffle
handshakes, checkpoint descriptors, and local-state metadata:

- canonical typed key encoding and schema identity, including null, decimal precision/scale,
  timestamp unit/timezone, and binary collation semantics;
- key hash algorithm/seed and vnode mapping;
- the vnode-0 convention for singleton global state;
- operator/table ID derivation;
- state-key ordering and value encoding; and
- timer-key ordering.

ABI v1 rejects floating keys, including every NaN and signed-zero representation, rather than
inventing equality semantics different from embedded Arrow grouping. It also rejects nested and
run-end-encoded keys. Integer-indexed dictionaries hydrate to their admitted logical value type;
index width is representation, not key identity. Strings and binary use raw bytes with no Unicode
normalization, decimal keys use the unscaled coefficient with precision/scale in schema identity,
and timestamps hash their stored epoch integer while unit and exact timezone remain schema
identity.

Golden vectors cover exact encoded bytes, the complete hash, and vnode for every supported family
and rejection class. Restore additionally validates the planned typed schema and every decoded key
against the artifact's claimed vnode before mutation. Any incompatible change requires an ABI bump
and explicit replay/migration or rejection; it must never silently reinterpret a checkpoint. ABI
v1 has no implicit mixed-version reader window. Assignment generation and worker identity fence
access but are not part of logical state keys, so ownership can change without rewriting every
key.

The physical key prefix is ordered by pipeline, operator/table, and vnode before the logical key.
This permits bounded vnode scans, bulk restore, range deletion, and quota attribution. The initial
layout uses one worker-local database with a small fixed set of physical keyspaces; pipeline,
operator, table, and vnode are logical prefixes, never separate databases. Phase 0 must confirm
that this layout gives acceptable failure isolation and cleanup cost. A database or keyspace per
vnode is forbidden; Fjall keyspaces are physical LSM trees with their own write buffers.

### 3. Batched local working-state service

The service contract is local and batch-oriented:

- multi-get and existence checks;
- ordered prefix/range scan with explicit result byte/row limits;
- one atomic write/delete/timer batch per processed Arrow batch;
- bulk ingest for restore;
- snapshot/freeze at a storage sequence;
- install, validate, and drop vnode ranges; and
- metrics plus resource reservations.

There is no `await`, database call, or object-store request per row. Operators deduplicate encoded
keys while evaluating an Arrow batch, reserve resources, and submit one state request. Cache-only
reads may complete inline; any operation that can fault on disk runs on a long-lived bounded
blocking-worker pool. Independent vnode/table lanes may run in parallel, while mutations for one
lane preserve order. A cold batch is deferred as a unit with bounded input and watermark holds;
the compute/event-loop thread never executes LSM I/O and no `spawn_blocking` task is created per
key. An aligned barrier waits for all pre-cut state requests before freezing the cut.

State changes that must agree—such as an accumulator and its last-emitted value, a window result
and timer deletion, or both sides of a changelog join—share one atomic write batch. Hot values use a
compact schema-versioned binary codec with schema metadata hoisted out of each value. Per-group
Arrow IPC streams are prohibited; the prior tier audit found their framing could dominate small
accumulator payloads. Checkpoint export may re-columnarize many logical records together.

#### Evidence-based local-LSM qualification

[Fjall 3.1.8](https://docs.rs/fjall/latest/fjall/) is a credible fit on paper: safe Rust, atomic
cross-keyspace write batches, consistent cross-keyspace snapshots, forward/reverse prefix and range
iteration, sorted bulk ingestion, a bounded block cache, configurable memtables/journals and worker
threads, and a documented stable disk-format policy. It also avoids RocksDB's C++ build and opaque
native allocator. LaminarDB already has Fjall-shaped benchmark and operational experience.

It is not accepted on API shape alone. The current public API has no native multi-get or range
tombstone, returned slices/snapshots can pin cache blocks, each keyspace has a separate memtable,
some compaction/cache/write-buffer counters are documented as hidden or experimental, and recent
3.1 releases include recovery/poisoned-write fixes. The service may coalesce point gets and perform
bounded scan/delete, but Phase 0 must prove that these do not violate tail latency or cleanup RTO
and must obtain stable observability needed for production pressure control.

Historical Windows/consumer-NVMe results from `7b6ad7aa` are warning data, not qualification:
with 300 million 240-byte values (74 GB), Fjall cold-read p99 was about 0.55 ms at 100 writes/s,
1.43 ms at 10k writes/s, and 6.9–7.7 ms near ingest saturation. That confirms write-pressure and
compaction can dominate the tail. The new harness must use the actual always-current state
workload—batched group updates, timer-range scans, snapshots, and checkpoint export—on target Linux
NVMe and report p99.9 as well as p99.

Fjall passes only if it meets the precommitted performance profile, exposes or can upstream stable
cache/memtable/journal/disk/compaction telemetry, obeys hard memory/disk/queue bounds, survives the
crash/corruption matrix, and supports the required portable restore/upgrade policy. The Phase 0
spike runs the same logical batches, timer scans, snapshot/export overlap, restore, cleanup, and
fault schedule against RocksDB rather than comparing unrelated vendor microbenchmarks. RocksDB's
multi-get, range delete, rate limiting, mature operational telemetry, and physical
[checkpoint](https://github.com/facebook/rocksdb/wiki/Checkpoints)/SST-ingest primitives are
advantages; its C++ build, native-memory accounting, platform burden, and compaction tuning are
costs. Physical checkpoints are whole-database, backend-specific mirrors, not portable vnode
artifacts. Select one production backend from evidence, record the rejected candidate and reason,
and keep the service contract independent of both.

The in-memory backend is required for model/differential tests and may serve explicitly bounded
local workloads. It is not the cluster production fallback: inability to open or govern the
qualified LSM keeps keyed cluster admission closed.

### 4. Resource governance

One worker-level governor owns reservations across:

- LSM block cache, memtables/write buffers, journals, pinned values/snapshots/iterators, background
  workers, OS page cache, and any native overhead of a fallback backend;
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
write rate. Returned Fjall slices must be released or copied before they pin cache blocks beyond a
batch. If RocksDB is selected, native allocations are included; Kafka Streams' 2026 RocksDB leak
fix is a useful warning against relying on Rust heap metrics alone.

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

Checkpoint artifacts use LaminarDB's versioned portable logical encoding rather than raw LSM
directories. Delta artifacts contain latest values/tombstones from the frozen journal; scheduled
full bases range-scan a snapshot asynchronously. Restore may use Fjall sorted ingestion or build
SSTs as a backend-specific optimization, but correctness is defined by portable records and
descriptor digests.

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

## Delivery, source, and sink composition

Operator-state correctness and end-to-end delivery are separate contracts that must compose. The
initial cluster release remains **at-least-once**. At the current baseline, cluster admission
rejects `BestEffort` and `ExactlyOnce`; `[LDB-0013]` continues to guard the latter. A certified
cluster source must be non-ephemeral and `Splittable`, with assignment-scoped checkpoint/handoff.
Kafka is currently the only built-in external source with that topology. Kafka source partitions
govern input ownership, while the SQL group/join key governs operator vnodes; checkpoint metadata
must bind both assignment versions and must never assume they are the same partitioning.

The existing source handoff already binds the exact checkpoint attempt, source assignment,
cursors, per-source watermarks, cluster watermark, and recovery frontier. Window support extends
that cut with vnode timer/frontier state rather than creating a second watermark authority. A
source drain or reassignment cannot advance the frontier past input that has not reached managed
state.

At-least-once recovery restores operator state and the source cursor from one sealed cut. It must
not double-apply replay within recovered state, lose timer/output bookkeeping, or skip a result;
external results flushed after that cut may appear again after a crash. The checkpoint tail keeps
the existing ordering: enqueue operator output, flush every durable sink, then seal source
positions. State capture and real sink-flush latency share the checkpoint deadline. A stable output
identity and provenance envelope must be added before the initial release. For the narrow
append-only `COUNT(*)`/`SUM` vertical, the count is the batching-independent logical state version;
identity binds that version and canonical group to deployment, pipeline, and operator identity,
while a separate canonical payload digest detects conflicting values at one version. The input
contract maps each logical group to one Kafka partition so group-local broker order is stable, and
`SUM` is initially limited to exact integer/decimal semantics. Vnode and partition ABI, assignment
version, node ID, boot UUID, and process term accompany the identity. A checkpoint attempt alone is
insufficient because replay can cross attempts and owners. This is evidence for at-least-once
correctness; it is not presented as exactly-once.

A cluster sink used by this release must be `DurableAtLeastOnce + MultiWriter` and accept the
operator's declared output mode. The first candidate is Kafka `envelope=append`; broker topic,
partitioning, acknowledgement, replication/min-ISR, election, DLQ, and retention settings are part
of the certified contract. Ordinary `CREATE STREAM` aggregates emit repeated full running
append-result snapshots, not only modified groups or a final monotonic aggregate. Kafka producer
idempotence cannot deduplicate recovery from a new producer incarnation. There is currently no
built-in cluster-admissible `FullChangelog` sink. Any retraction/full-changelog output remains
fail-closed until either a multiwriter changelog-log sink is certified or mutable sinks gain
key-affine assignment, old-writer fencing, deterministic operation IDs, and vnode handoff. Merely
marking a mutable sink `MultiWriter` is not sufficient.

A stale-owner append is defined by computation or sink admission after the writer lost process or
vnode authority, not by broker arrival time. The current append sink cannot retract an operation
already admitted to broker I/O before the fence; a late acknowledgement of that operation remains
valid at-least-once output and provenance lets the oracle classify it. Mutable output and
exactly-once require provider-side transaction/fencing rather than this observational contract.

End-to-end exactly-once is a later certification per concrete source/state/sink combination. It
requires an exact-certified source and a checkpoint-committable external sink whose transaction
atomically consumes the predecessor cursor and is fenced by deployment, pipeline/sink namespace,
checkpoint attempt, and live leader term. The engine already binds checkpoint decisions to leader
and assignment proofs; the missing portion is connector/provider operations that consume the same
authority and recover ambiguous commits. Local LSM WAL/fsync policy cannot supply that guarantee.

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
- no claimed production support when the fault/latency suite or independent release-candidate soak
  is skipped.

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

Production certification also requires a black-box soak that is independent of the implementation
and its in-process model tests. It runs the release-candidate binary in a production-like
multi-process deployment with real certified source, shared object store, and sink. An external
oracle—not LaminarDB's operator state—checks output/state progress, allowed duplicates, checkpoint
recovery, and stale-owner exclusion for every source/operator/output/sink scenario proposed for
production. The duration, event volume, fault/rebalance schedule, resource leak slopes, and pass
thresholds are committed before the run; raw logs, metrics, manifests, output digests,
configuration, and binary identity are retained for an independent reviewer. An
unexpected harness gap, unexplained anomaly, assertion failure, or relevant binary/configuration
change invalidates the evidence and requires a complete clean rerun. A canary, benchmark, or the
backend qualification soak is not a substitute.

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

### Restore the former Fjall cold tier or preselect an LSM — rejected

The removed tier cached checkpoint slices and used point operations; it did not own always-current
state, and its dirty-state coupling was unsafe. Restoring it would preserve the missing lifecycle.
Conversely, API checklists and old single-insert benchmarks are insufficient to preselect Fjall or
RocksDB. A bounded bake-off is justified because backend choice directly affects the tail,
resource governor, restore, cleanup, and operational surface; a permanent two-backend product is
not.

## Consequences and risks

Positive consequences:

- one bounded state lifecycle replaces operator-specific maps and snapshot rules;
- checkpoints retain their existing durable authority and portable artifacts;
- each operator can be admitted independently by a positive planner/runtime proof;
- local NVMe preserves the low-latency path while shared storage tolerates node/disk loss; and
- window timers and join cleanup become explicit, testable state rather than side structures.

Costs and risks:

- either LSM adds corruption, disk, tuning, and compaction risk; Fjall additionally needs proof for
  batched reads, cleanup, and stable governance telemetry, while RocksDB adds native build and
  allocator accounting;
- portable state encodings and stable operator IDs become long-lived compatibility contracts;
- asynchronous full/delta materialization needs strict generation retention and pressure control;
- hot keys can still serialize one vnode/operator even when storage is bounded;
- rebalance RTO grows with restored bytes until standby or incremental migration is justified; and
- embedded execution must not regress while maps are replaced incrementally.

Mitigations are the backend qualification spike, whole-process resource governor, version/golden
tests, bounded frozen generations, per-vnode skew metrics, checkpoint-cut rollout, delivery-matrix
checks, and admission flags that remain disabled until each vertical passes its evidence gate.

## State of the art considered (facts and LaminarDB inference)

| System/research | Relevant fact | Decision taken here |
|---|---|---|
| [Fjall 3.1.8 API](https://docs.rs/fjall/latest/fjall/) and [RocksDB operations](https://github.com/facebook/rocksdb/wiki/Basic-Operations) | Fjall offers safe-Rust batches/snapshots/ranges but lacks native multi-get and a mature public governance surface; RocksDB offers broader batch/operations controls at native-build cost | Run one workload/fault contract, select one production LSM, and retain portable Laminar artifacts |
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
