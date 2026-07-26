# ADR-008: Managed vnode-keyed working state for distributed operators

- **Status:** Proposed; Phase 0 remains open and cluster admission is unchanged
- **Date:** 2026-07-22
- **Last reconciled:** 2026-07-26 during Cycle 59
- **Decision scope:** Cluster `CREATE STREAM` aggregates, windows, and joins
- **Production/backend verdict:** TidesDB through the official `tidesdb/tidesdb-rs` binding,
  published as Cargo package `tidesdb`, is the selected worker-local implementation line; no
  backend is production-qualified and admission is **NO-GO**
- **Related:** [validation report](../reports/cluster-keyed-state-validation-2026-07-22.md),
  [implementation plan](../plans/distributed-keyed-stateful-operators.md),
  [current owner decisions](../reports/distributed-state-cycle-21-owner-decisions-2026-07-24.md),
  [Cycle 36 owner packet](../reports/distributed-state-cycle-36-owner-decision-packet-2026-07-25.md),
  [TidesDB package design](tidesdb-local-state-successor-design.md),
  [TidesDB T0 source closure](../reports/tidesdb-rs-t0-source-closure-2026-07-25.md), and
  [latest completed review](../reviews/distributed-keyed-state-cycle-59.md)

## Decision

LaminarDB will add one common, byte-governed, batch-oriented working-state service scoped by stable
pipeline, operator, table, vnode, and ownership identities. State access stays local; cold or
blocking work is coalesced per Arrow batch and kept off compute/event-loop threads. The in-memory
implementation is the semantic/lifecycle conformance subject only. The sole current broad-state
product target is a TidesDB worker-local spill profile through the official Rust binding,
contingent on its bounded package prescreen and full qualification. The architecture does not
intrinsically require an LSM or make the product choice production evidence.

Cluster-shared checkpoint storage and the existing `StateBackend` remain recovery authority. A
local store is disposable capacity/latency infrastructure: it cannot assign a vnode, authorize an
epoch, replace restore-before-activate/revoke fencing, or create source/sink exactly-once semantics.
No runtime backend dependency or adapter is authorized by this ADR state.

The provider-neutral Rust `object_store` path retains local file, S3, GCS, and Azure builders.
Cluster authority comes only from the exact checkpoint/state handles admitted by verified namespace
proof plus a cluster-shared `StateBackend`; `file://` is node-durable, not cluster-shared. An
engine-native remote tier cannot replace either handle or supply Laminar's inventory, exact-attempt
seal, coordinator decision, or restore-before-activate authority.

### Current backend and qualification state

| Track | Current disposition | Required next authority/evidence |
|---|---|---|
| In-memory | Reference/conformance-only; no product profile, admission schedule, fallback, or soak matrix | A separate future ADR/charter amendment before any bounded-memory product claim |
| Local-spill product profile | Sole current broad-state product target; the official TidesDB Rust binding remains selected but Cargo package `tidesdb v0.11.1` is stopped, unqualified, and unadmitted | Continue only backend-neutral Laminar lifecycle/checkpoint work. Wait for a repaired official package, repeat T0, then—only on a pass—complete T1, successor non-v4 profile/mapping, qualification, integration, and the independent product soak |
| Qualification contract | Cycle 38 project-owner direction accepts maintenance-health v2 and exact v4 for validation-only implementation; no GitHub approval workflow exists or is required for that scope; v1 remains immutable regression lineage | Permission is limited to standalone schemas, parsers, formulas, bounded readers, synthetic ineligible fixtures, and negative-capability tests. Candidate source construction, execution, runtime, and production remain separately closed |
| RocksDB 10.4.2 via `rocksdb` 0.24.0 | Mature operational LSM reference and immutable v1-v4 regression/comparison subject; not the product backend | No new adapter, source-closure, or qualification work is scheduled absent a new project-owner direction |
| Fjall 3.1.8 | Frozen v4 comparison/closure subject; stock scheduler/lifecycle/governance signals do not close the gate; no fork is planned | Reopen only through an explicit owner decision and a qualifying official upstream release |
| redb 4.1.0 | **PARKED after Cycle 34**; administrative status, not a formal `DEFER` result; design timebox exhausted; no candidate profile, adapter, mechanism result, or execution authority | No scheduled work. Reopen only through an explicit two-day/four-machine-hour micro-prescreen charter; otherwise retain as history. A favorable observation could only fund a later mapping/profile proposal |
| Official `tidesdb/tidesdb-rs` binding: Cargo package `tidesdb v0.11.1`, native 9.3.6 | **STOP_WAIT_FOR_UPSTREAM at T0** while remaining the selected integration line: the payload misses relevant later native correctness/memory-safety fixes, and one-CF transactions can acknowledge a short partial batch; the general cgroup envelope and mandatory public maintenance-health facts also remain unclosed. Restricted owner/lifetime containment passes | Do not run T1, add the dependency, or implement its adapter. Wait for a new official package, freeze the pair, and repeat T0. The repeat may assess a pre-output verified-commit/fail-stop design for the short-batch gap, but no Laminar facade can repair the missing native fixes. Native remote/filesystem object-store modes remain disabled |
| SurrealKV 0.21.2 | Rejected unmodified; no active candidate track | Correctness/liveness fork and new bounded prescreen authority before reconsideration |

The current source detail and rationale live in the
[placement analysis](../reports/state-working-state-options-2026-07-24.md),
[v2 direction](state-backend-maintenance-health-v2-proposal.md),
[v2 validation contract](state-backend-qualification-runner-v2-draft.md),
[Cycle 36 owner packet](../reports/distributed-state-cycle-36-owner-decision-packet-2026-07-25.md),
[candidate mapping designs](../reports/state-backend-maintenance-health-mapping-designs-2026-07-24.md),
[RocksDB closure](../reports/rocksdb-mechanism-source-closure-2026-07-24.md),
[redb prescreen](../testing/state-backend-redb-prescreen-v1.md), and
[TidesDB prescreen](../reports/tidesdb-static-prescreen-2026-07-25.md),
[TidesDB package design](tidesdb-local-state-successor-design.md), and
[TidesDB T0 source closure](../reports/tidesdb-rs-t0-source-closure-2026-07-25.md). These are evidence
and gate records; the integration policy is selected, but the current release is stopped and no
runtime dependency, qualification, or production admission follows.

The existing fixed vnode ABI, bounded shuffle, assignment/process fencing, aligned barriers,
per-vnode checkpoint artifacts, and exact-attempt seal are retained. Cluster admission will move
from SQL-shape exclusions plus permissive operator hooks to a planner-certified distribution/state
descriptor. Each stateful operator must declare its partition key, stable state tables, timers,
retention, output mode, checkpoint schema, and acquire/revoke behavior.

The implementation order is grouped aggregates, fixed event-time windows, then bounded interval
joins. Stateful streams may be enabled before cluster materialized views. `[LDB-4007]` remains
closed for keyed/windowed/join state and `[LDB-0013]` remains closed for cluster exactly-once. The
first keyed-state release remains at-least-once and requires a separately certified source/operator/
output/sink combination. Production additionally requires the independently operated,
immutable-release soak; backend qualification or a redb prescreen cannot substitute for it.

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
- operator/state-table ID derivation;
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

The physical key prefix is ordered by pipeline, operator/state-table, and vnode before the logical
key. This permits bounded vnode scans, bulk restore, range deletion, and quota attribution. The
selected TidesDB layout uses one worker-local database and one fixed physical managed-state column
family; pipeline, operator, table, vnode, and generation are logical prefixes, never separate
databases. Phase 0 must confirm acceptable isolation, cleanup cost, and hot-writer/disjoint-vnode
tails. A database or column family per vnode is forbidden. The frozen v4 Fjall/RocksDB reference
layout retains its distinct historical physical-keyspace semantics.

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

Cycle 42 closes one existing execution-layer gap without introducing the future working-state
service. Ordinary projection or planning errors that occur ahead of aggregate mutation retain
their existing disposition. Existing stronger dispositions also remain unchanged: a partial
shuffle send already requires recovery and a structural terminal failure still halts. Once
`process_batch` is entered, or aggregate output construction begins, an otherwise ordinary error
is conservatively an indeterminate stateful apply and forces coordinated recovery. The coordinator
publishes neither that cycle's output nor a newly due checkpoint after the recovery result. This is
not the future backend's sticky root/process poison: that mechanism remains part of an admitted
working-state owner and must prevent reuse of an ambiguous native root. No TidesDB dependency or
adapter follows.

Cycle 43 applies the stronger failed-before-apply outcome where the operator already has a natural
commit point. The analytic window-frame operator now computes its candidate retained tail, runs the
fallible residual projection, and replaces logical history only after that projection succeeds. A
projection error or cancellation while it is pending therefore leaves the prior history unchanged
and remains an ordinary pre-commit failure; replay cannot append the same rows twice. The successful
path performs the same tail calculation and projection as before, with no additional copy, scan,
lock, atomic, task, or I/O. This local correction does not supply vnode ownership, durable state,
rebalance, or cluster admission; broader EOWC and join mutation boundaries remain open.

Cycle 44 makes the current ASOF join's returned-error boundary explicit. Right-buffer ingest now
reports whether it installed rows only after every fallible preflight step; learning the right
schema is also treated as logical state change. A returned join/projection error requires recovery
only if that cycle changed either state, while a left-only error against unchanged retained state
remains ordinary. Every returned eviction error requires recovery because index pruning precedes
fallible compaction. Existing recovery/halt dispositions remain stronger. This covers returned
errors, not panic or cancellation after ingest; sticky attempt/root poison remains open. ASOF's
learned right schema is also not yet represented when an empty buffer is checkpointed, so complete
restore remains open and cluster admission remains unchanged.

Cycle 45 closes that local ASOF restore gap without changing the v1 rkyv buffer body. Checkpoint v2
adds a length-delimited, header-only Arrow schema appendix only when compaction leaves no retained
right rows; a retained batch remains the sole schema authority. Source-schema memory, appendix wire,
and decoded-schema memory are bounded at 256 KiB, 512 KiB, and 1 MiB. Byte-level frame preflight
checks declared Arrow bodies against the available payload before Arrow may allocate; schema streams
must contain only a schema plus EOS, and retained buffers exactly one batch plus EOS. Restore validates
schema/key/time compatibility and every persisted index-to-row mapping and tie order before atomically
replacing buffer, watermark, and learned schema. Checkpointability is enforced before first ingest
and during restore: first admission executes the exact bounded schema encoder and framing preflight,
while later pointer-distinct schemas recheck memory before structural equality because equality omits
allocation capacity. In-limit non-empty v1 checkpoints derive their schema; ambiguous empty v1 LEFT
checkpoints fail recovery-closed, while empty v1 INNER checkpoints remain compatible but cannot
recover prior learned-schema history. Pointer-identical live schemas stay on the immediate fast path
and drift is rejected before ingest. This is operator-local checkpoint correctness: cancellation/
panic poisoning, vnode ownership, rebalance, sink/source delivery contracts, backend qualification,
and cluster admission remain open.

Cycle 46 separates the current compute-root lifecycle from the borrowed graph API. Production
normal/shutdown/checkpoint paths await each graph pass without timeout or `select!`; an operator
panic unwinds and destroys the callback and graph before the compute-root supervisor faults and
restores. A directly borrowed `OperatorGraph::execute_cycle` future could nevertheless be dropped
after an operator mutated while graph inputs and partial results were future-local, leaving the same
graph reusable and checkpointable. A real ASOF/downstream-pending test reproduced that retained-
owner ambiguity before the fix. Each graph generation now owns a sticky atomic fence. One cycle-
level guard arms after the cluster rotation read fence but before input/state admission, disarms on
every explicit `Result`, and poisons on cancellation or unwind. A poisoned generation rejects
normal execution, checkpoint drain, whole-state capture, and per-vnode capture as an indeterminate
stateful apply; the callback exposes the same sticky condition to its existing pipeline-fault
consumer. Only a newly built graph restored from the last committed checkpoint can replay. The
normal path adds one atomic read and one `Arc` clone/drop per graph cycle, never per row/operator,
and adds no task, timeout, lock, or I/O. This fence does not cover a future cancellation introduced
after a checkpoint-drain graph pass returns while sink publication is pending, and it is not the
future backend-owned poison needed when a native operation may outlive the Rust graph. Those
delivery/native-owner boundaries, vnode ownership, backend qualification, and independent soak
remain open; cluster admission is unchanged.

Cycle 47 audits the next await boundary rather than adding a partial fence. A deterministic red
probe let one checkpoint-drain pass consume three buffered output batches, blocked the first batch
inside a real sink actor with a one-slot command queue, queued the second, and dropped the drain
future while the third awaited admission. The retained callback had no pipeline fault. This proves
the private borrowed future is not cancellation-safe after graph completion, but the production
owner chain never performs that drop: leader, source-less, and follower checkpoint paths directly
await the whole drain; nested deadline and process-lease cancellations return to the callback;
publication in replay-required and cluster modes records a fault and returns recovery, while local
best-effort enqueue loss records a sink timeout whose later fence blocks capture (`Skipped` after a
successful FIFO sync, otherwise `Failed`; overall drain-deadline expiry returns recovery); and root
unwind destroys the callback generation.

A checkpoint-drain-only graph guard is rejected for now. It would not cover the same hypothetical
normal-cycle MV/stream/sink publication boundary and cannot resolve coordinator-owned source
barriers or exact-attempt cleanup. The callback contract instead requires the whole drain future to
reach an explicit result or the complete callback/coordinator generation to be destroyed. Any
future outer timeout, `select!`, or task abort must first introduce one coordinator-owned attempt
transaction spanning the frozen input cut, graph and output publication, source-barrier ownership,
and exact-attempt cleanup. This decision adds no runtime or hot-path work and does not make native
backend calls cancellation-safe. Already accepted at-least-once sink output may duplicate after a
future recovery; cluster exactly-once remains rejected.

Cycle 48 closes a separate, production-reachable checkpoint/assignment race without changing this
future managed-state protocol. Staged vnode acquire/revoke work and an unexecuted current assignment
now make the graph non-quiescent and both snapshot APIs fail closed until one drain pass applies the
transition. After the sink FIFO fence, leader and follower capture paths take the existing rotation
read token before shuffle alignment, revalidate the admitted assignment certificate, and retain the
token through whole plus vnode mutable capture. Assignment adoption takes the opposing write token
before staging or publication. Post-shuffle supersession is recovery-required; proven pre-staging
supersession cancels without capture. The token is released before encoding, checkpoint-tail I/O,
or awaited cleanup; deadline-bounded shuffle transport and authority-settlement reads remain inside
because aligned channel state belongs to the cut.

The current global-aggregate exception is compositional: separate admitted stream queries can put
multiple vnode-0 aggregate operators in one graph. A later transition-apply failure can follow an
earlier mutation, but the existing checkpoint error maps to whole-generation recovery, publishes no
output/cut, and destroys that graph before returning. This containment justifies no second poison
for explicit returned errors. It does not authorize reuse of the sequential apply loop for future
keyed/window/join/MV state. Those operators still require the authoritative roster, complete
off-side prepare, and infallible whole-graph publish specified below.

Cycle 48 adds no normal row-path operation and no new lock, atomic, task, backend, or abstraction.
Checkpoint capture adds short staging-map/version checks and a deadline-bounded read acquisition on
the existing fence. Cycle 49 proves that immediate and deferred follower routes hold that token
through whole/vnode capture and release it before their durable tails. It also removes a latency
inversion: a non-abortable encoder can retain the sole serialization permit after its originating
attempt times out, so awaiting that permit under the rotation token could outlive the rebalance
writer's deadline. Because valid one-in-flight admission has no steady-state waiter, a contending
capture now returns `[LDB-6017]` synchronously before state mutation; the old encoder remains owned
and fenced until it exits, and no second sticky fault is created. The removed timeout wrappers
could not preempt the synchronous snapshot calls. This replaces one checkpoint-only await/timer
with a semaphore try-acquire and changes no normal row path. Synchronous capture duration and full
checkpoint-versus-rotation wait distributions remain independent-soak gates. Delivery remains
cluster at-least-once, and no source, sink, exactly-once, or `[LDB-4007]` capability changes.

#### Frozen Fjall/RocksDB evidence and selected TidesDB binding line

The Fjall/RocksDB material below is retained as exact reference and regression rationale, not the
active product work order. Cycle 40 records the official `tidesdb/tidesdb-rs` binding, published as
Cargo package `tidesdb`, as the only selected TidesDB integration line. Cycle 41 stops exact
v0.11.1/native 9.3.6 at T0. Native 9.3.14 remains
comparison/source evidence, retains the decisive silent-short defect, and cannot be substituted
behind the package. The same common workload and absolute gates still apply; the product choice and
vendor results are not qualification or production evidence.

[Fjall 3.1.8](https://docs.rs/fjall/3.1.8/fjall/) is a credible fit on paper: a Rust-native API and no
C++ storage engine, atomic
cross-keyspace write batches, consistent cross-keyspace snapshots, forward/reverse prefix and range
iteration, sorted bulk ingestion, a configurable block-cache capacity, configurable
memtables/journals and worker threads, and a documented stable disk-format policy. It also avoids
RocksDB's C++ build and opaque native allocator. LaminarDB already has Fjall-shaped benchmark and
operational experience.

It is not accepted on API shape alone. The current public API has no native multi-get or range
tombstone. Slices can retain backing buffers including cache blocks; snapshots and iterators retain
old MVCC versions and delay reclamation. Each keyspace has a separate memtable, while the O(1)
`Keyspace::clear()` cannot clean one vnode prefix in the shared-keyspace layout. Public counters
cover items such as write-buffer bytes, journals, disk use, approximate length, and fragmented blob
bytes, but stable cache-hit, compaction-debt/backlog, stall, and total-pressure telemetry is still
insufficient. The configured block-cache capacity is not a hard process-memory governor.

An atomic Fjall batch defines consistency, not power-loss durability: ordinary writes reach OS
buffers, and the selected policy must explicitly call `persist(SyncData|SyncAll)` or prove an
equivalent group-durability boundary. Version 3.1.8 was four days old at this ADR date and followed
recent [recovery](https://github.com/fjall-rs/fjall/releases/tag/3.1.1),
[clear/recovery](https://github.com/fjall-rs/fjall/releases/tag/3.1.4), and
[poisoned/buffered-write](https://github.com/fjall-rs/fjall/releases/tag/3.1.7) fixes. The service
may coalesce point gets and perform bounded scan/delete, but Phase 0 must prove tail latency,
cleanup RTO, crash behavior, and upgrade compatibility rather than treating the disk-format policy
as a substitute for N/N-1 testing.

Historical Windows/consumer-NVMe results from `7b6ad7aa` are warning data, not qualification:
with 300 million 240-byte values (74 GB), Fjall cold-read p99 was about 0.55 ms at 100 writes/s,
1.43 ms at 10k writes/s, and 6.9–7.7 ms near ingest saturation. That confirms write-pressure and
compaction can dominate the tail. The new harness must use the actual always-current state
workload—batched group updates, timer-range scans, snapshots, and checkpoint export—on target Linux
NVMe and report p99.9 as well as p99.

In this frozen reference analysis, any later patched/admitted Fjall subject passes only if it meets
the precommitted performance
profile, exposes stable cache/memtable/journal/disk/compaction telemetry, obeys hard memory/disk/
queue bounds, survives the crash/corruption matrix, and supports the required portable restore/
upgrade policy. Every admitted candidate runs the same logical batches, timer scans, snapshot/export
overlap, restore, cleanup, and fault schedule rather than comparing unrelated vendor microbenchmarks;
no adapter is authorized before candidate-specific mechanism closure. RocksDB's
multi-get, range delete, rate limiting, mature operational telemetry, and physical
[checkpoint](https://github.com/facebook/rocksdb/wiki/Checkpoints)/SST-ingest primitives are
advantages; its C++ build, native-memory accounting, platform burden, and compaction tuning are
costs. MultiGet benefits depend on table/filter configuration and the chosen Rust binding.
DeleteRange tombstones can degrade reads, one column family's write stall can stall the database,
and the generic rate limiter governs background flush/compaction I/O rather than WAL durability or
per-vnode QoS. Physical checkpoints are whole-database mirrors; column-family export is still not a
portable logical vnode artifact. SST ingest needs sorted input and may flush overlapping memtables
or briefly block writes, so it is a measured restore optimization. Qualification pins an exact
engine, wrapper/integration, source/build identity, and configuration before relying on any API
behavior.

The accepted [TidesDB package design](tidesdb-local-state-successor-design.md) uses only the official
binding's public safe API behind a Laminar-owned restricted facade: one database, one retained fixed
CF, one dedicated owner lane, lexical transactions/iterators, copied outputs, and child-before-
parent shutdown. It prohibits a private FFI, raw handles, callbacks, forks, patches, system-library
or native-version substitution, prior-directory reopen, native checkpoint, and native remote mode.
A new directory is restored only from Commit-admitted Laminar artifacts and no per-batch `FULL`
fence is added. Cycle 41 T0 proves owner/lifetime containment but stops v0.11.1: native 9.3.6 misses
relevant later native correctness/memory-safety fixes, can acknowledge a short partial one-CF
transaction, uses a host-derived memory floor, and lacks mandatory public maintenance-health facts.
A future pre-output verified-commit/fail-stop design may address only the acknowledgement gap and
must pay its measured hot-path cost. T1 is cancelled for this release. Immutable logical
cuts, portable restore, latency/concurrency, faults, delivery integration, and independent soak
remain unstarted hard gates. Re-entry requires a new official package and a complete repeated T0;
private native work remains prohibited.

The in-memory backend is required for model/differential tests and is the first placement-neutral
lifecycle implementation after the existing Phase 0 review gate. It remains reference/conformance-
only and is not a cluster product profile under this ADR. It is not the broad cluster production
fallback: inability to open or govern the qualified disk-backed backend keeps the general
local-spill profile closed. Any future bounded-memory product proposal starts with a separate ADR
amendment rather than inheriting the reference implementation's evidence.

### 4. Resource governance

One worker-level governor owns reservations across:

- disk-backend block cache, memtables/write buffers, journals, pinned values/snapshots/iterators,
  background workers, OS page cache, and all native overhead of the selected candidate;
- operator scratch data, decoded keys/values, Arrow input/output, and retained output;
- active, frozen, and not-yet-committed mutation generations;
- timer indexes and window/join side metadata;
- shuffle queues and bounded acquire/replay buffers; and
- local state bytes, temporary checkpoint files, compaction debt, and restore staging, including
  artifact/descriptor/payload bytes, chain links/depth, operators/vnodes per transition,
  groups/accumulators/rows, canonical key/state bytes, and output-buffer bytes.

Cardinality counters remain metrics, not safety limits. Reservations happen before mutation with a
documented maximum one-batch slack. Pressure first triggers safe flush/compaction and bounded
backpressure. If capacity is not recovered within the configured deadline, the pipeline faults in
a controlled way and recovers from the last committed cut. It must not OOM, silently drop state, or
invent eviction. TTL/retention deletes state only when it is part of the SQL/operator semantics.

Memory and disk have separate hard limits. Common disk-growth/write-amplification gates plus the
candidate-applicable maintenance-health and exact foreground-stall gates remain mandatory because
free disk alone does not prove that the state path can sustain its write rate. All retained backing
buffers must release or copy before a batch ends, and snapshots/iterators have bounded lifetimes so
old versions can be reclaimed. Native allocations are included for any C/C++ candidate, including
the selected TidesDB path; [Kafka Streams' 2026 RocksDB leak
fix](https://kafka.apache.org/blog/2026/06/25/apache-kafka-4.3.1-release-announcement/) is a useful
warning against relying on Rust heap metrics alone.

### 5. Checkpoint bridge

The working copy is local and rebuildable. Cluster recovery authority remains the exact
coordinator-admitted checkpoint in shared storage with a durable terminal Commit.

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
full bases range-scan a snapshot asynchronously. Restore may use a candidate's proved sorted or
bulk-ingestion facility as an optimization, but correctness is defined by portable records and
descriptor digests.

Keyed restore is a prepare/publish transaction, not an incremental merge of trusted bytes. Its
versioned envelope binds the partition ABI, vnode count and claimed vnode, key mode, canonical key
schema, operator identity, state-table/accumulator schema, payload codec, and explicit `FULL`,
`DELTA`, or `EMPTY` kind. Preflight uses schemas cached by the physical plan, validates every state
and emission key against the claimed vnode, checks complete-chain ordering and cross-vnode
disjointness, reserves the bounded restore budget, and computes an authoritative replacement for
the entire vnode namespace. Emission keys require exact arity and types; restore never casts them.
The current raw aggregate checkpoint is legacy input for the admitted global vnode-0 aggregate
only; a query fingerprint alone is not a keyed-state compatibility contract.

#### Artifact and schema contract

Routing compatibility and stored-state compatibility are separate identities. A grouped artifact
persists the hydrated `PartitionKeySchemaV1` used by the partition ABI and a Laminar-owned state
contract containing key mode, stable operator/state-table identity, ordered accumulator codec IDs
and versions, and exact logical input/output/null semantics. Dictionary inputs are hydrated before key
encoding; dictionary index width is never durable identity. Global state has an explicit
`GlobalSingleton` mode and is valid only on vnode 0—it is not inferred from an empty descriptor.

The first managed aggregate payload does **not** use Arrow IPC. COUNT/SUM state is a compact,
length-delimited Laminar row record, sorted strictly by canonical encoded key off the event-loop.
The contract determines a fixed state width: checked `u64` `COUNT(*)`, followed for each admitted
`SUM(Int64)` by checked `u64` non-null count and signed `i64` accumulator, all in canonical
big-endian form. COUNT's SQL result remains non-null `Int64` and SUM's remains nullable `Int64`.
For each group, the executor evaluates rows in fixed source order and checks count, non-null count,
and SUM at **every input prefix**, not just the coalesced batch result. It preflights all groups in
the Arrow batch before publishing one atomic state/output mutation; overflow in a late row or group
faults with no group or output from that batch applied. Thus `[MAX, +1, -1]` cannot pass merely
because it was coalesced into one batch. The same Laminar checked implementation becomes the
embedded/reference executor for this exact shape before cluster admission, with a compatibility
note for the former DataFusion 52.3 wrapping/profile-dependent behavior. Decimal, unsigned,
floating, `AVG`, `MIN`/`MAX`, retractions, and UDAFs remain codec-unavailable until separately
specified and tested.

Each non-empty payload row is `u32_be key_length | key_bytes | fixed_state_bytes`. A zero key length
is valid: ABI v1 admits a nonempty `Null` grouping schema, for which Arrow 57.2 encodes the row as
zero bytes; the persisted nonempty routing-schema descriptor distinguishes that keyed row from a
global singleton. `EMPTY` is identified only by artifact kind and row count. Key length, row count,
aggregate key bytes, state bytes, descriptor bytes, artifact bytes, and full-plus-delta chain
bytes are checked against the approved profile with checked arithmetic before allocation. Keys are
unique and strictly increasing, and every keyed row must hash to the claimed vnode. `EMPTY` is the
only zero-row representation. A `FULL` image replaces the entire operator/state-table/vnode namespace;
`DELTA` carries sorted latest values; append-only v1 has no deletes or tombstones. Restored routing
keys remain opaque hash/LSM identity and are never passed to Arrow `RowParser`, whose format assumes
converter-produced input; any future materializing consumer requires a strict, panic-free decoder.
Decode rejects a zero-row `FULL`/`DELTA` payload, a stored row with `COUNT(*) == 0` or
`COUNT(*) > i64::MAX`, a non-null SUM count greater than COUNT, and zero non-null count with a
nonzero accumulator. When the cached contract declares a non-nullable SUM input, its non-null count
must equal COUNT. Zero non-null count has canonical zero accumulator bytes and evaluates to SQL
`NULL`; otherwise the exact signed `i64` accumulator is the nullable `Int64` result. Cycle 5
publishes one normative binary layout with every magic, field offset, width, byte order, and digest
range plus frozen goldens, a private borrowed reader, and a test-only full-buffer fixture encoder.
The fixture encoder is not release code or the production streaming writer; this ADR does not treat an archived
Rust type or prose alone as the wire specification.
Current `last_updated_ms` is not part of v1 because no aggregate execution path consumes it.
Changed-group append output derives its stable
operation identity from the canonical key and checked count version, so v1 also carries no
`last_emitted` value. Timer/TTL or changelog codecs must add their own named state rather than
silently extending this record.

The payload sits inside the manually parsed, allocation-bounded
[managed state artifact v1 envelope](managed-state-artifact-format-v1.md). It binds exact
format/header length, inner `FULL`/`DELTA`/`EMPTY`, key mode, partition ABI, codec ID/version, total and
section lengths, row/key/state totals, checkpoint and parent identity, assignment version, vnode
count and vnode, owner-map certificate digest, stable operator/state-table/contract digests, and payload
digest. Reserved fields must be zero; total length must equal the supplied slice with no trailing
bytes. Descriptor bytes are compared byte-for-byte with the immutable plan-time contract after
their digest is checked. SHA-256 supplies corruption evidence, not authentication; trust and
encryption remain checkpoint-store/deployment properties.

Managed artifacts cannot be nested in the current `VnodePartial` rkyv object, whose attacker-sized
vectors are materialized before an inner payload can reserve memory. `VnodePartialV2` has an
unambiguous magic/version selected by the checkpoint manifest—decoders never try v2 and then fall
back to rkyv. Its canonical directory has a 160-byte header and one sorted, unique
operator/state-table/vnode entry for the authoritative roster. An entry is either `BODY`, naming an
exact inner FULL/DELTA/EMPTY slice, or `REFERENCE`, naming the exact parent checkpoint and entry
digest for unchanged state. Absence is corruption, `REFERENCE` is not empty state, and `EMPTY`
remains an authoritative zero-row base. BODY ranges are non-overlapping, in bounds, and exactly
cover the declared body region with no padding. The directory digest covers the directory and each
BODY entry covers its exact slice; a redundant whole-body digest is deliberately omitted.

Cycle 5 lands a private borrowed outer-structural V2 reader and a test-only full-buffer fixture
encoder. The outer reader validates checked layout, roster, ranges, entry kinds, ancestry shape, and
per-entry BODY digests against its expected source context. It does not authenticate the complete
object or establish aggregate-state semantics. Production composition must first match the complete
payload to the trusted seal/inventory digest and manifest selector, then invoke the expected inner
reader for every BODY with exact identity, kind, parent, codec, routing-schema, and state-contract
context. The fixture encoders compile only in tests, allocate complete vectors, and are not
production streaming writers.

The trusted checkpoint pointer first identifies the inventory object and its expected digest. A
metadata/HEAD request must expose its encoded length; restore rejects a value above
`transition_metadata_bytes_max` and acquires the global encoded-byte charge before GET, response
buffering, transport reassembly, alignment copy, or parsing. The inventory is streamed to that exact
cap, digest-verified, and then authenticates expected artifact type/version, content lengths and
digests, checkpoint/assignment provenance, and legacy/managed dispatch. Each artifact repeats the
same reserve-before-GET protocol. Short, long, or digest-mismatched bodies fail closed.

That trusted sealed-object composition, manifest dispatch, and bounded fetch path are not wired in
Cycle 5. The private structural reader must not be called directly on bytes fetched by the current
whole-object `read_partial` path and must not be used as evidence to relax cluster admission.

V2 parsing runs under the encoded charge; separate task/global **scratch** reservations cover
directory metadata, key/state bytes, rows, shadow ingestion, operators, and vnodes. Transition-owned
validated spool bytes are charged to the local-disk governor and retained until prepare completes;
they are never an unbounded in-memory copy. The candidate profile separately names per-artifact and
per-chain encoded caps, a directory-entry cap, a global encoded-byte pool, and per-task/global
scratch caps; that machine-readable profile remains the sole numerical source. One "artifact" is
the complete raw `VnodePartialV2` payload (excluding the existing fixed provenance wrapper), not
each inner BODY: its row, key-byte, state-byte, and encoded-byte caps are cumulative across all BODY
entries. Inner aggregate decodes consume one caller-owned, non-`Copy` mutable
`AggregateObjectBudget` ledger for the complete V2 object; the ledger is never reset per BODY.
Wrapper plus payload is checked before fetch. `resolved_parent_links_max` counts every
outer `REFERENCE` and inner `DELTA` parent edge; a FULL/EMPTY base has depth zero, exactly the
maximum is accepted, and maximum-plus-one is rejected. Whole-transition preflight resolves every
REFERENCE and validates every chain and row into that immutable spool before any operator callback.
Prepare then consumes the spool into abortable shadow LSM state.
Legacy rkyv/Arrow decoding is selected only by manifest/type proof for the currently admitted global
vnode-0 path.

Before the first artifact GET, checked inventory arithmetic reserves the transition's declared spool
bytes against the local state-disk governor and project quota. Spools are namespaced by transition
digest and become readable only through an atomic completion marker. A retry retains the same
completed spool; process restart discards incomplete/unreferenced spools and can rebuild them from
remote authority. Successful publication or terminal transition rejection releases and removes the
spool outside the ownership fence. Pressure/hard-stop policy includes live state, LSM amplification,
and all retained spools; restore backpressures rather than exceeding the reservation.

The initial codec registry admits only concrete reviewed Laminar implementations. Function names
are not codec identity because a UDAF can reuse a built-in name. Direct rkyv of live Rust/DataFusion
types and hash-map iteration order are not durable ABI. Fresh/populated, null-only, per-prefix
overflow, split/coalesced, late-group rollback, and impossible-restored-state goldens; truncation and
every-limit/max-plus-one vectors; duplicate/out-of-order/cross-vnode keys; checksum/provenance
failures; and N/N-1 compatibility tests precede a writer. Contract derivation is plan-time;
sorting, encoding, hashing, and restore decoding run on bounded blocking workers. None runs on the
record/event-loop hot path, and qualification still measures their CPU, memory, pause, and tail
effects.

The landed borrowed readers and frozen fixture vectors are admission-neutral conformance
primitives. They do not resolve REFERENCE/DELTA chains, produce the authoritative replacement namespace, ingest
shadow state, publish a graph transition, alter a checkpoint manifest, or relax `[LDB-4007]`.

#### Whole-graph publication boundary

One staged transition binds the exact committed checkpoint attempt and checkpoint assignment fence
to one target assignment version, nonzero vnode count, and owner-map digest. It contains acquired
base-plus-delta chains, revoked vnodes, and a digest of the complete state-lifecycle operator
inventory. Every required operator/vnode pair has an explicit `BODY` or `REFERENCE` entry, and each
resolved chain terminates in an authoritative `FULL` or `EMPTY` base; absence is corruption. The
old split staging maps and successful default vnode hooks are not eligible for the managed path.

Restore follows this protocol:

1. snapshot the exact staged transition without removing it;
2. preflight the whole batch—attempt/parent links, assignment provenance, vnode ranges,
   acquire/revoke disjointness, duplicate-free canonical operator inventory, explicit bases, plan
   contracts, key membership, resource limits, and topology—before any operator is called;
3. ask every lifecycle participant to prepare all of its acquired and revoked vnodes into shadow
   state; on any error, abort prepared shadows in reverse order and retain the identical staged
   transition with every acquired vnode still `Restoring`;
4. enter the graph's exclusive callback/publication section with source, shuffle, checkpoint, and
   output intake closed; acquire the existing rotation fence to freeze assignment authority, then
   revalidate the target owner-map digest, registry states, transport scope, and exact transition;
5. complete every fallible action before publication, then perform only unit-returning pointer or
   generation swaps, mark the complete acquired set `Active`, and remove that exact transition;
   and
6. leave the short publication section, destroy retired shard handles asynchronously, and open
   source/shuffle/output intake only after complete publication.

Sequential in-memory swaps are logically atomic because exclusive graph/callback serialization and
closed intake prevent a source row, shuffle row, checkpoint, or output from observing the graph
between them; the rotation fence separately prevents assignment change. Swaps retain old handles
so refcount drops and destructors cannot extend the fenced section. A process crash in that short
publication section discards the process image and reconstructs from the unchanged durable cut. A
changed assignment during asynchronous prepare aborts the shadows and publishes nothing.

Before any artifact fetch, graph construction runs a pure, fallible state-contract derivation for
every lifecycle participant and caches the exact incremental physical plan, implementation/codec
IDs, schemas, and digest. `Uninit` thereafter means that no working shards are installed and no
data-plane callback may run; it does not mean that the plan contract is unknown. A missing or
changed contract blocks the transition before row preflight. Prepare consumes the already validated
spool using that cached implementation and builds only abortable shadow state; DataFusion node-local
fallback remains rejected. The current flat aggregate maps are only diagnostic substrate:
production publication first shards
groups, emission state, dirty generations, timers, and deduplication metadata by vnode so prepare
can build a replacement shard and publish/revoke it with bounded pointer swaps instead of scanning
all groups.

Frozen generations remain referenced until the exact attempt containing their base/delta chain has
both a sealed inventory and the durable terminal `CheckpointVerdict::Commit` decision. Seal alone
cannot release them because a later durable Abort may still win. An aborted or failed capture
cannot clear its changes; the next attempt includes their union or emits a full rebase. A lost
materialization/upload response may re-emit the identical immutable cut, but does not re-enter the
lifecycle or reuse an allocated attempt ID. Every allocated checkpoint ID is burned permanently;
a numeric gap may have no outcome, capture, or seal. Whenever the first later changed capture has no
admitted immediately-preceding entry it emits FULL, or EMPTY when authoritative state is empty. An
unchanged vnode may REFERENCE an older admitted nonempty BODY.
If an intervening REFERENCE is admitted, a subsequent DELTA may name it as the immediately preceding
entry; resolution then follows both edges. Initial managed v1 requires sealed inventory plus durable
Commit for admission and deliberately does not reuse sealed-Abort state. Limits on concurrent
attempts, frozen bytes, and delta-chain length apply backpressure. A
mutable-capture or encoding error faults the pipeline rather than retrying against partially
consumed dirty state.

Capture and revoke operate on vnode-prefixed ranges and frozen mutation journals. They must not
scan every live group once per vnode on the barrier or ownership-transfer path. Qualification
therefore measures checkpoint/rebalance CPU time and scheduler stalls as well as storage latency;
constant-time generation freeze is not sufficient if record encoding, range deletion, or cleanup
later steals the operator hot path.

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

Acquire and revoke for one assignment change publish as the whole-graph transition defined above.
No acquired vnode becomes `Active` merely because its own operator loop finished while another
operator or vnode in that transition can still fail.

After acquisition, the successor's next checkpoint emits a full base for that vnode, bounding
cross-owner delta dependencies. Local range deletion is asynchronous and only follows durable
revocation; stale local data can never authorize ownership.

If cut-over measurements miss the agreed pause/RTO objective, a later ADR may add Megaphone-style
fine-grained logical-time migration or standby replicas. Neither complexity is on the initial
correctness path.

## Operator state models and rollout order

### Grouped aggregate

Managed tables hold the encoded group key and versioned accumulator state. Any timestamp, timer,
or emitted-value field requires a named semantic consumer; vestigial map-era fields are not copied
into the durable schema. Accumulator and output-enqueue mutations are atomic, and dirty tracking
belongs to the state service rather than a second operator map.

The first candidate is one append-only stage with one mandatory `COUNT(*)`, one `SUM` over a direct
`Int64` input column, and one or more direct grouping columns accepted by partition ABI v1. Output
aliases are naming only. Aggregate `FILTER`, `DISTINCT`, `ORDER BY`, explicit null treatment,
`HAVING`, derived aggregate/group expressions, multiple COUNT/SUM calls, and retractions remain
closed. Any preceding projection/filter expression must have a positive replay-determinism proof;
processing time, watermark-relative `now()`, volatile/random functions, AI calls, and unclassified
UDFs keep `[LDB-4007]`. Broader `COUNT`/`SUM` shapes, `AVG`, append-only or changelog `MIN`/`MAX`,
and arbitrary UDAFs remain closed until their arithmetic, null, state-growth, determinism, and
portable encoding contracts are reviewed. A UDAF must declare a stable serializer, merge/restore
compatibility, and resource behavior before it can be cluster-capable.

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

Certification does not infer the input set from producer acknowledgements: Kafka can persist a
record and lose its acknowledgement. The workload producer first durably records a stable event
intent, then the independent controller reads back every actual broker record through the frozen
partition high-watermarks and reconciles event ID/payload/offset against those intents. The oracle
models the reconciled broker log, including any physical retry records; an unknown or conflicting
record makes the run fail or invalid according to the charter.

At-least-once recovery restores operator state and the source cursor only from one
coordinator-admitted cut with a durable terminal Commit. It must not double-apply replay within
recovered state, lose timer/output bookkeeping, or skip a result;
external results flushed after that cut may appear again after a crash. The checkpoint tail keeps
the existing ordering: enqueue operator output, flush every durable sink, then seal source
positions. State capture and real sink-flush latency share the checkpoint deadline. A stable output
identity and two-level provenance envelope must be added before the initial release. Each data
record carries only envelope version/kind, replay-stable operation ID, writer-interval ID, and
checked admission sequence. For the narrow append-only `COUNT(*)`/`SUM(Int64)` vertical, the checked
count is the batching-independent logical state version; identity binds that version and canonical
group to deployment, pipeline identity and **incarnation**, and the exact sink/operator/output
scope. It excludes payload and checkpoint attempt because replay can cross attempts and owners. The
serialized Kafka payload bytes
are the comparison authority: the independent reader computes their SHA-256, so no duplicate digest
is transmitted on every record. Equal operation IDs require bit-identical bytes; different bytes
are a conflict.

The input contract maps each logical group to one Kafka partition so group-local broker order is
stable, and the planner rejects any expression that cannot reproduce the same group/SUM prefix from
replay. Intentional rewind/recreate gets a new incarnation; ordinary crash recovery retains it. SUM
checks every source-ordered prefix and faults the whole input batch atomically rather than wrapping.
Partition ABI, assignment/process provenance, sink shard, and owned vnode set live once in the
referenced interval marker. The oracle derives the expected vnode from the canonical key and frozen
ABI and verifies that the resolved marker owns it; it does not trust a row-supplied vnode. This is
evidence for at-least-once correctness; it is not presented as exactly-once.

A cluster sink used by this release must be `DurableAtLeastOnce + MultiWriter` and accept the
operator's declared output mode. The first candidate is Kafka `envelope=append`; broker topic,
partitioning, acknowledgement, replication/min-ISR, election, DLQ, and retention settings are part
of the certified contract. The first managed aggregate emits one current result for each distinct
group changed by an atomically applied input batch. Multiple rows for one group may be
coalesced, so intermediate count versions may be absent; output never scans or republishes every
resident group merely because another group changed. Versions increase within one writer-authority
interval. After a crash, an unsealed higher version may already be external while recovery starts
from an older sealed cut, so a new fenced writer interval may append lower legal prefixes before it
reaches the final version again. The same version always has the same operation ID and bit-identical
payload. Kafka producer idempotence cannot deduplicate recovery from a new producer incarnation.
There is currently no
built-in cluster-admissible `FullChangelog` sink. Any retraction/full-changelog output remains
fail-closed until either a multiwriter changelog-log sink is certified or mutable sinks gain
key-affine assignment, old-writer fencing, deterministic operation IDs, and vnode handoff. Merely
marking a mutable sink `MultiWriter` is not sufficient.

Cycle 50 confirms that this is a design requirement, not a description of the current Kafka path.
Graph execution returns batches by stream name and publication retains internal sink selection and
output-contract checks, but `SinkCommand::WriteBatch` carries only a row batch under a deadline.
Main Kafka records contain a payload, optional user key, and optional broker partition, but no
Laminar header, operation ID, assignment/process identity, writer interval, shard, or admission
sequence. The producer is idempotent with `acks=all`, but has no `transactional.id`; no successor
marker exists. Actor FIFO orders one process locally and broker offsets order appends within one
partition, but neither is an external Laminar authority boundary. Therefore this path remains
useful ALO infrastructure and is not eligible for the stale-writer certification gate.

The initial writer interval is not a null-authority exception. For the certified vertical, startup
opens sources only far enough to resolve the exact partition inventory and numeric exclusive start
baselines while readiness, source delivery, graph execution, and `WriteBatch` admission remain
closed. It then commits a zero-input bootstrap checkpoint/capsule containing those baselines, empty
managed state/timers, pipeline identity, and the assignment certificate. The future sink state
machine must permit this one unactivated empty checkpoint flush only after proving that no output was
computed, queued, or accepted. The first writer uses `predecessor = none`, commits its marker against
that bootstrap authority, and releases data admission only after confirmation. A source unable to
expose an exact checkpointable baseline before delivery remains closed. This one-time startup and
restart cost is part of the latency/RTO profile; a separate genesis authority would require a later
ADR amendment.

A stale-owner append is defined by computation or sink admission after the writer lost process or
vnode authority, not by broker acknowledgement time. Assignment/node/process metadata alone cannot
prove that boundary. Every output carries a writer-interval ID and a sink-admission sequence that
starts at zero and strictly increases within each `(sink-writer shard, writer interval)`. Activation
requires an externally auditable fence proof. For the Kafka candidate, a bounded sink-writer shard
has a stable
transactional ID derived from deployment, pipeline incarnation, sink, and shard—not the ephemeral
writer interval—so successor initialization broker-fences the old producer. In one confirmed
transaction the successor then writes a deterministic predecessor/successor interval marker to
every affected output partition. That marker carries deployment; pipeline incarnation and identity;
operator/output and sink identity; partition ABI; sink shard and owned vnode set; assignment
certificate/digest; owner node, boot incarnation, and durable process term; predecessor/successor
interval IDs; and the exact recovery-base `{epoch, checkpoint_id}` plus recovery-capsule digest
exposed by the immutable checkpoint-evidence view. The successor admits no data before the marker
commit is known successful. All subsequent output also uses transactions from that fenced producer
and is captured read-committed; transaction batching is bounded and must meet the latency profile.

Failure before bootstrap Commit retries startup because no data was admitted. After that Commit, an
exact marker transaction may retry in the same live writer interval only when the provider proves
the attempt was definitely rejected or successfully aborted. Any unproved outcome or writer
retirement restores the same bootstrap cut and creates/fences a new interval. An ambiguous marker
commit terminates that writer; the successor fences it and references the same bootstrap cut. No
path fabricates a later source position or admits data between these steps.

The oracle reads committed records, resolves each marker's recovery-base reference through the
immutable checkpoint view, uses the first valid marker for the successor as the immutable partition
cut, and rejects an old-interval record after it or replay whose causal source position precedes the
resolved sealed cut. A predecessor transaction committed before the marker remains legal even if
its acknowledgement arrived later; an in-flight transaction aborted by fencing is invisible.
Ambiguous marker commit is fatal to that writer process and a new interval must fence it before
retry; crash/fault tests bracket initialization and marker commit. Broker configuration,
transactional-ID derivation, markers, read-committed capture, exact recovery-base resolution, and
forced old producer rejection form the retained proof—timestamps or Laminar logs alone do not. This
provider-enforced fencing is qualified for latency and failure behavior but does not make delivery
exactly-once because source cursor, managed state, and Kafka transaction are not one atomic commit.
If this topology cannot meet the profile, the Kafka scenario stays closed.

Cycle 51 makes only those verdict semantics executable in the root-excluded
`tools/independent-soak-contract` fixture v2. The synthetic model has an explicit source-partition
inventory and pre-delivery baselines, resolves bootstrap and later recovery checkpoints separately
from each marker's current writer assignment, checks monotonic assignment/interval authority,
derives vnode-to-shard ownership, and distinguishes missing evidence (`RUN_INVALID`) from complete
evidence proving malformed or stale output (`PRODUCT_FAIL`). Assignment ownership and process term
are deliberately pre-reconciled fixture evidence; production still needs the separate supported
assignment and local-process views described below. The fixture ABI checks vnode-to-shard routing,
not final Kafka partition bytes. Cycle 51 added no wire encoding, Kafka transaction, runtime
dependency, public endpoint, delivery guarantee, or certification evidence. Cycle 52 adds only the
standalone byte contract and hostile decoding described next; the production gaps remain.

### Distributed output envelope v1

Cycle 52 freezes one admission-neutral binary envelope for the future Kafka output contract. This
is the sole normative byte table; the root-excluded standalone codec and other documents must
conform to it rather than define another format. It does not mean that current Kafka records carry
these bytes.

Every integer is unsigned big-endian. The common ten-byte prefix is:

| Offset | Width | Field |
| ---: | ---: | --- |
| 0 | 4 | magic `LDBO` |
| 4 | 1 | wire version, exactly `1` |
| 5 | 1 | kind: `1` data header, `2` partition marker |
| 6 | 2 | flags |
| 8 | 2 | body byte length, excluding this prefix |

The supplied slice must be exactly `10 + body_length` bytes. Unknown magic, version, kind, or flag;
truncation; a declared-length mismatch; and trailing bytes are errors with no fallback. The data
kind permits no flags and has this fixed 56-byte body:

| Body offset | Width | Field |
| ---: | ---: | --- |
| 0 | 32 | opaque replay-stable operation ID |
| 32 | 16 | nonzero writer-interval ID |
| 48 | 8 | checked sink-admission sequence |

The complete data header is therefore exactly **66 bytes**. Both IDs are nonzero; the codec accepts
the full `u64` sequence range because checked range reservation and overflow are state-machine
semantics, not alternate wire encodings. The Kafka payload stays separate and byte-for-byte
unchanged. No payload digest, key, vnode, shard, ABI, assignment, process, checkpoint, source
position, or broker metadata is repeated in this per-record header. Cycle 53 freezes the grouped
operation identity and pure writer-authority projection below; interval creation/rotation remains
part of the later transactional-producer state machine. This section freezes only transport bytes.

Marker flag bit 0 is `HAS_PREDECESSOR`; every other bit is invalid. Its body begins with this fixed
296-byte block:

| Body offset | Width | Field |
| ---: | ---: | --- |
| 0 | 16 | nonzero current/successor interval ID |
| 16 | 16 | predecessor interval ID, or all zero |
| 32 | 16 | non-nil deployment UUID bytes |
| 48 | 16 | nonzero pipeline-incarnation ID |
| 64 | 2 | pipeline-identity canonical version, exactly `3` |
| 66 | 32 | pipeline-identity SHA-256 |
| 98 | 2 | key-to-vnode ABI version, exactly `1` |
| 100 | 2 | sink-partitioning ABI version, exactly `1` |
| 102 | 2 | vnode count, `1..=65_535` |
| 104 | 8 | nonzero current assignment version |
| 112 | 32 | current assignment-certificate SHA-256 evidence reference |
| 144 | 8 | nonzero writer node ID |
| 152 | 16 | non-nil writer boot UUID bytes |
| 168 | 8 | nonzero durable writer-process term |
| 176 | 8 | nonzero recovery epoch |
| 184 | 8 | nonzero recovery checkpoint ID |
| 192 | 32 | recovery-capsule SHA-256 |
| 224 | 8 | nonzero recovery-base assignment version |
| 232 | 32 | recovery-base assignment-certificate SHA-256 evidence reference |
| 264 | 32 | sink-topology SHA-256 |

Every SHA-256 field is raw 32-byte output and cannot be all zero. The fixed block is followed, in
order, by four `u8 byte_length | UTF-8 bytes` fields: sink ID (maximum 128 bytes), operator ID
(128), output ID (128), and sink-writer shard ID (64). Each is nonempty and contains no NUL byte;
identity is exact UTF-8 bytes and this format performs no Unicode normalization. The final section
is an owned-vnode bitmap of exactly `ceil(vnode_count / 8)` bytes. Vnode `n` uses
`bitmap[n / 8] & (1 << (n % 8))`; at least one bit is set and unused high bits in the final byte are
zero. Recovery epoch and checkpoint ID are the same nonzero canonical attempt number. A first marker
clears `HAS_PREDECESSOR` and has an all-zero predecessor ID. A successor sets the flag and has a
nonzero predecessor ID different from its current interval.

The maximum marker body is **8,940 bytes**: 296 fixed bytes, four length bytes, 448 identifier bytes,
and the 8,192-byte bitmap for 65,535 vnodes. The complete marker is at most **8,950 bytes**. A
standalone batch characterization is capped before decoding at 65,536 data headers and 4,325,376
header bytes; payload bytes are outside that figure. Readers return borrowed identifier/bitmap
views, check caps and arithmetic before allocation or indexing, and consume the complete slice.
The cap applies to one codec-facing transport batch; a fixture case models a whole capture and may
contain multiple such batches. This is structural characterization, not a latency benchmark or an
allocator-instrumented production measurement.
There is no envelope checksum because Kafka's record-batch CRC protects the transported record;
the independent reader still compares the untouched payload bytes separately.

The intended Kafka placement reserves the case-sensitive header key `__ldb`. A future conforming
data record has exactly one such header whose value is the 66-byte envelope. A marker has exactly
one such header, a null Kafka key, and an empty but non-null Kafka value, and is written explicitly
to each affected partition; its common envelope bytes are identical across those partitions.
Broker topic, partition, offset, and timestamp remain capture metadata. Missing or duplicate
reserved headers fail closed; unrelated application headers remain allowed. This freezes intended
placement only—no current connector encodes, publishes, reads, or fences this contract.

### Grouped COUNT/SUM operation identity v1

The first managed-output vertical uses one sink-scoped logical operation identity only for keyed,
append-envelope `COUNT(*)` plus nullable `SUM(Int64)`. Windows, joins, changelog rows, and the single
global aggregate must define different semantic domains rather than reuse a count-version tuple.
For this vertical:

```text
operation_id_v1 = SHA256(operation_id_v1_preimage)
```

The preimage is the following exact concatenation. All integers are unsigned big-endian:

| Order | Width | Field |
| ---: | ---: | --- |
| 1 | 44 | ASCII `laminardb/grouped-count-sum/operation-id/v1\0` |
| 2 | 16 | non-nil deployment UUID bytes |
| 3 | 16 | nonzero pipeline-incarnation ID |
| 4 | 2 | pipeline-identity canonical version, exactly `3` |
| 5 | 32 | raw pipeline-identity SHA-256 |
| 6 | 1 | sink-ID byte length |
| 7 | variable | exact sink-ID UTF-8 bytes |
| 8 | 1 | operator-ID byte length |
| 9 | variable | exact operator-ID UTF-8 bytes |
| 10 | 1 | output-ID byte length |
| 11 | variable | exact output-ID UTF-8 bytes |
| 12 | 4 | canonical group-key byte length |
| 13 | variable | exact partition-key ABI-v1 Arrow-row bytes |
| 14 | 8 | checked `COUNT(*)` state version |

The three text identifiers use the envelope-v1 128-byte caps, are nonempty, contain no NUL, and
receive no Unicode normalization. The key length must fit `u32`; the key bytes are the already-owned
`PartitionKeyCodecV1` representation, not logical display text or a reconstruction from the output
batch. Zero key bytes are valid for a nonempty ABI-v1 `Null` grouping schema; this keyed domain is
never used for the ungrouped aggregate. Count is in `1..=i64::MAX` and is the batching-independent
state version. The raw 32-byte SHA-256 result is the data envelope's operation ID.

Deployment reset, intentional rewind/recreate, pipeline compatibility, and sink/operator/output
scope therefore change the ID. Payload bytes, `SUM`, checkpoint/recovery attempt, assignment,
vnode, shard, writer node/boot/process term, writer interval, admission sequence, and broker
position do not. Excluding those authority facts keeps a legal crash/rebalance replay stable;
excluding payload and `SUM` makes two serialized results for the same group/count an observable
conflict rather than two identities. A fixed SHA-256 context through output ID is prepared once,
then cloned once per distinct group actually emitted after successful atomic state and payload
construction. The row path appends only the borrowed key length/key and count; it performs no key
re-encoding, payload hash, coordination, or preimage allocation.

Cycle 53 models pipeline incarnation and stable operator/output identity as validated opaque inputs.
Production manifests/capsules do not yet provide a crash-stable, recreate-rotating pipeline-
incarnation lifecycle, and the current sink command has already lost the canonical group row and
checked count. The standalone derivation is therefore not runtime or certification evidence.

The companion pure authority projection keeps three sources distinct. Current assignment uses the
full `CheckpointAssignmentFence::digest()` certificate reference and complete vnode-to-`(node,
boot)` view; current durable process term comes separately from that writer's live process lease;
and recovery attempt/capsule/base assignment come from one immutable cluster Commit. It never uses
the older checkpoint leader term as the current writer term. Every planned shard vnode must be
owned by the current process. Current assignment may be newer than the recovery base, but not
older; equal versions require the same certificate digest. The current interval and optional
predecessor remain opaque nonzero 16-byte IDs, and the current ID is copied unchanged to marker and
data headers. The independent projection reconstructs the canonical owner-map digest and full
certificate digest from assignment version, ABI, ordered owners, and the sorted node/boot
participant roster, enforces the production 129-participant cap, and requires the supplied reference
to match. An inner owner-map digest, repeated-node boot disagreement, or stale digest after boot
rotation therefore fails. Interval allocation, rotation after ambiguous marker outcomes,
predecessor lifecycle, and checked sequence reservation belong to the next fake-producer state
machine.

### Validation-only transactional writer model v1

Cycle 54 adds a synchronous protocol model only in the root-workspace-excluded independent tool.
It consumes one already-prepared authority, an ordered nonempty affected-partition set, and explicit
nonzero transaction limits. It does not allocate interval IDs, serialize Kafka `transactional.id`,
contact a broker, wire a connector, or establish latency, allocation, fencing, delivery, or
production evidence. The five modeled states are `Uninitialized`, `MarkerPending`, `DataOpen`,
`TransactionInFlight { kind, phase, return_state }`, and terminal `TerminalPoison { point }`.

| From | Simulated result | To and externally visible model effect |
| --- | --- | --- |
| `Uninitialized`, initialize | confirmed / definitely rejected / ambiguous | `MarkerPending` / unchanged / terminal poison |
| stable state, begin | confirmed / definitely rejected / ambiguous | `TransactionInFlight(Begun)` / unchanged / terminal poison |
| `TransactionInFlight(Begun)`, send | confirmed / confirmed abort / ambiguous | `TransactionInFlight(Staged)` / prior stable state / terminal poison |
| `TransactionInFlight(Staged)`, commit | confirmed / confirmed abort / ambiguous | publish all staged records and return/open `DataOpen` / publish none and return to the prior stable state / terminal poison |

Initialization never opens data. The interval marker is encoded once and one identical value is
staged for every partition in the caller-supplied canonical affected set in one unsplittable
transaction. The fake cannot prove that this set is the complete broker-topology inventory. Only
its confirmed commit opens data; it is never co-batched with the first data transaction. Each data
transaction is independently all-or-none. A pure planner reports maximal in-order input ranges, but
execution takes exactly one planned range per call so a confirmed prefix cannot be hidden behind
the outcome of a later range.

Limits are inputs to this validation model, not new production constants. Record count is
additionally capped by the frozen 65,536-header codec limit. Modeled data bytes are exactly the
checked sum of `66 + payload_length` for each record; modeled marker bytes are
`encoded_marker_length * partition_count`. These figures exclude Kafka keys, record/request framing,
compression, broker limits, and allocator behavior. Zero records, noncanonical/unknown partitions,
an oversized singleton, arithmetic overflow, and count or byte excess fail before begin, sequence
reservation, or transcript mutation. Payloads remain borrowed while the model prepares inline
66-byte headers; its owned confirmed transcript is test bookkeeping and is not hot-path evidence.

One shard-local counter covers all partitions in an interval. A bounded data range provisionally
reserves contiguous sequences once, then derives row `i` as `start + i` without a lock or per-row
reservation. `Some(n)` means `n` is next and `None` means exhausted; the inclusive last value is
checked, so a one-record range at `u64::MAX` is legal and then exhausts the interval. A confirmed
commit advances the counter. A confirmed abort publishes nothing and retries the same borrowed
range and provisional sequence inside the same model call; an ambiguous result poisons the writer.
Scripts that would return control with an unresolved provisional range are invalid. This keeps the
first visible sequence at zero while permitting later capture gaps, as required by the v2 oracle.

The stable producer scope is the structured tuple `(deployment UUID, pipeline incarnation, sink ID,
shard ID)`. Its Kafka string encoding is frozen below. A first interval requires no predecessor. A
successor test may consume a token from a confirmed predecessor, must retain that scope, use a
different caller-supplied interval, name the exact predecessor, and restart sequence zero. This
proves only immediate linkage in the model, not durable global interval uniqueness: a new fake chain
can reuse an ID, and `A -> B -> A` is not rejected across separately constructed writers. Production
must durably reject reused confirmed, aborted, ambiguous, and retired interval IDs.

Every ambiguous initialize, begin, send, or commit result is terminal and all later writer calls
fail inertly. The confirmed-only model transcript does not advance after an ambiguous marker or data
commit; that absence is deliberately no visibility verdict. A marker may be present on every
supplied partition or absent, but data remains closed either way. The correct successor predecessor
is therefore the ambiguous interval only when read-committed reconciliation finds that marker;
otherwise it is the last visible predecessor (or none). Cycle 55 proves only the deterministic
real-broker subset: same-ID producer fencing, old open-transaction abortion, committed/aborted
consumer isolation, complete synthetic-topic marker fanout, and reserved-header placement. It does
not actuate or resolve an ambiguous `EndTxn`, qualify broker limits, or prove a production topology.
Those gaps remain fail-closed and **NO-GO**; even complete broker qualification would remain
at-least-once because source/state and Kafka do not share one atomic commit.

### Kafka transactional identity v1 and real-broker evidence boundary

Every bounded Kafka sink-writer shard derives one stable broker fencing identity as follows. `||`
means byte concatenation; lengths are unsigned one-byte values; and `lowercase_hex` emits two ASCII
characters per digest byte:

```text
transactional_id_v1 =
    "ldb.tx.v1." || lowercase_hex(SHA256(transactional_id_v1_preimage))

transactional_id_v1_preimage =
    ASCII("laminardb/kafka/transactional-id/v1\0")
    || deployment_uuid[16]
    || pipeline_incarnation[16]
    || u8(sink_id_utf8_length) || sink_id_utf8
    || u8(shard_id_utf8_length) || shard_id_utf8
```

The domain separator is exactly 36 bytes. Deployment and pipeline-incarnation values are the same
canonical nonzero 16-byte identities carried by marker v1. Sink and shard IDs use the marker-v1
rules: exact UTF-8 bytes, no normalization, nonempty, no NUL, and respective maxima of 128 and 64
bytes. The result is exactly 74 ASCII bytes: the ten-byte prefix and 64 lowercase hexadecimal
characters. For the sample scope `([0x22; 16], [0x33; 16], "sink", "shard")`, the frozen value is
`ldb.tx.v1.c49ace6d02eb21ec7a2dc4424d8c3b9680fc3cd828cd754fec079b800a37411a`.

Assignment, node, boot, process term, recovery attempt, checkpoint, writer interval, operator,
output, and topic partition are intentionally excluded. A replacement for the same stable shard
must reproduce the same value so broker initialization fences any predecessor generation. The
runtime must therefore make `shard_id` unique among concurrent writers in `(deployment, pipeline
incarnation, sink)`; otherwise intended peers would deliberately fence one another. If one shard
later needs concurrent producer authorities for distinct outputs, this scope must be revised before
runtime wiring rather than silently widening the tuple.

Cycle 55's root-workspace-excluded probe is protocol evidence, not connector code. On one disposable
three-partition, replication-factor-one Redpanda topic it may establish that a successor initialized
with this same identity fences an older producer, that a flushed open predecessor transaction is
hidden from `read_committed`, and that committed markers/data are visible across the complete
synthetic topic inventory with their exact wire headers. It cannot establish generic affected-topic
inventory, replicated durability/failover, timeout or size limits, TLS/authentication, hot-path
latency, source/state/sink atomicity, or an ambiguous commit outcome. A true ambiguity test must
externally prove that a specific `EndTxn` request reached the broker while its matching response was
lost, retire that producer, and reconcile the complete read-committed marker set through a fenced
successor. A broker kill, tiny client timeout, or generic proxy disconnect is not that proof.

### Matched EndTxn ambiguity actuator v1 (validation only)

The controlled ambiguity test is restricted to the exact Cycle 55 client and broker subject:
`rdkafka 0.39.0` / librdkafka `2.12.1`, Redpanda `v26.1.13`, one broker, loopback-only
PLAINTEXT, and no SASL. Librdkafka offers only `EndTxn` versions 0 and 1 and the tested broker offers
versions 0 through 3, so negotiation must produce non-flexible version 1. The actuator observes and
asserts that version; it never forces an API downgrade and fails closed on version drift.

Kafka frames are read as a signed big-endian `i32` length followed by exactly that many bytes under
a fixed validation-only cap. The selected request is API key 26 with request-header v1 and the exact
client ID, transactional ID, producer ID, producer epoch, and `committed=true`. Its v1 response is
response-header v0 plus `throttle_time_ms: i32` and `error_code: i16`. Correlation IDs are scoped by
the accepted connection generation. The proxy retains the complete bounded target request/response
bytes and SHA-256 values; it neither scans arbitrary TCP chunks nor remaps correlations. Complete
non-target frames during the active lifecycle are forwarded byte-for-byte over a dedicated
upstream connection.

Only these two deliberate classifications are valid:

- `FORWARDED_SUCCESS_RESPONSE_LOST`: the complete target request was written upstream; a complete
  response with the same connection-scoped correlation ID and error zero was read; and zero bytes
  of that response were written downstream.
- `PRE_FORWARD_REJECTION`: the complete target request was read, but zero target bytes were written
  upstream and no target response exists.

A completed upstream write without the exact response is still `FORWARDED_OUTCOME_UNKNOWN`.
Nonzero broker errors are `BROKER_REJECTED`. Unexpected partial frames outside signalled connection
teardown, a second target attempt, an unknown or mismatched correlation, target-version drift, or
any targeted downstream byte invalidate the run. Traffic from the exact target client after
evidence finalization is also fatal rather than forwarded.
The broker must advertise the proxy endpoint; using a proxy only as `bootstrap.servers` is invalid
because coordinator discovery could bypass it.

The matched actuation and the expected retriable local commit timeout must both complete before the
target producer is destroyed while the actuator remains armed, and every
connection observed with its exact client ID must close before a same-transactional-ID successor is
initialized. This order prevents a background retry from deciding the experiment. A successful
Redpanda `EndTxn` response proves the coordinator accepted the commit decision, not instantaneous
data-partition marker visibility, so the semantic verdict comes only from bounded eventual
reconciliation.

Reconciliation freezes each test partition's high watermark after successor fencing and before new
writes, directly assigns separate `read_committed` and `read_uncommitted` consumers from the
beginning, and requires every final consumer position to equal that frozen cut. The candidate
marker is selected only when it appears exactly once on every affected partition; absence on every
partition selects the last confirmed predecessor. Partial, duplicate, conflicting, or incomplete
capture admits no data. Four isolated topics cover marker and data transactions crossed with the
two classifications. The data cases always commit a successor marker and replay the same logical
record under its successor interval, so the exact committed/uncommitted transcript—not a client
timeout—decides which predecessor attempt was visible.

This actuator is deliberately not a reusable Kafka proxy. Multi-broker routing, TLS/SASL, arbitrary
protocol versions, or address rewriting require a framework re-evaluation rather than expansion of
this parser. Passing it still supplies no runtime connector, durable interval allocator, production
topology/limits, replicated durability, source/state/sink atomicity, qualifying latency result, backend
qualification, or independent soak evidence; production remains **NO-GO**.

The controller also needs supported, bounded evidence projections rather than private object-store
paths or text-log parsing. Cycle 57 adds the first such projection at authenticated
`GET /api/v1/cluster/local-evidence`. A successful, cache-disabled response is capped at 4 KiB and
contains only this exact process's live node/boot/process-term sample and its canonical boot-bound
durable `CheckpointAssignmentAdoption`. The controller performs one bounded checked-KV operation
for only that current stable-node slot, validates canonical encoding, requires it to match the
locally audited assignment fence, and rechecks process identity, lease, and the same fence after the
read. The route remains behind normal
startup/recovery serving gates and returns no evidence without a configured bearer token. It does
not scan `/cluster/vnodes`, expose the vnode owner vector or private storage paths, or relabel shared
durable publication as current local adoption.

Every checked-storage timeout/failure is unavailable (`503`), including the current object-store KV
adapter's inability to distinguish a malformed outer control envelope from I/O failure. `500` is
reserved for a logical value returned successfully to this method that then fails payload bounds,
canonicality, or same-version fence validation. The engineering consumer retries `503` only within
its absolute convergence deadline, so this diagnostic ambiguity cannot produce a pass; it remains an
operability limitation rather than independent invalid-state classification evidence.

Bearer authentication does not add transport encryption or request throttling. Production-like use
must therefore keep this route on loopback/a trusted network or behind a TLS ingress, and consumers
must poll at bounded control-plane cadence: every probe performs one logical checked-KV operation
plus local authority checks. The object-store KV implementation may need several physical metadata
and object requests to validate that operation's lease-bound control envelope. It is outside row
processing and checkpoint mutation, but it is not a high-frequency monitoring endpoint.

No retained authority currently records an exact local recovery phase or proves that this process
consumed a committed `Release`; the retained successful-`Start` acknowledgement is historical and
can remain unchanged across a later recovery, while `RecoveryMonitor::applied_gen` is a suppression/
settlement counter that can advance for missed or rejected rounds. Those facts must not be
reconstructed from coarse gates. Cycle 57 therefore narrows the earlier planned schema instead of
inventing or exposing weak lifecycle state.

Its existing three-node engineering harness sandwiches per-process evidence between stable durable
assignment reads and checks removal plus same-node boot/term rotation across hard kill/rejoin. This
closes only stable-serving local-adoption observability; it adds no row, state-mutation, rebalance,
or checkpoint hot-path work and is not independent soak evidence.

Two Windows/WSL2 Docker engineering runs satisfied those new convergence assertions, but neither
complete test passed: a zero-tail preflight missed the existing minimum sample count, then a
90-second run recorded only 98.81% of node1 checkpoint stalls at or below 1024 ms against the 99.00%
gate. Aggregate histograms cannot identify or correlate the two violating attempts, so no causality
is assigned to this endpoint. The red result and exact-attempt observability gap remain NO-GO
evidence.

The [Cycle 58 checkpoint-attempt evidence audit](../reports/checkpoint-attempt-evidence-audit-2026-07-26.md)
separates durable recovery authority from local performance evidence. Existing create-once outcomes
and content-addressed recovery capsules retain exact authority for some attempts subject to both
retention floors; current APIs cannot classify an arbitrary requested attempt without inference or
racing reads. They contain no stage timing. Aggregate Prometheus histograms cannot supply exact
attempts, maxima, process generations, or loss detection. Timing must not be added to recovery-
critical outcome/capsule wire formats or unbounded per-attempt metric labels.

The first required slice was therefore a preallocated process-local ledger with one bounded record
per pipeline-stall observation, exact attempt/assignment/process authority and stage nanoseconds,
plus sequence, overwrite, and recording-loss evidence. It covers pipeline stall, local barrier,
and aligned resume only; exact full-checkpoint and restorable-gate evidence remain open. Its
consumer performs no shared-store read.
Cycle 59 implements that bounded first slice. The checkpoint-control path writes one preallocated,
nonblocking record under the same guard scope as its three histogram observations; the protected
local endpoint is bounded, cache-disabled, process-bound, and paginated. The existing engineering
harness streams every record collected through each coherent observed cut to per-generation JSONL,
retains fixed diagnostic memory, rejects unread loss, and reconciles exact integer counts and
diagnostic buckets against the corresponding observed Prometheus cut.
Its assignment authority uses the full certificate digest after independently proving the local
owner-map adoption against the same converged fence. It observes only process generations and
assignment versions sampled at converged harness cuts; it is not a complete historical audit.

The corrected one-kill Windows/WSL2 engineering run at `7782a032` passed with 392 exact records
across four process generations, zero deadline exhaustion, a passing existing pipeline-stall gate,
and 100% of each three-family diagnostic `le=1.024` bucket. Its oracle accounted for all 79,996
expected IDs while tolerating and counting 2,758 duplicates; it did not prove duplicate byte
identity or sealed-cut replay legality and supplies no exactly-once evidence. The later
substitution-defense change in `1a6dff80` has focused deterministic coverage but was not part of
that empirical subject. The instrumentation A/B, exact full-checkpoint/restorable-gate
families, and read-only same-snapshot outcome/capsule audit remain open. `[LDB-4007]`,
`[LDB-0013]`, and the production **NO-GO** verdict are unchanged.

Selected attempts may then be joined at low cadence only through a new read-only, same-snapshot
core audit of the exact outcome, both retention floors, and validated live capsule reference;
compacted continuity must remain explicit and no later outcome may be relabelled as the requested
attempt. That audit also requires non-creating deployment-identity validation; current helpers may
create it. Capsule connector maps are provider-specific values, not normalized exclusive source
cuts, so that versioned per-connector projection remains open. The delivery sequence has completed
executable oracle semantics, byte-golden envelopes/markers, pure
identity/authority tests, fake transactional producer modeling, deterministic real Kafka/Redpanda
fencing/isolation, controlled ambiguous-outcome reconciliation, and the local-adoption engineering
consumer. Durable runtime interval authority and transactional source/state/sink integration remain
open. Only the later independently operated release-binary soak can certify the completed vertical.

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

The checked-in [`linux-nvme-v4` input](../../tools/state-backend-qual/profiles/linux-nvme-v4.freeze-candidate.json)
is accepted only for validation and immutable Fjall/RocksDB regression. Its exact roster and
RocksDB-specific controls cannot be relabelled as TidesDB. The selected TidesDB package line requires
a successor profile identity and candidate mapping only after a future package passes T0/T1; exact
v0.11.1 did not. The
standalone validator must continue to accept only explicitly ineligible forms, reject
measured/result fields, and have no runtime or backend dependency. Exact run authorization, evidence
for the package-admitted exact subject, the product connector/object-store profile, and the independent release
soak all remain outstanding. The v1-v3 profiles remain immutable validation/model regression
fixtures, and v4 supplies no candidate performance or selection evidence.

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

### Use heap state as the unrestricted cluster default — rejected

SQL key cardinality, timers, join rows, skew, frozen generations and restore scratch are not
intrinsically bounded. A memory implementation may be admitted only as an explicitly named profile
with pre-mutation reservations, hard limits, controlled exhaustion, portable checkpoints and a
qualified cold-restore/source-replay RTO. It never accepts a query under the general local-spill
profile and cannot silently evict semantic state.

### Object-store-primary LSM now — rejected

RisingWave Hummock demonstrates versioned and pinned snapshots, cache tiers, and explicit
compaction/vacuum ownership. Flink ForSt demonstrates remote SSTs, local file caching, asynchronous
State V2 access, and lightweight incremental checkpoints, but remains experimental and its current
implementation is being replaced. Materialize Persist demonstrates durable object-store
collections with transactional consensus metadata feeding cluster-local hydrated arrangements.
Together they show that remote state is a storage subsystem—not a backend setting—and needs
version authority, caches, compaction/GC ownership, async execution, and failure isolation.
LaminarDB does not currently have that subsystem. Reconsider only if measured local-disk operations
or recovery objectives justify its cost.

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

### Restore the former Fjall cold tier or select from old benchmark/API evidence — rejected

The removed tier cached checkpoint slices and used point operations; it did not own always-current
state, and its dirty-state coupling was unsafe. Restoring it would preserve the missing lifecycle.
Conversely, API checklists and old single-insert or vendor benchmarks are insufficient to qualify
Fjall, RocksDB, or TidesDB for production. The owner selected TidesDB and its official Rust binding
as the implementation line; that choice does not waive package source closure or constitute
production admission. Its absolute campaign is required because the backend directly affects the
tail, resource governor, restore, cleanup, and operational surface; a permanent two-backend product
is not planned.

## Consequences and risks

Positive consequences:

- one bounded state lifecycle replaces operator-specific maps and snapshot rules;
- checkpoints retain their existing durable authority and portable artifacts;
- each operator can be admitted independently by a positive planner/runtime proof;
- local NVMe preserves the low-latency path while shared storage tolerates node/disk loss; and
- window timers and join cleanup become explicit, testable state rather than side structures.

Costs and risks:

- any embedded disk store adds corruption, disk, tuning and cold-page/sync-tail risk; an LSM also
  adds compaction risk. Cycle 41 closes restricted TidesDB ownership containment but proves that the
  current package misses non-containable native fixes and cannot guarantee the general cgroup or
  mandatory health contract. Verified commit may contain ambiguous batch success only if it passes
  source, fault, and latency gates; portable fresh restore and immutable cuts remain untested. The path also adds a C build and
  native allocator accounting. Frozen Fjall/RocksDB references retain their documented governance/
  native costs, and redb-like B-trees add sole-writer and reclamation/resize risk;
- a separately certified memory profile adds an admission/soak matrix and controlled-exhaustion
  path; it is justified only if measured latency or deployment value repays that support cost;
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
| [Fjall 3.1.8 API](https://docs.rs/fjall/3.1.8/fjall/) and [RocksDB operations](https://github.com/facebook/rocksdb/wiki/Basic-Operations) | Fjall offers a Rust-native API without a C++ storage engine plus batches/snapshots/ranges, but lacks native multi-get/range tombstones and sufficient governance telemetry; RocksDB offers broader batch/operations controls at native-build/accounting cost | Retain the common workload/fault lessons and portable Laminar artifacts; qualify the separately selected target against absolute gates |
| [Flink 2.3 keyed state](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/concepts/stateful-stream-processing/) and [state backends](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/ops/state/state_backends/) | Key groups are the atomic redistribution unit; heap is low-latency but memory-bound; EmbeddedRocksDB supports large local state and incremental checkpoints | Keep fixed vnodes; start with a local disk-backed backend and portable checkpoints |
| [Arroyo state concepts](https://doc.arroyo.dev/concepts/) | The Rust engine keeps open-source working state in worker memory, explicitly limits it to worker RAM, and writes consistent remote Parquet checkpoints | Treat bounded memory plus remote recovery as a valid separately gated placement, not evidence that arbitrary state fits memory |
| [Flink ForSt](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/ops/state/disaggregated_state/) | Remote SSTs, local cache and async State V2 access enable lightweight checkpoints, but ForSt remains experimental; synchronous state is local unless explicitly overridden, and the current implementation is slated for replacement | Do not put object storage on LaminarDB's initial hot path |
| [Flink checkpoint backpressure guidance](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/ops/state/checkpointing_under_backpressure/) | Unaligned capture is useful when measured alignment delay is the problem and captures in-flight data | Keep aligned barriers; instrument before adding unaligned state |
| [RisingWave architecture](https://docs.risingwave.com/get-started/architecture), [v3.0 vnode mapping](https://github.com/risingwavelabs/risingwave/blob/v3.0.0/docs/dev/src/design/consistent-hash.md), and [v3.0 checkpoints](https://github.com/risingwavelabs/risingwave/blob/v3.0.0/docs/dev/src/design/checkpoint.md) | Key-to-vnode mapping/storage encoding stay invariant while Meta changes vnode ownership during scaling; barriers combine with Hummock cache, version, and compaction services | Reuse fixed vnode identity/barriers; do not imitate Hummock without its control/storage services |
| [Materialize views](https://materialize.com/docs/concepts/views/), [clusters](https://materialize.com/docs/concepts/clusters/), and [arrangements](https://materialize.com/docs/get-started/arrangements/) | Materialized results are durable/cross-cluster, while indexes and shared arrangements are cluster-local; views also hydrate maintenance state proportional to inputs/outputs | Keep materialized output separate and budget hydration; consider shared arrangements only after the common store works |
| [Spark 4.2 state](https://spark.apache.org/docs/4.2.0/streaming/apis-on-dataframes-and-datasets.html) and [streaming modes](https://spark.apache.org/docs/4.2.0/streaming/index.html) | Persisted hash state depends on stable shuffle partitions; RocksDB changelog checkpointing is optional, locality is preferred rather than fenced, and bounded memory is soft. Stateful checkpointed execution remains batch/cut-oriented; Continuous Processing is at-least-once | Borrow the local store/journal pattern and compatibility discipline, not Spark's ownership or latency model |
| [Kafka Streams runtime](https://kafka.apache.org/43/streams/developer-guide/running-app/), [rebalance protocol](https://kafka.apache.org/43/streams/developer-guide/streams-rebalance-protocol/), and [configuration](https://kafka.apache.org/43/streams/developer-guide/config-streams/) | Partition-derived tasks restore local stores from changelogs. Standbys exist; classic assignment's High Availability Assignor has warmups and rack-aware placement, while opt-in broker-authoritative `group.protocol=streams` in 4.3 does not yet provide that assignor. ALO is default and Kafka EOS is opt-in/Kafka-scoped | Learn from local restore/standby, but keep LaminarDB's connector-independent vnode and external-delivery authority |
| [Asynchronous Barrier Snapshotting](https://arxiv.org/abs/1506.08603) and [Flink state management](https://www.vldb.org/pvldb/vol10/p1718-carbone.pdf) | A short consistent cut can be separated from asynchronous state materialization | Freeze state at the existing aligned cut and upload outside the pause |
| [Dataflow Model](https://research.google/pubs/the-dataflow-model-a-practical-approach-to-balancing-correctness-latency-and-cost-in-massive-scale-unbounded-out-of-order-data-processing/) and [MillWheel](https://research.google/pubs/millwheel-fault-tolerant-stream-processing-at-internet-scale/) | Event time, watermarks, triggers, persistent per-key state, and timers are correctness concepts | Make timers/frontiers first-class managed state |
| [Differential Dataflow](https://www.microsoft.com/en-us/research/publication/differential-dataflow/) | Incremental collections carry changes/differences | Preserve explicit weights/multiplicity for changelog joins rather than overwriting rows |
| [Megaphone](https://www.vldb.org/pvldb/vol12/p1002-hoffmann.pdf) | Fine-grained migration can reduce latency spikes | Defer until checkpoint-cut migration is measured |
| [Disaggregated State Management in Flink](https://www.vldb.org/pvldb/vol18/p4846-mei.pdf) (2025) | Remote-primary state required asynchronous access, local caching, streamed updates, and lightweight checkpoint/recovery machinery | Keep synchronous object storage off the initial hot path; treat any future remote state as a subsystem |
| [CheckMate](https://arxiv.org/abs/2403.13629) | Coordinated checkpoints performed best under uniform load, while uncoordinated checkpoints could benefit skewed workloads and cyclic dataflows | Treat checkpoint mode as an empirical decision |
| [PGVal](https://www.vldb.org/pvldb/vol18/p585-tahir.pdf) | Observed end-to-end reliability varied with input rate, partition count, topology, parallelism, and fault type | Require a multidimensional output-oracle fault matrix including network faults |
| [Timely and Accurate Prefetching](https://arxiv.org/abs/2603.19890) (ICDE 2026) | Known future state keys can be prefetched to reduce cold-state latency | Defer; reconsider only if qualification shows cold reads dominate p99/p99.9 |

[Flink 2.3.0](https://flink.apache.org/2026/06/25/apache-flink-2.3.0-release-announcement/)
(2026-06-25), [Spark 4.2.0](https://spark.apache.org/releases/spark-release-4-2-0.html)
(2026-07-14), [Kafka 4.3.1](https://kafka.apache.org/blog/2026/06/25/apache-kafka-4.3.1-release-announcement/)
(2026-06-25), [RisingWave 3.0](https://github.com/risingwavelabs/risingwave/releases/tag/v3.0.0),
and Materialize's current 2026 documentation were checked for this decision. Vendor documentation
establishes what those systems do; the LaminarDB choices above are explicit design inferences, not
claims that another system proves this implementation correct.

## Revisit conditions

Reopen this ADR if backend qualification cannot meet the committed latency/resource profile, local
restore cannot meet the production RTO at the target state size, the vnode/partition ABI is found
incompatible with required SQL key semantics, or a shared-state design becomes justified by
measured operations rather than analogy. Any replacement must preserve positive admission proofs,
portable sealed recovery, byte governance, and fenced ownership.
