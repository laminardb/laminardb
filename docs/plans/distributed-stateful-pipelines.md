# Distributed Stateful Streaming Pipelines — Strawman Design

**Status:** STRAWMAN — every decision below is defensible but not final. Push back.
**Date:** 2026-04-17
**Author:** sujitn
**Scope:** Multi-instance stateful LaminarDB pipelines with shared state on
object storage, exactly-once end-to-end where the source and sink support it.
Source-agnostic: Kafka is one case, not the only one.

---

## Why this doc exists

We want LaminarDB to run across multiple instances so a single logical
pipeline can scale horizontally. "Horizontally" means:

- A Kafka topic with 32 partitions consumed by 4 instances (8 partitions each).
- A single Postgres CDC replication slot consumed by 1 instance, fanning out
  to 4 instances that own disjoint subsets of the state.
- A directory of 10K Parquet files consumed by 4 instances (2.5K files each).

Each has different partitioning properties at the source. The design has to
accommodate all three, not just the first one.

## Connector landscape (what we have to support)

Partitioning properties of the existing connectors — this is the table the
design is built against:

| Connector | Role | Native partitioning | Replayable? | 2PC primitive |
|---|---|---|---|---|
| Kafka | source | partitions | yes (offset) | — |
| Kafka | sink | — | — | transactions |
| Postgres CDC | source | **singleton** (logical replication slot) | yes (LSN) | — |
| MySQL CDC | source | **singleton** (binlog) | yes (file, pos) | — |
| WebSocket | source | singleton | **no** | — |
| Files | source | one split per file/directory | yes (file, byte offset) | — |
| Files | sink | one part file per writer | — | rename-on-commit |
| Postgres | sink | per-connection | — | connection-scoped tx |
| Delta Lake | sink | — | — | atomic metadata commit |
| Iceberg | sink | — | — | atomic metadata commit |
| MongoDB | sink | — | — | best-effort / idempotent |
| OTEL | source / sink | singleton stream | partial | — |
| Stdout | sink | — | — | at-least-once |

Three axes that actually matter for the distributed design:

1. **Source parallelism** — can the source natively split across instances
   (Kafka, Files) or does it require a single reader that then shuffles
   (CDC, WebSocket)?
2. **Source replayability** — can we rewind to a checkpointed position on
   recovery? Everything except WebSocket can.
3. **Sink 2PC mechanism** — what commit primitive does the sink offer?
   Different per sink; the coordinator is the same.

The distributed runtime has to abstract over all three. Kafka is the easy
case; CDC and WebSocket force the design to be properly generic.

## Assumptions (strawman numbers — correct them)

| Parameter | Assumed value |
|---|---|
| Events/sec/instance | 500K peak |
| Keyspace cardinality | 10⁵–10⁸ |
| Latency SLA (p99 end-to-end) | 1–5 s |
| Deployment | Same AZ, 2–8 instances |
| Exactly-once | Required where the source+sink support it |
| Max failover unavailability | 30 s |
| Object store | S3 (prod), MinIO / LocalFS (test) |

## The ten decisions

### 1. State locality: shared object store (RisingWave-style)

Compute instances are effectively stateless; aggregate and window state live
on shared `object_store`. Local RAM is a working set.

Unchanged from Kafka draft. Applies to every source/sink combination — the
source-agnostic part is cleanest here.

### 2. Key-to-vnode mapping: one hash, always shuffle

Row key → `vnode_id` via **one fixed hash function, applied universally**.
Strawman: **xxh3 (64-bit) → vnode_count**.

We considered murmur2 for Kafka co-location (Kafka's default producer uses
murmur2, so `partition = vnode` would be free). Rejected because:

1. Many deployments don't control upstream producers — co-location only
   fires when producers are under our management, which isn't the common
   case.
2. We need shuffle machinery for CDC and WebSocket regardless (they have no
   natural partitioning we can align with). Having two code paths ("some
   co-located, some shuffled") is more complex than one.
3. Flink's standard `keyBy` operator always shuffles; that's the industry
   precedent.

So: **always shuffle after source, in the common case**. Co-location becomes
a future per-pipeline optimization (a config flag that says "producer is
under our control, use murmur2, skip shuffle") — out of scope for v1.

**Consequence for the code I wrote:** `rendezvous_hash` in `state/vnode.rs`
stays deletable — HRW's selling point (minimal reshuffle when the assignment
*space* changes) doesn't apply to our use case. Replace with a single
`key_hash(bytes) -> u64` helper using `xxhash_rust::xxh3`, which is already
in the workspace.

**Consequence for deps:** `xxhash-rust` stays in `laminar-core` as a
non-optional dep (reversing my earlier plan to delete it).

### 3. Source-to-instance assignment: per-connector dispatch

We considered a generic `SourceConnector::enumerate_splits()` trait
(Flink's Source model). Rejected: four of six existing connectors are
singletons, one is Kafka (assignment delegated to the broker), one is
Files (trivial enumeration). A trait with default impls and a `Split`
enum is more abstraction than our connector shape demands. Arroyo's
approach — per-connector logic in the control plane — is the right size.

**The control-plane dispatch:**

```rust
fn assign_splits(connector: &ConnectorKind, live_instances: &[InstanceId])
    -> Vec<(SplitId, InstanceId)>
{
    match connector {
        ConnectorKind::Kafka { topic } => kafka::delegate_to_consumer_group(topic),
        ConnectorKind::Files { path } => files::enumerate_and_distribute(path, live_instances),
        ConnectorKind::PostgresCdc | ConnectorKind::MysqlCdc
            | ConnectorKind::WebSocket | ConnectorKind::Otel => {
            // Singleton source — pin to one instance.
            pin_to_lowest_id(live_instances)
        }
    }
}
```

~30 lines of dispatch. Each connector's logic lives in its own module
next to the existing connector code. No trait; no `Split` enum; if a
seventh connector type arrives, add a match arm.

**Connector partitioning reference:**

| Source | How assignment happens |
|---|---|
| Kafka | Join the Kafka consumer group; Kafka's broker assigns partitions. We subscribe to rebalance callbacks (§3.1 below). |
| Files | Enumerate paths, distribute by `xxh3(path) % live_instances.len()`. |
| Postgres CDC / MySQL CDC / WebSocket / OTEL | Pin to one instance; on failover, rebind to the new lowest-ID alive. |

**Every reader instance is a shuffle sender.** It reads its assigned
splits, key-hashes each event, and sends it to the instance owning the
target vnode. Uniform across Kafka (8 readers × 1/8 of the partitions)
and CDC (1 reader for the whole slot).

For CDC, the singleton reader fan-outs everything across the cluster.
Back-pressure is a real concern: a slow consumer back-pressures the
shuffle, the reader, and eventually the upstream replication slot.
Per-instance buffers + a staleness alarm on the reader handle it. Same
failure mode as Flink-CDC.

#### 3.1 Kafka consumer-group integration (not a one-liner)

"Delegated to Kafka consumer group" is three lines of prose but several
days of integration work. The real surface:

- **Rebalance callbacks.** `onPartitionsRevoked` / `onPartitionsAssigned`
  drive our internal state-ownership changes. On revoke, we flush
  in-flight state for the revoked partitions before Kafka reassigns them.
- **Cooperative-sticky strategy.** We pin
  `partition.assignment.strategy = CooperativeSticky` so rebalances
  move the minimum number of partitions, in stages (incremental
  rebalance). Revoked partitions come back in the next rebalance step,
  not all at once.
- **Session-timeout vs checkpoint cadence.** Consumer group session
  timeout has to exceed our checkpoint cadence. If cadence is 10s and
  session timeout is 10s, we'll be evicted mid-checkpoint under
  pressure. Tune `session.timeout.ms >= 3 × checkpoint_interval`.
- **Rebalance during checkpoint.** If Kafka reassigns partitions mid-
  checkpoint, the in-progress snapshot is invalidated for the revoked
  partitions. Options: abort the checkpoint and retry, or accept the
  snapshot and record the rebalance for replay. Phase C must pick one.

#### 3.2 Shuffle protocol: separate design doc

The shuffle operator is where most of the Phase C engineering lives.
Wire protocol (Arrow IPC over TCP? framed gRPC?), back-pressure
propagation (credit-based flow control), per-key ordering guarantees,
shuffle-channel snapshotting under aligned checkpoints, and connection
management (N² vs pooled) all need specifying before implementation.

**This gets its own design doc:** `docs/plans/shuffle-protocol.md`.
Expect ~2 weeks of design + review before Phase C can start
implementation of the operator itself. Calling it out here so the
Phase C estimate reflects the real scope.

**Consequence for existing code:** `cluster/partition/assignment.rs`
(`ConsistentHashAssigner`, currently at `crates/laminar-core/src/delta/partition/assignment.rs`) is unused.
Kafka consumer group handles partitioned sources; the rest is per-connector
dispatch. **Delete it.** (Phase A note: the `delta/` module itself is
renamed to `cluster/` for consistency with the `cluster-unstable`
feature flag — see Phase A deliverables.)

### 4. Watermark propagation: chitchat gossip (concrete, no trait)

Each instance publishes its watermarks to chitchat. Consumers (the
aggregate / window operators) read peers' watermarks for cross-instance
dependencies. Propagation: ~100ms–1s. Fine for 1–5s SLA.

**No `WatermarkSource` trait in v1.** The previous draft proposed one
with two impls (`InMemoryWatermarks` for tests, `ChitchatWatermarks` for
prod). That's the same pattern the review flagged last time: a trait
with one real impl and a test-double isn't worth the abstraction — it
just adds a seam that misleads readers into thinking there are two
transports. Tests use chitchat over loopback via its existing
`UdpTransport` or the built-in mock transport.

Concrete type: `ChitchatWatermarkBus` in `laminar-core`. If a second
transport is ever needed (coordinator-mediated, direct TCP for
sub-100ms targets), extract the trait from the second call site with
the benefit of hindsight.

**Consequence:** the in-memory watermark table I put in
`ObjectStoreBackend` is wrong in multi-instance mode. Replace it with a
`ChitchatWatermarkBus` reference passed at construction. Single concrete
type; no dyn dispatch.

### 5. Aggregate operator shape: partition-and-hold (Flink-style)

Each instance owns aggregate groups for its vnodes. No merger instance.
Events for a key arrive at the owning instance (either directly, for
co-located sources, or via shuffle, for non-co-located sources). Aggregate
runs locally. Emit happens per-instance.

This is source-agnostic. The source only affects whether the event gets to
the owner via Kafka partitioning or via a network shuffle.

### 6. Do we actually need `PartialAggregate`?

**No.** DataFusion's `Accumulator::state()` → `merge_batch()` is the monoid
we need for SUM / COUNT / MIN / MAX / AVG (AVG carries `(sum, count)`;
COUNT is a counter; MIN/MAX are idempotent). Cross-instance state migration
on rebalance uses the same primitives that already power checkpoint/restore.

**Consequence:** `state/partial_aggregate.rs` and the six hand-rolled impls
(SumI64, SumF64, CountU64, MinI64, MaxI64, AvgF64) are redundant with
DataFusion. **Delete all of them.**

### 7. Control plane: chitchat KV + object store, split by lifetime

The control plane has four jobs. Split them between the two systems we
already require, based on state lifetime:

| Job | Mechanism | Why |
|---|---|---|
| Membership / liveness | **chitchat** phi-accrual | Already in use. Gossip-speed. |
| Leader election | **chitchat**: lowest-ID alive | Trivial, correct, no lease bugs (see below). |
| Barrier coordination (announce, acks) | **chitchat KV** | Ephemeral; needs <200ms propagation; eventual consistency is fine. |
| Assignment (split → instance) | **chitchat KV**, backed by **object store** on change | Ephemeral for reads (hot path); durable on the object store so a full-cluster restart doesn't lose state. |
| Completed checkpoint manifests | **object store** | Must survive cluster death — recovery reads this. |
| Per-vnode partials | **object store** (§1) | Durable state, already specified. |

**The lifetime split matters.** chitchat's gossip KV is designed for
this kind of small, ephemeral, eventually-consistent coordination
state — that's why RisingWave, Quickwit, and others use it. Polling
object store every 5s to observe barrier announcements is wasteful
when chitchat propagates the same info in ~100ms and is already
running.

What *must* live on the object store is anything that has to outlive
the process lifetimes of all current members: completed checkpoint
manifests, per-vnode state partials, and a snapshot of the current
assignment (so a full cluster restart can recover without starting
from scratch).

**Leader election: lowest-ID-alive, not lease.**

An object-store lease with TTL is the canonical wrong answer here —
every implementation of this pattern I've seen in production has
introduced at least one post-mortem around clock skew, lease-renewal
races, or fenced-off zombie leaders. The question to ask is: **do we
need strong leadership?**

The leader's only job is triggering checkpoint barriers. If two
instances briefly both think they're leader during failover:

- Both announce barrier for epoch N via chitchat KV (idempotent —
  same epoch = same announcement)
- Both try to commit the manifest at `state/epoch=N/_COMMIT` via
  `PutMode::Create`. Only one wins. The other observes its write
  failed and backs off.
- No correctness issue — at worst, one wasted barrier announcement.

Chitchat's live-nodes set is observable by every member; `min(id)` is
a trivially-consistent weak leader. ~20 lines of code vs the
non-trivial correctness surface of a distributed lease. Use this.

**What still goes on the object store:**

```
s3://bucket/laminar/
  state/
    epoch=N/vnode=V/partial.bin          (durable, §1)
    epoch=N/_COMMIT                      (durable; CAS seals the epoch, §1)
  control/
    assignment-snapshot.json             (durable snapshot of current
                                          assignment; refreshed on change
                                          so cluster restart recovers)
    manifests/{N}.json                   (completed checkpoint manifests)
```

**What goes on chitchat KV:**

```
members/<id>/role                        (instance metadata, gossiped)
members/<id>/liveness                    (phi-accrual, gossiped)
control/leader                           (derived locally from live-nodes min-ID)
control/barriers/<epoch>                 (leader-announced, ephemeral)
control/barriers/<epoch>/ack/<id>        (per-instance ack, ephemeral)
control/assignment-version               (monotonic version counter for fencing)
```

**Why not openraft:** ~30 transitive crates, consensus debugging,
solves a problem we don't have.

**Why not Postgres / etcd:** new operational surface for a problem the
stack we're already running can handle.

**Why not chitchat-only:** no durable history. Full-cluster restart
would lose assignment and in-flight barriers. Object store
`assignment-snapshot.json` fills that gap without adding a service.

**Why not object-store-only:** adds ~500ms of coordination latency
(poll + CAS roundtrip) for every barrier step when chitchat does it in
~100ms already.

### 8. Failure recovery: cold, per-source offset replay

Instance dies → chitchat flags it (~5s) → control plane reassigns its
splits → new owner starts consuming from the last committed offset →
rehydrates state from last checkpoint.

Per-source offset semantics — already abstracted by `SourceCheckpoint`:

- Kafka: partition → offset map
- CDC: last committed LSN / (binlog-file, binlog-pos)
- Files: (file, byte-offset) map per split
- WebSocket: none — degrades to at-most-once (existing `supports_replay`
  field on `RegisteredSource` drives this)

Total unavailability on a partition/split: ~10–30s. Within SLA.

### 9. Exactly-once end-to-end: coordinator-driven 2PC, per-sink primitives

The existing 2PC scaffolding in `CheckpointCoordinator` already abstracts
over the sink commit mechanism:

```
sink.begin_epoch(n)
sink.precommit(n)       // stage data
coordinator.barrier_aligned_across_instances()
coordinator.persist_manifest()
sink.commit(n)          // atomically expose
```

The coordinator doesn't need to know **how** the sink commits — only that
the 2PC protocol completes. Per-sink implementations:

| Sink | Commit primitive | Failure → rollback |
|---|---|---|
| Kafka | `producer.commit_transaction()` | `abort_transaction()` |
| Postgres | `CONNECTION.COMMIT` | `ROLLBACK` |
| Delta Lake | atomic metadata commit | drop staged files |
| Iceberg | same as Delta | same |
| Files | `rename(staged, final)` | delete staged |
| MongoDB | idempotent upsert | (at-least-once only) |
| WebSocket / Stdout | at-least-once | no rollback — degrade |

The existing `DeliveryGuarantee::ExactlyOnce` flag on the pipeline validates
at startup that all sources replay and all sinks commit atomically. The
distributed version adds one step: the coordinator has to drive the 2PC
across instances, not within one process.

**Cross-instance barrier protocol:**

- **Leader** (lowest-ID alive per §7) writes
  `control/barriers/<epoch>` into chitchat KV to announce the barrier.
  Propagates to peers in ~100 ms via gossip.
- Each instance observes the announcement, triggers barrier injection on
  its sources, aligns its inputs, snapshots state to object store, then
  writes its `control/barriers/<epoch>/ack/<instance_id>` key into
  chitchat KV.
- Leader observes acks via gossip; once quorum is reached (all instances,
  or all minus a configurable timeout), it commits the manifest at
  `state/epoch=<N>/_COMMIT` on the object store via `PutMode::Create`
  (the durable seal). This is the only place the object store is in the
  barrier path.
- Leader then drives sink commit across all instances (see per-sink
  table above).
- Any sink commit failure → global rollback; leader aborts the epoch
  and the next barrier retries.

The slowest instance paces all checkpoints. Per-instance timeout +
quorum bounds this; a slow instance triggers rebalance (its splits
move to healthy peers). See §A.2 for the aligned-checkpoint
implications.

### 9.1 Multi-source pipelines: exactly-once semantics

A pipeline with two sources (e.g., Kafka topic + Postgres CDC) has two
independent checkpoint semantics at the source:

- Each source's position is captured in its own `SourceCheckpoint`
  variant (Kafka partition→offset map, CDC LSN, Files byte-offset
  map). Already abstracted.
- On recovery, each source resumes from its own recorded position.

**What exactly-once guarantees, and what it doesn't:**

- ✅ Within each source, no events are lost or duplicated.
- ✅ State is consistent across all sources at the checkpoint epoch
  boundary (barrier alignment guarantees this).
- ⚠️ **Cross-source relative order is not guaranteed.** Event A from
  Kafka and event B from CDC may arrive at a join operator in either
  order, even if one was "produced first" in wall-clock time. Watermark
  alignment across sources keeps event-time-sensitive operators
  correct (windowed joins, for instance, wait for watermark on both
  sides), but a pure stateful join on arrival order is not well-defined
  for multi-source pipelines.

**Contract for Phase C:** multi-source pipelines use event-time
alignment (watermarks on both sides); the global watermark of the
pipeline is `min(watermark_per_source)`. Joins, windows, and any
stateful operator consuming multi-source input must be event-time
driven, not arrival-time driven. Enforce at pipeline validation: reject
pipelines that join multi-source streams without event-time declared on
both sides.

### 10. Deployment shape: homogeneous instances

Every instance runs the full pipeline topology, owns a subset of vnodes,
reads its assigned splits, writes its owned state. Symmetric scaling.

Unchanged. Source-agnostic.

---

## What this means for the code I wrote

Now that the design is source-agnostic, the fate table looks like this:

| Piece | Fate | Why |
|---|---|---|
| `StateBackend` trait | **Keep.** Drop watermark methods from the trait. | Coordinator consumes it regardless of source. |
| `InProcessBackend` | **Keep.** | Embedded / single-instance. Source-independent. |
| `LocalBackend` | **Keep.** | Standalone server. Source-independent. |
| `ObjectStoreBackend` | **Keep, simplified.** Drop in-memory watermarks; hold a `ChitchatWatermarkBus` reference. No `WatermarkSource` trait. | Multi-instance state for any source. |
| `StateBackendConfig` enum | **Keep.** | All three variants have consumers. |
| `VnodeRegistry` | **Keep, simplified.** Replace `rendezvous_hash` with `key_hash` (xxh3). | §2 — one hash fits all. |
| `rendezvous_hash` | **Delete.** | §2 — HRW adds nothing over xxh3 + modulo. |
| `PartialAggregate` + 6 impls | **Delete.** | §5/§6 — DataFusion `Accumulator::state()` already does this. |
| `ConsistentHashAssigner` (`cluster/partition/assignment.rs`) | **Delete.** | §3 — Kafka consumer group + per-connector dispatch supersede. |
| `cluster/partition/guard.rs` (`PartitionGuardSet`) | **Reuse.** Drives assignment-version fencing in `ObjectStoreBackend`; replaces the ad-hoc `assignment_version: AtomicU64` fields. | §A.4 — already implements epoch-fenced partition ownership. |
| `cluster/coordination/mod.rs` (`DeltaManager`) | **Reuse, rename.** Rename to `ClusterManager`; drive lifecycle phases (Discovering → WaitingForAssignment → Active → Draining). | Existing state machine maps onto the new startup sequence. |
| `cluster/discovery/gossip_discovery.rs` | **Reuse.** Chitchat already wired; extend the KV tier for leader election + barrier coordination (§7). | No new protocol needed. |
| State backend integration test | **Keep, extend for multi-source scenarios.** | Becomes the multi-instance harness. |

New code we'll write:

1. `key_hash(bytes) -> u64` — thin wrapper over `xxhash_rust::xxh3::xxh3_64`.
   Ten lines, no new deps.
2. Per-connector split-enumeration logic in the control-plane module
   (`match connector_type { Kafka => ..., Files => ..., Cdc => singleton,
   ... }`). No `SplitEnumerator` trait.
3. `ChitchatWatermarkBus` — concrete type reading/writing per-instance
   watermark values through the chitchat KV tier. No trait.
4. `laminar-core::cluster::control` — the coordination module. Reads and
   writes `control/<epoch>` / `control/leader` / etc. in chitchat KV for
   the ephemeral path, plus `control/assignment-snapshot.json` on the
   object store for durability.
5. Shuffle operator — wire protocol, back-pressure, ordering.
   **Specified in its own design doc:** `docs/plans/shuffle-protocol.md`.
6. Cross-instance barrier protocol wiring in `CheckpointCoordinator`.

## Phased delivery

Phased by capability. Each phase has a **failure-scenario test matrix**
(see below) as the gate — happy-path throughput tests alone don't
validate a distributed system.

| Phase | Capability | Connectors enabled | Realistic duration |
|---|---|---|---|
| **A** | SB-06 manifest migration; `key_hash` (xxh3) + `VnodeRegistry` wired; review-driven deletions; rename `delta/` → `cluster/` module; audit `cluster/` for reusable pieces (`PartitionGuardSet`, `DeltaManager`) | Everything, single-instance, vnode-partitioned state | 1 wk |
| **B** | SB-08: `CheckpointCoordinator` consumes `Arc<dyn StateBackend>`; two-phase ordering doc; expose the four metrics in §A.5 from day one | Everything, single-instance, state on hot path | 1–2 wk |
| **C.1** | **Shuffle protocol design doc** (`docs/plans/shuffle-protocol.md`) — wire format, back-pressure, ordering, connection management | — | 2 wk (design only) |
| **C.2** | Cluster control (chitchat KV schema, leader election, barrier announce/ack, assignment snapshot on object store) | — | 1 wk |
| **C.3** | Shuffle operator implementation | All sources | 1–2 wk |
| **C.4** | Per-connector split-enumeration dispatch; Kafka consumer-group integration (§3.1 — rebalance callbacks, session tuning, cooperative-sticky) | Kafka, Files, CDC, WebSocket, OTEL | 1 wk |
| **C.5** | Cross-instance barrier protocol wiring; failure-scenario test matrix (below) | Full MVP | 1 wk |
| **D** | Distributed aggregate via DataFusion `Accumulator::state()`; multi-source event-time alignment (§9.1) | All sources that feed aggregates | 2–3 wk |
| **E** | Dynamic rebalance on membership change | Everything | 2–3 wk |

**Total realistic envelope: 10–14 weeks for Phases A–E**, not 8–12.
Phases C.1–C.5 are 6–8 weeks, not the 3 weeks the earlier estimate
listed — shuffle protocol, consumer-group integration, and failure
testing each pull their own weight.

**Failure-scenario test matrix (gate for Phase C.5 and every later phase).**
Integration tests must cover, at minimum:

- Kill instance mid-checkpoint (during snapshot phase)
- Kill leader during barrier announcement
- Kill instance mid-shuffle (in-flight buffer on the wire)
- Object store unavailable for 30s; pipeline degrades gracefully
- Kafka broker failover mid-checkpoint
- Chitchat partitioned (one instance can't reach others)
- Two instances concurrently decide they're leader during phi-accrual flakiness
- Source rebalance (Kafka `onPartitionsRevoked`) mid-checkpoint

These are the tests that prove exactly-once under failure. Happy-path
green CI is not sufficient to call Phase C done.

## Non-goals

- **Cross-region / multi-DC.** Same AZ for v1.
- **Hot standby.** Cold recovery at 30s is acceptable.
- **Raft-based consensus.** Chitchat + object store is enough.
- **Cross-source transactions** (2PC spanning source + sink of different
  systems). Stays coordinator-driven per-sink.
- **Arbitrary resharding of singleton sources.** Postgres CDC has one reader;
  we pin it, we don't try to parallelize the replication slot.
- **Partition count changes at runtime** (adding Kafka partitions, adding
  files mid-stream). Out of scope; requires design.

## Open questions

1. **State segment size on S3.** To recover 10 GB in <30s we need
   ~300 MB/s effective throughput = 3–10 parallel gets per vnode. Pick
   segment size accordingly — probably 100–500 MB per segment.
2. **Checkpoint interval in distributed mode.** Current default is 10s.
   Cross-instance 2PC adds per-barrier overhead. Raise to 30–60s?
3. **Sink transaction granularity.** Kafka limits producer transactions to
   one producer-ID per transaction. Per-instance sink transactions with a
   coordinator commit may be required — can we drive them consistently
   from the checkpoint initiator?
4. **Singleton-source back-pressure (CDC / WebSocket).** When one instance
   reads everything and shuffles, a slow downstream instance back-pressures
   the shuffle, which back-pressures the source, which falls behind the
   LSN/offset. Standard Flink-CDC failure mode; per-instance buffer tuning
   + staleness alarm on the reader.
5. **Idle vnodes.** Partition-and-hold keeps state for a vnode even if no
   events arrive. OK at 10⁵ keys, broken at 10⁸ keys. TTL or tiered storage
   needed past a threshold.
6. **Control plane on slow object stores.** If S3 GETs run 500 ms+ (bad
   region, congestion), barrier coordination slows. Mitigation: cache
   leader/assignment locally with short TTL; only round-trip to S3 on
   refresh. Open: what's the failure mode when an instance can't reach S3
   at all — freeze local processing, or emit degraded results?
7. **Future Kafka co-location flag.** If a user's Kafka producer is under
   our control, can we offer a config to match the producer hash and skip
   the shuffle? Mechanically simple but requires a producer-side SDK we
   don't ship. Parked until there's a driver.

(Previously Q7 — multi-source pipeline alignment — is now resolved in
§9.1: same `key_hash` across all sources, enforce event-time join
semantics at pipeline validation.)

## What happens next

If this design holds up under review:

1. Confirm or correct the assumptions table and the connector-landscape
   table.
2. Push back on any remaining concerns — the hash choice is now xxh3,
   the control plane is object-store-backed (no Postgres), and CDC is
   covered by the same shuffle path as Kafka.
3. Once locked, Phase A starts: manifest migration, `key_hash` (xxh3), and
   the review-driven deletions.

### Revision history

- **v1:** Kafka-specific. Proposed murmur2 + Postgres control plane.
- **v2:** Source/sink agnostic. xxh3 hashing; always-shuffle; chitchat +
  object store control plane; Phases C and E merged.
- **v2.1:** Added Addendum A on latency tuning.
- **v2.2 (this revision):** Review-driven cleanup.
  - Dropped speculative `WatermarkSource` and `SplitEnumerator` traits;
    per-connector dispatch instead.
  - Leader election by chitchat lowest-ID alive, not object-store lease.
  - Control-plane state split by lifetime: chitchat KV (ephemeral) vs.
    object store (durable history).
  - Explicit reuse of existing `cluster/` (née `delta/`) module —
    `PartitionGuardSet`, `DeltaManager`/`ClusterManager` lifecycle,
    chitchat discovery.
  - Added §3.1 Kafka consumer-group integration, §3.2 shuffle-protocol
    design-doc reference, §9.1 multi-source exactly-once semantics.
  - Phase plan expanded to C.1–C.5 subphases; realistic total 10–14
    weeks. Failure-scenario test matrix added as a gate.
  - Renamed "delta" terminology → "cluster" throughout.

---

## Addendum A: Latency tuning

Three knobs govern end-to-end tail latency in this design. Each has a
decision point where it's reasonable to over-engineer, and a simpler
alternative that's usually the right call. This section is the record of
which choice we're making and when to revisit.

The p99 latency floor for a pipeline in this design decomposes roughly as:

```
p99_end_to_end ≈ max(
    cadence + commit_duration,            ← exactly-once sinks
    cadence / 2,                          ← at-least-once sinks
    watermark_propagation + processing,   ← windowed operators
    barrier_alignment_wait,               ← multi-input operators under skew
)
```

Cadence usually dominates. Unaligned checkpoints matter only when the
fourth term starts biting. Watermark propagation matters only for windowed
or event-time-sensitive pipelines.

### A.1 Checkpoint cadence

**Current default:** 10s (`CheckpointConfig::default_interval`, set in
`checkpoint_coordinator.rs`).

**The tradeoff:** shorter cadence → lower tail latency for exactly-once
sinks; longer cadence → less 2PC overhead and fewer object-store writes.
Distributed 2PC inflates the overhead side — per-barrier work now includes
cross-instance coordination through the object store.

**Per-barrier cost in distributed mode** (rough estimate, S3 in same AZ):

| Step | Cost |
|---|---|
| Initiator writes `control/barriers/{epoch}.json` | ~100 ms |
| Each instance: receive barrier, snapshot state, flush to S3 | 200–2000 ms (scales with state size) |
| Each instance writes ack object | ~100 ms |
| Initiator polls for quorum | ~100–500 ms |
| Sink commit (per-sink) | 100–500 ms (Kafka tx ~100ms) |
| **Total** | **~1–3 s** |

So cadence floor is ~2s (below this, we're spending more than half the
time on coordination). Practical sweet spots:

- **Exactly-once, low-latency pipelines:** 5s cadence. p99 ~5–8s.
- **Exactly-once, standard:** 10s cadence. p99 ~10–13s.
- **At-least-once (sinks emit immediately):** 30–60s. Cadence only affects
  recovery RPO, not sink-visibility latency.

**Recommendation:** keep `CheckpointConfig::interval` per-pipeline
configurable (it already is). Default stays 10s. Surface the 2s floor in
config validation — reject `interval < 2s` for distributed mode with a
message explaining the coordination overhead.

**When to revisit:** if a pipeline's p99 SLA can't be met with 5s cadence
and commit_duration is the long pole, consider adaptive cadence — slow
down under load, speed up when idle. Flink has this via "back-pressure
aware checkpointing." Adds complexity; don't build unless needed.

### A.2 Unaligned checkpoints

**What they are:** the Chandy-Lamport barrier protocol requires an
operator with multiple inputs to wait until the barrier arrives on **all**
inputs before snapshotting and forwarding. Fast inputs block behind slow
ones. Flink 1.11+ added unaligned checkpoints: the operator snapshots as
soon as it sees the barrier on any input, captures the in-flight buffered
data from not-yet-barriered inputs into the snapshot, and forwards the
barrier immediately.

**When this matters for us:** aligned checkpoints are fine when every
operator is single-input. They hurt when:

- **Joins** have two input streams (left side, right side) — the slower
  stream paces the checkpoint.
- **Distributed aggregate** has N upstream instances shuffling to M
  owners — each owner has N inputs; slowest upstream instance paces.
- **Multi-source pipelines** joining two Kafka topics — same issue.

Under load or skew, barrier alignment wait can grow to tens of seconds.
That's the fourth term in the p99 decomposition; it's invisible at low
load and brutal at high load.

**The cost of building it:**

- Snapshot format changes — checkpoint now includes per-input in-flight
  buffer state.
- Recovery changes — must replay the captured buffers on the right
  inputs, in the right order.
- Barrier propagation changes — barriers can overtake data, which
  sequencing assumptions currently forbid.
- Flink's implementation is ~12 months of focused engineering. We'd be
  signing up for a scaled-down version of that.

**Recommendation: do NOT build in v1.** Aligned checkpoints are a known
limitation; document it, monitor barrier_alignment_wait as a metric,
accept the tail-latency cost.

**When to revisit:** two clear signals would force the design:

1. **Checkpoint completion time > cadence.** If cadence is 5s but
   barriers take 6s to complete under skew, we're falling behind. Can't
   just raise cadence — that defeats the latency goal. Unaligned is the
   real fix.
2. **Hot partition / skew complaints.** If one Kafka partition has 10×
   the traffic of others, the instance owning it paces everyone.
   Unaligned keeps tail latency bounded independent of the slowest input.

Before building unaligned, try simpler mitigations: re-partition to
reduce skew, add more instances, shorten cadence to reduce the absolute
wait. If none work, write an RFC and design it properly.

### A.3 Watermark propagation

**Current design (§4):** chitchat gossip. Each instance publishes its
per-vnode watermarks into the chitchat KV tier; peers observe changes
via gossip. Default propagation: 500 ms – 1 s.

**The latency cost:** windowed operators can't emit a window until the
watermark has advanced past `window_end` on every vnode contributing to
that window. A 1s watermark propagation adds 1s to window-close latency.
For 10-second windows that's 10% overhead — negligible. For 500-ms
tumbling windows it doubles emit latency — significant.

**Alternatives, in order of complexity:**

| Approach | Propagation | Cost |
|---|---|---|
| Chitchat gossip (current) | 500 ms – 1 s | Zero new code; eventually consistent |
| Chitchat with tighter interval (`gossip_interval = 100ms`) | ~200–400 ms | More gossip traffic; same protocol |
| Piggyback on barrier protocol | ~cadence | Sparse; misses in-between-barrier updates |
| Coordinator-mediated (leader broadcasts) | 20–50 ms | Central point; failover needed |
| Direct TCP mesh (instance-to-instance) | 5–20 ms | N² connections; membership-aware |

**Recommendation:** chitchat is adequate for 1–5 s latency SLAs. Ship as
a concrete `ChitchatWatermarkBus` (no trait — see §4). Tune
`gossip_interval` to 100 ms; that gets us ~200–400 ms propagation out
of the box. If a second transport is ever needed, extract a trait from
both call sites with the benefit of hindsight.

**One specific optimization worth flagging now, not building:** gossip
`min(per-vnode watermarks)` per instance instead of per-vnode values.
Reduces gossip payload from O(vnodes × instances) to O(instances) and
still lets windowed operators compute global watermark. Loses per-vnode
progress tracking — which only matters if some vnodes are idle and we
care about distinguishing "no events yet" from "events, just slow."
If idle-vnode tracking isn't needed, ship the coarser version. That's
a one-line change at build time.

**When to revisit:** if a pipeline needs p99 window-close < 200 ms,
chitchat isn't going to get there. Direct TCP or coordinator-mediated
becomes the right tool. This is a specific SLA requirement, not a default
need.

### A.4 Implications for the phase plan

- **Phase A (`cluster/` module audit):** reuse `PartitionGuardSet` for
  the assignment fence; reuse `DeltaManager` (renamed `ClusterManager`)
  for the startup lifecycle. No net-new distributed-lifecycle code.
- **Phase B (SB-08 wiring):** default cadence stays 10 s. Reject
  `interval < 2s` in cluster mode via a `CheckpointConfig` validation
  rule that points at this addendum. Export the four metrics below.
- **Phase C.3 (shuffle + watermarks):** concrete `ChitchatWatermarkBus`
  (no trait). Tune `gossip_interval` to 100 ms.
- **Phase D (distributed aggregate):** aligned checkpoints only. The
  `barrier_alignment_wait_ms` metric drives the "is unaligned needed?"
  conversation before we build it.
- **Not in scope (unless a driver emerges):** unaligned checkpoints,
  direct TCP watermarks, adaptive cadence.

### A.5 Metrics to expose from day one

Four metrics give the "revisit if…" clauses real evidence:
`laminardb_checkpoint_duration_ms{phase=...}` (cadence tuning),
`laminardb_barrier_alignment_wait_ms{operator=...}` (unaligned decision),
`laminardb_watermark_propagation_ms` (watermark-transport decision),
`laminardb_p99_end_to_end_ms{pipeline=...}` (the SLA). They land in
`engine_metrics.rs` at Phase B so we have baselines before any
multi-instance runs.
