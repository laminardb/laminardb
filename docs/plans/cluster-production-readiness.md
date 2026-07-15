# Plan: Cluster Mode Production Readiness (Distributed, Partition-Aware, Exactly-Once)

- **Status:** In progress. Cluster exactly-once remains deliberately rejected with `LDB-0013`.
- **Last audited:** 2026-07-14
- **Scope:** Make `cluster` a production-grade distributed streaming runtime:
  partition-aware sources, fault-tolerant state and elastic operation. Admit cluster
  exactly-once only after its remaining fences are proven.
- **Relationship to other plans:** This is a **prerequisite for**
  `docs/plans/lookup-source-production-mpp.md` Track B (distributed joins) — distributed
  non-aggregation operators and partitioned state are shared foundations. The lookup-MPP
  single-node track (Track A) does **not** depend on this and can proceed in parallel.
- **Targets (grounded in Flink / RisingWave / Arroyo, decided 2026-05-28):**
  - **Correctness:** retain the implemented aligned global cut; do not admit cluster
    exactly-once until the leader term also fences durable decisions and external sink commits.
  - **Sources:** engine-controlled split assignment (not Kafka consumer groups), per-split
    offsets in the checkpoint, dynamic partition discovery, cross-node watermark alignment.
  - **Elasticity:** fixed logical vnodes backed by one local LSM working copy per owner and
    immutable object-store checkpoint handles. **Failover first**, then elastic rescale at an
    assignment/checkpoint cut. A thin object-store adapter is not a live-state engine.
  - **Coordination (AD-0, settled):** hybrid — gossip for liveness, object-store CAS for
    fencing, **Postgres** as the authoritative control store (fenced leader lease, assignment,
    commit verdict, recovery epoch). See AD-0 below.

---

## How the reference systems do this (rationale)

The production systems converge on fixed logical partitions, but not on one storage design:

- **Correctness:** ABS aligned barriers + 2PC sinks (Flink, RW, Arroyo). Flink adds
  *unaligned* checkpoints so alignment can't stall under backpressure.
- **Partition-aware sources:** a central enumerator/assigner (Flink JobManager
  `SplitEnumerator`, RW meta, Arroyo controller) maps source splits → parallel reader
  subtasks via the engine's own assignment, using manual `assign()` — **not** Kafka consumer
  groups — so partition→subtask mapping is deterministic and consistent with checkpointed
  per-split offsets. Plus dynamic discovery and watermark alignment (Flink FLIP-182).
- **State:** Flink and Kafka Streams keep partitioned working state locally and recover from
  checkpoints/changelogs. RisingWave's shared Hummock LSM additionally requires a versioned
  metadata authority, pinned epochs, compaction ownership, garbage collection, and caches.
  Materialize similarly couples blob storage to transactional consensus. LaminarDB must first
  implement the local keyed-state path; its current object-store backend persists checkpoint
  artifacts and exact-attempt seals only.
- **Other execution models:** [Spark Structured Streaming](https://spark.apache.org/docs/latest/streaming/)
  binds state to a stable shuffle partitioning and uses RocksDB snapshots or changelog checkpoints;
  its exactly-once path remains micro-batch, while continuous processing is at-least-once.
  [ksqlDB](https://docs.confluent.io/platform/current/ksqldb/operate-and-deploy/capacity-planning.html)
  keeps one local RocksDB store per input partition and rebuilds it from compacted changelog topics;
  repartition topics are part of the durable dataflow. These validate fixed keyed partitions plus
  local working state, but do not justify putting Kafka or object-store I/O on LaminarDB's record
  path.
- **Control/recovery split:** [Arroyo](https://doc.arroyo.dev/architecture/) uses asynchronous
  barrier snapshots, remote object storage for production checkpoints, and PostgreSQL for
  distributed control metadata. Its local storage choices are deployment conveniences, not a
  reason to weaken cluster checkpoint authority.
- **Elasticity:** fixed logical partitions (Flink key groups; RisingWave/LaminarDB vnodes)
  decouple state ownership from physical parallelism. Reassignment and restore happen at a
  fenced checkpoint cut; gossip is never assignment or state authority.

LaminarDB has useful primitives—vnodes, assignment fences, aligned barriers, immutable checkpoint
artifacts, and a bounded shuffle—but it does not yet have a common live keyed-state engine. The
operator-owned maps and synchronous checkpoint scans are an architectural gap, not wiring work.

## Current state (audited 2026-07-14, `cluster`)

**Implemented:** gossip membership and failure detection; versioned assignment and state-write
fences; fixed logical vnodes assigned by deterministic rendezvous hashing; checkpoint-bound vnode
partials and adoption-time rehydration; and a TLS shuffle transport with assignment/recovery
fencing and bounded per-peer/per-node admission. The earlier distributed aggregate path remains as
implementation substrate, but keyed cluster DDL now fails closed because its operator-owned maps
have no live-state byte bound.

The checkpoint path now forms one **global aligned cut**. Sources hold at their barriers, the graph
drains pre-cut work, sinks are fenced, every process fans a shuffle barrier to the same frozen
participant roster, and state capture starts only after all peer barriers arrive. Roster changes,
sequence gaps, partial fan-out, and alignment timeout reject the attempt.

**Remaining production gates:**
- Cluster exactly-once fails closed with `LDB-0013`; checkpoint verdicts are term-fenced, but
  supported connectors lack certified term-fenced source handoff and external sink cursors.
- Shuffle `send_to` completion is local queue admission, not a remote-delivery acknowledgement.
  Sequence gaps and barrier high-water marks fence detected loss, but callers must not interpret a
  successful send as proof that a peer incorporated the row.
- Cluster support remains operator-specific. Unsupported stateful paths fail closed;
  fault, recovery, rescale, and latency coverage must pass for each admitted path.

## Goals

- Run distributed with Kafka where partitions are assigned across nodes by the engine,
  survive node loss with correct offset/state recovery, and add/remove nodes.
- End-to-end exactly-once across the distributed dataflow.
- Fault tolerance: a node crash never drops committed state; survivors adopt its vnodes.
- Robust, secure inter-node transport; graceful lifecycle; operable via an admin API + metrics.

## Non-goals (anti-over-engineering boundaries)

- **No Raft / custom consensus engine** unless the coordination-store decision (below) calls
  for it. The current weak-leader + durable-CAS-fence model is correct *if* the fence is
  airtight; we either keep it or adopt an external store — we do not hand-roll Raft.
- **No record-by-record live state migration.** Rescale is a fenced checkpoint-cut reassignment
  and reload with a bounded reconfiguration pause.
- **No multi-region / geo-replication** in this plan.
- **No bespoke RPC framework** — reuse the existing TLS gRPC shuffle transport, object store,
  and gossip.
- **No shared live state implemented as a thin object-store adapter.** Build one local batched
  keyed-state engine for embedded, single-node, and cluster workers. Shared object-backed live
  state is deferred until a term-fenced metadata/version/compaction/GC authority exists.

## AD-0 — Coordination store (SETTLED 2026-05-28: Hybrid + Postgres)

**Decision:** Adopt the **hybrid model (Option C)** with **Postgres** as the authoritative
control store. Responsibilities split by what each layer is good at:

- **Gossip (chitchat) keeps membership + failure detection** — fast liveness; never put a
  store round-trip on the liveness path. Unchanged from today.
- **Object-store CAS keeps fencing** (`assignment_version` gate + `_COMMIT` committer-id CAS)
  as defense-in-depth — the "even if the control plane has a bug, committed state cannot be
  corrupted" backstop. Retained, not removed.
- **Postgres becomes authoritative for:** a transactionally incremented **fencing term** plus a
  database-clock lease row (renewal and every control mutation compare term and owner), the
  **assignment** (vnode→node and partition→node rows), the **checkpoint commit verdict**, and
  **cluster-wide recovery-epoch agreement**.

**Why Postgres (not etcd):** matches Arroyo + the prior internal server-hardening
recommendation (2026-03-09, Postgres-reconciliation/Arroyo pattern); multi-row transactions
make assignment+epoch updates atomic; a queryable control DB gives the Phase 5 admin/
observability surface for free; most deployments already run a Postgres. etcd's operational
edges (quorum fsync, defrag, revision compaction) are worse for teams not already on it.

**Availability coupling:** the data plane does not touch Postgres, so a control-store outage
degrades to "no new checkpoints / no rebalance" (liveness), never a data-plane outage — same
as Flink/Arroyo. Run the control Postgres HA (RDS Multi-AZ / Patroni).

**Foundation work (lands with/before Phase 0, since authority fencing is the first consumer):**
introduce the control-store client + schema (lease, `assignments`, `checkpoint_epochs`, recovery pointer),
a fenced **leader lease** replacing lowest-live-id (`control/leader.rs:8`), and migrate the
authoritative `AssignmentSnapshot` from object-store CAS to Postgres rows (keep the CAS fence
as backstop). Make Postgres optional/disabled for single-instance mode.

---

## Phases

### Phase 0 — Authority and transport safety
- Term-fence durable per-node recovery-control records, preserve the global recovery generation
  across all process terms, and fail unknown gossip lifecycle state closed.
- Retain TLS identity, assignment/recovery fences, bounded byte admission, sequence-gap detection,
  and barrier high-water checks under link-loss and restart fault injection.
- Use one certified, sequenced data/barrier FIFO per peer with bounded queue, byte reservation,
  decode, and holdover admission. Barrier control bytes have separate admission without leaving
  that FIFO. Cluster subscriptions fail closed; local subscriptions never enter shuffle.
- Make shuffle slicing reject invalid owner/vnode inputs and prove row-count conservation. Reject
  reserved protocol field and stage names before execution.
- **Exit:** delayed writes from an expired process term cannot replace successor state, saturated
  data saturation cannot starve checkpoint barriers, no local enqueue is treated as remote delivery,
  and every detected loss rejects the cut before state is sealed.

### Phase 1 — Bounded state capture and async durable tail
- Add live-state byte accounting and reserve before mutation/capture so the configured bound is
  enforced before allocation, not after serialization.
- Replace synchronous full-state scans during the aligned pause with a bounded immutable/COW
  generation freeze. The stopped phase may drain and pin state but performs no remote I/O,
  compaction, full-state iteration, or encoding.
- Encode and upload in bounded cancellable chunks after processing resumes; publish immutable
  per-vnode handles through the existing exact-attempt seal/decision protocol. Delta/rebase policy
  is internal, not a public checkpoint dimension.
- **Exit:** event-loop heartbeat and ingestion p99 remain within target during large captures, RSS
  stays inside the configured budget, and kill/cancel before and after freeze/upload/seal cannot
  publish a partial cut.

### Phase 2 — Common keyed state, aggregate vertical slice
- Add the smallest batched API needed by operators: multi-get, ordered vnode/range scan, atomic
  write batch, bounded generation freeze, async materialize, restore, and drop-vnodes.
- Provide an in-memory reference implementation and one local production LSM implementation.
  Namespace state by deployment, pipeline fingerprint, stable operator/table/schema identity,
  vnode, and user key. Assignment generation and process incarnation are write fences and
  checkpoint provenance, not key components. Do one read/write batch per Arrow batch, never one
  await per row.
- Move grouped aggregate accumulators and `last_emitted` into one atomic keyed-state batch. Keep
  the old map path only as a differential oracle during the cycle, then delete it.
- **Exit:** output/state parity, crash/reopen, corruption, memory-bound, and 1→3→2 ownership tests
  pass in embedded, single-node, and cluster modes.

### Phase 3 — Partition-aware source ownership
- Use engine-owned split enumeration and assignment for Kafka and database snapshot/log splits;
  checkpoint exact per-split cursors and bind handoff to the same assignment/state handle.
- Add dynamic split discovery and cross-node watermark alignment. Gossip advertises liveness only;
  the durable control store owns assignment generations.
- **Exit:** nodes consume disjoint splits, owner death resumes from the decided cursor, and
  snapshot/log handoff plus watermark frontiers pass process-death integration tests.

### Phase 4 — Windows and timers
- Implement fixed tumbling/hopping windows over keyed tables and timer/range indexes; consolidate
  duplicated whole-node window state. Add session-window interval merging only after fixed-window
  range and eviction semantics pass.
- **Exit:** every admitted window snapshots, restores, and revokes per vnode; all other window
  shapes remain rejected in cluster mode.

### Phase 5 — Bounded joins and other stateful operators
- Implement bounded interval joins as two vnode-keyed time-indexed tables with watermark eviction,
  then incremental changelog joins as two atomic keyed multisets. Require co-partitioned shuffle.
- Process-time/unbounded joins require an explicit finite retention contract or remain rejected.
  Classify every operator as stateless, rebuildable read-only, vnode-keyed, or unsupported.
- **Exit:** future operators cannot enter a cluster graph without an explicit state/shuffle
  capability, and each admitted shape passes rescale/recovery/output-oracle tests.

### Phase 6 — Elastic lifecycle, authority, and operations
- Validate changing node counts, graceful drain, rolling upgrade, skew, owner death, and assignment
  rotation mid-checkpoint. Acquired state must restore before Active; revoked owners stop writes
  before dropping state.
- Complete connector handoff and external committer fencing before removing `LDB-0013`. Add per-stage
  shuffle fairness, drain/in-flight metrics, adaptive buffer targets, and recovery/state-cut SLOs.
- **Exit:** the cluster is operable under the full fault/soak matrix; shared object-backed live
  state remains deferred unless a version/compaction/GC metadata authority is proven.

---

## Sequencing & risk

- **Stateful operator path:** AD-0 → 0 → 1 → 2 → 4 → 5. **Clustered source-handoff path:**
  AD-0 → 0 → 1 → 2 → 3. Phase 6 certifies both under lifecycle faults; work on source ownership may
  proceed in parallel but cannot bypass the state-handle cut.
- **Phase 2 is the largest architectural change.** Phase 1 first establishes truthful memory and
  pause bounds so the new engine cannot inherit an unsafe capture path.
- **Phase 1's aligned cut is necessary but not sufficient** for cluster exactly-once; the
  `LDB-0013` leader-term and external-publication fence remains the admission gate.
- **Reuse the proven control/data-plane substrate**, but do not preserve operator-owned map and
  synchronous snapshot paths after their replacement passes differential validation.

## Open questions

- Whether measured alignment backpressure justifies unaligned checkpointing.
- Co-partitioning policy: should source-partition assignment be forced to align with vnode
  ownership (so a node consumes the partitions whose keys it owns), eliminating a reshuffle
  hop for keyed pipelines?
- Target cluster size / cloud + storage backend (S3?) / SLA — sizes the rescale and
  checkpoint-frequency design.
