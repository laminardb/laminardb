# Plan: Cluster Mode Production Readiness (Distributed, Partition-Aware, Exactly-Once)

- **Status:** Proposed (not started). Large multi-phase program.
- **Date:** 2026-05-28
- **Scope:** Make `cluster` a production-grade distributed streaming runtime:
  partition-aware sources (Kafka first), end-to-end exactly-once across the distributed
  dataflow, fault-tolerant state with failover, then elastic rescale and operational
  hardening.
- **Relationship to other plans:** This is a **prerequisite for**
  `docs/plans/lookup-source-production-mpp.md` Track B (distributed joins) — distributed
  non-aggregation operators (Phase 4 here) and barrier alignment (Phase 2) are shared
  foundations. The lookup-MPP single-node track (Track A) does **not** depend on this and
  can proceed in parallel.
- **Targets (grounded in Flink / RisingWave / Arroyo, decided 2026-05-28):**
  - **Correctness:** end-to-end exactly-once via aligned Chandy-Lamport barriers + the
    existing cross-node 2PC sink commit. (Unaligned checkpoints noted as a follow-up for
    backpressure resilience — Flink's lesson.)
  - **Sources:** engine-controlled split assignment (not Kafka consumer groups), per-split
    offsets in the checkpoint, dynamic partition discovery, cross-node watermark alignment.
  - **Elasticity:** vnode (256, fixed logical partitions) + object-store state; **failover
    first** (reload a dead node's vnodes onto survivors), then **elastic rescale** as the
    same machinery with a changing node count. This is the RisingWave model on the substrate
    LaminarDB already has.
  - **Coordination (AD-0, settled):** hybrid — gossip for liveness, object-store CAS for
    fencing, **Postgres** as the authoritative control store (fenced leader lease, assignment,
    commit verdict, recovery epoch). See AD-0 below.

---

## How the reference systems do this (rationale)

All three converge, and LaminarDB is architecturally closest to RisingWave:

- **Correctness:** ABS aligned barriers + 2PC sinks (Flink, RW, Arroyo). Flink adds
  *unaligned* checkpoints so alignment can't stall under backpressure.
- **Partition-aware sources:** a central enumerator/assigner (Flink JobManager
  `SplitEnumerator`, RW meta, Arroyo controller) maps source splits → parallel reader
  subtasks via the engine's own assignment, using manual `assign()` — **not** Kafka consumer
  groups — so partition→subtask mapping is deterministic and consistent with checkpointed
  per-split offsets. Plus dynamic discovery and watermark alignment (Flink FLIP-182).
- **Elasticity:** fixed logical partitions (Flink key groups; RW + LaminarDB vnodes)
  decoupled from physical parallelism, state in DFS/S3, rescale = reassign vnode ranges +
  reload from shared storage. **Failover and rescale are the same primitive.** RW is most
  elastic because state is in shared Hummock/S3 and reassignment rides a barrier.

LaminarDB already has the enabling primitives (vnodes, `object_store` state backend, CAS
fencing, barrier/2PC machinery); the gaps below are wiring, not missing architecture.

## Current state (audited 2026-05-28, `cluster`)

**Real and tested:** chitchat gossip membership + phi-accrual failure detection
(`crates/laminar-core/src/cluster/discovery/gossip_discovery.rs:253`); weak-by-design leader
= lowest live id (`control/leader.rs:8`); **enforced split-brain fencing** via
`assignment_version` gating object-store writes (`state/object_store.rs:104-123`) and
committer-id CAS on `_COMMIT` (`:201-211`); CAS'd versioned `AssignmentSnapshot`
(`control/snapshot.rs:189`) watched + rotated on membership change
(`crates/laminar-db/src/rebalance.rs:100-153`); **cross-node 2PC commit decision**
(`checkpoint_coordinator.rs:1066-1077`, `control/barrier.rs:215-278`); **distributed
aggregation** via a pre-agg row-shuffle bridge (`operator/sql_query.rs:450-538`, wired at
`pipeline_lifecycle.rs:485`); shuffle transport with reconnect (`shuffle/transport.rs:165`).

**Stubbed / missing (the work):**
- **Partition-aware source distribution is not implemented.** Source tasks are cluster-blind
  (`pipeline/streaming_coordinator.rs:317-367`); Kafka uses `subscribe()`/consumer groups
  (`kafka/source.rs:631-655`), disconnected from membership/vnodes/fencing/checkpoints. No
  node→partition assignment. `TODO(distributed): embed the lease epoch` (`kafka/sink_config.rs:58`).
- **No barrier alignment across the shuffle.** `BarrierTracker` exists but is unused in the
  db checkpoint path (`shuffle/mod.rs:9` only); followers checkpoint on a *gossip
  announcement* (`pipeline_callback.rs:239-242`), not an aligned data-plane barrier → the
  cut does not capture in-flight shuffle records. Exactly-once with the shuffle active is not
  actually correct today.
- **State is not redistributable.** Per-vnode `partial.bin` is a marker (`"ckpt:{id}"`,
  `checkpoint_coordinator.rs:587`), not state; `read_partial` is dead in prod; manifests are
  per-node (no global manifest / `instance_id`). A dead node's vnodes/state are **dropped**
  (asserted current behavior, `tests/cluster_e2e_failures.rs:205`); MV catalog restore across
  the cluster is **unimplemented** (`:288-293`).
- **No rescale.** `round_robin_assignment` reshuffles *all* vnodes on join/leave
  (`state/vnode.rs:206-231`), and with no state reload, recovers nothing.
- **Only aggregation distributes**; joins/windows/distinct are single-node.
  `ClusterRepartitionExec` is **dead/test-only** (`cluster.rs:455-457`).
- **Transport fragile:** no connect/write timeouts; an unreachable peer **fails the whole
  cycle** (`sql_query.rs:516`); no cross-node backpressure to sources.
- **No inter-node security:** plaintext, unauthenticated `Hello(node_id)` (`transport.rs:211`).
- **No graceful drain / rolling upgrade / scale-down** (`Draining` only logged,
  `server/cluster.rs:107`); admin API read-only (`http.rs:390-417`).
- **No Raft:** `raft_port` is config theatre (`cluster_config.rs:150`, `cluster.rs:237-240`);
  stale `Cargo.toml:14-16` comment claims rebalance "not yet implemented" (it is).

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
- **No zero-downtime live repartition.** None of Flink/RW/Arroyo do that; rescale is
  barrier-coordinated reassignment + reload (brief reconfiguration), which is the target.
- **No multi-region / geo-replication** in this plan.
- **No bespoke RPC framework** — reuse the existing length-prefixed shuffle transport +
  object store + gossip; add TLS via the existing rustls stack used by pgwire.
- **No new state backend** — extend the existing `object_store` backend to carry real
  per-vnode state; do not introduce complex embedded/external LSM databases.

## AD-0 — Coordination store (SETTLED 2026-05-28: Hybrid + Postgres)

**Decision:** Adopt the **hybrid model (Option C)** with **Postgres** as the authoritative
control store. Responsibilities split by what each layer is good at:

- **Gossip (chitchat) keeps membership + failure detection** — fast liveness; never put a
  store round-trip on the liveness path. Unchanged from today.
- **Object-store CAS keeps fencing** (`assignment_version` gate + `_COMMIT` committer-id CAS)
  as defense-in-depth — the "even if the control plane has a bug, committed state cannot be
  corrupted" backstop. Retained, not removed.
- **Postgres becomes authoritative for:** the **fenced leader lease** (advisory lock / lease
  row → prevents the dual-leader window the current lowest-id model only *tolerates*), the
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

**Foundation work (lands with/before Phase 1, since P1 is the first consumer):** introduce the
control-store client + schema (lease, `assignments`, `checkpoint_epochs`, recovery pointer),
a fenced **leader lease** replacing lowest-live-id (`control/leader.rs:8`), and migrate the
authoritative `AssignmentSnapshot` from object-store CAS to Postgres rows (keep the CAS fence
as backstop). Make Postgres optional/disabled for single-instance mode.

---

## Phases

### Phase 0 — Transport & safety hardening (small, unblocks everything)
- Add connect/write **timeouts** to the shuffle transport (`shuffle/transport.rs`); today a
  blocking `send_to` can hang forever.
- Peer-failure handling: an unreachable peer must **backpressure / trigger reassignment**,
  not fail the whole cycle (`sql_query.rs:516`). Distinguish transient (retry) from durable
  (rebalance) peer loss.
- **Inter-node TLS + authenticated handshake** on shuffle + control (reuse the pgwire rustls
  work); reject `Hello` from unauthenticated/mismatched ids.
- **Exit:** a node can crash or a link can blip without killing the cluster; all inter-node
  traffic is encrypted + authenticated.

### Phase 1 — Partition-aware source assignment (Kafka) — the headline
- Introduce an engine-side **split enumerator + assigner**: enumerate Kafka partitions,
  assign them to nodes via the **Postgres control store** (AD-0; co-partition with vnode
  ownership where the key hashes align), and switch the Kafka source from `subscribe()` to
  manual **`assign()`** of only its owned partitions. (Requires the AD-0 control-store
  foundation.)
- Per-split offsets become part of the engine checkpoint (already per-partition,
  `kafka/offsets.rs:20`); on reassignment the new owner resumes from the checkpointed offset.
- Reuse the **rebalance controller** (`rebalance.rs`) to reassign splits on membership change
  (cooperative: only moved partitions pause); handle **dynamic partition discovery** and
  topic repartitioning.
- **Cross-node watermark alignment:** extend the leader's cluster-min watermark to gate fast
  partitions (Flink FLIP-182 style) + idle-partition detection across nodes.
- **Exit:** N nodes consume disjoint Kafka partitions assigned by the engine; node loss
  reassigns its partitions to survivors which resume at the correct offsets; watermarks align
  across the cluster. Covered by a real Kafka cluster integration test (none exists today).

### Phase 2 — Consistent distributed checkpoint (correctness foundation)
- Wire `BarrierTracker` into the db checkpoint path so checkpoint barriers **align across the
  shuffle** (true Chandy-Lamport cut): each shuffle-input operator snapshots only after the
  barrier arrives on all input channels, buffering post-barrier records.
- Decide in-flight handling: **aligned** first (buffer); add **unaligned** (snapshot in-flight
  shuffle data) as a follow-up so alignment can't stall under backpressure.
- Replace "follower checkpoints on gossip announcement" with barrier-driven snapshotting.
- **Exit:** exactly-once holds with the shuffle active (distributed agg today, joins later);
  verified by a fault-injection test that asserts no double-count on failover.

### Phase 3 — Durable partitioned state + reload-on-failover (the big one)
- Write **real per-vnode operator/keyed state** to the `object_store` backend (replace the
  `partial.bin` marker), keyed by `(checkpoint_id, vnode)`.
- On failover/rebalance, a node adopting a vnode **loads that vnode's state** from shared
  storage (resurrect `read_partial` into the adopt/recovery path; today it's dead).
- Implement **cluster MV catalog restore** across nodes (currently unimplemented,
  `cluster_e2e_failures.rs:288`).
- **Cluster-wide recovery epoch agreement** so all nodes recover to a consistent global
  checkpoint (via the Postgres control store, AD-0).
- **Exit:** killing a node drops **zero** committed state; a survivor adopts its vnodes and
  resumes. The `cluster_e2e_failures.rs:205` "rows dropped" assertion flips to "rows
  recovered."

### Phase 4 — Distributed non-aggregation operators (converges with lookup-MPP Track B)
- Extend the shuffle bridge beyond aggregation: hash-partition keyed joins, windows, distinct,
  topk by key across vnodes, with **co-partition enforcement** (both join sides shuffled by the
  join key). This is the same machinery as `lookup-source-production-mpp.md` Phase 5
  (key-shard the lookup probe side).
- **Exit:** a keyed stream-stream join / windowed aggregation / distinct runs distributed,
  barrier-consistent (Phase 2), state-durable (Phase 3).

### Phase 5 — Elastic rescale + lifecycle
- Extend the failover machinery (Phase 3) to **changing node counts (N→M)**: barrier-coordinated
  vnode reassignment + reload from object store (RW model). Move toward consistent-hash-style
  vnode mapping so a join/leave moves a bounded fraction (today `round_robin` reshuffles all,
  `vnode.rs:206`).
- **Graceful drain** (act on `NodeState::Draining`, not just log it), **rolling upgrade**,
  scale-down.
- **Admin API** for cluster ops (drain / trigger rebalance / scale / view assignments),
  replacing the read-only endpoint (`http.rs:390`).
- **Exit:** add/remove nodes with state preserved; drain a node for upgrade without data loss.

### Phase 6 — Recovery coordination & observability
- Per-node + cluster metrics: per-partition lag, assignment map visibility, checkpoint
  alignment health, shuffle queue depth, barrier latency, fence rejections.
- Runbooks; correct the misleading `raft` config surface and the stale `Cargo.toml` comment.
- **Exit:** the cluster is operable and observable under production load.

---

## Sequencing & risk

- **Critical path:** 0 → 1 (headline) and 0 → 2 → 3 can proceed somewhat in parallel; 4
  depends on 2+3; 5 depends on 3; 6 throughout.
- **Phase 1** delivers the user's explicit ask (partition-aware Kafka) early and is buildable
  on the existing assignment/rebalance substrate without waiting for full distributed-state
  correctness — sources' "state" is mostly offsets.
- **Phase 3 is the largest and riskiest** (real partitioned state movement); it gates true
  fault tolerance and all elasticity. AD-0 must be settled before it.
- **Phase 2 is correctness-critical**: until it lands, distributed-agg exactly-once is
  nominal, not real.
- **Reuse over rebuild:** every phase extends proven substrate (vnodes, AssignmentSnapshot,
  CAS fence, 2PC, shuffle transport) rather than introducing new infrastructure — this is the
  RisingWave model on what LaminarDB already has.

## Open questions

- Aligned-only vs aligned+unaligned checkpoints for v1 (backpressure tolerance).
- Co-partitioning policy: should source-partition assignment be forced to align with vnode
  ownership (so a node consumes the partitions whose keys it owns), eliminating a reshuffle
  hop for keyed pipelines?
- Target cluster size / cloud + storage backend (S3?) / SLA — sizes the rescale and
  checkpoint-frequency design.
