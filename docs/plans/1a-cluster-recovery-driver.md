# 1A-cluster: leader-coordinated recovery driver (global restart-to-epoch)

Make a **fatal cycle error on one cluster node** recover correctly instead of corrupting the
distributed cut. Single-node 1A (`92fdf9d8`) recovers one node from its last checkpoint; in a
cluster with cross-node shuffle a local-only restart rewinds the faulted node to committed epoch
N while peers have produced/consumed shuffle rows for N+1 → lost/dup in-flight rows. This is
Flink's *global* recovery: drive every node back to one consistent epoch and reprocess.

Status: **NOT built.** A first attempt (reverted) hand-rolled a gossip-KV polling protocol with a
per-node fence and per-node target computation — it had a real divergence bug and no soak. This
plan is the correct approach. Build it only with the soak budget below; do not merge before the
soak is green.

## Semantics

On a fatal fault: all nodes rewind to N = highest epoch committed by **every** participant, and
reprocess. N is durable/restorable; epochs > N were never committed, so reprocessing is
exactly-once-safe (committed sinks at N are the truth; the EO 2PC dedups the re-emit). Recovery is
EO **only for replayable sources** (re-seek to N's offsets; `supports_replay`) and transactional /
idempotent-keyed sinks; a non-replayable source loses data on rewind and a non-transactional sink
re-emits duplicates — state these as the semantic boundary.

**Latency tradeoff (be honest about it).** Global recovery is a *full-cluster processing pause* on
every fault. Flink moved off this to **pipelined-region failover** as the default in 1.9 (only the
tasks in the failed pipelined region restart). The reason global is an acceptable **v1** here:
- For a single job with a **cross-node keyed shuffle, the pipelined region spans the whole job**, so
  region ≈ global anyway (RisingWave and Arroyo both recover the whole job from the last barrier).
- Regional failover's real win is **multi-query / embarrassingly-parallel** pipelines, where one
  query's fault must not pause the others — that is exactly the **1B blast-radius** work, which
  becomes *required* (not "later optional") to avoid a multi-tenant latency regression. Sequence 1B
  as the near-term follow-up, and adopt Flink's region model: restart the smallest set of
  pipeline-connected tasks, not the whole cluster.

## Correctness invariants (the attempt got these wrong — get them right)

1. **One target epoch, chosen once, broadcast explicitly.** Every node must rewind to the SAME N.
   Do NOT have each node read `highest_committed` independently — between the trigger and a node
   observing it, an in-flight checkpoint commit or a leader change advances `highest_committed`, so
   nodes diverge to different epochs → inconsistent cut. The leader fixes N at decision time and
   broadcasts the value.
2. **Cluster-wide fence before N is read, released only after all nodes restored.** Hold barrier
   injection from the moment recovery is decided until every live node acks restored-to-N. A
   per-node-local fence is wrong: leadership can change mid-recovery and the new leader's local flag
   is unset. Tie the fence to the recovery phase observed cluster-wide, not a local atomic.
3. **Idempotent + monotonic.** A re-fault during recovery, a node that was down at announce time,
   and duplicate observes must all converge. Track applied recovery by a monotonic id and the
   target epoch, both carried in the announcement.

## Design — reuse the existing control plane, don't hand-roll

The codebase already has `BarrierCoordinator` (leader `announce` → follower `observe`/`ack` →
quorum, over gRPC with gossip-KV fallback) and a per-epoch 2PC decision store. Recovery is
structurally the same announce/ack/quorum. Reuse it; do not build a parallel KV-polling protocol.

1. **`Phase::Recover` on the existing announcement.** Add `Recover` to `Phase`
   (`cluster/control/barrier.rs`) — a unit variant, so `Phase` stays `Copy`. Carry the target in
   the existing `BarrierAnnouncement.epoch`. Leader announces
   `{ epoch: N, phase: Recover, checkpoint_id }`; followers `observe` it (event-driven via the
   existing gRPC watch + gossip fallback — no polling).
2. **Leader decision.** On a fault report, the leader: stops injecting periodic barriers (fence),
   picks N = `CheckpointDecisionStore::highest_committed()` (re-add — the 2PC marker is written
   only after full-membership quorum, so its presence = committed cluster-wide), and announces
   `Recover(N)`. N is the fixed broadcast value.
3. **Per-node restore.** On observing `Recover(N)`: `set_recover_target_epoch(N)` → stop pipeline →
   `start()` → `recover_to_epoch(N)` (re-add these two small primitives — they were correct). Then
   ack restored-to-N via the existing `BarrierAck` path.
4. **Fence release.** Leader waits for the restored-to-N quorum (reuse `await_*_quorum`), then
   resumes barrier injection. The existing checkpoint gate stays held while `phase == Recover` is
   the latest observed announcement.
5. **Fault report.** A node hitting `ExitReason::Fault` in cluster mode does not locally restart
   (suppress the supervisor when coordinated recovery is on); it signals the leader via the same
   control plane (a fault ack / a `BarrierAck { ok: false }`-style signal). On leader fault, the
   new gossip-elected leader drives recovery.
6. **Guards.** Cluster-scoped restart budget (reuse `RestartPolicy` semantics) → escalate to a
   cluster-wide hard-fault surfaced in `/metrics` + status. Bound the restored-to-N wait.

Config: `[coordination] coordinated_recovery` (default off until soak-proven), wired
builder → server, same staged rollout the delta work used.

## Files

- `crates/laminar-core/src/cluster/control/barrier.rs`: `Phase::Recover`; leader announce + the
  restored-to-N ack/quorum (extend the existing announce/observe/ack, don't duplicate).
- `crates/laminar-core/src/checkpoint_decision.rs`: re-add `highest_committed`.
- `crates/laminar-db/src/recovery_manager.rs` + `checkpoint_coordinator.rs`: re-add
  `recover_to_epoch`.
- `crates/laminar-db/src/db.rs` + `pipeline_lifecycle.rs`: `recover_target_epoch` +
  `set_recover_target_epoch`; consume in `start_inner`; route `ExitReason::Fault` to the
  report-and-await path in cluster mode (suppress the local supervisor).
- `crates/laminar-db/src/pipeline_callback.rs`: checkpoint gate also holds while a recovery phase
  is in flight (reuse the controller's recovery state, derived from the observed phase — not a
  free-floating local atomic).
- `crates/laminar-server/...`: `coordinated_recovery` config + flag wiring.

## Soak validation (the acceptance gate — merge only when green)

Tooling (the reverted attempt's harness pieces were right — rebuild them):
- **Fault injector**: `#[cfg(debug_assertions)]` one-shot in `execute_cycle`, armed by
  `LAMINAR_FAULT_INJECT_AFTER_MS` (per-process, so the soak arms exactly one node). Inert in release.
- **Soak knobs** in `cluster_soak.rs`: `LAMINAR_SOAK_COORD_RECOVERY` (sets the config flag),
  `LAMINAR_SOAK_FAULT_INJECT_MS` (arms node 1). Reuse `three_node_kill9_soak` with
  `LAMINAR_SOAK_KILLS=0` so the injected fault is the only disruption.
- The pipeline must be **ExactlyOnce** for the fault to become `ExitReason::Fault`, so run with the
  Kafka EO sink (`LAMINAR_SOAK_KAFKA_BROKERS=127.0.0.1:29092`); the existing
  `verify_exactly_once_output` is the gap/dup proof.

Acceptance — all green:
1. **Follower fault**: node 1 faults mid-stream → cluster coordinates restart-to-N → commits resume
   AND every node's EO topic stays dense (no gap, no duplicate).
2. **Leader fault**: arm the fault on the leader (node 0) → new leader drives recovery → same EO
   assertion.
3. **Fault during rebalance churn**: combine with a membership change → no deadlock, EO holds.
4. Repeat each several rounds; epochs never regress; killed/faulted nodes rejoin.

Run the harness via `laminardb-test-harness/docker/compose` (redpanda `127.0.0.1:29092`); recreate
redpanda (down/up, not restart) if degraded. Build needs the OpenSSL env (rdkafka dev-dep). Build
one cargo invocation at a time and watch memory — the full server build links a large binary.

## Recovery latency (RTO) — the dimension that matters for "real-time"

The plan above gets *correctness*; this is what makes it *low-latency*. RTO ≈ fault-detect +
leader-coordinate + state-reload + reprocess-from-N. Attack each:

- **Don't rebuild state — read it from shared storage.** The engine already writes vnode partials
  and checkpoints to a shared object store, so a coordinated restart should reload epoch N from
  there (RisingWave's model: recovery reads the last consistent SSTable set from object storage with
  no upload/rebuild). Flink 2.0's disaggregated state reports **up to 49× faster recovery** precisely
  because state isn't re-materialized over the network.
- **Reuse local state when the node still has it.** If a restarting node already holds epoch N's
  state on local disk/cache, skip the object-store read (Flink *task-local recovery*). Biggest RTO
  lever; the current `db.stop()`/`db.start()` re-reads from the store unconditionally.
- **Avoid full graph teardown.** `stop()`/`start()` rebuilds the operator graph and reopens sources.
  The latency-optimized path resets operators to N and resumes in place (Flink/RisingWave do this).
  Acceptable to ship the full-restart path first, but track in-place reset as the RTO follow-up.
- **Small rewind via frequent + cheap checkpoints.** Reprocess time = (now − N). The 100ms floor +
  delta checkpoints already bound this; keep N close to head.
- **Leader-election cost.** When the *leader* faults, RTO includes gossip re-election (phi-accrual,
  ~seconds). Fold this into the budget; the leader-fault soak variant must measure it.
- **Coordinate processing resumption, not just checkpointing.** The fence holds barrier injection,
  but `start()` resumes source reads + cross-node shuffle immediately, so a node can shuffle to a
  peer still restarting. This is already self-healed by the existing **`ShuffleNotReady` deferral +
  convergence gate (`0bccbe14`)** — the shuffle defers until the peer is ready. Credit that reuse
  explicitly; it's why staggered restart is safe without a second processing-level fence.

Set an explicit **RTO target** (e.g. p99 < N seconds) and assert it in the soak.

## 2026 state of the art & research roadmap

Techniques to adopt (in roughly priority order), each tied to this engine. These improve checkpoint
latency, recovery time, and state cost — and several directly shrink the recovery rewind window.

**Checkpoint latency**
- **Unaligned checkpoints** (Flink FLIP-76): under backpressure/skew, aligned barriers block the
  fast channels until the slow one's barrier arrives, adding ≥ `t_align` to end-to-end latency and
  to checkpoint duration. Unaligned lets the barrier overtake records and snapshots in-flight data
  instead of waiting (EO, one concurrent checkpoint). This engine uses **aligned cross-node
  barriers**, so it pays exactly this stall — the single biggest checkpoint-latency win available,
  and it shrinks the rewind window (smaller N→head). Pair with **buffer debloating** (FLIP-183) to
  cut in-flight bytes so even aligned checkpoints (and unaligned snapshot size) stay small.
- **Generic log-based incremental checkpoints** (Flink FLIP-158): continuously upload a state
  changelog and materialize the base infrequently, so each checkpoint's async phase flushes only a
  small, stable delta → faster, more *stable* checkpoints → more frequent EO 2PC commits → lower
  end-to-end latency and *less replay on failover*. The engine's **delta checkpoints (Lever 2)** are
  the same idea; extending them (changelog for all operators, not just agg) is the path.

**Cluster recovery**
- **Pipelined-region failover** (Flink FLIP-1, default since 1.9): restart only the smallest
  pipeline-connected region. This *is* the 1B work — promote it from "later" to near-term.
- **Task-local recovery** (Flink): reuse on-node state to avoid DFS reload (the RTO lever above).
- **Approximate task-local recovery** (Flink FLIP-135): restart only the failed task, trading
  consistency for speed — an *at-least-once* fast path for non-EO pipelines; offer as an opt-in mode.
- **Recovery without state rebuild** (RisingWave Hummock): because state is already in object
  storage, restart reads the last consistent set rather than re-shuffling/rebuilding — the model the
  coordinated restart should follow here.

**State management**
- **Disaggregated state** (Flink 2.0 *ForSt*, VLDB 2025): DFS-primary state with local cache +
  **async state access** (State V2) to hide remote latency; reported **up to 94% shorter checkpoint
  duration, up to 49× faster recovery/rescale, ~50% cost**. This is the strategic direction for
  cloud-native recovery + elastic rescale, and it makes global restart-to-epoch cheap.
- **Cloud-native LSM state on object store** (RisingWave Hummock): multi-tier (memory/disk/object)
  LSM, append-only writes, per-barrier snapshot = natural MVCC, horizontal scale **without shuffling
  state**, sub-100ms latency over S3. The engine's **ADR-005 tiered state** + shared object-store
  backend are steps toward this; aligning the state layout with an LSM/SSTable + manifest model
  would unlock no-rebuild recovery and zero-copy rescale.
- **Async, non-blocking state access**: required to make remote/disaggregated state viable on the
  hot path (Flink 2.0 redesigned its operators for this). Relevant if state moves off-heap/remote.

Recent production resiliency work to track: *StreamShield* (Fang et al., ByteDance,
[arXiv:2602.03189](https://arxiv.org/abs/2602.03189), 2026) — production Flink resiliency at scale
via fine-grained fault-tolerance, a hybrid replication strategy, and HA under external systems;
maps onto the regional-failover and state-redundancy directions above.

Sequencing recommendation: (1) this driver as correct global v1; (2) unaligned + buffer debloating
(checkpoint latency, smaller rewind); (3) task-local / no-rebuild recovery (RTO); (4) regional
failover = 1B (multi-query latency isolation); (5) disaggregated state (cloud-native recovery +
rescale) as the larger strategic bet.

## Anti-patterns to avoid (from the reverted attempt)

- No parallel gossip-KV polling protocol — reuse `BarrierCoordinator` announce/ack/quorum.
- No per-node independent `highest_committed` read — broadcast one explicit target N.
- No per-node-local fence for a cluster-wide decision — tie the fence to the observed phase.
- No merge before the soak is green — unsoaked distributed recovery is the false-confidence trap.
