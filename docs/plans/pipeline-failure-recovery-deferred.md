# Pipeline failure recovery — deferred design (post-1A)

Single-node 1A landed in `feat(db): recover from a fatal cycle error (single-node 1A)`
(`92fdf9d8`): a fatal `execute_cycle` error under exactly-once now faults and recovers from
the last checkpoint, with an opt-in auto-restart supervisor + configurable `RestartPolicy`.

This document is the detailed, actionable plan for the four items the architecture plan
(`docs/plans/pipeline-failure-recovery-architecture.md`) deliberately deferred. Sequencing:
**Note 3 → Note 2** (small, independent, do first), then **1B** (after 1A, before cluster),
then **1A-cluster** (largest, depends on all of the above).

All file:line anchors verified against the tree at `92fdf9d8`.

---

## Status — what landed in the follow-up pass

- **Note 3 + Note 2 — DONE.** Note 2 (event-driven convergence gate) subsumes Note 3:
  the leader's checkpoint-convergence verdict is computed off the hot path by the
  snapshot watcher and published through a `watch` channel
  (`ClusterController::publish_converged` / `converged_watch`); the checkpoint gate is
  now a local borrow (`assignment_ready_for_checkpoint` → `converged_rx.borrow()`) with
  no per-checkpoint gossip scan. De-conflation falls out for free — the gate no longer
  bumps `last_checkpoint` on a convergence skip, so the first post-convergence checkpoint
  fires immediately. The throttle field Note 3 proposed was never needed.

- **1B — foundation DONE; full isolation deferred (corrected to L).** Recovery domains
  (weakly-connected components *including* sources — independent queries are disjoint
  sub-pipelines, so a global-epoch checkpoint is the union of per-domain consistent cuts)
  are computed in `compute_topo_order` (`node_domain` / `domain_of` / `failure_domain_count`)
  and wired into fatal-error blast-radius attribution. **Correction to the plan:** the
  "exclude sources" model creates a shared-source offset inconsistency (option-b dups a
  sibling's rows on replay); the *including-sources* model avoids it. Full fatal
  cross-domain isolation (let healthy domains commit while a faulted domain recovers
  alone) requires **in-flight per-domain recovery** — selective graph restore + per-source
  re-seek — neither of which exists (recovery is whole-pipeline stop+start; sources consume
  `restore_checkpoint` only at spawn). That is an **L**, not the **M** estimated here, and
  is the remaining work for 1B.

- **1A-cluster — foundation DONE; distributed driver deferred (soak-gated).** The
  deterministic, unit-tested mechanisms landed: leader epoch selector
  (`CheckpointDecisionStore::highest_committed`, exposed as `LaminarDB::cluster_recovery_target`),
  per-node recover-to-target (`RecoveryManager::recover_to_epoch` →
  `CheckpointCoordinator::recover_to_epoch`), and the node-side restart hook
  (`LaminarDB::set_recover_target_epoch` + `recover_target_epoch` consumed in `start_inner`).
  The remaining piece is the **distributed driver**: fault-report → leader decision →
  recover-directive broadcast → every node `set_recover_target_epoch` + coordinated restart
  → barrier-rearm fence (reusing the Note 2 convergence signal). It is implementable on
  these primitives but **gated on multi-node kill-9 soak** (depth > 1 cross-node shuffle),
  which the local environment cannot run reliably — so it is intentionally not yet wired
  into a live path.

---

## Note 3 — De-conflate the convergence-gate throttle. (XS, do first)

### Problem
`StreamingCoordinator::maybe_checkpoint` gates the periodic checkpoint on cluster
convergence. On a quiesce-skip it does `self.last_checkpoint = Instant::now()`
(`pipeline/streaming_coordinator.rs:939`) purely to throttle the gossip read behind the
gate to once per interval. This **conflates "checkpointed" with "skipped"**: a long
convergence stall keeps bumping `last_checkpoint`, so the interval clock is meaningless
during churn and the first post-convergence checkpoint is delayed by a full extra interval.

### Design
Add a dedicated `last_convergence_check: Instant` field to `StreamingCoordinator` and throttle
the gossip read on that, leaving `last_checkpoint` to mean only "a checkpoint committed".

### Steps
1. `pipeline/streaming_coordinator.rs`: add `last_convergence_check: Instant` to the struct +
   constructor + the two test struct literals.
2. In the convergence gate (around `:937-940`), replace the `last_checkpoint = now()`
   throttle-bump with `last_convergence_check = now()`, and guard the
   `assignment_ready_for_checkpoint().await` call behind
   `last_convergence_check.elapsed() >= interval` so the gossip read still runs at most
   once/interval.

### Effort/risk
XS / low. Pure throttle bookkeeping; no semantic change to when checkpoints commit.

### Validation
Existing cluster checkpoint tests stay green; assert (unit) that a simulated convergence
stall does not advance `last_checkpoint`. Folds naturally into Note 2 (which removes the
read entirely) — land Note 3 first so Note 2 is a clean deletion.

---

## Note 2 — Event-driven convergence gate (remove the per-interval gossip poll). (S)

### Problem
`ConnectorPipelineCallback::assignment_ready_for_checkpoint` (`pipeline_callback.rs:1502`)
calls `cc.read_adopted_versions().await` (`controller.rs:366`) — a gossip KV scan — every
time the leader's checkpoint gate is consulted. This shares the chitchat lock with the
barrier-alignment task; the architecture plan's don't-repeat list explicitly warns that
per-cycle/per-barrier gossip reads regress alignment timeouts. Even throttled (Note 3) it's
a hot-path gossip read on the checkpoint cadence.

### Design
Compute convergence **once, off the hot path**, in the snapshot watcher that already runs on
every node, and publish it through a `watch` channel the gate reads locally (zero gossip).
- `rebalance.rs spawn_snapshot_watcher` (`:95`) already observes assignment-version changes
  and calls `announce_adopted_version`. On the **leader**, after announcing, have it read the
  adopted-version map and compute `assignment_versions_converged(live, reported)` (reuse the
  existing fn), then publish the result into a new `watch::Sender<bool>` (or
  `watch::Sender<Option<u64>>` = "converged at version V").
- `ClusterController` exposes the matching `watch::Receiver` (mirror `members_watch`,
  `controller.rs:318`).
- `assignment_ready_for_checkpoint` becomes a non-async local read of
  `*converged_rx.borrow()` — no `.await`, no gossip. (Keep the trait method's `-> impl
  Future` shape for the single-node default; the body just returns `ready(borrowed)`.)

### Steps
1. `controller.rs`: add a `converged_for_checkpoint: watch::Sender<bool>` + `converged_watch()`
   receiver accessor.
2. `rebalance.rs` (leader branch of the snapshot watcher, `:218`+): after
   `announce_adopted_version`, compute convergence from `read_adopted_versions` +
   `live_instances` and `send` the bool. This read is already off Ring-0 (watcher task), so
   it does not touch the alignment path.
3. `pipeline_callback.rs:1502`: replace the inline gossip read with a borrow of the watch
   value cached on the callback (set at start from `cc.converged_watch()`).
4. Delete the Note 3 throttle once the read is free.

### Effort/risk
S / low. Net removal of a hot-path gossip read; the convergence math is unchanged
(`assignment_versions_converged` is already unit-tested). Risk is a stale watch value during
the brief window between a version change and the watcher recomputing — acceptable (the gate
is advisory; a missed-by-one-tick checkpoint just waits one interval).

### Validation
Re-run the kill-9 shuffle-alignment soak (the regression Note 2 targets): align-timeout count
should drop further vs. 1A. Unit-test the watcher's converged-publish on a synthetic
version map.

---

## 1B — Independent failure domains (blast-radius isolation). (M, after 1A)

### Problem
1A recovers (or the committed `ShuffleNotReady` path defers) at **whole-pipeline** granularity.
Co-located queries share only the source nodes, yet a fatal error in one query faults the
entire pipeline, and even a *recoverable* defer in one query needlessly stalls the cycle for
its siblings. The graph has no component boundary: `OperatorGraph` is a flat `nodes` +
`topo_order` with `source_node_ids` (`operator_graph.rs:276`) and `input_sources`.

### Design
Partition the DAG into weakly-connected components **excluding source nodes** (queries share
only sources), compute it once in `compute_topo_order` (`:1497`), and scope fault/defer
decisions to a component.
1. **Domain map.** In `compute_topo_order`, union-find over edges with both endpoints
   non-source → `node_domain: Vec<usize>` (domain id per node) + `domain_of_source_consumer`
   for fan-out from a shared source into multiple domains.
2. **Per-domain cycle outcome.** `execute_cycle` already iterates `topo_order` and the
   deferral seam is `execute_single_operator:1646`. Generalize: on a fatal error, mark the
   node's **domain** failed for this cycle and skip the rest of that domain's nodes; other
   domains finish and their sinks commit. The `depends_on_stream`/`is_shuffle_not_ready`
   defer becomes the per-domain recoverable case.
3. **Per-domain offsets — the hard part.** The coordinator's `committed_offsets` /
   `pending_offsets` are global. A domain that faults must not advance its share of a shared
   source's offset while sibling domains do. Requires either (a) per-(source,domain) offset
   tracking, or (b) holding the source offset at the min across all domains consuming it
   (simpler, slightly conservative on replay). Recommend (b) for v1: a faulted domain pins
   the shared source's committed offset to the pre-cycle value; siblings still process, but
   the source can't advance past the faulted domain's last-good offset until it recovers.

### Steps
- `operator_graph.rs`: build `node_domain` in `compute_topo_order`; expose
  `domain_of(node)` and `source_domains(source)`.
- `operator_graph.rs:execute_cycle`: track a `failed_domains: FxHashSet` for the cycle; gate
  `execute_single_operator` + `route_output` on the node's domain not being failed; return a
  per-domain result (`{committed_domains, failed_domains}`) instead of a single Err.
- `pipeline/streaming_coordinator.rs`: change `commit_pending_offsets` to commit only offsets
  for sources whose every consuming domain succeeded (option b); fault → recover only when a
  domain exhausts its in-cycle defers, scoped to that domain's sources.

### Effort/risk
M / medium. The offset model is the real cost (the coordinator's global offset assumption is
load-bearing). Do strictly after 1A so "what happens on fault" is settled before "to whom".

### Validation
Two-query graph sharing one source, one query erroring: assert the healthy query commits and
advances while the erroring one defers/recovers independently, and the shared source offset
does not skip the faulted query's rows (no gap/dup on either sink).

---

## 1A-cluster — Coordinated global restart-to-epoch. (L, separate project)

### Problem
Single-node 1A recovers one node from its local checkpoint. With cross-node shuffle, a fault
on one node leaves peers mid-epoch: the faulted node rewinds to committed epoch N while peers
have produced/consumed shuffle rows for epoch N+1, so a local-only restart corrupts the
distributed cut (lost or duplicated in-flight shuffle rows). This is Flink's region/global
recovery problem.

### Design
Leader-coordinated restart-to-a-consistent-epoch, reusing the existing control plane
(`ClusterController`, the checkpoint decision store, assignment snapshots, and the barrier
alignment already in `streaming_coordinator`):
1. **Fault report.** A node that hits 1A's `ExitReason::Fault` does **not** restart locally in
   cluster mode; instead it reports the fault (+ the epoch it was on) to the leader via a new
   gossip/RPC control message (mirror the Prepare/Abort phase announcements).
2. **Global recover decision.** The leader picks the recovery epoch = the highest epoch
   committed by **all** participants (from the decision store), announces
   `Recover(epoch=N)` to every node (new `control::Phase::Recover`).
3. **Coordinated restart.** Each node, on `Recover(N)`: abandons its open epoch, restores
   operator state + re-seeks sources to N's offsets (the 1A single-node path), and
   re-registers its shuffle endpoints. The leader then re-arms barrier injection only once
   all nodes ack readiness (the same convergence gate from Note 2).
4. **Guard.** Bounded global restarts (reuse `RestartPolicy` semantics at cluster scope) →
   escalate to a cluster-wide hard-fault surfaced in `/metrics` + status.

### Steps
- `cluster/control`: add `Phase::Recover { epoch }` + leader-side
  `announce_recover`/follower handling alongside the existing Prepare/Abort plumbing.
- `pipeline_callback.rs` / coordinator: in cluster mode, route `ExitReason::Fault` to a
  "report to leader, await Recover" path instead of the local supervisor (gate on
  `cluster_controller.is_some()`).
- Reuse `recovery_manager` for the per-node restore and the Note 2 convergence gate to fence
  barrier re-arm until all nodes are restored.

### Effort/risk
L / high. New control-plane phase + cross-node state machine; must interleave correctly with
in-flight rebalances (a fault during rebalance churn). Depends on 1A (the per-node restore),
Note 2 (the convergence signal), and benefits from 1B (domain-scoped recovery would let a
fault recover only the affected shuffle region rather than the whole job).

### Validation
Multi-node kill-9 with cross-node shuffle (depth ≥ 1): kill a non-leader mid-epoch, assert the
leader drives all nodes to a consistent recovery epoch with no gap/dup on an exactly-once
sink across the cluster. Extends the existing kill-9 EO soak harness to depth > 1.

---

## Dependency summary
```
Note 3 ──▶ Note 2 ──────────────┐
                                 ├──▶ 1A-cluster
1A (done) ──▶ 1B ────────────────┘
```
Note 3/Note 2 are independent of 1B and can land anytime. 1A-cluster is gated on 1A (done),
Note 2 (convergence signal), and is materially simpler if 1B lands first.
