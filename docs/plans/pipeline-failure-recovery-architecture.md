# Pipeline failure semantics & cycle isolation — architecture plan

Cold-start-ready. Surfaced while fixing the kill-9 shuffle-alignment + exactly-once-gap
bugs (`docs/plans/shuffle-barrier-after-kill-recovery.md`, committed `0bccbe14`). That work
patched one symptom; this plan addresses the underlying architecture it exposed.

## TL;DR
LaminarDB runs **every operator of every query in one synchronous `execute_cycle` pass**, and a
fatal operator error **silently drops the cycle and continues** (`[LDB-3020]`). That is two
defects: (a) one query's failure discards co-located queries' output (blast radius), and (b) any
fatal error is an exactly-once hole (records lost, no replay). Flink/RisingWave/Arroyo avoid both
by running operators independently and **recovering from the last checkpoint on any fatal error**.
The committed `ShuffleNotReady` deferral is a narrow point-fix of (a) for one error class.

Work, by value:
1. **1A — fatal cycle error → recover from last checkpoint (single-node).** Closes (b) for ALL
   errors using machinery that already exists. The big one.
2. **Note 2/3 — make the convergence gate event-driven + de-conflate the throttle.** Small.
3. **1B — partition the graph into independent failure domains.** Cleans up (a) structurally.
4. **1A-cluster — leader-coordinated global restart-to-epoch.** Large, separate project.

## Current architecture (grounded, with anchors)
- **Single-task synchronous cycle.** `StreamingCoordinator::run` (`pipeline/streaming_coordinator.rs`)
  drains source batches → `callback.execute_cycle(...)` → `write_to_sinks`. One tokio task, no
  per-operator tasks/channels (deliberate: low-latency / Ring-0 philosophy).
- **`execute_cycle`** (`operator_graph.rs:1774`) runs all nodes in `topo_order`; `execute_single_operator`
  (`:1589`) returns `Err` on the first failing operator → `execute_cycle` returns `Err`.
- **Coordinator error handling** (`streaming_coordinator.rs`, the `execute_cycle` match): on `Err`
  it calls `discard_pending_offsets()` + logs `[LDB-3020] SQL cycle error` + **continues**. The
  cycle's already-drained source rows are dropped (the EO gap); committed offsets stay put, but the
  live source has advanced, so the next successful cycle's offset jumps past the lost rows.
- **Recoverable deferral already exists** (`execute_single_operator:1640`): if the node is in
  `depends_on_stream` OR `e.is_shuffle_not_ready()`, the operator's input is **preserved** and it
  returns `Ok(())` — the cycle keeps running. This is the seam to generalize.
- **EO 2PC is the rollback unit.** A failed epoch already abandons/rolls back sink transactions:
  `CheckpointCoordinator::fail_epoch` → `rollback_sinks` (`checkpoint_coordinator.rs:1254`). The
  Kafka sink carries pending rows forward on a live abandon (`kafka/sink.rs:719` `begin_epoch`).
- **Restart capability is partial.** `DbState::Faulted` is a *recoverable* state: `start_inner`
  (`pipeline_lifecycle.rs:260`) rebuilds from `Faulted` (`:213`), restoring from the last
  checkpoint via `recovery_manager`. There is **no mid-run trigger** that moves a running pipeline
  to `Faulted` + restarts it on a cycle error.
- **Graph has no failure-domain notion.** `OperatorGraph` is a flat `nodes: Vec` + `topo_order`
  (`:272`), `source_node_ids` (`:276`), `input_sources` (`:282`, per-node upstreams),
  `output_routes` (`:154`, downstream edges). Queries are added via `add_query` (`:952`) and share
  only the source nodes.

## Reference engines (what "correct" looks like)
- **Flink:** operator (sub)tasks connected by network/local channels; "not ready" = channel
  backpressure (never a drop); ANY fatal error → job restart from the last checkpoint (region or
  global) → replay. Exactly-once via aligned barriers + 2PC sinks.
- **RisingWave:** actor-per-operator async tasks + channels; barrier flows through actors; fatal
  error → recovery from barrier; **scaling pauses barrier injection** (== our convergence gate).
- **Arroyo:** operator-per-task + channels + checkpoint recovery; same shape.

Two invariants LaminarDB lacks: (1) a failure never silently drops in-flight data; (2) fatal
errors recover-and-replay. (1) is partly handled by deferral for recoverable errors; (2) is the
gap.

---

## 1A — Fatal cycle error → recover from last checkpoint (single-node). PRIORITY.
**Problem:** `[LDB-3020]` discards a fatally-failed cycle and continues → records lost, no replay.
Exactly-once hole for any non-deferrable operator error.

**Design (leverages the existing EO epoch as the rollback boundary):**
1. Classify the error. Deferrable (`is_shuffle_not_ready`, `depends_on_stream`) → existing defer
   (unchanged). Otherwise → **fault**.
2. On a fault: abandon the in-flight epoch (`fail_epoch`/`rollback_sinks` — aborts the open sink
   txn) and transition the pipeline to `DbState::Faulted` with the error reason, then restart
   (`start()` already rebuilds from `Faulted`, restoring operator state + re-seeking sources to the
   last committed offsets via `recovery_manager`).
3. The coordinator loop must break/return so the restart can re-enter cleanly. Wire a fault signal
   from the `execute_cycle` `Err` arm to a "request restart" path
   (`pipeline_lifecycle.rs:1904` "stop the streaming pipeline so it can be restarted" is the seam).
4. Guard against fault loops: bound restarts (e.g. N within a window) → escalate to a hard
   `Faulted`-stuck state with the last error, rather than hot-looping restarts.

**Why it's tractable:** the rollback unit (epoch/2PC), the restore path (`recovery_manager`), and
the `Faulted`→`start()` rebuild all exist. The new work is the *mid-run trigger* + restart loop
guard, not new recovery machinery.

**Effort:** medium. **Risk:** medium — changes failure semantics (drop-and-continue → recover-and-
replay); must ensure restart restores *source offsets* (not just operator state) so there's no gap
*or* dup, and that EO sink txns are aborted before restart (Kafka fencing on the new producer's
`init_transactions` handles a crashed txn; an in-process restart must explicitly abort).

**Validation:** unit-test the classify + restart-loop guard. Integration: inject a fatal operator
error mid-run (a test operator that errors once), assert the pipeline recovers from the last
checkpoint with no gap/dup on an EO sink. Then the kill-9 soak stays green.

**Open question:** is "continue past a transient non-shuffle error" ever desired (at-least-once
mode)? If so, gate 1A on the delivery guarantee: ExactlyOnce → recover; AtLeastOnce → keep the
current continue (with a metric). Check `delivery_guarantee` on the callback.

## 1B — Independent failure domains (blast-radius isolation).
**Problem:** even with 1A, a fault recovers the *whole* pipeline; and a *recoverable* error in one
query shouldn't touch co-located queries. The graph has no component boundary.

**Design:** compute weakly-connected components of the DAG **excluding `source_node_ids`** (queries
share only sources) once in `compute_topo_order`; store a `node → domain` map. In `execute_cycle`,
on an operator error, scope the effect to that operator's domain (defer/fault only its component;
other components finish and their sinks commit). The committed `ShuffleNotReady` deferral is the
special case of this for one domain + one error class.

**Effort:** medium. **Risk:** medium — must keep per-domain source-offset bookkeeping consistent
(a domain that faults must not advance its share of the source offset while siblings do). Likely
needs per-domain `pending_offsets`, which is a non-trivial change to the coordinator's
offset model. Do AFTER 1A (1A defines "what happens on fault"; 1B defines "to whom").

**Validation:** a 2-query graph (one healthy, one erroring); assert the healthy query commits while
the erroring one defers/recovers independently.

## Note 2 — Event-driven convergence gate (replace per-interval gossip poll).
**Now:** `assignment_ready_for_checkpoint` (`pipeline_callback.rs`) does a gossip
`read_adopted_versions` scan each checkpoint interval (throttled via Note 3).
**Better:** the snapshot watcher (`rebalance.rs spawn_snapshot_watcher`) already runs on every node
and observes version changes; have the leader compute convergence there (it already scans for
metrics) and publish a single `watch`/`AtomicBool` "cluster converged at version V" that the gate
reads with zero hot-path gossip. `ClusterController` already exposes `members_watch`
(`controller.rs:24`) as the pattern to follow.
**Effort:** small. **Risk:** low.

## Note 3 — De-conflate the gate throttle.
**Now:** the gate does `self.last_checkpoint = Instant::now()` on a quiesce-skip to throttle the
gossip read to once/interval — conflating "checkpointed" with "skipped".
**Fix:** a dedicated `last_convergence_check: Instant` field (or drop it entirely once Note 2 makes
the read free). ~5 lines. Do with Note 2.

## 1A-cluster — Coordinated global restart-to-epoch. SEPARATE PROJECT.
Single-node 1A recovers one node. In a cluster with cross-node shuffle, a fault on one node needs a
*consistent* restart cut across participants (Flink region/global recovery). The leader would
announce "recover to committed epoch N", all nodes restore + re-align. Large; depends on 1A
landing first. Out of scope until 1A + 1B are in.

## Sequencing
1. **1A single-node** (correctness — start here).
2. **Note 2 + Note 3** (small, independent — can land anytime).
3. **1B** (after 1A).
4. **1A-cluster** (separate project).

## Don't-repeat list (from the prior work)
- Don't "retain `source_batches_buf` + replay" on cycle error → re-runs `execute_cycle` and
  re-writes downstream sink operators → DUPLICATES. (The reason 1A *recovers* instead of *replays
  in place*.)
- Don't add a per-cycle gossip/KV read on the cycle or barrier-fan-out path → shares the alignment
  task's chitchat lock and regresses alignment timeouts. (Why Note 2 must be event-driven.)
- Soak repro: `docs/plans/shuffle-barrier-after-kill-recovery.md` (commands, OpenSSL/ORT env,
  recreate-Redpanda-first). EO gap + alignment are both FLAKY — validate over ≥8 runs, delta-off
  and delta-on.
