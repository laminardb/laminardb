# Plan: Cross-node checkpoint barrier alignment for the row-shuffle

- **Status:** Proposed (not started). 2026-05-30.
- **Scope:** Close the at-least-once gap the cluster row-shuffle leaves at a checkpoint,
  for **both** the aggregate shuffle (`shuffle_pre_agg_batches`, `operator/sql_query.rs`) and
  the lookup-enrich key-shuffle (`shuffle_input`, `operator/lookup_enrich.rs`). Behind
  `cluster-unstable`.

## Problem

The row-shuffle ships rows peer→peer over `ShuffleSender` and drains inbound best-effort each
cycle (`drain_vnode_data_for`). It puts **no barrier on the shuffle channel**. At a checkpoint:

- Node A shipped rows to B during the cycles before checkpoint C, then advanced its source
  offset past them and snapshotted — those rows are **not** in A's state.
- If they are still in flight on the wire (or queued in B's receiver, undrained) when B
  snapshots, they are **not** in B's state either.

On restore from C the rows are lost: A's offset is past them, B never recorded them. This is a
real at-least-once violation, unique to the cross-node path. (Single-node is unaffected — no
shuffle.) The exit criterion of lookup Phase 5 explicitly deferred this as a shared gap.

## Approach — Chandy–Lamport alignment on the shuffle channel, at checkpoint time

The existing checkpoint is a **stop-the-world snapshot between cycles**, decided when source
barriers align at the `StreamingCoordinator`; `graph.snapshot_state()` then captures every
operator. We bolt barrier alignment onto that boundary rather than threading a marker through
the operator graph:

1. When checkpoint C is decided (source barriers aligned), **before `graph.snapshot_state()`**,
   the node runs `graph.align_shuffle_barriers(checkpoint_id).await`:
   - **Fan out** `ShuffleMessage::Barrier(C)` to every peer via `ShuffleSender::fan_out_barrier`.
     On each per-peer TCP connection this barrier follows all the `VnodeData` the node sent for
     rows ≤ C (the connection is FIFO), so a peer receiving `Barrier(C)` has received all our
     pre-C rows.
   - **Drain + align**: pull `(peer, msg)` from the `ShuffleReceiver`, feeding a `BarrierTracker`
     (inputs = self + N peers). For each `VnodeData(stage, _, batch)` from a **not-yet-barriered**
     peer, route it into operator `stage`'s state (see ingest hook). Stop when every peer's
     `Barrier(C)` has been observed.
2. `graph.snapshot_state()` now captures state that includes every pre-C row from every peer.

Deadlock-free: every node fans out *before* it blocks on the drain, so the barriers are already
on the wire; and the `ShuffleReceiver`'s accept/per-peer tasks run on the **main** runtime, not
the compute thread, so they keep delivering frames while compute awaits.

### Post-barrier frames: precluded by full-membership commit (no buffer)

In general Chandy–Lamport, after peer P's `Barrier(C)` you can still pull P's epoch-`C+1`
`VnodeData` (P kept processing), which must not enter checkpoint C — the "channel state" rule,
normally handled by per-peer buffering. **This engine doesn't need it:** `wait_for_quorum`
requires *every* live follower to ack (full-membership commit, not majority), and a node resumes
the next epoch only after it observes `Commit` — which the leader sends only once all have acked
(hence all have aligned). So no peer ships `C+1` while any node is still aligning `C`. We therefore
just fold every `VnodeData` until aligned, with no post-barrier buffer. **Precondition, documented
at the call site:** a future move to majority quorum (for straggler tolerance) would reintroduce
the case and require per-peer post-barrier buffering. (An earlier draft buffered defensively; the
review cut it as unreachable under the actual quorum policy.)

### One barrier per peer covers all stages

All operators share one `ShuffleSender` (one TCP connection per peer pair), so a single
`Barrier(C)` per peer follows the interleaved `VnodeData` of *every* stage (agg, lookup, …) on
that connection. So a multi-stage cluster pipeline needs just one fan-out per peer; on receipt
the node has all pre-C rows for all stages from that peer, and routes each drained `VnodeData`
to its operator by the stage tag. Alignment is per-node-pair, not per-operator.

## Validation against 2026 production practice + research

Checked against current systems and the recent literature; the approach is mainstream-correct:

- **Aligned coordinated barriers (Chandy–Lamport / "asynchronous barrier snapshot") are the
  production baseline in 2026.** Flink uses them by default; RisingWave injects barriers from a
  central meta node that flow through the DAG and **align at multi-input / exchange operators**;
  Arroyo is explicitly "Chandy–Lamport / ABS." CheckMate (arXiv 2403.13629, 2024) finds
  coordinated checkpointing "outperforms the uncoordinated and communication-induced protocols
  under uniformly distributed workloads," at near checkpoint-free throughput.
- **Our specialization is exactly the production rule, applied where it's needed.** Flink/
  RisingWave thread the barrier through *every* operator because each is its own task; LaminarDB's
  per-node graph is single-threaded and already snapshotted stop-the-world, so the **shuffle is the
  only multi-input/exchange boundary** — which is precisely where RisingWave says "barriers align."
  Draining each peer's pre-barrier `VnodeData` into state before snapshotting *is* the textbook
  aligned-ABS channel-consumption step. After the shuffle, the existing 2PC already coordinates
  sink commit across nodes, so no barrier needs to propagate further.
- **Known weakness — backpressure / stragglers.** Both Flink and CheckMate flag that aligned
  checkpoints stall when a peer straggles (markers blocked behind buffered data). Mitigation here:
  the alignment-drain has a **timeout → fail-the-checkpoint → retry** (checkpoints are periodic, so
  a dropped one is harmless), matching how aligned systems degrade.
- **Escape hatches we deliberately do *not* build now.** (a) Flink's **unaligned checkpoints**
  (FLIP-76) snapshot in-flight channel buffers instead of waiting — but that needs the engine to
  *observe* its in-flight data, and our shuffle's in-flight data sits partly in un-snapshottable
  kernel TCP buffers, so true unaligned would require an app-level send buffer first. Aligned is the
  right fit for a TCP shuffle. (b) CheckMate argues **uncoordinated checkpointing** (per-operator +
  message logging + dedup) wins under *skew*; it's the research frontier but a major rearchitecture
  (we'd add a replay log + dedup). Both are documented future directions if backpressure/skew proves
  painful — not v1.

## Reuse (no new transport)

- `BarrierTracker` (`shuffle/barrier_tracker.rs`) — alignment, today wired only into the
  test-only `ClusterRepartitionExec`.
- `ShuffleSender::fan_out_barrier` + `ShuffleMessage::Barrier` — already exist, unused in prod.
- `ShuffleReceiver::recv()` (peer-attributed) for the alignment drain; `drain_vnode_data_for`
  stays the per-cycle hot path.
- The checkpoint id is the cross-cluster identifier already in `CheckpointBarrier`.

## New surface

- `GraphOperator::ingest_shuffle(&mut self, batch)` (default no-op) — feed a peer's pre-barrier
  rows into operator state outside `process()`. Agg → `IncrementalAggState::process_batch`;
  lookup-enrich → push to `replay` (its in-flight is already checkpointed, so this is safe and
  re-uses the restore path). Only shuffle-consuming operators override it.
- `OperatorGraph::align_shuffle_barriers(checkpoint_id)` — owns the fan-out + drain/align +
  routing to `ingest_shuffle` by stage name. No-op when `cluster_shuffle` is `None`.
- Call site: wherever `snapshot_state()` is invoked for a cluster checkpoint (the pipeline
  checkpoint path), gated so single-node is untouched.

## Integration findings (from tracing the checkpoint path)

- **Where alignment hooks in:** the per-node operator snapshot is `OperatorGraph::snapshot_state()`,
  called from `PipelineCallback::capture_and_serialize_operator_state` (3 callers: leader
  `force_capture_and_checkpoint`, follower `maybe_follower_checkpoint`, and the barrier path).
  Alignment must run on the graph immediately before `snapshot_state`.
- **Resume timing → no post-barrier buffer needed.** `follower_checkpoint` blocks until **commit**,
  and `wait_for_quorum` requires *every* live follower (full membership, not a majority). A peer
  resumes the next epoch only after `Commit`, which the leader sends only once all have acked (all
  aligned) — so no peer ships epoch C+1 while any node is still aligning C. Alignment folds every
  `VnodeData` with no buffer; a future majority quorum would change this (see the subtlety section).
- **`staged` holdover must also be drained.** `drain_vnode_data_for` buckets other stages' frames
  into the receiver's `staged` map; a backpressured operator can leave pre-barrier rows there
  between cycles. Alignment must drain `staged` (new `ShuffleReceiver::drain_all_staged`) in
  addition to `recv()`-ing the live wire, or those rows are missed.
- **⚠ Leader 2PC reorder required (the risk).** The leader today **captures state before it
  announces `Prepare`** (`force_capture_and_checkpoint` captures, then `checkpoint_with_offsets`
  announces). If alignment lives inside capture, the leader fans out its barrier and blocks on
  follower barriers *before followers have seen `Prepare`* → **deadlock**. Phase 2 must reorder the
  leader to **announce `Prepare` → align → capture → await quorum**, and thread the cross-cluster
  `checkpoint_id` (followers get it from the `BarrierAnnouncement`; the leader generates it) into
  the capture path so every node fans out the *same* id. This is a deliberate change to the
  checkpoint protocol — get explicit sign-off before touching it.
- **`checkpoint_id` threading:** the barrier's id must match across nodes for `BarrierTracker` to
  align; it's the announced/generated cross-cluster id, available at announce time on the leader and
  in `ann.checkpoint_id` on followers.

## Phasing

1. **Foundation + alignment mechanism — DONE 2026-05-30 (mechanism only; not yet wired).**
   `GraphOperator::ingest_shuffle` (cfg `cluster-unstable`; agg → `process_batch`, lookup-enrich →
   `replay`); `OperatorGraph::align_shuffle_barriers` (fan-out via shared `state::peer_owners` +
   `BarrierTracker` drain + per-stage routing via `ingest_to_stage`); `ShuffleReceiver::drain_all_staged`
   to fold the per-stage holdover. No post-barrier buffer (precluded by full-membership commit; see
   the subtlety section). Reuses `fan_out_barrier` + `BarrierTracker` + `ShuffleMessage::Barrier`
   (no new transport). Verified by
   `align_shuffle_barriers_folds_peer_rows_then_aligns` (2-node loopback: a peer ships a row + its
   barrier; alignment folds the row into the target operator and completes on the peer's barrier).
   Clippy `-D warnings` clean with and without `cluster-unstable`. The method carries a temporary
   `#[allow(dead_code)]` + "wired in the follow-up" pointer until step 2 lands — it is exercised by
   the test, not dead, but has no production caller yet.
2. **Wiring via the leader 2PC reorder (Prepare-triggered) — DONE 2026-05-31.** The leader now
   announces `Prepare` *early* (`CheckpointCoordinator::announce_prepare`, before capture) so the
   flow is **announce `Prepare` → align → capture → await quorum**; followers do **observe `Prepare`
   → align → capture → ack**. `await_prepare_quorum` re-announces the identical `Prepare`
   idempotently (followers dedup by epoch). Wired in `PipelineCallback` at all three checkpoint
   paths (`force_capture_and_checkpoint`, `checkpoint_with_barrier`, `maybe_follower_checkpoint`)
   via `align_shuffle_for_leader` (leader) / inline `graph.align_shuffle_barriers(ann.checkpoint_id)`
   (follower). On leader alignment failure it announces `Abort` so prepared followers don't block.
   `#[allow(dead_code)]` removed.
   - **Bug 1 (e2e smoke caught it):** the per-cycle `drain_vnode_data_for` was *dropping* `Barrier`
     frames, so a peer's barrier arriving before the recipient entered alignment was lost → timeout.
     Fixed by stashing barriers (`ShuffleReceiver::{drain_vnode_data_for stashes, drain_staged_barriers
     drains}`); alignment observes the stash before the live wire.
   - **Bug 2 (rebalance test caught it): the fan-out set and the wait set are not the same under
     skewed ownership.** A node fans its barrier to the nodes it *ships to* (other vnode owners =
     `peer_owners`), but must wait for barriers from the nodes that *ship to it* — every live producer,
     gated by "do I own any vnode." A drained node (owns nothing) still ships to the owner, so the
     owner must wait for it even though it isn't a `peer_owner`. Using `peer_owners` for both
     deadlocked `checkpoint_after_rotation_carries_new_version` (all vnodes → leader: the leader had
     no owner-peers so skipped alignment while the follower waited on a barrier the leader never sent).
     Fix: `output = peer_owners(self)`; `input = owned_vnodes(self).is_empty() ? [] : live − self`,
     where `live` is the controller's membership (passed in — control-plane stays out of the shuffle
     config). `cluster_e2e_smoke` 30 s-timeout→pass; `cluster_rebalance_flow` + `cluster_2pc_flow` green.
3. **Recovery safety:** confirm ingested-at-checkpoint rows restore without loss or double-count.
   With alignment the origin's offset-advance and the owner's ingest land in the same checkpoint, so
   no double-count (agg) and idempotent replay (lookup) — to be proven with a restore test.
4. **Test:** a 2-node harness test that ships rows A→B, checkpoints mid-flight, restores, and
   asserts no row loss (and no duplication) across the shuffle boundary. (Needs step 2 wired.)

## Phase 2 recommendation: reorder (Prepare-triggered), not a separate round

Grounded in the architecture + how production systems do it:

- **It's how real systems work.** In Flink/RisingWave/Arroyo the *checkpoint-coordination signal*
  is what triggers barrier propagation + alignment at exchanges — there is no second coordination
  round. Here the cross-cluster signal is the 2PC `Prepare` announcement; making it trigger the
  shuffle barrier fan-out + alignment makes the shuffle barrier the exact cross-node analog of
  Flink's in-band barrier. One coordination flow, reusing the existing `Prepare`/ack/`Commit` 2PC.
- **A separate pre-2PC align round is a redundant second protocol** — extra round-trips and a
  parallel coordination mechanism duplicating what the 2PC already does. It only looked "safer"
  because it avoids editing the 2PC; in aggregate it is *more* moving parts, not fewer.
- **The reorder is localized and is exactly what removes the deadlock.** The only change is hoisting
  the leader's `Prepare` announce ahead of capture; the deadlock (leader blocks on follower barriers
  before followers see `Prepare`) is resolved precisely by announcing first. Followers are already
  `Prepare`-triggered.
- **Preserves the simplifications above.** Full-membership commit (no post-barrier buffer) and the
  per-node-pair single barrier both still hold under the reorder. If straggler tolerance later
  motivates majority quorum / unaligned checkpoints (per CheckMate / Flink FLIP-76), that is a
  separate, larger change — and *then* the post-barrier buffer returns.

## Risks / open questions

- **Agg double-count — resolved by alignment, not an open risk.** Alignment makes the origin's
  offset-advance and the owner's ingest land in the *same* checkpoint: A sends a row before it fans
  out `Barrier(C)`, so B folds it before B's C-snapshot while A's C-snapshot commits the offset past
  it. Restore from C → A doesn't re-read it, B has it. No double-count; lookup replay is idempotent
  besides. (Phase 3 still proves this with a restore test.)
- **Alignment latency** stalls the checkpoint until the slowest peer fans out — bounded by the
  checkpoint cadence; acceptable, but a hung peer needs a timeout (fail the checkpoint, retry).
- **Interaction with lookup-enrich's async worker:** in-flight fetches are already checkpointed;
  ingested rows just add to `replay`. Believed safe; verify in Phase 3.
- Scope is multi-session. Phase 1 is self-contained and safe to land first.

## Exit

A 2-node lookup/agg shuffle MV checkpointed while rows are in flight restores with **no loss and
no duplication** across the shuffle boundary; single-node and the per-cycle hot path are
unchanged; a hung peer fails the checkpoint (retry) rather than hanging.
