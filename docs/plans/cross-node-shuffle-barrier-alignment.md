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

### The one real subtlety: post-barrier frames

The receiver multiplexes all peers into one queue, so after peer P's `Barrier(C)` we may still
pull P's `VnodeData` for epoch **C+1** (P kept processing). Those must **not** enter checkpoint C.
The receiver already attributes every frame to its peer id, so during alignment we **buffer**
frames from already-barriered peers and re-feed them on the next cycle, routing into state only
frames from not-yet-barriered peers. This is the Chandy–Lamport "channel state" rule; it is the
part most easily gotten wrong.

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
- **Resume timing confirms the post-barrier buffer is load-bearing:** `follower_checkpoint` blocks
  until **commit**, and the leader commits on a *quorum* of acks. With quorum < full membership, a
  fast peer can commit and resume (ship epoch C+1 rows) while a straggler is still aligning C — so
  C+1 frames can arrive mid-alignment and must be buffered, not folded into C (else double-count).
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
   `replay`); `OperatorGraph::align_shuffle_barriers` (fan-out + `BarrierTracker` drain + per-stage
   routing via `ingest_to_stage`) + the post-barrier `pending_shuffle` buffer folded at the next
   alignment; `ShuffleReceiver::drain_all_staged` to fold the per-stage holdover. Reuses
   `fan_out_barrier` + `BarrierTracker` + `ShuffleMessage::Barrier` (no new transport). Verified by
   `align_shuffle_barriers_folds_peer_rows_then_aligns` (2-node loopback: a peer ships a row + its
   barrier; alignment folds the row into the target operator and completes on the peer's barrier).
   Clippy `-D warnings` clean with and without `cluster-unstable`. The method carries a temporary
   `#[allow(dead_code)]` + "wired in the follow-up" pointer until step 2 lands — it is exercised by
   the test, not dead, but has no production caller yet.
2. **Wiring (NEXT — separate, reviewed change; chosen approach: TBD).** Call `align_shuffle_barriers`
   before `snapshot_state` in `capture_and_serialize_operator_state`, threading the cross-cluster
   `checkpoint_id`. Per the integration findings this needs either the **leader 2PC reorder**
   (announce `Prepare` → align → capture) or a **separate pre-2PC align round** to avoid leader
   deadlock — the deliberate, sign-off-gated protocol change. Removes the `#[allow(dead_code)]`.
3. **Recovery safety:** confirm ingested-at-checkpoint rows restore without loss or double-count.
   With alignment the origin's offset-advance and the owner's ingest land in the same checkpoint, so
   no double-count (agg) and idempotent replay (lookup) — to be proven with a restore test.
4. **Test:** a 2-node harness test that ships rows A→B, checkpoints mid-flight, restores, and
   asserts no row loss (and no duplication) across the shuffle boundary. (Needs step 2 wired.)

## Risks / open questions

- **Agg double-count on replay.** If a pre-C row is both ingested into A's snapshot *and* re-fed
  by source-offset replay on restore, the additive agg double-counts. The fix hinges on whether
  source offsets are advanced before or after the shipped row is accounted — Phase 3 must settle
  this (likely: the shuffled row's *origin* source offset must not commit until the row is
  durably in the owner's snapshot; or the agg keys dedup). This is the crux correctness question.
- **Alignment latency** stalls the checkpoint until the slowest peer fans out — bounded by the
  checkpoint cadence; acceptable, but a hung peer needs a timeout (fail the checkpoint, retry).
- **Interaction with lookup-enrich's async worker:** in-flight fetches are already checkpointed;
  ingested rows just add to `replay`. Believed safe; verify in Phase 3.
- Scope is multi-session. Phase 1 is self-contained and safe to land first.

## Exit

A 2-node lookup/agg shuffle MV checkpointed while rows are in flight restores with **no loss and
no duplication** across the shuffle boundary; single-node and the per-cycle hot path are
unchanged; a hung peer fails the checkpoint (retry) rather than hanging.
