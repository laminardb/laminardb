# Sub-Second Checkpointing — Implementation Plan (ADR-003)

Companion to `docs/adr/ADR-003-sub-second-checkpointing.md`. Phases are
independently deployable and ordered by risk/value; each must keep
`cluster_integration` (laminar-db + laminar-core) green.

## Status

| Phase | Description | Status |
|-------|-------------|--------|
| 1 | Two-level completion — resume on `Aligned`, durable tail off the pipeline task | **DONE** (2026-06-10) |
| 2 | Pipelined epochs: allocate-at-admission ids, pre-mutex quorum stage, in-flight/staged-bytes caps | **DONE** (2026-06-10; see scope notes) |
| 3 | Incremental snapshots: `Full \| reference` vnode partials | **DONE** v1 (2026-06-10; group-level deltas = follow-up) |
| 4 | 100ms floor + admission guard; push-driven decision/resume waits | **DONE** (2026-06-10) |

## Phase 1 — Two-level completion (IMPLEMENTED)

### Protocol

The single barrier "Commit" is split into two cluster-visible levels:

- **Aligned(N)** — every node has aligned the cross-node shuffle and
  captured epoch N's state locally. The leader announces it after a
  *full-membership* quorum of capture acks (no object-store I/O on this
  path). Pipelines resume epoch N+1 processing on observing it.
- **Restorable(N)** — every artifact N references is durable: each
  node's sink pre-commit + manifest save + per-vnode partial uploads
  finished, the leader's durability gate sealed the epoch `_COMMIT`
  CAS marker, and the decision marker is written. Drives sink 2PC
  commit, the recovery point, pruning, and rebalance rehydration.

The shuffle-alignment invariant ("no peer ships epoch-N+1 rows while
another node is still folding pre-barrier rows into its epoch-N
snapshot", required by the no-post-barrier-buffering drain in
`operator_graph.rs::align_shuffle_barriers`) is **re-keyed, not
removed**: the resume gate moves from full-membership *Commit* to
full-membership *Aligned*. This also closes a latent leader-side
window — previously the leader resumed right after its own capture,
before knowing every peer had finished aligning.

### What changed where

Control plane (`laminar-core/src/cluster/control/`):

- `Phase::Aligned` variant + `Aligned` RPC in `proto/barrier.proto`
  (carries `min_watermark_ms` so resuming pipelines see cluster-wide
  event-time progress before the upload-gated Commit; the controller
  publishes it from `Aligned` as well as `Commit`).
- `BarrierCoordinator::observe` is now **latest-wins** in gRPC mode
  too (a relay task drains the incoming queue into a `watch`),
  matching the gossip-KV fallback the callers were already written
  for. This makes observation non-destructive, so the pipeline's
  resume gate and the background durable tail can watch concurrently
  without stealing announcements from each other.
- The gRPC and KV values merge by epoch (higher wins; same-epoch ties
  prefer gRPC arrival order). A `seq` ordering field briefly existed to
  disambiguate same-epoch retries, but Phase 2's allocate-at-admission
  abandonment removed epoch reuse, so it was deleted (merge test:
  `observe_merges_grpc_and_gossip_by_epoch`).
- `Aligned` fan-out is best-effort per peer: a missed delivery only
  delays that peer's resume until Commit or its gate timeout.

Coordinator (`laminar-db/src/checkpoint_coordinator.rs`):

- Leader (`checkpoint_inner`, cluster): `await_prepare_quorum` moved
  **before** the leader's own pre-commit/manifest/partial writes; on
  quorum it announces `Aligned`. The durability gate became
  `await_restorable_gate`: it **polls** `epoch_complete` (100ms with exponential backoff to 1s, up to
  `CheckpointConfig::restorable_gate_timeout`, default 30s) because
  followers now upload after acking. Split-brain commit markers still
  abort immediately; transient I/O errors retry until the deadline.
- Follower (`follower_checkpoint`): the ack moved **before**
  `follower_prepare` — it now means "aligned + captured", not
  "durably prepared". On a prepare failure after acking, a
  best-effort `ok=false` ack overwrites the capture ack (fast abort
  in KV mode; in gRPC mode the leader learns via gate timeout). The
  decision wait also recognizes epoch advancement (latest-wins
  observation can supersede Commit(N) with Prepare(N+1)): a newer
  epoch's announcement triggers an immediate decision-store check.

Pipeline (`laminar-db/src/pipeline_callback.rs`):

- Followers hand the durable tail (prepare + decision wait + 2PC
  commit/rollback) to a spawned task (`spawn_follower_tail`); the
  pipeline blocks only in `wait_for_aligned_resume` (push-driven, 30s
  bound, no-op without a cross-node shuffle). Tail bookkeeping lives
  in the shared `FollowerTailState` (in-flight epoch dedups the
  leader's idempotent Prepare re-announcements; committed epoch
  advances only on commit so retries are reprocessed). Commit
  completion flows through `checkpoint_complete_tx` — the same
  channel the leader's background persist uses — so `EpochCommitted`
  fan-out to sources and the wire-barrier publish are unchanged.
- The leader pipeline now also gates its resume on `Aligned`
  (bounded by quorum timeout + abort).
- New metric: `checkpoint_pipeline_stall_duration_seconds` — the
  ADR's Phase-1 success measure (stall = align + capture + resume
  gate, excluding the durable tail).

### Why partial-presence implies prepare-complete

`follower_prepare` runs strictly in the order sink pre-commit →
manifest save → vnode-partial writes. The leader's gate checks the
**full registry** of vnode partials, so full presence proves every
node finished its entire durable prepare — the leader's Commit
decision still waits for cluster-wide durability even though acks no
longer carry that meaning. (Caveat: a node owning zero vnodes is
invisible to the gate; today every node owns vnodes outside of
drain, and drain has its own checkpoint barrier.)

### Failure matrix (Phase 1)

| Failure | Outcome |
|---|---|
| Follower capture/alignment fails | No ack → leader quorum timeout (3s) → Abort; epoch abandoned, next barrier gets fresh ids |
| Follower prepare fails after ack | `ok=false` ack overwrite (KV fast path) or leader gate timeout → Abort; follower already rolled back its sinks |
| Leader upload/gate/marker fails after Aligned | Abort announced; pipelines already resumed — epoch N is simply not restorable and is abandoned |
| Aligned RPC lost to one peer | That peer resumes on Commit or its 30s gate timeout; epoch unaffected |
| Follower tail misses Commit (gossip lag + RPC loss) | Decision timeout → durable marker check → commit (pre-existing fallback); epoch advancement check accelerates it |
| Leader dies between Aligned and Commit | Followers' decision timeout → marker absent → rollback; new leader reconciles via `reconcile_prepared_on_init` |

Known accepted race (pre-existing class, now documented): a *stale*
`Aligned(N)` from an attempt that aborted **after** Aligned can
release a retry attempt's resume gate early. It requires an abort
after full capture quorum (gate timeout / marker failure) plus a
peer still mid-alignment on the retry within the announcement-
propagation window; consequences are bounded by the same
duplication-on-restore window that the leader's early resume had
before this change.

### Tests

- `barrier.rs`: gRPC Prepare→ack→Aligned→Commit flow (sequenced
  handshake because observation is latest-wins).
- `checkpoint_coordinator.rs`: `leader_announces_aligned_between_prepare_and_commit`
  (RecordingKv ordering), `restorable_gate_waits_for_async_follower_uploads`,
  `follower_prepare_failure_overwrites_capture_ack`, plus the
  existing gate/registry/abort suite re-validated.
- `pipeline_callback.rs`: resume-gate release on Aligned / newer
  epoch / no-shuffle; `FollowerTailState` lifecycle; dedup matrix.
- `tests/cluster_integration.rs` (16) and laminar-core
  `cluster_integration` (9): unchanged and green — the two-node 2PC
  mirrors exercise the new protocol end-to-end in KV mode.

## Phase 2 — Pipelined epochs (IMPLEMENTED)

Barrier cadence is decoupled from upload completion; epochs overlap
between `Aligned` and restorable, bounded by
`CheckpointConfig::max_in_flight_epochs` (default 4) and
`max_staged_bytes` (default 512 MiB; staging is in-memory `Bytes` —
the ADR allows "local disk (or memory)", and the byte cap is the
backlog bound). Admission lives in the streaming coordinator and
pauses at either cap (cadence degrades to upload speed).

What made it possible:
- **Allocate-at-admission ids** (`EpochAllocator`, lock-free): a failed
  epoch is *abandoned* (Flink semantics), never retried under the same
  ids, so an epoch's identity no longer depends on its predecessor's
  outcome. Failure paths consolidated in `fail_epoch` (announce Abort,
  rollback, begin the next epoch's sink transactions bounded by
  `rollback_timeout`).
- **Two-stage tails**: the capture quorum + `Aligned` announce run
  *before* the coordinator mutex (`run_prepare_quorum`), so resume is
  never queued behind an earlier epoch's uploads; followers ack
  pre-mutex for the same reason. The durable remainder serializes on
  the FIFO mutex → in-order restorability.
- **Mislabel guard**: `checkpoint_with_barrier` carries the barrier
  round's `checkpoint_id`; a slow follower round whose announcement
  was superseded (possible only with abandonment) is rejected instead
  of attributing its offsets to the newer epoch.

v1 scope notes (deliberate):
- Exactly-once pipelines are capped at depth 1 (a single-open-
  transaction sink cannot overlap epochs). Per-sink
  `max_in_flight_epochs` capability + producer pooling = follow-up.
- Follower tails serialize uploads *and* decision waits on their
  coordinator mutex; their backlog is bounded by the leader's caps.
  Splitting follower upload/commit drivers = follow-up.
- Concurrent quorum rounds (cadence < quorum RTT) can waste an epoch
  (one round's Prepare masks the other under latest-wins observation;
  the loser aborts via quorum timeout) — safe, documented.
- Tails enqueue on the FIFO mutex in spawn order, which matches
  admission order in practice but is not scheduler-guaranteed; an
  inversion is benign at depth > 1 because only at-least-once
  pipelines pipeline (full snapshots are independent and recovery
  takes the highest restorable manifest). Exactly-once stays depth 1.
- Depth > 1 fault injection is covered at the coordinator level
  (`overlapping_epoch_failure_is_isolated`: four overlapping tails,
  one epoch's upload partially fails → abandoned; successors commit;
  no reference partial ever points at the failed epoch). A two-node
  end-to-end depth > 1 harness remains follow-up.

## Phase 3 — Incremental snapshots (v1 IMPLEMENTED)

Per-vnode artifacts gained the `Full | reference` kind
(`VnodePartial.base_epoch`): a vnode whose serialized slices are
byte-identical to its last full upload writes a tiny reference
instead of re-uploading state — upload cost ∝ changed vnodes, which
is the dominant win under key skew. References resolve in one hop
(never chain), are forced back to full before the base ages out of
the `max_retained` prune window (prune only runs after a successful
checkpoint, so a restorable epoch's references are always above the
horizon), and bases are recorded only after every write in the epoch
lands. Counter: `checkpoint_unchanged_vnodes_total`.

Follow-up (the ADR's full vision): group-level delta changelogs
inside the agg operator (`Delta{base_epoch}` carrying changed/removed
groups), compaction every K epochs, chain reconstruction on
rehydration, and reference-liveness-aware pruning. The artifact kind
and reader plumbing landed here are forward-compatible with it.

## Phase 4 — Floor + polling removal (IMPLEMENTED)

- Cluster checkpoint-interval floor lowered 2s → 100ms; the Phase-2
  admission caps are the runtime guard (a tight interval degrades to
  upload speed at the caps instead of building an unbounded backlog).
- The follower decision wait (was a flat 50ms poll) and the Aligned
  resume gate (was 10ms) are push-driven off the gRPC announcement
  watch (`ClusterController::wait_for_barrier`), with a 250ms
  KV-fallback poll (25ms when no gRPC server is wired — gossip-only
  deployments).
- The per-cycle `observe_barrier` Prepare pickup remains poll-per-cycle
  (cheap in-memory read; the micro-batch cycle time bounds barrier
  frequency, as the ADR notes).

## Production readiness (assessed 2026-06-10)

Code-complete and internally verified (unit, two-node in-process
cluster, coordinator-level fault injection at depth > 1, wedged-sink
and abandoned-epoch coverage). **Not yet validated for production**;
gate on:

1. **Real multi-node soak** — HARNESS BUILT + PASSING:
   `cluster_soak.rs` (laminar-server) spawns 3 real binaries over the
   real gRPC control plane with a deterministic `generator` workload
   and kills nodes round-robin mid-epoch. Verified runs: 3 fault
   rounds, every node (incl. leader) killed + rejoined, epochs
   committing throughout (2 → 16). Node logs confirm cross-node
   shuffle alignment and pipelined epochs (N+1..N+3 aligning while
   N's tail held the durability gate). Remaining before flipping the
   100ms floor on in production: long-duration runs (hours, via
   `LAMINAR_SOAK_SECONDS`), 100ms cadence (`LAMINAR_SOAK_INTERVAL_MS`),
   MinIO/S3 backend (`LAMINAR_SOAK_CHECKPOINT_URL` + `_S3_*`), and an
   exactly-once sink-output diff (needs a transactional sink in the
   workload; the generator is deterministic precisely so that check is
   a recompute-and-compare).
2. **Depth > 1 end-to-end**: exercised implicitly by the soak
   (at-least-once workload, default depth 4 — logs show overlapping
   epochs); a targeted cross-node depth assertion is follow-up.
3. ~~Cap tuning exposure~~ — DONE: `max_in_flight_epochs` /
   `max_staged_bytes` are settable in the server `[checkpoint]` TOML.
4. ~~Aborted-manifest recovery audit~~ — DONE: recovery already skips
   Pending-sink manifests (tested); the real hazard found was recovery
   walking allocator ids *back* below an aborted epoch's seed,
   re-allocating it over stale artifacts — ids are now strictly
   monotonic (`recovery_never_walks_ids_back_onto_aborted_epochs`).

Operationally: 100ms is permission, not a promise — quorum RTT +
capture must fit the interval, and at the caps cadence degrades to
upload speed. Two documented benign races: a stale `Aligned` from an
attempt aborted post-quorum can release a successor's resume gate
early, and overlapping quorum rounds (cadence < quorum RTT) burn an
epoch.
## 100ms-cadence soak findings (2026-06-11)

The kill -9 soak at 100ms cadence (vs the passing 500ms runs) peeled
four distinct defects; three are fixed (`e0e95242`), one is open:

1. FIXED — serial doomed-gate burn: in-flight epochs each waited the
   full 30s durability gate for a dead node''s uploads, serialized on
   the coordinator mutex (depth 4 → ~2min commit stall). Gate now
   fail-fasts on unhealthy capture participants.
2. FIXED — gossip detection lag: membership can show a killed node
   Active for >30s, muting fix 1. Capture-quorum misses now mark peers
   unresponsive on the controller (TTL 60s, cleared on ack); gates
   consult it.
3. FIXED — quorum misclassification: connection-refused RPC errors
   counted as `Failed` (live NACK) instead of unreachable, bypassing
   the unresponsive signal. Transport errors now classify into
   `TimedOut{missing}`.
4. FIXED (`dc4376bd`) — rejoin lockstep livelock:
   a restarted node can start aligning checkpoint N after peers already
   sent their N barriers (lost while its transport was down), time out
   the full 30s, and land one epoch behind forever; the leader''s
   quorums miss its ack every epoch and nothing commits. The newer
   Prepare(N+1)/Abort(N) announcement releases the alignment wait
   *between* attempts but NOT mid-wait — `align_shuffle_barriers` (or
   its pipeline_callback wrapper) needs an announcement-driven release
   inside the wait loop. Soak evidence: run `soak-225504`, node0
   aligning ckpt 15 19:39:41→19:40:11 straight through Abort(15) +
   Prepare(16) at 19:39:44. Repro: `LAMINAR_SOAK_INTERVAL_MS=100`,
   fails within ~10 fault rounds at "progress after rejoin".

Survival progression at 100ms as fixes landed: 2 → 5 → 8 full fault
rounds. 500ms cadence: passes (3+ rounds, all nodes). Fix item 4, then
re-run 900s+ at 100ms until clean, before production sign-off on the
100ms floor.
### Remaining open defect (5) — orphaned-vnode window

Epochs admitted after membership drops a dead node but before the
(debounced, barrier-gated) rebalance rotates its vnodes away are
unsealable: every capture participant is healthy (no fail-fast
applies), but the dead node''s vnodes were captured by nobody, so the
durability gate burns its full 30s per epoch until rotation lands
(`set_vnode_set` → rotation floor then clears the backlog). Soak
evidence: run `soak-254772`, epoch 14 gate burn while epochs 15-18
reached quorum two-node. Candidate fixes, in design order:
(a) pause barrier admission while live membership disagrees with
vnode-assignment ownership (cheap, bounded stall = rotation latency;
matches the existing admission-cap machinery), or (b) gate epochs on
the vnodes owned by their capture participants only — protocol-level
change to `epoch_complete` semantics; interacts with recovery and
rehydration expectations of a sealed epoch. Also worth measuring: the
rebalance controller''s rotation latency after a hard kill — it waits
on a checkpoint barrier that may itself be stuck behind doomed gates.

At 100ms the soak now survives kills and rejoins until it hits this
window (~round 4); at 500ms all rounds pass. Sign-off order: fix (5),
then 900s+ clean at 100ms, then hours-long + MinIO + exactly-once
sink diff.