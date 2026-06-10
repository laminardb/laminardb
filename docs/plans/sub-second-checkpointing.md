# Sub-Second Checkpointing — Implementation Plan (ADR-003)

Companion to `docs/adr/ADR-003-sub-second-checkpointing.md`. Phases are
independently deployable and ordered by risk/value; each must keep
`cluster_integration` (laminar-db + laminar-core) green.

## Status

| Phase | Description | Status |
|-------|-------------|--------|
| 1 | Two-level completion — resume on `Aligned`, durable tail off the pipeline task | **DONE** (2026-06-10) |
| 2 | Local staging + async upload + virtual manifest commits | not started |
| 3 | Incremental (delta) snapshots, opt-in per operator | not started |
| 4 | Lower the 2s floor to ~100ms + adaptive guard; remove polling floors | not started |

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
- `BarrierAnnouncement.seq`: a leader-stamped monotonic announce
  sequence (stamped in `announce()` and per `wait_for_quorum` Prepare
  round; carried through the proto). The gRPC and KV values merge by
  `(epoch, seq)` — required because a retry of an aborted epoch reuses
  the same epoch/checkpoint id, so without `seq` a stale
  gRPC-delivered `Abort(N)` would mask the retry's gossiped
  `Prepare(N)` forever (livelock; regression test
  `kv_retry_prepare_supersedes_stale_grpc_abort`).
- `Aligned` fan-out is best-effort per peer: a missed delivery only
  delays that peer's resume until Commit or its gate timeout.

Coordinator (`laminar-db/src/checkpoint_coordinator.rs`):

- Leader (`checkpoint_inner`, cluster): `await_prepare_quorum` moved
  **before** the leader's own pre-commit/manifest/partial writes; on
  quorum it announces `Aligned`. The durability gate became
  `await_restorable_gate`: it **polls** `epoch_complete` (100ms, up to
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
  pipeline blocks only in `wait_for_aligned_resume` (10ms poll, 30s
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
| Follower capture/alignment fails | No ack → leader quorum timeout (3s) → Abort → retry same epoch |
| Follower prepare fails after ack | `ok=false` ack overwrite (KV fast path) or leader gate timeout → Abort; follower already rolled back its sinks |
| Leader upload/gate/marker fails after Aligned | Abort announced; pipelines already resumed — epoch N is simply not restorable; retry same epoch |
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

## Phase 2 — Local staging + async upload + virtual manifests

Goal: decouple barrier cadence from upload latency entirely; multiple
epochs in flight between Aligned and Restorable.

- Staging area `<data_dir>/staging/` (ephemeral; wiped at boot; the
  Helm `persistence.state` volume, never a new mount). Capture writes
  serialized state there (or memory) and the barrier acks immediately.
- A background uploader drains staged artifacts to the object store;
  caps on staged bytes/epochs backpressure the barrier cadence when
  hit (degrade to upload speed, never OOM).
- Manifest commits become *virtual*: an epoch is restorable when its
  manifest commits referencing uploaded artifacts (which may be shared
  across epochs). Restorability stays strictly in-order and gap-free
  (N commits only after N−1).
- Sinks declare `max_in_flight_epochs` (default 1 — a Kafka
  transactional producer cannot overlap epochs); visibility cadence
  for capability-1 sinks stays upload-bound until producer pooling.
- Fence: staged artifacts are stamped with the capture-time
  `assignment_version`; a `StaleVersion` rejection abandons the
  backlog (never re-stamp — the new owner rehydrates from the last
  restorable epoch).

## Phase 3 — Incremental snapshots

- `VnodePartial` v2: per-operator artifact kind `Full | Delta{base_epoch}`;
  mixed manifests valid (per-operator slices are independent).
- Opt-in per operator (agg operator first: dirty-group tracking
  between barriers); default stays full-snapshot.
- Compaction: full capture every K epochs bounds recovery replay and
  delta-chain length. Rebalance rehydration reconstructs base + chain.
- Pruning must track artifact liveness via manifest references, not
  `epoch < horizon` (interacts with the incremental prune cursor in
  `state/object_store.rs`).

## Phase 4 — Lower the floor

- Drop the 2s cluster minimum (`laminar-server/src/config.rs`) to
  100ms with a runtime adaptive guard (skip a barrier when the
  staging backlog or upload lag exceeds caps).
- Remove the polling floors: the follower decision wait (50ms) and
  the per-cycle `observe_barrier` become push-driven (the
  announcement `watch` from Phase 1 is the natural carrier); the
  resume-gate 10ms poll likewise.
- Micro-batch cycle time then bounds barrier frequency.
