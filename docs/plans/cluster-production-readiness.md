# Cluster production-readiness

Honest state of cluster mode on `feat/phase-c5-cluster-primitives`.

## What works today

- Chitchat gossip discovery with phi-accrual failure detection.
- `ShuffleSender` / `ShuffleReceiver`: TCP pool, `Hello` handshake,
  FIFO per connection, stale-entry purge.
- Row-shuffle bridge (`shuffle_pre_agg_batches`) wired into
  `SqlQueryOperator::lazy_init`. Streaming aggregates actually cross
  the wire.
- `ObjectStoreBackend` over S3/GCS/Azure/file://, with per-vnode
  marker writes, monotonic pointer fence on `latest.json`, split-brain
  detection on the `_COMMIT` marker.
- Chandy–Lamport `BarrierTracker` alignment.
- Coordinated vnode assignment via CAS-on-create `AssignmentSnapshot`
  in shared object storage (leader-only writer, CAS loser reloads).
- Cross-instance watermark fan-out over the barrier protocol.
- `assignment_version` fence on `write_partial` (split-brain guard).
- Backend fence wired from the stored `AssignmentSnapshot` generation
  on `db.start()` — the authoritative version survives restart instead
  of resetting to 0.
- Cross-instance sink 2PC via a durable `CheckpointDecisionStore`. The
  leader CAS-writes `Decision::Committed` to shared storage before
  announcing `Commit`, so a new leader elected mid-2PC recovers the
  cluster vote from the marker instead of defaulting to Abort.
  Every node consults the store on startup and drives local sinks to
  the recorded verdict; a follower whose `Commit` announcement is
  lost times out and consults the store rather than unconditionally
  rolling back.

## Known gaps

1. **Dynamic rebalance not implemented.** Vnode assignment is frozen
   at boot. Node join/leave does not re-shuffle.
2. **No TLS / no peer auth** on shuffle.
3. **Shuffle backpressure limited to local per-partition `mpsc(16)`.**
   No cluster-wide credit flow.
4. **Admin surface:** no `/cluster/ownership`, `/cluster/checkpoints`,
   `/cluster/drain` endpoints.
5. **Graceful drain:** `ClusterHandle::wait_for_shutdown` does not
   guarantee a final epoch before stopping discovery.

## Remaining work, grouped

**Operational.** Fix #1 (dynamic rebalance) together with source
replay for in-flight shuffle buffers. Add #4 (admin API).

**Security / polish.** #2, #3, #5 — separate PRs, not blockers for a
small-cluster rollout.

## What this PR closes

- Row-shuffle bridge is wired into the streaming aggregate path.
- `AssignmentSnapshot` CAS-create is in.
- Split-brain fence on `_COMMIT` is in.
- Stale shuffle-connection purge is in.
- The cluster-wide watermark rides the barrier protocol.
- `DistributedAggregateRule` + `CheckpointedRepartitionExec` deleted
  — they were scaffolding for a DF-native cluster-aggregate path
  that conflicted with row-shuffle and had no owner. Git history
  preserves both if a non-streaming distributed-aggregate path ever
  needs them.

## Honest naming

The original commit message `Phase A+B+C.1–C.5 primitives, wiring,
harness` on this branch was aspirational. With the `assignment_version`
fence wiring (2026-04-21) and cross-instance sink 2PC (2026-04-21)
landed, both correctness blockers are closed; what remains is
operational and security work that can ship behind flags for a
small-cluster rollout.
