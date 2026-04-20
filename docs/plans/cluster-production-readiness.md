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

## Known gaps

1. **Sink 2PC does not coordinate across instances.** Each node
   commits its own sinks locally. A leader that crashes between
   follower sink-prepare and follower sink-commit leaves sinks in
   inconsistent state.
2. **Assignment version does not persist across restarts.** On boot
   the coordinator's `assignment_version` resets to 0, defeating the
   Phase 1.4 fence until the first `AssignmentSnapshot` reload.
3. **Dynamic rebalance not implemented.** Vnode assignment is frozen
   at boot. Node join/leave does not re-shuffle.
4. **No TLS / no peer auth** on shuffle.
5. **Shuffle backpressure limited to local per-partition `mpsc(16)`.**
   No cluster-wide credit flow.
6. **Admin surface:** no `/cluster/ownership`, `/cluster/checkpoints`,
   `/cluster/drain` endpoints.
7. **Graceful drain:** `ClusterHandle::wait_for_shutdown` does not
   guarantee a final epoch before stopping discovery.

## Remaining work, grouped

**Correctness.** Fix #1 (cross-instance sink 2PC) and #2 (persist
`assignment_version` across restarts). Everything else can ship
behind flags; these two cannot.

**Operational.** Fix #3 (dynamic rebalance) together with source
replay for in-flight shuffle buffers. Add #6 (admin API).

**Security / polish.** #4, #5, #7 — separate PRs, not blockers for a
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

The commit message `Phase A+B+C.1–C.5 primitives, wiring, harness`
on this branch is aspirational. A more accurate summary:
**cluster foundations — shuffle bridge wired, cross-instance sink 2PC
deferred.** The Phase 0a + 0b scope in earlier drafts of this doc is
what actually landed; Phase 1 and Phase 2.1 remain open per the gaps
above.
