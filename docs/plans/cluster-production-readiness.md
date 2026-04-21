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
- Dynamic vnode rebalance on membership change. `AssignmentSnapshotStore`
  holds versioned snapshots (`v{N}.json` each a `PutMode::Create` CAS)
  so rotation is backend-agnostic. Every node runs a snapshot watcher
  that adopts newer versions into its `VnodeRegistry`; the leader
  additionally runs a debounced rebalance controller that forces a
  checkpoint (draining in-flight shuffle rows into durable state) and
  then CAS-writes the next snapshot. Racing leaders both CAS at
  `v{N+1}`; one wins, the loser adopts.

## Known gaps

1. **No TLS / no peer auth** on shuffle.
2. **Shuffle backpressure limited to local per-partition `mpsc(16)`.**
   No cluster-wide credit flow.
3. **Admin surface:** no `/cluster/ownership`, `/cluster/checkpoints`,
   `/cluster/drain` endpoints.
4. **Graceful drain:** `ClusterHandle::wait_for_shutdown` does not
   guarantee a final epoch before stopping discovery.

## Remaining work, grouped

**Operational.** Add #3 (admin API). Everything else can ship behind
flags; nothing below is a correctness blocker.

**Security / polish.** #1, #2, #4 — separate PRs, not blockers for a
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
harness` on this branch was aspirational. With the
`assignment_version` fence wiring (2026-04-21), cross-instance sink
2PC (2026-04-21), and dynamic rebalance (2026-04-21) landed, all
correctness blockers are closed and the cluster can live-resize.
What remains is operational polish (admin API, graceful drain) and
security (TLS, cluster-wide backpressure) — none of which block a
small-cluster rollout.
