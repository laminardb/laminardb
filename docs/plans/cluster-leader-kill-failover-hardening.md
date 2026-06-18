# Cluster leader-kill failover hardening

**Status:** FIXED + soak-validated 2026-06-17 (branch `feat/cluster-failover-hardening`).

## Problem

After a `kill -9`, the surviving cluster could stop committing checkpoints — the
real-binary soak (`crates/laminar-server/tests/cluster_soak.rs`,
`three_node_kill9_soak`) wedged on a kill or on a node rejoining, under both discovery
modes. No data loss (doomed epochs abort); just no progress. Timing-sensitive.

The wedge lived in cross-node shuffle barrier alignment and the surrounding failover
plumbing — independent of the coordinated-sink-commit / lease work that surfaced it.

## Root cause & fix

1. **Alignment fan-out ≠ wait set (kill deadlock).** Fan-out was derived from vnode
   ownership (`peer_owners`) while the wait set came from membership (`live`); during
   reconfiguration a node shipped its barrier to a stale/dead owner while a live peer
   waited for it forever. Fix: derive both from the same `live` set in
   `align_shuffle_barriers` (`operator_graph.rs`).

2. **Wait set couldn't shrink.** Replaced the fixed-count `BarrierTracker` with a
   `remaining` set that drops any peer leaving membership each tick (8s cap), so a peer
   that dies mid-align can't wedge the epoch.

3. **Dropped future-epoch barriers (follower rejoin).** A barrier for a later
   checkpoint received mid-align was dropped, so a lagging rejoiner never saw the
   barriers a faster peer already sent and was perpetually superseded. Fix: re-stash it
   (`ShuffleReceiver::stash_barrier`).

4. **Stale leader epoch (leader rejoin).** A reclaiming leader recovered to its last
   *committed* epoch, which lags the cluster's *in-flight* epoch, then re-announced ids
   caught-up followers skip. Fix: before allocating, advance the allocator past the
   cluster-wide max announced epoch (`ClusterController::max_announced_epoch`).

5. **Gossip never rebalanced (gossip kill wedge).** Gossip published the membership
   watch every tick unconditionally, so the rebalance controller's quiet-period
   debounce never settled and a dead node's vnodes were never shed (the gate then
   wedged). Fix: share `publish_if_changed` (already used by static) on the gossip path.

6. **Exactly-once violated on leader recovery (Kafka-diff soak).** The rebalance
   pre-rotation drain checkpoint runs through `db.checkpoint()` →
   `force_capture_and_checkpoint`, which built its request with *empty* source offsets.
   That checkpoint commits sinks and becomes a recovery point with no source offset, so
   a leader that recovers from it replays its source from the start and re-emits
   already-committed records (a killed leader produced 2684 duplicates). Fix: the forced
   checkpoint now captures the coordinator's current `committed_offsets` (consistent with
   the operator/sink state it snapshots), so recovery resumes at the right offset.

## Validation

```bash
cargo test -p laminar-server --features cluster --test cluster_soak \
  three_node_kill9_soak -- --ignored --nocapture
# env: OpenSSL vars + RUST_MIN_STACK=67108864; LAMINAR_SOAK_SECONDS,
# LAMINAR_SOAK_INTERVAL_MS, LAMINAR_SOAK_DISCOVERY=gossip|static
```

clippy clean; alignment + aligned-resume unit tests pass; `cluster_integration` 15/15
(2 MinIO-infra failures unrelated). Soak (`SECONDS=300 INTERVAL_MS=100`) green on both
**static** and **gossip** across all kill rounds. Exactly-once under rejoin is now
covered by the Kafka-diff soak (`LAMINAR_SOAK_KAFKA_BROKERS=...` rps=200): 6 kill rounds
(leader killed twice), all three per-node topics dense with zero duplicates/gaps under
`read_committed`. Reset the soak Redpanda (down/up, not restart) first — a broker
degraded by hours of kill-9 debris stalls sink commits and wedges the leader reclaim.

## Risk

Touches the highest-risk core (checkpoint / 2PC / shuffle / rebalance). Must not
regress steady state, the coordinated-commit/lease work, or exactly-once recovery.
