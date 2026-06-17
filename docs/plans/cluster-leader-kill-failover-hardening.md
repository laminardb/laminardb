# Cluster leader-kill failover hardening

**Status:** NOT STARTED — diagnosed 2026-06-17, scheduled for a dedicated session.
**Scope:** pre-existing cluster-checkpoint fault-tolerance gap, independent of the
coordinated-sink-commit / lease work that surfaced it. Do NOT bundle into that PR.

## Problem

After a node is `kill -9`'d, the surviving cluster cannot complete a checkpoint for a
long, sometimes unbounded window. The three-node real-binary soak
(`crates/laminar-server/tests/cluster_soak.rs`, `three_node_kill9_soak`) times out
("progress after kill: cluster commits to reach N") on the first kill, under **both**
discovery modes. No data loss (the gate correctly aborts doomed epochs); the cluster
just can't make progress.

This is NOT caused by the leader lease (`is_leader` fencing) — a baseline at the
pre-lease commit fails identically. The lease failover itself works (a new candidate
acquires the lease and acts as leader).

## Evidence (soak node logs)

Three layers, peeled in order:

1. **Barrier announce to departed peers — FIXED (`0253d6e3`).** `BarrierCoordinator::announce`
   targeted `kv.scan(BARRIER_ADDR_KEY)` (every node that ever registered an addr, never
   pruned) → Commit/Abort RPC to the dead node returns Err → epoch aborts forever. Now
   filtered to `Active` members via the `leader_election` watch (quorum/Prepare already
   used `live_instances()`).

2. **State durability gate waits on the dead node's vnodes — OPEN.**
   `LDB-6020 state durability gate timed out: not all vnodes persisted` (vnodes=64). The
   dead node's ~30 vnodes can't persist until rebalance reassigns them, and reassignment
   waits for the node to leave membership: **~14s static** (`dead_threshold=10`) /
   **~60s gossip** (`dead_node_grace_period`). Gate timeout is 10s/epoch, so it fails
   every epoch for that whole window.

3. **Shuffle barrier alignment stuck AFTER rebalance — OPEN (the real wedge).**
   `LDB-8002 shuffle barrier alignment timed out` persists for ~60s *after*
   rebalance/rehydration completes and the dead node is gone from the shuffle peer set —
   a post-reconfiguration barrier-lost state. Almost certainly the known-incomplete
   cross-node alignment work (see `cross-node-barrier-alignment` memory: Phase 3
   "restore-with-in-flight" was never closed).

Discovery mode does not help: gossip is slower than static (60s grace vs 14s). The soak
now defaults to gossip (`461d60f2`); `LAMINAR_SOAK_DISCOVERY=static` runs the other path.

## Fix plan (next session)

Layer 2 — shed dead nodes faster:
- Trigger rebalance / vnode-shed on `NodeState::Suspected`, not only on full removal, so
  the dead node's vnodes are reassigned within suspect-time (~3s) instead of the dead/grace
  window. Check `assignable_instances` / the rebalance controller trigger
  (`crates/laminar-db/src/rebalance.rs`) and the gate's vnode set
  (`checkpoint_coordinator.rs` `gate_vnode_set`).
- Tune `dead_node_grace_period` (gossip, `cluster.rs` ~line 381) / `dead_threshold`
  (static) — lower defaults trade GC-pause tolerance for faster failover.

Layer 3 — shuffle alignment stuck-state (the hard part):
- Reproduce in isolation; this is the cross-node barrier alignment Phase-3 gap. A
  reconfiguration (rebalance) mid-epoch drops an in-flight barrier so the new topology
  waits forever. Needs the restore-with-in-flight handling that was deferred.

## Validation

```
cargo test -p laminar-server --features cluster --test cluster_soak \
  three_node_kill9_soak -- --ignored --nocapture
# env: OpenSSL vars + RUST_MIN_STACK=67108864; knobs LAMINAR_SOAK_SECONDS,
# LAMINAR_SOAK_INTERVAL_MS, LAMINAR_SOAK_DISCOVERY=gossip|static
```
Green = the cluster commits ≥2 checkpoints within the 90s window after every kill, across
several round-robin rounds (leader = lowest node-id HASH = n2, not n0). Follower kills
(rounds 1–2) are easier than the leader kill (round 3).

## Risk

Touches the highest-risk core (checkpoint / 2PC / shuffle / rebalance). Must not regress
the working gossip-mode steady state, the coordinated-commit/lease work, or exactly-once
recovery. Validate with the existing kill-9 exactly-once soak path too.
