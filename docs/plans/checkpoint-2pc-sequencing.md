# Checkpoint 2PC sequencing (leader / follower)

Sub-doc of `distributed-stateful-pipelines.md` (§7, §9) and
`two-phase-ordering.md`. Choreographs the existing primitives
(`BarrierCoordinator`, `ClusterKv`, `state_backend.epoch_complete`)
into a leader/follower 2PC flow inside `CheckpointCoordinator`.

## Scope split

The handoff's item (1) is split because the durability gate is a no-op
until vnodes are wired — building 2PC around a vacuous gate ships
protocol theater.

- **(1a) Wire `vnode_set` from `VnodeRegistry`.** Populate the gate.
  One instance, one owned-vnode set. Gate becomes load-bearing:
  `epoch_complete(owned_vnodes)` actually checks partials on disk.
  Unblocks a real single-node durability test and gives (1b)
  something to gate on.
- **(1b) Leader/follower 2PC.** The ordering below. Cluster-mode only.

## Ordering

```
         LEADER                                FOLLOWER(s)
         ──────                                ───────────
  tick (leader only) → checkpoint_inner
       pre_commit_sinks + save_manifest
       announce(PREPARE, epoch) ───gossip──▶   observes PREPARE
                                                 pre_commit_sinks + save_manifest
                                                 ack(epoch, ok)
       wait_for_quorum ◀──────gossip───────────
       epoch_complete(owned_vnodes) ─── local to leader
              │
       ┌──────┴──────┐
       ▼             ▼
    COMMIT        ABORT  (announce either)
       │             │
  commit_sinks   rollback_sinks                 mirror: on COMMIT → commit_sinks
                                                        on ABORT  → rollback_sinks
```

Three gossip round-trips happy-path (PREPARE, ack, COMMIT), ~50–150 ms each.

## Phase

`barrier.rs` gains a plain enum serialized into the existing
`BarrierAnnouncement.flags` field (reuse, don't add a KV key):

```rust
#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Phase { Prepare = 1, Commit = 2, Abort = 3 }
```

Not bitflags — phases are mutually exclusive.

## Leader flow

One new block in `checkpoint_inner`, after `save_manifest` and before
the existing `epoch_complete` call:

1. If `!is_leader()`, return — caller shouldn't have invoked us.
2. `announce(Prepare, epoch)`.
3. `wait_for_quorum(epoch, followers \ self, 30s)`.
4. On `TimedOut | Failed`: `announce(Abort)`, `rollback_sinks`, return failed.
5. Fall through to existing `epoch_complete(owned_vnodes)` gate.
6. On gate fail: `announce(Abort)`, `rollback_sinks`, return failed.
7. `announce(Commit)` (best-effort), then existing commit block.

The caller (`pipeline_callback::checkpoint_loop`) gains one line:
`if controller.as_ref().is_some_and(|c| !c.is_leader()) { skip }`.

## Follower flow

A role-aware branch in the same periodic tick — **not** a separate
struct. Same task, conditional on `is_leader()`:

```
loop every 50ms:
  if is_leader() { leader_tick(); continue }
  let Some(ann) = observe_barrier() else continue;
  dispatch on (ann.phase, local_status):
    (Prepare, None)             → follower_prepare(ann.epoch); ack(ok|err)
    (Commit,  Prepared(e==ann)) → follower_commit(e)
    (Abort,   Prepared(e==ann)) → follower_abort(e)
    _                            → ignore (stale / out-of-order)
```

`follower_prepare` / `follower_commit` / `follower_abort` are three
thin methods on `CheckpointCoordinator` that reuse the existing
`pre_commit_sinks` / `save_manifest` / `commit_sinks_tracked` /
`rollback_sinks` blocks verbatim.

## Recovery (manifest-driven)

On startup, read the latest manifest. If it is for a prepared-not-
committed epoch (new `CheckpointPhaseOnDisk::Prepared`), the
inheriting leader announces `Abort` for that epoch before starting
a new one. Runtime role flips do not carry in-memory Prepared state
forward — everything flows through the manifest so cascading crashes
are handled identically.

This adds one column to the manifest and one startup check. No new
coordination surface.

## Failure matrix

| Case | Outcome |
|------|---------|
| Follower snapshot fails | Ack `ok=false` → leader sees `Failed` → Abort |
| Follower silent (crash / partition) | `wait_for_quorum` times out → Abort |
| Leader crashes after Prepare, before Commit/Abort | New leader on startup sees Prepared manifest → announces Abort for that epoch |
| Leader crashes after Commit local, follower has not yet mirrored | Broker-side transaction timeout cleans up unmirrored precommits (parent design §9) |
| Network partition mid-quorum | Abort on timeout; follower drops stale prepared state on next observed epoch |
| Stale ack from prior epoch | `wait_for_quorum` filters by epoch (already tested) |

## Tech debt called out

Chitchat gossip (eventually-consistent SWIM) is the wrong substrate
for hot-path control-plane RPCs at every checkpoint interval. Fine at
~3 nodes, degrades at scale. Migration target: once
`ShuffleSender/Receiver` lands (handoff item 2), the TCP pool carries
control frames too. `BarrierCoordinator`'s `ClusterKv` seam is
explicitly for this swap; `InMemoryKv` remains for tests.

## What (1a) + (1b) explicitly do NOT land

- Unaligned checkpoints (parent design §A.2)
- Durable checkpoint-id sequencer across leader changes (successor
  reads last manifest's id; good enough)
- Shuffle-integrated barriers (separate seam, arrives with item 2)
