# Phase C.5 handoff — resume this in a fresh session

**Status at checkpoint:** Primitives, wiring, and failure-scenario
harness complete. Three session-sized items remain. Read this doc
top to bottom to resume; follow the specific instructions at the
end to pick up productive work immediately.

---

## The design corpus (read these first)

Everything of substance lives in `docs/plans/`:

| Doc | What it is |
|---|---|
| `docs/plans/distributed-stateful-pipelines.md` | **Master design.** v2.2. Ten architectural decisions + connector landscape + Addendum A on latency. Source of truth. |
| `docs/plans/shuffle-protocol.md` | Wire format, flow control, ordering, connection management for cross-instance shuffle. v1 locked. |
| `docs/plans/two-phase-ordering.md` | The sink-precommit → state-commit → sink-commit sequencing for distributed 2PC. Gates SB-08 behavior. |
| `docs/plans/phase-c5-handoff.md` | This file. |

Skim the three design docs in order; you'll have enough context to
resume in ~15 minutes.

---

## What's shipped

### Phase A — cleanups + rename
- `delta/` module → `cluster/` (git-tracked rename preserving history)
- `mode = "delta"` → `mode = "cluster"` in server config
- `DeltaManager` → `ClusterManager`, `DeltaHandle` → `ClusterHandle`, `Profile::Delta` → `Profile::Cluster`
- Deleted: `PartialAggregate` + 6 impls (redundant with DF `Accumulator::state()`), `rendezvous_hash` (xxh3 modulo works), `ConsistentHashAssigner` (Kafka consumer group + per-connector dispatch supersede)
- `VNODE_COUNT` const → runtime `DEFAULT_VNODE_COUNT` + `validate(expected_vnode_count)` takes the runtime value
- Added `key_hash(bytes) -> u64` using `xxhash_rust::xxh3::xxh3_64`

### Phase B — checkpoint gate + metrics
- `CheckpointCoordinator` gained:
  - `state_backend: Option<Arc<dyn StateBackend>>` field
  - `vnode_set: Vec<u32>` field
  - Gate block between `Persisting` success and `Committing` that calls `state.epoch_complete(epoch, &vnodes)` if both are populated
  - Error codes `LDB-6020`..`LDB-6023` for gate misses
- `validate_config` rejects `checkpoint.interval < 2s` in cluster mode
- New metrics: `barrier_alignment_wait_seconds{operator}`, `watermark_propagation_seconds`, `pipeline_e2e_latency_seconds{pipeline}`

### Phase C.1/C.2/C.4 — primitives + dispatch (all under `crates/laminar-core/src/cluster/`)
- `control/leader.rs` — `leader_of(&[NodeId]) -> Option<NodeId>` (lowest non-sentinel)
- `control/snapshot.rs` — `AssignmentSnapshot` + `AssignmentSnapshotStore` over `object_store`
- `control/barrier.rs` — `ClusterKv` trait + `InMemoryKv` + `BarrierAnnouncement` + `BarrierAck` + `QuorumOutcome` + `BarrierCoordinator`
- `control/chitchat_kv.rs` — production `ChitchatKv` impl (feature-gated)
- `control/controller.rs` — `ClusterController` facade composing all of the above
- `split.rs` — `SplitId`, `SplitShape::{External, Enumerable, Singleton}`, `plan_assignment`

### Phase C.3 — shuffle primitives (`crates/laminar-core/src/shuffle/`)
- `message.rs` — `ShuffleMessage` + `read_message` / `write_message` over any `AsyncRead + AsyncWrite`; tag bytes (`0x01 Data`, `0x02 Barrier`, `0x03 Credit`, `0xFF Close`); Arrow IPC payload for Data; 64 MiB cap
- `flow.rs` — `CreditSemaphore` (bytes-denominated, acquire/grant/close)

### Phase C.5 — wiring + harness (so far)
- `LaminarDbBuilder::cluster_controller(Arc<ClusterController>)` method
- `LaminarDB.cluster_controller` field
- `CheckpointCoordinator::set_cluster_controller` + `cluster_controller()` accessor
- `pipeline_lifecycle.rs` installs the controller on coord construction
- `laminar-server/src/cluster.rs::start_cluster` constructs `ChitchatKv` → `ClusterController` from gossip discovery's chitchat handle
- `cluster::testing` module — `MiniCluster` with `spawn`, `spawn_partitionable`, `spawn_with_snapshot`, `join_node`, `kill` (clean-leave via `Left` announce + 150 ms gossip pause), `crash` (drop without announcing)
- `PartitionableTransport` — wraps chitchat's `UdpTransport`; `NetworkRules::{partition, heal, drop_pair}`
- `FaultyObjectStore` — wraps `Arc<dyn ObjectStore>` with runtime-flippable `ObjectStoreFault` enum
- `GossipDiscovery::start_with_transport<T>` — lets harness inject the partitionable transport
- `GossipDiscovery::chitchat_handle()` — accessor so other components share the chitchat instance
- Fixed `ClusterController::current_leader()` / `live_instances()` to filter by `NodeState::Active` (was a real bug the failover test caught)
- Fixed membership watcher to prefer `Active` when the same `NodeId` has multiple ChitchatId entries (rejoin window)

### Integration tests (all green, stable)
`crates/laminar-core/tests/`:

| File | Tests | Runtime |
|---|---|---|
| `cluster_integration.rs` | 9 pass + 1 ignored (same-id rejoin, chitchat quirk) | ~3.35 s |
| `shuffle_tcp_integration.rs` | 4 pass | ~0.22 s |
| `state_backend_integration.rs` | 3 pass | ~0.10 s |

Run with:
```bash
cargo test -p laminar-core --features cluster-unstable --tests
```

All of them pass in ~6.5 s total. 5× consecutive stable.

---

## The three remaining items

### (1) CheckpointCoordinator leader/follower flow — BIGGEST, MOST IMPACTFUL

**Status:** `ClusterController` is plumbed onto `CheckpointCoordinator` (field + setter wired end-to-end) but **not consumed in `checkpoint_inner()`**. Single-instance behavior is unchanged; multi-instance is currently N independent coordinators, not a coordinated 2PC.

**What needs building:**

In `crates/laminar-db/src/checkpoint_coordinator.rs::checkpoint_inner()`, between the current `Persisting` success and `Committing` steps:

```text
if let Some(cc) = &self.cluster_controller {
    if cc.is_leader() {
        // Leader path:
        cc.announce_barrier(&BarrierAnnouncement { epoch, checkpoint_id, flags }).await?;
        let expected = cc.live_instances();
        let followers: Vec<_> = expected.iter().filter(|id| **id != cc.instance_id()).copied().collect();
        match cc.wait_for_quorum(epoch, &followers, Duration::from_secs(30)).await {
            QuorumOutcome::Reached { .. } => { /* proceed to commit_sinks */ }
            QuorumOutcome::TimedOut { .. } | QuorumOutcome::Failed { .. } => {
                // Abort epoch; rollback sinks; record in metrics.
            }
        }
    } else {
        // Follower path: we don't drive checkpoints — the leader does.
        // Skip this invocation; the periodic trigger should be gated
        // on `is_leader()` upstream.
    }
}
```

Complications to handle:
- Follower-side `observe_barrier()` loop is currently missing — needs a separate background task or check in the periodic trigger
- Sink commit for exactly-once MUST happen in lockstep across instances. Currently each instance commits its own sinks independently. Either (a) only leader commits, or (b) followers commit in parallel after receiving a "go" signal from leader. Design doc §9 says all instances commit; the gate is the quorum wait.
- Ack from followers must happen AFTER state snapshot AND sink precommit — need to ensure both complete before `ack_barrier`
- Rollback on quorum miss must unwind both local state snapshot AND the sink precommit

**Suggested session plan:**
1. Write a short "leader-follower sequencing" design note (sub-doc under `docs/plans/`) that walks each failure case
2. Implement leader-path first (followers don't trigger checkpoints at all in single-writer mode; easy to test with a trivial 1-node cluster)
3. Add follower-path: background task on each non-leader instance that polls `observe_barrier`, snapshots, acks
4. New integration tests in `cluster_integration.rs` once both halves work

**Estimated scope:** ~500 LOC + 3-5 new integration tests. Real 1-week slice.

### (2) `ShuffleSender` + `ShuffleReceiver` high-level types — MEDIUM

**Status:** TCP + codec + credit flow proven via `shuffle_tcp_integration.rs`. Higher-level pool is mechanical.

**What needs building:**

`crates/laminar-core/src/shuffle/` gets three new files:

- `connection.rs` — `ShuffleConnection` wrapping one `TcpStream`: holds write half behind a mutex, spawns reader task that pushes to an `mpsc`, exposes `send(msg) -> Result`. Hello handshake as the first message (add `TAG_HELLO = 0x04` to `message.rs` carrying a u64 `NodeId`).
- `sender.rs` — `ShuffleSender` with a `DashMap<NodeId, Arc<ShuffleConnection>>` pool, peer-address registry (read from chitchat KV at `members/<id>/shuffle_addr`), lazy connect on first `send(peer, msg)`, idle-timeout eviction.
- `receiver.rs` — `ShuffleReceiver` wraps `TcpListener`, spawns accept loop, reads hello to identify peer, pushes `(from_id, msg)` onto a shared channel the operator consumes.

Integration test: two instances on loopback, bidirectional send, assert all messages delivered FIFO with correct sender attribution.

**Estimated scope:** ~400 LOC + 2-3 tests. 3-4 day slice.

### (3) Testcontainers-based Kafka scenarios — SEPARATE INFRA

**Status:** Not started. Requires `testcontainers` crate + Redpanda image.

**What's needed:**
- New feature flag `integration-docker` (or similar) — off by default in CI until infra is wired
- `MiniCluster::spawn_with_kafka(n, broker)` helper that spins Redpanda via testcontainers
- Tests: broker-kill mid-checkpoint, consumer-rebalance mid-checkpoint
- Blocks on (1) being done first — without the 2PC flow, these scenarios have nothing meaningful to observe

**Estimated scope:** ~1 week. Best picked up after (1) lands.

---

## Key context / gotchas for the resuming session

### Feature flag structure
- `laminar-core/cluster-unstable` pulls in `chitchat`, `tokio-util`, `anyhow`, `futures`
- `laminar-db/cluster-unstable` → `laminar-core/cluster-unstable`
- `laminar-server/cluster-unstable` → `laminar-db/cluster-unstable` + `laminar-core/cluster-unstable` + `dep:uuid` + `dep:xxhash-rust`
- Default build (no features) skips all cluster code — important for Windows CI (OpenSSL perl issue avoided via `--no-default-features`)

### Test-harness quirks
- Chitchat's default phi-accrual is too slow for tests (~30 s to flag a dead node). Harness uses `phi_threshold: 3.0`, `gossip_interval: 50 ms`, `dead_node_grace_period: 1 s`. Clean-leave via `kill()` announces `state = Left` before socket close → ~1 s failover.
- **Same-`node_id` rejoin** has a chitchat quirk: the old `Left` entry blocks gossip of the new `Active` entry. Documented in the ignored test `killed_node_can_rejoin`. Fix would be an explicit reap primitive (~100 LOC follow-up).
- Fresh-id join works (`fresh_node_can_join_running_cluster`) — which is the production k8s pod-rollover path anyway.

### Known review-driven principles (apply to new code)
1. **No speculative traits.** Extract a trait only when a second impl is needed. Current `ClusterKv` is justified (in-memory + chitchat); don't add more.
2. **No `ShuffleTransport` trait.** Concrete `ShuffleSender`/`Receiver`. Codec is transport-agnostic via `AsyncRead + AsyncWrite` already.
3. **Reuse existing machinery.** `PartitionGuardSet` (`cluster/partition/guard.rs`) is the fence primitive; don't reinvent. DataFusion `Accumulator::state()` is the distributed-aggregate monoid; don't wrap it.
4. **Design-doc-before-code for protocol changes.** The 2PC flow change (item 1 above) MUST have a short sequencing design note landing as a sub-doc before code. Otherwise every subtle failure case becomes a code review debate.

### Files with comments that point at future work
- `checkpoint_coordinator.rs` — `set_cluster_controller` + `cluster_controller()` methods are `#[allow(dead_code)]` with "C.5-final protocol change" comments pointing at item (1)
- `shuffle/mod.rs` — top-of-module doc calls out deferred higher-level types → item (2)
- `cluster/split.rs` — the `SplitShape` enum has no per-connector producer yet; connectors need `fn split_shape(&self) -> SplitShape` methods, which land when the full-engine harness lands

---

## Resume commands (run these first in a fresh session)

```bash
# Sanity that nothing bit-rotted.
cd C:/Users/sujit/source/laminardb
cargo test -p laminar-core --features cluster-unstable --tests
cargo clippy --workspace --features cluster-unstable --all-targets -- -D warnings
```

Expect:
- 3 integration test files, 16 tests pass + 1 ignored, ~6.5 s total
- `laminar-core` lib: 332 unit tests pass
- Workspace clippy clean

Then read the three design docs top-of-section above, then pick one
of the three items. Item (1) recommended — biggest impact, biggest
honest lift.

---

## Short index of files you'll touch most

**Production code:**
- `crates/laminar-db/src/checkpoint_coordinator.rs` — item (1)
- `crates/laminar-core/src/shuffle/connection.rs` (new) — item (2)
- `crates/laminar-core/src/shuffle/sender.rs` (new) — item (2)
- `crates/laminar-core/src/shuffle/receiver.rs` (new) — item (2)
- `crates/laminar-core/src/cluster/control/controller.rs` — may grow a `periodic_barrier_driver` helper for item (1)

**Tests:**
- `crates/laminar-core/tests/cluster_integration.rs` — add 2PC-flow tests for item (1)
- `crates/laminar-core/tests/shuffle_tcp_integration.rs` — add `Sender`/`Receiver` tests for item (2)

**Config / plumbing (usually doesn't need changes):**
- `crates/laminar-db/src/builder.rs`
- `crates/laminar-db/src/pipeline_lifecycle.rs`
- `crates/laminar-server/src/cluster.rs`
