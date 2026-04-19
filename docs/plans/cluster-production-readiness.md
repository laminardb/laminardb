# Cluster production-readiness plan

**Branch**: `feat/phase-c5-cluster-primitives` (in progress).
**Chosen sequencing**: Option 1 — full sequence, no customer pressure.
**Solo effort estimate**: 20-26 weeks. Parallel with 2 engineers: 14-18 weeks.

## Starting state

What works (tested):
- Shuffle transport: TCP pool, `Hello` handshake, FIFO per connection.
- `ClusterRepartitionExec` hash-routes rows to owning vnode's instance — *exec-level only; not engaged by streaming aggregates today (gap 0)*.
- `CheckpointedRepartitionExec` mirrors partials to state backend on aligned epochs.
- Chandy-Lamport `BarrierTracker` alignment.
- 2PC checkpoint (Prepare/Commit/Abort) via gossip KV — *follower polling fixed 2026-04-19; see [Real bugs surfaced](#real-bugs-surfaced-while-attempting-phase-0)*.
- Chitchat gossip discovery with phi-accrual.
- `ObjectStoreBackend` over S3/GCS/Azure/file://.

What doesn't (the gaps this plan fixes, ranked by severity):

**0. Streaming aggregates do not shuffle (NEW BLOCKER).** `SqlQueryOperator::lazy_init` (`operator/sql_query.rs:81`) detects aggregate queries and routes them to `IncrementalAggState`, which maintains in-memory per-group accumulators and **never invokes the DataFusion physical plan**. So `DistributedAggregateRule` — even when correctly registered on the streaming `SessionContext` — never sees aggregate queries, never rewrites, and `ClusterRepartitionExec` never runs in the streaming pipeline. Each node aggregates only its locally-pushed data. **Every gap below is moot until this is bridged**; "gap 1 Recovery unwired" can't recover state that never crossed the wire.

1. Recovery unwired: `CheckpointedRepartitionExec::with_recovery_epoch` is never called on startup. Restart → aggregates reset to zero. *Blocked by gap 0.*
2. Each node computes `round_robin_assignment` independently. No coordination.
3. No cross-instance watermark. Event-time operators are incorrect under distribution. *Blocked by gap 0 for aggregate queries.*
4. Split-brain unfenced (the `assignment_version` guard was removed in the cleanup because its advancer had no caller).
5. Sinks commit per-node. No cluster-wide 2PC across sinks.
6. Stale `ShuffleSender` pool entries never purge.
7. Vnode assignment frozen at boot. Node join/leave doesn't re-shuffle.
8. No TLS/auth on shuffle.
9. No admin surface. No cross-instance observability.
10. Shuffle backpressure carried only by the local per-partition `mpsc(16)`.

## Real bugs surfaced while attempting Phase 0

These were found during the Phase 0 implementation attempt on 2026-04-19. The first two are FIXED on this branch as part of unblocking the smoke test. The third is gap 0 above (the design choice for the bridge is in [Phase 0a](#phase-0a--row-shuffle-bridge-prerequisite-to-phase-0b)).

- **(B1) Optimizer rules registered via `LaminarDbBuilder::physical_optimizer_rule` did not reach the streaming pipeline's `SessionContext`.** The builder installed them on `LaminarDB::ctx` (used by `db.execute()`), but `pipeline_lifecycle::start_connector_pipeline` created a fresh `SessionContext` for the `OperatorGraph` without those rules. **FIXED**: rules + `target_partitions` now flow through new `physical_optimizer_rules` and `pipeline_target_partitions` fields on `LaminarDB`, applied to the `OperatorGraph`'s `SessionContext` via `SessionStateBuilder`.

- **(B2) Cluster followers never polled for barrier announcements.** `StreamingCoordinator::maybe_checkpoint` gated on `should_checkpoint` (timer-or-flag) and returned early before invoking `callback.maybe_checkpoint`, which is what routes followers to `maybe_follower_checkpoint`. With `interval_ms=None` (manual-only checkpointing, as in the test harness), the gate never opened, so the follower's `observe_barrier` poll never ran, and 2PC never converged. **FIXED**: coordinator now always invokes `callback.maybe_checkpoint(false, default)` first; the timer gate only controls leader-side barrier injection.

- **(B3) Streaming aggregates bypass the DataFusion physical plan** (gap 0 above). Phase 0a addresses this.

## Bonus bugs surfaced (not yet fixed; track separately)

- ~~`ObjectStoreBackend::epoch_complete` swallows `AlreadyExists` as success (`object_store.rs:140`). Two leaders racing both report committed. Split-brain commit hole on the durable-write side.~~ **FIXED**: both the HEAD fast-path and the CAS-loser branch now read the `_COMMIT` marker and compare the audit body against `self.instance_id`; mismatch returns `StateBackendError::SplitBrainCommit` so the losing leader aborts instead of double-committing.
- ~~`CheckpointedRepartitionExec` snapshot loop has no backpressure on `state_backend.write_partial` (`checkpointed_repartition.rs:266`). Serial per-vnode awaits on the critical path; 256 vnodes × 100ms S3 = 25s/epoch. (Phase 3.4 mentions but doesn't sequence.)~~ **FIXED**: per-vnode pipeline (`concat_batches` + IPC encode + `write_partial`) now runs up to `SNAPSHOT_CONCURRENCY = 32`-way in parallel via `buffer_unordered`. 256 vnodes × 100ms → ~800ms instead of 25s.
- ~~Recovery-epoch plumbing must invalidate cached physical plans if DDL compiled before recovery completed. Phase 1.1 must handle the ordering.~~ **FIXED (plumbing)**: `CheckpointedRepartitionExec::recovery_epoch` is now `Arc<AtomicU64>` read lazily at every `execute()`, and `DistributedAggregateRule` owns a shared handle cloned into every exec it mints (plus a `set_recovery_epoch(N)` setter). Publishing into the rule after DDL has already compiled plans still reaches the already-constructed execs. The remaining Phase 1.1 work is wiring `db.start()` to load `last_committed_epoch` from the checkpoint store and call `rule.set_recovery_epoch(N)`.
- *Note*: `server::start_cluster never calls set_gate_vnode_set(full)` from the original review was wrong — `pipeline_lifecycle.rs:224` does call it. Strike that finding.

## Guiding rule

Build the acceptance test first. Every phase exits when the test passes a new failure mode. No "it probably works."

---

## Phase 0a — Row-shuffle bridge (prerequisite to Phase 0b)

**~3-5 days.** Unblocks gap 0 so every subsequent phase is testable end-to-end.

### The gap

`SqlQueryOperator::lazy_init` takes a special fast path for aggregate queries via `IncrementalAggState` (per-group in-memory accumulators, `aggregate_state.rs:260`). This path bypasses DataFusion's physical plan entirely, so `DistributedAggregateRule` — even with B1 fixed — never sees aggregate SQL. Every node runs a single-node incremental aggregate over whatever data was locally pushed; there is no cross-node aggregation.

Options considered:
- **(α) Two-stage cluster-aware `IncrementalAggState`** (Flink/RisingWave-style): ship partial-state records, merge remotely. ~2-4 weeks, most efficient at scale, most general.
- **(β) Abandon the incremental path, use `CachedPlan` in cluster mode**: smaller in LOC but loses cross-cycle state composition semantics; would need a retraction/changelog layer on top of DataFusion's `AggregateExec`. Structural risk.
- **(γ) Row-shuffle wrapper** (chosen): keep `IncrementalAggState` unchanged; wrap it with a shim that hash-routes **raw rows** to vnode owners and drains incoming remote rows back into the same accumulator. ~500 LOC. Network-inefficient relative to α (ships rows, not partials) but correct for SUM/COUNT/AVG/MIN/MAX and good enough to unblock the plan. Phase-5 upgrade path to α is additive.

### Design (option γ)

New graph operator `ClusterShufflingAggregator` in `crates/laminar-db/src/operator/cluster_agg.rs`:

- Wraps an `IncrementalAggState` (constructed the same way `SqlQueryOperator::lazy_init` builds it today).
- Holds: `Arc<VnodeRegistry>`, `Arc<ShuffleSender>`, `Arc<ShuffleReceiver>`, `NodeId self_id`, group-column indices extracted from the SQL plan.
- `process(inputs)`:
  1. **Drain** inbound `ShuffleMessage::VnodeData` batches from the receiver (non-blocking, bounded drain).
  2. **Partition** this cycle's input rows by `vnode = key_hash(group_cols) % vnode_count`. Rows whose owner is `self_id` stay local; others are serialized and `ShuffleSender::send_to(owner, VnodeData)`.
  3. **Feed** local rows + drained remote rows into `inner.process(...)` — the accumulator sees a unified row stream and doesn't know about clustering.
  4. Output = `inner` output.
- `checkpoint()` / `restore()`: delegate to `inner`. State is the accumulator; the shuffle layer has no state worth durably preserving (in-flight rows are lost on crash — same as single-node IncrementalAggState loses in-flight rows between pushes).

### Wiring

1. Add `cluster_shuffle: Option<ClusterShuffleConfig>` field to `OperatorGraph`; setter `with_cluster_shuffle(registry, sender, receiver, self_id)`.
2. `SqlQueryOperator::new` takes an `Option<ClusterShuffleConfig>` through plumbing from the graph.
3. In `lazy_init`: if cluster-shuffle is `Some` AND the aggregate detection succeeded, wrap the `IncrementalAggState` inside a `ClusterShufflingAggregator` (new `QueryState::ClusterAgg` variant).
4. New field on `LaminarDB` (or reuse the shuffle Arcs already threaded through `DistributedAggregateRule`): `cluster_shuffle_config`. Populated by a new builder method `cluster_shuffle(registry, sender, receiver, self_id)`.
5. `start_connector_pipeline` reads the field and calls `graph.with_cluster_shuffle(...)` before `add_query`.
6. `DistributedAggregateRule` stays installed — it still rewrites for the rare `CachedPlan` path (non-aggregate, hash-partitioned queries if/when they show up). No conflict.

### Acceptance

- Unit test: `ClusterShufflingAggregator::process` splits a 4-row batch between two owners deterministically (4 vnodes, round-robin 2 peers, `SUM(value) GROUP BY key`).
- Integration: the existing `cluster_e2e_smoke.rs` test turns green without scenario changes.
- Regression: existing `aggregate_state` unit tests unaffected (wrapper is additive).

### What Phase 0a does NOT do

- Partial-state shuffling (α) — future work. Row-shuffle is O(n_rows × bytes_per_row) on the wire; partial-state is O(n_groups × bytes_per_state) which is much smaller at high cardinality.
- Watermark propagation across the shuffle — still handled by Phase 1.3.
- Barrier alignment through the row-shuffle — row-shuffle uses `ShuffleMessage::VnodeData` without barrier frames; checkpoint correctness relies on the existing coordinator 2PC (B2 fix), not on in-flight-row draining.
- Any change to non-aggregate streaming queries (they don't go through `IncrementalAggState`).

### Risks called out

- **R1**: Receiver-drain cadence matters. If a cycle emits 100 local rows but the remote side already sent 10,000, our unified input grows unexpectedly. Bound the drain at `cycle_budget_ns` or a max-rows cap.
- **R2**: `ShuffleSender` is FIFO per connection but unordered across sources. Aggregates are commutative+associative for SUM/COUNT/AVG; order doesn't matter. For MIN/MAX with NULLs or COUNT(DISTINCT) this is also fine. Non-commutative extensions (FIRST_VALUE, LAST_VALUE with event-time) would need ordering guarantees — flagged in docs, not in Phase 0a scope.
- **R3**: On restart, in-flight rows in the shuffle wire buffers are lost. Single-node IncrementalAggState has the same semantics (buffered-but-unprocessed rows lost on crash). Phase 1.1's `with_recovery_epoch` work plus the ordinary at-least-once replay of the source handles the pre-shuffle rows; cluster-aware source replay for in-flight shuffle buffers is a Phase 5 concern.

---

## Phase 0b — Acceptance test (prerequisite to Phase 1+)

**3-5 days.** Prerequisite for every phase below. Principal-engineer review of the earlier draft surfaced five correctness issues (C1-C5) and five high-priority revisions (H1-H5); the spec below bakes those fixes in. The row-shuffle bridge from Phase 0a is what makes this test runnable at all.

### Test layout — two files, not one

1. `crates/laminar-db/tests/cluster_e2e_smoke.rs` — one happy-path scenario. Must pass on this branch today with no feature gates or `#[ignore]`. This is the "proves the shuffle, planner rule, MV store, and cross-node gossip all work" gate.

2. `crates/laminar-db/tests/cluster_e2e_failures.rs` — failure-mode scenarios behind per-phase cargo feature gates (`phase-1-recovery`, `phase-2-rebalance`). Each gate flips on in CI when its phase lands, forcing the test to run rather than relying on `#[ignore]` (which rots — operational experience across every team that tried it).

### Shared harness (`tests/common/cluster_harness.rs`, included via `#[path]`)

`ClusterEngineHarness::spawn(n, vnode_count)`:
- `MiniCluster::spawn(n)` for gossip + `ClusterController` (lowest-id leader). Assert `nodes[0].controller.is_leader()` after convergence.
- Per node: `ShuffleReceiver::bind(id, 127.0.0.1:0)`; `ShuffleSender::new(id)` + cross-register all peers' `local_addr()`.
- **State backend (shared)**: ONE `LocalFileSystem` over ONE shared `TempDir`. Both nodes' `ObjectStoreBackend`s wrap it with distinct `instance_id`. `instance_id` is NOT a path prefix — it's audit metadata in the `_COMMIT` body; the `epoch=N/vnode=V/partial.bin` layout is intentionally shared. Mirrors `cluster_minio_flow.rs:72-81`.
- **Checkpoint store (per-node)**: each node gets its own `TempDir` + `ObjectStoreCheckpointStore` (or the builder's `object_store_url("file:///<per-node-dir>")`). Mirrors production's `nodes/{id}/` namespace (`server/cluster.rs:340-345`). **Do not share** — production doesn't.
- `VnodeRegistry::new(vnode_count)` + `round_robin_assignment(vnode_count, &peer_ids)`.
- `LaminarDB::builder().profile(Profile::Cluster).object_store_url(per_node_url).cluster_controller(...).state_backend(shared_backend).vnode_registry(...).physical_optimizer_rule(DistributedAggregateRule::new(...).with_state_backend(...)).target_partitions(vnode_count).build()`.
- **`db.start()` must be called on every node** — that's what wires the controller into the checkpoint coordinator (`pipeline_lifecycle.rs:194`). Omitting it silently reverts to single-node mode.

### Smoke scenario

- 2 nodes, 4 vnodes → round-robin assigns `{0, 2}` to leader, `{1, 3}` to follower.
- DDL on every node: `CREATE SOURCE src (key BIGINT, value BIGINT); CREATE MATERIALIZED VIEW sums AS SELECT key, SUM(value) AS total FROM src GROUP BY key`.
- **Pre-compute keys**: scan `0..1000`, compute `key_hash(row_convert(k)) % 4` using `laminar_core::state::key_hash` + `arrow::row::RowConverter`. Pick 4 keys whose vnode ∈ `{0, 2}` and 4 whose vnode ∈ `{1, 3}`. Push all 8 to **leader only**.
- **Determinism**: after push, call `db.checkpoint().await` on leader and await its completion. Do NOT poll for row count (can lock onto a transient wrong state when `MvStorageMode::Aggregate` replaces the batch mid-cycle).
- **Assertions**:
  - Union of `SELECT * FROM sums` across both nodes = 8 rows with correct sums (`key * 10`).
  - Follower's `SELECT * FROM sums` returns ≥ 1 row — **required**, proves the cross-node shuffle actually ran (otherwise target_partitions=4 + 4 vnodes could let everything land on leader and the test would pass while shuffle is broken — classic "gate test that hides bugs").
  - Both nodes' `checkpoint_coordinator.epoch()` within ±1 after the await.

### Failure scenarios (feature-gated)

1. **`crash_mid_stream_loses_in_flight`** — uses `NodeHandle::crash()` (not `kill()`; `kill` is a graceful `Left` announcement that bypasses phi-accrual and masks the production failure mode). Default build asserts that rows hashed to B's vnodes are **lost** while B is down — this is the current bug. `#[cfg(feature = "phase-2-rebalance")]` flips the assertion to "no loss, totals correct." Timeout sized for phi-accrual: `phi=3.0, gossip=50ms, grace=1s` → expect detection in 1-3s, scenario budget ≥ 5s.

2. **`restart_recovers_sum_aggregate`** — `#[cfg(feature = "phase-1-recovery")]`-gated. Scope is explicitly `SUM` + `MvStorageMode::Aggregate` (other MV modes tracked separately). Flow: push first half → `db.checkpoint().await` → shutdown both → rebuild harness pointing at same TempDirs → push second half → `db.checkpoint().await` → assert totals match full-stream union. Includes an **intermediate-snapshot assertion** taken immediately after restart but before the second push, so a "restart dropped state, second-half repopulated" bug can't masquerade as recovery. Also asserts post-restart epoch drift ≤ 1 between nodes (per-node `CheckpointStore` state can diverge across crashes; `checkpoint_coordinator.rs:275`).

### Out of scope for Phase 0 (tracked later)

- Watermark propagation across shuffle → Phase 1.3.
- Append-mode / windowed MV restore → separate work item.
- Leader failover mid-stream → Phase 1.2 / 1.4.
- TLS / peer auth → Phase 4.

### Bonus bugs

See the top-level [Bonus bugs surfaced](#bonus-bugs-surfaced-not-yet-fixed-track-separately) section — covers the full current list including the `set_gate_vnode_set` misdiagnosis (struck on re-reading the code).

---

## Phase 1 — Correctness foundations (~6-8 weeks)

### 1.1 Wire recovery (`~1 week`)

- On `db.start()`, load `last_committed_epoch` from `CheckpointStore`.
- Plumb into `DistributedAggregateRule` or via a new setter on the rule so each `CheckpointedRepartitionExec` constructed during planning gets `.with_recovery_epoch(N)`.
- AC: restart-after-checkpoint test passes.

### 1.2 Coordinated assignment (`~2 weeks`)

- Leader-elected instance computes `round_robin_assignment` after 2s debounce post-membership-stable.
- Writes to `AssignmentSnapshotStore` with `PutMode::Create` on `version+1`.
- Followers watch `AssignmentSnapshotStore`, rebuild `VnodeRegistry` on change.
- On leader crash, next leader reads snapshot before recomputing (no regression to an older version).
- AC: partition cluster, both halves independently arrive at the same assignment from the stored snapshot.

### 1.3 Cross-instance watermark (`~1.5 weeks`)

- Extend `Barrier` frame payload with `min_watermark_ms`.
- Leader collects per-follower watermarks at barrier announce time, fans out the min.
- Downstream operators consult barrier-carried min, not local.
- No new gossip key — reuses the barrier channel.
- AC: event-time session window across 2 nodes with skewed event-time produces correct output.

### 1.4 Split-brain fence (`~3-5 days`)

- Resurrect `assignment_version` on `ObjectStoreBackend::write_partial`.
- Version advances on snapshot rotation (see 1.2).
- Put rejected if caller's version < authoritative version.
- AC: force two nodes to claim same vnode; one is rejected.

**Dependencies**: 1.4 needs 1.2. Others parallelize.

---

## Phase 2 — Operational correctness (~8-10 weeks)

### 2.1 Cross-instance sink 2PC (`~3-4 weeks`)

- Extend `CheckpointCoordinator::follower_checkpoint`: sinks drive `prepare` phase before ack.
- Leader's `await_prepare_quorum` gates on state-complete AND every follower's sink-prepare success.
- `Commit` announcement triggers sink commit; `Abort` triggers rollback.
- Touches every sink impl (Kafka txns, Delta rename, Postgres tx).
- AC: kill node between sink-prepare and sink-commit; no half-committed external state.

### 2.2 Stale connection purge (`~3 days`)

- `ShuffleConnection::reader` on exit removes its pool entry.
- Use `Weak` in pool + upgrade check before `send_to`; on `None`, rediscover via KV.
- AC: kill peer, restart at different address, next `send_to` reconnects.

### 2.3 Dynamic rebalance (`~4-6 weeks`)

- Leader triggers new assignment on membership delta (uses 1.2's snapshot flow).
- Handoff: old owner flushes pending partials → writes `handover_ready` marker → new owner starts from epoch N+1.
- Kafka consumer-group rebalance aligned with assignment rotation.
- AC: scale 2→3 nodes mid-stream; no dropped rows; totals correct.

**Dependencies**: 2.3 depends on all of Phase 1. 2.1 and 2.2 parallelize.

---

## Phase 3 — Operability (~4-6 weeks)

| # | Gap | Work |
|---|---|---|
| 3.1 | Graceful drain | `ClusterHandle::wait_for_shutdown` drains in-flight epoch, triggers final checkpoint, then stops discovery. |
| 3.2 | Admin API | `GET /cluster/ownership`, `GET /cluster/checkpoints`, `POST /cluster/drain`, `POST /cluster/checkpoint`. |
| 3.3 | Shuffle metrics | Per-peer bytes/frames/rtt, per-operator barrier-align latency, per-partition mpsc depth. Use existing prometheus registry. |
| 3.4 | Shuffle backpressure | **Measure first.** tokio mpsc may suffice. If benchmarks show memory growth under stalled Final, resurrect `CreditSemaphore` and wire into `send_to`. Don't rebuild it on speculation. |

---

## Phase 4 — Security (~1.5 weeks)

| # | Gap | Work |
|---|---|---|
| 4.1 | TLS on shuffle | rustls + mTLS, cluster-wide CA, cert paths in `laminardb-cluster.toml`. |
| 4.2 | Peer auth | Signed `Hello` with a JWT bound to chitchat node id, verified via cluster CA. |

---

## Starting point next session

Phase 0 first. Write the E2E SQL test. Don't fix anything until the test exists and fails in predictable ways. The test itself will surface issues that may re-order this plan.

First concrete file: `crates/laminar-db/tests/cluster_e2e_sql.rs`.
