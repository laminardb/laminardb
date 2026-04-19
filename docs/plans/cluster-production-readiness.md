# Cluster production-readiness plan

**Branch**: `feat/phase-c5-cluster-primitives` (in progress).
**Chosen sequencing**: Option 1 — full sequence, no customer pressure.
**Solo effort estimate**: 20-26 weeks. Parallel with 2 engineers: 14-18 weeks.

## Starting state

What works (tested):
- Shuffle transport: TCP pool, `Hello` handshake, FIFO per connection.
- `ClusterRepartitionExec` hash-routes rows to owning vnode's instance.
- `CheckpointedRepartitionExec` mirrors partials to state backend on aligned epochs.
- Chandy-Lamport `BarrierTracker` alignment.
- 2PC checkpoint (Prepare/Commit/Abort) via gossip KV.
- Chitchat gossip discovery with phi-accrual.
- `ObjectStoreBackend` over S3/GCS/Azure/file://.

What doesn't (the gaps this plan fixes, ranked by severity):
1. Recovery unwired: `CheckpointedRepartitionExec::with_recovery_epoch` is never called on startup. Restart → aggregates reset to zero.
2. Each node computes `round_robin_assignment` independently. No coordination.
3. No cross-instance watermark. Event-time operators are incorrect under distribution.
4. Split-brain unfenced (the `assignment_version` guard was removed in the cleanup because its advancer had no caller).
5. Sinks commit per-node. No cluster-wide 2PC across sinks.
6. Stale `ShuffleSender` pool entries never purge.
7. Vnode assignment frozen at boot. Node join/leave doesn't re-shuffle.
8. No TLS/auth on shuffle.
9. No admin surface. No cross-instance observability.
10. Shuffle backpressure carried only by the local per-partition `mpsc(16)`.

## Guiding rule

Build the acceptance test first. Every phase exits when the test passes a new failure mode. No "it probably works."

---

## Phase 0 — Acceptance test (prerequisite)

**3-5 days.** Prerequisite for every phase below.

Write `crates/laminar-db/tests/cluster_e2e_sql.rs`:

- Two `MiniCluster` nodes.
- In-memory source producing `(key, value)` with 8 distinct keys.
- Real SQL through the full planner (not hand-built plans): `SELECT key, SUM(value) FROM src GROUP BY key`.
- Arrow sink collecting results.

Three scenarios:
- **Happy path**: all 8 keys present, sums correct.
- **Mid-stream kill**: kill node B, let A finish stream, totals correct after B rejoins.
- **Restart**: stop both, start both, feed rest of stream, totals match full stream (no double-counting).

All three must pass for the branch to be considered done.

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
