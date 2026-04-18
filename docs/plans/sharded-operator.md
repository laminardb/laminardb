# Sharded operator design — distributed GROUP BY

**Status:** proposal, v2 after the DataFusion spike. Needs sign-off on
§3 open decisions before code.

The shuffle transport, barrier tracker, 2PC coordinator, and
cross-instance durability gate all exist. They have no caller. This
doc defines the caller: a distributed `GROUP BY` that splices into
DataFusion's native two-stage aggregate by inserting a shuffle
between `AggregateExec::Partial` and `AggregateExec::FinalPartitioned`.

## 1. What the spike confirmed

DataFusion's physical plan already has the shape we need:

```
source → AggregateExec::Partial → RepartitionExec::Hash(keys) → AggregateExec::FinalPartitioned → downstream
```

- `AggregateExec::Partial` — per-input-partition, produces partial
  state via `GroupsAccumulator::state()` as `Vec<ArrayRef>`. Output
  schema = group columns + per-accumulator state columns.
- `AggregateExec::FinalPartitioned` — declares
  `Distribution::HashPartitioned(group_exprs)` as its required input
  distribution (confirmed at
  `datafusion-physical-plan/src/aggregates/mod.rs:1174`). The planner
  inserts `RepartitionExec::Hash` automatically to satisfy that.
- `GroupsAccumulator::merge_batch` — consumes partial state from
  another accumulator. This is how `FinalPartitioned` merges.

**The splice point is the `RepartitionExec::Hash` node.** It does
within-process hash repartitioning. For cross-instance, we replace it
with a `ClusterRepartitionExec` that ships non-owned rows to peers
via `ShuffleSender` and pulls peer rows in via `ShuffleReceiver`.

All aggregate math — SUM, COUNT, AVG, STDDEV, percentile, UDAFs —
stays in DataFusion. We don't reimplement any of it.

## 2. Building blocks

### 2.1 `ClusterRepartitionExec` (new, the only real code)

An `ExecutionPlan` that replaces `RepartitionExec::Hash` in the
distributed plan. Contract:

- **Input:** 1 partition of partial-state rows from
  `AggregateExec::Partial` (schema: group cols + state cols).
- **Output partitions:** K, where K is the count of vnodes owned by
  this instance. Each output partition corresponds to one owned
  vnode; `AggregateExec::FinalPartitioned` runs once per partition.
- **Routing (per input row):** hash group-by columns via `key_hash`,
  compute `vnode = hash % vnode_count`, look up `owner =
  registry.owner(vnode)`. If `owner == self`, push to the
  local-owned output partition for that vnode. Otherwise, buffer
  and flush via `ShuffleSender::send_to(owner, Data(batch))`.
- **Peer input:** a background task owns a `ShuffleReceiver`. On
  `ShuffleMessage::Data(batch)` from a peer, split the batch by
  vnode and push into the matching output partition.
- **Output schema:** same as input — partial state rows ready for
  the downstream `AggregateExec::FinalPartitioned` to merge.
- **Partitioning advertised:** `Partitioning::Hash(group_exprs,
  num_owned_vnodes)`. Satisfies `FinalPartitioned`'s required input
  distribution — so no second repartition gets inserted.

### 2.2 Barrier handling inside `ClusterRepartitionExec`

On seeing a checkpoint barrier on the local input, fan it out to all
peers via `fan_out_barrier(sender, peer_ids, barrier)` AND forward
into every local output partition. On receiving
`ShuffleMessage::Barrier` from a peer, feed it into the
per-peer-input of a `BarrierTracker(num_peers + 1)`. When every
input has observed the same barrier epoch, the operator has aligned
and emits a downstream barrier. This sequencing mirrors Flink's
in-band barrier alignment.

### 2.3 Per-vnode state snapshot

`AggregateExec::FinalPartitioned` runs per partition (= per owned
vnode). Its `GroupsAccumulator::state(EmitTo::All)` returns
`Vec<ArrayRef>` — the intermediate state. On barrier alignment we
wrap those arrays as a `RecordBatch`, serialize via Arrow IPC, and
call `state_backend.write_partial(vnode, epoch, bytes)`. On recovery
we read the bytes back, deserialize into a `RecordBatch`, and feed
it through `GroupsAccumulator::merge_batch`. No custom state
machinery.

## 3. Open decisions — need your call before coding

### 3.1 DataFusion planner integration
How do we ensure `ClusterRepartitionExec` gets inserted instead of
(or after) DF's `RepartitionExec::Hash`?

**Options:**
- **(A) `PhysicalOptimizerRule` that runs after DF's `EnforceDistribution`.**
  Walk the plan; find every `RepartitionExec::Hash` whose upstream is
  `AggregateExec::Partial` and downstream is `FinalPartitioned`;
  replace the `RepartitionExec::Hash` node with `ClusterRepartitionExec`.
  Simple, isolated, reuses DF's plan-building up to that point.
- **(B) Custom `ExtensionPlanner`** (what LookupJoin uses) that
  intercepts at logical→physical planning. More invasive; skips DF's
  normal aggregate planning.

Recommend (A). DF does the aggregate-mode selection and repartition
placement for us; we just swap the repartition node.

### 3.2 Cluster-mode on/off
Same as v1 §3.3: auto-enabled under `Profile::Cluster` with a `SET
distributed = false` escape hatch. The optimizer rule is a no-op
when the profile isn't cluster. Matches Flink's DataStream default.

### 3.3 Backpressure between local pipeline and shuffle
`ShuffleSender::send_to` awaits the writer mutex; a slow peer blocks
the sender. If the sender is inside `ClusterRepartitionExec::execute`,
a slow peer stalls the local aggregate pipeline too.

**Options:**
- **(A) Bounded buffered channel per peer.** Sender task drains the
  channel and awaits the network; the execute() path `send`s to the
  channel (non-blocking up to the bound). Backpressure at the bound
  — local aggregate slows down when a peer is slow, which is
  correct. Standard Flink pattern.
- **(B) Unbounded buffer** — simpler, unbounded memory risk on a
  stuck peer.

Recommend (A). Bound = a function of `MAX_PAYLOAD_BYTES` and ~100ms
of target end-to-end latency; start with 16 batches / 32 MiB.

### 3.4 Barrier-alignment deadline
If a peer dies while `BarrierTracker` is waiting for its barrier,
alignment stalls. Flink times out and fails the checkpoint.
**Recommend an explicit `align_timeout` (default 30s matching
`wait_for_quorum`)**; on timeout, emit a barrier with `ok=false` to
the coordinator — the 2PC flow already handles that path via
`QuorumOutcome::Failed`.

### 3.5 Restart path
On instance restart, each `AggregateExec::FinalPartitioned` needs to
rehydrate its `GroupsAccumulator` from the last committed partial.
**Recommend a thin wrapper around `FinalPartitioned`** — call it
`RecoverableFinalPartitioned` — that reads
`state_backend.read_partial(vnode, last_epoch)` on first execute,
decodes the bytes to a `RecordBatch`, and pushes it through
`merge_batch` before consuming upstream input. Isolates recovery to
one spot; leaves stock `AggregateExec` untouched.

## 4. Phasing (updated for DF integration)

Each phase ships independently.

### Phase A — `ClusterRepartitionExec` skeleton — ✓ DONE
- ExecutionPlan impl in `laminar-sql/src/datafusion/cluster_repartition.rs`.
- Router task drains the DF upstream, hashes group-by columns via
  `key_hash(xxh3_64)`, slices each batch by vnode via
  `arrow::compute::take`, ships remote slices through `ShuffleSender`,
  pushes local slices to per-partition bounded channels (cap 16).
- Dispatcher task drains `ShuffleReceiver` inbound and routes to the
  matching local output partition.
- **No barrier handling** (phase C).
- Integration test
  (`crates/laminar-sql/tests/cluster_repartition_integration.rs`):
  8-row batch on node A with 4-vnode / 2-peer topology; asserts A's
  output partitions contain exactly its owned-vnode keys and B's
  `ShuffleReceiver` sees the complementary keys — every row routed
  exactly once across the cluster.

**Known Phase A limitation (intentionally deferred):** inbound
dispatch into a multi-owned-partition instance requires a vnode
header on the wire; today it asserts a single owned partition on the
receive side (fine for the 2-node 4-vnode test). Fixed in Phase C
when barriers and vnode framing land together.

### Phase B — Physical optimizer rule — ✓ DONE
- `DistributedAggregateRule` in
  `laminar-sql/src/datafusion/distributed_aggregate_rule.rs`.
- Walks the plan bottom-up via `TreeNode::transform_up`; matches
  `RepartitionExec(Partitioning::Hash)` whose child is
  `AggregateExec::Partial`; resolves hash expressions to column
  indices via `Column::index()`; constructs
  `ClusterRepartitionExec::try_new` with the resolved indices.
- No-op when the upstream isn't Partial or when a hash expression
  isn't a bare column reference (latter deferred to Phase C).
- Two unit tests: splice works against a hand-built
  `Partial → RepartitionExec → FinalPartitioned` plan for
  `SELECT k, SUM(v) ... GROUP BY k`; rule is a no-op on a bare
  RepartitionExec with no Partial upstream.
- **Pending wiring:** registering the rule with `SessionStateBuilder`
  at cluster startup. That's a ~20 LOC integration into
  `pipeline_lifecycle` / `cluster.rs` and belongs next to the `SET
  distributed = false` session var, which lands with Phase D's
  planner integration.

### Phase C — Barrier alignment + vnode framing — ✓ DONE
- `ShuffleMessage::VnodeData(vnode, RecordBatch)` variant added
  (tag `0x05`) — sender pre-routes, receiver dispatches by carried
  vnode. Fixes the Phase A multi-partition dispatch limitation.
- `ClusterRepartitionExec` now carries a `BarrierTracker(N+1)` where
  N = peer count. Router is a `tokio::select!` over input / injected
  barriers / peer barriers, using `fan_out_barrier` to ship barriers
  across shuffle connections.
- Public API grew `inject_barrier(b)` (coordinator triggers a
  checkpoint) and `aligned_epoch_watch()` (Phase D wrappers subscribe
  to alignment events; `watch::Receiver<u64>` carrying the last
  aligned checkpoint_id).
- Integration test
  (`tests/cluster_repartition_barrier_flow.rs`) stands up two nodes
  over loopback TCP, injects a barrier on each side, verifies both
  `aligned_epoch_watch` channels fire with the same checkpoint id
  once every tracker input observes it.
- Input EOS no longer terminates the router — it falls out of the
  select and barriers continue to flow (checkpoint coordination
  outlives input exhaustion).

**Phase C scope split from original plan:** actual
`GroupsAccumulator::state()` snapshot and `state_backend.write_partial`
moved to Phase D, where they live with `RecoverableFinalPartitioned`.
Phase C delivers the alignment signal; Phase D consumes it.

### Phase D — Checkpoint writer (`CheckpointedRepartitionExec`) — ✓ DONE (split from original plan)
Original Phase D was `RecoverableFinalPartitioned` extracting
`GroupsAccumulator::state()` on alignment. That's blocked: DF's
`AggregateExec` doesn't expose the accumulator. Pragmatic rework:
**checkpoint the shuffled partial-state batches flowing INTO
`FinalPartitioned`** — same recovery semantics, achievable without
forking DF.

- `CheckpointedRepartitionExec` in
  `laminar-sql/src/datafusion/checkpointed_repartition.rs` wraps
  `ClusterRepartitionExec`. Each execute(partition) interposes a
  mapping closure on the upstream stream that clones the passing
  `RecordBatch` into a per-partition buffer behind a
  `parking_lot::Mutex`.
- Snapshot task subscribes to `aligned_epoch_watch`; on each fire,
  atomically swaps every partition's buffer out via
  `std::mem::replace`, `concat_batches` + `serialize_batch_stream`
  per-partition, and writes via `state_backend.write_partial(vnode,
  epoch, bytes)`. Publishes completion on a per-exec
  `checkpoint_watch` channel.
- Unit test `snapshot_writes_partial_state_on_alignment` — solo
  cluster, data flows, inject barrier, verify ≥1 vnode's partial
  shows up on `InProcessBackend` at the checkpoint epoch.

**Router fix shipped alongside:** input EOS now drops
`partition_txs`, so output streams terminate cleanly instead of
hanging when upstream is bounded. Barrier coordination (the point of
the router post-EOS) stays alive via the select arm going pending.

### Phase E.1 — Recovery replay — ✓ DONE
- `CheckpointedRepartitionExec::with_recovery_epoch(epoch)` builder
  method. When set, `execute(partition)` prepends a single recovery
  batch to the output stream: `state_backend.read_partial(vnode,
  epoch)` → `deserialize_batch_stream` → yielded once before live
  upstream data. Downstream `AggregateExec::FinalPartitioned`
  treats it as a normal partial-state input and merges via
  `merge_batch`.
- Recovery batches deliberately bypass the buffer — they're already
  durable; re-checkpointing would double-count at the next alignment.
- Unit test `recovery_replay_emits_prior_state_first`: seeds
  `(vnode=0, epoch=9)`, constructs a new exec with
  `.with_recovery_epoch(9)`, asserts the first stream yield is the
  recovered `RecordBatch`. Two laminar-sql lib tests green under
  `cluster-unstable`.

### Phase E.2a — Rule wired through LaminarDB — ✓ DONE
- `LaminarDbBuilder::physical_optimizer_rule(rule)` threads optimizer
  rules into `SessionStateBuilder::with_physical_optimizer_rule`.
- `laminar-server/src/cluster.rs::start_cluster` constructs
  `ShuffleReceiver` + `ShuffleSender` (publishes its bound address
  under `SHUFFLE_ADDR_KEY` for peer discovery), builds
  `DistributedAggregateRule`, and passes it through the builder.
- New test `distributed_rule_wiring::distributed_aggregate_rule_installed_on_session_state`
  opens a `LaminarDB` via the builder with the rule and asserts it
  appears in `SessionState::physical_optimizers()`. Combined with the
  rule's own unit tests (which exercise the rewrite on a hand-built
  Partial→Hash→FinalPartitioned plan), this closes the wiring loop.

### Phase E.2b — End-to-end SQL through cluster mode (pending)
- LaminarDB's base `SessionConfig` sets `target_partitions = 1`, so
  DataFusion picks `Final` + `CoalescePartitionsExec` instead of the
  `FinalPartitioned` shape the rule rewrites. For a real distributed
  GROUP BY to emerge through SQL in cluster mode, `target_partitions`
  needs to be set from the vnode count (or peer count) at cluster
  startup. Small wiring diff — separate session alongside the full
  end-to-end MiniCluster + MinIO test.

### Phase E — End-to-end SQL + MiniCluster + MinIO (~200 LOC)
- Actual `SELECT k, SUM(v) FROM source GROUP BY k` on a 2-node
  cluster with MinIO state backend.
- Verifies planner + repartition + aggregate + checkpoint + recovery
  all compose.

**Updated total:** ~1150 LOC code + ~400 LOC tests across 4–5
sessions (down from v1's ~1600 + 500). DF doing the aggregate work
is a big savings.

## 5. Risks (updated)

- **Splice correctness.** DF's `RepartitionExec::Hash` has properties
  (`emission_type`, `boundedness`, `ordering_preserving`) that our
  replacement must match. Missing one breaks downstream assumptions.
  **De-risk:** in Phase A, copy `RepartitionExec`'s `PlanProperties`
  setup as the template and verify the replacement passes DF's plan
  validation.
- **Hash function agreement.** DF's hash inside `RepartitionExec`
  uses `ahash` with a specific seed; we route via
  `key_hash(xxh3_64)` elsewhere. If these disagree we have
  catastrophic silent wrong-answer bugs. **De-risk:** standardize on
  `key_hash(xxh3_64)` inside `ClusterRepartitionExec`; DF's
  repartition isn't in the distributed path anymore, so DF's hash is
  moot.
- **Accumulator state size.** Arrow IPC of the partial state
  `RecordBatch` could be large at high cardinality. **De-risk:**
  instrument size, add a per-vnode budget with explicit error.
- **Alignment-timeout vs quorum-wait coupling.** Operator align
  timeout (§3.4) and coordinator `wait_for_quorum` (30s today) need
  to coexist. If operator times out first, follower's
  `follower_checkpoint` acks with `ok=false` → coordinator aborts.
  If coordinator times out first, operator still stalls but the
  epoch is aborted regardless. **De-risk:** set operator timeout =
  `wait_for_quorum - 5s` so the operator fails first and produces
  a meaningful error.

## 6. Scope lines

**In scope:**
- All DataFusion-supported aggregates out of the box (SUM, COUNT,
  AVG, STDDEV, MIN, MAX, array_agg, user UDAFs, etc.).
- Single-input aggregates with `GROUP BY`.
- Static peer set at operator construction.
- Aligned checkpoints only.
- MinIO-backed state backend.

**Explicit follow-ups:**
- Distributed JOIN (same splice pattern but
  `HashJoinExec::Partitioned`; scope similar).
- Distributed window functions (more subtle — window state is
  ordered, shuffling can break order).
- Skew-aware partitioning (pre-shuffle combine for hot keys).
- State spill-to-disk at large cardinality.
- Dynamic vnode rebalance.

## 7. What I need from you

1. **Sign-off on §3.1** (optimizer rule vs ExtensionPlanner).
2. **Sign-off on §3.3** (bounded per-peer channel for shuffle
   backpressure).
3. **Sign-off on §3.5** (the wrapper-exec approach for recovery).
4. **Yes/no on starting with Phase A** — `ClusterRepartitionExec`
   skeleton with zero barrier handling, integration test against
   a 2-node MiniCluster shipping a hand-crafted partial-state
   `RecordBatch`.

No code until this is signed off.
