# Cluster revoked-vnode operator-state cleanup

Status: **implemented** (Slices 1–4; follow-up to the cluster group-demotion rehydration-panic fix
`84b1cd8f`).

## Implemented

- **Slice 1** `IncrementalAggState::drop_vnodes` — clears `groups`, `last_emitted`, `dirty_keys`, the
  per-vnode delta maps, `delta_chain_len`, and (state-tier) `cold_groups`/`cold_vnodes`/`dirty_vnodes`
  for revoked vnodes. Unit test `drop_vnodes_purges_revoked_keeps_sibling`.
- **Slice 2** `GraphOperator::drop_owned_vnodes` (default no-op); `SqlQueryOperator` forwards to the
  agg state and resets `prev_owned`.
- **Slice 3** `Db::pending_revoke_vnodes` handle; `adopt_assignment_snapshot` stages
  `revoked = old − new`; `OperatorGraph::apply_revoked_vnodes` drains it each cycle **before**
  `apply_rehydrated_vnodes`; wired in `pipeline_lifecycle`. Drain test
  `apply_revoked_vnodes_drains_handle`.
- **Slice 4 (re-evaluated)** the `reset_acquired_vnodes` cold-group retain is now a no-op on every
  reachable path (drop-at-revoke + restart-starts-empty), so it is kept as documented
  defense-in-depth and the doc corrected to credit the `delta_chain_len` re-base — not the cold
  retain — for the restart FULL re-base.
- **Uninit-window hardening** (from adversarial review): a revoke arriving while the operator is still
  `Uninit` is deferred (`deferred_revoke_vnodes`) and applied after `lazy_init` folds
  `pending_restore`, so the restored-then-lost vnode can't double-count on re-acquire. Tests
  `reacquire_after_revoke_does_not_double_count` (operator-level, with doubling control) and
  `uninit_revoke_defers_drop_no_double_count`.

Validated: 845 laminar-db lib tests green under `state-tier`; clippy clean for cluster-only,
state-tier, and non-cluster.

## Problem

When a node loses ownership of vnodes on a rebalance, its in-memory **operator state for
those vnodes is never dropped**. `Db::adopt_assignment_snapshot`
(`crates/laminar-db/src/db.rs:585`) computes `old_owned`/`old_set` (623-624) and
`newly_acquired` (627-631) and acts only on the acquired set (stage rehydration / `mark_active`,
685-708). The **revoked** set (`old − new`) gets no operator-state handling — only registry
draining (`clear_draining`, 662), the coordinator `set_vnode_set` (670), and source-offset purge.

So `IncrementalAggState` keeps `groups`, `last_emitted`, `cold_groups`, `delta_chain_len`,
`dirty_keys`, `dirty_keys_by_vnode`, `removed_by_vnode`, `last_emitted_dirty_by_vnode`,
`cold_vnodes`, `dirty_vnodes` for keys it no longer owns.

### Consequences

1. **Double-count on re-acquire (correctness).** Rehydration applies the durable chain via
   `merge_groups`, which is **additive** (`merge_batch` on an Occupied group,
   `aggregate_state.rs:2044`). If a node lost vnode `v` and later re-acquires it, the rehydrated
   state is merged **on top of the stale leftover** → wrong aggregate values for `v`'s groups.
   This affects **all cluster delta aggregates**, not just group demotion.
2. **Unbounded memory.** State for permanently-lost vnodes is never freed.

### Why the panic fix does not cover this

`IncrementalAggState::reset_acquired_vnodes` (added in `84b1cd8f`) runs at capture for *acquired*
vnodes and clears `delta_chain_len` + `cold_groups` for them — but it deliberately does **not**
drop resident `groups`/`last_emitted` (at acquire-time those are the freshly-rehydrated state; the
stale leftover is indistinguishable there). The clean fix is to drop revoked-vnode state at
**revocation** time, so a later re-acquire merges into empty state.

## Severity

Correctness-under-churn, latent. The 3-node group-demotion kill-9 soak is green because its
assertions check progress/sink density, **not** exact per-group aggregate values, and a
lose-then-quickly-reacquire of the *same* vnode by the *same surviving node* is relatively rare.
Medium priority: real wrong-values bug, currently undetected.

## Design

Drop a revoked vnode's operator state at the **ownership-loss** point, on the **compute thread**
(the agg state lives there), before any later re-acquire merge. Per-rotation the revoked and
acquired sets are disjoint (a vnode is gained or lost, not both), so the double-count is strictly
cross-rotation — dropping at the revoking rotation is sufficient and safe.

**Safety:** the pre-rotation drain checkpoint (`adopt_draining_snapshot` + the drain) has already
handed the vnode's state to the new owner durably *before* `adopt_assignment_snapshot` rotates
ownership, so dropping the local copy post-rotation loses nothing.

Mirror the existing rehydration plumbing: `adopt_assignment_snapshot` stages the revoked set into a
shared handle; the operator graph drains it on the compute thread and drops each agg operator's
state for those vnodes — symmetric to `rehydrated_vnode_state` (`db.rs:176`/`701`) +
`OperatorGraph::apply_rehydrated_vnodes` (`operator_graph.rs:688`).

## Slices

1. **`IncrementalAggState::drop_vnodes(&mut self, revoked: &FxHashSet<u32>, vnode_count: u32)`**
   — remove every key/entry bucketed (via `delta_vnode_of`) to a revoked vnode from `groups`,
   `last_emitted`, `dirty_keys`, `dirty_keys_by_vnode`, `removed_by_vnode`,
   `last_emitted_dirty_by_vnode`, `delta_chain_len`; and (`state-tier`) `cold_groups`,
   `cold_vnodes`, `dirty_vnodes`. Bump `state_gen`, invalidate `size_cache`. Unit test: build two
   vnodes, drop one, assert its keys gone from every map and the other vnode untouched; assert
   `last_emitted ⊆ groups` holds.

2. **Operator hook.** `GraphOperator::drop_owned_vnodes(&mut self, revoked: &FxHashSet<u32>)`
   (default no-op); `SqlQueryOperator` forwards to the agg state when `QueryState::Agg`. Reset
   `prev_owned` accordingly so the next `take_newly_acquired` diff stays correct.

3. **Plumbing.** Add `pending_revoke_vnodes: Arc<Mutex<FxHashSet<u32>>>` on `Db`; in
   `adopt_assignment_snapshot` compute `revoked = old_set − new_owned` and extend the handle.
   `OperatorGraph` drains it each cycle (a sibling of `apply_rehydrated_vnodes`, NOT early-returning
   when only revokes are pending) and calls `drop_owned_vnodes` on every agg operator. Apply
   revoke-drops **before** acquire-merges within a drain pass (defensive; disjoint per rotation).

4. **Simplification opportunity.** With loss-time drop in place, a re-acquired vnode starts clean,
   so `reset_acquired_vnodes`' `cold_groups` retain becomes redundant for the re-acquire case
   (keep it for the first-capture/no-durable-acquire path — still needed to re-base
   `delta_chain_len`). Re-evaluate and trim, don't leave both doing the same job.

## Validation

- Integration test (`crates/laminar-db/tests/cluster_integration.rs`): node owns `v`, builds agg
  state, loses `v`, re-acquires `v`; assert the re-acquired group values equal the durable chain's
  (no doubling) and `last_emitted ⊆ groups`.
- `three_node_kill9_soak` (existing) — no regression; ideally extend its correctness check to
  assert exact agg values for a known key, OR add a focused soak that maximizes lose-then-reacquire
  churn (small vnode count, frequent kills) and diffs the agg output against a single-node oracle.
- 754 no-default + cluster + state-tier lib suites + clippy clean.

## Anchors

- `crates/laminar-db/src/db.rs:585` `adopt_assignment_snapshot` (revoked set unused), `:176`/`:701`
  `rehydrated_vnode_state` staging.
- `crates/laminar-db/src/operator_graph.rs:688` `apply_rehydrated_vnodes` (model for the drain).
- `crates/laminar-db/src/aggregate_state.rs:2000` `merge_groups` (additive merge, `:2044`),
  `reset_acquired_vnodes` (acquisition re-base).
