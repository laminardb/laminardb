# A1-emit Stage 3b — changelog ⋈ changelog incremental join (IVM)

Status: **planned (2026-06-28)**, cold-start ready. Branch `feat/shuffle-barrier-after-kill-recovery`.
Prereq DONE: tiered-state v2 group-granular KV (`docs/plans/tiered-state-v2-group-granularity.md`,
Slices 1–6) gives the per-join-key spill substrate. Owner scope: **inner + LEFT + multi-way**,
**hand-rolled IVM**, **tiered-state-backed**.

## The IVM math (two-sided)

For `A ⋈ B` with per-cycle deltas δA, δB (Z-set changelogs, `__weight`):
`output_delta = δA ⋈ B_new + A_old ⋈ δB`  where `B_new = B_old + δB`.
Then update both side states `A += δA`, `B += δB`. Weights multiply on a join match
(`w_out = w_a * w_b`); retracts (negative weight) net naturally. LEFT outer adds a per-left-key
match-count: emit a NULL-padded row when the count is 0, retract it when the first right match
arrives (and re-emit it when the last match retracts). Multi-way `A⋈B⋈C` chains pairwise — each
intermediate is itself a changelog feeding the next IVM join.

## State

Per side, indexed by join key: `join_key -> { full_row -> weight:i64 }` (an indexed Z-set). Access:
point-lookup by key (all rows+weights for a key), weighted upsert (drop at 0), scan (checkpoint),
per-key byte accounting.

`JoinStateStore` trait — `get(join_key) -> ZSet`, `upsert(join_key, row, weight)`, `scan`,
`estimated_bytes`:
- **in-memory impl** first (RowConverter + `HashMap<join_key_OwnedRow, HashMap<row_OwnedRow,i64>>`,
  like `MultisetState` but keyed by join key). Validates IVM correctness.
- **tier-backed impl** over the v2 per-group tier KV (`StateTierStore::{put/get/remove/scan}_group`,
  a join key == a "group"): hot keys in RAM, cold keys spilled, fetch-on-access promotion. Reuses
  the worker `*Group` protocol. This is why v2 was built first.

## Operator — `IncrementalJoinOperator`

Two-input (`input_port_count = 2`, `inputs[0]`=δA / `inputs[1]`=δB), like asof/stream-join wiring
in `operator_graph` (`add_query`/`wire_query_edges`, edges on ports 0/1). Per cycle:
1. read δA, δB (each a `__weight` changelog batch).
2. `out = δA ⋈ B_state(+δB applied incrementally) + A_state ⋈ δB` (the formula above), keyed by the
   ON columns; multiply weights; build the projected output (SELECT cols) + `__weight`.
3. apply δA→A_state, δB→B_state.
4. emit `out` (a joined changelog) → `Multiset` store (the join MV).
Stateful: `checkpoint`/`restore` both side stores via the group-delta path (reuse the columnar
encode + chain machinery the aggregate uses). `estimated_state_bytes` = both sides.

## Detection — `sql_analysis::detect_changelog_incremental_join`

Single equi-join (extend to N later), BOTH sides incremental MVs (vs Stage 3a, where the right is a
static dim). Reject asof/temporal/time-bound. Produce: join keys (left/right ON cols), join type
(inner/left), output projection. The operator does the weighted join in Rust (not a temp-SQL join,
which froze on `OnceAsync` in 3a) — so detection yields a structured config, not a rewritten SQL.

## Slices (build order — each validated + committed; default-OFF)

1. **Inner 2-way, in-memory state.** `JoinStateStore` trait + in-memory impl; `IncrementalJoinOperator`
   (inner only); detection; 2-input wiring; DDL guard/mode (→ `Multiset`, mark incremental). Integration
   test: `inc_mv_A ⋈ inc_mv_B` correct under updates on both sides (the δA⋈B + A⋈δB netting).
2. **LEFT outer.** Per-left-key match-count + NULL-pad emit/retract. Tests for 0→1 / last-match-retract.
3. **Checkpoint/restore.** Both side stores via the group-delta columnar path; restart test.
4. **Tier-backed state.** Swap the in-memory store for the v2 tier-backed impl (fetch-on-access). Soak
   (no-kill) for bounded RAM on a large-key-space join.
5. **Multi-way.** `A⋈B⋈C` as chained pairwise IVM joins; detection over N tables; tests.

## Guard / scope

Until S1 lands, `changelog ⋈ changelog` stays rejected by the Stage-2/3a guard
(`incremental_mv_consumer_error`, `[LDB-1300]`). Each slice flips a narrower part of the guard.

## Notes
- The 3a `ChangelogEnrichOperator` re-plans physical each cycle to dodge `HashJoinExec` `OnceAsync`
  staleness; the 3b operator avoids DataFusion join execution entirely (hand-rolled Z-set join), so it
  doesn't have that cost.
- Tier-backed join state inherits v2's constraint: needs delta (cluster-only today). In-memory state
  (S1–S3) has no such constraint — single-node 2-way IVM works without the tier.
