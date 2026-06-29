# State-tier hardening — deferred follow-ups

Status: **planned (2026-06-29).** Branch `feat/shuffle-barrier-after-kill-recovery`.
These are the gaps a grounded, adversarially-verified audit of the state-tier subsystem surfaced
that were NOT closed in the 0.28.0 "safe release" pass. The 0.28.0 pass shipped: cluster group
demotion default-OFF (embedded stays ON, soaked), the idle-TTL demotion fix (C3 below), dead-code
cleanup, and docs. Everything here is **cluster-path correctness or test/acceptance-gate work** that
needs soak validation this project's rules say must not be rushed.

Default posture until these land: group demotion is ON for embedded single-node (kill-9-soaked) and
OFF for cluster (`group_demotion(embedded)` in `crates/laminar-server/src/config.rs`). The cluster
correctness bugs below are therefore **not on a default path**; they must be fixed AND soaked before
cluster group demotion can default ON.

## C1 (HIGH) — barrier-alignment shuffle drain bypasses cold-group/cold-vnode promotion

**Where:** `crates/laminar-db/src/operator/sql_query.rs::ingest_shuffle` (~1268-1282) folds a shuffle
batch straight into `IncrementalAggState::process_batch` with NO cold check, unlike the steady-state
path `process_with_promotion` (~779-807) which calls `cold_vnodes_touched` + `cold_groups_touched`
and defers any batch touching a demoted group/vnode until its tier bytes are fetched back.
`ingest_shuffle` is reached only via `align_shuffle_barriers` → `ingest_to_stage`
(`operator_graph.rs` ~2449-2451 pre-staged rows, ~2498-2500 in-flight `VnodeData`), so peer rows that
arrive during checkpoint alignment never get the cold-aware second pass.

**Consequence:** if such a batch touches a group/vnode this node demoted in a prior epoch (still in
`cold_groups`, tier holds the authoritative state), `process_batch` rebuilds a fresh zeroed
accumulator. The key is then simultaneously resident AND cold → a later access-promotion
(`promote_group` → `apply_delta` REPLACE) loses the alignment increments, or the checkpoint emits the
group from both the resident slice and `cold_groups_by_vnode` → double-count. Either way a **silent
exactly-once violation**. Scope: cluster + state-tier group demotion only.

**Fix:** route `ingest_shuffle` through the same cold-handling as `process_with_promotion` — before
`process_batch`, compute `cold_vnodes_touched`/`cold_groups_touched`; if non-empty, `issue_fetch`/
`issue_fetch_group` and `defer(watermark, batch)` instead of processing. The promotion deferred queue
is already serialized into the checkpoint (`sql_query.rs` ~1138-1178), so deferred alignment rows are
captured + replayed on recovery rather than folded into a fresh accumulator. (Merely pausing demotion
during alignment is insufficient — `cold_groups` from prior epochs is the hazard.)

**Effort:** M. **Gate:** unit test (alignment batch touching a cold group must defer, not recreate) +
cluster kill-9 EO group soak.

## C2 (HIGH) — deferred re-base while `has_cold` lets the delta-chain base age past the prune horizon

**Where:** `crates/laminar-db/src/aggregate_state.rs` (~1778-1819) sets `force_full=false` whenever a
vnode `has_cold`, regardless of `delta_chain_len` (incremented unbounded). `delta_chain_max` is
clamped to `retain-1` (`pipeline_lifecycle.rs` ~759-762, comment "a chain base never ages out before
the chain head") — an invariant the `has_cold` path violates. The only safeguard is best-effort
proactive promotion (`sql_query.rs` ~747-757 via `try_send`, triggered only at
`delta_chain_len >= chain_max`, i.e. when the base is already at horizon+1). Prune is reference-blind
(`checkpoint_coordinator.rs` ~2554-2564: `horizon = epoch - max_retained`, no consult of
`base_epoch`). A missing chain link starts the operator fresh (`recovery_manager.rs` ~248-249).

**Consequence:** under cluster delta-primary, a vnode holding cold groups keeps the old FULL base
(which carries the demoted groups) alive on DELTA forever. If proactive promotion lags ~2 epochs
(easy with 100ms checkpoints + many cold groups, each a separate fetch), `base_epoch < horizon` and
prune deletes the base → recovery hits a missing link → the vnode's cold AND resident groups are
**silently lost**. Scope: cluster delta-primary + state-tier group demotion.

**Fix (preferred):** when a touched vnode has cold groups AND its chain is at the bound, force a FULL
re-base anyway and emit the cold groups durably in the same epoch — stage `StagedSlice::ColdGroups`
(or fold the tier bytes via `resolve_cold_groups`) alongside the resident FULL, exactly as the
non-delta path (`sql_query.rs` ~1358-1361) already does. The new base then carries both resident and
cold state; the old base is unreferenced; the `chain_max` invariant holds with no special-casing.
**Fallback:** reference-count/pin base epochs still referenced by any live `last_partial_epoch` chain
so prune cannot delete them (analogous to the existing coordinated-commit floor clamp).

**Effort:** L. **Gate:** unit test (demote groups → defer past chain_max → prune → recovery == full
baseline) + cluster kill-9 EO group soak.

## C3 (MEDIUM) — idle-TTL + demoted groups — PARTIALLY FIXED in 0.28.0; residual remains

**Fixed (0.28.0):** `demotable_groups` (`aggregate_state.rs`) is now idle-TTL-aware — it excludes
groups already past the eviction cutoff (`last_watermark_ms - idle_ttl_ms`), so they are retracted by
`evict_idle` instead of being demoted out from under it. Test
`demotable_groups_excludes_groups_past_idle_ttl`.

**Residual (deferred):** a group demoted while idle-but-within-TTL can later cross the TTL while cold.
`evict_idle` scans only `self.groups`, so a cold group past TTL is never retracted → its downstream
changelog row leaks past TTL and its cold-tier entry is never reclaimed. Full fix needs `evict_idle`
(or the demotion pass) to also scan `cold_groups` for keys past the cutoff and promote-then-retract
them — which requires round-tripping the tier blob to recover `last_emitted` for the retraction
(`drop_demoted_group` discards it). **Effort:** M.

## C4 (LOW, unverified) — runtime group-promotion fetch returning `None` retries forever

**Where (claim):** `sql_query.rs` ~729-742 (`drain_ready_groups` re-issues `issue_fetch_group` on
`Ok(Ok(None))`) and ~158-179. A genuinely-absent group (tier corruption / out-of-order drop) is
treated as transient → the touching batch stays deferred, `wants_input` backpressures, and the
operator wedges silently with no error. (The coordinator-side fetch correctly fails the epoch; this is
the runtime path.) **NOTE:** this finding's adversarial verification did not complete — verify against
current code before acting. **Fix:** bound retries; after N consecutive `None` for a key, escalate to
a hard error (a 6xxx checkpoint or 3xxx error) / rising-severity log. **Effort:** S.

## C5 (INFO) — no per-group size floor / batching

`demote_cold_groups` writes one `DemoteGroup` per group; many tiny idle groups → many fjall KV
entries (KV-separation tuned for KB..MB blobs) → write/space amplification + serial-worker pressure.
Correctness is unaffected (group keys are disjoint). Optional: coalesce sub-floor groups of the same
`(operator, vnode)` into one packed tier value, or skip demoting groups under a byte floor. Quantify
with `LAMINAR_SOAK_STATE_TIER_GROUP` before adding format complexity. **Effort:** M.

## Test / acceptance-gate gaps

- **Cluster exact-value correctness test (MEDIUM) — DONE.** The cluster soaks assert only commit
  progress, epoch monotonicity, `demotes>0 && fetches>0`, and EO sink density — never aggregate VALUES.
  `ClusterEngineHarness` now supports the cold tier (`spawn_delta_tier`, `tier_budget` param,
  `incremental_emit` when tiered), and **three** deterministic exact-value tests landed in
  `cluster_integration.rs::failures` (commits `8f204b99`, `5ce023e2`, `3941bf08`), all green + stable
  (~2-8s, CI-runnable, not `#[ignore]`):
  - `cluster_group_demotion_preserves_aggregate_values` — steady-state: 2-node cluster, tiny budget,
    demote → promote, union per-key totals == analytic expectation.
  - `cluster_demoted_groups_survive_crash_failover` — a follower demotes, then crashes; the survivor
    rehydrates the demoted groups from the durable delta chain (re-fed keys double).
  - `cluster_demoted_group_survives_lose_then_reacquire` — the `7528e24a` class: a vnode with a demoted
    group rotates off its node and back; the revoke path must drop A's resident + cold tracking before
    re-acquire rehydrates the chain (asserts A's total is single-counted, not doubled).
  None of these reproduced C1 or C2 (C1 needs the alignment-drain timing window; C2 needs the chain
  base aged past the prune horizon). **Remaining sub-case (LOW, for Phase 2 validation):** a
  many-checkpoint variant that drives `delta_chain_len` past `chain_max` with a vnode holding cold
  groups, to exercise C2's deferred-rebase prune race deterministically.
- **Kill-9 + group-demotion + EO-Kafka soak (HIGH-value, S).** The 3/3 green cluster group soak used
  the built-in generator (file://), not the EO Kafka sink. Demotion/promotion mutate the same per-vnode
  delta artifacts EO recovery replays. Run `three_node_kill9_soak` with
  `LAMINAR_SOAK_STATE_TIER=1 LAMINAR_SOAK_STATE_TIER_GROUP=1 LAMINAR_SOAK_DELTA_CHAIN_MAX=6
  LAMINAR_SOAK_KAFKA_BROKERS=...` and record dense `0..=max`, 0 dup/gap. (Do C1+C2 first — this soak is
  how they get validated.)
- **Soaks run in CI (MEDIUM).** All state-tier kill-9 soaks are `#[ignore]` (`cluster_soak.rs:596`
  embedded, `:715` cluster, `:1164` rotation); CI (`ci.yml:127`) skips `#[ignore]`. The default-ON
  embedded path's safety rests on a one-off manual run. **Fix:** a `workflow_dispatch` + weekly cron
  lane running `embedded_kill9_group_demotion_soak` (Docker-free, built-in generator) with
  `--run-ignored`; optionally a second job with a Redpanda service for the cluster EO soak. Keep off
  the per-PR critical path.
- **Bounded-RSS / endurance gate (LOW).** `embedded_kill9_group_demotion_soak` asserts an RSS ceiling
  only when `LAMINAR_SOAK_MAX_RESIDENT_BYTES` is set; default runs are ~1-2 min. **Fix:** assert a
  default RSS bound (GROUPS ≫ budget → `max_resident` within a multiple of budget) so the bounding
  property is regression-protected; separately run a multi-hour high-cardinality endurance soak on
  Linux/NVMe tracking RSS + fjall key count + compaction debt.
- **Linux-NVMe fjall p99 ≤ 1ms gate (LOW, perf-only).** ADR-005 Phase-0 gate; recorded numbers are
  Windows consumer-QLC only (tail exceeds 1ms under write pressure). Run the harness
  (`docs/plans/tiered-operator-state.md`) on datacenter Linux NVMe under sustained write load; record
  p99 vs the 1ms gate and the validated ingest ceiling. Performance, not correctness.

## Out of scope (separate A1-emit track) — NOT state-tier gaps

- **Tier-backed `JoinStateStore` (Slice 4, XL).** The changelog⋈changelog IVM join
  (`operator/incremental_join.rs`) has a `JoinStateStore` trait with only an in-memory impl; the
  tier-backed impl over the per-group KV is unstarted. Tracked under A1-emit Stage 3b, not here.
- **`IncrementalJoinOperator` unbounded RAM (LOW).** It reports `estimated_state_bytes` to the budget
  but overrides none of the tier hooks, so it can never be demoted (the in-memory IVM join retains
  Z-sets indefinitely by design). Default-OFF, gated `[LDB-1300]`. Interim hardening: a hard
  per-operator memory ceiling that fails loud (3xxx join error) instead of silently throttling source
  intake. Proper fix = the tier-backed store above.
