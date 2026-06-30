# Tier-backed IVM join state (A1-emit Stage 3b — Slice 4, XL)

Status: **scoped + adversarially reviewed, unstarted (2026-06-30).** This is the spill-to-disk
replacement for the in-memory changelog⋈changelog join's `JoinStateStore`. It is the last hardening item
adjacent to the state-tier work; it is tracked under the A1-emit track, NOT the state-tier track.
Default-OFF, `[LDB-1300]`-gated.

> **Review note.** An adversarial review of this plan against the code found 2 blockers and ~5 majors,
> all folded in below (see §3, §4, §5.2/§5.3/§5.7/§5.8, §6). The headline corrections: recovery
> dispatches through `apply_vnode_chain` (default **no-op** — a literal reading of an earlier draft would
> have **silently dropped every cold key on restart**), and the join's `encode_frame` checkpoint has no
> slot for cold-vnode markers or deferred batches, so the whole-op checkpoint format must be replaced.
> Do NOT start S4.4 until B1/B2/M4 (§6) are internalized.

> Written for a fresh session. Everything below is grounded in current code with file:line. Read
> §1–§3 to understand the surface, §4 for the design, **§5 (hazards) is the load-bearing part**, §6 for
> the recovery fork, §7 for stale-doc reconciliations, §8 for the slice plan.

## 0. Why

The IVM join operator (`operator/incremental_join.rs`) holds **two unbounded in-memory Z-sets**
(`left_state`, `right_state`). It reports `estimated_state_bytes` to the node memory budget so it
*throttles* source intake when over budget (`pipeline_callback.rs:1930` `state_over_budget` → pause
intake), but it overrides **none** of the tier hooks, so its state can never be demoted — it retains
Z-sets indefinitely by design. A large-key-space or skewed join therefore has no RAM bound except the
hard per-operator ceiling that fails the query (`operator_graph.rs:2084`, a 3xxx-class error). The
proper fix is a tier-backed store that spills cold join keys to the fjall cold tier, mirroring the
aggregate's group demotion. Interim mitigation (if this slice is not built soon): make that hard
ceiling fail loud with a join-specific 3xxx error rather than silently throttling intake forever.

## 1. Current architecture (grounded)

`crates/laminar-db/src/operator/incremental_join.rs` (prod lines 1–658).

- **Two-sided indexed Z-set.** `JoinStateStore` trait (`:32-42`) = `join_key -> {full_row ->
  multiplicity:i64}`. Methods (the REAL signatures — the planning docs are wrong, see §7): `upsert(key:
  &[ScalarValue], row: &[ScalarValue], weight: i64)`, `get(key) -> Vec<(row, w)>`, `contains_key(key)
  -> bool`, `snapshot() -> Vec<(row, w)>` (ALL rows, key dropped — re-derived from the row via
  `key_plain_pos`), `estimated_bytes()`. Default impl `InMemoryJoinState` (`:44-112`):
  `FxHashMap<Vec<ScalarValue>, FxHashMap<Box<[ScalarValue]>, i64>>` + incremental `bytes` accounting.
  **Keys are `&[ScalarValue]`, not arrow `OwnedRow`/`RowConverter`** — a tier impl needs an explicit
  key→bytes encode step.
- **Operator state** (`IncrementalJoinOperator`, `:175-188`): `left_state`/`right_state`
  (`Box<dyn JoinStateStore>`), `left_keys`/`right_keys` (ON column names), `left_outer: bool`,
  lazily-resolved `left_info`/`right_info` (`SideInfo` schema resolution), `out_cols`/`out_schema`.
- **Two input ports, no key routing.** Port 0 = left changelog → `left_state`; port 1 = right →
  `right_state` (`:549-550`, wired as plain DAG edges in `operator_graph.rs:1083-1089`). It does NOT
  consult watermarks (`_watermarks` ignored, `:546`).
- **The IVM delta has a hard CROSS-SIDE dependency** (`process`, `:544-631`): per cycle the output delta
  = term2 `A_old ⋈ δB` (`join_term(δB, Right)` probes `left_state.get(key)`, `:595`) computed BEFORE
  advancing right_state, then term1 `δA ⋈ B_new` (`join_term(δA, Left)` probes `right_state.get(key)`,
  `:618`). **Computing the delta for join key K requires BOTH sides' full Z-set for K resident.**
- **LEFT-outer bookkeeping** adds three more probes of the *other* side: `presence_old` (per-δB-key
  `right_state.contains_key` before δB, `:582-591`), the empty↔non-empty NULL-pad transition (re/retract
  pads for every resident left row at a flipped key — `left_state.get(key)`, `:606-614`), and
  `first_right` catch-up `emit_left_catchup` which iterates **`left_state.snapshot()` — ALL left rows**
  (`:412`, `:601`).
- **Whole-operator checkpoint, NO vnode/cluster participation.** `checkpoint`/`restore` (`:633-653`)
  serialize BOTH sides into ONE framed blob (`side_checkpoint_bytes` → `__weight` IPC per side,
  `encode_frame`/`decode_frame`, `:426-527`). The operator has **no** `vnode`, `hash_rows`,
  `attach_cluster_shuffle`, or `ingest_shuffle` (grep: zero matches) and overrides none of
  `checkpoint_by_vnode`/`demote_vnode`/`can_demote`/`attach_state_tier`/… — it recovers entirely from
  the whole-node manifest. **It is single-node only** (module doc `:13`).
- **No key-shuffle → keys are NOT vnode-co-partitioned.** Both sides arrive locally on ports 0/1;
  there is no co-partitioning of matching keys onto a shared vnode. **Consequence: the demotion unit
  cannot be a vnode; it must be the join key.**
- **Default-OFF, `[LDB-1300]`-gated.** The whole incremental path is gated on `incremental_emit`
  (default false, `config.rs:538`); the shape guard `detect_changelog_incremental_join`
  (`sql_analysis.rs:900-1012`) allows only INNER/LEFT equi-joins of two incremental MVs (rejects
  RIGHT/FULL, self-join, wildcards, expr projections, non-equi residual, WHERE/GROUP/etc.); the
  terminality guard `[LDB-1300]` (`ddl.rs:46-52`, `:1219-1247`) rejects sink/SUBSCRIBE consumers.

## 2. The aggregate tier model (the template to mirror)

The keyed aggregate (`operator/sql_query.rs` + `aggregate_state.rs`) is the only tier-backed operator.
Reuse its shape:
- Per-group cold set + dirty tracking: `cold_groups: AHashMap<OwnedRow, i64>`, `dirty_keys_by_vnode`,
  `can_demote_group`, `demotable_groups` (idle-first, clean, TTL-aware).
- Demote: `encode_group(key)` → tier `DemoteGroup` (await) → `drop_demoted_group(key)` (write-before-drop).
- Fetch-on-access: `AggPromotion` (`sql_query.rs:73-262`) — `cold_groups_touched(batch)` → `issue_fetch_group`
  + `defer(wm, batch)`; later `drain_ready_groups` → `apply_group_state`/`promote_group` → replay;
  `watermark_hold = min_deferred_watermark`, `wants_input` gates at 8192 deferred rows,
  `MAX_PROMOTION_FETCH_MISSES=32` escalation (the C4 fix).
- Checkpoint/recovery: `checkpoint_by_vnode` stages `StagedSlice::ColdGroups{group_keys}` for
  partially-demoted vnodes (resident rides the whole-node manifest); the coordinator's
  `resolve_cold_groups` (`checkpoint_coordinator.rs:587`) FetchGroups each + folds into one COLD-ONLY
  durable partial; restart `rehydrate_cold_vnodes` (`pipeline_lifecycle.rs:198`) merges it additively
  (disjoint cold⊥resident keys → no double-count). **Single-node uses this same path** via a single-owner
  vnode registry + `set_delta_enabled(true)` WITHOUT `delta_chain_max` (the whole-node manifest stays
  authoritative; cold groups ride cold-only partials) — `docs/plans/single-node-group-demotion.md`,
  shipped `7c9781c2`.

## 3. Reusable vs new vs extend

**Reuse as-is** (operator-agnostic, dispatches polymorphically through `GraphOperator`):
- `StateTierStore` per-group KV — `put_group/get_group/remove_group/scan_groups`, key layout
  `operator \0 vnode_be32 \0 group_key_bytes` (`state_tier/mod.rs:87`). Group bytes are arbitrary → a
  join key encodes straight in. No store/worker extension needed.
- The worker `TierRequest::{DemoteGroup, FetchGroup, DropGroup}`.
- The demotion driver: `state_over_budget` → `maybe_demote_state` → `graph.demote_cold_groups(to_free)`
  fan-out (`operator_graph.rs:2627`), `can_demote`, `set_state_tier`, `set_vnode_count`,
  `enable_group_delta_tracking`.
- `StagedSlice::{Cold, ColdGroups}`, `VnodePartial`, `resolve_op_chain`, `rehydrate_cold_vnodes`.
- The `AggPromotion` fetch-on-access pattern (port it to a `JoinPromotion`).

**Implement new on the join** (the ~9 `GraphOperator` hooks + a tier-backed store):
- `JoinTierStore`: a `JoinStateStore` impl over the per-group KV, adding cold-key tracking, dirty
  tracking on every `upsert`, `demotable_keys`, `cold_keys_touched`, `encode_key`/`drop_demoted_key`/
  `promote_key`, and a per-join-key two-sided codec (§4).
- Operator hooks: `attach_state_tier`, `enable_group_delta_tracking`, `demote_cold_groups`,
  `can_demote`/`demote_vnode` (key-granular semantics), `checkpoint_by_vnode`, `take_tier_cold_vnodes`,
  `has_pending_promotion`, `watermark_hold`, `wants_input`. A single-owner vnode model so keys bucket
  `vnode = key_hash(join_key) % vnode_count`.
- **Three DISTINCT recovery/promotion hooks — do not conflate them** (an earlier draft did, → B1):
  `apply_group_state` (`sql_query.rs:778,873`) = steady-state per-group promote (fetch-on-access);
  `apply_vnode_state` (`:1568`) = whole-vnode merge for live rebalance; **`apply_vnode_chain`
  (`:1618`) = the RESTART rehydrate path** that `rehydrate_cold_vnodes` actually calls
  (`pipeline_lifecycle.rs:241`). `apply_vnode_chain`'s trait default is a **silent `Ok(())` no-op**
  (`operator_graph.rs:106`); the agg only recovers because it OVERRIDES it (delegating the no-delta case
  to `apply_vnode_state`). The join MUST override `apply_vnode_chain` (decode the cold blob → upsert both
  sides) or every cold key is dropped on restart with no error.

**Extend (the non-operator-agnostic coordinator points):**
- `resolve_cold_groups`/`resolve_full_with_cold_groups` (`checkpoint_coordinator.rs:587/606`) call
  `merge_serialized_agg_cps` — **agg-specific** (rkyv-decodes `AggStateCheckpoint`, `append_disjoint`s
  columnar arrays). A join cold blob is a two-sided `encode_frame` per key, so merging N cold keys into
  one cold-only partial is a **frame-level REGROUP, not a byte concat**: collect every key's left IPC
  batches into one left side + every key's right into one right side, then re-`encode_frame`
  (`restore_side`/`parse_side` iterate batches per side and expect one schema-consistent side stream).
  Generalize the resolver over a per-operator codec tag.

## 4. Design

**Demotion unit = the join key** (settled intent across all prior docs; forced by §1's no-co-partition).

**Tier KV value = both sides' Z-set for one join key, packed into ONE blob**, stored at
`group = encode(join_key)`. One tier entry per cold join key keeps demotion/promotion atomic across the
two sides and matches the agg single-blob-per-group model. `encode_key` serializes left-Zset +
right-Zset (reuse the per-side `__weight` IPC framing + `encode_frame`). The alternative (suffix the side
into the group bytes → two entries per key) is rejected: it doubles tier ops and splits the atomicity the
delta computation needs (§5.1).

**`JoinTierStore`** wraps the per-group KV + a small resident hot map. On every `upsert` it marks the
key dirty (per single-owner vnode bucket) AND maintains the incremental `bytes` accounting the in-memory
store does (`incremental_join.rs:73-82`) — **decrementing `bytes` on demote and re-incrementing on
promote** (M5). If demotion doesn't shed `bytes`, `state_over_budget` never clears, `maybe_demote_state`
fires forever, and intake stays throttled — demotion silently fails to relieve pressure.
`demotable_keys` returns clean, idle-first keys. Demote: `encode_key` both sides → `DemoteGroup` (await)
→ drop both sides' rows for the key from the hot map → insert into `cold_keys`.

**Fetch-on-access (`JoinPromotion`).** In `process`, before ANY `contains_key`/`get`/`upsert`/presence
probe, compute the set of cold keys touched by `delta_a ∪ delta_b` (both ports). If non-empty, issue
`FetchGroup` for each and DEFER the whole cycle's batches. On a later cycle, `drain_ready` decodes the
packed blob → re-insert BOTH sides into the hot store → clear cold marker → replay. **Two-port
adaptation (M6):** the agg's `AggPromotion::defer(wm, batch)` is single-input/single-watermark; the join
takes two ports + two watermarks. Each deferred batch must be **tagged with its `JoinSide`** so replay
routes to the correct port, preserve per-side cross-cycle order, and `watermark_hold` returns
`min(left_wm, right_wm)`. (Cross-KEY replay reordering is safe — deferred and live cycles touch disjoint
keys by construction; side-routing and the hold value are not, so spell them out.) Mirror
`MAX_PROMOTION_FETCH_MISSES` escalation and the `wants_input` cap.

**Checkpoint/recovery — reuse the single-node agg cold-only-partial path (Path A, §6), but the whole-op
checkpoint FORMAT must change (B2).** The current `encode_frame(left, right)` (`:497-506`) is just
`[left][right]` — it has NO slot for (a) which vnodes hold cold keys (the agg carries
`AggOpCheckpoint.cold_vnodes`, read by `restore` into `pending_cold_rehydrate` so `take_tier_cold_vnodes`
works — without it `rehydrate_cold_vnodes` never runs and cold keys are lost) nor (b) deferred batches
(a checkpoint mid-promotion must persist them or they're lost while the source counts them consumed —
the agg carries `AggOpCheckpoint.deferred`). **Replace `encode_frame` with a struct (rkyv, like
`AggOpCheckpoint`) carrying `{ left, right, cold_vnodes, deferred: [(wm, side, ipc)] }`.** Then: resident
keys ride this whole-op manifest; `checkpoint_by_vnode` stages `StagedSlice::ColdGroups{group_keys=cold
join keys}`; the coordinator (extended `resolve_cold_groups`, frame-regroup) writes a cold-only durable
partial; restart `take_tier_cold_vnodes` (from the new `cold_vnodes`) → `rehydrate_cold_vnodes` →
**`apply_vnode_chain`** (override) decodes the join blob and `upsert`s both sides additively (cold⊥resident
disjoint → no double count). Enable via `enable_group_delta_tracking`-equivalent dirty tracking WITHOUT a
delta chain (whole-op manifest stays authoritative).

## 5. Hard correctness hazards (the load-bearing part)

These have NO aggregate analog and are why this is XL. A new session should design each explicitly with
a RED-first test before wiring demotion.

### 5.1 Two-sided atomic promotion
Computing K's delta needs left_state[K] AND right_state[K]. A fetch-on-access that promotes one side but
not the other (or a demote that sheds one side while the other is dirty) yields a wrong join result.
→ Demote and promote a key as an atomic pair (single packed blob). Never probe one side of a cold key
without the other being resident.

### 5.2 LEFT-outer presence-vs-cold (the subtle one)
`right_state.contains_key(K)` is used as "does K have a right MATCH" to decide NULL-padding
(`:586,:607,:621`). A demoted key reads `contains_key == false` → the join emits a **spurious NULL-pad**
(or fails to retract one). A cold key is NOT absent — it has state in the tier.
→ The deferring-the-whole-cycle approach (§4) DOES cover all per-cycle probes, because every key probed
in `presence_old` (`:586`), the transition loop (`:606-614`), the first-sight pad (`:619-624`), and both
`join_term` `get()`s is drawn from `delta_a`/`delta_b`. **But the load-bearing probe is the cross-side
`get()`** (`:610` reads `left_state.get(key)` for a `delta_b`-only key) — it works ONLY because demotion
is atomic per join key across both sides (§5.1). Surface that.
→ The "avoid the fetch when only presence is needed" optimization is **subtler than key-existence**:
`contains_key` is a per-SIDE question (does the RIGHT side hold rows for K), not "does K exist." A key
that is cold with left rows but an EMPTY right side (a normal LEFT-join unmatched key — `encode_frame`
encodes the empty side as `None`) must read `right-present == false`. So a single `cold_keys` existence
set is WRONG here; a presence-only index would need **per-side presence bits** (left-nonempty,
right-nonempty) stored resident. Prefer plain fetch-before-probe (option a); only add per-side presence
bits if a soak shows presence-churn fetch volume hurts.

### 5.3 `emit_left_catchup` full snapshot under cold state — and the `snapshot()` two-caller conflict (M4)
First-right catch-up iterates `left_state.snapshot()` over ALL left rows (`:412`). If the left side is
partially cold, a correct catch-up must include cold rows.
**The trap:** `snapshot()` (`:99-107`) has TWO callers with OPPOSITE needs — `emit_left_catchup` wants
ALL rows (incl. cold); `side_checkpoint_bytes` (`:434`, the manifest) must serialize **resident-only** so
the manifest stays disjoint from the cold-only partial (Path A). Making `snapshot()` "tier-aware" to fix
catch-up would put cold keys in BOTH the manifest and the `ColdGroups` partial → **double-count on
additive recovery** (the exact failure `single-node-group-demotion.md` warns about).
→ Split into `snapshot_resident()` (checkpoint, resident-only) vs an explicitly tier-aware catch-up scan
(`scan_groups`). And note both catch-up options are lossy: gating demotion until after `first_right`
(m10) means a LEFT join accumulating a huge left side BEFORE any right batch can't demote → it hits the
very ceiling §0 exists to remove. Prefer the tier-aware catch-up scan + resident-only manifest; pick
deliberately and document it.

### 5.4 Fetch-before-probe in `process`, and in any future `ingest_shuffle`
Mirror the C1 fix (`sql_query.rs:1328` `ingest_shuffle` cold-defer): NEVER fold a row touching a cold key
into a fresh/zeroed side store. The cold-key check must precede term2/term1 and the δ-apply upserts. The
join has only `process` today (no `ingest_shuffle` override), so the surface is smaller, but a
multi-way/clustered future reintroduces it.

### 5.5 Dirty tracking + `can_demote` for a Z-set key
`upsert` mutates a key's multiplicities. A key touched since the last capture is NOT demotable (its tier
bytes would not match the checkpoint). Track dirty per key; clear on capture. A clean key's Z-set is
value-preserving across demote→promote (Z-set net is exact), so correctness reduces to the
clean-since-capture gate + 5.1/5.2.

### 5.6 Idle-TTL / retraction interaction
The join has no idle-TTL eviction today (unlike the agg's `evict_idle`). If one is added later, the
C3-class "cold key crosses TTL while demoted" hazard reappears — out of scope for S4 but note it.

### 5.7 Key codec — the agg gets this free; the join does not
Join keys are `&[ScalarValue]` (`:34`), not arrow `OwnedRow`/`RowConverter`. The agg's whole tier path
(group_key bytes, `vnode = key_hash % vnode_count`, the per-group KV) rides for free on `RowConverter`.
The join must hand-build: a **reversible, type-tagged, NULL-safe, multi-column** `Vec<ScalarValue> →
bytes` encoding, and a **stable hash** for the vnode bucket that survives a restart (so `ColdGroups`
staging and rehydration agree on a key's vnode). Note NULL keys are KEPT on the LEFT side of a LEFT join
(`parse_side` `skip_null=false`, `:230,:568-571`) but can never match a right row → they should be
**excluded from demotability** (don't spend a tier entry on a key that only ever NULL-pads). This codec
is a prerequisite for everything else; build + property-test it first (round-trip + stable-hash).

### 5.8 "Cold keys touched" is per-side, not leading-column (m9)
The agg's `cold_groups_touched` (`aggregate_state.rs:2505`) extracts the key from the LEADING
`num_group_cols` via the shared `row_converter`. The join's key columns are NOT leading and DIFFER per
side (`SideInfo.key_idx`, distinct `left_keys`/`right_keys`). The join's "cold keys touched" must project
EACH side's batch through that side's `key_idx` and hash with the §5.7 codec — it cannot reuse the agg's
leading-column path. Porting `AggPromotion` hides this; budget for it.

## 6. Recovery design fork

The tier WIPES on restart, so cold-key truth must live in the durable checkpoint, not the tier.

- **Path A (recommended): single-owner vnode + cold-only partials.** Reuse the agg's proven single-node
  path: resident in the whole-op manifest, cold keys in `ColdGroups` partials, restart rehydrate. Pros:
  maximum reuse; recovery is the existing, soaked machinery. Cons / non-obvious work (the review's
  blockers): the join must gain a (single-owner) vnode model + `checkpoint_by_vnode`; **override
  `apply_vnode_chain`** (NOT `apply_vnode_state` — B1, §3) since that is what `rehydrate_cold_vnodes`
  calls and its default is a silent no-op; **replace `encode_frame` with a struct carrying `cold_vnodes`
  + `deferred`** (B2, §4) so `take_tier_cold_vnodes` and mid-promotion checkpoints work; and the cold
  blob's decode must **route rows to the correct SIDE** (a cold key has left AND right rows; the
  cold-only partial restores into left_state/right_state respectively). One more ordering subtlety
  (m11): a side that was never resident but has cold keys has no `SideInfo` after `restore` (its manifest
  `side_checkpoint_bytes` returned `None`), so `apply_vnode_chain` must resolve that side's `SideInfo`
  from the cold blob's own IPC before upserting. `resolve_cold_groups` must learn the join frame-regroup
  codec (§3).
- **Path B: join-local cold-aware whole-op checkpoint.** Keep the bespoke `encode_frame`; at checkpoint,
  fold the cold keys' tier bytes into the durable blob (the operator drains its own cold keys from the
  tier — async via the worker — at capture). Pros: no vnode/coordinator changes. Cons: a bespoke
  recovery path with no existing soak coverage; the operator must block capture on a tier fetch-all of
  cold keys (or keep a durable shadow, which fights demotion). Higher correctness risk.

**Recommendation: Path A.** It converts the join's recovery to the same cold-only-partial mechanism the
agg already soaks, and isolates the new code to one coordinator codec extension + the operator hooks.

## 7. Stale-doc reconciliations (do these first)

1. **Target the REAL trait.** Planning docs (`a1-emit-stage3b-incremental-join.md:24-27`,
   `tiered-state-v2-group-granularity.md:101-103`) describe `get/upsert/scan/estimated_bytes` over
   `OwnedRow`/`RowConverter`. The implemented trait is `upsert/get/contains_key/snapshot/estimated_bytes`
   over `&[ScalarValue]` — **no `scan`** (it's `snapshot`), plus `contains_key` (LEFT-join presence) and
   `snapshot` (checkpoint + catch-up) the docs never mention. Add an explicit key-encode step.
2. **The "needs delta (cluster-only)" blocker is STALE.** `a1-emit-stage3b-incremental-join.md:72`
   asserts tier-backed join state needs cluster-only delta. But single-node agg group demotion shipped
   (`7c9781c2`, default-ON embedded) via cold-only partials WITHOUT a delta chain. So a **single-node
   tier-backed join is feasible now** — re-derive, don't inherit the stale constraint.
3. **Checkpoint path mismatch.** Docs say "reuse the group-delta columnar path"; the operator actually
   uses a bespoke `encode_frame` full-side frame (`:638-652`). Path A reconciles this by adding
   `checkpoint_by_vnode` for cold keys while keeping `encode_frame` for resident.
4. **"Slice 4" collision.** v2 doc's Slice 4 = agg checkpoint/compaction; 3b doc's Slice 4 = this
   tier-backed join. The hardening doc's "Slice 4, XL" means **3b-S4** (this doc).
5. **API spelling.** It's `scan_groups` (plural), not `scan_group`.

## 8. Slice plan

Build in-memory correctness is done (3b S1–S3). This slice (3b-S4) breaks down as:

- **S4.0 — Interim loud ceiling (S).** Make the per-operator ceiling fail with a join-specific 3xxx
  error instead of silently throttling intake forever. Ships value immediately, independent of the rest.
- **S4.1 — Key codec + `JoinTierStore` + two-sided per-key blob (M→L).** FIRST build the §5.7 key codec
  (reversible, type-tagged, NULL-safe, multi-column) + stable hash, property-tested. Then the store over
  the per-group KV: `encode_key`/`decode` of both sides packed; **`bytes` accounting that decrements on
  demote, increments on promote (M5)**; dirty tracking on `upsert`; `cold_keys`, `demotable_keys`
  (exclude NULL-key left rows), `cold_keys_touched` (per-side `key_idx` projection, §5.8). Unit tests:
  encode→drop→promote round-trip; clean-vs-dirty gate; bytes-returns-to-baseline after promote.
- **S4.2 — Single-owner vnode + dirty-tracking enable (S).** Bucket keys by `key_hash % vnode_count`;
  honor `enable_group_delta_tracking`/`set_vnode_count`/`attach_state_tier`.
- **S4.3 — Fetch-on-access `JoinPromotion` (L).** Defer-before-probe in `process`; promote both sides
  atomically; **two-port side-tagged defer + `watermark_hold = min(left,right)` (M6)**; miss-escalation;
  `wants_input`. **Includes the §5.2 LEFT-outer presence-vs-cold design + RED test and the §5.3 catch-up
  decision (tier-aware scan + resident-only `snapshot`).**
- **S4.4 — Checkpoint/recovery via cold-only partials (XL — the blocker-heavy slice).** Replace
  `encode_frame` with the `{left, right, cold_vnodes, deferred}` struct (B2); `checkpoint_by_vnode`
  ColdGroups for cold keys staging resident-only manifest (`snapshot_resident`, M4); extend
  `resolve_cold_groups` with the join frame-REGROUP codec (M7); **override `apply_vnode_chain`** (B1) to
  decode + side-route + upsert, resolving a never-resident side's `SideInfo` from the blob (m11);
  `take_tier_cold_vnodes` from the new `cold_vnodes`. **Do NOT start until B1/B2/M4 are internalized.**
  Tests: restart-survives-demotion (resident + cold disjoint, no double count); checkpoint-mid-promotion
  persists+replays deferred batches; a never-resident-right-side LEFT-join cold key recovers.
- **S4.5 — Demotion trigger wiring + default-OFF flag (S).** `demote_cold_groups` body; config plumbing;
  keep `[LDB-1300]`/`incremental_emit` posture.
- **S4.6 — Soaks (acceptance gate, M).** No-kill bounded-RAM soak on a large-key-space join (the headline
  S4 gate); exact-value oracle vs an in-memory no-demotion run; a LEFT-outer-under-cold value test; a
  single-node kill-9 recovery soak (riskiest — build fresh, mirror `embedded_kill9_group_demotion_soak`).

## 9. Open decisions for the owner

- **Path A vs B (§6).** Recommendation A. Confirm before S4.4.
- **§5.2 presence probe:** plain fetch-before-probe (recommended) vs adding resident **per-side presence
  bits** (left-nonempty/right-nonempty — NOT a key-existence set, which is wrong). Decide from the soak's
  presence-churn fetch volume.
- **§5.3 catch-up:** tier-aware catch-up scan + resident-only `snapshot_resident` (recommended) vs gating
  demotion until post-`first_right` (which reintroduces the OOM cliff for right-late LEFT joins). Both
  lossy; pick deliberately.
- **Single-node vs cluster scope.** §7.2 says single-node is feasible now; the join is single-node only
  anyway. Confirm S4 targets single-node (embedded), deferring any clustered/multi-way join state.
- **Does S4 block on 3b S5 (multi-way)?** No — S4 is per-pairwise-join state; multi-way composes pairwise
  operators. S4 can ship before S5.

## 10. References

- Join operator: `crates/laminar-db/src/operator/incremental_join.rs` (trait `:32-42`, in-memory
  `:44-112`, operator `:175-204`, `process` `:544-631`, catch-up `:405-419`, checkpoint `:633-657`).
- Wiring/guards: `sql_analysis.rs:900-1012` (detector), `ddl.rs:46-52,1219-1247` (`[LDB-1300]`),
  `operator_graph.rs:1083-1089,1560-1566` (edges/construction), `config.rs:538` (`incremental_emit`).
- Tier surface: `operator_graph.rs:27-155` (`GraphOperator` hooks), `sql_query.rs:73-262,1374-1568`
  (AGG template + `AggPromotion`), `aggregate_state.rs` (cold sets, demote/promote, codec),
  `state_tier/mod.rs:43-92,150-195` + `worker.rs:27-147` (KV + worker),
  `checkpoint_coordinator.rs:24-69,510-669` (StagedSlice + resolvers),
  `pipeline_callback.rs:1930-1999` (driver), `pipeline_lifecycle.rs:198,1394` + `recovery_manager.rs:274`
  (recovery).
- Prior plans: `docs/plans/a1-emit-stage3b-incremental-join.md` (the 3b slice plan, S4),
  `docs/plans/tiered-state-v2-group-granularity.md` (the per-group KV substrate),
  `docs/plans/single-node-group-demotion.md` (the single-node cold-only-partial recovery to mirror),
  `docs/plans/state-tier-hardening-followups.md` (out-of-scope note + C1–C4 hazards to mirror).
- Memory: `[[a1-emit-stage1]]` (Stage 3b status), `[[tiered-state-adr005-plan]]` (tier subsystem).
