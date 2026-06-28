# Single-node v2 group demotion — enablement + restart durability

Status: **IMPLEMENTED 2026-06-28 (`7c9781c2`), default-OFF** — cold-only artifact recovery built + restart
integration test green (`single_node_group_demotion_survives_restart`); 839 state-tier lib tests + the
vnode-demotion test pass; clippy/fmt clean on state-tier + cluster. **Single-node kill-9 soak is the
remaining acceptance gate before any default-ON.** Branch `feat/shuffle-barrier-after-kill-recovery`.
Goal (DONE): make v2 group-granular demotion fire AND survive restart **single-node** (no controller).

## Why it doesn't work today (grounded)

- `demotable_groups` (`aggregate_state.rs:2218`) early-returns `[]` unless `emit_changelog &&
  delta_enabled && !dirty_all && delta_vnode_count == Some(vnode_count)`.
- `delta_enabled` is set only by `enable_delta_checkpoints` (`sql_query.rs:336`, sets `delta_chain_max`
  too) which is reached only when `cluster_shuffle.is_some() && delta_chain_max.is_some()`
  (`operator_graph.rs:1651-1656`); `set_delta_chain_max` needs the full controller stack
  (`pipeline_lifecycle.rs:730-754`). Single-node satisfies none → `delta_enabled` stays false.
- **state-tier implies cluster** (Cargo.toml), so under state-tier single-node the `cluster`-gated
  per-vnode capture (`capture_vnode_states`, `pipeline_callback.rs:1101`) DOES run, and
  `checkpoint_by_vnode` → `checkpoint_groups_by_vnode` (`sql_query.rs:1260`) which seeds
  `delta_vnode_count` when `delta_enabled` (`aggregate_state.rs:1681-1686`). So **seeding is free once
  `delta_enabled` is true** — no need to touch `checkpoint_groups`.
- `skip_whole_node_agg` = `delta_chain_max.is_some()` (`sql_query.rs:346`). With `delta_enabled=true`
  but `delta_chain_max=None`, it stays false → the **whole-node manifest stays the authoritative agg
  checkpoint** (the review's key requirement; do NOT set `delta_chain_max` — single-node has no
  delta-primary restart recovery, so it would lose ALL agg state).

## The restart-durability gap (the hard part)

- The tier is **wiped on startup** (`state_tier/mod.rs:43` `remove_dir_all`) — NOT durable. Demoted
  state must reach the **durable per-vnode partials** (object-store/file backend), which single-node
  writes every checkpoint (`write_vnode_partials`, `checkpoint_coordinator.rs:993`, runs single-node).
- `checkpoint_groups_by_vnode` captures only **resident** groups; `cold_groups` (dropped from
  `groups`) are in no durable capture → lost on restart. The whole-node manifest also captures only
  resident groups. `cold_vnodes` (whole-vnode demotion) is recovered via `pending_cold_rehydrate` /
  `rehydrate_cold_vnodes`, but cold GROUPS (sub-vnode) have no equivalent.
- **Precedent (cold VNODE):** `StagedSlice::Cold` (`checkpoint_coordinator.rs:28`) makes the
  coordinator `fetch_cold_slice` (`:493`) read a demoted vnode's bytes back from the tier into its
  partial on a forced full upload. We mirror this at **group granularity**.

## ⚠️ RECOVERY-SIDE CRUX (found 2026-06-28 during impl — the real hard part)

The capture side is easy (coordinator fetches cold groups + folds them into the durable partial). The
**recovery side is the blocker**, for two grounded reasons:
1. **Single-node restart reads the WHOLE-NODE MANIFEST, not the per-vnode partials.** Recovery does
   `graph.restore_from_bytes(manifest)` (`pipeline_lifecycle.rs:1328`) which restores resident groups
   from the `AggOpCheckpoint`; per-vnode partials are read back ONLY for `cold_vnodes`
   (`rehydrate_cold_vnodes`, `:1366`) or delta-primary (`:1379`, gated on `skip_whole_node_agg`). A
   group-demotion node has NEITHER → its partials (with the merged cold groups) are never read.
2. **The per-vnode rehydration apply (`merge_groups`, `aggregate_state.rs:1980`) is ADDITIVE, not
   REPLACE** ("merges into existing groups associatively"). So you cannot naively re-read a cold-group
   vnode's FULL partial on top of the manifest's resident groups — the overlapping resident groups
   would DOUBLE-COUNT. (Cold-VNODE recovery avoids this because the manifest OMITS whole cold vnodes,
   so there is no overlap.)

**Consequence:** cold groups must be recovered as a **cold-ONLY additive artifact** (so the additive
`merge_groups` adds only the cold groups, no resident overlap), OR single-node must gain
**per-vnode-primary recovery** (read FULL partials as authoritative, skip the manifest for tier aggs —
the "single-node delta restart recovery" the original review flagged as not-done). Both are real
checkpoint/recovery infrastructure. The clean option:

- **Cold-only artifact path (recommended).** Do NOT merge cold into the resident partial. The
  coordinator writes the fetched+merged cold groups as a SEPARATE cold-only `AggStateCheckpoint`
  (its own backend object / partial entry keyed distinctly from the resident slice). `AggOpCheckpoint`
  records the cold-group vnodes (like `cold_vnodes`). On restart, after the manifest restore,
  `rehydrate` reads each cold-group vnode's cold-only artifact and `merge_groups`-applies it
  ADDITIVELY (resident from manifest + cold added, no overlap). This is a new durable artifact + a new
  recovery branch — design + soak carefully.

The earlier "byte-merge into the FULL partial" plan below is CAPTURE-correct but RECOVERY-incorrect
(additive double-count) — superseded by the cold-only-artifact path. The pure pieces still apply:
`merge_serialized_agg_cps` (over `append_disjoint`), `cold_groups_by_vnode`, `FetchGroup`, and the
`set_delta_enabled`-no-chain-max enablement.

## Owner decision (2026-06-28): coordinator-fetch (mirror cold-vnode)

The coordinator reads each vnode's cold-group bytes from the tier into the vnode partial at checkpoint,
so restart recovers them. Preserves the RAM bound at runtime; durable; consistent with the existing
arch. (Rejected: promote-before-checkpoint = RAM spike to full at each checkpoint; tier-durable-on-restart
= reverses the deliberate wipe contract.)

### Why BYTE-MERGE (not carry-as-deltas)

`resolve_op_chain` (`recovery_manager.rs:275-294`) applies a partial's `deltas` only from partials
**after** the base partial (`base_idx + 1`), so a *self-contained* `operators=[resident] +
deltas=[cold groups]` partial would silently DROP the same-partial deltas. So cold groups must be
**byte-merged into the resident FULL `AggStateCheckpoint`** (one `operators` entry, all groups);
**recovery stays unchanged** (applies one FULL via `restore_groups`/`apply_vnode_state`).

`AggStateCheckpoint` is COLUMNAR (`aggregate_state/checkpoints.rs:30`): `keys_ipc` = one IPC batch of
all keys; `acc_state_ipc[i]` = one IPC batch per accumulator across all groups; `last_updated_ms` +
`last_emitted` are per-group Vecs. Merge = IPC-decode + `concat_batches` per column + re-encode +
concat the Vecs (same fingerprint). `encode_group` (`aggregate_state.rs:2133`) already produces a
1-group `AggStateCheckpoint`; demotion writes it to the tier via `TierRequest::DemoteGroup`
(`sql_query.rs:1320`, key `op\0vnode\0group`).

## Slices (build order, each tested, default-OFF)

1. **Merge core — MOSTLY EXISTS.** `AggStateCheckpoint::append_disjoint` (`aggregate_state.rs:466`,
   cluster-gated) already row-concatenates two columnar checkpoints over disjoint keys (cold vs
   resident groups of a vnode ARE disjoint) — keys_ipc + each acc_state_ipc via `concat_columnar_ipc`,
   plus `last_updated_ms`/`last_emitted`. So Slice 1 is a thin bytes wrapper
   `merge_serialized_agg_cps(&[Bytes]) -> Bytes`: rkyv-deserialize each (inverse of `serialize_agg_cp`
   = `rkyv::from_bytes::<AggStateCheckpoint>`), `append_disjoint` fold, rkyv-reserialize. The coordinator's
   only entry point — keeps IPC-format knowledge out of the coordinator. Unit test the wrapper.
2. **Tier fetch — REUSE `FetchGroup`.** `TierRequest::FetchGroup { operator, vnode, group, reply }`
   (`worker.rs:53`) already returns a group's bytes via `get_group`. The operator hands the coordinator
   the cold groups' `(vnode, group_key_bytes)` (it has them in `cold_groups`); the coordinator
   `FetchGroup`s each. No new tier request needed.
3. **Capture marker.** `aggregate_state::cold_groups_by_vnode(vnode_count) -> HashMap<u32, Vec<Vec<u8>>>`
   (vnode → cold group_key_bytes). New `StagedSlice::ColdGroups { resident: Option<Bytes>, group_keys:
   Vec<Vec<u8>> }`; `checkpoint_by_vnode` (`sql_query.rs:1260` non-delta branch) emits it for each
   vnode with ≥1 cold group — INCLUDING vnodes with only cold groups (resident=None). Force a FULL
   upload for these (skip the reference shortcut) — simplest correct; WA equals no-demotion for them.
4. **Coordinator fetch+merge.** In `write_vnode_partials`, for a `ColdGroups` slice: `FetchGroup` each
   key, `merge_serialized_agg_cps([resident?, ...cold])`, write the merged bytes as the `operators`
   entry, force full (no reference). A fetch/merge failure FAILS the epoch (mirror `fetch_cold_slice`'s
   "silently dropping a demoted slice breaks recovery" contract, `checkpoint_coordinator.rs:1013`).
5. **Enable wiring.** Single-node `set_delta_enabled(true)` when `state_tier_group_demotion` is on and
   the tier is attached, next to the single-node tier wiring (`pipeline_lifecycle.rs:785-802` +
   `operator_graph.rs:1663`). NO `delta_chain_max`. A new `OperatorGraph::enable_group_delta_tracking()`
   that calls `op.set_delta_enabled(true)` on agg ops (state-tier-gated), parallel to the cluster
   `enable_delta_checkpoints`.
6. **Restart integration test** (`tests/single_node_tier.rs` style): feed skewed load → demote groups
   → checkpoint → reopen → assert `SELECT * FROM mv` equals the full-recompute (all demoted groups
   recovered, no double-count). Then a **single-node no-kill soak** (bounded RAM + dense sink) and a
   **kill-9 soak** as the acceptance gate (the recovery sign-off; mirrors the cluster v2 Slice-6 soak).

## Risks / invariants

- Highest-risk subsystem (checkpoint coordinator + recovery). Recovery path UNCHANGED by design
  (merge produces a normal FULL `AggStateCheckpoint`) — the blast radius is the capture/merge side.
- A cold-group vnode FORCES a full upload each checkpoint (no reference shortcut) → checkpoint WA for
  those vnodes equals no-demotion; the RAM bound is the win. Acceptable for v1.
- `can_demote_group` already requires the group CLEAN (`!is_group_dirty`) so its tier bytes match a
  restorable checkpoint; a touched cold group is promoted back first (fetch-on-access, not cluster-gated).
- Soak is the acceptance gate before any default-ON, per the project rule on unsoaked recovery code.
