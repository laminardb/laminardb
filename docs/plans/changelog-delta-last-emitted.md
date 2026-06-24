# Delta checkpoints for changelog aggregates (Lever 2 v2 — `last_emitted` deltas)

> **STATUS: IMPLEMENTED** (commit `49e98a49` on `feat/shuffle-barrier-after-kill-recovery`).
> Contained to `aggregate_state.rs` — **no wire-format change** was needed: the changed
> entries ride in `AggVnodeDelta.changed.last_emitted`, an `AggStateCheckpoint` field that already
> round-trips through `serialize_agg_cp`→`OpDelta`→recovery (the FULL path already relied on it), so
> the `OpDelta` extension this plan proposed (step 2) was unnecessary. Removals reuse the existing
> group tombstones (eviction co-drops group + emission). Force-FULL gate dropped; FULL re-base now
> carries the dedup map (was zeroed). Unit test `delta_chain_replay_reproduces_changelog_last_emitted`
> green (groups + last_emitted match after FULL+delta replay; recovery re-emits nothing; later change
> emits identically). Default-OFF. **Remaining acceptance gate: the changelog-agg upsert-sink soak
> (below) — not runnable in the current env.**

Follow-up to the landed incremental delta checkpoints (`incremental-delta-checkpoint-lever2.md`,
commit on `feat/delta-checkpoint-lever2`). Today **changelog aggregates re-base FULL every epoch**
under delta capture, because a delta doesn't carry the `last_emitted` dedup map. This plan delta-encodes
`last_emitted` so EMIT CHANGES / aggregating-MV → upsert-sink pipelines get incremental capture too.

## Why changelog aggs force FULL today
`checkpoint_delta_by_vnode` (`crates/laminar-db/src/aggregate_state.rs`) early-returns a FULL capture
when `self.emit_changelog`:
```rust
if self.emit_changelog {
    let full = self.checkpoint_groups_by_vnode(vnode_count)?;
    self.delta_chain_len.clear();
    return Ok(full.into_iter().map(|(v, cp)| (v, VnodeCapture::Full(cp))).collect());
}
```
`encode_delta_for_vnode` sets `last_emitted: Vec::new()` in the changed `AggStateCheckpoint`, and
`apply_delta` doesn't touch `last_emitted`. So a delta would drop the dedup map → on recovery the
changelog diff (`emit_changelog_delta`) would re-emit unchanged groups or miss retractions = a
correctness bug. Hence the conservative force-FULL.

## What `last_emitted` is
`last_emitted: AHashMap<OwnedRow, Vec<ScalarValue>>` (field ~`aggregate_state.rs:543`) — the
last value emitted per group, used by `emit_changelog_delta` (~`:1258`) to diff current vs last so an
unchanged group emits nothing and a changed group emits an update/retraction. Mutation sites:
- `emit_changelog_delta` (~`:1297` read old, `:1318`/`:1323` `last_emitted.insert(key, current)`): a
  group emits ⇒ its `last_emitted` is set/updated.
- `evict_idle` (~`:969` `last_emitted.remove(key)`): idle eviction drops the entry (+ a retraction).
- `merge_groups` (~`:1919` `entry().or_insert`), `restore_groups` (~`:1505` wholesale replace),
  `apply_delta` (~`:1970` removes tombstoned keys).
- Captured FULL by `checkpoint_last_emitted` (~`:1449`) / `checkpoint_groups_by_vnode` (~`:1601`) as
  `Vec<EmittedCheckpoint{ key, values }>` (per-entry IPC; columnarizing is out of scope, as noted there).

## Design
Track and delta-encode `last_emitted` changes exactly like group changes, in a **separate dirty set**
(group-state changes and emission changes are different events — a group can change without emitting,
and emits happen in `emit_changelog_delta`, not `process_batch`).

1. **Dirty tracking.** Add `last_emitted_dirty_by_vnode: AHashMap<u32, AHashSet<OwnedRow>>` (+ a removed
   set, or reuse the existing `removed_by_vnode` since an evicted key drops both group and `last_emitted`
   together — confirm they always co-evict). Populate at the `last_emitted` mutation sites: insert in
   `emit_changelog_delta`, remove in `evict_idle`. Gate on `delta_vnode_count.is_some()` (zero cost when
   delta off), mirroring `dirty_keys_by_vnode`. Reset on capture / `restore_groups` / `merge_groups`.
2. **Delta payload.** Extend `OpDelta` (`vnode_partial.rs`) with the changed emission entries:
   `last_emitted_changed: Vec<u8>` (IPC of the changed `EmittedCheckpoint` list) and reuse
   `tombstones_ipc` for removed keys (group + emission removals coincide on eviction — verify; else add
   a separate emission-tombstone field). Keep it lean.
3. **Encode.** In `encode_delta_for_vnode`, also encode the `last_emitted` entries for the vnode's
   dirty emission keys (build `EmittedCheckpoint`s via the existing `checkpoint_last_emitted` helpers).
4. **Apply.** In `apply_delta`, decode and REPLACE the changed `last_emitted` entries (and remove the
   removed ones) alongside the group replace + tombstones — same REPLACE-per-key discipline.
5. **Drop the gate.** Remove the `if self.emit_changelog { force FULL }` early return in
   `checkpoint_delta_by_vnode` so changelog aggs take the delta path.

## Correctness invariant (load-bearing)
After recovery, `last_emitted` must EXACTLY equal what was emitted pre-crash, so the first post-recovery
`emit_changelog_delta` produces no duplicate and no missed update/retraction. `groups` and `last_emitted`
must stay consistent across base + chain replay (the base + every delta applied in order). This is the
whole reason changelog was deferred — get it right and tested.

## Validation
- **Unit:** a changelog agg (EMIT CHANGES), build a delta chain across several emit cycles (some groups
  change+emit, some go idle+evict, some re-add), then `apply_vnode_chain` into a fresh consumer and assert
  BOTH `groups` and `last_emitted` match the producer (extend the existing
  `delta_chain_replay_reproduces_full_baseline` with a changelog variant). Then feed one more batch through
  both and assert the emitted changelog rows are identical (no re-emit / no missed retraction).
- **Soak:** point a changelog agg at an upsert sink (Delta or Postgres) in `cluster_soak.rs` (a new
  `LAMINAR_SOAK_CHANGELOG_AGG` knob) and assert the sink's final table is cardinality-correct after a
  graceful rotation with delta on (mirror the green `graceful_rotation_kafka_soak` delta run). The
  pass-through EO sink already covers source→sink EO; this covers agg-state value correctness through an
  upsert sink.
- 791 db-cluster lib tests + clippy/fmt clean; delta default-OFF behavior unchanged.

## Notes / gotchas
- Don't conflate the two dirty sets: `dirty_keys_by_vnode` (group state, set in `process_batch`/merge)
  vs the new emission-dirty set (set in `emit_changelog_delta`). A group can be in one without the other.
- The existing `dirty_keys` set (changelog re-eval, distinct from `dirty_keys_by_vnode`) is set on
  `merge_groups` so merged groups re-evaluate — keep that path consistent when applying a chain.
- Eviction co-removes group + `last_emitted` (`evict_idle`), so one tombstone set likely covers both —
  verify before reusing `tombstones_ipc`; if any path removes one without the other, split the field.
- `EmittedCheckpoint` stays per-entry IPC (not columnar) — fine for v2; columnarize only if profiling says so.

## Anchors
- `crates/laminar-db/src/aggregate_state.rs`: `last_emitted` field (~`:543`); `emit_changelog_delta`
  (~`:1258`); `evict_idle` (~`:969`); `checkpoint_last_emitted` (~`:1449`); `encode_delta_for_vnode`,
  `apply_delta`, `checkpoint_delta_by_vnode` (the force-FULL gate).
- `crates/laminar-db/src/vnode_partial.rs`: `OpDelta`.
- `crates/laminar-server/tests/cluster_soak.rs`: soak knobs (add the changelog-agg variant).
