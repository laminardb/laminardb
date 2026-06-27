# Tiered state v2 — group-granularity demotion (prerequisite for IVM join state)

Status: **planned (2026-06-27)**, cold-start ready. Branch `feat/shuffle-barrier-after-kill-recovery`.
Driver: A1-emit Stage 3b (`changelog ⋈ changelog` incremental join) needs **per-join-key** state with
spill-to-disk; the v1 tier is vnode-blob granularity only. Owner decision: **build tiered-state v2
(group granularity) first, then the join on top** (v2 also unblocks skew-proof aggregate demotion).

## Where v1 stands (foundation that already exists)

- **v1 tier** (`crates/laminar-db/src/state_tier/{mod,worker}.rs`, feature `state-tier` ⇒ `cluster`):
  fjall-backed `StateTierStore` keyed `operator \0 vnode_be32 → slice bytes`. `put/get/remove` +
  `TierRequest::{Demote,Fetch,Drop}` worker (off-compute, `spawn_blocking`, bounded channel).
  Capacity-not-durability (no fsync, wiped on restart; truth = object-store checkpoint partials).
- **Per-vnode demotion** (`aggregate_state.rs::demote_vnode`): filters `groups` by
  `key_hash(k) % vnode_count == vnode`, drops them from `groups`+`last_emitted`, marks `cold_vnodes`,
  bumps `state_gen`. `mark_vnode_hot`/`mark_vnode_dirty` track lifecycle.
- **Demotion trigger** (`pipeline_callback.rs::demote_cold_state` ~388): rank idle vnodes
  (`coord.demotion_candidates()`), `slices_for_demotion(vnode)`, filter `graph.can_demote`,
  `tier_demote` (fjall write) → `graph.demote_vnode` (drop RAM) → `coord.mark_slice_demoted`;
  rollback `tier_drop` if demote loses a race.
- **Group-delta checkpoint artifacts — DONE & committed** (`5e6cc265`/`49e98a49`, default-OFF, soak-
  validated under kill-9; see memory `delta-checkpoint-lever2-phase3`). `VnodePartial` has a DELTA
  form (`deltas: Vec<(String, OpDelta)>`, `base_epoch` = chain parent); `aggregate_state.rs` has
  dirty-group tracking, `encode_delta_for_vnode`/`apply_delta` (REPLACE per key), per-vnode
  `checkpoint_delta_by_vnode`, `apply_vnode_chain` (FULL base + forward deltas), per-operator chain
  resolution in `recovery_manager.rs`. This is the group-granular **capture/recovery** half of v2.

**So v2's remaining work is the runtime *spill* half: demote/promote at GROUP granularity, a per-group
tier KV, and a FULL re-base that streams cold groups back from the tier.**

## v1 blocker v2 removes

v1 cannot drop individual cold groups because dropping them changes the serialized vnode bytes, so the
next FULL upload silently omits them (truth loses state). The delta artifacts remove this: a checkpoint
no longer needs whole-vnode byte-identity — it emits per-group deltas, and a FULL re-base reconstructs
the whole vnode (including cold groups) by **streaming them from the tier**.

## Design — group-granular spill

Model a "group" as the unit keyed by the operator's group key (the GROUP BY row; for the join, the
join key). Each group's state is independently serializable (it already is — `encode_delta_for_vnode`
serializes changed groups). Add a per-group resident/cold bit; cold groups live only in the tier.

Invariants:
- **A group may be demoted only when clean** (captured in a restorable checkpoint), exactly as v1
  requires for vnodes. Dirty groups (changed since last capture) stay resident.
- **Single writer**: the graph cycle drives all resident↔cold transitions; the coordinator only reads
  staged markers (same rule as v1, plan §4 of `tiered-operator-state.md`).
- **Promotion is fetch-on-access**: a row addressing a cold group triggers an async tier `Fetch`
  (Ring-1), the group rehydrates into the map, the row replays (mirror v1's deferred-rows pattern).
- **FULL re-base streams cold groups**: at `chain_max`, the FULL capture must include cold groups; it
  scans them from the tier (prefix scan by `(operator, vnode)`) rather than the in-memory map.

## Slices (build order — each validated + committed; default-OFF until soak)

### Slice 1 — per-group tier KV API (additive, isolated) — START HERE
`StateTierStore` gains group-granular keys `operator \0 vnode_be32 \0 group_key_bytes`:
`put_group/get_group/remove_group(operator, vnode, group_key, bytes)` and
`scan_groups(operator, vnode) -> impl Iterator<(group_key, Bytes)>` (fjall prefix scan). Worker gets
`TierRequest::{DemoteGroup,FetchGroup,DropGroup}` (+ a `ScanGroups` for re-base, or expose the store
directly to the Ring-1 capture path). Logical-bytes/slices accounting per group. Unit tests in
`state_tier/tests.rs` (round-trip, prefix-scan isolation between vnodes/operators, overwrite, remove).
Carries `#[allow(dead_code)]` until Slice 2 wires it (the delta work used the same pattern).

### Slice 2 — group-granular demotion in `AggregateState`
`demote_groups(cold_keys: &[OwnedRow], vnode_count)` (or `demote_cold_groups(target_bytes)` returning
the demoted keys): serialize each clean cold group, `put_group` to the tier, drop from `groups`/
`last_emitted`, record in a `cold_groups: HashMap<OwnedRow, vnode>` (or a per-vnode `cold_keys` set).
Rank cold candidates by `last_updated_ms` (idle-first). Keep `demote_vnode` for now (group demotion is
the new path behind a flag). Per-group byte accounting feeds `estimated_state_bytes`.

### Slice 3 — group-granular promotion (fetch-on-access)
On `process`, partition input rows by group key; for keys in `cold_groups`, issue async `FetchGroup`,
buffer the rows until the reply rehydrates the group (decode → insert into `groups`, clear cold bit,
mark dirty), then replay. Mirror the v1 per-vnode deferred-rows/promotion operator structure
(`sql_query.rs` promotion path). Backpressure when too many in-flight fetches.

### Slice 4 — checkpoint/compaction with demoted groups
Delta capture already emits O(dirty); cold groups are clean ⇒ absent from the delta (correct). The
FULL re-base at `chain_max` must include cold groups: extend the FULL capture to **stream cold groups
from the tier** (`scan_groups`) and merge them with resident groups before serializing. Recovery
(`apply_vnode_chain`) is unchanged (FULL base already carries all groups). Add a unit test:
demote some groups → FULL re-base → chain replay == full baseline (no lost groups).

### Slice 5 — budget trigger at group granularity
Move the demotion trigger from idle-vnode to idle-group ranking (`pipeline_callback`): rank cold-
eligible groups by idle time, demote until under budget. Per-group accounting; keep
`STATE_DEMOTE_MAX_PER_PASS`-style bounding. Metrics: per-group demote/promote counters, fetch p99.

### Slice 6 — soaks (the acceptance gate)
- **Bounded-RAM**: high-cardinality GROUP BY with state ≫ budget; assert stable RSS, no OOM, correct
  final answers, demotion/promotion counters moving.
- **Kill-9 EO** with demoted groups resident in the tier across a kill (tier wiped on restart ⇒
  rehydrate from partials, including cold groups via the FULL re-base path).
- Reuse `cluster_soak.rs` knobs; needs the OpenSSL+ORT env (memory `windows-test-openssl-env`).

## Then: Phase 3b — the IVM join on top

A `JoinStateStore` trait (`get(join_key) -> Z-set`, `upsert(join_key, row, weight)`, `scan`,
`estimated_bytes`) with an in-memory impl AND a tier-backed impl over the Slice-1 per-group KV (a join
key == a "group"). The `IncrementalJoinOperator` (inner + left + multi-way, hand-rolled `δA⋈B_new +
A_old⋈δB`) uses the trait; checkpoint via the group-delta path. See the design captured in memory
`a1-emit-stage1` (Phase 3b notes) — separate plan doc once v2 lands.

## Risks / notes
- Highest-risk subsystem (EO checkpoint, cluster, kill-9). Default-OFF until Slice-6 soaks are green.
- Per-group overhead: tiny groups ⇒ many fjall keys; KV-separation tuned for KB..MB blobs may need a
  per-group size floor (batch small cold groups into a sub-blob) — measure in Slice 6.
- `state-tier` ⇒ `cluster` feature; Windows builds need the OpenSSL env; cluster `--tests` clippy can
  flake on rdkafka-sys cmake — gate on `--features state-tier` lib + non-cluster `--tests`.
