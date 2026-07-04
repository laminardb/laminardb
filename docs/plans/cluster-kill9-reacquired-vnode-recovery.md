# Cluster kill-9 vnode recovery — root cause + fix (revision 5)

**Status:** implemented (2026-07-04), soak-gated. Fixes the cluster kill-9 aggregate under-count the
external per-group state-tier soak surfaced (harness doc §E5–E9). Branch
`fix/cluster-source-offset-handoff-recovery`.

## Soak trend (mismatches; 0 = pass)

| Scenario | #2 `793c9caa` | #3 `0fb07a90` | #4 `e78ef9e5` |
|---|---|---|---|
| follower kill (node-1) | **0 ✅** | 106 | 864 |
| eo_kill (node-0 seed/leader) | 661 | 2000 | 1489 |
| eo_kill_delta | 696 | 1998 | 840 |
| eo_kill_notier | 1736 | 878 | 865 |
| delta_kill | 989 | 598 | 534 |

#3 and #4 changed the adopt/re-acquire cut and regressed the follower case #2 had exact; both are
**reverted** (`74f92d5f`, `6ab5e04e`). #2 is the baseline; revision 5 is additive on top of it and
does not touch the adopt path.

## Root cause (code-verified)

Cluster boot recovery restores aggregate state from the **global first-writer-wins manifest**
(`ObjectStoreCheckpointStore` with empty prefix, `PutMode::Create` — one winner per epoch), which
holds only the **writing node's** vnode slices. Per-vnode partials, by contrast, are globally
complete: the durability gate seals an epoch only when every vnode has one.

The asymmetry that made this leader-kill-specific:

- **Follower killed:** the surviving leader sheds its vnodes before it rejoins. It boots owning
  nothing; its vnodes come back via `adopt_assignment_snapshot`, which rehydrates from per-vnode
  chains + resumes offsets from the handoff union at the seal. That path is exact (soak-proven).
- **Seed/leader killed:** no surviving controller sheds its vnodes before it restarts (election +
  rebalance debounce outlast a container restart), so it **boots still owning them**. With delta OFF
  its aggregate state comes only from the recovered manifest — empty for its vnodes whenever another
  node won that epoch's manifest race — while fix #2 stages complete source offsets. Result: a clean,
  share-sized (~1/3 of groups) under-count that varies run-to-run with the manifest race.
- **Delta ON** avoided the manifest dependency (`stage_owned_vnodes_for_delta_primary`) but read
  chains at `latest_committed_epoch` while offsets restored at `recovered.epoch()` — a divergent cut
  when a leader dies between seal and decision.

`sources_restored=0` is a red herring (coordinator recovers with an empty source slice).

## Fix (boot path only; adopt path untouched)

Per-vnode partials become the authoritative aggregate checkpoint in cluster mode, delta on or off:

1. **Write side:** `SqlQueryOperator::set_vnode_partials_authoritative` — whole-node agg capture into
   the manifest is skipped when cluster-sharded + durable backend (wired in `pipeline_lifecycle`).
   The manifest keeps non-agg operator state; embedded/single-node unchanged.
2. **Boot recovery:** `stage_owned_vnodes_from_chains(recovered.epoch())` runs for every cluster
   recovery with a durable backend (previously delta-ON only), staging each boot-owned vnode's chain
   at the **recovered manifest's epoch** — the same committed cut the source offsets restore at.
   `VnodeRehydrator::rehydrate_at(vnodes, epoch)` pins the read (a decision-committed manifest epoch
   is always sealed, so chains exist at that cut).
3. No overlap/double-apply: manifests written by this build carry no agg state in cluster, so the
   additive `merge_groups` chain apply lands on empty slices.

## Deliberately not done

- Adopt/re-acquire path: untouched (#3's lesson — it is exact for the follower case).
- Ingest barrier for still-Restoring vnodes: deferred; emission gating suffices for the current soak.
- Windowed (non-changelog) aggregates still ride the manifest and keep the per-node-incomplete
  exposure on a boot-owning restart — known residual, separate work.

## Acceptance (owner-run soak)

Cluster kill-9 per-group: 0 mismatches at **both** victims (node-0 seed/leader and node-1 follower),
delta ON and OFF, EO and ALO; follower case must stay 0 (guard against #3-style regression);
steady/no-kill gates stay green.
