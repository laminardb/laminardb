# Cluster kill-9 vnode recovery — root cause + fix (revisions 5–13)

## Revision 13 — the round must survive a killed leader and stale buffers

Rev-12 soak (§E17): the rejoin fault fires (`reported local fault`, `leader announced recovery`)
but seed kills STALL the round (`coord_complete=0` — "no committed epoch yet") and the follower's
COMPLETED rounds double the quantum (±3 → ±6, 825 → 1097). Three seams fixed (`6d1ffccc`):

1. **Skipped rounds consumed the fault** — `take_new_fault` marked the sequence handled before
   `drive_round` bailed on a missing target → no retry, ever. Pending faults are now recorded
   only when a round actually runs; a deferred round retries each 200ms poll.
2. **`compute_target_epoch` had no durable fallback** — DB-level decision store only; on the
   freshly-restarted reclaimed-leader seed it yields nothing while the seal exists. Falls back to
   `state_backend.latest_committed_epoch()`.
3. **The rewind kept the long-lived `ShuffleReceiver`'s buffers** — pre-rewind slices stashed
   there are ALSO replayed by their rewound senders; folding the buffered copy after the rewind
   double-counts (the ±3→±6 compounding). `restore_pipeline` now purges queued slices, staged
   holdovers, and stashed barriers between stop and start.

Known remaining seam (if a sub-quantum residual survives): the round has no stop-barrier — a
not-yet-rewound node keeps shuffling for the announce-to-observe window (~200–400ms) into an
already-rewound peer before its own rewind replays those records. Fix shape if needed: a
two-phase round (stop+ack quorum, then start), or gate source intake on a gossiped
`is_recovering` flag.

Soak expectations: seed kills now log "coordinated recovery complete" (target from the durable
seal); the ±6 collapses; the ±3 collapses with it (the rewind now replays the window exactly
once cluster-wide).

## Revision 12 — the in-flight shuffle window; rejoin triggers the coordinated rewind

Rev-11 soak (§E16): 0/5, and the notier per-group delta histogram decoded the residual —
quantized at ±3 (−3×326 over, +3/6/9×546 under), affecting ~872 groups > the 806-group vnode
share. 3 = the killed node's 3 re-acquired partitions × 1 record each per 12-record round-robin
burst: **the miscounted unit is the killed node's partition slice of every group**, i.e. the
in-flight cross-node shuffle window between the last seal and the kill:

- **+3 over**: the restarted node replays its partitions from the sealed cut and re-shuffles the
  window; survivors' surviving in-memory state already folded the pre-kill sends → double-fold.
- **−3/6/9 under**: records survivors consumed in the window and shuffled TO the dead node died
  with its memory; their offsets moved on, nobody re-sends → per-boundary loss for its groups.

This is the audit's "shuffle no-delivery-contract" P0 made concrete. Exactness requires the
cluster-wide rewind to the sealed cut that coordinated recovery already implements; the missing
piece was the **trigger** — a kill-9'd process cannot report a fault at death and its restart
resets the fault sequence. Fix (`5bb05bd5`): `LaminarDB::report_rejoin_fault` (no-op unless
`coordinated_recovery` on) called by `start_cluster` when a boot finds an existing assignment
snapshot → the leader announces → every node `recover_to_epoch(seal)` → the window replays
consistently on all nodes (the rev-5 boot staging is load-bearing inside the round's restore).

**HARNESS REQUIREMENT: emit `[supervision] coordinated_recovery = true` in the cluster server
TOML** (docker_compose.rs emits no `[supervision]` today) — without it the trigger no-ops.
Soak expectations: seed logs "reported local fault for coordinated cluster recovery" on rejoin;
leader logs "leader announced recovery" + "coordinated recovery complete"; all nodes rewind; the
±3 histogram collapses. Alternative long-term fix if rewind latency is unacceptable: offset-tagged
shuffle slices + receiver dedup (new protocol; parked).

## Revision 11 — the reader-spawn race: a missed rebind freezes the share

Rev-10 diagnostics (§E15) cross-cut: on the clean witness (`eo_kill_notier`, 4 MB, no demotion) the
Uninit fold restored the COMPLETE share (`groups=806, reemit=13`, every chain applied per the
Agg/drain logs) at both kills — yet the run lost ~the whole share, with per-group finals frozen at
the recovery cut. Complete state + complete emit + total loss ⇒ **post-recovery input never
arrived**.

Mechanism (`9495cf6c`): the Kafka reader task initialized `last_assignment_version` to the registry
version at its own spawn ("open() already assigned at the current version"). Rev-7 broke that
assumption: a restart boots unassigned@v0, `open()` assigns nothing, and the startup adopt (right
after `db.start()`) bumps the version. If the bump lands before the spawned reader runs its init,
the rotation check never fires and the node **never assigns its re-acquired partitions** — every
group frozen at the restored baseline. Seed-biased: only the seed's startup adopt is the acquiring
bump (its shed races its rejoin); a follower's acquiring bump arrives via the watcher seconds after
the reader initialized. Fix: initialize `last_assignment_version`/`last_drain_gen` to 0 — a
spurious first pass reconciles current-vs-owned and no-ops; no race window remains.

Confirmation grep (works on the OLD failing logs): the seed after each kill previously shows NO
`acquired partition resume offset` / `rebound partitions after vnode rotation` lines; with the fix
they must appear at the staged handoff cut. Residual candidate if 16 KB runs still lag the notier
result: cold/demoted groups absent from dirty-vnode captures (fold=72/112 vs share 806 — hot
residue), healed lazily by promotion on the Agg path but not by the fold — fix would be capture-side
(force-fetch cold bytes into FULL partials, as re-base already does).

## Revision 10 — rev-9 reverted; instrumentation for a decisive next run

Rev-9 soak (§E14): follower held 0 ✅; seed REGRESSED (901/859/677/866, over 2→8) — the
deferred-revoke cancel reverted (`a802374c`); **368060c6 (rev 8) remains the code high-water mark.**
The regression falsifies the flap model: under it the cancel was strictly state-preserving, so the
soak's dominant path differs from the unit-reproducible deferred-revoke inversion (that defect stays
real — its correct fix likely needs a per-vnode purge of the earlier fold, not a blanket cancel —
but it is not the soak's mechanism). Rather than a sixth blind fix, `8b275c3b` adds cold-path
instrumentation at the audit's discriminating points.

**Decision tree for the next seed-kill run (grep the seed's logs):**
1. `applied rehydrated vnode chain … groups=G` per vnode at each re-acquire —
   G≈0 → the chain content is short (write side: partials/reference/delta capture);
   G≈full (~50/vnode) but finals half → post-apply loss (fold drop or emission).
2. `rehydration_epoch` at restart-1 vs restart-2 — equal → sealing wedged between the kills →
   state-behind-offsets at restart-2 is the mechanism.
3. `source-offset handoff staged for acquire epoch=E` vs the same adopt's `rehydration_epoch` —
   any skew → the two seal reads diverged inside one adopt.
4. WARNs: `lazy_init fold: deferred revoke intersects re-acquired vnodes` (the flap IS live);
   `rehydration chain has no FULL base`; `no live operator for rehydrated slice`;
   `acquired partition has no handoff or local offset`.
5. `acquired partition resume offset` per partition — compare the replay start against the
   rehydrated epoch's expected cut; `from_handoff=false` = the local-snapshot fallback fired.

**Status:** implemented (2026-07-05), soak-gated. Fixes the cluster kill-9 aggregate under-count the
external per-group state-tier soak surfaced (harness doc §E5–E9). Branch
`fix/cluster-source-offset-handoff-recovery`.

## Revision 9 — a re-acquired vnode's chain cancels its deferred revoke

Rev-8 soak (§E13): follower 0 ✅ robust; seed still 558–726 pure loss ≈ its whole 13-vnode share,
per-group finals at exactly one burst (36/72), `rehydrated=13 rehydration_epoch=Some(9)` logged at
staging. Root cause (deterministic, found via a 5-way parallel code audit):

A restarted node's agg operator stays **Uninit until its first input**, which only arrives after a
rotation assigns partitions. The killed seed's rejoin races the shed publication (survivors need
lease takeover + phi + debounce vs a ~5s restart), so three rotations queue before the first cycle:
startup adopt of the stale owning snapshot, the late shed (revoke), and the give-back (chains).
Cycle 1 then runs the STALE revoke against the FRESH chains: `apply_revoked` before
`apply_rehydrated` (right for an initialized op), the Uninit op defers the revoke into
`deferred_revoke_vnodes`, the give-back chains fold into `pending_restore` — and **nothing removed
a re-acquired vnode from the deferred set**. `lazy_init` restores the sealed baseline, the
force-emit skips every deferred vnode, and the deferred drop destroys the just-restored state of
all 13 vnodes. Offsets resume at the sealed cut → the sealed burst is never replayed; the next
burst rebuilds from zero; later checkpoints seal the loss. The follower never hits it (its shed
completes while it is down — no acquire→revoke→re-acquire prefix); steady runs never revoke.

Fix (`852ea8e9`): `apply_vnode_chain` removes the vnode from `deferred_revoke_vnodes` (a chain
apply IS a re-acquire; logged "re-acquire supersedes deferred revoke"). Idempotent under the flap's
duplicate fold (`restore_groups`/`decode_last_emitted` are keyed last-wins; `apply_delta` is per-key
REPLACE). Guard test `uninit_reacquire_after_deferred_revoke_keeps_restored_state` reproduces the
first-cycle ordering and the 36-vs-72 signature (fails when the fix is neutered).

Audit residuals noted for later hardening (all verified real, none the soak's mechanism):
suppression-after-dedup-commit in the emit tail (self-heals via force-reemit today); the drain
consuming chains on apply-error/no-operator while still marking Active (loud-log candidate);
`is_recently_unresponsive` 60s TTL widening the shed race; empty FULL partials written for owned
vnodes while Uninit; ALO recovery accepting an unsealed manifest.

## Revision 8 — acquired partitions resume from the handoff, not the local snapshot

Rev-7 soak (§E12): follower 69 (over=10/under=6), seed 677–1117 mostly loss. Root cause found in the
vnode re-assign path (`kafka/source.rs`): an acquired partition resumed from the **local offset
snapshot first**, staged handoff second. The local snapshot is always stale for an acquired
partition — `restore()`'s recovered-manifest cut on a restart, or a pre-revoke position from an
earlier ownership stint whose operator state was dropped at revocation. Behind the seal it
double-folds replayed records (the follower's over residual); ahead of the seal it skips records in
neither the rehydrated state nor the replay (the seed's pure loss: its delayed shed races its
rejoin, so the revoke→re-acquire flap leaves the local snapshot ahead of the sealed cut). This also
explains the saga: fix #2 "fixed" the follower by making the local fallback coincidentally correct,
and every revision that shifted the restore cut nicked it again.

Fix (`c0f2bf8d`): `acquired_resume_offset` — handoff first, local as first-rotation fallback, WARN
on the silent startup-default fallback; prune revoked partitions from the local snapshot on
unassign (`retain_assigned`) so a stale stint position can't shadow a later handoff; skip the
manifest restore of keyed changelog MVs on cluster nodes (one writer's slice → ghost keys the
unfiltered distributed union double-counts; adopt's force-emit rebuilds them — Append MVs keep the
restore).

## Revision 7 — restarts boot unassigned; ownership only via adopt

Rev-6 soak (§E11): the follower guard failed (0 → 706 mixed) — that run's follower restarts happened
to **boot owning** (staged=8 at epochs 8 and 13), a path the rev-5 follower run never took (it booted
∅ both kills — the shed race decides). Every path ever measured exact flows through
`adopt_assignment_snapshot`; every failure (rev-5 stale under-count, rev-6 exposed mixed) lives on
the boot-owning path, where a node trusts its possibly-stale persisted snapshot and acts on assumed
ownership — consuming, folding, and emitting outside the adopt protocol (and split-braining with a
survivor that adopted a shed).

Fix: delete the boot-owning path. `resolve_vnode_assignment` boots a restart/joiner **unassigned at
version 0** (`VnodeRegistry::new_unassigned`); after `db.start()`, `start_cluster` re-loads the
stored snapshot and explicitly adopts it — offsets + chains at the sealed cut, Restoring gate, CL-4
force re-emit — with a bounded retry (static discovery has no snapshot watcher to re-drive a
deferred adoption). A full-cluster restart also re-acquires everything through adopt. First-boot
(CAS-create) still pre-owns: no state exists to recover. The rev-5 boot staging becomes dormant in
production (kept: in-repo harness configs still pre-own registries); rev-6's Uninit force-emit stays
load-bearing — the startup adopt can land before the first cycle.

## Revision 6 — force-emit chain-restored groups after the Uninit fold

Rev-5 soak (§E10): follower 0 ✅, EO over-counts gone, seed still under (839–1107). The staging log
proved chains were staged and applied — the loss was **emission**, not state: a chain applied while
the operator is still `Uninit` (boot staging always is — chains drain at the cycle top, before the
first `process`/`lazy_init`) folds via `restore_groups`, which restores `last_emitted` and clears
dirty. That contract ("groups == last_emitted, nothing pending") is right for the embedded manifest
restore but wrong for the cluster chain restore: this process's MV snapshot came from the
per-node-incomplete manifest, so the restored groups' MV rows are stale/absent, and the restored
`last_emitted` suppresses their first emit until NEW input — groups whose bursts already passed stay
stale forever (pure loss, no dups — the observed signature). The initialized (`Agg`-arm) applies
already call `force_reemit_acquired_vnode` — the reason the adopt path is exact; the delta-off chain
path even reached that call, but `if let QueryState::Agg` silently no-ops while Uninit.

Fix: vnodes whose chain lands while Uninit are recorded (`pending_restore_reemit`); after
`lazy_init` folds base+deltas, each is `force_reemit_acquired_vnode`d — skipping vnodes with a
deferred revoke, whose `last_emitted` the drop needs for retractions. Covers boot staging (delta on
and off) and an adopt that lands before first init. Guarded by
`uninit_chain_restore_reemits_groups_after_init` (fails without the fix).

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
