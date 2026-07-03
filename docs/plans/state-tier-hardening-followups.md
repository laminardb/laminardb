# State-Tier Hardening Followups (cluster group-demotion path)

Open items gating the flip of **cluster** group demotion from default-OFF
(`laminar-server/src/cluster.rs`) to default-ON. Referenced by ADR-005. Embedded single-node
group demotion is default-ON and kill-9-soaked; cluster stays vnode-granular until these close.

## The reported "2× double-count" was a soak-harness artifact

The Kafka state-tier soak reported a 3-node cluster emitting exactly 2× the correct COUNT for
every group. Root cause: the harness recreated the Kafka topics each run but reused the durable
checkpoint backend, so a fresh run recovered the previous run's committed state and reprocessed
the recreated input → every group counted twice (uniform 2× = whole input processed twice, not a
per-group demote/promote overlap). With an isolated checkpoint backend the same scenario passes
with exact per-group values and demotion/promotion heavily exercised. Fixed harness-side by
run-id-scoping the checkpoint backend. The engine already refuses to start (`LDB-6029`/`LDB-6030`)
when a recovered checkpoint is inconsistent with local state.

## Items

- **B1 — resident∩cold disjoint guard (DONE).** `IncrementalAggState::heal_resident_cold_overlap`
  runs before every per-vnode capture (both the `FullWithColdGroups` and `ColdGroups` paths), drops
  any group found in both, and reports the count via `laminardb_state_tier_overlap_total`. Replaces
  the release-compiled-out `debug_assert`.
- **B2 — fail-loud on misconfig (DONE).** `validate_state_tier` rejects `state_tier_group_demotion`
  without `state_tier_dir`. (Group demotion does not need `delta_chain_max`: it auto-enables agg
  delta tracking and `checkpoint_groups_by_vnode` sets `delta_vnode_count`.)
- **A2 — demotion suppressed under failover (accepted).** Rebalance rehydration marks vnodes dirty
  and refuses demotion until the next clean capture, so demotion and node-kills can't co-occur on
  cluster (the reference soak uses `kills=0`). Correctness-preserving; kept as a limitation.
- **D2 — Linux-NVMe p99 gate (open).** The formal cold-read p99 ≤ 1 ms gate (ADR-005) is still owed.

## Flip criteria (separate, reviewed step)

1. Cluster soak green on a clean/isolated backend: `demote_total>0`, `fetch_total>0`,
   `mismatches=0`, `missing_groups=0`, `source_lag=0`, `state_tier_overlap_total=0`. **Met.**
2. In-repo suite green (`cargo test -p laminar-db group_demotion`, cluster `group_demotion` tests)
   and embedded regressions. **Met.**
3. Kill-9 cluster soak: demoted groups survive a mid-soak node kill/restart with exact values
   (bounded by A2 — interleave quiet demotion windows with kill windows).
4. D2 perf gate (perf, not correctness).
