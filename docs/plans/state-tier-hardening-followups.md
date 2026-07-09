# State-Tier Followups

Cluster group demotion is default-ON (`laminar-server/src/cluster.rs`), matching embedded.
`[server] state_tier_group_demotion = false` falls back to vnode-granular. Referenced by ADR-005.

The correctness work that gated the flip has landed and is soaked: per-vnode captures are
self-contained (cold groups are force-fetched into the durable partial, since the tier is
node-local and wiped on restart), resident and cold group sets are healed disjoint before every
capture, and the cluster group path holds exact per-group values across repeated kill-9s under
coordinated recovery. Demotion and node kills now co-occur; the earlier "A2" limitation is gone.

## Open

- **D2 — Linux-NVMe p99 gate.** The formal cold-read p99 ≤ 1 ms gate (ADR-005) is still owed.
  Numbers recorded so far come from a consumer-QLC Windows box: the body passes at low write
  rates, the tail exceeds 1 ms under ingest pressure. Perf, not correctness.
