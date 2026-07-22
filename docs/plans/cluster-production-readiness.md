# Cluster production readiness — plan index

- **Status:** The former monolithic plan is superseded; this path is retained as an index.
- **Last audited:** 2026-07-22

Cluster production work has independent correctness boundaries. Keeping checkpoint delivery,
keyed working state, source placement, connector commits, and materialized reads in one roadmap
created duplicate decisions and made an aspirational component look implemented. Use the focused
documents below as the authorities.

## Current boundary

- Cluster delivery is `at_least_once`; `exactly_once` remains fail-closed with `[LDB-0013]`.
- `CREATE STREAM` admits stateless projection/filter pipelines and a single direct ungrouped
  aggregate stage on the exact incremental path.
- Grouped/windowed aggregates and stateful joins fail closed with `[LDB-4007]`.
- All cluster materialized views fail closed with `[LDB-4007]` because retained output and reads do
  not yet have a distributed assignment-fenced lifecycle.
- Fixed vnodes, shuffle, aligned checkpoints, checkpoint artifacts/seals, and assignment fencing
  exist. They are necessary substrate, not proof for every operator.

## Authoritative plans

| Area | Authority |
|---|---|
| Current admission and missing keyed capability | [2026 validation report](../reports/cluster-keyed-state-validation-2026-07-22.md) |
| Managed keyed state and operator design | [ADR-008](../architecture-decisions/ADR-008-managed-vnode-keyed-state.md) |
| Aggregate/window/join implementation sequence | [Distributed keyed/stateful plan](distributed-keyed-stateful-operators.md) |
| Checkpoint, CDC, connector delivery, and external commit certification | [Checkpoint and CDC production plan](checkpoint-production-correctness-2026.md) |
| Runtime fault containment and recovery | [Pipeline failure recovery architecture](pipeline-failure-recovery-architecture.md) |
| Live connector topology changes | [Hot-add connector plan](hot-add-connector-while-running.md) |

The removed narrative included a missing lookup-plan link, duplicated keyed-state design, a
“settled Postgres authority” not present in the current runtime, and claims stronger than the cited
Arroyo architecture supports. Historical prose is available in Git; it is not current design
authority. A future cross-cutting cluster decision should receive its own ADR rather than expanding
this index back into a second umbrella specification.
