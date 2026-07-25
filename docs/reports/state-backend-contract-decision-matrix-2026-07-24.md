# State-backend contract decision matrix — Cycle 18

- **Date:** 2026-07-24
- **Decision outcome:** `APPROVE_MAINTENANCE_HEALTH_V2_DIRECTION` recorded on 2026-07-24
- **Recommendation:** approve the v2 design direction
- **Production backend selected at Cycle 18:** none; Cycle 40 later selects official `tidesdb-rs`
  as the TidesDB integration line, not a qualified backend
- **Candidate execution authorized:** no
- **Cluster admission:** unchanged; `[LDB-4007]` and `[LDB-0013]` remain fail-closed
- **Proposal:** [maintenance-health contract v2](../architecture-decisions/state-backend-maintenance-health-v2-proposal.md)

**Superseded approval mechanism:** Cycle 38 retains this technical direction but removes the later
two-owner/protected-workflow ceremony for validation-only implementation. The consolidated contract
and current ADR are authoritative; candidate execution and production gates remain closed.

**Current authority:** the dated RocksDB recommendation below remains v4/source-gap provenance only.
The [Cycle 40 package design](../architecture-decisions/tidesdb-local-state-successor-design.md)
selects exact official `tidesdb-rs v0.11.1`/native 9.3.6 only as a restricted-facade prescreen
subject and creates no runtime or execution authority.

## Historical Cycle 18 recommendation

The additive v2 design direction is approved. Draft and review the consolidated contract using the
already-complete three candidate mapping designs.
At the time of this Cycle 18 recommendation, a later separate approval was expected to freeze the
consolidated contract, exact schemas, and numerical thresholds. Cycle 38 replaced that proposed
ceremony with a project-owner direction and ordinary technical review for validation-only work.
This keeps the common objective gates and exact foreground stall requirement
while removing one engine-neutral exact debt quantity that the Cycle 17 source audit found neither
narrowly implementable nor justified by the reviewed production monitoring contracts.

This recommendation does not instantiate `state-backend-runner-contract/v2`, approve schema or
validator implementation, select RocksDB, authorize native source construction, or authorize a
candidate run. The named owners can instead retain v1 if the exact byte population is an explicit
organizational requirement and they are willing to fund and maintain deeper engine instrumentation.
The candidate premises come from the [Cycle 17 RocksDB closure](rocksdb-mechanism-source-closure-2026-07-24.md),
the [static backend audit](state-backend-static-audit-2026-07-23.md), and the
[redb 4.1.0 mechanism note](redb-4.1.0-prescreen-mechanism-note-2026-07-23.md).

## Contract choices

| Decision factor | A — retain v1 exact debt | B — additive typed-health direction (**recommended**) |
|---|---|---|
| Common correctness/latency/resource gates | Retained | Existing values and semantics retained under new identity bindings |
| Foreground pressure stalls | Exact interval arm retained | Exact interval arm retained |
| Maintenance evidence | One complete direct pairwise-disjoint outstanding-work byte population | Candidate-native typed signals, evaluated independently under precommitted predicates |
| Cross-engine comparison | Common gates plus a nominally common debt maximum | Common gates only; native health is veto-only and never ranked |
| RocksDB work before mapping | Broad transition-consistent instrumentation/configuration proof plus stall observer | Complete the mapping design first; source review hypothesizes a bounded stall observer, while other source/binding work is unknown |
| Fjall work before mapping | Broad exact debt/stall/control patch | Paper-map required pressure/progress/error/control sources first; patch scope is unknown |
| redb treatment | May prove maintenance N/A; global writer remains outside the debt arm | May prove health N/A; global writer remains in common latency/C3/persistence gates |
| Hot-path risk | Potentially high if exact lifecycle bookkeeping touches scheduler/purge transitions; unmeasured | Expected to avoid exact lifecycle bookkeeping, but unmeasured until mapping designs and telemetry-on/off A/B evidence exist |
| Operational fit | Exact scalar is easy to compare but hard to relate to heterogeneous mechanisms | Matches native operational semantics but needs candidate-specific review and alerts |
| Evidence auditability | Uniform formula; source completeness is difficult | Heterogeneous values; explicit semantics/quality/cadence/overhead and closed predicates |
| Primary failure mode | Build expensive telemetry that changes the engine before selecting it | Grow a generic metrics DSL or tune candidate thresholds after seeing results |
| Guardrail | Fund exact source proof and measure instrumentation interference | Closed vocabulary, precommitted thresholds, no score/sum, common gates authoritative |

Option B is the preferred design hypothesis from the source and operational evidence, not a measured
cost or risk result. Its weakness—different native signals—is controlled by using them only as
fail-closed operational vetoes. Candidate comparison remains on identical workloads, open-loop
latency, throughput, common resources, pressure behavior, recovery, faults, and endurance. Paper
mappings and telemetry-on/off A/B evidence must test the expected implementation and hot-path
advantage before owners fund a candidate closure.

## Rejected shortcuts

| Shortcut | Reason rejected |
|---|---|
| Treat RocksDB's level estimate as v1 exact debt | Contradicts the pinned source and would reinterpret v1 evidence |
| Remove all maintenance observation | Finite latency can pass while backlog, reclamation, or background errors accumulate |
| Let redb pass because LSM fields are N/A | Absence of an LSM worker says nothing about global-writer contention, commit/fsync tails, file growth, repair, or crash durability |
| Compare a weighted native-health score | Unlike units and semantics can conceal an absolute failure and reward metric availability |
| Add every exposed engine property | Increases overhead and review surface without proving a production objective |
| Select the last candidate not blocked by the contract | Contract shape is not performance, correctness, durability, or operability evidence |

## Historical Cycle 18 backend carry consequence

| Candidate | If v1 is retained | If v2 is approved | Disposition before common evidence |
|---|---|---|---|
| RocksDB 10.4.2 / wrapper 0.24.0 | Fund broad maintenance lifecycle instrumentation/configuration proof and the complete stall observer | Complete its mapping design; after final contract and separate source-closure approval, prove the WBM/controller observer and bindings, then freeze mapping v2 | **Primary mature LSM track; blocked, not selected** |
| Fjall 3.1.8 | Fund exact debt/stall/control instrumentation before an adapter | Paper-map its pressure/progress/error/control obligation, then decide whether any patch is bounded; compare only if it closes | **Alternative Rust-native track; blocked** |
| redb 4.1.0 | Complete the native prescreen; a proved N/A arm does not admit it | Same prescreen first, then complete N/A/source and synchronous-contention mappings | **Prescreen hedge; deferred** |
| SurrealKV 0.21.2 | Correct snapshot retention and liveness before telemetry | Same correctness/liveness requirement | **Rejected unmodified** |

At Cycle 18, RocksDB was the recommended next paper-mapping investment because its operational
surface is mature and the reviewed stall gap appears plausibly bounded. That is a source-audit
hypothesis and work-allocation judgment, not a measured patch estimate or backend choice. redb
remains useful as an architectural hedge against native/C++ complexity, but only its independent
prescreen can determine whether the global writer is compatible with the low-latency multi-vnode
workload. Fjall can re-enter only if its mapping design demonstrates a bounded, maintainable
telemetry/control closure; no relative patch size is claimed yet.

## Absolute gates after either choice

No contract choice waives:

- atomic C1 read/write/range/snapshot/export/restore semantics;
- hot-writer/victim C3 p99/p99.9/max latency and bounded queue/admission behavior;
- whole-process memory, disk, device I/O, write/space amplification, FD, snapshot/version, and
  resource-tail ceilings;
- exact persistence calls, cache-loss truth table, kill/crash/power-loss/corruption/ENOSPC, N/N-1,
  recovery, and 24/72-hour backend endurance;
- vnode ownership epochs, checkpoint freeze/export/seal, restore-before-activate, rebalance fencing,
  and retention-safe cleanup;
- source replay/handoff and sink durability/topology/output-mode certification;
- separately proven exactly-once composition for each source/state/coordinator/sink combination;
  or
- an independently operated, immutable release-candidate product soak with a black-box oracle and
  pre-approved duration, event count, faults, rebalances, resource slopes, and invalid-run rules.

The local backend cannot manufacture distributed exactly-once. It supplies atomic working-state
operations and explicit persistence behavior; source offsets, checkpoint decisions, ownership, and
sink commit remain separate authorities.

## Owner decision record

The project owner recorded `APPROVE_MAINTENANCE_HEALTH_V2_DIRECTION` on 2026-07-24. This funds the
three mapping designs and consolidated contract/schema drafting only. Cycle 38 later authorized
validation-only implementation without the proposed two-owner workflow; it still does not authorize
a candidate or execution.

The alternatives retained here for audit history were:

- `APPROVE_MAINTENANCE_HEALTH_V2_DIRECTION`: fund the three mapping designs and consolidated
  contract/schema drafting only;
- `RETAIN_EXACT_MAINTENANCE_DEBT_V1`: fund a separately scoped engine-instrumentation decision and
  keep RocksDB/Fjall blocked until it closes; or
- `DEFER_CONTRACT_CHOICE`: continue only independently authorized work such as repairing the redb
  prescreen protocol, while cluster keyed/stateful admission remains closed.

After mapping-design and independent review, Cycle 38 accepted the complete consolidated contract,
schemas, formulas, and pre-result threshold ownership for validation-only implementation through the
project-owner direction and freezing commit. It still does not authorize native source
construction, an adapter, or candidate execution; each retains its separate gate.

At that time, silence or a generic “continue” could not be encoded as an owner outcome. Cycle 38 now
records the explicit project-owner validation-only direction. It still does not include backend
construction or candidate execution.
