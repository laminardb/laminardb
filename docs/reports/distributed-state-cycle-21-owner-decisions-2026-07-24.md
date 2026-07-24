# Distributed-state Cycle 21 owner decisions

- **Date:** 2026-07-24
- **Bounded-memory decision:** reference/conformance-only
- **Contract direction:** `APPROVE_MAINTENANCE_HEALTH_V2_DIRECTION`
- **Backend selected:** none
- **Candidate execution authorized:** no
- **Runtime/admission effect:** none; `[LDB-4007]` and `[LDB-0013]` remain fail-closed

## Recorded decisions

The project owner supplied both decisions explicitly:

1. bounded memory remains reference-only; and
2. the maintenance-health v2 design direction is approved.

The first decision removes a bounded-memory cluster product profile from the current ADR, phased
plan, and independent production-soak charter. The in-memory implementation remains required for
model, differential, lifecycle, and conformance testing. It supplies no cluster admission or
production evidence. Reopening a bounded-memory product profile requires a future ADR amendment and
fresh applicability, hard-cap, controlled-exhaustion, source-retention, restore/RTO, support, and
independent-soak approval.

The second decision completes Stage 1 of the
[maintenance-health v2 proposal](../architecture-decisions/state-backend-maintenance-health-v2-proposal.md).
It authorizes the already-complete three-candidate paper mappings and a consolidated contract/schema
freeze candidate for review. It does not instantiate the reserved v2 identities, approve numerical
thresholds, authorize v2 validator implementation, fund candidate source changes, add an adapter,
execute a candidate, select a backend, start Phase 1, or change production claims. A later explicit
`APPROVE_STATE_BACKEND_RUNNER_CONTRACT_V2` from the workload and operations owners must bind the
complete contract, reserved schema/formula semantics, exact v4 profile, threshold-ownership rules,
and independent reviews before v2 validation-only implementation. Candidate-native numerical
thresholds remain separately frozen in each immutable mapping and later qualification approval; the
direction decision does not approve them.

## Current profile and work authority

| Item | Current treatment | Next authority |
|---|---|---|
| In-memory working state | Semantic reference/conformance subject only | Separate future ADR amendment before any product profile |
| Local-spill working state | Sole current cluster production target; backend undecided | Phase 0 contract, candidate closure, qualification, selection, and independent product soak |
| Maintenance-health v2 | Direction approved; [freeze candidate](../architecture-decisions/state-backend-qualification-runner-v2-draft.md) unapproved | Final two-owner contract approval before reserved identities or validator implementation |
| redb 4.1.0 prescreen | Validation-only protocol repair permitted | Detached pre-run approval only after verifier, harness, oracle, actuator, classifier, and independent review exist |
| RocksDB/Fjall | Paper/source review only under their existing gates | Final v2 contract plus candidate-specific source authority |

This record does not turn the user-approved redb validation work into approval to run Docker smoke or
the native prescreen. The Cycle 16 `construction-only-no-decision` lane remains the only executable
redb code and cannot emit a prescreen disposition.
