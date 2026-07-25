# Distributed-state Cycle 21 owner decisions

- **Date:** 2026-07-24
- **Bounded-memory decision:** reference/conformance-only
- **Contract direction:** `APPROVE_MAINTENANCE_HEALTH_V2_DIRECTION`
- **Backend selected:** none
- **Candidate execution authorized:** no
- **Runtime/admission effect:** none; `[LDB-4007]` and `[LDB-0013]` remain fail-closed

**Current-state note (Cycle 38):** the direction decision below remains authoritative, but Cycle 38
supersedes the later proposed two-owner/protected-workflow mechanism and accepts the
[consolidated runner](../architecture-decisions/state-backend-qualification-runner-v2-draft.md) for
validation-only implementation through ordinary technical review and the freezing commit. This
chronological record does not reactivate older redb work or authorize candidate execution. The
project owner also makes TidesDB the preferred local-spill product candidate instead of RocksDB;
this prioritizes a bounded remediation/source-closure design but is not backend selection,
construction, execution, runtime admission, or production approval.

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
execute a candidate, select a backend, start Phase 1, or change production claims. Cycle 38 later
accepted the complete contract, schema/formula semantics, exact v4 profile, threshold-ownership
rules, and ordinary technical review for validation-only implementation without the proposed
protected approval workflow. Candidate-native numerical
thresholds remain separately frozen in each immutable mapping and later qualification approval; the
direction decision does not approve them.

## Current profile and work authority

| Item | Current treatment | Next authority |
|---|---|---|
| In-memory working state | Semantic reference/conformance subject only | Separate future ADR amendment before any product profile |
| Local-spill working state | Sole current cluster production target; TidesDB is preferred, but no backend is selected | Bounded TidesDB remediation/source-closure design, new non-v4 profile/mapping after closure, qualification, selection, and independent product soak |
| Maintenance-health v2 | Direction approved here; [validation contract](../architecture-decisions/state-backend-qualification-runner-v2-draft.md) accepted by Cycle 38 | Validation-only implementation authorized; candidate construction and execution remain closed |
| redb 4.1.0 prescreen | PARKED after Cycle 34; no profile, adapter, execution, or disposition | New explicitly approved bounded micro-prescreen charter before any work resumes |
| TidesDB | Preferred conditional candidate; current official Rust path remains rejected | Exact-current safe Rust/native design plus atomicity, recovery, read-cut, cgroup, health, profile, and separate execution gates |
| RocksDB/Fjall | Immutable v4/reference lineage only; no active product track | No new source, adapter, or run work absent a new project-owner direction |

This record does not authorize Docker smoke, native prescreen execution, or classification. The
Cycle 16 construction-only lane is historical and cannot emit a prescreen disposition; parking
supersedes its scheduled-work status.
