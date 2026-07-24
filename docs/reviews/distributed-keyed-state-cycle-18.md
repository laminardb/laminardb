# Distributed keyed state Cycle 18 review

- **Date:** 2026-07-24
- **Branch:** `feature/distributed-keyed-state-adr`
- **Cycle commits:** `ea9d7d37`, `adc87427`
- **Cycle outcome:** `V2_DIRECTION_PROPOSED_OWNER_DECISION_PENDING`
- **Production backend selected:** none
- **Candidate execution authorized:** no
- **Cluster admission:** unchanged and fail-closed under `[LDB-4007]` and `[LDB-0013]`
- **Production verdict:** **NO-GO**

## Outcome

Cycle 18 converts the Cycle 17 mechanism finding into a bounded owner decision. The
[maintenance-health v2 proposal](../architecture-decisions/state-backend-maintenance-health-v2-proposal.md)
recommends replacing only v1's universal exact maintenance-debt byte population with a
candidate-specific typed health arm. Exact foreground pressure-stall intervals and every common
correctness, open-loop latency, throughput, resource, target-I/O, pressure, persistence, recovery,
fault, and endurance gate remain mandatory.

Candidate health is a conjunctive operational veto, not a score. Signals retain native scope,
units, quality, cadence, reset behavior, overhead, and independently frozen predicates. The
proposal rejects a generic expression language, cross-engine sums, candidate-defined aggregation,
and opportunistically missing samples. A complete N/A arm requires exact-build, complete-process
proof that no asynchronous state-maintenance mechanism exists; it waives none of redb's global
writer, commit, durability, growth, recovery, or common latency/resource obligations.

The [decision matrix](../reports/state-backend-contract-decision-matrix-2026-07-24.md) recommends the
v2 direction as a design hypothesis, not a measured cost result or backend choice. It explicitly
separates four authorities:

1. a direction record permits formal mapping and consolidated-contract design;
2. a later two-owner `APPROVE_STATE_BACKEND_RUNNER_CONTRACT_V2` record freezes the complete
   contract, schemas, formulas, and pre-result thresholds and may authorize validator-only work;
3. a candidate-specific source-closure approval is required before native construction; and
4. an immutable candidate mapping follows source construction and adversarial activation proof.

No direction approval has been inferred from the user's generic continuation instruction. The
reserved v2 identities therefore do not yet exist as approved evidence contracts.

## Backend consequence

| Candidate | Cycle 18 status | Next decision-producing evidence |
|---|---|---|
| RocksDB 10.4.2 / wrapper 0.24.0 | **Primary paper-mapping track; blocked, not selected** | Mapping design, final contract choice, then separately approved source proof for all identified gaps |
| Fjall 3.1.8 | **Rust-native alternative; blocked** | Mapping design must establish whether any telemetry/control closure is bounded; relative patch size is unknown |
| redb 4.1.0 | **Prescreen hedge; deferred** | Complete its separately governed global-writer/commit/recovery prescreen; N/A maintenance evidence cannot admit it |
| SurrealKV 0.21.2 | **Rejected unmodified** | Correct snapshot-retention/liveness behavior before telemetry or qualification work |

## Six-pass cycle review

### 1. AI slop and contract consistency

**Pass after independent correction.** Review removed claims that RocksDB or Fjall patch size and
hot-path risk were already known, distinguished system facts from LaminarDB design inferences, and
fixed the initial conflation of direction approval with final contract approval. It also corrected
FAIL versus INVALID classification, asynchronous-maintenance scope, mapping/source-proof order,
mandatory sample populations, exact-version migration language, and the Materialize evidence pin.
Both the contract and evidence reviewers report PASS on the corrected documents.

### 2. Overengineering and hot path

**Pass for a design-only cycle.** The proposal changes one disputed v1 arm and retains the common
comparison surface. It adds no metrics DSL, weighted health score, cross-engine normalization, or
exhaustive property dump. Gate-bearing sources must be bounded and cheap; reads stay off the event
loop, no per-row query/FFI/allocation/lock/I/O/task-spawn path is allowed, and any synchronous
callback must use bounded preallocated storage. Telemetry-on/off A/B must measure throughput, every
gate-bearing latency percentile and maximum, CPU, memory, and observer resources before use. No
unearned numerical overhead budget was invented.

### 3. Unused code and dependencies

**Pass.** Cycle 18 changes documentation only. It adds no backend crate, native library, adapter,
feature flag, schema implementation, workflow, generated fixture, or unused runtime abstraction.

### 4. Production readiness, delivery, and soak

**NO-GO, correctly fail-closed.** DKS-Q2-006 remains open and no backend has qualified. The vnode
ownership/checkpoint/rebalance lifecycle is not implemented, and grouped aggregates, windows, and
stateful joins remain rejected in cluster mode. Exactly-once still requires separately proven
source-offset, state snapshot, coordinator-decision, recovery, and sink-commit composition for each
supported topology. No connector combination, 24/72-hour backend endurance run, or independently
operated immutable release-candidate product soak has passed. Neither the proposal nor its future
backend evidence can waive those gates.

### 5. Documentation, stale research, and overdocumentation

**Pass.** One delta proposal owns the normative design candidate and one matrix owns the owner
choice; the ADR, v1 baseline, and two plans received short links and authority corrections rather
than duplicated specifications. Primary operational references are official and versioned where
available; the RisingWave and Materialize inventories are pinned to exact 2026-07-24 commits, and
the remaining rolling operations page is labelled navigational. No stale Claude-memory or generated
research document was introduced or retained by this cycle.

### 6. Tests and empirical limits

**Document checks pass; runtime evidence remains intentionally absent.** `git diff --check` and all
relative Markdown links across the six changed design documents pass. Independent reviewers checked
the proposal against runner v1 and the pinned Flink, RisingWave, Materialize, Spark, Kafka Streams,
SILK, and RocksDB production evidence. No code, backend build, candidate run, latency measurement,
fault injection, endurance test, delivery test, or product soak was performed; none may be inferred
from this cycle.

## Cycle 19 entry condition

Without an owner direction record, continue only non-normative, read-only mapping drafts for
RocksDB, Fjall, and redb. Each draft must pin its intended source/build/configuration evidence; map
asynchronous mechanisms to the smallest objective-covering typed signal set; record scope, quality,
cadence, reset, activation, overhead, and unresolved bindings; and distinguish proposed threshold
bases from approved values. Do not instantiate v2 schemas, construct native observers, add an
adapter, run a candidate, or change cluster admission.
