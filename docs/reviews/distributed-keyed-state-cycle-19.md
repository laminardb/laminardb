# Distributed keyed state Cycle 19 review

- **Date:** 2026-07-24
- **Branch:** `feature/distributed-keyed-state-adr`
- **Cycle commits:** `63a8e1e4`, `498ce96f`
- **Cycle outcome:** `CANDIDATE_HEALTH_MAPPINGS_REVIEWED_CONTRACT_DECISION_PENDING`
- **Production backend selected:** none
- **Candidate construction or execution authorized:** no
- **Cluster admission:** unchanged and fail-closed under `[LDB-4007]` and `[LDB-0013]`
- **Production verdict:** **NO-GO**

## Outcome

Cycle 19 completes the non-normative paper mappings permitted as input to the proposed v2 direction.
The reviewed [mapping report](../reports/state-backend-maintenance-health-mapping-designs-2026-07-24.md)
finds:

- RocksDB has useful stock flush/compaction properties but still needs scheduled-compaction, typed
  purge, complete error/recovery, safe integer/listener binding, and exact foreground-stall sources.
  Its timed wait is a non-gating diagnostic, not a fabricated sampled boolean.
- Fjall has useful hidden diagnostics but needs scheduler/lifecycle repair, a coherent typed
  snapshot, complete worker/error/physical-delete health, resource-control resolution, and exact
  stalls. Adding counters without restoring stranded work would be insufficient.
- redb's exact crate supports a plausible whole-arm N/A, but only complete-process source proof and
  task/thread-aware native observation can approve it. The global writer, inline mutation/commit/
  reclaim/resize work, durability mode, recovery, and unreportable close failure remain separate
  vetoes.

The mapping vocabulary needs no score, DSL, cross-engine sum, or active terminal-command result for
these designs. Numerical thresholds, cadence, overhead limits, exact wire schemas, v4 identity and
candidate mappings remain unapproved. The report deliberately does not rank unbuilt patch cost.

## Backend carry consequence

| Candidate | Cycle 19 carry status | Next gate, not an implementation order |
|---|---|---|
| RocksDB 10.4.2 / wrapper 0.24.0 | **Observed design incomplete; blocked** | Final v2 contract, then separately approved enumerated source-closure specification/construction |
| Fjall 3.1.8 | **Observed design unsupported in stock source; blocked** | Explicit owner decision to accept scheduler/lifecycle fork or upstream ownership before source work |
| redb 4.1.0 | **N/A source-plausible; prescreen deferred** | Candidate-specific profile translation, one frozen durability mode, prescreen approval, native complete-process N/A observation, and fail-closed lifecycle design |
| SurrealKV 0.21.2 | **Rejected unmodified** | Correct snapshot-retention/liveness defects before telemetry reconsideration |

RocksDB, Fjall and redb now have different decision stages, not a performance rank. None proceeds to
an adapter, candidate campaign, selection, runtime dependency, or cluster admission by elimination.

## Six-pass cycle review

### 1. AI slop and contract consistency

**Pass after three independent correction rounds.** Review removed unsupported claims that one
unbuilt closure is cheaper, narrower or strongest. It found and corrected RocksDB's scheduled-bottom
compaction interval, heterogeneous purge populations, incomplete purge wait, recovery and mutable
listener semantics; Fjall's stranded scheduler work, full-operation in-flight state, worker
liveness, multi-writer observation bracket, physical unlink tail and pre-baseline error loopholes;
and redb's runtime-task, profile-equivalence, mode-cherry-picking, fault-actuator, close-observability
and independent-stall-N/A loopholes. The corrected RocksDB, Fjall and redb sections each received an
independent PASS.

Facts, design hypotheses and missing proof are labelled separately. Current gauges are not said to
reset to zero after recovery, estimates are not called debt, warning logs are not health counters,
and a blocking command is not represented as a sampled boolean. Unsupported and N/A remain distinct.

### 2. Overengineering and hot path

**Pass for a design-only cycle; observer cost remains a later veto.** The report keeps only typed
signals tied to backlog/in-flight, failure or tail objectives and moves blind/wrapping fields to
diagnostics. It rejects filesystem scans, per-row queries, dynamic vnode labels, heterogeneous queue
sums, weighted scores and a general expression evaluator.

Proposed polling runs off event loops. RocksDB still needs DB-mutex/property and callback/FFI A/B
evidence; its stock C listener allocation is not hidden. Fjall's scheduler repair, transition
updates, multi-writer bracket and strategy-pressure source need contention and telemetry-on/off A/B
proof. redb gains no observer credit: whole transaction mutation, writer acquisition, commit,
reclamation and lifecycle work remains timed. No numerical overhead claim was invented.

### 3. Unused code and dependencies

**Pass.** Cycle 19 changes documentation only. It adds no engine crate, native fork, feature, adapter,
schema implementation, validator, harness, binary, workflow, generated fixture or runtime
abstraction. LaminarDB's root/runtime manifests and locks remain free of all candidate backends.

### 4. Production readiness, delivery and soak

**NO-GO, correctly fail-closed.** No backend has passed C1/C2/C3, physical persistence, recovery,
fault or 24/72-hour endurance evidence. Vnode ownership epochs, checkpoint freeze/export/seal,
restore-before-activate, rebalance fencing and retention-safe cleanup are not implemented. Grouped
aggregates, windows and stateful joins therefore remain rejected in cluster mode.

Exactly-once remains a composition claim over source offset/handoff, state snapshot, coordinator
decision/recovery and sink commit/fencing for each certified topology. No real connector pair has
passed that matrix. No immutable release candidate has completed the independently operated
black-box product soak with pre-approved duration, volume, faults, rebalances, leak slopes, latency,
progress, invalid-run rules and a reviewer independent of the implementation team. Backend health
or endurance cannot substitute for that soak.

### 5. Documentation, stale research and overdocumentation

**Pass.** One combined candidate report owns the Cycle 19 mapping delta; three near-duplicate
candidate documents were avoided. The ADR, proposal and two plans received short links and gate
updates rather than copied schemas. The exact source identities and prior reports remain relevant and
content-pinned; no stale Claude-memory assertion or unverified generated research was promoted. No
existing research document became irrelevant because of this cycle, so none was deleted merely to
create churn.

### 6. Tests and empirical limits

**Document and source-review checks pass; runtime evidence is intentionally absent.** `git diff --check`
and relative Markdown links across every changed document pass. Three independent reviewers
checked the corrected mappings against the pinned RocksDB 10.4.2/wrapper 0.24.0, Fjall/lsm-tree
3.1.8 and redb 4.1.0 sources and returned PASS.

No candidate was compiled, modified or run. No workload, latency, observer-overhead, persistence,
fault, recovery, endurance, connector, exactly-once or product-soak result was produced. Source
inspection and a quiet bounded probe cannot be presented as production readiness.

## Cycle 20 entry boundary

No owner direction record has been inferred from a generic continuation. The mappings now provide
the requested decision input. A normative consolidated v2 contract/schema draft requires an explicit
`APPROVE_MAINTENANCE_HEALTH_V2_DIRECTION`; retaining v1 or deferring remains valid. Candidate source
construction additionally requires the final two-owner contract and candidate-specific closure
approval.

Without that decision, the next safe work is limited to reviewing the owner choice packet and
non-normative threshold/operability evidence, or independently authorized repair of the redb
prescreen verifier/protocol. Do not instantiate v2, choose a backend, construct an engine patch or
adapter, execute a candidate, or change cluster admission.
