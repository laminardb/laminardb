# Distributed keyed state — Core Cycle 9 review

- **Date:** 2026-07-28
- **Implementation commits:** `d3b3fadd`, `15359731`
- **Reviewed implementation head:** `15359731`
- **Scope:** reduce aggregate-transition live-capacity growth; attest transitive vnode lineage in
  immutable checkpoint artifacts; reject oversized or inconsistent restore input before parent-body
  reads; single-flight seal inventory loads
- **Slice verdict:** **PASS FOR SEALED LINEAGE AND GLOBAL-SINGLETON PRE-BODY CONTAINMENT**
- **Production/admission verdict:** **BLOCK**; `[LDB-4007]` and `[LDB-0013]` remain unchanged

## Decision and result

Aggregate transition preparation now completes validation and private fallible allocation before
growing live collections. Checked arithmetic replaces saturating capacity calculations, and live
maps reserve only net final growth after transitioned keys are removed. Exact `max_groups`
succeeds; max-plus-one and overflow fail without publishing logical state or unnecessarily growing
live group capacity. Prepared replacement and retired-state allocations can still coexist until
publication finishes, so this is not a complete transition-RSS bound.

`VnodePartialLineage` records the immediate parent attempt, transitive raw-payload bytes, and
physical-artifact count. Coordinators construct roots or checked extensions from the exact sealed
parent. Candidate lineage advances only after every vnode write lands; sealed partial and reusable-
upload lineage become successor-parent eligible only after the exact seal succeeds. An aborted or
unsealed FULL/REFERENCE cannot seed a successor, a late completion cannot overwrite a newer
reference lineage, promotion validates every vnode before mutating any, and revocation clears cached
lineage before reacquisition establishes a new FULL root.

The in-process and object-store backends bind lineage into immutable partial provenance and
checkpoint seals. The object-store wrapper advances to `LDBVP3`, header version 3 and 164 bytes;
checkpoint seals advance to version 8. The raw rkyv `VnodePartial` FULL/REFERENCE/DELTA payload
suffix is unchanged. There is no legacy fallback: older V2 wrappers and seal-7 state require an
explicit state/checkpoint reset or new namespace until a migration bridge is designed. The waiver
of public LaminarDB API compatibility does not make durable checkpoint incompatibility harmless.

Recovery validates and sums requested head lineage before its first vnode body read. Each chain is
constrained by its sealed byte/artifact accounting. Before a parent body GET, the reader loads the
parent seal and proves the child lineage exactly extends it; the decoded `base` must then name that
same sealed parent. Verified-body accounting includes reference-only artifacts omitted from the
retained apply chain. Transition staging proves the accounting covers every retained buffer before
accepting chains for graph/operator semantic decode or apply. Seal inventories use one success-only
asynchronous single-flight cell per attempt, so concurrent vnodes share a successful read while
transient failures remain retryable.

The reader cap is deliberately scoped as a **global-singleton compatibility envelope**. It is not a
production keyed-transition budget or memory reservation. There is no writer/Commit-domain
cluster-global cap, keyed multi-owner capability binding, decoded-expansion/RSS budget, apply-pause
deadline, or qualified local working-state backend. No TidesDB package, backend adapter, runtime
selector, window/join lifecycle, admission bypass, source/sink contract, delivery change, or soak
tooling was added.

The aggregate growth and built-in backend contracts are mode-neutral and compile in the embedded,
single-node, and cluster profiles. Requested-subset recovery and staging are cluster lifecycle work.
This slice does not claim all-mode behavioral recertification or make window/join state consumers
complete in any mode.

## Verification

| Check | Result |
|---|---:|
| `laminar-core` state suite | passed, 133/133 |
| vnode-chain rehydration suite | passed, 24/24 |
| vnode-transition staging suite | passed, 11/11 |
| focused coordinator lineage/lifecycle regressions | passed, 5/5 |
| `laminar-db` cluster test check | passed |
| `laminar-db` no-default and no-default-plus-cluster checks | passed |
| warnings-denied `laminar-core` all-features library Clippy | passed |
| warnings-denied `laminar-db` no-default-plus-cluster library Clippy | passed |
| `laminar-server --no-default-features` binary check | passed |
| minimal `laminar-server --no-default-features --features cluster` binary check | passed; not the broad default-feature matrix |
| exact cluster server test target, build-only | passed |
| formatting and diff hygiene | passed |
| broad default-feature server matrix | **not completed; Cargo stalled and timed out on this host** |
| broad non-library DB attempt | **not completed; Windows exhausted its paging file** |
| previous cluster failover/ALO engineering soak on the changed binary | **not run** |
| cluster exactly-once soak | **not applicable; `[LDB-0013]` remains fail-closed** |
| independent immutable-release-candidate soak | **not run; certification remains unavailable** |

The two broad host failures are not passes and are not classified as product regressions: neither
produced a completed test result. They must be rerun on a sufficiently provisioned Linux/CI or
equivalent host before any broad-matrix claim. Historical failover and at-least-once engineering
evidence remains recorded, but it cannot be transferred to the Cycle 9 binary. Cluster exactly-once
remains fail-closed rather than an executable soak profile.

## End-of-cycle review

- **AI slop and overengineering:** pass for this slice. It adds one concrete lineage value, one
  reader preflight, and one per-attempt single-flight cache around existing backend and recovery
  authority. It adds no generic quota framework, storage façade, backend candidate, feature toggle,
  compatibility bridge, or admission shortcut.
- **Hot path and latency:** pass only for row-path scope. No per-row lock, I/O, task, or backend call
  was added. Checkpoint and recovery paths perform checked metadata accounting, and parent seal
  reads are single-flighted. There is no p99/p99.9, allocation-pressure, request-count, RSS, or
  apply-pause result.
- **Unused code and maintainability:** pass with `DKS-CLEANUP-001` still open. Restore accounting is
  validated against every retained buffer at staging and is not stored on the pending transition
  for a redundant graph scan. Ordinary test fixtures derive accounting from their chains, vnode-
  cache retention uses one membership set instead of repeated
  quadratic scans, and misleading capacity/provenance names were corrected. Coordinator code and
  tests remain large; `rehydration_tests.rs` now mixes baseline, lineage, accounting, and concurrency
  cases and should be split before substantial further growth. Parallel coordinator lineage maps
  should become cohesive records when that state next changes, not through an unrelated refactor.
- **Production readiness:** **BLOCK**. A reader-only envelope cannot make an already committed
  multi-vnode cut safely restorable. Production still needs a durable cluster-global limit checked
  before Commit, an exact acquired-subset reservation held through fetch/decode/apply, separate
  metadata/request/spool/scratch/decoded/live/retired budgets, deadlines and minimum operational
  signals, a rolling/reset release policy, qualified optional working-state storage, complete
  delivery composition, window/timer and every persistent join-family lifecycle, and the independent
  immutable-RC soak.
- **Documentation and overdocumentation:** pass after reconciling the living ADR, plans, artifact
  format, validation report, and changelog with V3/seal 8 and the narrow reader guarantee. No new
  research or certification document was added; this review owns the cycle history.
- **Tests:** focused boundaries cover exact/max-plus-one accounting, pre-body rejection, parent
  mismatch, reference-only consumption, transient inventory failure, concurrent single-flight,
  abort/non-promotion, revoke/reacquire, and aggregate capacity behavior. The targeted matrix is
  green. Broad host-limited commands and every current-binary soak remain unclaimed.

## Next core slice

Core Cycle 10 should close one durable-authority gap: the Commit-domain raw restore-input budget.
After exact seal and participant-readiness validation, but before recovery-capsule persistence and
the durable Commit decision, traverse the exact parent-seal lineage from every required head,
verify every child attestation is the checked extension of its sealed parent through a root, and
only then compute the checked cluster-global transitive payload/artifact total. Compare it with one
versioned limit bound to exact cluster capability/configuration. A mismatch must Abort before
checkpoint-bound source-cursor publication, external sink phase-2 publication attributable to that
attempt, or successor-parent publication. Restore must consume that same cluster-identity-bound
contract and reject an acquired subset before its first body GET; comparison alone must not be
called a memory reservation.

Prove exact-limit success, max-plus-one pre-Commit Abort, no body read before target rejection, no
checkpoint-bound cursor or sink phase-2 publication attributable to the rejected attempt, no
successor-parent publication, and safe retry. Ordinary at-least-once output may pre-exist and replay;
this gate does not promise exactly-once isolation. Keep metadata/request/spool/decoder/RSS/pause,
vnode-sharded publication, backend qualification, delivery, windows/joins, and independent soak as
separately owned later gates. Do not add a runtime backend or relax admission in Cycle 10.
