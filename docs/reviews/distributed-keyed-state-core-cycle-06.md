# Distributed keyed state — Core Cycle 6 review

- **Date:** 2026-07-27
- **Implementation commit:** `141e7c48`
- **Scope:** replace split vnode restore/revoke staging with one exact transition and prevent a
  graph generation from reusing state after lifecycle or execution authority becomes indeterminate
- **Slice verdict:** **PASS FOR TRANSITION IDENTITY AND FAILURE CONTAINMENT**
- **Production/admission verdict:** **BLOCK**; `[LDB-4007]` and `[LDB-0013]` remain unchanged

## Decision and implementation

`PendingVnodeTransition` is now the single immutable input to one graph-level vnode ownership
change. It binds boot or adjacent-assignment origin, exact predecessor and target certificates,
current process incarnation, pipeline identity, the complete validated committed restore cut, and
canonical acquired/revoked vnode rosters. Assignment publication installs one `Arc`; completion
clears the slot only when it still contains that exact allocation, preventing an identical-looking
replacement from being erased.

Live assignment acquisition accepts only a restore cut whose assignment certificate is the exact
predecessor. Boot recovery may use an older coordinated cut because it reconstructs a fresh graph.
A replacement process is treated as owning none of its predecessor incarnation's memory, so it
restores every target-owned vnode and cannot issue old-process revokes or final-owner callbacks.
Boot preparation publishes only after the target assignment audit; an audited zero-owned target may
then retire stale pending work and restoring lifecycle.

The graph decodes and resolves the complete raw-chain batch before the first callback, revalidates
assignment, transport, lifecycle, and exact pending-slot authority after callbacks, and activates
the complete acquired roster only after all callbacks succeed. Callback failure, authority drift,
cancellation, panic, or a cluster terminal error after input admission clears the installed-state
binding before graph poison is visible. The exact transition remains for fresh-graph recovery.

This is deliberately not semantic atomic publication. Existing revoke and restore callbacks still
mutate operators directly and sequentially. A later callback can fail after an earlier mutation;
the poisoned generation is discarded rather than rolled back. An exact installed-state binding may
avoid redundant restore only while the database is Running and the assignment plus pipeline
identity still match. For `QueryState::Uninit`, callback success currently means only that bytes
were staged: Active/Installed can publish before lazy semantic installation and the staged path does
not yet validate each key against its declared vnode. This remains a production blocker.

## End-of-cycle review

- **AI slop and overengineering:** pass for the implemented slice. One pending slot replaces two
  mutable staging structures, and one exact success marker replaces process-incarnation inference.
  No backend, trait hierarchy, configuration, payload wire, admission switch, or compatibility mode
  was added.
- **Hot path:** pass for scope. Durable reads, transition validation, locks, decoding, and callbacks
  remain on boot, rebalance, checkpoint, or fault paths. Normal cluster graph execution adds one
  installed-state-handle `Arc` clone/drop per cycle, not per row or operator. The installed-state
  mutex is acquired only when lifecycle or fault handling retires or publishes authority.
- **Unused code and maintainability:** pass for the new ownership boundary, with cleanup still open.
  The unsafe public `RehydratedVnode` inspection surface is removed and the transition model is a
  private cohesive module. Large DB, graph-transition, lifecycle, and test modules plus remaining
  rehydration terminology remain `DKS-CLEANUP-001` work before independent soak. Living ADR/plan/
  report chronology also remains overlong; per-cycle history should stay in review records while
  the living documents are reduced to current decisions, sequencing, and evidence summaries.
- **Production readiness:** block. Authoritative operator/state-table rosters, explicit semantic
  empty state, prepare/publish/abort shadows, `QueryState::Uninit` plan/codec installation,
  transition-wide encoded-byte/object/decode/RSS/apply-pause bounds, request-amplification and tail
  evidence, custom-backend and live-seal lifecycle conformance, writer-policy fencing, managed
  working-state/backend qualification, source/state/sink exactly-once composition, minimum
  operational signals, final cleanup, and independent release-candidate soak remain open.
- **Documentation:** pass for the touched records. ADR-008, the Phase 0 execution plan, validation
  report, and changelog distinguish transition containment from the
  still-missing managed semantic lifecycle.
- **Tests:** pass for the completed rows below. No full default-feature DB suite, latency/RSS
  campaign, backend qualification, or independent soak has a Cycle 6 pass claim.

## Verification

| Check | Result |
|---|---:|
| `vnode_transition_staging::tests` with cluster | passed, 9/9 |
| `operator_graph::tests` with cluster | passed, 119/119 |
| `pipeline_lifecycle::boot_vnode_recovery_tests` with cluster | passed, 10/10 |
| initialized aggregate authoritative replacement: stale-key removal and wrong-vnode atomic rejection | passed, 2/2 focused |
| focused DB controller identity, adoption exclusion/publication race, and replacement-process tests | passed |
| `rebalance::tests` with cluster | passed, 31/31 |
| full cluster-feature `laminar-db` library suite | passed, 1,787; failed, 0; ignored, 1 profiling test |
| `cargo check -p laminar-db --tests --no-default-features --features cluster` | passed |
| `cargo check -p laminar-db --lib --no-default-features` | passed |
| warnings-denied Clippy, cluster and no-cluster library configurations | passed |
| `cargo fmt --all -- --check` and `git diff --check` | passed |
| full default-feature DB suite, latency, RSS, backend qualification, independent soak | **not run** |

## Next core slice

Make the operator/state-table lifecycle roster authoritative, then implement one real semantic
prepare/publish/abort shadow using a plan/codec contract derived before artifact fetch. Keep the
managed backend, admission guards, delivery widening, and soak tooling out of that slice.
