# Distributed keyed state — Core Cycle 5 review

- **Date:** 2026-07-27
- **Scope:** close transitive vnode-parent content identity and bound lineage traversal without
  changing the admitted raw payload wire or inventing an unenforceable restore-memory setting
- **Code commits:** `1913847d`, `6dfe7cec`, `cecf5f42`, `d389972a`
- **Slice verdict:** **PASS FOR THE LAMINAR-CONTROLLED BUILT-IN LIFECYCLE**
- **Production/admission verdict:** **BLOCK**; `[LDB-4007]` and `[LDB-0013]` remain unchanged

## Decision and implementation

A checkpoint attempt is unique only inside its create-once deployment/state namespace. Within that
scope, `StateBackend` now requires the first successful seal to fix one exact inventory until
retirement, and requires retirement to be monotonic and irreversible. The object-store backend
already supplies create-once seals and a durable prune floor. The volatile in-process reference
backend now publishes an in-memory floor while excluding attempt operations, removes retired
artifacts, returns no retired reads, and rejects every later write or seal below the floor.

Together with sealed length/digest reads and strictly decreasing parent links, this makes the raw
`CheckpointAttempt` parent field a sufficient transitive content identity under LaminarDB's
non-Byzantine storage model. A pruned/missing parent is unavailable and fails closed; it cannot be
replaced. Ancestry-aware retention remains an availability invariant. A custom backend that
remaps an attempt is non-conforming.

This is not yet durably self-proving after out-of-band deletion. If an administrator, provider
lifecycle rule, or storage-loss event deletes an unretired seal and artifacts, the current backend
has no ever-sealed tombstone above the prune floor. Production must qualify namespace lifecycle/IAM
against deletion/replacement or add a durable attempt ledger/parent content identity. Custom
backend conformance remains an admission gate.

The reader now reserves one slot before each physical-artifact lookup and rejects a vnode chain
over six physical artifacts. Six is the maximum for this binary's current writer policy: one FULL,
at most one direct REFERENCE, and at most four consecutive DELTAs. It intentionally does not use
the local node's narrower setting because retention policy is not yet bound into cluster identity.
Six is defensive current-policy containment, not a persisted rolling-upgrade invariant; a changed
writer policy requires capability/format fencing first.

No `max_restore_encoded_bytes` setting was added. Current seal metadata exposes head sizes only;
parents are discoverable only by fetching and decoding bodies. A head-only sum is false safety, a
reader-only limit could strand a valid committed cut, and pre-Commit body rereads would add restore
I/O to the checkpoint critical path. The later resource slice must attest per-vnode transitive
sizes, check the global total before capsule/Commit and sink phase 2, then reserve the target subset
before GET. Encoded bytes, retained/spooled bytes, decode scratch, decoded state/RSS, and apply
pause remain separate gates.

## End-of-cycle review

- **AI slop / overengineering:** pass. The cycle rejected a new payload frame, capability protocol,
  and public memory setting where the existing namespace/attempt contract and current metadata were
  sufficient for the normal built-in lifecycle. A future payload change has an explicit
  reader-first rollout requirement but no speculative runtime code.
- **Hot path:** pass for scope. All locks, counting, seal reads, and validation are on checkpoint,
  retention, boot, or rebalance paths. Normal row execution is unchanged. Restore performs at most
  six artifact-body reads per vnode, with the existing cross-vnode concurrency limit of 32. This
  does not bound seal-inventory metadata GETs; the current check-then-fetch cache is not single-flight,
  so request amplification and tail latency remain qualification work.
- **Unused code and maintainability:** pass for the touched slice. Both new bounds are production
  consumers, per-vnode names describe their scope, and no public facade/config/dependency was
  added. Moving lineage policy out of the oversized `pipeline_lifecycle.rs` and colocating its
  tests with recovery ownership remain `DKS-CLEANUP-001` work before independent soak.
- **Production readiness:** block. Aggregate restore bytes and RSS, transition identity/roster,
  prepare/publish/abort, semantic SQL install, delivery composition, backend qualification,
  custom-backend/storage-lifecycle conformance, rolling writer-policy fencing, latency evidence,
  cleanup, and independent release soak remain open.
- **Documentation:** pass. ADR, parent plan, Phase 0 plan, validation report, and this review agree
  that Laminar-controlled content identity/artifact traversal are contained while aggregate memory
  and pause are not.
- **Tests:** pass for the slice. The full DB/default-feature suite, latency/RSS tests, backend
  qualification, and independent soak were not run and have no pass claim.

## Verification

| Check | Result |
|---|---:|
| `state::in_process::tests` | passed, 14/14 |
| object-store fixed-inventory and durable-floor focused tests | passed, 2/2 |
| `recovery_manager::rehydration_tests` with cluster | passed, 17/17 |
| retention/delta/absolute-artifact policy test | passed, 1/1 |
| `cargo check -p laminar-db --tests --no-default-features --features cluster` | passed |
| `cargo check -p laminar-db --no-default-features` | passed |
| `cargo clippy -p laminar-db --no-default-features --features cluster -- -D warnings` | passed |
| `cargo clippy -p laminar-db --no-default-features -- -D warnings` | passed |
| `cargo check`/warnings-denied Clippy for `laminar-core` with cluster | passed |
| warnings-denied Clippy for `laminar-core` without defaults | passed |
| formatting and `git diff --check` | passed |
| full DB/default-feature suite, latency, RSS, backend qualification, independent soak | **not run** |

## Next core slice

Replace the split restore/revoke staging maps with one immutable pending-transition record. Bind the
audited predecessor and target fences, pipeline identity, exact committed restore cut, and canonical
acquired/revoked rosters before assignment publication and graph callbacks. Do not add a third
mutex, change the persisted payload, add TidesDB, relax admission, or resume soak tooling.
