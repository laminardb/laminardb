# Distributed keyed state — Core Cycle 4 review

- **Date:** 2026-07-27
- **Scope:** retain the authority-validated restore head, exact per-artifact sealed reads, complete
  attempt staging, and private chain-loader ownership without changing the admitted global
  aggregate's persisted wire
- **Code commits:** `56e2e08a`, `5fcb8517`, `74a38895`, `8dab9a36`, and corrective commit
  `8306e808`
- **Slice verdict:** **PASS FOR THE REDUCED, COMPATIBILITY-PRESERVING SLICE**
- **Production/admission verdict:** **BLOCK**; `[LDB-4007]` and `[LDB-0013]` remain unchanged

## Implemented boundary

Assignment acquisition and boot recovery retain the same `Arc<CheckpointSealInventory>` already
validated with the committed recovery capsule and source cut. They no longer validate one head and
then reread a substitutable equal-attempt inventory.

Built-in state backends accept the selected `SealedVnodePartial` plus a per-artifact byte limit.
The object-store implementation rejects an impossible or oversized envelope from metadata before
polling its body; both built-ins verify the returned payload's exact length and digest. Custom
backends that cannot implement this proof fail closed through the trait default.

The corrective slice preserves the established raw rkyv `VnodePartial` FULL, REFERENCE, and DELTA
layout used by the admitted cluster global aggregate. It removes the incompatible framed successor
and adds a frozen DELTA fixture to catch future layout drift. The child still names its parent by
bare checkpoint attempt. Reading a parent proves that artifact against the parent inventory observed
at restore time, but the child does not bind that exact inventory; self-consistent transitive parent
seal/body substitution therefore remains open.

The restore implementation remains private in `recovery_manager/vnode_chains.rs`; the former public
rehydrator exports stay removed and production construction requires a validated head. Graph
staging carries the complete checkpoint attempt and rejects mixed-attempt batches before callbacks
or vnode activation.

The per-artifact byte limit is not cumulative. Concurrent heads and discovered ancestors can retain
multiple individually valid artifacts, and archive alignment copies, decoded graph state, allocator
overhead, and publication work are outside it. Cycle 4 therefore makes no aggregate memory, RSS,
decode-expansion, or apply-pause claim.

## End-of-cycle review

- **AI slop and overengineering — corrected:** the framed wire, parent-fingerprint protocol,
  reader-wide budget, and single-flight cache were removed after compatibility and reachability
  review. The retained code closes only the authority and per-artifact boundaries it can prove.
- **Hot path — pass for scope:** all retained work is checkpoint, startup, or rebalance control
  path. No normal row-path branch, backend dependency, worker pool, or lock was added.
- **Unused code, naming, and module ownership — pass for the touched loader; final gate open:** the
  chain loader is private and named by its actual job. Repository-wide soak/certification helpers,
  zero-consumer APIs, fault-injection polling, dormant state formats, oversized modules, and stale
  research remain owned by `DKS-CLEANUP-001`.
- **Production readiness — block:** transitive ancestry is not immutable, aggregate restore resource
  bounds are absent, there is no authoritative transition/operator-table roster, callbacks are not
  prepare/publish/abort, SQL has no semantic install boundary, source/operator/sink delivery
  composition remains open, TidesDB is unqualified, and no independent release soak has run.
- **Documentation — reconciled:** the ADR, both plans, validation report, and this review distinguish
  exact-head/per-artifact closure from open ancestry and aggregate resource limits.
- **Tests — pass for scope:** the final wire fixture, sealed-chain/backend checks, graph and boot
  modules, focused coordinator regressions, both feature/Clippy matrices, formatting, diff, and link
  checks pass. The full DB/default-feature suite was not run and has no pass claim.

## Verification record

| Command or suite | Result |
|---|---:|
| `cargo check -p laminar-db --no-default-features` | passed |
| `cargo check -p laminar-db --no-default-features --features cluster` | passed |
| final `vnode_partial::tests`, including the frozen DELTA fixture | passed, 7/7 |
| `recovery_manager::rehydration_tests` | passed, 15/15 |
| `operator_graph::tests` | passed, 112/112 |
| `pipeline_lifecycle::boot_vnode_recovery_tests` | passed, 8/8 |
| focused coordinator/reference/delta regressions | passed, 4/4 |
| built-in exact/bounded sealed-partial reads | passed, 3/3 |
| both warnings-denied Clippy matrices | passed |
| `git diff --check` and relative-link scan over these five documents | passed |
| `cargo fmt --all -- --check` | passed |
| full DB/default-feature suite | **not run; no pass claim** |

The earlier broad Windows invocation timed out/broke its pipe only after the library-test binary
finished linking; a cached focused rerun is not a full-suite result. Nothing in this record is
latency, RSS, backend qualification, or independent soak evidence.

## Maintainability follow-up

The normative cleanup order remains
[DKS-CLEANUP-001](../plans/distributed-keyed-stateful-operators.md#core-workstream-reset). The final
repository-wide cleanup must precede the independent soak so the evidence names the maintainable
release candidate rather than obsolete helpers or experiments.

## Next core slice

Define the version-fenced upgrade and rollback contract that can bind each transitive parent
attestation without stranding established global-aggregate checkpoints. In the same bounded design
slice, specify aggregate chain/concurrency accounting separately from RSS, decode scratch, and
publication-pause budgets. Do not add TidesDB, relax admission, or resume soak/certification tooling.
