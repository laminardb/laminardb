# Distributed keyed state — TidesDB native fix review

- **Date:** 2026-07-28
- **Laminar evidence commit:** `c3e057ea`
- **Native patch commit:** `b80e424ae98540c61be81d83c85f03f43d93b1d0`
- **Upstream:** [TidesDB PR 664](https://github.com/tidesdb/tidesdb/pull/664)
- **Scope:** prevent two native false-success transaction outcomes and add deterministic regressions
- **Slice verdict:** **PASS FOR UPSTREAM REVIEW**
- **Production/admission verdict:** **BLOCK**; `[LDB-4007]` and `[LDB-0013]` remain fail-closed

## Decision and result

This cycle changed TidesDB native code, not Laminar runtime code. All four classic/unified,
stack/heap transaction callers now require `skip_list_put_batch` to return the requested count. The
unified 1,024-operation path returns `TDB_ERR_MEMORY` when batch preparation cannot allocate its
buffers instead of taking a fallback that can apply zero keys and report success.

The patch does not claim transaction rollback or restored atomicity. A short batch has already
written its WAL and may have inserted part of the memtable before the error is returned. A future
Laminar adapter must treat an ambiguous or possibly partial commit as
`StatefulOperatorPartialApply`, abandon the disposable local root, and restore from the last
Commit-admitted Laminar checkpoint. Known pre-apply failures need not poison the root.

No dependency, backend selector, storage facade, readback loop, two-phase commit, checksum,
scrubber, object-store hot path, or admission bypass was added. Laminar must consume a fixed official
native successor and matching official Rust source release, never this contributor fork.

## Verification

| Check | Result |
|---|---:|
| Four deterministic classic/unified stack/heap short-count cases, ASan/UBSan Debug | passed |
| Unified 1,024-operation allocation fault, ASan/UBSan Debug | passed |
| Existing skip-list partial-batch contract test, ASan/UBSan Debug | passed |
| Optimized Release: two new fault groups | passed |
| Optimized Release: basic transaction and six transaction-reset tests | passed, 7/7 |
| Optimized Release: classic large batch and two unified multi-key/multi-CF tests | passed, 3/3 |
| Broad ASan/UBSan CTest | **incomplete**; 5/11 executables passed before the 15-minute cutoff, stopped during clock-cache |
| `clang-format-14 --dry-run --Werror` and native diff hygiene | passed |
| DCO check on PR 664 | passed |
| Complete upstream CI/review and CLA/CCLA gate | pending |
| Laminar compile/test matrix | not run; no Laminar runtime code changed |
| Cluster failover/ALO/EO soaks | not run; paused and no Laminar binary changed |
| Independent immutable-release soak | not run; still mandatory before production |

The broad CTest result is deliberately not promoted to a pass. Its completed block-manager,
skip-list, compression, Bloom-filter, and manifest executables were green, but the remaining six
executables did not complete in this local timebox. Standard native tests are not soak evidence.

## End-of-cycle review

- **AI slop and overengineering:** pass for this slice. The production change is four exact-count
  checks plus removal of one invalid allocation fallback. The tests use existing internal state and
  allocator infrastructure; there is no production fault hook or speculative Laminar abstraction.
- **Hot path and latency:** pass for scope, not as a benchmark claim. Successful batch application
  adds one integer comparison per batch, not per key. The fallback deletion changes only an
  allocation-failure path. No new I/O, lock, allocation, readback, or durability fence was added.
- **Unused code and maintainability:** pass. Only `src/tidesdb.c` and `test/tidesdb__tests.c` are in
  the native commit. The exact-size fault affects `malloc` only and preserves existing
  `calloc`/`realloc` OOM behavior. QEMU skips and lower-case formatted comments follow upstream
  conventions. A newline-only `src/skip_list.c` worktree change was excluded from the commit.
- **Production readiness:** **BLOCK**. PR review/CI, a fixed native tag, an official matching Rust
  package, repeated entry validation, the `READ_COMMITTED` concurrent-reader question, capacity-
  failure lifecycle, the smallest all-mode Laminar adapter, vnode checkpoint/restore/rebalance,
  delivery integration, performance/resource qualification, and independent soak remain open.
- **Documentation and overdocumentation:** pass after correction. The empirical report is the
  canonical detailed record; the ADR, phase plan, and changelog carry only decision summaries and
  links. Historical Fjall/TidesDB material remains cited provenance rather than current authority;
  no newly superseded research file was identified for deletion in this cycle.
- **Tests:** focused fault and normal transaction paths are green in sanitizer and optimized builds.
  The incomplete broad lane, upstream matrix, Laminar matrix, prior cluster soaks, and independent
  soak are all stated as unclaimed rather than inferred from older binaries.

## Next gate

1. Let PR 664 complete upstream CI/review and satisfy the contributor agreement gate.
2. Require a fixed native successor tag and matching official `tidesdb-src-*`/`tidesdb` release.
3. Repeat the exact package-entry tests against those immutable released artifacts.
4. Only then review the smallest serialized-owner Laminar adapter/conformance slice for embedded,
   single-node, and cluster-with-admission-closed modes.

Backend-neutral managed-vnode lifecycle work may continue independently. Stateful cluster admission
does not change until the complete operator, checkpoint/rebalance, delivery, qualification, and
independent-soak gates pass.
