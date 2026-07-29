# Distributed keyed state — RocksDB adapter-entry source-closure review

- **Date:** 2026-07-29
- **Decision commit:** `cf48a4d4`
- **Decision:** [RocksDB adapter-entry source closure](../reports/rocksdb-0.24.0-adapter-entry-source-closure-2026-07-29.md)
- **Scope:** exact `rocksdb` 0.24.0 / bundled RocksDB 10.4.2 first-veto gate and active-plan
  reconciliation
- **Cycle verdict:** **PASS FOR STOP BEFORE DEPENDENCY OR ADAPTER**
- **Production/admission verdict:** **BLOCK**; no backend is selected and `[LDB-4007]` and
  `[LDB-0013]` remain fail-closed

## Result

The source gate correctly stops the released RocksDB path. Safe `compact_range*` methods are
synchronous and return `()` while the bundled C shim discards native `Status`; the same release has
no bounded, fallible in-process cancellation/close sequence. Timed waiting cannot cancel the
operation or recover its result. File-range deletion, per-vnode CFs/DBs, full-root rewriting, and a
sidecar do not supply the selected small all-mode adapter contract without creating a different
storage architecture.

The 10.4.2-to-11.1.2 delta is not itself the first veto under the narrow plain-DB/fresh-root profile.
It does require fresh roots after abnormal shutdown or upgrade and exclusions for round-robin/FIFO
compaction, advanced transactions/recovery, native rate-limited deletion, and other unused modes.
Those exclusions do not repair the cleanup/lifecycle API failure.

No dependency, lockfile entry, runtime implementation, helper, candidate executable, CI workflow,
soak tool, backend selector, or admission change landed.

## Independent-review corrections

Three independent source/integration reviews found no error that overturns STOP. Their substantive
wording and plan findings were corrected before this review closed:

| Finding | Resolution |
|---|---|
| Deep Phase 0/Phase 1 instructions still assumed a selected RocksDB backend would pass | Replaced with a new exact-official-release source/re-entry and integration decision; current Phase 1 remains backend-neutral. |
| `wait_for_compact` wording implied it cannot be invoked concurrently | Clarified that concurrency still neither cancels `compact_range` nor recovers discarded status; the selected single-owner sequence reaches it only after the blocking call. |
| Physical reclamation was described as impossible to prove at all | Narrowed to the actual failure: live-file/filesystem postconditions can observe eventual bytes, but cannot reconstruct native status or make the call bounded/fallible. |
| Windows/Linux bundled-build feasibility was stated without a build | Reclassified as an inspected build-script path; platform compilation/linking remains unverified. |
| Candidate-set wording could imply every published engine was exhaustively reviewed | Scoped the empty set to reviewed, owner-approved released candidates. |
| Re-entry accepted merely cancellation-aware cleanup | Now requires deadline-enforced completion or cancellation with bounded acknowledgement/final return, plus a physical postcondition. |
| License wording compressed distinct licenses | Recorded wrapper Apache-2.0, sys MIT/Apache-2.0/BSD-3-Clause, RocksDB GPL-2.0-or-Apache-2.0, and the LevelDB BSD notice. |
| Delta text understated the first create-CF leak and omitted the reverted range-metadata fix | Corrected both; the fresh-root-across-upgrades rule remains mandatory. |

## Verification

| Check | Result |
|---|---:|
| Exact wrapper/sys archive SHA-256 values | matched `ddb7af00...fce34f` and `cef2a00e...35de9` |
| Official wrapper tag `v0.24.0` and bundled/native tag identities | source-verified |
| Rust `compact_range* -> ()` and C `CompactRange` status discard | source-verified |
| Timed wait, void cancellation, Rust Drop, C close, and native wait behavior | source-verified |
| `delete_file_in_range*`, per-vnode CF/DB, full-root rewrite, suggestion, and sidecar alternatives | reviewed; none meets the frozen small all-mode contract |
| 426-commit `v10.4.2...v11.1.2` correctness/resource/lifecycle delta | screened and classified |
| Bundled-build override/provenance controls | source-verified; build not executed after veto |
| Cargo manifests, `Cargo.lock`, runtime source, CI, tests, and tooling | unchanged; no RocksDB package resolved |
| `cargo metadata --locked --no-deps` | passed |
| Changed-document relative links | passed, 145 targets across all cycle documents |
| `git diff --check` | passed; repository LF/CRLF conversion warnings only |
| Runtime compile/unit/failover/ALO/EO/soak tests | not run; no runtime subject changed, and candidate/soak work remains paused |

## End-of-cycle review

- **AI slop and overengineering:** pass. The first hard veto stopped construction. No generic
  backend trait, runtime enum, wrapper around a void API, polling observer, per-vnode engine/CF,
  sidecar IPC, qualification harness, or speculative fallback was added.
- **Hot path and latency:** unchanged. There is no new FFI, allocation, lock, task, queue, I/O, or
  row/batch path. The future contract still requires batched access on bounded owner lanes and does
  not accept sidecar latency without a separate measured decision.
- **Unused code and human maintainability:** pass for touched scope. No code or helper was added.
  Active authority now says no backend is selected. `DKS-CLEANUP-001` remains mandatory before any
  future local-state dependency, but mixing the large stopped-redb cleanup into this source-veto
  cycle would have obscured the decision.
- **Production readiness:** **BLOCK**. A local-spill engine, bounded all-mode lifecycle, hot/cold
  tail/resource/fault evidence, checkpoint/rebalance integration, source/sink delivery matrices,
  failover/ALO/EO regressions, qualification, cleanup, and independent immutable release-candidate
  soak all remain open.
- **Documentation:** pass after correction. One 140-line evidence report owns exact source/delta/
  re-entry detail; living documents were reconciled mostly by replacing stale direction. Historical
  reviews remain evidence rather than being rewritten.
- **Tests:** appropriate for a source/documentation stop. Structural checks passed. A happy-path
  database run could not prove the missing failure return or bounded lifecycle and was forbidden by
  the first-veto rule; no empirical or production claim is made.

## Next-cycle review plan

Resume only the backend-neutral Core Cycle 11 acquired-subset transition reservation. Review that
cycle for:

1. one explicit reservation owner and absolute acquisition deadline/cancellation, with release on
   publish, abort, poison, and graph replacement; reject duplicate ledgers or generic frameworks;
2. no per-row work, unbounded request/spool retention, hidden allocation, event-loop blocking, or
   unmeasured latency change;
3. clear names, small cohesive functions/modules, no dead/public compatibility surface, and no new
   soak/certification helper in production code;
4. fail-closed checkpoint/rebalance and delivery behavior, including focused failover/ALO/EO
   regressions if the touched boundary can affect them;
5. formatting, warnings-denied Clippy for both feature matrices, focused unit/fault tests,
   dead/public-code and documentation-link checks; and
6. the same AI-slop, overengineering, unused-code, production-readiness, overdocumentation, and test
   audit before the cycle closes.

Do not re-enter backend work until an exact official release has a concrete path through the failed
cleanup/lifecycle gates. Do not resume soak, A/B, observer, transcript, or certification tooling;
the independent soak remains a final gate for a cleaned, qualified immutable release candidate.
