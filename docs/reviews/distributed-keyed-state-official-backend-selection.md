# Distributed keyed state — official backend selection review

- **Date:** 2026-07-29
- **Decision commit:** `2a803cd9`
- **Scope:** collapse the released backend search to one adapter-entry path and remove the stopped
  redb construction job from required CI
- **Cycle verdict:** **PASS FOR BOUNDED ROCKSDB ADAPTER ENTRY**
- **Production/admission verdict:** **BLOCK**; qualification remains separately gated and
  `[LDB-4007]` and `[LDB-0013]` remain fail-closed

## Result

The strict vendor-authored-Rust shortlist is empty. Laminar's practical official-release rule is
therefore explicit: a canonical non-yanked crates.io release tied to a released bundled engine,
with no fork, git dependency, local native patch, system-library substitution, or unreleased fix.
Under that rule, only `rocksdb 0.24.0` -> `librocksdb-sys 0.17.3+10.4.2` -> bundled RocksDB 10.4.2
is carried into one bounded adapter-entry cycle.

This is not selection by a score or production approval. Current TidesDB fails acknowledged-write
correctness, Fjall 3.1.8 fails the released source lifecycle gate, and redb 4.1.0 remains stopped on
resource/lifecycle evidence. None is a runtime fallback. A RocksDB hard veto returns backend choice
to an owner decision and leaves admission closed.

RocksDB's teardown boundary is recorded accurately. Timed compaction waiting does not stop jobs,
background cancellation has no timeout/status, and Rust Drop calls void `rocksdb_close`. The entry
fault harness must therefore use a child process and can prove only process containment plus root
quarantine. It cannot claim bounded in-process shutdown. Production remains blocked unless that
operational containment is accepted for every supported mode or an official release exposes a
sufficiently bounded/fallible lifecycle.

## Verification

| Check | Result |
|---|---:|
| Current non-yanked crates.io releases for `tidesdb`, `fjall`, `redb`, and `rocksdb` | verified against primary release APIs |
| Exact RocksDB crate/sys/engine identities and checksums | matched the prior immutable source closure |
| Released Rust APIs for timed `wait_for_compact` and `cancel_all_background_work` | source-verified |
| Released Rust Drop calling void C `rocksdb_close` | source-verified |
| `librocksdb-sys` system-library override and `ROCKSDB_COMPILE=1` behavior | source-verified; hard provenance gate recorded |
| TidesDB 0.11.1 feature/source path, native 9.3.14, and open PR 664 status | independently reviewed |
| Fjall 3.1.8 and redb 4.1.0 dispositions | independently reviewed against exact source and empirical reports |
| Three independent reviews after corrections | no remaining substantive blocker |
| Changed-document relative links | passed |
| GitHub workflow YAML parse and `ci-success.needs` integrity | passed, 11 required jobs |
| Stopped redb required-CI job/references | removed |
| Root/runtime manifests and lockfile | no RocksDB runtime dependency |
| `git diff --check` | passed; repository LF/CRLF conversion warnings only |
| Runtime compile/unit/soak tests | not run; no runtime code or dependency changed, and soak/qualification remain paused |

## End-of-cycle review

- **AI slop and overengineering:** pass for the decision slice. One veto matrix replaces the live
  multi-engine queue. No scoring formula, generic backend selector, native telemetry fork, adapter,
  execution workflow, or automatic fallback was added. The obsolete redb construction job no
  longer consumes required CI.
- **Hot path and latency:** unchanged. No row-path code, FFI, allocation, lock, task, cache, or I/O
  changed. The selected design still requires Arrow-batch coalescing and bounded owner lanes off
  compute/event-loop threads; none of that is performance evidence.
- **Unused code and maintainability:** partial pass. Current authority is now consistent across the
  ADR and plans, and stale Fjall-specific generic wording was removed. The remaining redb
  qualification schemas/code and oversized protocol are explicit `DKS-CLEANUP-001` debt and must
  be removed before an adapter dependency lands; only a report-owned minimal reproducer may remain.
  ADR-008 is still too large and should be compacted during the final documentation cleanup, not
  expanded with another backend survey.
- **Production readiness:** **BLOCK**. The 10.4.2-to-current release delta, exact bundled linkage,
  all-mode adapter semantics, vnode cleanup/reclamation, I/O/full-disk behavior, in-process
  lifecycle decision, memory/disk/FD governance, background health, hot/cold tail latency,
  checkpoint/rebalance integration, delivery matrices, failover/ALO/EO regressions, qualification,
  code cleanup, and independent immutable-RC soak all remain open.
- **Documentation:** pass after reconciliation. The selection report owns the full matrix and entry
  contract; the ADR and active plans carry outcome/order, while dated TidesDB/Fjall/redb reports
  remain evidence rather than active preference. Frozen v4 preference metadata remains unchanged
  and explicitly has no current selection authority.
- **Tests:** appropriate for a documentation/workflow-only cycle. Structural checks passed. No
  runtime, qualification, A/B, observer, certification, failover, delivery, or soak result is
  claimed from older binaries.

## Next-cycle review plan

The next backend-specific cycle is capped at one engineer-day and zero soak/qualification
machine-hours. Review it in this order:

1. **First-four-hour stop gate:** audit material correctness, security, lifecycle, and resource
   fixes from bundled RocksDB 10.4.2 through current native 11.1.2. Stop before code if the released
   Rust path cannot consume a required fix.
2. **Provenance gate:** pin exact crate/sys versions and checksums, set `ROCKSDB_COMPILE=1`, reject
   relevant `*_LIB_DIR` overrides, freeze features/options, and prove the linked engine identity.
3. **Smallest-code gate:** permit only one private, single-owner adapter/conformance slice shared by
   embedded, single-node, and cluster-with-admission-closed modes. Reject a generic backend
   framework, public compatibility layer, per-row FFI, or backend types outside the facade.
4. **Correctness/lifecycle gate:** cover atomic two-table batches, snapshot export, fresh-root
   restore, vnode-prefix delete with held-snapshot semantics and quota-visible reclamation,
   foreground I/O/full-disk errors, and child-process Drop/termination/quarantine. Do not call
   process containment bounded in-process shutdown.
5. **Human-maintainability gate:** run formatting, warnings-denied Clippy, focused tests, dependency
   and public/dead-code scans; remove the obsolete redb qualification surface before accepting a
   RocksDB runtime dependency.
6. **Decision gate:** a pass may request separate qualification authority. It cannot resume soak,
   authorize a candidate run, relax admission, or claim production readiness.
