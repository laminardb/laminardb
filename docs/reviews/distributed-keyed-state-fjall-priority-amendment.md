# Distributed keyed state — Fjall 3.1.8 priority amendment review

- **Date:** 2026-07-28
- **Decision commit:** `b914cc35`
- **Reviewed decision head:** `b914cc35`
- **Scope:** decide whether stock official Fjall 3.1.8 should replace the stopped TidesDB work order
  as the next worker-local managed-state qualification subject
- **Decision verdict:** **PREFERRED FOR BOUNDED QUALIFICATION-ENTRY RECHECK**
- **Runtime/backend verdict:** **NO BACKEND SELECTED**
- **Production/admission verdict:** **BLOCK**; `[LDB-4007]` and `[LDB-0013]` remain unchanged

## Outcome

Stock official [`fjall` 3.1.8](https://github.com/fjall-rs/fjall/releases/tag/3.1.8), tag commit
`6debe706dbc53d6d0eb666aae5057671d5c1370f`, is the sole preferred worker-local qualification-entry
subject. It is Rust-native and has the primitive shapes needed by the proposed managed-state
facade: atomic batches, logically consistent snapshots, point access, and ordered prefix/range
iteration. It avoids a C++ storage engine and fits the project's Rust toolchain.

That is not an implementation selection or qualification result. The exact 3.1.8 source is the same
subject previously audited. Its deprecated global write-buffer configuration is not enforced,
journal sizing is a soft maintenance trigger rather than a hard disk/WAL cap, writes can
synchronously stall behind shared journal/flush work, and stable public maintenance, stall, and
background-error facts remain incomplete. Native multi-get, prefix/range delete, physical
checkpoint, and backup are absent. Concurrent visibility, crash behavior, cleanup cost, recovery
compatibility, and production tail latency have not been run.

Laminar may directly measure its own byte reservations, request queue, call duration/outcome,
sticky adapter error, process/cgroup state, and filesystem limits. Those observations cannot be
relabeled as invisible Fjall compaction/flush debt, scheduler progress, or unreported background
errors. Fjall's internal workers remain engine-owned and un-cancellable. Only a Laminar-owned
in-memory cache hit may execute inline; every Fjall API call would use a bounded foreground blocking
lane. Local Fjall files remain disposable and never become Laminar's checkpoint, vnode ownership,
rebalance, source/sink delivery, or exactly-once authority.

The next backend action is capped at one engineer-day and zero candidate machine-hours: recheck the
stock public/source contract for truthful minimum pressure, progress, failure, and fail-stop facts.
The first required internal fact unavailable without a fork stops Fjall. Only a complete pass may
fund a separately reviewed minimal adapter and later uniform/Zipf aggregate, timer/window, complete
join-family, checkpoint/restore, rebalance, crash/corruption/disk-pressure, N/N-1, resource, and
p99/p99.9/max qualification on target Linux/NVMe.

## Decision matrix

| Gate | Evidence | Result |
|---|---|---:|
| Exact release identity | Official 3.1.8 release and tag commit fixed; later build must also pin the exact `lsm-tree` resolution | **PASS** |
| Rust/toolchain fit | Rust-native engine; Fjall's documented Rust 1.90 floor is below Laminar's Rust 1.95 workspace floor and the reviewed local 1.96 toolchain | **PASS** |
| Required KV primitive shapes | Atomic batches, database snapshots, point access, ordered prefix/range iteration, explicit persistence modes | **PRESENT; BEHAVIOR UNQUALIFIED** |
| Global write-buffer bound | Deprecated builder value has no enforcement reader in exact 3.1.8 source | **FAIL AS A HARD BOUND** |
| Journal/disk bound | Journal-size option triggers maintenance; it is not a hard capacity ceiling | **FAIL AS A HARD BOUND** |
| Maintenance/error health | Existing Fjall v2 mapping is `OBSERVED_DESIGN_UNSUPPORTED_IN_STOCK_SOURCE`; Laminar observations cover only direct facts | **OPEN / ENTRY VETO** |
| Foreground hot path | Calls can block; journal serialization, flush interaction, cache misses, scans, and cleanup need actual batched workloads | **NOT RUN** |
| p99/p99.9/max and Zipf | Historical warning data is non-qualifying; exact managed aggregate/window/join profile has not run | **NOT RUN** |
| Crash/corruption/resource robustness | Fresh-root recovery, ambiguous outcomes, `kill -9`, corruption, `ENOSPC`, memory/disk/FD limits, and N/N-1 remain unproved | **NOT RUN** |
| Distributed checkpoint/rebalance/EO | Supplied by Laminar lifecycle, portable `StateBackend` artifacts, fencing, and connector protocols—not Fjall | **NOT A FJALL CAPABILITY** |
| Dependency, adapter, candidate execution | Workspace and lock remain Fjall/TidesDB-free; no adapter or candidate run landed | **NONE** |
| Production and cluster admission | Independent immutable-release-candidate soak and all earlier gates remain outstanding | **NO-GO** |

## Verification

| Check | Result |
|---|---:|
| Exact official release/tag/source review | passed |
| Three independent contract/correctness/maintainability re-reviews | passed after corrections |
| `git diff --check` | passed |
| Relative links in all 16 amended Markdown files | passed |
| `cargo fmt --all -- --check` | passed |
| Root `Cargo.toml` / `Cargo.lock` backend-dependency diff | none |
| Frozen runner v2 bytes and diff | unchanged; 66,870 bytes, SHA-256 `661102f9dcb46934f966f25cc91934ea0d1fa6a487323fb383cbabd0a5e166e3` |
| Rust unit/integration tests | not run; documentation/source-only decision with no Rust change |
| Candidate benchmark, fault run, soak, or certification | not run and not authorized |

## End-of-cycle review

- **AI slop:** pass after independent review removed an invented entry verdict, selection overclaims,
  a false claim that Laminar could own Fjall's internal maintenance, and watchdog substitution for
  unobservable engine facts. Current language distinguishes primitive presence from behavior and
  production evidence.
- **Overengineering and hot path:** pass for this amendment. No dependency, adapter, generic backend
  framework, runner, profile, observer, or soak tooling was added. The next decision is a short
  source-only stop/go check. Future calls remain batch-coalesced and off compute/event-loop threads;
  all latency claims remain open.
- **Unused code and maintainability:** pass. No code was added. Current authority is concentrated in
  ADR-008 and the two plans; dated TidesDB and Fjall reports are explicitly historical evidence.
  Their source identities, rejected mechanisms, and inbound audit links remain relevant, so none
  was deleted. `docs/research` is already empty.
- **Production readiness:** **BLOCK**. Hard resource containment, truthful minimum health, latency,
  fault recovery, all-mode and complete join-family conformance, existing cluster failover/ALO/EO-
  eligible regressions, final cleanup, and the independent immutable-RC soak all remain mandatory.
- **Documentation:** pass. Frozen v4 bytes and historical verdicts were not relabeled; stale active
  TidesDB work orders are marked superseded. This review is the sole new cycle-history document.
- **Tests:** appropriate for a documentation/source-only amendment. Formatting, link, dependency,
  immutable-file, and diff checks passed. No candidate or production result is claimed.

## Next action

Run only the bounded stock-Fjall source/contract closure described above. If it fails, record the
single decisive unsupported fact and return backend choice to an explicit owner decision without
building Fjall, surveying more engines, or activating a fallback. If it passes, seek separate scope
for the smallest all-mode adapter/conformance vertical while keeping cluster stateful admission
closed.
