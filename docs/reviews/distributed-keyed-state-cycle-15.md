# Distributed keyed state Cycle 15 review

- **Date:** 2026-07-23
- **Branch:** `feature/distributed-keyed-state-adr`
- **Hosted-CI tested head:** `1cc095bc871012e874001abff14187a373a5102b`
- **Hosted-CI verdict:** **GO** for configured-platform execution only
- **Candidate execution verdict:** **BLOCK** on DKS-Q2-001 through DKS-Q2-008 and owner approval
- **Backend selection verdict:** **BLOCK** additionally on DKS-Q2-009/C3
- **Cluster admission verdict:** unchanged and fail-closed under `[LDB-4007]`
- **Production verdict:** **NO-GO**; no distributed-state implementation or independent soak exists

## Outcome

Cycle 15 published the existing feature branch solely to execute its configured GitHub Actions
matrix. It did not open or merge a pull request. [Run
30047503740](https://github.com/laminardb/laminardb/actions/runs/30047503740) exercised commit
`1cc095bc` on hosted Linux x86_64, Windows x86_64, and native Linux arm64.

The three targeted ineligible state/Zipf jobs passed on the first attempt. The first overall attempt
failed in one unchanged broad Windows coordinated-recovery test. After code comparison, prior-CI
comparison, state-transition analysis, and 1,250 local stress runs, the failed job and dependent
aggregate gate were rerun once. The complete Windows suite and aggregate `CI Success` then passed.
No sleep, retry loop, weaker assertion, production-guard change, or runtime patch was introduced.

Commit `35946cd4` records the hosted result and its limits. It is a documentation-only closeout
commit after the tested head; it was deliberately not represented as part of run 30047503740. No
LaminarDB runtime crate, admission rule, connector, state backend, candidate dependency, production
hot path, or qualification result changed in this cycle.

## Hosted evidence

| Path | Observed environment | Result | Limit |
|---|---|---|---|
| Linux x86_64 oracle | Ubuntu 24.04.4, runner 2.336.0, image `20260720.247.2`, CPython 3.13.1, gmpy2 2.3.1 | 14 tests passed; CLI emitted exactly 865,397 bytes with SHA-256 `8ad14317bdb1f12d67b9f823bea0759d33034e4c01164c2dbac90ad870f2474b` | Same-tool canonical stability, not candidate conformance |
| Native Linux arm64 | `ubuntu-24.04-arm`, runner 2.336.0, image `20260719.67.1`, CPython 3.13.1, native aarch64 gmpy2 2.3.1 wheel | 111 standalone-validator library tests with `zipf-feasibility` enabled passed in debug and release; 14 oracle tests passed | Hosted arm feasibility, not target-host evidence |
| Hosted Windows x86_64 state job | Windows Server 2025 build 26100, runner 2.335.1, image `20260714.173.1`, CPython 3.13.1, gmpy2 2.3.1 | All-target/all-feature debug suite: 128 passed, one additional benchmark ignored; 111-test release library suite and 14 oracle tests passed | Hosted feasibility, not qualification or soak |

Every oracle record remains marked ineligible, non-authorizing, and not independently reviewed.
The hosted matrix closes only the prior “configured but unobserved” platform gap in DKS-Q2-001. It
does not close Z4 or Z5 and supplies no candidate comparator, approved distribution/error metric,
finite-grid rejection measurement, retry proof, observation-bound installation receipt, workload
assignment, or independent owner operation.

## Windows failure classification

Attempt 1 failed
`coordinated_recovery::tests::evidence_only_worker_consumes_tombstoned_release_after_stopped_quorum`
at `crates/laminar-db/src/coordinated_recovery/tests.rs:860`: the final pending-fault inventory was
not empty after 3,240 other tests had passed. The test and affected implementation were unchanged
from the main-line comparison, and the exact test had passed in two recent hosted Windows runs.

The failure is unresolved and cannot be classified as production-safe from this observation. A
state/code audit found a fail-closed path consistent with the symptom: an ownerless terminal
settlement that rejects an authority or continuity recheck retains fencing and requests a successor
fault rather than silently accepting the state. The failed assertion does not prove that path was
the cause, and existing CI output does not identify a rejected recheck; a narrower race diagnosis
would therefore be speculation.

A pre-existing local Windows test binary passed 250 sequential and 1,000 concurrency-16 runs. Its
SHA-256 is `8330213baee1ab67fc1d38c96daf5ab6084a3ffdc9c559a35019a94585a49848`. A current-source
integration-target build failed with Windows OS error 1455 when the host exhausted its paging file;
a reduced lib-only build then reached its five-minute time limit without a result. The stress is
therefore supporting diagnostic evidence only. On the single failed-job rerun, the exact test passed
in 0.055 seconds and the complete 5,772-test Windows suite passed with 19 skipped; aggregate `CI
Success` passed.

One clean rerun does not repair, erase, or soak the first failure. If it recurs, further blind reruns
stop. The next action is structured settlement-rejection instrumentation and outcome-specific
analysis; production guards and fail-closed behavior must not be weakened to satisfy the test.

## Production and backend boundary

The backend dispositions are unchanged:

- Fjall 3.1.8 **FAILS DKS-Q2-006 as published** because the required complete, cheap debt, stall,
  compaction-I/O, cache/pinned-memory, applied-option, snapshot-retention observations, and global
  controls are not available under the frozen contract.
- RocksDB 10.4.2 through wrapper 0.24.0 remains **BLOCKED** on database/shared-resource stall proof,
  native-memory accounting, synchronous-FFI hot-tail evidence, and global control composition.
- redb 4.1.0 remains **DEFERRED** until owners approve a fail-closed external verifier and isolated
  native Linux/XFS/NVMe prescreen with its redb-specific profile and mapping.
- SurrealKV 0.21.2 remains **REJECTED unmodified** pending correctness fixes or a funded pinned fork
  before observability work.

No local store by itself supplies vnode ownership, epoch fencing, checkpoint decisions, source
offset sealing, sink commit, restore/replay, state transfer, rollback, timers, or operator schemas.
Exactly-once still requires a compatible replayable/fenceable source, vnode-epoch state snapshot,
durable coordinator decision, and transactional or explicitly idempotent sink. Unsupported
source/sink combinations must remain rejected or explicitly receive a weaker delivery contract.

No latency, allocation, lock-contention, I/O-amplification, checkpoint-pause, recovery-time,
rebalance, delivery, backend, or physical-fault claim was tested here. `[LDB-4007]` must continue to
reject keyed aggregates, windowed aggregates, and stateful joins in cluster mode.

## Six-pass cycle review

### 1. AI slop and contract consistency

**Result: pass after evidence correction.** The documents distinguish configured execution,
same-tool stability, current-head CI, local diagnostic stress, qualification, target-host evidence,
and independent soak. The pre-existing binary and paging-file limitation are explicit. The first
failure is retained rather than rewritten as a pass.

### 2. Overengineering and hot path

**Result: pass.** This cycle used the already configured jobs and made no runtime change. No generic
CI evidence framework, automatic flake retry, backend abstraction, adapter, or hot-path telemetry was
added. Further oracle or runner work stops until the missing numerical policy and owners exist.

### 3. Unused code and dependencies

**Result: pass.** No code or dependency was added. The tested standalone tools and CI paths are
already referenced by the frozen qualification design. Runtime manifests remain unchanged.

### 4. Production readiness, delivery, and soak

**Result: NO-GO, correctly fail-closed.** Hosted CI is not a target-host qualification, fault test,
endurance run, or independent black-box soak. No immutable release candidate or frozen soak charter
was handed to an independent operator. Checkpoint/rebalance, hot-path budgets, source/sink delivery,
and exactly-once gates remain wholly open.

### 5. Documentation, stale research, and overdocumentation

**Result: pass.** Four existing documents were updated and this single cycle record added. No new
design fork was created. No `docs/research`, `.claude`, or `CLAUDE.md` corpus is tracked; the ignored
local `docs/research` junction is outside the repository. Existing validation, ADR, plan, pinned
source audit, and historical review documents remain relevant, so none was removed.

### 6. Tests and empirical limits

**Result: hosted matrix pass with one retained intermittent failure.** Attempt 2 ended green and
aggregate `CI Success` passed. Miri and Release Build were intentionally skipped by their workflow
conditions. The targeted Linux x86_64, native Linux arm64, and Windows x86_64 checks passed, but the
same implementation team initiated and interpreted them. No candidate backend, target
Linux/XFS/NVMe system, delivery failure, recovery campaign, or independent soak ran.

## Cycle 16 entry and review plan

Do not create another speculative framework. The next material work begins only when one of these
existing gates receives its named-owner input:

1. Workload and operations owners freeze the Zipf sampler/case assignment, approved comparison
   metrics, retry policy, dependency receipt/SBOM, and independent-operation procedure. Then add the
   equality/one-step tests and detached candidate comparator without changing the reserved identity.
2. Storage and performance owners fund either the complete Fjall telemetry/control patch or the
   complete RocksDB shared-stall/native-resource audit. redb remains a separate approved prescreen,
   not a backend selected by elimination.
3. DKS-Q2-002 through DKS-Q2-005 receive frozen matrices, sizes, rates, schedules, lifecycle values,
   and owners before candidate execution. DKS-Q2-006 through DKS-Q2-009 still require native fault,
   endurance, concurrency, and tail-latency evidence before selection.
4. If the Windows recovery outcome recurs, add structured rejection-reason diagnostics and test the
   classified state transitions. Do not add arbitrary sleeps, polling, or guard weakening.
5. Preserve the implementation order in ADR-008: vnode routing/epochs and state/timer schemas,
   checkpoint and restore, rebalance transfer/fencing, certified at-least-once connectors, then only
   capability-compatible exactly-once. Admission remains closed until the production gates pass.
6. Before any production-ready claim, an independent team must operate the immutable artifact under
   the frozen 24/72-hour black-box soak and fault charter and report the result without implementation
   team reinterpretation.
7. End Cycle 16 with the same six independent review passes: AI slop/contract, overengineering/hot
   path, unused code/dependencies, production/delivery/soak, documentation/stale research, and
   tests/CI.
