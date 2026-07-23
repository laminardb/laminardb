# Distributed keyed state Cycle 14 review

- **Date:** 2026-07-23
- **Branch:** `feature/distributed-keyed-state-adr`
- **Bounded oracle verdict:** **GO** only as an explicitly ineligible numerical prototype
- **Candidate execution verdict:** **BLOCK** on DKS-Q2-001 through DKS-Q2-008 and owner approval
- **Backend selection verdict:** **BLOCK** additionally on DKS-Q2-009/C3
- **Cluster admission verdict:** unchanged and fail-closed under `[LDB-4007]`
- **Production verdict:** **NO-GO**; no distributed-state implementation or independent soak exists

## Outcome

Cycle 14 took one policy-neutral part of DKS-Q2-001 and stopped before candidate or backend work:

- `55084099` adds a standalone CPython/gmpy2/MPFR ideal-math interval oracle whose output is
  explicitly ineligible and cannot execute or inspect a backend;
- `18b024bc` configures its hash-pinned tests on Linux x86_64, Windows x86_64, and native Linux
  arm64 and makes the Linux job part of `ci-success`;
- `069478ff` records the prototype's exact evidence boundary in the Zipf ADR, runner ADR, and Phase
  0 plan; and
- `045fd54b` corrects stale redb wording: strict schemas and fixtures exist, while the external
  verifier, harness, approval, redb-specific mapping, and evidence do not.

No LaminarDB runtime crate, admission rule, state backend, connector, candidate dependency,
execution command, or production hot path changed. No Fjall, RocksDB, redb, or SurrealKV workload
ran. `[LDB-4007]`, source/sink delivery, exactly-once, checkpoint/rebalance, and independent-soak
claims are unchanged.

## Material decisions

### DKS-Q2-001 gained a decision input, not qualification evidence

`tools/state-backend-zipf-oracle` evaluates the ideal `q = 99/100` rejection-inversion formulas
without importing the Rust candidate or its detached literal corpus. It builds an explicit MPFR
context, propagates directed intervals, retries ambiguous setup and proposal decisions at 256, 512,
1,024, 2,048, and 4,096 bits, and fails unresolved rather than selecting an endpoint. The bounded
search covers 92 domains and fixed 53-bit grid controls.

The canonical observation contains 471 NDJSON records, is 865,397 bytes, and has SHA-256
`8ad14317bdb1f12d67b9f823bea0759d33034e4c01164c2dbac90ad870f2474b`. Every detachable record says
`NOT QUALIFICATION EVIDENCE`, `qualification_eligible=false`,
`validation_authorizes_execution=false`, and `independently_reviewed=false`. Header fields state
that candidate results, distribution-error thresholds, rejection mass, retry probability, and
dependency-installation attestation were not computed.

Two separate-agent internal code reviews gave GO for this bounded prototype after the implementation
stopped inheriting ambient MPFR context, escalated setup uncertainty and ambiguous pre-clamp floors,
proved the symbolic `u=0` containment, rejected CLI arguments, and repeated evidence flags on every
line. Those reviews did not approve the sampler or qualification. Exact-equality and
one-interval-step tests around both acceptance inequalities remain a promotion requirement.

DKS-Q2-001, Z4, and Z5 remain open. Promotion still requires an actual dependency installation
receipt and complete license/SBOM record, a detached candidate-output comparator, approved
head/CDF/tail/total-variation and finite-grid rejection metrics, a retry proof or new total-sampler
identity, native-arm execution, workload/case assignment, sampler/null interference tests, and
independently operated workload/operations-owner review.

The dependency decisions use the official [gmpy2 2.3.1
release](https://pypi.org/project/gmpy2/2.3.1/), [gmpy2 context
contract](https://gmpy2.readthedocs.io/en/latest/contexts.html), and [MPFR 4.2.2
release](https://www.mpfr.org/mpfr-current/). Exact permitted CPython 3.13 wheel hashes are checked
into `requirements.txt`; no third-party wheel or source is vendored.

### Cross-platform CI is configured, not observed

The initial CI patch contained invalid YAML because the unquoted `:all:` pip option appeared in a
plain scalar. Separate review caught it before commit; block scalars fixed all three steps and
actionlint 1.7.12 then accepted the workflow. The Linux job verifies the CLI notice, byte count,
and hash. Windows and arm64 reuse the existing qualification jobs, whose oracle tests enforce the
same canonical output and evidence flags. The arm job explicitly requests `architecture: arm64` on
GitHub's documented [`ubuntu-24.04-arm`
runner](https://docs.github.com/en/actions/reference/runners/github-hosted-runners).

The branch has no upstream and was not pushed. Therefore GitHub CI, hosted Windows, and native
Linux arm64 have not run for these commits. Docker Desktop/WSL Linux x86_64 is useful local
cross-runtime smoke, but is neither native-arm nor target-host evidence. A push is an external write
and requires explicit authorization; configured jobs cannot be reported as passing jobs.

### DKS-Q2-006 is an observability and control contract, not an LSM API check

A candidate must expose or externally prove every applicable source of maintenance debt, pressure
stall, compaction/device I/O, native memory, retained snapshots, and applied configuration, with
cheap gate-bearing observations and enforceable global limits. Candidate-specific mechanisms may
be `not_applicable` only with exact source and configuration proof; unsupported signals cannot be
encoded as zero. Common XFS quota, cgroup dirty/writeback/device I/O, process memory, lifecycle,
pressure, and target-device traces are also required.

The pinned-source disposition remains:

- Fjall 3.1.8 **FAILS DKS-Q2-006 as published**. Do not fund an adapter unless the
  storage/performance owner funds a reviewed patch or upstream route for stable debt, stall,
  compaction-I/O, cache/pinned-memory, applied-option, and snapshot-retention observations plus
  adapter-owned reservation/governor controls.
- RocksDB 10.4.2 through wrapper 0.24.0 remains **BLOCKED**. Its available stall ticker does not
  prove complete write-buffer-manager/database-scope coverage, and native memory, synchronous FFI,
  shared write control, checkpoint flush, and SST-ingest pauses still need external accounting and
  hot-tail evidence.
- redb 4.1.0 remains **DEFERRED**. No Docker smoke or native prescreen is authorized without named
  workload/operations approval, an external semantic/attestation verifier, and a fail-closed
  harness. A later native Linux/XFS/NVMe prescreen must cover global-writer victim tails, both
  commit paths, quick repair, crash atomicity/reopen, and source-backed N/A arms. Even a pass only
  funds an additive redb-specific profile/mapping and adapter review.
- SurrealKV 0.21.2 remains **REJECTED unmodified**. Reconsideration requires an explicitly funded
  pinned correctness fork or upstream fixes, then forced-compaction/liveness screening before
  telemetry investment.

No adapter, dependency pin, prescreen run, or new backend abstraction is justified this cycle.

### Delivery and production constraints remain separate from the local store

A local backend cannot produce end-to-end exactly-once: it cannot seal source offsets, fence old
owners, durably coordinate a checkpoint decision, or commit a sink transaction. The initial scope
in ADR-008 remains certified at-least-once with a splittable non-ephemeral source and a
`DurableAtLeastOnce + MultiWriter` sink. Exactly-once later requires connector capability
negotiation plus checkpoint composition: replayable/fenced source positions, vnode-epoch ownership,
state and timer snapshots, a durable coordinator decision, and a transactional or explicitly
idempotent sink. Unsupported source/sink pairs must be rejected or receive an explicit weaker
delivery contract; they cannot be silently promoted.

The missing production capability is still the complete vnode-keyed lifecycle: deterministic key
routing and state placement; state/timer schemas; barrier and unaligned-checkpoint behavior under
backpressure; atomic source-offset/state/sink coordination; restore and replay; epoch fencing;
incremental snapshot retention; rebalance state transfer, activation, rollback, and cleanup; and
operator-specific aggregate/window/join state. All of it must meet bounded CPU, allocation, lock,
I/O-amplification, and tail-latency budgets. Backend selection alone closes none of these items.

## Six-pass cycle review

### 1. AI slop and contract consistency

**Result: pass after correction.** Separate-agent internal oracle reviews found concrete context,
escalation, clamp, containment, CLI, and per-record evidence-boundary defects; each was fixed and
covered by a negative test. Separate-agent internal CI review caught invalid YAML before commit.
The documents distinguish same-tool stability, implementation isolation, independent operation,
candidate comparison, and qualification evidence instead of collapsing them into “independent
oracle.”

### 2. Overengineering and hot path

**Result: bounded pass with a stop condition.** The new dependency lives in one unpublished,
standalone tool and never enters a runtime manifest. Its caps are fixed at 92 domains, 1,024
records, 1 MiB output, 65,536 search steps, and 4,096-bit precision. It also has a cooperative
60-second generation deadline; that deadline is not a hard process/runtime cap. No runner plug-in,
backend trait, generic interval package, artifact upload, or production telemetry path was added.
The tool is substantial because outward-rounded branch decisions are the error surface being
examined; further oracle/framework work stops until owners approve metrics and sampler policy.
Production hot-path latency is untouched and unmeasured.

### 3. Unused code and dependencies

**Result: pass for this scope.** The CLI is exercised by tests and CI; the generated observation is
not checked in. The only new dependency is hash-pinned gmpy2 in the isolated Python tool. Runtime,
Fjall, RocksDB, redb, SurrealKV, Arrow, and DataFusion manifests remain unchanged. No candidate
adapter or unused generalized interface was added.

### 4. Production readiness, delivery, and soak

**Result: NO-GO, correctly fail-closed.** There is no vnode ownership service, distributed keyed
store, checkpoint execution, rebalance transfer, epoch fence, source-offset protocol, sink commit
protocol, connector certification, or end-to-end delivery implementation. No native physical-fault,
cache-loss, N/N-1 recovery, 24/72-hour endurance, or independently operated black-box release soak
ran. `[LDB-4007]` must remain unchanged. Docker/WSL and same-team tests cannot substitute for the
independent soak required before a production-ready claim.

### 5. Documentation, stale research, and overdocumentation

**Result: pass after one wording correction.** The existing ADR and plan were amended rather than
forked into another design. The static audit's stale statement that redb still lacked strict schemas
was corrected; it now names the verifier, harness, approval, profile/mapping, and evidence actually
missing. No `docs/research`, `.claude`, or `CLAUDE.md` corpus is tracked, and the ignored local
`docs/research` junction points outside this repository. Current validation reports, ADRs, pinned
source audits, and historical cycle reviews remain relevant, so no document is removed.

### 6. Tests, CI, and empirical limits

**Result: targeted pass; hosted and production evidence pending.** CPython 3.13.1 with gmpy2 2.3.1,
MPFR 4.2.2, GMP 6.3.0, and MPC 1.4.0 passes all 14 oracle tests on Windows x86_64 and the pinned
`python:3.13.1-slim-bookworm` Linux x86_64 image. Both produce exactly 865,397 bytes and the
expected SHA-256. The state qualification tool passes 128 debug all-target/all-feature tests with
one intentional ignored benchmark and 111 release Zipf-feature tests. `git diff --check` and
actionlint 1.7.12 pass.

The broad LaminarDB workspace matrix was not rerun because no runtime code or manifest changed and
the Windows host has previously exhausted its paging file on that matrix. Provisioned CI owns that
coverage. GitHub Actions, hosted Windows, native arm64, target Linux/XFS/NVMe, backend candidate,
fault/endurance, and independent-soak results remain absent and cannot be inferred from local smoke.

## Cycle 15 entry and review plan

Do not start another speculative framework. Cycle 15 should begin only when at least one material
entry condition is authorized:

1. With explicit permission to push, run the required branch CI and resolve rather than waive any
   hosted Windows or native-arm failure. Record exact run, runner image, Python, dependency, Rust,
   byte-count, and hash identities; do not call the result qualification evidence.
2. With workload/operations-owner approval of the sampler and exact case assignment, add the
   equality/one-step oracle tests, artifact installation receipt/SBOM, detached candidate
   comparator, approved finite-grid distribution metrics, and analytical retry proof. Otherwise
   reserve a new total-sampler identity rather than mutating the provisional one.
3. With a storage/performance-owner investment decision, fund either the complete Fjall telemetry
   patch/upstream path or the complete RocksDB stall/native-resource audit. Keep redb deferred and
   SurrealKV rejected unless their explicit prerequisites are funded; do not select by elimination.
4. Freeze DKS-Q2-002 through DKS-Q2-005 values and ownership before implementing an approved-plan or
   attempt runner. Then close DKS-Q2-006 through DKS-Q2-009 with exact candidate, fault, endurance,
   and shared-database concurrency evidence before backend selection.
5. Preserve the production sequence in ADR-008: certified at-least-once scope first, then optional
   exactly-once only for capability-compatible sources and sinks; checkpoint/rebalance state
   machines and hot-path budgets precede cluster admission.
6. Before any production-ready claim, hand an immutable release artifact and frozen charter to a
   separate team for the independent black-box soak. The implementation team must not operate or
   reinterpret that result.
7. End the cycle with separate-agent AI-slop/consistency, overengineering/hot-path, unused-code,
   production/delivery/soak, documentation/stale-research, and tests/CI passes. Keep `[LDB-4007]`
   and the production NO-GO until all applicable gates and the independent soak pass.
