# Distributed keyed state Cycle 13 review

- **Date:** 2026-07-23
- **Branch:** `feature/distributed-keyed-state-adr`
- **Synthetic integration verdict:** **GO** after separate agent review and correction
- **Candidate execution verdict:** **BLOCK** on DKS-Q2-001 through DKS-Q2-008 and named-owner approval
- **Backend selection verdict:** **BLOCK** additionally on DKS-Q2-009/C3
- **Cluster admission verdict:** unchanged and fail-closed under `[LDB-4007]`
- **Production verdict:** **NO-GO**; there is no distributed-state implementation, candidate run,
  native fault/endurance result, or independent production-soak result

## Outcome

Cycle 13 integrated previously isolated qualification parsers without creating a candidate runner:

- `4e5af260` changes common-resource and mechanism observations from whole-artifact slices to
  exact-length streaming readers with bounded record memory and population digests;
- `e03b17ab` adds `validate-mechanism-bundle`, its strictly synthetic/ineligible input schema,
  detached literal artifact corpus, adversarial tests, fixed 64 KiB file buffering, and a
  non-gating real-CLI measurement harness;
- `f6c30bb6` moves pre-existing Zipf literals into an explicitly non-independent, ineligible
  detached corpus and adds required Windows x86_64 and native Linux arm64 CI jobs; and
- `f41bbe82` removes the last unused stall-validator argument identified by the final separate-agent
  unused-code pass.

No LaminarDB runtime crate, admission rule, backend adapter, candidate dependency, or execution
command changed. No Fjall, RocksDB, or redb workload ran. Source/sink delivery, exactly-once,
checkpoint/rebalance, and independent-soak claims are unchanged.

## Material decisions

### Synthetic mechanism integration, not an approved runner

`state-backend-mechanism-bundle-validation-input/v1` is structurally fixed to
`synthetic_fixture`, `fixture_ineligible=true`, `qualification_eligible=false`, and
`validation_authorizes_execution=false`. It content-addresses exact profile, mapping, common
samples/cuts, conditionally applicable debt/stall artifacts, and target-device summary. Every
binary artifact is capped at 256 MiB, parsed with a maximum 160-byte record and fixed 64 KiB buffer,
and hashed in the same pass.

The integration checks mapping-arm presence, raw hashes/lengths, nominal and cut populations,
canonical debt tail tags, claimed measurement/write-stop/last-terminal chronology, origin-offset
overflow, exact stall censoring at measurement end, device identity/windows, and trace anomalies.
It distinguishes malformed/invalid observation evidence from non-authoritative adverse candidate
signals. Tail deadline, stable-tail duration above the profile bound, debt/stall/device threshold
excess, device errors, and incomplete device requests are signals; none is an attempt verdict.

The validator does not derive write stop or last terminal from a workload plan, attest the claimed
clock source, inspect source-proof contents, classify the environment, apply complete attempt
precedence, or approve execution. It accepts only regular final entries in a trusted, quiescent
local fixture directory. Length and SHA-256 bind consumed content, but the current precheck/open
sequence is not a hostile-directory security API; a future approved validator requires no-follow,
handle-relative opens and opened-handle identity checks.

DKS-Q2-005/006 remain open. The command exists to eliminate unused parser code and expose
cross-wire errors before a real approval design, not to stand in for that design.

### Tooling hot path and measurement

The production record path remains untouched. Within the offline validator, the original direct
small-read file path was replaced by a fixed 64 KiB `BufReader`; raw SHA-256 is updated only for
bytes returned to the parser. The parser retains one record, and cap/cap-plus-one behavior is
tested without checking in a 256 MiB blob.

The ignored release harness generated 400,000 common-resource records and invoked the real child
CLI five times over 64,011,844 artifact bytes. It excludes fixture generation from the timed
region and has no pass threshold:

| Host | Toolchain | Median | Throughput | Memory observation |
|---|---|---:|---:|---:|
| Windows x86_64 | Rust 1.95 release | 79 ms | 769.06 MiB/s | sampled child peak 9,580,544 bytes |
| Docker Desktop/WSL Linux x86_64 | Rust 1.95 release | 51 ms | 1,193.79 MiB/s | `/usr/bin/time -v` MaxRSS 7,740 KiB |

These are non-gating validator-tool observations on one development machine. They are not backend,
candidate, target-host, low-latency product, or qualification evidence. Shared-runner thresholds
remain unjustified.

### DKS-Q2-001 remains open

The detached Zipf corpus has SHA-256
`dd6c569cfef0a82627e280b4a0072b9a898f5467dc1ab07683c5ffeaf1c97c32` and states that it was
transcribed from the pre-existing Cycle 8 tests. Moving literals out of the implementation module
reduces coupling, but does not make their producer independent.

Windows and Linux x86_64 Rust 1.95 debug/release checks pass. Required CI now uses GitHub's
documented [`ubuntu-24.04-arm` hosted-runner
label](https://docs.github.com/en/actions/reference/runners/github-hosted-runners) for native Linux
arm64 debug/release checks and adds Windows release checks. The arm64 CI job has not run on this
branch, and a local emulated arm64 compile exceeded five minutes without a result; neither is
counted as target conformance.

DKS-Q2-001 still requires an independently implemented MPFR/interval audit and thresholds,
analytical retry-probability proof, sampler/null interference evidence, Z1-Z8 closure, exact
workload/case assignment, licensing/SBOM completion, and named workload/operations-owner approval
of bounded retry versus a new total sampler.

### Backend investment boundary

The [pinned-source static-audit disposition](../reports/state-backend-static-audit-2026-07-23.md)
does not change:

- unmodified Fjall 3.1.8 **fails DKS-Q2-006** because the applicable stable maintenance-debt and
  pressure-stall observations are absent. Do not build its adapter unless the storage/performance
  owner funds an exact-source patch/upstream path for those signals and enforceable global limits;
  otherwise remove it from the future campaign;
- the current RocksDB 10.4.2 binding remains **blocked**, not selected by elimination. Do not build
  its adapter until a source audit or patch proves complete database/write-buffer-manager-scope
  stall coverage and native memory/resource accounting; and
- redb 4.1.0 remains **deferred**. Its schemas do not supply the missing semantic verifier, owners,
  harness, approved native probe, additive profile, or mapping. No prescreen was authorized.

Cycle 13 therefore spends no adapter implementation budget. The next material step is a named-owner
investment/drop decision, not another speculative backend abstraction.

## Six-pass cycle review

### 1. AI slop and contract consistency

**Result: pass after correction.** Two separate agent review passes found that the first patch
silently clipped future stall endpoints, used clock-incoherent cuts, omitted artifact-origin
overflow checks, allowed incomplete cut chronology, and could open a pre-existing special file
before rejecting it. The implementation and negative tests now reject each case. Exact tail
`== gate` passes and `gate + 1 ns` signals. No generated research prose or same-code fixture is
presented as independent evidence.

### 2. Overengineering and hot path

**Result: pass with a stop condition.** The work consumes existing wires through one bounded CLI;
it does not introduce a runner framework, PKI, candidate plug-in system, or production trait. Fixed
buffering avoids per-record file syscalls, and the detached hex corpus stays reviewable without
large binary blobs. Further synthetic schema growth is stopped until an approved-plan design or
independent numerical oracle consumes it.

### 3. Unused code and dependencies

**Result: pass for this scope.** The mechanism readers now have a real CLI consumer and detached
integration tests. The Zipf corpus replaces in-module literals and slightly reduces test-code
duplication. No Cargo manifest or lockfile changed; runtime, Fjall, RocksDB, redb, Arrow, and
DataFusion dependencies remain absent. The redb schemas still lack a verifier/harness and remain
explicitly non-decisional.

### 4. Production readiness, delivery, and soak

**Result: NO-GO, correctly fail-closed.** There is still no vnode ownership, keyed state store,
checkpoint artifact execution, rebalance transfer, source-offset fence, sink transaction protocol,
connector capability negotiation, or end-to-end exactly-once implementation. No native physical
fault, cache-loss, 24/72-hour endurance, or independent black-box production soak ran. Docker/WSL
tool checks cannot substitute for any of them.

### 5. Documentation, stale research, and overdocumentation

**Result: pass.** Runner, Zipf, and Phase 0 documents state the exact synthetic boundary and stale
redb “schemas absent” wording is corrected. No `docs/research` corpus or `.claude` configuration is
tracked on this branch; the old tracked research/handoff material had already been removed before
this feature branch. Ignored local configuration is outside the project evidence set. Current
validation, ADR, source-audit, and historical cycle-review documents remain relevant, so no
additional project document is deleted. This review consolidates results rather than adding another
design surface.

### 6. Tests, CI, and empirical limits

**Result: targeted pass; native arm64 CI and production evidence pending.** Windows and pinned
Linux x86_64 Rust 1.95 pass format, all-target/all-feature Clippy with warnings denied, and 128 tests
with one intentionally ignored benchmark. Debug and release Zipf feature runs each pass 111 library
tests on both x86_64 hosts. The CI YAML parses, all-feature lint/test coverage is required, and the
new Windows and native arm64 jobs participate in `ci-success`. GitHub CI itself has not run here.

The broad LaminarDB workspace matrix was not rerun because the commits are isolated tooling/docs
and the Windows host previously exhausted its paging file on that matrix. Provisioned CI owns that
coverage. No result here is candidate or production evidence.

## Cycle 14 implementation and review plan

1. Run the required branch CI and resolve—not waive—native arm64 or Windows failures. Record the
   exact runner image/toolchain result; configured jobs alone do not close platform conformance.
2. Obtain named workload/operations-owner decisions for the Zipf bounded-retry policy and exact
   case assignment. If approved, build an independently implemented numerical/error corpus and
   retry proof; otherwise choose a new sampler identity. Do not call the transcribed corpus
   independent.
3. Obtain the storage/performance-owner decision to fund or drop Fjall telemetry and to fund or
   stop the RocksDB complete-stall-source audit. Keep redb deferred without its verifier, owners,
   and harness; do not start adapters by default.
4. Run the postponed non-authoritative M2 codec spike only if its exact filler, copy, preparation,
   ring-memory, and null-control measurements can be reported together. Do not freeze a workload
   encoding or DKS-Q2-004/005 schedule from partial throughput numbers.
5. Design the future approved-plan/attempt validator only after DKS-Q2-001 through DKS-Q2-005 have
   executable values and source-proof ownership. Replace the trusted-directory assumption with
   race-free no-follow opens before it accepts real evidence.
6. End the cycle with separate-agent AI-slop/consistency, overengineering/hot-path, unused-code,
   production/delivery/soak, documentation/stale-research, and tests/CI review passes. Keep
   `[LDB-4007]` and the production NO-GO until the separate team executes and reviews the immutable
   release-artifact soak charter.
