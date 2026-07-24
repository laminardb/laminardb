# Distributed keyed state Cycle 16 review

- **Date:** 2026-07-24
- **Branch:** `feature/distributed-keyed-state-adr`
- **Hosted-CI tested head:** `c26763a22829c2150f4dba00187881897c719f7a`
- **Hosted run:** [30072229655](https://github.com/laminardb/laminardb/actions/runs/30072229655)
- **Hosted-CI verdict:** **PASS on the first attempt**, including required `CI Success`
- **Construction-runner verdict:** **GO for bounded construction portability only**
- **Formal redb prescreen verdict:** **DEFER**; the formal protocol and runner are not approved or executable
- **Backend-investment recommendation:** RocksDB mechanism closure first; redb native prescreen as an
  optional bounded hedge
- **Production backend selected:** none
- **Cluster admission:** unchanged and fail-closed under `[LDB-4007]` and `[LDB-0013]`
- **Production verdict:** **NO-GO**; no backend qualification, distributed lifecycle, or independent
  product soak exists

## Outcome

Cycle 16 added an isolated, construction-only workspace for exact `redb =4.1.0`, default features
disabled. It compiles and exercises a four-table state layout without adding redb, an adapter, or a
feature flag to the LaminarDB runtime workspace. The independent gate has no redb dependency and
reconstructs the expected records from a deterministic operation schedule. Every output declares
itself ineligible for prescreen, qualification, backend selection, delivery, production, fault, and
soak claims.

The checked-in [hosted job](../../.github/workflows/ci.yml) verifies the exact redb checksum, root
lock isolation, gate dependency isolation, formatting, linting, tests, release construction, a
bounded disposable run, read-only scan, database hash stability, and the detached canonical export.
Its 512-MiB file cap and 60-second process deadlines are safety bounds, not qualification thresholds.
The job uploads no artifact and is a required input to `CI Success` only so construction regressions
cannot silently land.

The [carry-forward matrix](../reports/state-backend-carry-forward-matrix-2026-07-24.md) is the
Cycle 16 decision aid. It recommends the next qualification investment but deliberately does not
select a production backend. The ADR and phase plan now prevent adapter work from starting merely
because a backend is the last candidate standing.

This review is a documentation-only closeout after the hosted-CI tested head. It is not represented
as part of run 30072229655 and changes no runtime, workflow, tool, guard, or tested behavior.

## Empirical construction result

The final local Windows run and the pinned, network-disabled Linux/amd64 Docker run both produced:

- exactly 64 MiB of logical state across `state`, `timer`, `join_left`, and `join_right`;
- exact prior-value validation for every insert, overwrite, and delete;
- one returned I1, I2, and QR durability-mode transaction plus a synchronized sole-writer HOLD;
- a separate-process `open_read_only` scan with the database SHA-256 unchanged;
- 65,536 canonical rows and 67,174,456 export bytes; and
- export SHA-256 `a82240b51daf373ce03bbff9cd70bede90eda1b8433ef39e6be0754dd76e7290`.

The Docker run used Linux/amd64 image
`rust@sha256:6258907abe69656e41cd992e0b705cdcfabcbbe3db374f92ed2d47121282d4a1`,
Rust/Cargo 1.95.0, four CPUs, a 6-GiB container limit, a 512-MiB file limit, and 60-second external
deadlines. Its disposable database was tmpfs inside Docker Desktop/WSL, not native XFS or dedicated
NVMe. Construction wall times, file-size ratios, and the synchronized HOLD duration are diagnostic
only and are excluded from the backend recommendation.

The hosted `ubuntu-24.04` job passed on its first attempt in 58 seconds on Ubuntu 24.04.4, runner
2.336.0, image `20260720.247.2`, and exact Rust 1.95.0. It reproduced the exact row count, export
size, and export digest above under the same file and process safety bounds. All required workflow
jobs and `CI Success` passed on that first attempt. This proves construction portability on one
hosted Linux runner. It supplies no native-device, latency, crash, recovery, endurance, or
backend-fitness evidence.

## Backend judgment matrix

| Candidate | Cycle 16 disposition | Decisive reason | Smallest honest next investment |
|---|---|---|---|
| Fjall 3.1.8 | **FAIL DKS-Q2-006 as published** | Required complete, cheap debt/stall/compaction-I/O/cache/pinned/snapshot/applied-option observations and global controls are absent | Fund a pinned fork or upstream telemetry/control patch before adapter work |
| RocksDB 10.4.2 via `rocksdb` 0.24.0 | **BLOCK; primary closure track** | Most primitives and controls exist, but the binding does not prove complete write-buffer-manager/database-scope stall episodes; native memory, shared controls, and synchronous-FFI tails remain open | Approve a bounded source/binding closure with exact build/SBOM/options; stop if any stall class remains unobservable |
| redb 4.1.0 | **DEFER; prescreen only** | One database-wide, non-cancellable writer and synchronous commit/resize/repair/close work may defeat disjoint-vnode tail goals; no formal native evidence exists | Repair and approve the prescreen contract, verifier, native harness, crash actuator, oracle, classifier, and owners before running it |
| SurrealKV 0.21.2 | **REJECT unmodified** | Snapshot registration can unregister a still-live sequence used by compaction, with further liveness and recovery gaps | Correctness/liveness fork first; only then propose a bounded prescreen |

The recommended owner judgment is therefore: carry RocksDB only into DKS-Q2-006 mechanism closure,
keep redb only as a separately approved prescreen hedge, and carry neither into a runtime adapter yet.
This recommendation is based on the width of known exact-source gaps, not a benchmark score or a
measured engineering-cost ranking. A Rust-native/no-C++ policy would require an explicit funded
Fjall patch or redb prescreen decision; it must not silently promote either candidate.

## redb boundary and open protocol work

The construction lane is not the formal [redb prescreen](../testing/state-backend-redb-prescreen-v1.md).
Before a native prescreen can be signed or run, owners must resolve and implement:

- descriptor roles and fixed artifact locators;
- signature algorithm, trust root, revocation, and a non-circular signed preimage;
- separate decision thresholds and safety caps;
- the inconsistent completion and artifact-retention rules;
- the five-hour schedule arithmetic and frozen key/operation fixture;
- an external approval verifier and immutable runner/toolchain identities;
- a native supervisor/harness, process-level crash actuator, semantic oracle, and fail-closed result
  classifier; and
- workload and operations owner approval on a quiet native Linux/XFS/NVMe host.

Even `PRESCREEN_PASS` would fund the later redb profile, persistence/mechanism mapping, and adapter
design review. It would not admit redb to C1/C2/C3, select it, or make it production-ready.

## Production, hot path, delivery, and soak boundary

No local store supplies distributed ownership. The production design still needs vnode routing,
monotonic ownership epochs, live-term fencing, atomic state/timer/join mutations, aligned checkpoint
decisions, portable remote artifacts, verified restore into an unservable generation, rebalance
transfer, rollback, and retention-safe cleanup. Backend calls that may block, allocate, synchronize,
or cross FFI cannot be placed on the async event loop; C2/C3 must measure hot-writer/victim tails,
checkpoint pause, disk/RSS growth, and restore interference under bounded queues and admission.

Exactly-once is a composed source/state/coordinator/sink property, not a backend feature. Each source
profile must prove replayable cursors, partition/vnode mapping, term-fenced handoff, and cursor sealing
in the checkpoint decision. Each sink/update-mode profile must prove a checkpoint-composed
transactional commit or an explicitly fenced/idempotent protocol, including ambiguous-commit
recovery. Unsupported combinations remain fail-closed or advertise only their separately certified
weaker delivery contract.

The planned 24–72-hour backend endurance campaign is necessary but insufficient. Production remains
NO-GO until an independent team operates an immutable product artifact against a pre-approved
duration and event-count charter, target hardware, black-box correctness oracle, connector/fault
matrix, and acceptance criteria. No independent soak occurred in Cycle 16.

## Six-pass cycle review

### 1. AI slop and contract consistency

**Pass after correction.** Independent reviews removed premature backend-decision language,
unsupported safe-Rust and native-filesystem claims, a timing-based HOLD gate, a tautological test,
circular pin sequencing, and inconsistent redb approval language. Construction, prescreen,
qualification, selection, delivery, and production outcomes remain separate.

### 2. Overengineering and hot path

**Pass.** The new code is an isolated disposable runner and detached gate. No backend abstraction,
runtime adapter, event-loop call, pressure observer, or hot-path dependency was added. Work stops at
the cheapest decision-producing boundary: RocksDB mechanism closure or the separately approved redb
prescreen.

### 3. Unused code and dependencies

**Pass.** Both candidate tests and all seven redb-free gate tests exercise current behavior. Every
standalone dependency is used. Exact redb 4.1.0 appears only in the tool's own lockfile; root
manifests, root lock, runtime crates, and admission guards are unchanged.

### 4. Production readiness, delivery, and soak

**NO-GO, correctly fail-closed.** No candidate passed DKS-Q2-006 through the common campaign, no
vnode/checkpoint/rebalance lifecycle exists, source/sink delivery combinations are uncertified, and
no independent product soak ran. `[LDB-4007]` and `[LDB-0013]` remain mandatory.

### 5. Documentation, stale research, and overdocumentation

**Pass.** The matrix is the single Cycle 16 comparison and links to the exact-source audits instead
of copying them. The protocol links back to the canonical construction result. No `docs/research`,
`.claude`, `CLAUDE.md`, or Claude-memory corpus is tracked, so there was no obsolete generated
research file to remove. Existing audit, ADR, protocol, plan, and historical review documents remain
decision-relevant.

### 6. Tests and empirical limits

**Construction pass only.** Rust 1.95 formatting, two candidate tests, seven adversarial/detached
gate tests, and clippy with warnings denied passed locally. Changed-document relative links, CI YAML,
runtime/root-lock isolation, Windows construction, pinned Docker construction, and the hosted Ubuntu
job passed. The full required hosted workflow and aggregate gate also passed on the first attempt.
No native XFS/NVMe latency, power-loss/crash, C1/C2/C3, fault/endurance, connector, delivery, or
independent-soak campaign ran.

A focused read-only audit also confirmed that the common ineligible mechanism-bundle validator now
enforces adjacent cut ordering and artifact clock bounds. Its path stat/open sequence is accepted
only under the documented trusted, quiescent synthetic-fixture scope. Race-free no-follow,
handle-identity opening becomes mandatory if that validator is ever promoted to qualification
evidence or used with attacker-writable directories.

## Cycle 17 entry condition

Do not select a backend or start an adapter by elimination. The next cycle should first obtain the
owner decision and a numeric effort/elapsed-time cap for RocksDB's exact DKS-Q2-006 source/binding
closure. In parallel only if explicitly funded, repair the redb protocol blockers and build its
external native prescreen; the current construction runner must not be repurposed as evidence.

If RocksDB closes the mechanism contract, admit it to the same approved C1/C2/C3, fault/endurance,
restore, and tail-latency campaign required of every candidate. If it cannot close and redb's formal
prescreen passes, design the additive redb profile before any adapter. If both stop, explicitly fund
a Fjall patch/upstream track or keep distributed keyed state unavailable. Independent product soak
remains the final release gate regardless of backend.
