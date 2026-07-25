# Distributed keyed state Cycle 39 review

- **Date:** 2026-07-25
- **Branch:** `feature/distributed-keyed-state-adr`
- **Cycle outcome:** TidesDB selected instead of RocksDB as the worker-local implementation target;
  one-CF disposable-state/fresh-portable-restore design accepted
- **Backend/candidate/provider executed:** no
- **Runtime backend, dependency, adapter, schema, workflow, command, or admission change:** none
- **Bounded memory:** reference/conformance-only
- **Independent production soak:** not run
- **Production verdict:** **NO-GO**; TidesDB is unqualified and unadmitted, and `[LDB-4007]` plus
  `[LDB-0013]` remain fail-closed

## Outcome

The project-owner decision selects TidesDB as the sole current worker-local state implementation
target. RocksDB and Fjall remain immutable v4 reference/regression subjects, redb remains parked,
and bounded memory remains semantic/reference-only. Selection is not qualification: a failed hard
gate disqualifies TidesDB and returns alternatives to an explicit owner decision. No fallback is
silently activated.

The accepted
[TidesDB selected-target design](../architecture-decisions/tidesdb-local-state-successor-design.md)
uses one worker-local database and one fixed prefixed managed-state CF. Local persistence is
asynchronous and disposable. Every process incarnation creates an exclusive new root and restores
only a coordinator-admitted portable Laminar cut. Opening or serving a prior native directory,
native checkpoint, and native remote storage are outside the initial safe surface. There is no per-
batch `FULL` fence. Laminar's Rust `object_store` path remains sole portable local/S3/GCS/Azure
checkpoint transport and recovery authority.

The current official `tidesdb/tidesdb-rs` binding path (Cargo package `tidesdb v0.11.1`) remains
rejected because it does not bind the reviewed native 9.3.14 subject and its ownership surface is
unsafe for this use. The selected construction
shape is a narrow project-private exact-current Rust/C facade, not a broad wrapper repair. Any patch
creates a new exact subject identity.

## Decision matrix

| Track | Cycle 39 role | Current evidence | Next permitted step |
|---|---|---|---|
| TidesDB native 9.3.14 + narrow project-private Rust integration | Selected worker-local target; unqualified and unadmitted | Exact-source prescreen plus accepted design only; no build or run | Separately authorize the two-stage kill-fast source-construction prescreen |
| Official `tidesdb/tidesdb-rs` binding (`tidesdb v0.11.1`) | Rejected integration path | Native revision mismatch and unsafe ownership/callback surface | None; do not repair the broad wrapper |
| RocksDB 10.4.2 / Fjall 3.1.8 | Immutable v4 reference/regression lineage | Historical source and validation-contract evidence only | No active source, adapter, or run work |
| redb 4.1.0 | Parked historical B-tree hedge | Design timebox exhausted without a formal candidate result | No work without a new bounded charter |
| In-memory state | Semantic/lifecycle conformance reference | Model and differential value only | No product profile or fallback without an ADR amendment |

## Correctness and recovery boundary

One processed Arrow input batch is the atomic state/result/output publication unit. The adapter
preflights and reserves every canonical vnode unit before native entry, acquires vnode lanes in
canonical order, hides reads/checkpoint capture/results/output until all units succeed, and publishes
the complete batch once. A short apply, partial result, or unresolved post-entry outcome poisons the
entire local incarnation; no successful prefix becomes visible. Synchronous native calls cannot be
cancelled after entry, and their owners/root remain retained until quiescence is proved or the
process fail-stops safely.

Recovery starts with old-owner fencing. It admits either a coordinator-committed empty genesis cut
or one sealed durable-Commit checkpoint, restores into an unservable fresh root, validates the
logical digest and source cursor, initializes the fenced source assignment, activates once, and then
replays/catches up. Native process-death retention, reopen, `persist_data`, `persist_all`, setup
close/reopen, and cache-loss-prefix arms are unsupported rather than passed or `N/A`; the successor
replaces them with portable export -> exclusive new root -> restore.

At-least-once remains the first delivery target. Exactly-once remains a later composition of an
exact-certified replayable/fenced source, one state/checkpoint cut and coordinator decision, and a
checkpoint-committable ambiguity-recoverable fenced sink. Local WAL mode cannot create or weaken
that composition.

## Bounded next investment

This cycle authorizes no source construction. A later explicit task must bind the exact subject or
patch set, isolated workspace, targets, toolchain, allowed actions, and cost. Its kill-fast limits
are:

1. at most half an engineering day and zero candidate machine hours for source/build identity,
   ownership/close, comparator, legal/distribution, and immediate stops; then
2. only after a pass, at most one engineering day and four machine hours for the smallest narrow-
   wrapper feasibility slice. Compilation, linkage, and dynamic smoke execution must each be
   expressly authorized.

Either cap returns `INSUFFICIENT_CLOSURE`. A favorable feasibility result funds a separately
estimated proof package; it does not create an adapter, successor profile, qualification result, or
run authority.

## Six-pass cycle review

### 1. AI slop, evidence, and consistency

**Pass after correction.** Three independent read-only reviews found and corrected material issues:
an unsafe-wrapper draft did not fully bind patched/native subjects and non-cancellable calls; an
early state design published per vnode rather than per whole Arrow batch; and active plans retained
v4 paired-candidate/native-reopen assumptions. The final documents distinguish historical source
facts, selected design, unproved obligations, and actual evidence. No vendor benchmark is promoted
to qualification evidence.

### 2. Overengineering, hot path, and latency

**Pass for design.** The initial target has one CF, no native restart protocol, no native checkpoint
or remote tier, no broad wrapper repair, and a kill-fast construction timebox. The service performs
one bounded request per processed Arrow batch. Per-row FFI, futures, tasks, allocations, metrics,
logs, transactions, fsync, and object-store calls remain forbidden. Qualification must gate open-
loop queue/service/end-to-end p99.9 and maximum latency under resident, spill/cold, timer/range/join,
compaction, checkpoint-overlap, moving-hotspot, and hot-writer/disjoint-vnode cases.

### 3. Unused code and scope

**Pass.** No runtime code, dependency, wrapper, adapter, schema, fixture, workflow, feature flag,
candidate artifact, or command was added. The frozen v2 runner and v4 profile were not edited. The
change is limited to the selected-target design, active authority reconciliation, and this review.

### 4. Production readiness, delivery, and soak

**Pass as an explicit NO-GO.** Safe FFI, exact-count/visibility, immutable cuts, fresh restore,
genesis, cgroup/resource control, maintenance health, C2/C3 tails, fault/endurance, RTO/retention,
rebalance, delivery integration, and provider-neutral checkpoint faults remain conjunctive gates.
No backend campaign, cluster integration, connector scenario, or independent black-box soak ran.
The unchanged-release independently operated soak remains mandatory before any production claim.

### 5. Documentation and research hygiene

**Pass.** Active ADR/plan authority now consistently says “TidesDB selected target, production
NO-GO.” Dated RocksDB/Fjall/redb/SurrealKV/TidesDB reports still carry exact negative evidence and
decision provenance, so none became irrelevant merely because the product target changed. Their
stale work-order language is explicitly superseded instead of being mistaken for current direction.
No new redundant schema identity was frozen before source closure.

### 6. Tests and reproducibility

**Pass.** The frozen runner remains 66,870 bytes with SHA-256
`661102f9dcb46934f966f25cc91934ea0d1fa6a487323fb383cbabd0a5e166e3` and has zero diff. The unchanged
v4 profile remains 7,838 bytes with SHA-256
`94652d30153d998628d4e1d2b5da87bce59f5064192eeba9e9331f3f40507392`.

- `cargo fmt --all -- --check`: pass.
- `cargo fmt --manifest-path tools/state-backend-qual/Cargo.toml -- --check`: pass.
- `cargo test --locked --manifest-path tools/state-backend-qual/Cargo.toml --all-targets
  --all-features`: 157 passed; one explicitly ignored non-gating throughput/RSS observation.
- `git diff --check`: pass.
- Changed-document relative Markdown links: 13 checked, zero missing before this review was added;
  the final check includes this review.
- Three independent final audits—FFI/build/legal/ownership, state/recovery/delivery, and operations/
  hot-path/resource/lineage—return PASS with no unresolved blocker.
- No TidesDB, other backend, Docker/WSL workload, provider API, cloud resource, cluster soak, or
  independent production soak ran.

## Cycle 40 entry and review plan

The next candidate-specific cycle is the bounded source-construction kill gate, but it starts only
after an explicit task binds its exact subject, actions, workspace, target, toolchain, and cost.
Before committing that cycle, reviewers will repeat these passes:

1. reject unpinned source/build/legal claims and any result not produced by the authorized subject;
2. stop at the time/machine cap and reject a broad wrapper, local-recovery protocol, remote tier, or
   normal-hot-path instrumentation;
3. remove unused facade/API surface and keep forbidden native capabilities structurally absent;
4. preserve production NO-GO, fail-stop ambiguity, portable recovery, connector delivery, and
   independent-soak gates;
5. update only successor documents justified by source proof, leaving v4 immutable; and
6. run the authorized ownership/ABI/build/static checks and any expressly authorized smoke under
   exact artifact retention, followed by independent review.
