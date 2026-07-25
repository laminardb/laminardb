# Distributed keyed state Cycle 25 review

- **Date:** 2026-07-25
- **Branch:** `feature/distributed-keyed-state-adr`
- **Protocol commit:** `3bddfcc9`
- **Cycle outcome:** `REDB_RUN_EVIDENCE_CONTRACT_FROZEN_IMPLEMENTATION_BLOCKED`
- **Bounded memory:** reference/conformance-only; no current product or soak profile
- **Maintenance-health v2:** direction approved; exact runner-v2 contract and implementation remain
  gated
- **Current product target:** local spill; backend not selected
- **Candidate construction or execution authorized:** no
- **Cluster admission:** unchanged and fail-closed under `[LDB-4007]` and `[LDB-0013]`
- **Independent production soak:** not run
- **Production verdict:** **NO-GO**

## Outcome

Cycle 25 freezes the semantic run-root, process-control, terminal-stop, incomplete-evidence and Docker
smoke contracts in the
[redb 4.1.0 prescreen protocol](../testing/state-backend-redb-prescreen-v1.md). It adds no formal
schema, dispatcher, supervisor, process launcher, deletion path, candidate dependency, backend or
execution command.

The formal packet is now explicitly distinct from the implemented synthetic Cycle 22 input. The
legacy `/v1` approval payload remains an ineligible 28-row regression contract. Any future formal run
requires the reserved, unimplemented `/v2` payload/receipt with a common 29-object immutable approval
input version, including an independently constructed 64-MiB fixture. Those input objects are
reverified under opaque live storage authority and are not duplicated into the separately capped
2-GiB result-evidence closure.

Dispatch is two-stage. A live admission capability may enter the protected runner and freeze actual
target/preflight and exact run-start bytes, but cannot open redb. A disjoint exact-run-start child
capability gates every candidate-affecting child, opener/scanner, clean-control and actuator start.
For native runs, an outer-runner-owned root/control lease survives only supervisor-worker failure and
can mint closure-only recovery; packet bytes cannot recreate it. A dormant/armed/release handshake
records process identity before candidate work and separates process-intent, process-release and
slot-attempt ordinal domains.

The native raw manifest is a projection of a mandatory, append-only campaign-control journal and its
exact 105-row schedule ledger. Terminal rejection is restricted to eligible small-crash rows and
three exact invariant-code/observation pairings. The terminal verifier constructs the only permitted
finding bytes and a private one-shot token; dispatch closes before those exact bytes are published,
and only durable `TERMINAL_STOP_LATCHED` commit creates
`TERMINAL_CORRECTNESS_STOP_LATCHED`. Missing acknowledgement or any incomplete terminal-persistence
stage can produce only a valid closed `DEFER`; malformed control state produces no outcome.

Report envelopes bind the exact raw manifest plus the immutable approval inputs they consumed.
The evidence-close manifest owns the complete expected-leaf reconciliation:
`retained_valid`, `retained_invalid` or `unavailable`. Stable malformed bytes remain retained;
never-published leaves may be unavailable; loss of a manifest-listed object is unfinalizable.

Docker has a separate no-decision contract over ten exact 64-MiB cases. Its count-derived cuts are
disjoint, its bootstrap and oracle edge cases are deterministic, its seven incomplete reasons have
one fixed rank order, and it uses a Docker-tagged validator/oracle and evidence-close/cleanup/index
spine. It has no native mechanism report, terminal latch, native outcome or selection effect.

The final protocol file SHA-256 reviewed by all three independent reviewers is
`d94e121ffac774d5500bc5360266131513307d40f897a565efd2f380eda9b13a`.

## Six-pass cycle review

### 1. AI slop and contract consistency

**Pass after three independent blocking reviews and repair.** Early drafts had real defects: a
terminal token did not bind the exact finding bytes; missing leaves and nonterminal stops had no
durable owner; preflight and child launch shared one impossible capability; report crashes could not
produce their own lifecycle envelope; the 2-GiB result cap accidentally included multi-GiB fixture
inputs; and Docker cuts were not a deterministic projection.

Follow-up reviews found a restart authority that could have been recreated from packet paths, an
untracked child between launch and identity capture, actuator omission from stop acknowledgement,
conflated row/process ordinals, a report-input gap for schemas and goldens, and noncanonical bootstrap
causes. Each was repaired. Governance, protocol and adversarial reviewers returned PASS over the
identical final hash. Deserialized content still cannot create admission, child-dispatch, storage,
recovery, terminal or finalization authority.

### 2. Overengineering, hot path and latency

**Pass for validation-contract scope.** The control journal, launch handshake and immutable-input
split are the minimum mechanisms needed to prevent a false result across process crash, cleanup and
replay. No generic PKI, provider abstraction, workflow engine, process supervisor framework or
storage backend was implemented.

All work is prescreen control-plane design. No LaminarDB record, Arrow batch, state lookup, timer,
join, checkpoint, source, sink or rebalance hot path changed. No candidate writer acquisition,
commit, recovery or tail latency was measured.

### 3. Unused code and dependencies

**Pass.** The protocol commit changes one Markdown file. It changes no Rust source, Cargo manifest,
lockfile, feature flag or dependency and adds no redb, Fjall, RocksDB or alternative backend.
No `/v2` schema, validator, dispatcher, lease, process broker, journal, report producer, cleanup
implementation, outcome constructor or trusted-state API exists.

The Cycle 22/23 validators remain intentionally limited to synthetic copied-content checks and return
authorization-unverified, ineligible summaries. The Cycle 16 construction-only tool remains
no-decision and was not executed.

### 4. Production readiness, delivery, exactly once and independent soak

**NO-GO, correctly fail-closed.** Cycle 25 executed no redb workload, Docker/WSL smoke, native
campaign, fault injection or soak. It produced no writer-rate, p99/p999 latency, crash recovery,
resource, endurance, C1/C2/C3 or production evidence. No state backend has been selected.

LaminarDB still lacks vnode ownership epochs, checkpoint freeze/export/seal,
restore-before-activate, rebalance fencing and retention-safe cleanup. Cluster keyed aggregates,
windows and stateful joins therefore remain rejected by `[LDB-4007]`.

Exactly once still requires a replayable source/offset cut, a sealed vnode-state cut, a recoverable
coordinator decision, and a transactional or idempotent fenced sink. This cycle changes no source or
sink capability and cannot satisfy `[LDB-0013]`. An immutable release candidate must still pass the
separately approved backend endurance campaign and an independently operated production-like soak.
A redb prescreen pass can substitute for neither.

### 5. Documentation, stale research and overdocumentation

**Pass for a safety-critical normative contract.** The cycle extends the existing prescreen protocol
instead of adding a parallel ADR or repeating the backend research. Descriptive object names replace
ambiguous one-letter aliases in the new section. Exact behavior needed to prevent false authority,
self-hash cycles or unsafe cleanup is normative; undecided byte layouts, caps and durability
primitives remain explicit blockers rather than invented implementation detail.

No external research or Claude-memory claim was treated as evidence. Existing backend/research
documents remain relevant decision or audit history, so none was deleted solely to reduce file count.

### 6. Tests and empirical boundary

**Relevant validation regression passes; the root all-target Windows gate is not green.** In the
isolated `tools/state-backend-qual` workspace,
`cargo test --all-targets --all-features` reports 148 passed and one explicitly ignored non-gating
throughput/RSS observation. Root `cargo fmt --all -- --check`,
`cargo clippy --all-targets --all-features -- -D warnings`, and `git diff --check` pass.

A separate root `cargo test --all-targets --all-features` attempt failed during compilation on this
Windows checkout with pre-existing `E0463` missing-rlib errors for LaminarDB integration targets and
an unresolved-import error in the LaminarDB lib-test target. This docs-only cycle did not change those
targets. The failure is recorded, not reclassified as a passing gate.

These checks did not parse or implement the new `/v2`/Cycle 25 contracts, open redb, launch a
candidate, exercise Docker/WSL, delete a database, call a provider/store, derive a prescreen outcome
or perform an independent soak.

## Cycle 26 entry boundary

Continue validation-only work without candidate construction or execution:

1. freeze exact `/v2` approval/receipt, immutable-input-version, target/preflight, run-start and raw-
   manifest schemas, role registries and proved byte/node/cardinality caps;
2. freeze campaign-control frame domains, torn-tail/durability rules, ordinal conservation and literal
   positive/negative goldens before any journal or process-broker implementation;
3. freeze report-envelope, expected-leaf, evidence-close and Docker-control wires and independently
   prove their artifact DAG and failure matrix; and
4. keep every admission/child/storage/recovery capability unconstructible and do not add a runtime
   backend, redb dependency, candidate execution, deletion path, backend selection, cluster admission
   change or production claim.
