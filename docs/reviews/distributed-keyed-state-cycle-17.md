# Distributed keyed state Cycle 17 review

- **Date:** 2026-07-24
- **Branch:** `feature/distributed-keyed-state-adr`
- **Cycle commits:** `a352caf5`, `5da90a23`
- **Cycle outcome:** `CLOSURE_STOPPED_AT_STAGE_0`
- **Production backend selected:** none
- **Cluster admission:** unchanged and fail-closed under `[LDB-4007]` and `[LDB-0013]`
- **Production verdict:** **NO-GO**

## Outcome

Cycle 17 independently audited the exact `rocksdb =0.24.0` /
`librocksdb-sys =0.17.3+10.4.2` source before starting the bounded mechanism-binding work
recommended in Cycle 16. The archive hashes match the recorded identities, and relevant extracted
sources match the official wrapper/sys and bundled RocksDB commits.

The [closure report](../reports/rocksdb-mechanism-source-closure-2026-07-24.md) establishes two
independent DKS-Q2-006 gaps. Complete foreground stall intervals are not available because the
write-buffer-manager wait bypasses the controller ticker/listener surfaces. That gap appears
amenable to a slow-path native observer plus C and safe-Rust bindings, but its correctness, loss
handling, clock semantics, and overhead remain unproved. More importantly,
`estimate-pending-compaction-bytes` is a level-rewrite projection, not v1's complete, direct,
pairwise-disjoint outstanding-maintenance byte population. Closing that arm requires substantially
broader configuration proof and/or transition-consistent engine instrumentation.

The source audit therefore stopped before a fork, dependency, native build, forced-path test, or
candidate artifact. This is a correct Stage-0 stop, not a failed benchmark or rejection of RocksDB.
The normative v1 contract remains unchanged. Owners must choose whether to retain v1 and fund the
larger scope, or approve an additive successor with a new identity and candidate-typed health
signals. No old artifact may be reinterpreted under a successor.

The static audit, Cycle 16 matrix, qualification contract, and ADR now carry explicit corrections or
supersession notices. Historical text is preserved so the decision trail shows why the earlier
stall-only recommendation changed.

## Backend consequence

| Candidate | Cycle 17 disposition | Next decision-producing work |
|---|---|---|
| RocksDB 10.4.2 | **BLOCK under v1; no adapter** | Owner contract-or-fork choice; only then consider the bounded stall observer |
| redb 4.1.0 | **DEFER; prescreen track only** | Complete the separately governed native writer/commit/recovery prescreen; N/A telemetry cannot select it |
| Fjall 3.1.8 | **FAIL unmodified under v1; no adapter** | Re-evaluate only after the owner choice defines the candidate-specific obligation, or fund a native telemetry/control patch |
| SurrealKV 0.21.2 | **REJECT unmodified** | Correctness and liveness fork before any qualification work |

The recommended contract successor, if approved, retains common end-to-end latency, resource-tail,
disk-growth, write-amplification, device-I/O, pressure, snapshot/restore, and failure-to-quiesce
vetoes. It replaces only the universal exact-debt scalar with reviewed candidate-native signals
whose scope, units, estimation error, overhead, configuration, and thresholds are explicit. This is
a proposal, not an owner decision.

## Six-pass cycle review

### 1. AI slop and contract consistency

**Pass after correction.** The audit caught the earlier unsupported statement that RocksDB's
pending-compaction estimate could satisfy v1. Independent source reviews also removed categorical
language that v1 rejects every estimate, invented outcome terminology and numeric budgets,
uncited “small patch” claims, and wording that implied the audit could authorize a replacement
contract. Engine facts, contract interpretation, work authorization, candidate disposition, and
backend selection are now separate.

### 2. Overengineering and hot path

**Pass by stopping.** Requiring one exact engine-neutral byte population for all asynchronous
maintenance is now explicitly flagged for owner review because documented production systems use
typed estimates, counts, queue times, stall intervals, and external latency/resource signals. The
rule was not silently weakened. No scheduler bookkeeping, callback, allocation, lock, polling loop,
FFI call, or runtime dependency was added. A future observer must remain on actual slow paths and
prove nonblocking/loss behavior, monotonic-clock compatibility, and telemetry-on/off tail overhead.

### 3. Unused code and dependencies

**Pass.** Cycle 17 is documentation and exact-source analysis only. Root/runtime manifests and
lockfiles remain free of RocksDB, Fjall, and redb backend dependencies; no unused adapter, feature
flag, wrapper, fixture, workflow, or generated artifact was added.

### 4. Production readiness, delivery, and soak

**NO-GO, correctly fail-closed.** No backend has qualified, and LaminarDB still lacks the complete
vnode ownership/checkpoint/rebalance lifecycle for these operators. Exactly-once remains a composed
source-offset, state, coordinator-decision, and sink-commit property; no source/sink combination is
certified by this audit. Crash/power-loss, C1/C2/C3, 24/72-hour endurance, connector/fault, and
independently operated product-soak evidence are absent. `[LDB-4007]` and `[LDB-0013]` remain release
vetoes.

### 5. Documentation, stale research, and overdocumentation

**Pass.** One source-closure report holds the detailed proof; the ADR and older reports contain only
short corrections and links. The Cycle 16 matrix remains clearly marked as historical rather than
being rewritten. No `docs/research`, tracked `.claude`, `CLAUDE.md`, or Claude-memory corpus exists
in this tree, so there was no stale generated research file to remove. The new report is retained
because it reverses a backend-investment premise and records exact source identities.

### 6. Tests and empirical limits

**Source checks pass; construction is intentionally N/A.** Both crate SHA-256 values were
recomputed. Relevant pinned source paths and the current RocksDB 11.1.2 freshness screen received
independent review. Changed-document relative links and `git diff --check` pass. No backend build,
candidate run, latency result, fault injection, endurance run, delivery test, or soak was performed;
none may be inferred from the source result.

## Cycle 18 entry condition

Draft the smallest additive DKS-Q2-006 successor as a non-approved decision proposal. It must show
the exact v1 delta, preserve common objective vetoes, define candidate-specific typed health
inventories without false cross-engine scoring, and state how N/A, configuration proof,
instrumentation overhead, quiescence, and alert thresholds remain fail-closed. Compare that proposal
against retaining v1 and funding native engine bookkeeping. Do not resume the RocksDB patch, add a
runtime backend, or run a candidate qualification before the owner choice.
