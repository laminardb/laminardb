# Distributed keyed state Cycle 28 review

- **Date:** 2026-07-25
- **Branch:** `feature/distributed-keyed-state-adr`
- **Research commit:** `47fd1f09`
- **Cycle outcome:** `TIDESDB_RUST_PATH_REJECTED_NATIVE_RESEARCH_ONLY`
- **Bounded memory:** reference/conformance-only; no current product or soak profile
- **Maintenance-health v2:** direction approved; candidate mapping remains gated
- **Current product target:** local spill; backend not selected
- **Candidate or backend execution authorized or performed:** no
- **Cluster admission:** unchanged and fail-closed under `[LDB-4007]` and `[LDB-0013]`
- **Independent production soak:** not run
- **Production verdict:** **NO-GO**

## Outcome

Cycle 28 statically prescreens TidesDB native `v9.3.14`, the official Rust `v0.11.1` wrapper,
and the primary artifacts behind its reported Zipf advantages.

The current Rust path is rejected. It selects native `9.3.6`, does not encode parent-child
lifetimes, and exposes an unsynchronized callback/context race and no contained panic policy. The
current native engine remains research-only: default multi-column-family transactions are not
crash-atomic, both layouts can expose partial transactions and acknowledge partially inserted
batches, unified-WAL recovery can fail open, and acknowledgement has ambiguous failure paths. Native
checkpoint also omits unflushed unified state and lacks a proved atomic cut. Its internal pressure
budget is host-memory-derived rather than cgroup-safe, and its health surface does not expose every
required stall and background-error signal.

The Zipf results preserve a performance hypothesis but cannot select a backend. Prominent
comparisons disable durability, use different stack layers or revisions, omit consistent production
tail and recovery evidence, and do not exercise LaminarDB's checkpoint, restore, rebalance,
source/sink, or bounded-memory contracts.

TidesDB does not replace RocksDB, Fjall, or redb. RocksDB remains a mature reference/closure track,
Fjall remains in the frozen comparison lineage, and redb remains validation-only under its separate
protocol. No backend is selected.

## Six-pass cycle review

### 1. AI slop, evidence, and contract consistency

**Pass after independent benchmark-artifact, exact-source, and report reviews.** The report pins
exact native and Rust commits and distinguishes source-proved defects, published vendor results,
inferences, and untested hypotheses.

Review repairs explicitly captured unified checkpoint omission, fail-open WAL replay, the Rust
callback race, partial transaction visibility in both layouts, and unchecked partial batch success.
They corrected the memory-budget warning and hard-cap distinction, aligned delivery claims with
ADR-008, and narrowed the built-in `uint8_t` Zipf generator finding without attributing it to the
unavailable external runner. Vendor artifacts remain vendor evidence, not independent
qualification.

### 2. Overengineering, hot path, and latency

**Pass for the static-prescreen scope.** This cycle added no adapter, abstraction framework, runner
profile, or runtime path. One focused report contains the audit, gate matrix, carry-forward decision,
delivery boundary, and re-entry conditions.

The report records the necessary hot-path constraints: Arrow-batch coalescing, bounded blocking
service lanes, bounded admission and buffers, no per-row FFI telemetry, allocation, logging, task
spawn, or fsync, and open-loop p99/p99.9 measurement including queueing and maintenance
interference. No latency property is inferred from closed-loop throughput.

### 3. Unused code and dependencies

**Pass.** The cycle is documentation-only. No Cargo manifest, lockfile, runtime feature, backend
implementation, FFI binding, runner candidate, public API, Docker launcher, or cleanup path changed.
Exact-source checkouts were temporary research inputs, not repository dependencies or approved
candidate identities.

### 4. Production readiness, delivery, exactly once, and independent soak

**NO-GO, correctly fail-closed.** No candidate was built or executed, no crash or cache-loss fault
was injected, and no endurance or independent production soak ran. LaminarDB still lacks the
complete vnode-ownership, checkpoint-sealing, restore-before-activate, rebalance-fencing, and
retention-safe cleanup lifecycle. The initial distributed-state release remains at-least-once.

A later exactly-once certification requires an exact-certified replay-stable and assignment-fenced
source, a sealed vnode-state/timer/output cut, a recoverable coordinator decision, and a checkpoint-
committable external sink transaction that atomically consumes the predecessor cursor and is fenced
by deployment, pipeline/sink namespace, checkpoint attempt, and live leader term, including
ambiguous-commit recovery. A local backend cannot supply these connector or coordinator properties.

### 5. Documentation and research hygiene

**Pass.** No still-relevant RocksDB, Fjall, redb, or SurrealKV research was removed; those dated
reports remain decision history. No Claude-memory assertion was promoted without independent source
review. The report supersedes only the proposition that a vendor Zipf result is sufficient to
replace RocksDB, so no redundant carry-forward addendum was created.

### 6. Tests and empirical boundary

**The validation-only repository slice is green; no backend evidence was produced.**

- `cargo fmt --all -- --check`: pass.
- `cargo fmt --manifest-path tools/state-backend-qual/Cargo.toml -- --check`: pass.
- Isolated `cargo test --locked --manifest-path tools/state-backend-qual/Cargo.toml --all-targets
  --all-features`: 157 passed, one explicitly ignored non-gating parser throughput/RSS observation.
- Staged `git diff --check` and relative Markdown-link validation: pass.
- No TidesDB, RocksDB, Fjall, redb, native-target, Docker, or WSL candidate workload ran.
- Candidate-bearing CI was not invoked because current authority forbids candidate execution.
- The known Windows root all-target missing-rlib/`E0463` issue remains open and is not reclassified.

## Cycle 29 entry boundary

Resume validation-only native and Docker actual-target/preflight contract work:

1. freeze exact source domains, predicates, scalar encodings, reason codes, and maximum-width fixture
   evidence before schemas or caps;
2. do not freeze run-start or raw-manifest caps until the native and Docker registries prove their
   maxima;
3. keep approval-input storage-version authority blocked pending a provider, version, retention,
   freshness, and TOCTOU contract;
4. preserve the reviewed Docker broker topology and durable launch-ledger prerequisites before
   implementation; and
5. add no runtime backend, candidate dependency, candidate execution, cluster-admission change,
   backend selection, or production claim.
