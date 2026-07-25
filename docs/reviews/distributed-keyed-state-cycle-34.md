# Distributed keyed state Cycle 34 review

- **Date:** 2026-07-25
- **Branch:** `feature/distributed-keyed-state-adr`
- **Cycle commits reviewed:** `93f037ad`, `4c60d261`, `f5eb9ea4`, `082ad0de`, and `ba20fc2a`
- **Cycle outcome:** current decisions consolidated; redb prescreen parked after its bounded design
  timebox
- **Candidate, container, provider workflow, native mechanism, or backend execution:** none
- **Runtime dependency, adapter, schema, workflow, or cluster-admission change:** none
- **Bounded memory:** reference/conformance-only
- **Current product target:** one qualified local-spill backend; none selected
- **Independent production soak:** not run
- **Production verdict:** **NO-GO**; `[LDB-4007]` and `[LDB-0013]` remain fail-closed

## Outcome

ADR-008 now holds the short current-state matrix. Detailed backend and mechanism investigations stay
in their focused evidence records rather than accumulating in the leading decision. The architecture
still requires a backend-neutral, byte-governed worker-local state service; portable per-vnode
checkpoints remain recovery authority. No local engine supplies vnode ownership, checkpoint
coordination, rebalance fencing, or source/sink delivery semantics.

The redb 4.1.0 validation track is **PARKED**. This is an administrative stop after the Cycle 34
design timebox, not a formal prescreen classifier and not evidence against the engine. There is no
scheduled redb protocol, provider, Docker, IPC, schema, collector, adapter, or execution work. A
future owner may reopen only a new one-page micro-prescreen capped at two engineering days and four
machine-hours. That charter may make an administrative invest/stop recommendation; it cannot emit
the archived campaign's formal `PRESCREEN_PASS`, `PRESCREEN_NO_GO`, `DEFER`, or
`REJECT_EXACT_PIN` outcomes. A favorable observation would fund a later profile proposal, not
backend selection or execution.

Before the stop, static source closure corrected the remaining evidence design. The recommended
crash observation is an in-adapter marker; the assembly ABI alternative would require a different
compatible crash decision. The external evidence authority is a renewable term, not a cloud VM
fence. Delete request identity and a signed final-absence receipt must be bound independently and
rechecked before successor activation. The default Docker snapshotter path was rejected because
unconditional BuildKit adds another external containerd client; only a classic-`vfs` configuration
remained a static hypothesis. Its exact plugin/service population, Engine request schedule, helper
probes, asynchronous GC, and connection inventory were described but never implemented or run.

No conclusion from this cycle qualifies redb, chooses a backend, changes the initial at-least-once
delivery target, or reduces the independent-soak obligation.

## Six-pass cycle review

### 1. AI slop, evidence, and consistency

**Pass after independent correction.** Reviews caught and corrected a non-composable assembly/
seqpacket decision, unsupported provider-fence language, ambiguous operation IDs and absence-receipt
verification, an omitted containerd namespace service, the BuildKit architecture-probe count, and
scheduled/asynchronous maintenance wording. The timebox review also separated administrative
`PARKED` from formal prescreen classifiers and removed stale instructions to continue the archived
campaign.

### 2. Overengineering, hot path, and latency

**Pass by stopping the expanding candidate-specific control-plane design.** The redb track consumed
its design timebox and is no longer active. None of its marker, provider, kernel, Docker, or evidence
machinery entered LaminarDB's record, timer, join, state-access, or checkpoint hot paths. A future
backend still has to meet open-loop p99/p99.9, hot-key/Zipf, disjoint-vnode interference,
maintenance-stall, restore-tail, and bounded-observer gates; this cycle produced no latency evidence.

### 3. Unused code and dependencies

**Pass.** Changes are decision and source-review documents only. No Cargo dependency, native
library, adapter, feature, fixture, generated artifact, provider resource, container image, or
workflow was added. There is no unused implementation.

### 4. Production readiness, delivery, exactly once, and soak

**NO-GO, correctly fail-closed.** The first distributed keyed-state release remains at-least-once.
Exactly-once still requires a replay-stable and assignment-fenced source, one state/timer/output cut,
a recoverable coordinator decision, and a checkpoint-committable sink with live-term fencing and
ambiguous-commit recovery. Backend qualification cannot supply those connector capabilities.
Independent operators must run the immutable release candidate through the production soak before
any production-ready claim.

### 5. Documentation and research hygiene

**Pass.** The ADR is now the compact current-state entry point, and Docker/provider details live in
focused reports. The cumulative redb protocol is retained only as dated provenance and is not an
active backlog. No research record was deleted: each still supports a live decision, rejected path,
or reproducible evidence boundary. Future candidate work should replace short status sections or add
focused addenda, not extend the historical ledger.

### 6. Tests and empirical boundary

**Pass for the documentation/qualification slice; no backend evidence was produced.**

- `cargo fmt --all -- --check`: pass.
- `cargo fmt --manifest-path tools/state-backend-qual/Cargo.toml -- --check`: pass.
- `cargo test --locked --manifest-path tools/state-backend-qual/Cargo.toml --all-targets
  --all-features`: 157 passed; one explicitly ignored non-gating throughput/RSS observation.
- `git diff --check`, exact `N01`--`N29` and `D01`--`D21` registries, decision-literal uniqueness,
  and tracked relative Markdown links: pass.
- No redb, TidesDB, RocksDB, Fjall, Docker/WSL workload, cloud API, provider resource, backend
  candidate, or soak ran.

## Cycle 35 entry boundary

Perform one bounded, static TidesDB recheck only. Treat its object-store feature as optional and
zero-weight for local-backend selection. Preserve LaminarDB's provider-neutral S3/GCS/Azure/local
checkpoint authority; remote SST/WAL placement is not a checkpoint lifecycle. Stop without build or
benchmark if the current exact Rust, atomicity, recovery, resource, or maintenance-health gates
still fail. Record a small carry/stop matrix and return to the common backend-neutral qualification
contract rather than opening another candidate protocol.
