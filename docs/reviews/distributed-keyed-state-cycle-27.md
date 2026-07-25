# Distributed keyed state Cycle 27 review

- **Date:** 2026-07-25
- **Branch:** `feature/distributed-keyed-state-adr`
- **Contract commit:** `1cbe7634`
- **Cycle outcome:** `TARGET_PREFLIGHT_IDENTITY_DAG_CHECK_REGISTRY_FROZEN_SCHEMAS_BLOCKED`
- **Bounded memory:** reference/conformance-only; no current product or soak profile
- **Maintenance-health v2:** direction approved; implementation remains gated
- **Current product target:** local spill; backend not selected
- **Candidate or backend execution authorized or performed:** no
- **Cluster admission:** unchanged and fail-closed under `[LDB-4007]` and `[LDB-0013]`
- **Independent production soak:** not run
- **Production verdict:** **NO-GO**

## Outcome

Cycle 27 freezes the class-specific native and Docker actual-target and initial-preflight
identities, their acyclic authority DAG, neutral observation registries, exact initial check
registries and deterministic failed-versus-incomplete projection. It does not add their schemas,
collectors, validators, maximum-width fixtures or execution authority.

Initial preflight can prove only its own collection-time facts. It cannot claim later release
lateness, boundary quietness, candidate/noncandidate write attribution or cache-reset evidence.
Observed policy mismatches fail their checks; unavailable, ambiguous, over-cap, changed, regressed,
overflowed or late observations make only dependent checks incomplete. The native contract keeps
collector, campaign-parent and future child cgroups distinct and retains separate marker,
device-write, throttle, memory, thermal, kernel and broker sources.

The Docker contract selects one reviewed broker topology before dispatch and binds it through the
target, preflight, future run-start and launch-ledger header. A mandatory durable launch ledger
owns create/start/release/acknowledgement ambiguity, including zero-launch closure. Broker release
commit must precede process acknowledgement. A disposable preflight volume must be removed and
proven absent before publication; real case copies occur only after run-start and are bound before
database-opening release. No schedule row count is treated as a process, frame or raw-artifact cap.

## Six-pass cycle review

### 1. AI slop and contract consistency

**Pass after three independent adversarial reviews.** Repairs removed contradictory native process
wording, separated target observations from future-run evidence, made failure/incomplete causes
deterministic, distinguished Docker create from process start, required committed release before
acknowledgement, and bound the selected topology across every future authority object. The final
DAG is acyclic and live dispatcher authority is not inferred from a retained receipt.

### 2. Overengineering, hot path and latency

**Pass for this validation-only scope.** The detail is confined to one canonical protocol and
represents provenance, durability and process-lifecycle invariants required for fail-closed
evidence. No generic collector framework, wire codec, process broker, backend adapter or runtime
operator was added. Nothing changed the record, Arrow batch, state lookup, timer, checkpoint,
source, sink or rebalance hot path, and no latency result was produced.

### 3. Unused code and dependencies

**Pass.** This cycle changed documentation only. No schema, fixture, validator, collector, public
API, Cargo manifest, lockfile, runtime feature, backend dependency, process launcher, Docker client
or cleanup implementation was added. The reserved identities and roles are future contract inputs,
not claims that an implementation exists.

### 4. Production readiness, delivery and exactly once

**NO-GO, correctly fail-closed.** No backend or candidate ran, no crash or rebalance fault was
injected, and no production-like or independent soak was performed. Cycle 27 provides no
throughput, p95/p99/p999 latency, recovery, resource, maintenance-stall, checkpoint or endurance
evidence.

LaminarDB still lacks vnode ownership epochs, checkpoint freeze/export/seal,
restore-before-activate, rebalance fencing and retention-safe cleanup. Exactly once still requires
a replayable source/offset cut, sealed vnode-state cut, recoverable coordinator decision, and a
transactional or idempotent fenced sink. Source and sink capability admission is unchanged.

### 5. Documentation and stale research

**Pass.** Normative material remains in the existing prescreen protocol; this review records only
the decision and evidence boundary. No external or Claude-memory research was promoted to fact,
and no still-relevant research document was removed. Schemas, binary wire details, caps and goldens
remain explicitly blocked rather than being invented ahead of maximum-width fixtures.

### 6. Tests and empirical boundary

**The isolated validation tool is green; no candidate-bearing full workflow was invoked.**

- `cargo fmt --all -- --check`: pass.
- `cargo fmt --manifest-path tools/state-backend-qual/Cargo.toml -- --check`: pass.
- Isolated `cargo test --locked --manifest-path tools/state-backend-qual/Cargo.toml --all-targets
  --all-features`: 157 passed, one explicitly ignored non-gating parser throughput/RSS observation.
- `git diff --check`: pass, apart from benign Windows LF-to-CRLF checkout warnings.

The root Windows all-target test was not rerun for this documentation-only cycle. Its known
missing-rlib/`E0463` failure remains open and is not reclassified. Full CI was not invoked because
its required `redb-prescreen-construction` job executes a candidate, which current authority
forbids. No redb, Docker, WSL, backend, native-target or candidate workload ran.

## Cycle 28 entry boundary

Continue validation-only work without candidate or backend execution:

1. statically prescreen TidesDB against the same mandatory correctness, durability, maintenance,
   observability, recovery, Rust-integration and licensing criteria used for other candidates;
2. audit any Zipf performance claim for equivalent durability, cache, dataset, concurrency,
   compaction and tail-latency settings before treating it as evidence;
3. retain RocksDB, Fjall and redb as comparison/reference candidates until evidence supports a
   decision; do not add a TidesDB dependency or execute it;
4. define exact source domains, predicates and scalar encodings plus maximum-width fixtures before
   freezing actual-target or preflight schemas and caps; and
5. separately select and prove one Docker broker topology before implementation.
