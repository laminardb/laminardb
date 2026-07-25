# Distributed keyed state Cycle 35 review

- **Date:** 2026-07-25
- **Branch:** `feature/distributed-keyed-state-adr`
- **Cycle outcome:** current TidesDB subject stopped after a bounded static recheck; object-store
  portability separated from local-backend selection
- **Evidence:** current official source/release and existing LaminarDB checkpoint code only
- **Candidate downloaded, built, linked, configured, or executed:** no
- **Runtime dependency, adapter, schema, workflow, or cluster-admission change:** none
- **Current product target:** one qualified local-spill backend; none selected
- **Bounded memory:** reference/conformance-only
- **Independent production soak:** not run
- **Production verdict:** **NO-GO**; `[LDB-4007]` and `[LDB-0013]` remain fail-closed

## Outcome

The bounded recheck preserves the Cycle 28 hard rejection of the current official TidesDB Rust path
and stops further candidate-specific work. The exact subjects remain native `9.3.14`, Rust `0.11.1`,
and the wrapper's default native payload `9.3.6`. Parent/child FFI lifetimes and callback safety,
partial successful batch apply, unified-WAL recovery, immutable checkpoint/read-cut completeness,
container memory governance, and maintenance-health evidence still fail or remain blocked. No
performance claim can offset these conjunctive pre-execution gates.

TidesDB object storage is optional and has zero backend-selection weight. Native
`tidesdb_objstore_t` is a pluggable function-pointer interface, but the shipped implementations are
filesystem and S3, and the public Rust wrapper offers only filesystem and feature-gated S3
configuration paths. GCS is documented through the S3-compatibility claim, not as a distinct native
connector; no shipped Azure Blob or public-Rust Azure path was found.

This provider gap means TidesDB remote mode cannot replace LaminarDB's existing provider-neutral
checkpoint path. LaminarDB retains `ObjectStoreCheckpointStore` over Rust `object_store`, with local,
S3, GCS, and Azure schemes. TidesDB's continuously changing engine-specific SST/WAL/manifest image is
a remote capacity/recovery mechanism, not a sealed exact-attempt vnode checkpoint, ownership fence,
coordinator decision, or source/sink transaction.

If a future local TidesDB subject first becomes viable, remote cold/capacity tiering remains a
separate ADR driven by measured need. Remote WAL acknowledgement and frozen reads would introduce
network latency/availability, outage backlog, disk-headroom, and remote-copy RPO choices. None may
enter the per-record or event-loop hot path implicitly.

The [TidesDB report](../reports/tidesdb-static-prescreen-2026-07-25.md) contains the exact stop and
owner-authorized re-entry boundary. It authorizes no dependency, adapter, build, candidate run, or
object-store integration.

## Six-pass cycle review

### 1. AI slop, evidence, and consistency

**Pass after three independent reviews and correction.** The review distinguishes the native
pluggable interface from the two shipped connector implementations, calls the Rust surface
configuration paths rather than safe builders, labels the GCS compatibility conclusion as an
inference, scopes Azure to paths not found, and limits remote-copy RPO to whole-local-volume loss.
It also added checkpoint completeness to re-entry and removed language that could self-authorize a
future adapter review. Exact release identities and the redb `PARKED` carry state are consistent.

### 2. Overengineering, hot path, and latency

**Pass by enforcing the timebox.** No TidesDB protocol, profile, adapter, benchmark, or remote-tier
design is opened. Object storage has zero local-backend selection weight. A possible future signal
can only fund a separately approved half-day static delta proposal, followed at most by a separately
approved one-day local-only feasibility review. Network operations remain outside LaminarDB's event
loop and record-level state path. TidesDB has produced no Laminar p99/p99.9, hot-key, maintenance,
victim-isolation, restore, or outage evidence.

### 3. Unused code and dependencies

**Pass.** This cycle changes one existing evidence report, one ADR row, and this review. It adds no
crate, native library, feature, connector, adapter, schema, fixture, generated artifact, workflow,
cloud resource, or executable code.

### 4. Production readiness, delivery, exactly once, and soak

**NO-GO, correctly fail-closed.** A local or remote engine cannot supply vnode assignment,
restore-before-activate, old-owner fencing, aligned source/state/output cuts, coordinator recovery,
or sink ambiguity handling. The initial release remains at-least-once per separately certified
source/operator/output/sink combination. Independent operators must still run the immutable release
candidate through the full production soak before any production-ready claim.

### 5. Documentation and research hygiene

**Pass.** The current conclusion is a focused addendum to the existing TidesDB evidence record and a
single compact ADR row; no duplicate report or candidate protocol was added. The stale instruction
to continue redb was corrected to `PARKED`. No research document is removed because every dated
backend record still supports a live decision, rejected path, or reproducible evidence boundary.

### 6. Tests and empirical boundary

**Pass for the documentation/qualification slice; no backend or object-store evidence was produced.**

- `cargo fmt --all -- --check`: pass.
- `cargo fmt --manifest-path tools/state-backend-qual/Cargo.toml -- --check`: pass.
- `cargo test --locked --manifest-path tools/state-backend-qual/Cargo.toml --all-targets
  --all-features`: 157 passed; one explicitly ignored non-gating throughput/RSS observation.
- `git diff --check`, tracked relative Markdown links, and the exact `N01`--`N29` and
  `D01`--`D21` registries: pass.
- New external primary-source links were independently checked; no candidate, backend, connector,
  Docker/WSL workload, provider API, cloud resource, or soak ran.

## Cycle 36 entry boundary

Return to common architecture rather than prescreening another engine. Reconcile the remaining
pre-final maintenance-health-v2/qualification-contract decisions into one short owner-decision
packet, verify that candidate-neutral local-state primitives and checkpoint/export boundaries are
complete, and identify the smallest validation-only implementation step. Do not select, construct,
download, or execute a backend; do not add an adapter or change cluster admission. Repeat these six
review passes at cycle close.
