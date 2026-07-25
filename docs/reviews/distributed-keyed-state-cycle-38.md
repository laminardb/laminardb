# Distributed keyed state Cycle 38 review

- **Date:** 2026-07-25
- **Branch:** `feature/distributed-keyed-state-adr`
- **Cycle outcome:** validation-only runner contract accepted without a GitHub approval ceremony;
  TidesDB preferred over RocksDB for the conditional local-spill product track
- **Backend/candidate/provider executed:** no
- **Runtime backend, dependency, adapter, schema, command, or admission change:** none
- **Bounded memory:** reference/conformance-only
- **Independent production soak:** not run
- **Production verdict:** **NO-GO**; no backend is selected or admitted, and `[LDB-4007]` plus
  `[LDB-0013]` remain fail-closed

## Outcome

The project-owner direction removes the proposed PF4/PF5 protected-provider and two-principal
ceremony for validation-only work. Repository inspection found no DKS approval workflow under
`.github/workflows`, so no workflow file was deleted. Ordinary CI remains unchanged. The accepted
[runner v2 validation contract](../architecture-decisions/state-backend-qualification-runner-v2-draft.md)
authorizes only standalone schemas, bounded parsers/readers, formulas, deterministic synthetic
execution-ineligible fixtures, and negative-capability tests. It must have no run, dispatch,
approve, select, or qualify command; backend/candidate/cloud/network/process dependency; runtime or
admission consumer; or output claiming qualification evidence. Candidate construction, execution,
selection, runtime integration, and production remain separate explicit decisions.

The owner also directs that TidesDB replace RocksDB as the preferred local-spill product candidate.
Preference sets remediation and qualification priority; it does not make the inspected official
Rust path safe, waive an absolute gate, select a production backend, or authorize a dependency,
source build, adapter, benchmark, or run. The exact-source findings in the
[TidesDB prescreen](../reports/tidesdb-static-prescreen-2026-07-25.md) remain valid: `tidesdb-rs`
0.11.1 selects native 9.3.6 rather than reviewed native 9.3.14, and safety, atomicity, recovery,
read-cut, cgroup-resource, maintenance-health, and Laminar-specific evidence gates remain open.

## Current decision matrix

| Track | Product role after Cycle 38 | Evidence status | Next permitted work |
|---|---|---|---|
| TidesDB native 9.3.14 plus repaired exact-current Rust integration | Project-owner-preferred local-spill candidate; not selected or admitted | Current official Rust subject rejected; every recorded correctness, recovery, resource, health, latency, fault, and endurance gate remains open | A bounded, docs/source-review-only remediation and source-closure design; no download, build, adapter, or execution |
| RocksDB 10.4.2 through `rocksdb` 0.24.0 | Mature immutable reference/regression subject; no active product track | v1-v4 source and validation provenance retained; not qualification evidence | No source, adapter, or run work without a new owner direction |
| Fjall 3.1.8 | Immutable v4 Rust-LSM reference | Scheduler/lifecycle/governance gaps remain | No active product work; a new fork/upstream decision would be required |
| redb 4.1.0 | Parked bounded B-tree hedge | Design timebox exhausted without a formal disposition | No work unless a new bounded micro-prescreen charter reopens it |
| In-memory state | Semantic/lifecycle conformance reference only | Useful for model and differential testing; no broad-state capacity claim | No product profile or fallback without a later ADR amendment |

LaminarDB still needs a qualified worker-local working-state backend for the general profile because
bounded memory is reference-only. It does not need an engine to supply vnode ownership, checkpoint
authority, rebalance fencing, or exactly-once delivery. Those remain Laminar coordinator,
checkpoint, and connector-composition responsibilities.

## TidesDB closure boundary

Before source construction, the bounded design must bind the following proof obligations. A
separately authorized construction task must then close them before a successor profile or run is
proposed:

1. the exact wrapper, native source, ABI, build, lock, SBOM, feature, allocator, and licence identity;
2. Rust parent/child and duplicate-handle lifetimes, ordered shutdown, synchronized callbacks,
   justified thread-safety, panic containment, and sanitizer/Miri evidence;
3. unified-memtable plus FULL-sync semantics, exact-count all-or-nothing batch apply, and atomic
   point/iterator visibility;
4. fail-closed WAL replay, forensic retention, unknown-commit handling, acknowledgement truth, and
   cleanup semantics;
5. one immutable cross-column-family read cut and portable Laminar export/restore, with native
   checkpoint repaired or prohibited from the admitted path;
6. cgroup-aware pressure below the external hard limit with allocator, page-cache, temporary, FD,
   WAL/SST, disk, and amplification accounting; and
7. a maintenance-health-v2 mapping for exact foreground stalls and general background failures,
   collected boundedly off the event-loop and per-row hot path.

Only after construction closes them may a successor TidesDB profile, mapping, profile-binding, and
bundle lineage be proposed. The 7,838-byte v4 profile is immutable Fjall/RocksDB reference data and
cannot be renamed or reinterpreted. An exact run then needs separate authorization over candidate,
plan, target, isolation, limits, and cost before the common correctness, open-loop latency, durability,
fault, Zipf/hot-victim, restore/rebalance, and 24/72-hour campaign. Integration and the independently
operated unchanged-release product soak remain later vetoes.

TidesDB native remote storage stays disabled and has zero local-selection weight. LaminarDB's Rust
`object_store` path for local, S3, GCS, and Azure artifacts remains the sole portable checkpoint and
distributed-recovery authority. Native SST/WAL/manifest state cannot replace the exact-attempt
inventory, seal, coordinator decision, retention, or restore-before-activate lifecycle.

## Six-pass cycle review

### 1. AI slop, evidence, and consistency

**Pass after correction.** Independent audits caught and corrected three material ambiguities: live
plans still routed source work to RocksDB; an early draft implied all v2 schemas could be reused for
TidesDB even though mapping/bundle wires bind exact v4; and historical reports presented superseded
RocksDB priority as current. Active plans now prefer TidesDB while dated source findings remain
explicitly historical. No performance result is promoted into selection evidence.

### 2. Overengineering, hot path, and latency

**Pass for this documentation-only cycle.** Removing the validation-only approval ceremony reduces
governance machinery. No runtime or hot-path code changed. The future contract still forbids per-row
FFI, metrics queries, allocation, locking, I/O, task spawning, database transactions, or fsync;
blocking/cold work remains batched and off event-loop lanes. TidesDB service, queue, FFI, compaction,
write-stall, checkpoint-overlap, and hot-key tails must be measured before selection.

### 3. Unused code and scope

**Pass.** No code, dependency, schema, fixture, workflow, adapter, feature flag, or candidate artifact
was added or removed. No redb protocol/source/fixture changed. Apart from one exact-contract LF pin
in `.gitattributes`, the modified files are decision, plan, evidence-supersession, and review
documents only.

### 4. Production readiness, delivery, and soak

**Pass as an explicit NO-GO.** The local store cannot provide source replay/fencing, sink commit or
ambiguity recovery, coordinator consensus, or vnode ownership. The initial distributed-state target
remains scenario-certified at-least-once; exactly-once remains a later exact source/state/sink
composition. No cluster admission changed. No backend campaign, integration test, cluster soak, or
independent black-box soak ran, so no production-ready claim is made.

### 5. Documentation and research hygiene

**Pass.** Current ADRs and plans now carry authority; dated RocksDB/Fjall/redb/TidesDB reports remain
useful exact-source evidence and decision provenance, so deleting them would remove auditability.
Their obsolete work-priority statements are marked superseded instead. The provider-neutral
checkpoint boundary, bounded-memory decision, and independent-soak charter remain consistent.

### 6. Tests and reproducibility

**Pass.** The final runner contract is 66,870 bytes with SHA-256
`661102f9dcb46934f966f25cc91934ea0d1fa6a487323fb383cbabd0a5e166e3`; it is UTF-8 without BOM,
LF-only, ends in one LF, and has an explicit LF attribute so the committed and checked-out bytes
remain identical across supported platforms. The unchanged v4 profile is 7,838 bytes with SHA-256
`94652d30153d998628d4e1d2b5da87bce59f5064192eeba9e9331f3f40507392`, also UTF-8 without BOM,
LF-only, and ending in one LF.

- `cargo fmt --all -- --check`: pass.
- `cargo fmt --manifest-path tools/state-backend-qual/Cargo.toml -- --check`: pass.
- `cargo test --locked --manifest-path tools/state-backend-qual/Cargo.toml --all-targets
  --all-features`: 157 passed; one explicitly ignored non-gating throughput/RSS observation.
- `git diff --check`, changed-document relative Markdown links, and exact `N01`-`N29` / `D01`-`D21`
  registries: pass.
- No candidate, backend, Docker/WSL workload, object-store provider API, cloud resource, cluster
  soak, or independent production soak ran.

## Cycle 39 entry boundary

First write the bounded TidesDB remediation/source-closure and successor-contract delta described
above, without downloading, building, or executing the candidate. Then implement only the genuinely
reusable, candidate-neutral parsers/evaluators, formulas, bounded readers, synthetic execution-
ineligible fixtures, and negative-capability tests justified by that design. Retain v4 fixture/delta
regression coverage, but do not instantiate unused v4-only containers. Do not add or execute a
candidate until a later explicit task binds that authority.
