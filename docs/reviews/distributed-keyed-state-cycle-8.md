# Distributed keyed state — Cycle 8 review

- **Date:** 2026-07-22
- **Scope:** provisional deterministic Zipf decision and private numerical feasibility corpus
- **Cycle verdict:** **APPROVE** for provisional design and non-evidence feasibility tests only
- **Zipf admission verdict:** **BLOCK** on DKS-Q2-001/Z1--Z8
- **Candidate execution/selection verdict:** **BLOCK** on DKS-Q2-001 through DKS-Q2-009
- **Production/admission verdict:** **BLOCK**; `[LDB-4007]` and `[LDB-0013]` remain unchanged

Cycle 8 selects a conditional O(1)-memory Zipf method, freezes enough arithmetic to test its
feasibility, and keeps that code out of every default library and executable build. It does not add
a workload-v2 schema, counter, case stream, runner plan, backend adapter, benchmark command,
qualification result, or cluster runtime path. It does not establish cross-target determinism,
distribution accuracy, retry probability, schedule headroom, Fjall suitability, or production
readiness.

## Reviewed changes and boundary

This review covers `a3e07c37..fc5af1d8`:

- `031b8c7b` selects rejection-inversion conditionally, separates Zipf from the existing hot mix,
  rejects YCSB/full tables for this contract, and names eight remaining sub-blockers;
- `d54d4dd3` adds a private `cfg(test)` plus non-default `zipf-feasibility` arithmetic corpus using
  optional `libm 0.2.16`;
- `a8b5c687` runs that explicitly non-evidence corpus in debug and release in the existing Linux CI
  scaffold; and
- `fc5af1d8` additionally pins negative-half and closest-below-zero inverse failures; the
  negative-quarter boundary was already in `d54d4dd3`.

The ideal reference is finite rank mass proportional to `(rank + 1)^(-99/100)`. The executable
candidate freezes binary64 constants, Apache-compatible operation association, top-53-bit uniform
mapping, separate multiply/add, two acceptance paths, a 64-proposal cap, and fail-closed numerical
errors. Its output remains an approximation subject to later high-precision audit.

The published `libm 0.2.16` crate checksum now present in the standalone lockfile is
`b6d2cec3eae94f9f509c767b45932f1ada8350c4bdb85af2fcab4a3c14807981`. This is dependency
provenance, not owner approval or a proof of reproducibility.

Three independent read-only AI review roles challenged the decision and code; they are not human,
workload-owner, or operations-owner approval. Blocking findings included an
undefined case-stream hash, workload-v1 identity leakage, ambiguous ordinal/reset semantics,
unfrozen failure predicates, native floating-point risk, infeasible soak preflight, generator
interference, natural vnode skew, a soak/backend identity conflict, floating reassociation, a
default public API, a bypassable one-proposal method, and an accepted negative-inverse interval.
The contract was narrowed or corrected; matters requiring case, numerical, platform, or owner
evidence remain explicit Z blockers.

## Six review passes

### 1. AI slop and evidence integrity

**Result: pass for a provisional selection; DKS-Q2-001 remains blocked.** The ADR distinguishes the
ideal rational distribution from the finite binary64 implementation and does not call the latter
“exact Zipf.” Ordinary Rust/native transcendental functions, target-specific outputs, modulo
fallback, seed changes, and retry spillover into another row are prohibited. The 64-attempt cap and
all numerical failures reject a finite plan when found by preflight; encountering one during an
execution makes that attempt INVALID. Neither path silently selects a different distribution.

Primary sources were checked independently: the Hörmann--Derflinger paper record, Apache Commons
RNG 1.7 source, YCSB 0.17 source, current Rust floating-point documentation, and `libm 0.2.16`
metadata. YCSB was not adopted merely because its conventional exponent is 0.99: its O(N) zeta
setup, mutable random source, and approximate floating construction do not satisfy this contract.

Literal test values are labelled candidate conformance, not an independent oracle. No unretained
interactive comparison is counted as evidence. The required MPFR/interval error audit and
Linux/AArch64 artifact matrix remain open.

### 2. Overengineering, hot path, and latency

**Result: pass for the feasibility slice; latency remains unproven.** O(N) CDF/alias tables were
rejected because the current domain can exceed 1.2 billion keys and multi-gigabyte benchmark tables
would alter cache and memory pressure. Small tables were rejected because a resident hot subset
would weaken spill conclusions. Precomputed streams and segmented approximations remain new-design
fallbacks rather than hidden alternatives under the same identity.

The candidate transform is O(1) setup/memory, bounded to 64 attempts, and allocates nothing per
proposal. That is not performance evidence. Each row may still pay multi-block SHA-256 plus
software log/exp work, while deduplication can leave the backend with far fewer operations. Before
use, every exact batch/rate needs sampler-on versus counter-only/null preparation, scheduler, queue,
CPU, topology/SMT/LLC, thermal, and interference gates with headroom. The observed delta cannot be
subtracted from candidate latency; preparation remains in offered end-to-end time.

### 3. Unused code and API

**Result: pass.** The numerical module is private and compiled only under
`cfg(all(test, feature = "zipf-feasibility"))`. The feature is non-default; `libm` is optional and
absent from the default active dependency graph. The reserved sampler identity is not exported.
There is no public proposal API, runner flag, CLI command, workload hook, counter, backend trait, or
Laminar runtime dependency.

The private single-proposal evaluator exists solely to inject threshold/rejection vectors. It
cannot be used by a default consumer to bypass the retry cap or invent a fallback. Removing the
finite checks before the null-path measurements would weaken fail-closed behavior, so their cost is
left for evidence rather than guessed away.

### 4. Production readiness, delivery, and independent soak

**Result: BLOCK, correctly fail-closed.** Z1--Z8 still require the non-circular case body,
workload-v2 schemas, per-scenario lifecycle/cardinality semantics, exact approved math artifact,
finite-precision error/retry bounds, named cases and interference gates, separate soak workload
identity, and licensing/SBOM record. DKS-Q2-002 through DKS-Q2-009 remain open as well.

No Fjall or RocksDB call ran. Fjall remains an incumbent candidate to audit and qualify, not a
fit-for-purpose conclusion. No vnode checkpoint, ownership fence, restore activation, rebalance,
source offset cut, sink prepare/commit/reconciliation, delivery guarantee, or exactly-once protocol
was implemented. These backend-local feasibility tests cannot prove any of them.

The independent production soak retains its own driver/workload manifest, fresh seed, conservation
rules, null-sink headroom, fault schedule, immutable release artifact, and separate operator. It may
reuse an approved Zipf transform later but cannot inherit this backend counter or profile seed by
implication. No soak was run, and a short rank file may not be looped as endurance evidence.

### 5. Documentation and over-documentation

**Result: pass after consolidation.** One ADR owns the Zipf choice, alternatives, arithmetic,
counter proposal, hot-path gates, and Z blockers. The C1 model and C2 runner ADRs only link to it and
continue to say v1 is not Zipf-capable. The soak charter remains the production-certification
authority. No second research diary, generated market survey, candidate result, or backend
selection report was added.

The detailed arithmetic is warranted because reassociating two mathematically equivalent products
changed the reproducibility contract during review. Implementation-only literals stay in private
tests instead of being duplicated through prose. No existing research document was removed in this
cycle: the reviewed tracked documents remain relevant, while ignored external research/memory
locations were neither trusted as authority nor modified.

### 6. Tests and checks

**Result: pass for local/default and feature-gated conformance.** Exact Rust 1.95 checks after all
review fixes were:

| Command/check | Result |
|---|---|
| `cargo +1.95.0 test --locked --all-targets` | PASS, 85 total tests; no Zipf module |
| same with `--features zipf-feasibility` | PASS, 94 total tests |
| same with `--release --features zipf-feasibility` | PASS, 94 total tests |
| default and feature `clippy --locked --all-targets -- -D warnings` | PASS |
| `cargo +1.95.0 fmt --all -- --check` | PASS |
| default dependency tree lookup for `libm` | absent; optional feature not active |
| feature dependency tree | only `libm/force-soft-floats` through `zipf-feasibility` |
| CI YAML parse | PASS |
| fully qualified cluster admission regression | PASS, 1/1; 1,660 filtered out |
| `git diff --check` | PASS |

The feature tests freeze constant bits; Taylor/expmath threshold values on both sides and signs;
setup values at small, profile-boundary, and maximum domains; upper/lower numerical clamps; hostile
domains and non-finite values; both acceptance branches; top-53/low-11 behavior; ordinary rejection,
rejection then acceptance, exact 64-attempt exhaustion; and a 100,000-sample candidate digest in
debug and release. They do not prove the ideal PMF, SHA-256 counter/case identity, Linux/AArch64
equality, offered-load capacity, backend behavior, or production correctness.

The admission regression used:

```text
cargo +1.95.0 test -p laminar-db --lib --no-default-features --features cluster \
  db::tests::cluster_query_shape_admission_is_pre_mutation_and_mode_derived -- --exact
```

## Next-cycle implementation and review plan

Cycle 9 should continue closing inputs without exposing a measurement command:

1. freeze the canonical non-circular case body, workload-v2/result identity boundary, and exact
   named hot-mix/Zipf cases together with DKS-Q2-002/003 lifecycle/cardinality semantics;
2. build an independent high-precision numerical/error and retry-bound harness with thresholds
   approved before any backend data, then run exact debug/release goldens on declared Linux x86-64
   and AArch64 artifacts;
3. define and gate counter-only/null sampler capacity, preparation tails, scheduler/queue headroom,
   CPU/topology/thermal interference, and raw-versus-deduplicated rates for every named shape;
4. statically audit pinned Fjall/RocksDB persistence, telemetry, quota/writeback, option-dump, and
   semantic adapter mappings without executing a performance campaign; and
5. keep the independent soak workload/seed/evidence identity separate while completing the
   physical-fault, concurrency, endurance, signing, and retention contracts.

The next review repeats the six passes and treats a missing platform, threshold, case body, owner,
or evidence artifact as BLOCK. Candidate execution still requires named workload and operations
owner approval. Production readiness still requires complete source/sink/exactly-once lifecycle
evidence and a separately operated independent soak against the release candidate.
