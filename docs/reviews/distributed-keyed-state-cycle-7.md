# Distributed keyed state — Cycle 7 review

- **Date:** 2026-07-22
- **Scope:** provisional C2 runner/evidence contract and intrinsic latency/resource wire validators
- **Cycle verdict:** **APPROVE** for contract and synthetic validation scaffolding only
- **Candidate execution/selection verdict:** **BLOCK** on DKS-Q2-001 through DKS-Q2-009
- **Production/admission verdict:** **BLOCK**; `[LDB-4007]` and `[LDB-0013]` remain unchanged

Cycle 7 defines how later backend evidence must be produced and validates only intrinsic binary
framing from caller-bounded byte slices. It creates no executable runner plan, benchmark command,
backend adapter, candidate observation, qualification verdict, or admission path. Fjall and RocksDB
remain proposed candidates. No source/checkpoint/sink protocol or independent production soak ran.

## Reviewed changes and boundary

This review covers `f3bb61c2..3fd207a5`:

- `a0ff8732` adds the candidate-neutral runner/evidence ADR, links it from the C1 model, and makes a
  shared-database concurrency campaign mandatory before backend selection; and
- `3fd207a5` adds allocation-free-per-record validators for the finite latency stream, periodic
  resource stream, and exact resource cuts, plus a literal synthetic integration golden.

The ADR freezes open-loop offered arrivals, scheduled/enqueued/dispatch/service/return/terminal
timestamps, outcome masks, result-ring bounds, bracketed resource observations, adverse rounding,
arbitrary-precision slope arithmetic, generation-switched restore, persistence uncertainty,
attempt classification, and an authenticated completion root. Its named blockers deliberately
leave exact workload cases, owner-approved gates, telemetry mappings, physical faults, endurance,
and concurrent selection evidence unresolved rather than inventing results.

Three independent reviewers repeatedly audited the contract. Initial reviews blocked it on, among
other issues, an overflowing `i128` OLS formula, incomplete crash outcomes, unsafe generation reuse,
RocksDB durability overclaim, non-reproducible resource boundaries, Linux 6.1 `memory.peak`
semantics, result-ring backpressure, delayed filesystem writeback, mutable manifest roots, and
missing concurrent selection evidence. Each was corrected before approval. All reviewers explicitly
stated that approval does not authorize execution, selection, admission, or a production claim.

## Six review passes

### 1. AI slop and evidence integrity

**Result: pass for provisional C2.** Stable domain bytes, field order, lengths, tags, reserved bytes,
stage masks, and outcome compatibility are explicit. The independent test golden writes literal
domains and field order rather than importing the parser's private layout constants. Checked count
and length arithmetic runs before record iteration, ordinals/indices are canonical, absent latency
stages are zero, and every trailing byte is rejected.

Review removed ambiguous crash precedence and added a bounded result-slot lifecycle, fixed-size
overflow report, terminal compare-and-swap, and quarantining. Resource parsing was narrowed after
cross-review: zero-count streams remain structurally parseable, while missing samples, cadence,
counter reset, cross-stream ordering, formulas, and attempt status remain future plan-aware policy.
No generated number is described as an observed latency, throughput, durability, or resource result.

### 2. Overengineering, hot path, and latency

**Result: pass for the tooling slice; production hot-path latency remains unproven.** The validators
are linear borrowed-slice scans with fixed-size summaries and no per-record success-path allocation.
They add no parser framework, async runtime, serializer, histogram library, arbitrary-precision
dependency, candidate dependency, background worker, or runner execution loop.

The ADR deliberately separates offered end-to-end, scheduler, state-queue, runner-preparation, and
candidate-service intervals. It requires raw finite samples, null-adapter calibration, observer
controls, a single-worker service case, and a separate deterministic shared-database concurrency
campaign. Those are measurement requirements, not performance evidence. No claim is made about
p99/p99.9, event-loop stalls, compaction, checkpoint pause, restore RTO, or scaling.

### 3. Unused code and API

**Result: pass.** Module exposure is validation-only; `main.rs` is unchanged and has no command that
can run a candidate or manufacture evidence. Wire constants without an external consumer were kept
private, resource summaries were reduced to record count, maximum skew, and tail tag, and the
redundant module-level unsafe attribute was removed. The public validators and summaries are the
intentional next-layer boundary for a later bounded manifest validator and are fully exercised.

There is no dormant Fjall/RocksDB dependency, adapter trait, execution flag, draft/empty runner
plan, PASS/COMPLETE API, admission option, or runtime integration to remove.

### 4. Production readiness, delivery, and independent soak

**Result: BLOCK, correctly fail-closed.** DKS-Q2-001 through DKS-Q2-008 block a real v1 plan and
candidate run. DKS-Q2-009 additionally blocks selection until deterministic disjoint-vnode lanes,
hot-writer/victim contention, mixed point/range/snapshot work, global stalls/resources, and
barrier-controlled lifecycle races pass against both candidates in one shared database.

Fjall 3.1.8 remains the incumbent to qualify, not a fit-for-purpose conclusion. Its persistence
modes, experimental telemetry, backpressure sleeps, cleanup behavior, and resource observability
still need exact-source conformance and physical fault/endurance evidence. RocksDB 10.4.2 through
the pinned 0.24.0 binding remains the comparison; `SyncWAL` is not treated as `persist_all`, and
`use_fsync` is correctly an open-time configuration.

No checkpoint coordinator, vnode ownership fence, rebalance execution, source offset cut, sink
prepare/commit/reconciliation, delivery guarantee, or exactly-once path was implemented. Backend
evidence cannot satisfy those product gates. The independent soak must still be run by a separate
operator against an immutable release artifact; unit tests, self-run chaos, candidate endurance,
canaries, and this review cannot replace it.

### 5. Documentation and over-documentation

**Result: pass.** The runner ADR is long because it is the normative authority for three binary
wires, timing populations, formulas, lifecycle layout, persistence boundaries, classification, and
evidence trust. It does not repeat ADR-008's distributed operator/checkpoint design or the soak
charter. The Phase 0 plan owns sequencing, C1 owns logical oracle semantics, and the soak charter
owns independent production certification.

Provisional, unsupported, proposed, and blocked states are named rather than hidden in prose.
Primary or pinned sources constrain Fjall, RocksDB, Linux cgroup, pacing, and provenance claims.
No duplicate research diary, marketing comparison, backend-selection report, or obsolete generated
research document was added.

### 6. Tests and checks

**Result: pass for intrinsic framing.** Exact Rust 1.95 checks after all review fixes were:

| Command/check | Result |
|---|---|
| `cargo +1.95.0 test --locked --all-targets` | PASS, 85 total tests |
| `cargo +1.95.0 clippy --locked --all-targets -- -D warnings` | PASS |
| `cargo +1.95.0 fmt --all -- --check` | PASS |
| fully qualified cluster admission regression | PASS, 1/1; 1,660 filtered out |
| `git diff --check` | PASS |

The admission regression used:

```text
cargo +1.95.0 test -p laminar-db --lib --no-default-features --features cluster \
  db::tests::cluster_query_shape_admission_is_pre_mutation_and_mode_derived -- --exact
```

Tests cover every latency mask/outcome pairing, returned-crash exclusion, ordinal gaps, absent-stage
bytes, timestamp inversions and zero-valued present stages; every truncation and trailing length;
domain mutation, zero/maximum/caller-bounded counts, and checked encoded-length overflow. Resource
tests cover zero-count structural streams, indices, brackets, skew, cut count/tag order, both tail
tags, reserved bytes, and deliberate deferral of cross-record time/counter policy. The integration
golden independently freezes all three literal domains, big-endian field order, exact lengths, and
compact summaries. These tests do not exercise pacing, backend calls, disk faults, formulas,
classification, or production behavior.

## Next-cycle implementation and review plan

Cycle 8 should close plan inputs before adding any measurement command. In order:

1. resolve DKS-Q2-001 with a reviewed cross-platform deterministic Zipf algorithm, identity, and
   independent golden vectors;
2. freeze the explicit non-Cartesian case matrix, long-stream state control, exact rates/counts,
   balanced pair order, service/oracle/skew gates, result/raw byte ceilings, and synthetic complete
   plan fixture for DKS-Q2-002 through DKS-Q2-005;
3. audit pinned Fjall and RocksDB calls for semantic adapter conformance, persistence, telemetry,
   XFS project quota, cgroup writeback, and option dumps without running performance experiments;
4. add a bounded, closed plan/manifest validator only after every `runner-plan/v1` required field is
   real—never a draft, empty, or `executable=false` escape hatch; and
5. specify C3 concurrency, physical-fault, endurance, approval/completion signing, and immutable
   retention contracts before selection evidence exists.

The next closing review repeats the six passes: independently derive goldens and formulas; challenge
measurement perturbation and unnecessary abstraction; remove unused fields and commands; audit
production, source/sink/exactly-once and independent-soak blockers; keep one authority per contract;
and test hostile inputs, exact boundaries, invalid campaigns, physical faults, and concurrency.
Candidate execution still requires named workload and operations owner approval. Production
readiness still requires the separately owned independent soak to pass.
