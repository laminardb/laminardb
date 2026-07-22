# Distributed keyed state — Cycle 1 review

- **Date:** 2026-07-22
- **Scope:** Phase 0 execution contract, independent-soak contract, and operator capability inventory
- **Verdict:** **APPROVE WITH OWNED FOLLOW-UPS** for the first Phase 0 slice only
- **Admission:** unchanged; keyed aggregates, windowed aggregates, stateful joins, and cluster
  materialized views remain fail-closed with `[LDB-4007]`

This review approves the plan and the admission-neutral inventory in `6c73793b`. It does not mark
Phase 0 complete, select Fjall or RocksDB, change cluster delivery from at-least-once, claim
exactly-once, or declare any distributed stateful operator production-ready.

## Reviewed changes

- `b02bc8fa` — made an independent black-box soak a mandatory production gate;
- `52ca8eaf` — added the file-level Phase 0 execution plan and soak charter; and
- `6c73793b` — made every graph operator declare its cluster state/capability shape.

The inventory commit has no DDL consumer. Existing cluster query-shape validation remains the sole
admission authority.

## Review passes

### 1. AI slop and evidence

**Result: pass after independent review corrections.**

- The first SQL classifier draft was blocked because analytic aggregates such as `SUM(x) OVER`
  could be confused with a global aggregate. Analytic functions are now rejected before aggregate
  classification.
- A second draft was blocked because absence of a recognized aggregate was being treated as proof
  of statelessness. Stateless classification now requires one direct table source, no join, CTE,
  derived query, distinct, ordering, limit, subquery, or other complex clause, and every function
  must resolve through DataFusion's scalar-function registry.
- The same structural gate now runs before aggregate classification. Nested, CTE, joined, and
  `SELECT DISTINCT` aggregate shapes therefore remain unclassified and fail closed.
- Grouped window aggregates carry a distinct reason covering vnode-scoped timers, watermark
  eviction, output, checkpoint, and rebalance lifecycle. They are not collapsed into the ordinary
  grouped-map explanation.
- The final independent static review returned `APPROVE` with no remaining concrete blocker.

### 2. Over-engineering and hot path

**Result: pass.**

- The change adds one private descriptor and one required trait method. It adds no state backend,
  scheduler, queue, feature flag, configuration, dependency, public API, or admission framework.
- A `GraphOperator` implementation cannot compile without an explicit declaration. There is no
  permissive default and no inference from optional vnode checkpoint hooks.
- Capability classification and debug logging happen during operator construction or replacement,
  not during batch processing, shuffle, state lookup, checkpoint capture, or sink publication.
- An intermediate cached capability field on every `GraphNode` was removed because it had no
  consumer. `SqlQueryOperator` retains only its once-computed descriptor so a later graph lookup
  does not reparse SQL.

### 3. Unused and dead code

**Result: pass for this slice.**

- Graph construction and replacement consume the mandatory declaration for cold-path diagnostic
  logging; the graph does not retain an unused copy.
- All 16 production implementations and 14 test probes declare a capability. The exhaustive
  implementation enum and match create an explicit review point for new physical implementations.
- The descriptor is deliberately not wired to DDL. Its types remain crate-private and are named
  `DdlGuarded`, not “supported” or “certified,” to prevent an accidental readiness claim.
- No Fjall or RocksDB code was introduced. The removed historical Fjall tier remains removed.

### 4. Production readiness, delivery, and soak

**Result: not production-ready; correctly fail-closed.**

The following remain blocking Phase 0 evidence with named human owners still required:

1. freeze numerical workload, end-to-end latency, state-service latency, event-loop stall,
   checkpoint, restore, disk, memory, FD, queue, and compaction limits before backend results;
2. freeze the typed SQL key/partition ABI and compatibility/rollback rules;
3. run the same batched aggregate, timer/window, join, snapshot, restore, cleanup, crash,
   corruption, `ENOSPC`, and endurance contract against exact Fjall and RocksDB candidates;
4. select one backend from evidence and delete the losing qualification adapter;
5. prove the first Kafka source → grouped COUNT/SUM → durable multiwriter append sink scenario as
   cluster at-least-once, including flush-before-source-seal and stale-owner fencing;
6. retain exactly-once, FullChangelog, mutable sink, and materialized-view claims as separate closed
   capability rows; and
7. pass the independent soak charter using immutable release-candidate bits, an external oracle,
   real dependencies, fixed gates, retained failed/invalid attempts, and an independent reviewer.

The existing engineering soak is not independent production evidence. No soak was run in this
cycle, and the Docker Rust 1.93/workspace Rust 1.95 mismatch remains a release-candidate blocker.

### 5. Documentation

**Result: pass.**

- The validation report remains current-state authority, ADR-008 remains design authority, and the
  parent plus Phase 0 plans remain sequencing authority.
- The soak charter explicitly says `certification_eligible=false`; it does not invent thresholds or
  imply that the existing two-minute engineering workflow is certification.
- This slice did not revive removed Claude/Fjall guidance or create a second competing design. The
  earlier stale Claude handoff and duplicate cluster roadmap remain removed or reduced to links.
- Detail is concentrated in the ADR, Phase 0 plan, and soak charter. The implementation adds only
  local comments needed to preserve the admission-neutral and fail-closed boundary.

### 6. Tests and checks

**Result: pass for the scoped slice.**

| Command/filter | Result |
|---|---|
| `cargo test -p laminar-db --lib --no-default-features --features cluster capability -- --nocapture` | PASS, 2/2; 1,626 filtered |
| `cargo test -p laminar-db --lib --no-default-features capability -- --nocapture` | PASS, 2/2; 1,226 filtered |
| `cargo test -p laminar-db --lib --no-default-features --features cluster db::tests::cluster_query_shape_admission_is_pre_mutation_and_mode_derived -- --exact` | PASS, 1/1; 1,627 filtered |
| `cargo clippy -p laminar-db --lib --no-default-features --features cluster -- -D warnings` | PASS |
| `cargo clippy -p laminar-db --lib --no-default-features -- -D warnings` | PASS |
| `cargo fmt --all -- --check` | PASS |
| `git diff --check` | PASS |

The focused test build compiles every production and test-only `GraphOperator`, which is the
compile-time tripwire this slice needs. The full workspace, Docker/Kafka/object-store suites,
multi-process chaos, backend qualification, performance/endurance runs, and independent soak were
not run and are not represented as passing.

## Next-cycle review plan

The next implementation cycle starts with the frozen workload/ABI/delivery contracts, not a
runtime LSM adapter or guard removal. Its closing reviewer must repeat all six passes and:

- reject any numerical threshold chosen after Fjall or RocksDB measurements are visible;
- inspect the standalone qualification harness for deterministic requests, atomic batches,
  independent oracle digests, complete run identity, and zero production-crate dependencies;
- search for per-row blocking, executor stalls, unbounded queues/maps, hidden native memory,
  compaction debt, and source/sink backpressure gaps;
- verify source assignment, vnode state/timer/output ownership, sink flush, checkpoint seal, and
  assignment fencing across the same fault cut;
- reject dual-backend production scaffolding or code retained only for a losing candidate;
- distinguish backend endurance, engineering soak, and independent release certification; and
- leave `[LDB-4007]` closed if any correctness, delivery, latency, resource, upgrade, soak, or
  evidence-retention owner remains unresolved.
