# Distributed keyed state — Cycle 3 review

- **Date:** 2026-07-22
- **Scope:** schema/artifact trust-boundary audit, whole-graph restore audit, routing-schema bounds,
  research refresh, and production-soak resource coverage
- **Verdict:** **APPROVE WITH OWNED FOLLOW-UPS** for this Phase 0 slice only
- **Production/admission verdict:** **BLOCK**; keyed aggregates, windowed aggregates, stateful joins,
  and cluster materialized views remain fail-closed with `[LDB-4007]`

This review approves the admission-neutral routing ABI hardening and the updated design through
`0978e279`. It does not approve the current raw aggregate payload for keyed restore, a graph
lifecycle implementation, Fjall or RocksDB, any exactly-once combination, or a production-ready
claim. No cluster guard was relaxed.

## Reviewed changes

- `b0ac1a7a` and `12a34c38` introduced a generic Arrow descriptor and strict one-batch IPC helper;
- independent review blocked both as production trust boundaries: the descriptor omitted identity
  scopes and resource bounds, while Arrow 57.2 could allocate from attacker-declared IPC lengths;
- `1e8b1a59` removed the generic IPC helper and all-Arrow descriptor, replacing them with the
  bounded routing-only `PartitionKeySchemaV1` identity tied to partition ABI v1;
- `f4ded97b` stopped rejected deep/wide Arrow types from cloning their recursive schema trees;
- `401d2c8d` recorded that the key/schema limits are structural ABI safety ceilings, not measured
  deployment SLOs, and related the 128-KiB allocation cap to the maximum admitted encoding; and
- `0978e279` made the ADR's artifact decoder, graph publication, delivery, research-audit, and soak
  requirements exact without wiring them into the live runtime.

## Review passes

### 1. AI slop and evidence

**Result: pass after corrective commits.**

- The cycle did not preserve code merely because it had already been generated. The unused generic
  IPC helper and speculative all-Arrow physical descriptor were deleted after hostile review.
- The retained descriptor is deliberately narrow: ordered key types and nullability are identity;
  SQL aliases are ignored; dictionaries hydrate; field metadata, floats, and nested types fail
  closed. Exact bytes/digests freeze every admitted family.
- The graph diagnosis is empirical. The exact test proves today's first operator can mutate before
  a later apply failure, even though the vnode stays `Restoring`. The ADR therefore requires one
  assignment-scoped prepare/publish transaction instead of describing the current loop as atomic.
- State-of-art claims use current primary sources and distinguish local LSM, shared remote state,
  checkpoint artifacts, and delivery semantics. Flink 2.3, Spark 4.2, Kafka 4.3.1, and Fjall 3.1.8
  release timing and feature nuance were rechecked; the Kafka row now distinguishes standby support
  from the missing High Availability Assignor in the new protocol.
- The two visible `docs/research` files were rejected as obsolete evidence. Public copies were
  already removed in `52daf683`; the visible directory and `.claude` are ignored junctions into a
  separate dirty private repository, so this branch did not mutate them through an alias.

### 2. Over-engineering, hot path, and latency

**Result: pass for this slice; production latency remains unproven.**

- `1e8b1a59` removes more speculative format code than it adds. Physical artifact identity returns
  only with the concrete DTO, writer, and hostile-input decoder that define its semantics.
- Routing-schema construction is bounded plan-time work. It does not run per row or per processing
  batch. Existing vectorized Arrow-row encoding, xxh3 hashing, and modulo vnode mapping remain the
  record path; the only routing change rejects more than 256 selected columns before allocation.
- Descriptor growth is bounded and geometric rather than repeated exact reallocation. Recursive
  dictionary inspection stops at depth 32; unsupported composites return a constant family label
  without walking or cloning attacker-shaped trees.
- No LSM dependency, local database, artifact DTO, restore queue, graph shadow state, or permissive
  feature toggle was added. The earlier broad live aggregate-artifact prototype was discarded.
- Whole-map capture/revoke scans, synchronous serialization, missing byte governance, and absent
  spill remain named tail-latency blockers. Unit tests are not presented as latency evidence.

### 3. Unused and dead code

**Result: approve with one owned Phase 0 follow-up.**

- The strict one-batch helper and its tests are gone; no unsafe convenience decoder remains unused
  behind a public symbol.
- `PartitionKeySchemaV1` shares its type/resource gate with `PartitionKeyCodecV1`, and shuffle uses
  the new over-wide-key error before allocation. Its bytes are not yet consumed by an artifact or
  admission path. That is acceptable only as a Phase 0 ABI primitive while admission is closed;
  the artifact-contract slice must consume it from one plan-owned key layout, or remove it before
  Phase 0 exits.
- No second hash, vnode mapper, physical-schema policy, compatibility shim, or alternate backend was
  retained. The losing LSM qualification adapter must also be deleted after selection.

### 4. Production readiness, delivery, and independent soak

**Result: BLOCK, correctly fail-closed.**

Before the first grouped aggregate can be admitted, the project still needs:

1. a dedicated bounded artifact DTO and concrete built-in codec registry binding semantic state,
   canonical non-dictionary physical fields, partition ABI, vnode count/identity, operator/table
   identity, `FULL`/`DELTA`/`EMPTY`, chain ancestry, and exact dependency/codec versions;
2. an artifact-specific IPC preflight which holds a global restore reservation, validates every
   signed length/range and schema frame before Arrow parses bytes, rejects compression/dictionary
   messages initially, and fuzzes arbitrary bounded input without panic or amplification;
3. an authoritative lifecycle roster and one whole-graph prepare/publish boundary with closed
   intake, exclusive callback serialization, assignment fencing, infallible bounded shard swaps,
   all-vnode activation, retained retry material, and retired-handle destruction outside the fence;
4. vnode-owned managed working state with atomic batch+journal updates, truthful resource control,
   timers, bounded blocking workers, cheap generation freeze, portable full/delta capture, and
   complete local-disk-loss recovery;
5. numerical Linux/NVMe profiles and identical conformance, crash, compaction, resource, and
   24–72-hour endurance evidence for Fjall 3.1.8 and one exact RocksDB binding, followed by one
   selection and deletion of the loser;
6. a certified source/operator/sink composition. The first candidate remains Kafka-to-Kafka
   durable at-least-once append snapshots with replay-stable operation identity. Exactly-once stays
   independently blocked by `[LDB-0013]`; and
7. immutable release-candidate bits passing the independently owned black-box soak with real Kafka,
   object storage, target storage hardware, external oracle, scheduled faults/rebalances, leak and
   tail-latency gates, and all failed/invalid runs retained.

The soak charter now requires artifact/descriptor/payload, chain, operator/vnode, IPC, decoded
Arrow, and concurrent restore-staging ceilings. No independent soak has run, so production-ready is
not an eligible verdict.

### 5. Documentation and over-documentation

**Result: approve with an owned consolidation follow-up.**

- The validation report owns current evidence and gaps; ADR-008 owns normative architecture; the
  Phase 0 plan owns near-term file/tasks/commit gates; the master plan owns phase sequencing; and
  the soak charter owns certification. The report's repeated graph protocol was shortened to an ADR
  link in this cycle.
- The five authorities are still long and repeat parts of the artifact and lifecycle contracts.
  Before Phase 0 closes, the documentation owner must remove duplicated normative prose from the
  master plan and report, keeping detailed protocol in ADR-008 and executable steps in Phase 0.
  This is not permission to remove acceptance tests or safety gates.
- Obsolete private research is neither cited nor silently deleted through junctions. Its private
  archive/removal needs a separate authorized cleanup in the owning repository.
- No document says the generic IPC decoder, physical descriptor, managed state, LSM choice, or
  keyed admission has landed.

### 6. Tests and checks

**Result: pass for the scoped slice.**

| Command/filter | Result |
|---|---|
| `state::partition_key::tests` | PASS, 15/15 |
| `shuffle::routing::tests::row_hashing_rejects_invalid_dimensions_without_panicking` | PASS, 1/1 |
| `cargo test -p laminar-core --lib` | PASS, 562/562 |
| `db::tests::cluster_query_shape_admission_is_pre_mutation_and_mode_derived` | PASS, 1/1 |
| `operator_graph::tests::rehydration_apply_failure_faults_without_activating_vnode` | PASS, 1/1; confirms the current blocker |
| `aggregate_state::tests::embedded_float_grouping_remains_supported_without_partition_codec_gate` (`--no-default-features`) | PASS, 1/1 |
| `cargo check -p laminar-db --lib --no-default-features` | PASS |
| `cargo check -p laminar-db --lib --no-default-features --features cluster` | PASS |
| Matching `cargo clippy ... -- -D warnings` configurations | PASS |
| `cargo clippy -p laminar-core --all-targets -- -D warnings` | PASS |
| `cargo fmt --all -- --check` and `git diff --check` | PASS |

Parallel Windows test invocations caused Cargo build-lock waits and command-wrapper timeouts; no
compiler or test failure was reported. The final core and exact DB tests were rerun serially and are
the results above.

The full workspace, backend qualification, performance/endurance runs, Docker/Kafka/MinIO suites,
multi-process chaos, and independent release-candidate soak were not run and are not represented as
passing.

## Next-cycle review plan

Cycle 4 should freeze the numerical qualification profile and the aggregate artifact/codec contract
without opening admission or attaching a production LSM. Its closing independent reviewer must:

- repeat these six passes and reject placeholders, result-derived thresholds, floating dependency
  versions, spoofable UDAF names, or a DTO that directly archives the live Rust checkpoint type;
- require exact maximum/one-over tests for every envelope, chain, roster, vnode, row, group, IPC,
  decoded-memory, and concurrent restore reservation limit;
- prove the decoder rejects huge advertised metadata/body lengths, compression, dictionary messages,
  wrong schema/nullability, zero/two batches, missing EOS, trailing bytes, and arbitrary fuzz before
  Arrow can amplify allocation;
- prove every operator/vnode chain preflights before any callback and that late operator/later vnode
  failure changes no live state, activates nothing, and retains the exact retry transition;
- inspect the record path for schema work, hashing, IPC, blocking I/O, per-row futures/allocations,
  recursive clones, full-map scans, and event-loop stalls;
- run the backend-neutral qualification model before either Fjall or RocksDB adapter, keep exact
  pins isolated from runtime crates, and reject a winner without comparable raw evidence; and
- keep `[LDB-4007]`, `[LDB-0013]`, materialized views, windows, joins, and production-ready status
  blocked if any correctness, latency, resource, delivery, compatibility, operability, or
  independent-soak owner remains unresolved.
