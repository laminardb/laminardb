# Distributed keyed state — Cycle 2 review

- **Date:** 2026-07-22
- **Scope:** partition/delivery contract freeze, partition ABI v1, independent-soak contract
  scaffold, aggregate restore audit, and admission-neutral aggregate vnode hardening
- **Verdict:** **APPROVE WITH OWNED FOLLOW-UPS** for this Phase 0 slice only
- **Production/admission verdict:** **BLOCK**; keyed aggregates, windowed aggregates, stateful joins,
  and cluster materialized views remain fail-closed with `[LDB-4007]`

This review approves the contracts and the small partition-path hardening through `562cc590`. It
does not approve the current raw aggregate payload as a keyed restore format, select Fjall or
RocksDB, change cluster delivery from at-least-once, claim exactly-once, run an independent soak,
or declare any distributed stateful operator production-ready.

## Reviewed changes

- `d93f3fe5` — froze partition, source/sink, checkpoint-tail, and first delivery-scenario contracts;
- `04f2787a` — made the proposed aggregate output identity replay-stable and payload-independent;
- `e9b1fc2b` and `69761ddf` — introduced partition ABI v1 and preserved shuffle error precedence
  and allocation behavior after independent hot-path review;
- `4ef75be6` — added a standalone, machine-readable, explicitly ineligible independent-soak
  contract and CI tripwire;
- `3799d8a3` — documented the versioned prepare/commit vnode restore boundary and the current
  `O(touched vnodes x groups)` capture/revoke tail-latency blocker; and
- `562cc590` — routed aggregate capture, delta bookkeeping, emission state, and revoke through the
  same encoded-key mapping and rejected vnode-count drift before mutation.

## Review passes

### 1. AI slop and evidence

**Result: pass after independent-review corrections.**

- The partition ABI is frozen by exact encoded-byte, xxh3 hash, and vnode goldens. Its type gate is
  exhaustive and intentionally rejects floats, nested values, and run-end encoding in cluster
  routing rather than implying unsupported SQL semantics are stable.
- A first routing implementation was blocked because it added a `Vec<DataType>` allocation and
  changed lookup/type error precedence. `69761ddf` restored two pre-sized allocations and the
  previous fail-closed error ordering. The independent re-review returned `APPROVE`.
- The delivery candidate is concrete: Kafka input, grouped `COUNT(*)` plus exact `SUM`, repeated
  full append-result snapshots, Kafka append sink, and object-store checkpoints. Operation identity
  excludes payload bytes; payload digest and assignment/writer provenance are separate fields.
- The soak scaffold's first static generation/stale-owner model was blocked because its fixture
  could not prove those facts. Those claims were removed and remain unresolved certification
  gates. Legal at-least-once prefixes, identical replay, conflicts, double application, missing
  finals, and incomplete cuts remain independently checkable.
- The aggregate audit did not turn one vnode-label assertion into a false restore claim. It exposed
  the missing envelope, exact schemas, tagged chains, authoritative FULL semantics, graph-wide
  prepare/commit, and restore budgets, then left that implementation for a dedicated slice.
- Independent reviews approved the final documentation and aggregate hardening diffs.

### 2. Over-engineering, hot path, and latency

**Result: pass for this slice; production latency remains unproven.**

- Shuffle still performs one key-column collection and one sort-field collection. Aggregate
  tracking hashes existing `OwnedRow` bytes through a static ABI function; it constructs no codec,
  re-encodes no key, allocates no per-row object, and does not affect the ordinary embedded path
  while delta tracking is disabled.
- The aggregate change adds no state backend, restore queue, checkpoint envelope, feature toggle,
  delivery promise, or admission consumer. Vnode-count rotation fails closed because no safe
  repartition-generation lifecycle exists yet.
- The soak validator is a standalone workspace with exact dependencies and no LaminarDB path or
  workspace dependency. Production crates do not carry certification-only code.
- No Fjall or RocksDB adapter was added before workload numbers, resource gates, fault gates, and
  target Linux/NVMe evidence are frozen. The design retains one backend-neutral state-service
  contract and requires deletion of the losing qualification adapter.
- Current whole-map discovery, per-vnode FULL scans, synchronous revoke, allocator retention, and
  absent byte budgets remain explicit latency blockers. No throughput or tail-latency number is
  inferred from unit tests.

### 3. Unused and dead code

**Result: pass.**

- `PartitionKeyCodecV1` is used by shuffle routing and aggregate vnode mapping; its builder exists
  only to preserve one-pass request/error behavior. Tests lock supported representations and
  dictionary hydration rather than maintaining a second encoder.
- All fallible `drop_vnodes` production callers propagate its error. No ignored result, compatibility
  shim, alternate hash, or unused restore validator remains.
- The independent-soak CLI, schema, draft contract, and fixture are exercised by unit/CLI tests and
  by a required CI job. Removed fixture fields and unused messages were not retained speculatively.
- There is still no Fjall dependency at HEAD. Historical cold-tier code and stale Claude guidance
  were not revived.

### 4. Production readiness, delivery, and soak

**Result: BLOCK, correctly fail-closed.**

The following are required before the first grouped aggregate can be admitted:

1. add a versioned keyed artifact envelope binding partition ABI, vnode count and claimed vnode,
   canonical key schema, operator identity, accumulator-state schema, and payload version;
2. cache plan-owned exact key/accumulator schemas, reject coercion/defaulted state, retain vnode and
   chain identity while uninitialized, preflight all chains and resource reservations, prove
   membership/disjointness, and publish one authoritative vnode replacement atomically;
3. replace operator maps with one byte-governed state service whose batches atomically update state
   and dirty journals, whose barrier freeze is bounded, and whose capture/revoke use vnode ranges
   rather than repeated whole-map scans;
4. freeze named numerical latency, stall, resource, restore, cleanup, compaction, and endurance
   gates; run the same workload/fault contract against Fjall 3.1.8 and the pinned RocksDB candidate;
   select one from evidence and delete the losing adapter;
5. prove Kafka source handoff, state cut, append-sink flush, replay-stable operation envelope, and
   old-owner fencing together. This first vertical remains durable at-least-once; exactly-once stays
   independently guarded by `[LDB-0013]`;
6. retain FullChangelog, mutable sinks, materialized views, windows/timers, and two-input joins as
   separate closed capability rows until their own state, output, cleanup, and rebalance lifecycles
   pass; and
7. build immutable release-candidate bits and pass the independent black-box soak with real Kafka,
   object storage, target storage hardware, external oracle, fault injection, retained evidence,
   and an independent reviewer.

The checked-in soak contract remains `certification_eligible=false` with ten unresolved gates. It
is a schema/fixture/validator scaffold, not an independent run. The existing short engineering soak
is not substituted for production certification.

### 5. Documentation and over-documentation

**Result: pass.**

- The validation report distinguishes baseline findings from the narrow fixes now on this branch.
  It still states that no distributed keyed execution path is empirically certified.
- ADR-008 owns the long-lived state-service, checkpoint, ownership, delivery, and LSM decision. The
  Phase 0 execution plan owns file-level sequencing and gates; the soak charter owns independent
  evidence requirements. No second competing architecture was introduced.
- Restore detail was added because the raw payload audit invalidated a smaller proposed assertion,
  not to imply that the design has landed. Legacy raw compatibility is limited to the admitted
  global vnode-0 aggregate in the proposed cluster contract; embedded behavior is unchanged.
- Research assertions remain linked to primary system documentation/research. No obsolete local
  research document was retained merely because it was generated previously.
- Detail is kept in the validation report, ADR, Phase 0 plan, soak charter, and cycle review. The
  runtime change carries only the comments needed to protect the ABI and generation invariant.

### 6. Tests and checks

**Result: pass for the scoped slice.**

| Command/filter | Result |
|---|---|
| Partition ABI v1 focused tests | PASS, 12/12 |
| `cargo test -p laminar-core --lib` | PASS, 556/556 |
| Independent-soak validator unit tests | PASS, 9/9 |
| Independent-soak CLI integration tests | PASS, 4/4 |
| `aggregate_state::vnode_partition_tests` (cluster lib-test binary) | PASS, 4/4 |
| `aggregate_state::tests::drop_vnodes_purges_revoked_keeps_sibling` | PASS, 1/1 |
| `aggregate_state::tests::global_changelog_delta_checkpoint_roundtrips` | PASS, 1/1 |
| `db::tests::cluster_query_shape_admission_is_pre_mutation_and_mode_derived` | PASS, 1/1 |
| `aggregate_state::tests::embedded_float_grouping_remains_supported_without_partition_codec_gate` (`--no-default-features`) | PASS, 1/1 |
| `cargo check -p laminar-db --lib --no-default-features` | PASS |
| `cargo check -p laminar-db --lib --no-default-features --features cluster` | PASS |
| `cargo clippy -p laminar-db --lib --no-default-features -- -D warnings` | PASS |
| `cargo clippy -p laminar-db --lib --no-default-features --features cluster -- -D warnings` | PASS |
| `cargo fmt --all -- --check` and `git diff --check` | PASS |

The standalone soak tool also passed formatting, clippy, schema/fixture CLI validation, draft
ineligibility output, and dependency-isolation checks during its commit review. The cluster lib-test
link completed after the command wrapper timed out; focused filters were run against that exact
fresh binary. An earlier accidental command without `--lib` attempted integration targets and hit
the Windows paging-file limit. It is neither a product failure nor passing evidence, and no broad
suite claim is made.

The full workspace, Docker/Kafka/MinIO suites, multi-process chaos, backend qualification,
performance/endurance runs, and independent release-candidate soak were not run and are not
represented as passing.

## Next-cycle review plan

The next cycle should implement the keyed artifact/envelope and restore **prepare** model without
opening admission or adding an LSM. Its closing reviewer must repeat all six passes and:

- reject any payload accepted without explicit ABI/version, vnode count/identity, plan-owned exact
  schemas, checksums, group/byte limits, and all-key membership validation;
- prove that uninitialized staging preserves base-plus-delta chain and vnode identity, that all
  chains preflight before mutation, and that a late failure exposes no partial operator/graph state;
- require authoritative FULL replacement to remove absent same-vnode keys while preserving sibling
  vnodes, with corruption, empty-state, wrong-type, wrong-vnode, duplicate-key, and cross-chain
  disjointness tests;
- inspect the record and restore paths for re-encoding, blocking I/O, unbounded decoding, accidental
  clones, per-row allocation, executor stalls, and `O(vnodes x groups)` scans;
- rerun embedded float grouping, global vnode-0, exact `[LDB-4007]`, source/sink capability, and
  partition-golden regressions;
- reject a backend adapter, exactly-once claim, or soak-eligibility change as out of scope; and
- keep Phase 1 and production blocked if any correctness, latency, resource, delivery, upgrade,
  observability, or independent-evidence owner remains unresolved.
