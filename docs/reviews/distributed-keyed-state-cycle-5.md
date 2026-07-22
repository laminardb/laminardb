# Distributed keyed state — Cycle 5 review

- **Date:** 2026-07-22
- **Scope:** admission-neutral managed aggregate envelope and outer `VnodePartialV2` reader
  contracts, test-only conformance encoders, and frozen wire vectors
- **Verdict:** **APPROVE WITH OWNED FOLLOW-UPS** for this Cycle 5 slice only
- **Production/admission verdict:** **BLOCK**; keyed aggregates, windowed aggregates, stateful joins,
  and cluster materialized views remain fail-closed with `[LDB-4007]`; cluster exactly-once remains
  separately rejected with `[LDB-0013]`

This review approves unwired compatibility-reader primitives and exact conformance vectors. It does
not approve a checkpoint writer, manifest selector, object fetch, chain resolver, restore
transaction, working-state service, LSM, connector delivery combination, or production rollout.

## Reviewed changes and review history

Cycle 5 covers `cea7b508..cf323355`:

- `cea7b508` bounds resolved ancestry and directory size in the candidate profile;
- `fcf4e005` adds private borrowed aggregate and outer-directory codecs;
- `e3ac8874` freezes the first aggregate and V2 regression vectors;
- `4a6d409e` replaces synthetic keys with real partition-ABI-v1 bytes, enforces the non-nullable SUM
  invariant, makes the aggregate object budget monotonic, removes the redundant V2 whole-body hash,
  and adds a composed FULL-to-DELTA vector;
- `5044f010` aligns ADR-008, the normative byte layout, and Phase 0 with the actual trust boundary;
  and
- `c7dd361a` restores production-private visibility, makes full-buffer encoders and budget cloning
  test-only, renames the structural-only fixture, and assigns the reader-first dead-code allowance;
  and
- `cf323355` gates the final encoder-only method after the closing slop re-review.

The first independent reviews returned **BLOCK**. They found producer-impossible Binary keys in
populated goldens, an unenforced non-nullable SUM invariant, a reusable `Copy` object budget,
redundant full-body hashing, placeholder V2 bodies presented too broadly, stale progress prose, and
outer structural validation described too much like trusted artifact validation. Those findings
were corrected and re-reviewed.

A final slop review then found that a sibling composition test had widened the V2 module throughout
production code, fixture encoders still compiled in release builds, the budget remained clonable,
and the reader-first dead-code allowances had no dated owner. Commit `c7dd361a` gates the wider
module visibility and encoders to tests, makes `Clone` test-only, and assigns **DKS-P1-001** to the
distributed-state lifecycle implementation with a 2026-08-31-or-first-consumer deadline.

## Review passes

### 1. AI slop and evidence

**Result: pass for this slice.**

- Populated aggregate vectors are generated through `PartitionKeyCodecV1` and pinned to literal
  ABI-v1 bytes. A Null grouping key remains the valid zero-length case.
- The composed vector uses a four-vnode keyspace and a real managed FULL body. Its decoded
  contextual entry digest is the exact outer and inner parent of the DELTA body.
- Internal SHA-256 fields are described as corruption detection, not authentication. Production
  trust begins with the sealed inventory digest and manifest selector outside these readers.
- The V2 reader is consistently called outer-structural. It cannot be cited as proof of semantic
  BODY validity, fetched-parent validity, or restore atomicity.
- No current-versus-proposed claim relies on the removed private/Claude research copies. No new
  research diary or duplicate ADR was added.

### 2. Over-engineering, hot path, and latency

**Result: pass for this slice; production latency remains unproven.**

- Both readers borrow caller-owned bytes, perform checked linear scans, and allocate no collections.
  They are private, have no runtime call site, and cannot enter record processing or the event loop.
- The redundant V2 whole-body hash and its 32-byte header field were removed. The directory digest
  still binds entry metadata and each BODY digest still binds its exact contiguous slice.
- Full-buffer encoders now compile only in tests. They freeze compatibility vectors but are not a
  512-MiB-capable production checkpoint writer.
- Sorting, routing-schema derivation, hashing, object-store I/O, chain traversal, and LSM work remain
  assigned to bounded checkpoint/restore workers. No claim is made about their p99 or p99.9 cost.
- The production writer still needs reserved streaming or spill output, checkpoint-pause and CPU
  measurements, backpressure, and cancellation behavior under the approved numerical profile.

### 3. Unused and dead code

**Result: pass with one dated, pre-admission removal issue.**

- The wider `pub(crate)` V2 visibility exists only under `cfg(test)`; release builds keep the module
  private. Encode-only DTOs/functions and aggregate/V2 writers are test-only.
- `AggregateObjectBudget` is neither `Copy` nor `Clone` in release builds. Tests can clone templates
  to verify exact/max-plus-one and failure-without-charge behavior.
- The structural placeholder fixture is explicitly named `vnode_partial_outer_mixed.hex`; real inner
  envelopes live in the composed FULL and DELTA fixtures. Temporary fixture printers were removed.
- The two unwired production readers still need module-level dead-code allowances. **DKS-P1-001** is
  owned by the distributed-state lifecycle implementation and is due by 2026-08-31 or in the first
  trusted manifest-selected composition commit, whichever is earlier. That commit must remove both
  allowances before reader capability advertisement or admission changes.
- No dependency, alternate format, runtime adapter, LSM wrapper, or admission option was added.

### 4. Production readiness, delivery, and independent soak

**Result: BLOCK, correctly fail-closed.**

Before a managed reader can restore state, one production composition path must:

1. select the exact format from a trusted sealed manifest and verify the complete payload digest;
2. reserve encoded bytes before bounded fetch and separately charge decoder/ingestion/spool memory;
3. own one non-resettable `AggregateObjectBudget` across every real BODY in a V2 object;
4. resolve exact FULL/EMPTY, DELTA, and REFERENCE ancestry with depth 8 accepted and 9 rejected,
   cycle/fork/missing-parent detection, accumulated chain-byte limits, and no callback before full
   validation;
5. validate cross-chain replacement invariants, build abortable shadow state, and publish one fenced
   graph transition; and
6. stream or spool production writes under artifact budgets and prove crash cleanup and retry
   identity.

Operator execution still lacks the shared whole-Arrow-batch checked COUNT/SUM transaction. A late
group overflow must leave every group and output unchanged, and split/coalesced replay must agree.
The current group-local preview primitive is not that executor.

No LSM has been selected. Fjall and RocksDB still require the same backend-neutral semantic model,
exact pins, crash/corruption/ENOSPC/compaction/resource tests, hot/cold and skew latency, recovery
RTO, and endurance evidence. No source/state/sink combination has completed its ALO delivery oracle;
exactly-once remains a separate certification problem. Most importantly, no independent operator
has run the immutable release artifact through the required black-box production soak. Unit tests,
self-run benchmarks, chaos tests, canaries, and backend endurance cannot substitute for that soak.

### 5. Documentation and over-documentation

**Result: pass.**

- [Managed state artifact format v1](../architecture-decisions/managed-state-artifact-format-v1.md)
  is the single normative byte-layout authority. ADR-008 owns architecture and trust decisions; the
  Phase 0 plan links to the format and owns remaining work.
- Stale future-tense claims were replaced with the exact landed/unwired state. Repeated Cycle 5
  progress prose in the Phase 0 tail was reduced to a link.
- The aggregate state-table identity name, 160-byte V2 header, absence of a whole-body digest,
  mutable object budget, test-only encoders, and non-nullable SUM invariant agree across code and
  documents.
- The format detail is justified compatibility documentation, not an implementation diary. Broader
  lifecycle, delivery, LSM, and soak material remains in its existing authority rather than being
  copied into another design.

### 6. Tests and checks

**Result: pass for the changed scope.**

| Command/check | Result |
|---|---|
| `cargo fmt --all -- --check` | PASS |
| `git diff --check` | PASS |
| `cargo check -p laminar-db --lib --no-default-features` | PASS |
| `cargo clippy -p laminar-db --lib --tests --no-default-features -- -D warnings` | PASS |
| `cargo test -p laminar-db --lib --no-default-features artifact_v1` | PASS, 18/18 |
| `cargo test -p laminar-db --lib --no-default-features vnode_partial::v2` | PASS, 12/12 |
| Frozen aggregate, outer V2, and composed FULL/DELTA tests | PASS, 3/3 |
| Independent exact cluster admission command below | PASS twice after final visibility cleanup, 1/1 each |

The independent admission command was:

```text
cargo test -p laminar-db --lib --no-default-features --features cluster \
  db::tests::cluster_query_shape_admission_is_pre_mutation_and_mode_derived -- --exact
```

It confirmed that keyed/windowed/stateful shapes still reject with `[LDB-4007]`, while the admitted
stateless and single global aggregate cases retain their existing behavior. An earlier root command
used the short test name together with `--exact`; it matched zero tests and is explicitly not
evidence. The fully qualified independent command corrected that mistake.

The vectors cover exact bytes, empty/null-only state, canonical keys, FULL/DELTA ancestry, semantic
corruption after digest repair, duplicate/order/vnode errors, limits, cumulative budget exhaustion,
all-offset truncation, hostile structural mutations, expected-context mismatch, and arbitrary
bounded no-panic input. They do not cover a production fetch/resolver, depth 8/9, whole-batch
rollback, restore publication, delivery fault matrix, LSM behavior, full workspace/integration
services, performance qualification, or independent production soak.

## Owned follow-ups before the next implementation gate

- **DKS-P1-001 / distributed-state lifecycle:** install the sole trusted sealed outer-plus-inner
  composition path, make budget reuse structural, remove reader dead-code allowances, and keep it
  private until capability negotiation is ready.
- **Artifact/restore owner:** implement reserve-before-fetch, partial-download rejection, chain
  resolver, exact referent checks, shadow ingestion, atomic graph publication, and crash cleanup.
- **Aggregate executor owner:** implement and model-check the whole-batch checked COUNT/SUM mutation
  and output transaction in embedded/reference form before cluster admission.
- **Storage/performance owner:** run equivalent exact-pin Fjall and RocksDB qualification, select one
  from evidence, and delete the rejected adapter.
- **Connector/delivery owner:** complete Kafka source reconciliation, writer fencing, partition
  markers, ambiguous-commit tests, and the supported ALO matrix; do not infer exactly-once.
- **Independent soak owner:** run the unchanged release artifact under the frozen charter only after
  every prerequisite gate passes, retaining invalid and failed attempts.

## Next-cycle review plan

Cycle 6 may implement the backend-neutral qualification model and isolated exact-pin Fjall/RocksDB
spikes. It must not add either backend to runtime crates or relax admission. Its closing reviewers
must repeat:

1. **AI slop:** verify workload semantics, pins, runner identity, thresholds, and every result from
   raw artifacts; reject invented precision and candidate-profile numbers presented as evidence.
2. **Over-engineering/hot path:** keep adapters behind the standalone spike boundary, challenge
   every tuning knob, and measure write stalls, compaction, snapshot overlap, and tail latency.
3. **Unused code:** exercise both adapters through the same model, then retain neither as production
   code; assign/remove every ignored path or temporary allowance.
4. **Production readiness:** audit corruption, crash, ENOSPC, cancellation, resource ceilings,
   recovery RTO, operability, format upgrade/rollback, and evidence ownership while production stays
   blocked.
5. **Documentation:** record raw evidence and selection criteria by reference, remove superseded
   backend speculation, and avoid duplicating the normative state format.
6. **Tests:** require deterministic model digests, differential traces, fault ordinals, exact-limit
   and max-plus-one cases, subprocess crash evidence, resource sampling, long endurance, and explicit
   invalid-run handling.

Any correctness, resource, compatibility, delivery, owner, or independent-soak gap keeps production
and cluster admission blocked.
