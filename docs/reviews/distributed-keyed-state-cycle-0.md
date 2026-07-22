# Distributed keyed state — Cycle 0 review

- **Date:** 2026-07-22
- **Scope:** validation, ADR, plan, admission regression tests, and documentation audit
- **Verdict:** **APPROVE WITH OWNED FOLLOW-UPS** for the design cycle only
- **Admission:** unchanged; every keyed/window/join/MV cluster guard remains closed

This verdict approves the evidence and proposed direction for owner review. It does not declare
ADR-008 accepted, Phase 0 complete, Fjall or RocksDB qualified, or any distributed stateful
operator production-ready.

## Reviewed changes

- `0f4b37ff` — admission validation, explicit keyed/global-window regression cases, stale tracked
  Claude handoff removal;
- `3d9e93db` — managed vnode-keyed state ADR;
- `b75e881f` — phased operator plan; and
- `4278dd33` — LSM qualification, hot-path scheduling, and delivery/source/sink composition.

The current tree was checked against the baseline `1e2f8429`. Production implementation was
deliberately out of scope.

## Review passes

### 1. AI-slop and evidence

**Result: pass for this cycle.**

- Code inspection replaced the generic “no vnode lifecycle” explanation with the actual split:
  aggregate shuffle/capture/restore/revoke exists, but live group state is unbounded; windows and
  joins have wider timer/co-partition/output lifecycle gaps.
- The report distinguishes outer `[LDB-0005]` rendering from nested `[LDB-4007]`, one aggregate
  node from one aggregate expression, `CREATE STREAM` from blanket-rejected cluster MVs, and
  configured cluster mode from current owner count.
- Connector claims were checked against typed contracts and registrations. Cluster is ALO-only;
  Kafka is the sole current built-in splittable external source; no current cluster sink accepts
  FullChangelog. Ordinary append-result streams were not incorrectly described as sinkless.
- The initial assumption that Fjall is current was disproved. Git history and the changelog show
  that the former Fjall 3.1.5 cold tier was removed for checkpoint correctness; current HEAD has no
  Fjall dependency. Historical single-insert results are labelled warning data, not qualification.
- 2025–2026 system claims cite primary project documentation or original papers. CheckMate and the
  2026 CDC result are labelled preprints; no vendor architecture is treated as proof of LaminarDB
  correctness.
- No invented latency target was retrofitted to measurements. Phase 0 requires numerical SLO/RTO
  values to be committed before backend optimization.

### 2. Over-engineering

**Result: pass with one Phase 0 choice outstanding by design.**

- The plan retains existing vnodes, shuffle, aligned barriers, assignment/process fences,
  checkpoint artifacts, and exact-attempt seal. It does not add consensus, a control-plane rewrite,
  object-store-primary live state, unaligned checkpoints, dual writes, live record migration, or
  standby replicas.
- The working-state boundary is shared because aggregates, timers, and joins all need the same
  batch/checkpoint/ownership/resource invariants. A new crate is prohibited until dependency
  direction or a second non-DB consumer justifies it.
- Fjall and RocksDB adapters exist only for a time-bounded qualification. Phase 0 selects one and
  deletes the losing spike; maintaining two production LSMs is rejected.
- Materialized output and external exactly-once remain separate programs. Operator compute support
  cannot silently widen connector or MV support.

### 3. Unused and dead code

**Result: pass for changed code; follow-up owned by Phase 0 implementation lead.**

- No production code, dependency, feature flag, configuration, metric, adapter, or public API was
  added. The changed admission-test helper returns a message that every new assertion consumes.
- The unsafe historical Fjall tier remains deleted; the plan explicitly forbids reviving its
  read-before-write accounting, per-group Arrow IPC, or single point-operation wrapper.
- Phase 0 must resolve the currently discarded `has_ownership_partitioned_state` result and remove
  map-era dirty/capture scaffolding only after differential equivalence. This is an explicit audit
  item, not pre-emptive cleanup in a documentation cycle.

### 4. Production readiness

**Result: not yet production-ready, correctly fail-closed.**

Blocking evidence, owned by the Phase 0/backend and connector leads:

1. commit a numerical target hardware/workload/SLO/RTO profile;
2. qualify Fjall 3.1.8 and a pinned RocksDB binding under identical batched state, timer, export,
   restore, cleanup, crash, disk-full, resource, and tail-latency gates; select one;
3. prove bounded compute-thread deferral, queue pressure, state/cache/memtable/journal/snapshot/OS
   memory accounting, compaction debt, and local-disk cleanup;
4. certify a Kafka/state/append-sink ALO scenario with CP-5 flush-before-source-seal ordering and a
   no-gap/allowed-duplicate oracle;
5. keep retraction/FullChangelog publication closed until a suitable multiwriter log sink or
   assignment-fenced mutable-sink lifecycle exists;
6. pass deterministic crash, multi-process, object-store, connector, rolling-upgrade, security, and
   `1 -> 3 -> 2` rebalance evidence before any admission flag changes; and
7. before any production-ready claim, pass the separately chartered black-box soak using unchanged
   release-candidate bits, real source/object-store/sink dependencies, an external oracle, retained
   raw artifacts, and a reviewer independent of the implementation. An integration run, backend
   soak, or canary is not a substitute.

Exactly-once remains independently rejected by `[LDB-0013]`. A local LSM fsync does not replace an
exact-certified source and a leader-term-fenced external sink transaction.

### 5. Documentation

**Result: pass.**

- The validation report is evidence authority, ADR-008 is design authority, and the phased plan is
  sequencing authority. README, architecture, SQL reference, and checkpoint plan link to them.
- The stale tracked `docs/AGENT_KNOWLEDGE.md` was removed. The 236-line duplicate cluster roadmap
  was reduced to an index; its absent lookup-plan link, unimplemented “settled Postgres” choice,
  duplicate keyed-state design, and overstated citations are no longer current authority.
- No second research diary was generated. Ignored cache/checkpoint/incremental-emit ADRs and schema
  research were retained because they still describe current code or another active area; their
  cluster claims are not branch authority. The obsolete Claude `fix-plans/state-backend.md` was
  removed from the external `.claude` junction because it conflated checkpoint artifacts with live
  state and proposed stale OpenRaft/config work. That local deletion is not recoverable through
  repository Git.
- The ADR and plan are intentionally detailed deliverables, but repeated current-state prose was
  consolidated into links. The next cycle must shorten or supersede text it invalidates.

### 6. Tests and checks

**Result: pass for the scoped validation.**

All Rust commands used `--no-default-features --features cluster`:

| Command/filter | Result |
|---|---|
| `cargo test -p laminar-db --lib ... db::tests::cluster_query_shape_admission_is_pre_mutation_and_mode_derived -- --exact` | PASS, 1/1; 1,625 filtered |
| `cargo test -p laminar-db --lib ... pipeline_lifecycle::connector_admission_tests::source_contract_admission_matrix_is_fail_closed -- --exact` | PASS, 1/1; 1,625 filtered |
| `cargo test -p laminar-db --lib ... pipeline_lifecycle::connector_admission_tests::sink_contract_admission_matrix_is_fail_closed -- --exact` | PASS, 1/1; 1,625 filtered |
| `cargo test -p laminar-db --test incremental_emit ... incremental_emit_snapshot_matches_full_recompute -- --exact` | PASS, 1/1 |
| `cargo test -p laminar-db --lib ... db::tests::test_nullif_float_with_int_literal_runs_without_error -- --exact` | PASS, 1/1 |
| `cargo test -p laminar-db --lib ... db::tests::asof_join_in_materialized_view_emits_backward_match -- --exact` | PASS, 1/1 |
| `cargo test -p laminar-db --lib ... operator::interval_join::tests::test_checkpoint_roundtrip -- --exact` | PASS, 1/1 |
| `cargo test -p laminar-db --test cluster_integration ... rebalance::dead_aggregate_owner_advances_to_a_successor_recovery_quorum -- --exact` | PASS, 1/1, 7.91 s |
| `cargo test -p laminar-db --test cluster_integration ... failures::zero_vnode_workers_start_idle_without_joining_assignment_quorum -- --exact` | PASS, 1/1, 4.20 s |
| `cargo test -p laminar-db --test cluster_integration ... failures::sealed_materialized_view_manifest_is_rejected_by_every_node_after_restart -- --exact` | PASS, 1/1, 6.89 s |

One first MV invocation used the wrong `rebalance::` module prefix and matched zero tests. It is
recorded as invalid evidence; `--list` identified the correct `failures::` name and the corrected
exact invocation above passed 1/1. Temporary diagnostic prints used during admission inspection
were reversed before commit.

Repository checks also passed:

- `cargo fmt --all -- --check`;
- `cargo clippy -p laminar-db --lib --no-default-features --features cluster -- -D warnings`
  (completed in 1 min 31 s);
- `git diff --check`; and
- a relative-link check over all nine changed Markdown deliverables/public documents.

Real multi-process soak, MinIO/object-store integration, Kafka/Docker, server HTTP/Flight, chaos,
and performance suites were not run. They are prerequisites for a later production cycle, not
represented as passing here.

## Next-cycle review plan

Before Phase 1 can begin, the Phase 0 implementation lead must open the cycle with named human
owners for the backend and connector gates above. The closing reviewer must then repeat all six
passes and specifically:

- compare the committed workload/SLO profile with raw, reproducible Fjall/RocksDB results;
- search for per-row blocking/futures, unbounded queues/maps, hidden native or pinned memory, and
  per-vnode physical databases;
- reject abstraction or configuration retained only for the losing backend spike;
- inspect every crash/fault oracle for zero matched cases, retries, and allowed-duplicate wording;
- verify source assignment, SQL vnode assignment, state/timer/output cut, sink flush, and durable
  decision across the same fault matrix;
- verify that the independent soak charter was fixed before results, cannot use LaminarDB internals
  as its oracle, invalidates unexplained gaps/retries, and requires a full rerun after relevant
  release-binary or configuration changes; and
- leave `[LDB-4007]` closed if any correctness, resource, tail-latency, operability, compatibility,
  documentation, or test owner is unresolved.
