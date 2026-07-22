# Cluster keyed/stateful operator validation — 2026-07-22

**Branch:** `feature/distributed-keyed-state-adr`

**Baseline:** `1e2f8429` (`main`, 2026-07-22)

**Scope:** admission and lifecycle validation only; no cluster capability is enabled by this work.

## Verdict

The reported fail-closed boundary is genuine, with three qualifications:

1. It applies to cluster `CREATE STREAM`. Cluster materialized views are rejected with
   `[LDB-4007]` even when their query is stateless.
2. The diagnostic's generic wording is older than part of the implementation. A grouped aggregate
   already has pre-aggregate vnode shuffle and aggregate-specific per-vnode capture, restore, and
   revoke code. It remains rejected because its live groups are held in unbounded operator-owned
   maps with no enforceable byte budget or spillable hot-state tier. Windows and joins have larger
   lifecycle gaps.
3. Restoring operator admission alone would not certify every runnable pipeline. Cluster delivery
   is currently at-least-once only, source/sink topology is checked independently, and there is no
   built-in cluster-admissible FullChangelog sink for retraction output.

The current cluster admission matrix is therefore:

| SQL shape | Cluster `CREATE STREAM` | Embedded/single-node | Exact cluster reason |
|---|---:|---:|---|
| Projection/filter | Admitted | Supported | Stateless path |
| One ungrouped aggregate stage containing direct `COUNT`/`SUM`/`AVG`/`MIN`/`MAX` calls | Admitted, subject to the restrictions below | Supported | Singleton global state is routed to vnode 0 and uses the exact incremental path |
| `GROUP BY`, without a window | `[LDB-4007]` | Supported | Group map has no hard live-state byte budget or spill tier |
| Running tumbling/hopping aggregate | `[LDB-4007]` | Supported | Window columns become grouping keys and hit the same grouped-state rejection |
| `EMIT ON WINDOW CLOSE` / `EMIT FINAL` | `[LDB-4007]` | Supported | Whole-operator window state has no vnode lifecycle |
| Analytic/window-frame operator | `[LDB-4007]` | Supported | No vnode-keyed state lifecycle |
| Stream, ASOF, temporal, temporal-probe, lookup, or changelog join | `[LDB-4007]` | Supported when the local operator's own boundedness contract is met | No co-partitioned operator/output-state lifecycle |
| Any materialized view | `[LDB-4007]` | Supported | Distributed materialized output and read ownership are not certified |

“One global aggregate” means one logical aggregate node per stream query, not one aggregate
expression or one such stream in the cluster. That node may contain multiple direct aggregate
calls. It must be constructible as `IncrementalAggState`; have no group columns or `DISTINCT`; and
use functions classified as `count`, `sum`, `avg`, `min`, or `max`. `MIN`/`MAX` over a changelog is
also rejected because the counted multiset is unbounded. Derived or nested shapes that create
multiple stages or miss the exact incremental path fail closed.

## Code validation

### Admission is mode-derived and pre-mutation

`LaminarDB::handle_create_stream` calls both cluster validators after reserving the catalog name
but before committing catalog, planner, subscription, connector, or table state. The reservation is
rolled back on error. The validator checks configured runtime mode, not the current number of
owners, so a configured one-owner cluster has the same safety boundary as a multi-owner cluster.

Evidence:

- [`handle_create_stream`](../../crates/laminar-db/src/ddl.rs) calls
  `validate_cluster_query_shape_before_plan` and `validate_cluster_query_shape` before the planned
  query is registered.
- [`validate_cluster_query_shape_before_plan`](../../crates/laminar-db/src/ddl.rs) rejects
  `DISTINCT`, window-close/final emission, analytic frames, AI in-flight state, temporal filters,
  and specialized join routes.
- [`validate_cluster_query_shape`](../../crates/laminar-db/src/ddl.rs) rejects every join plan and
  DataFusion join fallback, multi-stage aggregates, incomplete shuffle/ownership configuration,
  and aggregates outside the exact incremental execution path.
- [`CLUSTER_STATE_LIFECYCLE_UNSUPPORTED`](../../crates/laminar-core/src/error_codes.rs) is
  `[LDB-4007]`.
- `handle_create_materialized_view` has an independent blanket cluster rejection because an MV's
  retained output is state too.

Startup is a second enforcement point. Persisted catalog entries are revalidated before connector
creation, and residual cluster materialized state prevents startup. This prevents an old manifest
or injected in-memory catalog from bypassing current DDL admission.

### Aggregate support is partial substrate, not an admitted feature

[`IncrementalAggState::cluster_state_rejection`](../../crates/laminar-db/src/aggregate_state.rs)
rejects every `num_group_cols != 0` with the current, precise reason: operator-owned map state has
no live-state byte budget. That check is the immediate blocker for plain and running-window keyed
aggregates.

The same type nevertheless implements:

- a stable key hash to vnodes and pre-aggregate row shuffle;
- per-vnode full and delta checkpoint encoding;
- base-plus-delta restore and acquisition;
- state removal for revoked vnodes; and
- assignment-aware shuffle/barrier integration through `SqlQueryOperator`.

That partial restore path has an additional integrity gap which does not affect an admitted keyed
cluster workload today because admission remains closed. Restore validates the internal shape and
query fingerprint of an aggregate slice, but it does not prove that every decoded key hashes to
the vnode named by the checkpoint artifact. The uninitialized staging path also retains base and
delta payloads without their vnode identity, so it cannot perform that proof after the SQL key
schema is available. An initialized apply path can therefore merge a well-formed slice under the
wrong vnode label. Phase 0 must centralize the existing Arrow-row/xxh3 partition codec, preserve
the vnode tag for every staged base and delta, and preflight the complete chain against the planned
key schema and vnode count before mutating live state.

The feature branch began that work in `562cc590`: capture, delta tracking, `last_emitted`
bucketing, recovery bookkeeping, and revoke now call one allocation-free mapping over the existing
encoded `OwnedRow`, and an active delta generation rejects a changed or zero vnode count before
capture/revoke mutation. This is partition-path hardening only. It does not add a persisted vnode
count, a count-rotation lifecycle, payload membership validation, or tagged restore chains.

The audit found that vnode membership is coupled to several other restore invariants and therefore
must not be patched as an isolated assertion:

- the raw aggregate payload has no envelope carrying partition ABI, vnode count and claimed vnode,
  canonical key-schema fingerprint, or accumulator-state schema/version;
- accumulator validation checks payload count and row count, but an empty payload can be decoded as
  absent state and rebuild a stateful accumulator at its default unless expected state fields are
  known independently;
- `last_emitted` keys are cast toward planned types and an empty keyed tuple can take the global
  sentinel path, so exact arity/type checks must precede conversion;
- a FULL apply replaces only keys present in its payload; a stale live key in the same vnode but
  absent from the authoritative image can survive unless the complete vnode namespace is replaced;
- at the validated baseline, delta bucketing remembered one vnode count while capture accepted
  another. `562cc590` now rejects that local drift, but artifacts still do not bind their vnode
  count or define an explicit generation-rotation lifecycle;
- an initialized chain is transactionally decoded off-side, but staged bases and deltas lose chain
  identity and graph-level application can mutate an earlier operator before a later preflight
  fails; and
- ordinary grouped batch processing mutates groups and accumulators sequentially, so a late error
  can leave an in-memory partial batch. Cluster execution must recover from the sealed cut rather
  than retry that batch locally until the managed state write is one atomic transaction.

The safe design is a prepared vnode-restore transaction: validate the complete tagged chain,
planned key and accumulator schemas, resource reservation, membership, disjointness, and
authoritative replacement set first; then publish it with an infallible commit. Legacy raw payloads
may remain only for the currently admitted singleton global aggregate. A partial validator over the
existing raw keyed payload is not a production restore contract.

This code is useful implementation substrate, but it does not make the operator production safe.
The current one-million-group guard counts entries, not bytes. The live `groups`, changelog
emission state, distinct/counting side state, dirty generations, Arrow buffers, and allocator
retention are not governed by one truthful byte budget, and there is no local spillable
working-state engine. Capture also walks and serializes operator maps rather than freezing a
state-engine generation in constant time. Initial delta FULL capture can scan all groups and all
`last_emitted` entries once per touched vnode; delta discovery and revoke also scan live maps
synchronously. That approaches `O(vnodes x groups)` work at the checkpoint/rebalance cut and is a
direct tail-latency blocker, not merely an implementation detail.

### Windows and joins have additional gaps

`EowcQueryOperator`/`CoreWindowState` checkpoint local whole-operator state, but do not override the
graph's per-vnode capture/apply/drop hooks. Distributed windows also need vnode-owned event-time
timers, watermark/frontier state, late-data policy, firing/output bookkeeping, and atomic cleanup.

Join implementations checkpoint local state but likewise do not implement the graph's vnode hooks.
A production distributed join additionally needs both inputs shuffled with one canonical join-key
encoding, side-specific vnode tables and time indexes, watermarks/retention for bounded cleanup,
multiset or changelog weights, unmatched-row timers for outer joins, and assignment-fenced output
ownership. Merely serializing the current hash tables per vnode would leave routing, bounds, tail
latency, and recovery correctness unresolved.

The default no-op vnode methods on `GraphOperator` are safe only because DDL admission is explicit
and fail-closed. They are not a scalable proof mechanism: a new stateful operator can compile
without declaring a distribution contract. The ADR replaces that implicit convention with a
planner-visible capability descriptor and runtime assertions.

### Delivery and connector constraints are independent guards

Cluster runtime admission currently rejects `BestEffort` and `ExactlyOnce`; only `AtLeastOnce` is
accepted. A cluster source must be non-ephemeral and `Splittable`, and a cluster sink must be
`DurableAtLeastOnce + MultiWriter`. At this baseline, Kafka is the only built-in external source
with the required cluster topology. Several built-in append sinks qualify, but no built-in
cluster-admissible sink accepts `FullChangelog`.

Evidence is the typed capability vocabulary in
[`connector.rs`](../../crates/laminar-connectors/src/connector.rs), its exhaustive compatibility
checks in [`pipeline_lifecycle.rs`](../../crates/laminar-db/src/pipeline_lifecycle.rs), and each
connector's registered descriptor—not an inference from connector names.

That does not block every grouped stream. Ordinary `CREATE STREAM` queries are registered as
non-incremental and emit append-result rows, so their compatible append-sink paths can be certified.
The current aggregate emits a repeated full running snapshot of all groups each processing cycle,
not only changed groups or a final monotonic result. It does mean retraction/full-changelog modes
cannot be called production-ready merely because the operator can compute them. A mutable cluster
sink also needs key-affine assignment and stale-writer fencing; a `MultiWriter` flag alone does not
establish ownership.

The current source handoff already binds checkpoint attempt, source assignment version, cursors,
per-source watermarks, cluster watermark, and recovery frontier. SQL-key vnode placement is a
separate shuffle from Kafka source-partition placement, so a stateful checkpoint must preserve both
authorities. Under at-least-once, recovered state and the source cursor share a coherent sealed cut,
but external records flushed after that cut may repeat. Local LSM durability cannot turn this into
end-to-end exactly-once; that later guarantee needs connector/provider commits fenced by the same
leader/assignment proof already used by the checkpoint coordinator.

The first candidate Kafka-to-Kafka append path has one more certification blocker. Kafka source is
replayable and splittable, and the append sink declares durable at-least-once multiwriter
capability, but aggregate output is currently serialized without a replay-stable logical operation
ID or ownership-provenance envelope. The independent oracle therefore cannot tell a legal replay
from internal double application or work admitted by a stale owner. Sink flush ordering and
connector capability flags do not replace that evidence.

### Fjall is historical, not current infrastructure

The current baseline contains no Fjall dependency or state-tier module. Fjall 3.1 was previously
used by the v0.26-era optional `state-tier` feature as a rebuildable cold cache for demoted
checkpoint slices/groups. The current [`CHANGELOG.md`](../../CHANGELOG.md) records why commit
`1e2f8429` removed it: a correctness defect allowed demotion to clear vnode dirtiness before
durable checkpoint authority, so a failed attempt could recover older bytes. The deleted tier was
not always-current working state and is not safe substrate to restore wholesale.

Its archived benchmark is useful warning data, not production qualification. It used individual
point inserts and uniform cold reads; the wrapper also read before write/remove for byte gauges and
copied returned values. Formal target-Linux/NVMe testing never ran. Current Fjall 3.1.8 has useful
atomic batches, snapshots, range scans, and sorted ingestion, but lacks native multi-get/range
delete and a mature supported memory/compaction observability surface. The ADR therefore requires
the same real state workload and fault gates against Fjall and RocksDB, then selects one production
backend rather than assuming the historical dependency is fit.

## Empirical validation

The validation uses a real configured cluster builder with a controller, shared object-store
checkpoint backend, shuffle sender/receiver, and an eight-vnode registry. It intentionally assigns
all vnodes to one owner: rejection in this setup proves that admission follows configured mode and
does not accidentally become permissive when only one process is present.

The repository test `cluster_query_shape_admission_is_pre_mutation_and_mode_derived` exercises:

- `[LDB-4007]` for a plain keyed aggregate, global and keyed windows, window-close emission,
  bounded interval join, temporal filter, derived/nested aggregate, stateless MV, and aggregate MV;
- successful cluster creation of a filtered projection and one global `SUM` stage;
- absence of catalog/planner/connector/subscription residue after every rejected DDL;
- startup rejection of residual materialized state; and
- separation from embedded mode, where local query rules apply instead of `[LDB-4007]`.

The rendered error is an outer generic invalid-operation `[LDB-0005]` containing the specific
`[LDB-4007]` cluster lifecycle code and reason. Tests search for the specific nested code.

The integration test `sealed_materialized_view_manifest_is_rejected_by_every_node_after_restart`
injects a persisted keyed MV and verifies that each node rejects both initial startup and restart
with `[LDB-4007]`.
Local unit/integration tests separately execute grouped aggregates, window-close aggregates, ASOF
joins, temporal joins, and incremental changelog joins. Those local tests establish operator
availability; they do not imply that every unbounded SQL join is semantically admissible.

Clean targeted commands use `--no-default-features --features cluster` throughout. Results:

| Test filter | Result | Evidence |
|---|---:|---|
| `db::tests::cluster_query_shape_admission_is_pre_mutation_and_mode_derived` (`--lib --exact`) | PASS, 1/1 | Exact admission matrix and no-residue rollback; rerun after adding explicit plain-keyed and global-window assertions |
| `pipeline_lifecycle::connector_admission_tests::source_contract_admission_matrix_is_fail_closed` (`--lib --exact`) | PASS, 1/1 | Exhaustive delivery/consistency/topology source matrix |
| `pipeline_lifecycle::connector_admission_tests::sink_contract_admission_matrix_is_fail_closed` (`--lib --exact`) | PASS, 1/1 | Exhaustive delivery/consistency/topology/output-mode sink matrix |
| `incremental_emit_snapshot_matches_full_recompute` (`--test incremental_emit --exact`) | PASS, 1/1 | Embedded grouped aggregate matches full recomputation across four multi-key batches |
| `db::tests::test_nullif_float_with_int_literal_runs_without_error` (`--lib --exact`) | PASS, 1/1 | Embedded tumbling window closes and emits |
| `db::tests::asof_join_in_materialized_view_emits_backward_match` (`--lib --exact`) | PASS, 1/1 | Embedded ASOF MV produces expected backward matches |
| `operator::interval_join::tests::test_checkpoint_roundtrip` (`--lib --exact`) | PASS, 1/1 | Local interval-join buffered state restores and matches a later batch |
| `rebalance::dead_aggregate_owner_advances_to_a_successor_recovery_quorum` (`--test cluster_integration --exact`) | PASS, 1/1 | Multi-node global aggregate checkpoints, loses vnode-0 owner, and reaches successor recovery quorum |
| `failures::zero_vnode_workers_start_idle_without_joining_assignment_quorum` (`--test cluster_integration --exact`) | PASS, 1/1 | Three-node stateless stream runs with zero-vnode workers |
| `failures::sealed_materialized_view_manifest_is_rejected_by_every_node_after_restart` (`--test cluster_integration --exact`) | PASS, 1/1 | Persisted unsupported MV is rejected by every node before and after restart |

The exact invocations and timings are repeated in the Cycle 0 review. Real multi-process soak,
MinIO/object-store integration, Kafka/Docker, and server HTTP/Flight admission suites were not run;
they require heavier process or external-service setup and are not represented as passing evidence.
The ADR/plan consequently forbid a production-ready claim until an independently reviewed,
black-box release-candidate soak passes with real source/object-store/sink dependencies.

The admission-neutral hardening in `562cc590` was then checked separately:

| Current-branch check | Result | Evidence |
|---|---:|---|
| `aggregate_state::vnode_partition_tests` (cluster lib-test binary) | PASS, 4/4 | Existing raw capture/merge/idempotence plus new shuffle/capture/drop parity and pre-mutation drift rejection; not keyed-envelope validation |
| `aggregate_state::tests::drop_vnodes_purges_revoked_keeps_sibling` | PASS, 1/1 | Revoke retains sibling-vnode state after the fallible count check was added |
| `aggregate_state::tests::global_changelog_delta_checkpoint_roundtrips` | PASS, 1/1 | The admitted global aggregate remains pinned to vnode 0 |
| `db::tests::cluster_query_shape_admission_is_pre_mutation_and_mode_derived` | PASS, 1/1 | The `[LDB-4007]` feature matrix remains closed while stateless/global shapes remain admitted |
| `aggregate_state::tests::embedded_float_grouping_remains_supported_without_partition_codec_gate` (`--no-default-features`) | PASS, 1/1 | Embedded planning and execution still accept a float key excluded from cluster partition ABI v1 |

Both cluster and no-feature `cargo check` and `cargo clippy -D warnings` configurations passed, as
did formatting and diff checks. These focused results do not exercise keyed cluster restore.

These are admission and focused operator tests, not a production certification. No existing test
demonstrates a distributed keyed operator processing remote rows through crash, restore, and
rebalance, because that path is deliberately unreachable.

## The missing capability, precisely

LaminarDB does **not** need a second cluster coordinator or a generic “checkpoint system.” It needs
a managed keyed working-state capability connected to the distributed execution contract:

1. **Stable distribution ABI:** one canonical typed key codec shared by shuffle, state, and restore;
   exact encoded-byte/hash/vnode vectors; fixed vnode count; stable operator and state-table IDs;
   schema/version compatibility; artifact-to-vnode membership validation; and an explicit
   global-vnode convention.
2. **Batch hot-state API:** local low-latency point/multi-get, prefix/range scan, atomic write batch,
   timer index, range delete, and truthful memory/disk accounting. The public record path must not
   await object storage per row.
3. **Bounded resources:** reservations before mutation and hard limits covering state-engine
   caches/write buffers, operator side state, dirty/frozen generations, timers, retained output,
   Arrow batches, and compaction debt; bounded backpressure followed by a controlled fault instead
   of OOM.
4. **Checkpoint bridge:** atomic state mutation plus dirty-journal update, cheap generation freeze
   at an aligned barrier, asynchronous full/delta artifact production, exact-attempt sealing, and
   safe retry/rebase after failed capture.
5. **Ownership lifecycle:** install and validate acquired vnode chains before input is released;
   fence the old owner before revoke; suppress or bounded-buffer rows for restoring vnodes; drop
   local ranges only after authority changes.
6. **Operator contracts:** aggregates externalize accumulators/emission state; windows externalize
   event-time timers and firing state; joins co-partition both inputs and externalize both indexed
   sides plus unmatched-output state.
7. **Materialized output lifecycle:** separately partition, checkpoint, restore, and serve retained
   MV output before cluster MVs can be admitted.
8. **Delivery composition:** bind supported source handoff, operator state/timers/output identity,
   compatible sink mode, and checkpoint-tail ordering; keep unsupported changelog and exactly-once
   combinations fail-closed.
9. **Proof:** differential semantics, deterministic fault injection at every cut, skew and quota
   tests, process-death/rebalance output oracles, recovery compatibility, and published latency and
   resource profiles.

The existing `StateBackend` must not be mistaken for item 2. Its contract explicitly persists
immutable per-checkpoint-attempt vnode artifacts and an exact-attempt durability seal. It has no
hot get/put/scan/write-batch/timer interface and is correctly kept as remote recovery authority.

## Documentation audit

| Artifact | Disposition | Reason |
|---|---|---|
| `docs/AGENT_KNOWLEDGE.md` | Remove | Stale Claude handoff memory: it describes already-completed transport/barrier work as future work, uses machine-local links, and is not authoritative project documentation |
| `docs/plans/checkpoint-production-correctness-2026.md` | Keep, cross-link | Current and relevant; correctly distinguishes checkpoint artifacts from hot keyed state |
| `docs/plans/cluster-production-readiness.md` | Keep, narrow | Relevant umbrella plan; the focused ADR/plan becomes the source of truth for keyed operators |
| `docs/ARCHITECTURE.md` | Correct | Its JSON/checkpoint wording and admission description are stale |
| `docs/SQL_REFERENCE.md` and `README.md` | Correct | State the exact stream/global-aggregate/MV boundary |
| `.claude/fix-plans/state-backend.md` | Remove locally | Stale Claude plan: it conflated artifact `StateBackend` with live state, proposed speculative OpenRaft/config tiers, and described already-changed code; the path is an ignored junction, so deletion is not a branch commit |
| ignored `docs/adr` and `docs/research` content | Retain outside branch authority | The accepted cache/checkpoint and implemented incremental-emit notes still explain current code; schema research concerns another active area. Their cluster assertions do not override the tracked validation/ADR |

No tracked `docs/research` directory exists on this baseline. Relevant primary sources are retained
as citations in the design ADR; superseded narrative is not copied into a new research dump. The
removed Claude fix-plan lived through a junction at
`C:\Users\sujit\.claude-private-configs\laminardb\.claude`; repository Git cannot recover it.

## Confidence and limitations

Confidence is high for the admission boundary because independent pre-plan, post-plan, startup,
and MV guards converge on the same error and targeted tests execute those paths. Confidence is
also high that a common managed hot-state tier is absent: the checkpoint backend explicitly denies
that role and no local LSM dependency or equivalent API is present in the current tree. Fjall's
deleted cold-cache history does not change that conclusion.

This report does not claim production behavior for an unreachable distributed stateful data path.
It also does not claim cluster exactly-once; cluster delivery remains at-least-once and
`[LDB-0013]` continues to guard exactly-once independently of this feature.
