# Cluster keyed/stateful operator validation — 2026-07-22

**Branch:** `feature/distributed-keyed-state-adr`

**Baseline:** `1e2f8429` (`main`, 2026-07-22)

**Scope:** admission and lifecycle validation only; no cluster capability is enabled by this work.

**Current authority:** the [Cycle 40 package design](../architecture-decisions/tidesdb-local-state-successor-design.md)
selects the official `tidesdb/tidesdb-rs` binding, Cargo package `tidesdb`, as the intended
integration line. [Cycle 41 T0](tidesdb-rs-t0-source-closure-2026-07-25.md) stops exact v0.11.1/
native 9.3.6 pending a new official package. Neither decision changes this validation evidence or
cluster admission.

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

The branch later experimented with a generic Arrow schema descriptor (`b0ac1a7a`) and strict
single-record-batch IPC helper (`12a34c38`). Cycle 3 review (`1e8b1a59`, `f4ded97b`) narrowed the
former to the bounded,
opaque routing identity actually consumed by `PartitionKeySchemaV1` and removed the latter. The
generic helper used Arrow 57.2's `StreamReader`, which can allocate from IPC-declared metadata/body
lengths before proving those bytes exist. The reviewed first `COUNT(*)`/`SUM(Int64)` artifact is
specified to use a bounded Laminar row codec and will not call Arrow on restored state. Any future
IPC codec must first reserve a global restore budget and preflight hostile framing. These changes
remain admission-neutral and do not close the runtime lifecycle gap.

The audit found that vnode membership is coupled to several other restore invariants and therefore
must not be patched as an isolated assertion:

- the raw aggregate payload has no envelope carrying partition ABI, vnode count and claimed vnode,
  canonical key-schema fingerprint, or accumulator-state schema/version;
- the payload directly archives the live `AggStateCheckpoint` Rust type rather than a bounded,
  independently versioned wire DTO, so its compatibility surface is implicit;
- FULL and DELTA rows inherit `AHashMap`/`AHashSet` iteration order, so identical logical state does
  not have canonical bytes across insertion order, process hash seed, or restart;
- columnar checkpoint writers synthesize every Arrow IPC field as nullable `c0...cN` instead of
  preserving the accumulator's declared semantic fields. For example, a non-nullable COUNT state
  is physically described as nullable, so one descriptor cannot honestly identify both forms;
- the ordinary IPC helper returns the first record batch and does not require exactly one batch,
  the end-of-stream marker, or complete byte consumption. Exact persisted restore must reject a
  second batch/trailing bytes and validate declared lengths/decoded expansion before Arrow parsing;
- accumulator validation checks payload count and row count, but an empty payload can be decoded as
  absent state and rebuild a stateful accumulator at its default unless expected state fields are
  known independently;
- `last_emitted` keys are cast toward planned types and an empty keyed tuple can take the global
  sentinel path, so exact arity/type checks must precede conversion;
- `last_updated_ms` is written and round-tripped but has no aggregate execution, TTL, or eviction
  reader, so copying it into a durable managed schema would ossify vestigial state;
- the resolved DataFusion 52.3 non-distinct integer/decimal SUM uses wrapping addition, while COUNT
  uses ordinary integer addition and the release profile does not enable overflow checks. The soak
  profile does enable them, so behavior can differ by build profile. A portable artifact cannot
  repair that arithmetic; the first managed vertical needs Laminar-owned checked semantics or an
  independently enforced input/state bound before a writer or admission proof exists;
- Arrow 57.2's row parser assumes converter-produced well-formed bytes and may panic on malformed
  input. Restored partition-key bytes must remain opaque for hash/LSM identity, or pass a separate
  strict decoder before any materialization; artifact bytes cannot be fed directly to `RowParser`;
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

The graph-level audit makes the restore gap more precise. `apply_rehydrated_vnodes` removes all
currently owned chains from the shared staging map before it decodes them, applies one vnode and
one operator at a time, and marks each vnode active immediately after that vnode's operators
return. A failure therefore discards the exact staged retry material, can leave an earlier
operator mutated, and can leave an earlier vnode active when a later vnode fails. The existing
`rehydration_apply_failure_faults_without_activating_vnode` test records this behavior explicitly:
the first test operator is applied before the second operator's injected failure, even though the
vnode remains `Restoring`. Revocation has the same sequential partial-graph boundary.

The current partial also has no authoritative operator inventory. It derives the operator names
from entries that happen to be present, so omission cannot distinguish an intentionally empty
state from a missing/corrupt lifecycle participant. `GraphOperator` supplies successful no-op
defaults for vnode apply/drop, allowing a stateful implementation without hooks to discard named
state silently if admission ever becomes permissive. `RehydratedVnode` retains only an epoch and
raw chain bytes; adoption drops the exact checkpoint attempt, checkpoint/target assignment
identity, vnode count, and owner-map identity, and uses `unwrap_or_default()` for a missing acquired
chain. Finally, an uninitialized SQL operator can queue bytes and be followed by the graph's
`Restoring -> Active` transition before asynchronous plan construction proves the exact aggregate
implementation and state schema. These are independent reasons that the existing apply path
cannot be reused for keyed admission.

The required replacement is the assignment-scoped prepare/publish protocol in
[ADR-008](../architecture-decisions/ADR-008-managed-vnode-keyed-state.md#whole-graph-publication-boundary):
authoritative roster and explicit empty state, whole-batch preflight, off-side shadows, exclusive
graph publication, and no activation after partial success. A validator patched onto the current
raw keyed payload would not satisfy that boundary.

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
the same real state workload and fault gates before selecting a backend rather than assuming the
historical dependency is fit. Cycle 40 later selected the official `tidesdb/tidesdb-rs` binding,
Cargo package `tidesdb`, as the TidesDB integration line. Cycle 41 stopped exact v0.11.1 at T0;
qualification and production admission remain outstanding.

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

The current branch's admission-neutral hardening was then checked separately:

| Current-branch check | Result | Evidence |
|---|---:|---|
| `state::partition_key::tests` | PASS, 15/15 | Typed row/hash/vnode ABI plus bounded routing-only schema identity, every admitted family golden, alias/order/nullability policy, dictionary hydration, and exact resource/type gates |
| `cargo test -p laminar-core --lib` | PASS, 562/562 | Complete core library regression set after the Cycle 3 safety follow-up |
| Generic strict IPC experiment | REMOVED after review | Arrow 57.2 parses attacker-declared lengths too early; artifact-specific preflight, a global encoded-byte charge, and separate task/global scratch charges remain explicit Phase 0 blockers |
| `aggregate_state::vnode_partition_tests` (cluster lib-test binary) | PASS, 4/4 | Existing raw capture/merge/idempotence plus new shuffle/capture/drop parity and pre-mutation drift rejection; not keyed-envelope validation |
| `aggregate_state::tests::drop_vnodes_purges_revoked_keeps_sibling` | PASS, 1/1 | Revoke retains sibling-vnode state after the fallible count check was added |
| `aggregate_state::tests::global_changelog_delta_checkpoint_roundtrips` | PASS, 1/1 | The admitted global aggregate remains pinned to vnode 0 |
| `db::tests::cluster_query_shape_admission_is_pre_mutation_and_mode_derived` | PASS, 1/1 | The `[LDB-4007]` feature matrix remains closed while stateless/global shapes remain admitted |
| `aggregate_state::tests::embedded_float_grouping_remains_supported_without_partition_codec_gate` (`--no-default-features`) | PASS, 1/1 | Embedded planning and execution still accept a float key excluded from cluster partition ABI v1 |
| `operator_graph::tests::rehydration_apply_failure_faults_without_activating_vnode` | PASS, 1/1 | Empirically confirms the current unsafe boundary: the first operator is mutated before a later failure, although the vnode stays `Restoring`; this is blocker evidence, not a desired regression contract |

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
5. **Ownership lifecycle:** preserve one exact committed-cut/target-assignment transition; require
   an authoritative operator roster and explicit empty state; preflight all acquired/revoked
   vnodes and prepare every operator off-side; publish only infallible graph-wide shard swaps under
   the execution fence; activate the complete set once; retain the exact transition on failure;
   fence the old owner before revoke; and drop local ranges only after authority changes.
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
| former `docs/AGENT_KNOWLEDGE.md` | Removed in `0f4b37ff` | Stale Claude handoff memory described completed transport/barrier work as future work and was not authoritative project documentation |
| `docs/plans/checkpoint-production-correctness-2026.md` | Keep, cross-link | Current and relevant; correctly distinguishes checkpoint artifacts from hot keyed state |
| `docs/plans/cluster-production-readiness.md` | Keep, narrow | Relevant umbrella plan; the focused ADR/plan becomes the source of truth for keyed operators |
| `docs/ARCHITECTURE.md` | Corrected in `706270dc` | Now distinguishes immutable checkpoint artifacts from absent live keyed state |
| `docs/SQL_REFERENCE.md` and `README.md` | Corrected in `706270dc` | State the exact stream/global-aggregate/MV boundary |
| former `.claude/fix-plans/state-backend.md` | Removed outside the tracked tree | Stale Claude plan conflated artifact `StateBackend` with live state and proposed unrelated control-plane work; none of it is used as design authority |
| private `docs/research/extensible-schema-traits.md` and `schema-inference-design.md` | Reject as current evidence; public copies already removed in `52daf683` | They describe removed/unwired registries plus unsupported DDL, format, inference, zero-copy, and latency claims; neither reflects the current connector implementation |

No tracked `docs/research` artifact exists on this baseline. The visible path and `.claude` are
ignored Windows junctions into a separate, already-dirty private configuration repository.
Deleting through either path would silently mutate context outside this branch, so Cycle 3 did not
do so. The two obsolete
research files need a separately authorized private-repository archive/removal commit; they are not
copied, cited, or treated as evidence here. Relevant primary sources live as citations in the ADR,
not another research dump.

## Confidence and limitations

Confidence is high for the admission boundary because independent pre-plan, post-plan, startup,
and MV guards converge on the same error and targeted tests execute those paths. Confidence is
also high that a common managed hot-state tier is absent: the checkpoint backend explicitly denies
that role and no local LSM dependency or equivalent API is present in the current tree. Fjall's
deleted cold-cache history does not change that conclusion.

This report does not claim production behavior for an unreachable distributed stateful data path.
It also does not claim cluster exactly-once; cluster delivery remains at-least-once and
`[LDB-0013]` continues to guard exactly-once independently of this feature.
