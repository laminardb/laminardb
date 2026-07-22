# Cluster keyed/stateful operator validation — 2026-07-22

**Branch:** `feature/distributed-keyed-state-adr`  
**Baseline:** `1e2f8429` (`main`, 2026-07-22)  
**Scope:** admission and lifecycle validation only; no cluster capability is enabled by this work.

## Verdict

The reported fail-closed boundary is genuine, with two qualifications:

1. It applies to cluster `CREATE STREAM`. Cluster materialized views are rejected with
   `[LDB-4007]` even when their query is stateless.
2. The diagnostic's generic wording is older than part of the implementation. A grouped aggregate
   already has pre-aggregate vnode shuffle and aggregate-specific per-vnode capture, restore, and
   revoke code. It remains rejected because its live groups are held in unbounded operator-owned
   maps with no enforceable byte budget or spillable hot-state tier. Windows and joins have larger
   lifecycle gaps.

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

This code is useful implementation substrate, but it does not make the operator production safe.
The current one-million-group guard counts entries, not bytes. The live `groups`, changelog
emission state, distinct/counting side state, dirty generations, Arrow buffers, and allocator
retention are not governed by one truthful byte budget, and there is no local spillable
working-state engine. Capture also walks and serializes operator maps rather than freezing a
state-engine generation in constant time.

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

The integration test `persisted_keyed_mv_fails_closed_on_every_cluster_node` injects a persisted
keyed MV and verifies that each node rejects both initial startup and restart with `[LDB-4007]`.
Local unit/integration tests separately execute grouped aggregates, window-close aggregates, ASOF
joins, temporal joins, and incremental changelog joins. Those local tests establish operator
availability; they do not imply that every unbounded SQL join is semantically admissible.

Commands and results for this baseline are recorded here after a clean targeted run:

```text
cargo test -p laminar-db --features cluster cluster_query_shape_admission_is_pre_mutation_and_mode_derived
RESULT: PASS — 1 passed, 0 failed (the final clean run used --no-default-features and --exact)

cargo test -p laminar-db --features cluster persisted_keyed_mv_fails_closed_on_every_cluster_node
RESULT: PASS — existing integration coverage; see the final cycle review for the exact invocation

cargo test -p laminar-db --test incremental_emit --no-default-features --features cluster \
  incremental_emit_snapshot_matches_full_recompute -- --exact
RESULT: PASS — 1 passed, 0 failed; embedded grouped state matched full recomputation
```

These are admission and focused operator tests, not a production certification. No existing test
demonstrates a distributed keyed operator processing remote rows through crash, restore, and
rebalance, because that path is deliberately unreachable.

## The missing capability, precisely

LaminarDB does **not** need a second cluster coordinator or a generic “checkpoint system.” It needs
a managed keyed working-state capability connected to the distributed execution contract:

1. **Stable distribution ABI:** canonical key encoding and hash, fixed vnode count, stable operator
   and state-table IDs, schema/version compatibility, and an explicit global-vnode convention.
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
8. **Proof:** differential semantics, deterministic fault injection at every cut, skew and quota
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
| ignored `docs/adr`, `docs/research`, and `.claude` content | Do not mutate in this branch | These paths are machine-local, ignored, and in the `.claude` case junctioned outside the repository; they cannot be reviewed or removed as versioned project artifacts |

No tracked `docs/research` directory exists on this baseline. Relevant primary sources are retained
as citations in the design ADR; superseded narrative is not copied into a new research dump.

## Confidence and limitations

Confidence is high for the admission boundary because independent pre-plan, post-plan, startup,
and MV guards converge on the same error and targeted tests execute those paths. Confidence is
also high that a common managed hot-state tier is absent: the checkpoint backend explicitly denies
that role and no local LSM dependency or equivalent API is present.

This report does not claim production behavior for an unreachable distributed stateful data path.
It also does not claim cluster exactly-once; cluster delivery remains at-least-once and
`[LDB-0013]` continues to guard exactly-once independently of this feature.
