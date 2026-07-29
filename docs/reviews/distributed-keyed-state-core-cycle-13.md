# Distributed keyed state — Core Cycle 13 review

- **Date:** 2026-07-29
- **Implementation commit:** `abd49797`
- **Scope:** freeze aggregate key-group cardinality across construction and vnode lifecycle calls
- **Slice verdict:** **PASS FOR IMMUTABLE AGGREGATE ROUTING IDENTITY**
- **Production/admission verdict:** **BLOCK**; `[LDB-4007]` and `[LDB-0013]` remain unchanged

## Decision and result

Before this cycle, `IncrementalAggState` had no construction-time routing identity. Its optional
`delta_vnode_count` was empty until the first per-vnode capture, so that first caller could select
any nonzero topology. The same field then acted both as routing authority and as the flag that
enabled delta dirty-key tracking.

Core Cycle 13 requires a typed `KeyGroupCount` when aggregate state is constructed and retains it
immutably. Embedded and single-node operators receive `LOCAL_KEY_GROUP_COUNT`; cluster lazy
initialization derives the exact attached registry count; DDL and persisted-query preflight use the
same checkpoint topology authority. A global aggregate in an N-key-group cluster retains N but
continues to map its single logical key to vnode zero.

Every aggregate full/delta capture and the production managed restore/revoke/rebalance transition
now rejects a different caller count. SQL capture validates before changing `prev_owned` or
acquired-vnode chain bookkeeping. Managed transition validation happens before rkyv payload decode
and private state staging, and aggregate preparation repeats it before reservation or live-map
work. Key-group-count rescaling therefore requires a new state identity and explicit repartition;
it cannot silently reinterpret the live maps.

The old optional count is replaced by `delta_tracking_active`, which owns only whether a per-vnode
baseline has started dirty tracking. While inactive, the record path retains one guard and performs
no tracking hash, insertion, allocation, lock, atomic operation, task, or I/O. When active, routing
uses the immutable count. No compatibility overload or routing abstraction was added.

No checkpoint body, manifest, assignment fence, query fingerprint, public LaminarDB API, backend
dependency, runtime selector, source/sink contract, delivery rule, admission rule, soak helper, or
certification tool changed.

## Verification

| Check | Result |
|---|---:|
| `cargo check -p laminar-db --no-default-features` | passed |
| `cargo check -p laminar-db --no-default-features --features cluster` | passed |
| no-default aggregate-state module | passed, 62/62 active; 1 profiling test ignored |
| no-default managed aggregate initialization | passed, 1/1 |
| cluster aggregate-state module | passed, 72/72 active; 1 profiling test ignored |
| cluster vnode-partition lifecycle module | passed, 15/15 |
| cluster SQL aggregate lifecycle module | passed, 19/19 |
| exact cluster query-shape admission regression | passed, 1/1 |
| no-default library Clippy with `-D warnings` | passed |
| cluster library Clippy with `-D warnings` | passed |
| formatting and diff hygiene | passed |
| broader `operator_graph::tests` filter | **no result; Windows link timed out** |
| broad workspace/integration matrix | **not run** |
| prior cluster failover/ALO/EO engineering soaks on this binary | **not run; paused** |
| independent immutable release-candidate soak | **not run; required before production** |

The tests prove local identity is one, custom cluster identity is exact, aggregate key mapping still
matches shuffle ABI v1, and multi-key-group global state emits an explicit vnode-zero image. A
mismatch before the first full or delta baseline preserves logical state and every delta/rebase
collection. A later mismatch does the same with tracking active. SQL capture preserves
`prev_owned`, and transition mismatch preserves the live image and bookkeeping. Former synthetic
4/8/16/64-vnode fixtures now construct or attach the topology they actually exercise.

Read-only review found no blocker in correctness, test coverage, hot-path scope, naming,
constructor authority, or abstraction scope. The internal shuffle attachment method can
technically be called after initialization; production graph construction attaches exactly once
before mandatory managed-state initialization, and registry cardinality itself is immutable. No
production reattachment path exists, so redesigning that API is outside this cycle.

## End-of-cycle review

- **AI slop and overengineering:** pass. One typed field, one cold validator, and one accurately
  named activation flag replace duplicated authority. Test helpers contain mechanical local-count
  setup without creating a runtime compatibility path.
- **Hot path and latency:** pass for scope only. Inactive delta tracking still performs one guard
  and no tracking work. Active tracking widens the stored nonzero count from `u16` to `u32` before
  the existing hash. No benchmark or tail-latency result is claimed.
- **Unused code and maintainability:** pass. All four construction routes select an explicit
  authority; stale `delta_vnode_count` references are removed; the count, routing conversion,
  validator, and activation flag have separate roles. Warnings-denied library Clippy passes in both
  feature modes.
- **Production readiness:** **BLOCK**. Aggregate groups and changelog state remain monolithic maps;
  per-vnode capture, revoke, and publication still scan or move state proportional to map size.
  There is no bounded pointer-swap shard publication, complete RSS/decode/pause envelope, qualified
  working-state backend, state-family consumer beyond the aggregate reference, key-group-count
  rescale protocol, delivery recertification, latency/resource profile, or independent soak.
- **Documentation and overdocumentation:** pass. This is the only new cycle-history document; the
  existing ADR, plans, validation report, artifact boundary, and changelog carry current authority.
- **Tests:** pass for the affected invariant and admission boundary. The timed-out broad graph
  filter, workspace/integration suites, and current-binary failover/ALO/EO soaks remain unclaimed.

## Next bounded work

Design the concrete aggregate vnode-shard layout against the now-immutable count, including how the
existing pre-aggregate routing result can avoid a redundant record-path hash. Then migrate one
coherent aggregate state image—groups, changelog emission state, and its dirty bookkeeping—without
dual shadow maps or a speculative storage abstraction. Preserve whole-node checkpoint behavior and
`[LDB-4007]`; prove ordinary processing and capture semantics before attempting allocation-free
pointer-swap transition publication. Backend selection, joins/windows, and soak remain separate.
