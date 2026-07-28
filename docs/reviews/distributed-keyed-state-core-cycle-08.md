# Distributed keyed state — Core Cycle 8 review

- **Date:** 2026-07-28
- **Implementation commit:** `ce6fe9da`
- **Scope:** atomic managed-vnode transition publication for the existing SQL aggregate
  participant, including checkpoint restore, revoke, final-owner exit, and capability-drift
  containment
- **Slice verdict:** **PASS FOR DETERMINISTIC AGGREGATE PREPARE/ABORT/PUBLISH LIFECYCLE**
- **Production/admission verdict:** **BLOCK**; `[LDB-4007]` and `[LDB-0013]` remain unchanged

## Decision and result

The graph now owns a four-phase managed transition for `SqlAggregateV1`: prepare, abort, publish,
and finish. Preparation completely decodes and validates every authoritative FULL plus ordered
DELTA chain into a key-level mutation plan with private replacement collections. It combines
revoke and restore, reserves publication and retirement collections, and checks final group
cardinality without changing logical rows or dirty/delta bookkeeping. It may grow live-map
capacity, which can remain after a later failure or abort.

The graph prepares applicable aggregate participants in canonical order. Any participant or
prepublication authority failure aborts and finishes every attempted participant and retains the
pending transition without poisoning. Publication begins only while the exact pending slot and
installed-state handle are locked and assignment, transport, registry, and roster authority still
match. Participant publication is unit-returning; the graph then activates the complete vnode set,
installs the exact binding, and clears the exact pending transition. It releases both locks before
retiring displaced aggregate state and the previous binding. An unwind after publication begins
clears installed authority and poisons the graph generation.

The same lifecycle handles committed final-owner exit. Aggregate restore also preserves changelog
dirtiness for groups introduced only by a delta and removes stale failed-generation rebase markers
for revoked vnodes. Exact final cardinality is checked against `max_groups`; a failed or dropped
prepared transition leaves logical aggregate and ownership bookkeeping unchanged, but it can leave
additional reserved capacity/RSS.

Cluster graph construction rejects any cached `Rejected` operator capability and requires the
post-initialization capability descriptor to equal the cached implementation, state class, cluster
status, and managed-state contract. A SQL shape that initializes as an aggregate despite an
unmanaged classifier result, such as aggregate plus `LIMIT`, remains fail-closed with `[LDB-4007]`.

No raw vnode-artifact format, graph ABI, public LaminarDB API, backend dependency, runtime selector,
source/sink contract, or delivery guarantee changed. The existing mode-neutral aggregate
initialization remains shared by embedded, single-node, and cluster pipelines; the authority-fenced
vnode publication protocol is cluster-specific. Keyed aggregates, windows, materialized views, and
all persistent join families remain unadmitted.

## Verification

| Check | Result |
|---|---:|
| three new graph success/drift regressions | passed, 3/3 |
| full cluster-feature `laminar-db` library suite | passed, 1,826; failed, 0; ignored, 1 profiling test |
| serialized cluster integration suite | passed, 23/23 |
| cluster and no-default `laminar-db` checks | passed |
| warnings-denied cluster and no-default `laminar-db` library Clippy | passed |
| cluster-feature `laminar-server` check | passed |
| exact query-shape `[LDB-4007]` sentinel | passed, 1/1 |
| exact delivery/build `[LDB-0013]` sentinels | passed, 2/2 |
| formatting and diff hygiene | passed |
| independent immutable-release-candidate soak | **not run; certification remains unavailable** |

The first parallel cluster-integration invocation passed 21/23; two tests timed out waiting for the
test harness's shared-namespace proof. The complete serial rerun passed both affected cases and all
23 tests. The two shared-namespace timeouts remain recorded; the serial pass does not erase them or
by itself prove their root cause.

## End-of-cycle review

- **AI slop and overengineering:** pass for this slice. It adds one concrete aggregate lifecycle
  and reuses graph, assignment, registry, and installed-binding authority. It adds no generic state
  façade, backend adapter, TidesDB execution, feature toggle, compatibility bridge, health surface,
  or admission bypass.
- **Hot path and latency:** pass only for row-path scope. No per-row lock, I/O, task, or backend call
  was added. Rebalance preparation scans and clones keys from the full unsharded aggregate maps, so
  it can be O(total live aggregate state) even for a small vnode transition. It can also hold live
  state and prepared replacements simultaneously and retain added capacity after abort;
  publication and retirement remain proportional to transitioned state. There is no p99/p99.9,
  RSS, or bounded-pause claim.
- **Unused code and maintainability:** pass with Cycle 9 debt. Legacy raw mutation hooks were
  removed from production builds and retained under `cfg(test)`; duplicated retired-state
  fields and transition vnode-count authority were removed. The aggregate prepare function,
  vnode-transition module, and graph test module are too large and must be split along resource/
  lifecycle phases.
- **Production readiness:** block. There is no transition-wide encoded/object/request/spool/scratch/
  decoded/live-prepared-retired budget, apply/retirement deadline, vnode-sharded bounded-cost swap,
  qualified hot-state backend, minimum maintenance/error health contract, window/timer lifecycle,
  join lifecycle, complete source/state/sink delivery composition, or independent immutable-RC
  soak.
- **Documentation:** pass after replacing stale sequential-callback claims. Historical Cycle 7
  evidence is unchanged; backend and soak history were not duplicated.
- **Tests:** deterministic and serialized integration gates pass. This cycle ran no soak, backend
  candidate, A/B, observer, transcript, or certification tooling.

## Next core slice

Cycle 9 will implement the transition-wide resource and deadline budget listed in the production-
readiness finding above. Limits must fail before fetch or logical mutation where possible, remain
consumed across every participant, and prove exact-limit and max-plus-one behavior without OOM or
cursor/output advance.

That work will also split aggregate preparation into decode, validation, replacement construction,
and reservation phases; separate managed publication from transition artifact/authority resolution;
and move the new lifecycle probes out of the monolithic graph test file. Backend qualification,
including the stopped official `tidesdb` package line, remains timeboxed and untouched. Admission
stays closed until bounded vnode-owned state, operator-specific window/join contracts, delivery
constraints, production health signals, latency/resource evidence, and the independent soak exist.
