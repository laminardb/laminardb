# Distributed keyed state — Core Cycle 15 review

- **Date:** 2026-07-29
- **Implementation commit:** `f4e3e572`
- **Scope:** lower-bound retained-memory accounting for managed aggregate vnode state
- **Slice verdict:** **PASS FOR THE FOCUSED AGGREGATE ACCOUNTING SLICE**
- **Production/admission verdict:** **BLOCK**; `[LDB-4007]` and `[LDB-0013]` remain unchanged

## Decision and result

Managed aggregate state now reports separately owned live, prepared, and retired retained bytes.
The estimate includes topology element storage, inline vnode storage, logical collection elements,
owned-row payloads, and accumulator-reported bytes. It deliberately excludes hash-table bucket and
control storage, allocator metadata and fragmentation, nested or shared changelog payloads, RSS,
and transient checkpoint/output scratch space. The metric is therefore a saturating lower-bound
charge, not an allocator measurement or memory limit.

The implementation keeps cached per-vnode charges and does not inspect every accumulator on the
record path. New groups use structural accounting only. Dynamic accumulator sizes are reconciled at
checkpoint, restore, and rebalance boundaries. Delta checkpoint reconciliation is restricted to the
dirty entries already selected for encoding; clean groups are not scanned or asked for their
potentially expensive `Accumulator::size()`. Accounting failure or overflow cannot fault the data or
delivery path; overflow is represented by a saturated sentinel.

The graph exports `managed_state_accounted_bytes{operator,phase}` for `live`, `prepared`, and
`retired`. Prepared and retired peaks since the previous publication are recorded outside the
Prometheus publication lock, so short ownership overlaps remain observable. Removed operators have
their series cleared. The SQL operator documents that the current charge covers aggregate working
state only; lifecycle metadata, joins, windows, backend caches, and process memory are not silently
presented as covered.

Independent review found a checkpoint-tail defect in the first implementation: delta accounting
refreshed the whole vnode after encoding its dirty subset. Generic `Accumulator::size()` can scan all
retained distinct values for retractable `MIN`/`MAX`, so that refresh defeated incremental delta
latency. The final implementation folds accounting over only the encoded dirty entries. A regression
places dirty and clean groups in the same vnode, establishes a full baseline, captures a one-group
delta, and proves the clean accumulator receives no `size()` call. Review also removed duplicate
restore reconciliation and restored an infallible recovery-image commit API.

No backend dependency, runtime backend selector, stateful cluster admission, public query API,
checkpoint wire format, source/sink contract, delivery guarantee, soak helper, or certification
tool changed. TidesDB 9.3.15 remains a preferred qualification candidate, not a runtime-selected or
production-approved LaminarDB backend.

## Verification

| Check | Result |
|---|---:|
| cluster aggregate-state module | passed, 144 active; 1 profiling test ignored |
| no-default aggregate-state module | passed, 115 active; 1 profiling test ignored |
| clean-group delta accounting regression | passed |
| live/prepared/retired transition accounting | passed |
| cold-cadence publication and removed-series regression | passed |
| managed aggregate initialization regression | passed |
| exact cluster `[LDB-4007]` admission sentinel | passed |
| exact cluster `[LDB-0013]` delivery/build sentinels | passed |
| `laminar-db` no-default and cluster library Clippy with `-D warnings` | passed |
| cluster build, formatting, and diff hygiene | passed |
| end-to-end lifecycle metric wiring | **not run; required** |
| maximum-topology and retractable-aggregate performance | **not run; required** |
| previous failover/ALO/EO soaks on this binary | **not run; paused** |
| independent immutable release-candidate soak | **not run; required before production** |

The no-default test target emitted only pre-existing unrelated test warnings. They are not presented
as a warning-free whole-test-target result.

## Backend qualification status

The native transaction-lifecycle fix is merged in TidesDB 9.3.15. The official `tidesdb-rs` wrapper
still needs its additive 9.3.15 source feature and the corresponding
`tidesdb-src-v9-3-15@0.1.0` publication before LaminarDB can consume it from crates.io. The proposed
wrapper change is in [tidesdb-rs PR #42](https://github.com/tidesdb/tidesdb-rs/pull/42); its DCO
check passes. The native release tag also still declares 9.3.14 in CMake metadata, tracked by
[TidesDB issue #665](https://github.com/tidesdb/tidesdb/issues/665). These upstream items block a
normal dependency qualification run, but do not justify a local fork or speculative adapter in this
cycle. Portable checkpoint storage remains provider-neutral through Rust `object_store`; native S3
support is not part of the LaminarDB backend contract.

## End-of-cycle review

- **Code:** pass for the slice. Accounting is concrete aggregate-vnode code and publication plumbing,
  with no generic state-store abstraction or backend selector.
- **Tests:** pass for accounting arithmetic, overflow, structural recomputation, lifecycle ownership,
  cold publication, cleanup, delta selectivity, and the unchanged fail-closed sentinels. Integration,
  performance, failover, and independent-soak evidence remains outstanding.
- **AI slop:** pass. Names describe accounted ownership and phases; the metric help text states its
  lower-bound scope and exclusions instead of implying complete resident-memory measurement.
- **Overengineering:** pass. Cached structural charges and lifecycle reconciliation address the
  observed need without allocator hooks, per-record full-state scans, or a speculative backend API.
- **Unused code:** pass. Both affected library feature modes pass warnings-denied Clippy; test-only
  probes remain test-gated.
- **Maintainability:** pass for this slice. Arithmetic and ownership snapshots are isolated in the
  aggregate accounting module, while graph publication remains separate.
- **Production readiness:** **BLOCK**. Admission remains closed. Required gates include maximum-roster
  sampling cost, delta-selection cost, dirty retractable `MIN`/`MAX` tail latency, full window/join
  state support, backend resource and failure policy, delivery/failover recertification, and an
  independently operated immutable-binary soak.
- **Overdocumentation:** pass. This review records only the cycle decisions, evidence, limitations,
  and next gates; it does not duplicate the design ADR.

## Next bounded work

Benchmark the current 32-cycle metric sampling against a maximum active-vnode roster and measure
delta capture with large retractable aggregates. If the tail is material, maintain O(1) aggregate
totals or use time-cadenced sampling, and give retractable accumulators O(1) retained-byte charges.
Add one end-to-end lifecycle metric test. Keep backend qualification separate and timeboxed, and keep
cluster stateful admission closed until the broader checkpoint, rebalance, delivery, failover, and
independent-soak gates are complete.
