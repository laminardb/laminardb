# Distributed keyed state — Fjall 3.1.8 source-closure review

- **Date:** 2026-07-28
- **Decision commit:** `924caf00`
- **Reviewed decision head:** `924caf00c84899cabae396e9e18b623c06ae3d13`
- **Scope:** bounded read-only adapter-entry source/contract closure for stock Fjall 3.1.8
- **Slice verdict:** **PASS FOR A SOURCE-PROVEN STOP**
- **Backend verdict:** **NO BACKEND SELECTED**
- **Production/admission verdict:** **BLOCK**; `[LDB-4007]` and `[LDB-0013]` remain unchanged

## Outcome

The canonical
[source-closure report](../reports/fjall-3.1.8-adapter-entry-source-closure-2026-07-28.md) records
`OBSERVED_DESIGN_UNSUPPORTED_IN_STOCK_SOURCE` for exact stock Fjall 3.1.8 at commit
`6debe706dbc53d6d0eb666aae5057671d5c1370f`. The candidate has useful atomic-batch, consistent-
snapshot, point, and ordered-range primitives. It nevertheless fails LaminarDB's all-mode adapter
entry because a background worker can return an error, poison, and exit without decrementing the
private active-thread count, while `DatabaseInner::drop` waits without a deadline for that count to
reach zero.

The frozen v2 maintenance-health contract supplies a separate contract-specific stop: automatic
rotation can warn and discard a version-history maintenance error without poison or a complete
stable public failure signal. Native pressure limits, lossy private scheduling, synchronous stalls,
and hidden/incomplete diagnostics remain supporting risks, not the primary universal claim.

No Fjall dependency, adapter, build, candidate run, admission change, delivery change, soak work, or
runtime code landed. A stock child-process filesystem-fault test could corroborate the teardown
hang, but it was outside the authorized zero-candidate-machine-hour source closure and is not needed
to establish the source invariant. No performance result is claimed.

The owner-stated TidesDB pivot remains a separate bounded source-re-entry decision. The historical
v0.11.1/native 9.3.6 result does not select a current package, and this review authorizes no TidesDB
dependency, adapter, object-store coupling, or execution.

## Verification

| Check | Result |
|---|---:|
| Exact Fjall tag/commit and clean source identity | passed |
| Worker start/error/drop and early partial-spawn source trace | passed |
| Warning-only version-history maintenance trace | passed |
| Frozen v2 mapping consistency review | passed |
| Adversarial source-correctness review | passed after empirical/partial-spawn precision fixes |
| Adversarial maintainability and authority review | passed after agent-independence and next-scope wording fixes |
| Changed Markdown local-link validation | passed, 8/8 files at decision commit |
| `cargo fmt --all -- --check` | passed |
| `git diff --check` | passed |
| `Cargo.toml` / `Cargo.lock` change | none |
| Frozen v2 runner size and SHA-256 | unchanged: 66,870 bytes; `661102f9dcb46934f966f25cc91934ea0d1fa6a487323fb383cbabd0a5e166e3` |
| Fjall candidate build or runtime fault reproduction | **not run; outside source-only scope** |
| Latency, resource, crash, endurance, or soak evidence | **not run and not claimed** |

## End-of-cycle review

- **AI slop:** pass after correction. The decision is exact-release and contract scoped. It does not
  say Fjall cannot store data, is universally unsafe, or can never qualify. It distinguishes the
  unavoidable teardown lifecycle defect from the stricter frozen telemetry veto and from risks a
  Laminar wrapper or external limits might contain.
- **Overengineering:** pass. Work stopped at the first source-proven veto. No generic backend
  framework, adapter, fault harness, sidecar, fork, patch series, benchmark, or alternate-engine
  survey was created. A sidecar is recorded only as a rejected scope expansion.
- **Unused code and maintainability:** pass for this documentation-only slice. No code or helper was
  added. Exact source links use the immutable commit rather than a moving branch. The decision
  details live in one canonical report; the ADR, plans, validation report, options report,
  changelog, and superseded priority review contain only the authority updates needed to avoid live
  contradictions.
- **Hot path and latency:** no row, checkpoint, restore, or control path changed. There is no latency
  improvement or regression evidence to transfer. Any later backend must still pass uniform/Zipf
  aggregates, windows/timers, all admitted join families, checkpoint overlap, resource pressure,
  p99/p99.9/max, and endurance gates on the exact target.
- **Production readiness:** **BLOCK**. There is no selected disk-backed state engine, all-mode
  adapter, native-root failure lifecycle, absolute qualification, connector/delivery composition,
  broad cluster failover/ALO/EO-eligible regression run on an integrated backend, or independent
  immutable release-candidate soak. Existing fail-closed guards remain correct.
- **Documentation and overdocumentation:** pass. One canonical evidence report and this required
  cycle review were added. Historical TidesDB and Fjall records were not rewritten; supersession and
  current-authority pointers preserve provenance without presenting old work orders as live.
- **Tests:** appropriate for a source-only decision. Source identities and paths were inspected,
  three parallel agent analyses challenged the verdict, local links/format/diff hygiene passed, and
  manifests plus the frozen runner stayed unchanged. Rust/runtime suites were not rerun because no
  Rust/runtime input changed; a candidate execution would be a new authorized empirical slice.

## Next-cycle review plan

For the separately scoped TidesDB re-entry or the next backend-neutral core slice, finish the cycle
with the same six-part check:

1. challenge every conclusion for exact-version evidence, causal reachability, and unsupported
   performance or production claims;
2. remove speculative abstractions, wrappers, observers, and alternate paths not required by the
   smallest vertical slice;
3. inventory every new file, feature, helper, warning allowance, configuration knob, and public API
   for a real owner/caller, removing or renaming unclear and unused items;
4. review hot-path blocking, allocation, copying, synchronization, failure ambiguity, hard resource
   containment, checkpoint/restore/rebalance, delivery, and all-mode behavior;
5. keep one canonical decision/evidence document, mark superseded instructions, and delete stale or
   duplicated research that no longer informs the decision; and
6. record focused and broad tests actually run, retain every unrun gate explicitly, rerun existing
   cluster failover/ALO/EO-eligible coverage after integration, and require the separately operated
   immutable release-candidate soak before any production-ready claim.
