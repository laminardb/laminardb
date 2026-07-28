# Distributed keyed state — Core Cycle 10 review

- **Date:** 2026-07-28
- **Implementation commits:** `55ea2c67`, `d71bc926`, `d27e0327`, `fc041108`, `3ee1663b`
- **Reviewed implementation head:** `3ee1663b`
- **Scope:** bind a versioned cluster restore-input contract; verify complete vnode ancestry before
  capsule persistence/Commit and before restore body reads; prevent rejected sealed attempts from
  becoming successor parents
- **Slice verdict:** **PASS FOR COMMIT-DOMAIN RAW-LINEAGE AUTHORITY**
- **Production/admission verdict:** **BLOCK**; `[LDB-4007]` and `[LDB-0013]` remain unchanged

## Decision and result

`VnodeRestoreLimits` version 1 records the current `global_singleton_compatibility` profile. For
checkpoint staged-byte limit `B`, current maximum chain depth `D`, and deployment vnode count `V`,
the profile derives a cluster payload ceiling of `B * D` and artifact ceiling of `V * D` with
checked arithmetic. `VnodeRestoreContract` carries those limits plus the exact metadata-verified
cluster lineage payload and artifact totals. The small payload ceiling is intentional containment
for the only admitted stateful cluster shape; it is not a production multi-owner keyed-state
budget.

Every assignment-certified participant readiness record attests the same limits. Recovery capsule
version 6 binds the agreed limits and exact totals to the existing deployment, pipeline, assignment,
seal, and readiness authority. Participant readiness keys and payloads also advance from version 5
to 6. Older capsule-v5/readiness-v5 cluster cuts require an explicit state/checkpoint reset or new
namespace; no compatibility fallback or format sniffing was added.

After seal and complete readiness/capsule cross-validation, but before capsule persistence and the
durable Commit decision, the leader metadata-traverses every required vnode from its exact head to
a root. Each child lineage must equal the checked extension of the corresponding vnode entry in its
immutable parent seal. Per-artifact and per-chain limits are enforced, all sums use checked
arithmetic, and the recomputed complete-cut totals must match the durable contract. Successful
parent inventories are cached per verification call; vnode bodies are not read during this proof.

Restore repeats the complete metadata proof before any vnode body read and, when a target runtime
context exists, requires it to derive limits exactly equal to the committed limits. Changing the
staged-byte or retained-depth configuration across a live cut therefore requires a reset/new
namespace until a versioned capability-superset rule exists. The validated head retains the exact
ancestor inventories for body loading. The acquired subset is preflighted against the committed
totals before its first body GET; the existing bounded reads, decoded-parent identity checks, and
staging receipt remain in force.

Leader successor-parent publication is now a two-step operation: a fallible immutable promotion
batch is prepared before the durable decision, and the already-validated batch is applied
infallibly only after the exact Commit wins. A post-seal ancestry, readiness, or contract failure
publishes Abort when durable authority is available; an unresolved Abort requires recovery. Neither
path contaminates a later reference/delta base or acknowledges the source cut. This also avoids a
post-Commit fallible return that could suppress acknowledgement of an irrevocable source cut.

No row hot path changes. The additional work is confined to checkpoint and restore control paths.
No TidesDB dependency, local working-state backend, runtime selector, storage abstraction, keyed/
window/join admission, source/sink mode, delivery guarantee, or soak/certification tooling was
added.

## Verification

| Check | Result |
|---|---:|
| `laminar-core` capsule/limit focused tests | passed, 12/12 |
| metadata-only vnode lineage traversal tests | passed, 4/4 |
| cluster recovery-capsule tests | passed, 7/7 |
| pre-body restore rejection/read-count regression | passed, 1/1 |
| post-seal readiness Abort, non-promotion, and fresh-root retry regression | passed, 1/1 |
| forged-parent lineage zero-body Abort, non-promotion, and safe retry regression | passed, 1/1 |
| vnode-chain rehydration suite | passed, 25/25 |
| vnode-transition staging suite after audit correction | passed, 11/11 |
| checkpoint-coordinator module suite | passed, 141/141 |
| coordinated-committer module suite | passed, 23/23 |
| `laminar-core` cluster test build | passed |
| `laminar-db` cluster test check, including external integration compilation | passed |
| `laminar-db` no-default and no-default-plus-cluster checks | passed |
| warnings-denied cluster library Clippy for `laminar-core` and `laminar-db` | passed |
| warnings-denied `laminar-db` cluster-tests Clippy | passed |
| formatting and diff hygiene | passed |
| broad full `laminar-db` unit suite | **not run; focused affected modules only** |
| previous cluster failover/ALO engineering soak on this binary | **not run; paused** |
| cluster exactly-once soak | **not applicable; `[LDB-0013]` remains fail-closed** |
| independent immutable-release-candidate soak | **not run; required before production** |

Windows linking took several minutes for the focused unit binary but completed successfully. The
focused results are not a substitute for the broad matrix. Historical failover and at-least-once
soak results remain useful regression provenance but cannot be transferred to the Cycle 10 binary.

## End-of-cycle review

- **AI slop and overengineering:** pass for the bounded slice. One durable contract and one focused
  lineage verifier express the missing authority directly. The work reuses recovery capsule,
  readiness, seal, backend, and coordinator types; it adds no generic quota framework, backend
  façade, candidate dependency, feature switch, migration framework, or admission bypass.
- **Hot path and latency:** pass only for row-path scope. There is no per-record I/O, allocation,
  task, or synchronization change. Checkpoint and restore add bounded metadata reads and checked
  traversal; successful seal inventories are shared within one proof. Request count, response
  buffering, checkpoint duration, restore pause, allocation pressure, and p99/p99.9 remain
  unmeasured and must not be inferred from this result.
- **Unused code and maintainability:** pass for the changed code. Traversal/accounting lives in the
  focused `vnode_restore_lineage` module instead of extending the coordinator monolith. Test-only
  constructors and constants are gated, and cluster/no-default warning checks are clean. The large
  coordinator and test modules remain existing cleanup debt; unrelated splitting is intentionally
  deferred until a cohesive boundary is changed. `DKS-CLEANUP-001` remains open for the final
  human-maintainability pass. Independent audit caught a test helper modeling the acquired subset
  as the complete committed head; it now always builds `0..vnode_count`, its redundant subset
  parameter was removed, and the affected transition suite passes 11/11.
- **Production readiness:** **BLOCK**. The contract compares metadata; it does not acquire memory or
  request permits. Production still needs held raw response/spool reservations, bounded request
  concurrency, wrapper/seal metadata accounting, decoder scratch/expansion and decoded RSS limits,
  live/prepared/retired residency limits, bounded publication/retirement pause, vnode-scalable
  publication, an absolute restore/acquisition deadline with cancellation, minimum truthful health
  signals, a qualified optional worker-local state tier, complete operator and delivery contracts,
  fault coverage, latency/resource profiles, and the independent immutable-RC soak.
- **Documentation and overdocumentation:** pass after reconciling the ADR, phased plans, artifact
  format, validation report, and changelog. This review is the only new cycle-history document; no
  speculative research or certification package was added.
- **Tests:** focused boundaries cover exact limits, payload/artifact max-plus-one, overflow and
  version rejection, participant disagreement, complete ancestry, forged arithmetic, missing
  seals, zero preflight body reads, integrated forged-parent Abort/non-promotion, and both prior-
  parent and fresh-root safe retry. The affected coordinator, committer, rehydration, and transition
  suites are green. The broad suite and all current-binary soaks remain explicitly unclaimed.

## Next core slice

Core Cycle 11 should make acquired raw restore input resource-owning rather than comparison-only.
After deriving the exact acquired subset from the already-validated durable contract, reserve its
raw payload bytes and artifact permits for the lifetime of the immutable transition. Bound body
request concurrency with those permits, cap aggregate in-flight response/spool retention, apply one
absolute restore/acquisition deadline with cancellation, and prove deterministic release on
publication, Abort, poison, launch failure, and graph replacement. Reject before the first body GET
when the reservation cannot be acquired.

Keep wrapper/seal metadata, decoder scratch/expansion, decoded/live/prepared/retired RSS,
publication pause, vnode sharding, backend qualification, health, delivery, windows/joins, and
independent soak as explicit later gates. Do not add a runtime backend or relax admission in this
slice.
