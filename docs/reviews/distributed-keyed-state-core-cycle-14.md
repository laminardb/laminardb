# Distributed keyed state — Core Cycle 14 review

- **Date:** 2026-07-29
- **Implementation commit:** `32dcec29`
- **Scope:** coherent vnode-sharded aggregate working state, direct lifecycle operations, and
  reuse of existing uniform route metadata
- **Slice verdict:** **PASS FOR THE FOCUSED AGGREGATE PHYSICAL-LAYOUT SLICE**
- **Production/admission verdict:** **BLOCK**; `[LDB-4007]` and `[LDB-0013]` remain unchanged

## Decision and result

`IncrementalAggState` no longer maintains flat group, changelog, dirty, and delta collections which
must be scanned or repartitioned during vnode work. It now owns a fixed table derived from its
immutable `KeyGroupCount`. Each populated entry is a lazy boxed `AggregateVnodeState` containing
that vnode's groups, `last_emitted`, emit/checkpoint dirty sets, delta-chain depth, and forced-full-
rebase marker. A sorted active-vnode roster makes whole-state iteration O(active vnodes), avoiding
an O(configured key groups) regression for the admitted global aggregate. Embedded, single-node,
and global state use the same structure and remain confined to vnode zero.

Whole checkpoint and restore retain the established aggregate representation and fingerprint.
Restore builds a complete replacement slot set before publication. Managed restore/revoke decodes
and validates every claimed vnode into private replacement shards before it touches live state;
publication swaps only transitioned slot pointers plus the prepared active roster under the graph
fence. Retained slots preserve pointer identity, and displaced boxes remain in the retired object
until post-fence cleanup. Per-vnode capture now addresses one shard directly.

Existing route metadata is retained through live transport and in-memory aligned replay. Exact
local routes and singleton remote route sets pass a uniform vnode hint to aggregate processing,
avoiding a redundant hash. Mixed owner-coalesced and configless batches deliberately retain one
partition hash per unique encoded group rather than adding a per-row network sidecar. Replay rebuilt
from a checkpoint also uses that exact fallback because the hint is deliberately not durable. The
hint is trusted internal routing metadata; validating every key on the record path would erase the
intended saving.

Independent review found two defects before the commit. An idle delta with no emitted-dirty keys
could treat absence as “serialize every `last_emitted` entry”; the delta path now passes an explicit
empty set and a named regression proves an unchanged dedup map is omitted. Replay tests had also
been weakened while adapting direct flat-map access: evaluated accumulator values were replaced by
timestamps and exact output batches by row counts. The final tests again compare evaluated group
state, the changelog dedup map, and complete output batches.

No backend dependency or selector, user-facing LaminarDB/query/admission API, checkpoint structs,
schema, codec, envelope, or fingerprint, transport schema, admission rule, source/sink contract,
delivery guarantee, soak helper, or certification tool changed. One additive transport-envelope
accessor exposes route metadata that the existing message already carried.

## Verification

| Check | Result |
|---|---:|
| no-default aggregate-state module | passed, 63 active; 1 profiling test ignored |
| cluster aggregate-state module | passed, 74 active; 1 profiling test ignored |
| cluster vnode-partition lifecycle module | passed, 16/16 |
| cluster SQL delta/replay module | passed, 21/21 |
| exact cluster `[LDB-4007]` admission sentinel | passed, 1/1 |
| exact cluster `[LDB-0013]` delivery sentinel | passed, 1/1 |
| core transport exact metadata tests | passed, 1/1 each (two tests) |
| graph aligned-route/replay regression | passed, 1/1 |
| `laminar-db` no-default and cluster Clippy with `-D warnings` | passed |
| `laminar-core` cluster Clippy with `-D warnings` | passed |
| formatting and diff hygiene | passed |
| broad workspace/integration matrix | **not run** |
| prior failover/ALO/EO soaks on this binary | **not run; paused** |
| independent immutable release-candidate soak | **not run; required before production** |

The no-default library-test target reported only pre-existing test-build warnings. They are not
introduced by this slice and are not represented as a clean whole-target warning claim.

## Independent audits

- **Correctness and atomicity:** pass for this slice after the idle-delta and weakened-test fixes.
  Preparation leaves live slots untouched; publication is a whole-slot pointer swap; abort and late
  failure preserve the complete live image.
- **Hot path:** pass for scoped structure. Active traversal removed the first review's O(key-group-
  count) global-path concern, and uniform route hints avoid a redundant hash. Mixed/restart hashing
  and trusted-hint fault validation remain explicit measurement/design gates.
- **Tests and maintainability:** pass for the affected invariants. Tests use logical snapshots and
  pointer identities instead of reaching into a duplicate flat representation. The new private
  vnode module keeps storage mechanics out of the aggregate evaluator.
- **Production claim:** blocked. Focused deterministic tests and lint gates are not integration,
  failover, performance, or independent soak evidence.

## End-of-cycle review

- **Code:** pass. One concrete aggregate layout replaces the dual flat/sharded lifecycle model;
  there is no generic state-service abstraction or speculative backend adapter.
- **Tests:** pass for whole/per-vnode checkpoint semantics, sparse slots, restore/revoke overlap,
  abort, pointer identity, route metadata, aligned replay, and fail-closed sentinels. Broad
  workspace/integration and current-binary failover suites remain unclaimed.
- **AI slop:** pass. Names describe aggregate vnode state, active slots, and route hints directly;
  no certification or backend vocabulary was attached to an unqualified slice.
- **Overengineering:** pass. The design uses a fixed option table, boxed concrete shards, and one
  active roster. It adds neither a generic backend trait nor a per-row route sidecar.
- **Unused code:** pass. Warnings-denied library Clippy is green in the affected feature modes; test
  helpers are test-gated.
- **Maintainability:** pass for this slice. Per-vnode storage is isolated in a small private module,
  publication ownership is explicit, and unsafe iteration was avoided.
- **Production readiness:** **BLOCK**. Required gates include mixed/restart hash and latency
  measurement, trusted-hint fault handling, fixed-table RSS and complete live-byte accounting, a
  qualified backend/resource policy if memory bounds require one, byte-identical checkpoint
  compatibility proof, failover EO/ALO recertification, and an independently operated soak.
- **Overdocumentation:** pass. This review records only the cycle evidence and boundaries; the ADR,
  plans, and validation report remain the design and status authorities.

## Next bounded work

Measure and bound the aggregate layout's fixed and live working-set footprint without per-record
full-state scans, and quantify any incremental accounting cost. Decide the trusted-hint validation
boundary and benchmark the uniform, mixed, and restart/replay hash paths. Keep cluster stateful
admission closed until backend/resource, delivery, failover, compatibility, and independent-soak
gates are satisfied.
