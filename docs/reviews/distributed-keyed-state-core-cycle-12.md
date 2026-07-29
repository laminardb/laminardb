# Distributed keyed state — Core Cycle 12 review

- **Date:** 2026-07-29
- **Implementation commit:** `e9010153`
- **Scope:** bound owned decoding of the admitted legacy vnode-partial outer archive
- **Slice verdict:** **PASS FOR CURRENT-PROFILE OUTER-CONTAINER ALLOCATION**
- **Production/admission verdict:** **BLOCK**; `[LDB-4007]` and `[LDB-0013]` remain unchanged

## Decision and result

The established raw-rkyv `VnodePartial` wire remains unchanged. Before this cycle, sealed-chain
traversal and graph preflight each called `rkyv::from_bytes` before enforcing the managed operator
roster. A valid bounded payload could therefore make owned `Vec<(String, Vec<u8>)>` allocations
from self-declared archive counts before the current compatibility profile rejected extra entries.

Core Cycle 12 adds one checked restore decoder. It first uses safe checked `rkyv::access` to validate
and borrow the archive, rejects more than the single outer state entry admitted by
`global_singleton_compatibility`, rejects a root carrying delta state, and only then deserializes
owned outer containers from the already-checked view. Both sealed-chain decode sites and graph
preflight consume the committed restore profile. Corrupt or over-profile input returns
`[LDB-6051]` before operator preparation, callbacks, live-state mutation, or vnode activation.

Synthetic tests for graph-wide prepare-all/abort-all still exercise multiple managed participants.
Their test-only decoder ceiling is widened only when the authoritative synthetic graph roster has
more than one participant; empty and singleton test graphs use the production decoder unchanged.
This is not a runtime compatibility path or a second production profile.

No record hot path, state backend, runtime selector, public API, wire format, admission rule,
source/sink contract, delivery guarantee, soak helper, or certification tool changed.

## Verification

| Check | Result |
|---|---:|
| `cargo check -p laminar-db --no-default-features` | passed |
| `cargo check -p laminar-db --no-default-features --features cluster` | passed |
| legacy vnode-partial codec/restore tests | passed, 11/11 |
| complete sealed-chain rehydration module | passed, 27/27 |
| vnode-transition filter | passed, 24/24 |
| outer-amplification head, parent, graph, and codec tests | passed, 4/4 |
| zero-roster, duplicate-name, roster-mismatch, and multi-participant regressions | passed |
| no-default library Clippy with `-D warnings` | passed |
| cluster library Clippy with `-D warnings` | passed |
| formatting and diff hygiene | passed |
| broad workspace/integration matrix | **not run** |
| prior cluster failover/ALO/EO engineering soaks on this binary | **not run; paused** |
| independent immutable release-candidate soak | **not run; required before production** |

The loader tests use real sealed ancestry. An invalid child is rejected after exactly its head body
and before its valid parent body; an accepted delta head leading to an invalid parent is rejected
after exactly those two bodies. Graph preflight proves no callback, activation, poison, or loss of
pending authority. Truncated unaligned input returns an error without panic, while valid unaligned
input still decodes through the bounded path.

An independent review initially blocked the slice on a zero-roster test divergence, a no-cluster
dead-code gate, a parent-I/O fixture with no parent, missing coverage of the second loader decode
site, and displaced duplicate-name coverage. All five were corrected before the implementation
commit. Re-review approved the resulting boundary.

## End-of-cycle review

- **AI slop and overengineering:** pass after correction. The change is one profile-specific
  decoder and direct call-site routing. It does not introduce a generic allocator/quota framework,
  wire migration, alternate backend, or speculative abstraction. The test-only multi-participant
  seam is explicitly confined to synthetic protocol fixtures.
- **Hot path and latency:** pass for scope only. Record processing has no new branch, allocation,
  lock, task, or I/O. Restore performs one checked archive traversal before deserialization; the
  decoder reuses that checked view rather than validating it again inside the call. Restore tail
  latency remains unmeasured.
- **Unused code and maintainability:** pass. Restore-only functions compile only for cluster or
  tests. The former general decoder is test-only because every production restore decode now uses
  the bounded entry point. Names describe the production and synthetic-roster roles directly.
- **Production readiness:** **BLOCK**. Checked archive validation still traverses encoded entries;
  unaligned input still makes one full aligned copy. This cycle does not bound inner aggregate/
  Arrow decode, decoder scratch, wrapper/seal or response allocation, total decoded/live/prepared/
  retired RSS, full-map scans, publication/retirement pause, or vnode-scale swap cost. There is no
  qualified working-state backend, second state-family consumer, delivery recertification,
  latency/resource profile, or independent soak.
- **Documentation and overdocumentation:** pass. This is the only new cycle-history document; the
  existing ADR, plans, validation report, artifact boundary, and changelog carry current authority.
- **Tests:** pass for the affected boundaries. Broad feature/integration suites and current-binary
  failover/ALO/EO soaks remain explicitly unclaimed.

## Next bounded work

Freeze one immutable key-group identity in aggregate state across embedded, single-node, and
cluster construction before changing its physical maps. Then migrate the aggregate to concrete
vnode-owned shards in reviewable steps: ordinary processing/checkpoint semantics first, followed by
per-vnode capture and allocation-free pointer-swap publication. Do not combine a join/window
consumer with that migration, add a backend, or relax admission. A real `IncrementalJoinV1`
consumer follows only after the shard/resource/pause boundary is credible. Independent soak remains
the final cleaned-release-candidate gate.
