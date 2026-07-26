# Distributed keyed state Cycle 53 review

- **Date:** 2026-07-26
- **Scope:** step 3 of the Cycle 50 delivery-evidence sequence: freeze the first grouped operation
  identity and pure writer-authority projection in the standalone independent-soak tool
- **Cycle outcome:** exact operation-ID v1 derivation, fixture consumption, and fail-closed current
  assignment/process/recovery projection are executable and independently reviewed
- **Admission/backend outcome:** unchanged; no runtime crate, Kafka path, state backend, endpoint,
  metric, capability, or admission guard changed
- **Production verdict:** **NO-GO**; lifecycle inputs remain unwired and no independent soak ran

## Result and exact boundary

The sole normative operation preimage is the
[ADR-008 grouped identity](../architecture-decisions/ADR-008-managed-vnode-keyed-state.md#grouped-countsum-operation-identity-v1).
The root-workspace-excluded `tools/independent-soak-contract` crate uses SHA-256 with one invariant
context through output ID, then clones it and appends the borrowed canonical group-key length/bytes
and checked count. The hot derivation constructs no preimage buffer, does not re-encode a key or
hash payload, and returns one fixed 32-byte value. Its maximum-envelope-batch test repeatedly uses
the same context and caller-owned output; it is structural characterization, not a latency or
allocator measurement.

Fixture v2 now supplies explicit production `PartitionKeyCodecV1` Arrow-row goldens for `alpha`
and `beta`. The mapping is lowercase, exact, and bijective over its ledger groups. Every data
header is independently re-derived from the decoded raw payload's logical key, the mapped canonical
bytes, and count. Changing the fixture ID map and envelope together still fails. Changing only
`SUM` leaves the ID stable and is caught by the existing payload/result rules; changing count also
fails identity derivation. The byte-identical `alpha/count/2` replay remains the same ID across
writer intervals.

The separate pure authority projection accepts a current assignment version/full-certificate-digest
reference plus complete vnode owner and sorted participant views, a separately supplied current
process lease, one immutable committed recovery view, the exact shard plan, and opaque current/
predecessor interval IDs. It reconstructs the production owner-map and full certificate digests,
including participant boots and the 129-participant cap. It therefore rejects an inner owner-map
digest, same-node boot disagreement, and a stale digest after boot rotation. It never uses the
checkpoint leader term. Current assignment may be newer than the recovery base; older current
versions and equal-version/different-certificate inputs fail. Planned vnodes must be canonically
ordered and currently owned by the writer's node and boot, while the claimed term must exactly match
the current lease. Every frozen marker field and the data interval are asserted through encode/
decode.

This is not a runtime authority source. Production still has no durable recreate-rotating pipeline
incarnation, no sink-command handoff for the owned canonical group key and checked count, and no
writer-interval allocation/rotation state machine.

## Independent review corrections

Pre-implementation audits corrected the initial identity shorthand by adding sink and output scope,
using u8 lengths for the already-capped text IDs, retaining a u32 canonical-key length, and choosing
the fixed `laminardb/grouped-count-sum/operation-id/v1\0` domain. They also required the marker's
assignment hash to mean `CheckpointAssignmentFence::digest()`, separated assignment node/boot from
the current `ProcessLease` term, permitted current assignment 8 over recovery base 7, and deferred
interval creation to the next state machine.

Implementation review then found that the first projection test asserted only a subset of marker
fields and lacked an explicit replay/recreate plus maximum-batch characterization. The final test
asserts every marker field, proves stable crash replay and changed-incarnation rotation, and derives
65,536 IDs into caller-owned output without adding a batch API. A separate authority review then
rejected arbitrary nonzero assignment hashes; source-shaped full-certificate goldens, independent
recomputation, boot-only/stale/inner-digest rejection, and the 129/130 participant boundary close
that gap. The cycle review link was created before final documentation/link validation.

## AI slop review

**Pass.** There is one operator-specific domain, one preseeded derivation, and one authority
projection. No generic identity registry, metadata framework, interval generator, fake runtime
trait, or speculative window/join identity was added. Windows and joins must define their own
semantic domains later.

## Overengineering and hot-path review

**Pass for this standalone slice.** Invariant identity bytes are hashed once. Per emitted distinct
group, derivation clones the fixed SHA state and appends only a u32 length, borrowed key, and count.
Payload hashing remains an independent-reader concern and marker provenance remains off the row
path. Authority preparation is an interval/marker operation, not per-row work. The exact
`sha2 = "=0.10.9"` dependency is confined to the standalone lockfile and matches the root
workspace's existing SHA-256 line.

## Unused-code review

**Pass for validation-only code.** Identity derivation is consumed by the v2 verifier. Every
authority input is either rejected by a focused negative test or projected into a field asserted
against the frozen codec. The rejected batch abstraction was removed; callers simply retain one
context and their own output storage. No production placeholder, feature flag, adapter, endpoint,
metric, or backend dependency was added.

## Production-readiness review

**NO-GO.** Pipeline-incarnation creation/persistence/rotation, stable operator/output lifecycle,
canonical key/count runtime handoff, interval allocation and ambiguous-commit recovery, bounded
sequence reservation, transactional producer fencing, real broker visibility, supported authority
views, state-backend qualification, measured tail latency, exactly-once proof, cluster admission,
and the independently operated immutable-release soak are all absent. Cluster keyed aggregates,
windows, and stateful joins remain fail-closed.

## Documentation review

**Pass.** ADR-008 owns the only normative preimage table. The phase plan, phased implementation
plan, validation report, and soak charter link to it and preserve the standalone/no-production
boundary. Existing backend research remains clearly historical, rejected, parked, or selected-but-
stopped decision lineage; none became obsolete because of this identity slice, so none was removed.

## Test review

**Pass for the pure identity/authority scope.** Final validation includes:

- 53 library tests, 6 CLI tests, and doc tests through the standalone locked manifest;
- Clippy over every standalone target with warnings denied;
- a literal 177-byte preimage and independent SHA-256 golden, all identity axes, length ambiguity,
  empty key, count bounds, replay/recreate, exact five-ID fixture integration, and label-plus-wire
  mutation resistance;
- full current/recovery assignment ordering, source-shaped owner-map/certificate digests, 129/130
  participant and boot-rotation boundaries, process term, roster/shard ownership, interval,
  marker-field, and data-header propagation coverage;
- standalone and root formatting, direct v1/v2 CLI distributions, dependency hygiene, local links,
  and `git diff --check`; and
- independent identity/semantic, authority, and hot-path/scope review after the corrections above.

These checks are not a Kafka integration test, production benchmark, backend run, distributed soak,
or certification result.

## Cycle 54 review plan

Implement only step 4 of the frozen sequence: an in-memory fake transactional-producer state
machine around the already-frozen bytes and pure authority input. Do not add Kafka, runtime wiring,
a backend, admission, endpoint, metric, or guarantee change.

1. **AI slop:** model only uninitialized, marker-pending, data-open, transaction-in-flight, and
   terminal-poison states; do not build a generic broker abstraction.
2. **Overengineering/hot path:** batch already-derived fixed headers and borrowed payloads under
   explicit count/byte limits; reserve checked sequence ranges once per batch and measure no
   production latency from the fake.
3. **Unused code:** drive every transition through deterministic tests; add no connector hook or
   interval allocator without a consumer.
4. **Production readiness:** require confirmed first marker before data, atomic marker/data visibility,
   terminal death on ambiguous begin/send/commit outcomes, no interval reuse, and checked overflow;
   retain **NO-GO** and make no exactly-once claim.
5. **Documentation:** record state-machine semantics once in ADR-008 and keep other updates as short
   links; preserve Cycle 55 as the real Kafka/Redpanda boundary.
6. **Tests:** cover initialization, predecessor/no-predecessor markers, batching splits, sequence
   reservation and `u64` overflow, deterministic abort, ambiguous commit, post-poison rejection,
   marker-before-data, and crash/successor replay without executing a broker.
