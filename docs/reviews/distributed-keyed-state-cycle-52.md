# Distributed keyed state Cycle 52 review

- **Date:** 2026-07-26
- **Scope:** step 2 of the Cycle 50 delivery-evidence sequence: freeze compact data-header and
  partition-marker bytes in the standalone independent-soak tool
- **Cycle outcome:** envelope v1 has literal goldens, strict bounded encode/decode, hostile-input
  coverage, and v2 semantic-fixture consumption
- **Admission/backend outcome:** unchanged; no runtime crate, Kafka path, state backend, dependency,
  endpoint, metric, capability, or admission guard changed
- **Production verdict:** **NO-GO**; no production record carries this envelope and no independent
  soak ran

## Result and exact boundary

The sole normative byte layout is the
[ADR-008 distributed-output envelope](../architecture-decisions/ADR-008-managed-vnode-keyed-state.md#distributed-output-envelope-v1).
The root-workspace-excluded `tools/independent-soak-contract` crate now has a dependency-free,
private envelope-v1 codec conforming to it. Data encoding returns a fixed 66-byte stack array and
decoding borrows the operation and writer-interval IDs. Marker encoding uses an exact bounded
caller buffer, marker decoding borrows its identifier and vnode-bitmap fields, and a marker cannot
exceed 8,950 bytes.

Fixture v2 carries lowercase literal envelope hex for every data record and marker. An explicit,
validated fixture-only ID map bridges existing semantic labels to fixed-width opaque bytes; it is
not the deferred operation-identity algorithm. The oracle decodes the literal bytes, compares every
field with the semantic evidence, requires common marker bytes to match across affected partitions,
and reports deterministic malformed, unknown-ID, mismatch, and common-marker-byte diagnostics.
Fixture v1 and its classifications are unchanged.

The data-batch API rejects more than 65,536 headers or 4,325,376 fixed-header bytes before the first
header is decoded, then validates borrowed exact slices without retaining decoded records. That cap
describes one codec-facing transport batch; an oracle case is a whole capture and may span multiple
batches. Pointer/capacity assertions and direct source inspection characterize structure only. No
latency distribution or allocator instrumentation was measured.

The intended future Kafka placement reserves one exact `__ldb` header while leaving payload bytes
untouched, but this cycle adds no connector behavior. Topic, partition, offset, timestamp,
transactional fencing, and broker visibility remain outside the standalone envelope codec.

## Independent review corrections

Reviews corrected the initial draft before approval: pipeline identity uses canonical version 3;
recovery epoch and checkpoint ID must be the same nonzero attempt; a successor cannot name itself as
predecessor; vnode bitmaps need at least one owned vnode plus exact 1/7/8/9/65,535 boundaries and
zero padding; assignment digests are evidence references rather than embedded certificates; and
the documented sizes are envelope sizes, not complete Kafka-record sizes. The maximum-batch test
now puts an invalid header first in a max-plus-one input, proving count preflight precedes inspection.

A final review also rejected a fixture-map rule that forced writer node IDs, process terms, and
checkpoint IDs into one globally unique numeric namespace. Those fields are typed and may legally
share a value. The map now permits that, uses only a known-value set for diagnostic classification,
and has a full marker regression where all three values equal 44 while a different known value
still produces `wire_envelope_mismatch`.

## AI slop review

**Pass.** There is one version, one common prefix, and only two message kinds. The implementation is
a small hand codec, not a serializer framework. Every frozen field is required by the Cycle 51
model or future placement contract, and the fixture bridge explicitly does not pretend to derive
production identities.

## Overengineering and hot-path review

**Pass for this bounded standalone slice.** The per-record envelope contains only operation ID,
writer interval, and admission sequence. Payload digest, vnode, shard, ABI, assignment, process,
checkpoint, and broker facts remain out of the row hot path. Data encoding is fixed-size and stack
based; decoding is borrowed. Common provenance is paid once per partition marker. The batch test is
structural characterization, not a throughput, tail-latency, or production allocation claim.

## Unused-code review

**Pass.** Both kinds are generated from independent literal goldens, decoded, compared with v2
semantics, re-encoded, and attacked by boundary tests. The codec is deliberately private and
dead-code-tolerant outside tests because production wiring is forbidden in this cycle. No runtime
placeholder, feature flag, generic adapter, dependency, endpoint, or metric was added.

## Production-readiness review

**NO-GO.** Stable `operation_id_v1` and writer-interval derivation, runtime authority propagation,
Kafka producer fencing and transactions, supported checkpoint/assignment/process evidence, real
broker tests, backend qualification, admission changes, measured hot-path latency, exactly-once
proof, and the independently operated release-binary soak are all absent. Cluster keyed aggregates,
windows, and stateful joins remain fail-closed.

## Documentation review

**Pass.** ADR-008 is the only byte table. The phase plan, phased implementation plan, validation
report, and soak charter link to it and state the standalone/no-production boundary. Existing
backend research remains labelled as historical, rejected, parked, or selected-but-stopped
decision lineage; none became obsolete because of this codec work, so none was removed.

## Test review

**Pass for the standalone byte-contract scope.** Final validation includes:

- 42 library tests, 6 CLI tests, and doc tests through the standalone locked manifest;
- Clippy over all standalone targets with warnings denied;
- standalone and root formatting checks plus `git diff --check`;
- direct v1 and v2 CLI verification, preserving v1's `3 MODEL_MATCH / 4 PRODUCT_FAIL / 2
  RUN_INVALID` distribution and giving one v2 `MODEL_MATCH`;
- local-link and dependency-hygiene checks; and
- independent codec/semantic and hot-path/scope approvals after the corrections above.

These checks freeze standalone bytes and rejection behavior. They are not a Kafka integration test,
latency benchmark, state-backend run, distributed soak, or certification result.

## Cycle 53 review plan

Implement only step 3 of the frozen sequence: pure, stable operation identity plus exact writer and
assignment-authority propagation tests. Do not add a producer state machine, Kafka connector,
runtime backend, admission change, endpoint, metric, or certification behavior.

1. **AI slop:** define one domain-separated `operation_id_v1` preimage and one minimal authority
   projection; do not create a generic identity or metadata framework.
2. **Overengineering/hot path:** use unambiguous canonical bytes, precompute invariant identity
   context, avoid payload hashing and per-row provenance expansion, and characterize batch cost
   without presenting it as a production latency result.
3. **Unused code:** require every identity input and authority field to change a golden or fail a
   propagation assertion; add no disconnected runtime hook.
4. **Production readiness:** keep crash replay stable, make intentional rewind/recreate distinct,
   fail closed on absent or contradictory assignment/writer authority, and retain **NO-GO**.
5. **Documentation:** define the identity preimage once in ADR-008 and keep other documents as
   concise links; do not duplicate the contract.
6. **Tests:** use hand-authored byte/digest goldens; cover length-prefix ambiguity, every identity
   axis, group-key bytes and version boundaries, crash versus rewind, assignment rotation, wrong
   owner/process term, and exact propagation into both data and marker inputs.
