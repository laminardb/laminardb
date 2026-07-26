# Distributed keyed state Cycle 51 review

- **Date:** 2026-07-26
- **Scope:** step 1 of the Cycle 50 delivery-evidence sequence: make the missing distributed-output
  authority semantics executable in the standalone independent-soak fixture
- **Cycle outcome:** schema v2 now proves the frozen bootstrap, recovery, assignment, marker,
  ownership, replay, ordering, byte-equality, and final-result relationships
- **Admission/backend outcome:** unchanged; no runtime crate, Kafka path, state backend, dependency,
  endpoint, metric, capability, or admission guard changed
- **Production verdict:** **NO-GO**; the fixture is synthetic, root-workspace-excluded, and explicitly
  `certification_eligible=false`, and no independent soak ran

## Result and exact boundary

`tools/independent-soak-contract` now dispatches oracle fixtures explicitly by schema version. The
existing v1 verifier and its nine-case fixture retain their behavior. V2 is a separate typed,
unknown-field-denying model; unsupported versions fail explicitly and every CLI result still starts
with `NOT CERTIFICATION EVIDENCE`.

The canonical v2 case fixes an exact source inventory, including an empty partition whose
pre-delivery baseline is nonzero, and an exact sink inventory. A zero-input bootstrap checkpoint at
assignment 7 precedes the first marker and sink admission. A later recovery checkpoint seals the
exclusive source cut and resolves its base assignment independently of the successor's current
assignment 8. Every sink partition carries consistent predecessor and successor marker chains.

Data verification derives expected operations from the broker-shaped source ledger between each
partition's baseline and frozen exclusive cut. It checks raw payload bytes, logical operation
identity, group-result versions, per-`(shard, writer interval)` admission sequence, writer/assignment
authority, and independently derived vnode-to-shard ownership. Byte-identical replay is legal
across writer intervals only when its raw causal source offset is at or after the recovery cut;
versions must strictly rise within one interval. The oracle also checks the exact final grouped
count/sum state.

The verdict boundary is deliberate:

| Evidence condition | V2 verdict |
| --- | --- |
| Canonical complete and consistent evidence | `MODEL_MATCH` |
| Missing/incomplete source, sink, checkpoint, bootstrap, or assignment authority | `RUN_INVALID` |
| Checkpoint/assignment authority selected for a different run | `RUN_INVALID` |
| Complete sink capture with a wrong-run marker | `PRODUCT_FAIL` |
| Complete evidence proving stale ownership, illegal replay, conflict, misordering, or wrong result | `PRODUCT_FAIL` |

This remains a semantic fixture, not a production envelope. Assignment ownership and local process
term are pre-reconciled test evidence, although production must obtain them from separate supported
views. The fixture freezes key-to-vnode and vnode-to-shard semantics, not exact Kafka partition or
header bytes. It supplies no transactional producer, broker fencing, public evidence projection,
runtime integration, backend qualification, or exactly-once proof.

## Independent review corrections

The first independent reviews rejected several important edges before approval. The final model
now includes empty source partitions and nonzero baselines; rejects empty authority identities and
assignment regression; resolves recovery-base assignments through authority evidence; treats a raw
offset equal to an exclusive recovery cut as legal; requires versions to increase within one writer
interval; checks exact source and sink key sets; and separates wrong-run authority (`RUN_INVALID`)
from wrong-run product markers (`PRODUCT_FAIL`). Tests also cover the first-marker, marker-chain,
envelope-version, overflow, and exact diagnostic-set boundaries. Two semantic reviewers approved
the corrected source without editing it.

## AI slop review

**Pass.** The change executes the already-frozen oracle rules in the existing standalone tool. It
does not invent a second runtime authority, checkpoint protocol, sink abstraction, or backend. The
canonical fixture uses concrete offsets, assignments, partitions, markers, and outputs rather than
placeholder prose or self-reported expected verdicts.

## Overengineering and hot-path review

**Pass.** All work is outside the Laminar workspace and hot path. V2 is intentionally separate from
v1 so compatibility logic does not obscure either contract. Common writer provenance remains on
partition markers; the data model does not add a payload digest or per-row vnode. Exact compact
bytes, caps, and allocation behavior are deferred to the next bounded cycle instead of being mixed
into the semantic model.

## Unused-code review

**Pass.** Every v2 evidence field participates in parsing or a verdict rule, and mutation tests
exercise the material boundaries. An initially tautological bootstrap check and an unused helper
argument were removed. Clippy with warnings denied passes, and no runtime placeholder, feature flag,
dependency, or generic adapter was added.

## Production-readiness review

**NO-GO.** The executable model narrows ambiguity but does not make its evidence observable from a
cluster. No Kafka transaction/fencing behavior, public assignment/checkpoint/process evidence,
stable runtime operation identity, backend integration, failover path, end-to-end latency result,
or independent release-binary soak exists. Cluster keyed aggregates, windows, and stateful joins
must remain fail-closed, and no delivery guarantee changes.

## Documentation review

**Pass.** ADR-008, both implementation plans, the validation report, and the soak charter now state
that v1 is the legacy limited fixture and v2 is synthetic executable semantics only. They also
record its pre-reconciled assignment/process limitation, vnode-to-shard-only ABI boundary, next
wire-format step, and unchanged production NO-GO. Existing backend research remains labelled as
historical, rejected, parked, or selected-but-stopped decision lineage; none became obsolete in this
cycle, so none was removed.

## Test review

**Pass for the bounded fixture scope.** Final validation includes:

- 22 library tests, 6 CLI tests, and doc tests via the standalone locked manifest;
- Clippy over all standalone targets with warnings denied;
- standalone and root formatting checks plus `git diff --check`;
- direct v1 and v2 CLI verification, preserving v1's `3 MODEL_MATCH / 4 PRODUCT_FAIL / 2
  RUN_INVALID` distribution and giving one v2 `MODEL_MATCH`;
- local-link and dependency-hygiene checks; and
- independent semantic and scope reviews after correcting the initial findings.

These tests prove the standalone model's deterministic classifications. They are not a backend
candidate run, Kafka integration test, distributed soak, or certification result.

## Cycle 52 review plan

Implement only step 2 of the frozen sequence: compact byte-golden data headers and partition
markers inside the standalone tool.

1. **AI slop:** define one minimal versioned wire contract whose fields are consumed by the v2
   oracle; do not create a generic serialization framework.
2. **Overengineering/hot path:** keep common provenance in markers, retain compact per-record
   identity/interval/sequence only, and measure encoded size and allocation-sensitive batch decode.
3. **Unused code:** require byte goldens and malformed-input tests for every version, flag, length,
   and cap; add no runtime hook.
4. **Production readiness:** fail closed on unknown versions, truncation, oversized counts/strings,
   arithmetic overflow, trailing bytes, and invalid UTF-8 while preserving the overall NO-GO.
5. **Documentation:** record exact byte/cap decisions once in ADR-008 and link concise summaries;
   do not duplicate a wire specification across reports.
6. **Tests:** cover stable encode/decode goldens, hostile decoding, size ceilings, cross-version
   rejection, and a bounded batch benchmark. Do not add Kafka, runtime, backend, endpoint, metric,
   admission, or certification behavior in this cycle.
