# Iceberg row-mutation capability decision

- **Status:** accepted
- **Updated:** 2026-08-28
- **Applies to:** embedded and single-node Iceberg connectors; cluster admission remains unchanged

## Decision

LaminarDB exposes typed `merge-on-read` and `copy-on-write` Iceberg write modes but rejects both
during connector open against the released `iceberg-rust` 0.10.1 dependency. Rejection happens
before catalog access or data-file creation. Changelog reads fail at the same boundary.

The connector does not translate either mutation mode to append, set table properties as a proxy
for row mutation, edit Iceberg metadata JSON, or construct manifests outside the Iceberg library.
Production writes use only the public atomic `FastAppend` transaction action.

## Resolved API evidence

The 0.10.1 public API provides `FastAppend`, rolling data-file writers, partition splitting, and an
equality-delete writer. It does not provide public transaction actions for `RowDelta`,
`OverwriteFiles`, `RewriteFiles`, `ReplacePartitions`, or `DeleteFiles`. It also lacks a public
position-delete writer and complete scan-side reconciliation of data and delete files. Equality
delete-file construction alone cannot provide atomic merge-on-read semantics.

## Enablement conditions

Merge-on-read may be enabled only after one released dependency set provides and tests:

- atomic `RowDelta` publication of data and equality or position delete files;
- sequence-number-correct scan-side delete reconciliation;
- identifier-field validation and bounded per-checkpoint changelog collapse;
- replay reconciliation proving at most one logical commit after an unknown outcome; and
- delete-file compaction and observability integrated with fenced maintenance.

Copy-on-write may be enabled only after one released dependency set provides and tests:

- bounded affected-file planning with existing deletes applied;
- atomic add-and-remove transaction actions with serializable conflict validation;
- deterministic replacement-file replay and unknown-outcome reconciliation; and
- an explicit limit failure instead of a whole-table rewrite fallback.

Each mode requires its own capability gate and recovery matrix. Cluster exactly-once admission is a
separate decision and is not implied by local connector support.

## Consequences

Append snapshot reads, append lineage reads, direct append writes, and coordinated exactly-once
append writes can evolve independently without overstating row-mutation support. An upstream API
gap produces a stable `FeatureUnsupported` error rather than weaker delivery semantics.
