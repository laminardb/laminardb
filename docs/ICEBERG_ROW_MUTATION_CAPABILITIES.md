# Iceberg row-mutation capability decision

- **Status:** accepted
- **Updated:** 2026-08-30
- **Applies to:** Iceberg connectors in embedded, single-node, and cluster modes

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

Each mode requires its own capability gate and recovery matrix. Cluster exactly-once admission is
independently limited to coordinated append through a REST catalog and direct S3/S3A storage, with
static bearer or no catalog authentication and no access delegation. That append certification
does not imply cluster support for either row-mutation mode.

## Consequences

Append snapshot reads, append lineage reads, direct append writes, and coordinated exactly-once
append writes can evolve independently without overstating row-mutation support. An upstream API
gap produces a stable `FeatureUnsupported` error rather than weaker delivery semantics.

## Maintenance boundary

The released dependency exposes a snapshot-expiration action, but LaminarDB does not invoke it
from a connector instance. Safe expiration needs one fenced authority with the complete inventory
of active source cursors, retained checkpoint recovery points, and unresolved external commits.
No such cross-pipeline maintenance authority exists today. Connector-local expiration would risk
removing replay or reconciliation state.

While a checkpoint participant remains alive, it retains a bounded, exact in-memory inventory of
staging paths and final files created by its current epoch. Before a descriptor is issued, Abort or
close deletes only those exact owned paths. After descriptor issuance, a proven durable Abort does
the same; a successor epoch or close removes staging paths but leaves potentially published final
files intact. The checkpoint abort seal retains durable participant descriptors across process
loss. Local recovery or the current cluster leader reconciles publication evidence, deletes only
the exact descriptor paths, and durably marks cleanup before checkpoint node data can be sealed.
An unresolved publication fences that cleanup. This lifecycle is not a table-wide orphan scan and
does not infer reachability from object-store listings. Process loss before descriptor durability
can still strand staging or finalized paths; reclaiming those requires the shared fenced
maintenance authority described above.

The released API also lacks the transaction actions needed for data-file compaction, delete-file
rewrites, manifest rewrites, and format-aware orphan cleanup. LaminarDB therefore starts no
per-connector maintenance loop and never deletes files from a publication whose outcome is
unknown. These operations remain externally managed until a shared fenced maintenance lifecycle
and the necessary released Iceberg actions are both available.
