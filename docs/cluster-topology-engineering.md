# Topology authority implementation checkpoint

## Implemented transition

The current state transition is only:

```text
Uninitialized --existing cold catalog seal--> LegacySealed
LegacySealed --fenced identical-inventory adoption--> Versioned(topology 1)
```

Core pre-cut admission now implements `Planned -> Aborted`, including abort on a
leader term change or durable recovery fault. It reserves an exact candidate and
assignment without authorizing candidate actors. No record can commit topology 2.
Preparing, Quiescing, CutPrepared, Committed, Activating and Active still need the
working cut, restore, retirement and release contracts. Runtime DDL stays fenced.

The existing append-only `LeaderLeaseStore` is the serialization point. Each
authority append uses a create-only sequence object and the store's conditional
head publication/reconciliation contract. This repository does not use Raft.
Adoption uses that exact append machinery rather than publishing a second head.
The durable upgrade identity is its immutable append; ordinary head recovery can
finish publication after cancellation or an unknown write response.

## Identities and evidence

`TopologyVersion` is a nonzero logical counter with checked succession.
`TopologyOperationId` is a non-nil request UUID. Neither replaces checkpoint
epoch/attempt, assignment version, leader fencing token, process incarnation, or
catalog-object generation. `CatalogManifestRef.version` remains encoding 1.

`LegacyTopologyBaseline` contains protocol 1, topology 1, the original catalog
reference, existing canonical deployment UUID, operation UUID and exact authority
sequence. Validation binds it to the lease's sealed inventory. It cannot change
the inventory, generations or state ABI. The adoption API reads and validates
the original blob and existing deployment identity before appending; it never
initializes a missing identity or rewrites historical checkpoints.

Encoding 12 omits the new optional fields, preserving its canonical serialization.
Encoding 13 requires a valid baseline. Encoding 14 adds assignment reservations
and the pre-cut request journal. It may precede baseline adoption; missing baseline
metadata still means an unversioned legacy catalog. Every later lease, checkpoint,
assignment, retention, fault and release append preserves the encoding and baseline.
Successor validation rejects downgrade or baseline replacement. Old binaries
reject unsupported encodings/admission fields; an old writer paused after reading encoding 12
cannot overwrite the upgrade's create-only successor. Coordinated binary upgrade
is still a caller precondition because format rejection cannot retire old actors.

Reads validate the catalog blob, retained adoption append and deployment identity
from one immutable authority snapshot. Absence of metadata explicitly means
LegacySealed, never an inferred current version. Cleanup retains the adoption
append permanently as one extra authority root. A prune snapshot taken before
adoption cannot delete a later sequence. Future migration roots and replay pins
still need integration with checkpoint/artifact retention floors.

## Pre-cut admission and assignment serialization

`publish_assignment_drain` stages the exact canonical snapshot under a content
address, then reserves it through the existing shared authority append before
writing the assignment snapshot. The reservation survives caller cancellation and
lease changes. Snapshot watchers and the rebalance driver materialize the original
intent after interruption. Materialization does not transfer vnode ownership or
open intake. An exact drain/recovery decision must settle the reservation; drain
settlement materializes its intent before clearing it. Recovery's existing separate
materialization winner continues to fence delayed old raw snapshot writes.

The production graceful-drain writer uses this path. The low-level snapshot CAS is
storage materialization, not cluster admission. Core callers supply the actual
namespace-verified assignment store owned by the controller; HTTP callers cannot
choose a store. Seed assignment creation still precedes migration admission.

`admit_topology_plan` checks an explicitly adopted parent, exact ordered predecessor
inventory, assignment map and boot roster, unresolved authority and prior consumed
assignments. It stages the target and canonical request, then appends one payload-bound
reservation. Assignment reservations, recovery decisions and checkpoint artifact
admission contend on that same sequence. There is no check-then-publish gap between
an admitted drain intent and topology admission. This does not yet certify participant
capabilities, operator compatibility, source positions or a checkpoint cut.

Only one request can be Planned. Identical retries return the original durable
status, including a prior abort and retry by a replacement leader process; a
different payload with the same identity fails. Current leader authority is still
required. Frozen participant membership is checked for fresh admission, not for
returning an existing request's result.
Legacy adoption identities cannot be reused for migration requests. New ordinary
checkpoint/assignment admission is rejected during Planned. Renewal preserves it;
a new leader term or recovery fault atomically aborts it and retains recovery
evidence. This policy applies only to the implemented pre-cut phase. Future committed
phases must recover the target instead of using this abort helper.

Plan payloads are capped at 32 KiB and the retained request journal at 64 identities.
Both admission and explicit abort allow 16 CAS attempts within 15 seconds. Reads
audit canonical plan/catalog blobs and the retained admission/disposition appends.
Pruning retains those appends and any pending drain admission. Settled drain proposal
cleanup follows the admitted assignment-decision floor in bounded batches. Request
journal eviction and orphan candidate/plan cleanup are not implemented; a full
journal rejects further admission. No public submit endpoint or cutover worker is
enabled, so this bound is not advertised as a complete migration retention policy.

## Bounds, ownership and locks

Adoption allows at most 16 CAS attempts within a 15 second deadline. Status/catalog
metadata reads have a 15 second deadline. Existing limits still cover authority
JSON (256 KiB), catalog bytes (8 MiB), inventory cardinality and reference formats.
No new dependency or unsafe code is introduced.

The store primitive has no new local lock. DB status clones its catalog-store
handle under a short parking_lot guard, releases it before I/O, and checks local
activation fences after the read. Exact startup replay records the version only
after the complete ordered inventory matches. Source intake release, Running
state and live process/recovery authority are separately required for local
activation. No synchronous lock spans an await in the new code.

All additions run on control/API/startup paths. The record/batch push, operator
execution, Arrow ownership, shuffle envelope and sink publication paths are
unchanged. There are no new per-row checks, serialization, locks or allocations.
There is not yet a topology generation check at transport/install boundaries;
that missing protection is a reason live migration remains disabled.

## Adoption failure matrix

| Boundary/failure | Replacement/retry behavior | Deterministic coverage |
| --- | --- | --- |
| Before create | Legacy inventory remains authoritative | Cancellation before create |
| Concurrent same inventory | One append wins; contenders return the original baseline | Concurrent identities/identical retry |
| Lease renewal wins create race | Preserve renewal and retry exact evidence | Semaphore-controlled append race |
| Leader term changes before create | Fence old proof; do not adopt | Term-loss race |
| Create succeeds, response/head publication lost | Read/discover the exact created record; preserve operation identity | Lost-response and cancellation tests |
| Renewal/checkpoint after adoption | Preserve baseline and historical outcome links | Unresolved checkpoint plus later Commit |
| Cleanup after many renewals | Retain and audit adoption anchor | Forced prune test |
| Missing/corrupt catalog, anchor or deployment | Fail closed; never infer a version or create identity | Damaged-artifact/reconstruction tests |
| Blocked status read | Cancel at deadline; retry the read | Paused-time blocking-store test |
| Local recovery/process fence | Durable version stays visible; local activation becomes null | DB status and HTTP authorization tests |

These are authority/adoption tests. They do not prove state-preserving graph
migration, participant-complete activation or exactly-once external effects.

## Required next integration

1. Integrate the implemented admission reservation with a DB-owned cutover worker.
   Bind its checkpoint attempt before permitting cut barrier/artifact admission;
   ordinary checkpoints remain blocked while the pre-cut reservation is Planned.
2. Certify the frozen owner-complete/evidence process rosters, candidate identity,
   compatibility mapping and protocol on all required participants.
3. Establish the old graph's committed checkpoint cut without blocking source
   barrier arrival. Reconcile old prepared sink outcomes and observe retirement.
4. Atomically bind target catalog, exact cut, state mappings, concrete source start
   positions, progress/frontiers and durable migration roots in shared authority.
5. Restore/install the target before participant-complete release, with stale
   graph/shuffle/sink completion fences and target-only post-commit recovery.
6. Wire public SQL and atomic multi-object submission, dry run, expected parent,
   payload-bound idempotency and detached durable ownership. Do not reuse bootstrap.
7. Preserve unchanged subscription object/sequence identity through the explicit
   pipeline-identity mapping. Whole-graph hashes currently differ on additions;
   skipping their check is unsafe.
8. Change restart configuration assertions only after target precedence is durable,
   and run the stateful multi-process migration/restart oracle and fault matrix.

The [progress file](cluster-topology-migrations-progress.md) records commands,
results and unfinished certification. The [queue benchmark evidence](test-evidence/topology-adoption-2026-09-30/README.md)
measures existing steady queue behavior only.
