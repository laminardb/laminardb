# Topology authority implementation checkpoint

## Implemented transition

The current state transition is only:

```text
Uninitialized --existing cold catalog seal--> LegacySealed
LegacySealed --fenced identical-inventory adoption--> Versioned(topology 1)
```

Old-topology preparation implements `Planned -> Quiescing -> CutPrepared`, with
pre-target-commit abort on a leader term change, definitive checkpoint Abort or
durable recovery fault. A prepared cut includes the exact committed checkpoint
and every frozen process's application receipt; intake and successor sink epochs
remain held. It reserves an exact candidate without authorizing candidate actors.
No record can commit topology 2. Local additive candidate compilation and definition
compatibility descriptors are implemented. Durable participant certificates, target
restore, retirement, Committed/Activating/Active and release remain unfinished.
Runtime DDL stays fenced.

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
metadata still means an unversioned legacy catalog. Encoding 15 binds the exact
old-topology checkpoint inventory, Commit and frozen application roster and
requires an adopted baseline. Every later lease, checkpoint,
assignment, retention, fault and release append preserves the encoding and baseline.
Successor validation rejects downgrade or baseline replacement. Old binaries
reject unsupported encodings/admission fields; an old writer paused after reading encoding 12
cannot overwrite the upgrade's create-only successor. Coordinated binary upgrade
is still a caller precondition because format rejection cannot retire old actors.

Reads validate the catalog blob, retained adoption append and deployment identity
from one immutable authority snapshot. Absence of metadata explicitly means
LegacySealed, never an inferred current version. Cleanup retains the adoption
append permanently as one extra authority root. A prune snapshot taken before
adoption cannot delete a later sequence. Live old-cut roots are now protected from
checkpoint artifact-floor advancement; target migration roots and replay mappings
still need integration.

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
capabilities, operator compatibility or new-source activation positions.

Only one request can be preparing. Identical retries return the original durable
status, including a prior abort and retry by a replacement leader process; a
different payload with the same identity fails. Current leader authority is still
required. Frozen participant membership is checked for fresh admission, not for
returning an existing request's result.
Legacy adoption identities cannot be reused for migration requests. New ordinary
checkpoint/assignment admission is rejected throughout preparation except for the
exact atomically bound old cut. Renewal preserves it;
a new leader term or recovery fault atomically aborts it and retains recovery
evidence. This policy applies only before target commit. Future committed
phases must recover the target instead of using this abort helper.

Plan payloads are capped at 32 KiB and the retained request journal at 64 identities.
Both admission and explicit abort allow 16 CAS attempts within 15 seconds. Reads
audit canonical plan/catalog blobs and the retained admission/disposition appends.
Pruning retains those appends and any pending drain admission. Settled drain proposal
cleanup follows the admitted assignment-decision floor in bounded batches. Request
journal eviction and orphan candidate/plan cleanup are not implemented; a full
journal rejects further admission. No public submit endpoint or cutover worker is
enabled, so this bound is not advertised as a complete migration retention policy.

## Local candidate planning

`LaminarDB::validate_cluster_topology_change` audits the adopted parent through
the existing catalog authority. Under the existing asynchronous catalog read
lock, it captures live canonical definitions and, for a Running database, checks
the coordinator's exact bound pipeline identity. It replays that inventory into
a private Created database, reconciles only private catalog generations, compiles
the parent, appends supported CREATEs privately and compiles the complete target.
Before returning it rechecks the live definitions, authority and lifecycle.

The private database shares only frozen connector factories. It has no controller,
catalog authority, transport, runtime, source/sink actors or retained history.
DDL's resource-presence checks consume a snapshot of the live process's shuffle
and vnode availability; normal DBs still check actual handles. That private
snapshot carries no execution, assignment, restore or publication authority.
Source queues use the existing minimum 1,024 channel slots and a one-entry empty
snapshot ring regardless of production buffer settings. Their consumer is dropped
immediately, so input is rejected and no queue drain task is spawned. At most one candidate
compiler runs locally. Parent and target managed graphs are dropped sequentially;
empty managed-state initialization still uses the configured state budget.

Planning reuses typed DDL, physical query-shape certification, schema resolution,
source/role and sink delivery admission, sink predicate compilation, graph
construction and managed-state initialization. Connector constructors/contract
methods supply metadata; no lifecycle method or latest-position query runs.
Replay-immutable changelog filters and reserved engine-column restrictions match
the existing sink path. Unmapped internal managed operators fail closed instead
of being assigned an optimizer index as a durable identity.

Definition hashes come from the same canonical payload as strict
`PipelineIdentity` encoding 7. Its serialization and ordinary recovery checks are
unchanged. Each compatibility hash separately binds the catalog name/kind and
generation, global ABI/config hash, canonical definition, resolved Arrow schema,
physical capability/managed-state contract, connector implementation name/version,
connector/cancellation contract and
sorted dependency identities. Dependency hashes bind the transitive closure.
Preserved descriptors must match exactly after the additive compilation. Target
manifest references are computed but never written. Custom UDF/UDAF and optimizer
rule implementations are rejected because their implementation identity is not
certified by this descriptor.

Local descriptor format 1 is scoped explicitly to `LocalCandidatePlan`. It
classifies additions as future-only, leaves concrete source positions unresolved,
and identifies the six activation requirements still missing. It is not a
participant receipt, state-restore mapping, source ownership token or target
commit certificate. The core pre-cut plan currently does not bind this descriptor.
Future participant certification must persist agreement over the same exact
candidate and frozen process roster before target preparation can advance.

Input is bounded to 64 individual CREATEs/256 KiB SQL, 256 total objects and a
1 MiB encoded descriptor, with a 30 second end-to-end asynchronous deadline.
The console-authenticated HTTP route adds a 512 KiB JSON-body limit and returns
local scope, parent conflicts, unsupported operations, busy and deadline results.
No new dependency, scheduler or per-record work is added.

## Old-topology checkpoint cut

The existing manual checkpoint owner drives a reserved cut. Periodic admission
defers before reserving an attempt. The controller uses its configured assignment
store and live leader/process gates; one shared append installs `Quiescing`, the
exact artifact inventory and its leader proof before Prepare or source barrier
publication. A retry can only reuse that same attempt, deployment, pipeline ABI
and complete owner/boot roster. An ordinary, unbound or mixed-flag barrier cannot
cross the reservation.

`TOPOLOGY_CUT` uses the existing source barrier, shuffle alignment, asynchronous
operator drain, sink fence and checkpoint capture contracts. Source intake closes
after its barriers arrive, before mutable state capture. A topology cut rejects
retained intermediate shuffle replay instead of treating it as a final cut. The
existing full pipeline fingerprint and state checks remain mandatory.

The normal old-topology terminal Commit and the operation's exact checkpoint
reference are persisted in the same authority append. This is a checkpoint Commit
under T, not a topology-change Commit. It leaves `Quiescing` visible. The leader
then finishes the existing globally aggregated external sink settlement; its
receipt follows that settlement. Followers report local checkpoint application.
Only the complete frozen roster, including the original leader, yields
`CutPrepared`. Each runtime-owned tail keeps successor sink publication sealed.
Missing receipts or uncertain sink responses never imply sink failure or permit
target execution. Completion publication failure reports a continuation error
while preserving a successful old checkpoint; recovery must reconcile it.

Cancellation or a lost write response is resolved by the exact operation and
attempt. Authority pruning pins admission, disposition, cut-binding and old
Commit appends. While preparation is live, status audits the canonical committed
index and cleanup cannot advance its artifact floor beyond the cut. After abort,
ordinary checkpoint/replay retention owns that index; historical operation status
still audits its retained authority anchors and does not require artifacts already
retired by normal retention.

Requested abort is allowed before binding or after all application receipts.
An unresolved `Quiescing` cut requires coordinated recovery rather than assuming
its sink outcome. An abort never reopens local intake by itself. Recovery/restart
must resume T from its reconciled committed progress; a committed old cut is not
rewound. This increment has no target worker or direct local resume shortcut.

The cut sets a dedicated local hold under the existing authority-transition lock.
Assignment refresh may retain its current certificate for checkpoint tails, but
cannot reopen intake or admit a successor sink epoch while that hold is set.
Only consumption of an authorized coordinated recovery Release clears the hold,
after retirement, restore/readiness and exact authority checks. A rejected release
preserves it. A fresh process starts with intake fenced by the existing startup
recovery protocol.

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

All additions run on control/API/startup paths. Checkpoint admission now reads
shared topology authority; cut completion adds bounded control appends. These
reads can affect checkpoint control latency and require separate measurement.
The callback retains the current exact leader proof when checkpoint reservation
returns an ID, before deadline, process and Prepare-time rechecks. If topology
admission wins after ordinary flag selection, that attempt reaches a definitive
Abort before any source barrier or intake hold. Its original proof owns cleanup;
a replacement proof cannot be substituted. The unchanged Planned request can
then bind a later cut using the same operation identity.
Reservation-only Abort requires an idle coordinator, no prepared local state,
sink intents or transactional sinks, and no admitted checkpoint artifacts. The
authority append enforces artifact absence atomically with admission. Exact retry
audits the original Abort append, so a settled admitted Abort cannot be relabeled
as unused. Once admission or capture has begun, normal cluster Abort continues
to require coordinated recovery.
The record/batch push, operator execution, Arrow ownership and shuffle envelope
paths are unchanged. There are no new per-row checks, serialization, locks or allocations.
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

## Old-cut failure matrix

| Boundary/failure | Result under current authority |
| --- | --- |
| Ordinary flag audit precedes topology admission, Prepare follows it | Retire the reserved attempt with its original proof; leave the same request Planned and intake open |
| Deadline/process check fails after exact reservation | Cleanup retains the reservation's proof even when Prepare has not run; changed authority fences it |
| Artifact admission wins a reservation-only Abort append | Reject the shortcut; preserve admitted ownership and require normal recovery |
| Cut append stalls before create | Deadline leaves the original Planned request and no artifact admission |
| New term wins the cut-binding sequence | Candidate aborts; delayed old binding is fenced before Prepare |
| Binding or Commit succeeds but response/caller is lost | Read the exact operation/attempt; preserve the binding and definitive old checkpoint |
| Old checkpoint Abort | Candidate is Aborted; admitted artifacts remain owned until exact cleanup |
| Commit exists but sink settlement or receipts are missing | Keep Quiescing and intake/sink succession held; reconcile through normal recovery |
| Final process receipt succeeds but response is lost | Status/exact retry resolves the original CutPrepared append |
| Assignment watcher refreshes its certificate during a held cut | Retain the certificate for checkpoint tails; preserve intake hold and exclude successor sink admission |
| Recovery release loses its authority or a replacement fault wins | Keep the hold and intake closed; only an authorized retry can clear it |
| Process, leader or recovery fence changes after old Commit | Abort the candidate, preserve T's irreversible checkpoint and resume through coordinated recovery |
| Live cut index or authority anchor is damaged | Fail closed, including status and authority pruning; do not infer completion |
| Newer artifact floor races with a live prepared cut | Refuse floor advancement beyond the cut; retain its canonical index |

Deterministic tests use the existing object-store fault injection and exact
semaphores at append boundaries. The three-process scenario covers a real manual
cut and restart after CutPrepared; it does not cover target installation or
post-target-commit failure. Required transactional sink migration certification
remains absent.

## Required next integration

1. Integrate candidate planning with a DB-owned migration worker and its existing
   manual checkpoint owner. The old-cut binding, capture and hold are implemented;
   detached submission/target-stage ownership remain unfinished.
2. Certify the frozen owner-complete/evidence process rosters, candidate identity,
   compatibility mapping and protocol on all required participants.
3. Observe superseded actor retirement after the reconciled old checkpoint cut.
4. Atomically bind target catalog, exact cut, state mappings, concrete source start
   positions, progress/frontiers and durable migration roots in shared authority.
5. Restore/install the target before participant-complete release, with stale
   graph/shuffle/sink completion fences and target-only post-commit recovery.
6. Wire public SQL and atomic multi-object submission, expected parent,
   payload-bound idempotency and detached durable ownership. Local dry run exists;
   it does not advance admission. Do not reuse bootstrap.
7. Preserve unchanged subscription object/sequence identity through the explicit
   pipeline-identity mapping. Whole-graph hashes currently differ on additions;
   skipping their check is unsafe.
8. Change restart configuration assertions only after target precedence is durable,
   and run the stateful multi-process migration/restart oracle and fault matrix.

The [progress file](cluster-topology-migrations-progress.md) records commands,
results and unfinished certification. The [cut validation evidence](test-evidence/topology-cut-2026-10-01/README.md)
includes the real cut/abort/restart oracle, gate hold observations, failure logs
and existing queue comparison. The latest [local candidate validation evidence](test-evidence/topology-planning-2026-10-01/README.md)
records matching reports on all three running stateful processes and the subsequent
cut/abort/restart oracle. These results do not certify target migration or
production latency.
