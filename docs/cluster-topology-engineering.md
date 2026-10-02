# Topology authority implementation checkpoint

## Implemented transition

The current state transition is only:

```text
Uninitialized --existing cold catalog seal--> LegacySealed
LegacySealed --fenced identical-inventory adoption--> Versioned(topology 1)
```

Old-topology preparation implements `Planned -> Preparing -> Quiescing -> CutPrepared`, with
pre-target-commit abort on a leader term change, definitive checkpoint Abort or
durable recovery fault. A prepared cut includes the exact committed checkpoint
and every frozen process's application receipt; intake and successor sink epochs
remain held. It reserves an exact candidate without authorizing candidate actors.
No record can commit topology 2. Local additive candidate compilation and definition
compatibility descriptors and durable participant certificates are implemented.
Exact-cut state/progress root staging includes stateless downstream additions and sealed
new-source initialization requirements. Private target restore preparation is
implemented. Actor retirement, topology Commit, install/activation and release
remain unfinished.
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
requires an adopted baseline. Encoding 16 binds the canonical candidate descriptor
and exact-process preparation certificates; new cuts require preparation protocol 2.
Encoding 17 pins immutable migration-root requirements after CutPrepared. It preserves
the same protocol-2 plan, old catalog and checkpoint allocator; it grants no target authority.
Encoding 18 requires support for source-initialization roots. Root encoding 2 carries
the new global source cursors; encoding-1 bodies retain their exact canonical bytes.
Every later lease, checkpoint,
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
checkpoint artifact-floor advancement. A staged migration root retains that live cut
pin and its own immutable authority anchor. Post-target-commit retention and replay
consumption of these mappings still need integration.

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
an admitted drain intent and topology admission. Preparation protocol 2 additionally binds the canonical local candidate report.
Admission alone does not certify participant agreement or resolve new-source positions.

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
commit certificate. The protocol-2 core plan binds the exact canonical report by
SHA-256 and length. Each required participant independently recompiles it before
persisting its agreement; a matching read-only response alone remains insufficient.

Input is bounded to 64 individual CREATEs/256 KiB SQL, 256 total objects and a
1 MiB encoded descriptor, with a 30 second end-to-end asynchronous deadline.
The console-authenticated HTTP route adds a 512 KiB JSON-body limit and returns
local scope, parent conflicts, unsupported operations, busy and deadline results.
No new dependency, scheduler or per-record work is added.

## Durable participant preparation

The same typed format-1 report is used by dry-run and durable preparation. Its
public JSON and digest encoding are unchanged; managed-codec names are now owned
strings so durable decode cannot rely on static lifetimes. `stage_topology_compatibility`
writes a content-addressed immutable report with bounded read-back. Admission binds
its reference in the immutable protocol-2 plan and upgrades shared authority to 16.
It audits exact deployment, parent/target catalogs, incarnations/classifications,
full pipeline identities and descriptor digest. It does not trust a report as
proof that any required participant has compiled it.

`LaminarDB::prepare_cluster_topology_operation` reads the authoritative target,
derives its additive CREATEs and invokes the existing isolated compiler. The caller
supplies only an operation UUID. The entire locally compiled report must equal the
admitted report. The configured controller checks live boot/term around compilation,
its exact durable local assignment adoption, current frozen assignment and process
lease authority before publishing a certificate. Process leases can use a separate
namespace from checkpoint/catalog storage; the controller supplies the configured
`ProcessLeaseAuthority` rather than deriving one from a checkpoint-store handle.

The shared append records one sorted participant/boot/process-term/protocol receipt
and its immutable authority sequence. The first receipt advances `Planned` to
`Preparing`; only the complete frozen owner/evidence roster sets `complete_sequence`.
Identical retries return existing evidence. Missing capability, divergent reports,
unknown/stale boots or terms, changed assignments, leader changes and recovery
abort/fence preparation. A completed append survives cancellation or a lost response
and remains queryable through operation status. Term-change/pre-commit abort retains
certificates and any successful parent checkpoint. Status/pruning audit and retain
each certificate append as well as the descriptor and admission binding.

New cut binding requires every frozen process certificate, current exact durable
process terms and the descriptor's full parent pipeline identity. Historical protocol-1
reservations remain readable/abortable but cannot begin a new cut. Existing historical
cut receipts remain readable and recoverable. Authority format rejection and create-only
successors fence older writers; a coordinated binary upgrade remains necessary to
retire older actors. Certificates prove compilation agreement, never actor retirement,
source positions, state restoration, target readiness or output authorization.

Preparation uses the existing single compiler and 30 second compile limit inside a
45 second total request deadline. Certificate publication uses 16 CAS attempts within
15 seconds. Reports are limited to 1 MiB, plans to 32 KiB, rosters to the existing 129
participants, journals to 64 requests and authority records to the existing 256 KiB
encoding limit. Large retained rosters can hit that record limit before 64 requests.
No detached migration worker or new task is created. Periodic checkpoint admission
defers incomplete preparation and held cuts without allocating a cut or faulting the
pipeline. Prepare-time races still retire reserved attempts with their original proof.

The DB clones its controller handle under a short synchronous lock and drops that
guard before reading or compiling. Compilation finishes before certificate publication;
no live catalog or assignment lock spans those awaits. The existing compiler permit
bounds local work, and the fenced authority CAS serializes receipts with other transitions.

| Preparation boundary | Durable/recovery behavior |
| --- | --- |
| Divergent report or missing protocol | Reject without appending a certificate; the frozen request remains unresolved |
| Only part of the frozen roster agrees | Retain receipts in Preparing; defer ordinary checkpoints and reject a new cut |
| Boot, process term or assignment changes | Reject stale evidence; current recovery/leader authority aborts the uncommitted candidate |
| Certificate append succeeds but response/caller disappears | Status audits the exact retained append; an identical live-process retry returns the original certificate |
| Old certificate anchor or descriptor is damaged | Fail closed in status, cut admission and authority pruning |

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

## Exact-cut migration root staging

`LaminarDB::stage_cluster_topology_migration_root` uses the existing controller,
configured process/assignment authorities and checkpoint store. The DB must be
Running with the old cut held, intake closed and recovery/shutdown fences clear.
The authority requires the admitting leader, unchanged assignment, complete
current process certificates and `CutPrepared`. New sources require their configured
connector's read-only initialization contract. Built-in Kafka certifies explicit
topic inventories with earliest/latest; other connectors fail closed by default.

The root reads only the exact committed index and its checksummed participant
manifest metadata. It never reads or rewrites node state or Arrow output segments.
The shared checkpoint validator verifies exact source/snapshot/channel progress,
exclusive full vnode ownership and subscription publication metadata. Recovery
uses the same progress check. State ranges, timers, watermarks, output segments,
source positions and sink decisions remain referenced through the old cut.

Every preserved catalog object has its exact kind, incarnation and certified
compatibility digest. Stream state uses the existing `graph:<canonical name>`
identity; unrecognized frames and managed streams without preserved vnode state
are rejected. New stateless streams/sinks are explicitly future-only. No existing
managed stream can silently receive empty state.

Preserved subscriptions retain their stream generation, schema, final operator,
distribution, query/changelog/retention contracts and complete partition sequence
vector. Their required target certificate changes only the pipeline identity.
Target installation must consume this explicit mapping instead of recomputing a
stream generation from the new whole-graph hash. These certificates are staged
requirements; they do not authorize target replay or output.

The canonical root is create-only and content-addressed. One format-17 or format-18 authority
append pins its reference and first sequence while leaving `CutPrepared` and
catalog T unchanged. Retries return that immutable binding. Status and pruning
audit its canonical body, exact certified plan/cut and first append. A live root
retains the existing cut artifact-floor pin. Abort retains its metadata and
authority evidence; ordinary checkpoint/replay retention then owns old artifacts.
Target-commit retention remains unfinished.

New-source positions use the existing `ConnectorCheckpoint` encoding, including
Kafka's numeric next-to-read baselines for empty/never-read partitions. They have
no checkpoint attempt and no source-assignment version: the complete global
inventory is an initialization requirement, never evidence of processed input or
ownership. Name, new catalog generation and compatibility digest must match the
certified descriptor. The target installer must validate the cursor, adopt only
assigned channels and fault if retention/inventory changed; that consumption is
still unfinished. Ordinary Kafka startup continues to reject unsealed `latest`
with guaranteed delivery.

The DB uses its existing single compiler slot, privately replays the immutable
target with configured factories, reconciles durable stream generations and rechecks its strict pipeline/environment
identity, and calls only new-source initialization hooks. The authority invokes
this resolver after complete current process/assignment checks. A new root is
sealed create-only at
`control/topology-source-root-staging/v1/<operation UUID>/<plan SHA-256>.json` before
content-addressed root publication. This slot is not an authority head or target
commit. Its first canonical vector wins simultaneous attempts. A retry reads the
slot before resolving connectors and reconstructs the exact metadata requirements;
after publication, the authoritative body no longer depends on the slot. Reads
cancelled before a successful seal grant no boundary. Unknown seal outcomes are
resolved by read-back; missing/corrupt/noncanonical/oversized evidence fails closed.
A replacement leader aborts the existing pre-commit operation, preserving the
parent checkpoint. It cannot use a staging slot to run or commit the target.

Kafka discovers all explicit partitions and their broker low/high watermarks
without subscribing, assigning, polling records or committing group offsets.
`earliest` seals each numeric low watermark; `latest` seals each numeric high
watermark. This is a partition vector, not an atomic cross-partition snapshot or
a wall-clock activation timestamp. High-watermark initialization explicitly skips
the prefix before that vector, including transactional records in that prefix;
it does not change isolation or delivery guarantees of any running source.
The contract uses the existing
[rdkafka watermark metadata API](https://docs.rs/rdkafka/0.39.0/rdkafka/consumer/trait.Consumer.html#tymethod.fetch_watermarks).
Topic patterns, mutable broker group offsets, timestamps and specific-offset modes
remain uncertified for migration initialization.

Kafka allows 64 explicit topics/4,096 partitions and a 10 second total lookup
budget, bounded by the authority's existing 15 second deadline. One process-wide
metadata-client permit remains in the existing tracked native task through final
drop, so cancelled retries cannot accumulate clients. Native creation/read/drop
stay off Tokio workers; automatic topic creation and offset commit/storage are
disabled. Source actors, sink effects and per-record paths are unchanged.
The root and its staging slot are each at most 1 MiB. Existing cleanup does not
sweep this control prefix. With the existing 64-request journal bound, source-root
slots add at most 64 MiB; their eventual cleanup belongs to journal retention.

The DB call has a 30 second total deadline; the authority allows 16 CAS attempts
within 15 seconds. Root bodies are bounded at 1 MiB and participant metadata at
16 MiB in aggregate before reads. Sequential metadata reads bound preparation
memory independently of state/output payload size. There is no new task owner,
scheduler, generic workflow, dependency or per-record work.

| Failure | Result |
| --- | --- |
| Missing/changed manifest, divergent progress or state identity | Reject without a root authority append |
| Unresolved new source or incompatible subscription | Reject with an explicit initialization/compatibility reason |
| Deadline/cancellation before append | Preserve CutPrepared; an unreferenced content blob grants no authority |
| Source slot created but response/caller is lost before authority append | Read the same canonical slot; never reevaluate the sealed latest vector |
| Concurrent different unsealed metadata reads | First create-only vector wins; discarded reads grant no boundary |
| Source slot is corrupt or exceeds 1 MiB | Reject before connector I/O; do not reset the slot |
| Append succeeds but response/caller is lost | Exact status/retry resolves the original root binding |
| Leader/process/assignment changes | Fence staging; coordinated recovery retains the old cut |
| Root or first authority anchor is damaged | Status and pruning fail closed |

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

Private restore preparation now connects the staged root to the existing strict
recovery loader and operator codecs. `TopologyRestoreInput` is opaque and comes
from the controller's actual configured process/assignment authorities. Its full
prepared roster, exact admitting leader, assignment and committed old cut are
checked before loading, and checked again by the DB before returning an image.
The loader keeps the historical parent identity and reconstructs the complete
root from verified manifests before reading state. Ordinary recovery remains
strict; there is no target-fingerprint bypass.

The isolated compiler can retain its unstarted graph using existing frozen
shuffle context for channel decoding. It installs no live graph/vnode handles or
actors. Local frames use the same roster checks, checksum reads and state codecs
as ordinary restore. Subscription generations and next sequences come from the
root, rather than a fresh target-wide hash. Existing source positions retain their
real checkpoint origin; initialized positions remain unowned with no processed
attempt. Kafka checks the sealed inventory and retention bounds without resealing.

An opaque `PreparedTopologyRestore` owns the existing compiler permit. It cannot
be cloned, constructed or executed by an external caller. Errors, cancellation
and a 45-second deadline discard partial state while retaining the old hold. No
new authority encoding, log append, worker, dependency or generic migration
framework is introduced. Manifest reads remain eight at a time, subscription
segment reads four at a time, and existing verified chunk read bounds remain.
Their borrowed work items are materialized before awaiting so the restore future
can safely move to an owned Tokio task; no payload is cloned for this change.

The target's managed state and verified encoded payload are separately bounded
by the configured state budget, with configured node-read limits, 16 MiB total
manifest metadata and a 1 MiB root. Encoded payloads are dropped before cursor
validation. The held old graph, decoded target, codec scratch and object-store
read buffers still overlap. Total RSS and migration allocation amplification
require measurement; no performance certification follows from those limits.

| Private restore failure | Result |
| --- | --- |
| Root absent, unknown process/boot/term, incomplete roster or changed assignment | Reject before target state preparation |
| Root requirements differ from verified parent manifests | Reject before state payload reads |
| Ordinary recovery attempts the target fingerprint against the parent cut | Retain the existing fingerprint mismatch rejection |
| Payload/segment missing, corrupt or exceeds configured limits | Drop partial target; keep the exact parent hold |
| Cursor inventory changes or retention passes a sealed cursor | Reject; never resolve a replacement latest boundary |
| Source validation blocks or caller cancels after decoding | Deadline/cancellation drops image and releases the compiler permit |
| Recovery or authority changes after decoding | Reject the late image; no target receipt or activation |
| Caller retains a successful image | Keep the compiler busy; future install must revalidate its authority |

1. Integrate candidate planning with a DB-owned migration worker and its existing
   manual checkpoint owner. The old-cut binding, capture and hold are implemented;
   detached submission/target-stage ownership remain unfinished.
2. Drive the implemented exact-process certification path from detached submission;
   explicit local preparation is available, but automatic collection remains unfinished.
3. Observe superseded actor retirement after the reconciled old checkpoint cut.
4. Drive the implemented private restore preparation from owned migration work,
   then atomically bind target catalog and root at the logical topology-change Commit.
5. Restore/install the target before participant-complete release, with stale
   graph/shuffle/sink completion fences and target-only post-commit recovery.
6. Wire public SQL and atomic multi-object submission, expected parent,
   payload-bound idempotency and detached durable ownership. Local dry run exists;
   it does not advance admission. Do not reuse bootstrap.
7. Consume staged subscription identity/frontier mappings during target install
   and replay. Whole-graph hashes differ on additions; skipping their check is unsafe.
8. Change restart configuration assertions only after target precedence is durable,
   and run the stateful multi-process migration/restart oracle and fault matrix.

The [progress file](cluster-topology-migrations-progress.md) records commands,
results and unfinished certification. The [cut validation evidence](test-evidence/topology-cut-2026-10-01/README.md)
includes the real cut/abort/restart oracle, gate hold observations, failure logs
and existing queue comparison. The [local candidate validation evidence](test-evidence/topology-planning-2026-10-01/README.md)
records matching dry-run reports. The latest [participant certification evidence](test-evidence/topology-preparation-2026-10-01/README.md)
records independently compiled durable receipts on all three running stateful
processes and the subsequent cut/abort/restart oracle. The
[root staging evidence](test-evidence/topology-root-2026-10-01/README.md) records
immutable state/progress requirements from those real checkpoint manifests and
their retained binding after restart. The latest
[source initialization evidence](test-evidence/topology-sources-2026-10-02/README.md)
records sealed Kafka earliest/latest cursors, cancellation/concurrency boundaries,
legacy root bytes and the subsequent three-process cut/abort/full-restart oracle.
The [private restore evidence](test-evidence/topology-restore-2026-10-02/README.md)
records exact root authorization, operator decoding, cursor availability and
bounded-image ownership/cancellation checks.
These results do not certify target migration
or production latency.
