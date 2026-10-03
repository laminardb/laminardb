# Topology authority implementation checkpoint

## Implemented transition

The catalog transitions are:

```text
Uninitialized --existing cold catalog seal--> LegacySealed
LegacySealed --fenced identical-inventory adoption--> Versioned(topology 1)
Versioned(T) --atomic target catalog/root Commit--> Versioned(T+1, installation pending)
```

Old-topology preparation implements `Planned -> Preparing -> Quiescing -> CutPrepared`, with
pre-target-commit abort on a leader term change, definitive checkpoint Abort or
durable recovery fault. A prepared cut includes the exact committed checkpoint
and every frozen process's application receipt; intake and successor sink epochs
remain held. It reserves an exact candidate without authorizing candidate actors.
The internal DB/core path can commit topology 2 with its exact recoverable root.
Local additive candidate compilation and definition
compatibility descriptors and durable participant certificates are implemented.
Exact-cut state/progress root staging includes stateless downstream additions and sealed
new-source initialization requirements. Private target restore preparation is
implemented. Exact-root parent retirement now observes existing actor/connector
owners while retaining the runtime and namespace fences. Exact-process target
preparation receipts now record those observations in the same authority log.
Explicit private reconstruction after Commit is implemented, including before the
first target checkpoint. Exact-Commit transport preparation and local runtime
installation with intake held are implemented. Current installation receipts,
participant-complete durable Release and local application are now implemented.
The existing DB-owned recovery supervisor now drives these migration phases.
Automatic target runtime recovery and public submission remain unfinished.
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
sequence. Validation retains it as the first committed inventory. Later decisions
form a checked version/manifest chain ending at the lease's catalog reference.
Adoption cannot change the inventory, generations or state ABI. The adoption API reads and validates
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
Encoding 19 records historical target restore/parent retirement observations under
the same prepared cut. Each reporting process must implement target preparation
protocol 3; the admitted candidate plan remains protocol 2. Earlier encodings omit
the empty receipt vector and retain their original bytes. Older writers fail closed
on encoding 19; this storage gate does not by itself retire cached actors.
Encoding 20 admits protocol-4 target preparation evidence and irreversible target
Commit. Protocol-3 receipts remain readable but cannot authorize Commit or be
rewritten as protocol 4; abort and admit a new operation after coordinated upgrade.
The protocol-2 plan and root encodings remain unchanged. Earlier statuses omit
the absent Commit field. All participants must certify protocol 4 before Commit.
Encoding 21 adds protocol-5 exact-runtime installation receipts and Release. Earlier
statuses omit the absent activation field. A receipt requires actual held actor,
state and transport readiness. Every current owner/evidence process must certify
protocol 5 before Release. Encoding 20 readers reject the upgrade; capability
advertisements do not replace coordinated binary upgrade or actor retirement.
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
pin and its own immutable authority anchor. A pending committed target retains
every preparation/Commit/root/old-cut authority anchor and the old checkpoint
artifact pin. Consumption after target installation/checkpoints and reference-aware
retention cleanup still need integration.

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

Only one request can be preparing or awaiting committed target installation.
Identical retries return the original durable
status, including a prior abort and retry by a replacement leader process; a
different payload with the same identity fails. Current leader authority is still
required. Frozen participant membership is checked for fresh admission, not for
returning an existing request's result.
Legacy adoption identities cannot be reused for migration requests. New ordinary
checkpoint/assignment admission is rejected throughout preparation except for the
exact atomically bound old cut. Renewal preserves it;
a new leader term or recovery fault atomically aborts it and retains recovery
evidence. This policy applies only before target Commit. A committed operation
retains its Commit under leader replacement or a recovery fault and rejects abort.

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
Pending target Commit retains this pin. Post-activation root retirement remains unfinished.

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
Record push and Arrow batch ownership remain unchanged. Logical topology checks now
run at graph/ownership/batch boundaries and the existing stream handshake; see the
transport preparation checkpoint below. There are no new per-row checks,
serialization, locks or allocations. Target actors and participant-complete Release
remain required before live migration can be enabled.

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

## Private target restore and retirement

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

The private image can now drive `retire_cluster_topology_parent`. Its compiler
guard identifies the originating DB without a second identity registry. Current
root authority and the exact live parent are checked before retirement and after
terminal observation. The existing stop path has one explicit topology-retirement
authority: it retains `ShuttingDown`, the cut hold and the checkpoint namespace,
rather than publishing `Created` or enabling a public restart. No authority format,
phase, receipt, scheduler, dependency or per-record check changes.

Retirement uses the existing lock order: startup ownership/state claim, topology
write ownership, lifecycle mutex, watcher mutex, then the graph-rotation write
fence. Synchronous guards do not cross awaits. The watcher stays in its DB mutex
while joined; sources, sinks and connector children stay in their stable registries
across cancellation. The existing stop code observes compute exit, retires vnode
claims, waits for issued checkpoint decisions, reconciles the sink-open witness and
observes all connector termination. A sink close result alone never grants success.
Only after those checks and a fresh complete root authorization is the image's
local retirement observation set. The target remains inactive and still owns the
compiler permit. Commit and the future installer must revalidate this potentially stale
observation; old shuffle transport remains process-owned and needs generation fencing.

| Parent retirement failure | Result |
| --- | --- |
| Foreign image, lost intake hold or stale authority before stop | Reject without cancelling the parent runtime |
| Compute, source/sink actor or connector child remains live | Deadline/cancellation retains the runtime boundary, namespace and unresolved owners |
| Watcher panic or runtime/recovery fault | No retirement observation; recovery or terminal shutdown must take over |
| Leader/process/assignment/recovery changes during stop | Keep the parent stopped and intake held; no readiness receipt or target authorization |
| Retry with the same current image | Resume existing cleanup and revalidate the same root; no new catalog/root write |
| Image is dropped after retirement | Free private target state and compiler slot; retain the runtime/cut/namespace fences |
| Pre-commit abort followed by coordinated recovery | Existing recovery stop can take over the retired parent; resume T from its reconciled cut |

`certify_cluster_topology_target_preparation` connects a retained image and the
same observed lifecycle to a durable historical receipt. It always rechecks
retirement, then the controller checks local process/adoption around the authority
append. The DB accepts no caller-supplied root, process identity or terminal flag.
The existing compiler permit stays with the image. The 45-second total cooperative
budget covers cleanup, publication and final checks; no new detached worker or
task registry is introduced. Retirement releases its asynchronous lifecycle locks
before control-store I/O; public lifecycle/mutation fences keep the held boundary.
No synchronous guard crosses an await.

One sorted receipt binds the exact candidate-certificate participant/boot/term,
target preparation protocol and the first immutable append. New DB observations
use protocol 4; historical protocol-3 observations remain readable. The enclosing
operation fixes the plan, descriptor, assignment, root and old checkpoint. Each
append adds exactly one participant after root publication. Full preparation
requires all frozen owner/evidence processes. Authority reads audit every receipt
against its original append; pruning retains every anchor, including after abort.
Existing 129-participant and 256 KiB authority bounds remain enforced.

Receipt/status progress may advance while another image is being restored or held.
`same_restore_requirements` compares every immutable input field, including phase,
leader, process term, assignment, cut, root, checkpoint, target and descriptor.
It permits only the separately validated receipt/status progress. Ordinary
PipelineIdentity and historical checkpoint bytes are unchanged.

| Target preparation failure | Result |
| --- | --- |
| Only part of the frozen roster reports | Retain partial observations in CutPrepared; no target authorization |
| Another participant appends concurrently | Existing authority CAS retries without changing either restore binding |
| Create succeeds but reply is lost or waiter cancels | Authoritative status/read reconciliation finds the same append; identical retry appends nothing |
| Deadline before create | Retain the old cut/root and unresolved runtime owners; no receipt |
| Leader replacement wins the pending create | Reject the stale writer and durably abort preparation under the new term |
| Exact retry after process/assignment/recovery changes | Reject even if its historical receipt already exists |
| Image is dropped after receipt | Retain historical proof and the cut/namespace hold; installation must obtain and revalidate a target image |
| Receipt anchor missing, corrupt or rewritten | Fail closed; do not manufacture a receipt from transport/local state |

These are historical restore/retirement observations, not installed receiver/sink
readiness or proof of image residency. Commit revalidates current authority and
all exact processes and provides explicit post-Commit root reconstruction.
Target generation fencing, installation and participant-complete Release remain
required before output. No public SQL/HTTP submission or target output is enabled.

## Atomic target Commit and private reconstruction

`LaminarDB::commit_cluster_topology_target(&mut image)` re-observes the held parent
through the existing lifecycle, records this process's protocol-4 preparation and
commits through its configured leader controller. Every exact frozen process must
have protocol-4 evidence and current original process/assignment authority. The
current catalog, latest parent checkpoint Commit, root, compiled descriptor and
retained image must agree. The DB revalidates sealed source positions using read-only
metadata before the decision, without resolving `latest` again. Unresolved checkpoint,
cleanup, assignment or recovery authority rejects Commit. Sixteen CAS attempts
share a 15-second authority budget;
the DB's total retirement/write/recheck budget is 45 seconds.

One create-only authority append sets both `lease.catalog_manifest` and the
operation's immutable `commit`, advancing the logical version once. The existing
bounded journal supplies the current committed decision; no second head, scheduler,
registry or per-record work is introduced. The original baseline, object
incarnations, checkpoint allocator and terminal outcome links remain unchanged.
Success reports Committed with no locally active version. Ordinary checkpoint,
assignment and topology admission remain held. Existing parent recovery Release
and ordinary startup reject this pending target. Cached parent recovery-admission
snapshots also become invalid; a prior Release cannot authorize parent intake.

`recover_committed_cluster_topology(operation_id)` reconstructs a private image
from the current Commit and its exact root. It works on a Created DB or a still-held
retired parent after losing its image. The controller audits the current leader,
every current process term, exact local durable adoption and current assignment
before and after reads. Restarted boots may use a newer assignment version with
the same vnode owner digest, domain, ABI and complete stable participant roster;
rescaling and changed ownership remain rejected. Historical source attempts,
manifest bytes, checksums and parent PipelineIdentity remain exact. The ordinary
target fingerprint path still rejects the parent checkpoint. No target checkpoint
or historical acknowledgement is fabricated. New-source cursors are validated,
never resolved again. The compiler slot, state/payload budgets and 45-second
restore deadline are reused.

The committed inventory takes precedence during replay. Cold bootstrap accepts
either the complete current inventory or the exact complete adopted bootstrap,
whose preserved ordered prefix is certified by the additive Commit audit. Arbitrary
subsets and changed definitions reject. No startup configuration can revert the
committed catalog. Transport generation fencing is implemented below. Runtime
installation and participant-complete Release must be implemented before ordinary
startup or output can be enabled.

| Boundary/failure | Result |
| --- | --- |
| Missing protocol-4 process, stale image or original process/assignment | Reject before Commit; preserve held parent/root |
| Leader replacement wins the Commit slot | Pre-Commit abort under replacement authority; retain the old checkpoint |
| Commit create succeeds but response/caller is lost | Read the same operation; target stays committed; retry or reconstruct its exact root |
| Leader change or recovery fault after Commit | Preserve target decision; reject abort and parent Release |
| Private reconstruction cancelled, timed out or corrupt | Drop partial image/permit; retain Commit, root and runtime hold; retry reconstruction |
| Commit/root/adoption authority anchor missing or malformed | Fail closed on status/catalog authorization; never assume the parent is current |

These tests certify authority/private reconstruction only. Public submission,
actor installation, Release and full multi-process target recovery remain unfinished.

## Committed transport preparation, 2026-10-02

`prepare_cluster_topology_transport(&mut image)` binds the retained private graph
and both directions of the process-owned shuffle fabric to the exact committed
logical version and catalog SHA-256. It accepts no caller-supplied Commit, authority
or retirement flag. It reobserves parent actor termination, validates sealed cursors
without resolving them again, holds the existing assignment-adoption and execution
rotation locks, and audits the current complete process/assignment/adoption roster
around publication. Its total cooperative budget is 45 seconds. Created recovery
requires no runtime/connector owners; the retired parent retains its namespace.

`ShuffleTopologyFence` is a small Copy identity, separate from assignment versions,
recovery generations and serialization protocols. The existing assignment locks
publish both endpoint bindings; the existing delivery mutex serializes the loss
audit, pending admissions and sequence reset. Installation cancels old scope tokens,
connections, blocked sends and handshake tokens. Old queued/staged data, frontiers
and barriers are filtered before loss accounting. Unrepaired loss, expired process
leases and inactive or mismatched assignments reject before either endpoint changes.
Assignment changes and recovery retain the topology conflict floor; identical target
retries preserve sequence continuity. No loss is forgiven by topology publication.

Handshake request/response and leading Hello carry the exact version/digest pair.
Zero version plus empty digest explicitly denotes the legacy fabric. Partial,
malformed, divergent and legacy identities reject on a migrated fabric. The client
checks the echoed pair, so a legacy binary that ignores new fields cannot open a
migrated stream. Data/frontier/barrier payloads and Arrow schemas are unchanged.
No catalog lookup, hash, serialization, new lock or allocation runs per row. Managed
operators and the graph use cheap version atomics at existing batch/ownership
boundaries; actual stream admission compares the full immutable digest. Retained
asynchronous send plans capture the graph's fixed binding and cannot borrow the new
identity from mutable process endpoints. Private binding updates operator transport
configuration without reattaching operators or changing decoded state.

This internal method performs no authority append, actor startup, local catalog or
checkpoint-coordinator replacement, input acknowledgement or output release. The
image remains private; intake/cut stay held and the operation remains Committed.
Cancellation after local publication retains the target fence and hold; retry the
same image or reconstruct from the immutable root. Reconstruction accepts only the
authorized parent/target fabric (or empty Created fabric), never a divergent digest.
The low-level endpoint method is trusted control-path infrastructure and supplies
no readiness or Release permit. Protocol-4 preparation proves Commit support, not
participant-complete installation capability; future Release must certify that
capability and actual target state/receivers/sinks on every required current process.

Real loopback gRPC tests cover stale traffic, sequence continuity, blocked sends,
pending admissions and loss preservation. DB tests use the actual aggregate codecs,
strict parent root, sealed source cursors, controlled watcher and OS namespace lock.
Their direct private codec execution is not a target runtime/output test. The
[transport evidence](test-evidence/topology-transport-2026-10-02/README.md) records
validation and limits. Automatic recovery, actors, Release and the real multi-process
migration/restart/performance oracle remain unfinished; LDB-6043 remains.

## Atomic startup from sealed source positions, 2026-10-02

`SourcePosition::Initialized` carries the complete unowned new-source cursor from
the migration root. It has no engine checkpoint attempt and does not assert that
the skipped prefix was processed. `PreparedTopologySourcePosition::startup_position`
converts preserved positions to their exact `Resume` attempt/cursor and new positions
to `Initialized`. This control-path conversion supplies no Commit, readiness or
Release authority; an installer still revalidates the immutable root/current
ownership and retains intake until participant-complete Release.

Atomic source startup rejects initialized BestEffort requests and assigned cursors
before I/O. The runtime also requires the connector's explicit sealed-start
capability; the default rejects, even if a custom source has metadata discovery
hooks. Kafka implements the capability with its existing bounded/tracked metadata
validation, complete inventory/channel checks and retained low/high offset bounds.
The sealed numeric vector is reused verbatim, including empty partitions; `latest`
is never resolved again. Guaranteed ordinary Initial/latest remains rejected.
Other built-ins explicitly reject this position before external I/O.

Kafka validates the global unowned cursor before creating an active consumer, then
installs numeric offsets through the existing manual assignment path. Current vnode
ownership selects disjoint channel subsets. Metadata validation retains the existing
10-second total budget, 64-topic/4096-partition bounds and semaphore through native
client destruction, including caller cancellation. Startup retains the existing
runtime stage deadline, process lease checks, cleanup and terminal task ownership.
The reader remains deferred until polling; auto reset cannot replace a lost cursor.

The streaming coordinator seeds committed progress only from durable Resume.
Initialized starts neither seed committed offsets nor acknowledge pre-boundary
input. An actual owned source actor test observes control servicing while the
intake gate stays held, then joins terminal cleanup. Native librdkafka and separate
real Redpanda fixtures check numeric startup, retries after high watermarks move,
post-boundary records and absent broker acknowledgements. The fixture's later
Resume checks the cursor codec with a supplied attempt; it does not certify an
engine checkpoint or a full topology restart.

There is no new framework, authority format, dependency, registry or per-record
work. This is an installation prerequisite, not target installation or Release.
The [source startup evidence](test-evidence/topology-source-start-2026-10-02/README.md)
records validation and its limits. Runtime catalog/coordinator/sink installation,
current participant readiness and automatic post-Commit recovery remain unfinished;
LDB-6043 remains.

## Held committed runtime installation, 2026-10-03

`install_committed_cluster_topology(image)` reuses transport preparation and the
existing sticky startup attempt, detached driver, catalog replay, source/sink
preparation and compute watcher. It consumes one DB-owned committed image, with no
caller-supplied root or readiness flag. The schema-only candidate is dropped and
the decoded graph moves into the ordinary runtime; the compiler permit stays owned
until the handoff finishes. State is neither copied nor decoded again.

The startup claim accepts an observed retired parent or a Created DB with no
unresolved owners. Current full-roster Commit, assignment, process/adoption,
source cursor availability and exact transport generation are revalidated. Catalog
inventory, target pipeline identity and execution environment must equal the
certified descriptor. Assignment adoption remains locked through preparation and
graph-ready publication. The unchanged catalog handles, incarnations and restored
subscription sequence/frontier mappings feed the existing target callback.

The target coordinator retains the immutable parent outcome, committed index and
manifest as its historical predecessor. It does not relabel them, allocate a
target checkpoint or acknowledge source history. Its next capture requires full
vnode state when that predecessor's pipeline differs from the bound target; later
target checkpoints resume the ordinary incremental policy. Source startup converts
the image's positions to exact Resume or Initialized requests and preflights every
source before opening target sinks. Reference tables without a migration mapping
reject. Initial external sink epochs stay deferred.

Sources service controls, sinks are owned and the compute loop reports local
readiness while intake remains gated. The existing installed vnode marker binds
the exact target pipeline and assignment. The final check observes coordinator,
graph, watcher, source and sink ownership and revalidates shared authority. A local
Running state is not a durable installation receipt or active topology. The
operation stays Committed, locally active version remains absent and ordinary start
or `set_source_gate(false)` cannot authorize intake.

One cooperative 45-second deadline spans transport and owned startup. The startup
owner outlives caller cancellation; cleanup uses existing bounded terminal joins.
The compute watcher is registered before any readiness wait, and failed-start
cleanup joins it before retiring graph claims and connector owners. A timeout keeps
unresolved handles fenced. Commit and checkpoint namespace ownership survive
installation failure; retry reconstructs the same root without resolving latest.
No framework, authority encoding, dependency or per-record work is introduced.

| Boundary | Result |
| --- | --- |
| Caller disconnects after claim | Existing startup owner continues with the same deadline and sticky result |
| Unsupported sealed-source startup | Reject before target sink I/O; preserve Commit and holds |
| Source start fails or installation deadline expires | Observe terminal cleanup, retain namespace/Commit and reconstruct for retry |
| Process authority is lost during startup | Reject local readiness; retire/retain existing owners with intake held |
| Local installation succeeds | Running control loop, no target epoch admission, receipt, Release or active version |
| Created DB has no target checkpoint | Install from the exact committed migration root and historical parent |

The [installation evidence](test-evidence/topology-installation-2026-10-03/README.md)
uses real aggregate codecs, callback routing and owned connector actors. A test-only
gate opening observes preserved state plus post-cut rows; it is not production
Release. The fixture has one configured process identity and controlled source/sink
I/O. Participant-complete capabilities/readiness, stale sink completion fencing,
automatic recovery and the real multi-process migration/performance oracle remain
unfinished. LDB-6043 remains.

## Installed runtime certification and Release, 2026-10-03

`Committed -> Activating -> Active` now uses the same authority append as the
catalog, checkpoint, assignment and recovery decisions. `TopologyActivation`
freezes the current complete assignment and process roster, including zero-vnode
evidence processes. Each installation receipt binds its exact boot/process term,
protocol 5, unique local runtime UUID and immutable append sequence. It cannot
stand in for the historical private-restore/parent-retirement receipt.

The existing DB control executor owns `certify_installed_cluster_topology`,
`release_installed_cluster_topology` and `apply_cluster_topology_release`. One
existing compiler slot bounds local control work; its strong DB owner and shared
45-second cooperative deadline survive caller cancellation. Lock order is compiler
slot, topology, lifecycle, then assignment. No synchronous lock spans an await.
State remains in the installed graph; certification copies only bounded metadata.

Certification checks the target coordinator and vnode binding, live compute
watcher, exact source/sink actor counts, sealed-source actors, sink control
acknowledgements and current target receiver mesh. Source actor liveness is
separate from terminal connector-child ownership. A dead actor with a retained
child continues blocking succession but cannot certify readiness. Successful
certification keeps intake held and admits no initial target sink epoch.

Each sink actor owns a revocation token shared with handles and connector-operation
waits. Abort revokes it synchronously before cancelling the actor. A revoked
operation cannot invoke a connector, accept a same-poll late completion or report
a buffered successful acknowledgement. A same-name successor owns a distinct
token. Connector-child tracking still governs terminal observation; rejecting a
late success does not undo an external effect or settle an unknown sink outcome.
Graceful retirement still reconciles and closes the old connector before revocation.

The current leader rechecks every process and publishes Release only after the
exact current roster is complete. An unreleased round can be superseded on a
leader change only by recollecting all runtime observations. Published Release
and Commit are immutable. A harmless later leader change can authorize the same
Release, but another process, runtime UUID or assignment cannot reuse it.
Status/catalog reads audit exact retained receipt/Release appends; pruning pins
all their sequences. Protocol, phase, duplicate sequence and evidence rewrites
fail closed.
Release does not consume the migration root. Its parent checkpoint artifact pin
remains after Active until explicit root retirement accounts for recovery/replay
references. This conservative bound can stop artifact-floor advancement; root
consumption and journal reclamation remain required before public migration.

Every participant applies Release separately. It reaudits current authority,
reconciles any sink-open witness, admits target sink epochs through the existing
coordinator, reaudits actor/process/assignment liveness and opens intake last.
`Active` means a durable full-roster Release; `locally_active_version` additionally
requires this process's exact live runtime and applied Release. A dead actor makes
that local field absent without rewriting the durable decision.

Held assignment refresh retains the exact controller/transport certificate while
keeping intake closed. Released refresh uses the certified Release until the first
target checkpoint. A checkpoint admitted in that interval must bind the committed
target pipeline/deployment; parent checkpoints cannot be relabelled. Refresh can
coexist with that exact target checkpoint. Ordinary recovery still rejects the
historical parent cut before a target checkpoint; root-backed automatic recovery
requires its own integration and cannot borrow the original runtime's receipt.

| Boundary | Required result |
| --- | --- |
| Missing/dead actor or incomplete receiver mesh | No installation receipt; keep intake held |
| Incomplete/mixed/stale roster | No Release; retain Commit and available receipts |
| Successful write with a lost response | Resolve the same operation/round from shared authority |
| Caller disconnect after local control claim | Existing bounded DB owner continues |
| Leader changes before Release | Recollect the complete current installation roster |
| Leader changes after Release | Retain the original Release and revalidate current authority |
| Process/assignment/fault fence or actor failure during application | Keep the durable target and close/retain local intake |
| Apply fails after sink epoch admission | Retain the coordinator's witness and reconcile on retry |
| Original runtime dies after Release | Durable Active stays visible; locally active becomes absent |

The [activation evidence](test-evidence/topology-activation-2026-10-03/README.md)
uses exact multi-process authority fixtures and actual local restored aggregate,
callback and owned actors with controlled at-least-once connectors. It does not
certify transactional target installation, automatic root recovery, public
submission, or a real multi-process migration/restart/performance oracle. LDB-6043
remains until those paths are complete.

## Required next integration

1. Wire target-only post-Commit recovery through the existing stopped/recovered/
   release quorum. A replacement process/runtime needs new runtime evidence and
   recovery authority; an original installation receipt cannot authorize it. Use a
   newer exact target checkpoint when present and explicitly authorize the migration
   root otherwise, preserving original historical checkpoint identity.
2. Wire public SQL and atomic multi-object submission, expected parent,
   payload-bound idempotency and detached durable ownership. Local dry run exists;
   it does not advance admission. Do not reuse bootstrap.
3. Preserve the implemented root subscription identity/frontier mappings through
   automatic target recovery and public submission. Whole-graph hashes differ on
   additions; skipping their check is unsafe.
4. Run the stateful multi-process migration/restart oracle and fault matrix. Durable target catalog
   precedence and exact original-bootstrap assertions are implemented.

## Database-owned migration phase progress, 2026-10-03

The existing recovery monitor drives already admitted operations on every node.
Its long-lived future owns one `TopologyDriver` and at most one private restored
image, which retains the existing compiler permit. Private graph operators are
Send rather than Sync, so this sole owner remains outside shared monitor
observations. The monitor future is pinned once per generation to avoid large
stack moves. The one private image and active restore/phase futures also use the
heap after the first debug test exposed a stack overflow at CI's unchanged 4 MiB
setting. These allocations occur only on the control path; no record-path
allocation or additional scheduler is introduced.

Each healthy poll performs one phase action using the existing fenced methods.
Participants independently compile before the leader invokes the manual
checkpoint route. Checkpoint tails supply the application receipts while the
worker waits in Quiescing. The leader stages the root; every process privately
restores, observes retirement and certifies protocol four. Complete preparation
permits Commit. Each process then installs its held runtime, certifies protocol
five and observes the complete Release before applying it locally. A follower
updates its retained image from the already published Commit without publishing
another decision. All authority/process/assignment/actor checks remain in the
phase methods.

The idle head hint is only `(operation UUID, status sequence)`, under the existing
15-second read bound. It grants no permission and does not audit immutable blobs.
Every phase uses the definitive audited status and existing authority methods.
Completed local operations can skip repeated root/descriptor reads. A locally
held image or unapplied installed Release takes precedence over a newer journal
entry, so a lagging node finishes its own boundary first.

Local faults, recovery, drain, shutdown and process fencing discard the private
image before recovery acquires the compiler slot. A definitive pre-Commit abort
with a held cut queues the existing coordinated-recovery request. Dropping an
image never clears the hold. The same retired, still-held parent may reconstruct
its private image after monitor loss; current exact-root/process/assignment
checks still apply. An installation failure observes cleanup and retries the
immutable committed root without resolving source initialization again.

The phase methods retain their existing 15/30/45-second budgets and CAS bounds.
If an audited phase makes no durable progress for 180 seconds, the driver releases
private preparation and requests coordinated recovery. A manual checkpoint
already owning an exact attempt still waits for its terminal cleanup; abandoning
that owner on a worker timer would permit overlapping lifecycle work. Repeated
errors are logged only when their diagnostic changes.

This driver does not authorize a replacement runtime after Active. The original
Release UUID belongs to its installed generation. Automatic target recovery and
whole-cluster startup remain the next prerequisite to public submission; a newer
target checkpoint takes precedence during private recovery selection. Public SQL
and startup guards remain. The local fixture validates phase ownership and actual
held actors, not multi-process migration, transactional sinks or a restarted boot.

## Target checkpoint continuity and private recovery, 2026-10-03

The first checkpoint under the released target retains the exact historical root
as its predecessor. Ordinary index continuity still requires the same pipeline
identity and source inventory. A distinct authority check permits that one edge
only through its retained, audited topology Commit, complete Release, descriptor
and root. It verifies both original identities, deployment, owner map/ABI, complete
source inventories and continuing source watermarks. No historical index or
manifest is rewritten. Subsequent target checkpoints use ordinary continuity.

Subscription continuity uses the same audited edge. The predecessor certificate
and exclusive frontier must equal the sealed parent mapping. An in-memory
comparison view uses its exact target certificate while preserving sequences;
stored bytes and digests remain historical. The normal certificate/sequence check
then validates the first target range. This adds control-path artifact checks only.

`committed_topology_recovery_input` selects the greatest current target Commit
index, or the original root if no target checkpoint has committed. Missing, damaged,
foreign or ownership-incompatible newer evidence fails; it never falls back to an
older root. Full process/assignment/leader and Commit-head rechecks share a 15-second
budget. A concurrent newer Commit requires fresh selection. Replacement boots may
select the same state with the unchanged owner map and stable participant roster.
Selection grants private reads, never original runtime Release permission.

`prepare_cluster_topology_recovery` reuses the one compiler permit, isolated catalog,
strict checkpoint reader and existing target operator codecs. A target checkpoint
uses its target identity directly; the parent mapping applies only to a root.
Source cursors, watermarks and subscription exclusive frontiers come from the same
selected cut. Every target source requires its committed cursor, including sources
added by the migration; missing progress cannot reuse initial latest positions.
Verified encoded state is freed after decoding. Original root reconstruction rejects
once target progress exists. Selected recovery images cannot use the original
migration installation/Release APIs.

Automatic recovery Start/Release and whole-cluster startup are still guarded.
They must bind this selection to the existing stopped/restored/release quorum;
public SQL, removal/replacement and reference-aware reclamation remain unfinished.
The focused fixture loads real aggregate/state/subscription codecs but supplies
installation evidence as an authority fixture. It does not certify runtime recovery,
replacement actor startup, brokers or real multi-process restart.

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
The [parent retirement evidence](test-evidence/topology-retirement-2026-10-02/README.md)
records task/connector terminal observation and retained lifecycle/namespace fences.
The [target preparation evidence](test-evidence/topology-target-preparation-2026-10-02/README.md)
records durable exact-process observations, concurrent/lost/cancelled appends and
retained receipt anchors.
The [Commit and reconstruction evidence](test-evidence/topology-commit-2026-10-02/README.md)
records the atomic catalog/root decision, retained Commit across failures and
strict private reconstruction before a target checkpoint.
The [transport preparation evidence](test-evidence/topology-transport-2026-10-02/README.md)
records real gRPC generation changes and held exact-Commit DB preparation.
The [source startup evidence](test-evidence/topology-source-start-2026-10-02/README.md)
records sealed numeric Kafka startup, retry/acknowledgement checks and owned held actors.
The [installation evidence](test-evidence/topology-installation-2026-10-03/README.md)
records exact-root target runtime handoff with intake held and its failure/retry boundaries.
These results do not certify target migration
or production latency.
