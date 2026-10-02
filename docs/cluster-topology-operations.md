# Cluster topology status and upgrade checkpoint

This checkpoint implements explicit legacy catalog adoption, core admission,
an old-topology checkpoint cut, topology/operation status, local candidate validation
and durable preparation of already admitted candidates. Exact-cut root staging
is available through the DB/core library for stateless downstream additions and
new sources with a supported sealed initialization contract. Private target
restore preparation and observed parent retirement are available through the DB
library after root publication.
It does **not** implement runtime topology migration. The existing `LDB-6043`
guard still rejects cluster CREATE/DROP/ALTER requests outside cold bootstrap
and durable replay. There is no supported migration submission or activation route.

| Operation | Current support |
| --- | --- |
| Initial cold bootstrap and exact sealed-catalog replay | Existing behavior |
| Read durable topology and local activation status | Implemented |
| Read an admitted pre-cut request's durable status | Implemented, console authorization |
| Dry-run an additive candidate against an adopted parent | Local compile and compatibility descriptor; no durable admission |
| Reserve/abort a candidate | Core library primitives; no public submit route or target worker |
| Certify an already admitted candidate on this process | Local preparation API; complete frozen roster required before a new cut |
| Prepare and hold an exact old-topology cut | Existing manual checkpoint path after participant-complete internal admission/preparation; no target activation |
| Stage exact-cut state/progress/subscription requirements | DB/core library, held cut and complete current roster; no target restore/output authority |
| Seal new-source initial positions | Same internal staging call; explicit Kafka topics with earliest/latest |
| Prepare a private restored target image | DB library; verified parent state and sealed cursors; no target install/Commit/Release |
| Observe retirement of a prepared image's parent actors | DB library; exact current root authority and terminal task proofs; namespace and cut stay held |
| Adopt the identical legacy inventory as topology 1 | Core library primitive; coordinated binary upgrade required |
| Add an independent pipeline or downstream stream/sink | Local dry-run supported for replayable source/stateless stream/durable sink; activation remains rejected |
| Remove or replace objects | Rejected; state, sink and subscription contracts unfinished |
| Change keys, windows, state schema, source identity or sink semantics | Rejected; requires certified transformation/replay contracts |

## Read status

`GET /api/v1/cluster/topology` uses the console authorization policy. A diagnostic
read token does not authorize it. The response is not cached. A non-cluster server
returns 404; missing/incorrect console credentials return 401. The handler and
the underlying control read have a 15 second deadline.

An HTTP request has this form, substituting your existing address and console
token:

```http
GET /api/v1/cluster/topology HTTP/1.1
Host: 127.0.0.1:8080
Authorization: Bearer <configured-console-token>
```

The request and these states are exercised by the server router test
`topology_status_reads_explicit_legacy_and_adopted_authority_without_activation`:

- `catalog.state = "uninitialized"`: no inventory has been sealed.
- `catalog.state = "legacy_sealed"`: the exact manifest is sealed, but there is
  no admitted logical version. Both version fields are null.
- `catalog.state = "versioned"`: the baseline includes its exact manifest,
  deployment UUID, successful operation UUID and adoption authority sequence.
  `committed_version` is 1.
- `locally_active_version` is populated only after exact replay on this process,
  Running state, intake release, a live process lease, and clear recovery and
  terminal fences. It never certifies activation of other participants. It may
  be null while `committed_version` is populated.

Status is an observation, not an ownership grant. It may change immediately
after a response. Existing authority reads can reconcile publication of a
previously created authority record; the status request does not admit a new
topology operation or allocate a checkpoint/deployment identity.

## Validate an additive candidate

`POST /api/v1/cluster/topology/validate` uses console authorization and the existing
serving gates. It runs locally on the addressed process; it does not forward to
the leader. The catalog must already have an explicitly adopted version, and
the process must have replayed that exact parent. Created and Running databases
can validate; startup, recovery and shutdown reject validation.

```http
POST /api/v1/cluster/topology/validate HTTP/1.1
Host: 127.0.0.1:8080
Authorization: Bearer <configured-console-token>
Content-Type: application/json

{
  "expected_parent_version": 1,
  "statements": [
    "CREATE STREAM new_projection AS SELECT id, value FROM existing_source WHERE value > 0"
  ]
}
```

Each array entry must be one additive `CREATE SOURCE`, `CREATE STREAM` or
`CREATE SINK`. Sources require explicit columns and replayable, cluster-supported
connectors; sinks require supported durable connectors and compatible input
semantics. New streams must be stateless. Unchanged managed aggregates, supported
windows and joins are mapped by their exact definitions, incarnation, schema,
state codec and dependency closure. Object names alone are insufficient.
Reference tables, materialized views, replacement, removal, new stateful streams,
catalog-only ingress/output and custom function/optimizer implementations are
unsupported. The existing DDL, physical planner and connector admission checks
still reject unsupported query shapes, placement, filters and delivery contracts.

A successful response has `scope = "local_candidate_plan"`, exact parent/target
manifest references, both strict pipeline identities, a deterministic
`compatibility_sha256`, and sorted `objects`. Each object is classified as
`preserve` or `add_future_only`, with an explicit initialization requirement.
Preserved objects require the reconciled cut's state and progress. New streams
and sinks require future-only cut boundaries. New sources require concrete initial
source/partition positions resolved once and persisted before target commit.
This endpoint does not discover positions or imply historical replay/backfill.

Validation constructs a private catalog and empty managed graph, with small empty
source queues. It reads the adopted parent and rechecks its authority, without
writing a target manifest, admitting a request, allocating a checkpoint, copying
retained history, opening/polling sources, opening/publishing sinks or closing live
intake. The strict recovery fingerprint remains mandatory; the changed target
identity cannot restore an ordinary parent checkpoint through this API.

`required_before_activation` lists the missing authorization: complete participant
plan agreement, reconciled old cut, durable initialization/progress mappings,
observed actor retirement, atomic target commit and installed-target Release.
Even identical successful results from every node are observations, not durable
participant certificates or authorization to submit/activate the target. Normal
SQL still returns LDB-6043 after successful validation.

Bounds are one compiler per process, 1..64 statements, 256 KiB of SQL, 256 total
catalog objects, a 1 MiB compatibility descriptor and a 30 second deadline.
HTTP JSON bodies are capped at 512 KiB. Responses are uncached. Parent conflicts
return 409; malformed/bounded SQL requests return 400; unsupported semantic
operations return 422; a busy compiler returns 429 with `Retry-After: 1`;
deadline expiry returns 504; unavailable authority or serving fences return 503.
Cancellation releases private planning ownership without a durable operation to
resume. Authentication, media-type and JSON-shape failures use the existing
router's status codes.

## Legacy upgrade and restart

Authority formats 12 through 18 remain readable. A coordinated binary upgrade is
required for this build: the first serialized assignment drain writes format 14,
even before logical catalog adoption. Stop and observe termination of the old
server processes, then start every required participant with the new binary and
the same configuration and durable namespaces. Mixed-binary operation is
unsupported. Format rejection does not retire cached actors or replace process
and sink fencing.

Upgrading binaries alone does not adopt the inventory. Normal startup
continues to replay the same sealed catalog with its existing generations and
checkpoint identities.

The core `CatalogManifestStore::adopt_legacy_topology` API requires the current
leader proof, the exact sealed manifest reference, the existing deployment UUID,
and a nonzero operation UUID reused on retries. Its caller must first complete a
coordinated binary upgrade of every required participant. Adoption writes format
13 or preserves a later format when earlier authority admission already upgraded it. Older
authority readers fail closed; mixed-version adoption is unsupported.

There is no operator-facing adoption CLI or write endpoint in this increment.
The API is a foundation for the migration coordinator. Do not manually edit
authority JSON to adopt a deployment. Do not delete the catalog, checkpoint
directory or control namespace. Exact inventory replay remains mandatory;
post-migration bootstrap-configuration precedence is still unfinished because
no target topology can be committed yet.

Adoption appends metadata without rewriting catalog or checkpoint bytes. A lost
response/cancelled call may already have admitted the baseline. Read authoritative
status and retry the same operation and evidence under the current leader proof.
Concurrent compatible adoption calls converge on the original winner, whose
operation UUID is returned. Changed manifest/deployment evidence is rejected.

## Pre-cut request status

`GET /api/v1/cluster/topology/operations/{operation_id}` uses the same console
authorization and 15 second deadline. It returns the original operation UUID,
canonical plan reference, admitting leader proof, admission/disposition authority
sequences and `state`. Malformed/nil UUIDs return 400; an unknown migration request
returns 404 with `Cache-Control: no-store`. Baseline adoption is reported by the
catalog-status endpoint, not this migration-request journal.

The implemented `state.phase` values are `planned`, `preparing`, `quiescing`, `cut_prepared`
and `aborted`. Aborted includes `state.reason`: `requested`, `leader_changed`,
`checkpoint_aborted` or `recovery`. A reservation never implies that the
candidate catalog is committed or locally active. The committed catalog remains
topology 1. Identical retries resolve to the original status, including an abort;
reusing an identity with a different payload fails. The core journal retains at
most 64 identities and rejects further admission until journal retention exists.

This route is exercised against admitted/aborted requests by the existing HTTP
router fixture. Local dry-run validation is available separately; there is still
no supported HTTP/SQL submission or manual admission command. Do not create a
reservation by editing authority files.

An internally admitted cut is bound to its exact old deployment, pipeline ABI,
assignment/boot roster and checkpoint attempt before Prepare. Its optional `cut`
contains the inventory, binding sequence, definitive checkpoint Commit reference
and completed process roster. `quiescing` remains visible after Commit while
application receipts are missing. The leader's receipt follows aggregated external
sink settlement; every frozen process must also finish its local cut. `cut_prepared`
means intake and successor sink output remain held. Protocol-2 cuts also require
complete durable compilation agreement. It does not mean that the candidate is
installed, old actors retired or the target active. Committed catalog version
remains 1 and locally active version is null while intake is held.

There is no target worker in this checkpoint. If testing the internal cut path,
use coordinated recovery or restart on the same namespace to resume the original
topology. A new leader/recovery fault aborts the uncommitted candidate and retains
the old checkpoint Commit. Recovery must reconcile prepared sink outcomes before
release; it cannot rewind a committed checkpoint or treat a timeout as an Abort.
Explicit abort alone does not reopen intake. The real-process cut/abort test uses
the existing authenticated manual checkpoint route and all-process restart; it
does not submit a supported topology migration.

Assignment refresh cannot release a held cut. The current certificate remains
available to its checkpoint tails, while intake and successor sink admission stay
closed. An authorized coordinated recovery release clears the local hold after
the existing retirement and readiness checks; a rejected release keeps it closed.

## Certify an admitted candidate locally

`POST /api/v1/cluster/topology/operations/{operation_id}/prepare` uses console
authorization and startup/serving gates. It is intentionally local: call every
required exact process rather than forwarding all requests to the leader. The
request has no planning payload; the server independently compiles the immutable
candidate already bound by core admission. This endpoint does not submit a new
migration. There is still no supported public admission/activation command.

Successful status includes `preparation.compatibility`, sorted `certificates`
(participant node/boot, process term, exact protocol and authority sequence) and
`complete_sequence`. A missing complete sequence means participants are still
required. Intake remains open while they prepare; ordinary checkpoints defer.
Only the complete frozen roster permits the old-topology cut. Certificates are
retained after abort and restart for audit; they do not grant a restarted boot
permission to reuse an old process's certificate.

Preparation has a 45 second end-to-end deadline and uses the same single local
compiler/30 second compilation bounds as dry-run. Responses are uncached. Busy
compilation returns 429; divergence/non-running parent returns 409; unsupported
protocol returns 422; fencing/unavailable authority returns 503; deadline or an
uncertain append returns 504. On cancellation or uncertainty, read operation status
and retry the same identity. A successful certificate append may outlive the request.

Preparation protocol 2 upgrades authority to encoding 16. Upgrade all required
binaries together before internally admitting such a request. Old protocol-1
requests remain readable/abortable but cannot start new uncertified cuts. Descriptor
agreement does not resolve source positions, restore target state, retire actors,
commit topology 2 or authorize target output. Normal SQL remains guarded by LDB-6043.

## Stage an exact-cut root internally

The DB/core library can stage immutable state/progress/subscription requirements
and supported new-source positions after every participant applies the
old cut. The DB call requires Running state, the held cut and closed intake, and
uses the controller's configured process/assignment authorities. There is no HTTP
root-staging or migration submission route in this increment.

Operation status then includes `migration_root.root` (SHA-256 and exact byte
length) and its first `authority_sequence`. Authority format 17 pins downstream-only
roots; format 18 pins roots with sealed new-source positions.
The phase remains `cut_prepared`, and the committed catalog remains topology 1.
The root preserves the exact old checkpoint, object incarnations, state mappings
and subscription sequence vectors. It does not restore a target graph, permit
target output or release intake. New sources require the configured connector's
read-only initialization contract. Built-in Kafka supports explicit topics with
`earliest`/`latest`: the root retains the first sealed numeric low/high watermark
for every partition, including empty partitions at zero. This is a partition
vector rather than one timestamp. Other connectors, topic patterns, broker group
offsets, timestamps and specific-offset initialization remain rejected.

The source-root staging slot is create-only and tied to the exact operation,
payload and cut. Once sealed, cancellation or a lost response before authority
publication cannot move a `latest` boundary on retry. A retry never resets damaged
evidence or substitutes a new request identity. A new leader aborts the old
pre-commit operation and resumes topology T through coordinated recovery.
Cursor discovery consumes no records, acknowledges no input and starts no target
actor or sink. The target installation path still needs to consume these cursors
under committed authority before intake release; guaranteed ordinary Kafka startup
continues rejecting unsealed `latest`.

The DB call has a 30 second deadline, including an authority staging budget of
15 seconds/16 append attempts. Participant manifest metadata is capped at 16 MiB
in aggregate and the root body at 1 MiB. Retrying the same prepared operation
returns its original binding; a lost response or cancellation can leave a successful
append. Query status to resolve uncertainty. Abort preserves the root metadata
and old checkpoint decision; normal coordinated recovery resumes topology 1.

## Prepare a private restored target internally

After root publication, `LaminarDB::prepare_cluster_topology_restore(operation_id)`
can return one unstarted target image. There is no HTTP restore or installation
route. The call requires a Running parent, the held old cut, closed intake, the
admitting leader, every current process certificate and the unchanged assignment.
It rechecks these fences after restoring state and validating source cursors.

The existing isolated compiler replays the immutable target and reconciles exact
catalog incarnations. Recovery verifies the historical parent index/manifests and
rebuilds every root requirement before state reads. Existing operator codecs decode
local state with the frozen ownership roster and existing fenced transport handles
as channel-state context. Preserved subscription generations and exclusive sequence
frontiers must equal the root. Historical checkpoints retain their parent identity;
ordinary recovery using the target fingerprint still fails.

Existing sources retain their actual committed attempt/cursor/assignment. New
sources retain the sealed global unowned cursor without an invented processed
attempt. Kafka validates its exact topic/partition inventory and current retention
bounds using read-only metadata; it never resolves `latest` again. A changed
inventory or expired/beyond-end cursor fails preparation. Final installation must
validate cursors again and select current owned partitions before source startup.
Other connectors must explicitly implement this validation contract.

The image retains the single local compiler permit until dropped. A second
preparation or planning request receives the existing busy error. Cancellation,
failure or the 45-second deadline drops partial state and frees the permit without
releasing the parent hold. The root is capped at 1 MiB, aggregate manifests at
16 MiB, verified local graph payload and decoded state use the configured managed
state budget, and node reads use the configured checkpoint limit. Encoded buffers
are released after decoding. The held old graph, target image, codec scratch and
read buffers coexist transiently; these limits are not a total-process RSS cap.

Success writes no authority receipt, changes no committed catalog/coordinator,
starts no source/sink actor and permits no target output. A retained image can
become stale. Parent retirement is a separate internal step. Atomic topology Commit,
installation and participant-complete Release remain required. LDB-6043 stays in place.

## Retire the held parent internally

`LaminarDB::retire_cluster_topology_parent(&mut image)` accepts only an image
prepared by that same database. There is no HTTP retirement route or automatic
migration worker. The call checks the exact root, old checkpoint, admitting leader,
complete current preparation roster, process and assignment before stopping the
parent and after observing its termination.

The existing lifecycle owns compute, source and sink tasks and their connector
children throughout cleanup. Joining compute, signalling cancellation or receiving
a sink close result alone is insufficient. Retirement also settles checkpoint
decision work and the sink-open witness and waits for every retained source/sink
actor and connector child to be terminal. The 45-second total budget includes
authority reads and cleanup. A deadline or cancelled waiter retains unresolved
handles and the namespace lock; retry with the same image while authority remains
current. A runtime fault requires coordinated recovery.

Successful retirement leaves the runtime in `ShuttingDown`, with intake closed,
the old cut held, the checkpoint namespace still owned and the parent catalog and
coordinator identity unchanged. The private target remains unstarted. Public
start/stop cannot release this boundary, and dropping the image does not reopen
intake. Terminal shutdown and authorized coordinated recovery retain their existing
cleanup paths; after a pre-commit abort, recovery resumes the unchanged topology
from its reconciled checkpoint. Do not reset the namespace or checkpoints.

`image.parent_retirement_observed()` records a local observation. It appends no
durable readiness receipt and grants no target output permission. The installer
must revalidate the observation, current authority and transport generation before
Commit/install/Release. Process-lifetime shuffle handles remain in place; target
generation fencing and installation are unfinished. This step alone does not
activate a migration or certify transactional sink migration behavior.

## Errors and recovery

| Code | Meaning and response |
| --- | --- |
| `LDB-6043` | Runtime topology migration is unavailable; use the existing inventory |
| `LDB-6060` | Invalid/missing catalog or topology evidence; repair the underlying artifact, without resetting identity |
| `LDB-6061` | Parent/payload conflict, unresolved admission, full journal or unknown operation; reread authority and resolve the stated condition |
| `LDB-6062` | Leader proof fenced; acquire current authority before retrying |
| `LDB-6063` | Unsupported topology protocol; complete the coordinated binary upgrade |
| `LDB-6064` | Bounded operation contention/uncertain outcome, or status-read timeout; the message distinguishes these cases |
| `LDB-6065` | Shared authority I/O/validation failed; restore access or exact persisted evidence |
| `LDB-6066` | Candidate operation or compatibility contract is unsupported; use the reported supported subset |

The status endpoint returns 503 for unavailable/corrupt authority or serving
fences, and 504 for a read deadline. It must not return a successful empty or
legacy fallback for a damaged adopted deployment. Missing deployment identity
or missing adoption anchor also blocks catalog startup replay. Keep gates closed
and recover the original artifacts from the deployment's storage procedures.

No target cutover-pause duration or migration activation can be certified in
this increment. See the [engineering checkpoint](cluster-topology-engineering.md)
and [remaining work](cluster-topology-migrations-progress.md).
