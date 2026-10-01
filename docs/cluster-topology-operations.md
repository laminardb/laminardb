# Cluster topology status and upgrade checkpoint

This checkpoint implements explicit legacy catalog adoption, core admission,
an old-topology checkpoint cut, topology/operation status and local candidate validation.
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
| Prepare and hold an exact old-topology cut | Existing manual checkpoint path for an internal core reservation; no target activation |
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
and sinks require future-only cut boundaries. New sources require concrete latest
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

Authority formats 12 through 15 remain readable. A coordinated binary upgrade is
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
13 or preserves format 14/15 when earlier authority admission already upgraded it. Older
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

The implemented `state.phase` values are `planned`, `quiescing`, `cut_prepared`
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
means intake and successor sink output remain held. It does not mean that the
candidate is compatible, installed, retired or active. Committed catalog version
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
