# Cluster topology status and upgrade checkpoint

This increment implements explicit legacy catalog adoption and topology status.
It does **not** implement runtime topology migration. The existing `LDB-6043`
guard still rejects cluster CREATE/DROP/ALTER requests outside cold bootstrap
and durable replay. There is no supported migration submission or dry-run route.

| Operation | Current support |
| --- | --- |
| Initial cold bootstrap and exact sealed-catalog replay | Existing behavior |
| Read durable topology and local activation status | Implemented |
| Adopt the identical legacy inventory as topology 1 | Core library primitive; coordinated binary upgrade required |
| Add an independent pipeline or downstream stream/sink | Rejected; checkpoint cutover and graph installation unfinished |
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

## Legacy upgrade and restart

Existing authority format 12 remains readable and writable until explicit
adoption. Upgrading binaries alone does not adopt the inventory. Normal startup
continues to replay the same sealed catalog with its existing generations and
checkpoint identities.

The core `CatalogManifestStore::adopt_legacy_topology` API requires the current
leader proof, the exact sealed manifest reference, the existing deployment UUID,
and a nonzero operation UUID reused on retries. Its caller must first complete a
coordinated binary upgrade of every required participant. Format 13 makes old
authority readers fail closed; it does not stop already-running old actors or
replace process/sink fencing. Mixed-version adoption is unsupported.

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

## Errors and recovery

| Code | Meaning and response |
| --- | --- |
| `LDB-6043` | Runtime topology migration is unavailable; use the existing inventory |
| `LDB-6060` | Invalid/missing catalog or topology evidence; repair the underlying artifact, without resetting identity |
| `LDB-6061` | Expected manifest/deployment conflict; reread authority and resolve the mismatch |
| `LDB-6062` | Leader proof fenced; acquire current authority before retrying |
| `LDB-6063` | Unsupported adoption protocol; complete the coordinated binary upgrade |
| `LDB-6064` | Bounded operation contention/uncertain outcome, or status-read timeout; the message distinguishes these cases |
| `LDB-6065` | Shared authority I/O/validation failed; restore access or exact persisted evidence |

The status endpoint returns 503 for unavailable/corrupt authority or serving
fences, and 504 for a read deadline. It must not return a successful empty or
legacy fallback for a damaged adopted deployment. Missing deployment identity
or missing adoption anchor also blocks catalog startup replay. Keep gates closed
and recover the original artifacts from the deployment's storage procedures.

No cutover-pause duration or migration recovery procedure can be certified in
this increment. See the [engineering checkpoint](cluster-topology-engineering.md)
and [remaining work](cluster-topology-migrations-progress.md).
