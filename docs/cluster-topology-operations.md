# Cluster topology migrations

Supported cluster DDL uses a coordinated processing pause and the existing checkpoint/recovery coordinator. Admission is asynchronous: a receipt identifies a durable operation; it does not certify activation. Qualification results and outstanding work are tracked in [progress](cluster-topology-migrations-progress.md).

## Supported changes

| Change | Boundary and contract |
| --- | --- |
| Independent source → stateless stream → durable sink | New sources start at once-resolved, persisted connector positions. New streams and sinks process future input after Release. |
| Stateless downstream stream or compatible sink on an existing pipeline | Future input after the cut; no historical backfill. |
| Unchanged managed aggregates, supported windows and joins | Exact definitions, incarnations, codecs, state, timers, watermarks, source positions and publication frontiers survive the cut. |
| Full cluster restart | Reconstructs the committed target from its greatest exact target checkpoint or authorized migration root. |
| Keys, windows, schemas, source identity, sink semantics, new stateful operators, materialized views, reference tables, custom functions/optimizer implementations | Rejected until a certified transformation or initialization contract exists. |
| Removal and replacement | Remain rejected in this checkpoint. |

An unchanged SQL name alone does not prove compatibility. Kafka earliest/latest sources with explicit topics support sealed initialization. Other sources require implemented pause, replay, cursor-validation and atomic startup contracts. Sinks must support the selected delivery mode. At-least-once configurations can replay duplicates; arbitrary external effects are not exactly-once.

## Upgrade an existing deployment

1. Stop and observe termination of every old server process. Upgrade all required processes together, retaining their configuration, deployment identity and durable namespaces. Mixed binary operation is unsupported.
2. Start the new processes and let normal checkpoint/recovery readiness complete. Startup replays the original sealed catalog unchanged.
3. Read `GET /api/v1/cluster/topology` with the configured console bearer. For a legacy catalog, copy the exact `catalog.manifest` reference and the existing deployment UUID into the explicit adoption request:

```http
POST /api/v1/cluster/topology/adopt
Authorization: Bearer <console-token>
Content-Type: application/json

{
  "operation_id": "00000000-0000-0000-0000-000000000501",
  "expected_manifest": <exact-manifest-reference-from-status>,
  "expected_deployment_id": "<existing-canonical-deployment-UUID>",
  "coordinated_upgrade_complete": true
}
```

The manifest placeholder represents the complete JSON object returned by status. Obtain the deployment UUID from checkpoint-deployment/identity.json in the same checkpoint namespace; adoption never creates a missing identity. The operator assertion is required, and does not itself terminate or fence old actors. Concurrent compatible adoption converges on the original winner. The response contains that winner's UUID, which can differ from a concurrent caller's UUID.

Adoption preserves catalog bytes, hashes, generations and checkpoint references. Never edit authority JSON or delete/reset storage to perform this upgrade. Authority formats through 23 remain readable. A public protocol-6 admission writes at least format 23 before the old cut, so earlier readers reject the head; the coordinated shutdown remains necessary to retire cached old actors.

## Validate and submit

Dry-run is local, effect-free and separately available at `POST /api/v1/cluster/topology/validate`:

```json
{
  "expected_parent_version": 1,
  "statements": [
    "CREATE STREAM future_projection AS SELECT id, value FROM trades WHERE value > 0"
  ]
}
```

The response binds the exact parent/target inventories, both strict pipeline identities and every object's compatibility/initialization requirements. It does not admit the operation, resolve latest positions, open connectors, copy live state or grant output authority. Every frozen process subsequently recompiles the immutable admitted candidate independently.

For one atomic multi-object candidate, use `POST /api/v1/cluster/topology/operations` with a caller-generated nonzero UUID:

```json
{
  "operation_id": "00000000-0000-0000-0000-000000000502",
  "expected_parent_version": 1,
  "statements": [
    "CREATE SOURCE added_source (id BIGINT NOT NULL, value BIGINT NOT NULL) FROM KAFKA ('bootstrap.servers' = '127.0.0.1:19092', 'group.id' = 'topology-example', 'topic' = 'new-input', 'startup.mode' = 'latest')",
    "CREATE STREAM added_stream AS SELECT id, value FROM added_source",
    "CREATE SINK added_sink FROM added_stream INTO KAFKA ('bootstrap.servers' = '127.0.0.1:19092', 'topic' = 'new-output')"
  ]
}
```

Create the external topics and supply the deployment's actual broker address. The entire graph is validated before atomic expected-parent admission. HTTP 202 returns the durable operation. All subsequent phases belong to the DB's existing recovery monitor, even after disconnect, cancellation or lost response. Follower submission makes at most one authenticated hop to the durable leader with the same UUID, payload, parent and remaining deadline.

Ordinary supported `CREATE SOURCE`, `CREATE STREAM` and `CREATE SINK` on a running cluster use the same coordinator through embedded `LaminarDB::execute` or `POST /api/v1/sql`. SQL returns `result_type = "TOPOLOGY MIGRATION"` and `topology_operation` with HTTP 202. Embedded `DdlInfo.applied` is false at admission. The generated UUID is also retained in uncertain SQL errors; HTTP exposes it in `x-laminar-topology-operation-id`. Ordinary semicolon-separated statements remain sequential and are not an atomic migration; use the array API for related objects.

For retry control, prefer the array API. Retry the same UUID, exact statement bytes and parent version. Identical retries return the original status, including an abort or a completed older operation, before compilation. Changed payload reuse fails. After an uncertain SQL response, query its generated UUID; do not repeat SQL blindly with a new operation.

Console authorization and existing serving fences apply to every route. Diagnostic-read credentials cannot authorize topology reads or writes. JSON rejects unknown fields, nil operation IDs and zero versions. Submission has a 45-second total deadline, including bounded retries while a periodic checkpoint or its artifact cleanup finishes. These retries preserve the compiled candidate and request identity. Validation has one compiler, 30 seconds, 1..64 statements, 256 KiB of SQL, 256 catalog objects and a 1 MiB descriptor. JSON bodies are capped at 512 KiB for submission/validation and 16 KiB for adoption. Forwarded receipts are capped at 1 MiB and redirects are disabled.

## Observe progress and recover

`GET /api/v1/cluster/topology/operations/{operation_id}` reads definitive audited status. `GET /api/v1/cluster/topology` separates `committed_version` from this process's `locally_active_version`. Read/status routes have a 15-second deadline and return `Cache-Control: no-store`.

| Phase | Meaning |
| --- | --- |
| `planned`, `preparing` | Parent is still authoritative; full frozen process agreement is required before a cut. |
| `quiescing` | Exact old checkpoint is in progress or awaiting definitive sink/application settlement. Sources keep enough control traffic open to complete the barrier. |
| `cut_prepared` | Complete old cut; intake remains held while roots, stopped actors and target preparations are established. |
| `committed`, `activating` | Irreversible target Commit exists. Activation/recovery remains required; local rollback is forbidden. |
| `active` | Durable Release covers the full installed process roster. Each process still validates and applies its own current runtime authority. |
| `aborted` | Pre-Commit terminal decision. A committed old cut may remain; coordinated recovery reconciles effects and resumes the parent without rewinding progress. |

A persisted manifest alone never reports local activation. Missing participants remain required; they are not removed to manufacture Release. Checkpoint epochs continue monotonically. After Commit, a replacement process uses a new runtime UUID and coordinated recovery round; it cannot consume an original runtime's Release.

Restart with the same durable namespace and either the complete current inventory or the exact complete original adopted bootstrap. Durable target authority takes precedence over that original bootstrap. Changed definitions and arbitrary subsets reject. Recovery selects the greatest target checkpoint, or the exact authorized migration root before the first target checkpoint, and rechecks state/source availability and full readiness before intake Release.

Timeout or absent acknowledgement is not proof that a durable write or sink commit failed. Query status and retry the same identity. Pre-Commit leader/process failure follows the existing abort/recovery path. After Commit, preserve target authority and recover it. A reversal is another checked forward migration.

Errors preserve registry codes: 400 malformed/bounded request; 409 stale parent, conflicting UUID or unresolved operation; 422 unsupported contract/protocol; 429 compiler busy; 504 bounded deadline or uncertain append; 503 unavailable/fenced authority. Busy/deadline responses include `Retry-After: 1`. Non-cluster topology routes return 404; missing or incorrect console credentials return 401.

## Replay and retention

Unchanged streams retain incarnation and output sequence identities. Reader reconnect crosses pipeline identities only through exact released roots while keeping all schema/query/distribution/changelog/retention contracts strict. Dropped/recreated identity reuse is prohibited. AS OF EPOCH remains an existing replay boundary, not durable named-consumer acknowledgement storage.

Cleanup audits checkpoint predecessor edges and exact horizon references. Missing/corrupt roots stop deletion. This checkpoint retains migration roots and their old state pins conservatively, and its journal rejects admission at 64 retained operation identities. Root consumption and journal reclamation remain outstanding work; Release alone is insufficient retirement authority.

## Qualification

See [progress](cluster-topology-migrations-progress.md) and the linked source/test evidence. Controlled connector tests, authority fixtures, real Kafka/S3 processes and comparative performance results have distinct scopes. No zero-downtime or unqualified production latency claim is made. The original cut/abort soak does not certify target activation; the new public migration soak must pass separately.
