# Cluster subscriptions over a partitioned committed output log

- **Status:** implementation contract for the initial production release
- **Date:** 2026-08-23
- **Runtime scope:** cluster runtime; local embedded and standalone subscription behaviour is
  compatibility-frozen
- **Default contract:** `delivery=committed`, `ordering=partition`, `replay=durable`

## Current implementation and failure mode

Local subscriptions use one process-memory `SubscriptionRegistry` log per named object. The log is
byte bounded, assigns a process-local scalar sequence, retains an optional byte-bounded suffix, and
publishes a barrier only after a local checkpoint commits. `SubscriptionPortal` owns a cursor over
that shared log and applies a compiled physical `WHERE` expression before returning `PortalFrame`
values.

Cluster runtime deliberately disables `open_subscription()` before schema lookup, filter
compilation, reader attachment, or external I/O. The checkpoint callback also skips subscription
cut reservation, abort, publication, and invalidation in cluster mode. This is correct today:

- aggregate state is vnode-partitioned, but `IncrementalAggState::emit_changelog_delta()` combines
  all locally resident vnodes into one anonymous Arrow batch;
- the local registry has one scalar sequence and process-local history, so it cannot represent a
  distributed output cut;
- `last_emitted` is updated when the aggregate batch is built, before a durable subscription sink
  can accept the output;
- cluster checkpoint manifests and the committed checkpoint index do not bind subscription output;
- a gateway can see only its node's in-memory registry and cannot recover history after failure.

Removing the cluster guard without replacing all five properties would permit partial results,
gaps, stale-owner publication, and false progress.

### Relevant current call graph

```text
SQL / API open
  laminar-server HTTP WebSocket / pgwire / laminar-db typed facade
    -> LaminarDB::open_subscription
       -> ensure_subscription_runtime_supported        (cluster rejection)
       -> topology_ddl_lock.read
       -> lookup_subscription_schema
       -> filter_compile::compile
       -> SubscriptionRegistry::subscribe
       -> SubscriptionPortal::{open, open_with_filter}
          -> SubscriptionReader::{next, try_read}
          -> SubscriptionPortal::process_read
             -> filter_compile::apply
             -> PortalFrame::{Batch, Barrier, Lagged, Error}

record path
  StreamingCoordinator::execute_cycle
    -> ConnectorPipelineCallback::execute_cycle
       -> OperatorGraph::execute_cycle
          -> SqlQueryOperator::process_cluster
             -> IncrementalAggState::process_batch_for_vnode
             -> IncrementalAggState::emit
                -> emit_changelog_delta               (currently merges vnodes)
                   -> mutate last_emitted / dirty keys / deleted groups
    -> update_mv_stores
    -> push_to_streams
       -> SubscriptionRegistry::send_batch             (local process log)
    -> write_to_sinks

checkpoint
  StreamingCoordinator checkpoint admission/barrier lifecycle
    -> PipelineCallback::reserve_subscription_cut       (local only)
    -> ConnectorPipelineCallback checkpoint capture
       -> OperatorGraph vnode state captures
       -> CheckpointCoordinator::pack_checkpoint
       -> CheckpointStore::save_checkpoint
          -> immutable node data, then participant manifest
       -> await_prepared_participant_manifests
       -> CheckpointCoordinator::build_committed_index
          -> CommittedCheckpointIndex::validate_participant_manifests
       -> CheckpointDecisionStore::create_committed_checkpoint
       -> CheckpointDecisionStore::record_outcome(Commit)
    -> PipelineCallback::publish_barrier                (local only)
  abort paths
    -> PipelineCallback::abort_subscription_cut         (local only)

recovery / reassignment
  authoritative Commit outcome
    -> CommittedCheckpointIndex
    -> verified participant manifests and state ranges
    -> aggregate vnode restore
  assignment change
    -> PendingVnodeTransition
    -> OperatorGraph::prepare_vnode_transition
    -> IncrementalAggState::prepare_owned_vnode_transition
    -> publish_prepared_vnode_transition behind rotation fence
```

## Target semantics

The initial release supports named, non-windowed, planner-certified managed aggregate streams:

```sql
SUBSCRIBE stream;
SUBSCRIBE stream WHERE <predicate>;
SUBSCRIBE stream AS OF EPOCH <n>;
```

The ordinary form attaches at the current committed tail. Rows in checkpoints committed before
attachment are not replayed. It emits later committed partition frames and one progress event per
whole-cluster committed checkpoint.

`AS OF EPOCH n` loads the authoritative committed index for epoch `n`, verifies that it belongs to
the current stream generation, and starts each partition at that checkpoint's exclusive frontier.
It then emits the committed suffix through later retained checkpoints. Replay is checkpoint
granular. It is not storage of a named consumer's last acknowledged cursor; a client that saves
epoch `n` and disconnects part-way through the next interval can receive that interval again.

Delivery order is strict within each output partition. Interleaving among partitions is fair but
has no semantic order. Arrival order is not a cluster-wide total order, event-time order, or SQL
sort order.

## Identities and output-distribution certificate

Persisted subscription metadata uses distinct strong types:

```text
StreamGeneration        SHA-256 identity for one durable stream incarnation
OutputPartitionId       vnode identifier
PartitionSequence       frame position within one generation and partition
OutputFrameId           (generation, partition, sequence)
PartitionFrontier       first sequence not covered by a checkpoint
OutputSegmentRef        immutable segment identity and integrity metadata
SubscriptionProtocolVersion
```

For the current create-once cluster catalog, `StreamGeneration` binds the deployment incarnation,
canonical catalog object identity, canonical DDL/query identity, and pipeline identity. Resetting
or replacing the durable catalog namespace creates a new deployment and therefore a new stream
generation. A future replicated topology-version protocol must persist a new object incarnation
on every drop/recreate; it may not derive a replacement generation solely from a reused name or
identical DDL.

Planning produces an `OutputDistributionCertificate`. It binds:

- stream name and generation;
- final graph operator identity;
- partition ABI and vnode count;
- canonical grouping-key expression fingerprint, or the global singleton marker;
- output schema fingerprint and changelog mode;
- query and pipeline fingerprints.

The only initial certificates are `VnodePartitioned` for a direct, non-windowed managed keyed SQL
aggregate and `Singleton` for the existing global-aggregate vnode-zero path. The initial external
release admits keyed aggregates; singleton support remains internal until its cluster release tests
pass. A certificate is evidence, not a guess made by a gateway. Recovery, writer activation,
reassignment, checkpoint merge, and subscription open all compare the exact certificate.

## Partitioning model

The internal subscription output sink is not an aggregation engine. Existing routing remains:

```text
canonical aggregate key
  -> PartitionKeyCodecV1 / stable vnode hash
  -> assignment-certified vnode owner
  -> AggregateVnodeState
  -> output partition with OutputPartitionId == vnode
```

A global aggregate retains the existing vnode-zero singleton execution strategy. No partial/final
aggregation and no leader data-plane hop is introduced.

Aggregate emission produces a sidecar `PartitionedOutputBatch`. Ordinary graph edges and external
sinks continue to consume the unchanged Arrow batch; the subscription output sink alone consumes
the sidecar partition and generation metadata. User schemas never receive reserved sequence
columns.

Dirty keys are ordered by canonical encoded group-key bytes within each vnode. For one key, a
retraction precedes its replacement insertion. Deterministic frame splitting uses the ordered key
roster and a fixed bounded row/byte limit. Different vnodes never share a logical frame.

## Sequencing model

The first frame sequence is `0`. Each vnode-owned aggregate state stores `next_sequence`, meaning
the first unassigned frame sequence. Assignment occurs only after overflow preflight and produces
contiguous frame IDs. Empty emission does not consume a sequence.

The same vnode state frame that stores aggregate groups, `last_emitted`, and dirty-key recovery
state stores `next_sequence`. It therefore checkpoints, restores, and moves with vnode ownership
without a global sequence map or record-path lock.

An append validates stream generation, pipeline generation, assignment version and digest,
participant boot incarnation, vnode ownership, and expected next sequence. An identical duplicate
frame ID and digest is idempotent. A different digest is a terminal consistency failure. A gap,
regression, overflow, or stale authority fails the cycle and requires recovery before intake can
continue.

Aggregate output bookkeeping is transactional at the cycle boundary. Immutable per-vnode output
and proposed `last_emitted`/deletion changes are prepared first. The bounded output sink either
accepts the complete cycle roster or accepts none. Only then are proposed changelog bookkeeping
and sequence increments published. Any failure after operator mutation but before this boundary
forces checkpoint recovery and discards all uncommitted writer state before another cycle runs.

## Checkpoint lifecycle

Each participant owns a bounded pending writer on the compute task. It only retains complete
partition frames and receipts; it performs no object-store, file, or network I/O. At checkpoint
capture the compute task freezes the exact pre-cut roster and frontier for every locally owned
certified partition, transfers ownership to the existing checkpoint tail, and immediately starts a
new bounded post-cut buffer only after the capture has been accepted.

The background checkpoint tail:

1. seals deterministic bounded Arrow IPC segments for all pre-cut frames;
2. validates and create-writes immutable segment objects;
3. builds a canonical node subscription manifest;
4. includes that manifest in the participant checkpoint manifest;
5. persists ordinary state/node data and the participant manifest;
6. waits for every assignment participant;
7. merges and validates one complete canonical partition-frontier vector;
8. binds it into the existing `CommittedCheckpointIndex`;
9. creates the immutable index and records the terminal Commit outcome.

Only step 9 makes output visible. Participant readiness, completed uploads, a leader hint, or an
object listing is not committed subscription progress.

Abort publishes no output progress. Captured frames remain uncommitted recovery input only until
the exact attempt resolves; immutable uploads not reachable from a committed retained index become
orphans eligible for grace-period cleanup. An in-doubt terminal outcome requires recovery and is
never guessed.

## Manifest and object-store model

Participant checkpoint manifests contain canonical node-local subscription entries. The committed
checkpoint index contains canonical cluster-wide entries. One logical stream entry contains:

```text
protocol version
stream identity and generation
output-distribution certificate digest
schema fingerprint
canonical partition frontiers
canonical immutable segment references
manifest digest
```

Every certified partition appears exactly once across participants and belongs to the owner named
by the index assignment fence. Frontiers are ascending, duplicate-free, in range, and complete for
the certificate vnode domain. A frontier is the first sequence not covered by the committed cut.
For a predecessor cut, new segment ranges must begin exactly at its frontier and end at the new
frontier. Empty ranges keep the frontier unchanged.

Segments use bounded Arrow IPC and a self-validating envelope containing protocol version,
deployment, stream identity/generation, partition, first and exclusive-end sequence, frame and row
counts, schema fingerprint, encoded length, payload digest, and checkpoint attempt. Metadata is
validated before allocating from encoded lengths.

Provider-neutral paths are deterministic and do not contain credentials:

```text
subscription-output/v1/<deployment>/<stream-id>/<generation>/<partition>/
  <first>-<end>-<payload-sha256>.arrow
```

Create-once collision handling loads and validates the existing object. Exact metadata and digest
is idempotent; any difference is a terminal consistency error.

## Recovery and reassignment

Startup selects the exact authoritative committed outcome and index, validates every subscription
manifest and required reference, and restores output `next_sequence` in the same vnode state cut
as aggregate groups, dirty-key state, and `last_emitted`. Corrupt or incomplete output metadata
prevents the compute generation from opening intake. Recovery never falls back to an older cut.

Live reassignment imports the committed donor vnode state, including output sequence state, behind
the existing rotation fence. The predecessor writer is fenced by assignment version, assignment
digest, and boot incarnation before the prepared successor state is published. Previously
committed segments remain valid shared history independent of current ownership.

## Gateway model

Every server node can be a gateway. A cluster portal is stateless apart from bounded connection
queues, bounded segment decoders, and a bounded manifest cache. It reads authoritative checkpoint
outcomes and indexes from the shared store, verifies all referenced metadata and payload digests,
and maintains one cursor per partition.

The merge scheduler round-robins ready partition cursors. A temporarily slow object read does not
block ready partitions, while a bounded concurrency limit prevents one reader from opening every
segment at once. Each partition is emitted only at its exact expected sequence. Missing, corrupt,
overlapping, duplicate-conflicting, pruned, or unavailable committed data terminates the portal
with a structured error; it never skips ahead.

Tail attachment captures the latest committed checkpoint reference while holding the topology read
guard and begins at that index's partition frontiers. Replay attachment resolves the exact requested
committed epoch through outcome/index authority and verifies retention before reading segments.
Following a new committed index is idempotent and validates predecessor continuity before exposing
frames or progress.

## Wire compatibility

`SubscriptionPortal` becomes backend-neutral while preserving the local reader and local frame
semantics. Backend selection occurs after schema, generation, distribution certificate, and filter
resolution under the topology read lock.

WebSocket data and progress envelope versions remain backward compatible. Cluster data frames may
add optional `stream_generation`, `partition`, `partition_sequence`, and `committed_epoch` fields.
Clients must not infer cross-partition order.

pgwire keeps the user data schema unchanged. Existing explicit control/progress rows carry epoch
and checkpoint progress. Partition metadata is not injected into SQL rows. Cursor `FETCH` remains
bounded and may cross segment/checkpoint boundaries.

Typed Rust subscriptions continue returning typed batches/rows. An optional frame-level API
exposes cluster metadata without changing `FromBatch` conversion.

Opaque resume tokens, when emitted, are versioned and integrity protected with a server-held key.
They bind stream generation and a checkpoint/frontier digest, and contain no object paths,
credentials, node addresses, or client-interpreted assignment details. `AS OF EPOCH` remains
available without tokens.

## Structured failures

Correctness failures use stable error variants for unsupported distribution, generation mismatch,
uncommitted/pruned epoch, corrupt manifest or segment, missing segment, schema mismatch, sequence
gap, conflicting duplicate, stale writer, changed assignment, unavailable backend, lag, invalid or
expired token, and lost retention. Wire adapters map variants to stable protocol error codes while
retaining bounded human-readable context.

## Resource bounds and backpressure

The implementation has hard limits for frame bytes, segment bytes, pending bytes per stream and
partition, partitions per stream, segments and manifest bytes per checkpoint, concurrent uploads,
retry deadline, gateway read concurrency, decoded frame bytes, manifest-cache entries, and
connection queue bytes. Limits reuse checkpoint timeout, vnode count, subscription retention bytes,
and existing Arrow IPC bounds where those already express the same resource.

The compute task never waits for a subscriber. Writer-capacity exhaustion returns a recovery error
and closes intake; output is never dropped. A slow gateway consumer is disconnected with
`SubscriberLagged`, retaining its last whole-checkpoint progress boundary for explicit epoch replay.
Object-store delay is bounded by the checkpoint deadline. Retry queues and orphan cleanup are
bounded by exact retained-index traversal and a grace period.

Retention keeps a bounded suffix of committed checkpoint indexes and every segment reachable from
them. It preserves the latest cut and active supported replay pins. GC follows index predecessor
links; listings may find orphan candidates but never establish reachability. Uncertain reachability
prevents deletion. This is not a named-consumer cursor or indefinite pin.

## Security model

Gateways remain behind existing WebSocket/HTTP and pgwire authentication and authorization. No new
unauthenticated endpoint is introduced. Internal control and shuffle traffic retain existing TLS
and mTLS conventions. Shared-store credentials stay server-side.

Manifests, segments, and tokens are treated as untrusted bytes: version, canonical metadata,
cardinality, encoded lengths, schemas, and digests are validated before proportional allocation or
Arrow decoding. Traces exclude row payloads, credentials, tokens, paths, and secrets.

## Observability

Existing Prometheus registry patterns expose bounded-cardinality counters, gauges, and histograms
for active readers, opens/failures, committed frames/rows/bytes, segments and write failures,
manifest/integrity failures, stale-writer rejections, sequence gaps, replay work/pruning, lag
disconnects, pending/retained/orphan bytes, checkpoint prepare latency, commit visibility, and
manifest refresh latency. Labels are limited to stable mode/result classes; raw stream names,
subscriber IDs, vnodes, and error strings are forbidden.

Structured tracing records open/close, generation, start mode, selected checkpoint, partition and
segment counts, replay completion, lag termination, corruption, stale writer rejection, checkpoint
publication, and orphan cleanup. The cluster status API reports aggregate reader/writer health and
bounded byte/error totals without credentials or per-stream high-cardinality detail.

## Hard invariants and failure behaviour

| Invariant | Enforcement | Failure behaviour |
|---|---|---|
| Single writer | append authority matches generation, pipeline, assignment certificate, owner, and process incarnation | reject stale writer; terminal recovery fault |
| No silent gaps | reader expects exact next sequence and verifies every segment | structured terminal integrity/retention error |
| Checkpoint atomicity | subscription cut is part of the existing committed index and terminal Commit outcome | previous complete cut or next complete cut; never partial visibility |
| State/output consistency | aggregate bookkeeping, sequence, pending frames, and state are one cycle/checkpoint cut | atomic append failure or coordinated recovery before intake |
| Generation isolation | every cursor, manifest, segment, and token binds generation | `GenerationMismatch` |
| Rebalance continuity | sequence is vnode state and transition publication uses the existing rotation fence | successor stays fenced on mismatch |
| Bounded resources | fixed writer, upload, decoder, cache, and connection limits | backpressure/recovery or lag disconnect; never silent drop |
| Replay integrity | full envelope, range, length, row count, schema, and SHA-256 validation | terminal corruption/missing error |
| Explicit ordering | only partition sequence is exposed as ordering metadata | no global-order field or claim |
| Fail-closed admission | exact output-distribution certificate required before backend I/O | stable unsupported-plan error |

## Operator admission matrix

| Plan shape | Initial cluster subscription status | Reason |
|---|---|---|
| Named non-windowed managed keyed aggregate | supported after all release gates | stable vnode-owned final output |
| Existing global aggregate | internally representable as vnode zero; externally gated | singleton-specific release coverage pending |
| Stateless projection/filter | fail-closed | stable output partition identity is not propagated |
| Raw join output | fail-closed | no certified final output distribution |
| Named join output feeding a separate keyed aggregate | aggregate stream may qualify | final aggregate owns a distinct certified distribution |
| Windowed aggregate | fail-closed | output/timer lifecycle not certified for subscriptions |
| Materialized view | fail-closed | whole-node image has no distributed output lifecycle |
| Fused join and aggregate | fail-closed | already outside cluster SQL admission |
| Arbitrary UDAF | fail-closed | distribution/determinism not certified |

Admission is evaluated before segment, object-store, connector, or gateway I/O.

## Phased release plan

### Phase 0: contracts, admission, and compatibility

Add strong persisted identities, canonical manifest validation, structured errors, and explicit
output-distribution certificates. Generalize the portal/read abstraction without changing local
behaviour. Cluster subscription remains externally disabled. Gate: every eligible aggregate has an
exact certificate or is rejected, protocol/version tests pass, and no cluster backend is reachable.

### Phase 1: vnode-preserving aggregate output

Emit deterministic per-vnode changelog batches, add vnode-owned sequence state, checkpoint and
transfer it, and make output/bookkeeping publication transactional. Cluster subscription remains
disabled. Gate: partition identity, deterministic ordering/splitting, continuation, stale-owner,
failure, and local-regression tests plus before/after hot-path benchmarks pass.

### Phase 2: checkpointed durable output log

Add bounded immutable segment encoding/upload, participant and global manifests, restore, retention,
orphan cleanup, and crash/integrity injection. Cluster subscription remains disabled. Gate: every
crash point yields only the previous or next complete cut; corruption and resource exhaustion fail
closed.

### Phase 3: multi-active committed gateways

Add backend selection, fair bounded replay/follow readers, WebSocket/pgwire/typed metadata support,
metrics/status, and three-node recovery/lag/retention coverage. Relax the blanket guard only after
all Phase 3 tests and the real MinIO-backed subscription soak pass for the exact admitted scope.

## Later phases (not enabled here)

### Phase 4: optional live relay

An explicit `delivery=live, ordering=partition` mode would relay from current owners with bounded
buffers, attempt identity, pending/committed state, invalidation, and at-least-once recovery. It may
not replace committed delivery as the default or claim exactly-once external delivery.

### Phase 5: durable named consumers

Add consumer groups, fenced sessions/leases, CAS-updated partition cursor vectors, monotonic
acknowledgement, retention pins/expiry, authorization, and audit events. An epoch replay boundary is
not a consumer acknowledgement.

### Phase 6: additional operator coverage

Certify stable output distributions for stateless source partitions, bounded joins, windowed
aggregates, and materialized views. Materialized views require a partitioned snapshot plus
`SnapshotComplete(frontier vector)` before changelog after that vector.

### Phase 7: aggregate scaling extensions

Partial/final global aggregation requires per-function proof of mergeability, associativity,
commutativity, retractions, overflow/decimal behaviour, floating-point reproducibility, and
determinism under repartition.

### Phase 8: optional global total order

Global order requires an explicit consensus-backed or external replicated ordered log. It must not
route the data plane through the current shared-store leader.

## Unresolved later-phase work

- replicated online topology DDL and a durable per-object recreation generation;
- authenticated resume-token key rotation and cross-version migration policy;
- durable named-reader acknowledgement and retention pin expiry;
- certified partition propagation for stateless and join outputs;
- snapshot-plus-changelog materialized-view protocol;
- live speculative relay and invalidation;
- optional global ordered-log backend;
- independently operated long-duration certification profiles beyond the initial keyed-aggregate
  committed mode.
