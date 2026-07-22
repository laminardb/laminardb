# Checkpoint and CDC production plan — 2026

**Status:** corrective implementation is present but not production-certified. The current commit
must pass deterministic, connector-integration, fault, soak, and latency gates before any guarantee
is widened. Cluster exactly-once remains rejected by LDB-0013.

Distributed aggregate/window/join working state is designed separately in
[ADR-008](../architecture-decisions/ADR-008-managed-vnode-keyed-state.md) and its
[phased plan](distributed-keyed-stateful-operators.md). This plan remains authoritative for
checkpoint/CDC/connector delivery and external commit certification, not keyed-operator admission.

LaminarDB uses a decision-led checkpoint protocol. An immutable, provenance-checked state seal and
one durable decision commit the recovery cut. External sink publication is re-driven from that
decision and is not part of the pipeline pause.

## Current protocol invariants

1. Connector contracts and runtime durability are admitted before connector I/O. Unsupported
   delivery, placement, state, or sink combinations fail closed.
2. Every attempt represents one never-reused nonzero checkpoint identity, bound to deployment and
   pipeline identity. The current duplicated epoch and checkpoint-ID fields must be equal at every
   runtime, wire, and storage boundary; failed attempts are abandoned rather than reused.
3. Barriers align source and shuffle input before capturing source cursors with operator state.
   A source cursor never bisects its upstream replay unit; PostgreSQL emits a committed transaction
   whole even when it exceeds the normal batch target.
4. Every participant persists immutable vnode partials, commit descriptors, source handoff,
   manifest, and a final readiness record under the exact attempt. Cluster artifacts bind the
   assignment, writer process, leader proof, length, and digest.
5. A canonical seal is created only after exact vnode, descriptor, and participant coverage is
   verified. Zero-vnode workers stay fenced and do not join the assignment or checkpoint quorum.
6. A valid Prepared manifest is the local write-ahead witness. Its epoch has one create-once
   terminal key, so a delayed Commit and startup Abort race to one immutable winner without an
   extra normal-path registry write. Startup validates provenance and settles every live Prepared
   witness before inventory selection. Source acknowledgement follows the durable engine decision;
   designated external publication follows asynchronously.
7. Cluster commits require the implemented content-addressed recovery capsule. It binds the exact
   assignment and process roster, seal, participant manifests, source union, watermarks, and
   portable state. Its implementation still requires the final fault and soak matrix.
8. Retention is bounded by durable decision and external-publication floors. A deployment-scoped
   tombstone floor prevents stale recreation below the retained window. Shutdown retains issued
   durable tasks until terminal completion and never detaches them behind released lifecycle
   fences.

## Runtime and state policy

| Runtime | Admitted delivery | Required recovery authority |
|---|---|---|
| Embedded library | Best effort, at least once, and the internal generator-to-Delta exact candidate | At-least-once requires node-durable checkpoint/decision storage; exactly-once also requires node-durable state, the built-in local decision store, an exact-certified source, and an exclusive deployment lock |
| Single-node server | Same protocol and guarantees as embedded | Same node-durable local authority and ownership requirements |
| Cluster | Contract-eligible at least once only | Cluster-shared checkpoint/state storage, exact assignment/process fencing, and durable recovery control |
| Cluster exactly-once | Rejected by LDB-0013 | Requires a leader term consumed by both the durable engine decision and every external sink cursor commit |
| Local exactly-once with an injected decision/object store | Rejected by LDB-0014 | Remains closed while the injected store erases the provenance of the built-in exclusive owner |

The current StateBackend stores checkpoint-attempt artifacts; it is not the hot keyed-state engine.
Embedded and single-node execution should keep hot state in memory or local NVMe and recover from a
node-durable checkpoint. Cluster recovery authority stays in shared object storage; local memory or
NVMe may be a secondary cache, never the only copy. A local LSM working-state tier should be added
only with state-size and latency evidence. A RisingWave-style object-store-primary LSM is a separate
storage engine, not a label to apply to ordinary checkpoint blobs.
Removing the experimental tier also removed its approximate live-state budget. Fixed group-count
guards do not bound variable-width keys, emission state, or allocator retention. Cluster admission
therefore rejects keyed aggregates and windows until the common keyed-state engine enforces a byte
budget; embedded and single-node operation must not be described as byte-bounded yet.

Cluster shuffle is bound to the exact assignment owner vector and boot-incarnation roster. Streams
use assignment-scoped sequence domains, barriers carry high-water marks, and fan-out must cover the
exact peer roster. Process loss triggers recovery and source replay; LaminarDB does not need a
Spark-style external batch-shuffle service unless measurements prove recomputation cannot meet the
recovery objective.

Gossip and static discovery provide membership, addresses, failure suspicion, and low-latency
notification transport. They do not choose assignments, source cursors, checkpoint verdicts, or
external commit terms. Both discovery modes must consume the same durable authority and pass the
same fault matrix.

## Effective connector matrix

| Connector path | Current contract | Embedded/single-node | Cluster |
|---|---|---|---|
| Deterministic generator source | Replayable, exact-certified singleton | At-least-once; the only source admitted for the local exact candidate | Rejected: singleton placement and cluster exact remain closed |
| Kafka source | Replayable, splittable when engine-assigned | At-least-once for explicit engine-owned topic/partition baselines; exact delivery rejected by LDB-5037 pending certification; dynamic group ownership is best effort | At-least-once after exact assignment and drain certification |
| Kafka sink | DurableAtLeastOnce with durable broker acknowledgement; weaker acknowledgements are ephemeral | At-least-once; never CheckpointCommittable | At-least-once |
| PostgreSQL CDC source | CommitCoupled singleton | Fresh Initial startup is rejected before I/O; exact-checkpoint resume is read-only and binds the PostgreSQL cluster, exact WAL timeline/socket, database, publication definition, slot properties, and source filters; unsupported TRUNCATE publications fail admission; whole transactions are emitted atomically; exactly-once admission remains closed pending bootstrap and certification | Rejected until fenced singleton placement exists; timeline changes and failover remain rejected |
| MongoDB CDC source | Replayable singleton | Versioned post-batch/resume/start-after replay binds the deployment and collection UUID; exact delivery is rejected by LDB-5037 pending certification; there is no initial collection snapshot or transaction-group guarantee | Rejected until fenced singleton placement exists |
| Delta Lake source | Ephemeral singleton | Local best effort only | Rejected |
| Iceberg source | Ephemeral singleton; REST catalog | Local best effort only | Rejected |
| PostgreSQL/MongoDB append sink | DurableAtLeastOnce, multiwriter | At-least-once | At-least-once |
| PostgreSQL/MongoDB mutable sink | DurableAtLeastOnce, singleton | At-least-once | Rejected until keyed or singleton writer handoff is fenced |
| Delta Lake sink | DurableAtLeastOnce normally; coordinated append is CheckpointCommittable | Coordinated append is reachable for exact validation only with the certified generator and remains uncertified end to end | At-least-once for a shared append target; exactly-once rejected |
| Iceberg sink | DurableAtLeastOnce REST-catalog append; never CheckpointCommittable | At-least-once | At-least-once only for a shared multiwriter warehouse |
| MySQL CDC source | Not exposed | Bounded decoding, snapshot, and replay are prerequisites for re-entry | Not exposed |

Kafka broker offsets are monitoring progress, not recovery authority. MongoDB replay tokens provide
event-level replay and do not prove an upstream transaction boundary. PostgreSQL and MongoDB
exactly-once sinks will require a short target transaction that applies deterministic operations
and conditionally advances an exact predecessor cursor; ambiguous responses are resolved by
reading that cursor.

## Public configuration

Keep public choices to runtime mode, requested delivery, source scope/start policy, connection and
security data, durable storage location and namespace, checkpoint cadence and end-to-end deadline,
recovery objective, deterministic sink keys, and hard memory/local-cache budgets. The server
exposes one optional `server.key_groups` expert setting in cluster mode only. Embedded and
single-node runtimes resolve to one key group; cluster defaults to 256 and may override it only
before the deployment namespace is created. Persist the resolved value and reject every later
mismatch. Do not expose the hash, encoder, seed, source partition count, placement, gossip
consistency, compaction strategy, or
separate backend-specific vnode-capacity matrices.

Consistency, topology, and sink input mode remain typed internal connector contracts because they
enforce independent admission proofs; they are not user options. Derive writer identity,
transaction alignment, snapshot concurrency, batching, retry cadence, shuffle behavior, and
publication ownership. State writer identity is private: local modes use a local audit identity and
cluster mode derives it from the runtime node.

Use one absolute checkpoint-attempt deadline, one connector health deadline carried from enqueue
to acknowledgement, and a private bounded cleanup budget. Stage-specific timeout resets, writer
IDs, per-sink checkpoint statuses, and independent polling thresholds do not belong in the public
checkpoint surface.

## Implementation order

1. **Close durable authority races first.** Resolve delayed terminal-outcome and recovery-capsule
   writes, term-fence every durable per-node recovery-control value, preserve the global recovery
   generation across process terms, and fail unknown gossip lifecycle state closed. These are
   recovery-cut prerequisites, not state-engine work.
2. **Freeze the immutable deployment and partition contract.** Replace duplicated state/checkpoint
   roots with one durable storage URL and deployment namespace. Resolve `server.key_groups` once from
   runtime mode, persist it, and remove backend-specific vnode capacities and unchecked numeric
   fallbacks. Version the canonical Arrow-key encoding, ordered key fields, hash, global key group,
   and source-split mapping as one partitioning ABI. Bind that ABI to catalog/runtime identity,
   assignments, checkpoints, shuffle handshakes, and recovery before enabling rescale. Pin reviewed
   golden vectors; an encoder or vector change requires an ABI bump and deliberate state rejection.
   Local guaranteed delivery must reject temporary or absent storage; cluster must reject node-local
   storage. Remove dead profile/parallelism/retention choices before certifying deployment artifacts,
   and never render a guaranteed Helm mode onto `emptyDir`.
3. **Certify the corrective checkpoint core.** Complete compile and deterministic tests for seal
   provenance and bounds, decision ambiguity, recovery capsules, assignment adoption, retention,
   shutdown tails, and corruption. Run the finite local process-death output oracle and remove dead
   code before another feature cycle.
4. **Fail closed on mutable capture errors.** Operator and vnode capture can consume dirty sets or
   drain accumulators before rebuilding them. Any capture error must fault the pipeline and recover
   from the last committed cut before sources resume. Prove this with an injected drain/rebuild
   failure in local and cluster execution; an in-memory retry is not safe.
5. **Cancel superseded shuffle scope before rotation.** Bind every blocking connect, send, receive,
   queue, byte permit, and stream slot to the exact assignment/recovery scope. Invalidation,
   suspension, rewind, or replacement must cancel that scope before waiting for the rotation fence.
   Once a newer durable assignment is audited, close old source and shuffle authority before any
   handoff or state read, carry one absolute deadline, and remain fenced on failure.
6. **Isolate shuffle delivery classes and harden routing.** Give checkpointed data/barriers and
   ephemeral subscriptions separate bounded queues, byte reservations, and holdovers under one
   node memory cap and separate connections. Preserve one FIFO domain for checkpointed data plus
   barriers. Make routing
   return errors for invalid owner/vnode inputs, prove row-count conservation, and reject reserved
   protocol field/stage names at DDL admission.
7. **Certify the cluster at-least-once protocol on stateless and explicitly bounded small-state
   graphs.** Test the exact shuffle roster, sequence/high-water loss detection, reconnect, process
   replacement, rebalance, recovery Release, capsule restore, and authority retention under static
   and gossip discovery. This is protocol evidence, not stateful cluster production certification,
   and must not be reported as exactly-once evidence.
8. **Make snapshot capture bounded and non-blocking.** Capture one concrete immutable checkpoint
   image in the aligned section, then encode and upload it in one owned blocking job. Use a single
   fallible byte reservation across graph, materialized views, tables, and vnodes; retain the charge
   until a timed-out worker actually exits. Optimize dirty-only vnode capture in a separate ancestry
   cycle. Validate event-loop and ingestion p99 plus RSS before lifting any cluster stateful gate.
9. **Finish durable authority and generic singleton placement.** Keep the exclusive local
   implementation for embedded/single. Use a linearizable cluster authority, with PostgreSQL as
   the first implementation, for owner terms, assignments, decisions, recovery rounds, and source
   handoff. Non-owners stay dormant; an old owner cannot acknowledge source progress or write a
   mutable sink after handoff.
10. **Benchmark, then implement common keyed working state without changing recovery truth.** Run a
   working set larger than RAM on target Linux NVMe under sustained mixed reads, writes,
   checkpointing, and compaction. Gate latency percentiles, throughput, CPU, RSS, write
   amplification, crash reopen, and corruption in all three runtime modes before choosing the LSM.
   Shared checkpoints remain primary in cluster and local recovery remains an optimization. Keep
   compaction and remote persistence off the record path.
11. **Complete PostgreSQL source correctness.** The current resume path binds the system, timeline,
   database, publication, slot, and filter identity at admission, then revalidates the system,
   timeline, database, slot cursor, and WAL upper bound on the exact replication socket. Timeline
   changes and TRUNCATE publications fail closed. Next, create an exported-snapshot logical slot
   while its replication session remains open, run internally bounded parallel snapshot readers,
   then continue WAL from the same consistent point. Add continuous publication and slot drift
   fencing before a durable decision, validate failover-slot ancestry and readiness, support
   streamed transactions, and reject unsupported two-phase messages. Certify local at-least-once,
   local exact, then clustered singleton failover.
12. **Complete MongoDB source correctness.** Certify the current event-token and post-batch replay,
   then add a bounded snapshot/change-stream repair protocol. Bind deployment, FCV, scope,
   pipeline/options, and collection UUIDs; fail closed on history loss and unsupported invalidation
   transitions. Do not advertise transaction-group atomicity.
13. **Certify database sink throughput, then add exact cursors.** Preserve bounded PostgreSQL COPY
   and MongoDB bulk paths for low-latency at-least-once. Add key-affine or fenced singleton
   placement for mutable cluster writes. Implement target-transaction cursor protocols locally
   before cluster fencing and ambiguous-commit tests.
14. **Add remaining recoverable external publication.** Stage Kafka records durably and publish
   them with an exact namespaced cursor in a fresh transaction after the engine decision. Certify
   Delta first; Iceberg remains at-least-once until it has a real predecessor-cursor committer.
   Remove LDB-0013 only for each concrete source/state/sink combination that passes the complete
   stale-leader, partition, rebalance, process-death, and ambiguous-commit matrix—never globally.

## Validation gates

- Format, strict lint, and all relevant feature combinations for core, connectors, DB, and server.
- Deterministic tests for every immutable record, bound, conflict, timeout, cancellation, retention,
  and recovery invariant.
- Docker integration tests for PostgreSQL logical slots/snapshot/failover, MongoDB replica-set and
  sharded replay/history loss, Kafka, and object-store lakehouse commits.
- Integration commands must assert the expected selected-test count and fail when Docker, Kafka,
  MinIO, PostgreSQL, or MongoDB is unreachable; a zero-test or dependency-skip exit is not evidence.
- Fault injection before and after capture, seal, Prepared publication, decision, source feedback,
  external cursor commit, rebalance, and shutdown. Recovery assertions use the exact canonical
  checkpoint identity and reject split epoch/checkpoint-ID encodings.
- Local exactly-once soak uses the finite hard-kill source/state/output oracle, not a clean-close
  recovery smoke test. Cluster
  at-least-once soak checks no gaps or coherent state rollback while treating duplicates according
  to its advertised contract.
- Performance gates record rows and bytes per second, p50/p95/p99 source-to-output latency, batch
  residence, checkpoint stall, durable completion, external-publication lag, spill, and recovery.
- Remove superseded code and unused configuration after every feature cycle, then rerun the
  proportional gates. The final Linux fault matrix and latency record must name the exact commit.

## Primary research and implementation references

- Apache Flink 2.3 [fault tolerance](https://nightlies.apache.org/flink/flink-docs-stable/docs/learn-flink/fault_tolerance/),
  [parallelism and key groups](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/execution/parallel/),
  [state backends](https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/state/state_backends/),
  [experimental disaggregated state](https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/state/disaggregated_state/),
  and [network tuning](https://nightlies.apache.org/flink/flink-docs-stable/docs/deployment/memory/network_mem_tuning/)
- Apache Spark 4.2.0 [state and shuffle invariants](https://spark.apache.org/docs/latest/streaming/additional-information.html),
  RisingWave [stable vnode mapping](https://risingwavelabs.github.io/risingwave/design/consistent-hash.html),
  Apache Kafka 4.3.1 [task and partition assignment](https://kafka.apache.org/43/streams/developer-guide/streams-rebalance-protocol/),
  Apache Arrow 57.2 [row encoding](https://arrow.apache.org/rust/arrow_row/struct.RowConverter.html), and Arroyo
  [architecture](https://doc.arroyo.dev/architecture/)
- PostgreSQL 18 [replication protocol](https://www.postgresql.org/docs/18/protocol-replication.html),
  [logical decoding](https://www.postgresql.org/docs/18/logicaldecoding-explanation.html), and
  [logical replication failover](https://www.postgresql.org/docs/18/logical-replication-failover.html)
- MongoDB [change streams](https://www.mongodb.com/docs/manual/changestreams/), current
  [production guidance](https://www.mongodb.com/docs/manual/administration/change-streams-production-recommendations/),
  and the accepted [driver specification](https://github.com/mongodb/specifications/blob/master/source/change-streams/change-streams.md)
- Apache Flink CDC 3.6 [PostgreSQL](https://nightlies.apache.org/flink/flink-cdc-docs-release-3.6/docs/connectors/flink-sources/postgres-cdc/)
  and [MongoDB](https://nightlies.apache.org/flink/flink-cdc-docs-release-3.6/docs/connectors/flink-sources/mongodb-cdc/)
- [DBLog](https://arxiv.org/abs/2010.12597), the CDC-specific 2026 preprint on
  [certified virtual cuts](https://arxiv.org/abs/2605.31475), and Moonlink's
  [Arrow/NVMe design](https://github.com/Mooncake-Labs/moonlink)
- the [CheckMate preprint](https://arxiv.org/abs/2403.13629) for workload-sensitive checkpoint protocol choice
  and [PGVal](https://www.vldb.org/pvldb/vol18/p585-tahir.pdf) for end-to-end guarantee validation
  under injected process and network faults
