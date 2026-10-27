# Delivery guarantees

A pipeline runs under one delivery guarantee (`delivery` in the server configuration,
`delivery_guarantee` when embedded). Every source and sink in the pipeline must admit that
guarantee, and admission is decided from the connector configuration before any connector I/O.
A composition that is not admitted fails at startup; it is never downgraded silently.

- **Best effort (BE)**: no replay contract. Output can be lost on failure. Not available in
  cluster mode.
- **At-least-once (ALO)**: committed state and source positions are restored after a failure and
  input after the last checkpoint is replayed, so output can repeat.
- **Exactly-once (EO)**: requires a source certified for exact replay and a sink that publishes
  each checkpoint's output in one fenced, checkpoint-committed step. Committed output appears once.

## Admission matrix

Generated from the admission code by `delivery_matrix_doc_matches_admission`
(`crates/laminar-db/src/pipeline_lifecycle/delivery_matrix_tests.rs`) for one representative
configuration per row; options that change a connector's contract are listed as separate rows.
Rows and pairs show the guarantees the connector contracts admit. SQL shape, topology and
connector-specific limits elsewhere in this documentation can still reject a pipeline.

<!-- delivery-matrix:begin -->

### Sources

| Source | Embedded / single-node | Cluster |
|---|---|---|
| Kafka | EO, ALO, BE | EO, ALO |
| Kafka (Debezium) | EO, ALO, BE | EO, ALO |
| PostgreSQL CDC | ALO | rejected |
| MongoDB change stream | ALO, BE | rejected |
| Delta Lake change feed | ALO, BE | rejected |
| Iceberg (snapshot read) | BE | rejected |
| Iceberg (append read) | ALO, BE | rejected |
| Files | ALO, BE | rejected |
| RabbitMQ | ALO | rejected |
| IBM MQ | ALO | rejected |
| NATS JetStream | BE | rejected |
| WebSocket | BE | rejected |
| OpenTelemetry | BE | rejected |
| FIX TagValue | EO, ALO | rejected |
| Generator | EO, ALO, BE | rejected |

### Sinks

| Sink | Embedded / single-node | Cluster |
|---|---|---|
| Kafka | EO, ALO, BE | ALO |
| Delta Lake append (S3/S3A) | EO, ALO, BE | EO, ALO |
| Delta Lake append (Azure) | EO, ALO, BE | ALO |
| Delta Lake upsert | ALO, BE | rejected |
| Iceberg append (REST + S3) | EO, ALO, BE | EO, ALO |
| PostgreSQL | ALO, BE | ALO |
| MongoDB | ALO, BE | ALO |
| Files | ALO, BE | rejected |
| RabbitMQ | ALO, BE | ALO |
| IBM MQ | ALO, BE | ALO |
| NATS JetStream | ALO, BE | ALO |
| WebSocket server | BE | rejected |

### Embedded / single-node: strongest guarantee per pair

| Source \ Sink | Kafka | Delta Lake append (S3/S3A) | Delta Lake append (Azure) | Delta Lake upsert | Iceberg append (REST + S3) | PostgreSQL | MongoDB | Files | RabbitMQ | IBM MQ | NATS JetStream | WebSocket server |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| Kafka | EO | EO | EO | ALO | EO | ALO | ALO | ALO | ALO | ALO | ALO | BE |
| Kafka (Debezium) | EO | EO | EO | ALO | EO | ALO | ALO | ALO | ALO | ALO | ALO | BE |
| PostgreSQL CDC | ALO | ALO | ALO | ALO | ALO | ALO | ALO | ALO | ALO | ALO | ALO | — |
| MongoDB change stream | ALO | ALO | ALO | ALO | ALO | ALO | ALO | ALO | ALO | ALO | ALO | BE |
| Delta Lake change feed | ALO | ALO | ALO | ALO | ALO | ALO | ALO | ALO | ALO | ALO | ALO | BE |
| Iceberg (snapshot read) | BE | BE | BE | BE | BE | BE | BE | BE | BE | BE | BE | BE |
| Iceberg (append read) | ALO | ALO | ALO | ALO | ALO | ALO | ALO | ALO | ALO | ALO | ALO | BE |
| Files | ALO | ALO | ALO | ALO | ALO | ALO | ALO | ALO | ALO | ALO | ALO | BE |
| RabbitMQ | ALO | ALO | ALO | ALO | ALO | ALO | ALO | ALO | ALO | ALO | ALO | — |
| IBM MQ | ALO | ALO | ALO | ALO | ALO | ALO | ALO | ALO | ALO | ALO | ALO | — |
| NATS JetStream | BE | BE | BE | BE | BE | BE | BE | BE | BE | BE | BE | BE |
| WebSocket | BE | BE | BE | BE | BE | BE | BE | BE | BE | BE | BE | BE |
| OpenTelemetry | BE | BE | BE | BE | BE | BE | BE | BE | BE | BE | BE | BE |
| FIX TagValue | EO | EO | EO | ALO | EO | ALO | ALO | ALO | ALO | ALO | ALO | — |
| Generator | EO | EO | EO | ALO | EO | ALO | ALO | ALO | ALO | ALO | ALO | BE |

### Cluster: strongest guarantee per pair

| Source \ Sink | Kafka | Delta Lake append (S3/S3A) | Delta Lake append (Azure) | Delta Lake upsert | Iceberg append (REST + S3) | PostgreSQL | MongoDB | Files | RabbitMQ | IBM MQ | NATS JetStream | WebSocket server |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| Kafka | ALO | EO | ALO | — | EO | ALO | ALO | — | ALO | ALO | ALO | — |
| Kafka (Debezium) | ALO | EO | ALO | — | EO | ALO | ALO | — | ALO | ALO | ALO | — |
| PostgreSQL CDC | — | — | — | — | — | — | — | — | — | — | — | — |
| MongoDB change stream | — | — | — | — | — | — | — | — | — | — | — | — |
| Delta Lake change feed | — | — | — | — | — | — | — | — | — | — | — | — |
| Iceberg (snapshot read) | — | — | — | — | — | — | — | — | — | — | — | — |
| Iceberg (append read) | — | — | — | — | — | — | — | — | — | — | — | — |
| Files | — | — | — | — | — | — | — | — | — | — | — | — |
| RabbitMQ | — | — | — | — | — | — | — | — | — | — | — | — |
| IBM MQ | — | — | — | — | — | — | — | — | — | — | — | — |
| NATS JetStream | — | — | — | — | — | — | — | — | — | — | — | — |
| WebSocket | — | — | — | — | — | — | — | — | — | — | — | — |
| OpenTelemetry | — | — | — | — | — | — | — | — | — | — | — | — |
| FIX TagValue | — | — | — | — | — | — | — | — | — | — | — | — |
| Generator | — | — | — | — | — | — | — | — | — | — | — | — |

<!-- delivery-matrix:end -->

## Evidence for exactly-once

Admission is a static check on connector configuration. This table records the real-process
evidence behind each admitted exactly-once composition and exactly what that evidence exercises.

| Composition | Mode | Evidence |
|---|---|---|
| Kafka → Delta Lake append (S3/S3A) | Cluster | `three_node_eo_join_kill9_soak`: repeated `kill -9` of the leader and followers inside checkpoints, and leaders frozen with `SIGSTOP` at the committed-index and sink-commit gates, then resumed after a successor commits; the stale leader must fence itself and exit. Frozen-input oracles require every output row exactly once. Emulated S3 (SeaweedFS). |
| Kafka → Iceberg append (REST + S3) | Cluster | `three_node_eo_join_kill9_soak`: the same join also writes to Iceberg through every `kill -9` and paused-leader round; the table's data files must hold every join pair exactly once, and every snapshot must belong to exactly one checkpoint. `iceberg_cluster_soak`: leader killed at the external commit gate and restarted; one snapshot per checkpoint and an exact data-file oracle. |
| Kafka → Delta Lake append | Embedded / single-node | `single_node_eo_join_kill9_soak`: repeated `kill -9` of the server inside checkpoints; frozen-input oracles require every output row exactly once. Emulated S3 (SeaweedFS). |
| Kafka → Kafka | Embedded / single-node | `crates/laminar-connectors/tests/kafka_exactly_once.rs`, `crates/laminar-db/tests/kafka_exactly_once_e2e.rs` |
| FIX → Delta Lake append | Embedded / single-node | `crates/laminar-db/tests/fix_source_exactly_once.rs` |

## Materialized view reads

A materialized view is not a sink, so the pipeline's delivery guarantee does not describe it.
Checkpoints restore view state together with source positions, so in every mode a committed view
holds each input row once, under at-least-once delivery as well. What a reader sees depends on the
read level (`ReadLevel` embedded, `laminar.read_level` over pgwire, `read_level` over HTTP):

| Read level | Embedded / single-node | Cluster |
|---|---|---|
| `latest` | Default. The latest completed cycle. Recovery can roll it back, so a later read can show less input than an earlier one. | Rejected: a node holds only its part of each view. |
| `committed` | Every view the statement reads, at one committed checkpoint. | Default. The same, on any node, from every node's checkpointed state. |
| `recent` | A committed read at a checkpoint that holds every record the views' sources had upstream when the read arrived. | The same, on any node. |

A committed read:

- equals the view's query over the input each source had delivered at that checkpoint;
- reads every view in one statement, a join or union of views included, at the same checkpoint;
- is never rolled back. With `min_epoch`, and on one pgwire connection, a read is never older than
  the one before;
- is as fresh as the checkpoint interval plus the time to commit a checkpoint.

A recent read gives read-your-writes through Kafka: a record the producer had acknowledged before
the read arrived is in the result, and later records may be too. When the read arrives it asks
each Kafka source behind the views it reads, through streams and views of views, how far its
topics have been written, then reads the newest committed checkpoint whose positions reach that
far. Transaction markers, and under the default `read_committed` isolation the records of open or
aborted transactions, are not waited for. When no checkpoint holds them yet, the read waits for
one, usually up to a checkpoint interval plus the commit, longer while a source lags behind its
topics, and fails after 30 seconds instead of returning an older checkpoint. With `[checkpoint]
recent_read_interval` set (`StreamCheckpointConfig::recent_read_interval_ms` embedded), a waiting
read brings the next periodic checkpoint forward to that long after the previous one ended; a
cluster node forwards the request to the leader, which alone starts checkpoints. The early
checkpoint is an ordinary periodic one: it commits exactly-once sinks, and waits out recovery
rounds and topology changes like any other. Only Kafka sources
with a topic list can report how far they have been written: a view whose input includes another
connector, a Kafka topic pattern, or rows pushed through the API refuses a recent read before
waiting. Reference tables are looked up, not waited for.

`SUBSCRIBE` to a view in a cluster delivers its rows at a committed checkpoint, then each later
checkpoint's change, each followed by a progress frame. Folding the frames up to a progress frame
gives the rows a committed read at that epoch returns.

A node fault starts a coordinated recovery round on every node, typically 10 to 50 seconds. During
it, a cluster node keeps serving committed data: pgwire and `POST /api/v1/sql` answer reads, and
open WebSocket subscriptions stay open and continue with the next checkpoint once the round ends.
Other SQL statements and the other HTTP routes, `/ready` included, answer 503 until it ends.
No checkpoint commits during the round, so a recent read issued then waits for it to end or fails
at its deadline.

A subscription ends when its node stops. To continue it without a gap, reconnect to another node
with `as_of_epoch` set to the last progress epoch: it restarts with the view's rows at that epoch,
which equal the rows already folded, then continues with the changes. While that epoch is still
retained the reconnect succeeds; otherwise it fails with 410 and the client starts again from the
newest checkpoint.

| Evidence | Mode | What it exercises |
|---|---|---|
| `three_node_eo_view_read_kill9_soak` | Cluster | A keyed view, a view of its rows without the key, an append view and a per-partition progress view over the Kafka input of `three_node_eo_join_kill9_soak`, through its `kill -9` and paused-leader rounds. A pgwire reader rotates across the nodes reading all four in one statement: each read must equal the views over the input prefix its progress rows show, with no gap or duplicate in any partition, the prefix never shrinks, the same epoch reads the same rows on every node, and no read is older than the session's floor. After each pgwire session the reader also reads the node over HTTP, checked by row counts and sums; neither surface may refuse a read during recovery. A second reader does the same at the `recent` level, and each of its reads must also hold, on every Kafka partition, the highest id the broker had acknowledged to the producer before the read was sent; with `recent_read_interval = "1s"` the leader starts the checkpoint those reads wait for early, on requests forwarded from every node. A WebSocket subscription to the keyed view must stay open through each fault on another node, moves to another gateway after each round, resumes at its last progress epoch whenever a connection ends, and must fold to the reader's rows at every epoch both saw, including each resumed snapshot. After the input stops, every node must read every produced row over pgwire and HTTP, and in a `recent` read without a floor. Emulated S3 (SeaweedFS). |
| `cluster_conformance` view cases | Cluster (in-process, three nodes) | Each view case read at the default committed level on another node and subscribed before any input, compared with batch SQL. |
| `cluster::recovery_round_tests::committed` | Cluster (two processes) | Views restored on a survivor after a node is lost, seeded by a topology migration, and handed over by a draining node. |
