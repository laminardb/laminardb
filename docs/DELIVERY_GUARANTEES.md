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
| Kafka → Delta Lake append (S3/S3A) | Cluster | `three_node_eo_join_kill9_soak`: repeated `kill -9` of the leader and followers inside checkpoints; frozen-input oracles require every output row exactly once. Emulated S3 (MinIO). |
| Kafka → Iceberg append (REST + S3) | Cluster | `iceberg_cluster_soak`: leader killed at the external commit gate and restarted; one snapshot per checkpoint and an exact data-file oracle. |
| Kafka → Delta Lake append | Embedded / single-node | `single_node_eo_join_kill9_soak`: repeated `kill -9` of the server inside checkpoints; frozen-input oracles require every output row exactly once. Emulated S3 (MinIO). |
| Kafka → Kafka | Embedded / single-node | `crates/laminar-connectors/tests/kafka_exactly_once.rs`, `crates/laminar-db/tests/kafka_exactly_once_e2e.rs` |
| FIX → Delta Lake append | Embedded / single-node | `crates/laminar-db/tests/fix_source_exactly_once.rs` |
