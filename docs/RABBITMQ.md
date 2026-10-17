# RabbitMQ source and sink

LaminarDB reads from and writes to RabbitMQ over AMQP 0-9-1 with the `lapin` client. The source
consumes one classic or quorum queue and acknowledges a message only after the checkpoint that
contains it is durable. The sink publishes to one exchange with publisher confirms and mandatory
routing. Routing, TTL, dead-lettering, delivery limits, priorities, and queue limits stay with the
broker: exchanges and bindings route, and policies or declaration arguments own the rest.

## Before you start

RabbitMQ support is the optional `rabbitmq` build feature, off by default. It is pure Rust and needs
no native libraries:

```bash
cargo build --release -p laminar-server --features rabbitmq
```

The source needs checkpointing and at-least-once delivery (`[server] delivery = "at_least_once"`
or `LaminarConfig::delivery_guarantee`). A best-effort pipeline rejects it before connecting,
because without checkpoints nothing would ever acknowledge its messages.

## What's supported

| Area | Supported | Rejected before any I/O |
|---|---|---|
| Protocol | AMQP 0-9-1, tested against RabbitMQ 4.3.6 with `lapin` 4.12.2 | AMQP 1.0 and the RabbitMQ stream protocol |
| Queues (source) | Classic and quorum queues, existing or declared on request | Stream queues: declared ones are rejected at configuration; an existing one is detected by its `x-stream-offset` header and stops the source before anything is handed off |
| Formats | `JSON`, `CSV` (a message may hold several lines), and `RAW` (`value BYTEA` keeps bytes exactly; `value VARCHAR` requires UTF-8) | `AVRO`, `DEBEZIUM`, `FPML` |
| Delivery | At-least-once with checkpointing; a persistent sink is durable at-least-once | Exactly-once (neither direction is certified) and best-effort sources |
| Deployment | Embedded and single-node server. In cluster mode the sink runs on every node as an independent at-least-once writer, under the same rule as Kafka and NATS output | Cluster mode rejects the source: it is a singleton |
| TLS | `amqps://` with peer and hostname verification against the Web PKI roots plus an optional CA bundle, and mutual TLS with a PEM certificate and PKCS#8 key | Plain `amqp://` with TLS options |

## Reading a queue

```sql
CREATE SOURCE orders (id BIGINT, amount DOUBLE, customer VARCHAR) FROM RABBITMQ (
    'url' = 'amqps://broker.example.com:5671/sales',
    'username' = 'laminar',
    'password' = '${RABBITMQ_PASSWORD}',
    'queue' = 'orders',
    'prefetch.count' = '5000',
    'metadata.columns' = 'routing_key,redelivered,message_id,headers'
) FORMAT JSON;
```

Raw bytes, preserved exactly (including non-UTF-8 payloads):

```sql
CREATE SOURCE blobs (value BYTEA) FROM RABBITMQ (
    'url' = 'amqp://127.0.0.1:5672/%2f', 'username' = 'laminar',
    'password' = '${RABBITMQ_PASSWORD}', 'queue' = 'blobs'
) FORMAT RAW;
```

Credentials belong in `${ENV}` references: the durable catalog refuses literal secrets, including a
password inside `url`.

### Source options

| Option | Default | Native property / API |
|---|---|---|
| `url` (required) | | AMQP URI; `heartbeat`, `frame_max`, `channel_max`, `connection_timeout`, and `auth_mechanism` query parameters are passed to the client. Several comma-separated URLs are failover endpoints of one broker or cluster, tried in order at each start; they must share a virtual host. |
| `username`, `password` | URI userinfo, else `guest` | SASL PLAIN credentials; conflict with userinfo in `url` |
| `connection.name` | `laminardb-rabbitmq-source` | Client-provided connection name shown by the broker |
| `connect.timeout.ms` | `10000` | Per-endpoint connect bound (or the URI's `connection_timeout`, not both) |
| `tls.ca.location`, `tls.cert.location`, `tls.key.location` | | PEM CA bundle added to the trusted roots; PEM client certificate and PKCS#8 key for mutual TLS |
| `queue` (required) | | Queue to consume |
| `queue.declare` | `false` | `true` declares the queue idempotently (`queue.declare` with `durable`, `x-queue-type`, and `queue.arguments`); `false` only checks that it exists (passive declare) |
| `queue.durable`, `queue.type`, `queue.arguments` | `true`, broker default, none | Declaration-only settings; rejected unless `queue.declare = 'true'` |
| `queue.bind.exchange`, `queue.bind.routing.keys`, `queue.bind.arguments` | | Idempotent `queue.bind` calls at start |
| `prefetch.count` | `1000` | `basic.qos` prefetch (1 to 65535) |
| `consumer.tag`, `consumer.exclusive`, `consumer.arguments` | broker tag, `false`, none | `basic.consume` arguments, for example `{"x-priority": 10}` |
| `max.message.bytes` | `16777216` | Largest accepted body (RabbitMQ's default `max_message_size`) |
| `max.batch.bytes` | `33554432` | Payload bytes decoded per batch |
| `on.decode.error` | `fail` | `fail` stops the source; `reject` sends `basic.reject` without requeue |
| `metadata.columns` | none | See below |

`queue.arguments`, `queue.bind.arguments`, and `consumer.arguments` are JSON objects of scalars:
numbers become signed 64-bit integers (or doubles), strings become long strings, and booleans stay
booleans.

### Metadata columns

`metadata.columns` appends typed columns after the payload, in this order no matter how you list
them: `_exchange`, `_routing_key` (Utf8), `_redelivered` (Boolean), `_delivery_count` (Int64, the
`x-delivery-count` header quorum queues maintain), `_message_id`, `_correlation_id` (Utf8),
`_timestamp` (the AMQP `timestamp`, whole seconds, as a millisecond timestamp), `_content_type`,
`_content_encoding` (Utf8), `_delivery_mode`, `_priority` (UInt8), `_expiration` (the per-message
TTL property in milliseconds, as text), `_reply_to`, `_type`, `_user_id`, `_app_id` (Utf8), and
`_headers`, a list of `{key, value}` where the value bytes are strings and byte arrays verbatim,
numbers and booleans as text, timestamps as seconds, decimals as `12345e-2`, nested tables and
arrays as JSON, and `void` as NULL. Every value is what the broker delivered, so a redelivery
reproduces it; no receive time is exposed. When a codec turns one message into several rows (CSV
lines), each row repeats that message's metadata.

## Writing to an exchange

```sql
CREATE SINK alerts FROM high_value INTO RABBITMQ (
    'url' = 'amqps://broker.example.com:5671/sales',
    'username' = 'laminar', 'password' = '${RABBITMQ_PASSWORD}',
    'exchange' = 'alerts', 'routing.key.column' = 'region',
    'message.id.column' = 'alert_id', 'header.columns' = 'tenant'
) FORMAT JSON;

CREATE SINK copied FROM passthrough INTO RABBITMQ (
    'url' = 'amqp://127.0.0.1:5672/%2f', 'username' = 'laminar',
    'password' = '${RABBITMQ_PASSWORD}', 'routing.key' = 'blobs.out'
) FORMAT RAW;
```

Columns used as the routing key or a message property are left out of the payload, so `FORMAT RAW`
can carry one binary or text column alongside them.

### Sink options

| Option | Default | Native property / API |
|---|---|---|
| connection options | | As for the source; the default connection name is `laminardb-rabbitmq-sink` |
| `exchange` | `""` (default exchange) | Target exchange; checked with a passive declare at open |
| `exchange.declare`, `exchange.type`, `exchange.durable` | `false`, , `true` | Idempotent `exchange.declare` |
| `routing.key` or `routing.key.column` | one is required | Publish routing key (Utf8 column, at most 255 bytes, no NULLs) |
| `mandatory` | `true` | `mandatory` publish flag; a returned message fails the write |
| `delivery.mode` | `persistent` | `delivery-mode` 2 or 1 |
| `content.type` | by format | `application/json`, `text/csv`, `text/plain; charset=utf-8`, or `application/octet-stream` |
| `content.encoding`, `reply.to`, `type`, `app.id` | | Literal message properties |
| `message.id.column`, `correlation.id.column` | | Utf8 columns for `message-id` and `correlation-id`; NULL omits the property |
| `priority` or `priority.column` | | `priority` (0 to 255) |
| `expiration.ms` | | `expiration`, the broker-enforced per-message TTL |
| `header.columns` | | Columns written as typed headers: Utf8 and Binary as long strings, Boolean, Int8 to Int64 and UInt8 to UInt32 as the matching AMQP integer, Float32/64 as float/double; NULL omits the header |
| `max.pending`, `max.pending.bytes` | `4096`, `67108864` | Publishes and payload bytes awaiting a broker confirm |
| `confirm.timeout.ms` | `30000` | Bound on waiting for confirms within one write or flush |

## Delivery, recovery, and failures

**Source acknowledgement boundary.** The consumer uses manual acknowledgements and a finite
`basic.qos` prefetch. Each poll hands deliveries to the pipeline in delivery-tag order without
acknowledging them. When a checkpoint captures the source, its cursor records this connection's
generation and the highest handed-off delivery tag. Only after that checkpoint is durable does the
source send one `basic.ack(multiple)` for that tag. Delivery tags increase on a channel, and
deliveries are handed off in order, so every outstanding tag at or below it is a message the
committed checkpoint contains. Messages rejected by `on.decode.error = 'reject'` were settled
individually and are not outstanding. The cursor tag is always a handed-off tag, so the source never
acknowledges an already-settled tag; the broker would close the channel if it did. A cursor from an
earlier connection generation acknowledges nothing: its channel is gone, and the broker requeued its
deliveries when it closed.

**Failure.** Any channel or connection loss, consumer cancellation, decode failure under `fail`, or
acknowledgement failure ends the source generation. The broker requeues every unacknowledged
delivery, and the engine restarts from the last durable checkpoint, so messages handed off after it
are delivered again (`_redelivered` is true). Duplicates are expected; loss is not. The client's own
automatic recovery is deliberately off, because delivery tags belong to one channel.

| What goes wrong | What happens |
|---|---|
| The process is killed | The broker requeues unacknowledged deliveries; the restarted pipeline reads them again |
| The connection or channel drops, or the broker restarts | The generation ends; the server's supervisor restarts the pipeline from its last checkpoint, trying the `url` endpoints in order |
| A message doesn't decode | `fail`: the source stops and the message is redelivered after restart. `reject`: the broker dead-letters it through the queue's dead-letter exchange, or discards it if there is none |
| A message is over `max.message.bytes` | The same as a decode failure |
| A checkpoint fails | Nothing is acknowledged; the next checkpoint covers the same deliveries |
| Checkpoints stall | Unacknowledged deliveries reach `prefetch.count` and the broker stops delivering. A delivery unacknowledged for `consumer_timeout` (30 minutes by default) closes the channel, which ends the generation |
| The sink's publish is nacked, returned, or its confirm is lost | The sink generation ends with the outcome classified (definite failure or unknown outcome); replay from the last checkpoint may publish duplicates |

**Sizing.** Unacknowledged deliveries are bounded by `prefetch.count`, and every one of them waits
for a checkpoint. Throughput is therefore at most about `prefetch.count` messages per checkpoint
interval plus commit latency: with 1-second checkpoints, `prefetch.count = 5000` allows roughly
5,000 messages per second. Client memory for buffered deliveries is at most `prefetch.count` times
the broker's `max_message_size`. AMQP's prefetch is count-only; RabbitMQ does not implement
`prefetch_size`. Keep the checkpoint interval far below `consumer_timeout`.

**Poison messages and neighbours.** Quorum queues count deliveries (`x-delivery-count`) and, from
RabbitMQ 4.0, apply a default delivery limit of 20, after which the message is dead-lettered or
dropped. Every restart redelivers all of the uncommitted messages. Their counts rise together, so
size the limit well above the number of restarts you expect. `on.decode.error = 'reject'` only asks
the broker to dead-letter the message. It does not prove durable dead-letter storage. For that,
use a quorum queue with `dead-letter-strategy: at-least-once` and `overflow: reject-publish`, and
provision the dead-letter exchange and queue yourself.

**Sink durability.** A confirm means the broker accepted the message for every queue it routed to.
It is durable only when the message is persistent (the default) and the routed queues are durable,
and it survives broker node loss only for replicated (quorum) queues. AMQP gives no way to check this
through the connection, so the connector cannot verify it; check it when you provision. A
`delivery.mode = 'transient'` sink is declared ephemeral to the engine. With
`mandatory = 'false'` the broker confirms unroutable messages and silently discards them.

## Provisioning and policies

The connector uses existing objects by default and never deletes or redeclares a different
definition (a conflicting declaration fails at start with `406 PRECONDITION_FAILED`). Mutable
settings belong in policies, which apply without redeploying the connector:

```bash
rabbitmqctl declare queue --vhost=sales name=orders queue_type=quorum durable=true
rabbitmqctl set_policy --vhost sales orders-dlx '^orders$' \
  '{"dead-letter-exchange":"orders.dlx","delivery-limit":50,"dead-letter-strategy":"at-least-once","overflow":"reject-publish"}' \
  --apply-to quorum_queues
rabbitmqctl set_permissions --vhost sales laminar '^$' '^(alerts)$' '^orders$'
```

The runtime user needs read permission on the consumed queue (and configure permission only if it
declares or binds), and write permission on the target exchange. It never needs administrator
rights. `queue.type` and `x-max-priority` are declaration-only, so use `queue.declare` or provision
them in advance. Use policies for TTL, length limits, dead-lettering, and delivery limits.
Arguments sent at declaration take precedence over policies, so don't put policy-managed keys in
`queue.arguments`.

## Monitoring

Every metric has one `source` or `sink` label (the catalog source name, or the sink's connection
name), never queue, routing-key, or message identifiers.

| Metric | Meaning |
|---|---|
| `rabbitmq_source_messages_total`, `_bytes_total`, `_rows_total` | Deliveries, payload bytes, and rows handed to the pipeline |
| `rabbitmq_source_redelivered_total` | Handed-off deliveries flagged as redelivered |
| `rabbitmq_source_acked_total` | Deliveries acknowledged after a durable checkpoint |
| `rabbitmq_source_rejected_total` | Deliveries rejected by `on.decode.error = 'reject'` |
| `rabbitmq_source_unacked` | Handed-off deliveries awaiting a checkpoint |
| `rabbitmq_sink_published_total`, `_bytes_total`, `_confirmed_total` | Publishes, bytes, and broker confirms |
| `rabbitmq_sink_nacked_total`, `_returned_total`, `_ambiguous_total` | Nacks, unroutable returns, and lost outcomes |
| `rabbitmq_sink_pending_confirms`, `_pending_bytes` | Publishes awaiting a confirm |

## Tested behaviour

Tests ran on 2026-10-17 against RabbitMQ 4.3.6 (single node, and a three-node cluster from
`tests/docker/rabbitmq-cluster-compose.yml`) on an arm64 macOS development host. The same suites
are wired to run in CI's `rabbitmq-integration` job.

`crates/laminar-connectors/tests/rabbitmq_integration.rs` covers:

- acknowledgement only on a durable commit, with repeated, stale, and empty notifications;
- the prefetch bound;
- decode failures that stop the source and redeliver;
- rejection through a dead-letter policy;
- a live policy change while consuming;
- incompatible and missing queues;
- stream-queue rejection;
- a forced connection close;
- confirmed, mandatory, and priority publishing with native properties;
- unroutable returns;
- a missing exchange;
- pipelined confirms;
- TLS: trusted CA, hostname verification, and a required client certificate, each rejected when
  missing or wrong.

`crates/laminar-db/tests/rabbitmq_e2e.rs` covers SQL JSON and byte-exact binary round trips,
redelivery after a restart without loss, metadata columns across a restart, and the best-effort
rejection.

`crates/laminar-server/tests/rabbitmq_kill.rs` runs the standalone server with a confirmed
publisher and an independent oracle. Every scenario reached the output with no published message
missing:

| Scenario | Queues | Messages | Faults | Duplicates |
|---|---|---|---|---|
| `SIGKILL` of the server every 6 s | classic | 19,997 | 7 | 1,014 |
| `SIGKILL` of the server every 6 s | quorum | 21,885 | 8 | 811 |
| `docker restart` of the broker every 13 s | classic | 11,999 | 3 | 142 |
| `docker kill` of the connected cluster node | quorum (3 replicas) | 10,600 | 1 | 5 |

In the node-loss run the restarted pipeline reached the next `url` endpoint and caught up in 19 s.

`crates/laminar-server/tests/cluster_soak/messaging_sinks.rs` runs the sink in a three-node
cluster: a Kafka source (Redpanda) feeds a projection, every node publishes its share of the rows
to one classic queue, and checkpoints go to a shared S3 store (SeaweedFS). The leader was
hard-killed twice and each follower once, every time while paused inside a checkpoint after the
sinks flushed. Every node published before the first kill and after it rejoined, and each
published 25–40% of the rows in the 30 s steady phase that followed. Of 69,639 messages none was
missing from the queue; 4,010 arrived more than once. This leg runs weekly in the `Checkpoint
fault and soak` workflow.
Those results hold for durable, persistent messages on these queue types. They do not extend to
other queue types, the stream protocol, or broker versions.

**Broker outages and the restart budget.** A lost broker ends the pipeline, and the server's
supervisor restarts it from the last checkpoint. The default budget is 5 restarts in 60 seconds,
with backoff capped at 30 seconds. An outage that outlasts it leaves the pipeline faulted until
someone restarts it. For brokers that may be down longer, widen `[supervision]` (the broker-restart
test uses `max_restarts = 100`). Embedded databases don't supervise by default; call
`enable_supervision` or restart the pipeline yourself.

## Performance

`crates/laminar-db/tests/messaging_bench.rs` (ignored; run in release) drives an embedded
`source → stream → sink` pipeline at a fixed offered rate for 12 seconds per cell. A confirmed
publisher and the output consumer run in the same process. Latency runs from each message's
scheduled send time to its receipt from the output queue, so it includes both broker hops, the
pipeline, the sink's publisher confirms, and any queueing behind a slow pipeline. Messages are
persistent on durable classic queues; delivery is at-least-once with local checkpoints. Each
message is one row, so rows/s equals messages/s.

Measured on 2026-10-17 against single-node RabbitMQ 4.3.6 in Docker on an arm64 macOS laptop. These
are development-host numbers; rerun on target hardware before sizing a deployment.

| Codec | Bytes | Checkpoint | Delivered msg/s | p50 ms | p99 ms | p99.9 ms | Max ms |
|---|---|---|---|---|---|---|---|
| JSON | 128 | 300 ms | 5,004 | 7.7 | 23.6 | 39.9 | 45.0 |
| JSON | 128 | 2 s | 5,002 | 7.4 | 22.6 | 36.0 | 42.7 |
| JSON | 1,024 | 300 ms | 5,003 | 7.7 | 21.0 | 33.0 | 40.1 |
| JSON | 1,024 | 2 s | 5,001 | 7.6 | 22.0 | 35.1 | 41.3 |
| JSON | 16,384 | 300 ms | 5,003 | 8.3 | 23.4 | 31.4 | 38.7 |
| JSON | 16,384 | 2 s | 5,001 | 8.1 | 24.3 | 39.8 | 43.8 |
| raw | 128 | 300 ms | 5,003 | 7.8 | 22.5 | 33.9 | 41.0 |
| raw | 128 | 2 s | 5,003 | 7.6 | 22.1 | 37.2 | 46.7 |
| raw | 1,024 | 300 ms | 5,003 | 7.7 | 20.7 | 33.0 | 40.7 |
| raw | 1,024 | 2 s | 5,002 | 7.6 | 25.9 | 38.6 | 42.8 |
| raw | 16,384 | 300 ms | 5,002 | 8.1 | 23.8 | 38.3 | 44.8 |
| raw | 16,384 | 2 s | 5,001 | 7.9 | 24.1 | 46.7 | 56.0 |

Offered rate: 5,000 messages/s; every message arrived. Peak resident memory of the whole test
process rose from 79 to 99 MiB across the matrix, and CPU time was 6.4 to 11.9 seconds per
12-second cell. Pushed harder (JSON, 1 KiB, 300 ms), 15,000 messages/s offered were all delivered
(p50 9.9 ms, p99 29.7 ms). At 30,000 offered, the pipeline delivered about 24,900 messages/s with
latency growing to about 2 seconds as the backlog built.

## FpML and other formats

The source decodes through the shared `RecordDeserializer` contract, and a message may become zero,
one, or several rows; metadata is repeated per row. A future FpML integration adapts the FpML
`FormatDecoder` to that contract, gets the complete message body and metadata, and uses the same
acknowledgement and `reject` lifecycle. FpML is not supported by these connectors today.
