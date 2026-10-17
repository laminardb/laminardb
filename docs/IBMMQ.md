# IBM MQ source and sink

LaminarDB reads from and writes to IBM MQ queues through the MQI, using IBM's own client library
(`libmqm_r`) through the `mqi` crate. The source gets messages under syncpoint and commits each
checkpoint's unit of work only after the checkpoint is durable. Poison messages follow the queue's
own `BOTHRESH` and `BOQNAME`. The sink puts under syncpoint and commits each unit of work before a
write returns. It is not an AMQP, JMS, or REST bridge.

## Before you start

IBM MQ support is the optional `ibmmq` build feature, off by default:

```bash
cargo build --release -p laminar-server --features ibmmq
```

Building needs no IBM software: the client library is loaded at run time with `dlopen`, so the
feature also builds on CI machines without MQ. To run, install an IBM MQ client yourself and make it
loadable. LaminarDB does not bundle or redistribute it:

- **Linux x86-64 and Windows:** the IBM MQ redistributable client
  (`public.dhe.ibm.com/ibmdl/export/pub/software/websphere/messaging/mqdev/redist/`), or a full MQ
  client or server installation.
- **macOS (x86-64, arm64):** the IBM MQ macOS Toolkit
  (`.../mqdev/mactoolkit/`).
- **Linux arm64, ppc64le, s390x:** the client of an IBM MQ installation for that platform.

Then either set `'client.library'` to the library file, or set `MQ_INSTALLATION_PATH` (the
connector looks for `lib64/libmqm_r.so`, `lib64/libmqm_r.dylib`, or `bin64/mqm.dll`), or put the
library on the platform loader path. One process loads one client library; a second source or sink
naming a different library fails at start. The IBM client runtime is supported by IBM under your
MQ license. The `mqi` Rust binding is a community crate that IBM does not support.

The source needs checkpointing and at-least-once delivery; a best-effort pipeline rejects it.

## What's supported

| Area | Supported | Rejected before any I/O |
|---|---|---|
| API | MQI through IBM's client (`MQCONNX`, `MQOPEN`, `MQGET`, `MQPUT`, `MQPUT1`, `MQCMIT`, `MQBACK`, `MQINQ`, message handles) | AMQP, JMS, REST, and MQ topics/subscriptions |
| Objects | Queues: local, alias, cluster, and (sink) remote definitions | Topics |
| Formats | `JSON`, `CSV` (several lines per message), and `RAW` (`value BYTEA` keeps bytes exactly; `value VARCHAR` requires UTF-8) | `AVRO`, `DEBEZIUM`, `FPML` |
| Messages | Complete messages up to `max.message.bytes`, including group members | Segmented messages (`MQMF_SEGMENT`), which would be partial records |
| Delivery | At-least-once with checkpointing; a persistent sink is durable at-least-once | Exactly-once, XA, and best-effort sources |
| Deployment | Embedded and single-node server. In cluster mode the sink runs on every node as an independent at-least-once writer, under the same rule as Kafka and NATS output | Cluster mode rejects the source: it is a singleton |
| Reconnect | Source: disabled. Sink: `disabled`, `qmgr`, or `any` | Source reconnect (a reconnect backs out the unit of work holding uncheckpointed messages) |
| TLS | `tls.cipher.spec`, key repository, certificate label, and `SSLPEER` filter on an explicit channel; a CCDT's channel definition owns its own TLS settings | TLS options combined with `ccdt.url` |

## Connecting

```sql
CREATE SOURCE payments (id BIGINT, amount DOUBLE) FROM IBMMQ (
    'queue.manager' = 'QM1',
    'channel' = 'APP.SVRCONN',
    'connection.name' = 'mq1.example.com(1414),mq2.example.com(1414)',
    'username' = 'laminar', 'password' = '${MQ_PASSWORD}',
    'tls.cipher.spec' = 'ANY_TLS13_OR_HIGHER',
    'tls.key.repository' = '/etc/laminardb/mq/client',
    'tls.peer.name' = 'CN=QM1,O=Example',
    'queue' = 'PAYMENTS.IN',
    'metadata.columns' = 'msg_id,correl_id,backout_count,put_timestamp,properties'
) FORMAT JSON;

CREATE SINK settled FROM payment_results INTO IBMMQ (
    'queue.manager' = 'QM1', 'ccdt.url' = 'file:///etc/laminardb/mq/ccdt.json',
    'username' = 'laminar', 'password' = '${MQ_PASSWORD}',
    'queue' = 'PAYMENTS.SETTLED', 'correlation.id.column' = 'request_id',
    'property.columns' = 'tenant'
) FORMAT JSON;
```

Raw bytes in both directions:

```sql
CREATE SOURCE frames (value BYTEA) FROM IBMMQ (
    'queue.manager' = 'QM1', 'channel' = 'APP.SVRCONN', 'connection.name' = 'mq1(1414)',
    'username' = 'laminar', 'password' = '${MQ_PASSWORD}', 'queue' = 'FRAMES.IN'
) FORMAT RAW;
CREATE SINK frames_out FROM passthrough INTO IBMMQ (
    'queue.manager' = 'QM1', 'channel' = 'APP.SVRCONN', 'connection.name' = 'mq1(1414)',
    'username' = 'laminar', 'password' = '${MQ_PASSWORD}', 'queue' = 'FRAMES.OUT'
) FORMAT RAW;
```

**Channel selection and precedence.** `channel` with `connection.name` builds an `MQCD` passed
in `MQCNO`. That overrides `ccdt.url`, which in turn overrides the client's environment
(`MQSERVER`, `MQCCDTURL`, `MQCHLLIB`/`MQCHLTAB`, `mqclient.ini`). Set one of the two options, or
neither to defer to the environment; setting both is rejected. A `connection.name` list is tried in
order by the client. `queue.manager` is the `MQCONNX` name: leave it out to accept whichever queue
manager the channel definition reaches.

**Reconnect.** The `reconnect` option sets the `MQCNO` reconnect option explicitly, so
`mqclient.ini`'s `DefRecon` does not apply implicitly. IBM's rule still holds:
`DefRecon=DISABLED` in `mqclient.ini` disables reconnection even when the application requests
it. With `qmgr` the client reconnects only to the same queue manager. With `any` it may reach a
different one. Messages in a unit of work that was open on the old queue manager are backed out
there, and this connector no longer consumes them. Sink reconnect is accepted but not tested
against a live queue manager; the tested recovery path is the engine restart that `disabled`
uses.

**Timeouts.** Blocking MQI calls run on one dedicated thread per connector, never on the async
runtime. `operation.timeout.ms` (default 30 s) bounds how long LaminarDB waits for one operation:
a poll, a commit, or one complete sink write including all of its units of work. An expired wait
ends the connector generation. It does not cancel an MQI call already running; that call finishes
on its thread, and the generation is then backed out and disconnected. Raise
`operation.timeout.ms` for very large sink batches. TCP connect timeouts come from the MQ client
(`TCP:Connect_Timeout` in `mqclient.ini`).

## Source

| Option | Default | Native field / API |
|---|---|---|
| `queue` (required) | | `MQOPEN` with `MQOO_INPUT_AS_Q_DEF`, `MQOO_INQUIRE`, `MQOO_FAIL_IF_QUIESCING` |
| `wait.interval.ms` | `100` | `MQGMO` `WaitInterval` for the first message of a poll (later gets in the poll don't wait) |
| `max.message.bytes` | `4194304` | Receive buffer; MQ's default `MAXMSGL` |
| `max.batch.bytes` | `16777216` | Payload bytes per poll |
| `max.uow.messages` | `10000` | Messages per unit of work (keep at or below the queue manager's `MAXUMSGS`) |
| `max.uow.bytes` | `67108864` | Payload bytes per unit of work |
| `on.decode.error` | `fail` | `fail`, or `backout-queue` to move the message to `BOQNAME` in the same unit of work |
| `convert` | `false` | `MQGMO_CONVERT` to UTF-8 (CCSID 1208) for convertible formats such as `MQSTR` |
| `metadata.columns` | none | See below |

Every get uses `MQGMO_SYNCPOINT | MQGMO_FAIL_IF_QUIESCING` explicitly, never the platform-dependent
default. Message properties are always requested in a message handle (`MQGMO_PROPERTIES_IN_HANDLE`),
so the body never carries an `MQRFH2` header the connector would have to guess at. By default the
body is delivered byte for byte with no character conversion; `convert = 'true'` is the only way to
get MQ to convert it.

### Metadata columns

`metadata.columns` appends, in this canonical order:

- `_msg_id`, `_correl_id`, `_group_id`: `FixedSizeBinary(24)`.
- `_msg_seq_number`, `_msg_flags`, `_msg_type`: Int32.
- `_format`: Utf8, trimmed.
- `_ccsid`, `_encoding` (the delivered payload's), `_persistence` (0 or 1), `_priority`: Int32.
- `_expiry`: Int32, the remaining expiry at retrieval in tenths of a second, `-1` for unlimited.
- `_backout_count`: Int32.
- `_reply_to_queue`, `_reply_to_queue_manager`, `_put_application_name`, `_user_identifier`:
  Utf8. The last two are the received context, as the queue manager recorded it.
- `_put_timestamp`: `PutDate` and `PutTime` (GMT, hundredths) as a millisecond timestamp, NULL if
  blank.
- `_properties`: a list of `{name, value}`. Values are strings and byte strings verbatim, numbers
  and booleans as text, and null as NULL.

`_backout_count` and `_expiry` change on redelivery; every other field is preserved.

## Sink

| Option | Default | Native field / API |
|---|---|---|
| `queue` (required) | | `MQOPEN` with `MQOO_OUTPUT` |
| `target.queue.manager` | | `MQOD` `ObjectQMgrName` |
| `persistence` | `persistent` | `MQMD` `Persistence`: `persistent`, `not-persistent`, or `as-queue-definition` |
| `priority` or `priority.column` | queue default | `MQMD` `Priority` (0 to 9) |
| `expiry` | unlimited | `MQMD` `Expiry` in tenths of a second |
| `message.id.column` | new id | `MQMD` `MsgId` from a Binary or Utf8 column (at most 24 bytes, zero-padded); NULL gets a new id (`MQPMO_NEW_MSG_ID`) |
| `correlation.id.column` | none | `MQMD` `CorrelId`, the same way |
| `reply.to.queue`, `reply.to.queue.manager` | | `MQMD` `ReplyToQ`, `ReplyToQMgr` |
| `message.format`, `ccsid` | `MQSTR` for text, `MQFMT_NONE` for binary; `1208` | `MQMD` `Format`, `CodedCharSetId` |
| `property.columns` | | Columns written as typed message properties (`MQSETMP`): Utf8 as string, Binary as byte string, Boolean, Int8 to Int64, Float32/64; NULL omits the property |
| `max.uow.messages` | `1000` | Messages per committed unit of work |

The sink sets no context: the queue manager records the connector's own user and application.
Columns mapped to descriptor fields or properties are left out of the payload.

## Delivery, recovery, and failures

**Source commit boundary.** All gets of a generation join the connection's one unit of work. A poll
gets only what it hands to the pipeline in the same call, with no read-ahead, so the unit of work
holds exactly the handed-off messages plus any poison moves. Each batch carries its own cursor, so
polling between checkpoints never pauses. When a checkpoint barrier captures the source, the cursor
records the number of messages in the unit of work, and intake pauses: polls return nothing until
that checkpoint is durable. On the durable-commit notification the source issues
`MQCMIT` and resumes. A cursor that doesn't cover the whole unit of work cannot be committed; a
unit of work commits as a whole, and partial commits are refused. If a checkpoint fails, the next
one captures the same unit of work.

**The pause costs latency.** Intake waits from barrier capture to the durable-commit notification
once per checkpoint, so new messages wait on the queue for roughly the time it takes to persist a
checkpoint. Intake also pauses when `max.uow.messages`, `max.uow.bytes`, or the queue manager's
`MAXUMSGS` (`MQRC_SYNCPOINT_LIMIT_REACHED`) is reached, and resumes after the next commit. Size these
above one checkpoint interval of traffic. A source with only manual checkpoints pauses until the
next manual checkpoint.

**Size the recovery log for the checkpoint interval.** The source's unit of work stays open for one
checkpoint interval. When the queue manager's recovery log fills, it rolls back the long-running
unit of work that pins it (`AMQ7469` in its error log), and the source's next get or `MQCMIT`
returns `MQRC_BACKED_OUT`. The generation then ends and the messages are redelivered after a
restart. Keep the checkpoint interval well below the time the queue manager's total log traffic
takes to fill its primary and secondary log extents. In the benchmark below, 16 KiB messages at
1,000/s with a 2-second interval filled the developer image's default log (five 16 MiB extents); a
300 ms interval did not.

**Backout, never commit, on exit.** On distributed platforms `MQDISC` commits an open unit of work.
Every exit path backs out explicitly first: close, failure, drop, and panic unwinding on the worker
thread. A killed process loses its connection, and the queue manager backs out. Either way, every
uncheckpointed message returns to the queue with `BackoutCount` incremented, and the restarted
pipeline reads it again (duplicates, no loss). A poll that is cancelled after it got messages
leaves those messages in the unit of work without handing them off. It taints the unit of work,
which then never commits, so the generation ends and the messages are backed out.

**Poison messages.** At start the source reads `BOTHRESH` and `BOQNAME` once (it needs `+inq` on
the queue) and checks that `BOQNAME` opens for output and resolves to the same queue manager. A
message whose `BackoutCount` reaches `BOTHRESH` is `MQPUT1`'d to `BOQNAME` under the same syncpoint,
with its ids, format, CCSID, encoding, persistence, priority, and properties. The move commits or
backs out together with the checkpoint. With `on.decode.error = 'backout-queue'`, messages the
codec rejects and messages over `max.message.bytes` move the same way. An oversized message is
received whole for the move, matched by `MsgId` and never truncated. Startup fails when:

- `BOTHRESH` is set without `BOQNAME`;
- `BOQNAME` resolves to another queue manager (a remote definition can't move atomically);
- `backout-queue` is requested without a usable route.

The connector never falls back to the dead-letter queue.

**`BackoutCount` counts against neighbours.** A backout increments the count of every message in
the unit of work: an engine restart caused by something else, such as a sink outage, raises the
counts of all uncheckpointed messages. After `BOTHRESH` such restarts, healthy messages are moved
too. Set `BOTHRESH` well above the restarts you expect, or 0 to disable automatic moves and rely on
`on.decode.error`.

| What goes wrong | What happens |
|---|---|
| The process is killed | The queue manager backs out; the restarted pipeline reads the messages again |
| The connection breaks | The generation ends; the server's supervisor restarts the pipeline from its last checkpoint |
| A message doesn't decode | `fail`: the source stops, the message is backed out and redelivered (and eventually moved by `BOTHRESH`). `backout-queue`: moved to `BOQNAME` |
| A message is over `max.message.bytes` | `fail`: the source stops and the message stays on the queue untouched. `backout-queue`: moved whole |
| A segmented message arrives | The source stops; segments are never delivered as records |
| `MQCMIT` returns `MQRC_BACKED_OUT`, or its outcome is unknown | The generation ends; the messages may be redelivered |
| The queue manager rolls back the unit of work to release log space | The next get or `MQCMIT` returns `MQRC_BACKED_OUT`; the generation ends and the messages are redelivered |
| A sink put fails | Its unit of work is backed out; with an earlier unit of the same batch already committed, the outcome is reported as unknown. The sink generation ends and replay may duplicate |

**Sink durability.** A write returns after `MQCMIT`, so persistent messages are logged by the queue
manager. `not-persistent` and `as-queue-definition` sinks are declared ephemeral to the engine,
because the connector does not check the queue's `DEFPSIST`.

## Provisioning

The connector never defines, alters, or deletes MQ objects. Provision them with MQSC:

```
DEFINE QLOCAL('PAYMENTS.IN') DEFPSIST(YES) BOTHRESH(10) BOQNAME('PAYMENTS.BACKOUT') MAXDEPTH(500000)
DEFINE QLOCAL('PAYMENTS.BACKOUT') DEFPSIST(YES)
DEFINE QLOCAL('PAYMENTS.SETTLED') DEFPSIST(YES)
SET AUTHREC PROFILE('PAYMENTS.IN') OBJTYPE(QUEUE) PRINCIPAL('laminar') AUTHADD(GET,INQ,BROWSE)
SET AUTHREC PROFILE('PAYMENTS.BACKOUT') OBJTYPE(QUEUE) PRINCIPAL('laminar') AUTHADD(PUT)
SET AUTHREC PROFILE('PAYMENTS.SETTLED') OBJTYPE(QUEUE) PRINCIPAL('laminar') AUTHADD(PUT)
SET AUTHREC OBJTYPE(QMGR) PRINCIPAL('laminar') AUTHADD(CONNECT)
```

The runtime user needs connect authority. The source needs `+get` and `+inq` on its queue and `+put`
on `BOQNAME`, and the sink needs `+put` on its target. No administrative authority is required, and
no set-context authority: the connector passes no context.

## Monitoring

Every metric has one `source` or `sink` label (the catalog source name, or the sink's queue name).

| Metric | Meaning |
|---|---|
| `ibmmq_source_messages_total`, `_bytes_total`, `_rows_total` | Messages and bytes got under syncpoint; rows decoded |
| `ibmmq_source_redelivered_total` | Messages got with a nonzero `BackoutCount` |
| `ibmmq_source_backout_moves_total` | Messages moved to `BOQNAME` |
| `ibmmq_source_committed_total` | Messages committed after durable checkpoints |
| `ibmmq_source_uncommitted` | Messages in the open unit of work |
| `ibmmq_sink_put_total`, `_bytes_total`, `_commits_total` | Messages, bytes, and units of work committed |
| `ibmmq_sink_backouts_total`, `_ambiguous_total` | Backed-out writes and unknown commit outcomes |

## Tested behaviour

Tests ran on 2026-10-17 on an arm64 macOS client (IBM MQ macOS Toolkit 10.0.1.0) against a
Rosetta-emulated amd64 queue manager (IBM MQ Advanced for Developers 9.4.0.26). Treat them as
development-host evidence. They don't certify a platform, and they aren't Linux CI results; the
live suites are a manual lane because the server image is amd64-only and licensed for development.

Connector tests (`crates/laminar-connectors/tests/ibmmq_integration.rs`) cover:

- commit only at a durable checkpoint, with the seal;
- backout on close instead of commit;
- incremented `BackoutCount`;
- atomic poison moves that keep descriptors and properties;
- decode and oversize policies;
- unusable and remote backout queues;
- `MQMD` and property round trips;
- explicit CCSID conversion;
- sink units of work and `MAXMSGL` refusal;
- bad credentials;
- CCDT;
- a cancelled poll that taints its unit of work.

SQL tests (`crates/laminar-db/tests/ibmmq_e2e.rs`) cover JSON and byte-exact binary round trips,
backout-and-replay without loss across a restart, and metadata columns across a restart.
`crates/laminar-server/tests/ibmmq_kill.rs` hard-kills the standalone server while a publisher
runs; every published message reached the output, with duplicates.
`crates/laminar-server/tests/cluster_soak/messaging_sinks.rs` runs the sink in a three-node
cluster: a Kafka source (Redpanda) feeds a projection, every node puts its share of the rows to
one local queue, and checkpoints go to a shared S3 store (SeaweedFS). The leader was hard-killed
twice and each follower once, every time while paused inside a checkpoint after the sinks
flushed. Every node put messages before the first kill and after it rejoined, and each put
29–40% of the rows in the 30 s steady phase that followed. Of 71,726 messages none was missing
from the queue; 4,306 arrived more than once. TLS, sink reconnect, and
queue-manager failover were not tested against a live queue manager.

## Performance

`crates/laminar-db/tests/messaging_bench.rs` (ignored; run in release) drives an embedded
`source → stream → sink` pipeline at a fixed offered rate for 12 seconds per cell. The publisher
and the output consumer are IBM MQ clients in the same process. Latency runs from each message's
scheduled send time to its receipt from the output queue, so it includes both queue hops, the
pipeline, the sink's `MQCMIT`, and any queueing behind a slow pipeline. Every message is
persistent; delivery is at-least-once with local checkpoints. Each message is one row, so rows/s
equals messages/s.

Measured on 2026-10-17 with the arm64 macOS client against the Rosetta-emulated amd64 queue
manager described above. Emulation dominates these numbers: they show behaviour, not capacity.
Rerun on target hardware before sizing a deployment.

| Codec | Bytes | Checkpoint | Delivered msg/s | p50 ms | p99 ms | p99.9 ms | Max ms |
|---|---|---|---|---|---|---|---|
| JSON | 128 | 300 ms | 995 | 37 | 228 | 251 | 263 |
| JSON | 128 | 2 s | 1,001 | 26 | 194 | 229 | 240 |
| JSON | 1,024 | 300 ms | 1,001 | 29 | 207 | 240 | 252 |
| JSON | 1,024 | 2 s | 1,000 | 27 | 228 | 324 | 336 |
| JSON | 16,384 | 300 ms | 993 | 37 | 263 | 307 | 319 |
| JSON | 16,384 | 2 s | faulted after 1,532 messages: log full, `MQRC_BACKED_OUT` | | | | |
| raw | 128 | 300 ms | 998 | 52 | 280 | 309 | 319 |
| raw | 128 | 2 s | 1,000 | 25 | 166 | 247 | 259 |
| raw | 1,024 | 300 ms | 1,000 | 55 | 599 | 707 | 719 |
| raw | 1,024 | 2 s | 1,001 | 29 | 222 | 293 | 305 |
| raw | 16,384 | 300 ms | 1,000 | 35 | 263 | 370 | 382 |
| raw | 16,384 | 2 s | faulted after 1,495 messages: log full, `MQRC_BACKED_OUT` | | | | |

Offered rate: 1,000 messages/s. Peak resident memory of the whole test process ranged from 40 to
150 MiB, and CPU time was 2.3 to 2.9 seconds per 12-second cell. A shorter interval pauses intake
more often, which shows in its median. Pushed harder (JSON, 1 KiB, 300 ms), the pipeline delivered
about 2,600 messages/s when offered 3,000, with latency growing with the backlog (p50 1.4 s).

## FpML and other formats

The source decodes on its worker thread through the shared `RecordDeserializer` contract, and a
message may become zero, one, or several rows, with metadata repeated per row. A future FpML
integration adapts the FpML `FormatDecoder` to that contract, gets the complete message body
(byte-preserving by default) with its descriptor and properties, and uses the same
`fail`/`backout-queue` lifecycle. FpML is not supported by this connector today.
