# SQL Dialect Reference

> Practical guide to LaminarDB's SQL dialect: gotchas, working patterns, tested examples.

LaminarDB uses [Apache DataFusion](https://datafusion.apache.org/) as its SQL engine. While most standard SQL works, streaming-specific operations have differences from other streaming databases (Flink SQL, ksqlDB, etc.). This reference documents patterns that are **confirmed working** in LaminarDB embedded mode.

---

## Quick Reference

| What you might try | What actually works | Notes |
|---|---|---|
| `TUMBLE_START(ts, ...)` | `tumble(ts, ...)` | Returns `Timestamp(Millisecond)` directly |
| `FIRST(price)` / `LAST(price)` | `first_value(price)` / `last_value(price)` | DataFusion aggregate function names |
| `ts - INTERVAL '10' SECOND` | `ts - INTERVAL '10' SECOND` | Native on `TIMESTAMP` columns since v0.20.1 |
| `CASE WHEN ... THEN vol ELSE 0` | `CASE WHEN ... THEN vol ELSE CAST(0 AS BIGINT)` | ELSE branch must match column type |
| `SHOW TABLES` | `SHOW SOURCES` / `SHOW STREAMS` | LaminarDB uses source/stream terminology |
| `date_trunc('hour', ts)` | `date_trunc('hour', ts)` | Available via DataFusion 52 built-ins |
| `UNNEST(array_col)` | `UNNEST(array_col)` | Available via DataFusion 52 built-ins |

---

## Sources

Columns can be omitted for metadata-capable or built-in source formats. Creation
resolves a validated contract, and durable deployments commit it before activation.
Explicit columns remain authoritative. See [connector schema resolution](SCHEMA_RESOLUTION.md)
for format policies, query-derived sinks, sampling and recovery/migration behavior.

Create data sources using `CREATE SOURCE`. Event-time columns must be
declared as `TIMESTAMP`. LaminarDB uses Arrow `Timestamp(_)` internally
at any precision and rescales to milliseconds via the Arrow cast kernel.

```sql
CREATE SOURCE trades (
    account_id VARCHAR NOT NULL,
    symbol     VARCHAR NOT NULL,
    side       VARCHAR NOT NULL,
    price      DOUBLE NOT NULL,
    volume     BIGINT NOT NULL,
    order_ref  VARCHAR NOT NULL,
    ts         TIMESTAMP NOT NULL,
    WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
)
```

Note: `TIMESTAMP` in DDL maps to `Timestamp(Microsecond, None)` on the
Arrow side. Connectors that produce their own schemas (OTel, Kafka+Avro,
CDC) may use a different precision (nanosecond for OTel's
`_laminar_received_at`, millisecond for CDC's `_ts_ms`); all precisions
compose correctly with `INTERVAL` arithmetic and window functions.

For a direct source, `INSERT INTO ... VALUES` accepts a signed integer Unix
epoch microsecond literal for a `TIMESTAMP` column. For example,
`INSERT INTO events VALUES ('a', 60, 100000)` inserts an event at 0.1 seconds
after the epoch. String timestamp literals are not accepted by this insertion
path.

A FIX session source (`fix` feature) declares every column and maps it to a tag path; see
[FIX source](FIX_SOURCE.md) for options, the mapping grammar, and the recovery contract:

```sql
CREATE SOURCE fills (
    exec_id       VARCHAR NOT NULL,
    last_px       DECIMAL(18, 8),
    transact_time TIMESTAMP NOT NULL,
    party_ids     ARRAY<VARCHAR>,
    _fix_seq_num  BIGINT NOT NULL,
    WATERMARK FOR transact_time AS transact_time - INTERVAL '2' SECOND
) FROM FIX (
    host = 'fix.venue.example', port = '9443',
    'sender.comp.id' = 'LAMINAR', 'target.comp.id' = 'VENUE',
    password = '${FIX_PASSWORD}',
    'dictionary.path' = '/etc/laminardb/fix/venue-FIX44.xml',
    messages = '8', 'journal.path' = '/var/lib/laminardb/fix/venue',
    'column.exec_id' = '17', 'column.last_px' = '31',
    'column.transact_time' = '60', 'column.party_ids' = '453.448'
)
```

FpML documents (`fpml` feature) arrive through the files, Kafka, RabbitMQ, or IBM MQ source with
`FORMAT FPML`; a projection profile supplies the schema, so declare at most a watermark. A FIX
source reads them from a field such as SecurityXML with `'fpml.field' = '1185'` and appends them as
an `fpml` struct column. See [FpML source](FPML_SOURCE.md) for profiles, options, and the recovery
contract:

```sql
CREATE SOURCE rates (
    WATERMARK FOR creation_time_utc AS creation_time_utc - INTERVAL '1' SECOND
) FROM FILES (
    'path' = '/data/fpml', 'fpml.profile' = 'fpml-5-13-confirmation-rates'
) FORMAT FPML;
```

**Rust side:**
```rust
#[derive(Record)]
pub struct Trade {
    pub account_id: String,
    pub symbol: String,
    pub side: String,
    pub price: f64,
    pub volume: i64,
    pub order_ref: String,
    #[event_time]
    pub ts: i64,  // epoch microseconds; the record macro handles the Arrow mapping
}
```

---

## Window Types

### TUMBLE (Fixed windows)

Non-overlapping windows of fixed size. Every event belongs to exactly one window.

```sql
CREATE STREAM ohlc AS
SELECT symbol,
       tumble(ts, INTERVAL '5' SECOND) AS window_start,
       first_value(price) AS open,
       MAX(price) AS high,
       MIN(price) AS low,
       last_value(price) AS close,
       SUM(volume) AS volume,
       COUNT(*) AS trade_count
FROM trades
GROUP BY symbol, tumble(ts, INTERVAL '5' SECOND)
```

**Window offset for timezone alignment:**

```sql
-- Tumble with 8-hour offset (align to UTC+8 day boundaries)
SELECT symbol,
       tumble(ts, INTERVAL '1' HOUR, INTERVAL '8' HOUR) AS window_start,
       COUNT(*) AS trade_count
FROM trades
GROUP BY symbol, tumble(ts, INTERVAL '1' HOUR, INTERVAL '8' HOUR)
```

**Key points:**
- Use lowercase `tumble()`. `TUMBLE()` also works, but lowercase is canonical.
- `tumble()` returns `Timestamp(Millisecond)` directly. There is no `TUMBLE_START()` function.
- Optional third argument for timezone offset alignment
- Window closes when watermark passes window end

### HOP (Sliding windows)

Overlapping windows: each event appears in multiple windows. Useful for smoothing/baselines.

```sql
CREATE STREAM vol_baseline AS
SELECT symbol,
       SUM(volume) AS total_volume,
       COUNT(*) AS trade_count,
       AVG(price) AS avg_price
FROM trades
GROUP BY symbol, HOP(ts, INTERVAL '2' SECOND, INTERVAL '10' SECOND)
```

**Key points:**
- First interval = slide, second interval = window size
- Each event appears in `size / slide` windows (here: 10/2 = 5)
- More output rows than TUMBLE. Plan downstream capacity accordingly.

### SESSION (Gap-based windows)

Windows that close after a period of inactivity. Groups bursts of events.

```sql
CREATE STREAM rapid_fire AS
SELECT account_id,
       COUNT(*) AS burst_trades,
       SUM(volume) AS burst_volume,
       MIN(price) AS low,
       MAX(price) AS high
FROM trades
GROUP BY account_id, SESSION(ts, INTERVAL '2' SECOND)
```

**Key points:**
- Session closes when watermark passes `last_event_ts + gap_duration`
- Useful for detecting bursts/spikes in per-entity activity
- Each entity (GROUP BY key) has independent sessions

---

## Joins

Local and cluster joins use the same vnode state, checkpoint, recovery, and rebalance lifecycle
under at-least-once and exactly-once delivery. The bounded path supports `INNER`, `LEFT`, `RIGHT`,
`FULL`, `LEFT SEMI`, `RIGHT SEMI`, `LEFT ANTI`, and `RIGHT ANTI` for append-only inputs and for the
certified mutable-source route described below.

### Bounded event-time join

Join two sources within a time window. Both sources must have compatible timestamps.

```sql
CREATE STREAM suspicious_match AS
SELECT t.symbol AS symbol,
       t.price AS trade_price,
       t.volume AS volume,
       o.order_id AS order_id,
       o.account_id AS account_id,
       o.side AS side,
       o.price AS order_price,
       t.price - o.price AS price_diff
FROM trades t
INNER JOIN orders o
ON t.tenant_id = o.tenant_id
AND t.symbol = o.symbol
AND o.ts BETWEEN t.ts AND t.ts + INTERVAL '10' SECOND;
```

**Key points:**

- Each input must carry a watermark on its `TIMESTAMP NOT NULL` event-time column: a source that
  declares it, or an append-only stream that forwards that column unchanged through a projection
  or filter. Append-only pairs use the ordinary path. If either input is a mutable connector, both
  connectors must expose ordered deterministic row positions and a replayable recovery contract.
- Append-only bounded intervals retain live hot-add support in a running local checkpoint-disabled
  pipeline; their post-projection is initialized before the first cycle can route or retain input.
  Mutable bounded intervals and consumers of their changelog are fixed startup topology and must be
  created while stopped.
- A keyed-upsert input needs an explicit primary key containing every equality key and its
  event-time column. A full-changelog input needs one exact trailing non-null `BIGINT __weight`;
  append-only and keyed-upsert source schemas cannot declare that column.
- A mutable source may feed only its admitted bounded joins. Mutable joins do not admit pushed-down
  source predicates, volatile projection/filter functions, or explicit references/aliases for the
  engine-owned `__weight` column. Their output is a full changelog, so every downstream projection
  preserves the trailing weight and external sinks must declare full-changelog support.
- In local and single-node recoverable runtime, a changelog may enrich against a static reference
  table whose snapshot is checkpointed. Process-local `INSERT INTO` is available only when local
  checkpointing is disabled; recoverable deployments load reference data before intake and restore
  the checkpointed image. Cluster execution rejects this enrichment until every participant and
  future owner can bind the same snapshot identity. This ensures a later retraction joins the same
  dimension values as its original insertion.
- Equality keys may contain one or more ordered `VARCHAR`/`BIGINT` columns. Types must match at each
  position; SQL `NULL` keys do not match.
- The directional predicate is `right.ts BETWEEN left.ts AND left.ts + positive_finite_bound`.
- Every projected expression needs an explicit alias, including columns whose names are unique;
  every projected or filtered column must also use its left/right input qualifier. Nested
  projection/filter subqueries are not supported by the flattened pair projection.
- Outer and anti unmatched rows become final only when the opposite input watermark closes their
  possible match interval.
- Cross, unbounded, general non-equality, intermediate-input, and multi-way joins fail closed on
  this bounded stream-stream path. `FOR SYSTEM_TIME AS OF` and `TEMPORAL PROBE JOIN` use the
  separate managed vnode-keyed temporal path for one direct `INNER` or `LEFT` join in single-node
  and cluster mode. `ASOF JOIN` is not an alias; use `FOR SYSTEM_TIME AS OF`.

To aggregate rows from any of the eight kinds, name the join output and create a separate keyed
aggregate stage:

```sql
CREATE STREAM matched AS
SELECT t.account_id AS account_id, t.volume AS volume
FROM trades t
INNER JOIN orders o
ON t.account_id = o.account_id
AND o.ts BETWEEN t.ts AND t.ts + INTERVAL '10' SECOND;

CREATE STREAM matched_totals AS
SELECT account_id, SUM(volume) AS total_volume, COUNT(*) AS match_count
FROM matched
GROUP BY account_id;
```

The same aggregate may be written over the join in one statement; it is split into the same two
stages (see [Nested aggregates and aggregates over a join](#nested-aggregates-and-aggregates-over-a-join)).
A cycle that would exceed 262,144 output rows or 64 MiB causes a terminal controlled failure;
operators cannot resume or spill that fanout.

### Temporal ASOF join

```sql
CREATE STREAM valued_trades AS
SELECT t.trade_id AS trade_id, q.price AS quote_price
FROM trades t
LEFT JOIN quotes FOR SYSTEM_TIME AS OF t.ts AS q
  ON t.symbol = q.symbol;
```

This direct two-input path supports `INNER` and `LEFT`. The left source must be append-only; the
right source must declare its equality keys as a primary key and may be append-only or keyed-upsert.
Both event-time columns require watermarks. A result becomes final only after the right watermark
passes its probe time. Configure `server.temporal_join_idle_history_retention` to bound version
history when an input idles. The same rules apply in single-node and cluster mode.

---

## Aggregation Patterns

### CASE WHEN inside aggregates

Split aggregations by condition within a single stream:

```sql
CREATE STREAM wash_score AS
SELECT account_id,
       symbol,
       SUM(CASE WHEN side = 'buy' THEN volume ELSE CAST(0 AS BIGINT) END) AS buy_volume,
       SUM(CASE WHEN side = 'sell' THEN volume ELSE CAST(0 AS BIGINT) END) AS sell_volume,
       SUM(CASE WHEN side = 'buy' THEN 1 ELSE 0 END) AS buy_count,
       SUM(CASE WHEN side = 'sell' THEN 1 ELSE 0 END) AS sell_count
FROM trades
GROUP BY account_id, symbol, TUMBLE(ts, INTERVAL '5' SECOND)
```

**Key point:** The `ELSE` branch must match the column type. Use `CAST(0 AS BIGINT)` when summing BIGINT columns. `ELSE 0` alone produces INT32, causing a type mismatch.

### Computed columns

Arithmetic in SELECT works inline:

```sql
SELECT symbol,
       MAX(price) - MIN(price) AS price_range,
       AVG(price) * COUNT(*) AS notional
FROM trades
GROUP BY symbol, tumble(ts, INTERVAL '5' SECOND)
```

### Managed aggregate functions

The non-windowed managed path used by named join outputs in local and cluster mode supports these
non-`DISTINCT` aggregates:

| Function | Notes |
|----------|-------|
| `COUNT(*)` / `COUNT(col)` | Row/non-null count |
| `SUM(col)` | Sum (respects type) |
| `AVG(col)` | Average (returns DOUBLE) |
| `MIN(col)` / `MAX(col)` | Min/max |

`first_value` and `last_value` are available on supported local window paths, not on the distributed
named-aggregate path. `DISTINCT` aggregates and `MIN`/`MAX` over changelog inputs remain rejected
because bounded retractable extrema state is not supported. Mutable bounded-join output can feed
`COUNT`, `SUM`, and `AVG` aggregate stages or a full-changelog sink; append-only join output keeps
the ordinary row stream.

### Nested aggregates and aggregates over a join

A statement that no single operator can run is split into internal stages named
`<stream>$stage<n>`, each admitted by the rules for its own shape in every mode:

```sql
-- A join stage, then a keyed aggregate over it.
CREATE STREAM matched_totals AS
SELECT t.account_id AS account_id, SUM(t.volume) AS total_volume
FROM trades t
INNER JOIN orders o
ON t.account_id = o.account_id
AND o.ts BETWEEN t.ts AND t.ts + INTERVAL '10' SECOND
GROUP BY t.account_id;

-- An aggregate stage that emits changes, then an aggregate over its changelog.
CREATE STREAM accounts_by_trade_count AS
SELECT trade_count, COUNT(*) AS accounts
FROM (SELECT account_id, COUNT(*) AS trade_count FROM trades GROUP BY account_id)
GROUP BY trade_count
EMIT CHANGES;
```

- An aggregate over one two-way join reads a join stage that projects each input column the
  aggregate uses as `<qualifier>_<column>` (`t.volume` becomes `t_volume`). Every column the
  aggregate reads must carry its input qualifier, and `WHERE` filters the join stage.
- An aggregate over a subquery in `FROM` that aggregates or joins reads that subquery as a stage.
  An aggregating subquery emits changes, so the outer aggregate supports `COUNT`, `SUM` and `AVG`
  but not `MIN` or `MAX`. In cluster mode a windowed subquery is rejected, because cluster windows
  emit only closed windows.
- Stages belong to their stream. `SHOW STREAMS` and `SUBSCRIBE` see only the stream,
  `DROP STREAM` removes its stages, and no other statement may read, drop or create a
  `$stage` name. A failed `CREATE` removes the stages it created.
- A join of three or more inputs still needs named two-way stages: a join's output carries no
  event time, so an interval join cannot read it.

### Streaming UDFs

| Function | Returns | Use |
|----------|---------|-----|
| `tumble(ts, size)` | `Timestamp(Millisecond)` (window start) | `GROUP BY tumble(...)` |
| `tumble(ts, size, offset)` | `Timestamp(Millisecond)` | Timezone-aligned window boundaries |
| `tumble_end(ts, size)` | `Timestamp(Millisecond)` | Explicit window-end column in `SELECT` |
| `hop(ts, slide, size)` / `slide(ts, size, slide)` | `Timestamp(Millisecond)` | Sliding / hopping windows |
| `hop_end(ts, slide, size)` | `Timestamp(Millisecond)` | Window end for hop |
| `session(ts, gap)` | `Timestamp(Millisecond)` | Session windows |
| `cumulate(ts, step, max_size)` | `Timestamp(Millisecond)` | Cumulating windows (parsed; pipeline support pending) |
| `cumulate_end(ts, step, max_size)` | `Timestamp(Millisecond)` | Cumulating window end |
| `proctime()` | `Timestamp(Millisecond)` | Processing-time stamp at evaluation |

---

## Sinks and Subscriptions

After creating a stream, create a sink and subscribe to get results in Rust:

```sql
CREATE SINK ohlc_sink FROM ohlc
```

Sink input must be a named source or stream; inline `CREATE SINK ... FROM (SELECT ...)` queries are
rejected until they have a named graph node and the same schema/changelog admission. A stream that
carries `__weight` requires a sink whose connector contract supports full changelogs; the runtime
passes positive and negative weights through unchanged and fails closed if the weight is missing or
malformed.

A connector sink may read a source directly in embedded and single-node mode; cluster mode
requires a stream between them. A keyed-upsert source (for example MongoDB CDC
`output.mode=document`) can feed direct sinks keyed on exactly the source `PRIMARY KEY`, when
nothing else reads it: they receive the source columns plus `_op` (`U` = put, `D` = key-only delete).
PostgreSQL `write.mode=upsert` with `changelog.mode=true` and Delta `write.mode=upsert` qualify.
Otherwise the source must be owned by a route that keeps keyed state (a temporal-join right side
or a bounded interval join); sink filters and plain streams over it are rejected before any I/O.

```rust
// FromRow struct fields must match SELECT column order exactly
#[derive(FromRow)]
pub struct OhlcRow {
    pub symbol: String,      // 1st SELECT column
    pub window_start: i64,   // 2nd SELECT column
    pub open: f64,           // 3rd SELECT column
    // ... etc
}

let sub = db.subscribe::<OhlcRow>("ohlc")?;

// Poll for results (non-blocking)
while let Some(rows) = sub.poll() {
    for row in &rows {
        println!("{}: O={} H={} L={} C={}", row.symbol, row.open, row.high, row.low, row.close);
    }
}
```

**Critical:** `FromRow` struct field order must match the SQL `SELECT` column order. Field names don't matter, only position does.

### Kafka

Kafka partitions do not decide aggregation groups. Every row is routed by its `GROUP BY` key, so
rows with the same key aggregate into one group even when they arrive on different partitions:

```sql
CREATE SOURCE orders (id BIGINT NOT NULL, customer BIGINT NOT NULL, amount BIGINT NOT NULL)
FROM KAFKA ('bootstrap.servers' = 'broker:9092', 'topic' = 'orders',
            'group.id' = 'orders-app', 'startup.mode' = 'earliest',
            'latency.profile' = 'low_latency') FORMAT JSON;

CREATE MATERIALIZED VIEW customer_totals AS
SELECT customer, SUM(amount) AS total, COUNT(*) AS orders FROM orders GROUP BY customer;

-- One record per change; the newest record per customer is the current total.
CREATE SINK customer_totals_out FROM customer_totals INTO KAFKA (
    'bootstrap.servers' = 'broker:9092', 'topic' = 'customer-totals',
    'key.column' = 'customer', 'key.format' = 'text',
    'envelope' = 'upsert', 'partition.count' = '6') FORMAT JSON;
```

`envelope = 'upsert'` writes each key to `murmur2(key) % partition.count` and a NULL value when a
key is deleted; it rejects a topic whose partition count differs, because a changed count would
split one key's history across partitions. Append sinks route keyed records the same way and
spread unkeyed batches across partitions. `latency.profile` (`low_latency`, `balanced`,
`throughput`) supplies batching defaults; explicit options still win.

With exactly-once delivery (`delivery_guarantee: ExactlyOnce` embedded, `delivery =
"exactly_once"` in the server) each Kafka sink also needs `'commit.topic'`, a topic with
`cleanup.policy=compact`. Output then becomes visible to `isolation.level=read_committed` readers
once per committed checkpoint, after a crash at any point included; `read_uncommitted` readers can
also see aborted attempts. Cluster deployments keep Kafka output at-least-once. A Kafka source
keyed by message key (key + value = upsert, key + NULL = delete) is not available; use the
Debezium envelope for keyed changes.

### RabbitMQ and IBM MQ

`FROM RABBITMQ` / `INTO RABBITMQ` (`rabbitmq` feature) and `FROM IBMMQ` / `INTO IBMMQ` (`ibmmq`
feature) read queues and write exchanges or queues with at-least-once delivery. Both sources are
commit-coupled: a message is acknowledged (RabbitMQ) or committed (`MQCMIT`) only after the
checkpoint containing it is durable, so they need checkpointing and at-least-once delivery, and
cluster mode rejects them. In cluster mode the sinks run on every node as independent
at-least-once writers, like Kafka output. `FORMAT RAW` with a `value BYTEA` column keeps payload
bytes exactly; `metadata.columns` appends typed broker metadata after the declared columns.

```sql
CREATE SOURCE orders (id BIGINT, amount DOUBLE) FROM RABBITMQ (
    'url' = 'amqps://broker:5671/sales', 'username' = 'laminar',
    'password' = '${RABBITMQ_PASSWORD}', 'queue' = 'orders',
    'metadata.columns' = 'routing_key,redelivered') FORMAT JSON;

CREATE SINK large_orders FROM big INTO IBMMQ (
    'queue.manager' = 'QM1', 'channel' = 'APP.SVRCONN', 'connection.name' = 'mq1(1414)',
    'username' = 'laminar', 'password' = '${MQ_PASSWORD}', 'queue' = 'ORDERS.LARGE') FORMAT JSON;
```

See [RabbitMQ](RABBITMQ.md) and [IBM MQ](IBMMQ.md) for options, the acknowledgement and commit
boundaries, poison-message handling, and tested behaviour.

### Cluster SQL boundary

Cluster `CREATE STREAM` admits projection/filter pipelines, supported non-windowed keyed aggregates,
the eight bounded join kinds described above, and managed `TUMBLE`, `HOP`, and `SESSION`
aggregates. A cluster window reads a source, or an append-only stream that forwards the source's
event-time column unchanged; it requires a bounded watermark on that column and `EMIT ON WINDOW
CLOSE` or `EMIT FINAL`. A join output may feed a keyed aggregate, either as a named stream or in
one statement that is split into stages; windowed joins remain rejected. A stream may sit at most
three remote exchanges (joins, aggregates, or windows) downstream of its sources, the depth for
which checkpoint pauses are measured; a deeper pipeline is rejected with `[LDB-4007]`.

Cluster `CREATE MATERIALIZED VIEW` admits the query shapes `CREATE STREAM` admits:

- a keyed or global aggregate view;
- a projection or filter over such a view, keeping or dropping its key columns;
- an append-only view: a projection over a source, a closed window, or an interval join.

Each node keeps the result rows of the vnodes it owns, checkpoints them with the aggregate, and
moves them with their vnodes when ownership changes. A view that drops or changes its input's
key places each row by the whole row, so the node that produces a row sends it to the node that
owns it; that counts as one more remote exchange. Each node keeps the part of an append-only
view's log that it produced. When a node leaves, through rebalancing or failure, another node
takes over its log in the next checkpoint without copying it. A topology migration that adds,
replaces or drops other objects keeps every view and its rows. A migration may also add a view
(`CREATE MATERIALIZED VIEW`) or drop one (`DROP MATERIALIZED VIEW` without `CASCADE`), but not
drop and recreate the same view. A new view over an existing aggregate or view starts from that
input's rows at the migration's checkpoint, then follows its later changes; a new view over a
source starts empty there and sees only later rows. A new view over a stream that only reshapes
an aggregate's changes is rejected, because no node keeps that stream's rows; define it over the
aggregate. `LaminarDB::query_committed` on any node reads every view at one committed checkpoint,
from every node's checkpointed state; a node that owns no vnodes (draining, or not yet assigned)
learns of no later checkpoint, so it keeps reading the last one it took part in. `SELECT` of a
view on a cluster node fails, because the node holds only its part. `SUBSCRIBE` to a view is
rejected with `[LDB-4007]` before connector I/O.

An append-only view's interval retention follows one event-time frontier per view, committed
with each checkpoint: the lowest frontier among the nodes whose input is active, never moving
back. Every node keeps the same rows, wherever they were written. A row that arrives behind that
frontier minus the interval is dropped and counted in `mv_rows_below_horizon_total`; embedded and
single-node runtimes count their drops the same way. A `'<n> rows'` retention applies on each
node separately, so a cluster keeps up to `n` rows per node and may keep fewer after a node
leaves; when a node takes over another's log, the adopted rows count as its newest. A cluster
checkpoint retains at most 24,576 view-log segments across all nodes, an even share per node
(8,192 each on three nodes), and a rebalance is refused before it starts when the remaining nodes
could not take over the leaving nodes' logs within their share.

The materialized-view form of `SUBSCRIBE` below applies to embedded and single-node runtimes.

### SUBSCRIBE over the Postgres wire protocol

When the server is started with `pgwire_bind` set, materialized views can be streamed directly to any libpq client (psql, JDBC, asyncpg, etc.):

```sql
SUBSCRIBE <name> [AS OF EPOCH <n>] [WHERE <predicate>]
```

- `<name>` may be a materialized view or a resolved named stream. A bare source is not subscribable.
- The optional `WHERE` clause is compiled by DataFusion against the target's schema and applied per batch before the row reaches the wire. Named streams whose output schema is not resolved reject `WHERE`.
- The query stays open until the client disconnects. Local runtimes preserve their existing delivery behaviour; cluster data becomes visible only after a complete cluster checkpoint commits.
- Cluster `SUBSCRIBE` is supported only for named, non-windowed managed keyed aggregate streams whose planner certificate binds the aggregate grouping to stable vnode ownership. Stateless streams, raw join output, windowed aggregates, materialized views, and uncertified plans fail before subscription backend I/O.
- Every cluster server is a gateway over the same shared committed history; clients do not need to locate current vnode owners.
- Cluster ordering is strict and contiguous within each vnode output partition. Fair gateway merging defines no order between partitions and does not imply arrival, event-time, SQL-sort, or global total order.
- Partition sequences describe committed source-log frames. A `WHERE` predicate can remove every row from a frame, so optional frame metadata observed by a filtered client can have non-adjacent partition sequences without indicating that a matching row was skipped.

#### Reconnect without gaps: `RETAIN HISTORY` + `AS OF EPOCH`

Create the stream with bounded history:

```sql
CREATE STREAM positions AS
  SELECT account_id, SUM(pnl) AS total_pnl
  FROM raw_trades
  GROUP BY account_id
  WITH ('retain_history' = '64mb');
```

Local runtimes keep the suffix in their existing byte-bounded registry. For the supported cluster
aggregate scope, history is a byte-bounded suffix of verified immutable Arrow segments referenced
by authoritative committed checkpoint indexes in shared object storage. Object listing is never
commit authority.

WebSocket consumers receive `progress` frames containing `epoch` and `checkpoint_id`. Pgwire adds
`__laminar_kind`, `__laminar_epoch`, and `__laminar_checkpoint_id` control columns without changing
the user data schema. Cluster progress is emitted only after the complete participant roster and
partition-frontier vector commit. Durably record a marker after processing its preceding data, then
resume from the exclusive frontier of that checkpoint:

```sql
SUBSCRIBE positions AS OF EPOCH 42;
```

If epoch `42` was not committed, was pruned, belongs to another generation of the named stream, or
references missing/corrupt history, the server returns a structured terminal error rather than
skipping rows. `AS OF EPOCH` is checkpoint-granular durable replay, not acknowledgement storage for
a named consumer. A disconnect after processing part of the next checkpoint interval can therefore
cause that interval to be delivered again.

A bare `SUBSCRIBE positions` attaches at the current committed tail: rows committed before attachment
are excluded, and rows exposed by later committed checkpoints are delivered.

#### Cursored consumption: `DECLARE` / `FETCH` / `CLOSE`

For libpq clients that drive flow control via `\set FETCH_COUNT n` (psql, JDBC with `setFetchSize`):

```sql
DECLARE c CURSOR FOR SUBSCRIBE positions WHERE total_pnl > 0;
FETCH FORWARD 100 FROM c;
FETCH FORWARD 100 FROM c;
CLOSE c;
```

The cursor is forward-only, lives for the duration of the connection, and shares the same `WHERE` and `AS OF EPOCH` semantics as bare `SUBSCRIBE`.

---

## Watermarks

Watermarks tell the engine "no more data before this time." Windows emit when the watermark passes the window boundary.

```rust
// Advance watermark on all sources
source.watermark(current_ts + 10_000);  // 10s ahead covers HOP(10s) windows
```

**Key points:**
- Advance watermark on **all** sources (both sides of a join)
- Watermark should be at least `current_ts + largest_window_size`
- HOP(10s) needs watermark 10s ahead; SESSION(2s) needs 2s past last event
- LaminarDB processes in 100ms micro-batch ticks. Results appear after the next tick.

---

## Introspection

### SHOW Commands

List registered sources, sinks, and streams with metadata:

```sql
-- Returns columns: name, connector, format, watermark, ...
SHOW SOURCES;

-- Returns columns: name, input, sql, ...
SHOW SINKS;
SHOW STREAMS;

-- Reconstruct the original DDL for a source or sink
SHOW CREATE SOURCE trades;
SHOW CREATE SINK my_sink;
```

### EXPLAIN

`EXPLAIN CREATE STREAM` also reports how each input reaches the stream and what the stream
provides downstream, without creating it:

```sql
EXPLAIN CREATE STREAM per_symbol AS
  SELECT symbol, COUNT(*) AS n FROM trades GROUP BY symbol EMIT CHANGES;
-- exchange.input.trades  repartition on (symbol Utf8) from arbitrary, append-only, event time ts
-- exchange.output        hash(symbol Utf8), weighted changelog
```

An input is `forward` (rows stay where they are), `repartition on (...)` (rows move to the
owner of their key), `colocated on (...)` (an aggregate whose input is already hashed on its
group key, so no rows move), `gather partials to vnode N` (a global aggregate: in cluster mode
each other node sends one partial row per input batch instead of its rows), or `gather to vnode N`
(a window without a group key). Join inputs are named
`left:<input>` and `right:<input>`. The output is `hash(...)`, `singleton(...)`, or
`arbitrary`, with its changelog kind: `append-only`, `weighted changelog`, or `running state`
(an aggregate without `EMIT CHANGES`), and `event time <column>` when the rows carry a source
watermark on that column (a source's own, or one a projection forwards unchanged).

For a statement that is split into stages, `EXPLAIN` lists each stage's SQL as a
`stage.<name>` row, followed by that stage's `stage.<name>.exchange.*` rows. An aggregate that
groups by exactly the key its join stage is hashed on (that stage's `exchange.output` row) reads
the stage `colocated`, so no rows move between them.

### EXPLAIN ANALYZE

Execute a query and report execution metrics:

```sql
EXPLAIN ANALYZE SELECT symbol, COUNT(*) FROM trades GROUP BY symbol;
-- Returns: rows_produced, execution_time_ms, batches_processed
```

---

## Common Gotchas

### 1. Event-time columns must be `TIMESTAMP`, not `BIGINT`

The event-time path requires a non-null Arrow `Timestamp(_)` column.
Declare `ts TIMESTAMP NOT NULL` (at any precision your connector emits);
numeric and nullable event-time columns fail during admission.

```sql
CREATE SOURCE trades (symbol VARCHAR, price DOUBLE, ts TIMESTAMP NOT NULL,
                      WATERMARK FOR ts AS ts - INTERVAL '5' SECOND);

-- Join predicates on Timestamp columns compose with INTERVAL:
WHERE o.ts BETWEEN t.ts AND t.ts + INTERVAL '10' SECOND
```

If you need an `i64` millis derived column for downstream consumption
(e.g. a dashboard that wants a plain number), cast the Timestamp and
divide by the precision factor:

```sql
CAST(ts AS BIGINT) / 1000   -- Timestamp(Microsecond) → epoch millis
CAST(ts AS BIGINT) / 1000000 -- Timestamp(Nanosecond) → epoch millis
```

### 2. Type mismatch in CASE WHEN

```sql
-- FAILS: ELSE 0 is INT32, volume is BIGINT
SUM(CASE WHEN side = 'buy' THEN volume ELSE 0 END)

-- WORKS: explicit cast
SUM(CASE WHEN side = 'buy' THEN volume ELSE CAST(0 AS BIGINT) END)
```

### 3. Window function names

```sql
-- FAILS
SELECT TUMBLE_START(ts, INTERVAL '5' SECOND), FIRST(price), LAST(price)

-- WORKS
SELECT tumble(ts, INTERVAL '5' SECOND), first_value(price), last_value(price)
```

### 4. FromRow field ordering

```rust
// SQL: SELECT symbol, SUM(volume) AS total, AVG(price) AS avg_price

// WRONG: field names don't matter, ORDER matters
#[derive(FromRow)]
pub struct Bad {
    pub avg_price: f64,  // This gets the 1st column (symbol), not avg_price!
    pub symbol: String,
    pub total: i64,
}

// CORRECT: matches SELECT column order
#[derive(FromRow)]
pub struct Good {
    pub symbol: String,    // 1st column
    pub total: i64,        // 2nd column
    pub avg_price: f64,    // 3rd column
}
```

### 5. Both sources must advance watermarks for bounded joins

```rust
// WRONG for state finality: unmatched rows cannot finalize and old state cannot be evicted
trade_source.watermark(ts + 10_000);

// CORRECT: advance both
trade_source.watermark(ts + 10_000);
order_source.watermark(ts + 10_000);
```

An inner match can emit before either watermark advances. Both sources must still define and
advance watermarks for bounded state cleanup and for outer or anti unmatched-row finalization.

### 6. Chained aggregates need `EMIT CHANGES` upstream

Without an `EMIT` clause, an aggregate stream re-emits each updated group on every cycle and never
retracts the earlier value. A downstream aggregate, window, or interval or temporal join would
treat every one of those snapshots as new input, so `CREATE STREAM` and `CREATE MATERIALIZED VIEW`
reject that input. This applies whether the consumer reads the aggregate stream directly or through
projection/filter streams or lookup joins, including a projection declared with `EMIT CHANGES`: an
emit clause changes what an aggregate emits, while a projection or lookup join forwards whatever
its input emits.

```sql
-- WRONG: rejected, per_key re-emits running state
CREATE STREAM per_key AS SELECT k, COUNT(*) AS n FROM events GROUP BY k;

-- CORRECT: per_key emits a retracting changelog
CREATE STREAM per_key AS SELECT k, COUNT(*) AS n FROM events GROUP BY k EMIT CHANGES;
CREATE STREAM histogram AS SELECT n, COUNT(*) AS keys FROM per_key GROUP BY n;
```

A server configuration or sealed cluster catalog containing this chain is rejected when it is
replayed at startup.

A non-windowed aggregate inside `CREATE MATERIALIZED VIEW` always emits its changelog, global
aggregates included, so an aggregate or projection over a view needs no `EMIT` clause. The same
rule makes a window or join over such a view fail, and a sink that reads the view must accept
full changelogs.

### 7. Append-only views keep rows by a declared retention

A view whose query only appends rows (a projection or filter over a source, a window with `EMIT ON
WINDOW CLOSE` or `EMIT FINAL`, or an interval join) keeps its rows as a log. With checkpointing,
each checkpoint writes the rows appended since the previous one as one segment beside the
checkpoint's state, memory holds only those rows, and `SELECT` reads committed segments back from
the checkpoint store. Retention decides which rows a read returns and which segments later
checkpoints keep:

- `WITH (retention = '<interval>')`, for example `'2 hours'`: rows whose event time is at or after
  the view's watermark minus the interval. The event-time column is the source's watermark column
  when the query forwards it unchanged, or the window column of a windowed view, which keeps a
  window while its end (start plus window size, or plus the gap for a session) is inside the
  interval. A view that declares nothing keeps 1 hour.
- `WITH (retention = '<n> rows')`: the latest `n` rows. A view without an event-time column (over
  an interval join, or over a source without a watermark) must declare it.
- `WITH (retention = 'unbounded')`: every row. It needs checkpointing, because without it the
  whole log stays in memory.
- A changelog view rejects `retention`.

```sql
CREATE MATERIALIZED VIEW minute_volume AS
SELECT symbol, tumble(ts, INTERVAL '1' MINUTE) AS window_start, SUM(qty) AS volume
FROM trades GROUP BY symbol, tumble(ts, INTERVAL '1' MINUTE)
EMIT ON WINDOW CLOSE
WITH (retention = '6 hours');
```

Rows waiting for the next checkpoint count against `materialized_view_max_rows` and
`materialized_view_max_bytes`, and a cycle that would exceed them fails instead of dropping rows.
Without checkpointing the whole retained log stays in memory under the same limits. A row whose
event time is NULL is not returned once the view has a watermark. Earlier releases kept the last
1,000 batches of an append view instead.

Each checkpoint adds at most one segment per view. After each checkpoint, a background task
merges consecutive older segments of a view, up to 16 MiB each, into one segment without the rows
its retention no longer keeps, and the next checkpoint writes it in their place; reads do not
change. A view therefore keeps a few dozen segments plus about one per 8 to 16 MiB of retained
rows, whatever its age. One checkpoint retains at most 32,768 segments across all views, so
`unbounded` views can grow to roughly 256 to 512 GiB of encoded rows per node before checkpoints
fail with an error naming the largest view.

### 8. Committed reads see one checkpoint; `freshness` bounds how old it may be

`SELECT` over materialized views reads the last completed cycle, which recovery can roll back.
The Rust API `LaminarDB::query_committed(sql, at)` reads every view a query scans at one committed
checkpoint instead, and returns the checkpoint's epoch and when it was taken:

- `CommittedRead::Latest` reads the newest committed checkpoint.
- `CommittedRead::AtLeast(epoch)` waits for a checkpoint at or after `epoch`. Pass an epoch a
  previous result returned to read no earlier than it.
- `CommittedRead::Exact(epoch)` reads that checkpoint, and fails once checkpoint cleanup retired
  it. Cleanup keeps only the newest checkpoint and any checkpoint a read holds.

Such a result is never rolled back and never mixes checkpoints. It is as fresh as the checkpoint
interval. A committed read needs checkpointing, may scan only materialized views, and holds its
checkpoint for at most 30 seconds, which also bounds the wait for `AtLeast`. A view with a
row-count retention is not exact, because which rows the cap keeps depends on arrival order.
Equality filters on every key column of an aggregate view read only the state of that key's
vnode, and range filters on the event-time column of an append view kept by an interval or
`unbounded` skip segments outside the range.
Decoded checkpoint state is cached up to `materialized_view_read_cache_bytes` (256 MiB).

`WITH (freshness = '<interval>')` declares the most committed reads of a view may lag. `CREATE`
rejects it when checkpoints are manual or disabled, or when it is below the checkpoint interval.
While the newest committed checkpoint is older than a view's freshness, a warning is logged and
`mv_freshness_breaches_total{view}` counts the breach once; `mv_committed_staleness_ms` exports
the age.

```sql
CREATE MATERIALIZED VIEW positions AS
SELECT account, SUM(qty) AS qty FROM fills GROUP BY account
WITH (freshness = '10 seconds');
```

---

## Tested Combinations

The following patterns are confirmed working in LaminarDB embedded mode (tested in [laminardb-test](https://github.com/laminardb/laminardb-test) and [laminardb-fraud-detect](https://github.com/laminardb/laminardb-fraud-detect)):

| Pattern | Example |
|---------|---------|
| TUMBLE + multiple aggregates | SUM, COUNT, AVG, MIN, MAX, first_value, last_value |
| TUMBLE + CASE WHEN in SUM | Buy/sell volume split |
| TUMBLE + computed columns | MAX(price) - MIN(price) |
| HOP + aggregates | Rolling volume baselines |
| SESSION + aggregates | Burst detection |
| All bounded join kinds + time window | Inner, outer, semi, and anti correlation |
| Cascading materialized views | Stream A -> Stream B -> Stream C |
| 5+ concurrent streams, 2 sources | Single LaminarDB instance, sub-ms latency |
| Multiple GROUP BY columns | account_id + symbol + window |
| SHOW SOURCES/SINKS/STREAMS | Metadata listing with connector/format info |
| SHOW CREATE SOURCE/SINK | DDL reconstruction |
| EXPLAIN ANALYZE | Query plan with execution metrics |
| TUMBLE with offset | Timezone-aligned window boundaries |

---

## Type Mapping

| SQL Type | Arrow Type | Rust Type | Notes |
|----------|------------|-----------|-------|
| `VARCHAR` | `Utf8` | `String` | |
| `BIGINT` | `Int64` | `i64` | Use for volumes, counts; **not** event-time columns |
| `TIMESTAMP` | `Timestamp(Microsecond)` | `i64` µs | Declared in DDL |
| `DOUBLE` | `Float64` | `f64` | Use for prices, averages |
| `INT` / `INTEGER` | `Int32` | `i32` | Avoid mixing with BIGINT in CASE WHEN |
| `BOOLEAN` | `Boolean` | `bool` | |

Connector-produced schemas may use other `Timestamp(_)` precisions:

| Column | Precision | Source |
|--------|-----------|--------|
| `_laminar_received_at` | `Timestamp(Nanosecond)` | OTel |
| `_timestamp` | `Timestamp(Millisecond)` | Kafka metadata |
| `file_modification_time` | `Timestamp(Millisecond)` | Files connector |

Despite the `_ms` / `_ns` suffixes in some historical names, these are
real Arrow `Timestamp` columns, not `Int64`. `INTERVAL` arithmetic and
window functions compose correctly against any precision.

---

*Tracks LaminarDB 0.22 on DataFusion 52.x / Arrow 57.x. Corrections welcome: open an issue or PR.*
