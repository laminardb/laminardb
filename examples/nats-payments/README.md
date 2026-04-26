# LaminarDB — payments + fraud-check streaming demo

Two correlated payment streams over NATS:

* `payments.initiated`  — a payment is created (someone hits Pay)
* `payments.scored`     — the fraud system returns a score 50ms–1s later

LaminarDB joins them on the fly and writes two Iceberg tables:

1. **`payments_summary`** — rolling 1-minute volume by region and method.
2. **`payments_with_fraud_score`** — interval join (2-second window).
   Each row is one payment matched to its fraud score, with the
   wall-clock latency between the two events.

The headline number, computed live as payments flow:

```
region    scored   p50_ms   p95_ms   p99_ms   blocked   review
us-east   41,832       72      198      482       248      613
us-west   41,201       74      201      497       247      615
eu-west   40,907       75      210      521       251      609
ap-south  40,615       78      215      540       243      598
```

Two Rust binaries do the streaming work (`nats-server` + `laminardb`);
three containers (RustFS, Postgres, Lakekeeper) provide the lakehouse.

## Prerequisite

Lakekeeper bakes its in-cluster RustFS endpoint (`http://rustfs:9000`)
into Iceberg manifest paths. Add a hosts entry on your machine so
DuckDB can resolve the name from the host:

    # /etc/hosts (Linux/macOS) or C:\Windows\System32\drivers\etc\hosts
    127.0.0.1   rustfs

The published `9000:9000` Docker port forwards `rustfs:9000` to the
running container.

## Run it

```bash
# 1. NATS + RustFS + Postgres + Lakekeeper.
docker compose -f examples/nats-payments/docker-compose.yml up -d

# 2. Build laminardb.
cargo build --release -p laminar-server \
    --no-default-features \
    --features mimalloc,nats,iceberg,websocket

# 3. Run the server.
./target/release/laminardb --config examples/nats-payments/config.toml

# 4. In another terminal, start the publisher (default 10K payments/s).
pip install nats-py
python examples/nats-payments/gen.py
# python examples/nats-payments/gen.py --rate 50000   # 50K/s
# python examples/nats-payments/gen.py --rate 0       # flat-out

# 5. Wait ~90 seconds, then query.
pip install duckdb
python examples/nats-payments/query.py
```

## What the pipelines do

### `payments_summary` — tumbling 1-minute rollup

```sql
SELECT region, method, COUNT(*), SUM(amount_usd), AVG(amount_usd)
FROM payments
GROUP BY TUMBLE(event_time, INTERVAL '1' MINUTE), region, method
EMIT ON WINDOW CLOSE
```

`window_start, window_end` auto-projected by the rewriter.

### `payments_with_fraud_score` — stream-to-stream interval join

```sql
SELECT
    p.payment_id, p.region, p.method, p.amount_usd,
    f.fraud_score, f.outcome,
    p.event_time                                              AS initiated_at,
    f.event_time                                              AS scored_at,
    CAST(f.event_time - p.event_time AS BIGINT) / 1000000     AS score_latency_ms
FROM payments p
JOIN fraud_checks f
    ON p.payment_id = f.payment_id
    AND f.event_time BETWEEN p.event_time AND p.event_time + INTERVAL '2' SECOND
```

Routes through laminardb's `IntervalJoinOperator`. Both streams need
advancing watermarks for the operator to emit. Payments whose score
arrives outside the 2-second window — or never — fall out of this
table; that's the operationally meaningful "fraud system slow" signal.

## Throughput

Live throughput off the `/metrics` endpoint:

```bash
python examples/nats-payments/bench.py
```

Sample output (10K/s sustained):

```
    time   ingest/s   emitted/s  commits   total_in  total_emit
15:14:01    19,800        0        12      610,012          0
15:14:02    19,795        0        13      620,017          0
...
15:14:12    19,801    9,800       14      720,118     45,300
```

`ingest/s` is the combined source delta (payments + fraud_checks
≈ 2× the publisher rate); `emitted/s` is rows out of both pipelines
(join hits + windowed rollup); `commits` is the cumulative sink
commit count.

## Explore

- Lakekeeper UI  → http://localhost:8182
- RustFS console → http://localhost:9001  (rustfsadmin / rustfsadmin)
- NATS monitor   → http://localhost:8222
- LaminarDB HTTP → http://127.0.0.1:8080

## Tear down

```bash
docker compose -f examples/nats-payments/docker-compose.yml down -v
```

## Files

| File                 | Purpose                                                |
|----------------------|--------------------------------------------------------|
| `config.toml`        | Sources, pipelines, sinks                              |
| `pipeline.sql`       | Reference copies of the two SELECT statements          |
| `docker-compose.yml` | NATS + RustFS + Postgres + Lakekeeper                  |
| `gen.py`             | NATS publisher (payments + fraud-score producers)      |
| `bench.py`           | live `/metrics` scraper for ingest + commit rates      |
| `query.py`           | DuckDB Iceberg reader + fraud-latency percentiles      |
