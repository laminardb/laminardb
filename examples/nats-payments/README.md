# LaminarDB — NATS → Iceberg payments demo

Payment events stream off a NATS subject. LaminarDB tumbles them into
1-minute windows by region+method and writes the rollup to Iceberg via a
REST catalog (Lakekeeper). Results queryable from DuckDB.

Two Rust binaries do the streaming work (`nats-server` + `laminardb`);
three containers (RustFS, Postgres, Lakekeeper) provide the lakehouse.
RustFS is the S3-compatible Rust object store standing in for MinIO.

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
# 1. NATS + RustFS + Postgres + Lakekeeper. The lakekeeper-init container
#    bootstraps the catalog and creates the `demo` warehouse pointing at
#    the `warehouse` RustFS bucket.
docker compose -f examples/nats-payments/docker-compose.yml up -d

# 2. Build laminardb (skip postgres-cdc/mysql-cdc — they pull native
#    OpenSSL via Perl on Windows). NATS, Iceberg, and websocket are all
#    you need for this demo.
cargo build --release -p laminar-server \
    --no-default-features \
    --features mimalloc,nats,iceberg,websocket

# 3. Run the server.
./target/release/laminardb --config examples/nats-payments/config.toml

# 4. In another terminal, start the publisher (defaults to 10K msg/s).
pip install nats-py
python examples/nats-payments/gen.py            # 10_000/s, 4 producers
# python examples/nats-payments/gen.py --rate 50000   # 50K/s
# python examples/nats-payments/gen.py --rate 0       # flat-out

# 5. Wait ~90 seconds (one tumbling minute closes + first commit), then query.
pip install duckdb
python examples/nats-payments/query.py
```

## What the pipeline does

```sql
SELECT
    region, method,
    COUNT(*)                                            AS payment_count,
    SUM(amount_usd)                                     AS total_usd,
    AVG(amount_usd)                                     AS avg_usd,
    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END)  AS failed_count
FROM payments
GROUP BY
    TUMBLE(event_time, INTERVAL '1' MINUTE),
    region, method
EMIT ON WINDOW CLOSE
```

`window_start` and `window_end` are auto-projected at the head of the
SELECT by the streaming-windowed-GROUP-BY rewriter, so the Iceberg
table ends up with eight columns.

## Throughput + latency

Two ways to see the engine work:

```bash
# Live throughput off the /metrics endpoint (Ctrl-C for summary).
python examples/nats-payments/bench.py
```

Sample output (10K/s sustained):

```
    time   ingest/s   flushed/s  commits   total_in   total_out
14:32:01    10,012        0        0      10,012          0
14:32:02    10,005        0        0      20,017          0
...
14:33:00    10,001    1,920       1     601,210      1,920
```

Latency percentiles, computed off committed Iceberg rows:

```
python examples/nats-payments/query.py | tail -20
```

```
  n  p50_close_ms  p95_close_ms  p99_close_ms  p50_e2e_ms  p95_e2e_ms  p99_e2e_ms
 32         3000         5500          6000        4200        7200        8100
```

* `close_latency_ms` — between `window_end` and the emit timestamp the
  pipeline stamped on the row. How long after the window logically
  closed did the engine commit. Bounded by `[checkpoint] interval`.
* `end_to_end_ms` — between the freshest `event_time` in the window and
  the emit timestamp. Publish → readable in Iceberg.

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
| `config.toml`        | Source, pipeline, sink — what `laminardb` actually reads |
| `pipeline.sql`       | Reference copy of the SELECT (config has the live one) |
| `docker-compose.yml` | NATS + RustFS + Postgres + Lakekeeper                  |
| `gen.py`             | NATS publisher — `pip install nats-py`                 |
| `bench.py`           | live `/metrics` scraper for ingest + commit rates      |
| `query.py`           | DuckDB Iceberg reader + latency percentiles            |
