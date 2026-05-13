# Kafka → Iceberg time-series demo

Five-minute walkthrough: Redpanda topic of synthetic crypto trade ticks
→ a 1-minute OHLC materialized view in LaminarDB → Apache Iceberg table
on MinIO → DuckDB read-back with a tick-weighted hourly VWAP.

## Prerequisites

- Docker + Compose v2
- Rust toolchain matching `rust-toolchain.toml` (1.85)
- Python 3.10+ with `confluent-kafka` (`pip install confluent-kafka`)
- DuckDB 1.5+ (`winget install DuckDB.cli` / `brew install duckdb`)

The DuckDB `iceberg` and `httpfs` extensions auto-install on first run.

## Files

```
examples/kafka-iceberg-timeseries/
├── docker-compose.yml   # redpanda + minio + iceberg-rest + bucket init
├── generate_ticks.py    # synthetic-tick producer (BTC-USD/ETH-USD/SOL-USD)
├── demo.sql             # source → 1-minute OHLC MV → Iceberg sink (reference; baked into laminar.toml)
├── laminar.toml         # server config that bootstraps demo.sql before pipeline start
├── query.sql            # DuckDB read-back: bars + hourly VWAP via iceberg-REST catalog
└── README.md            # you are here
```

## Run

### 1. Start the infra

```sh
docker compose up -d
docker exec laminar-demo-redpanda rpk topic create crypto.ticks --partitions 3 --replicas 1
```

### 2. Start LaminarDB with the demo config

The pipeline DDL (`CREATE SOURCE`, `CREATE MATERIALIZED VIEW`, `CREATE
SINK`) is embedded in `laminar.toml` as the top-level `sql = """..."""`
field and runs before `pipeline.start()`. Streaming-DDL is rejected on
pgwire and on the HTTP `/api/v1/sql` endpoint after pipeline start, so
the config TOML is the only working bootstrap channel.

PowerShell:

```powershell
cd examples\kafka-iceberg-timeseries
$env:RUST_LOG = "info"
..\..\target\release\laminardb.exe --config laminar.toml
```

bash / zsh:

```sh
cd examples/kafka-iceberg-timeseries
RUST_LOG=info ../../target/release/laminardb --config laminar.toml
```

Wait until you see `HTTP API listening on 127.0.0.1:7777`. Verify the
three pipeline objects:

```sh
curl -s http://127.0.0.1:7777/api/v1/sources
curl -s http://127.0.0.1:7777/api/v1/sinks
curl -s http://127.0.0.1:7777/api/v1/streams
```

### 3. Drive the source

```sh
python generate_ticks.py --rate 100 --duration 180
```

~18,000 ticks across three symbols over 3 minutes — enough for two to
three complete 1-minute windows plus watermark advancement. Watch the
server log: each closed window produces an `iceberg commit succeeded
epoch=N rows=3` line (3 rows = one per symbol).

### 4. Read the Iceberg table from DuckDB

PowerShell:

```powershell
Get-Content query.sql | duckdb
```

bash / zsh:

```sh
duckdb < query.sql
```

First block: most recent 30 OHLC bars. Second block: hourly VWAP
(`SUM(notional) / SUM(volume)` — never `AVG(close)` and never the
weighted-incorrect `AVG(notional / volume)`).

`query.sql` reads via the iceberg-REST catalog (`AUTHORIZATION_TYPE
'none'`, port 8181), not by globbing `s3://`. DuckDB 1.5.x refuses
filesystem-glob Iceberg reads without `unsafe_enable_version_guessing`,
and even when forced can land on stale `metadata.json` from prior runs
still in the persistent MinIO volume. The catalog path always returns
the current snapshot.

### 5. Cleanup

```sh
docker compose down -v
```

`-v` wipes the named volumes (Iceberg manifests, Parquet files, Kafka
log segments).

## What the pipeline does

| Stage | Behavior |
|---|---|
| `crypto_ticks` source | Reads `crypto.ticks` Kafka topic with `event.time.column = ts` and `WATERMARK FOR ts AS ts - INTERVAL '5' SECOND` (allows 5s of out-of-orderness before a window closes). |
| `ohlc_1m` MV | `GROUP BY TUMBLE(ts, INTERVAL '1' MINUTE), symbol` produces non-overlapping per-symbol 1-minute buckets. `EMIT ON WINDOW CLOSE` makes the MV append-only — required by Iceberg's exactly-once 2PC contract. |
| `ohlc_iceberg_sink` | 2PC commit per epoch (`exactly_once = true`, `two_phase_commit = true`). `auto.create='true'` authors the unpartitioned table on first commit. |
| `query.sql` | Reads the Iceberg table through the iceberg-REST catalog; computes tick-weighted hourly VWAP from per-bar `notional` and `volume` so per-tick weighting is preserved across bars of different volume. |

## Gotchas

- **Window close is watermark-driven.** A 1-minute window only emits
  once the watermark passes `window_end + 5s`. After the producer stops
  the watermark stalls, so the final partial minute won't commit until
  more events arrive.
- **`FIRST_VALUE` / `LAST_VALUE` for open/close are order-sensitive.**
  The producer partitions by symbol and Kafka preserves per-partition
  order, so per-symbol tick order equals event-time order within a
  window. Replaying out-of-order data through a single Kafka partition
  makes the open/close columns unspecified.
- **Iceberg writes are unpartitioned.** To partition by, say,
  `days(window_start)` or `bucket(N, symbol)`, pre-create the table in
  the catalog with the desired spec and set `auto.create='false'`.
