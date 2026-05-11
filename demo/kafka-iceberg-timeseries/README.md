# Kafka → Iceberg time-series demo

> ## STATUS: PARTIALLY WORKING — DO NOT TREAT AS A SHIPPED REFERENCE
>
> Bringing this demo up against `0.21.10` revealed real issues:
>
> 1. **The end-to-end commit path is currently blocked.** The Iceberg
>    sink rejects every checkpoint with `schema mismatch: Iceberg
>    column 'window_start' is NOT NULL but missing from pipeline`.
>    Auto-created Iceberg table has `window_start required=true,
>    type=long`, but the per-`pre_commit` batches arriving at the sink
>    don't carry that column. **Zero rows land in Iceberg.** Tracked
>    as INDEX-C0; it is a runtime inconsistency between
>    `auto.create='true'` schema inference and the MV's actual emit
>    path. Until that is fixed in laminar-db, step 5 below
>    (`duckdb < query.sql`) will return an empty table.
> 2. The original README told you to apply `demo.sql` via `psql -f` on
>    pgwire. **That doesn't work** — pgwire rejects all three streaming
>    DDL statements with `not supported on pgwire (use HTTP /api/v1/sql)`
>    (see INDEX-C4). The corrected workflow uses the config TOML's
>    `sql = """..."""` field as the bootstrap channel, which runs the
>    DDL **before** `pipeline.start()` (see INDEX-C5). The 5-step run
>    below has been rewritten accordingly.
> 3. The shipped `docker-compose.yml` had a fabricated
>    `apache/iceberg-rest-fixture:1.6.1` tag (no such tag exists; pinned
>    to `1.9.2` now) and a `redpanda-init` container that fails to
>    create the topic (must be done manually with
>    `docker exec laminar-demo-redpanda rpk topic create crypto.ticks`).
>    See INDEX-S5 / INDEX-S6.
>
> The audit-doc and post-drafts have been updated so any prose you read
> elsewhere reflects this state. Do not republish the LinkedIn / HN
> drafts under `.claude/post-drafts/POSTS.md` until C0 is closed.

Five-minute walkthrough: Redpanda topic of synthetic crypto trade ticks
→ a 1-minute OHLC materialized view in LaminarDB → Apache Iceberg
table on MinIO → DuckDB read-back with a tick-weighted hourly VWAP.

Every keyword and option key in `demo.sql` traces to a finding in
[`AUDIT.md`](../../.claude/post-drafts/AUDIT.md). Limitations are
called out in **Current limitations** below — they are not worked
around silently.

## Prerequisites

- Docker + Compose v2
- Rust toolchain matching `rust-toolchain.toml` (1.85)
- `psql` (libpq client)
- Python 3.10+ with `confluent-kafka` (`pip install confluent-kafka`)
- DuckDB 0.10+ (`brew install duckdb` / `apt install duckdb`)

The Iceberg extension and httpfs are auto-installed by DuckDB on first
run; you need outbound HTTP for that one fetch only.

## Layout

```
demo/kafka-iceberg-timeseries/
├── docker-compose.yml      # Redpanda + MinIO + iceberg-rest + bucket/topic init
├── generate_ticks.py       # synthetic-tick producer (BTC-USD/ETH-USD/SOL-USD)
├── demo.sql                # source → 1-minute OHLC MV → Iceberg sink
├── query.sql               # DuckDB read-back: bars + hourly VWAP
└── README.md               # you are here
```

## 5-step run (corrected after the bring-up audit)

1. **Start the infra.**

   ```sh
   docker compose up -d
   docker compose ps             # wait for redpanda/minio/iceberg-rest to be healthy
   docker exec laminar-demo-redpanda \
       rpk topic create crypto.ticks --partitions 3 --replicas 1
   ```

   The manual topic create is per INDEX-S6: the `redpanda-init`
   container's `rpk` invocation tries `[::1]:9092` and fails.

2. **Bake the pipeline DDL into a config file.** `laminar.toml` (in
   this directory) does this — note that the `sql = """..."""` block
   sits **above** `[server]`. TOML scoping silently re-parents keys
   below a table header, so `sql` placed under `[server]` becomes
   `server.sql` (no such field, silently ignored — INDEX-S9).

3. **Start LaminarDB.** The bootstrap SQL runs before
   `pipeline.start()` (`server.rs:321-334`). Streaming-DDL `CREATE
   SOURCE / SINK / MV` is rejected on pgwire (INDEX-C4) and rejected
   over HTTP after the pipeline is running (INDEX-C5), so this
   is the channel.

   ```sh
   cargo run --release -p laminar-server -- --config laminar.toml
   ```

   Verify three pipeline objects landed:

   ```sh
   curl -s http://127.0.0.1:7777/api/v1/sources
   curl -s http://127.0.0.1:7777/api/v1/sinks
   curl -s http://127.0.0.1:7777/api/v1/streams
   ```

4. **Drive the source.**

   ```sh
   pip install confluent-kafka
   python generate_ticks.py --rate 100 --duration 180
   ```

   ~18 000 ticks across three symbols over 3 minutes — enough for two
   complete 1-minute windows plus watermark advancement.

5. **Read the Iceberg table from DuckDB.**

   ```sh
   duckdb < query.sql
   ```

   First block: most recent 30 OHLC bars. Second block: hourly VWAP
   (`SUM(notional) / SUM(volume)` — never AVG-of-AVG).

   **In the current `0.21.10` state this returns an empty result** —
   the sink commit fails on every epoch (INDEX-C0). The Iceberg table
   itself is created (you can see its schema at
   `http://127.0.0.1:8181/v1/namespaces/demo/tables/ohlc_1m`), but no
   data files have been committed.

## What it actually does

| File / step | Does |
|---|---|
| `crypto_ticks` source | Reads `crypto.ticks` Kafka topic with `event.time.column = ts` and a `WATERMARK FOR ts AS ts - INTERVAL '5' SECOND` — i.e. allows 5 s of out-of-orderness before a window closes. (`source_parser.rs:184-238`, `kafka/config.rs:800`) |
| `ohlc_1m` MV | `GROUP BY TUMBLE(ts, INTERVAL '1' MINUTE), symbol` produces non-overlapping 1-minute buckets per symbol. `EMIT ON WINDOW CLOSE` makes the MV append-only — required by Iceberg's exactly-once 2PC contract. (`window_rewriter.rs:60-85,217-230`; `EmitClause::OnWindowClose` at `statements.rs:425-427`) |
| `ohlc_iceberg_sink` | 2PC commit per epoch: `IcebergSink::capabilities` advertises `exactly_once = true`, `two_phase_commit = true`, `partitioned = false` (`iceberg.rs:496-497,611-613`). `auto.create='true'` authors the table on first commit. |
| `query.sql` | Reads the Iceberg manifest via DuckDB's iceberg extension; computes hourly VWAP from `SUM(notional)/SUM(volume)` so per-tick weighting is preserved across bars of different volume. |

## Current limitations (taken verbatim from AUDIT.md)

These are real gaps in the `0.21.10` codebase, not aspirational TODOs.
They are listed here because each one shapes a choice in `demo.sql`.

| AUDIT § | Tag | Impact on this demo |
|---|---|---|
| §1.5 [GAP] | `arg_min(price, ts)` / `arg_max(price, ts)` not in the aggregate registry | Demo uses `FIRST_VALUE(price)` / `LAST_VALUE(price)`. These are flagged order-sensitive (`aggregation_parser.rs:99-109`); the producer keys by symbol so per-partition order ⇒ per-symbol time order, which makes the open/close correct in practice. If you replay out-of-order data into a single Kafka partition, those columns become unspecified. |
| §2.2 [GAP] | Iceberg sink writes unpartitioned files (`caps.partitioned == false`, `iceberg.rs:613`) | `ohlc_1m` is laid down without partition transforms (`days(window_start)`, `bucket(N, symbol)`, …). Read-back works fine; query performance over months of data won't. To partition, pre-create the table in the catalog with the desired spec and set `auto.create='false'`. |
| §3.1 / §3.2 [PARTIAL] | Ring 0/1/2 boundary is convention + a workspace `clippy.toml disallowed-types` lint, not a compiler-checked tier | Doesn't change demo semantics. Listed here so claims downstream don't overstate the enforcement. |
| §3.6 [GAP] | `core_affinity` is a transitive dep of `foyer-storage`, not used anywhere in `crates/` to pin the compute thread | The `laminar-compute` thread runs a `tokio::runtime::Builder::new_current_thread()` runtime (`pipeline_lifecycle.rs:1454`); it is **not** CPU-pinned. Don't expect deterministic single-core latency. |
| §1.4 [GAP] | No MONTH / YEAR / WEEK interval units | All windows in the demo are MINUTE-scale. Use HOUR/DAY for longer rollups; do calendar windows in the read-side query (DuckDB has them). |
| §1.3 [GAP] | Window functions (`TUMBLE`, `HOP`, …) are GROUP BY expressions only — there is no `FROM TUMBLE(events, …)` table-valued form | Demo uses the GROUP BY form. |
| §3.5 [GAP] | No OTLP **sink** in the codebase | Mentioned because earlier prose has paired it with Kafka; OTLP is source-only here. Iceberg / Kafka / NATS / Postgres / Files / Delta sinks are the ones that opt into 2PC + exactly-once. WebSocket sink does not. |
| §5 [GAP] | No `laminar validate file.sql` CLI subcommand | Step 0 of this README would be "validate before applying" but there is no CLI. Programmatic equivalent is `laminar_sql::parser::parse_streaming_sql(&sql)`. A small standalone validator lives in `validate/` — `(cd validate && cargo run --release -- ../demo.sql)`. It splits on `;` (string-aware) and parses each statement; non-zero exit on the first parse error. |

> If any later prose (LinkedIn / r/rust / HN) cites a feature that's
> tagged [GAP] above, it is wrong. The fix is to rewrite the prose,
> not to handwave the gap.

## Cleanup

```sh
docker compose down -v        # wipes the named volumes too
```

The Iceberg manifests and Parquet files live inside the `minio-data`
named volume; `down -v` removes them.
