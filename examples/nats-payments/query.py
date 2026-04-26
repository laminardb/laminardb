#!/usr/bin/env python3
"""
Reads payment_summary from Lakekeeper via DuckDB's iceberg extension
and prints both the windowed rollup and engine latency percentiles.

    pip install duckdb
    python query.py

Run after ~90 seconds of `gen.py` so at least one tumbling minute has
closed and the sink has committed.

Prerequisite: `rustfs` must resolve to `127.0.0.1` from the host so
DuckDB can fetch the manifest paths Lakekeeper bakes into table
metadata. See README.

Latency definitions:
* close_latency_ms — between window_end and emitted_at; measures how
  long after a window logically closed the engine produced its row.
  Real "engine commit cadence" indicator.
* end_to_end_ms — between max event_time in the window and emitted_at;
  measures publish→materialized lag for the freshest event in each row.
"""

import duckdb


con = duckdb.connect()
con.execute("FORCE INSTALL iceberg FROM core_nightly;")
con.execute("LOAD iceberg;")
con.execute("INSTALL httpfs; LOAD httpfs;")

con.execute("""
    CREATE SECRET rustfs (
        TYPE      S3,
        KEY_ID    'rustfsadmin',
        SECRET    'rustfsadmin',
        ENDPOINT  'rustfs:9000',
        URL_STYLE 'path',
        USE_SSL   false
    )
""")

con.execute("""
    ATTACH 'demo' AS catalog (
        TYPE               ICEBERG,
        ENDPOINT           'http://localhost:8181/catalog',
        AUTHORIZATION_TYPE 'none'
    )
""")

# Pull every row into a temp view we can query twice (rollup + percentiles).
con.execute("""
    CREATE OR REPLACE TEMP VIEW summary AS
    SELECT
        epoch_ms(window_start)::TIMESTAMP AS window_start,
        epoch_ms(window_end)::TIMESTAMP   AS window_end,
        region,
        method,
        payment_count,
        ROUND(total_usd, 2) AS total_usd,
        ROUND(avg_usd, 2)   AS avg_usd,
        failed_count,
        emitted_at,
        max_event_time,
        date_diff('millisecond', epoch_ms(window_end)::TIMESTAMP, emitted_at)
            AS close_latency_ms,
        date_diff('millisecond', max_event_time, emitted_at)
            AS end_to_end_ms
    FROM catalog.finance.payment_summary
""")

print("\n--- payment summary by region and method (latest 40) ---------")
df = con.execute("""
    SELECT window_start, window_end, region, method,
           payment_count, total_usd, avg_usd, failed_count
    FROM summary
    ORDER BY window_start DESC, total_usd DESC
    LIMIT 40
""").fetchdf()
print(df.to_string(index=False))
print(f"\n{len(df)} rows")

print("\n--- engine latency (over all committed window-rows) ----------")
lat = con.execute("""
    SELECT
        COUNT(*)                                            AS n,
        CAST(quantile_cont(close_latency_ms,  0.50) AS BIGINT) AS p50_close_ms,
        CAST(quantile_cont(close_latency_ms,  0.95) AS BIGINT) AS p95_close_ms,
        CAST(quantile_cont(close_latency_ms,  0.99) AS BIGINT) AS p99_close_ms,
        CAST(quantile_cont(end_to_end_ms,     0.50) AS BIGINT) AS p50_e2e_ms,
        CAST(quantile_cont(end_to_end_ms,     0.95) AS BIGINT) AS p95_e2e_ms,
        CAST(quantile_cont(end_to_end_ms,     0.99) AS BIGINT) AS p99_e2e_ms
    FROM summary
""").fetchdf()
print(lat.to_string(index=False))
