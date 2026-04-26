#!/usr/bin/env python3
"""
Reads the demo's two Iceberg tables via Lakekeeper:

* finance.payments_summary           — tumbling 1-min rollup
* finance.payments_with_fraud_score  — payments ⨝ fraud-score (2s window)

    pip install duckdb
    python query.py

Run AFTER stopping gen.py — the tables accumulate parquet files fast
at 10K/s, and DuckDB's concurrent httpfs reads contend with
laminardb's ongoing writes against RustFS. Stopping the publisher
gives DuckDB an idle store to scan.

Prerequisite: `rustfs` must resolve to `127.0.0.1` from the host so
DuckDB can fetch manifest paths Lakekeeper bakes into table metadata.
See README.
"""

import time

import duckdb


con = duckdb.connect()
con.execute("INSTALL iceberg FROM core_nightly;")
con.execute("LOAD iceberg;")
con.execute("INSTALL httpfs; LOAD httpfs;")
# Throttle parallel httpfs GETs — at 10K/s ingest the table accumulates
# many small parquet files and RustFS occasionally drops connections
# under DuckDB's default concurrency.
con.execute("SET threads=2;")

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


def materialise(local_name: str, source: str, attempts: int = 3) -> None:
    """Materialise an Iceberg table into a local DuckDB table; retry on
    transient httpfs connect errors."""
    for i in range(attempts):
        try:
            con.execute(f"CREATE OR REPLACE TABLE {local_name} AS SELECT * FROM {source}")
            return
        except duckdb.IOException as e:
            if i == attempts - 1:
                raise
            print(f"  retry {local_name} ({i+1}/{attempts}) after: {e}", flush=True)
            time.sleep(1.0)


print("loading payments_summary ...", flush=True)
materialise("summary", "catalog.finance.payments_summary")
print("loading payments_with_fraud_score ...", flush=True)
materialise("joined", "catalog.finance.payments_with_fraud_score")


def section(title: str) -> None:
    print(f"\n--- {title} " + "-" * max(0, 60 - len(title) - 5))


section("payments rollup by region and method (latest 16)")
df = con.execute("""
    SELECT
        epoch_ms(window_start)::TIMESTAMP AS window_start,
        epoch_ms(window_end)::TIMESTAMP   AS window_end,
        region, method,
        payment_count,
        ROUND(total_usd, 2) AS total_usd,
        ROUND(avg_usd, 2)   AS avg_usd
    FROM summary
    ORDER BY window_start DESC, total_usd DESC
    LIMIT 16
""").fetchdf()
print(df.to_string(index=False))

section("payments matched with fraud score (latest 20)")
df = con.execute("""
    SELECT payment_id, region, method,
           ROUND(amount_usd, 2) AS amount_usd,
           fraud_score, outcome,
           score_latency_ms
    FROM joined
    ORDER BY scored_at DESC
    LIMIT 20
""").fetchdf()
print(df.to_string(index=False))

section("fraud-check latency by region")
df = con.execute("""
    SELECT
        region,
        COUNT(*)                                                       AS scored,
        CAST(quantile_cont(score_latency_ms, 0.50) AS BIGINT)          AS p50_ms,
        CAST(quantile_cont(score_latency_ms, 0.95) AS BIGINT)          AS p95_ms,
        CAST(quantile_cont(score_latency_ms, 0.99) AS BIGINT)          AS p99_ms,
        CAST(SUM(CASE WHEN outcome = 'blocked' THEN 1 ELSE 0 END) AS BIGINT) AS blocked,
        CAST(SUM(CASE WHEN outcome = 'review'  THEN 1 ELSE 0 END) AS BIGINT) AS review
    FROM joined
    GROUP BY region
    ORDER BY region
""").fetchdf()
print(df.to_string(index=False))
