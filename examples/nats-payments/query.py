#!/usr/bin/env python3
"""
Reads the demo's two Iceberg tables via Lakekeeper:

* finance.payments_summary           — tumbling 1-min rollup
* finance.payments_with_fraud_score  — payments ⨝ fraud-score (2s window)

    pip install duckdb
    python query.py

Run after ~90 seconds of `gen.py` so at least one tumbling minute has
closed and join rows have flowed.

Prerequisite: `rustfs` must resolve to `127.0.0.1` from the host so
DuckDB can fetch manifest paths Lakekeeper bakes into table metadata.
See README.
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
    FROM catalog.finance.payments_summary
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
    FROM catalog.finance.payments_with_fraud_score
    ORDER BY scored_at DESC
    LIMIT 20
""").fetchdf()
print(df.to_string(index=False))

section("fraud-check latency by region")
df = con.execute("""
    SELECT
        region,
        COUNT(*)                                        AS scored,
        CAST(quantile_cont(score_latency_ms, 0.50) AS BIGINT) AS p50_ms,
        CAST(quantile_cont(score_latency_ms, 0.95) AS BIGINT) AS p95_ms,
        CAST(quantile_cont(score_latency_ms, 0.99) AS BIGINT) AS p99_ms,
        SUM(CASE WHEN outcome = 'blocked' THEN 1 ELSE 0 END)  AS blocked,
        SUM(CASE WHEN outcome = 'review'  THEN 1 ELSE 0 END)  AS review
    FROM catalog.finance.payments_with_fraud_score
    GROUP BY region
    ORDER BY region
""").fetchdf()
print(df.to_string(index=False))
