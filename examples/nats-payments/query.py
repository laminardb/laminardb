#!/usr/bin/env python3
"""
Reads payment_summary from Lakekeeper via DuckDB's iceberg extension.

    pip install duckdb
    python query.py

Run after ~90 seconds of `gen.py` so at least one tumbling minute has
closed and the sink has committed.

Prerequisite: `rustfs` must resolve to `127.0.0.1` from the host so
DuckDB can fetch the manifest paths Lakekeeper bakes into table
metadata. See README.

Throughput and commit cadence (real engine signals) come from
bench.py scraping /metrics — this script is the readback.
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

print("\n--- payment summary by region and method (latest 40) ---------")
df = con.execute("""
    SELECT
        epoch_ms(window_start)::TIMESTAMP AS window_start,
        epoch_ms(window_end)::TIMESTAMP   AS window_end,
        region,
        method,
        payment_count,
        ROUND(total_usd, 2) AS total_usd,
        ROUND(avg_usd, 2)   AS avg_usd,
        failed_count
    FROM catalog.finance.payment_summary
    ORDER BY window_start DESC, total_usd DESC
    LIMIT 40
""").fetchdf()
print(df.to_string(index=False))
print(f"\n{len(df)} rows")
