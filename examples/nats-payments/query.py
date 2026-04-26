#!/usr/bin/env python3
"""
Reads payment_summary committed by laminardb to MinIO and prints a
window-by-window rollup.

    pip install duckdb
    python query.py

Run after ~90 seconds of `gen.py` so at least one tumbling minute has
closed and the sink has committed.

We `parquet_scan` the data files directly instead of going through
DuckDB's iceberg_scan(REST). Lakekeeper bakes its configured endpoint
(`http://minio:9000`) into table metadata, and DuckDB's iceberg
extension follows that endpoint as-is from the host where `minio`
doesn't resolve. The Parquet files themselves live in MinIO and read
fine over the host-published `localhost:9000` endpoint.
"""

import os
import duckdb


WAREHOUSE_GLOB = os.environ.get(
    "PAYMENT_SUMMARY_GLOB",
    "s3://warehouse/finance/*/*/data/*.parquet",
)

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")
con.execute("""
    SET s3_endpoint          = 'localhost:9000';
    SET s3_access_key_id     = 'minioadmin';
    SET s3_secret_access_key = 'minioadmin';
    SET s3_use_ssl           = false;
    SET s3_url_style         = 'path';
""")

print("\n--- payment summary by region and method ---------------------")
df = con.execute(f"""
    SELECT
        epoch_ms(window_start)::TIMESTAMP AS window_start,
        epoch_ms(window_end)::TIMESTAMP   AS window_end,
        region,
        method,
        payment_count,
        ROUND(total_usd, 2) AS total_usd,
        ROUND(avg_usd, 2)   AS avg_usd,
        failed_count
    FROM parquet_scan('{WAREHOUSE_GLOB}')
    ORDER BY window_start DESC, total_usd DESC
    LIMIT 40
""").fetchdf()

print(df.to_string(index=False))
print(f"\n{len(df)} rows")
