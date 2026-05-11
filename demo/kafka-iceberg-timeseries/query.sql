-- query.sql — DuckDB read-back for the OHLC bars produced by demo.sql.
--
-- Run from a DuckDB shell with no arguments other than this file:
--     duckdb -init query.sql
-- or, to feed straight into stdout:
--     duckdb < query.sql
--
-- The Iceberg extension is downloaded on first INSTALL and cached;
-- network access is required only on the first run.

INSTALL iceberg;
LOAD iceberg;
INSTALL httpfs;
LOAD httpfs;

-- MinIO is reached over plaintext HTTP path-style. The DuckDB s3_*
-- settings only affect THIS DuckDB process — they don't leak to the
-- iceberg-rest catalog or to laminar-server.
SET s3_endpoint           = 'localhost:9000';
SET s3_access_key_id      = 'minioadmin';
SET s3_secret_access_key  = 'minioadmin';
SET s3_url_style          = 'path';
SET s3_use_ssl            = false;
SET s3_region             = 'us-east-1';

-- ──────────────────────────────────────────────────────────────────────
-- Most recent 1-minute bars (the raw output of the materialized view).
-- ──────────────────────────────────────────────────────────────────────
.print
.print "── 1-minute OHLC bars (most recent first) ──"
SELECT
    window_start,
    window_end,
    symbol,
    open,
    high,
    low,
    close,
    volume,
    notional,
    tick_count
FROM iceberg_scan(
    's3://warehouse/wh/demo/ohlc_1m',
    allow_moved_paths = true
)
ORDER BY window_start DESC, symbol
LIMIT 30;

-- ──────────────────────────────────────────────────────────────────────
-- Tick-weighted hourly VWAP rollup.
--
-- VWAP = SUM(price * qty) / SUM(qty), aggregated over all ticks in the
-- hour. We rebuild it from the per-bar `notional` and `volume` columns
-- — never AVG(close) and never AVG(notional / volume), both of which
-- are weighted-incorrectly when bar volumes differ.
-- ──────────────────────────────────────────────────────────────────────
.print
.print "── Tick-weighted hourly VWAP (last 24 hour-buckets per symbol) ──"
WITH bars AS (
    SELECT *
    FROM iceberg_scan(
        's3://warehouse/wh/demo/ohlc_1m',
        allow_moved_paths = true
    )
)
SELECT
    symbol,
    date_trunc('hour', window_start)         AS hour,
    SUM(notional) / NULLIF(SUM(volume), 0)   AS vwap,
    SUM(volume)                              AS hourly_volume,
    SUM(tick_count)                          AS hourly_ticks
FROM bars
GROUP BY symbol, date_trunc('hour', window_start)
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY symbol
    ORDER BY hour DESC
) <= 24
ORDER BY hour DESC, symbol;
