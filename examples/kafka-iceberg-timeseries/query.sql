-- query.sql — DuckDB read-back for the OHLC bars produced by demo.sql.
--
-- Reads the Iceberg table via the iceberg-rest catalog rather than by
-- globbing the warehouse bucket. The catalog path is correct: it always
-- resolves to the table's current snapshot, even when stale metadata
-- files from earlier runs sit in MinIO. DuckDB 1.5.x refuses the
-- filesystem-glob path by default (`unsafe_enable_version_guessing`),
-- and even when forced it can land on an old `metadata.json`.
--
-- Run:
--     duckdb < query.sql
--
-- The Iceberg and httpfs extensions are downloaded on first INSTALL and
-- cached; network access is required only on the first run.

INSTALL iceberg;
LOAD iceberg;
INSTALL httpfs;
LOAD httpfs;

-- S3 credentials for the data files (MinIO). The catalog ATTACH below
-- handles metadata; the data-file reads still go straight to s3://.
CREATE OR REPLACE SECRET demo_s3 (
    TYPE S3,
    KEY_ID 'minioadmin',
    SECRET 'minioadmin',
    ENDPOINT 'localhost:9000',
    URL_STYLE 'path',
    USE_SSL false,
    REGION 'us-east-1'
);

-- iceberg-rest is unauthenticated in the demo compose stack, hence
-- AUTHORIZATION_TYPE 'none'. The catalog is reachable at port 8181.
ATTACH 'demo_iceberg' AS demo_cat (
    TYPE ICEBERG,
    ENDPOINT 'http://localhost:8181',
    AUTHORIZATION_TYPE 'none'
);

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
    ROUND(volume, 4)   AS volume,
    ROUND(notional, 2) AS notional,
    tick_count
FROM demo_cat.demo.ohlc_1m
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
    SELECT * FROM demo_cat.demo.ohlc_1m
)
SELECT
    symbol,
    date_trunc('hour', window_start)                     AS hour,
    ROUND(SUM(notional) / NULLIF(SUM(volume), 0), 2)     AS vwap,
    ROUND(SUM(volume), 4)                                AS hourly_volume,
    SUM(tick_count)                                      AS hourly_ticks
FROM bars
GROUP BY symbol, date_trunc('hour', window_start)
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY symbol
    ORDER BY hour DESC
) <= 24
ORDER BY hour DESC, symbol;
