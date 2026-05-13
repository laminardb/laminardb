-- demo.sql — Kafka → 1-minute OHLC → Iceberg
--
-- Each clause traces to an audited grammar production. Citations are to
-- AUDIT.md sections (§N.M).
--
-- ──────────────────────────────────────────────────────────────────────
-- 1. Kafka source: JSON ticks with a 5-second event-time watermark.
--    - CREATE SOURCE … FROM KAFKA(…) FORMAT JSON     (§1.1, §2.1, §1.7)
--    - WATERMARK FOR ts AS ts - INTERVAL '5' SECOND   (§1.2, §1.4)
--    - INTERVAL units restricted to MS/SECOND/MINUTE/HOUR/DAY (§1.4)
-- ──────────────────────────────────────────────────────────────────────
CREATE SOURCE crypto_ticks (
    ts        TIMESTAMP,
    symbol    VARCHAR,
    price     DOUBLE,
    qty       DOUBLE,
    WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
) FROM KAFKA (
    'bootstrap.servers'      = 'localhost:9092',
    'topic'                  = 'crypto.ticks',
    'group.id'               = 'laminar-ohlc-demo',
    'startup.mode'           = 'earliest',
    'event.time.column'      = 'ts',
    'max.out.of.orderness.ms' = '5000'
) FORMAT JSON;

-- ──────────────────────────────────────────────────────────────────────
-- 2. 1-minute OHLC materialized view.
--    - CREATE MATERIALIZED VIEW … AS SELECT … EMIT …  (§1.1, §1.8)
--    - GROUP BY TUMBLE(ts, INTERVAL '1' MINUTE)       (§1.3)
--    - FIRST_VALUE / LAST_VALUE for open / close      (§1.5)
--      Substitution: arg_min(price, ts) / arg_max(price, ts) would be
--      the textbook pick, but the aggregate registry has no arg_min /
--      arg_max / min_by / max_by (AUDIT §1.5 [GAP]). FIRST_VALUE and
--      LAST_VALUE are flagged order-sensitive
--      (aggregation_parser.rs:99-109); the generator partitions by
--      symbol and Kafka preserves per-partition order, so per-symbol
--      tick order is the same as event-time order within a 1-minute
--      window.
--    - SUM(price * qty) drives the tick-weighted hourly VWAP rollup
--      computed in query.sql.                         (prompt requirement)
--    - EMIT ON WINDOW CLOSE: append-only, exactly the Iceberg sink
--      contract (statements.rs:425-427, EmitClause::OnWindowClose).
-- ──────────────────────────────────────────────────────────────────────
CREATE MATERIALIZED VIEW ohlc_1m AS
SELECT
    TUMBLE(ts, INTERVAL '1' MINUTE)     AS window_start,
    TUMBLE_END(ts, INTERVAL '1' MINUTE) AS window_end,
    symbol,
    FIRST_VALUE(price) AS open,
    MAX(price)         AS high,
    MIN(price)         AS low,
    LAST_VALUE(price)  AS close,
    SUM(qty)           AS volume,
    SUM(price * qty)   AS notional,
    COUNT(*)           AS tick_count
FROM crypto_ticks
GROUP BY
    TUMBLE(ts, INTERVAL '1' MINUTE),
    TUMBLE_END(ts, INTERVAL '1' MINUTE),
    symbol
EMIT ON WINDOW CLOSE;

-- ──────────────────────────────────────────────────────────────────────
-- 3. Iceberg sink for the OHLC bars.
--    - CREATE SINK <name> FROM <table> INTO ICEBERG(…) (§1.6, §2.2)
--    - Required keys: catalog.uri, warehouse, namespace, table.name
--      (iceberg_config.rs:76-80)
--    - auto.create='true' lets the sink author the table on first
--      epoch; the inferred schema is unpartitioned because the sink's
--      capabilities expose `partitioned: false`
--      (iceberg.rs:613, AUDIT §2.2 [GAP]). For partitioned writes,
--      pre-create the table in the catalog with the desired partition
--      spec and set auto.create='false'.
--    - catalog.property.* keys are forwarded verbatim to the
--      iceberg-rust REST catalog (iceberg_config.rs:82).
-- ──────────────────────────────────────────────────────────────────────
CREATE SINK ohlc_iceberg_sink FROM ohlc_1m
INTO ICEBERG (
    'catalog.uri'                            = 'http://localhost:8181',
    'warehouse'                              = 's3://warehouse/wh',
    'namespace'                              = 'demo',
    'table.name'                             = 'ohlc_1m',
    'storage.type'                           = 's3',
    'auto.create'                            = 'true',
    'compression'                            = 'zstd',
    'catalog.property.s3.endpoint'           = 'http://localhost:9000',
    'catalog.property.s3.access-key-id'      = 'minioadmin',
    'catalog.property.s3.secret-access-key'  = 'minioadmin',
    'catalog.property.s3.path-style-access'  = 'true',
    'catalog.property.s3.region'             = 'us-east-1'
);
