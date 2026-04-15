-- LaminarDB Mark-Out Demo on BTC-USDT-PERP.
-- Binance USDT-M futures trade and bookTicker streams.

-- ── 1. Sources ────────────────────────────────────────────────────

CREATE SOURCE btc_trades (
    s VARCHAR,
    p DOUBLE,
    q DOUBLE,
    m BOOLEAN,
    "T" TIMESTAMP,
    WATERMARK FOR "T" AS "T" - INTERVAL '2' SECOND
) FROM WEBSOCKET (
    url = 'wss://fstream.binance.com/ws/btcusdt@trade',
    format = 'json'
);

CREATE SOURCE btc_book (
    s VARCHAR,
    b DOUBLE,
    a DOUBLE,
    "E" TIMESTAMP,
    WATERMARK FOR "E" AS "E" - INTERVAL '2' SECOND
) FROM WEBSOCKET (
    url = 'wss://fstream.binance.com/ws/btcusdt@bookTicker',
    format = 'json'
);

-- ── 2. Book mid-price + spread ────────────────────────────────────

CREATE STREAM book_mid AS
SELECT
    s,
    (b + a) / 2.0 AS mid_price,
    (a - b)       AS spread,
    "E"
FROM btc_book;

-- ── 3. Mark-outs at 0s / 1s / 5s / 30s horizons ──────────────────
-- Probe emits one row per (trade × horizon); signed_markout_bps is positive
-- when the price moved in the aggressor's favour. Probe alias is `mo` to
-- avoid colliding with the left-side column `p` (trade price).
CREATE STREAM markouts_long AS
SELECT
    s,
    p                                        AS trade_price,
    q                                        AS quantity,
    CASE WHEN m THEN 'SELL' ELSE 'BUY' END   AS side,
    mid_price                                AS mid_at_offset,
    spread                                   AS spread_at_offset,
    mo_offset_ms                             AS offset_ms,
    CASE WHEN m = FALSE
         THEN (mid_price - p) / p * 10000.0
         ELSE (p - mid_price) / p * 10000.0
    END AS signed_markout_bps,
    "T"
FROM btc_trades t
TEMPORAL PROBE JOIN book_mid mp
    ON (s) TIMESTAMPS ("T", "E")
    LIST (0s, 1s, 5s, 30s) AS mo;

-- ── 5. Rolling 30s aggregates ────────────────────────────────────

CREATE STREAM flow_toxicity_30s AS
SELECT
    offset_ms,
    COUNT(*)                                                    AS trade_count,
    SUM(quantity)                                               AS total_volume,
    AVG(signed_markout_bps)                                     AS avg_markout_bps,
    STDDEV(signed_markout_bps)                                  AS stddev_markout_bps,
    AVG(CASE WHEN signed_markout_bps < 0 THEN 1.0 ELSE 0.0 END) AS adverse_selection_rate,
    AVG(spread_at_offset)                                       AS avg_spread
FROM markouts_long
GROUP BY offset_ms, TUMBLE("T", INTERVAL '30' SECOND)
EMIT ON WINDOW CLOSE;

CREATE STREAM flow_by_side_30s AS
SELECT
    offset_ms,
    side,
    COUNT(*)                AS trade_count,
    AVG(signed_markout_bps) AS avg_markout_bps,
    SUM(quantity)           AS volume
FROM markouts_long
GROUP BY offset_ms, side, TUMBLE("T", INTERVAL '30' SECOND)
EMIT ON WINDOW CLOSE;

CREATE STREAM vwap_markout_30s AS
SELECT
    offset_ms,
    SUM(signed_markout_bps * quantity) / SUM(quantity) AS vwap_markout_bps,
    SUM(quantity)                                      AS total_volume
FROM markouts_long
GROUP BY offset_ms, TUMBLE("T", INTERVAL '30' SECOND)
EMIT ON WINDOW CLOSE;

-- ── 6. Toxicity alerts ───────────────────────────────────────────

CREATE STREAM toxicity_alerts AS
SELECT *
FROM flow_toxicity_30s
WHERE offset_ms = 1000
  AND (adverse_selection_rate > 0.6 OR avg_markout_bps < -2.0);

-- ── 7. WebSocket sinks — one port per dashboard panel ────────────

CREATE SINK markouts_sink FROM markouts_long
INTO WEBSOCKET (
    mode         = 'server',
    'bind.address' = '127.0.0.1:9001',
    path           = '/markouts',
    format       = 'json'
);

CREATE SINK toxicity_sink FROM flow_toxicity_30s
INTO WEBSOCKET (
    mode         = 'server',
    'bind.address' = '127.0.0.1:9002',
    path         = '/toxicity',
    format       = 'json'
);

CREATE SINK by_side_sink FROM flow_by_side_30s
INTO WEBSOCKET (
    mode         = 'server',
    'bind.address' = '127.0.0.1:9003',
    path         = '/by_side',
    format       = 'json'
);

CREATE SINK alerts_sink FROM toxicity_alerts
INTO WEBSOCKET (
    mode         = 'server',
    'bind.address' = '127.0.0.1:9004',
    path         = '/alerts',
    format       = 'json'
);
