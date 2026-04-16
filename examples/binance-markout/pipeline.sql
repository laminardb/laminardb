-- LaminarDB Multi-Horizon Mark-Out Demo on BTC-USDT-PERP.
-- Binance USDT-M futures trade and bookTicker streams.
-- Horizons: 5s / 15s / 30s / 60s. Primary horizon (scatter, alerts) is 5s.

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

-- ── 3. Mark-outs at 5s / 15s / 30s / 60s horizons ────────────────
-- One probe operator, one shared right-side buffer. Each trade emits
-- four rows (one per offset) with `offset_ms` as a column.
-- Reference is the trade price (execution markout).

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
    LIST (5s, 15s, 30s, 60s) AS mo;

-- ── 4. Markout curve (centrepiece) ────────────────────────────────
-- Per 1-minute window, per aggressor side: the shape of markout across
-- horizons. Wide form so the dashboard can plot one polyline per side.
-- Each trade contributes 4 rows to markouts_long, so COUNT/SUM / 4.

CREATE STREAM markout_curve AS
SELECT
    side,
    AVG(CASE WHEN offset_ms = 5000  THEN signed_markout_bps END) AS avg_5s,
    AVG(CASE WHEN offset_ms = 15000 THEN signed_markout_bps END) AS avg_15s,
    AVG(CASE WHEN offset_ms = 30000 THEN signed_markout_bps END) AS avg_30s,
    AVG(CASE WHEN offset_ms = 60000 THEN signed_markout_bps END) AS avg_60s,
    COUNT(*) / 4                                                 AS trade_count,
    SUM(quantity) / 4.0                                          AS total_volume
FROM markouts_long
GROUP BY side, TUMBLE("T", INTERVAL '1' MINUTE)
EMIT ON WINDOW CLOSE;

-- ── 5. 30-second per-offset metrics (heatmap + cards) ────────────
-- Long form keyed by offset_ms. Heatmap reads every row; metric cards
-- filter to 5000 and 60000.

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

-- ── 6. Toxicity alerts ───────────────────────────────────────────
-- Fired when 5s adverse selection breaches 60%, or 5s avg markout
-- runs sufficiently negative. 5s is the primary horizon.

CREATE STREAM toxicity_alerts AS
SELECT
    'HIGH_ADVERSE_SELECTION'                          AS alert_type,
    offset_ms,
    adverse_selection_rate,
    avg_markout_bps,
    trade_count
FROM flow_toxicity_30s
WHERE offset_ms = 5000
  AND (adverse_selection_rate > 0.6 OR avg_markout_bps < -2.0);

-- ── 6a. Flow regime classifier (per side, 1-min cadence) ─────────
-- Projects markout_curve into a regime label using short- vs long-horizon
-- markout shape. Operator-level decision aid: tells you whether to adjust.

CREATE STREAM flow_regime AS
SELECT
    *,
    CASE
      WHEN avg_5s IS NULL OR avg_60s IS NULL                                THEN 'UNKNOWN'
      WHEN avg_5s > -0.5 AND avg_5s < 0.5 AND avg_60s > -0.5 AND avg_60s < 0.5
                                                                            THEN 'CLEAN'
      WHEN avg_5s >  2.0 AND avg_60s >  2.0                                 THEN 'INFORMED'
      WHEN avg_5s < -2.0 AND avg_60s < -2.0                                 THEN 'ADVERSE'
      WHEN (avg_5s > 2.0 OR avg_5s < -2.0)
            AND avg_60s > -0.5 AND avg_60s < 0.5                            THEN 'TEMP_IMPACT'
      WHEN avg_5s > -1.0 AND avg_5s < 1.0
            AND (avg_60s > 2.0 OR avg_60s < -2.0)                           THEN 'SLOW_INFORMED'
      ELSE                                                                        'MIXED'
    END AS regime
FROM markout_curve;

-- ── 6b. MM quote-skew signal (30-second cadence) ─────────────────
-- Pivots 60s markouts by aggressor side in one aggregation. Tells a maker
-- how to adjust quotes: skew_bps = direction, max_adverse_rate = size cut.

CREATE STREAM quote_signal AS
SELECT
    AVG(CASE WHEN side = 'BUY'  THEN signed_markout_bps END)   AS buy_avg_60s,
    AVG(CASE WHEN side = 'SELL' THEN signed_markout_bps END)   AS sell_avg_60s,
    (AVG(CASE WHEN side = 'BUY'  THEN signed_markout_bps END)
       - AVG(CASE WHEN side = 'SELL' THEN signed_markout_bps END)) / 2.0
                                                                AS skew_bps,
    AVG(CASE WHEN side = 'BUY'  AND signed_markout_bps < 0 THEN 1.0
             WHEN side = 'BUY'                              THEN 0.0 END)
                                                                AS buy_adverse,
    AVG(CASE WHEN side = 'SELL' AND signed_markout_bps < 0 THEN 1.0
             WHEN side = 'SELL'                             THEN 0.0 END)
                                                                AS sell_adverse,
    GREATEST(
        AVG(CASE WHEN side = 'BUY'  AND signed_markout_bps < 0 THEN 1.0
                 WHEN side = 'BUY'                              THEN 0.0 END),
        AVG(CASE WHEN side = 'SELL' AND signed_markout_bps < 0 THEN 1.0
                 WHEN side = 'SELL'                             THEN 0.0 END)
    )                                                           AS max_adverse_rate,
    COUNT(*)                                                    AS trade_count
FROM markouts_long
WHERE offset_ms = 60000
GROUP BY TUMBLE("T", INTERVAL '30' SECOND)
EMIT ON WINDOW CLOSE;

-- ── 7. WebSocket sinks — one port per dashboard panel ────────────

CREATE SINK markouts_sink FROM markouts_long
INTO WEBSOCKET (
    mode           = 'server',
    'bind.address' = '127.0.0.1:9001',
    path           = '/markouts',
    format         = 'json'
);

CREATE SINK curve_sink FROM markout_curve
INTO WEBSOCKET (
    mode           = 'server',
    'bind.address' = '127.0.0.1:9002',
    path           = '/curve',
    format         = 'json'
);

CREATE SINK toxicity_sink FROM flow_toxicity_30s
INTO WEBSOCKET (
    mode           = 'server',
    'bind.address' = '127.0.0.1:9003',
    path           = '/toxicity',
    format         = 'json'
);

CREATE SINK alerts_sink FROM toxicity_alerts
INTO WEBSOCKET (
    mode           = 'server',
    'bind.address' = '127.0.0.1:9004',
    path           = '/alerts',
    format         = 'json'
);

CREATE SINK regime_sink FROM flow_regime
INTO WEBSOCKET (
    mode           = 'server',
    'bind.address' = '127.0.0.1:9005',
    path           = '/regime',
    format         = 'json'
);

CREATE SINK signal_sink FROM quote_signal
INTO WEBSOCKET (
    mode           = 'server',
    'bind.address' = '127.0.0.1:9006',
    path           = '/signal',
    format         = 'json'
);
