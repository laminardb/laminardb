-- LaminarDB Market Microstructure Intelligence
-- 4 sources, 8 SQL stages, 3 window types, sub-us latency.
--
-- Demonstrates:
--   json.path    = envelope unwrapping (combined WebSocket streams)
--   json.explode = array-to-rows (orderbook depth levels)
--
-- Combined streams reduce 12 WebSocket connections to 2.
-- Depth adds 2 more for BTC orderbook = 4 total connections.
--
-- Cross-path selection example (not used here but supported):
--   json.column.stream_name = 'stream'
-- would extract the envelope's "stream" field into a column while
-- json.path navigates into "data" for the remaining columns.

-- ── Layer 0: Combined Trade Source ───────────────────────────────
-- One WebSocket carries all 6 symbols via Binance combined stream.
-- json.path = 'data' unwraps {"stream":"...","data":{trade fields}}.

CREATE SOURCE all_trades (
    s VARCHAR, p DOUBLE, q DOUBLE, "T" TIMESTAMP, m BOOLEAN,
    WATERMARK FOR "T" AS "T" - INTERVAL '0' SECOND
) FROM WEBSOCKET (
    url = 'wss://stream.binance.com:9443/stream?streams=btcusdt@trade/ethusdt@trade/solusdt@trade/dogeusdt@trade/xrpusdt@trade/bnbusdt@trade',
    format = 'json',
    json.path = 'data'
);

-- ── Layer 0: Combined Book Ticker Source ─────────────────────────

CREATE SOURCE all_books (
    s VARCHAR, b DOUBLE, "B" DOUBLE, a DOUBLE, "A" DOUBLE, u BIGINT
) FROM WEBSOCKET (
    url = 'wss://stream.binance.com:9443/stream?streams=btcusdt@bookTicker/ethusdt@bookTicker/solusdt@bookTicker/dogeusdt@bookTicker/xrpusdt@bookTicker/bnbusdt@bookTicker',
    format = 'json',
    json.path = 'data'
);

-- ── Layer 0: BTC Depth (orderbook) ──────────────────────────────
-- json.path navigates to the "bids"/"asks" array.
-- json.explode maps positional elements: [[price, qty], ...] → rows.
-- String values are coerced to DOUBLE by the default Coerce strategy.

CREATE SOURCE btc_depth_bids (
    price DOUBLE, qty DOUBLE
) FROM WEBSOCKET (
    url = 'wss://stream.binance.com:9443/ws/btcusdt@depth20',
    format = 'json',
    json.path = 'bids',
    json.explode = 'price,qty'
);

CREATE SOURCE btc_depth_asks (
    price DOUBLE, qty DOUBLE
) FROM WEBSOCKET (
    url = 'wss://stream.binance.com:9443/ws/btcusdt@depth20',
    format = 'json',
    json.path = 'asks',
    json.explode = 'price,qty'
);

-- ── Layer 0b: Depth Passthrough ───────────────────────────────
-- Passthrough streams so the TUI can subscribe to depth data.

CREATE STREAM depth_bids AS SELECT price, qty FROM btc_depth_bids;
CREATE STREAM depth_asks AS SELECT price, qty FROM btc_depth_asks;

-- ── Layer 1: Spread Monitor (unified) ───────────────────────────

CREATE STREAM spreads AS
SELECT s AS symbol,
       a - b AS spread,
       (a - b) / ((a + b) / 2.0) * 10000.0 AS spread_bps,
       b AS bid_price, a AS ask_price,
       "B" AS bid_qty, "A" AS ask_qty,
       "B" / ("B" + "A") AS bid_pressure
FROM all_books;

-- ── Layer 2: VWAP + OHLC (unified, tumbling 10s) ───────────────
-- GROUP BY s naturally partitions windows per symbol.

CREATE STREAM vwap AS
SELECT s AS symbol, SUM(p * q) / SUM(q) AS vwap, AVG(p) AS avg_price,
       MIN(p) AS low, MAX(p) AS high, SUM(q) AS volume, COUNT(*) AS trades,
       (MAX(p) - MIN(p)) / AVG(p) * 10000.0 AS volatility_bps,
       SUM(CASE WHEN m THEN q ELSE 0.0 END) / SUM(q) AS maker_ratio
FROM all_trades GROUP BY s, TUMBLE("T", INTERVAL '10' SECOND) EMIT ON WINDOW CLOSE;

-- ── Layer 2b: Micro-OHLC (unified, tumbling 5s) ────────────────

CREATE STREAM micro AS
SELECT s AS symbol,
       COUNT(*) AS ticks,
       SUM(p * q) / SUM(q) AS vwap,
       MIN(p) AS low, MAX(p) AS high,
       SUM(q) AS volume,
       SUM(CASE WHEN NOT m THEN q ELSE 0.0 END) AS buy_vol,
       SUM(CASE WHEN m THEN q ELSE 0.0 END) AS sell_vol
FROM all_trades GROUP BY s, TUMBLE("T", INTERVAL '5' SECOND) EMIT ON WINDOW CLOSE;

-- ── Layer 3: Rolling Momentum (unified, hopping 5s/30s) ────────

CREATE STREAM momentum AS
SELECT s AS symbol,
       AVG(p) AS avg_30s,
       MIN(p) AS low_30s, MAX(p) AS high_30s,
       SUM(q) AS vol_30s, COUNT(*) AS trades_30s,
       (MAX(p) - MIN(p)) / AVG(p) * 10000.0 AS range_bps_30s
FROM all_trades GROUP BY s, HOP("T", INTERVAL '5' SECOND, INTERVAL '30' SECOND) EMIT ON WINDOW CLOSE;

-- ── Layer 4: Activity Bursts (unified, session 2s gap) ─────────

CREATE STREAM bursts AS
SELECT s AS symbol,
       COUNT(*) AS burst_trades,
       SUM(q) AS burst_volume,
       (MAX(p) - MIN(p)) / AVG(p) * 10000.0 AS burst_range_bps,
       SUM(CASE WHEN m THEN q ELSE 0.0 END) / SUM(q) AS maker_ratio
FROM all_trades GROUP BY s, SESSION("T", INTERVAL '2' SECOND) EMIT ON WINDOW CLOSE;

-- ── Layer 5: Trade Signals (unified, derived from VWAP) ────────

CREATE STREAM signals AS
SELECT symbol, vwap, avg_price, volume, trades, volatility_bps, maker_ratio,
       CASE WHEN avg_price > vwap * 1.001 THEN 'SELL'
            WHEN avg_price < vwap * 0.999 THEN 'BUY'
            ELSE 'HOLD' END AS signal
FROM vwap;
