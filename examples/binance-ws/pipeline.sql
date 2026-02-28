-- LaminarDB Binance Demo: Real-time Streaming SQL
-- 6 symbols x 3 stages = 18 pipeline stages, all in SQL.

-- ── Sources: one WebSocket per symbol ────────────────────────────

CREATE SOURCE btcusdt (
    s VARCHAR, p DOUBLE, q DOUBLE, "T" BIGINT,
    WATERMARK FOR "T" AS "T" - INTERVAL '0' SECOND
) FROM WEBSOCKET (url = 'wss://stream.binance.com:9443/ws/btcusdt@trade', format = 'json');

CREATE SOURCE ethusdt (
    s VARCHAR, p DOUBLE, q DOUBLE, "T" BIGINT,
    WATERMARK FOR "T" AS "T" - INTERVAL '0' SECOND
) FROM WEBSOCKET (url = 'wss://stream.binance.com:9443/ws/ethusdt@trade', format = 'json');

CREATE SOURCE solusdt (
    s VARCHAR, p DOUBLE, q DOUBLE, "T" BIGINT,
    WATERMARK FOR "T" AS "T" - INTERVAL '0' SECOND
) FROM WEBSOCKET (url = 'wss://stream.binance.com:9443/ws/solusdt@trade', format = 'json');

CREATE SOURCE dogeusdt (
    s VARCHAR, p DOUBLE, q DOUBLE, "T" BIGINT,
    WATERMARK FOR "T" AS "T" - INTERVAL '0' SECOND
) FROM WEBSOCKET (url = 'wss://stream.binance.com:9443/ws/dogeusdt@trade', format = 'json');

CREATE SOURCE xrpusdt (
    s VARCHAR, p DOUBLE, q DOUBLE, "T" BIGINT,
    WATERMARK FOR "T" AS "T" - INTERVAL '0' SECOND
) FROM WEBSOCKET (url = 'wss://stream.binance.com:9443/ws/xrpusdt@trade', format = 'json');

CREATE SOURCE bnbusdt (
    s VARCHAR, p DOUBLE, q DOUBLE, "T" BIGINT,
    WATERMARK FOR "T" AS "T" - INTERVAL '0' SECOND
) FROM WEBSOCKET (url = 'wss://stream.binance.com:9443/ws/bnbusdt@trade', format = 'json');

-- ── VWAP: 10-second tumbling windows ─────────────────────────────

CREATE STREAM vwap_btcusdt AS
SELECT s AS symbol, SUM(p * q) / SUM(q) AS vwap, AVG(p) AS avg_price,
       MIN(p) AS low, MAX(p) AS high, SUM(q) AS volume, COUNT(*) AS trades,
       (MAX(p) - MIN(p)) / AVG(p) * 10000.0 AS volatility_bps
FROM btcusdt GROUP BY s, TUMBLE("T", INTERVAL '10' SECOND) EMIT ON WINDOW CLOSE;

CREATE STREAM vwap_ethusdt AS
SELECT s AS symbol, SUM(p * q) / SUM(q) AS vwap, AVG(p) AS avg_price,
       MIN(p) AS low, MAX(p) AS high, SUM(q) AS volume, COUNT(*) AS trades,
       (MAX(p) - MIN(p)) / AVG(p) * 10000.0 AS volatility_bps
FROM ethusdt GROUP BY s, TUMBLE("T", INTERVAL '10' SECOND) EMIT ON WINDOW CLOSE;

CREATE STREAM vwap_solusdt AS
SELECT s AS symbol, SUM(p * q) / SUM(q) AS vwap, AVG(p) AS avg_price,
       MIN(p) AS low, MAX(p) AS high, SUM(q) AS volume, COUNT(*) AS trades,
       (MAX(p) - MIN(p)) / AVG(p) * 10000.0 AS volatility_bps
FROM solusdt GROUP BY s, TUMBLE("T", INTERVAL '10' SECOND) EMIT ON WINDOW CLOSE;

CREATE STREAM vwap_dogeusdt AS
SELECT s AS symbol, SUM(p * q) / SUM(q) AS vwap, AVG(p) AS avg_price,
       MIN(p) AS low, MAX(p) AS high, SUM(q) AS volume, COUNT(*) AS trades,
       (MAX(p) - MIN(p)) / AVG(p) * 10000.0 AS volatility_bps
FROM dogeusdt GROUP BY s, TUMBLE("T", INTERVAL '10' SECOND) EMIT ON WINDOW CLOSE;

CREATE STREAM vwap_xrpusdt AS
SELECT s AS symbol, SUM(p * q) / SUM(q) AS vwap, AVG(p) AS avg_price,
       MIN(p) AS low, MAX(p) AS high, SUM(q) AS volume, COUNT(*) AS trades,
       (MAX(p) - MIN(p)) / AVG(p) * 10000.0 AS volatility_bps
FROM xrpusdt GROUP BY s, TUMBLE("T", INTERVAL '10' SECOND) EMIT ON WINDOW CLOSE;

CREATE STREAM vwap_bnbusdt AS
SELECT s AS symbol, SUM(p * q) / SUM(q) AS vwap, AVG(p) AS avg_price,
       MIN(p) AS low, MAX(p) AS high, SUM(q) AS volume, COUNT(*) AS trades,
       (MAX(p) - MIN(p)) / AVG(p) * 10000.0 AS volatility_bps
FROM bnbusdt GROUP BY s, TUMBLE("T", INTERVAL '10' SECOND) EMIT ON WINDOW CLOSE;

-- ── Signals: BUY / SELL / HOLD derived from VWAP divergence ─────

CREATE STREAM signals_btcusdt AS
SELECT symbol, vwap, avg_price, volume, trades, volatility_bps,
       CASE WHEN avg_price > vwap * 1.001 THEN 'SELL'
            WHEN avg_price < vwap * 0.999 THEN 'BUY'
            ELSE 'HOLD' END AS signal
FROM vwap_btcusdt;

CREATE STREAM signals_ethusdt AS
SELECT symbol, vwap, avg_price, volume, trades, volatility_bps,
       CASE WHEN avg_price > vwap * 1.001 THEN 'SELL'
            WHEN avg_price < vwap * 0.999 THEN 'BUY'
            ELSE 'HOLD' END AS signal
FROM vwap_ethusdt;

CREATE STREAM signals_solusdt AS
SELECT symbol, vwap, avg_price, volume, trades, volatility_bps,
       CASE WHEN avg_price > vwap * 1.001 THEN 'SELL'
            WHEN avg_price < vwap * 0.999 THEN 'BUY'
            ELSE 'HOLD' END AS signal
FROM vwap_solusdt;

CREATE STREAM signals_dogeusdt AS
SELECT symbol, vwap, avg_price, volume, trades, volatility_bps,
       CASE WHEN avg_price > vwap * 1.001 THEN 'SELL'
            WHEN avg_price < vwap * 0.999 THEN 'BUY'
            ELSE 'HOLD' END AS signal
FROM vwap_dogeusdt;

CREATE STREAM signals_xrpusdt AS
SELECT symbol, vwap, avg_price, volume, trades, volatility_bps,
       CASE WHEN avg_price > vwap * 1.001 THEN 'SELL'
            WHEN avg_price < vwap * 0.999 THEN 'BUY'
            ELSE 'HOLD' END AS signal
FROM vwap_xrpusdt;

CREATE STREAM signals_bnbusdt AS
SELECT symbol, vwap, avg_price, volume, trades, volatility_bps,
       CASE WHEN avg_price > vwap * 1.001 THEN 'SELL'
            WHEN avg_price < vwap * 0.999 THEN 'BUY'
            ELSE 'HOLD' END AS signal
FROM vwap_bnbusdt;
