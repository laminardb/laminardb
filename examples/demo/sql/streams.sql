-- Streaming pipelines for the Market Data demo.
-- Each CREATE STREAM defines a named continuous query.

-- 1. OHLC bars: per-symbol price aggregation with VWAP (5-second tumbling windows)
CREATE STREAM ohlc_bars AS
SELECT
    symbol,
    MIN(price) as min_price,
    MAX(price) as max_price,
    COUNT(*) as trade_count,
    SUM(volume) as total_volume,
    SUM(price * volume) / SUM(volume) as vwap
FROM market_ticks
GROUP BY symbol, tumble(ts, INTERVAL '5' SECOND);

-- 2. Volume metrics: buy/sell volume split per symbol (5-second tumbling windows)
CREATE STREAM volume_metrics AS
SELECT
    symbol,
    SUM(CASE WHEN side = 'buy' THEN volume ELSE 0 END) as buy_volume,
    SUM(CASE WHEN side = 'sell' THEN volume ELSE 0 END) as sell_volume,
    SUM(CASE WHEN side = 'buy' THEN volume ELSE 0 END) -
        SUM(CASE WHEN side = 'sell' THEN volume ELSE 0 END) as net_volume,
    COUNT(*) as trade_count
FROM market_ticks
GROUP BY symbol, tumble(ts, INTERVAL '5' SECOND);

-- 3. Spread metrics: bid-ask spread statistics per symbol (5-second tumbling windows)
CREATE STREAM spread_metrics AS
SELECT
    symbol,
    AVG(ask - bid) as avg_spread,
    MIN(ask - bid) as min_spread,
    MAX(ask - bid) as max_spread
FROM market_ticks
GROUP BY symbol, tumble(ts, INTERVAL '5' SECOND);

-- 4. Anomaly alerts: volume aggregation for high-volume detection (5-second tumbling windows)
CREATE STREAM anomaly_alerts AS
SELECT
    symbol,
    COUNT(*) as trade_count,
    SUM(volume) as total_volume
FROM market_ticks
GROUP BY symbol, tumble(ts, INTERVAL '5' SECOND);

-- 5. Enriched orders: ASOF JOIN orders with latest market tick
-- SQL definition (executed via application-level batch merge):
-- SELECT o.order_id, o.symbol, o.side, o.quantity, o.price as order_price,
--        t.price as market_price, t.bid, t.ask, o.price - t.price as slippage
-- FROM order_events o
-- ASOF JOIN market_ticks t MATCH_CONDITION(o.ts >= t.ts) ON o.symbol = t.symbol
