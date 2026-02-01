-- Streaming pipelines for the Market Data demo.
-- Each CREATE STREAM defines a named continuous query.

-- 1. OHLC bars: per-symbol price aggregation with VWAP
CREATE STREAM ohlc_bars AS
SELECT
    symbol,
    MIN(price) as min_price,
    MAX(price) as max_price,
    COUNT(*) as trade_count,
    SUM(volume) as total_volume,
    SUM(price * volume) / SUM(volume) as vwap
FROM market_ticks
GROUP BY symbol;

-- 2. Volume metrics: buy/sell volume split per symbol
CREATE STREAM volume_metrics AS
SELECT
    symbol,
    SUM(CASE WHEN side = 'buy' THEN volume ELSE 0 END) as buy_volume,
    SUM(CASE WHEN side = 'sell' THEN volume ELSE 0 END) as sell_volume,
    SUM(CASE WHEN side = 'buy' THEN volume ELSE 0 END) -
        SUM(CASE WHEN side = 'sell' THEN volume ELSE 0 END) as net_volume,
    COUNT(*) as trade_count
FROM market_ticks
GROUP BY symbol;

-- 3. Spread metrics: bid-ask spread statistics per symbol
CREATE STREAM spread_metrics AS
SELECT
    symbol,
    AVG(ask - bid) as avg_spread,
    MIN(ask - bid) as min_spread,
    MAX(ask - bid) as max_spread
FROM market_ticks
GROUP BY symbol;

-- 4. Anomaly alerts: volume aggregation for high-volume detection
CREATE STREAM anomaly_alerts AS
SELECT
    symbol,
    COUNT(*) as trade_count,
    SUM(volume) as total_volume
FROM market_ticks
GROUP BY symbol;
