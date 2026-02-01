-- Sinks for the Market Data demo.
-- In embedded mode these are in-memory sinks (subscribed via the API).

CREATE SINK ohlc_output FROM ohlc_bars;
CREATE SINK volume_output FROM volume_metrics;
CREATE SINK spread_output FROM spread_metrics;
CREATE SINK anomaly_output FROM anomaly_alerts;
