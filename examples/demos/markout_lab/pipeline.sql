-- LaminarDB Live Markout Lab: this exact SQL is executed and displayed by the application.

-- 1. Credential-free Binance BTCUSDT top-of-book updates. This is real market data;
--    the fills below remain explicitly simulated and never reach the exchange.
CREATE SOURCE quotes (
    event_type VARCHAR NOT NULL,
    sequence BIGINT NOT NULL,
    symbol VARCHAR NOT NULL,
    bid_px DOUBLE NOT NULL,
    ask_px DOUBLE NOT NULL,
    bid_qty DOUBLE NOT NULL,
    ask_qty DOUBLE NOT NULL,
    event_ts TIMESTAMP NOT NULL,
    PRIMARY KEY (symbol),
    WATERMARK FOR event_ts AS event_ts
) FROM "markout-live-binance" (
    url = 'wss://data-stream.binance.vision/ws/btcusdt@ticker',
    'connect.timeout.ms' = '15000',
    'read.timeout.ms' = '5000'
);

-- Visitor actions enter through a bounded in-process source. Prices are never
-- accepted from the browser; the backend binds each fill to the latest live quote.
CREATE SOURCE simulated_fills (
    demo_run_id VARCHAR NOT NULL,
    fill_id VARCHAR NOT NULL,
    source_event_id BIGINT NOT NULL,
    strategy VARCHAR NOT NULL,
    symbol VARCHAR NOT NULL,
    side VARCHAR NOT NULL,
    quantity DOUBLE NOT NULL,
    fill_px DOUBLE NOT NULL,
    fee_bps DOUBLE NOT NULL,
    mid_at_fill DOUBLE NOT NULL,
    spread_bps_at_fill DOUBLE NOT NULL,
    decision_reason VARCHAR NOT NULL,
    fill_model VARCHAR NOT NULL,
    event_ts TIMESTAMP NOT NULL,
    WATERMARK FOR event_ts AS event_ts
) FROM "markout-fills";

-- 2. Quote midpoint and spread calculation for the market subscription.
CREATE STREAM market_events AS
SELECT
    'live-binance' AS demo_run_id,
    q.sequence AS sequence,
    q.symbol AS symbol,
    q.bid_px AS bid_px,
    q.ask_px AS ask_px,
    q.bid_qty AS bid_qty,
    q.ask_qty AS ask_qty,
    (q.bid_px + q.ask_px) / 2.0 AS mid_px,
    (q.ask_px - q.bid_px) / ((q.bid_px + q.ask_px) / 2.0) * 10000.0 AS spread_bps,
    'live_binance' AS regime,
    CAST(q.event_ts AS BIGINT) AS event_ts
FROM quotes AS q
WHERE q.event_type = '24hrTicker';

-- 3. Fill presentation, including execution-time spread capture in SQL.
CREATE STREAM fill_output AS
SELECT
    f.demo_run_id AS demo_run_id,
    f.fill_id AS fill_id,
    f.source_event_id AS source_event_id,
    f.strategy AS strategy,
    f.symbol AS symbol,
    f.side AS side,
    f.quantity AS quantity,
    f.fill_px AS fill_px,
    f.fee_bps AS fee_bps,
    f.mid_at_fill AS mid_at_fill,
    f.spread_bps_at_fill AS spread_bps_at_fill,
    CASE WHEN f.side = 'BUY' THEN 1.0 ELSE -1.0 END
        * (f.mid_at_fill - f.fill_px) / f.fill_px * 10000.0 AS spread_capture_bps,
    f.decision_reason AS decision_reason,
    f.fill_model AS fill_model,
    CAST(f.event_ts AS BIGINT) AS event_ts
FROM simulated_fills AS f;

-- 4. One temporal probe join expands every fill into all five reference horizons.
-- 5. The same managed output calculates row-level liquidity-provider economics. LaminarDB's
--    supported temporal topology then feeds the named result directly into keyed aggregates.
CREATE STREAM markout_events AS
SELECT
    f.demo_run_id AS demo_run_id,
    f.fill_id AS fill_id,
    f.strategy AS strategy,
    f.symbol AS symbol,
    f.side AS side,
    f.quantity AS quantity,
    f.fill_px AS fill_px,
    f.mid_at_fill AS mid_at_fill,
    probe.offset_ms AS horizon_ms,
    CAST(probe.probe_time AS BIGINT) * 1000 AS probe_ts,
    CAST(q.event_ts AS BIGINT) AS reference_quote_ts,
    (q.bid_px + q.ask_px) / 2.0 AS future_mid_px,
    CASE WHEN f.side = 'BUY' THEN 1.0 ELSE -1.0 END
        * (f.mid_at_fill - f.fill_px) / f.fill_px * 10000.0 AS spread_capture_bps,
    CASE WHEN f.side = 'BUY' THEN 1.0 ELSE -1.0 END
        * (((q.bid_px + q.ask_px) / 2.0) - f.mid_at_fill)
        / f.fill_px * 10000.0 AS post_fill_drift_bps,
    CASE WHEN f.side = 'BUY' THEN 1.0 ELSE -1.0 END
        * (((q.bid_px + q.ask_px) / 2.0) - f.fill_px)
        / f.fill_px * 10000.0 AS gross_markout_bps,
    CASE WHEN f.side = 'BUY' THEN 1.0 ELSE -1.0 END
        * f.quantity * (((q.bid_px + q.ask_px) / 2.0) - f.fill_px) AS gross_markout_pnl,
    -f.fee_bps * f.quantity * f.fill_px / 10000.0 AS fee_pnl,
    CASE WHEN f.side = 'BUY' THEN 1.0 ELSE -1.0 END
        * f.quantity * (((q.bid_px + q.ask_px) / 2.0) - f.fill_px)
        - f.fee_bps * f.quantity * f.fill_px / 10000.0 AS net_markout_pnl,
    (CAST(probe.probe_time AS BIGINT) * 1000 - CAST(q.event_ts AS BIGINT))
        / 1000 AS quote_age_ms,
    CASE WHEN q.event_ts IS NULL THEN FALSE ELSE TRUE END AS covered,
    f.decision_reason AS decision_reason,
    f.fill_model AS fill_model
FROM simulated_fills AS f
LEFT TEMPORAL PROBE JOIN quotes AS q
    ON (symbol)
    TIMESTAMPS (event_ts, event_ts)
    LIST (0s, 1s, 5s, 15s, 30s)
    AS probe;

-- 6. Aggregate sums and counts. Ratios are intentionally deferred.
CREATE STREAM markout_sums AS
SELECT
    demo_run_id,
    strategy,
    horizon_ms,
    SUM(net_markout_pnl) AS total_net_markout_pnl,
    SUM(spread_capture_bps * quantity * fill_px) AS spread_capture_value,
    SUM(quantity * fill_px) AS filled_notional,
    COUNT(*) AS covered_fills,
    SUM(CASE WHEN net_markout_pnl < 0.0 THEN 1 ELSE 0 END) AS adverse_fills
FROM markout_events
WHERE covered = TRUE
GROUP BY demo_run_id, strategy, horizon_ms;

-- 7. Chart-ready weighted markout metrics; never an average of averages.
CREATE STREAM strategy_curve AS
SELECT
    demo_run_id,
    strategy,
    horizon_ms,
    spread_capture_value / filled_notional AS weighted_spread_capture_bps,
    10000.0 * total_net_markout_pnl / filled_notional AS weighted_net_markout_bps,
    total_net_markout_pnl,
    filled_notional,
    covered_fills,
    100.0 * CAST(adverse_fills AS DOUBLE) / CAST(covered_fills AS DOUBLE) AS adverse_fill_rate
FROM markout_sums;

-- 8. Nullable dashboard KPI updates derived from the canonical aggregate stream.
CREATE STREAM dashboard_summary AS
SELECT
    demo_run_id,
    strategy,
    horizon_ms,
    CASE WHEN horizon_ms = 0
        THEN weighted_spread_capture_bps ELSE NULL END AS spread_capture_0s_bps,
    CASE WHEN horizon_ms = 5000
        THEN weighted_net_markout_bps ELSE NULL END AS weighted_markout_5s_bps,
    CASE WHEN horizon_ms = 30000
        THEN total_net_markout_pnl ELSE NULL END AS hypothetical_pnl_30s,
    CASE WHEN horizon_ms = 0
        THEN filled_notional ELSE NULL END AS filled_notional,
    CASE WHEN horizon_ms = 5000
        THEN adverse_fill_rate ELSE NULL END AS adverse_fill_rate_5s
FROM strategy_curve;
