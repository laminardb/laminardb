-- Reference copies of the two pipelines.
-- Live SQL lives in config.toml's [[pipeline]] sql fields.

-- 1. Tumbling 1-minute rollup over the payments stream.
SELECT
    region,
    method,
    COUNT(*)          AS payment_count,
    SUM(amount_usd)   AS total_usd,
    AVG(amount_usd)   AS avg_usd
FROM payments
GROUP BY
    TUMBLE(event_time, INTERVAL '1' MINUTE),
    region,
    method
EMIT ON WINDOW CLOSE;


-- 2. Stream-to-stream interval join on payment_id with a 2-second window.
--    Each row is one payment matched with its fraud score; score_latency_ms
--    is the wall-clock between the two events.
-- Right-side (fraud_checks) columns need explicit AS aliases — the
-- interval-join operator rewrites table-qualified refs internally to
-- `{col}_{right_table}`, so without an alias the emitted batch column
-- is `fraud_score_fraud_checks`, not `fraud_score`.
SELECT
    p.payment_id  AS payment_id,
    p.region      AS region,
    p.method      AS method,
    p.amount_usd  AS amount_usd,
    f.fraud_score AS fraud_score,
    f.outcome     AS outcome,
    p.event_time                                              AS initiated_at,
    f.event_time                                              AS scored_at,
    (CAST(f.event_time AS BIGINT) - CAST(p.event_time AS BIGINT)) / 1000000 AS score_latency_ms
FROM payments p
JOIN fraud_checks f
    ON p.payment_id = f.payment_id
    AND f.event_time BETWEEN p.event_time AND p.event_time + INTERVAL '2' SECOND;
