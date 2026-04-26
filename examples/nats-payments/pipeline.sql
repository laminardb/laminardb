-- Reference copy of the pipeline SQL.
--
-- LaminarDB's [[pipeline]] section takes the SQL inline as a string, so
-- the authoritative copy lives in config.toml. This file is here for
-- readability and editor highlighting only — the server does not load it.
--
-- Source `payments` is declared in config.toml ([[source]]) with a flat
-- JSON schema. event_time arrives as RFC3339 and is parsed into a
-- TIMESTAMP column by the JSON decoder. Watermark is configured on
-- event_time with a 10s out-of-order tolerance.
--
-- The window rewriter auto-prepends window_start, window_end to the
-- SELECT list whenever it sees a windowed GROUP BY.

SELECT
    region,
    method,
    COUNT(*)                                            AS payment_count,
    SUM(amount_usd)                                     AS total_usd,
    AVG(amount_usd)                                     AS avg_usd,
    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END)  AS failed_count
FROM payments
GROUP BY
    TUMBLE(event_time, INTERVAL '1' MINUTE),
    region,
    method
EMIT ON WINDOW CLOSE
