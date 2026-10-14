-- The trading desk: venue-independent SQL over the `executions` source. It uses standard
-- FIX 4.4+ values only: Side (54) 1 = buy, 2 = sell; ExecType (150) 0 = new, 4 = canceled,
-- 5 = replaced, 8 = rejected, F = trade. FIX 4.2 venues report trades as ExecType 1 and 2.

-- Live net position and cash per account and instrument.
CREATE MATERIALIZED VIEW positions AS
SELECT account,
       symbol,
       SUM(CASE WHEN side = '1' THEN last_qty ELSE -last_qty END) AS net_qty,
       SUM(CASE WHEN side = '1' THEN -(last_qty * last_px) ELSE last_qty * last_px END) AS net_cash,
       SUM(last_qty * last_px) AS turnover,
       COUNT(*) AS fills
FROM executions
WHERE exec_type = 'F'
GROUP BY account, symbol;

-- Order flow per instrument and minute.
CREATE MATERIALIZED VIEW flow_1m AS
SELECT symbol,
       tumble(transact_time, INTERVAL '1' MINUTE) AS minute,
       SUM(CASE WHEN exec_type = '0' THEN 1 ELSE 0 END) AS orders,
       SUM(CASE WHEN exec_type = 'F' THEN 1 ELSE 0 END) AS fills,
       SUM(CASE WHEN exec_type = '4' THEN 1 ELSE 0 END) AS cancels,
       SUM(CASE WHEN exec_type = '8' THEN 1 ELSE 0 END) AS rejects,
       SUM(CASE WHEN exec_type = 'F' THEN last_qty END) AS volume,
       SUM(CASE WHEN exec_type = 'F' THEN last_qty * last_px END) AS notional
FROM executions
GROUP BY symbol, tumble(transact_time, INTERVAL '1' MINUTE)
EMIT ON WINDOW CLOSE;

-- Risk-check rejects per account and minute.
CREATE MATERIALIZED VIEW rejects_1m AS
SELECT account,
       tumble(transact_time, INTERVAL '1' MINUTE) AS minute,
       SUM(CASE WHEN exec_type IN ('0', '8') THEN 1 ELSE 0 END) AS orders,
       SUM(CASE WHEN exec_type = '8' THEN 1 ELSE 0 END) AS rejects
FROM executions
GROUP BY account, tumble(transact_time, INTERVAL '1' MINUTE)
EMIT ON WINDOW CLOSE;

-- Large prints: fills worth a million or more.
CREATE MATERIALIZED VIEW large_fills AS
SELECT transact_time, account, symbol, side, last_qty, last_px, last_qty * last_px AS notional
FROM executions
WHERE exec_type = 'F' AND last_qty * last_px >= 1000000;

-- Order bursts: ten or more new orders from one account in one instrument with less than a
-- second between them.
CREATE MATERIALIZED VIEW order_bursts AS
SELECT account,
       symbol,
       COUNT(*) AS orders,
       SUM(order_qty) AS quantity,
       MIN(transact_time) AS first_order,
       MAX(transact_time) AS last_order
FROM executions
WHERE exec_type = '0'
GROUP BY account, symbol, SESSION(transact_time, INTERVAL '1' SECOND)
HAVING COUNT(*) >= 10
EMIT ON WINDOW CLOSE;

-- Drop-copy delay: from the venue's TransactTime to the moment the report was journaled.
CREATE MATERIALIZED VIEW latency_1m AS
SELECT tumble(transact_time, INTERVAL '1' MINUTE) AS minute,
       COUNT(*) AS reports,
       AVG((date_part('epoch', _fix_received_at) - date_part('epoch', transact_time)) * 1000.0)
           AS avg_ms,
       MAX((date_part('epoch', _fix_received_at) - date_part('epoch', transact_time)) * 1000.0)
           AS max_ms
FROM executions
GROUP BY tumble(transact_time, INTERVAL '1' MINUTE)
EMIT ON WINDOW CLOSE;

-- Feed completeness: reports recovered by resend after an outage, and possible gaps the
-- counterparty could not fill.
CREATE MATERIALIZED VIEW feed_health AS
SELECT _fix_session AS session,
       COUNT(*) AS reports,
       MAX(_fix_seq_num) AS last_seq,
       MAX(_fix_series) AS series,
       SUM(CASE WHEN _fix_poss_dup THEN 1 ELSE 0 END) AS resent,
       SUM(CASE WHEN _fix_gap_before THEN 1 ELSE 0 END) AS possible_gaps,
       MAX(_fix_received_at) AS last_received
FROM executions
GROUP BY _fix_session;

-- Audit trail: every report, exactly once, in a Delta Lake table.
CREATE SINK audit_trail FROM executions INTO "delta-lake" (
    'table.path' = '${DESK_DATA}/audit',
    'write.mode' = 'append',
    'auto.create' = 'true'
);
