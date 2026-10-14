-- Drop-copy reconciliation in DuckDB: every ExecutionReport the venue sent (its ledger) against
-- the Delta Lake audit trail LaminarDB wrote. Run through `./run.sh reconcile`, which sets
-- DESK_DATA. Stop the venue and allow one checkpoint interval first.
INSTALL delta;
LOAD delta;

CREATE TEMP TABLE sent AS
SELECT exec_id FROM read_csv(getenv('DESK_DATA') || '/venue/ledger.csv', all_varchar = true);

CREATE TEMP TABLE audit AS
SELECT exec_id FROM delta_scan(getenv('DESK_DATA') || '/audit');

SELECT (SELECT count(*) FROM sent) AS venue_sent,
       (SELECT count(*) FROM audit) AS audit_rows,
       (SELECT count(*) - count(DISTINCT exec_id) FROM audit) AS duplicates,
       (SELECT count(*) FROM sent ANTI JOIN audit USING (exec_id)) AS missing,
       (SELECT count(*) FROM audit ANTI JOIN sent USING (exec_id)) AS unexpected;
