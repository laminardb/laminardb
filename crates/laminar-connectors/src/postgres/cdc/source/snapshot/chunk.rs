//! One chunk: a short read-only transaction that locks the table, reads its snapshot and WAL
//! position, passes the visibility guards, and reads the next primary-key range.

use std::time::Duration;

use tokio::time::Instant;
use tokio_postgres::error::SqlState;
use tokio_postgres::{Client, SimpleQueryMessage};

use crate::connector::ConnectorTaskOwner;
use crate::error::ConnectorError;

use super::super::super::config::PostgresCdcConfig;
use super::super::super::postgres_io::{
    self, layout_fingerprint_sql, ControlConnection, CONNECT_TIMEOUT,
};
use super::super::super::typed_rows::RowLayout;
use super::super::{RowImage, RowValues};
use super::classify::key_row;
use super::merge::{encode_key, HeldRows};
use super::{ChunkedSnapshot, CopyPlan, HeldChunk, Lsn, PgSnapshot, Upper};

/// A chunk is an atomic replay unit, so its size is bounded by memory rather than `max_records`.
const MAX_CHUNK_ROWS: usize = 65_536;
/// Above the server-side bounds of one attempt: a 5 s lock wait and 20 s statements.
const ATTEMPT_TIMEOUT: Duration = Duration::from_secs(30);
const RETRY_MIN: Duration = Duration::from_millis(100);
const RETRY_MAX: Duration = Duration::from_secs(1);
const STUCK_REPORT_INTERVAL: Duration = Duration::from_secs(30);
const WAL_STATUS_INTERVAL: Duration = Duration::from_secs(10);
const REFERENCE_MAX_AGE: Duration = Duration::from_secs(60);
const MAX_PENDING: usize = 1_024;
const REPORTED_XIDS: usize = 20;

/// Backoff of chunk attempts that must wait: a lock, a lost session, or a transaction that has
/// reached the stream but not yet the procarray.
#[derive(Debug, Default)]
pub(super) struct Retry {
    at: Option<Instant>,
    delay: Duration,
    since: Option<Instant>,
    reported: Option<Instant>,
}

impl Retry {
    fn ready(&self) -> bool {
        self.at.is_none_or(|at| Instant::now() >= at)
    }

    fn schedule(&mut self) {
        let now = Instant::now();
        self.delay = (self.delay * 2).clamp(RETRY_MIN, RETRY_MAX);
        self.at = Some(now + self.delay);
        self.since.get_or_insert(now);
    }

    /// Whether a wait has lasted long enough to be reported, at most once per interval.
    fn report_due(&mut self) -> bool {
        let now = Instant::now();
        let stuck = self
            .since
            .is_some_and(|since| now.duration_since(since) >= STUCK_REPORT_INTERVAL);
        let due = stuck
            && self
                .reported
                .is_none_or(|at| now.duration_since(at) >= STUCK_REPORT_INTERVAL);
        if due {
            self.reported = Some(now);
        }
        due
    }
}

/// Why a chunk attempt ended without a held chunk.
enum Stop {
    /// Try again after the backoff.
    Retry(String),
    /// The source fails.
    Fail(ConnectorError),
}

impl From<ConnectorError> for Stop {
    fn from(error: ConnectorError) -> Self {
        match error {
            ConnectorError::ConnectionFailed(reason) => Self::Retry(reason),
            error => Self::Fail(error),
        }
    }
}

fn statement_failure(context: &str, error: &tokio_postgres::Error) -> Stop {
    if error.code() == Some(&SqlState::LOCK_NOT_AVAILABLE) {
        return Stop::Retry(format!("PostgreSQL CDC {context}: {error}"));
    }
    Stop::from(statement_error(context, error))
}

/// A lost session is retryable; any other statement failure is not.
pub(super) fn statement_error(context: &str, error: &tokio_postgres::Error) -> ConnectorError {
    let message = format!("PostgreSQL CDC {context}: {error}");
    if postgres_io::is_connection_failure(error.code().map(SqlState::code)) {
        ConnectorError::ConnectionFailed(message)
    } else {
        ConnectorError::ReadError(message)
    }
}

fn begin_statement(plan: &CopyPlan) -> String {
    // The lock comes before the snapshot: a table rewrite that commits between a snapshot and
    // the read leaves that snapshot an empty table.
    format!(
        "BEGIN ISOLATION LEVEL REPEATABLE READ, READ ONLY; \
         SET LOCAL lock_timeout = '5s'; SET LOCAL statement_timeout = '20s'; \
         LOCK TABLE {} IN ACCESS SHARE MODE; \
         SELECT pg_catalog.pg_current_snapshot()::text, \
                pg_catalog.pg_current_wal_insert_lsn()::text, {}",
        plan.table,
        layout_fingerprint_sql(plan.relation_id)
    )
}

fn read_statement(plan: &CopyPlan, bound: Option<&[String]>, rows: usize) -> String {
    let keys = plan
        .keys
        .iter()
        .map(|key| key.name.as_str())
        .collect::<Vec<_>>()
        .join(", ");
    let after = bound.map_or_else(String::new, |bound| {
        format!(" WHERE ROW({keys}) > {}", key_row(plan, bound))
    });
    // The marker is WAL written after `high`: once the stream decoded it, a keepalive's
    // position is past `high` even when nothing else is written.
    format!(
        "SELECT {} FROM {}{after} ORDER BY {keys} LIMIT {rows}; \
         SELECT pg_catalog.pg_logical_emit_message(false, 'laminar', '', true); COMMIT",
        plan.columns, plan.table
    )
}

/// The rows of the `statement`-th statement (from 0) of a simple query.
fn statement_rows(
    messages: &[SimpleQueryMessage],
    statement: usize,
) -> impl Iterator<Item = &tokio_postgres::SimpleQueryRow> {
    let mut completed = 0;
    messages.iter().filter_map(move |message| match message {
        SimpleQueryMessage::Row(row) if completed == statement => Some(row),
        SimpleQueryMessage::CommandComplete(_) => {
            completed += 1;
            None
        }
        _ => None,
    })
}

/// The statement of [`begin_statement`] that reads the snapshot, after `BEGIN`, two `SET`s and
/// the `LOCK`.
const SNAPSHOT_STATEMENT: usize = 4;

/// End a failed attempt's transaction; `false` when the session must be replaced.
async fn rollback(client: &Client) -> bool {
    match tokio::time::timeout(CONNECT_TIMEOUT, client.batch_execute("ROLLBACK")).await {
        Ok(Ok(())) => true,
        Ok(Err(error)) => {
            tracing::debug!(%error, "PostgreSQL CDC chunk rollback failed; the session is replaced");
            false
        }
        Err(_) => false,
    }
}

/// The fetched rows of one chunk.
struct ReadRows {
    rows: HeldRows,
    last: Option<RowValues>,
    count: usize,
    widest_text_bytes: usize,
}

fn held_rows(
    plan: &CopyPlan,
    layout: &RowLayout,
    messages: &[SimpleQueryMessage],
) -> Result<ReadRows, ConnectorError> {
    let key_columns: Vec<usize> = plan.keys.iter().map(|key| key.declared).collect();
    let mut read = ReadRows {
        rows: HeldRows::default(),
        last: None,
        count: 0,
        widest_text_bytes: 0,
    };
    for row in statement_rows(messages, 0) {
        let values: RowValues = (0..layout.columns.len())
            .map(|index| row.get(index).map(|text| text.as_bytes().into()))
            .collect();
        let text_bytes = values
            .iter()
            .map(|value| value.as_deref().map_or(0, <[u8]>::len))
            .sum::<usize>();
        read.widest_text_bytes = read.widest_text_bytes.max(text_bytes);
        let key = encode_key(&values, &key_columns)?;
        let bytes = layout.planned_row_bytes(text_bytes)?;
        read.rows.apply(RowImage::Put, key, values.clone(), bytes)?;
        read.last = Some(values);
        read.count += 1;
    }
    Ok(read)
}

impl ChunkedSnapshot {
    /// The snapshot session, opened if needed.
    async fn take_connection(
        &mut self,
        config: &PostgresCdcConfig,
        owner: &ConnectorTaskOwner,
    ) -> Result<ControlConnection, ConnectorError> {
        if let Some(connection) = self.connection.take() {
            return Ok(connection);
        }
        let guard = owner.track().ok_or_else(|| {
            ConnectorError::Internal(
                "PostgreSQL CDC connector generation is already retired".into(),
            )
        })?;
        postgres_io::connect(config, &self.application_name, guard).await
    }

    fn keep_connection(&mut self, connection: ControlConnection) {
        if !connection.client().is_closed() {
            self.connection = Some(connection);
        }
    }

    /// Keep the session after `error` unless the error suggests it is unusable, such as a
    /// timed-out statement whose answer may still arrive.
    fn keep_connection_after(&mut self, connection: ControlConnection, error: &ConnectorError) {
        if !matches!(error, ConnectorError::ConnectionFailed(_)) {
            self.keep_connection(connection);
        }
    }

    /// Attempt the next chunk once the backoff allows. A wait (lock, lost session, a guard not
    /// yet satisfied) schedules a retry; only a contract or invariant failure is an error.
    pub(super) async fn fetch(
        &mut self,
        config: &PostgresCdcConfig,
        owner: &ConnectorTaskOwner,
        layout: &RowLayout,
        slot: &str,
    ) -> Result<(), ConnectorError> {
        if !self.retry.ready() {
            return Ok(());
        }
        let attempt = match self.take_connection(config, owner).await {
            Ok(connection) => {
                self.attempt_on(connection, layout, config.arrow_build_bytes(), slot)
                    .await
            }
            Err(error) => Err(Stop::from(error)),
        };
        match attempt {
            Ok(held) => {
                self.retry = Retry::default();
                self.floor = None;
                self.held = Some(held);
                Ok(())
            }
            Err(Stop::Retry(reason)) => {
                self.wait_after(&reason);
                Ok(())
            }
            Err(Stop::Fail(error)) => Err(error),
        }
    }

    /// Whether the snapshot session was lost and its reconnection is still backing off.
    pub(super) fn reconnect_pending(&self) -> bool {
        self.connection.is_none() && !self.retry.ready()
    }

    /// Back off copying after a retryable failure, warning when it has lasted 30 seconds.
    pub(super) fn wait_after(&mut self, reason: &str) {
        self.retry.schedule();
        if self.retry.report_due() {
            tracing::warn!(
                table = %self.plan.table,
                %reason,
                "PostgreSQL CDC initial snapshot keeps waiting to copy; streaming continues"
            );
        } else {
            tracing::debug!(%reason, "PostgreSQL CDC chunk will be retried");
        }
    }

    /// One bounded attempt on `connection`, kept only when it is known to be outside a
    /// transaction: a timed-out statement's answer may still arrive on it.
    async fn attempt_on(
        &mut self,
        connection: ControlConnection,
        layout: &RowLayout,
        budget: usize,
        slot: &str,
    ) -> Result<HeldChunk, Stop> {
        let Ok(attempt) = tokio::time::timeout(
            ATTEMPT_TIMEOUT,
            self.attempt(connection.client(), layout, budget, slot),
        )
        .await
        else {
            return Err(Stop::Retry(format!(
                "a chunk attempt took more than {} seconds; the session is replaced",
                ATTEMPT_TIMEOUT.as_secs()
            )));
        };
        if attempt.is_ok() || rollback(connection.client()).await {
            self.keep_connection(connection);
        }
        attempt
    }

    async fn attempt(
        &mut self,
        client: &Client,
        layout: &RowLayout,
        budget: usize,
        slot: &str,
    ) -> Result<HeldChunk, Stop> {
        self.check_slot_retention(client, slot).await?;
        let messages = client
            .simple_query(&begin_statement(&self.plan))
            .await
            .map_err(|error| statement_failure("chunk snapshot", &error))?;
        let (snapshot, high) = self.chunk_snapshot(&messages)?;
        self.reference = Some((snapshot.clone(), Instant::now()));
        if !self.guards_pass(client, &snapshot).await {
            return Err(Stop::Retry(
                "transactions the copy must include are not visible yet".into(),
            ));
        }
        let statement = read_statement(&self.plan, self.bound.as_deref(), self.fetch_rows);
        let messages = match client.simple_query(&statement).await {
            Ok(messages) => messages,
            Err(error) => {
                if error.code() == Some(&SqlState::QUERY_CANCELED) {
                    self.fetch_rows = (self.fetch_rows / 2).max(1);
                    return Err(Stop::Retry(format!("chunk read timed out: {error}")));
                }
                return Err(statement_failure("chunk read", &error));
            }
        };
        self.held_chunk(layout, budget, snapshot, high, &messages)
    }

    /// Parse the chunk's snapshot, WAL position, and table layout, failing closed on drift.
    fn chunk_snapshot(
        &self,
        messages: &[SimpleQueryMessage],
    ) -> Result<(PgSnapshot, Lsn), ConnectorError> {
        let row = statement_rows(messages, SNAPSHOT_STATEMENT)
            .next()
            .ok_or_else(|| {
                ConnectorError::ReadError("PostgreSQL chunk snapshot returned no row".into())
            })?;
        let snapshot = PgSnapshot::parse(row.get(0).unwrap_or_default())?;
        let high_text = row.get(1).unwrap_or_default();
        let high = high_text.parse::<Lsn>().map_err(|error| {
            ConnectorError::ReadError(format!(
                "invalid WAL insert position '{high_text}': {error}"
            ))
        })?;
        if row.get(2) != Some(self.plan.fingerprint.as_str()) {
            return Err(ConnectorError::SchemaMismatch(format!(
                "PostgreSQL table {} changed its columns, types, replica identity, or primary key \
                 during the initial snapshot; the copy stops instead of reading a rewritten table",
                self.plan.table
            )));
        }
        Ok((snapshot, high))
    }

    /// The floor guard (the slot's snapshot is covered) and the pending guard (every
    /// transaction whose effect on uncopied keys was not emitted is visible).
    async fn guards_pass(&mut self, client: &Client, snapshot: &PgSnapshot) -> bool {
        let floor_covered = self
            .floor
            .as_ref()
            .is_none_or(|floor| snapshot.covers(floor));
        let pending_seen = snapshot.settle(&mut self.pending);
        if floor_covered && pending_seen {
            return true;
        }
        if self.retry.report_due() {
            let mut waiting: Vec<u64> = self
                .floor
                .as_ref()
                .map(|floor| snapshot.behind(floor, REPORTED_XIDS))
                .unwrap_or_default();
            waiting.extend(self.pending.iter().take(REPORTED_XIDS));
            report_waiting(client, &self.plan.table, &waiting).await;
        }
        false
    }

    fn held_chunk(
        &mut self,
        layout: &RowLayout,
        budget: usize,
        snapshot: PgSnapshot,
        high: Lsn,
        messages: &[SimpleQueryMessage],
    ) -> Result<HeldChunk, Stop> {
        let read = held_rows(&self.plan, layout, messages)?;
        self.widest_row_bytes = self.widest_row_bytes.max(read.widest_text_bytes);
        if read.rows.bytes() > budget {
            return Err(self.shrink(budget));
        }
        let upper = match (read.count >= self.fetch_rows, read.last) {
            (true, Some(last)) => Upper::Key(self.key_texts(&last)?),
            _ => Upper::End,
        };
        let widest = layout.planned_row_bytes(self.widest_row_bytes)?.max(1);
        self.fetch_rows = (budget / 2 / widest).clamp(1, MAX_CHUNK_ROWS);
        Ok(HeldChunk {
            upper,
            snapshot,
            high,
            rows: read.rows,
            passed: false,
            keepalive_requested: Instant::now(),
        })
    }

    /// Halve the chunk after it outgrew the Arrow-build budget; one row that does not fit fails.
    fn shrink(&mut self, budget: usize) -> Stop {
        if self.fetch_rows <= 1 {
            return Stop::Fail(ConnectorError::ReadError(format!(
                "PostgreSQL CDC snapshot rows of up to {} bytes exceed the Arrow-build budget of \
                 {budget} bytes; raise max.buffered.bytes",
                self.widest_row_bytes
            )));
        }
        self.fetch_rows /= 2;
        Stop::Retry("the chunk outgrew the Arrow-build budget".into())
    }

    /// Drop the held chunk after merged changes grew it past the budget, and fetch a smaller one
    /// later. Every transaction merged into it is pending, so the next chunk's snapshot includes
    /// it.
    pub(super) fn abandon_held(&mut self) {
        self.held = None;
        self.fetch_rows = (self.fetch_rows / 2).max(1);
    }

    /// The primary-key texts of `values` in index order.
    fn key_texts(&self, values: &RowValues) -> Result<Vec<String>, ConnectorError> {
        self.plan
            .keys
            .iter()
            .map(|key| {
                let bytes = values
                    .get(key.declared)
                    .and_then(Option::as_deref)
                    .ok_or_else(|| {
                        ConnectorError::ReadError("PostgreSQL chunk row has a NULL key".into())
                    })?;
                String::from_utf8(bytes.to_vec()).map_err(|_| {
                    ConnectorError::ReadError("PostgreSQL primary-key text is not UTF-8".into())
                })
            })
            .collect()
    }

    /// Fail before streaming would find the retained WAL removed.
    async fn check_slot_retention(&mut self, client: &Client, slot: &str) -> Result<(), Stop> {
        if Instant::now() < self.next_wal_status {
            return Ok(());
        }
        self.next_wal_status = Instant::now() + WAL_STATUS_INTERVAL;
        let (status, safe_bytes) = postgres_io::slot_wal_status(client, slot).await?;
        match status.as_str() {
            "lost" => Err(Stop::Fail(ConnectorError::ReadError(format!(
                "PostgreSQL removed WAL retained by slot '{slot}' during the initial snapshot \
                 (max_slot_wal_keep_size); {}",
                super::super::claim::reset_instructions(slot)
            )))),
            "unreserved" => {
                tracing::warn!(
                    slot,
                    safe_wal_size = ?safe_bytes,
                    "PostgreSQL CDC initial snapshot is close to losing its retained WAL"
                );
                Ok(())
            }
            _ => Ok(()),
        }
    }

    /// Refresh the snapshot that stream xids are widened against, and prune pending xids that
    /// became visible, when the reference is old or too many are pending.
    ///
    /// # Errors
    /// Returns `ConnectionFailed` when the session is lost; the drain then waits.
    pub(super) async fn refresh_reference(
        &mut self,
        config: &PostgresCdcConfig,
        owner: &ConnectorTaskOwner,
    ) -> Result<(), ConnectorError> {
        let fresh = self
            .reference
            .as_ref()
            .is_some_and(|(_, taken)| taken.elapsed() < REFERENCE_MAX_AGE);
        if fresh && self.pending.len() <= MAX_PENDING {
            return Ok(());
        }
        let connection = self.take_connection(config, owner).await?;
        let taken = tokio::time::timeout(
            CONNECT_TIMEOUT,
            connection
                .client()
                .simple_query("SELECT pg_catalog.pg_current_snapshot()::text"),
        )
        .await;
        let messages = match taken {
            Ok(Ok(messages)) => messages,
            Ok(Err(error)) => {
                let error = statement_error("snapshot refresh", &error);
                self.keep_connection_after(connection, &error);
                return Err(error);
            }
            Err(_) => {
                return Err(ConnectorError::ConnectionFailed(
                    "PostgreSQL CDC snapshot refresh timed out after 10 seconds".into(),
                ));
            }
        };
        self.keep_connection(connection);
        let text = statement_rows(&messages, 0)
            .next()
            .and_then(|row| row.get(0))
            .ok_or_else(|| ConnectorError::ReadError("pg_current_snapshot returned no row".into()))?
            .to_string();
        let snapshot = PgSnapshot::parse(&text)?;
        snapshot.settle(&mut self.pending);
        self.reference = Some((snapshot, Instant::now()));
        Ok(())
    }

    /// Classify stream keys on the snapshot session; see [`super::classify::classify`].
    pub(super) async fn classify(
        &mut self,
        config: &PostgresCdcConfig,
        owner: &ConnectorTaskOwner,
        keys: &[Vec<&str>],
    ) -> Result<Vec<(bool, bool)>, ConnectorError> {
        if self.bound.is_none() && self.held.is_none() {
            return Ok(vec![(false, false); keys.len()]);
        }
        let connection = self.take_connection(config, owner).await?;
        let classified = super::classify::classify(
            connection.client(),
            &self.plan,
            self.bound.as_deref(),
            self.held.as_ref().map(|held| &held.upper),
            keys,
        )
        .await;
        match &classified {
            Ok(_) => self.keep_connection(connection),
            Err(error) => self.keep_connection_after(connection, error),
        }
        classified
    }
}

/// Warn about the sessions whose transactions keep the copy waiting, with their wait events: a
/// `SyncRep` wait means a synchronous standby has not confirmed the commit.
async fn report_waiting(client: &Client, table: &str, xids: &[u64]) {
    let low_words: Vec<String> = xids
        .iter()
        .map(|xid| (xid % (1 << 32)).to_string())
        .collect();
    let sessions = client
        .query(
            "SELECT pg_catalog.format('pid %s xid %s (state %s, wait %s/%s, application_name %L)', \
                    pid, backend_xid, state, wait_event_type, wait_event, application_name) \
             FROM pg_catalog.pg_stat_activity WHERE backend_xid::text = ANY($1)",
            &[&low_words],
        )
        .await
        .map(|rows| rows.iter().map(|row| row.get::<_, String>(0)).collect::<Vec<_>>());
    match sessions {
        Ok(sessions) => tracing::warn!(
            table,
            xids = ?xids,
            sessions = %sessions.join("; "),
            "PostgreSQL CDC initial snapshot is waiting for committed transactions to become \
             visible; the stream keeps flowing, only copying waits"
        ),
        Err(error) => tracing::warn!(
            table,
            xids = ?xids,
            %error,
            "PostgreSQL CDC initial snapshot is waiting for committed transactions to become \
             visible"
        ),
    }
}
