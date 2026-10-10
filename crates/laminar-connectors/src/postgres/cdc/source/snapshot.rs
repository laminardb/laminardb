//! Initial snapshot read through the slot's exported snapshot.
//!
//! The snapshot transaction imports the snapshot exported when the slot was created, so it sees
//! exactly the transactions committed before the slot's consistent point; streaming then starts
//! at that point with no gap or overlap. An exported snapshot cannot be re-imported once its
//! session ends, so an interrupted snapshot is not resumable.
//!
//! No row is emitted until a cursor naming the slot's snapshot phase has committed. A restart
//! before that resumes from the claim, which copies the table again from a new slot; without the
//! gate, rows a sink already wrote could go stale behind that second copy.

use std::time::Duration;

use arrow_array::{RecordBatch, UInt32Array};
use tokio_postgres::error::SqlState;
use tokio_postgres::SimpleQueryMessage;

use crate::connector::SourceBatch;
use crate::error::ConnectorError;

use super::super::config::PostgresCdcConfig;
use super::super::postgres_io::{self, ControlConnection};
use super::super::typed_rows::{RowBuilder, RowLayout};
use super::drain::snapshot_order_key;
use super::{Lsn, Phase, PostgresCdcSource};

const CURSOR: &str = "laminar_cdc_snapshot";
/// Row widths are unknown until the first fetch, which therefore reads one row; later fetches
/// are sized from the widest row seen.
const INITIAL_FETCH_ROWS: usize = 1;
const WAL_STATUS_INTERVAL: Duration = Duration::from_secs(10);

/// An open cursor over the declared columns inside the imported snapshot.
pub(super) struct SnapshotReader {
    connection: ControlConnection,
    consistent_point: Lsn,
    fetch_rows: usize,
    widest_row_bytes: usize,
    emitted_rows: u64,
    next_wal_status: tokio::time::Instant,
    /// A cursor naming this snapshot committed, so rows may be emitted.
    released: bool,
}

fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

impl SnapshotReader {
    /// Import `snapshot_name` and declare the read cursor.
    ///
    /// Must complete while the exporting replication session is still idle.
    ///
    /// # Errors
    /// Returns an error when the snapshot cannot be imported or the cursor declared.
    pub(super) async fn open(
        connection: ControlConnection,
        config: &PostgresCdcConfig,
        layout: &RowLayout,
        snapshot_name: &str,
        consistent_point: Lsn,
    ) -> Result<Self, ConnectorError> {
        if snapshot_name.is_empty()
            || !snapshot_name
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() || byte == b'-')
        {
            return Err(ConnectorError::ReadError(format!(
                "PostgreSQL returned an unexpected exported snapshot name '{snapshot_name}'"
            )));
        }
        let columns = layout
            .columns
            .iter()
            .map(|column| quote_identifier(&column.name))
            .collect::<Vec<_>>()
            .join(", ");
        // The transaction idles until the snapshot gate opens, about one checkpoint interval; a
        // server idle-in-transaction timeout must not mistake that wait for an abandoned session.
        let statement = format!(
            "BEGIN ISOLATION LEVEL REPEATABLE READ, READ ONLY; \
             SET TRANSACTION SNAPSHOT '{snapshot_name}'; \
             SET LOCAL idle_in_transaction_session_timeout = 0; \
             DECLARE {CURSOR} NO SCROLL CURSOR FOR SELECT {columns} FROM {}.{}",
            quote_identifier(&config.table.schema),
            quote_identifier(&config.table.name)
        );
        tokio::time::timeout(
            postgres_io::CONNECT_TIMEOUT,
            connection.client().batch_execute(&statement),
        )
        .await
        .map_err(|_| {
            ConnectorError::ConnectionFailed("snapshot import timed out after 10 seconds".into())
        })?
        .map_err(|error| {
            let message = format!("import PostgreSQL snapshot: {error}");
            // Retrying a refusal (privileges, a dropped table) would create a slot per attempt.
            if postgres_io::is_connection_failure(error.code().map(SqlState::code)) {
                ConnectorError::ConnectionFailed(message)
            } else {
                ConnectorError::ConfigurationError(message)
            }
        })?;
        Ok(Self {
            connection,
            consistent_point,
            fetch_rows: INITIAL_FETCH_ROWS,
            widest_row_bytes: 0,
            emitted_rows: 0,
            next_wal_status: tokio::time::Instant::now(),
            released: false,
        })
    }

    pub(super) fn release(&mut self) {
        self.released = true;
    }

    /// Fail before streaming would find the retained WAL already removed.
    async fn check_slot_retention(&mut self, slot_name: &str) -> Result<(), ConnectorError> {
        if tokio::time::Instant::now() < self.next_wal_status {
            return Ok(());
        }
        self.next_wal_status = tokio::time::Instant::now() + WAL_STATUS_INTERVAL;
        let (status, safe_bytes) =
            postgres_io::slot_wal_status(self.connection.client(), slot_name).await?;
        match status.as_str() {
            "lost" => Err(ConnectorError::ReadError(format!(
                "PostgreSQL removed WAL retained by slot '{slot_name}' during the initial \
                 snapshot (max_slot_wal_keep_size); drop the slot, clear downstream targets, and \
                 restart with more WAL headroom"
            ))),
            "unreserved" => {
                tracing::warn!(
                    slot = slot_name,
                    safe_wal_size = ?safe_bytes,
                    "PostgreSQL CDC initial snapshot is close to losing its retained WAL"
                );
                Ok(())
            }
            _ => Ok(()),
        }
    }

    /// Fetch the next rows, or `None` once the table is exhausted.
    async fn fetch(
        &mut self,
        layout: &RowLayout,
        rows: &mut RowBuilder,
        max_records: usize,
        byte_budget: usize,
    ) -> Result<Option<(RecordBatch, u64)>, ConnectorError> {
        let count = self.fetch_rows.min(max_records).max(1);
        let messages = self
            .connection
            .client()
            .simple_query(&format!("FETCH FORWARD {count} FROM {CURSOR}"))
            .await
            .map_err(|error| {
                ConnectorError::ReadError(format!("PostgreSQL snapshot fetch: {error}"))
            })?;
        for message in &messages {
            let SimpleQueryMessage::Row(row) = message else {
                continue;
            };
            let text_bytes = (0..layout.columns.len())
                .map(|index| row.get(index).map_or(0, str::len))
                .sum::<usize>();
            self.widest_row_bytes = self.widest_row_bytes.max(text_bytes);
            rows.append(
                layout,
                |index, _| Ok(row.get(index).map(str::as_bytes)),
                false,
                layout.weighted.then_some(1),
                layout.planned_row_bytes(text_bytes)?,
            )?;
        }
        if rows.len() == 0 {
            return Ok(None);
        }
        if rows.retained_bytes() > byte_budget {
            return Err(ConnectorError::ReadError(format!(
                "PostgreSQL CDC snapshot rows of up to {} bytes exceed the Arrow-build budget of \
                 {byte_budget} bytes; raise max.buffered.bytes",
                self.widest_row_bytes
            )));
        }
        let widest = layout.planned_row_bytes(self.widest_row_bytes)?.max(1);
        self.fetch_rows = (byte_budget / 2 / widest).clamp(1, max_records.max(1));
        let first_ordinal = self.emitted_rows;
        let emitted = u64::try_from(rows.len())
            .map_err(|_| ConnectorError::Internal("snapshot row count overflow".into()))?;
        self.emitted_rows = self.emitted_rows.saturating_add(emitted);
        Ok(Some((rows.finish(layout)?, first_ordinal)))
    }

    /// End the snapshot transaction and close its session.
    async fn finish(self) -> Result<(), ConnectorError> {
        let committed = self.connection.client().batch_execute("COMMIT").await;
        self.connection.close().await;
        committed.map_err(|error| {
            ConnectorError::ReadError(format!("PostgreSQL snapshot commit: {error}"))
        })
    }
}

impl PostgresCdcSource {
    pub(super) async fn poll_snapshot(
        &mut self,
        max_records: usize,
    ) -> Result<Option<SourceBatch>, ConnectorError> {
        let byte_budget = self.config.arrow_build_bytes();
        let slot = self.slot_name()?.to_string();
        let (Phase::Snapshot(reader), Some(layout), Some(rows)) = (
            &mut self.phase,
            self.layout.as_ref(),
            self.open_rows.as_mut(),
        ) else {
            return Err(ConnectorError::Internal(
                "PostgreSQL CDC snapshot state is incomplete".into(),
            ));
        };
        if !reader.released {
            return Ok(None);
        }
        let fetched = match reader.check_slot_retention(&slot).await {
            Ok(()) => reader.fetch(layout, rows, max_records, byte_budget).await,
            Err(error) => Err(error),
        };
        match fetched {
            Ok(Some((records, first_ordinal))) => self
                .snapshot_batch(records, first_ordinal)
                .map_err(|error| self.fail(error))
                .map(Some),
            Ok(None) => {
                self.complete_snapshot().await?;
                Ok(None)
            }
            Err(error) => Err(self.fail(error)),
        }
    }

    fn snapshot_batch(
        &self,
        records: RecordBatch,
        first_ordinal: u64,
    ) -> Result<SourceBatch, ConnectorError> {
        let rows = records.num_rows();
        let order_keys = (0..rows)
            .map(|row| {
                u64::try_from(row)
                    .ok()
                    .and_then(|row| first_ordinal.checked_add(row))
                    .map(snapshot_order_key)
                    .ok_or_else(|| ConnectorError::Internal("snapshot row ordinal overflow".into()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let positions = self.row_positions(
            order_keys.iter().map(<[u8; 9]>::as_slice),
            UInt32Array::from(vec![0; rows]),
        )?;
        self.metrics
            .record_snapshot_rows(u64::try_from(rows).unwrap_or(u64::MAX));
        self.metrics.record_batch();
        Ok(SourceBatch::positioned(records, positions)?.with_checkpoint(self.cursor()))
    }

    /// Hand off from the finished snapshot to streaming at the slot's consistent point.
    async fn complete_snapshot(&mut self) -> Result<(), ConnectorError> {
        let Phase::Snapshot(reader) = std::mem::replace(&mut self.phase, Phase::Idle) else {
            return Err(ConnectorError::Internal(
                "PostgreSQL CDC snapshot completed outside the snapshot phase".into(),
            ));
        };
        let consistent_point = reader.consistent_point;
        let rows = reader.emitted_rows;
        self.polled_lsn = consistent_point;
        if let Err(error) = reader.finish().await {
            return Err(self.fail(error));
        }
        tracing::info!(
            table = %self.config.table,
            rows,
            %consistent_point,
            "PostgreSQL CDC initial snapshot complete; streaming from the slot's consistent point"
        );
        self.begin_streaming(consistent_point).await
    }
}
