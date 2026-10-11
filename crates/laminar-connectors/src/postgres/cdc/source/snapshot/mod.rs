//! Chunked initial snapshot.
//!
//! The slot streams from its consistent point while the table is copied in primary-key chunks,
//! each read in its own short `REPEATABLE READ` transaction. A streamed change belongs to a chunk
//! when the chunk's snapshot does not yet include its transaction; that is decided by
//! transaction visibility, not by LSN, because a transaction can reach the slot before
//! snapshots treat it as committed. A fetched chunk is held, with such changes merged into it,
//! until the stream has passed the WAL position read with its snapshot; it is then emitted as
//! the table's state at that position. The cursor carries the stream position and the copy's
//! progress, so slot feedback continues during the copy and an upsert source resumes inside it.
//!
//! Nothing is read or emitted until a cursor naming the slot's snapshot phase has committed: a
//! restart from the claim may copy from another point.

mod chunk;
mod classify;
mod merge;
mod stream;
mod visibility;

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow_array::{RecordBatch, UInt32Array};
use tokio::time::Instant;

use crate::connector::{SourceBatch, SourceMutation};
use crate::error::ConnectorError;

use super::super::config::TableName;
use super::super::postgres_io::{ControlConnection, IndexKeyColumn};
use super::super::typed_rows::{RowBuilder, RowLayout};
use super::checkpoint::SnapshotPosition;
use super::drain::chunk_order_key;
use super::{Lsn, Phase, PostgresCdcSource};
use chunk::Retry;
use merge::HeldRows;
pub(crate) use visibility::PgSnapshot;

/// The copy's fixed SQL pieces, read at startup.
#[derive(Debug, Clone)]
pub(crate) struct CopyPlan {
    /// Quoted `schema.table`.
    pub(crate) table: String,
    /// Quoted declared columns, in declared order.
    pub(crate) columns: String,
    /// The primary key in index order.
    pub(crate) keys: Vec<KeyColumn>,
    pub(crate) relation_id: u32,
    /// The table layout at startup; a chunk transaction that sees another fails closed, since a
    /// rewrite such as `ALTER COLUMN … TYPE` is not MVCC-safe.
    pub(crate) fingerprint: String,
}

/// One primary-key column in index order.
#[derive(Debug, Clone)]
pub(crate) struct KeyColumn {
    /// Position among the declared columns.
    pub(crate) declared: usize,
    /// Quoted column name.
    pub(crate) name: String,
    /// `format_type` of the column.
    pub(crate) type_sql: String,
    /// Quoted, schema-qualified index collation, for collatable types.
    pub(crate) collation: Option<String>,
}

fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

impl CopyPlan {
    /// The copy of `table` into `layout`, ordered by the primary-key index `keys`.
    ///
    /// # Errors
    /// Returns an error when a key column is not declared.
    pub(crate) fn new(
        table: &TableName,
        layout: &RowLayout,
        relation_id: u32,
        keys: &[IndexKeyColumn],
        fingerprint: String,
    ) -> Result<Self, ConnectorError> {
        let keys = keys
            .iter()
            .map(|key| {
                let declared = layout
                    .columns
                    .iter()
                    .position(|column| column.name == key.name)
                    .ok_or_else(|| {
                        ConnectorError::SchemaMismatch(format!(
                            "primary-key column '{}' of PostgreSQL table {table} is not declared",
                            key.name
                        ))
                    })?;
                Ok(KeyColumn {
                    declared,
                    name: quote_identifier(&key.name),
                    type_sql: key.type_sql.clone(),
                    collation: key.collation.clone(),
                })
            })
            .collect::<Result<Vec<_>, ConnectorError>>()?;
        let columns = layout
            .columns
            .iter()
            .map(|column| quote_identifier(&column.name))
            .collect::<Vec<_>>()
            .join(", ");
        Ok(Self {
            table: format!(
                "{}.{}",
                quote_identifier(&table.schema),
                quote_identifier(&table.name)
            ),
            columns,
            keys,
            relation_id,
            fingerprint,
        })
    }
}

/// Why the chunked copy cannot read `table` exactly, or `None` when it can. Chunk bounds and
/// streamed keys are compared on the server with the index's collations, which must agree with
/// equality of the key texts, and the copy reads WAL positions only a primary has.
pub(crate) fn copy_problem(
    table: &TableName,
    in_recovery: bool,
    keys: &[IndexKeyColumn],
) -> Option<String> {
    if in_recovery {
        return Some(format!(
            "the initial snapshot of {table} needs a primary server: it reads WAL insert \
             positions, which a standby does not have; connect to the primary or use \
             snapshot.mode=never"
        ));
    }
    if keys.is_empty() {
        return Some(format!("PostgreSQL table {table} has no primary-key index"));
    }
    keys.iter().find_map(|key| {
        let name = &key.name;
        if !key.deterministic {
            Some(format!(
                "primary-key column '{name}' of {table} uses a non-deterministic collation, so \
                 different key texts can be equal; the initial snapshot cannot match streamed \
                 keys to copied ones"
            ))
        } else if !key.column_collation {
            Some(format!(
                "the primary-key index of {table} orders column '{name}' with a collation other \
                 than the column's"
            ))
        } else if !key.default_opclass {
            Some(format!(
                "the primary-key index of {table} uses a non-default operator class for column \
                 '{name}'"
            ))
        } else {
            None
        }
    })
}

/// The upper end of a fetched chunk's key range.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Upper {
    /// The chunk's last key, in index order.
    Key(Vec<String>),
    /// The chunk read to the end of the table.
    End,
}

/// A fetched chunk waiting for the stream to pass `high`.
struct HeldChunk {
    upper: Upper,
    snapshot: PgSnapshot,
    /// `pg_current_wal_insert_lsn()` read after the snapshot was taken: every transaction the
    /// snapshot sees ends at or before it.
    high: Lsn,
    rows: HeldRows,
    /// A keepalive reached `high`, so every transaction ending by `high` has arrived.
    passed: bool,
    keepalive_requested: Instant,
    /// Rows emitted so far, and the slot of `rows` the next piece starts at.
    emitted: usize,
    next_slot: usize,
}

/// How often a held chunk asks again for a keepalive: an answer sent before the walsender
/// decoded the chunk's marker stays below `high`.
const KEEPALIVE_REQUEST_INTERVAL: std::time::Duration = std::time::Duration::from_millis(100);

impl HeldChunk {
    fn keepalive_due(&mut self) -> bool {
        let due = !self.passed && self.keepalive_requested.elapsed() >= KEEPALIVE_REQUEST_INTERVAL;
        if due {
            self.keepalive_requested = Instant::now();
        }
        due
    }
}

/// State of the copy between polls.
pub(super) struct ChunkedSnapshot {
    plan: Arc<CopyPlan>,
    application_name: String,
    /// A cursor naming the slot's snapshot phase committed.
    released: bool,
    /// Every key at or below this, in index order, is copied and emitted.
    bound: Option<Vec<String>>,
    chunks: u64,
    /// Transactions whose effect on keys not yet emitted as copied state was dropped or merged
    /// into a held chunk; every later chunk snapshot must see them.
    pending: BTreeSet<u64>,
    /// The slot's visibility snapshot, until a chunk snapshot covered it.
    floor: Option<PgSnapshot>,
    held: Option<HeldChunk>,
    /// The latest snapshot taken, against which stream xids are widened.
    reference: Option<(PgSnapshot, Instant)>,
    fetch_rows: usize,
    widest_row_bytes: usize,
    retry: Retry,
    next_wal_status: Instant,
    connection: Option<ControlConnection>,
    copied_rows: u64,
}

impl ChunkedSnapshot {
    /// The copy of a new slot from its visibility snapshot, held until a cursor naming the slot
    /// commits.
    pub(super) fn new(
        plan: Arc<CopyPlan>,
        application_name: String,
        floor: Option<PgSnapshot>,
    ) -> Self {
        Self {
            plan,
            application_name,
            released: false,
            bound: None,
            chunks: 0,
            pending: BTreeSet::new(),
            floor,
            held: None,
            reference: None,
            fetch_rows: 1,
            widest_row_bytes: 0,
            retry: Retry::default(),
            next_wal_status: Instant::now(),
            connection: None,
            copied_rows: 0,
        }
    }

    /// The copy a committed cursor recorded.
    ///
    /// # Errors
    /// Fails closed when the recorded bound does not have one value per primary-key column.
    pub(super) fn resume(
        plan: Arc<CopyPlan>,
        application_name: String,
        position: SnapshotPosition,
    ) -> Result<Self, ConnectorError> {
        if let Some(bound) = position
            .bound
            .as_ref()
            .filter(|bound| bound.len() != plan.keys.len())
        {
            return Err(ConnectorError::ConfigurationError(format!(
                "PostgreSQL CDC checkpoint records a snapshot bound of {} values for a primary key \
                 of {} columns",
                bound.len(),
                plan.keys.len()
            )));
        }
        Ok(Self {
            released: true,
            bound: position.bound,
            chunks: position.chunks,
            pending: position.pending.into_iter().collect(),
            ..Self::new(plan, application_name, position.floor)
        })
    }

    pub(super) fn released(&self) -> bool {
        self.released
    }

    pub(super) fn release(&mut self) {
        self.released = true;
    }

    /// The cursor position: the stream at `lsn` and the copy's progress. A held chunk is never
    /// recorded; a restart fetches it again.
    pub(super) fn position(&self, consistent_point: Lsn, lsn: Lsn) -> SnapshotPosition {
        SnapshotPosition {
            consistent_point,
            lsn,
            bound: self.bound.clone(),
            chunks: self.chunks,
            pending: self.pending.iter().copied().collect(),
            floor: self.floor.clone(),
        }
    }

    /// Note a keepalive at `wal_end`; returns how far the resumable position may follow it,
    /// which is never past a held chunk's `high`.
    pub(super) fn observe_keepalive(&mut self, wal_end: Lsn) -> Lsn {
        match self.held.as_mut() {
            Some(held) => {
                held.passed |= wal_end >= held.high;
                held.high.min(wal_end)
            }
            None => wal_end,
        }
    }

    /// A released copy holding an empty final chunk read under `snapshot` at `high`.
    #[cfg(test)]
    pub(super) fn holding(mut self, snapshot: PgSnapshot, high: Lsn) -> Self {
        self.released = true;
        self.reference = Some((snapshot.clone(), Instant::now()));
        self.held = Some(HeldChunk {
            upper: Upper::End,
            snapshot,
            high,
            rows: HeldRows::default(),
            passed: false,
            keepalive_requested: Instant::now(),
            emitted: 0,
            next_slot: 0,
        });
        self
    }

    /// `high` of the held chunk: the drain stops before any transaction ending after it.
    pub(super) fn held_high(&self) -> Option<Lsn> {
        self.held.as_ref().map(|held| held.high)
    }

    /// The held chunk once the stream passed it and nothing ending at or before its `high` is
    /// left to drain.
    fn passed(&self, first_end: Option<Lsn>, last_end: Option<Lsn>) -> Option<&HeldChunk> {
        let held = self.held.as_ref()?;
        let passed = held.passed || last_end.is_some_and(|end| end > held.high);
        (passed && first_end.is_none_or(|end| end > held.high)).then_some(held)
    }

    /// Record a piece of `rows` rows of the held chunk, ending before `next_slot`, as emitted.
    /// The last piece records the chunk, advancing the bound to its upper key.
    fn record_piece(&mut self, rows: usize, next_slot: usize, last: bool) {
        self.copied_rows = self
            .copied_rows
            .saturating_add(u64::try_from(rows).unwrap_or(u64::MAX));
        let Some(held) = self.held.as_mut() else {
            return;
        };
        held.emitted = held.emitted.saturating_add(rows);
        held.next_slot = next_slot;
        if !last {
            return;
        }
        let Some(held) = self.held.take() else {
            return;
        };
        self.chunks = self.chunks.saturating_add(1);
        if let Upper::Key(upper) = held.upper {
            self.bound = Some(upper);
        }
    }
}

/// Up to `limit` held rows from slot `start` as copied state: `+1` images in changelog mode,
/// puts in upsert mode. Returns the slot after the last row taken.
fn chunk_records(
    layout: &RowLayout,
    rows: &HeldRows,
    start: usize,
    limit: usize,
) -> Result<(RecordBatch, Option<Vec<SourceMutation>>, usize), ConnectorError> {
    let mut builder = RowBuilder::new(layout);
    let mut next_slot = start;
    for (next, values, bytes) in rows.rows_from(start).take(limit) {
        builder.append(
            layout,
            |index, _| Ok(values[index].as_deref()),
            false,
            layout.weighted.then_some(1),
            bytes,
        )?;
        next_slot = next;
    }
    let mutations = (!layout.weighted).then(|| vec![SourceMutation::Put; builder.len()]);
    Ok((builder.finish(layout)?, mutations, next_slot))
}

impl PostgresCdcSource {
    /// One copy step: open the stream once released, decode, drain whole transactions, emit a
    /// held chunk once the stream passed it, or fetch the next chunk.
    pub(super) async fn poll_snapshot(
        &mut self,
        max_records: usize,
    ) -> Result<Option<SourceBatch>, ConnectorError> {
        if !matches!(&self.phase, Phase::Snapshot(snapshot) if snapshot.released()) {
            return Ok(None);
        }
        if self.wal_rx.is_none() {
            self.begin_streaming(self.polled_lsn).await?;
        }
        let decoded = self.decode_queued(max_records)?;
        let mut batch = self.drain_snapshot(max_records).await?;
        if batch.is_none() {
            batch = self
                .emit_held_chunk(max_records)
                .map_err(|error| self.fail(error))?;
        }
        if batch.is_none() && matches!(self.phase, Phase::Snapshot(_)) {
            self.advance_copy(decoded).await?;
        }
        if batch.is_some() || !self.pending_payloads.is_empty() || !self.committed.is_empty() {
            self.data_ready.notify_one();
        }
        Ok(batch)
    }

    /// Fetch the next chunk when none is held, or ask the server where the stream stands when
    /// nothing arrived while one is.
    async fn advance_copy(&mut self, decoded: bool) -> Result<(), ConnectorError> {
        let quiet = !decoded && self.committed.is_empty();
        let slot = self
            .claim
            .as_ref()
            .map(|claim| claim.slot().to_string())
            .unwrap_or_default();
        let (Phase::Snapshot(snapshot), Some(layout)) = (&mut self.phase, self.layout.as_ref())
        else {
            return Err(ConnectorError::Internal(
                "PostgreSQL CDC copy state is incomplete".into(),
            ));
        };
        let request = if let Some(held) = snapshot.held.as_mut() {
            quiet && held.keepalive_due()
        } else {
            let fetched = snapshot
                .fetch(&self.config, &self.task_owner, layout, &slot)
                .await;
            if let Err(error) = fetched {
                return Err(self.fail(error));
            }
            snapshot.held.is_some()
        };
        // The marker each chunk writes after reading `high` lets the server's next keepalive
        // pass `high` even on an idle server, where the insert position can sit a page header
        // above the last record.
        if let (true, Some(applied)) = (request, &self.applied_lsn) {
            applied.request_keepalive();
        }
        Ok(())
    }

    /// Emit the held chunk, in pieces of at most `max_rows` rows, once the stream has passed its
    /// `high` and every transaction ending at or before it was drained. Its rows are the table's
    /// state at `high` and no longer change: the stream delivers in commit order, so nothing
    /// ending at or before `high` arrives after it passed. The copy's progress advances only
    /// with the last piece and after every fallible step, so a barrier captured between pieces
    /// or after a failed poll records the chunk as not emitted, and a restart fetches it again.
    fn emit_held_chunk(&mut self, max_rows: usize) -> Result<Option<SourceBatch>, ConnectorError> {
        let first_end = self
            .committed
            .front()
            .map(|transaction| transaction.end_lsn);
        let last_end = self.committed.back().map(|transaction| transaction.end_lsn);
        let Phase::Snapshot(snapshot) = &self.phase else {
            return Ok(None);
        };
        let Some(held) = snapshot
            .passed(first_end, last_end)
            .filter(|_| max_rows > 0)
        else {
            return Ok(None);
        };
        let (batch, next_slot) = self.chunk_batch(held, snapshot.chunks, max_rows)?;
        let count = batch.as_ref().map_or(0, SourceBatch::num_rows);
        let last = held.rows.rows_from(next_slot).next().is_none();
        let finished = last && held.upper == Upper::End;
        if finished {
            self.build_staged_transactions()?;
        }
        let Phase::Snapshot(snapshot) = &mut self.phase else {
            return Err(ConnectorError::Internal(
                "PostgreSQL CDC left the snapshot phase while emitting a chunk".into(),
            ));
        };
        snapshot.record_piece(count, next_slot, last);
        if finished {
            tracing::info!(
                table = %self.config.table,
                rows = snapshot.copied_rows,
                chunks = snapshot.chunks,
                "PostgreSQL CDC initial snapshot complete"
            );
            self.phase = Phase::Streaming;
        }
        let Some(batch) = batch else {
            return Ok(None);
        };
        self.metrics
            .record_snapshot_rows(u64::try_from(count).unwrap_or(u64::MAX));
        self.metrics.record_batch();
        Ok(Some(batch.with_checkpoint(self.cursor())))
    }

    /// The next piece of at most `max_rows` rows of `held`, the `ordinal`-th chunk, positioned
    /// after every transaction ending at or before its `high`, and the slot after it; `None`
    /// when merged deletions emptied the chunk.
    fn chunk_batch(
        &self,
        held: &HeldChunk,
        ordinal: u64,
        max_rows: usize,
    ) -> Result<(Option<SourceBatch>, usize), ConnectorError> {
        let layout = self.layout.as_ref().ok_or_else(|| {
            ConnectorError::Internal("PostgreSQL CDC copies before its schema binding".into())
        })?;
        let (records, mutations, next_slot) =
            chunk_records(layout, &held.rows, held.next_slot, max_rows)?;
        let count = records.num_rows();
        if count == 0 {
            return Ok((None, next_slot));
        }
        let key = chunk_order_key(held.high, ordinal);
        let sub_offsets = (held.emitted..held.emitted + count)
            .map(|row| {
                u32::try_from(row).map_err(|_| {
                    ConnectorError::Internal("PostgreSQL CDC chunk row ordinal overflow".into())
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let positions = self.row_positions(
            std::iter::repeat_n(key.as_slice(), count),
            UInt32Array::from(sub_offsets),
        )?;
        let mut batch = SourceBatch::positioned(records, positions)?;
        if let Some(mutations) = mutations {
            batch = batch.with_mutations(mutations)?;
        }
        Ok((Some(batch), next_slot))
    }
}

#[cfg(test)]
mod tests;
