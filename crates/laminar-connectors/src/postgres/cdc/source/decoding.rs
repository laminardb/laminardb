//! WAL decoding into typed rows with transaction-aware memory admission.
//!
//! Decoded rows of the open transaction and of committed-but-undrained transactions share the
//! decoded-stage budget. A new transaction is not started while that work sits above the high
//! watermark, and a row that would overflow the budget is deferred until committed work drains.
//! Only a single open transaction that cannot fit on its own is an error.

use crate::connector::SourceMutation;
use crate::error::ConnectorError;

use super::super::decoder::{
    decode_message, pg_timestamp_to_unix_ms, ColumnValue, OldTuple, TupleData, WalMessage,
};
use super::super::schema::RelationInfo;
use super::super::schema_resolution::validate_relation;
use super::super::typed_rows::{RowBuilder, RowLayout};
use super::reader::logical_wal_payload_bytes;
use super::{
    CommittedTransaction, ConnectorState, Lsn, OpenTransaction, OwnedWalPayload, PostgresCdcSource,
    WalPayload,
};
use crate::postgres::cdc::config::OutputMode;

/// What happened to one raw payload offered to the decoder.
pub(super) enum Decoded {
    Applied,
    /// The payload must wait until committed transactions drain; it is returned unchanged.
    Deferred(OwnedWalPayload),
}

/// One emitted row: which image it reads and how it is applied.
#[derive(Clone, Copy)]
enum RowImage {
    Put,
    Tombstone,
    Weighted(i64),
}

impl PostgresCdcSource {
    /// Decoded bytes a drain can release.
    pub(super) fn drainable_bytes(&self) -> usize {
        self.committed_bytes.saturating_add(
            self.open_rows
                .as_ref()
                .map_or(0, RowBuilder::retained_bytes),
        )
    }

    /// Decoded-stage capacity left for transaction data after the bound relation metadata.
    fn event_byte_limit(&self) -> usize {
        let metadata = self
            .relation
            .as_ref()
            .and_then(|relation| relation.retained_bytes().ok())
            .unwrap_or(0);
        self.config.decoded_event_bytes().saturating_sub(metadata)
    }

    pub(super) fn event_high_watermark(&self) -> usize {
        let limit = self.event_byte_limit();
        limit.saturating_sub(limit / 5)
    }

    /// Decode one payload, or hand it back when committed work must drain first.
    ///
    /// # Errors
    /// Returns an error for malformed protocol data, contract violations, or a single open
    /// transaction that exceeds the decoded-stage budget on its own.
    pub(super) fn process_owned_wal_payload(
        &mut self,
        payload: OwnedWalPayload,
    ) -> Result<Decoded, ConnectorError> {
        let message = match &payload.payload {
            WalPayload::Begin { .. } => {
                if !self.committed.is_empty()
                    && self.drainable_bytes() >= self.event_high_watermark()
                {
                    return Ok(Decoded::Deferred(payload));
                }
                None
            }
            WalPayload::XLogData { data, .. } => {
                let message = decode_message(data.clone())
                    .map_err(|e| ConnectorError::ReadError(format!("pgoutput decode: {e}")))?;
                let planned = self.planned_change_bytes(&message)?;
                if self.drainable_bytes().saturating_add(planned) > self.event_byte_limit() {
                    if !self.committed.is_empty() {
                        return Ok(Decoded::Deferred(payload));
                    }
                    return Err(self.oversized_transaction(planned));
                }
                Some(message)
            }
            WalPayload::Commit { .. } | WalPayload::KeepAlive { .. } => None,
        };
        let received = u64::try_from(logical_wal_payload_bytes(&payload.payload))
            .map_err(|_| ConnectorError::Internal("PostgreSQL CDC byte metric overflow".into()))?;
        self.metrics.record_bytes(received);
        match (payload.payload, message) {
            (WalPayload::XLogData { wal_end, .. }, Some(message)) => {
                self.apply_message(message)?;
                self.write_lsn = self.write_lsn.max(Lsn::new(wal_end));
            }
            (
                WalPayload::Begin {
                    final_lsn,
                    commit_ts_us,
                    ..
                },
                None,
            ) => {
                Self::validate_timestamp(commit_ts_us, "BEGIN")?;
                self.begin_transaction(Lsn::new(final_lsn))?;
            }
            (
                WalPayload::Commit {
                    end_lsn,
                    commit_ts_us,
                    lsn,
                },
                None,
            ) => {
                Self::validate_timestamp(commit_ts_us, "COMMIT")?;
                self.commit_transaction(Lsn::new(lsn), Lsn::new(end_lsn))?;
            }
            (WalPayload::KeepAlive { wal_end }, None) => {
                self.write_lsn = self.write_lsn.max(Lsn::new(wal_end));
            }
            _ => {
                return Err(ConnectorError::Internal(
                    "PostgreSQL CDC payload decoding lost its message".into(),
                ));
            }
        }
        Ok(Decoded::Applied)
    }

    fn validate_timestamp(commit_ts_us: i64, boundary: &str) -> Result<(), ConnectorError> {
        pg_timestamp_to_unix_ms(commit_ts_us)
            .map(|_| ())
            .map_err(|error| {
                ConnectorError::ReadError(format!("pgoutput {boundary} timestamp decode: {error}"))
            })
    }

    fn oversized_transaction(&mut self, planned: usize) -> ConnectorError {
        let final_lsn = self
            .open_transaction
            .as_ref()
            .map_or(Lsn::ZERO, |transaction| transaction.final_lsn);
        let rows = self.open_rows.as_ref().map_or(0, RowBuilder::len);
        self.fail(ConnectorError::ReadError(format!(
            "PostgreSQL CDC transaction committing at {final_lsn} exceeds the decoded-stage \
             budget on its own: {} bytes retained after {rows} rows plus {planned} for the next \
             row exceed {} bytes; raise max.buffered.bytes (one third is the decoded stage) \
             above the largest captured transaction",
            self.drainable_bytes(),
            self.event_byte_limit()
        )))
    }

    /// Planned retained bytes of the rows a decoded change will emit; zero for other messages.
    fn planned_change_bytes(&self, message: &WalMessage) -> Result<usize, ConnectorError> {
        let Some(layout) = self.layout.as_ref() else {
            return Ok(0);
        };
        let (rows, text_bytes) = match message {
            WalMessage::Insert(insert) => (1, declared_text_bytes(layout, &insert.new_tuple, None)),
            WalMessage::Update(update) => {
                let old = update.old_tuple.as_ref().and_then(full_old_tuple);
                (
                    2,
                    declared_text_bytes(layout, &update.new_tuple, old).saturating_add(
                        old.map_or(0, |old| declared_text_bytes(layout, old, None)),
                    ),
                )
            }
            WalMessage::Delete(delete) => (
                1,
                full_old_tuple(&delete.old_tuple)
                    .map_or(0, |old| declared_text_bytes(layout, old, None)),
            ),
            _ => return Ok(0),
        };
        layout
            .planned_row_bytes(text_bytes)?
            .checked_mul(rows)
            .ok_or_else(|| ConnectorError::ReadError("PostgreSQL CDC row size overflow".into()))
    }

    /// Apply one decoded message without admission checks.
    pub(super) fn apply_message(&mut self, message: WalMessage) -> Result<(), ConnectorError> {
        match message {
            WalMessage::Begin(begin) => self.begin_transaction(begin.final_lsn),
            WalMessage::Commit(commit) => {
                self.commit_transaction(commit.commit_lsn, commit.end_lsn)
            }
            WalMessage::Relation(relation) => self.announce_relation(&RelationInfo {
                relation_id: relation.relation_id,
                namespace: relation.namespace,
                name: relation.name,
                replica_identity: char::from(relation.replica_identity),
                columns: relation.columns,
            }),
            WalMessage::Insert(insert) => {
                self.require_bound_relation(insert.relation_id)?;
                let image = self.put_or_weighted(1);
                self.emit_row(&insert.new_tuple, None, image)?;
                self.metrics.record_insert();
                Ok(())
            }
            WalMessage::Update(update) => {
                self.require_bound_relation(update.relation_id)?;
                let old = update
                    .old_tuple
                    .as_ref()
                    .and_then(full_old_tuple)
                    .ok_or_else(|| self.missing_old_image("UPDATE"))?;
                self.emit_update(old, &update.new_tuple)?;
                self.metrics.record_update();
                Ok(())
            }
            WalMessage::Delete(delete) => {
                self.require_bound_relation(delete.relation_id)?;
                let old = full_old_tuple(&delete.old_tuple)
                    .ok_or_else(|| self.missing_old_image("DELETE"))?;
                let image = match self.config.output_mode {
                    OutputMode::Upsert => RowImage::Tombstone,
                    OutputMode::Changelog => RowImage::Weighted(-1),
                };
                self.emit_row(old, None, image)?;
                self.metrics.record_delete();
                Ok(())
            }
            WalMessage::Truncate(_) => Err(self.fail(ConnectorError::ReadError(format!(
                "TRUNCATE of {} cannot be represented as row changes and would leave the target \
                 diverged; drop slot '{}', clear downstream targets, and start the source again \
                 for a fresh snapshot",
                self.config.table, self.config.slot_name
            )))),
            WalMessage::Origin(_) | WalMessage::Type(_) => Ok(()),
        }
    }

    fn put_or_weighted(&self, weight: i64) -> RowImage {
        match self.config.output_mode {
            OutputMode::Upsert => RowImage::Put,
            OutputMode::Changelog => RowImage::Weighted(weight),
        }
    }

    fn missing_old_image(&mut self, operation: &str) -> ConnectorError {
        self.fail(ConnectorError::ReadError(format!(
            "PostgreSQL CDC {operation} on {} carried no complete old row; the table must keep \
             REPLICA IDENTITY FULL",
            self.config.table
        )))
    }

    fn emit_update(&mut self, old: &TupleData, new: &TupleData) -> Result<(), ConnectorError> {
        match self.config.output_mode {
            OutputMode::Changelog => {
                self.emit_row(old, None, RowImage::Weighted(-1))?;
                self.emit_row(new, Some(old), RowImage::Weighted(1))
            }
            OutputMode::Upsert => {
                if self.key_changed(old, new)? {
                    self.emit_row(old, None, RowImage::Tombstone)?;
                }
                self.emit_row(new, Some(old), RowImage::Put)
            }
        }
    }

    fn key_changed(&self, old: &TupleData, new: &TupleData) -> Result<bool, ConnectorError> {
        let layout = self.bound_layout()?;
        for column in layout.columns.iter().filter(|column| column.is_key) {
            if resolve_value(new, Some(old), column.tuple_index)?
                != resolve_value(old, None, column.tuple_index)?
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn emit_row(
        &mut self,
        tuple: &TupleData,
        old: Option<&TupleData>,
        image: RowImage,
    ) -> Result<(), ConnectorError> {
        if self.open_transaction.is_none() {
            return Err(self.fail(ConnectorError::ReadError(
                "PostgreSQL CDC received a row change outside a transaction".into(),
            )));
        }
        let Some(layout) = self.layout.as_ref() else {
            return Err(ConnectorError::Internal(
                "PostgreSQL CDC row decoded before its schema binding".into(),
            ));
        };
        if tuple.columns.len() != layout.tuple_width {
            self.state = ConnectorState::Failed;
            return Err(ConnectorError::ReadError(format!(
                "PostgreSQL CDC tuple has {} columns, but the bound relation has {}",
                tuple.columns.len(),
                layout.tuple_width
            )));
        }
        let planned = layout.planned_row_bytes(declared_text_bytes(layout, tuple, old))?;
        let (key_only, weight, mutation) = match image {
            RowImage::Put => (false, None, Some(SourceMutation::Put)),
            RowImage::Tombstone => (true, None, Some(SourceMutation::Tombstone)),
            RowImage::Weighted(weight) => (false, Some(weight), None),
        };
        let rows = self.open_rows.as_mut().ok_or_else(|| {
            ConnectorError::Internal("PostgreSQL CDC row builder is missing".into())
        })?;
        if u32::try_from(rows.len()).is_err() {
            self.state = ConnectorState::Failed;
            return Err(ConnectorError::ReadError(
                "PostgreSQL CDC transaction exceeds u32::MAX emitted rows".into(),
            ));
        }
        let appended = rows.append(
            layout,
            |_, column| resolve_value(tuple, old, column.tuple_index),
            key_only,
            weight,
            planned,
        );
        if let Err(error) = appended {
            self.state = ConnectorState::Failed;
            return Err(error);
        }
        if let Some(mutation) = mutation {
            self.open_mutations.push(mutation);
        }
        Ok(())
    }

    fn bound_layout(&self) -> Result<&RowLayout, ConnectorError> {
        self.layout.as_ref().ok_or_else(|| {
            ConnectorError::Internal("PostgreSQL CDC row decoded before its schema binding".into())
        })
    }

    fn require_bound_relation(&mut self, relation_id: u32) -> Result<(), ConnectorError> {
        let bound = self.relation.as_ref().map(|relation| relation.relation_id);
        if bound == Some(relation_id) && self.relation_announced {
            return Ok(());
        }
        Err(self.fail(ConnectorError::ReadError(format!(
            "pgoutput sent a change for relation {relation_id}, but only an announced {} \
             (oid {}) is bound",
            self.config.table,
            bound.unwrap_or_default()
        ))))
    }

    fn announce_relation(&mut self, incoming: &RelationInfo) -> Result<(), ConnectorError> {
        let Some(bound) = self.relation.as_ref() else {
            return Err(ConnectorError::Internal(
                "PostgreSQL CDC relation announced before its binding".into(),
            ));
        };
        if let Err(error) = validate_relation(bound, incoming) {
            return Err(self.fail(error));
        }
        self.relation_announced = true;
        Ok(())
    }

    fn begin_transaction(&mut self, final_lsn: Lsn) -> Result<(), ConnectorError> {
        if self.open_transaction.is_some() {
            return Err(self.fail(ConnectorError::ReadError(
                "PostgreSQL CDC received BEGIN before the current transaction committed".into(),
            )));
        }
        self.open_transaction = Some(OpenTransaction { final_lsn });
        Ok(())
    }

    fn commit_transaction(&mut self, commit_lsn: Lsn, end_lsn: Lsn) -> Result<(), ConnectorError> {
        let Some(transaction) = self.open_transaction.as_ref() else {
            return Err(self.fail(ConnectorError::ReadError(
                "PostgreSQL CDC received COMMIT without an open transaction".into(),
            )));
        };
        let last_resumable = self.committed.back().map_or(self.polled_lsn, |committed| {
            committed.end_lsn.max(self.polled_lsn)
        });
        let boundary_error = if commit_lsn != transaction.final_lsn {
            Some(format!(
                "COMMIT LSN {commit_lsn} does not match BEGIN final LSN {}",
                transaction.final_lsn
            ))
        } else if end_lsn < commit_lsn {
            Some(format!(
                "COMMIT end LSN {end_lsn} is before commit LSN {commit_lsn}"
            ))
        } else if end_lsn < last_resumable {
            Some(format!(
                "COMMIT end LSN {end_lsn} is behind the last emitted or queued LSN {last_resumable}"
            ))
        } else {
            None
        };
        if let Some(reason) = boundary_error {
            return Err(self.fail(ConnectorError::ReadError(format!(
                "PostgreSQL CDC {reason}"
            ))));
        }
        self.open_transaction = None;
        let layout = self.layout.as_ref();
        let records = match (self.open_rows.as_mut(), layout) {
            (Some(rows), Some(layout)) if rows.len() > 0 => Some(rows.finish(layout)?),
            _ => None,
        };
        let mutations: Box<[SourceMutation]> = std::mem::take(&mut self.open_mutations).into();
        let retained_bytes = records
            .as_ref()
            .map_or(0, arrow_array::RecordBatch::get_array_memory_size)
            .saturating_add(mutations.len());
        self.committed_bytes = self.committed_bytes.saturating_add(retained_bytes);
        self.committed.push_back(CommittedTransaction {
            end_lsn,
            records,
            mutations,
            retained_bytes,
        });
        self.write_lsn = self.write_lsn.max(end_lsn);
        self.metrics.record_transaction();
        self.metrics
            .set_replication_lag_bytes(self.replication_lag_bytes());
        Ok(())
    }

    /// Raw messages queued by deterministic tests, applied without admission checks.
    #[cfg(test)]
    pub(super) fn enqueue_wal_data(&mut self, data: Vec<u8>) {
        self.pending_messages.push_back(data);
    }

    #[cfg(test)]
    pub(super) fn process_pending_messages(&mut self) -> Result<(), ConnectorError> {
        while let Some(data) = self.pending_messages.pop_front() {
            let result = decode_message(bytes::Bytes::from(data))
                .map_err(|error| ConnectorError::ReadError(format!("pgoutput decode: {error}")))
                .and_then(|message| self.apply_message(message));
            if let Err(error) = result {
                self.state = ConnectorState::Failed;
                return Err(error);
            }
        }
        Ok(())
    }
}

fn full_old_tuple(old: &OldTuple) -> Option<&TupleData> {
    match old {
        OldTuple::Full(tuple) => Some(tuple),
        OldTuple::Key(_) => None,
    }
}

/// The text value of one column. An unchanged TOAST value reads the complete old row; it is
/// never mistaken for SQL `NULL`.
fn resolve_value<'a>(
    tuple: &'a TupleData,
    old: Option<&'a TupleData>,
    index: usize,
) -> Result<Option<&'a [u8]>, ConnectorError> {
    match tuple.columns.get(index) {
        Some(ColumnValue::Text(bytes)) => Ok(Some(bytes)),
        Some(ColumnValue::Null) => Ok(None),
        Some(ColumnValue::Unchanged) => match old.and_then(|old| old.columns.get(index)) {
            Some(ColumnValue::Text(bytes)) => Ok(Some(bytes)),
            Some(ColumnValue::Null) => Ok(None),
            Some(ColumnValue::Unchanged) | None => Err(ConnectorError::ReadError(format!(
                "PostgreSQL CDC column {index} is an unchanged TOAST value with no complete old \
                 row to restore it from"
            ))),
        },
        None => Err(ConnectorError::ReadError(format!(
            "PostgreSQL CDC tuple is missing column {index}"
        ))),
    }
}

fn declared_text_bytes(layout: &RowLayout, tuple: &TupleData, old: Option<&TupleData>) -> usize {
    layout
        .columns
        .iter()
        .map(|column| {
            resolve_value(tuple, old, column.tuple_index)
                .ok()
                .flatten()
                .map_or(0, <[u8]>::len)
        })
        .fold(0, usize::saturating_add)
}
