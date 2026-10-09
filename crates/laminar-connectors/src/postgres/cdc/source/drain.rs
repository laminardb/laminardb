//! Transaction-atomic emission of decoded rows as positioned source batches.

use arrow_array::{BinaryArray, RecordBatch, UInt32Array};

use crate::connector::{SourceBatch, SourceMutation, SourceRowPositions};
use crate::error::ConnectorError;

use super::checkpoint::{write_cursor, CursorPhase};
use super::{CommittedTransaction, ConnectorState, Lsn, PostgresCdcSource};
use crate::postgres::cdc::config::OutputMode;

/// Order-key tag of snapshot rows; all of them sort before every streamed transaction.
pub(super) const SNAPSHOT_ORDER_TAG: u8 = 0;
/// Order-key tag of streamed rows, followed by the transaction's commit end LSN.
const WAL_ORDER_TAG: u8 = 1;

impl PostgresCdcSource {
    pub(super) fn fail_on_terminal_wal_error(&mut self) -> Result<(), ConnectorError> {
        let message = self.wal_terminal_error.as_ref().and_then(|error| {
            error
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take()
        });
        match message {
            Some(message) => Err(self.fail(ConnectorError::ReadError(message))),
            None => Ok(()),
        }
    }

    /// Number of leading committed transactions forming the next batch: always at least one,
    /// then whole transactions while the row target and Arrow-build budget allow.
    fn select_transactions(&self, max_rows: usize) -> usize {
        let arrow_limit = self.config.arrow_build_bytes();
        let mut selected = 0_usize;
        let mut rows = 0_usize;
        let mut bytes = 0_usize;
        for transaction in &self.committed {
            let transaction_rows = transaction
                .records
                .as_ref()
                .map_or(0, RecordBatch::num_rows);
            let next_rows = rows.saturating_add(transaction_rows);
            let next_bytes = bytes.saturating_add(transaction.retained_bytes);
            if rows != 0
                && transaction_rows != 0
                && (next_rows > max_rows || next_bytes > arrow_limit)
            {
                break;
            }
            selected += 1;
            rows = next_rows;
            bytes = next_bytes;
        }
        selected
    }

    /// Emit committed transactions without exposing a cursor inside a transaction.
    ///
    /// `max_rows` is a batching target, not permission to split a transaction: logical
    /// replication resumes only at a commit boundary, so a transaction larger than the target is
    /// emitted whole. The decoded-stage budget bounds its size.
    pub(super) fn drain_committed(
        &mut self,
        max_rows: usize,
    ) -> Result<Option<SourceBatch>, ConnectorError> {
        if self.committed.is_empty() || max_rows == 0 {
            return Ok(None);
        }
        let count = self.select_transactions(max_rows);
        let selected: Vec<CommittedTransaction> = self.committed.drain(..count).collect();
        let released: usize = selected
            .iter()
            .map(|transaction| transaction.retained_bytes)
            .sum();
        self.committed_bytes = self.committed_bytes.saturating_sub(released);
        let Some(last) = selected.last() else {
            return Ok(None);
        };
        self.polled_lsn = last.end_lsn;
        let batch = self.positioned_batch(&selected);
        batch.inspect_err(|_| self.state = ConnectorState::Failed)
    }

    fn positioned_batch(
        &self,
        selected: &[CommittedTransaction],
    ) -> Result<Option<SourceBatch>, ConnectorError> {
        let batches: Vec<&RecordBatch> = selected
            .iter()
            .filter_map(|transaction| transaction.records.as_ref())
            .collect();
        let records = match batches.as_slice() {
            [] => return Ok(None),
            [single] => (*single).clone(),
            many => arrow_select::concat::concat_batches(&self.schema, many.iter().copied())
                .map_err(|error| {
                    ConnectorError::Internal(format!("PostgreSQL CDC batch assembly: {error}"))
                })?,
        };
        let mut order_keys = Vec::with_capacity(records.num_rows());
        let mut sub_offsets = Vec::with_capacity(records.num_rows());
        for transaction in selected {
            let rows = transaction
                .records
                .as_ref()
                .map_or(0, RecordBatch::num_rows);
            let order_key = wal_order_key(transaction.end_lsn);
            for row in 0..rows {
                order_keys.push(order_key);
                sub_offsets.push(u32::try_from(row).map_err(|_| {
                    ConnectorError::Internal("PostgreSQL CDC row ordinal overflow".into())
                })?);
            }
        }
        let positions = self.row_positions(
            order_keys.iter().map(<[u8; 9]>::as_slice),
            UInt32Array::from(sub_offsets),
        )?;
        let mut batch = SourceBatch::positioned(records, positions)?;
        if self.config.output_mode == OutputMode::Upsert {
            let mutations: Vec<SourceMutation> = selected
                .iter()
                .flat_map(|transaction| transaction.mutations.iter().copied())
                .collect();
            batch = batch.with_mutations(mutations)?;
        }
        self.metrics.record_batch();
        Ok(Some(batch.with_checkpoint(write_cursor(
            &self.config,
            self.checkpoint_binding.as_ref(),
            CursorPhase::Streaming(self.polled_lsn),
        ))))
    }

    /// Positions in one partition named by the slot, whose WAL history is one ordered stream.
    pub(super) fn row_positions<'a>(
        &self,
        order_keys: impl ExactSizeIterator<Item = &'a [u8]>,
        sub_offsets: UInt32Array,
    ) -> Result<SourceRowPositions, ConnectorError> {
        let rows = order_keys.len();
        SourceRowPositions::try_new(
            BinaryArray::from_iter_values(std::iter::repeat_n(
                self.config.slot_name.as_bytes(),
                rows,
            )),
            BinaryArray::from_iter_values(order_keys),
            sub_offsets,
        )
    }
}

fn wal_order_key(end_lsn: Lsn) -> [u8; 9] {
    let mut key = [WAL_ORDER_TAG; 9];
    key[1..].copy_from_slice(&end_lsn.as_u64().to_be_bytes());
    key
}

/// Order key of the `ordinal`-th snapshot row.
pub(super) fn snapshot_order_key(ordinal: u64) -> [u8; 9] {
    let mut key = [SNAPSHOT_ORDER_TAG; 9];
    key[1..].copy_from_slice(&ordinal.to_be_bytes());
    key
}
