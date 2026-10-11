//! Streamed transactions during the copy. Their rows wait as texts until the drain knows where
//! each key lies: copied keys are emitted, changes the held chunk's snapshot misses are merged
//! into it, and the rest are dropped for a later chunk to copy; upsert tombstones are always
//! emitted.

use arrow_array::UInt32Array;

use crate::connector::{SourceBatch, SourceMutation};
use crate::error::ConnectorError;

use super::super::super::typed_rows::{RowBuilder, RowLayout};
use super::super::drain::wal_order_key;
use super::super::{CommittedTransaction, Phase, PostgresCdcSource, StagedRow};
use super::merge::{encode_key, row_action, KeyClass, RowAction};

/// Append a staged row as emitted, returning its upsert mutation.
pub(crate) fn append_staged(
    rows: &mut RowBuilder,
    layout: &RowLayout,
    row: &StagedRow,
) -> Result<Option<SourceMutation>, ConnectorError> {
    let (key_only, weight, mutation) = row.image.parts();
    rows.append(
        layout,
        |index, _| Ok(row.values[index].as_deref()),
        key_only,
        weight,
        row.bytes,
    )?;
    Ok(mutation)
}

/// Rows emitted by one drain, with their positions.
#[derive(Default)]
struct Emitted {
    order_keys: Vec<[u8; 9]>,
    sub_offsets: Vec<u32>,
    mutations: Vec<SourceMutation>,
}

impl PostgresCdcSource {
    /// Drain whole committed transactions during the copy, stopping before any that ends after
    /// the held chunk's `high`.
    pub(in super::super) async fn drain_snapshot(
        &mut self,
        max_rows: usize,
    ) -> Result<Option<SourceBatch>, ConnectorError> {
        let count = self.leading_transactions(max_rows);
        let reconnecting =
            matches!(&self.phase, Phase::Snapshot(snapshot) if snapshot.reconnect_pending());
        if count == 0 || reconnecting {
            return Ok(None);
        }
        let ranges = match self.classify_leading(count).await {
            Ok(ranges) => ranges,
            Err(ConnectorError::ConnectionFailed(reason)) => {
                if let Phase::Snapshot(snapshot) = &mut self.phase {
                    snapshot.wait_after(&reason);
                }
                return Ok(None);
            }
            Err(error) => return Err(self.fail(error)),
        };
        let selected: Vec<CommittedTransaction> = self.committed.drain(..count).collect();
        let released: usize = selected
            .iter()
            .map(|transaction| transaction.retained_bytes)
            .sum();
        self.committed_bytes = self.committed_bytes.saturating_sub(released);
        self.apply_drained(&selected, &ranges)
            .map_err(|error| self.fail(error))
    }

    /// How many leading committed transactions the next drain takes: a batch as outside the
    /// copy, stopping before the first that ends after a held chunk's `high`.
    pub(super) fn leading_transactions(&self, max_rows: usize) -> usize {
        if max_rows == 0 {
            return 0;
        }
        let count = self.select_transactions(max_rows);
        let high = match &self.phase {
            Phase::Snapshot(snapshot) => snapshot.held_high(),
            _ => None,
        };
        high.map_or(count, |high| {
            self.committed
                .iter()
                .take(count)
                .take_while(|transaction| transaction.end_lsn <= high)
                .count()
        })
    }

    /// Ask the server where the keys of the leading `count` transactions lie.
    async fn classify_leading(
        &mut self,
        count: usize,
    ) -> Result<Vec<(bool, bool)>, ConnectorError> {
        let Phase::Snapshot(snapshot) = &mut self.phase else {
            return Err(ConnectorError::Internal(
                "PostgreSQL CDC drains a copy outside the snapshot phase".into(),
            ));
        };
        snapshot
            .refresh_reference(&self.config, &self.task_owner)
            .await?;
        let mut keys = Vec::new();
        for transaction in self.committed.iter().take(count) {
            let staged = transaction.staged.as_deref().ok_or_else(|| {
                ConnectorError::Internal(
                    "PostgreSQL CDC decoded a transaction outside the copy during it".into(),
                )
            })?;
            for row in staged {
                let key = snapshot
                    .plan
                    .keys
                    .iter()
                    .map(|column| {
                        row.values
                            .get(column.declared)
                            .and_then(Option::as_deref)
                            .and_then(|bytes| std::str::from_utf8(bytes).ok())
                            .ok_or_else(|| {
                                ConnectorError::ReadError(
                                    "PostgreSQL CDC primary-key text is missing or not UTF-8"
                                        .into(),
                                )
                            })
                    })
                    .collect::<Result<Vec<&str>, _>>()?;
                keys.push(key);
            }
        }
        snapshot
            .classify(&self.config, &self.task_owner, &keys)
            .await
    }

    /// Emit, merge, or drop each row of `selected` by its key range, and record the
    /// transactions a later chunk must see.
    pub(super) fn apply_drained(
        &mut self,
        selected: &[CommittedTransaction],
        ranges: &[(bool, bool)],
    ) -> Result<Option<SourceBatch>, ConnectorError> {
        let (Phase::Snapshot(snapshot), Some(layout)) = (&mut self.phase, self.layout.as_ref())
        else {
            return Err(ConnectorError::Internal(
                "PostgreSQL CDC copy state is incomplete".into(),
            ));
        };
        let reference = snapshot
            .reference
            .as_ref()
            .map(|(reference, _)| reference.clone())
            .ok_or_else(|| {
                ConnectorError::Internal("PostgreSQL CDC has no xid reference".into())
            })?;
        let key_columns: Vec<usize> = snapshot.plan.keys.iter().map(|key| key.declared).collect();
        let budget = self.config.arrow_build_bytes();
        let weighted = layout.weighted;
        let mut rows = RowBuilder::new(layout);
        let mut emitted = Emitted::default();
        let mut ranges = ranges.iter();
        for transaction in selected {
            let xid = reference.widen(transaction.xid)?;
            let mut pending = snapshot.held.is_some();
            for (index, row) in transaction.staged.iter().flatten().enumerate() {
                let &(copied, in_held) = ranges.next().ok_or_else(|| {
                    ConnectorError::Internal("PostgreSQL CDC key classification is short".into())
                })?;
                let class = match (copied, snapshot.held.as_ref()) {
                    (true, _) => KeyClass::Copied,
                    (false, Some(held)) if in_held => KeyClass::Held {
                        visible: held.snapshot.sees(xid),
                    },
                    (false, _) => KeyClass::Uncopied,
                };
                pending |= class == KeyClass::Uncopied;
                let action = row_action(class, row.image);
                if matches!(action, RowAction::Merge | RowAction::MergeAndEmit) {
                    if let Some(held) = snapshot.held.as_mut() {
                        let key = encode_key(&row.values, &key_columns)?;
                        held.rows
                            .apply(row.image, key, row.values.clone(), row.bytes)?;
                    }
                }
                if matches!(action, RowAction::Emit | RowAction::MergeAndEmit) {
                    emitted
                        .mutations
                        .extend(append_staged(&mut rows, layout, row)?);
                    emitted.order_keys.push(wal_order_key(transaction.end_lsn));
                    emitted.sub_offsets.push(u32::try_from(index).map_err(|_| {
                        ConnectorError::Internal("PostgreSQL CDC row ordinal overflow".into())
                    })?);
                }
            }
            if pending {
                snapshot.pending.insert(xid);
            }
        }
        if snapshot
            .held
            .as_ref()
            .is_some_and(|held| held.rows.bytes() > budget)
        {
            tracing::debug!(
                budget,
                "PostgreSQL CDC abandons a chunk that merges outgrew"
            );
            snapshot.abandon_held();
        }
        let records = (rows.len() > 0).then(|| rows.finish(layout)).transpose()?;
        let batch = match records {
            Some(records) => {
                let positions = self.row_positions(
                    emitted.order_keys.iter().map(<[u8; 9]>::as_slice),
                    UInt32Array::from(emitted.sub_offsets),
                )?;
                let batch = SourceBatch::positioned(records, positions)?;
                Some(if weighted {
                    batch
                } else {
                    batch.with_mutations(emitted.mutations)?
                })
            }
            None => None,
        };
        if let Some(last) = selected.last() {
            self.polled_lsn = last.end_lsn;
        }
        let Some(batch) = batch else {
            return Ok(None);
        };
        self.metrics.record_batch();
        Ok(Some(batch.with_checkpoint(self.cursor())))
    }

    /// Turn every staged transaction into built rows once the copy is complete: from then on
    /// every key is copied, so every row is emitted.
    pub(in super::super) fn build_staged_transactions(&mut self) -> Result<(), ConnectorError> {
        let layout = self.layout.as_ref().ok_or_else(|| {
            ConnectorError::Internal("PostgreSQL CDC copies before its schema binding".into())
        })?;
        for transaction in &mut self.committed {
            let Some(staged) = transaction.staged.take() else {
                continue;
            };
            let mut rows = RowBuilder::new(layout);
            let mut mutations = Vec::new();
            for row in &staged {
                mutations.extend(append_staged(&mut rows, layout, row)?);
            }
            transaction.records = (rows.len() > 0).then(|| rows.finish(layout)).transpose()?;
            transaction.mutations = mutations.into();
        }
        let open = self
            .open_transaction
            .as_mut()
            .and_then(|transaction| transaction.staged.take());
        if let (Some(open), Some(rows)) = (open, self.open_rows.as_mut()) {
            for row in &open.rows {
                self.open_mutations
                    .extend(append_staged(rows, layout, row)?);
            }
        }
        Ok(())
    }
}
