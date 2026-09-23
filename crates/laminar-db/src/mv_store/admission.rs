//! Preflight live MV quotas without cloning complete retained state.

use arrow::array::RecordBatch;
use laminar_core::streaming::retained_arrow_bytes;

use super::multiset::MultisetDelta;
use super::upsert::UpsertDelta;
use super::{MvEntry, MvStorageMode};
use crate::DbError;

#[derive(Clone, Copy)]
pub(super) struct MvLimits {
    pub rows: usize,
    pub bytes: usize,
}

impl MvLimits {
    pub fn validate(self, name: &str, rows: usize, bytes: usize) -> Result<(), DbError> {
        if bytes == usize::MAX {
            return Err(size_overflow());
        }
        if rows > self.rows || bytes > self.bytes {
            return Err(DbError::MaterializedViewQuotaExceeded {
                view: name.to_owned(),
                rows,
                bytes,
                max_rows: self.rows,
                max_bytes: self.bytes,
            });
        }
        Ok(())
    }
}

pub(super) enum PreparedUpdate<'a> {
    Unchanged,
    Batches {
        evicted: usize,
        incoming: &'a [RecordBatch],
        rows: usize,
        bytes: usize,
    },
    Upsert(UpsertDelta),
    Multiset(MultisetDelta),
}

pub(super) fn size_overflow() -> DbError {
    DbError::Storage("materialized-view retained-memory accounting overflow".into())
}

impl MvEntry {
    fn validate_input_schema(&self, name: &str, batch: &RecordBatch) -> Result<(), DbError> {
        let weighted = matches!(
            self.mode,
            MvStorageMode::Upsert { .. } | MvStorageMode::Multiset
        );
        let schema = batch.schema_ref();
        if !weighted && std::sync::Arc::ptr_eq(&self.schema, schema) {
            return Ok(());
        }
        let mut expected = self.schema.fields().iter();
        let mut weights = 0;
        for (field, column) in schema.fields().iter().zip(batch.columns()) {
            if weighted && field.name() == super::WEIGHT_COLUMN {
                weights += 1;
                if field.data_type() != &arrow::datatypes::DataType::Int64 {
                    return Err(DbError::MaterializedView(format!(
                        "MV '{name}' weight must be Int64"
                    )));
                }
                continue;
            }
            let valid = expected.next().is_some_and(|expected| {
                expected.name() == field.name()
                    && expected.data_type() == field.data_type()
                    && (expected.is_nullable() || column.null_count() == 0)
            });
            if !valid {
                return Err(DbError::MaterializedView(format!(
                    "MV '{name}' input schema does not match its declared schema"
                )));
            }
        }
        if weighted && weights == 0 {
            return Err(DbError::MaterializedView(format!(
                "MV '{name}' changelog is missing weight"
            )));
        }
        if expected.next().is_some() || weights != usize::from(weighted) {
            return Err(DbError::MaterializedView(format!(
                "MV '{name}' input schema does not match its declared schema"
            )));
        }
        Ok(())
    }

    pub(super) fn prepare_cycle<'a>(
        &self,
        name: &str,
        batches: &'a [RecordBatch],
        limits: MvLimits,
        snapshot: bool,
    ) -> Result<PreparedUpdate<'a>, DbError> {
        for batch in batches {
            self.validate_input_schema(name, batch)?;
        }
        if batches.iter().all(|batch| batch.num_rows() == 0) {
            return Ok(PreparedUpdate::Unchanged);
        }
        match &self.mode {
            MvStorageMode::Aggregate => {
                let (rows, bytes) = batch_usage(batches.iter())?;
                limits.validate(name, rows, bytes)?;
                Ok(PreparedUpdate::Batches {
                    evicted: self.batches.len(),
                    incoming: batches,
                    rows,
                    bytes,
                })
            }
            MvStorageMode::Append { max_batches } => {
                self.prepare_append(name, batches, limits, *max_batches)
            }
            MvStorageMode::Upsert { .. } => self
                .upsert
                .as_ref()
                .ok_or_else(|| DbError::Storage("MV is missing its upsert state".into()))?
                .prepare_cycle(name, batches, limits)
                .map(PreparedUpdate::Upsert),
            MvStorageMode::Multiset => self
                .multiset
                .as_ref()
                .ok_or_else(|| DbError::Storage("MV is missing its multiset state".into()))?
                .prepare_cycle(name, batches, limits, snapshot)
                .map(PreparedUpdate::Multiset),
        }
    }

    fn prepare_append<'a>(
        &self,
        name: &str,
        batches: &'a [RecordBatch],
        limits: MvLimits,
        max_batches: usize,
    ) -> Result<PreparedUpdate<'a>, DbError> {
        let (mut rows, mut bytes) = (self.rows, self.approx_bytes);
        let (mut evicted, mut first_new, mut count) = (0, 0, self.batches.len());
        for batch in batches.iter().filter(|batch| batch.num_rows() > 0) {
            let charge = retained_arrow_bytes(batch);
            // A single oversized batch must not bypass retention by being the last batch.
            limits.validate(name, batch.num_rows(), charge)?;
            rows = rows
                .checked_add(batch.num_rows())
                .ok_or_else(size_overflow)?;
            bytes = bytes.checked_add(charge).ok_or_else(size_overflow)?;
            count = count.checked_add(1).ok_or_else(size_overflow)?;
            while count > max_batches.max(1) || rows > limits.rows || bytes > limits.bytes {
                // INVARIANT: each incoming batch fits alone; evictions stop before it.
                // Both cursors advance through bounded retained/input batch sequences.
                let old = if let Some(old) = self.batches.get(evicted) {
                    evicted += 1;
                    old
                } else {
                    while batches[first_new].num_rows() == 0 {
                        first_new += 1;
                    }
                    let old = &batches[first_new];
                    first_new += 1;
                    old
                };
                rows -= old.num_rows();
                bytes -= retained_arrow_bytes(old);
                count -= 1;
            }
        }
        Ok(PreparedUpdate::Batches {
            evicted,
            incoming: &batches[first_new..],
            rows,
            bytes,
        })
    }

    pub(super) fn apply_prepared(&mut self, update: PreparedUpdate<'_>) {
        match update {
            PreparedUpdate::Unchanged => {}
            PreparedUpdate::Batches {
                evicted,
                incoming,
                rows,
                bytes,
            } => {
                self.batches.drain(..evicted);
                self.batches.extend(
                    incoming
                        .iter()
                        .filter(|batch| batch.num_rows() > 0)
                        .cloned(),
                );
                self.rows = rows;
                self.approx_bytes = bytes;
            }
            PreparedUpdate::Upsert(delta) => {
                if let Some(state) = self.upsert.as_mut() {
                    state.apply_prepared(delta);
                    self.approx_bytes = state.approx_bytes;
                }
            }
            PreparedUpdate::Multiset(delta) => {
                if let Some(state) = self.multiset.as_mut() {
                    state.apply_prepared(delta);
                    self.approx_bytes = state.approx_bytes;
                }
            }
        }
    }
}

pub(super) fn batch_usage<'a>(
    batches: impl Iterator<Item = &'a RecordBatch>,
) -> Result<(usize, usize), DbError> {
    batches.filter(|batch| batch.num_rows() > 0).try_fold(
        (0usize, 0usize),
        |(rows, bytes), batch| {
            Ok((
                rows.checked_add(batch.num_rows())
                    .ok_or_else(size_overflow)?,
                bytes
                    .checked_add(retained_arrow_bytes(batch))
                    .ok_or_else(size_overflow)?,
            ))
        },
    )
}
