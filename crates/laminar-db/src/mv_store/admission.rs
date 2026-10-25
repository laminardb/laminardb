//! Preflight live MV quotas without cloning complete retained state.

use arrow::array::RecordBatch;

use super::append_log::LogUpdate;
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
    Log(LogUpdate<'a>),
    Upsert(UpsertDelta),
    Multiset(MultisetDelta),
}

pub(super) fn size_overflow() -> DbError {
    DbError::Storage("materialized-view retained-memory accounting overflow".into())
}

impl MvEntry {
    fn validate_input_schema(&self, name: &str, batch: &RecordBatch) -> Result<(), DbError> {
        let weighted = matches!(self.mode, MvStorageMode::Changelog { .. });
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
        if let Some(log) = &self.log {
            return log.prepare(name, batches, limits).map(PreparedUpdate::Log);
        }
        match (&self.upsert, &self.multiset) {
            (Some(upsert), None) => upsert
                .prepare_cycle(name, batches, limits)
                .map(PreparedUpdate::Upsert),
            (None, Some(multiset)) => multiset
                .prepare_cycle(name, batches, limits, snapshot)
                .map(PreparedUpdate::Multiset),
            _ => Err(DbError::Storage(format!(
                "MV '{name}' is missing its changelog state"
            ))),
        }
    }

    pub(super) fn apply_prepared(&mut self, update: PreparedUpdate<'_>) {
        match update {
            PreparedUpdate::Unchanged => {}
            PreparedUpdate::Log(update) => {
                if let Some(log) = self.log.as_mut() {
                    log.apply(update);
                    self.approx_bytes = log.bytes;
                }
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
