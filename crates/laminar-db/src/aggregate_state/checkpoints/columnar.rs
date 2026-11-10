//! Columnar checkpoint encoding under a cumulative working-byte limit.
//!
//! Aggregate and window frames store group keys and accumulator states as one Arrow IPC batch
//! per column set instead of one batch per group.

use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion_common::ScalarValue;

use crate::error::DbError;

/// Working bytes left for one frame's encode: retained output plus each step's transient scratch.
pub(crate) struct EncodeBudget {
    remaining: usize,
    owner: &'static str,
}

impl EncodeBudget {
    pub(crate) const fn new(max_working_bytes: usize, owner: &'static str) -> Self {
        Self {
            remaining: max_working_bytes,
            owner,
        }
    }

    pub(crate) fn retain_roster<T>(&mut self, len: usize, component: &str) -> Result<(), DbError> {
        let bytes = self.product(len, std::mem::size_of::<T>(), component)?;
        self.retain(bytes, component)
    }

    pub(crate) fn retain_encoded(
        &mut self,
        encoded: Vec<u8>,
        component: &str,
    ) -> Result<Vec<u8>, DbError> {
        self.retain(encoded.capacity(), component)?;
        Ok(encoded)
    }

    /// The output limit left once `scratch` transient bytes are set aside.
    pub(crate) fn transient_limit(
        &self,
        scratch: usize,
        component: &str,
    ) -> Result<usize, DbError> {
        self.remaining.checked_sub(scratch).ok_or_else(|| {
            DbError::Checkpoint(format!(
                "{} {component} scratch space exceeded its cumulative checkpoint byte limit",
                self.owner
            ))
        })
    }

    /// Scratch for building `rows` × `columns` arrays from scalars holding `payload` bytes.
    pub(crate) fn array_scratch(
        &self,
        payload: usize,
        rows: usize,
        columns: usize,
        component: &str,
    ) -> Result<usize, DbError> {
        let copied_payload = self.product(payload, 2, component)?;
        let cells = self.product(rows, columns, component)?;
        let cell_overhead = self.product(cells, 32, component)?;
        let array_roster = self.product(columns, std::mem::size_of::<ArrayRef>(), component)?;
        self.sum([copied_payload, cell_overhead, array_roster], component)
    }

    pub(crate) fn sum(
        &self,
        values: impl IntoIterator<Item = usize>,
        component: &str,
    ) -> Result<usize, DbError> {
        values.into_iter().try_fold(0_usize, |total, value| {
            total
                .checked_add(value)
                .ok_or_else(|| self.overflow(component))
        })
    }

    fn product(&self, left: usize, right: usize, component: &str) -> Result<usize, DbError> {
        left.checked_mul(right)
            .ok_or_else(|| self.overflow(component))
    }

    fn retain(&mut self, bytes: usize, component: &str) -> Result<(), DbError> {
        self.remaining = self.remaining.checked_sub(bytes).ok_or_else(|| {
            DbError::Checkpoint(format!(
                "{} {component} exceeded its cumulative checkpoint byte limit",
                self.owner
            ))
        })?;
        Ok(())
    }

    fn overflow(&self, component: &str) -> DbError {
        DbError::Checkpoint(format!(
            "{} {component} checkpoint accounting overflow",
            self.owner
        ))
    }
}

pub(crate) fn arrays_to_ipc_bounded(
    arrays: &[ArrayRef],
    max_bytes: usize,
) -> Result<Vec<u8>, DbError> {
    if arrays.is_empty() {
        return Ok(Vec::new());
    }
    let fields: Vec<Arc<Field>> = arrays
        .iter()
        .enumerate()
        .map(|(i, a)| Arc::new(Field::new(format!("c{i}"), a.data_type().clone(), true)))
        .collect();
    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema, arrays.to_vec())
        .map_err(|e| DbError::Pipeline(format!("columnar batch build: {e}")))?;
    laminar_core::serialization::serialize_batches_stream_bounded(
        batch.schema().as_ref(),
        std::iter::once(&batch),
        max_bytes,
    )
    .map_err(|e| DbError::Pipeline(format!("columnar IPC encode: {e}")))
}

/// Group keys as one IPC batch in the row converter's column types; empty without GROUP BY or
/// without groups.
pub(crate) fn encode_group_keys<'a>(
    row_converter: &arrow::row::RowConverter,
    group_columns: usize,
    keys: impl Iterator<Item = &'a arrow::row::OwnedRow> + Clone,
    budget: &mut EncodeBudget,
) -> Result<Vec<u8>, DbError> {
    let rows = keys.clone().count();
    if rows == 0 || group_columns == 0 {
        return Ok(Vec::new());
    }
    let payload = budget.sum(keys.clone().map(|key| key.as_ref().len()), "group keys")?;
    let scratch = budget.array_scratch(payload, rows, group_columns, "group keys")?;
    let limit = budget.transient_limit(scratch, "group keys")?;
    let arrays = row_converter
        .convert_rows(keys.map(arrow::row::OwnedRow::row))
        .map_err(|error| DbError::Checkpoint(format!("group key array build: {error}")))?;
    let encoded = arrays_to_ipc_bounded(&arrays, limit)?;
    budget.retain_encoded(encoded, "group keys")
}

/// Each accumulator's state across `groups` as one IPC batch whose row `j` is group `j`'s state.
pub(crate) fn encode_accumulator_states<'a>(
    groups: &(impl Iterator<Item = &'a [Vec<ScalarValue>]> + Clone),
    budget: &mut EncodeBudget,
) -> Result<Vec<Vec<u8>>, DbError> {
    let Some(first) = groups.clone().next() else {
        return Ok(Vec::new());
    };
    let accumulators = first.len();
    if groups.clone().any(|states| states.len() != accumulators) {
        return Err(DbError::Checkpoint(
            "checkpoint capture has inconsistent accumulator arity".into(),
        ));
    }
    let rows = groups.clone().count();
    budget.retain_roster::<Vec<u8>>(accumulators, "accumulator state roster")?;
    let mut encoded = Vec::with_capacity(accumulators);
    for accumulator in 0..accumulators {
        let arity = first[accumulator].len();
        if groups
            .clone()
            .any(|states| states[accumulator].len() != arity)
        {
            return Err(DbError::Checkpoint(
                "checkpoint capture has inconsistent state arity".into(),
            ));
        }
        let payload = budget.sum(
            groups
                .clone()
                .flat_map(|states| states[accumulator].iter().map(ScalarValue::size)),
            "accumulator state",
        )?;
        let scratch = budget.array_scratch(payload, rows, arity, "accumulator state")?;
        let limit = budget.transient_limit(scratch, "accumulator state")?;
        let columns = (0..arity)
            .map(|column| {
                ScalarValue::iter_to_array(
                    groups
                        .clone()
                        .map(|states| states[accumulator][column].clone()),
                )
                .map_err(|error| {
                    DbError::Checkpoint(format!("accumulator state array build: {error}"))
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let batch = arrays_to_ipc_bounded(&columns, limit)?;
        encoded.push(budget.retain_encoded(batch, "accumulator state")?);
    }
    Ok(encoded)
}
