#![deny(clippy::disallowed_types)]

//! Adapts internal weighted changelog batches for append-only sinks.

use std::borrow::Cow;
use std::sync::Arc;

use arrow::array::{BooleanArray, RecordBatch};

/// Drop rows with non-positive `__weight` and strip the column for non-changelog sinks.
/// Fail-closed: errors return an empty batch rather than leaking negatives into the sink.
pub(crate) fn prepare_for_sink(batch: &RecordBatch, changelog_sink: bool) -> Cow<'_, RecordBatch> {
    if changelog_sink {
        return Cow::Borrowed(batch);
    }
    let Ok(idx) = batch
        .schema()
        .index_of(crate::aggregate_state::WEIGHT_COLUMN)
    else {
        return Cow::Borrowed(batch);
    };
    let stripped_schema = {
        let fields: Vec<_> = batch
            .schema()
            .fields()
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != idx)
            .map(|(_, f)| f.as_ref().clone())
            .collect();
        Arc::new(arrow::datatypes::Schema::new(fields))
    };
    let empty = || Cow::Owned(RecordBatch::new_empty(Arc::clone(&stripped_schema)));

    let Some(weights) = batch
        .column(idx)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
    else {
        tracing::error!(
            "prepare_for_sink: __weight column is not Int64; dropping batch \
             to avoid leaking it to an append-only sink"
        );
        return empty();
    };
    // Keep only rows with positive weight.
    let mask: BooleanArray = weights.iter().map(|w| Some(w.unwrap_or(0) > 0)).collect();
    let Ok(filtered) = arrow::compute::filter_record_batch(batch, &mask) else {
        tracing::error!("prepare_for_sink: filter_record_batch failed; dropping batch");
        return empty();
    };
    // Strip the __weight column.
    if filtered.num_columns() == 0 {
        return Cow::Owned(filtered);
    }
    let indices: Vec<usize> = (0..filtered.num_columns()).filter(|&i| i != idx).collect();
    if let Ok(projected) = filtered.project(&indices) {
        Cow::Owned(projected)
    } else {
        tracing::error!("prepare_for_sink: failed to strip __weight; dropping batch");
        empty()
    }
}
