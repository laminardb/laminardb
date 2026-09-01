use arrow::array::RecordBatch;
use arrow::datatypes::DataType;

use crate::error::DbError;

// Keep batches created here small enough to bound both accumulator work and the temporary
// old-plus-new Arrow buffers held while concatenating one group.
pub(super) const LOCAL_AGG_COALESCE_TARGET_BATCH_BYTES: usize = 256 * 1024;
pub(super) const LOCAL_AGG_COALESCE_MAX_BATCH_ROWS: usize = 1_024;

#[derive(Clone, Copy)]
pub(super) enum AggregateBatchCoalescing {
    Input,
    #[cfg(feature = "cluster")]
    PublishedOutput,
}

impl AggregateBatchCoalescing {
    const fn context(self) -> &'static str {
        match self {
            Self::Input => "local input before state application",
            #[cfg(feature = "cluster")]
            Self::PublishedOutput => "published output",
        }
    }

    const fn preserves_weighted_boundaries(self) -> bool {
        matches!(self, Self::Input)
    }
}

/// Whether Arrow concatenation is representation-stable for this aggregate batch type.
///
/// Dictionary and nested encodings can make independently valid batches fail only when their
/// value spaces or offsets are merged. Preserve their original boundaries unless a type-specific
/// coalescing proof is added.
fn certifies_aggregate_concat_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Null
            | DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64
            | DataType::Timestamp(_, _)
            | DataType::Date32
            | DataType::Date64
            | DataType::Time32(_)
            | DataType::Time64(_)
            | DataType::Duration(_)
            | DataType::Interval(_)
            | DataType::Binary
            | DataType::FixedSizeBinary(_)
            | DataType::LargeBinary
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Decimal32(_, _)
            | DataType::Decimal64(_, _)
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
    )
}

fn flush_group(
    op_name: &str,
    group: &mut Vec<RecordBatch>,
    output: &mut Vec<RecordBatch>,
    mode: AggregateBatchCoalescing,
) -> Result<(), DbError> {
    match group.len() {
        0 => Ok(()),
        1 => {
            output.push(
                group
                    .pop()
                    .expect("single aggregate coalescing group batch"),
            );
            Ok(())
        }
        _ => {
            let schema = group[0].schema();
            let combined =
                arrow::compute::concat_batches(&schema, group.as_slice()).map_err(|error| {
                    DbError::Pipeline(format!(
                        "aggregate '{op_name}' {} concat failed: {error}",
                        mode.context()
                    ))
                })?;
            validate_combined_batch(op_name, &combined, mode)?;
            group.clear();
            output.push(combined);
            Ok(())
        }
    }
}

fn validate_combined_batch(
    op_name: &str,
    batch: &RecordBatch,
    mode: AggregateBatchCoalescing,
) -> Result<(), DbError> {
    let logical_bytes = batch_logical_bytes(op_name, batch, mode)?;
    if batch.num_rows() <= LOCAL_AGG_COALESCE_MAX_BATCH_ROWS
        && logical_bytes <= LOCAL_AGG_COALESCE_TARGET_BATCH_BYTES
    {
        return Ok(());
    }
    Err(DbError::Pipeline(format!(
        "aggregate '{op_name}' coalesced {} exceeded its target: {} rows/{logical_bytes} bytes (limits: {} rows/{} bytes)",
        mode.context(),
        batch.num_rows(),
        LOCAL_AGG_COALESCE_MAX_BATCH_ROWS,
        LOCAL_AGG_COALESCE_TARGET_BATCH_BYTES,
    )))
}

fn batch_logical_bytes(
    op_name: &str,
    batch: &RecordBatch,
    mode: AggregateBatchCoalescing,
) -> Result<usize, DbError> {
    laminar_core::shuffle::logical_batch_bytes(batch).map_err(|error| {
        DbError::Pipeline(format!(
            "aggregate '{op_name}' {} size accounting failed: {error}",
            mode.context()
        ))
    })
}

/// Coalesce compatible aggregate batches without changing row or schema order.
///
/// The input is consumed so concatenation overlaps new Arrow buffers with only the current group.
/// Existing oversized batches are preserved: these limits constrain only batches created here.
pub(super) fn coalesce_aggregate_batches(
    op_name: &str,
    batches: Vec<RecordBatch>,
    mode: AggregateBatchCoalescing,
) -> Result<Vec<RecordBatch>, DbError> {
    // Retraction validity is checked at each weighted input boundary. Published output has already
    // passed this validation, so adjacent vnode batches may be combined for ordinary graph edges.
    if mode.preserves_weighted_boundaries()
        && batches.iter().any(|batch| {
            batch
                .schema()
                .index_of(laminar_core::changelog::WEIGHT_COLUMN)
                .is_ok()
        })
    {
        return Ok(batches);
    }
    if batches.iter().any(|batch| {
        batch
            .schema()
            .fields()
            .iter()
            .any(|field| !certifies_aggregate_concat_type(field.data_type()))
    }) {
        return Ok(batches);
    }

    let mut output = Vec::new();
    let mut group = Vec::new();
    let mut group_rows = 0usize;
    let mut group_bytes = 0usize;
    for batch in batches.into_iter().filter(|batch| batch.num_rows() != 0) {
        let batch_rows = batch.num_rows();
        let batch_bytes = batch_logical_bytes(op_name, &batch, mode)?;
        let independently_coalescible = batch_rows <= LOCAL_AGG_COALESCE_MAX_BATCH_ROWS
            && batch_bytes <= LOCAL_AGG_COALESCE_TARGET_BATCH_BYTES;
        if !independently_coalescible {
            flush_group(op_name, &mut group, &mut output, mode)?;
            group_rows = 0;
            group_bytes = 0;
            output.push(batch);
            continue;
        }

        let same_schema = group
            .first()
            .is_none_or(|first: &RecordBatch| first.schema().as_ref() == batch.schema().as_ref());
        let next_rows = group_rows.checked_add(batch_rows);
        let next_bytes = group_bytes.checked_add(batch_bytes);
        let fits = same_schema
            && next_rows.is_some_and(|rows| rows <= LOCAL_AGG_COALESCE_MAX_BATCH_ROWS)
            && next_bytes.is_some_and(|bytes| bytes <= LOCAL_AGG_COALESCE_TARGET_BATCH_BYTES);
        if !fits {
            flush_group(op_name, &mut group, &mut output, mode)?;
            group_rows = 0;
            group_bytes = 0;
        }
        group.push(batch);
        group_rows += batch_rows;
        group_bytes += batch_bytes;
    }
    flush_group(op_name, &mut group, &mut output, mode)?;
    Ok(output)
}
