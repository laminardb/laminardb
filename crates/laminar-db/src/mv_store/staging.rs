//! Bound transient keyed deltas and their Arrow-to-row conversion input.

use arrow::array::{ArrayData, ByteView, OffsetSizeTrait, RecordBatch};
use arrow::datatypes::DataType;

use super::admission::{size_overflow, MvLimits};
use super::WEIGHT_COLUMN;
use crate::DbError;

// Arrow row encoding adds null/list/length markers and pads short variable-width values.
// Sixteen bytes per value plus twice the buffers bounds those encodings, including booleans.
const ROW_ENCODING_VALUE_OVERHEAD: usize = 16;

pub(super) struct StagingBudget {
    limits: MvLimits,
    rows: usize,
    bytes: usize,
}

impl StagingBudget {
    pub(super) fn new(limits: MvLimits, entry_overhead: usize) -> Self {
        // Allow an entire replacement, including old-row retractions, before final-state
        // admission. The additional entry metadata is bounded by the same touched-row cap.
        let rows = limits.rows.saturating_mul(2);
        let bytes = limits
            .bytes
            .saturating_mul(2)
            .saturating_add(rows.saturating_mul(entry_overhead));
        Self {
            limits: MvLimits { rows, bytes },
            rows: 0,
            bytes: 0,
        }
    }

    pub(super) fn validate_input(&self, name: &str, batch: &RecordBatch) -> Result<(), DbError> {
        self.limits.validate(name, batch.num_rows(), 0)?;
        let mut bytes = 0usize;
        for (field, column) in batch.schema_ref().fields().iter().zip(batch.columns()) {
            if field.name() == WEIGHT_COLUMN {
                continue;
            }
            bytes = bytes
                .checked_add(input_bytes(&column.to_data())?)
                .ok_or_else(size_overflow)?;
            self.limits.validate(name, batch.num_rows(), bytes)?;
        }
        Ok(())
    }

    pub(super) fn replace(
        &mut self,
        name: &str,
        old: Option<usize>,
        new: Option<usize>,
    ) -> Result<(), DbError> {
        let rows = (self.rows - usize::from(old.is_some()))
            .checked_add(usize::from(new.is_some()))
            .ok_or_else(size_overflow)?;
        let bytes = (self.bytes - old.unwrap_or(0))
            .checked_add(new.unwrap_or(0))
            .ok_or_else(size_overflow)?;
        self.limits.validate(name, rows, bytes)?;
        self.rows = rows;
        self.bytes = bytes;
        Ok(())
    }
}

fn input_bytes(data: &ArrayData) -> Result<usize, DbError> {
    let rows = data
        .len()
        .checked_mul(ROW_ENCODING_VALUE_OVERHEAD)
        .ok_or_else(size_overflow)?;
    let buffers = data.buffers().iter().try_fold(0usize, |total, buffer| {
        total.checked_add(buffer.len()).ok_or_else(size_overflow)
    })?;
    let children = data.child_data().iter().try_fold(0usize, |total, child| {
        total
            .checked_add(input_bytes(child)?)
            .ok_or_else(size_overflow)
    })?;
    let payload = buffers
        .checked_add(children)
        .and_then(|bytes| bytes.checked_mul(2))
        .ok_or_else(size_overflow)?;
    // Charge repeated values, not repeated descriptors/key buffers. Counting backing
    // storage alone misses alias expansion; multiplying all buffers rejects ordinary input.
    let aliases = match data.data_type() {
        DataType::Dictionary(..) => {
            let values = data.child_data().first().ok_or_else(size_overflow)?;
            data.len()
                .checked_mul(max_value_bytes(values)?)
                .ok_or_else(size_overflow)?
        }
        DataType::Utf8View | DataType::BinaryView => view_lengths(data)
            .try_fold(0usize, usize::checked_add)
            .and_then(|bytes| bytes.checked_mul(2))
            .ok_or_else(size_overflow)?,
        DataType::ListView(_)
        | DataType::LargeListView(_)
        | DataType::Union(..)
        | DataType::RunEndEncoded(..) => {
            data.len().checked_mul(payload).ok_or_else(size_overflow)?
        }
        _ => 0,
    };
    payload
        .checked_add(aliases)
        .and_then(|bytes| bytes.checked_add(rows))
        .ok_or_else(size_overflow)
}

fn view_lengths(data: &ArrayData) -> impl Iterator<Item = usize> + '_ {
    data.buffer::<u128>(0)
        .iter()
        .take(data.len())
        .enumerate()
        .filter(|(index, _)| !data.nulls().is_some_and(|nulls| nulls.is_null(*index)))
        .map(|(_, view)| ByteView::from(*view).length as usize)
}

fn max_value_bytes(data: &ArrayData) -> Result<usize, DbError> {
    let width = match data.data_type() {
        DataType::Null => 0,
        DataType::Boolean => 1,
        DataType::FixedSizeBinary(width) => usize::try_from(*width).map_err(|_| size_overflow())?,
        DataType::Utf8 | DataType::Binary => max_offset_length::<i32>(data)?,
        DataType::LargeUtf8 | DataType::LargeBinary => max_offset_length::<i64>(data)?,
        DataType::Utf8View | DataType::BinaryView => view_lengths(data).max().unwrap_or(0),
        kind => match kind.primitive_width() {
            Some(width) => width,
            // Complex dictionary values use the whole child as a conservative bound.
            None => return input_bytes(data),
        },
    };
    width
        .checked_mul(2)
        .and_then(|bytes| bytes.checked_add(ROW_ENCODING_VALUE_OVERHEAD))
        .ok_or_else(size_overflow)
}

fn max_offset_length<O: OffsetSizeTrait>(data: &ArrayData) -> Result<usize, DbError> {
    data.buffer::<O>(0)
        .windows(2)
        .take(data.len())
        .try_fold(0, |largest, offsets| {
            let start = offsets[0].to_usize().ok_or_else(size_overflow)?;
            let end = offsets[1].to_usize().ok_or_else(size_overflow)?;
            let length = end.checked_sub(start).ok_or_else(size_overflow)?;
            Ok(largest.max(length))
        })
}
