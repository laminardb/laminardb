//! MIN and MAX over a changelog: every admitted value with its net weight, so retracting the
//! current extreme reveals the next one. Values are keyed by their Arrow row encoding, which
//! orders them as `ExtremumState` does: floats in IEEE total order, strings and binaries in byte
//! order. The row encoding is not stable across Arrow versions, so checkpoints store the values
//! as an Arrow array's buffers instead.

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{make_array, Array, ArrayData};
use arrow::buffer::Buffer;
use arrow::datatypes::DataType;
use arrow::row::{RowConverter, SortField};
use datafusion_common::ScalarValue;

use super::min_max::Extremum;
use super::sum::require_array_type;
use super::{aggregate_error, AggregateInput};
use crate::error::DbError;

const CHECKPOINT_FORMAT: u8 = 1;

/// Charged per distinct value beyond its key bytes: the key's box, the weight, and B-tree node
/// slack.
const ENTRY_OVERHEAD_BYTES: usize = 48;

/// Each distinct admitted value of one group, by row encoding, with its positive net weight.
#[derive(Clone, Debug)]
pub(crate) struct RetractableExtremumState {
    value_type: DataType,
    converter: Arc<RowConverter>,
    weights: BTreeMap<Box<[u8]>, i64>,
    key_bytes: usize,
}

impl RetractableExtremumState {
    pub(super) fn try_new(input_type: &DataType, output_type: &DataType) -> Result<Self, DbError> {
        if input_type != output_type || !admits(input_type) {
            return Err(DbError::Unsupported(format!(
                "[{}] MIN/MAX over a changelog does not support {input_type:?} -> {output_type:?}",
                laminar_core::error_codes::SQL_UNSUPPORTED
            )));
        }
        let converter = RowConverter::new(vec![SortField::new(input_type.clone())])
            .map_err(|error| aggregate_error("MIN/MAX row encoding", error))?;
        Ok(Self {
            value_type: input_type.clone(),
            converter: Arc::new(converter),
            weights: BTreeMap::new(),
            key_bytes: 0,
        })
    }

    pub(super) fn check_input(
        &self,
        extremum: Extremum,
        input: &AggregateInput<'_>,
    ) -> Result<(), DbError> {
        require_array_type(input.value, &self.value_type, extremum.name())
    }

    /// Applies each admitted non-NULL row's weight. On error the state is as it was.
    pub(super) fn update_rows(
        &mut self,
        extremum: Extremum,
        input: &AggregateInput<'_>,
        rows: &[u32],
    ) -> Result<(), DbError> {
        let weights = input.weights()?;
        let encoded = input.row_encoding()?;
        let nulls = input.nulls;
        let admitted = || {
            input
                .selected(rows)
                .filter(|&row| nulls.is_none_or(|nulls| nulls.is_valid(row)))
        };
        for (applied, row) in admitted().enumerate() {
            if let Err(error) = self.add(extremum, encoded.row(row).as_ref(), weights.value(row)) {
                // Newest first, so each step restores a state that existed.
                let undo = admitted().take(applied).collect::<Vec<_>>();
                for row in undo.into_iter().rev() {
                    self.add(extremum, encoded.row(row).as_ref(), -weights.value(row))
                        .expect("reverting an applied weight cannot fail");
                }
                return Err(error);
            }
        }
        Ok(())
    }

    fn add(&mut self, extremum: Extremum, key: &[u8], weight: i64) -> Result<(), DbError> {
        let name = extremum.name();
        if let Some(current) = self.weights.get_mut(key) {
            let next = current
                .checked_add(weight)
                .ok_or_else(|| DbError::Pipeline(format!("{name} value weight overflow")))?;
            match next.cmp(&0) {
                Ordering::Greater => *current = next,
                Ordering::Equal => {
                    self.weights.remove(key);
                    self.key_bytes -= key.len();
                }
                Ordering::Less => {
                    return Err(DbError::Pipeline(format!(
                        "{name} value weight became negative after retraction"
                    )));
                }
            }
            return Ok(());
        }
        match weight.cmp(&0) {
            Ordering::Greater => {
                self.weights.insert(key.into(), weight);
                self.key_bytes += key.len();
                Ok(())
            }
            Ordering::Equal => Ok(()),
            Ordering::Less => Err(DbError::Pipeline(format!(
                "{name} retracted a value it does not hold"
            ))),
        }
    }

    pub(super) fn evaluate(&self, extremum: Extremum) -> Result<ScalarValue, DbError> {
        let extreme = match extremum {
            Extremum::Min => self.weights.first_key_value(),
            Extremum::Max => self.weights.last_key_value(),
        };
        let Some((key, _)) = extreme else {
            return ScalarValue::try_from(&self.value_type)
                .map_err(|error| aggregate_error("MIN/MAX null result", error));
        };
        let parser = self.converter.parser();
        let values = self
            .converter
            .convert_rows([parser.parse(key)])
            .map_err(|error| aggregate_error("MIN/MAX value decode", error))?;
        ScalarValue::try_from_array(&values[0], 0)
            .map_err(|error| aggregate_error("MIN/MAX value", error))
    }

    /// One `Binary` scalar: the format byte, the value count, each weight, then the buffers of
    /// a non-NULL array of the values in ascending order.
    pub(super) fn checkpoint_state(&self) -> Vec<ScalarValue> {
        let parser = self.converter.parser();
        // INVARIANT: every key was produced by this converter from a column of `value_type`.
        let values = self
            .converter
            .convert_rows(self.weights.keys().map(|key| parser.parse(key)))
            .expect("MIN/MAX keys decode with the converter that encoded them");
        let data = values[0].to_data();
        let count = u32::try_from(self.weights.len()).expect("distinct values fit u32");
        let mut encoded = Vec::with_capacity(
            9 + self.weights.len() * 8 + data.buffers().iter().map(|b| 8 + b.len()).sum::<usize>(),
        );
        encoded.push(CHECKPOINT_FORMAT);
        encoded.extend_from_slice(&count.to_le_bytes());
        for weight in self.weights.values() {
            encoded.extend_from_slice(&weight.to_le_bytes());
        }
        let buffers = u32::try_from(data.buffers().len()).expect("an array's buffers fit u32");
        encoded.extend_from_slice(&buffers.to_le_bytes());
        for buffer in data.buffers() {
            let length = u64::try_from(buffer.len()).expect("a buffer length fits u64");
            encoded.extend_from_slice(&length.to_le_bytes());
            encoded.extend_from_slice(buffer.as_slice());
        }
        vec![ScalarValue::Binary(Some(encoded))]
    }

    /// Adds a checkpoint's weights. On error the state is as it was.
    pub(super) fn merge_checkpoint_state(
        &mut self,
        extremum: Extremum,
        checkpoint: &[ScalarValue],
    ) -> Result<(), DbError> {
        let name = extremum.name();
        let [ScalarValue::Binary(Some(encoded))] = checkpoint else {
            return Err(DbError::Pipeline(format!(
                "{name} changelog checkpoint state must be one non-NULL Binary"
            )));
        };
        let (weights, values) = decode_checkpoint(&self.value_type, encoded)
            .map_err(|detail| DbError::Pipeline(format!("{name} changelog checkpoint {detail}")))?;
        let rows = self
            .converter
            .convert_columns(&[values])
            .map_err(|error| aggregate_error("MIN/MAX checkpoint encoding", error))?;
        for index in 1..rows.num_rows() {
            if rows.row(index - 1) >= rows.row(index) {
                return Err(DbError::Pipeline(format!(
                    "{name} changelog checkpoint values are not strictly ascending"
                )));
            }
        }
        for (index, &weight) in weights.iter().enumerate() {
            if let Err(error) = self.add(extremum, rows.row(index).as_ref(), weight) {
                for (index, &weight) in weights.iter().enumerate().take(index).rev() {
                    self.add(extremum, rows.row(index).as_ref(), -weight)
                        .expect("reverting an applied weight cannot fail");
                }
                return Err(error);
            }
        }
        Ok(())
    }

    pub(super) fn nested_retained_bytes(&self) -> usize {
        self.key_bytes + self.weights.len() * ENTRY_OVERHEAD_BYTES
    }
}

/// Types whose row encoding orders as `ExtremumState` compares, and whose arrays are flat.
fn admits(data_type: &DataType) -> bool {
    match data_type {
        DataType::Interval(_) => false,
        DataType::Boolean
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => true,
        other => other.is_primitive(),
    }
}

/// The weights and the values array of one checkpoint state, checked against `value_type`.
fn decode_checkpoint(
    value_type: &DataType,
    encoded: &[u8],
) -> Result<(Vec<i64>, arrow::array::ArrayRef), String> {
    let mut cursor = Cursor(encoded);
    if cursor.take(1)? != [CHECKPOINT_FORMAT] {
        return Err("has an unknown format".into());
    }
    let count = usize::try_from(u32::from_le_bytes(cursor.array()?))
        .map_err(|_| "value count overflows".to_string())?;
    if count.saturating_mul(8) > cursor.0.len() {
        return Err("is truncated".into());
    }
    let weights = (0..count)
        .map(|_| {
            let weight = i64::from_le_bytes(cursor.array()?);
            if weight > 0 {
                Ok(weight)
            } else {
                Err("holds a non-positive weight".to_string())
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    let buffer_count = u32::from_le_bytes(cursor.array()?);
    if usize::try_from(buffer_count).map_or(true, |count| count.saturating_mul(8) > cursor.0.len())
    {
        return Err("is truncated".into());
    }
    let buffers = (0..buffer_count)
        .map(|_| {
            let length = usize::try_from(u64::from_le_bytes(cursor.array()?))
                .map_err(|_| "buffer length overflows".to_string())?;
            Ok(Buffer::from(cursor.take(length)?))
        })
        .collect::<Result<Vec<_>, String>>()?;
    if !cursor.0.is_empty() {
        return Err("has trailing bytes".into());
    }
    let data = ArrayData::try_new(value_type.clone(), count, None, 0, buffers, Vec::new())
        .map_err(|error| format!("values are invalid: {error}"))?;
    Ok((weights, make_array(data)))
}

struct Cursor<'a>(&'a [u8]);

impl<'a> Cursor<'a> {
    fn take(&mut self, length: usize) -> Result<&'a [u8], String> {
        if length > self.0.len() {
            return Err("is truncated".into());
        }
        let (taken, rest) = self.0.split_at(length);
        self.0 = rest;
        Ok(taken)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], String> {
        Ok(self.take(N)?.try_into().expect("took exactly N bytes"))
    }
}
