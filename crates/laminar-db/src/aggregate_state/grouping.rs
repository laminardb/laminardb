//! One input batch's rows grouped by encoded group key.

use std::hash::BuildHasher;
use std::sync::Arc;

use arrow::array::{downcast_primitive_array, Array, ArrayRef, Int64Array, PrimitiveArray};
use arrow::buffer::{BooleanBuffer, NullBuffer};
use arrow::datatypes::{ArrowPrimitiveType, DataType, ToByteSlice};
use arrow::row::{RowConverter, Rows};
use hashbrown::hash_table::{Entry, HashTable};
use rustc_hash::FxBuildHasher;

use super::group_map::GroupKey;
use crate::error::DbError;

/// A batch's row-encoded group keys.
pub(crate) enum EncodedKeys {
    /// Every row's key, in batch order.
    PerRow(Rows),
    /// Each group's key once, in first-appearance order, and each row's group.
    PerGroup { keys: Rows, group_of_row: Vec<u32> },
}

/// Groups a single fixed-width key column on its values' bits, so each group's key is
/// row-encoded once. Two rows group together exactly when their row encodings are equal: an
/// admitted type's row encoding is a bijection on the value's bits, and every null encodes alike.
pub(crate) struct FixedWidthKeyGrouper {
    data_type: DataType,
}

/// PERF: a smaller batch is encoded row by row. The group array costs about 25 ns per batch, and
/// grouping on values saves about 3 ns per row (measured: +23 ns at 1 row, +13 ns at 4).
const MIN_GROUPED_ROWS: usize = 16;

impl FixedWidthKeyGrouper {
    /// A grouper for `group_types`, or `None` unless it is one column of an admitted type.
    pub(crate) fn for_key(group_types: &[DataType]) -> Option<Self> {
        match group_types {
            [data_type] if groups_on_values(data_type) => Some(Self {
                data_type: data_type.clone(),
            }),
            _ => None,
        }
    }

    pub(crate) fn encode(
        &self,
        key: &ArrayRef,
        converter: &RowConverter,
    ) -> Result<EncodedKeys, DbError> {
        if !key.data_type().equals_datatype(&self.data_type) {
            return Err(DbError::Pipeline(format!(
                "aggregate key column is {}, expected {}",
                key.data_type(),
                self.data_type
            )));
        }
        if key.len() < MIN_GROUPED_ROWS {
            return converter
                .convert_columns(std::slice::from_ref(key))
                .map(EncodedKeys::PerRow)
                .map_err(|error| DbError::Pipeline(format!("row conversion: {error}")));
        }
        row_count(key.len())?;
        let (group_values, group_of_row): (ArrayRef, _) = downcast_primitive_array!(
            key => {
                let (values, group_of_row) = group_values(key);
                (Arc::new(values), group_of_row)
            },
            other => {
                return Err(DbError::Pipeline(format!(
                    "aggregate key column {other} is not fixed-width"
                )))
            }
        );
        let keys = converter
            .convert_columns(&[group_values])
            .map_err(|error| DbError::Pipeline(format!("row conversion: {error}")))?;
        Ok(EncodedKeys::PerGroup { keys, group_of_row })
    }
}

/// Each group's value, in first-appearance order, and each row's group. `key` holds at most
/// `u32::MAX` rows.
fn group_values<T: ArrowPrimitiveType>(key: &PrimitiveArray<T>) -> (PrimitiveArray<T>, Vec<u32>) {
    let estimated_groups = (key.len() / 4).max(16);
    let mut groups: HashTable<(T::Native, u32)> = HashTable::with_capacity(estimated_groups);
    let mut values = Vec::with_capacity(estimated_groups);
    let mut group_of_row = Vec::with_capacity(key.len());
    let mut null_group = None;
    let mut group_count = 0;
    let mut next_group = |values: &mut Vec<T::Native>, value| {
        values.push(value);
        group_count += 1;
        group_count - 1
    };
    for (row, value) in key.values().iter().enumerate() {
        if key.is_null(row) {
            let group =
                *null_group.get_or_insert_with(|| next_group(&mut values, T::Native::default()));
            group_of_row.push(group);
            continue;
        }
        let bits = value.to_byte_slice();
        let group = match groups.entry(
            FxBuildHasher.hash_one(bits),
            |(seen, _)| seen.to_byte_slice() == bits,
            |(seen, _)| FxBuildHasher.hash_one(seen.to_byte_slice()),
        ) {
            Entry::Occupied(occupied) => occupied.get().1,
            Entry::Vacant(vacant) => {
                let group = next_group(&mut values, *value);
                vacant.insert((*value, group));
                group
            }
        };
        group_of_row.push(group);
    }
    let nulls = null_group.map(|null_group| {
        NullBuffer::new(BooleanBuffer::collect_bool(values.len(), |group| {
            group != null_group as usize
        }))
    });
    let values =
        PrimitiveArray::<T>::new(values.into(), nulls).with_data_type(key.data_type().clone());
    (values, group_of_row)
}

fn groups_on_values(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
            | DataType::Date32
            | DataType::Date64
            | DataType::Timestamp(_, _)
            | DataType::Decimal128(_, _)
    )
}

/// A batch's groups in first-appearance order, with all row indices in one buffer. Each group's
/// rows stay in batch order, so accumulators see the same sequence as a per-group scan. Each row
/// is hashed once; the resident lookup reuses its group's hash.
pub(super) struct BatchGroups<'a> {
    keys: Vec<GroupKey<'a>>,
    input_weights: Vec<i64>,
    /// Exclusive end of each group's rows in `rows`; a group starts where the previous one ends.
    ends: Vec<u32>,
    rows: Vec<u32>,
}

impl<'a> BatchGroups<'a> {
    pub(super) fn try_new(
        keys: &'a EncodedKeys,
        input_weights: Option<&Int64Array>,
    ) -> Result<Self, DbError> {
        match keys {
            EncodedKeys::PerRow(rows) => Self::from_rows(rows, input_weights),
            EncodedKeys::PerGroup { keys, group_of_row } => {
                Self::from_group_ids(keys, group_of_row, input_weights)
            }
        }
    }

    fn from_rows(rows: &'a Rows, input_weights: Option<&Int64Array>) -> Result<Self, DbError> {
        let num_rows = row_count(rows.num_rows())?;
        let estimated_groups = (rows.num_rows() / 4).max(16);
        let mut group_ids: HashTable<(GroupKey<'a>, u32)> =
            HashTable::with_capacity(estimated_groups);
        let mut keys: Vec<GroupKey<'a>> = Vec::with_capacity(estimated_groups);
        let mut weights: Vec<i64> = Vec::with_capacity(estimated_groups);
        let mut ends: Vec<u32> = Vec::with_capacity(estimated_groups);
        let mut group_of_row = Vec::with_capacity(rows.num_rows());
        let mut group_count = 0;
        for row_idx in 0..num_rows {
            let key = GroupKey::new(rows.row(row_idx as usize));
            let group = match group_ids.entry(
                key.hash(),
                |(seen, _)| seen.bytes() == key.bytes(),
                |(seen, _)| seen.hash(),
            ) {
                Entry::Occupied(occupied) => occupied.get().1,
                Entry::Vacant(vacant) => {
                    let group = group_count;
                    vacant.insert((key, group));
                    group_count += 1;
                    keys.push(key);
                    weights.push(0);
                    ends.push(0);
                    group
                }
            };
            add_row(
                &mut weights[group as usize],
                &mut ends[group as usize],
                input_weights,
                row_idx,
            )?;
            group_of_row.push(group);
        }
        Ok(Self::sorted(
            keys,
            weights,
            ends,
            num_rows,
            group_of_row.into_iter().map(|group| group as usize),
        ))
    }

    fn from_group_ids(
        group_keys: &'a Rows,
        group_of_row: &[u32],
        input_weights: Option<&Int64Array>,
    ) -> Result<Self, DbError> {
        let num_rows = row_count(group_of_row.len())?;
        let groups = group_keys.num_rows();
        let keys = (0..groups)
            .map(|group| GroupKey::new(group_keys.row(group)))
            .collect();
        let mut weights = vec![0; groups];
        let mut ends = vec![0; groups];
        for (row_idx, &group) in (0..num_rows).zip(group_of_row) {
            add_row(
                &mut weights[group as usize],
                &mut ends[group as usize],
                input_weights,
                row_idx,
            )?;
        }
        Ok(Self::sorted(
            keys,
            weights,
            ends,
            num_rows,
            group_of_row.iter().map(|&group| group as usize),
        ))
    }

    /// `ends` holds each group's row count on entry.
    fn sorted(
        keys: Vec<GroupKey<'a>>,
        input_weights: Vec<i64>,
        mut ends: Vec<u32>,
        num_rows: u32,
        group_of_row: impl Iterator<Item = usize>,
    ) -> Self {
        // Counts become start offsets, and the stable scatter moves each start to its group's end.
        let mut start = 0;
        for slot in &mut ends {
            let count = *slot;
            *slot = start;
            start += count;
        }
        let mut sorted = vec![0; num_rows as usize];
        for (row_idx, group) in (0..num_rows).zip(group_of_row) {
            let slot = &mut ends[group];
            sorted[*slot as usize] = row_idx;
            *slot += 1;
        }
        Self {
            keys,
            input_weights,
            ends,
            rows: sorted,
        }
    }

    pub(super) fn len(&self) -> usize {
        self.keys.len()
    }

    /// Each group's key, row indices, and summed input weight.
    pub(super) fn iter(&self) -> impl Iterator<Item = (GroupKey<'a>, &[u32], i64)> + '_ {
        let mut start = 0;
        self.keys
            .iter()
            .zip(&self.input_weights)
            .zip(&self.ends)
            .map(move |((key, weight), &end)| {
                let rows = &self.rows[start as usize..end as usize];
                start = end;
                (*key, rows, *weight)
            })
    }
}

fn row_count(rows: usize) -> Result<u32, DbError> {
    u32::try_from(rows).map_err(|_| {
        DbError::Pipeline(format!(
            "aggregate batch has {rows} rows; row indices are limited to u32"
        ))
    })
}

#[inline]
fn add_row(
    weight: &mut i64,
    count: &mut u32,
    input_weights: Option<&Int64Array>,
    row_idx: u32,
) -> Result<(), DbError> {
    let row_weight = input_weights.map_or(1, |column| column.value(row_idx as usize));
    *weight = weight
        .checked_add(row_weight)
        .ok_or_else(|| DbError::Pipeline("aggregate input weight overflow".into()))?;
    *count += 1;
    Ok(())
}

#[cfg(test)]
mod tests {
    use arrow::array::{
        Date32Array, Date64Array, Decimal128Array, Float32Array, Float64Array, Int16Array,
        Int32Array, Int8Array, StringArray, TimestampMicrosecondArray, TimestampMillisecondArray,
        TimestampNanosecondArray, TimestampSecondArray, UInt16Array, UInt32Array, UInt64Array,
        UInt8Array,
    };
    use arrow::compute::take;
    use arrow::row::SortField;

    use super::*;

    fn converter(data_type: &DataType) -> RowConverter {
        RowConverter::new(vec![SortField::new(data_type.clone())]).unwrap()
    }

    fn per_row(column: &ArrayRef) -> EncodedKeys {
        let rows = converter(column.data_type())
            .convert_columns(&[Arc::clone(column)])
            .unwrap();
        EncodedKeys::PerRow(rows)
    }

    fn strings(keys: Vec<Option<&str>>) -> EncodedKeys {
        per_row(&(Arc::new(StringArray::from(keys)) as ArrayRef))
    }

    /// Each group's encoded key, row indices and summed weight.
    fn grouped(keys: &EncodedKeys, weights: Option<&Int64Array>) -> Vec<(Vec<u8>, Vec<u32>, i64)> {
        BatchGroups::try_new(keys, weights)
            .unwrap()
            .iter()
            .map(|(key, rows, weight)| (key.bytes().to_vec(), rows.to_vec(), weight))
            .collect()
    }

    #[test]
    fn groups_keep_first_appearance_and_batch_row_order() {
        let keys = strings(vec![
            Some("b"),
            Some("a"),
            Some("b"),
            None,
            Some("a"),
            Some("b"),
        ]);
        let EncodedKeys::PerRow(rows) = &keys else {
            unreachable!()
        };
        let weights = Int64Array::from(vec![1, 2, 3, 4, 5, -6]);
        assert_eq!(BatchGroups::try_new(&keys, None).unwrap().len(), 3);
        assert_eq!(
            grouped(&keys, Some(&weights)),
            vec![
                (rows.row(0).data().to_vec(), vec![0, 2, 5], -2),
                (rows.row(1).data().to_vec(), vec![1, 4], 7),
                (rows.row(3).data().to_vec(), vec![3], 4),
            ]
        );
        let weights: Vec<i64> = grouped(&keys, None)
            .into_iter()
            .map(|(_, _, weight)| weight)
            .collect();
        assert_eq!(weights, vec![3, 2, 1]);
    }

    #[test]
    fn group_weight_overflow_rejects_the_batch() {
        let keys = strings(vec![Some("a"), Some("a")]);
        let weights = Int64Array::from(vec![i64::MAX, 1]);
        let Err(DbError::Pipeline(message)) = BatchGroups::try_new(&keys, Some(&weights)) else {
            panic!("an overflowing group weight must reject the batch");
        };
        assert_eq!(message, "aggregate input weight overflow");
    }

    /// One column per admitted key type, with the values whose encodings are easiest to conflate:
    /// nulls, both zeros, NaNs with different payloads, and each type's extremes.
    fn admitted_columns() -> Vec<ArrayRef> {
        macro_rules! extremes {
            ($array:ty, $native:ty) => {
                <$array>::from(vec![
                    Some(1),
                    None,
                    Some(<$native>::MIN),
                    Some(1),
                    Some(<$native>::MAX),
                    None,
                    Some(0),
                    Some(<$native>::MIN),
                ])
            };
        }
        macro_rules! floats {
            ($array:ty, $native:ty, $nan_payload:expr) => {
                <$array>::from(vec![
                    Some(0.0),
                    Some(-0.0),
                    Some(<$native>::NAN),
                    Some($nan_payload),
                    Some(-<$native>::NAN),
                    Some(<$native>::NAN),
                    None,
                    Some(-0.0),
                    Some(<$native>::INFINITY),
                    Some(<$native>::NEG_INFINITY),
                    Some($nan_payload),
                ])
            };
        }
        vec![
            Arc::new(extremes!(Int8Array, i8)),
            Arc::new(extremes!(Int16Array, i16)),
            Arc::new(extremes!(Int32Array, i32)),
            Arc::new(extremes!(Int64Array, i64)),
            Arc::new(extremes!(UInt8Array, u8)),
            Arc::new(extremes!(UInt16Array, u16)),
            Arc::new(extremes!(UInt32Array, u32)),
            Arc::new(extremes!(UInt64Array, u64)),
            Arc::new(floats!(Float32Array, f32, f32::from_bits(0x7fc0_0001))),
            Arc::new(floats!(
                Float64Array,
                f64,
                f64::from_bits(0x7ff8_0000_0000_0001)
            )),
            Arc::new(extremes!(Date32Array, i32)),
            Arc::new(extremes!(Date64Array, i64)),
            Arc::new(extremes!(TimestampSecondArray, i64)),
            Arc::new(extremes!(TimestampMillisecondArray, i64).with_timezone("+05:30")),
            Arc::new(extremes!(TimestampMicrosecondArray, i64)),
            Arc::new(extremes!(TimestampNanosecondArray, i64).with_timezone("UTC")),
            Arc::new(
                extremes!(Decimal128Array, i128)
                    .with_precision_and_scale(38, 4)
                    .unwrap(),
            ),
        ]
    }

    #[test]
    fn fixed_width_keys_group_exactly_as_their_row_encodings() {
        for values in admitted_columns() {
            let data_type = values.data_type().clone();
            let converter = converter(&data_type);
            let grouper = FixedWidthKeyGrouper::for_key(std::slice::from_ref(&data_type))
                .unwrap_or_else(|| panic!("{data_type} must be admitted"));
            let column = arrow::compute::concat(&[&values, &values, &values]).unwrap();
            let reversed: Vec<u32> = (0..u32::try_from(column.len()).unwrap()).rev().collect();
            let reversed = take(&column, &UInt32Array::from(reversed), None).unwrap();
            for batch in [&column, &reversed, &column] {
                let weights = Int64Array::from_iter_values((1..).take(batch.len()));
                let keys = grouper.encode(batch, &converter).unwrap();
                assert!(matches!(keys, EncodedKeys::PerGroup { .. }));
                assert_eq!(
                    grouped(&keys, Some(&weights)),
                    grouped(&per_row(batch), Some(&weights)),
                    "{data_type}"
                );
            }
        }
    }

    #[test]
    fn small_batches_are_encoded_row_by_row() {
        let grouper = FixedWidthKeyGrouper::for_key(&[DataType::Int64]).unwrap();
        let column: ArrayRef = Arc::new(Int64Array::from(vec![1; MIN_GROUPED_ROWS - 1]));
        let keys = grouper
            .encode(&column, &converter(&DataType::Int64))
            .unwrap();
        assert!(matches!(keys, EncodedKeys::PerRow(_)));
    }

    #[test]
    fn other_keys_keep_the_row_path() {
        for group_types in [
            vec![DataType::Utf8],
            vec![DataType::Boolean],
            vec![DataType::Float16],
            vec![DataType::Time64(arrow::datatypes::TimeUnit::Nanosecond)],
            vec![DataType::Dictionary(
                Box::new(DataType::Int32),
                Box::new(DataType::Int64),
            )],
            vec![DataType::Int64, DataType::Int64],
        ] {
            assert!(
                FixedWidthKeyGrouper::for_key(&group_types).is_none(),
                "{group_types:?}"
            );
        }
    }

    #[test]
    fn a_key_column_of_another_type_is_rejected() {
        let grouper = FixedWidthKeyGrouper::for_key(&[DataType::Int64]).unwrap();
        let column: ArrayRef = Arc::new(Int32Array::from(vec![1]));
        let Err(DbError::Pipeline(message)) = grouper.encode(&column, &converter(&DataType::Int64))
        else {
            panic!("a mistyped key column must be rejected");
        };
        assert_eq!(message, "aggregate key column is Int32, expected Int64");
    }
}
