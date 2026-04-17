//! Group-key derivation: Arrow `OwnedRow` ↔ `Vec<ScalarValue>` conversions.

use std::sync::{Arc, OnceLock};

use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use datafusion_common::ScalarValue;

use crate::error::DbError;

pub(crate) fn row_to_scalar_key_with_types(
    converter: &arrow::row::RowConverter,
    row_key: &arrow::row::OwnedRow,
    group_types: &[DataType],
) -> Result<Vec<ScalarValue>, DbError> {
    let row_as_cols = converter
        .convert_rows(std::iter::once(row_key.row()))
        .map_err(|e| DbError::Pipeline(format!("row→key: {e}")))?;

    let mut sv_key = Vec::with_capacity(group_types.len());
    for (col_idx, arr) in row_as_cols.iter().enumerate() {
        let sv = ScalarValue::try_from_array(arr, 0)
            .map_err(|e| DbError::Pipeline(format!("group key decode: {e}")))?;
        if sv.data_type() == group_types[col_idx] {
            sv_key.push(sv);
        } else {
            sv_key.push(sv.cast_to(&group_types[col_idx]).unwrap_or(sv));
        }
    }
    Ok(sv_key)
}

/// Singleton row used as the group key when `GROUP BY` is absent.
pub(crate) fn global_aggregate_key() -> arrow::row::OwnedRow {
    static SENTINEL: OnceLock<arrow::row::OwnedRow> = OnceLock::new();
    SENTINEL
        .get_or_init(|| {
            let conv =
                arrow::row::RowConverter::new(vec![arrow::row::SortField::new(DataType::Boolean)])
                    .unwrap();
            let col: ArrayRef = Arc::new(arrow::array::BooleanArray::from(vec![false]));
            conv.convert_columns(&[col]).unwrap().row(0).owned()
        })
        .clone()
}

pub(crate) fn scalar_key_to_owned_row(
    row_converter: &arrow::row::RowConverter,
    sv_key: &[ScalarValue],
    target_types: &[DataType],
) -> Result<arrow::row::OwnedRow, DbError> {
    if sv_key.is_empty() {
        return Ok(global_aggregate_key());
    }
    let arrays: Vec<ArrayRef> = sv_key
        .iter()
        .enumerate()
        .map(|(i, sv)| {
            let arr = sv
                .to_array()
                .map_err(|e| DbError::Pipeline(format!("scalar to array: {e}")))?;
            if i < target_types.len() && arr.data_type() != &target_types[i] {
                arrow::compute::cast(&arr, &target_types[i])
                    .map_err(|e| DbError::Pipeline(format!("key type cast: {e}")))
            } else {
                Ok(arr)
            }
        })
        .collect::<Result<_, _>>()?;
    let rows = row_converter
        .convert_columns(&arrays)
        .map_err(|e| DbError::Pipeline(format!("key to row: {e}")))?;
    Ok(rows.row(0).owned())
}
