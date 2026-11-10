//! The row key of global aggregates.

use std::sync::{Arc, OnceLock};

use arrow::array::ArrayRef;
use arrow::datatypes::DataType;

/// Stable singleton row key for global aggregates (no GROUP BY).
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
