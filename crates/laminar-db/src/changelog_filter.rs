#![deny(clippy::disallowed_types)]

//! Fail-closed validation at the weighted-changelog sink boundary.

use arrow::array::{Array, Int64Array, RecordBatch};
use arrow::datatypes::DataType;
use laminar_core::changelog::WEIGHT_COLUMN;

/// Validate one sink-bound batch against the sink's admitted update model.
///
/// A weighted batch must carry one exact, trailing, non-null `Int64` `__weight` field whose
/// values are all non-null and non-zero. It is never collapsed, filtered, or stripped here: only
/// a `FullChangelog` sink may receive it, unchanged.
pub(crate) fn validate_sink_input(
    batch: &RecordBatch,
    accepts_full_changelog: bool,
    expects_changelog: bool,
) -> Result<(), String> {
    let schema = batch.schema();
    let mut weight_index = None;
    for (index, field) in schema.fields().iter().enumerate() {
        if field.name().eq_ignore_ascii_case(WEIGHT_COLUMN) && weight_index.replace(index).is_some()
        {
            return Err(format!(
                "weighted sink input contains more than one case-insensitive {WEIGHT_COLUMN} field"
            ));
        }
    }

    let Some(weight_index) = weight_index else {
        if expects_changelog {
            return Err(format!(
                "sink input admitted as a changelog is missing its exact trailing {WEIGHT_COLUMN} field"
            ));
        }
        return Ok(());
    };
    let field = schema.field(weight_index);
    if field.name() != WEIGHT_COLUMN
        || weight_index + 1 != schema.fields().len()
        || field.data_type() != &DataType::Int64
        || field.is_nullable()
    {
        return Err(format!(
            "weighted sink input requires one exact trailing non-null Int64 {WEIGHT_COLUMN} field"
        ));
    }
    let weights = batch
        .column(weight_index)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| format!("weighted sink input {WEIGHT_COLUMN} array is not Int64"))?;
    if weights.null_count() != 0 {
        return Err(format!(
            "weighted sink input {WEIGHT_COLUMN} contains NULL values"
        ));
    }
    if let Some(row) = weights.values().iter().position(|weight| *weight == 0) {
        return Err(format!(
            "weighted sink input {WEIGHT_COLUMN} is zero at row {row}"
        ));
    }
    if !accepts_full_changelog {
        return Err(
            "weighted sink input requires a FullChangelog sink; append-only or keyed-upsert semantics would lose retractions"
                .into(),
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    fn batch(fields: Vec<Field>, columns: Vec<ArrayRef>) -> RecordBatch {
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
    }

    fn canonical_weighted(weights: Int64Array) -> RecordBatch {
        batch(
            vec![
                Field::new("value", DataType::Utf8, false),
                Field::new(WEIGHT_COLUMN, DataType::Int64, false),
            ],
            vec![
                Arc::new(StringArray::from(vec!["a", "b"])),
                Arc::new(weights),
            ],
        )
    }

    #[test]
    fn plain_rows_are_valid_for_every_sink_mode() {
        let plain = batch(
            vec![Field::new("value", DataType::Utf8, false)],
            vec![Arc::new(StringArray::from(vec!["a"]))],
        );

        validate_sink_input(&plain, false, false).unwrap();
        validate_sink_input(&plain, true, false).unwrap();
        let error = validate_sink_input(&plain, true, true).unwrap_err();
        assert!(error.contains("missing"), "{error}");
    }

    #[test]
    fn canonical_weights_are_preserved_only_for_full_changelog_sinks() {
        let weighted = canonical_weighted(Int64Array::from(vec![1, -2]));

        validate_sink_input(&weighted, true, true).unwrap();
        validate_sink_input(&weighted, true, false).unwrap();
        let error = validate_sink_input(&weighted, false, false).unwrap_err();
        assert!(error.contains("FullChangelog"), "{error}");
        let values = weighted
            .column_by_name(WEIGHT_COLUMN)
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(values.values(), &[1, -2]);
    }

    #[test]
    fn malformed_weight_envelopes_fail_closed() {
        let cases = [
            batch(
                vec![
                    Field::new("__WEIGHT", DataType::Int64, false),
                    Field::new("value", DataType::Utf8, false),
                ],
                vec![
                    Arc::new(Int64Array::from(vec![1])) as ArrayRef,
                    Arc::new(StringArray::from(vec!["a"])) as ArrayRef,
                ],
            ),
            batch(
                vec![
                    Field::new(WEIGHT_COLUMN, DataType::Int64, false),
                    Field::new("value", DataType::Utf8, false),
                ],
                vec![
                    Arc::new(Int64Array::from(vec![1])) as ArrayRef,
                    Arc::new(StringArray::from(vec!["a"])) as ArrayRef,
                ],
            ),
            batch(
                vec![
                    Field::new("value", DataType::Utf8, false),
                    Field::new(WEIGHT_COLUMN, DataType::Utf8, false),
                ],
                vec![
                    Arc::new(StringArray::from(vec!["a"])) as ArrayRef,
                    Arc::new(StringArray::from(vec!["1"])) as ArrayRef,
                ],
            ),
            batch(
                vec![
                    Field::new("value", DataType::Utf8, false),
                    Field::new(WEIGHT_COLUMN, DataType::Int64, true),
                ],
                vec![
                    Arc::new(StringArray::from(vec!["a"])) as ArrayRef,
                    Arc::new(Int64Array::from(vec![1])) as ArrayRef,
                ],
            ),
            batch(
                vec![
                    Field::new("value", DataType::Utf8, false),
                    Field::new(WEIGHT_COLUMN, DataType::Int64, false),
                    Field::new("__WEIGHT", DataType::Int64, false),
                ],
                vec![
                    Arc::new(StringArray::from(vec!["a"])) as ArrayRef,
                    Arc::new(Int64Array::from(vec![1])) as ArrayRef,
                    Arc::new(Int64Array::from(vec![1])) as ArrayRef,
                ],
            ),
        ];

        for malformed in cases {
            assert!(validate_sink_input(&malformed, true, false).is_err());
            assert!(validate_sink_input(&malformed, false, false).is_err());
        }
    }

    #[test]
    fn null_and_zero_weight_values_fail_closed() {
        let nullable = batch(
            vec![
                Field::new("value", DataType::Utf8, false),
                Field::new(WEIGHT_COLUMN, DataType::Int64, true),
            ],
            vec![
                Arc::new(StringArray::from(vec!["a", "b"])),
                Arc::new(Int64Array::from(vec![Some(1), None])),
            ],
        );
        for malformed in [nullable, canonical_weighted(Int64Array::from(vec![1, 0]))] {
            assert!(validate_sink_input(&malformed, true, false).is_err());
            assert!(validate_sink_input(&malformed, false, false).is_err());
        }
    }
}
