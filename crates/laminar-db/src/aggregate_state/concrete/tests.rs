use std::sync::Arc;

use super::*;
use arrow::array::{Decimal128Array, Float64Array, Int64Array, StringArray};
use datafusion_expr::{AggregateUDF, AggregateUDFImpl};

fn spec<T: AggregateUDFImpl + 'static>(
    implementation: T,
    input_type: DataType,
    return_type: DataType,
    count_star: bool,
) -> AggFuncSpec {
    AggFuncSpec {
        udf: Arc::new(AggregateUDF::new_from_impl(implementation)),
        input_types: vec![input_type],
        input_col_indices: vec![0],
        output_name: "value".into(),
        return_type,
        is_count_star: count_star,
        filter_col_index: None,
    }
}

#[test]
fn cloned_append_sum_is_an_exact_cut() {
    let spec = spec(Sum::new(), DataType::Int64, DataType::Int64, false);
    let mut live = ConcreteAggregateState::try_new(&spec, ConcreteInputMode::AppendOnly).unwrap();
    live.update_batch(&[Arc::new(Int64Array::from(vec![1, 2, 3]))])
        .unwrap();
    let captured = live.clone();

    live.update_batch(&[Arc::new(Int64Array::from(vec![4]))])
        .unwrap();

    assert_eq!(
        captured.checkpoint_state().unwrap(),
        vec![ScalarValue::Int64(Some(6))]
    );
    assert_eq!(live.evaluate().unwrap(), ScalarValue::Int64(Some(10)));

    let mut restored =
        ConcreteAggregateState::try_new(&spec, ConcreteInputMode::AppendOnly).unwrap();
    restored
        .merge_checkpoint_state(&captured.checkpoint_state().unwrap())
        .unwrap();
    assert_eq!(restored.evaluate().unwrap(), ScalarValue::Int64(Some(6)));
}

#[test]
fn decimal_avg_widens_running_sum_and_preserves_scale() {
    let spec = spec(
        Avg::new(),
        DataType::Decimal128(10, 2),
        DataType::Decimal128(14, 6),
        false,
    );
    let mut state = ConcreteAggregateState::try_new(&spec, ConcreteInputMode::AppendOnly).unwrap();
    let values = Decimal128Array::from(vec![Some(9_999_999_999), Some(9_999_999_999), None])
        .with_precision_and_scale(10, 2)
        .unwrap();
    state.update_batch(&[Arc::new(values)]).unwrap();

    let checkpoint = state.checkpoint_state().unwrap();
    assert_eq!(
        checkpoint,
        vec![
            ScalarValue::UInt64(Some(2)),
            ScalarValue::Decimal128(Some(19_999_999_998), 38, 2),
        ]
    );
    assert_eq!(
        state.evaluate().unwrap(),
        ScalarValue::Decimal128(Some(99_999_999_990_000), 14, 6)
    );

    let mut restored =
        ConcreteAggregateState::try_new(&spec, ConcreteInputMode::AppendOnly).unwrap();
    restored.merge_checkpoint_state(&checkpoint).unwrap();
    assert_eq!(restored.evaluate().unwrap(), state.evaluate().unwrap());
}

#[test]
fn weighted_arithmetic_uses_existing_checkpoint_shapes() {
    let sum_spec = spec(Sum::new(), DataType::Int64, DataType::Int64, false);
    let mut sum = ConcreteAggregateState::try_new(&sum_spec, ConcreteInputMode::Weighted).unwrap();
    sum.update_batch(&[
        Arc::new(Int64Array::from(vec![10, 20])),
        Arc::new(Int64Array::from(vec![1, 1])),
    ])
    .unwrap();
    sum.update_batch(&[
        Arc::new(Int64Array::from(vec![10, 30])),
        Arc::new(Int64Array::from(vec![-1, 1])),
    ])
    .unwrap();
    assert_eq!(
        sum.checkpoint_state().unwrap(),
        vec![
            ScalarValue::Decimal128(Some(50), 38, 0),
            ScalarValue::Int64(Some(2)),
        ]
    );
    assert_eq!(sum.evaluate().unwrap(), ScalarValue::Int64(Some(50)));

    let avg_spec = spec(Avg::new(), DataType::Float64, DataType::Float64, false);
    let mut avg = ConcreteAggregateState::try_new(&avg_spec, ConcreteInputMode::Weighted).unwrap();
    avg.update_batch(&[
        Arc::new(Float64Array::from(vec![10.0, 20.0])),
        Arc::new(Int64Array::from(vec![1, 1])),
    ])
    .unwrap();
    avg.update_batch(&[
        Arc::new(Float64Array::from(vec![10.0, 30.0])),
        Arc::new(Int64Array::from(vec![-1, 1])),
    ])
    .unwrap();
    assert_eq!(
        avg.checkpoint_state().unwrap(),
        vec![
            ScalarValue::Float64(Some(50.0)),
            ScalarValue::Int64(Some(2)),
        ]
    );
    assert_eq!(avg.evaluate().unwrap(), ScalarValue::Float64(Some(25.0)));
}

#[test]
fn invalid_arithmetic_is_rejected_without_mutating_state() {
    let sum_spec = spec(Sum::new(), DataType::Int64, DataType::Int64, false);
    let mut append_sum =
        ConcreteAggregateState::try_new(&sum_spec, ConcreteInputMode::AppendOnly).unwrap();
    assert!(append_sum
        .update_batch(&[Arc::new(Int64Array::from(vec![i64::MAX, 1]))])
        .is_err());
    assert_eq!(append_sum.evaluate().unwrap(), ScalarValue::Int64(None));

    let mut weighted_sum =
        ConcreteAggregateState::try_new(&sum_spec, ConcreteInputMode::Weighted).unwrap();
    assert!(weighted_sum
        .update_batch(&[
            Arc::new(Int64Array::from(vec![i64::MAX, 1])),
            Arc::new(Int64Array::from(vec![1, 1])),
        ])
        .is_err());
    assert_eq!(weighted_sum.evaluate().unwrap(), ScalarValue::Int64(None));
    assert!(weighted_sum
        .merge_checkpoint_state(&[
            ScalarValue::Decimal128(Some(i128::from(i64::MAX) + 1), 38, 0),
            ScalarValue::Int64(Some(1)),
        ])
        .is_err());
    assert_eq!(weighted_sum.evaluate().unwrap(), ScalarValue::Int64(None));

    let count_spec = spec(Count::new(), DataType::Int64, DataType::Int64, true);
    let mut weighted_count =
        ConcreteAggregateState::try_new(&count_spec, ConcreteInputMode::Weighted).unwrap();
    assert!(weighted_count
        .update_batch(&[
            Arc::new(Int64Array::from(vec![0])),
            Arc::new(Int64Array::from(vec![-1])),
        ])
        .is_err());
    assert_eq!(
        weighted_count.evaluate().unwrap(),
        ScalarValue::Int64(Some(0))
    );

    let avg_spec = spec(Avg::new(), DataType::Float64, DataType::Float64, false);
    let mut weighted_avg =
        ConcreteAggregateState::try_new(&avg_spec, ConcreteInputMode::Weighted).unwrap();
    assert!(weighted_avg
        .update_batch(&[
            Arc::new(Float64Array::from(vec![f64::INFINITY])),
            Arc::new(Int64Array::from(vec![1])),
        ])
        .is_err());
    assert_eq!(weighted_avg.evaluate().unwrap(), ScalarValue::Float64(None));
}

#[test]
fn extrema_are_append_only_and_use_datafusion_ordering() {
    let min_spec = spec(Min::new(), DataType::Utf8, DataType::Utf8, false);
    let mut min =
        ConcreteAggregateState::try_new(&min_spec, ConcreteInputMode::AppendOnly).unwrap();
    min.update_batch(&[Arc::new(StringArray::from(vec![
        Some("pear"),
        None,
        Some("apple"),
    ]))])
    .unwrap();
    assert_eq!(
        min.checkpoint_state().unwrap(),
        vec![ScalarValue::Utf8(Some("apple".into()))]
    );
    assert!(min.nested_retained_bytes() >= "apple".len());

    let error =
        ConcreteAggregateState::try_new(&min_spec, ConcreteInputMode::Weighted).unwrap_err();
    assert!(error.to_string().contains("append-only"));
}
