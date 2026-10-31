use std::sync::Arc;

use super::*;
use arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, Decimal128Array, Float64Array, Int64Array, StringArray,
};
use arrow::datatypes::Int64Type;
use datafusion_expr::{AggregateUDF, AggregateUDFImpl};

/// Every row of a value array and, in weighted mode, a weight array.
fn whole(values: &[ArrayRef]) -> (AggregateInput<'_>, Vec<u32>) {
    let weights = values.get(1).map(AsArray::as_primitive::<Int64Type>);
    let rows = (0..u32::try_from(values[0].len()).unwrap()).collect();
    (AggregateInput::new(&values[0], None, weights), rows)
}

trait WholeArrays {
    fn update_all(&mut self, values: &[ArrayRef]) -> Result<(), DbError>;
    #[cfg(feature = "cluster")]
    fn delta_all(&self, values: &[ArrayRef]) -> Result<Vec<ScalarValue>, DbError>;
}

impl WholeArrays for ConcreteAggregateState {
    fn update_all(&mut self, values: &[ArrayRef]) -> Result<(), DbError> {
        let (input, rows) = whole(values);
        self.update_rows(&input, &rows)
    }

    #[cfg(feature = "cluster")]
    fn delta_all(&self, values: &[ArrayRef]) -> Result<Vec<ScalarValue>, DbError> {
        let (input, rows) = whole(values);
        self.batch_delta(&input, &rows)
    }
}

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
    live.update_all(&[Arc::new(Int64Array::from(vec![1, 2, 3]))])
        .unwrap();
    let captured = live.clone();

    live.update_all(&[Arc::new(Int64Array::from(vec![4]))])
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
    state.update_all(&[Arc::new(values)]).unwrap();

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
    sum.update_all(&[
        Arc::new(Int64Array::from(vec![10, 20])),
        Arc::new(Int64Array::from(vec![1, 1])),
    ])
    .unwrap();
    sum.update_all(&[
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
    avg.update_all(&[
        Arc::new(Float64Array::from(vec![10.0, 20.0])),
        Arc::new(Int64Array::from(vec![1, 1])),
    ])
    .unwrap();
    avg.update_all(&[
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
        .update_all(&[Arc::new(Int64Array::from(vec![i64::MAX, 1]))])
        .is_err());
    assert_eq!(append_sum.evaluate().unwrap(), ScalarValue::Int64(None));

    let mut weighted_sum =
        ConcreteAggregateState::try_new(&sum_spec, ConcreteInputMode::Weighted).unwrap();
    assert!(weighted_sum
        .update_all(&[
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
        .update_all(&[
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
        .update_all(&[
            Arc::new(Float64Array::from(vec![f64::INFINITY])),
            Arc::new(Int64Array::from(vec![1])),
        ])
        .is_err());
    assert_eq!(weighted_avg.evaluate().unwrap(), ScalarValue::Float64(None));
}

#[test]
fn append_sum_adds_each_batch_sum_to_its_total() {
    let int_spec = spec(Sum::new(), DataType::Int64, DataType::Int64, false);
    let ints = |values: Vec<Option<i64>>| -> ArrayRef { Arc::new(Int64Array::from(values)) };
    let mut sum =
        ConcreteAggregateState::try_new(&int_spec, ConcreteInputMode::AppendOnly).unwrap();
    sum.update_all(&[ints(vec![None])]).unwrap();
    assert_eq!(sum.evaluate().unwrap(), ScalarValue::Int64(None));
    sum.update_all(&[ints(vec![Some(i64::MAX - 1)])]).unwrap();
    // Added row by row this batch would pass i64::MAX; its own sum is zero.
    sum.update_all(&[ints(vec![Some(5), Some(-5)])]).unwrap();
    let error = sum.update_all(&[ints(vec![Some(2)])]).unwrap_err();
    assert!(error.to_string().contains("SUM state overflow"), "{error}");
    assert_eq!(
        sum.evaluate().unwrap(),
        ScalarValue::Int64(Some(i64::MAX - 1))
    );

    let decimal = DataType::Decimal128(10, 2);
    let decimal_spec = spec(Sum::new(), decimal.clone(), decimal, false);
    let decimals = |values: Vec<i128>| -> ArrayRef {
        Arc::new(
            Decimal128Array::from(values)
                .with_precision_and_scale(10, 2)
                .unwrap(),
        )
    };
    let mut sum =
        ConcreteAggregateState::try_new(&decimal_spec, ConcreteInputMode::AppendOnly).unwrap();
    let error = sum
        .update_all(&[decimals(vec![9_999_999_999, 1])])
        .unwrap_err();
    assert!(error.to_string().contains("SUM batch precision"), "{error}");
    sum.update_all(&[decimals(vec![9_999_999_999])]).unwrap();
    let error = sum.update_all(&[decimals(vec![1])]).unwrap_err();
    assert!(error.to_string().contains("SUM state precision"), "{error}");
    assert_eq!(
        sum.evaluate().unwrap(),
        ScalarValue::Decimal128(Some(9_999_999_999), 10, 2)
    );
}

#[test]
fn extrema_are_append_only_and_use_datafusion_ordering() {
    let min_spec = spec(Min::new(), DataType::Utf8, DataType::Utf8, false);
    let mut min =
        ConcreteAggregateState::try_new(&min_spec, ConcreteInputMode::AppendOnly).unwrap();
    min.update_all(&[Arc::new(StringArray::from(vec![
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

#[cfg(feature = "cluster")]
#[test]
fn batch_delta_merges_like_update_rows() {
    use ConcreteInputMode::{AppendOnly, Weighted};

    let ints = |values: &[Option<i64>]| -> ArrayRef { Arc::new(Int64Array::from(values.to_vec())) };
    let floats =
        |values: &[Option<f64>]| -> ArrayRef { Arc::new(Float64Array::from(values.to_vec())) };
    let weights = |weights: &[i64]| -> ArrayRef { Arc::new(Int64Array::from(weights.to_vec())) };
    let decimals = |values: &[Option<i128>]| -> ArrayRef {
        Arc::new(
            Decimal128Array::from(values.to_vec())
                .with_precision_and_scale(10, 2)
                .unwrap(),
        )
    };
    let int64 = DataType::Int64;
    let float64 = DataType::Float64;
    let cases = [
        (
            spec(Count::new(), int64.clone(), int64.clone(), false),
            AppendOnly,
            vec![vec![ints(&[Some(1), None])], vec![ints(&[None])]],
        ),
        (
            spec(Count::new(), int64.clone(), int64.clone(), true),
            Weighted,
            vec![
                vec![ints(&[Some(1), None]), weights(&[1, 1])],
                vec![ints(&[None]), weights(&[-1])],
            ],
        ),
        (
            spec(Sum::new(), int64.clone(), int64.clone(), false),
            AppendOnly,
            vec![vec![ints(&[Some(5), Some(-9)])], vec![ints(&[None])]],
        ),
        (
            spec(Sum::new(), int64.clone(), int64.clone(), false),
            Weighted,
            vec![
                vec![ints(&[Some(10), Some(20)]), weights(&[1, 1])],
                vec![ints(&[Some(20)]), weights(&[-1])],
            ],
        ),
        (
            spec(Sum::new(), float64.clone(), float64.clone(), false),
            Weighted,
            vec![
                vec![floats(&[Some(0.5), None]), weights(&[2, 1])],
                vec![floats(&[Some(0.5)]), weights(&[-1])],
            ],
        ),
        (
            spec(
                Avg::new(),
                DataType::Decimal128(10, 2),
                DataType::Decimal128(14, 6),
                false,
            ),
            AppendOnly,
            vec![
                vec![decimals(&[Some(150), None])],
                vec![decimals(&[Some(-25)])],
            ],
        ),
        (
            spec(Avg::new(), float64.clone(), float64.clone(), false),
            Weighted,
            vec![
                vec![floats(&[Some(1.0), Some(3.0)]), weights(&[1, 1])],
                vec![floats(&[Some(3.0)]), weights(&[-1])],
            ],
        ),
        (
            spec(Min::new(), float64.clone(), float64.clone(), false),
            AppendOnly,
            vec![
                vec![floats(&[Some(2.0), None])],
                vec![floats(&[Some(-1.0)])],
            ],
        ),
        (
            spec(Max::new(), int64.clone(), int64, false),
            AppendOnly,
            vec![vec![ints(&[Some(2), None])], vec![ints(&[None])]],
        ),
    ];
    for (spec, mode, batches) in cases {
        let template = ConcreteAggregateState::try_new(&spec, mode).unwrap();
        let mut updated = template.clone();
        let mut merged = template.clone();
        for values in &batches {
            updated.update_all(values).unwrap();
            merged
                .merge_checkpoint_state(&template.delta_all(values).unwrap())
                .unwrap();
        }
        assert_eq!(
            merged.checkpoint_state().unwrap(),
            updated.checkpoint_state().unwrap(),
            "{} {mode:?}",
            spec.udf.name()
        );
    }
}

#[test]
fn row_updates_read_only_admitted_rows_in_place() {
    let values: ArrayRef = Arc::new(Float64Array::from(vec![
        Some(1.5),
        Some(100.0),
        None,
        Some(2.25),
        Some(-4.0),
    ]));
    let weights = Int64Array::from(vec![2, 9, 1, -1, 3]);
    let filter = BooleanArray::from(vec![Some(true), Some(true), Some(true), None, Some(true)]);
    let rows = [0, 2, 3, 4];
    let admitted: ArrayRef = Arc::new(Float64Array::from(vec![Some(1.5), None, Some(-4.0)]));
    let admitted_weights: ArrayRef = Arc::new(Int64Array::from(vec![2, 1, 3]));

    for (implementation, weighted) in [
        (Arc::new(Count::new()) as Arc<dyn AggregateUDFImpl>, false),
        (Arc::new(Count::new()), true),
        (Arc::new(Sum::new()), false),
        (Arc::new(Sum::new()), true),
        (Arc::new(Avg::new()), false),
        (Arc::new(Avg::new()), true),
        (Arc::new(Min::new()), false),
        (Arc::new(Max::new()), false),
    ] {
        let return_type = if implementation.name() == "count" {
            DataType::Int64
        } else {
            DataType::Float64
        };
        let spec = AggFuncSpec {
            udf: Arc::new(AggregateUDF::new_from_shared_impl(implementation)),
            input_types: vec![DataType::Float64],
            input_col_indices: vec![0],
            output_name: "value".into(),
            return_type,
            is_count_star: false,
            filter_col_index: None,
        };
        let mode = if weighted {
            ConcreteInputMode::Weighted
        } else {
            ConcreteInputMode::AppendOnly
        };
        let mut in_place = ConcreteAggregateState::try_new(&spec, mode).unwrap();
        let weights = weighted.then_some(&weights);
        in_place
            .update_rows(&AggregateInput::new(&values, Some(&filter), weights), &rows)
            .unwrap();

        let mut copied = ConcreteAggregateState::try_new(&spec, mode).unwrap();
        let copied_values = if weighted {
            vec![Arc::clone(&admitted), Arc::clone(&admitted_weights)]
        } else {
            vec![Arc::clone(&admitted)]
        };
        copied.update_all(&copied_values).unwrap();
        assert_eq!(
            in_place.checkpoint_state().unwrap(),
            copied.checkpoint_state().unwrap(),
            "{} weighted={weighted}",
            spec.udf.name()
        );
    }
}
