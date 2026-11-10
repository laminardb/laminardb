use std::sync::Arc;

use super::*;
use arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, Decimal128Array, FixedSizeBinaryArray, Float64Array,
    Int32Array, Int64Array, IntervalYearMonthArray, StringArray, StructArray, UInt32Array,
};
use arrow::datatypes::{Field, Int32Type, Int64Type, TimeUnit};
use datafusion::functions_aggregate::min_max::{MaxAccumulator, MinAccumulator};
use datafusion_expr::{Accumulator, AggregateUDF, AggregateUDFImpl};

/// Every row of a value array and, in weighted mode, a weight array. Callers check the input
/// against the state first, as `AggregateInput::resolve_all` does.
fn whole(values: &[ArrayRef]) -> (AggregateInput<'_>, Vec<u32>) {
    let weights = values.get(1).map(AsArray::as_primitive::<Int64Type>);
    let rows = (0..u32::try_from(values[0].len()).unwrap()).collect();
    let input = AggregateInput::new(&values[0], None, weights);
    let input = if weights.is_some() {
        input.with_row_encoding().unwrap()
    } else {
        input
    };
    (input, rows)
}

trait WholeArrays {
    fn update_all(&mut self, values: &[ArrayRef]) -> Result<(), DbError>;
    #[cfg(feature = "cluster")]
    fn delta_all(&self, values: &[ArrayRef]) -> Result<Vec<ScalarValue>, DbError>;
}

impl WholeArrays for ConcreteAggregateState {
    fn update_all(&mut self, values: &[ArrayRef]) -> Result<(), DbError> {
        let (input, rows) = whole(values);
        self.check_input(&input)?;
        self.update_rows(&input, &rows)
    }

    #[cfg(feature = "cluster")]
    fn delta_all(&self, values: &[ArrayRef]) -> Result<Vec<ScalarValue>, DbError> {
        let (input, rows) = whole(values);
        self.check_input(&input)?;
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
        captured.checkpoint_state(),
        vec![ScalarValue::Int64(Some(6))]
    );
    assert_eq!(*live.evaluate().unwrap(), ScalarValue::Int64(Some(10)));

    let mut restored =
        ConcreteAggregateState::try_new(&spec, ConcreteInputMode::AppendOnly).unwrap();
    restored
        .merge_checkpoint_state(&captured.checkpoint_state())
        .unwrap();
    assert_eq!(*restored.evaluate().unwrap(), ScalarValue::Int64(Some(6)));
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

    let checkpoint = state.checkpoint_state();
    assert_eq!(
        checkpoint,
        vec![
            ScalarValue::UInt64(Some(2)),
            ScalarValue::Decimal128(Some(19_999_999_998), 38, 2),
        ]
    );
    assert_eq!(
        *state.evaluate().unwrap(),
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
        sum.checkpoint_state(),
        vec![
            ScalarValue::Decimal128(Some(50), 38, 0),
            ScalarValue::Int64(Some(2)),
        ]
    );
    assert_eq!(*sum.evaluate().unwrap(), ScalarValue::Int64(Some(50)));

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
        avg.checkpoint_state(),
        vec![
            ScalarValue::Float64(Some(50.0)),
            ScalarValue::Int64(Some(2)),
        ]
    );
    assert_eq!(*avg.evaluate().unwrap(), ScalarValue::Float64(Some(25.0)));
}

#[test]
fn invalid_arithmetic_is_rejected_without_mutating_state() {
    let sum_spec = spec(Sum::new(), DataType::Int64, DataType::Int64, false);
    let mut append_sum =
        ConcreteAggregateState::try_new(&sum_spec, ConcreteInputMode::AppendOnly).unwrap();
    assert!(append_sum
        .update_all(&[Arc::new(Int64Array::from(vec![i64::MAX, 1]))])
        .is_err());
    assert_eq!(*append_sum.evaluate().unwrap(), ScalarValue::Int64(None));

    let mut weighted_sum =
        ConcreteAggregateState::try_new(&sum_spec, ConcreteInputMode::Weighted).unwrap();
    assert!(weighted_sum
        .update_all(&[
            Arc::new(Int64Array::from(vec![i64::MAX, 1])),
            Arc::new(Int64Array::from(vec![1, 1])),
        ])
        .is_err());
    assert_eq!(*weighted_sum.evaluate().unwrap(), ScalarValue::Int64(None));
    assert!(weighted_sum
        .merge_checkpoint_state(&[
            ScalarValue::Decimal128(Some(i128::from(i64::MAX) + 1), 38, 0),
            ScalarValue::Int64(Some(1)),
        ])
        .is_err());
    assert_eq!(*weighted_sum.evaluate().unwrap(), ScalarValue::Int64(None));

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
        *weighted_count.evaluate().unwrap(),
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
    assert_eq!(
        *weighted_avg.evaluate().unwrap(),
        ScalarValue::Float64(None)
    );
}

#[test]
fn append_sum_adds_each_batch_sum_to_its_total() {
    let int_spec = spec(Sum::new(), DataType::Int64, DataType::Int64, false);
    let ints = |values: Vec<Option<i64>>| -> ArrayRef { Arc::new(Int64Array::from(values)) };
    let mut sum =
        ConcreteAggregateState::try_new(&int_spec, ConcreteInputMode::AppendOnly).unwrap();
    sum.update_all(&[ints(vec![None])]).unwrap();
    assert_eq!(*sum.evaluate().unwrap(), ScalarValue::Int64(None));
    sum.update_all(&[ints(vec![Some(i64::MAX - 1)])]).unwrap();
    // Added row by row this batch would pass i64::MAX; its own sum is zero.
    sum.update_all(&[ints(vec![Some(5), Some(-5)])]).unwrap();
    let error = sum.update_all(&[ints(vec![Some(2)])]).unwrap_err();
    assert!(error.to_string().contains("SUM state overflow"), "{error}");
    assert_eq!(
        *sum.evaluate().unwrap(),
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
        *sum.evaluate().unwrap(),
        ScalarValue::Decimal128(Some(9_999_999_999), 10, 2)
    );
}

#[test]
fn extrema_choose_their_state_by_input_mode() {
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
        min.checkpoint_state(),
        vec![ScalarValue::Utf8(Some("apple".into()))]
    );
    assert!(min.nested_retained_bytes() >= "apple".len());

    assert!(matches!(
        ConcreteAggregateState::try_new(&min_spec, ConcreteInputMode::Weighted).unwrap(),
        ConcreteAggregateState::RetractableMin(_)
    ));
}

#[test]
fn only_values_held_as_emitted_are_lent() {
    let text = || -> ArrayRef { Arc::new(StringArray::from(vec!["pear", "apple"])) };
    let ints = || -> ArrayRef { Arc::new(Int64Array::from(vec![4, 2])) };
    let weights = || -> ArrayRef { Arc::new(Int64Array::from(vec![1, 1])) };
    let cases = [
        (
            spec(Min::new(), DataType::Utf8, DataType::Utf8, false),
            ConcreteInputMode::AppendOnly,
            vec![text()],
            Some(ScalarValue::Utf8(Some("apple".into()))),
        ),
        (
            spec(Max::new(), DataType::Utf8, DataType::Utf8, false),
            ConcreteInputMode::AppendOnly,
            vec![text()],
            Some(ScalarValue::Utf8(Some("pear".into()))),
        ),
        (
            spec(Sum::new(), DataType::Int64, DataType::Int64, false),
            ConcreteInputMode::AppendOnly,
            vec![ints()],
            Some(ScalarValue::Int64(Some(6))),
        ),
        (
            spec(Count::new(), DataType::Int64, DataType::Int64, false),
            ConcreteInputMode::AppendOnly,
            vec![ints()],
            None,
        ),
        (
            spec(Avg::new(), DataType::Float64, DataType::Float64, false),
            ConcreteInputMode::AppendOnly,
            vec![Arc::new(Float64Array::from(vec![4.0, 2.0]))],
            None,
        ),
        (
            spec(Sum::new(), DataType::Int64, DataType::Int64, false),
            ConcreteInputMode::Weighted,
            vec![ints(), weights()],
            None,
        ),
        (
            spec(Min::new(), DataType::Utf8, DataType::Utf8, false),
            ConcreteInputMode::Weighted,
            vec![text(), weights()],
            None,
        ),
    ];
    for (spec, mode, values, lent) in cases {
        let mut state = ConcreteAggregateState::try_new(&spec, mode).unwrap();
        state.update_all(&values).unwrap();
        let name = format!("{} {mode:?}", spec.udf.name());
        match (state.evaluate().unwrap(), lent) {
            (Cow::Borrowed(value), Some(expected)) => assert_eq!(value, &expected, "{name}"),
            (Cow::Owned(_), None) => {}
            (value, _) => panic!("{name} evaluated to {value:?}"),
        }
    }
}

/// A column of each type MIN and MAX compare in place, and three types whose rows they copy, all
/// with NULLs. The floats hold both zeros, NaNs of both signs and both infinities.
fn extremum_columns() -> Vec<ArrayRef> {
    const ROWS: i64 = 40;
    let signed: ArrayRef = Arc::new(Int64Array::from_iter(
        (0..ROWS).map(|i| (i % 7 != 3).then_some((i * 37) % 101 - 50)),
    ));
    let unsigned: ArrayRef = Arc::new(Int64Array::from_iter(
        (0..ROWS).map(|i| (i % 7 != 3).then_some((i * 37) % 101)),
    ));
    let special = [
        0.0,
        -0.0,
        f64::NAN,
        -f64::NAN,
        f64::from_bits(0x7ff8_0000_0000_0001),
        f64::INFINITY,
        f64::NEG_INFINITY,
        1.5,
        -2.5,
    ];
    let floats: ArrayRef = Arc::new(Float64Array::from_iter((0..ROWS).map(|i| {
        (i % 7 != 3).then(|| special[usize::try_from(i * 5).unwrap() % special.len()])
    })));
    let strings: ArrayRef = Arc::new(StringArray::from_iter(
        (0..ROWS).map(|i| (i % 5 != 2).then(|| format!("v{}", (i * 13) % 17))),
    ));
    let cast = |array: &ArrayRef, to: DataType| arrow::compute::cast(array, &to).unwrap();
    let signed32 = cast(&signed, DataType::Int32);

    let mut columns = vec![
        Arc::clone(&signed),
        Arc::clone(&floats),
        Arc::clone(&strings),
        Arc::new(BooleanArray::from_iter(
            (0..ROWS).map(|i| (i % 6 != 1).then_some(i % 3 == 0)),
        )) as ArrayRef,
    ];
    columns.extend(
        [
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Decimal32(9, 2),
            DataType::Decimal64(18, 2),
            DataType::Decimal128(20, 2),
            DataType::Decimal256(40, 2),
            DataType::Date64,
            DataType::Time64(TimeUnit::Microsecond),
            DataType::Time64(TimeUnit::Nanosecond),
            DataType::Timestamp(TimeUnit::Second, None),
            DataType::Timestamp(TimeUnit::Millisecond, Some("+01:00".into())),
            DataType::Timestamp(TimeUnit::Microsecond, None),
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            DataType::Duration(TimeUnit::Second),
            DataType::Duration(TimeUnit::Millisecond),
            DataType::Duration(TimeUnit::Microsecond),
            DataType::Duration(TimeUnit::Nanosecond),
        ]
        .map(|to| cast(&signed, to)),
    );
    columns.extend(
        [
            DataType::Date32,
            DataType::Time32(TimeUnit::Second),
            DataType::Time32(TimeUnit::Millisecond),
        ]
        .map(|to| cast(&signed32, to)),
    );
    columns.extend(
        [
            DataType::UInt8,
            DataType::UInt16,
            DataType::UInt32,
            DataType::UInt64,
        ]
        .map(|to| cast(&unsigned, to)),
    );
    columns.extend([DataType::Float16, DataType::Float32].map(|to| cast(&floats, to)));
    columns.extend(
        [
            DataType::LargeUtf8,
            DataType::Utf8View,
            DataType::Binary,
            DataType::LargeBinary,
            DataType::BinaryView,
        ]
        .map(|to| cast(&strings, to)),
    );
    columns.push(Arc::new(IntervalYearMonthArray::from_iter(
        signed32.as_primitive::<Int32Type>().iter(),
    )));
    columns.push(Arc::new(
        FixedSizeBinaryArray::try_from_sparse_iter_with_size(
            (0..ROWS).map(|i| (i % 4 != 0).then(|| [u8::try_from(i % 5).unwrap(), 9])),
            2,
        )
        .unwrap(),
    ));
    columns.push(Arc::new(StructArray::from(vec![(
        Arc::new(Field::new("a", DataType::Int64, true)),
        signed,
    )])));
    columns
}

fn datafusion_extremum(spec: &AggFuncSpec) -> Box<dyn Accumulator> {
    if spec.udf.name() == "min" {
        Box::new(MinAccumulator::try_new(&spec.return_type).unwrap())
    } else {
        Box::new(MaxAccumulator::try_new(&spec.return_type).unwrap())
    }
}

#[test]
fn extrema_update_and_merge_like_datafusion_on_every_type() {
    for values in extremum_columns() {
        for implementation in [
            Arc::new(Min::new()) as Arc<dyn AggregateUDFImpl>,
            Arc::new(Max::new()),
        ] {
            let data_type = values.data_type().clone();
            let spec = AggFuncSpec {
                udf: Arc::new(AggregateUDF::new_from_shared_impl(implementation)),
                input_types: vec![data_type.clone()],
                input_col_indices: vec![0],
                output_name: "value".into(),
                return_type: data_type.clone(),
                is_count_star: false,
                filter_col_index: None,
            };
            let name = format!("{} {data_type}", spec.udf.name());
            let mut updated =
                ConcreteAggregateState::try_new(&spec, ConcreteInputMode::AppendOnly).unwrap();
            let mut merged = updated.clone();
            let mut expected = datafusion_extremum(&spec);
            for (offset, length) in [(0, 15), (15, 13), (28, 12)] {
                let batch = values.slice(offset, length);
                let filter = BooleanArray::from_iter(
                    (0..length).map(|i| (i % 6 != 0).then_some(i % 6 != 5)),
                );
                let rows: Vec<u32> = (0..u32::try_from(length).unwrap())
                    .filter(|row| row % 4 != 1)
                    .collect();
                let input = AggregateInput::new(&batch, Some(&filter), None);
                let admitted = input.take_selected(&rows).unwrap();
                updated.update_rows(&input, &rows).unwrap();
                expected.update_batch(&[Arc::clone(&admitted)]).unwrap();
                let mut delta = datafusion_extremum(&spec);
                delta.update_batch(&[admitted]).unwrap();
                merged
                    .merge_checkpoint_state(&delta.state().unwrap())
                    .unwrap();

                let expected_state = expected.state().unwrap();
                assert_eq!(updated.checkpoint_state(), expected_state, "{name}");
                assert_eq!(merged.checkpoint_state(), expected_state, "{name} merged");
                assert_eq!(
                    updated.nested_retained_bytes(),
                    expected.size() - std::mem::size_of::<ScalarValue>(),
                    "{name} retained bytes"
                );
            }
        }
    }
}

#[test]
fn extremum_rejects_another_type_without_mutating() {
    let spec = spec(Min::new(), DataType::Int64, DataType::Int64, false);
    let mut min = ConcreteAggregateState::try_new(&spec, ConcreteInputMode::AppendOnly).unwrap();
    let text = [ScalarValue::Utf8(Some("1".into()))];
    assert!(min.merge_checkpoint_state(&text).is_err());

    min.update_all(&[Arc::new(Int64Array::from(vec![4]))])
        .unwrap();
    assert!(min.merge_checkpoint_state(&text).is_err());
    assert!(min
        .merge_checkpoint_state(&[ScalarValue::Int32(Some(1))])
        .is_err());
    assert!(min
        .update_all(&[Arc::new(Int32Array::from(vec![1]))])
        .is_err());
    assert_eq!(min.checkpoint_state(), vec![ScalarValue::Int64(Some(4))]);
}

#[test]
fn inputs_are_type_checked_once_per_batch_against_each_template() {
    use arrow::record_batch::RecordBatch;
    use ConcreteInputMode::{AppendOnly, Weighted};

    let batch =
        RecordBatch::try_from_iter([("value", Arc::new(Int32Array::from(vec![1])) as ArrayRef)])
            .unwrap();
    let weights = Int64Array::from(vec![1]);
    let resolve = |spec: &AggFuncSpec, mode| {
        let template = ConcreteAggregateState::try_new(spec, mode).unwrap();
        let weights = (mode == Weighted).then_some(&weights);
        AggregateInput::resolve_all(&batch, std::slice::from_ref(spec), &[template], weights)
            .map(|_| ())
    };
    // COUNT reads only validity, so it takes a column of any type.
    let count = spec(Count::new(), DataType::Int64, DataType::Int64, false);
    resolve(&count, AppendOnly).unwrap();
    let sum = spec(Sum::new(), DataType::Int64, DataType::Int64, false);
    let avg = spec(Avg::new(), DataType::Float64, DataType::Float64, false);
    let min = spec(Min::new(), DataType::Int64, DataType::Int64, false);
    let max = spec(Max::new(), DataType::Int64, DataType::Int64, false);
    for (spec, mode, message) in [
        (&sum, AppendOnly, "SUM expected input Int64, got Int32"),
        (
            &sum,
            Weighted,
            "weighted SUM expected input Int64, got Int32",
        ),
        (&avg, AppendOnly, "AVG expected input Float64, got Int32"),
        (
            &avg,
            Weighted,
            "weighted AVG expected input Float64, got Int32",
        ),
        (&min, AppendOnly, "MIN expected input Int64, got Int32"),
        (&max, Weighted, "MAX expected input Int64, got Int32"),
    ] {
        let error = resolve(spec, mode).unwrap_err();
        assert!(error.to_string().contains(message), "{error}");
    }
    let error = AggregateInput::resolve_all(&batch, &[sum], &[], None)
        .map(|_| ())
        .unwrap_err();
    assert!(error.to_string().contains("template arity"), "{error}");
}

#[test]
fn unchecked_inputs_of_another_type_fail_without_mutating() {
    use ConcreteInputMode::{AppendOnly, Weighted};

    let values: ArrayRef = Arc::new(Int32Array::from(vec![1]));
    let weights = Int64Array::from(vec![1]);
    let int_sum = spec(Sum::new(), DataType::Int64, DataType::Int64, false);
    let float_sum = spec(Sum::new(), DataType::Float64, DataType::Float64, false);
    let avg = spec(Avg::new(), DataType::Float64, DataType::Float64, false);
    let min = spec(Min::new(), DataType::Int64, DataType::Int64, false);
    for (spec, mode) in [
        (&int_sum, AppendOnly),
        (&int_sum, Weighted),
        (&float_sum, Weighted),
        (&avg, AppendOnly),
        (&avg, Weighted),
        (&min, AppendOnly),
    ] {
        let mut state = ConcreteAggregateState::try_new(spec, mode).unwrap();
        let before = state.checkpoint_state();
        let input = AggregateInput::new(&values, None, (mode == Weighted).then_some(&weights));
        assert!(state.update_rows(&input, &[0]).is_err());
        assert_eq!(state.checkpoint_state(), before);
    }
}

#[test]
fn extremum_state_does_not_widen_concrete_state() {
    assert!(std::mem::size_of::<ExtremumState>() <= std::mem::size_of::<SumState>());
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
        (
            spec(Min::new(), DataType::Utf8, DataType::Utf8, false),
            AppendOnly,
            vec![
                vec![Arc::new(StringArray::from(vec![Some("pear"), None])) as ArrayRef],
                vec![Arc::new(StringArray::from(vec![Some("fig"), Some("plum")]))],
            ],
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
            merged.checkpoint_state(),
            updated.checkpoint_state(),
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
            in_place.checkpoint_state(),
            copied.checkpoint_state(),
            "{} weighted={weighted}",
            spec.udf.name()
        );
    }
}

fn extremum_spec(implementation: Arc<dyn AggregateUDFImpl>, data_type: &DataType) -> AggFuncSpec {
    AggFuncSpec {
        udf: Arc::new(AggregateUDF::new_from_shared_impl(implementation)),
        input_types: vec![data_type.clone()],
        input_col_indices: vec![0],
        output_name: "value".into(),
        return_type: data_type.clone(),
        is_count_star: false,
        filter_col_index: None,
    }
}

/// Each step's weight for row `i`; the net weight of a row never goes below zero.
fn changelog_steps(rows: usize) -> Vec<Vec<i64>> {
    let step = |weight: fn(usize) -> i64| (0..rows).map(weight).collect::<Vec<_>>();
    vec![
        step(|_| 1),
        step(|i| if i % 3 == 0 { -1 } else { 0 }),
        step(|i| match (i % 6, i % 4) {
            (0, _) => 2,
            (_, 1) if i % 3 != 0 => -1,
            _ => 0,
        }),
        step(|i| if i % 6 == 0 { -1 } else { 0 }),
        step(|i| if i % 6 == 0 { -1 } else { 0 }),
    ]
}

#[test]
fn changelog_extrema_match_datafusion_over_net_values_on_every_admitted_type() {
    for values in extremum_columns() {
        let data_type = values.data_type().clone();
        for implementation in [
            Arc::new(Min::new()) as Arc<dyn AggregateUDFImpl>,
            Arc::new(Max::new()),
        ] {
            let spec = extremum_spec(implementation, &data_type);
            let name = format!("{} {data_type}", spec.udf.name());
            let Ok(mut state) = ConcreteAggregateState::try_new(&spec, ConcreteInputMode::Weighted)
            else {
                assert!(
                    matches!(data_type, DataType::Interval(_) | DataType::Struct(_)),
                    "{name} must be admitted"
                );
                continue;
            };
            let template = state.clone();
            let mut net = vec![0_i64; values.len()];
            for weights in changelog_steps(values.len()) {
                let weights: ArrayRef = Arc::new(Int64Array::from(weights));
                let rows: Vec<u32> = (0..u32::try_from(values.len()).unwrap())
                    .filter(|&row| weights.as_primitive::<Int64Type>().value(row as usize) != 0)
                    .collect();
                let input =
                    AggregateInput::new(&values, None, Some(weights.as_primitive::<Int64Type>()))
                        .with_row_encoding()
                        .unwrap();
                state.update_rows(&input, &rows).unwrap();
                for (net, weight) in net.iter_mut().zip(weights.as_primitive::<Int64Type>()) {
                    *net += weight.unwrap();
                }

                let held = UInt32Array::from_iter_values(
                    (0..u32::try_from(values.len()).unwrap()).filter(|&row| net[row as usize] > 0),
                );
                let mut expected = datafusion_extremum(&spec);
                expected
                    .update_batch(&[arrow::compute::take(&values, &held, None).unwrap()])
                    .unwrap();
                let expected = expected.evaluate().unwrap();
                assert_eq!(*state.evaluate().unwrap(), expected, "{name}");

                let mut restored = template.clone();
                restored
                    .merge_checkpoint_state(&state.checkpoint_state())
                    .unwrap();
                assert_eq!(*restored.evaluate().unwrap(), expected, "{name} restored");
                assert_eq!(
                    restored.checkpoint_state(),
                    state.checkpoint_state(),
                    "{name} restored checkpoint"
                );
                assert_eq!(
                    restored.nested_retained_bytes(),
                    state.nested_retained_bytes(),
                    "{name} retained bytes"
                );
            }
            let weights = Int64Array::from_iter_values(net.iter().map(|net| -net));
            let rows: Vec<u32> = (0..u32::try_from(values.len()).unwrap()).collect();
            state
                .update_rows(
                    &AggregateInput::new(&values, None, Some(&weights))
                        .with_row_encoding()
                        .unwrap(),
                    &rows,
                )
                .unwrap();
            assert!(state.evaluate().unwrap().is_null(), "{name} drained");
            assert_eq!(state.nested_retained_bytes(), 0, "{name} drained bytes");
        }
    }
}

fn weighted_max() -> ConcreteAggregateState {
    let spec = spec(Max::new(), DataType::Int64, DataType::Int64, false);
    ConcreteAggregateState::try_new(&spec, ConcreteInputMode::Weighted).unwrap()
}

fn apply(
    state: &mut ConcreteAggregateState,
    values: Vec<i64>,
    weights: Vec<i64>,
) -> Result<(), DbError> {
    state.update_all(&[
        Arc::new(Int64Array::from(values)),
        Arc::new(Int64Array::from(weights)),
    ])
}

#[test]
fn changelog_extremum_errors_leave_the_state_as_it_was() {
    let mut max = weighted_max();
    apply(&mut max, vec![4, 7, 7], vec![1, 1, 1]).unwrap();
    let before = max.checkpoint_state();

    let error = apply(&mut max, vec![9, 7, 7, 5], vec![1, -1, -2, -1]).unwrap_err();
    assert!(error.to_string().contains("negative"), "{error}");
    assert_eq!(max.checkpoint_state(), before);
    let error = apply(&mut max, vec![9, 5], vec![1, -1]).unwrap_err();
    assert!(error.to_string().contains("does not hold"), "{error}");
    assert_eq!(max.checkpoint_state(), before);
    let error = apply(&mut max, vec![6, 6, 1], vec![1, -1, -5]).unwrap_err();
    assert!(error.to_string().contains("does not hold"), "{error}");
    assert_eq!(max.checkpoint_state(), before);
    let error = apply(&mut max, vec![1, 4], vec![1, i64::MAX]).unwrap_err();
    assert!(error.to_string().contains("overflow"), "{error}");
    assert_eq!(max.checkpoint_state(), before);
    assert!(max
        .update_all(&[
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![1])),
        ])
        .is_err());
    assert_eq!(*max.evaluate().unwrap(), ScalarValue::Int64(Some(7)));

    apply(&mut max, vec![7, 7], vec![-1, -1]).unwrap();
    assert_eq!(*max.evaluate().unwrap(), ScalarValue::Int64(Some(4)));
    apply(&mut max, vec![4], vec![-1]).unwrap();
    assert_eq!(*max.evaluate().unwrap(), ScalarValue::Int64(None));
}

#[test]
fn changelog_extremum_rejects_malformed_checkpoints_without_mutating() {
    let mut source = weighted_max();
    apply(&mut source, vec![2, 5], vec![3, 1]).unwrap();
    let [ScalarValue::Binary(Some(valid))] = &source.checkpoint_state()[..] else {
        panic!("a changelog MAX checkpoints one Binary");
    };
    // format, count, two weights, buffer count, one 16-byte buffer of the values 2 and 5
    assert_eq!(valid.len(), 1 + 4 + 16 + 4 + 8 + 16);
    let swapped = {
        let mut bytes = valid.clone();
        bytes[33..41].copy_from_slice(&5_i64.to_le_bytes());
        bytes[41..49].copy_from_slice(&2_i64.to_le_bytes());
        bytes
    };
    let with = |offset: usize, replacement: &[u8]| {
        let mut bytes = valid.clone();
        bytes[offset..offset + replacement.len()].copy_from_slice(replacement);
        bytes
    };
    let mut malformed = vec![
        with(0, &[2]),
        with(1, &u32::MAX.to_le_bytes()),
        with(5, &0_i64.to_le_bytes()),
        with(13, &(-1_i64).to_le_bytes()),
        with(21, &2_u32.to_le_bytes()),
        with(21, &u32::MAX.to_le_bytes()),
        with(25, &8_u64.to_le_bytes()),
        swapped,
        [valid.as_slice(), &[0]].concat(),
    ];
    malformed.extend((0..valid.len()).map(|length| valid[..length].to_vec()));

    let mut target = weighted_max();
    apply(&mut target, vec![2], vec![1]).unwrap();
    let before = target.checkpoint_state();
    for bytes in malformed {
        assert!(
            target
                .merge_checkpoint_state(&[ScalarValue::Binary(Some(bytes.clone()))])
                .is_err(),
            "{bytes:?}"
        );
        assert_eq!(target.checkpoint_state(), before);
    }
    for state in [
        vec![ScalarValue::Binary(None)],
        vec![ScalarValue::Int64(Some(2))],
        vec![],
    ] {
        assert!(target.merge_checkpoint_state(&state).is_err());
    }
    assert_eq!(target.checkpoint_state(), before);

    target
        .merge_checkpoint_state(&[ScalarValue::Binary(Some(valid.clone()))])
        .unwrap();
    assert_eq!(*target.evaluate().unwrap(), ScalarValue::Int64(Some(5)));
    apply(&mut target, vec![5, 2, 2, 2], vec![-1, -1, -1, -1]).unwrap();
    assert_eq!(*target.evaluate().unwrap(), ScalarValue::Int64(Some(2)));
}

#[test]
fn changelog_extremum_rejects_a_checkpoint_of_invalid_utf8() {
    let spec = spec(Max::new(), DataType::Utf8, DataType::Utf8, false);
    let mut source = ConcreteAggregateState::try_new(&spec, ConcreteInputMode::Weighted).unwrap();
    source
        .update_all(&[
            Arc::new(StringArray::from(vec!["ab"])),
            Arc::new(Int64Array::from(vec![1])),
        ])
        .unwrap();
    let [ScalarValue::Binary(Some(valid))] = &source.checkpoint_state()[..] else {
        panic!("a changelog MAX checkpoints one Binary");
    };
    let mut invalid = valid.clone();
    *invalid.last_mut().unwrap() = 0xFF;
    let mut target = ConcreteAggregateState::try_new(&spec, ConcreteInputMode::Weighted).unwrap();
    let error = target
        .merge_checkpoint_state(&[ScalarValue::Binary(Some(invalid))])
        .unwrap_err();
    assert!(error.to_string().contains("values are invalid"), "{error}");
    target
        .merge_checkpoint_state(&[ScalarValue::Binary(Some(valid.clone()))])
        .unwrap();
    assert_eq!(
        *target.evaluate().unwrap(),
        ScalarValue::Utf8(Some("ab".into()))
    );
}

#[test]
fn changelog_extremum_state_does_not_widen_concrete_state() {
    assert!(
        std::mem::size_of::<retractable::RetractableExtremumState>()
            <= std::mem::size_of::<SumState>()
    );
}
