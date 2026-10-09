use std::sync::Arc;

use super::*;

#[test]
fn mappings_match_parameter_rust_types() {
    assert_eq!(postgres_type(&DataType::UInt16).unwrap().sql(), "int4");
    assert_eq!(postgres_type(&DataType::UInt32).unwrap().sql(), "int8");
    assert_eq!(
        arrow_to_pg_ddl_type(&DataType::Float64).unwrap(),
        "DOUBLE PRECISION"
    );
    assert_eq!(
        arrow_type_to_pg_array_cast(&DataType::Timestamp(TimeUnit::Microsecond, None), 2).unwrap(),
        "$2::timestamp[]"
    );
}

#[test]
fn unsupported_types_never_fall_back_to_text() {
    let list = DataType::List(Arc::new(arrow_schema::Field::new(
        "item",
        DataType::Int32,
        true,
    )));
    assert!(postgres_type(&list).is_err());
    assert!(postgres_type(&DataType::FixedSizeBinary(16)).is_err());
    assert!(postgres_type(&DataType::Timestamp(TimeUnit::Nanosecond, None)).is_err());
}

#[cfg(feature = "postgres-sink")]
#[test]
fn uint64_above_bigint_range_is_an_error() {
    let values = arrow_array::UInt64Array::from(vec![u64::MAX]);
    let error = validate_postgres_array_values(&values).unwrap_err();
    assert!(!error.is_transient());
    let error = error.to_string();
    assert!(
        error.contains("BIGINT") && error.contains(&u64::MAX.to_string()),
        "{error}"
    );
}

#[cfg(feature = "postgres-sink")]
#[test]
fn copy_uint64_uses_the_same_bigint_wire_type_as_unnest() {
    use arrow_array::Array as _;

    let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
        "value",
        DataType::UInt64,
        false,
    )]));
    let batch = arrow_array::RecordBatch::try_new(
        schema,
        vec![Arc::new(arrow_array::UInt64Array::from(vec![7]))],
    )
    .unwrap();
    let normalized = postgres_copy_batch(&batch).unwrap();

    assert_eq!(normalized.schema().field(0).data_type(), &DataType::Int64);
    let values = normalized
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .unwrap();
    assert_eq!(values.value(0), 7);
}

#[cfg(feature = "postgres-sink")]
#[test]
fn negative_fractional_timestamps_use_euclidean_division() {
    let millis = to_naive_datetime(-1, TimeUnit::Millisecond).unwrap();
    assert_eq!(millis.and_utc().timestamp(), -1);
    assert_eq!(millis.and_utc().timestamp_subsec_nanos(), 999_000_000);

    let micros = to_naive_datetime(-1, TimeUnit::Microsecond).unwrap();
    assert_eq!(micros.and_utc().timestamp(), -1);
    assert_eq!(micros.and_utc().timestamp_subsec_nanos(), 999_999_000);
}

#[cfg(feature = "postgres-sink")]
#[test]
fn out_of_range_non_null_temporal_value_is_rejected() {
    let values = arrow_array::Date32Array::from(vec![i32::MAX]);
    assert!(validate_postgres_array_values(&values).is_err());
}

#[test]
fn decimals_map_to_exact_numeric_with_text_parameters() {
    let money = DataType::Decimal128(18, 2);
    assert_eq!(postgres_type(&money).unwrap().sql(), "numeric");
    assert_eq!(arrow_to_pg_ddl_type(&money).unwrap(), "NUMERIC(18, 2)");
    assert_eq!(
        arrow_type_to_pg_array_cast(&money, 3).unwrap(),
        "$3::text[]::numeric[]"
    );
    assert!(postgres_type(&DataType::Decimal128(10, -2)).is_err());
}

#[test]
fn existing_numeric_columns_must_hold_every_value_without_rounding() {
    let typmod = |precision: i32, scale: i32| ((precision << 16) | scale) + 4;
    let money = DataType::Decimal128(18, 2);
    assert!(numeric_column_holds(&money, -1));
    assert!(numeric_column_holds(&money, typmod(18, 2)));
    assert!(numeric_column_holds(&money, typmod(20, 4)));
    assert!(!numeric_column_holds(&money, typmod(18, 1)));
    assert!(!numeric_column_holds(&money, typmod(10, 2)));
}

#[cfg(feature = "postgres-sink")]
#[test]
fn numeric_wire_keeps_zero_digit_groups_around_the_decimal_point() {
    let wire = |value: i128, scale: u8| {
        let mut buffer = Vec::new();
        numeric_wire(value, scale, &mut buffer);
        buffer
            .chunks(2)
            .map(|word| i16::from_be_bytes([word[0], word[1]]))
            .collect::<Vec<_>>()
    };
    // [ndigits, weight, sign, dscale, digits...]
    assert_eq!(wire(100_005, 5), [3, 0, 0, 5, 1, 0, 5000]);
    assert_eq!(wire(1234, 8), [1, -2, 0, 8, 1234]);
    assert_eq!(wire(-1_234_567, 2), [3, 1, 0x4000, 2, 1, 2345, 6700]);
    assert_eq!(wire(10_000, 0), [1, 1, 0, 0, 1]);
    assert_eq!(wire(0, 2), [0, 0, 0, 2]);
    let widest = wire(10_i128.pow(38) - 1, 0);
    assert_eq!(widest[..4], [10, 9, 0, 0]);
    assert_eq!(widest[4], 99);
}
