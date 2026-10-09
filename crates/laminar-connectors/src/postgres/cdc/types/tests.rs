use super::*;

fn column(type_oid: u32, type_modifier: i32) -> PgColumn {
    PgColumn::new("c".into(), type_oid, type_modifier, false)
}

fn numeric_typmod(precision: i32, scale: i32) -> i32 {
    ((precision << 16) | (scale & 0x7ff)) + VARHDRSZ
}

#[test]
fn lossless_pairs_bind_to_their_text_conversion() {
    let utc = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
    let naive = DataType::Timestamp(TimeUnit::Microsecond, None);
    for (oid, declared, expected) in [
        (BOOL_OID, DataType::Boolean, ValueKind::Bool),
        (INT2_OID, DataType::Int16, ValueKind::Int16),
        (INT4_OID, DataType::Int32, ValueKind::Int32),
        (INT8_OID, DataType::Int64, ValueKind::Int64),
        (FLOAT4_OID, DataType::Float32, ValueKind::Float32),
        (FLOAT8_OID, DataType::Float64, ValueKind::Float64),
        (TEXT_OID, DataType::Utf8, ValueKind::Utf8),
        (VARCHAR_OID, DataType::Utf8, ValueKind::Utf8),
        (JSONB_OID, DataType::Utf8, ValueKind::Utf8),
        (UUID_OID, DataType::Utf8, ValueKind::Utf8),
        (BYTEA_OID, DataType::Binary, ValueKind::Binary),
        (DATE_OID, DataType::Date32, ValueKind::Date32),
        (
            TIME_OID,
            DataType::Time64(TimeUnit::Microsecond),
            ValueKind::Time64Micros,
        ),
        (TIMESTAMP_OID, naive.clone(), ValueKind::TimestampMicros),
        (TIMESTAMPTZ_OID, naive, ValueKind::TimestampTzMicros),
        (TIMESTAMPTZ_OID, utc, ValueKind::TimestampTzMicros),
    ] {
        assert_eq!(bind_value_kind(&column(oid, -1), &declared), Ok(expected));
    }
}

#[test]
fn numeric_requires_matching_constrained_decimal() {
    assert_eq!(
        bind_value_kind(
            &column(NUMERIC_OID, numeric_typmod(12, 2)),
            &DataType::Decimal128(12, 2)
        ),
        Ok(ValueKind::Decimal128 {
            precision: 12,
            scale: 2
        })
    );
    let mismatch = bind_value_kind(
        &column(NUMERIC_OID, numeric_typmod(12, 2)),
        &DataType::Decimal128(12, 3),
    )
    .unwrap_err();
    assert!(mismatch.contains("DECIMAL(12,2)"), "{mismatch}");
    let unbounded =
        bind_value_kind(&column(NUMERIC_OID, -1), &DataType::Decimal128(38, 9)).unwrap_err();
    assert!(unbounded.contains("unconstrained"), "{unbounded}");
    let too_wide = bind_value_kind(
        &column(NUMERIC_OID, numeric_typmod(40, 2)),
        &DataType::Decimal128(38, 2),
    )
    .unwrap_err();
    assert!(too_wide.contains("no exact"), "{too_wide}");
    let negative_scale = bind_value_kind(
        &column(NUMERIC_OID, numeric_typmod(10, -2)),
        &DataType::Decimal128(10, 0),
    )
    .unwrap_err();
    assert!(negative_scale.contains("no exact"), "{negative_scale}");
}

#[test]
fn lossy_or_unknown_pairs_never_fall_back_to_text() {
    for (oid, declared) in [
        (INT8_OID, DataType::Int32),
        (INT4_OID, DataType::Utf8),
        (
            TIMESTAMP_OID,
            DataType::Timestamp(TimeUnit::Millisecond, None),
        ),
        (
            TIMESTAMP_OID,
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        ),
        (
            TIMESTAMPTZ_OID,
            DataType::Timestamp(TimeUnit::Microsecond, Some("Europe/London".into())),
        ),
        (INTERVAL_OID, DataType::Utf8),
        (TEXT_ARRAY_OID, DataType::Utf8),
        (99_999, DataType::Utf8),
    ] {
        let error = bind_value_kind(&column(oid, -1), &declared).unwrap_err();
        assert!(error.contains("supported:"), "{error}");
    }
}

#[test]
fn type_names_cover_rejections() {
    assert_eq!(pg_type_name(INT4_OID), "int4");
    assert_eq!(pg_type_name(INTERVAL_OID), "interval");
    assert_eq!(pg_type_name(99_999), "an unsupported type");
}
