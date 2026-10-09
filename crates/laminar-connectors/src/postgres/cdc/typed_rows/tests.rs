use arrow_array::cast::AsArray;
use arrow_array::types::{
    Date32Type, Decimal128Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type,
    Time64MicrosecondType, TimestampMicrosecondType,
};
use arrow_array::Array;
use arrow_schema::{Field, Schema, TimeUnit};

use super::*;

fn column(name: &str, tuple_index: usize, kind: ValueKind, nullable: bool) -> BoundColumn {
    BoundColumn {
        name: name.into(),
        tuple_index,
        kind,
        nullable,
        is_key: tuple_index == 0,
    }
}

fn layout(columns: Vec<(BoundColumn, DataType)>, weighted: bool) -> RowLayout {
    let mut fields = columns
        .iter()
        .map(|(column, data_type)| Field::new(&column.name, data_type.clone(), column.nullable))
        .collect::<Vec<_>>();
    if weighted {
        fields.push(Field::new("__weight", DataType::Int64, false));
    }
    RowLayout {
        tuple_width: columns.len(),
        columns: columns.into_iter().map(|(column, _)| column).collect(),
        schema: Arc::new(Schema::new(fields)),
        weighted,
    }
}

fn single(kind: ValueKind, data_type: DataType, text: &str) -> Result<ArrayRef, ConnectorError> {
    let layout = layout(vec![(column("v", 0, kind, true), data_type)], false);
    let mut rows = RowBuilder::new(&layout);
    rows.append(&layout, |_, _| Ok(Some(text.as_bytes())), false, None, 1)?;
    Ok(Arc::clone(rows.finish(&layout)?.column(0)))
}

#[test]
fn scalar_text_round_trips_exactly() {
    let value = single(ValueKind::Int16, DataType::Int16, "-32768").unwrap();
    assert_eq!(value.as_primitive::<Int16Type>().value(0), i16::MIN);
    let value = single(ValueKind::Int32, DataType::Int32, "2147483647").unwrap();
    assert_eq!(value.as_primitive::<Int32Type>().value(0), i32::MAX);
    let value = single(ValueKind::Int64, DataType::Int64, "-9223372036854775808").unwrap();
    assert_eq!(value.as_primitive::<Int64Type>().value(0), i64::MIN);
    let value = single(ValueKind::Float64, DataType::Float64, "0.30000000000000004").unwrap();
    assert_eq!(
        value.as_primitive::<Float64Type>().value(0),
        0.300_000_000_000_000_04
    );
    let value = single(ValueKind::Float32, DataType::Float32, "-Infinity").unwrap();
    assert_eq!(
        value.as_primitive::<Float32Type>().value(0),
        f32::NEG_INFINITY
    );
    let value = single(ValueKind::Float64, DataType::Float64, "NaN").unwrap();
    assert!(value.as_primitive::<Float64Type>().value(0).is_nan());
    let value = single(ValueKind::Bool, DataType::Boolean, "t").unwrap();
    assert!(value.as_boolean().value(0));
    let value = single(ValueKind::Utf8, DataType::Utf8, "héllo\t\"x\"").unwrap();
    assert_eq!(value.as_string::<i32>().value(0), "héllo\t\"x\"");
}

#[test]
fn numeric_keeps_its_declared_scale() {
    let kind = ValueKind::Decimal128 {
        precision: 12,
        scale: 4,
    };
    let value = single(kind, DataType::Decimal128(12, 4), "-12345678.0105").unwrap();
    let decimals = value.as_primitive::<Decimal128Type>();
    assert_eq!(decimals.value(0), -123_456_780_105);
    assert_eq!(decimals.scale(), 4);
    for text in ["NaN", "Infinity", "1234567890123.0000"] {
        assert!(
            single(kind, DataType::Decimal128(12, 4), text).is_err(),
            "{text}"
        );
    }
}

#[test]
fn temporal_text_uses_utc_and_microseconds() {
    let value = single(ValueKind::Date32, DataType::Date32, "1970-01-02").unwrap();
    assert_eq!(value.as_primitive::<Date32Type>().value(0), 1);
    let time = DataType::Time64(TimeUnit::Microsecond);
    let value = single(ValueKind::Time64Micros, time, "01:00:00.000001").unwrap();
    assert_eq!(
        value.as_primitive::<Time64MicrosecondType>().value(0),
        3_600_000_001
    );
    let naive = DataType::Timestamp(TimeUnit::Microsecond, None);
    for (text, micros) in [
        ("1970-01-01 00:00:01", 1_000_000),
        ("1969-12-31 23:59:59.999999", -1),
    ] {
        let value = single(ValueKind::TimestampMicros, naive.clone(), text).unwrap();
        assert_eq!(
            value.as_primitive::<TimestampMicrosecondType>().value(0),
            micros
        );
    }
    let utc = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
    for (text, micros) in [
        ("1970-01-01 00:00:01.5+00", 1_500_000),
        ("1970-01-01 05:30:00+05:30", 0),
    ] {
        let value = single(ValueKind::TimestampTzMicros, utc.clone(), text).unwrap();
        let timestamps = value.as_primitive::<TimestampMicrosecondType>();
        assert_eq!(timestamps.value(0), micros);
        assert_eq!(timestamps.timezone(), Some("UTC"));
    }
    for (kind, data_type, text) in [
        (ValueKind::Date32, DataType::Date32, "infinity"),
        (ValueKind::Date32, DataType::Date32, "0044-03-15 BC"),
        (ValueKind::TimestampMicros, naive, "infinity"),
    ] {
        assert!(single(kind, data_type, text).is_err(), "{text}");
    }
}

#[test]
fn bytea_accepts_only_hex_output() {
    let value = single(ValueKind::Binary, DataType::Binary, "\\x00ff10").unwrap();
    assert_eq!(value.as_binary::<i32>().value(0), [0x00, 0xff, 0x10]);
    for text in ["\\000", "\\x0", "\\xzz"] {
        assert!(single(ValueKind::Binary, DataType::Binary, text).is_err());
    }
}

#[test]
fn nulls_key_only_rows_and_weights_are_explicit() {
    let layout = layout(
        vec![
            (column("id", 0, ValueKind::Int64, false), DataType::Int64),
            (column("label", 1, ValueKind::Utf8, true), DataType::Utf8),
        ],
        true,
    );
    let mut rows = RowBuilder::new(&layout);
    let values = [Some("7".as_bytes()), Some("seven".as_bytes())];
    rows.append(
        &layout,
        |_, c| Ok(values[c.tuple_index]),
        false,
        Some(1),
        10,
    )
    .unwrap();
    rows.append(
        &layout,
        |_, c| Ok(values[c.tuple_index]),
        true,
        Some(-1),
        10,
    )
    .unwrap();
    let null_label = [Some("8".as_bytes()), None];
    rows.append(
        &layout,
        |_, c| Ok(null_label[c.tuple_index]),
        false,
        Some(1),
        10,
    )
    .unwrap();
    assert_eq!((rows.len(), rows.retained_bytes()), (3, 30));
    let batch = rows.finish(&layout).unwrap();
    assert_eq!(rows.len(), 0);
    let labels = batch.column(1).as_string::<i32>();
    assert_eq!(labels.value(0), "seven");
    assert!(
        labels.is_null(1),
        "a key-only row never carries non-key values"
    );
    assert!(labels.is_null(2));
    let weights = batch.column(2).as_primitive::<Int64Type>();
    assert_eq!(weights.values().as_ref(), [1, -1, 1]);

    let missing_key = [None, Some("x".as_bytes())];
    let error = rows
        .append(
            &layout,
            |_, c| Ok(missing_key[c.tuple_index]),
            false,
            Some(1),
            10,
        )
        .unwrap_err();
    assert!(error.to_string().contains("declared NOT NULL"), "{error}");
}
