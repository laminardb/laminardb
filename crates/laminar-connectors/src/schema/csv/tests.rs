use super::*;
use crate::schema::traits::FormatEncoder;
use arrow_array::cast::AsArray;
use arrow_schema::{Field, Schema};

fn make_schema(fields: Vec<(&str, DataType, bool)>) -> SchemaRef {
    Arc::new(Schema::new(
        fields
            .into_iter()
            .map(|(name, dt, nullable)| Field::new(name, dt, nullable))
            .collect::<Vec<_>>(),
    ))
}

fn csv_record(line: &str) -> RawRecord {
    RawRecord::new(line.as_bytes().to_vec())
}

fn csv_block(lines: &str) -> RawRecord {
    RawRecord::new(lines.as_bytes().to_vec())
}

// ── Basic decode tests ────────────────────────────────────

#[test]
fn test_decode_empty_batch() {
    let schema = make_schema(vec![("id", DataType::Int64, false)]);
    let decoder = CsvDecoder::new(schema.clone());
    let batch = decoder.decode_batch(&[]).unwrap();
    assert_eq!(batch.num_rows(), 0);
    assert_eq!(batch.schema(), schema);
}

#[test]
fn test_decode_single_row_with_header() {
    let schema = make_schema(vec![
        ("id", DataType::Int64, false),
        ("name", DataType::Utf8, true),
    ]);
    let decoder = CsvDecoder::new(schema);
    let records = vec![csv_block("id,name\n42,Alice")];
    let batch = decoder.decode_batch(&records).unwrap();

    assert_eq!(batch.num_rows(), 1);
    assert_eq!(
        batch
            .column(0)
            .as_primitive::<arrow_array::types::Int64Type>()
            .value(0),
        42
    );
    assert_eq!(batch.column(1).as_string::<i32>().value(0), "Alice");
}

#[test]
fn test_decode_multiple_rows() {
    let schema = make_schema(vec![
        ("x", DataType::Int64, false),
        ("y", DataType::Float64, false),
    ]);
    let decoder = CsvDecoder::new(schema);
    let records = vec![csv_block("x,y\n1,1.5\n2,2.5\n3,3.5")];
    let batch = decoder.decode_batch(&records).unwrap();

    assert_eq!(batch.num_rows(), 3);
    let x_col = batch
        .column(0)
        .as_primitive::<arrow_array::types::Int64Type>();
    assert_eq!(x_col.value(0), 1);
    assert_eq!(x_col.value(1), 2);
    assert_eq!(x_col.value(2), 3);
}

#[test]
fn test_decode_all_types() {
    let schema = make_schema(vec![
        ("bool_col", DataType::Boolean, false),
        ("int_col", DataType::Int64, false),
        ("float_col", DataType::Float64, false),
        ("str_col", DataType::Utf8, false),
    ]);
    let decoder = CsvDecoder::new(schema);
    let records = vec![csv_block(
        "bool_col,int_col,float_col,str_col\ntrue,42,3.14,hello",
    )];
    let batch = decoder.decode_batch(&records).unwrap();

    assert_eq!(batch.num_rows(), 1);
    assert!(batch.column(0).as_boolean().value(0));
    assert_eq!(
        batch
            .column(1)
            .as_primitive::<arrow_array::types::Int64Type>()
            .value(0),
        42
    );
    let f = batch
        .column(2)
        .as_primitive::<arrow_array::types::Float64Type>()
        .value(0);
    assert!((f - 3.14).abs() < f64::EPSILON);
    assert_eq!(batch.column(3).as_string::<i32>().value(0), "hello");
}

// ── Null handling ─────────────────────────────────────────

#[test]
fn test_decode_null_string_default() {
    // Default null_string is empty string.
    let schema = make_schema(vec![
        ("a", DataType::Int64, true),
        ("b", DataType::Utf8, true),
    ]);
    let decoder = CsvDecoder::new(schema);
    let records = vec![csv_block("a,b\n,")];
    let batch = decoder.decode_batch(&records).unwrap();

    assert!(batch.column(0).is_null(0));
    assert!(batch.column(1).is_null(0));
}

#[test]
fn test_decode_null_string_custom() {
    let schema = make_schema(vec![("val", DataType::Int64, true)]);
    let config = CsvDecoderConfig {
        null_string: "NA".into(),
        ..Default::default()
    };
    let decoder = CsvDecoder::with_config(schema, config);
    let records = vec![csv_block("val\nNA\n42")];
    let batch = decoder.decode_batch(&records).unwrap();

    assert_eq!(batch.num_rows(), 2);
    assert!(batch.column(0).is_null(0));
    assert_eq!(
        batch
            .column(0)
            .as_primitive::<arrow_array::types::Int64Type>()
            .value(1),
        42
    );
}

// ── Field count mismatch strategies ───────────────────────

#[test]
fn test_mismatch_null_strategy() {
    let schema = make_schema(vec![
        ("a", DataType::Int64, true),
        ("b", DataType::Utf8, true),
        ("c", DataType::Int64, true),
    ]);
    let decoder = CsvDecoder::new(schema);
    // Row only has 2 fields, schema expects 3.
    let records = vec![csv_block("a,b,c\n1,hello")];
    let batch = decoder.decode_batch(&records).unwrap();

    assert_eq!(batch.num_rows(), 1);
    assert_eq!(
        batch
            .column(0)
            .as_primitive::<arrow_array::types::Int64Type>()
            .value(0),
        1
    );
    assert_eq!(batch.column(1).as_string::<i32>().value(0), "hello");
    assert!(batch.column(2).is_null(0)); // padded with null
}

#[test]
fn test_mismatch_skip_strategy() {
    let schema = make_schema(vec![
        ("a", DataType::Int64, false),
        ("b", DataType::Int64, false),
    ]);
    let config = CsvDecoderConfig {
        field_count_mismatch: FieldCountMismatchStrategy::Skip,
        ..Default::default()
    };
    let decoder = CsvDecoder::with_config(schema, config);
    // One good row, one bad row (too few fields).
    let records = vec![csv_block("a,b\n1,2\n3")];
    let batch = decoder.decode_batch(&records).unwrap();

    assert_eq!(batch.num_rows(), 1); // bad row skipped
    assert_eq!(
        batch
            .column(0)
            .as_primitive::<arrow_array::types::Int64Type>()
            .value(0),
        1
    );
}

#[test]
fn test_mismatch_reject_strategy() {
    let schema = make_schema(vec![
        ("a", DataType::Int64, false),
        ("b", DataType::Int64, false),
    ]);
    let config = CsvDecoderConfig {
        field_count_mismatch: FieldCountMismatchStrategy::Reject,
        ..Default::default()
    };
    let decoder = CsvDecoder::with_config(schema, config);
    let records = vec![csv_block("a,b\n1")]; // too few fields
    let result = decoder.decode_batch(&records);

    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("field count mismatch"));
}

// ── Delimiter options ─────────────────────────────────────

#[test]
fn test_pipe_delimiter() {
    let schema = make_schema(vec![
        ("a", DataType::Int64, false),
        ("b", DataType::Utf8, false),
    ]);
    let config = CsvDecoderConfig {
        delimiter: b'|',
        ..Default::default()
    };
    let decoder = CsvDecoder::with_config(schema, config);
    let records = vec![csv_block("a|b\n42|hello")];
    let batch = decoder.decode_batch(&records).unwrap();

    assert_eq!(
        batch
            .column(0)
            .as_primitive::<arrow_array::types::Int64Type>()
            .value(0),
        42
    );
    assert_eq!(batch.column(1).as_string::<i32>().value(0), "hello");
}

#[test]
fn test_tab_delimiter() {
    let schema = make_schema(vec![
        ("a", DataType::Int64, false),
        ("b", DataType::Utf8, false),
    ]);
    let config = CsvDecoderConfig {
        delimiter: b'\t',
        ..Default::default()
    };
    let decoder = CsvDecoder::with_config(schema, config);
    let records = vec![csv_block("a\tb\n42\thello")];
    let batch = decoder.decode_batch(&records).unwrap();

    assert_eq!(
        batch
            .column(0)
            .as_primitive::<arrow_array::types::Int64Type>()
            .value(0),
        42
    );
    assert_eq!(batch.column(1).as_string::<i32>().value(0), "hello");
}

#[test]
fn test_semicolon_delimiter() {
    let schema = make_schema(vec![
        ("a", DataType::Int64, false),
        ("b", DataType::Utf8, false),
    ]);
    let config = CsvDecoderConfig {
        delimiter: b';',
        ..Default::default()
    };
    let decoder = CsvDecoder::with_config(schema, config);
    let records = vec![csv_block("a;b\n99;world")];
    let batch = decoder.decode_batch(&records).unwrap();

    assert_eq!(
        batch
            .column(0)
            .as_primitive::<arrow_array::types::Int64Type>()
            .value(0),
        99
    );
    assert_eq!(batch.column(1).as_string::<i32>().value(0), "world");
}

// ── Comment lines ─────────────────────────────────────────

#[test]
fn test_comment_lines() {
    let schema = make_schema(vec![("val", DataType::Int64, false)]);
    let config = CsvDecoderConfig {
        comment: Some(b'#'),
        ..Default::default()
    };
    let decoder = CsvDecoder::with_config(schema, config);
    let records = vec![csv_block("val\n# this is a comment\n42\n# another\n99")];
    let batch = decoder.decode_batch(&records).unwrap();

    assert_eq!(batch.num_rows(), 2);
    let col = batch
        .column(0)
        .as_primitive::<arrow_array::types::Int64Type>();
    assert_eq!(col.value(0), 42);
    assert_eq!(col.value(1), 99);
}

// ── Skip rows ─────────────────────────────────────────────

#[test]
fn test_skip_rows() {
    let schema = make_schema(vec![("val", DataType::Int64, false)]);
    let config = CsvDecoderConfig {
        skip_rows: 2,
        ..Default::default()
    };
    let decoder = CsvDecoder::with_config(schema, config);
    let records = vec![csv_block("val\nskip1\nskip2\n42\n99")];
    let batch = decoder.decode_batch(&records).unwrap();

    // "skip1" and "skip2" are skipped (parse errors counted), then 42 and 99.
    // Actually skip_rows skips first N data rows; skip1/skip2 aren't valid i64
    // so they'd be parse errors. But the skip_rows logic skips before type
    // coercion, so they won't generate errors.
    assert_eq!(batch.num_rows(), 2);
}

// ── No header mode ────────────────────────────────────────

#[test]
fn test_no_header() {
    let schema = make_schema(vec![
        ("col0", DataType::Int64, false),
        ("col1", DataType::Utf8, false),
    ]);
    let config = CsvDecoderConfig {
        has_header: false,
        ..Default::default()
    };
    let decoder = CsvDecoder::with_config(schema, config);
    let records = vec![csv_block("1,alpha\n2,beta")];
    let batch = decoder.decode_batch(&records).unwrap();

    assert_eq!(batch.num_rows(), 2);
    let col0 = batch
        .column(0)
        .as_primitive::<arrow_array::types::Int64Type>();
    assert_eq!(col0.value(0), 1);
    assert_eq!(col0.value(1), 2);
}

// ── Multiple records (streaming) ──────────────────────────

#[test]
fn test_multiple_raw_records() {
    // Simulate streaming: each RawRecord is one CSV line (no header).
    let schema = make_schema(vec![
        ("id", DataType::Int64, false),
        ("val", DataType::Float64, false),
    ]);
    let config = CsvDecoderConfig {
        has_header: false,
        ..Default::default()
    };
    let decoder = CsvDecoder::with_config(schema, config);
    let records = vec![
        csv_record("1,1.5"),
        csv_record("2,2.5"),
        csv_record("3,3.5"),
    ];
    let batch = decoder.decode_batch(&records).unwrap();

    assert_eq!(batch.num_rows(), 3);
    let id_col = batch
        .column(0)
        .as_primitive::<arrow_array::types::Int64Type>();
    let val_col = batch
        .column(1)
        .as_primitive::<arrow_array::types::Float64Type>();
    assert_eq!(id_col.value(0), 1);
    assert_eq!(id_col.value(2), 3);
    assert!((val_col.value(1) - 2.5).abs() < f64::EPSILON);
}

// ── Quoted fields ─────────────────────────────────────────

#[test]
fn test_quoted_fields_with_delimiter() {
    let schema = make_schema(vec![
        ("name", DataType::Utf8, false),
        ("desc", DataType::Utf8, false),
    ]);
    let decoder = CsvDecoder::new(schema);
    let records = vec![csv_block("name,desc\n\"Smith, John\",\"A, B\"")];
    let batch = decoder.decode_batch(&records).unwrap();

    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.column(0).as_string::<i32>().value(0), "Smith, John");
    assert_eq!(batch.column(1).as_string::<i32>().value(0), "A, B");
}

#[test]
fn test_quoted_fields_with_newline() {
    let schema = make_schema(vec![
        ("id", DataType::Int64, false),
        ("text", DataType::Utf8, false),
    ]);
    let decoder = CsvDecoder::new(schema);
    let records = vec![csv_block("id,text\n1,\"line1\nline2\"")];
    let batch = decoder.decode_batch(&records).unwrap();

    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.column(1).as_string::<i32>().value(0), "line1\nline2");
}

#[test]
fn test_escaped_quotes_rfc4180() {
    // RFC 4180: doubled quotes within quoted field.
    let schema = make_schema(vec![("val", DataType::Utf8, false)]);
    let decoder = CsvDecoder::new(schema);
    let records = vec![csv_block("val\n\"She said \"\"hello\"\"\"")];
    let batch = decoder.decode_batch(&records).unwrap();

    assert_eq!(batch.num_rows(), 1);
    assert_eq!(
        batch.column(0).as_string::<i32>().value(0),
        "She said \"hello\""
    );
}

// ── Timestamp parsing ─────────────────────────────────────

#[test]
fn test_decode_timestamp() {
    let schema = make_schema(vec![(
        "ts",
        DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
        false,
    )]);
    let decoder = CsvDecoder::new(schema);
    let records = vec![csv_block("ts\n2025-01-15 10:30:00.000")];
    let batch = decoder.decode_batch(&records).unwrap();

    assert_eq!(batch.num_rows(), 1);
    assert!(!batch.column(0).is_null(0));
}

#[test]
fn test_decode_timestamp_iso8601_fallback() {
    let schema = make_schema(vec![(
        "ts",
        DataType::Timestamp(TimeUnit::Nanosecond, None),
        false,
    )]);
    let decoder = CsvDecoder::new(schema);
    let records = vec![csv_block("ts\n2025-01-15T10:30:00Z")];
    let batch = decoder.decode_batch(&records).unwrap();

    assert_eq!(batch.num_rows(), 1);
    assert!(!batch.column(0).is_null(0));
}

// ── Date parsing ──────────────────────────────────────────

#[test]
fn test_decode_date() {
    let schema = make_schema(vec![("d", DataType::Date32, false)]);
    let decoder = CsvDecoder::new(schema);
    let records = vec![csv_block("d\n2025-06-15")];
    let batch = decoder.decode_batch(&records).unwrap();

    assert_eq!(batch.num_rows(), 1);
    assert!(!batch.column(0).is_null(0));
    // 2025-06-15 is day 20254 since epoch.
    let days = batch
        .column(0)
        .as_primitive::<arrow_array::types::Date32Type>()
        .value(0);
    let expected = chrono::NaiveDate::from_ymd_opt(2025, 6, 15)
        .unwrap()
        .signed_duration_since(chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
        .num_days();
    #[allow(clippy::cast_possible_truncation)]
    {
        assert_eq!(days, expected as i32);
    }
}

// ── Boolean parsing ───────────────────────────────────────

#[test]
fn test_decode_boolean_variants() {
    let schema = make_schema(vec![("b", DataType::Boolean, false)]);
    let config = CsvDecoderConfig {
        has_header: false,
        ..Default::default()
    };
    let decoder = CsvDecoder::with_config(schema, config);
    let records = vec![csv_block("true\nfalse\n1\n0\nyes\nno\nt\nf\ny\nn")];
    let batch = decoder.decode_batch(&records).unwrap();

    assert_eq!(batch.num_rows(), 10);
    let col = batch.column(0).as_boolean();
    assert!(col.value(0)); // true
    assert!(!col.value(1)); // false
    assert!(col.value(2)); // 1
    assert!(!col.value(3)); // 0
    assert!(col.value(4)); // yes
    assert!(!col.value(5)); // no
    assert!(col.value(6)); // t
    assert!(!col.value(7)); // f
    assert!(col.value(8)); // y
    assert!(!col.value(9)); // n
}

// ── Parse error counting ──────────────────────────────────

#[test]
fn test_parse_error_count() {
    let schema = make_schema(vec![("val", DataType::Int64, true)]);
    let decoder = CsvDecoder::new(schema);
    let records = vec![csv_block("val\nnot_a_number\n42\nalso_bad")];
    let batch = decoder.decode_batch(&records).unwrap();

    assert_eq!(batch.num_rows(), 3);
    assert!(batch.column(0).is_null(0));
    assert_eq!(
        batch
            .column(0)
            .as_primitive::<arrow_array::types::Int64Type>()
            .value(1),
        42
    );
    assert!(batch.column(0).is_null(2));
    assert_eq!(decoder.parse_error_count(), 2);
}

// ── Extra fields ignored ──────────────────────────────────

#[test]
fn test_extra_fields_truncated() {
    let schema = make_schema(vec![("a", DataType::Int64, false)]);
    let decoder = CsvDecoder::new(schema);
    // Row has 3 fields but schema only has 1.
    let records = vec![csv_block("a\n42,extra1,extra2")];
    let batch = decoder.decode_batch(&records).unwrap();

    // Extra fields silently ignored (flexible mode).
    // field_count (3) != num_fields (1), but Null strategy just pads/truncates.
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(
        batch
            .column(0)
            .as_primitive::<arrow_array::types::Int64Type>()
            .value(0),
        42
    );
}

// ── FormatDecoder trait ───────────────────────────────────

#[test]
fn test_format_name() {
    let schema = make_schema(vec![("a", DataType::Int64, false)]);
    let decoder = CsvDecoder::new(schema);
    assert_eq!(decoder.format_name(), "csv");
}

#[test]
fn test_output_schema() {
    let schema = make_schema(vec![
        ("a", DataType::Int64, false),
        ("b", DataType::Utf8, true),
    ]);
    let decoder = CsvDecoder::new(schema.clone());
    assert_eq!(decoder.output_schema(), schema);
}

#[test]
fn test_decode_one() {
    let schema = make_schema(vec![("x", DataType::Int64, false)]);
    let config = CsvDecoderConfig {
        has_header: false,
        ..Default::default()
    };
    let decoder = CsvDecoder::with_config(schema, config);
    let record = csv_record("99");
    let batch = decoder.decode_one(&record).unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(
        batch
            .column(0)
            .as_primitive::<arrow_array::types::Int64Type>()
            .value(0),
        99
    );
}

// ── Edge cases ────────────────────────────────────────────

#[test]
fn test_mixed_line_endings() {
    let schema = make_schema(vec![("val", DataType::Int64, false)]);
    let config = CsvDecoderConfig {
        has_header: false,
        ..Default::default()
    };
    let decoder = CsvDecoder::with_config(schema, config);
    let records = vec![csv_block("1\r\n2\n3\r\n")];
    let batch = decoder.decode_batch(&records).unwrap();
    assert_eq!(batch.num_rows(), 3);
}

#[test]
fn test_unicode_values() {
    let schema = make_schema(vec![("name", DataType::Utf8, false)]);
    let decoder = CsvDecoder::new(schema);
    let records = vec![csv_block("name\nこんにちは\nüber\nnaïve")];
    let batch = decoder.decode_batch(&records).unwrap();

    assert_eq!(batch.num_rows(), 3);
    assert_eq!(batch.column(0).as_string::<i32>().value(0), "こんにちは");
    assert_eq!(batch.column(0).as_string::<i32>().value(1), "über");
    assert_eq!(batch.column(0).as_string::<i32>().value(2), "naïve");
}

#[test]
fn test_trailing_comma() {
    // Trailing comma creates an extra empty field.
    let schema = make_schema(vec![
        ("a", DataType::Int64, false),
        ("b", DataType::Int64, true),
    ]);
    let decoder = CsvDecoder::new(schema);
    let records = vec![csv_block("a,b\n1,")];
    let batch = decoder.decode_batch(&records).unwrap();

    assert_eq!(batch.num_rows(), 1);
    assert_eq!(
        batch
            .column(0)
            .as_primitive::<arrow_array::types::Int64Type>()
            .value(0),
        1
    );
    // Empty string matches default null_string → null.
    assert!(batch.column(1).is_null(0));
}

#[test]
fn test_backslash_escape() {
    let schema = make_schema(vec![("val", DataType::Utf8, false)]);
    let config = CsvDecoderConfig {
        escape: Some(b'\\'),
        ..Default::default()
    };
    let decoder = CsvDecoder::with_config(schema, config);
    let records = vec![csv_block("val\n\"hello \\\"world\\\"\"")];
    let batch = decoder.decode_batch(&records).unwrap();

    assert_eq!(batch.num_rows(), 1);
    assert_eq!(
        batch.column(0).as_string::<i32>().value(0),
        "hello \"world\""
    );
}

// ── CsvEncoder tests ──────────────────────────────────────

#[test]
fn test_csv_encode_basic() {
    let schema = make_schema(vec![
        ("id", DataType::Int64, false),
        ("name", DataType::Utf8, false),
    ]);

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(arrow_array::Int64Array::from(vec![1, 2])),
            Arc::new(arrow_array::StringArray::from(vec!["Alice", "Bob"])),
        ],
    )
    .unwrap();

    let encoder = CsvEncoder::new(schema);
    let records = encoder.encode_batch(&batch).unwrap();

    assert_eq!(records.len(), 2);
    assert_eq!(std::str::from_utf8(&records[0]).unwrap(), "1,Alice");
    assert_eq!(std::str::from_utf8(&records[1]).unwrap(), "2,Bob");
}

#[test]
fn test_csv_encode_with_header() {
    let schema = make_schema(vec![
        ("id", DataType::Int64, false),
        ("name", DataType::Utf8, false),
    ]);

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(arrow_array::Int64Array::from(vec![1])),
            Arc::new(arrow_array::StringArray::from(vec!["Alice"])),
        ],
    )
    .unwrap();

    let config = CsvEncoderConfig {
        has_header: true,
        ..Default::default()
    };
    let encoder = CsvEncoder::with_config(schema, config);
    let records = encoder.encode_batch(&batch).unwrap();

    assert_eq!(records.len(), 2); // header + 1 data row
    assert_eq!(std::str::from_utf8(&records[0]).unwrap(), "id,name");
    assert_eq!(std::str::from_utf8(&records[1]).unwrap(), "1,Alice");
}

#[test]
fn test_csv_encode_tab_delimiter() {
    let schema = make_schema(vec![
        ("a", DataType::Int64, false),
        ("b", DataType::Utf8, false),
    ]);

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(arrow_array::Int64Array::from(vec![42])),
            Arc::new(arrow_array::StringArray::from(vec!["hello"])),
        ],
    )
    .unwrap();

    let config = CsvEncoderConfig {
        delimiter: b'\t',
        ..Default::default()
    };
    let encoder = CsvEncoder::with_config(schema, config);
    let records = encoder.encode_batch(&batch).unwrap();

    assert_eq!(records.len(), 1);
    assert_eq!(std::str::from_utf8(&records[0]).unwrap(), "42\thello");
}

#[test]
fn test_csv_encode_empty_batch() {
    let schema = make_schema(vec![("x", DataType::Int64, false)]);
    let batch = RecordBatch::new_empty(schema.clone());
    let encoder = CsvEncoder::new(schema);
    let records = encoder.encode_batch(&batch).unwrap();
    assert!(records.is_empty());
}

#[test]
fn test_csv_encode_nulls() {
    let schema = make_schema(vec![
        ("id", DataType::Int64, false),
        ("value", DataType::Int64, true),
    ]);

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(arrow_array::Int64Array::from(vec![1, 2])),
            Arc::new(arrow_array::Int64Array::from(vec![Some(10), None])),
        ],
    )
    .unwrap();

    let encoder = CsvEncoder::new(schema);
    let records = encoder.encode_batch(&batch).unwrap();

    assert_eq!(records.len(), 2);
    assert_eq!(std::str::from_utf8(&records[0]).unwrap(), "1,10");
    assert_eq!(std::str::from_utf8(&records[1]).unwrap(), "2,");
}
