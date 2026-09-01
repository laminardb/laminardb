use super::*;
use arrow_array::Array;
use arrow_schema::{Field, Schema};

fn json_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, true),
        Field::new("value", DataType::Utf8, true),
    ]))
}

#[test]
fn test_parse_json_batch() {
    let parser = MessageParser::new(
        json_schema(),
        MessageFormat::Json,
        JsonDecoderConfig::default(),
    );
    let messages: Vec<&[u8]> = vec![
        br#"{"id": "1", "value": "hello"}"#,
        br#"{"id": "2", "value": "world"}"#,
    ];

    let batch = parser.parse_batch(&messages).unwrap();
    assert_eq!(batch.num_rows(), 2);
    assert_eq!(batch.num_columns(), 2);
}

#[test]
fn json_explode_cannot_exceed_the_actor_row_budget() {
    let parser = MessageParser::new(
        json_schema(),
        MessageFormat::Json,
        JsonDecoderConfig {
            json_explode: Some(vec!["id".into(), "value".into()]),
            ..JsonDecoderConfig::default()
        },
    );
    let message = br#"[["1","a"],["2","b"],["3","c"]]"#;

    let error = parser
        .parse_batch_bounded(&[message], 2)
        .unwrap_err()
        .to_string();

    assert!(error.contains("2-row batch limit"), "{error}");
}

#[test]
fn test_parse_json_missing_field() {
    let parser = MessageParser::new(
        json_schema(),
        MessageFormat::Json,
        JsonDecoderConfig::default(),
    );
    let messages: Vec<&[u8]> = vec![br#"{"id": "1"}"#];

    let batch = parser.parse_batch(&messages).unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert!(batch.column(1).is_null(0));
}

#[test]
fn test_parse_json_numeric_values() {
    let parser = MessageParser::new(
        json_schema(),
        MessageFormat::Json,
        JsonDecoderConfig::default(),
    );
    let messages: Vec<&[u8]> = vec![br#"{"id": "1", "value": 42}"#];

    let batch = parser.parse_batch(&messages).unwrap();
    assert_eq!(batch.num_rows(), 1);
}

#[test]
fn test_parse_binary_batch() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "frame",
        DataType::Binary,
        false,
    )]));
    let parser = MessageParser::new(
        schema.clone(),
        MessageFormat::Binary,
        JsonDecoderConfig::default(),
    );
    let messages: Vec<&[u8]> = vec![b"hello", b"world"];

    let batch = parser.parse_batch(&messages).unwrap();
    assert_eq!(batch.num_rows(), 2);
    assert_eq!(batch.schema(), schema);
    let frames = batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::BinaryArray>()
        .unwrap();
    assert_eq!(frames.value(0), b"hello");
    assert_eq!(frames.value(1), b"world");
}

#[test]
fn test_parse_large_binary_preserves_declared_schema() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "packet",
        DataType::LargeBinary,
        true,
    )]));
    let parser = MessageParser::new(
        schema.clone(),
        MessageFormat::Binary,
        JsonDecoderConfig::default(),
    );
    let messages: Vec<&[u8]> = vec![b"one", b"two"];

    let batch = parser.parse_batch(&messages).unwrap();

    assert_eq!(batch.schema(), schema);
    let packets = batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::LargeBinaryArray>()
        .unwrap();
    assert_eq!(packets.value(0), b"one");
    assert_eq!(packets.value(1), b"two");
}

#[test]
fn test_binary_format_rejects_ambiguous_or_non_binary_schemas() {
    let schemas = [
        Arc::new(Schema::empty()),
        Arc::new(Schema::new(vec![
            Field::new("first", DataType::Binary, false),
            Field::new("second", DataType::Binary, false),
        ])),
        Arc::new(Schema::new(vec![Field::new(
            "payload",
            DataType::Utf8,
            false,
        )])),
    ];

    for schema in schemas {
        let error =
            MessageParser::validate_format_schema(&schema, &MessageFormat::Binary).unwrap_err();
        assert!(matches!(error, ConnectorError::SchemaMismatch(_)));
    }
}

#[test]
fn test_invalid_binary_schema_fails_even_for_empty_input() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "payload",
        DataType::Utf8,
        false,
    )]));
    let parser = MessageParser::new(schema, MessageFormat::Binary, JsonDecoderConfig::default());

    assert!(matches!(
        parser.parse_batch(&[]),
        Err(ConnectorError::SchemaMismatch(_))
    ));
}

#[test]
fn test_parse_csv_batch() {
    let parser = MessageParser::new(
        json_schema(),
        MessageFormat::Csv {
            delimiter: ',',
            has_header: false,
        },
        JsonDecoderConfig::default(),
    );
    let messages: Vec<&[u8]> = vec![b"1,hello", b"2,world"];

    let batch = parser.parse_batch(&messages).unwrap();
    assert_eq!(batch.num_rows(), 2);
}

#[test]
fn test_parse_empty() {
    let parser = MessageParser::new(
        json_schema(),
        MessageFormat::Json,
        JsonDecoderConfig::default(),
    );
    let messages: Vec<&[u8]> = vec![];

    let batch = parser.parse_batch(&messages).unwrap();
    assert_eq!(batch.num_rows(), 0);
}

#[test]
fn test_parse_invalid_json() {
    let parser = MessageParser::new(
        json_schema(),
        MessageFormat::Json,
        JsonDecoderConfig::default(),
    );
    let messages: Vec<&[u8]> = vec![b"not json"];

    assert!(parser.parse_batch(&messages).is_err());
}

#[test]
fn test_parse_json_typed_columns() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("price", DataType::Float64, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let parser = MessageParser::new(schema, MessageFormat::Json, JsonDecoderConfig::default());
    let messages: Vec<&[u8]> = vec![
        br#"{"id": 1, "price": 99.5, "name": "Widget"}"#,
        br#"{"id": 2, "price": 10.0, "name": "Gadget"}"#,
    ];

    let batch = parser.parse_batch(&messages).unwrap();
    assert_eq!(batch.num_rows(), 2);

    // Columns should have the declared types, not Utf8.
    assert_eq!(batch.column(0).data_type(), &DataType::Int64);
    assert_eq!(batch.column(1).data_type(), &DataType::Float64);
    assert_eq!(batch.column(2).data_type(), &DataType::Utf8);

    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .unwrap();
    assert_eq!(ids.value(0), 1);
    assert_eq!(ids.value(1), 2);
}

#[test]
fn test_parse_json_coerces_string_numbers() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "price",
        DataType::Float64,
        false,
    )]));
    let parser = MessageParser::new(schema, MessageFormat::Json, JsonDecoderConfig::default());
    let messages: Vec<&[u8]> = vec![br#"{"price": "187.52"}"#];

    let batch = parser.parse_batch(&messages).unwrap();
    assert_eq!(batch.column(0).data_type(), &DataType::Float64);
    let prices = batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::Float64Array>()
        .unwrap();
    assert!((prices.value(0) - 187.52).abs() < f64::EPSILON);
}
