use arrow_array::{Array, BinaryArray, Decimal128Array, Int64Array, StringArray};
use arrow_schema::{Field, Schema};
use mongodb::bson::{doc, oid::ObjectId, Binary, Decimal128, Document, RawDocumentBuf};

use super::*;

fn schema(fields: Vec<Field>) -> SchemaRef {
    Arc::new(Schema::new(fields))
}

fn projection(fields: Vec<Field>, objectid: &[&str], json: Option<&str>) -> DocumentProjection {
    DocumentProjection::try_new(
        &schema(fields),
        &["_id".to_string()],
        &objectid
            .iter()
            .map(|c| (*c).to_string())
            .collect::<Vec<_>>(),
        json,
    )
    .unwrap()
}

fn raw(document: &Document) -> RawDocumentBuf {
    RawDocumentBuf::from_document(document).unwrap()
}

fn build_put(
    projection: &DocumentProjection,
    document: &Document,
) -> Result<RecordBatch, ConnectorError> {
    let key = raw(&doc! { "_id": document.get("_id").unwrap().clone() });
    let document = raw(document);
    projection
        .build(&[DocumentRow::Put {
            key: &key,
            document: &document,
        }])
        .map(|(batch, _)| batch)
}

#[test]
fn object_ids_and_strings_never_share_a_column() {
    let id = ObjectId::parse_str("65a1b2c3d4e5f60718293a4b").unwrap();
    let pinned = projection(
        vec![Field::new("_id", DataType::Utf8, false)],
        &["_id"],
        None,
    );
    let batch = build_put(&pinned, &doc! { "_id": id }).unwrap();
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(ids.value(0), "65a1b2c3d4e5f60718293a4b");
    let error = build_put(&pinned, &doc! { "_id": "65a1b2c3d4e5f60718293a4b" }).unwrap_err();
    assert!(error.to_string().contains("String"), "{error}");

    let strings = projection(vec![Field::new("_id", DataType::Utf8, false)], &[], None);
    let error = build_put(&strings, &doc! { "_id": id }).unwrap_err();
    assert!(error.to_string().contains("objectid.columns"), "{error}");
    assert!(!error.is_transient());
}

#[test]
fn integers_and_decimals_are_exact_or_rejected() {
    let typed = projection(
        vec![
            Field::new("_id", DataType::Int64, false),
            Field::new("big", DataType::Int64, true),
            Field::new("price", DataType::Decimal128(10, 2), true),
            Field::new("ratio", DataType::Float64, true),
        ],
        &[],
        None,
    );
    let batch = build_put(
        &typed,
        &doc! {
            "_id": 7_i32,
            "big": 9_007_199_254_740_993_i64,
            "price": "12.30".parse::<Decimal128>().unwrap(),
            "ratio": 0.5,
        },
    )
    .unwrap();
    let big = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(big.value(0), 9_007_199_254_740_993);
    let price = batch
        .column(2)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(price.value(0), 1230);

    for (field, value) in [
        (
            "price",
            mongodb::bson::Bson::Decimal128("1.234".parse().unwrap()),
        ),
        (
            "price",
            mongodb::bson::Bson::Decimal128("123456789.1".parse().unwrap()),
        ),
        (
            "price",
            mongodb::bson::Bson::Decimal128("NaN".parse().unwrap()),
        ),
        ("price", mongodb::bson::Bson::Double(1.5)),
        ("big", mongodb::bson::Bson::Double(1.0)),
        ("ratio", mongodb::bson::Bson::Int64(1)),
        ("big", mongodb::bson::Bson::String("1".into())),
    ] {
        let mut document = doc! { "_id": 1_i64 };
        document.insert(field, value.clone());
        assert!(
            build_put(&typed, &document).is_err(),
            "{field}={value:?} must not be coerced"
        );
    }
    assert_eq!(decimal_unscaled("1.2E+3", 10, 2), Some(120_000));
    assert_eq!(decimal_unscaled("-0.50", 10, 2), Some(-50));
    assert_eq!(decimal_unscaled("1.230", 10, 2), Some(123));
    assert_eq!(decimal_unscaled("Infinity", 10, 2), None);
}

#[test]
fn dates_binary_and_nested_values_map_exactly_or_fail() {
    let typed = projection(
        vec![
            Field::new("_id", DataType::Utf8, false),
            Field::new("at", DataType::Timestamp(TimeUnit::Microsecond, None), true),
            Field::new("blob", DataType::Binary, true),
            Field::new("nested", DataType::Utf8, true),
        ],
        &[],
        None,
    );
    let batch = build_put(
        &typed,
        &doc! {
            "_id": "a",
            "at": mongodb::bson::DateTime::from_millis(1_700_000_000_123),
            "blob": Binary { subtype: BinarySubtype::Generic, bytes: vec![1, 2] },
        },
    )
    .unwrap();
    let at = batch
        .column(1)
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap();
    assert_eq!(at.value(0), 1_700_000_000_123_000);
    let blob = batch
        .column(2)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    assert_eq!(blob.value(0), [1, 2]);

    let error = build_put(&typed, &doc! { "_id": "a", "nested": { "x": 1 } }).unwrap_err();
    assert!(
        error.to_string().contains("document.json.column"),
        "{error}"
    );
    let uuid = Binary {
        subtype: BinarySubtype::Uuid,
        bytes: vec![0; 16],
    };
    assert!(build_put(&typed, &doc! { "_id": "a", "blob": uuid }).is_err());
}

#[test]
fn json_column_preserves_the_whole_document_including_null_versus_missing() {
    let typed = projection(
        vec![
            Field::new("_id", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("doc", DataType::Utf8, true),
        ],
        &[],
        Some("doc"),
    );
    let batch = build_put(&typed, &doc! { "_id": "a", "name": null, "tags": ["x"] }).unwrap();
    assert!(batch.column(1).is_null(0));
    let doc = batch
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let value: serde_json::Value = serde_json::from_str(doc.value(0)).unwrap();
    assert!(value["name"].is_null(), "explicit null stays a JSON null");
    assert!(value.get("missing").is_none(), "missing fields stay absent");
    assert_eq!(value["tags"], serde_json::json!(["x"]));

    let key = raw(&doc! { "_id": "a" });
    let (tombstone, mutations) = typed
        .build(&[DocumentRow::Tombstone { key: &key }])
        .unwrap();
    assert_eq!(mutations, [SourceMutation::Tombstone]);
    assert!(tombstone.column(1).is_null(0));
    assert!(
        tombstone.column(2).is_null(0),
        "no document is fabricated for a delete"
    );
}

#[test]
fn projection_validation_rejects_unsafe_declarations() {
    let key = || Field::new("_id", DataType::Utf8, false);
    let cases: Vec<(Vec<Field>, Vec<String>, &str)> = vec![
        (vec![key()], vec![], "PRIMARY KEY"),
        (
            vec![key(), Field::new("name", DataType::Utf8, false)],
            vec!["name".into()],
            "must include _id",
        ),
        (
            vec![key(), Field::new("name", DataType::Utf8, false)],
            vec!["_id".into()],
            "must be nullable",
        ),
        (
            vec![key(), Field::new("_op", DataType::Utf8, true)],
            vec!["_id".into()],
            "reserved",
        ),
        (
            vec![key(), Field::new("a.b", DataType::Utf8, true)],
            vec!["_id".into()],
            "top-level",
        ),
        (
            vec![key(), Field::new("f", DataType::Float32, true)],
            vec!["_id".into()],
            "not supported",
        ),
    ];
    for (fields, primary_key, needle) in cases {
        let error =
            DocumentProjection::try_new(&schema(fields), &primary_key, &[], None).unwrap_err();
        assert!(error.to_string().contains(needle), "{needle}: {error}");
    }
    let error = DocumentProjection::try_new(
        &schema(vec![key()]),
        &["_id".to_string()],
        &["missing".to_string()],
        None,
    )
    .unwrap_err();
    assert!(error.to_string().contains("undeclared"), "{error}");
}
