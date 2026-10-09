use super::*;
use mongodb::bson::{doc, oid::ObjectId, Decimal128, RawDocumentBuf};

#[test]
fn operation_classes_cover_every_documented_change_event() {
    for (operation, class) in [
        ("insert", ChangeOperation::Insert),
        ("update", ChangeOperation::Update),
        ("replace", ChangeOperation::Replace),
        ("delete", ChangeOperation::Delete),
        ("invalidate", ChangeOperation::Invalidate),
        ("drop", ChangeOperation::Drop),
        ("rename", ChangeOperation::Rename),
        ("dropDatabase", ChangeOperation::DropDatabase),
        ("create", ChangeOperation::Metadata),
        ("createIndexes", ChangeOperation::Metadata),
        ("dropIndexes", ChangeOperation::Metadata),
        ("modify", ChangeOperation::Metadata),
        ("shardCollection", ChangeOperation::Metadata),
        ("refineCollectionShardKey", ChangeOperation::Metadata),
        ("reshardCollection", ChangeOperation::Metadata),
        ("futureOperation", ChangeOperation::Unknown),
        ("I", ChangeOperation::Unknown),
    ] {
        assert_eq!(ChangeOperation::classify(operation), class, "{operation}");
    }
}

#[test]
fn canonical_extended_json_keeps_bson_types_distinct() {
    let id = ObjectId::parse_str("65a1b2c3d4e5f60718293a4b").unwrap();
    let document = RawDocumentBuf::from_document(&doc! {
        "oid": id,
        "hex": "65a1b2c3d4e5f60718293a4b",
        "small_long": 5_i64,
        "small_int": 5_i32,
        "big": 9_007_199_254_740_993_i64,
        "price": "12.345".parse::<Decimal128>().unwrap(),
        "at": mongodb::bson::DateTime::from_millis(1_700_000_000_123),
        "nothing": mongodb::bson::Bson::Null,
        "nested": { "list": [1_i32, "a"] },
    })
    .unwrap();
    let text = canonical_document_extjson(&document).unwrap();
    let value: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert_eq!(
        value["oid"],
        serde_json::json!({"$oid": "65a1b2c3d4e5f60718293a4b"})
    );
    assert_eq!(value["hex"], serde_json::json!("65a1b2c3d4e5f60718293a4b"));
    assert_eq!(value["small_long"], serde_json::json!({"$numberLong": "5"}));
    assert_eq!(value["small_int"], serde_json::json!({"$numberInt": "5"}));
    assert_eq!(
        value["big"],
        serde_json::json!({"$numberLong": "9007199254740993"})
    );
    assert_eq!(
        value["price"],
        serde_json::json!({"$numberDecimal": "12.345"})
    );
    assert_eq!(
        value["at"],
        serde_json::json!({"$date": {"$numberLong": "1700000000123"}})
    );
    assert!(value["nothing"].is_null());
    assert!(value.get("missing").is_none());

    let back = mongodb::bson::Bson::try_from(value).unwrap();
    assert_eq!(
        back.as_document().unwrap(),
        &document.to_document().unwrap(),
        "canonical Extended JSON must round-trip exactly"
    );
}
