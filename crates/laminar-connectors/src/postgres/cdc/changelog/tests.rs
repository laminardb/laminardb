use super::*;
use crate::postgres::cdc::schema::RelationInfo;
use crate::postgres::cdc::types::PgColumn;
use crate::postgres::cdc::types::{INT8_OID, TEXT_OID};
use bytes::Bytes;

fn sample_relation() -> RelationInfo {
    RelationInfo {
        relation_id: 16384,
        namespace: "public".to_string(),
        name: "users".to_string(),
        replica_identity: 'd',
        columns: vec![
            PgColumn::new("id".to_string(), INT8_OID, -1, true),
            PgColumn::new("name".to_string(), TEXT_OID, -1, false),
        ],
    }
}

#[test]
fn test_tuple_to_json() {
    let relation = sample_relation();
    let tuple = TupleData {
        columns: vec![
            ColumnValue::Text(Bytes::from_static(b"42")),
            ColumnValue::Text(Bytes::from_static(b"Alice")),
        ],
    };

    let encoded_len = tuple_json_encoded_len(&tuple, &relation).unwrap();
    let json = tuple_to_json(&tuple, &relation, encoded_len).unwrap();
    assert_eq!(json.len(), encoded_len);
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed["id"], "42");
    assert_eq!(parsed["name"], "Alice");
}

#[test]
fn test_tuple_to_json_with_null() {
    let relation = sample_relation();
    let tuple = TupleData {
        columns: vec![
            ColumnValue::Text(Bytes::from_static(b"42")),
            ColumnValue::Null,
        ],
    };

    let encoded_len = tuple_json_encoded_len(&tuple, &relation).unwrap();
    let json = tuple_to_json(&tuple, &relation, encoded_len).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed["id"], "42");
    assert!(parsed["name"].is_null());
}

#[test]
fn test_tuple_to_json_unchanged_omitted() {
    let relation = sample_relation();
    let tuple = TupleData {
        columns: vec![
            ColumnValue::Text(Bytes::from_static(b"42")),
            ColumnValue::Unchanged,
        ],
    };

    let encoded_len = tuple_json_encoded_len(&tuple, &relation).unwrap();
    let json = tuple_to_json(&tuple, &relation, encoded_len).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed["id"], "42");
    // unchanged columns are omitted
    assert!(parsed.get("name").is_none());
}

#[test]
fn key_old_tuple_omits_unavailable_non_identity_fields() {
    let relation = RelationInfo {
        columns: vec![
            PgColumn::new("id".to_string(), INT8_OID, -1, true),
            PgColumn::new("name".to_string(), TEXT_OID, -1, false),
            PgColumn::new("note".to_string(), TEXT_OID, -1, false),
        ],
        ..sample_relation()
    };
    let old_tuple = OldTuple::Key(TupleData {
        columns: vec![
            ColumnValue::Text(Bytes::from_static(b"42")),
            ColumnValue::Text(Bytes::from_static(b"unavailable")),
            ColumnValue::Null,
        ],
    });

    let encoded_len = old_tuple_json_encoded_len(&old_tuple, &relation).unwrap();
    let json = old_tuple_to_json(&old_tuple, &relation, encoded_len).unwrap();

    assert_eq!(json, r#"{"id":"42"}"#);
    assert_eq!(json.len(), encoded_len);
}

#[test]
fn full_old_tuple_retains_non_key_fields_and_explicit_null() {
    let relation = sample_relation();
    let old_tuple = OldTuple::Full(TupleData {
        columns: vec![
            ColumnValue::Text(Bytes::from_static(b"42")),
            ColumnValue::Null,
        ],
    });

    let encoded_len = old_tuple_json_encoded_len(&old_tuple, &relation).unwrap();
    let json = old_tuple_to_json(&old_tuple, &relation, encoded_len).unwrap();

    assert_eq!(json, r#"{"id":"42","name":null}"#);
    assert_eq!(json.len(), encoded_len);
}

#[test]
fn key_old_tuple_requires_full_relation_cardinality() {
    let relation = sample_relation();
    let old_tuple = OldTuple::Key(TupleData {
        columns: vec![ColumnValue::Text(Bytes::from_static(b"42"))],
    });

    let error = old_tuple_json_encoded_len(&old_tuple, &relation).unwrap_err();
    assert!(error.to_string().contains("column count"), "{error}");
}

#[test]
fn test_events_to_record_batch_insert() {
    let events = vec![ChangeEvent {
        table: "users".to_string(),
        op: CdcOperation::Insert,
        lsn: Lsn::new(0x100),
        ts_ms: 1_700_000_000_000,
        before: None,
        after: Some(r#"{"id":"1","name":"Alice"}"#.to_string()),
    }];

    let plan = plan_record_batch(&events).unwrap();
    let batch = events_to_record_batch(events, &plan).unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.num_columns(), 6);
}

#[test]
fn test_events_to_record_batch_mixed() {
    let events = vec![
        ChangeEvent {
            table: "users".to_string(),
            op: CdcOperation::Insert,
            lsn: Lsn::new(0x100),
            ts_ms: 1_700_000_000_000,
            before: None,
            after: Some(r#"{"id":"1"}"#.to_string()),
        },
        ChangeEvent {
            table: "users".to_string(),
            op: CdcOperation::Update,
            lsn: Lsn::new(0x200),
            ts_ms: 1_700_000_000_001,
            before: Some(r#"{"id":"1","name":"Alice"}"#.to_string()),
            after: Some(r#"{"id":"1","name":"Bob"}"#.to_string()),
        },
        ChangeEvent {
            table: "users".to_string(),
            op: CdcOperation::Delete,
            lsn: Lsn::new(0x300),
            ts_ms: 1_700_000_000_002,
            before: Some(r#"{"id":"1"}"#.to_string()),
            after: None,
        },
    ];

    let plan = plan_record_batch(&events).unwrap();
    let batch = events_to_record_batch(events, &plan).unwrap();
    assert_eq!(batch.num_rows(), 3);
}

#[test]
fn test_events_to_record_batch_empty() {
    let events: Vec<ChangeEvent> = vec![];
    let plan = plan_record_batch(&events).unwrap();
    let batch = events_to_record_batch(events, &plan).unwrap();
    assert_eq!(batch.num_rows(), 0);
    assert_eq!(batch.num_columns(), 6);
}

#[test]
fn test_cdc_operation_as_str() {
    assert_eq!(CdcOperation::Insert.as_str(), "I");
    assert_eq!(CdcOperation::Update.as_str(), "U");
    assert_eq!(CdcOperation::Delete.as_str(), "D");
}

#[test]
fn json_preflight_matches_all_escape_classes() {
    let relation = RelationInfo {
        columns: vec![PgColumn::new(
            "control\nkey".to_string(),
            TEXT_OID,
            -1,
            false,
        )],
        ..sample_relation()
    };
    let tuple = TupleData {
        columns: vec![ColumnValue::Text(Bytes::from_static(
            b"quote\" slash\\ newline\n tab\t",
        ))],
    };
    let encoded_len = tuple_json_encoded_len(&tuple, &relation).unwrap();
    let json = tuple_to_json(&tuple, &relation, encoded_len).unwrap();
    assert_eq!(json.len(), encoded_len);
    serde_json::from_str::<serde_json::Value>(&json).unwrap();
}

#[test]
fn json_preflight_rejects_invalid_text_and_column_count_drift() {
    let relation = sample_relation();
    let invalid_text = TupleData {
        columns: vec![
            ColumnValue::Text(Bytes::from_static(&[0xff])),
            ColumnValue::Null,
        ],
    };
    assert!(tuple_json_encoded_len(&invalid_text, &relation)
        .unwrap_err()
        .to_string()
        .contains("UTF-8"));

    let truncated = TupleData {
        columns: vec![ColumnValue::Null],
    };
    assert!(tuple_json_encoded_len(&truncated, &relation)
        .unwrap_err()
        .to_string()
        .contains("column count"));
}

#[test]
fn arrow_plan_covers_actual_retained_buffers() {
    let events = vec![
        ChangeEvent {
            table: "public.users".into(),
            op: CdcOperation::Insert,
            lsn: Lsn::new(1),
            ts_ms: 1,
            before: None,
            after: Some("{\"id\":\"1\"}".into()),
        },
        ChangeEvent {
            table: "public.users".into(),
            op: CdcOperation::Update,
            lsn: Lsn::new(2),
            ts_ms: 2,
            before: Some("{\"id\":\"1\"}".into()),
            after: Some("{\"id\":\"2\"}".into()),
        },
    ];
    let plan = plan_record_batch(&events).unwrap();
    let planned = plan.retained_bytes;
    let batch = events_to_record_batch(events, &plan).unwrap();
    let actual = batch
        .columns()
        .iter()
        .map(|column| column.get_buffer_memory_size())
        .sum::<usize>();
    assert!(actual <= planned, "{actual} > {planned}");
}
