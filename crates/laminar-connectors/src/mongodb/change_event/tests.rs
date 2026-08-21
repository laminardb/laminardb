use super::*;

#[test]
fn test_operation_type_as_str() {
    assert_eq!(OperationType::Insert.as_str(), "I");
    assert_eq!(OperationType::Update.as_str(), "U");
    assert_eq!(OperationType::Replace.as_str(), "R");
    assert_eq!(OperationType::Delete.as_str(), "D");
    assert_eq!(OperationType::Drop.as_str(), "DROP");
    assert_eq!(OperationType::Rename.as_str(), "RENAME");
    assert_eq!(OperationType::Invalidate.as_str(), "INVALIDATE");
}

#[test]
fn test_namespace_full_name() {
    let ns = Namespace {
        db: "mydb".to_string(),
        coll: "users".to_string(),
    };
    assert_eq!(ns.full_name(), "mydb.users");
    assert_eq!(ns.to_string(), "mydb.users");
}

#[test]
fn test_update_description_default() {
    let ud = UpdateDescription::default();
    assert!(ud.updated_fields.is_empty());
    assert!(ud.removed_fields.is_empty());
    assert!(ud.truncated_arrays.is_empty());
    assert!(ud.disambiguated_paths.is_empty());
}

#[test]
fn test_change_event_serde_roundtrip() {
    let event = MongoDbChangeEvent {
        operation_type: OperationType::Insert,
        namespace: Namespace {
            db: "test".to_string(),
            coll: "docs".to_string(),
        },
        document_key: r#"{"_id": "abc123"}"#.to_string(),
        full_document: Some(r#"{"_id": "abc123", "name": "Alice"}"#.to_string()),
        update_description: None,
        cluster_time_secs: 1_700_000_000,
        cluster_time_inc: 1,
        resume_token: r#"{"_data": "token123"}"#.to_string(),
        wall_time_ms: 1_700_000_000_000,
    };

    let json = serde_json::to_string(&event).unwrap();
    let deserialized: MongoDbChangeEvent = serde_json::from_str(&json).unwrap();
    assert_eq!(deserialized.operation_type, OperationType::Insert);
    assert_eq!(deserialized.namespace.db, "test");
}
