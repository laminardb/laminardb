use super::*;

#[test]
fn test_raw_record_builder() {
    let record = RawRecord::new(b"hello".to_vec())
        .with_key(b"k1".to_vec())
        .with_timestamp(1000)
        .with_header("content-type", b"application/json".to_vec());

    assert_eq!(record.key.as_deref(), Some(b"k1".as_slice()));
    assert_eq!(record.value, b"hello");
    assert_eq!(record.timestamp, Some(1000));
    assert!(record.headers.contains_key("content-type"));
}

#[test]
fn test_source_metadata_empty() {
    let meta = SourceMetadata::empty();
    assert!(meta.is_empty());
    assert!(meta.downcast_ref::<String>().is_none());
}

#[test]
fn test_source_metadata_typed() {
    let meta = SourceMetadata::new(42u64);
    assert!(!meta.is_empty());
    assert_eq!(meta.downcast_ref::<u64>(), Some(&42u64));
    assert!(meta.downcast_ref::<String>().is_none());
}

#[test]
fn test_source_metadata_debug() {
    let empty = SourceMetadata::empty();
    assert!(format!("{empty:?}").contains("empty"));

    let full = SourceMetadata::new("data");
    assert!(format!("{full:?}").contains("opaque"));
}

#[test]
fn test_field_meta_builder() {
    let meta = FieldMeta::new()
        .with_field_id(1)
        .with_description("User ID")
        .with_source_type("BIGINT")
        .with_default("0")
        .with_property("pii", "true");

    assert_eq!(meta.field_id, Some(1));
    assert_eq!(meta.description.as_deref(), Some("User ID"));
    assert_eq!(meta.source_type.as_deref(), Some("BIGINT"));
    assert_eq!(meta.default_expr.as_deref(), Some("0"));
    assert_eq!(meta.properties.get("pii").map(String::as_str), Some("true"));
}
