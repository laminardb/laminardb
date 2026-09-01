use super::*;
use crate::postgres::cdc::types::{INT4_OID, INT8_OID, TEXT_OID};

fn sample_relation() -> RelationInfo {
    RelationInfo {
        relation_id: 16384,
        namespace: "public".to_string(),
        name: "users".to_string(),
        replica_identity: 'd',
        columns: vec![
            PgColumn::new("id".to_string(), INT8_OID, -1, true),
            PgColumn::new("name".to_string(), TEXT_OID, -1, false),
            PgColumn::new("age".to_string(), INT4_OID, -1, false),
        ],
    }
}

#[test]
fn test_relation_full_name_public() {
    let rel = sample_relation();
    assert_eq!(rel.full_name().unwrap(), "public.users");
}

#[test]
fn test_relation_full_name_custom_schema() {
    let mut rel = sample_relation();
    rel.namespace = "app".to_string();
    assert_eq!(rel.full_name().unwrap(), "app.users");
}

#[test]
fn test_relation_full_name_rejects_empty_components() {
    let mut rel = sample_relation();
    rel.namespace.clear();
    assert!(rel.full_name().is_err());
    rel.namespace = "public".into();
    rel.name.clear();
    assert!(rel.full_name().is_err());
}

#[test]
fn test_relation_cache() {
    let mut cache = RelationCache::new();
    assert!(cache.is_empty());

    cache.insert(sample_relation()).unwrap();
    assert_eq!(cache.len(), 1);
    assert!(cache.get(16384).is_some());
    assert!(cache.get(99999).is_none());
}

#[test]
fn test_cache_replace() {
    let mut cache = RelationCache::new();
    cache.insert(sample_relation()).unwrap();

    let mut updated = sample_relation();
    updated
        .columns
        .push(PgColumn::new("email".to_string(), TEXT_OID, -1, false));
    cache.insert(updated).unwrap();

    assert_eq!(cache.len(), 1);
    assert_eq!(cache.get(16384).unwrap().columns.len(), 4);
}

#[test]
fn test_cache_clear() {
    let mut cache = RelationCache::new();
    cache.insert(sample_relation()).unwrap();
    cache.clear();
    assert!(cache.is_empty());
}

#[test]
fn test_cdc_envelope_schema() {
    let schema = cdc_envelope_schema();
    assert_eq!(schema.fields().len(), 6);
    assert_eq!(schema.field(0).name(), "_table");
    assert_eq!(schema.field(1).name(), "_op");
    assert_eq!(schema.field(2).name(), "_lsn");
    assert_eq!(schema.field(3).name(), "_ts_ms");
    assert_eq!(schema.field(4).name(), "_before");
    assert_eq!(schema.field(5).name(), "_after");
    // _before and _after are nullable
    assert!(schema.field(4).is_nullable());
    assert!(schema.field(5).is_nullable());
}
