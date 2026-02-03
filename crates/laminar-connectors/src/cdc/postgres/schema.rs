//! `PostgreSQL` relation schema cache.
//!
//! Caches the schema (column metadata) for each relation received in
//! `pgoutput` Relation messages. Required because DML messages only
//! reference relations by OID --- the schema must be looked up from this cache.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, SchemaRef};

use super::types::PgColumn;

/// Cached information about a `PostgreSQL` relation (table).
#[derive(Debug, Clone)]
pub struct RelationInfo {
    /// The relation OID from `pgoutput`.
    pub relation_id: u32,

    /// Schema (namespace) name.
    pub namespace: String,

    /// Table name.
    pub name: String,

    /// Replica identity setting: 'd' (default), 'n' (nothing),
    /// 'f' (full), 'i' (index).
    pub replica_identity: char,

    /// Column descriptors in ordinal order.
    pub columns: Vec<PgColumn>,
}

impl RelationInfo {
    /// Returns the fully qualified table name: `namespace.name`.
    #[must_use]
    pub fn full_name(&self) -> String {
        if self.namespace.is_empty() || self.namespace == "public" {
            self.name.clone()
        } else {
            format!("{}.{}", self.namespace, self.name)
        }
    }

    /// Generates an Arrow schema from the relation's columns.
    #[must_use]
    pub fn arrow_schema(&self) -> SchemaRef {
        let fields: Vec<Field> = self
            .columns
            .iter()
            .map(|col| {
                // All CDC columns are nullable since DELETE may have
                // unchanged TOAST values or partial replica identity.
                Field::new(&col.name, col.arrow_type(), true)
            })
            .collect();
        Arc::new(Schema::new(fields))
    }

    /// Returns the key column names based on replica identity.
    #[must_use]
    pub fn key_columns(&self) -> Vec<&str> {
        self.columns
            .iter()
            .filter(|c| c.is_key)
            .map(|c| c.name.as_str())
            .collect()
    }
}

/// Cache of relation schemas received from `pgoutput` Relation messages.
///
/// The decoder populates this cache as it encounters Relation messages.
/// DML decoders look up column metadata by relation ID.
#[derive(Debug, Clone, Default)]
pub struct RelationCache {
    relations: HashMap<u32, RelationInfo>,
}

impl RelationCache {
    /// Creates an empty relation cache.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds or replaces a relation in the cache.
    pub fn insert(&mut self, info: RelationInfo) {
        self.relations.insert(info.relation_id, info);
    }

    /// Looks up a relation by its OID.
    #[must_use]
    pub fn get(&self, relation_id: u32) -> Option<&RelationInfo> {
        self.relations.get(&relation_id)
    }

    /// Returns the number of cached relations.
    #[must_use]
    pub fn len(&self) -> usize {
        self.relations.len()
    }

    /// Returns `true` if no relations are cached.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.relations.is_empty()
    }

    /// Clears the cache.
    pub fn clear(&mut self) {
        self.relations.clear();
    }
}

/// Builds the CDC envelope schema used by [`PostgresCdcSource`](super::source::PostgresCdcSource).
///
/// This schema wraps change events in a uniform envelope with metadata
/// columns, making it compatible with any table structure.
#[must_use]
pub fn cdc_envelope_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("_table", DataType::Utf8, false),
        Field::new("_op", DataType::Utf8, false),
        Field::new("_lsn", DataType::UInt64, false),
        Field::new("_ts_ms", DataType::Int64, false),
        Field::new("_before", DataType::Utf8, true),
        Field::new("_after", DataType::Utf8, true),
    ]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cdc::postgres::types::{INT4_OID, INT8_OID, TEXT_OID};

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
        assert_eq!(rel.full_name(), "users");
    }

    #[test]
    fn test_relation_full_name_custom_schema() {
        let mut rel = sample_relation();
        rel.namespace = "app".to_string();
        assert_eq!(rel.full_name(), "app.users");
    }

    #[test]
    fn test_arrow_schema_generation() {
        let rel = sample_relation();
        let schema = rel.arrow_schema();
        assert_eq!(schema.fields().len(), 3);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(*schema.field(0).data_type(), DataType::Int64);
        assert_eq!(schema.field(1).name(), "name");
        assert_eq!(*schema.field(1).data_type(), DataType::Utf8);
    }

    #[test]
    fn test_key_columns() {
        let rel = sample_relation();
        let keys = rel.key_columns();
        assert_eq!(keys, vec!["id"]);
    }

    #[test]
    fn test_relation_cache() {
        let mut cache = RelationCache::new();
        assert!(cache.is_empty());

        cache.insert(sample_relation());
        assert_eq!(cache.len(), 1);
        assert!(cache.get(16384).is_some());
        assert!(cache.get(99999).is_none());
    }

    #[test]
    fn test_cache_replace() {
        let mut cache = RelationCache::new();
        cache.insert(sample_relation());

        let mut updated = sample_relation();
        updated
            .columns
            .push(PgColumn::new("email".to_string(), TEXT_OID, -1, false));
        cache.insert(updated);

        assert_eq!(cache.len(), 1);
        assert_eq!(cache.get(16384).unwrap().columns.len(), 4);
    }

    #[test]
    fn test_cache_clear() {
        let mut cache = RelationCache::new();
        cache.insert(sample_relation());
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
}
