//! MySQL table schema cache.
//!
//! Caches TABLE_MAP_EVENT information for resolving row events
//! to their corresponding table schemas.

use std::collections::HashMap;

use arrow_schema::{DataType, Field, Schema};

use super::decoder::TableMapMessage;
use super::types::MySqlColumn;

/// Table schema information from TABLE_MAP_EVENT.
#[derive(Debug, Clone)]
pub struct TableInfo {
    /// Table ID (internal MySQL identifier).
    pub table_id: u64,
    /// Database name.
    pub database: String,
    /// Table name.
    pub table: String,
    /// Column definitions.
    pub columns: Vec<MySqlColumn>,
    /// Arrow schema derived from columns.
    pub arrow_schema: Schema,
}

impl TableInfo {
    /// Creates a new table info from a TABLE_MAP message.
    #[must_use]
    pub fn from_table_map(msg: &TableMapMessage) -> Self {
        let fields: Vec<Field> = msg
            .columns
            .iter()
            .map(|col| Field::new(&col.name, col.to_arrow_type(), col.nullable))
            .collect();

        Self {
            table_id: msg.table_id,
            database: msg.database.clone(),
            table: msg.table.clone(),
            columns: msg.columns.clone(),
            arrow_schema: Schema::new(fields),
        }
    }

    /// Returns the fully-qualified table name.
    #[must_use]
    pub fn full_name(&self) -> String {
        format!("{}.{}", self.database, self.table)
    }

    /// Returns the number of columns.
    #[must_use]
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    /// Finds a column by name.
    #[must_use]
    pub fn find_column(&self, name: &str) -> Option<(usize, &MySqlColumn)> {
        self.columns
            .iter()
            .enumerate()
            .find(|(_, c)| c.name == name)
    }
}

/// Cache of table schemas indexed by table ID.
///
/// MySQL sends TABLE_MAP_EVENT before row events to describe the schema.
/// This cache stores the mappings for efficient lookup.
#[derive(Debug, Default)]
pub struct TableCache {
    /// Table ID → TableInfo mapping.
    by_id: HashMap<u64, TableInfo>,
    /// (database, table) → table_id mapping.
    by_name: HashMap<(String, String), u64>,
}

impl TableCache {
    /// Creates a new empty cache.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Updates the cache with a TABLE_MAP message.
    pub fn update(&mut self, msg: &TableMapMessage) {
        let info = TableInfo::from_table_map(msg);
        let key = (msg.database.clone(), msg.table.clone());

        self.by_name.insert(key, msg.table_id);
        self.by_id.insert(msg.table_id, info);
    }

    /// Gets table info by table ID.
    #[must_use]
    pub fn get(&self, table_id: u64) -> Option<&TableInfo> {
        self.by_id.get(&table_id)
    }

    /// Gets table info by database and table name.
    #[must_use]
    pub fn get_by_name(&self, database: &str, table: &str) -> Option<&TableInfo> {
        let key = (database.to_string(), table.to_string());
        self.by_name.get(&key).and_then(|id| self.by_id.get(id))
    }

    /// Returns the number of cached tables.
    #[must_use]
    pub fn len(&self) -> usize {
        self.by_id.len()
    }

    /// Returns true if the cache is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.by_id.is_empty()
    }

    /// Clears the cache.
    pub fn clear(&mut self) {
        self.by_id.clear();
        self.by_name.clear();
    }

    /// Returns all cached table names.
    #[must_use]
    pub fn table_names(&self) -> Vec<String> {
        self.by_id.values().map(TableInfo::full_name).collect()
    }
}

/// Builds the CDC envelope schema for MySQL CDC records.
///
/// The envelope wraps the actual row data with CDC metadata:
/// - `_table`: Source table name
/// - `_op`: Operation type (I/U/D)
/// - `_ts_ms`: Event timestamp
/// - `_binlog_file`: Binlog filename
/// - `_binlog_pos`: Position in binlog
/// - `_gtid`: GTID (if available)
/// - `_before`: Before image (for updates/deletes)
/// - `_after`: After image (for inserts/updates)
#[must_use]
pub fn cdc_envelope_schema(table_schema: &Schema) -> Schema {
    let before_fields = table_schema
        .fields()
        .iter()
        .map(|f| Field::new(f.name(), f.data_type().clone(), true))
        .collect::<Vec<_>>();

    let after_fields = table_schema
        .fields()
        .iter()
        .map(|f| Field::new(f.name(), f.data_type().clone(), true))
        .collect::<Vec<_>>();

    Schema::new(vec![
        Field::new("_table", DataType::Utf8, false),
        Field::new("_op", DataType::Utf8, false),
        Field::new("_ts_ms", DataType::Int64, false),
        Field::new("_binlog_file", DataType::Utf8, true),
        Field::new("_binlog_pos", DataType::UInt64, true),
        Field::new("_gtid", DataType::Utf8, true),
        Field::new("_before", DataType::Struct(before_fields.into()), true),
        Field::new("_after", DataType::Struct(after_fields.into()), true),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cdc::mysql::types::mysql_type;

    fn make_test_columns() -> Vec<MySqlColumn> {
        vec![
            MySqlColumn::new("id".to_string(), mysql_type::LONGLONG, 0, false, false),
            MySqlColumn::new("name".to_string(), mysql_type::VARCHAR, 255, true, false),
            MySqlColumn::new(
                "created_at".to_string(),
                mysql_type::TIMESTAMP,
                0,
                true,
                false,
            ),
        ]
    }

    fn make_test_table_map() -> TableMapMessage {
        TableMapMessage {
            table_id: 100,
            database: "testdb".to_string(),
            table: "users".to_string(),
            columns: make_test_columns(),
        }
    }

    #[test]
    fn test_table_info_from_table_map() {
        let msg = make_test_table_map();
        let info = TableInfo::from_table_map(&msg);

        assert_eq!(info.table_id, 100);
        assert_eq!(info.database, "testdb");
        assert_eq!(info.table, "users");
        assert_eq!(info.column_count(), 3);
        assert_eq!(info.full_name(), "testdb.users");
    }

    #[test]
    fn test_table_info_arrow_schema() {
        let msg = make_test_table_map();
        let info = TableInfo::from_table_map(&msg);

        assert_eq!(info.arrow_schema.fields().len(), 3);
        assert_eq!(info.arrow_schema.field(0).name(), "id");
        assert_eq!(info.arrow_schema.field(0).data_type(), &DataType::Int64);
        assert!(!info.arrow_schema.field(0).is_nullable());

        assert_eq!(info.arrow_schema.field(1).name(), "name");
        assert_eq!(info.arrow_schema.field(1).data_type(), &DataType::Utf8);
        assert!(info.arrow_schema.field(1).is_nullable());
    }

    #[test]
    fn test_table_info_find_column() {
        let msg = make_test_table_map();
        let info = TableInfo::from_table_map(&msg);

        let (idx, col) = info.find_column("name").unwrap();
        assert_eq!(idx, 1);
        assert_eq!(col.name, "name");

        assert!(info.find_column("nonexistent").is_none());
    }

    #[test]
    fn test_table_cache_empty() {
        let cache = TableCache::new();
        assert!(cache.is_empty());
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn test_table_cache_update_and_get() {
        let mut cache = TableCache::new();
        let msg = make_test_table_map();

        cache.update(&msg);

        assert!(!cache.is_empty());
        assert_eq!(cache.len(), 1);

        let info = cache.get(100).unwrap();
        assert_eq!(info.table, "users");
    }

    #[test]
    fn test_table_cache_get_by_name() {
        let mut cache = TableCache::new();
        let msg = make_test_table_map();
        cache.update(&msg);

        let info = cache.get_by_name("testdb", "users").unwrap();
        assert_eq!(info.table_id, 100);

        assert!(cache.get_by_name("testdb", "nonexistent").is_none());
    }

    #[test]
    fn test_table_cache_clear() {
        let mut cache = TableCache::new();
        cache.update(&make_test_table_map());
        assert_eq!(cache.len(), 1);

        cache.clear();
        assert!(cache.is_empty());
    }

    #[test]
    fn test_table_cache_table_names() {
        let mut cache = TableCache::new();
        cache.update(&make_test_table_map());

        let names = cache.table_names();
        assert_eq!(names, vec!["testdb.users"]);
    }

    #[test]
    fn test_table_cache_multiple_tables() {
        let mut cache = TableCache::new();

        cache.update(&TableMapMessage {
            table_id: 100,
            database: "db1".to_string(),
            table: "users".to_string(),
            columns: make_test_columns(),
        });

        cache.update(&TableMapMessage {
            table_id: 101,
            database: "db1".to_string(),
            table: "orders".to_string(),
            columns: make_test_columns(),
        });

        assert_eq!(cache.len(), 2);
        assert!(cache.get(100).is_some());
        assert!(cache.get(101).is_some());
    }

    #[test]
    fn test_cdc_envelope_schema() {
        let msg = make_test_table_map();
        let info = TableInfo::from_table_map(&msg);
        let envelope = cdc_envelope_schema(&info.arrow_schema);

        assert_eq!(envelope.fields().len(), 8);
        assert_eq!(envelope.field(0).name(), "_table");
        assert_eq!(envelope.field(1).name(), "_op");
        assert_eq!(envelope.field(2).name(), "_ts_ms");
        assert_eq!(envelope.field(3).name(), "_binlog_file");
        assert_eq!(envelope.field(4).name(), "_binlog_pos");
        assert_eq!(envelope.field(5).name(), "_gtid");
        assert_eq!(envelope.field(6).name(), "_before");
        assert_eq!(envelope.field(7).name(), "_after");

        // Verify struct fields
        if let DataType::Struct(fields) = envelope.field(6).data_type() {
            assert_eq!(fields.len(), 3);
        } else {
            panic!("_before should be a struct");
        }
    }
}
