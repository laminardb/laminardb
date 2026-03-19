//! Redis lookup source implementation.
//!
//! Provides a `LookupSource` backed by Redis, supporting both hash-based
//! and string-based key lookups. Data is loaded via `SCAN` + `HGETALL`
//! (for hashes) or `SCAN` + `GET` (for strings) and stored in an
//! in-memory `HashMap` for fast lookups.
//!
//! ## Usage
//!
//! ```rust,no_run
//! use std::sync::Arc;
//! use arrow_schema::{DataType, Field, Schema};
//! use laminar_connectors::lookup::redis_source::{
//!     RedisLookupSource, RedisLookupSourceConfig, RedisValueType,
//! };
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let config = RedisLookupSourceConfig {
//!     url: "redis://127.0.0.1:6379".into(),
//!     key_pattern: "user:*".into(),
//!     key_column: "user_id".into(),
//!     value_type: RedisValueType::Hash,
//!     ..Default::default()
//! };
//! let schema = Arc::new(Schema::new(vec![
//!     Field::new("user_id", DataType::Utf8, false),
//!     Field::new("name", DataType::Utf8, true),
//!     Field::new("email", DataType::Utf8, true),
//! ]));
//! let source = RedisLookupSource::connect(config, schema).await?;
//! # Ok(())
//! # }
//! ```

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;

use laminar_core::lookup::predicate::Predicate;
use laminar_core::lookup::source::{ColumnId, LookupError, LookupSource, LookupSourceCapabilities};

/// How Redis values are stored for lookup table entries.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum RedisValueType {
    /// Each key maps to a Redis Hash (`HGETALL`).
    /// Hash field names map to Arrow schema column names.
    #[default]
    Hash,
    /// Each key maps to a JSON string (`GET`).
    /// The JSON object fields map to Arrow schema column names.
    String,
}

impl RedisValueType {
    /// Parse from a string (case-insensitive).
    ///
    /// # Errors
    ///
    /// Returns [`LookupError::Internal`] if the value is unknown.
    pub fn parse(s: &str) -> Result<Self, LookupError> {
        match s.to_lowercase().as_str() {
            "hash" | "hashes" => Ok(Self::Hash),
            "string" | "strings" | "json" => Ok(Self::String),
            other => Err(LookupError::Internal(format!(
                "unknown redis value type: '{other}' (expected: hash, string)"
            ))),
        }
    }
}

/// Configuration for [`RedisLookupSource`].
#[derive(Clone)]
pub struct RedisLookupSourceConfig {
    /// Redis connection URL (e.g., `redis://127.0.0.1:6379/0`).
    pub url: String,

    /// Key pattern for `SCAN` (e.g., `user:*`, `product:*`).
    /// Uses Redis glob-style patterns.
    pub key_pattern: String,

    /// Name of the column that holds the key extracted from each Redis key.
    /// The key is extracted by stripping the pattern prefix (e.g., `user:123` -> `123`).
    pub key_column: String,

    /// How values are stored in Redis.
    pub value_type: RedisValueType,

    /// Number of keys per `SCAN` iteration (default: 1000).
    pub scan_count: usize,

    /// Maximum batch size for lookups (default: 1000).
    pub max_batch_size: usize,

    /// Database number (default: 0).
    pub db: u16,
}

impl fmt::Debug for RedisLookupSourceConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Redact the full userinfo component (username + password) to prevent credential leaks.
        let redacted_url = match url::Url::parse(&self.url) {
            Ok(mut parsed) => {
                if !parsed.username().is_empty() || parsed.password().is_some() {
                    let _ = parsed.set_username("***");
                    let _ = parsed.set_password(Some("***"));
                }
                parsed.to_string()
            }
            Err(_) => "***".to_string(),
        };
        f.debug_struct("RedisLookupSourceConfig")
            .field("url", &redacted_url)
            .field("key_pattern", &self.key_pattern)
            .field("key_column", &self.key_column)
            .field("value_type", &self.value_type)
            .field("scan_count", &self.scan_count)
            .field("max_batch_size", &self.max_batch_size)
            .field("db", &self.db)
            .finish()
    }
}

impl Default for RedisLookupSourceConfig {
    fn default() -> Self {
        Self {
            url: String::new(),
            key_pattern: String::new(),
            key_column: String::new(),
            value_type: RedisValueType::Hash,
            scan_count: 1000,
            max_batch_size: 1000,
            db: 0,
        }
    }
}

/// A `LookupSource` backed by Redis.
///
/// Loads data from Redis at construction time using `SCAN` and stores
/// it in an in-memory `HashMap`. Lookups are served from memory.
///
/// For hash-typed keys, each Redis hash field is mapped to an Arrow
/// column. For string-typed keys, the value is parsed as JSON and
/// fields are mapped to columns.
pub struct RedisLookupSource {
    config: RedisLookupSourceConfig,
    /// Key bytes to single-row `RecordBatch`.
    data: HashMap<Vec<u8>, RecordBatch>,
    /// Arrow schema of the output.
    schema: SchemaRef,
    /// Number of rows loaded.
    row_count: u64,
    /// Redis client for health checks and refresh.
    client: redis::Client,
}

impl fmt::Debug for RedisLookupSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RedisLookupSource")
            .field("config", &self.config)
            .field("data_len", &self.data.len())
            .field("schema", &self.schema)
            .field("row_count", &self.row_count)
            .finish_non_exhaustive()
    }
}

/// Derive the key suffix from a full Redis key given the pattern prefix.
///
/// For pattern `user:*`, key `user:123` -> `123`.
/// For pattern `*`, key `foo` -> `foo`.
fn extract_key_suffix(key: &str, pattern: &str) -> String {
    // Strip the prefix before '*' and the suffix after '*' from the key.
    // e.g., pattern "user:*:profile" + key "user:123:profile" → "123"
    if let Some(star_pos) = pattern.find('*') {
        let prefix = &pattern[..star_pos];
        let suffix = &pattern[star_pos + 1..];
        let stripped = key.strip_prefix(prefix).unwrap_or(key);
        let stripped = stripped.strip_suffix(suffix).unwrap_or(stripped);
        stripped.to_string()
    } else {
        key.to_string()
    }
}

/// Build a single-row `RecordBatch` from a map of `field_name` to `string_value`,
/// using the Arrow schema for type coercion. The key column is populated from
/// `key_value`.
fn fields_to_record_batch(
    schema: &SchemaRef,
    key_column: &str,
    key_value: &str,
    fields: &HashMap<String, String>,
) -> Result<RecordBatch, LookupError> {
    use arrow_array::{
        BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, StringArray,
    };
    use arrow_schema::DataType;

    let mut columns: Vec<Arc<dyn arrow_array::Array>> = Vec::with_capacity(schema.fields().len());

    for field in schema.fields() {
        let col_name = field.name().as_str();
        let raw_value = if col_name == key_column {
            Some(key_value.to_string())
        } else {
            fields.get(col_name).cloned()
        };

        let array: Arc<dyn arrow_array::Array> = match (field.data_type(), &raw_value) {
            (DataType::Utf8, _) => Arc::new(StringArray::from(vec![raw_value.as_deref()])),
            (DataType::LargeUtf8, _) => Arc::new(arrow_array::LargeStringArray::from(vec![
                raw_value.as_deref(),
            ])),
            (DataType::Int16, Some(v)) => {
                let parsed: Option<i16> = v.parse().ok();
                Arc::new(Int16Array::from(vec![parsed]))
            }
            (DataType::Int32, Some(v)) => {
                let parsed: Option<i32> = v.parse().ok();
                Arc::new(Int32Array::from(vec![parsed]))
            }
            (DataType::Int64, Some(v)) => {
                let parsed: Option<i64> = v.parse().ok();
                Arc::new(Int64Array::from(vec![parsed]))
            }
            (DataType::Float32, Some(v)) => {
                let parsed: Option<f32> = v.parse().ok();
                Arc::new(Float32Array::from(vec![parsed]))
            }
            (DataType::Float64, Some(v)) => {
                let parsed: Option<f64> = v.parse().ok();
                Arc::new(Float64Array::from(vec![parsed]))
            }
            (DataType::Boolean, Some(v)) => {
                let parsed: Option<bool> = match v.to_lowercase().as_str() {
                    "true" | "1" | "yes" => Some(true),
                    "false" | "0" | "no" => Some(false),
                    _ => None,
                };
                Arc::new(BooleanArray::from(vec![parsed]))
            }
            // NULL for missing or unparseable values
            (dt, _) => arrow_array::new_null_array(dt, 1),
        };
        columns.push(array);
    }

    RecordBatch::try_new(Arc::clone(schema), columns)
        .map_err(|e| LookupError::Internal(format!("build row batch: {e}")))
}

impl RedisLookupSource {
    /// Connect to Redis and load all matching keys into memory.
    ///
    /// Uses `SCAN` with the configured key pattern to discover keys,
    /// then loads values based on the configured value type.
    ///
    /// # Errors
    ///
    /// Returns [`LookupError::Connection`] if Redis is unreachable,
    /// or [`LookupError::Internal`] for data conversion errors.
    pub async fn connect(
        config: RedisLookupSourceConfig,
        schema: SchemaRef,
    ) -> Result<Self, LookupError> {
        let client = redis::Client::open(config.url.as_str())
            .map_err(|e| LookupError::Connection(format!("redis client: {e}")))?;

        let mut con = client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| LookupError::Connection(format!("redis connect: {e}")))?;

        // SELECT the configured database if non-zero.
        if config.db != 0 {
            redis::cmd("SELECT")
                .arg(config.db)
                .query_async::<()>(&mut con)
                .await
                .map_err(|e| {
                    LookupError::Connection(format!("SELECT db {} failed: {e}", config.db))
                })?;
        }

        let mut data = HashMap::new();
        let mut row_count = 0u64;

        // SCAN to discover keys matching the pattern.
        let mut cursor: u64 = 0;
        loop {
            let (next_cursor, keys): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(&config.key_pattern)
                .arg("COUNT")
                .arg(config.scan_count)
                .query_async(&mut con)
                .await
                .map_err(|e| LookupError::Query(format!("SCAN failed: {e}")))?;

            for key in &keys {
                let key_suffix = extract_key_suffix(key, &config.key_pattern);

                let fields_map: HashMap<String, String> = match config.value_type {
                    RedisValueType::Hash => redis::cmd("HGETALL")
                        .arg(key)
                        .query_async(&mut con)
                        .await
                        .map_err(|e| LookupError::Query(format!("HGETALL {key} failed: {e}")))?,
                    RedisValueType::String => {
                        let val: Option<String> = redis::cmd("GET")
                            .arg(key)
                            .query_async(&mut con)
                            .await
                            .map_err(|e| LookupError::Query(format!("GET {key} failed: {e}")))?;

                        match val {
                            Some(json_str) => {
                                match serde_json::from_str::<HashMap<String, serde_json::Value>>(
                                    &json_str,
                                ) {
                                    Ok(json_map) => json_map
                                        .into_iter()
                                        .filter_map(|(k, v)| {
                                            let s = match v {
                                                serde_json::Value::String(s) => s,
                                                serde_json::Value::Null => return None,
                                                other => other.to_string(),
                                            };
                                            Some((k, s))
                                        })
                                        .collect(),
                                    Err(e) => {
                                        tracing::warn!(
                                            key = %key,
                                            error = %e,
                                            "Failed to parse Redis JSON value, skipping key"
                                        );
                                        continue;
                                    }
                                }
                            }
                            None => HashMap::new(),
                        }
                    }
                };

                let batch =
                    fields_to_record_batch(&schema, &config.key_column, &key_suffix, &fields_map)?;

                data.insert(key_suffix.as_bytes().to_vec(), batch);
                row_count += 1;
            }

            cursor = next_cursor;
            if cursor == 0 {
                break;
            }
        }

        tracing::info!(
            pattern = %config.key_pattern,
            rows = row_count,
            "Redis lookup source loaded"
        );

        Ok(Self {
            config,
            data,
            schema,
            row_count,
            client,
        })
    }

    /// Create a `RedisLookupSource` from pre-loaded data (for testing).
    #[cfg(any(test, feature = "testing"))]
    pub fn from_data(
        config: RedisLookupSourceConfig,
        schema: SchemaRef,
        data: HashMap<Vec<u8>, RecordBatch>,
    ) -> Result<Self, LookupError> {
        let client = redis::Client::open("redis://127.0.0.1:6379")
            .map_err(|e| LookupError::Connection(format!("redis client: {e}")))?;
        let row_count = data.len() as u64;
        Ok(Self {
            config,
            data,
            schema,
            row_count,
            client,
        })
    }
}

impl LookupSource for RedisLookupSource {
    fn query(
        &self,
        keys: &[&[u8]],
        _predicates: &[Predicate],
        _projection: &[ColumnId],
    ) -> impl std::future::Future<Output = Result<Vec<Option<RecordBatch>>, LookupError>> + Send
    {
        let results: Vec<Option<RecordBatch>> = keys
            .iter()
            .map(|k| self.data.get::<[u8]>(k).cloned())
            .collect();
        async move { Ok(results) }
    }

    fn capabilities(&self) -> LookupSourceCapabilities {
        LookupSourceCapabilities {
            supports_predicate_pushdown: false,
            supports_projection_pushdown: false,
            supports_batch_lookup: true,
            max_batch_size: self.config.max_batch_size,
        }
    }

    #[allow(clippy::unnecessary_literal_bound)]
    fn source_name(&self) -> &str {
        "redis"
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn estimated_row_count(&self) -> Option<u64> {
        Some(self.row_count)
    }

    async fn health_check(&self) -> Result<(), LookupError> {
        let mut con = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| LookupError::Connection(format!("health check connect: {e}")))?;
        redis::cmd("PING")
            .query_async::<String>(&mut con)
            .await
            .map_err(|e| LookupError::Query(format!("health check PING: {e}")))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Array, Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("score", DataType::Int64, true),
        ]))
    }

    fn make_batch(schema: &SchemaRef, id: &str, name: &str, score: i64) -> RecordBatch {
        RecordBatch::try_new(
            Arc::clone(schema),
            vec![
                Arc::new(StringArray::from(vec![id])),
                Arc::new(StringArray::from(vec![Some(name)])),
                Arc::new(Int64Array::from(vec![Some(score)])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_extract_key_suffix() {
        assert_eq!(extract_key_suffix("user:123", "user:*"), "123");
        assert_eq!(
            extract_key_suffix("product:abc:def", "product:*"),
            "abc:def"
        );
        assert_eq!(extract_key_suffix("foo", "*"), "foo");
        assert_eq!(extract_key_suffix("unknown", "prefix:*"), "unknown");
        // Non-prefix patterns: strip both prefix and suffix around '*'
        assert_eq!(
            extract_key_suffix("user:123:profile", "user:*:profile"),
            "123"
        );
        assert_eq!(extract_key_suffix("cache:abc:v2", "cache:*:v2"), "abc");
        // Suffix doesn't match — keep what's after prefix
        assert_eq!(
            extract_key_suffix("user:123:other", "user:*:profile"),
            "123:other"
        );
    }

    #[test]
    fn test_fields_to_record_batch() {
        let schema = test_schema();
        let mut fields = HashMap::new();
        fields.insert("name".to_string(), "Alice".to_string());
        fields.insert("score".to_string(), "42".to_string());

        let batch = fields_to_record_batch(&schema, "user_id", "u1", &fields).unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 3);

        let id_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(id_col.value(0), "u1");

        let name_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(name_col.value(0), "Alice");

        let score_col = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(score_col.value(0), 42);
    }

    #[test]
    fn test_fields_to_record_batch_missing_fields() {
        let schema = test_schema();
        let fields = HashMap::new(); // no fields at all

        let batch = fields_to_record_batch(&schema, "user_id", "u1", &fields).unwrap();
        assert_eq!(batch.num_rows(), 1);

        // key column should still be populated
        let id_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(id_col.value(0), "u1");

        // other columns should be null
        let name_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(name_col.is_null(0));
    }

    #[tokio::test]
    async fn test_from_data_query() {
        let schema = test_schema();
        let mut data = HashMap::new();
        data.insert(b"u1".to_vec(), make_batch(&schema, "u1", "Alice", 100));
        data.insert(b"u2".to_vec(), make_batch(&schema, "u2", "Bob", 200));

        let config = RedisLookupSourceConfig {
            url: "redis://127.0.0.1:6379".into(),
            key_pattern: "user:*".into(),
            key_column: "user_id".into(),
            ..Default::default()
        };

        let source = RedisLookupSource::from_data(config, schema, data).unwrap();

        // Hit
        let results = source.query(&[b"u1".as_slice()], &[], &[]).await.unwrap();
        assert_eq!(results.len(), 1);
        assert!(results[0].is_some());

        // Miss
        let results = source.query(&[b"u999".as_slice()], &[], &[]).await.unwrap();
        assert_eq!(results.len(), 1);
        assert!(results[0].is_none());

        // Batch
        let keys: Vec<&[u8]> = vec![b"u1", b"u999", b"u2"];
        let results = source.query(&keys, &[], &[]).await.unwrap();
        assert_eq!(results.len(), 3);
        assert!(results[0].is_some());
        assert!(results[1].is_none());
        assert!(results[2].is_some());
    }

    #[tokio::test]
    async fn test_capabilities() {
        let schema = test_schema();
        let config = RedisLookupSourceConfig {
            url: "redis://127.0.0.1:6379".into(),
            key_pattern: "user:*".into(),
            key_column: "user_id".into(),
            max_batch_size: 500,
            ..Default::default()
        };

        let source = RedisLookupSource::from_data(config, schema, HashMap::new()).unwrap();
        let caps = source.capabilities();
        assert!(!caps.supports_predicate_pushdown);
        assert!(!caps.supports_projection_pushdown);
        assert!(caps.supports_batch_lookup);
        assert_eq!(caps.max_batch_size, 500);
    }

    #[test]
    fn test_source_name() {
        let schema = test_schema();
        let config = RedisLookupSourceConfig::default();
        let source = RedisLookupSource::from_data(config, schema, HashMap::new()).unwrap();
        assert_eq!(source.source_name(), "redis");
    }

    #[test]
    fn test_estimated_row_count() {
        let schema = test_schema();
        let mut data = HashMap::new();
        data.insert(b"k1".to_vec(), make_batch(&schema, "k1", "A", 1));
        data.insert(b"k2".to_vec(), make_batch(&schema, "k2", "B", 2));

        let config = RedisLookupSourceConfig::default();
        let source = RedisLookupSource::from_data(config, schema, data).unwrap();
        assert_eq!(source.estimated_row_count(), Some(2));
    }

    #[test]
    fn test_config_defaults() {
        let config = RedisLookupSourceConfig::default();
        assert!(config.url.is_empty());
        assert!(config.key_pattern.is_empty());
        assert!(config.key_column.is_empty());
        assert_eq!(config.value_type, RedisValueType::Hash);
        assert_eq!(config.scan_count, 1000);
        assert_eq!(config.max_batch_size, 1000);
        assert_eq!(config.db, 0);
    }

    #[test]
    fn test_redis_value_type_parse() {
        assert_eq!(RedisValueType::parse("hash").unwrap(), RedisValueType::Hash);
        assert_eq!(
            RedisValueType::parse("string").unwrap(),
            RedisValueType::String
        );
        assert_eq!(
            RedisValueType::parse("json").unwrap(),
            RedisValueType::String
        );
        assert!(RedisValueType::parse("invalid").is_err());
    }

    #[test]
    fn test_debug_impl_redacts_userinfo() {
        let config = RedisLookupSourceConfig {
            url: "redis://admin:s3cret@myhost:6379/0".into(),
            key_pattern: "user:*".into(),
            key_column: "user_id".into(),
            ..Default::default()
        };
        let debug = format!("{config:?}");
        assert!(debug.contains("RedisLookupSourceConfig"));
        assert!(debug.contains("user:*"));
        // Both username and password must be redacted
        assert!(
            !debug.contains("s3cret"),
            "password must be redacted in Debug output"
        );
        assert!(
            !debug.contains("admin"),
            "username must be redacted in Debug output"
        );
        assert!(
            debug.contains("***"),
            "redacted placeholder must appear in Debug output"
        );
        // Host should still be visible
        assert!(debug.contains("myhost"));
    }

    #[test]
    fn test_debug_impl_no_password_unchanged() {
        let config = RedisLookupSourceConfig {
            url: "redis://myhost:6379".into(),
            key_pattern: "key:*".into(),
            key_column: "id".into(),
            ..Default::default()
        };
        let debug = format!("{config:?}");
        assert!(debug.contains("myhost:6379"));
    }

    #[test]
    fn test_debug_impl_source() {
        let schema = test_schema();
        let config = RedisLookupSourceConfig {
            url: "redis://localhost".into(),
            key_pattern: "user:*".into(),
            key_column: "user_id".into(),
            ..Default::default()
        };
        let source = RedisLookupSource::from_data(config, schema, HashMap::new()).unwrap();
        let debug = format!("{source:?}");
        assert!(debug.contains("RedisLookupSource"));
        assert!(debug.contains("user:*"));
    }

    /// Test JSON value type parsing path: numbers, booleans, nested objects
    /// are converted to their string representation for Arrow columns.
    #[test]
    fn test_fields_to_record_batch_json_value_types() {
        use arrow_array::{BooleanArray, Float64Array};

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("count", DataType::Int64, true),
            Field::new("rate", DataType::Float64, true),
            Field::new("active", DataType::Boolean, true),
            Field::new("meta", DataType::Utf8, true),
        ]));

        // Simulate what the JSON parsing path produces: numbers and booleans
        // are converted to string via serde_json::Value::to_string(), nested
        // objects become their JSON representation.
        let mut fields = HashMap::new();
        fields.insert("count".to_string(), "42".to_string());
        fields.insert("rate".to_string(), "3.14".to_string());
        fields.insert("active".to_string(), "true".to_string());
        // Nested object comes through as its JSON string
        fields.insert("meta".to_string(), r#"{"nested":"value"}"#.to_string());

        let batch = fields_to_record_batch(&schema, "id", "k1", &fields).unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 5);

        // Verify key column
        let id_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(id_col.value(0), "k1");

        // Verify integer parsing
        let count_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(count_col.value(0), 42);
        assert!(!count_col.is_null(0));

        // Verify float parsing
        let rate_col = batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((rate_col.value(0) - 3.14).abs() < f64::EPSILON);
        assert!(!rate_col.is_null(0));

        // Verify boolean parsing
        let active_col = batch
            .column(3)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert_eq!(active_col.value(0), true);
        assert!(!active_col.is_null(0));

        // Verify nested object stored as string
        let meta_col = batch
            .column(4)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(meta_col.value(0), r#"{"nested":"value"}"#);
        assert!(!meta_col.is_null(0));
    }

    /// LargeUtf8 columns must use LargeStringArray, not StringArray.
    #[test]
    fn test_fields_to_record_batch_large_utf8() {
        use arrow_array::LargeStringArray;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("description", DataType::LargeUtf8, true),
        ]));

        let mut fields = HashMap::new();
        fields.insert("description".to_string(), "a long description".to_string());

        let batch = fields_to_record_batch(&schema, "id", "k1", &fields).unwrap();
        assert_eq!(batch.num_rows(), 1);

        // Key column is Utf8
        let id_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(id_col.value(0), "k1");

        // Description column is LargeUtf8 — must downcast as LargeStringArray
        let desc_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .unwrap();
        assert_eq!(desc_col.value(0), "a long description");
    }

    /// Test that unparseable numeric values become null.
    #[test]
    fn test_fields_to_record_batch_unparseable_number() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("score", DataType::Int64, true),
        ]));

        let mut fields = HashMap::new();
        fields.insert("score".to_string(), "not_a_number".to_string());

        let batch = fields_to_record_batch(&schema, "id", "k1", &fields).unwrap();
        let score_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert!(
            score_col.is_null(0),
            "unparseable number should produce null"
        );
    }
}
