//! `MongoDB` connector configuration.
//!
//! Provides [`MongoDbSourceConfig`] for the CDC change stream source and
//! [`MongoDbSinkConfig`] for the write sink. Both support construction
//! from a generic [`ConnectorConfig`] key-value map and programmatic builders.
//!
//! # Pipeline Validation
//!
//! The source config validates that user-supplied aggregation pipeline stages
//! do not modify the `_id` field. `MongoDB` 8.x throws a server-side error if
//! `_id` is projected away; validating at construction prevents runtime failures.

use std::time::Duration;

use crate::config::ConnectorConfig;
use crate::error::ConnectorError;

use super::resume_token::ResumeTokenStoreConfig;
use super::timeseries::CollectionKind;
use super::write_model::WriteMode;

/// Mode for requesting full documents on update events.
///
/// Controls the `fullDocument` option on the change stream cursor.
/// The choice has significant correctness and performance implications.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FullDocumentMode {
    /// Default: only the delta (`updateDescription`) for update events.
    /// `fullDocument` is `None` on updates.
    #[default]
    Delta,

    /// `fullDocument: "updateLookup"` — fetches the current document from
    /// the collection on update events.
    ///
    /// **Warning**: Dangerous on high-churn collections with `$match` filters.
    /// If the matched document is quickly deleted, the lookup returns `null`
    /// and can cause "Resume Token Not Found" errors. Prefer `WhenAvailable`
    /// for high-churn patterns.
    UpdateLookup,

    /// `fullDocument: "required"` — the collection must have
    /// `changeStreamPreAndPostImages` enabled. Returns
    /// `SourceError::PostImageNotEnabled` if not configured.
    RequirePostImage,

    /// `fullDocument: "whenAvailable"` — safest option for high-churn
    /// collections. Returns the post-image when available, `null` otherwise.
    WhenAvailable,
}

str_enum!(FullDocumentMode, lowercase, ConnectorError, "unknown full document mode",
    Delta => "delta";
    UpdateLookup => "updatelookup", "update_lookup";
    RequirePostImage => "requirepostimage", "require_post_image", "required";
    WhenAvailable => "whenavailable", "when_available"
);

/// Configuration for the `MongoDB` CDC source connector.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MongoDbSourceConfig {
    /// `MongoDB` connection URI (e.g., `mongodb://host:27017`).
    pub connection_uri: String,

    /// Database name.
    pub database: String,

    /// Collection name, or `"*"` to watch all collections in the database.
    pub collection: String,

    /// Full document retrieval mode for update events.
    pub full_document_mode: FullDocumentMode,

    /// Additional aggregation pipeline stages injected before the change
    /// stream opens. Callers can inject `$match`, `$project`, etc.
    ///
    /// **Constraint**: Pipeline stages must not modify the `_id` field.
    /// This is validated at construction.
    #[serde(default)]
    pub pipeline: Vec<serde_json::Value>,

    /// Whether to enable `$changeStreamSplitLargeEvent` (`MongoDB` ≥ 6.0.9).
    #[serde(default)]
    pub split_large_events: bool,

    /// Resume token persistence configuration.
    #[serde(default)]
    pub resume_token_store: ResumeTokenStoreConfig,

    /// `getMore` await timeout in milliseconds.
    pub max_await_time_ms: Option<u64>,

    /// Cursor batch size hint.
    pub batch_size: Option<u32>,

    /// For fresh starts with no persisted token, start at this operation time.
    /// Encoded as `{ t: <seconds>, i: <increment> }`.
    pub start_at_operation_time: Option<(u32, u32)>,

    /// Maximum events to buffer before applying backpressure.
    #[serde(default = "default_max_buffered_events")]
    pub max_buffered_events: usize,

    /// Maximum records to return per `poll_batch` call.
    #[serde(default = "default_max_poll_records")]
    pub max_poll_records: usize,
}

fn default_max_buffered_events() -> usize {
    100_000
}

fn default_max_poll_records() -> usize {
    1000
}

impl Default for MongoDbSourceConfig {
    fn default() -> Self {
        Self {
            connection_uri: "mongodb://localhost:27017".to_string(),
            database: String::new(),
            collection: String::new(),
            full_document_mode: FullDocumentMode::default(),
            pipeline: Vec::new(),
            split_large_events: false,
            resume_token_store: ResumeTokenStoreConfig::default(),
            max_await_time_ms: Some(1000),
            batch_size: Some(1000),
            start_at_operation_time: None,
            max_buffered_events: default_max_buffered_events(),
            max_poll_records: default_max_poll_records(),
        }
    }
}

impl MongoDbSourceConfig {
    /// Creates a new source config with required fields.
    #[must_use]
    pub fn new(connection_uri: &str, database: &str, collection: &str) -> Self {
        Self {
            connection_uri: connection_uri.to_string(),
            database: database.to_string(),
            collection: collection.to_string(),
            ..Self::default()
        }
    }

    /// Returns `true` if the source is configured to watch all collections.
    #[must_use]
    pub fn is_database_watch(&self) -> bool {
        self.collection == "*"
    }

    /// Validates the configuration.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError::ConfigurationError` for invalid settings.
    pub fn validate(&self) -> Result<(), ConnectorError> {
        crate::config::require_non_empty(&self.connection_uri, "connection_uri")?;
        crate::config::require_non_empty(&self.database, "database")?;
        crate::config::require_non_empty(&self.collection, "collection")?;

        if self.max_poll_records == 0 {
            return Err(ConnectorError::ConfigurationError(
                "max_poll_records must be > 0".to_string(),
            ));
        }
        if self.max_buffered_events == 0 {
            return Err(ConnectorError::ConfigurationError(
                "max_buffered_events must be > 0".to_string(),
            ));
        }

        // split_large_events requires raw change stream access not available
        // in the mongodb v3 driver (ChangeStreamEvent drops unknown fields).
        if self.split_large_events {
            return Err(ConnectorError::UnsupportedOperation(
                "split_large_events requires raw change stream access not supported \
                 by the mongodb v3 driver; set split_large_events = false"
                    .to_string(),
            ));
        }

        // Validate pipeline does not modify the _id field.
        validate_pipeline_no_id_modification(&self.pipeline)?;

        Ok(())
    }

    /// Parses configuration from a generic [`ConnectorConfig`].
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError` if required keys are missing or invalid.
    pub fn from_config(config: &ConnectorConfig) -> Result<Self, ConnectorError> {
        let mut cfg = Self {
            connection_uri: config.require("connection.uri")?.to_string(),
            database: config.require("database")?.to_string(),
            collection: config.require("collection")?.to_string(),
            ..Self::default()
        };

        if let Some(mode) = config.get_parsed::<FullDocumentMode>("full.document.mode")? {
            cfg.full_document_mode = mode;
        }
        if let Some(split) = config.get_parsed::<bool>("split.large.events")? {
            cfg.split_large_events = split;
        }
        if let Some(timeout) = config.get_parsed::<u64>("max.await.time.ms")? {
            cfg.max_await_time_ms = Some(timeout);
        }
        if let Some(batch) = config.get_parsed::<u32>("batch.size")? {
            cfg.batch_size = Some(batch);
        }
        if let Some(max) = config.get_parsed::<usize>("max.poll.records")? {
            cfg.max_poll_records = max;
        }
        if let Some(max) = config.get_parsed::<usize>("max.buffered.events")? {
            cfg.max_buffered_events = max;
        }

        cfg.validate()?;
        Ok(cfg)
    }
}

/// Validates that a pipeline does not modify the `_id` field.
///
/// `MongoDB` 8.x throws a server error if `_id` is projected away or
/// modified in change stream pipeline stages. We fail fast at config
/// time instead.
fn validate_pipeline_no_id_modification(
    pipeline: &[serde_json::Value],
) -> Result<(), ConnectorError> {
    for (i, stage) in pipeline.iter().enumerate() {
        if let Some(obj) = stage.as_object() {
            // Check $project stages.
            if let Some(proj) = obj.get("$project") {
                if let Some(proj_obj) = proj.as_object() {
                    if let Some(id_val) = proj_obj.get("_id") {
                        // _id: 0 or _id: false means exclusion.
                        if id_val == &serde_json::Value::from(0)
                            || id_val == &serde_json::Value::from(false)
                        {
                            return Err(ConnectorError::ConfigurationError(format!(
                                "pipeline stage {i}: $project must not exclude _id field — \
                                 MongoDB change streams require _id for resume tokens"
                            )));
                        }
                    }
                }
            }

            // Check $unset stages.
            if let Some(unset) = obj.get("$unset") {
                let fields: Vec<&str> = match unset {
                    serde_json::Value::String(s) => vec![s.as_str()],
                    serde_json::Value::Array(arr) => {
                        arr.iter().filter_map(serde_json::Value::as_str).collect()
                    }
                    _ => Vec::new(),
                };
                if fields.contains(&"_id") {
                    return Err(ConnectorError::ConfigurationError(format!(
                        "pipeline stage {i}: $unset must not remove _id field"
                    )));
                }
            }

            // Check $addFields / $set that overwrite _id.
            for op in &["$addFields", "$set"] {
                if let Some(fields) = obj.get(*op) {
                    if let Some(fields_obj) = fields.as_object() {
                        if fields_obj.contains_key("_id") {
                            return Err(ConnectorError::ConfigurationError(format!(
                                "pipeline stage {i}: {op} must not modify _id field"
                            )));
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

/// Write concern level for `MongoDB` write operations.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WriteConcernLevel {
    /// Write acknowledged by the majority of replica set members.
    #[default]
    Majority,
    /// Write acknowledged by the specified number of nodes.
    Nodes(u32),
}

/// Write concern configuration for `MongoDB` operations.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct WriteConcernConfig {
    /// The write concern level.
    #[serde(default)]
    pub w: WriteConcernLevel,
    /// Whether to wait for journal commit.
    #[serde(default = "default_journal")]
    pub journal: bool,
    /// Write concern timeout in milliseconds.
    pub timeout_ms: Option<u64>,
}

fn default_journal() -> bool {
    true
}

impl Default for WriteConcernConfig {
    fn default() -> Self {
        Self {
            w: WriteConcernLevel::default(),
            journal: true,
            timeout_ms: None,
        }
    }
}

/// Configuration for the `MongoDB` sink connector.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MongoDbSinkConfig {
    /// `MongoDB` connection URI.
    pub connection_uri: String,

    /// Target database name.
    pub database: String,

    /// Target collection name.
    pub collection: String,

    /// Whether the target is a standard or time series collection.
    #[serde(default)]
    pub collection_kind: CollectionKind,

    /// Write operation mode.
    #[serde(default)]
    pub write_mode: WriteMode,

    /// Maximum documents per batch flush.
    #[serde(default = "default_sink_batch_size")]
    pub batch_size: usize,

    /// Maximum time between flushes in milliseconds.
    #[serde(default = "default_flush_interval_ms")]
    pub flush_interval_ms: u64,

    /// `true` = ordered bulk write (fail-fast); `false` = unordered
    /// (higher throughput).
    ///
    /// Ordered writes stop on the first error, preserving operation order.
    /// Unordered writes attempt all operations and report all failures,
    /// achieving higher throughput at the cost of non-deterministic ordering.
    #[serde(default = "default_ordered")]
    pub ordered: bool,

    /// Write concern configuration.
    #[serde(default)]
    pub write_concern: WriteConcernConfig,
}

fn default_sink_batch_size() -> usize {
    500
}

fn default_flush_interval_ms() -> u64 {
    1000
}

fn default_ordered() -> bool {
    true
}

impl Default for MongoDbSinkConfig {
    fn default() -> Self {
        Self {
            connection_uri: "mongodb://localhost:27017".to_string(),
            database: String::new(),
            collection: String::new(),
            collection_kind: CollectionKind::default(),
            write_mode: WriteMode::default(),
            batch_size: default_sink_batch_size(),
            flush_interval_ms: default_flush_interval_ms(),
            ordered: default_ordered(),
            write_concern: WriteConcernConfig::default(),
        }
    }
}

impl MongoDbSinkConfig {
    /// Creates a new sink config with required fields.
    #[must_use]
    pub fn new(connection_uri: &str, database: &str, collection: &str) -> Self {
        Self {
            connection_uri: connection_uri.to_string(),
            database: database.to_string(),
            collection: collection.to_string(),
            ..Self::default()
        }
    }

    /// Returns the flush interval as a `Duration`.
    #[must_use]
    pub fn flush_interval(&self) -> Duration {
        Duration::from_millis(self.flush_interval_ms)
    }

    /// Validates the configuration.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError::ConfigurationError` for invalid settings.
    pub fn validate(&self) -> Result<(), ConnectorError> {
        crate::config::require_non_empty(&self.connection_uri, "connection_uri")?;
        crate::config::require_non_empty(&self.database, "database")?;
        crate::config::require_non_empty(&self.collection, "collection")?;

        if self.batch_size == 0 {
            return Err(ConnectorError::ConfigurationError(
                "batch_size must be > 0".to_string(),
            ));
        }

        // Time series collections only support Insert mode.
        if let CollectionKind::TimeSeries(_) = &self.collection_kind {
            super::write_model::validate_timeseries_write_mode(&self.write_mode)?;
        }

        Ok(())
    }

    /// Parses configuration from a generic [`ConnectorConfig`].
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError` if required keys are missing or invalid.
    pub fn from_config(config: &ConnectorConfig) -> Result<Self, ConnectorError> {
        let mut cfg = Self {
            connection_uri: config.require("connection.uri")?.to_string(),
            database: config.require("database")?.to_string(),
            collection: config.require("collection")?.to_string(),
            ..Self::default()
        };

        if let Some(batch) = config.get_parsed::<usize>("batch.size")? {
            cfg.batch_size = batch;
        }
        if let Some(interval) = config.get_parsed::<u64>("flush.interval.ms")? {
            cfg.flush_interval_ms = interval;
        }
        if let Some(ordered) = config.get_parsed::<bool>("ordered")? {
            cfg.ordered = ordered;
        }
        if let Some(mode) = config.get("write.mode") {
            cfg.write_mode = match mode {
                "insert" => WriteMode::Insert,
                "cdc_replay" | "cdc-replay" => WriteMode::CdcReplay,
                "upsert" => {
                    let keys = config
                        .require("write.mode.key_fields")?
                        .split(',')
                        .map(|s| s.trim().to_string())
                        .collect();
                    WriteMode::Upsert { key_fields: keys }
                }
                "replace" => WriteMode::Replace {
                    upsert_on_missing: config
                        .get_parsed::<bool>("write.mode.upsert_on_missing")?
                        .unwrap_or(false),
                },
                other => {
                    return Err(ConnectorError::ConfigurationError(format!(
                        "unknown write.mode: {other}"
                    )));
                }
            };
        }
        if let Some(journal) = config.get_parsed::<bool>("write_concern.journal")? {
            cfg.write_concern.journal = journal;
        }
        if let Some(timeout) = config.get_parsed::<u64>("write_concern.timeout_ms")? {
            cfg.write_concern.timeout_ms = Some(timeout);
        }

        cfg.validate()?;
        Ok(cfg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Source config tests ──

    #[test]
    fn test_source_config_default() {
        let cfg = MongoDbSourceConfig::default();
        assert_eq!(cfg.connection_uri, "mongodb://localhost:27017");
        assert_eq!(cfg.full_document_mode, FullDocumentMode::Delta);
        assert!(!cfg.split_large_events);
        assert_eq!(cfg.max_poll_records, 1000);
    }

    #[test]
    fn test_source_config_new() {
        let cfg = MongoDbSourceConfig::new("mongodb://db:27017", "mydb", "users");
        assert_eq!(cfg.connection_uri, "mongodb://db:27017");
        assert_eq!(cfg.database, "mydb");
        assert_eq!(cfg.collection, "users");
    }

    #[test]
    fn test_source_config_database_watch() {
        let cfg = MongoDbSourceConfig::new("mongodb://db:27017", "mydb", "*");
        assert!(cfg.is_database_watch());

        let cfg = MongoDbSourceConfig::new("mongodb://db:27017", "mydb", "users");
        assert!(!cfg.is_database_watch());
    }

    #[test]
    fn test_source_config_validate_empty_uri() {
        let mut cfg = MongoDbSourceConfig::new("", "db", "coll");
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("connection_uri"));
    }

    #[test]
    fn test_source_config_validate_empty_database() {
        let cfg = MongoDbSourceConfig::new("mongodb://db:27017", "", "coll");
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("database"));
    }

    #[test]
    fn test_source_config_validate_zero_max_poll() {
        let mut cfg = MongoDbSourceConfig::new("mongodb://db:27017", "db", "coll");
        cfg.max_poll_records = 0;
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("max_poll_records"));
    }

    #[test]
    fn test_source_config_split_large_events_rejected() {
        let mut cfg = MongoDbSourceConfig::new("mongodb://db:27017", "db", "coll");
        cfg.split_large_events = true;
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("split_large_events"));
    }

    #[test]
    fn test_source_config_from_connector_config() {
        let mut config = ConnectorConfig::new("mongodb-cdc");
        config.set("connection.uri", "mongodb://host:27017");
        config.set("database", "testdb");
        config.set("collection", "events");
        config.set("full.document.mode", "update_lookup");
        config.set("max.poll.records", "500");

        let cfg = MongoDbSourceConfig::from_config(&config).unwrap();
        assert_eq!(cfg.connection_uri, "mongodb://host:27017");
        assert_eq!(cfg.database, "testdb");
        assert_eq!(cfg.collection, "events");
        assert_eq!(cfg.full_document_mode, FullDocumentMode::UpdateLookup);
        assert_eq!(cfg.max_poll_records, 500);
    }

    #[test]
    fn test_source_config_from_config_missing_required() {
        let config = ConnectorConfig::new("mongodb-cdc");
        assert!(MongoDbSourceConfig::from_config(&config).is_err());
    }

    // ── Pipeline validation tests ──

    #[test]
    fn test_pipeline_valid_match() {
        let pipeline = vec![serde_json::json!({
            "$match": { "operationType": "insert" }
        })];
        validate_pipeline_no_id_modification(&pipeline).unwrap();
    }

    #[test]
    fn test_pipeline_id_excluded_in_project() {
        let pipeline = vec![serde_json::json!({
            "$project": { "_id": 0 }
        })];
        let err = validate_pipeline_no_id_modification(&pipeline).unwrap_err();
        assert!(err.to_string().contains("_id"));
    }

    #[test]
    fn test_pipeline_id_excluded_false() {
        let pipeline = vec![serde_json::json!({
            "$project": { "_id": false }
        })];
        let err = validate_pipeline_no_id_modification(&pipeline).unwrap_err();
        assert!(err.to_string().contains("_id"));
    }

    #[test]
    fn test_pipeline_id_unset() {
        let pipeline = vec![serde_json::json!({ "$unset": "_id" })];
        let err = validate_pipeline_no_id_modification(&pipeline).unwrap_err();
        assert!(err.to_string().contains("_id"));
    }

    #[test]
    fn test_pipeline_id_unset_array() {
        let pipeline = vec![serde_json::json!({ "$unset": ["_id", "other"] })];
        let err = validate_pipeline_no_id_modification(&pipeline).unwrap_err();
        assert!(err.to_string().contains("_id"));
    }

    #[test]
    fn test_pipeline_id_addfields() {
        let pipeline = vec![serde_json::json!({
            "$addFields": { "_id": "overwritten" }
        })];
        let err = validate_pipeline_no_id_modification(&pipeline).unwrap_err();
        assert!(err.to_string().contains("_id"));
    }

    #[test]
    fn test_pipeline_id_set() {
        let pipeline = vec![serde_json::json!({
            "$set": { "_id": "overwritten" }
        })];
        let err = validate_pipeline_no_id_modification(&pipeline).unwrap_err();
        assert!(err.to_string().contains("_id"));
    }

    #[test]
    fn test_pipeline_valid_project_includes_id() {
        let pipeline = vec![serde_json::json!({
            "$project": { "_id": 1, "name": 1 }
        })];
        validate_pipeline_no_id_modification(&pipeline).unwrap();
    }

    // ── Full document mode tests ──

    #[test]
    fn test_full_document_mode_fromstr() {
        assert_eq!(
            "delta".parse::<FullDocumentMode>().unwrap(),
            FullDocumentMode::Delta
        );
        assert_eq!(
            "update_lookup".parse::<FullDocumentMode>().unwrap(),
            FullDocumentMode::UpdateLookup
        );
        assert_eq!(
            "updatelookup".parse::<FullDocumentMode>().unwrap(),
            FullDocumentMode::UpdateLookup
        );
        assert_eq!(
            "required".parse::<FullDocumentMode>().unwrap(),
            FullDocumentMode::RequirePostImage
        );
        assert_eq!(
            "when_available".parse::<FullDocumentMode>().unwrap(),
            FullDocumentMode::WhenAvailable
        );
        assert!("bad".parse::<FullDocumentMode>().is_err());
    }

    #[test]
    fn test_full_document_mode_display() {
        assert_eq!(FullDocumentMode::Delta.to_string(), "delta");
        assert_eq!(FullDocumentMode::UpdateLookup.to_string(), "updatelookup");
    }

    // ── Sink config tests ──

    #[test]
    fn test_sink_config_default() {
        let cfg = MongoDbSinkConfig::default();
        assert_eq!(cfg.batch_size, 500);
        assert_eq!(cfg.flush_interval_ms, 1000);
        assert!(cfg.ordered);
        assert!(matches!(cfg.collection_kind, CollectionKind::Standard));
    }

    #[test]
    fn test_sink_config_new() {
        let cfg = MongoDbSinkConfig::new("mongodb://db:27017", "mydb", "events");
        assert_eq!(cfg.connection_uri, "mongodb://db:27017");
        assert_eq!(cfg.database, "mydb");
        assert_eq!(cfg.collection, "events");
    }

    #[test]
    fn test_sink_config_validate_empty_uri() {
        let cfg = MongoDbSinkConfig::new("", "db", "coll");
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("connection_uri"));
    }

    #[test]
    fn test_sink_config_validate_zero_batch_size() {
        let mut cfg = MongoDbSinkConfig::new("mongodb://db:27017", "db", "coll");
        cfg.batch_size = 0;
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("batch_size"));
    }

    #[test]
    fn test_sink_config_timeseries_upsert_rejected() {
        let mut cfg = MongoDbSinkConfig::new("mongodb://db:27017", "db", "ts");
        cfg.collection_kind =
            CollectionKind::TimeSeries(super::super::timeseries::TimeSeriesConfig {
                time_field: "ts".to_string(),
                meta_field: None,
                granularity: super::super::timeseries::TimeSeriesGranularity::Seconds,
                expire_after_seconds: None,
            });
        cfg.write_mode = WriteMode::Upsert {
            key_fields: vec!["id".to_string()],
        };
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("time series"));
    }

    #[test]
    fn test_sink_config_flush_interval() {
        let cfg = MongoDbSinkConfig::default();
        assert_eq!(cfg.flush_interval(), Duration::from_millis(1000));
    }

    #[test]
    fn test_sink_config_from_connector_config() {
        let mut config = ConnectorConfig::new("mongodb-sink");
        config.set("connection.uri", "mongodb://host:27017");
        config.set("database", "testdb");
        config.set("collection", "out");
        config.set("batch.size", "1000");
        config.set("ordered", "false");

        let cfg = MongoDbSinkConfig::from_config(&config).unwrap();
        assert_eq!(cfg.batch_size, 1000);
        assert!(!cfg.ordered);
    }

    #[test]
    fn test_write_concern_default() {
        let wc = WriteConcernConfig::default();
        assert!(matches!(wc.w, WriteConcernLevel::Majority));
        assert!(wc.journal);
        assert!(wc.timeout_ms.is_none());
    }
}
