//! `MongoDB` connector configuration.
//!
//! Provides [`MongoDbSourceConfig`] for the CDC change stream source and
//! [`MongoDbSinkConfig`] for the write sink. Both support construction
//! from a generic [`ConnectorConfig`] key-value map and programmatic builders.
//!
//! User-supplied change-stream pipelines are limited to `$match` stages so they
//! cannot alter `MongoDB`'s resume token.

use std::time::Duration;

use crate::config::ConnectorConfig;
use crate::error::ConnectorError;

use super::timeseries::{CollectionKind, TimeSeriesConfig, TimeSeriesGranularity};
use super::write_model::WriteMode;

const REMOVED_SOURCE_CONFIG_KEYS: &[&str] = &[
    "batch.size",
    "max.buffered.events",
    "max.await.time.ms",
    "resume.token.store",
    "split.large.events",
    "max.poll.records",
];

const SOURCE_CONFIG_KEYS: &[&str] = &[
    "connection.uri",
    "database",
    "collection",
    "full.document.mode",
    "pipeline",
    "max.buffered.bytes",
    "laminar.source.name",
    "_arrow_schema",
    "_primary_key_columns",
];

pub(super) const DEFAULT_MAX_BUFFERED_BYTES: usize = 64 * 1024 * 1024;
pub(super) const MAX_PIPELINE_STAGES: usize = 64;
pub(super) const MAX_PIPELINE_JSON_BYTES: usize = 256 * 1024;
const MIN_BUFFERED_BYTES: usize = 1024 * 1024;
const MAX_BUFFERED_BYTES: usize = 4 * 1024 * 1024 * 1024;
const ESTIMATED_BUFFERED_ITEM_BYTES: usize = 64 * 1024;
const MAX_READER_CHANNEL_ITEMS: usize = 4096;
const MAX_CURSOR_BATCH_ITEMS: u32 = 1000;

const SINK_CONFIG_KEYS: &[&str] = &[
    "connection.uri",
    "database",
    "collection",
    "flush.interval.ms",
    "write.mode",
    "write.mode.key_fields",
    "timeseries.time_field",
    "timeseries.meta_field",
    "timeseries.granularity",
    "timeseries.bucket_max_span_seconds",
    "timeseries.bucket_rounding_seconds",
    "timeseries.expire_after_seconds",
    "delivery.guarantee",
    "sink.write.timeout.ms",
    "_arrow_schema",
];

const REMOVED_SINK_CONFIG_KEYS: &[&str] = &["batch.size", "write_concern.timeout_ms"];

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

    /// `fullDocument: "required"` — the collection must have
    /// `changeStreamPreAndPostImages` enabled. Admission fails before the
    /// change stream opens when the option is disabled.
    #[serde(rename = "required")]
    RequirePostImage,
}

str_enum!(FullDocumentMode, lowercase, ConnectorError, "unknown full document mode",
    Delta => "delta";
    RequirePostImage => "required"
);

/// Configuration for the `MongoDB` CDC source connector.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MongoDbSourceConfig {
    /// `MongoDB` connection URI (e.g., `mongodb://host:27017`).
    pub connection_uri: String,

    /// Database name.
    pub database: String,

    /// Fixed collection name.
    pub collection: String,

    /// Full document retrieval mode for update events.
    pub full_document_mode: FullDocumentMode,

    /// Additional `$match` stages applied when the change stream opens.
    #[serde(default)]
    pub pipeline: Vec<serde_json::Value>,

    /// Maximum retained decoded bytes across the reader channel and poll buffer.
    #[serde(default = "default_max_buffered_bytes")]
    pub max_buffered_bytes: usize,
}

fn default_max_buffered_bytes() -> usize {
    DEFAULT_MAX_BUFFERED_BYTES
}

impl Default for MongoDbSourceConfig {
    fn default() -> Self {
        Self {
            connection_uri: "mongodb://localhost:27017".to_string(),
            database: String::new(),
            collection: String::new(),
            full_document_mode: FullDocumentMode::default(),
            pipeline: Vec::new(),
            max_buffered_bytes: default_max_buffered_bytes(),
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

    /// Private queue capacity derived from the one public ownership budget.
    pub(crate) fn reader_channel_capacity(&self) -> usize {
        (self.max_buffered_bytes / ESTIMATED_BUFFERED_ITEM_BYTES).clamp(1, MAX_READER_CHANNEL_ITEMS)
    }

    /// Cursor batch hint derived from the one public ownership budget.
    pub(crate) fn cursor_batch_size(&self) -> u32 {
        u32::try_from(self.reader_channel_capacity())
            .unwrap_or(MAX_CURSOR_BATCH_ITEMS)
            .min(MAX_CURSOR_BATCH_ITEMS)
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
        if self.collection == "*" {
            return Err(ConnectorError::ConfigurationError(
                "MongoDB CDC collection must name one fixed collection; database-wide wildcard \
                 streams cannot be bound to an immutable collection UUID"
                    .into(),
            ));
        }

        if !(MIN_BUFFERED_BYTES..=MAX_BUFFERED_BYTES).contains(&self.max_buffered_bytes) {
            return Err(ConnectorError::ConfigurationError(format!(
                "max.buffered.bytes must be between {MIN_BUFFERED_BYTES} and \
                     {MAX_BUFFERED_BYTES}"
            )));
        }

        validate_pipeline(&self.pipeline)?;

        Ok(())
    }

    /// Parses configuration from a generic [`ConnectorConfig`].
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError` if required keys are missing or invalid.
    #[allow(clippy::too_many_lines)]
    pub fn from_config(config: &ConnectorConfig) -> Result<Self, ConnectorError> {
        if let Some(key) = REMOVED_SOURCE_CONFIG_KEYS
            .iter()
            .find(|key| config.get(key).is_some())
        {
            let reason = if *key == "max.buffered.events" {
                "retained ownership is bounded by max.buffered.bytes instead"
            } else {
                "the connector did not execute it"
            };
            return Err(ConnectorError::ConfigurationError(format!(
                "MongoDB CDC property '{key}' is not supported: {reason}"
            )));
        }

        config.reject_unknown_properties(SOURCE_CONFIG_KEYS, "MongoDB CDC")?;

        let mut cfg = Self {
            connection_uri: config.require("connection.uri")?.to_string(),
            database: config.require("database")?.to_string(),
            collection: config.require("collection")?.to_string(),
            ..Self::default()
        };

        if let Some(mode) = config.get_parsed::<FullDocumentMode>("full.document.mode")? {
            cfg.full_document_mode = mode;
        }
        if let Some(pipeline) = config.get("pipeline") {
            cfg.pipeline = parse_pipeline_property(pipeline)?;
        }
        if let Some(max) = config.get_parsed::<usize>("max.buffered.bytes")? {
            cfg.max_buffered_bytes = max;
        }

        cfg.validate()?;
        Ok(cfg)
    }
}

fn canonicalize_json(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Array(values) => {
            serde_json::Value::Array(values.into_iter().map(canonicalize_json).collect())
        }
        serde_json::Value::Object(object) => {
            let mut fields: Vec<_> = object.into_iter().collect();
            fields.sort_unstable_by(|left, right| left.0.cmp(&right.0));
            serde_json::Value::Object(
                fields
                    .into_iter()
                    .map(|(key, value)| (key, canonicalize_json(value)))
                    .collect(),
            )
        }
        scalar => scalar,
    }
}

pub(crate) fn canonical_pipeline_json(pipeline: &[serde_json::Value]) -> String {
    let canonical = canonicalize_json(serde_json::Value::Array(pipeline.to_vec()));
    serde_json::to_string(&canonical).expect("serde_json::Value serialization cannot fail")
}

fn normalized_pipeline(
    pipeline: &[serde_json::Value],
) -> Result<Vec<serde_json::Value>, ConnectorError> {
    if pipeline.len() > MAX_PIPELINE_STAGES {
        return Err(ConnectorError::ConfigurationError(format!(
            "MongoDB CDC pipeline has {} stages; the maximum is {MAX_PIPELINE_STAGES}",
            pipeline.len()
        )));
    }

    let encoded = serde_json::to_vec(pipeline).map_err(|error| {
        ConnectorError::ConfigurationError(format!(
            "MongoDB CDC pipeline cannot be encoded as JSON: {error}"
        ))
    })?;
    if encoded.len() > MAX_PIPELINE_JSON_BYTES {
        return Err(ConnectorError::ConfigurationError(format!(
            "MongoDB CDC pipeline is {} bytes; the maximum is {MAX_PIPELINE_JSON_BYTES}",
            encoded.len()
        )));
    }

    let canonical: Vec<_> = pipeline.iter().cloned().map(canonicalize_json).collect();
    for (i, stage) in canonical.iter().enumerate() {
        let obj = stage.as_object().ok_or_else(|| {
            ConnectorError::ConfigurationError(format!("pipeline stage {i} must be a JSON object"))
        })?;
        if obj.len() != 1 || !obj.contains_key("$match") {
            return Err(ConnectorError::ConfigurationError(format!(
                "pipeline stage {i} is unsafe: MongoDB CDC only supports $match stages because \
                 projection/replacement stages can alter the resume token"
            )));
        }
        if !obj["$match"].is_object() {
            return Err(ConnectorError::ConfigurationError(format!(
                "pipeline stage {i} must contain a JSON object as $match"
            )));
        }
        mongodb::bson::to_document(stage).map_err(|error| {
            ConnectorError::ConfigurationError(format!(
                "pipeline stage {i} cannot be represented as BSON: {error}"
            ))
        })?;
    }

    let canonical_bytes = canonical_pipeline_json(&canonical).len();
    if canonical_bytes > MAX_PIPELINE_JSON_BYTES {
        return Err(ConnectorError::ConfigurationError(format!(
            "canonical MongoDB CDC pipeline is {canonical_bytes} bytes; the maximum is \
             {MAX_PIPELINE_JSON_BYTES}"
        )));
    }
    Ok(canonical)
}

fn parse_pipeline_property(raw: &str) -> Result<Vec<serde_json::Value>, ConnectorError> {
    if raw.len() > MAX_PIPELINE_JSON_BYTES {
        return Err(ConnectorError::ConfigurationError(format!(
            "MongoDB CDC pipeline property is {} bytes; the maximum is \
             {MAX_PIPELINE_JSON_BYTES}",
            raw.len()
        )));
    }
    let value: serde_json::Value = serde_json::from_str(raw).map_err(|error| {
        ConnectorError::ConfigurationError(format!(
            "MongoDB CDC pipeline must be valid JSON: {error}"
        ))
    })?;
    let serde_json::Value::Array(pipeline) = value else {
        return Err(ConnectorError::ConfigurationError(
            "MongoDB CDC pipeline must be a JSON array".into(),
        ));
    };
    normalized_pipeline(&pipeline)
}

/// Restrict change-stream pipelines to bounded `$match` stages.
fn validate_pipeline(pipeline: &[serde_json::Value]) -> Result<(), ConnectorError> {
    normalized_pipeline(pipeline).map(|_| ())
}

impl MongoDbSourceConfig {
    pub(crate) fn normalize_pipeline(&mut self) -> Result<(), ConnectorError> {
        self.pipeline = normalized_pipeline(&self.pipeline)?;
        Ok(())
    }
}

/// Configuration for the `MongoDB` sink connector.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
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

    /// Maximum time between flushes in milliseconds.
    #[serde(default = "default_flush_interval_ms")]
    pub flush_interval_ms: u64,
}

fn default_flush_interval_ms() -> u64 {
    250
}

impl Default for MongoDbSinkConfig {
    fn default() -> Self {
        Self {
            connection_uri: "mongodb://localhost:27017".to_string(),
            database: String::new(),
            collection: String::new(),
            collection_kind: CollectionKind::default(),
            write_mode: WriteMode::default(),
            flush_interval_ms: default_flush_interval_ms(),
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
        if self.collection == "*" {
            return Err(ConnectorError::ConfigurationError(
                "MongoDB sink collection must name one fixed destination".into(),
            ));
        }

        if self.flush_interval_ms == 0 {
            return Err(ConnectorError::ConfigurationError(
                "flush_interval_ms must be > 0".to_string(),
            ));
        }
        if let WriteMode::Upsert { key_fields } = &self.write_mode {
            let mut unique = std::collections::HashSet::with_capacity(key_fields.len());
            for key in key_fields {
                if key.trim().is_empty() {
                    return Err(ConnectorError::ConfigurationError(
                        "write.mode.key_fields must not contain empty fields".to_string(),
                    ));
                }
                if matches!(key.as_str(), "_op" | "__weight") {
                    return Err(ConnectorError::ConfigurationError(format!(
                        "write.mode.key_fields cannot use engine metadata field '{key}'"
                    )));
                }
                if key.starts_with('$') || key.contains('.') {
                    return Err(ConnectorError::ConfigurationError(format!(
                        "write.mode.key_fields entry '{key}' must be a top-level MongoDB field and cannot start with '$' or contain '.'"
                    )));
                }
                if !unique.insert(key.as_str()) {
                    return Err(ConnectorError::ConfigurationError(format!(
                        "write.mode.key_fields contains duplicate field '{key}'"
                    )));
                }
            }
            if unique.is_empty() {
                return Err(ConnectorError::ConfigurationError(
                    "write.mode.key_fields must contain at least one field".to_string(),
                ));
            }
        }

        // Time series collections only support Insert mode.
        if let CollectionKind::TimeSeries(time_series) = &self.collection_kind {
            time_series.validate()?;
            super::write_model::validate_timeseries_write_mode(&self.write_mode)?;
        }

        Ok(())
    }

    /// Parses configuration from a generic [`ConnectorConfig`].
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError` if required keys are missing or invalid.
    #[allow(clippy::too_many_lines)]
    pub fn from_config(config: &ConnectorConfig) -> Result<Self, ConnectorError> {
        if let Some(key) = REMOVED_SINK_CONFIG_KEYS
            .iter()
            .find(|key| config.get(key).is_some())
        {
            let replacement = match *key {
                "batch.size" => "fixed memory and MongoDB wire limits govern batching",
                "write_concern.timeout_ms" => {
                    "sink.write.timeout.ms governs the complete write deadline"
                }
                _ => unreachable!("removed sink key must have a migration message"),
            };
            return Err(ConnectorError::ConfigurationError(format!(
                "MongoDB sink property '{key}' is not supported; {replacement}"
            )));
        }
        config.reject_unknown_properties(SINK_CONFIG_KEYS, "MongoDB sink")?;

        let mut cfg = Self {
            connection_uri: config.require("connection.uri")?.to_string(),
            database: config.require("database")?.to_string(),
            collection: config.require("collection")?.to_string(),
            ..Self::default()
        };

        if let Some(interval) = config.get_parsed::<u64>("flush.interval.ms")? {
            cfg.flush_interval_ms = interval;
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
                other => {
                    return Err(ConnectorError::ConfigurationError(format!(
                        "unknown write.mode: {other}"
                    )));
                }
            };
        }

        if config.get("write.mode.key_fields").is_some()
            && !matches!(&cfg.write_mode, WriteMode::Upsert { .. })
        {
            return Err(ConnectorError::ConfigurationError(
                "MongoDB sink property 'write.mode.key_fields' is only valid for write.mode=upsert"
                    .to_string(),
            ));
        }
        if let Some(time_field) = config.get("timeseries.time_field") {
            if time_field.trim().is_empty() {
                return Err(ConnectorError::ConfigurationError(
                    "timeseries.time_field must not be empty".to_string(),
                ));
            }
            let meta_field = config
                .get("timeseries.meta_field")
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(String::from);
            let granularity = if let Some(gran_str) = config.get("timeseries.granularity") {
                match gran_str.to_lowercase().as_str() {
                    "seconds" => TimeSeriesGranularity::Seconds,
                    "minutes" => TimeSeriesGranularity::Minutes,
                    "hours" => TimeSeriesGranularity::Hours,
                    "custom" => {
                        let span =
                            config.require_parsed::<u32>("timeseries.bucket_max_span_seconds")?;
                        let rounding =
                            config.require_parsed::<u32>("timeseries.bucket_rounding_seconds")?;
                        TimeSeriesGranularity::custom(span, rounding)?
                    }
                    other => {
                        return Err(ConnectorError::ConfigurationError(format!(
                            "unknown timeseries granularity: {other}"
                        )));
                    }
                }
            } else {
                TimeSeriesGranularity::Seconds
            };

            let expire_after_seconds =
                config.get_parsed::<u64>("timeseries.expire_after_seconds")?;

            cfg.collection_kind = CollectionKind::TimeSeries(TimeSeriesConfig {
                time_field: time_field.to_string(),
                meta_field,
                granularity,
                expire_after_seconds,
            });
        } else if let Some(key) = [
            "timeseries.meta_field",
            "timeseries.granularity",
            "timeseries.bucket_max_span_seconds",
            "timeseries.bucket_rounding_seconds",
            "timeseries.expire_after_seconds",
        ]
        .into_iter()
        .find(|key| config.get(key).is_some())
        {
            return Err(ConnectorError::ConfigurationError(format!(
                "MongoDB sink property '{key}' requires 'timeseries.time_field'"
            )));
        }

        if !matches!(
            &cfg.collection_kind,
            CollectionKind::TimeSeries(TimeSeriesConfig {
                granularity: TimeSeriesGranularity::Custom { .. },
                ..
            })
        ) {
            if let Some(key) = [
                "timeseries.bucket_max_span_seconds",
                "timeseries.bucket_rounding_seconds",
            ]
            .into_iter()
            .find(|key| config.get(key).is_some())
            {
                return Err(ConnectorError::ConfigurationError(format!(
                    "MongoDB sink property '{key}' is only valid for timeseries.granularity=custom"
                )));
            }
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
        assert_eq!(cfg.max_buffered_bytes, 64 * 1024 * 1024);
    }

    #[test]
    fn test_source_config_new() {
        let cfg = MongoDbSourceConfig::new("mongodb://db:27017", "mydb", "users");
        assert_eq!(cfg.connection_uri, "mongodb://db:27017");
        assert_eq!(cfg.database, "mydb");
        assert_eq!(cfg.collection, "users");
    }

    #[test]
    fn test_source_config_validate_empty_uri() {
        let cfg = MongoDbSourceConfig::new("", "db", "coll");
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
    fn source_config_rejects_database_wildcard_watch() {
        let cfg = MongoDbSourceConfig::new("mongodb://db:27017", "db", "*");
        let error = cfg.validate().unwrap_err();
        assert!(error.to_string().contains("fixed collection"), "{error}");
        assert!(error.to_string().contains("UUID"), "{error}");
    }

    #[test]
    fn source_buffer_byte_bound_has_a_finite_operational_range() {
        let mut cfg = MongoDbSourceConfig::new("mongodb://db:27017", "db", "coll");
        for invalid in [MIN_BUFFERED_BYTES - 1, MAX_BUFFERED_BYTES + 1] {
            cfg.max_buffered_bytes = invalid;
            let error = cfg.validate().unwrap_err();
            assert!(error.to_string().contains("max.buffered.bytes"), "{error}");
        }
        for valid in [MIN_BUFFERED_BYTES, MAX_BUFFERED_BYTES] {
            cfg.max_buffered_bytes = valid;
            cfg.validate().unwrap();
        }
    }

    #[test]
    fn removed_source_properties_are_rejected_explicitly() {
        for key in REMOVED_SOURCE_CONFIG_KEYS {
            let mut config = ConnectorConfig::new("mongodb-cdc");
            config.set("connection.uri", "mongodb://host:27017");
            config.set("database", "testdb");
            config.set("collection", "events");
            config.set(*key, "removed-value");
            let error = MongoDbSourceConfig::from_config(&config).unwrap_err();
            assert!(error.to_string().contains(key));
        }
    }

    #[test]
    fn test_source_config_from_connector_config() {
        let mut config = ConnectorConfig::new("mongodb-cdc");
        config.set("connection.uri", "mongodb://host:27017");
        config.set("database", "testdb");
        config.set("collection", "events");
        config.set("full.document.mode", "required");
        config.set(
            "pipeline",
            r#"[{"$match":{"z":1,"operationType":"insert"}}]"#,
        );
        config.set("max.buffered.bytes", "33554432");

        let cfg = MongoDbSourceConfig::from_config(&config).unwrap();
        assert_eq!(cfg.connection_uri, "mongodb://host:27017");
        assert_eq!(cfg.database, "testdb");
        assert_eq!(cfg.collection, "events");
        assert_eq!(cfg.full_document_mode, FullDocumentMode::RequirePostImage);
        assert_eq!(
            canonical_pipeline_json(&cfg.pipeline),
            r#"[{"$match":{"operationType":"insert","z":1}}]"#
        );
        assert_eq!(cfg.max_buffered_bytes, 32 * 1024 * 1024);
        assert_eq!(cfg.reader_channel_capacity(), 512);
        assert_eq!(cfg.cursor_batch_size(), 512);
    }

    #[test]
    fn cursor_batch_hint_is_derived_from_buffer_budget() {
        let mut cfg = MongoDbSourceConfig::new("mongodb://db:27017", "db", "coll");
        cfg.max_buffered_bytes = MIN_BUFFERED_BYTES;
        assert_eq!(cfg.reader_channel_capacity(), 16);
        assert_eq!(cfg.cursor_batch_size(), 16);

        cfg.max_buffered_bytes = DEFAULT_MAX_BUFFERED_BYTES;
        assert_eq!(cfg.reader_channel_capacity(), 1024);
        assert_eq!(cfg.cursor_batch_size(), 1000);

        cfg.max_buffered_bytes = usize::MAX;
        assert_eq!(cfg.reader_channel_capacity(), MAX_READER_CHANNEL_ITEMS);
        assert_eq!(cfg.cursor_batch_size(), 1000);
    }

    #[test]
    fn test_source_config_from_config_missing_required() {
        let config = ConnectorConfig::new("mongodb-cdc");
        assert!(MongoDbSourceConfig::from_config(&config).is_err());
    }

    #[test]
    fn source_config_rejects_unknown_property() {
        let mut config = ConnectorConfig::new("mongodb-cdc");
        config.set("connection.uri", "mongodb://host:27017");
        config.set("database", "testdb");
        config.set("collection", "events");
        config.set("max.await.time.mss", "25");

        let error = MongoDbSourceConfig::from_config(&config).unwrap_err();
        assert!(error.to_string().contains("max.await.time.mss"));
    }

    // ── Pipeline validation tests ──

    #[test]
    fn test_pipeline_valid_match() {
        let pipeline = vec![serde_json::json!({
            "$match": { "operationType": "insert" }
        })];
        validate_pipeline(&pipeline).unwrap();
    }

    #[test]
    fn pipeline_stage_must_be_a_document() {
        let error = validate_pipeline(&[serde_json::json!("$match")]).unwrap_err();
        assert!(error.to_string().contains("must be a JSON object"));
    }

    #[test]
    fn pipeline_stage_must_be_bson_representable() {
        let pipeline = vec![serde_json::json!({
            "$match": { "value": u64::MAX }
        })];
        let error = validate_pipeline(&pipeline).unwrap_err();
        assert!(error.to_string().contains("cannot be represented as BSON"));
    }

    #[test]
    fn pipeline_only_accepts_match_stages() {
        for stage in [
            serde_json::json!({ "$project": { "_id": 1, "name": 1 } }),
            serde_json::json!({ "$unset": "_id" }),
            serde_json::json!({ "$set": { "_id": "overwritten" } }),
            serde_json::json!({ "$replaceRoot": { "newRoot": "$fullDocument" } }),
            serde_json::json!({ "$match": {}, "$project": { "_id": 1 } }),
        ] {
            let error = validate_pipeline(&[stage]).unwrap_err();
            assert!(error.to_string().contains("unsafe"), "{error}");
        }
    }

    #[test]
    fn pipeline_property_requires_a_bounded_json_array() {
        let mut config = ConnectorConfig::new("mongodb-cdc");
        config.set("connection.uri", "mongodb://host:27017");
        config.set("database", "testdb");
        config.set("collection", "events");

        config.set("pipeline", r#"{"$match":{}}"#);
        let error = MongoDbSourceConfig::from_config(&config).unwrap_err();
        assert!(error.to_string().contains("JSON array"), "{error}");

        let stages = vec![serde_json::json!({ "$match": {} }); MAX_PIPELINE_STAGES + 1];
        config.set("pipeline", serde_json::to_string(&stages).unwrap());
        let error = MongoDbSourceConfig::from_config(&config).unwrap_err();
        assert!(error.to_string().contains("maximum"), "{error}");

        let oversized = format!(
            r#"[{{"$match":{{"payload":"{}"}}}}]"#,
            "x".repeat(MAX_PIPELINE_JSON_BYTES)
        );
        config.set("pipeline", oversized);
        let error = MongoDbSourceConfig::from_config(&config).unwrap_err();
        assert!(error.to_string().contains("maximum"), "{error}");
    }

    #[test]
    fn pipeline_is_recursively_canonicalized() {
        let pipeline =
            parse_pipeline_property(r#"[{"$match":{"z":1,"a":{"y":2,"b":3}}}]"#).unwrap();
        assert_eq!(
            canonical_pipeline_json(&pipeline),
            r#"[{"$match":{"a":{"b":3,"y":2},"z":1}}]"#
        );
    }

    #[test]
    fn pipeline_match_expression_must_be_a_document() {
        let error = validate_pipeline(&[serde_json::json!({ "$match": "insert" })]).unwrap_err();
        assert!(error.to_string().contains("as $match"), "{error}");
    }

    // ── Full document mode tests ──

    #[test]
    fn test_full_document_mode_fromstr() {
        assert_eq!(
            "delta".parse::<FullDocumentMode>().unwrap(),
            FullDocumentMode::Delta
        );
        assert_eq!(
            "required".parse::<FullDocumentMode>().unwrap(),
            FullDocumentMode::RequirePostImage
        );
        assert!("update_lookup".parse::<FullDocumentMode>().is_err());
        assert!("when_available".parse::<FullDocumentMode>().is_err());
        assert!("bad".parse::<FullDocumentMode>().is_err());
    }

    #[test]
    fn test_full_document_mode_display() {
        assert_eq!(FullDocumentMode::Delta.to_string(), "delta");
        assert_eq!(FullDocumentMode::RequirePostImage.to_string(), "required");
    }

    // ── Sink config tests ──

    #[test]
    fn test_sink_config_default() {
        let cfg = MongoDbSinkConfig::default();
        assert_eq!(cfg.flush_interval_ms, 250);
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
    fn test_sink_config_validate_zero_flush_interval() {
        let mut cfg = MongoDbSinkConfig::new("mongodb://db:27017", "db", "coll");
        cfg.flush_interval_ms = 0;
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("flush_interval_ms"));
    }

    #[test]
    fn sink_requires_a_fixed_destination_collection() {
        let cfg = MongoDbSinkConfig::new("mongodb://db:27017", "db", "*");
        let error = cfg.validate().unwrap_err();
        assert!(error.to_string().contains("fixed destination"));
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
        assert_eq!(cfg.flush_interval(), Duration::from_millis(250));
    }

    #[test]
    fn test_sink_config_from_connector_config() {
        let mut config = ConnectorConfig::new("mongodb-sink");
        config.set("connection.uri", "mongodb://host:27017");
        config.set("database", "testdb");
        config.set("collection", "out");
        config.set("write.mode", "upsert");
        config.set("write.mode.key_fields", "id");

        let cfg = MongoDbSinkConfig::from_config(&config).unwrap();
        assert!(matches!(cfg.write_mode, WriteMode::Upsert { .. }));
    }

    #[test]
    fn removed_sink_properties_are_rejected() {
        for key in REMOVED_SINK_CONFIG_KEYS {
            let mut config = ConnectorConfig::new("mongodb-sink");
            config.set("connection.uri", "mongodb://host:27017");
            config.set("database", "testdb");
            config.set("collection", "out");
            config.set(*key, "1000");

            let error = MongoDbSinkConfig::from_config(&config).unwrap_err();
            assert!(error.to_string().contains(key));
        }
    }

    #[test]
    fn sink_upsert_keys_must_be_non_empty_and_unique() {
        for keys in [
            vec![],
            vec![String::new()],
            vec!["id".to_string(), "id".to_string()],
            vec!["_op".to_string()],
            vec!["$expr".to_string()],
            vec!["customer.id".to_string()],
        ] {
            let mut config = MongoDbSinkConfig::new("mongodb://host:27017", "db", "out");
            config.write_mode = WriteMode::Upsert { key_fields: keys };
            assert!(config.validate().is_err());
        }
    }

    #[test]
    fn sink_config_rejects_mode_irrelevant_properties() {
        let cases = [
            ("insert", "write.mode.key_fields", "id"),
            ("insert", "ordered", "false"),
            ("insert", "write.mode.upsert_on_missing", "true"),
            ("insert", "write_concern.journal", "false"),
        ];

        for (mode, key, value) in cases {
            let mut config = ConnectorConfig::new("mongodb-sink");
            config.set("connection.uri", "mongodb://host:27017");
            config.set("database", "testdb");
            config.set("collection", "out");
            config.set("write.mode", mode);
            config.set(key, value);

            let error = MongoDbSinkConfig::from_config(&config).unwrap_err();
            assert!(error.to_string().contains(key), "{error}");
        }

        let mut config = ConnectorConfig::new("mongodb-sink");
        config.set("connection.uri", "mongodb://host:27017");
        config.set("database", "testdb");
        config.set("collection", "out");
        config.set("write.mode", "replace");
        let error = MongoDbSinkConfig::from_config(&config).unwrap_err();
        assert!(error.to_string().contains("replace"), "{error}");
    }

    #[test]
    fn sink_config_rejects_irrelevant_timeseries_properties() {
        for key in [
            "timeseries.meta_field",
            "timeseries.granularity",
            "timeseries.bucket_max_span_seconds",
            "timeseries.bucket_rounding_seconds",
            "timeseries.expire_after_seconds",
        ] {
            let mut config = ConnectorConfig::new("mongodb-sink");
            config.set("connection.uri", "mongodb://host:27017");
            config.set("database", "testdb");
            config.set("collection", "out");
            config.set(key, "60");

            let error = MongoDbSinkConfig::from_config(&config).unwrap_err();
            assert!(error.to_string().contains(key), "{error}");
        }

        let mut config = ConnectorConfig::new("mongodb-sink");
        config.set("connection.uri", "mongodb://host:27017");
        config.set("database", "testdb");
        config.set("collection", "out");
        config.set("timeseries.time_field", "timestamp");
        config.set("timeseries.granularity", "seconds");
        config.set("timeseries.bucket_max_span_seconds", "60");

        let error = MongoDbSinkConfig::from_config(&config).unwrap_err();
        assert!(error
            .to_string()
            .contains("timeseries.bucket_max_span_seconds"));
    }

    #[test]
    fn sink_config_rejects_unknown_property() {
        let mut config = ConnectorConfig::new("mongodb-sink");
        config.set("connection.uri", "mongodb://host:27017");
        config.set("database", "testdb");
        config.set("collection", "out");
        config.set("flush.intervall.ms", "10");

        let error = MongoDbSinkConfig::from_config(&config).unwrap_err();
        assert!(error.to_string().contains("flush.intervall.ms"));
    }

    #[test]
    fn test_sink_config_timeseries_parsing() {
        // Test standard granularity with metadata and TTL
        let mut config = ConnectorConfig::new("mongodb-sink");
        config.set("connection.uri", "mongodb://host:27017");
        config.set("database", "testdb");
        config.set("collection", "ts_out");
        config.set("timeseries.time_field", "timestamp");
        config.set("timeseries.meta_field", "sensor_id");
        config.set("timeseries.granularity", "minutes");
        config.set("timeseries.expire_after_seconds", "86400");

        let cfg = MongoDbSinkConfig::from_config(&config).unwrap();
        if let CollectionKind::TimeSeries(ts) = cfg.collection_kind {
            assert_eq!(ts.time_field, "timestamp");
            assert_eq!(ts.meta_field.as_deref(), Some("sensor_id"));
            assert_eq!(ts.granularity, TimeSeriesGranularity::Minutes);
            assert_eq!(ts.expire_after_seconds, Some(86400));
        } else {
            panic!("Expected TimeSeries collection kind");
        }

        // Test custom granularity
        let mut config_custom = ConnectorConfig::new("mongodb-sink");
        config_custom.set("connection.uri", "mongodb://host:27017");
        config_custom.set("database", "testdb");
        config_custom.set("collection", "ts_custom");
        config_custom.set("timeseries.time_field", "timestamp");
        config_custom.set("timeseries.granularity", "custom");
        config_custom.set("timeseries.bucket_max_span_seconds", "3600");
        config_custom.set("timeseries.bucket_rounding_seconds", "3600");

        let cfg_custom = MongoDbSinkConfig::from_config(&config_custom).unwrap();
        if let CollectionKind::TimeSeries(ts) = cfg_custom.collection_kind {
            assert_eq!(ts.time_field, "timestamp");
            assert_eq!(
                ts.granularity,
                TimeSeriesGranularity::Custom {
                    bucket_max_span_seconds: 3600,
                    bucket_rounding_seconds: 3600,
                }
            );
        } else {
            panic!("Expected TimeSeries collection kind");
        }
    }

    #[test]
    fn test_sink_config_timeseries_empty_time_field_rejected() {
        let mut config = ConnectorConfig::new("mongodb-sink");
        config.set("connection.uri", "mongodb://host:27017");
        config.set("database", "testdb");
        config.set("collection", "ts");
        config.set("timeseries.time_field", "  ");
        let err = MongoDbSinkConfig::from_config(&config).unwrap_err();
        assert!(err.to_string().contains("time_field"));
    }
}
