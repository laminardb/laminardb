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
mod tests;
