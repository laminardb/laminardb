//! MongoDB CDC source connector configuration.
//!
//! Provides [`MongoCdcConfig`] with all settings needed to connect to
//! a MongoDB deployment and stream change events.

use std::time::Duration;

use crate::config::ConnectorConfig;
use crate::error::ConnectorError;

/// Configuration for the MongoDB CDC source connector.
#[derive(Debug, Clone)]
pub struct MongoCdcConfig {
    // ── Connection ──
    /// MongoDB connection URI (e.g., `mongodb://host:27017`).
    pub connection_uri: String,

    /// Database name to watch for changes.
    pub database: String,

    /// Collection name to watch (empty = watch entire database).
    pub collection: Option<String>,

    // ── Change Stream ──
    /// Aggregation pipeline stages to filter the change stream.
    /// Each entry is a JSON document string (e.g., `{"$match": {"operationType": "insert"}}`).
    pub pipeline: Vec<String>,

    /// Resume token from a previous change stream position.
    pub resume_token: Option<String>,

    /// Whether to include the full document in change events.
    pub full_document: FullDocumentMode,

    /// Whether to include the pre-image of the document before the change.
    pub full_document_before_change: FullDocumentBeforeChangeMode,

    // ── Tuning ──
    /// Timeout for each poll operation.
    pub poll_timeout: Duration,

    /// Maximum records to return per poll.
    pub max_poll_records: usize,

    /// Maximum number of change events to buffer.
    pub max_buffered_events: usize,

    // ── Filtering ──
    /// Collections to include (empty = all collections in database).
    pub collection_include: Vec<String>,

    /// Collections to exclude from change stream.
    pub collection_exclude: Vec<String>,
}

impl Default for MongoCdcConfig {
    fn default() -> Self {
        Self {
            connection_uri: "mongodb://localhost:27017".to_string(),
            database: String::new(),
            collection: None,
            pipeline: Vec::new(),
            resume_token: None,
            full_document: FullDocumentMode::UpdateLookup,
            full_document_before_change: FullDocumentBeforeChangeMode::Off,
            poll_timeout: Duration::from_millis(100),
            max_poll_records: 1000,
            max_buffered_events: 100_000,
            collection_include: Vec::new(),
            collection_exclude: Vec::new(),
        }
    }
}

impl MongoCdcConfig {
    /// Creates a new config with required fields.
    #[must_use]
    pub fn new(connection_uri: &str, database: &str) -> Self {
        Self {
            connection_uri: connection_uri.to_string(),
            database: database.to_string(),
            ..Self::default()
        }
    }

    /// Creates a config targeting a specific collection.
    #[must_use]
    pub fn with_collection(connection_uri: &str, database: &str, collection: &str) -> Self {
        Self {
            connection_uri: connection_uri.to_string(),
            database: database.to_string(),
            collection: Some(collection.to_string()),
            ..Self::default()
        }
    }

    /// Parses configuration from a generic [`ConnectorConfig`].
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError` if required keys are missing or values are
    /// invalid.
    pub fn from_config(config: &ConnectorConfig) -> Result<Self, ConnectorError> {
        let mut cfg = Self {
            connection_uri: config.require("connection.uri")?.to_string(),
            database: config.require("database")?.to_string(),
            ..Self::default()
        };

        cfg.collection = config.get("collection").map(String::from);

        if let Some(pipeline) = config.get("pipeline") {
            cfg.pipeline = pipeline
                .split(';')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
        }

        cfg.resume_token = config.get("resume.token").map(String::from);

        if let Some(mode) = config.get_parsed::<FullDocumentMode>("full.document")? {
            cfg.full_document = mode;
        }
        if let Some(mode) =
            config.get_parsed::<FullDocumentBeforeChangeMode>("full.document.before.change")?
        {
            cfg.full_document_before_change = mode;
        }

        if let Some(timeout) = config.get_parsed::<u64>("poll.timeout.ms")? {
            cfg.poll_timeout = Duration::from_millis(timeout);
        }
        if let Some(max) = config.get_parsed::<usize>("max.poll.records")? {
            cfg.max_poll_records = max;
        }
        if let Some(max) = config.get_parsed::<usize>("max.buffered.events")? {
            cfg.max_buffered_events = max;
        }

        if let Some(collections) = config.get("collection.include") {
            cfg.collection_include = collections
                .split(',')
                .map(|s| s.trim().to_string())
                .collect();
        }
        if let Some(collections) = config.get("collection.exclude") {
            cfg.collection_exclude = collections
                .split(',')
                .map(|s| s.trim().to_string())
                .collect();
        }

        cfg.validate()?;
        Ok(cfg)
    }

    /// Validates the configuration.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError::ConfigurationError` for invalid settings.
    pub fn validate(&self) -> Result<(), ConnectorError> {
        crate::config::require_non_empty(&self.connection_uri, "connection.uri")?;
        crate::config::require_non_empty(&self.database, "database")?;
        if self.max_poll_records == 0 {
            return Err(ConnectorError::ConfigurationError(
                "max.poll.records must be > 0".to_string(),
            ));
        }
        if self.max_buffered_events == 0 {
            return Err(ConnectorError::ConfigurationError(
                "max.buffered.events must be > 0".to_string(),
            ));
        }
        Ok(())
    }

    /// Returns whether a collection should be included based on include/exclude lists.
    #[must_use]
    pub fn should_include_collection(&self, collection: &str) -> bool {
        if self.collection_exclude.iter().any(|c| c == collection) {
            return false;
        }
        if self.collection_include.is_empty() {
            return true;
        }
        self.collection_include.iter().any(|c| c == collection)
    }
}

/// Controls whether the full document is included in change events.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FullDocumentMode {
    /// Do not include the full document.
    Off,
    /// Include the full document for update events (lookup the current version).
    #[default]
    UpdateLookup,
    /// Include the full document for all events (requires MongoDB 6.0+).
    WhenAvailable,
    /// Require the full document for all events (fails if unavailable).
    Required,
}

str_enum!(FullDocumentMode, lowercase_nodash, String, "unknown full document mode",
    Off => "off", "default";
    UpdateLookup => "updatelookup", "update_lookup", "update-lookup";
    WhenAvailable => "whenavailable", "when_available", "when-available";
    Required => "required"
);

/// Controls whether the pre-image is included in change events.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FullDocumentBeforeChangeMode {
    /// Do not include the pre-image.
    #[default]
    Off,
    /// Include the pre-image when available.
    WhenAvailable,
    /// Require the pre-image (fails if unavailable).
    Required,
}

str_enum!(FullDocumentBeforeChangeMode, lowercase_nodash, String, "unknown full document before change mode",
    Off => "off";
    WhenAvailable => "whenavailable", "when_available", "when-available";
    Required => "required"
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let cfg = MongoCdcConfig::default();
        assert_eq!(cfg.connection_uri, "mongodb://localhost:27017");
        assert!(cfg.database.is_empty());
        assert!(cfg.collection.is_none());
        assert!(cfg.pipeline.is_empty());
        assert!(cfg.resume_token.is_none());
        assert_eq!(cfg.full_document, FullDocumentMode::UpdateLookup);
        assert_eq!(
            cfg.full_document_before_change,
            FullDocumentBeforeChangeMode::Off
        );
        assert_eq!(cfg.max_poll_records, 1000);
        assert_eq!(cfg.max_buffered_events, 100_000);
    }

    #[test]
    fn test_new_config() {
        let cfg = MongoCdcConfig::new("mongodb://db.example.com:27017", "mydb");
        assert_eq!(cfg.connection_uri, "mongodb://db.example.com:27017");
        assert_eq!(cfg.database, "mydb");
        assert!(cfg.collection.is_none());
    }

    #[test]
    fn test_with_collection() {
        let cfg = MongoCdcConfig::with_collection("mongodb://localhost:27017", "mydb", "users");
        assert_eq!(cfg.database, "mydb");
        assert_eq!(cfg.collection.as_deref(), Some("users"));
    }

    #[test]
    fn test_from_connector_config() {
        let mut config = ConnectorConfig::new("mongodb-cdc");
        config.set("connection.uri", "mongodb://mongo.local:27017");
        config.set("database", "testdb");
        config.set("collection", "events");
        config.set("full.document", "when_available");
        config.set("max.poll.records", "500");

        let cfg = MongoCdcConfig::from_config(&config).unwrap();
        assert_eq!(cfg.connection_uri, "mongodb://mongo.local:27017");
        assert_eq!(cfg.database, "testdb");
        assert_eq!(cfg.collection.as_deref(), Some("events"));
        assert_eq!(cfg.full_document, FullDocumentMode::WhenAvailable);
        assert_eq!(cfg.max_poll_records, 500);
    }

    #[test]
    fn test_from_config_missing_required() {
        let config = ConnectorConfig::new("mongodb-cdc");
        assert!(MongoCdcConfig::from_config(&config).is_err());
    }

    #[test]
    fn test_from_config_missing_database() {
        let mut config = ConnectorConfig::new("mongodb-cdc");
        config.set("connection.uri", "mongodb://localhost:27017");
        assert!(MongoCdcConfig::from_config(&config).is_err());
    }

    #[test]
    fn test_validate_empty_uri() {
        let mut cfg = MongoCdcConfig::new("", "db");
        cfg.connection_uri = String::new();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_validate_empty_database() {
        let mut cfg = MongoCdcConfig::default();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_validate_zero_max_poll() {
        let mut cfg = MongoCdcConfig::new("mongodb://localhost:27017", "db");
        cfg.max_poll_records = 0;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_validate_zero_max_buffered() {
        let mut cfg = MongoCdcConfig::new("mongodb://localhost:27017", "db");
        cfg.max_buffered_events = 0;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_full_document_mode_fromstr() {
        assert_eq!(
            "off".parse::<FullDocumentMode>().unwrap(),
            FullDocumentMode::Off
        );
        assert_eq!(
            "updatelookup".parse::<FullDocumentMode>().unwrap(),
            FullDocumentMode::UpdateLookup
        );
        assert_eq!(
            "update_lookup".parse::<FullDocumentMode>().unwrap(),
            FullDocumentMode::UpdateLookup
        );
        assert_eq!(
            "whenavailable".parse::<FullDocumentMode>().unwrap(),
            FullDocumentMode::WhenAvailable
        );
        assert_eq!(
            "required".parse::<FullDocumentMode>().unwrap(),
            FullDocumentMode::Required
        );
        assert!("bad".parse::<FullDocumentMode>().is_err());
    }

    #[test]
    fn test_full_document_before_change_fromstr() {
        assert_eq!(
            "off".parse::<FullDocumentBeforeChangeMode>().unwrap(),
            FullDocumentBeforeChangeMode::Off
        );
        assert_eq!(
            "when_available"
                .parse::<FullDocumentBeforeChangeMode>()
                .unwrap(),
            FullDocumentBeforeChangeMode::WhenAvailable
        );
        assert_eq!(
            "required".parse::<FullDocumentBeforeChangeMode>().unwrap(),
            FullDocumentBeforeChangeMode::Required
        );
        assert!("bad".parse::<FullDocumentBeforeChangeMode>().is_err());
    }

    #[test]
    fn test_collection_filtering() {
        let mut cfg = MongoCdcConfig::new("mongodb://localhost:27017", "db");
        // No filters -> include all
        assert!(cfg.should_include_collection("users"));

        // Include list
        cfg.collection_include = vec!["users".to_string(), "orders".to_string()];
        assert!(cfg.should_include_collection("users"));
        assert!(!cfg.should_include_collection("logs"));

        // Exclude overrides include
        cfg.collection_exclude = vec!["users".to_string()];
        assert!(!cfg.should_include_collection("users"));
    }

    #[test]
    fn test_from_config_with_pipeline() {
        let mut config = ConnectorConfig::new("mongodb-cdc");
        config.set("connection.uri", "mongodb://localhost:27017");
        config.set("database", "testdb");
        config.set(
            "pipeline",
            r#"{"$match": {"operationType": "insert"}}; {"$project": {"fullDocument": 1}}"#,
        );

        let cfg = MongoCdcConfig::from_config(&config).unwrap();
        assert_eq!(cfg.pipeline.len(), 2);
    }

    #[test]
    fn test_from_config_collection_include() {
        let mut config = ConnectorConfig::new("mongodb-cdc");
        config.set("connection.uri", "mongodb://localhost:27017");
        config.set("database", "testdb");
        config.set("collection.include", "users, orders");

        let cfg = MongoCdcConfig::from_config(&config).unwrap();
        assert_eq!(cfg.collection_include, vec!["users", "orders"]);
    }

    #[test]
    fn test_from_config_with_resume_token() {
        let mut config = ConnectorConfig::new("mongodb-cdc");
        config.set("connection.uri", "mongodb://localhost:27017");
        config.set("database", "testdb");
        config.set("resume.token", "82636F6C6C...");

        let cfg = MongoCdcConfig::from_config(&config).unwrap();
        assert_eq!(cfg.resume_token.as_deref(), Some("82636F6C6C..."));
    }
}
