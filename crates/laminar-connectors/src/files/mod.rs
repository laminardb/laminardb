//! AutoLoader-style file source and sink connectors.
//!
//! Watches local directories or cloud storage for new files, infers and
//! evolves schemas automatically, and checkpoints exactly which files have
//! been processed.
//!
//! # Formats
//!
//! - CSV (via [`CsvDecoder`](crate::schema::CsvDecoder))
//! - JSON Lines (via [`JsonDecoder`](crate::schema::JsonDecoder))
//! - Plain text (via [`TextLineDecoder`](crate::files::text_decoder::TextLineDecoder))
//! - Apache Parquet (via [`ParquetDecoder`](crate::schema::parquet::ParquetDecoder))
//!
//! # Example DDL
//!
//! ```sql
//! CREATE SOURCE logs (ts BIGINT, level VARCHAR, message VARCHAR)
//! WITH (
//!     connector = 'files',
//!     path = '/data/logs/*.csv',
//!     format = 'csv'
//! );
//! ```

use std::sync::Arc;

use crate::config::ConnectorInfo;
use crate::registry::ConnectorRegistry;

pub mod config;
pub mod discovery;
pub mod manifest;
pub mod sink;
pub mod source;
pub mod text_decoder;

pub use config::{FileFormat, FileSinkConfig, FileSourceConfig, SinkMode};
pub use manifest::{FileEntry, FileIngestionManifest};
pub use sink::FileSink;
pub use source::FileSource;
pub use text_decoder::TextLineDecoder;

/// Registers the file source connector in the registry.
///
/// This is called by `LaminarDB::register_builtin_connectors()` when the
/// `files` feature is enabled, and makes `connector = 'files'` available
/// in `CREATE SOURCE` statements.
pub fn register_file_source(registry: &ConnectorRegistry) {
    let info = ConnectorInfo {
        name: "files".to_string(),
        display_name: "File Source (AutoLoader)".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        is_source: true,
        is_sink: false,
        config_keys: vec![],
    };
    registry.register_source("files", info, Arc::new(|| Box::new(FileSource::new())));
}

/// Registers the file sink connector in the registry.
///
/// Makes `connector = 'files'` available in `CREATE SINK` statements.
pub fn register_file_sink(registry: &ConnectorRegistry) {
    let info = ConnectorInfo {
        name: "files".to_string(),
        display_name: "File Sink".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        is_source: false,
        is_sink: true,
        config_keys: vec![],
    };
    registry.register_sink("files", info, Arc::new(|| Box::new(FileSink::new())));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_register_file_source() {
        let registry = ConnectorRegistry::new();
        register_file_source(&registry);

        let sources = registry.list_sources();
        assert!(sources.contains(&"files".to_string()));

        let info = registry.source_info("files").unwrap();
        assert_eq!(info.name, "files");
        assert!(info.is_source);
        assert!(!info.is_sink);
    }

    #[test]
    fn test_register_file_sink() {
        let registry = ConnectorRegistry::new();
        register_file_sink(&registry);

        let sinks = registry.list_sinks();
        assert!(sinks.contains(&"files".to_string()));

        let info = registry.sink_info("files").unwrap();
        assert_eq!(info.name, "files");
        assert!(!info.is_source);
        assert!(info.is_sink);
    }

    #[test]
    fn test_create_source_from_registry() {
        let registry = ConnectorRegistry::new();
        register_file_source(&registry);

        let config = crate::config::ConnectorConfig::new("files");
        let source = registry.create_source(&config);
        assert!(source.is_ok());
    }

    #[test]
    fn test_create_sink_from_registry() {
        let registry = ConnectorRegistry::new();
        register_file_sink(&registry);

        let config = crate::config::ConnectorConfig::new("files");
        let sink = registry.create_sink(&config);
        assert!(sink.is_ok());
    }
}
