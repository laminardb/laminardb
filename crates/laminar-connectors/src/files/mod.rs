//! File source and sink connectors.

use std::sync::Arc;

use crate::config::ConnectorInfo;
use crate::registry::ConnectorRegistry;

pub mod arrow_ipc_codec;
pub mod config;
pub mod discovery;
pub mod manifest;
pub mod sink;
pub mod source;
pub mod text_decoder;

pub use config::{FileFormat, FileSinkConfig, FileSourceConfig};
pub use manifest::FileIngestionManifest;
pub use sink::FileSink;
pub use source::FileSource;
pub use text_decoder::TextLineDecoder;

/// Registers the file source connector in the registry.
///
/// This is called by `LaminarDB::register_builtin_connectors()` when the
/// `files` feature is enabled, and makes `FROM FILES (...)` available in
/// `CREATE SOURCE` statements.
///
/// # Errors
///
/// Returns an error if the connector name is already registered or the registry is frozen.
pub fn register_file_source(
    registry: &ConnectorRegistry,
) -> Result<(), crate::error::ConnectorError> {
    use crate::config::ConfigKeySpec;
    let info = ConnectorInfo {
        name: "files".to_string(),
        display_name: "File Source (AutoLoader)".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        is_source: true,
        is_sink: false,
        config_keys: vec![
            ConfigKeySpec::required("path", "Directory path, glob pattern, or cloud storage URL"),
            ConfigKeySpec::optional(
                "format",
                "Data format (csv, tsv, json, jsonl, text, txt, parquet, arrow)",
                "auto-detect",
            ),
            ConfigKeySpec::optional(
                "glob_pattern",
                "Optional glob pattern to filter files by name",
                "*",
            ),
        ],
    };
    registry.register_source(
        "files",
        info,
        Arc::new(|registry: Option<&Arc<prometheus::Registry>>| {
            Ok(Box::new(FileSource::with_registry(
                registry.map(Arc::as_ref),
            )))
        }),
    )
}

/// Registers the file sink connector in the registry.
///
/// Makes `INTO FILES (...)` available in `CREATE SINK` statements.
///
/// # Errors
///
/// Returns an error if the connector name is already registered or the registry is frozen.
pub fn register_file_sink(
    registry: &ConnectorRegistry,
) -> Result<(), crate::error::ConnectorError> {
    use crate::config::ConfigKeySpec;
    let info = ConnectorInfo {
        name: "files".to_string(),
        display_name: "File Sink".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        is_source: false,
        is_sink: true,
        config_keys: vec![
            ConfigKeySpec::required("path", "Output directory path"),
            ConfigKeySpec::required("format", "Output format (csv, json, text, parquet, arrow)"),
            ConfigKeySpec::optional("prefix", "Immutable output file name prefix", "part"),
        ],
    };
    registry.register_sink(
        "files",
        info,
        Arc::new(|_config, registry: Option<&Arc<prometheus::Registry>>| {
            Ok(Box::new(FileSink::with_registry(registry.map(Arc::as_ref))))
        }),
    )
}
#[cfg(test)]
mod tests;
