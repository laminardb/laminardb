//! OpenTelemetry (OTLP/gRPC) source connector.
//!
//! Receives trace spans, metrics, or logs from OTel exporters and
//! collectors via the standard OTLP/gRPC protocol. Each source handles
//! one signal type on its own port.
//!
//! ```sql
//! CREATE SOURCE traces FROM OTEL (port = '4317', signals = 'traces');
//! CREATE SOURCE metrics FROM OTEL (port = '4318', signals = 'metrics');
//! CREATE SOURCE logs FROM OTEL (port = '4319', signals = 'logs');
//! ```

#![allow(clippy::doc_markdown)] // "OTel" is a proper name, not code

pub mod config;
pub mod convert;
pub mod schema;
pub mod server;
pub mod source;

pub use config::{OtelSignal, OtelSourceConfig};
pub use source::OtelSource;

use std::sync::Arc;

use crate::config::ConnectorInfo;
use crate::registry::ConnectorRegistry;

use self::config::otel_source_config_keys;
use self::schema::traces_schema;

/// Registers the OTel source connector with the given registry.
///
/// After registration, the runtime can instantiate `OtelSource` by
/// name when processing `CREATE SOURCE ... FROM OTEL (...)`.
pub fn register_otel_source(registry: &ConnectorRegistry) {
    let info = ConnectorInfo {
        name: "otel".to_string(),
        display_name: "OpenTelemetry OTLP/gRPC Source".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        is_source: true,
        is_sink: false,
        config_keys: otel_source_config_keys(),
    };

    registry.register_source(
        "otel",
        info,
        Arc::new(|| Box::new(OtelSource::new(traces_schema()))),
    );
}
