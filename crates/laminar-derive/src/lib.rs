//! Derive macros for `LaminarDB`.
//!
//! Provides `#[derive(Record)]`, `#[derive(FromRecordBatch)]`, and
//! `#[derive(ConnectorConfig)]` to eliminate boilerplate.
//!
//! # Example
//!
//! ```rust,ignore
//! use laminar_derive::{Record, FromRecordBatch, ConnectorConfig};
//!
//! #[derive(Record)]
//! struct Trade {
//!     symbol: String,
//!     price: f64,
//!     #[event_time]
//!     timestamp: i64,
//! }
//!
//! #[derive(FromRecordBatch)]
//! struct OhlcBar {
//!     symbol: String,
//!     open: f64,
//!     high: f64,
//!     low: f64,
//!     close: f64,
//! }
//!
//! #[derive(ConnectorConfig)]
//! struct MySourceConfig {
//!     #[config(key = "bootstrap.servers", required)]
//!     bootstrap_servers: String,
//!
//!     #[config(key = "batch.size", default = "1000")]
//!     batch_size: usize,
//!
//!     #[config(key = "timeout.ms", default = "30000", duration_ms)]
//!     timeout: std::time::Duration,
//!
//!     #[config(key = "api.key", env = "MY_API_KEY")]
//!     api_key: Option<String>,
//! }
//! ```

extern crate proc_macro;

use proc_macro::TokenStream;

use syn::{parse_macro_input, DeriveInput};

mod connector_config;
mod from_record_batch;
mod record;

/// Derive the `Record` trait for a struct.
///
/// Generates `Record::schema()`, `Record::to_record_batch()`, and
/// `Record::event_time()` implementations automatically.
///
/// # Supported Field Types
///
/// | Rust Type | Arrow `DataType` |
/// |-----------|-----------------|
/// | `bool` | `Boolean` |
/// | `i8` | `Int8` |
/// | `i16` | `Int16` |
/// | `i32` | `Int32` |
/// | `i64` | `Int64` |
/// | `u8` | `UInt8` |
/// | `u16` | `UInt16` |
/// | `u32` | `UInt32` |
/// | `u64` | `UInt64` |
/// | `f32` | `Float32` |
/// | `f64` | `Float64` |
/// | `String` | `Utf8` |
/// | `Vec<u8>` | `Binary` |
/// | `Option<T>` | nullable variant of `T` |
///
/// # Attributes
///
/// - `#[event_time]` — marks a field as the event time column
/// - `#[column("name")]` — overrides the Arrow column name
/// - `#[nullable]` — marks a non-Option field as nullable in the schema
#[proc_macro_derive(Record, attributes(event_time, column, nullable))]
pub fn derive_record(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    record::expand_record(input)
        .unwrap_or_else(|e| e.to_compile_error())
        .into()
}

/// Derive the `FromRecordBatch` trait for a struct.
///
/// Generates code to deserialize Arrow `RecordBatch` rows into typed structs.
///
/// Fields are matched by name (or `#[column("name")]` override). Type
/// mismatches produce a runtime error.
///
/// # Attributes
///
/// - `#[column("name")]` — maps the field to a different Arrow column name
#[proc_macro_derive(FromRecordBatch, attributes(column))]
pub fn derive_from_record_batch(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    from_record_batch::expand_from_record_batch(input)
        .unwrap_or_else(|e| e.to_compile_error())
        .into()
}

/// Derive `FromRow` for a struct.
///
/// Like `FromRecordBatch`, generates inherent `from_batch` and
/// `from_batch_all` methods, and also implements `laminar_db::FromBatch`.
#[proc_macro_derive(FromRow, attributes(column))]
pub fn derive_from_row(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    from_record_batch::expand_from_row(input)
        .unwrap_or_else(|e| e.to_compile_error())
        .into()
}

/// Derive configuration parsing for connector config structs.
///
/// Generates `from_config()`, `validate()`, and `config_keys()` methods
/// to eliminate boilerplate in connector configuration parsing.
///
/// # Attributes
///
/// - `#[config(key = "...")]` — specifies the config key name
/// - `#[config(required)]` — marks the field as required
/// - `#[config(default = "...")]` — provides a default value
/// - `#[config(env = "...")]` — reads from environment variable as fallback
/// - `#[config(description = "...")]` — documentation for the key
/// - `#[config(duration_ms)]` — parses the value as `Duration` from milliseconds
///
/// # Example
///
/// ```rust,ignore
/// #[derive(ConnectorConfig)]
/// struct MyConfig {
///     #[config(key = "bootstrap.servers", required, description = "Kafka brokers")]
///     bootstrap_servers: String,
///
///     #[config(key = "batch.size", default = "1000")]
///     batch_size: usize,
///
///     #[config(key = "timeout.ms", default = "30000", duration_ms)]
///     timeout: std::time::Duration,
///
///     #[config(key = "api.key", env = "MY_API_KEY")]
///     api_key: Option<String>,
/// }
///
/// // Generated methods:
/// // - MyConfig::from_config(&ConnectorConfig) -> Result<Self, ConnectorError>
/// // - MyConfig::validate(&self) -> Result<(), ConnectorError>
/// // - MyConfig::config_keys() -> Vec<ConfigKeySpec>
/// ```
#[proc_macro_derive(ConnectorConfig, attributes(config))]
pub fn derive_connector_config(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    connector_config::expand_connector_config(input)
        .unwrap_or_else(|e| e.to_compile_error())
        .into()
}
