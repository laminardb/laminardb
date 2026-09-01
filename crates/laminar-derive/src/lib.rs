//! Derive macros for `LaminarDB`: `Record`, `FromRecordBatch`, `FromRow`,
//! `ConnectorConfig`. See each derive for details.

#![allow(clippy::disallowed_types)]

extern crate proc_macro;

use proc_macro::TokenStream;

use syn::{parse_macro_input, DeriveInput};

mod connector_config;
mod from_record_batch;
mod record;

/// Generates `Record::schema`, `to_record_batch`, `event_time`.
/// Attributes: `#[event_time]`, `#[column("name")]`, `#[nullable]`.
#[proc_macro_derive(Record, attributes(event_time, column, nullable))]
pub fn derive_record(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    record::expand_record(input)
        .unwrap_or_else(|e| e.to_compile_error())
        .into()
}

/// Deserialise Arrow `RecordBatch` rows into typed structs. Fields match by
/// name; `#[column("name")]` overrides. Type mismatches error at runtime.
#[proc_macro_derive(FromRecordBatch, attributes(column))]
pub fn derive_from_record_batch(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    from_record_batch::expand_from_record_batch(input)
        .unwrap_or_else(|e| e.to_compile_error())
        .into()
}

/// Like `FromRecordBatch` plus an impl of `laminar_db::FromBatch`.
#[proc_macro_derive(FromRow, attributes(column))]
pub fn derive_from_row(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    from_record_batch::expand_from_row(input)
        .unwrap_or_else(|e| e.to_compile_error())
        .into()
}

/// Generates `from_config`, `validate`, `config_keys` for connector configs.
/// Attributes: `key`, `required`, `default`, `env`, `description`, `duration_ms`.
#[proc_macro_derive(ConnectorConfig, attributes(config))]
pub fn derive_connector_config(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    connector_config::expand_connector_config(input)
        .unwrap_or_else(|e| e.to_compile_error())
        .into()
}
