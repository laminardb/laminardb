//! CSV format decoder implementing [`FormatDecoder`](super::traits::FormatDecoder).
//!
//! Implements F-SCHEMA-005:
//!
//! - [`CsvDecoder`] — Ring 1 decoder: CSV bytes → Arrow `RecordBatch`
//! - [`CsvDecoderConfig`] — Full `FORMAT CSV (...)` SQL option mapping
//! - [`FieldCountMismatchStrategy`] — Configurable handling of malformed rows

pub mod decoder;

pub use decoder::{CsvDecoder, CsvDecoderConfig, FieldCountMismatchStrategy};
