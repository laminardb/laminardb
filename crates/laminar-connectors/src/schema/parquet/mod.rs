//! Parquet format decoder, encoder, and schema provider (F-SCHEMA-007).
//!
//! Feature-gated behind `parquet-lookup`. Provides:
//!
//! - [`ParquetDecoder`] — decodes complete Parquet file bytes into Arrow
//!   `RecordBatch`es via `ParquetRecordBatchReaderBuilder` with projection
//!   pushdown and row-group filtering.
//! - [`ParquetEncoder`] — encodes Arrow `RecordBatch`es into Parquet file
//!   bytes with configurable compression and row-group sizing.
//! - [`ParquetSchemaProvider`] — reads Parquet file footer to extract an
//!   authoritative Arrow schema with per-field metadata.

mod decoder;
mod encoder;
mod provider;

pub use decoder::{ParquetDecoder, ParquetDecoderConfig, RowGroupPredicate};
pub use encoder::{ParquetEncoder, ParquetEncoderConfig};
pub use provider::ParquetSchemaProvider;
