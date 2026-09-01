//! Parquet format decoder and encoder (F-SCHEMA-007).
//!
//! Feature-gated behind `parquet-lookup`. Provides:
//!
//! - [`ParquetDecoder`] — decodes complete Parquet file bytes into Arrow
//!   `RecordBatch`es via `ParquetRecordBatchReaderBuilder` with projection
//!   pushdown and row-group filtering.
//! - [`ParquetEncoder`] — encodes Arrow `RecordBatch`es into Parquet file
//!   bytes with configurable compression and row-group sizing.

mod decoder;
mod encoder;

pub use decoder::{ParquetDecoder, ParquetDecoderConfig, RowGroupPredicate};
pub use encoder::{ParquetEncoder, ParquetEncoderConfig};
