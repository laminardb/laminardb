//! JSON format decoder, encoder, and JSONB binary format.
//!
//! Implements F-SCHEMA-004:
//!
//! - [`JsonDecoder`] — Ring 1 decoder: JSON bytes → Arrow `RecordBatch`
//! - [`JsonEncoder`] — Ring 1 encoder: Arrow `RecordBatch` → JSON bytes
//! - [`JsonbEncoder`] / [`JsonbAccessor`] — JSONB binary format for
//!   O(log n) field access on Ring 0

pub mod decoder;
pub mod encoder;
pub mod jsonb;

pub use decoder::{JsonDecoder, JsonDecoderConfig, TypeMismatchStrategy, UnknownFieldStrategy};
pub use encoder::JsonEncoder;
pub use jsonb::{JsonbAccessor, JsonbEncoder};
