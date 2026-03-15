//! JSON format decoder, encoder, and JSONB binary format.
//!
//! - [`JsonDecoder`] — decoder: JSON bytes → Arrow `RecordBatch`
//! - [`JsonEncoder`] — encoder: Arrow `RecordBatch` → JSON bytes
//! - [`JsonbEncoder`] / [`JsonbAccessor`] — JSONB binary format for
//!   O(log n) field access

pub mod decoder;
pub mod encoder;
pub mod jsonb;

pub use decoder::{JsonDecoder, JsonDecoderConfig, TypeMismatchStrategy, UnknownFieldStrategy};
pub use encoder::JsonEncoder;
pub use jsonb::{JsonbAccessor, JsonbEncoder};
