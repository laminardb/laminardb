//! Protobuf format decoder (F-SCHEMA-008).
//!
//! Feature-gated behind `protobuf`. Provides:
//!
//! - [`ProtobufDecoder`] — decodes binary protobuf messages into Arrow
//!   `RecordBatch`es using self-describing `FileDescriptorSet` metadata.
//! - [`ProtobufDecoderConfig`] — configuration for message type selection
//!   and decoding options.

mod decoder;

pub use decoder::{ProtobufDecoder, ProtobufDecoderConfig};
