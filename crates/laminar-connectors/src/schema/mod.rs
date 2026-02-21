//! Schema inference, resolution, and evolution framework.
//!
//! This module implements the foundation of LaminarDB's extensible
//! connector schema system (F-SCHEMA-001/002/003):
//!
//! - **Capability traits** ([`traits`]) — six opt-in traits that connectors
//!   implement to declare schema capabilities
//! - **Schema resolver** ([`resolver`]) — five-level priority chain for
//!   determining a source's Arrow schema
//! - **Format inference** ([`inference`]) — registry of format-specific
//!   schema inferencers with built-in JSON, CSV, and raw support
//! - **Bridge adapters** ([`bridge`]) — adapters between legacy
//!   [`RecordDeserializer`](crate::serde::RecordDeserializer) /
//!   [`RecordSerializer`](crate::serde::RecordSerializer) and the new
//!   `FormatDecoder` / `FormatEncoder` traits
//! - **JSON format** ([`json`]) — JSON decoder, encoder, and JSONB binary
//!   format (F-SCHEMA-004)
//! - **CSV format** ([`csv`]) — CSV decoder with DuckDB-style type coercion
//!   (F-SCHEMA-005)
//! - **Schema evolution** ([`evolution`]) — diff, evaluate, and apply schema
//!   changes with configurable compatibility modes (F-SCHEMA-009)
//!
//! # Architecture
//!
//! ```text
//! SourceConnector
//!   ├── as_schema_provider()      → SchemaProvider
//!   ├── as_schema_inferable()     → SchemaInferable
//!   ├── as_schema_registry_aware()→ SchemaRegistryAware
//!   └── as_schema_evolvable()     → SchemaEvolvable
//!
//! SchemaResolver::resolve()
//!   1. Full DDL         → Declared
//!   2. Registry         → Registry { schema_id }
//!   3. Provider         → SourceProvided
//!   4. Inference        → Inferred { sample_count }
//!   5. Error
//! ```

pub mod bridge;
pub mod csv;
pub mod error;
pub mod evolution;
pub mod inference;
pub mod json;
pub mod resolver;
pub mod traits;
pub mod types;

#[cfg(feature = "kafka")]
pub mod avro;

#[cfg(feature = "parquet-lookup")]
pub mod parquet;

// ── Re-exports for convenience ─────────────────────────────────────

pub use error::{SchemaError, SchemaResult};
pub use inference::{
    CsvFormatInference, FormatInference, FormatInferenceRegistry, JsonFormatInference,
    RawFormatInference, FORMAT_INFERENCE_REGISTRY,
};
pub use resolver::{
    DeclaredColumn, DeclaredSchema, FieldOrigin, ResolutionKind, ResolvedSchema, SchemaResolver,
};
pub use traits::{
    ColumnProjection, CompatibilityMode, ConfigOption, ConfigValueType, ConnectorConfigSchema,
    EvolutionVerdict, FieldInferenceDetail, FormatDecoder, FormatEncoder, InferenceConfig,
    InferenceWarning, InferredSchema, NumberInference, ArrayInference, RegisteredSchema,
    RegistryConfig, RegistryCredentials, RegistrySchemaType, SchemaChange, SchemaEvolvable,
    SchemaInferable, SchemaProvider, SchemaRegistryAware, WarningSeverity,
};
pub use types::{FieldMeta, RawRecord, SinkConfig, SourceConfig, SourceMetadata};
pub use csv::{CsvDecoder, CsvDecoderConfig, FieldCountMismatchStrategy};
pub use evolution::{
    DefaultSchemaEvolver, EvolutionResult, EvolutionTrigger, SchemaEvolutionEngine,
    SchemaHistory, SchemaHistoryEntry, diff_schemas_by_name, is_safe_widening,
};
pub use json::{
    JsonDecoder, JsonDecoderConfig, JsonEncoder, JsonbAccessor, JsonbEncoder,
    TypeMismatchStrategy, UnknownFieldStrategy,
};
#[cfg(feature = "kafka")]
pub use avro::{
    AvroDecoderMode, AvroFormatDecoder, AvroFormatEncoder, avro_to_arrow_schema,
    avro_to_arrow_type,
};
#[cfg(feature = "parquet-lookup")]
pub use parquet::{
    ParquetDecoder, ParquetDecoderConfig, ParquetEncoder, ParquetEncoderConfig,
    ParquetSchemaProvider, RowGroupPredicate,
};
