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
pub mod error;
pub mod inference;
pub mod resolver;
pub mod traits;
pub mod types;

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
