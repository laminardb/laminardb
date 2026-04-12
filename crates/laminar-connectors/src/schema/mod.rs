//! Schema evolution, format codecs, and connector schema traits.

pub mod csv;
pub mod error;
pub mod evolution;
pub mod json;
pub mod traits;
pub mod types;

#[cfg(any(feature = "parquet-lookup", feature = "files"))]
pub mod parquet;

pub use csv::{
    CsvDecoder, CsvDecoderConfig, CsvEncoder, CsvEncoderConfig, FieldCountMismatchStrategy,
};
pub use error::{SchemaError, SchemaResult};
pub use evolution::{
    diff_schemas_by_name, is_safe_widening, EvolutionResult, EvolutionTrigger, SchemaEvolution,
    SchemaEvolutionEngine, SchemaHistory, SchemaHistoryEntry,
};
pub use json::{
    JsonDecoder, JsonDecoderConfig, JsonEncoder, JsonbAccessor, JsonbEncoder, TypeMismatchStrategy,
    UnknownFieldStrategy,
};
#[cfg(any(feature = "parquet-lookup", feature = "files"))]
pub use parquet::{
    ParquetDecoder, ParquetDecoderConfig, ParquetEncoder, ParquetEncoderConfig,
    ParquetSchemaProvider, RowGroupPredicate,
};
pub use traits::{
    ArrayInference, ColumnProjection, CompatibilityMode, ConfigOption, ConfigValueType,
    ConnectorConfigSchema, EvolutionVerdict, FieldInferenceDetail, FormatDecoder, FormatEncoder,
    InferenceConfig, InferenceWarning, InferredSchema, NumberInference, RegisteredSchema,
    RegistryConfig, RegistryCredentials, RegistrySchemaType, SchemaChange, SchemaProvider,
    SchemaRegistryAware, WarningSeverity,
};
pub use types::{FieldMeta, RawRecord, SinkConfig, SourceConfig, SourceMetadata};
