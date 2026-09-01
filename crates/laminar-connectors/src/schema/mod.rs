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
    ParquetDecoder, ParquetDecoderConfig, ParquetEncoder, ParquetEncoderConfig, RowGroupPredicate,
};
pub use traits::{
    ArrayInference, ColumnProjection, CompatibilityMode, EvolutionVerdict, FieldInferenceDetail,
    FormatDecoder, FormatEncoder, InferenceConfig, InferenceWarning, InferredSchema,
    NumberInference, SchemaChange, WarningSeverity,
};
pub use types::{FieldMeta, RawRecord, SourceMetadata};
