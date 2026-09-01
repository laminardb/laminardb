//! DataFusion integration for SQL processing.

/// Marker UDFs for the `ai_*` SQL functions (rewritten by the AI operator).
pub mod ai_udf;
mod bridge;
mod channel_source;
/// Lambda higher-order functions for arrays and maps (F-SCHEMA-015 Tier 3)
pub mod complex_type_lambda;
/// Array, Struct, and Map scalar UDFs (F-SCHEMA-015)
pub mod complex_type_udf;
mod exec;
/// End-to-end streaming SQL execution
pub mod execute;
/// Format bridge UDFs for inline format conversion
pub mod format_bridge_udf;
/// LaminarDB streaming JSON extension UDFs (F-SCHEMA-013)
pub mod json_extensions;
/// SQL/JSON path query compiler and scalar UDFs
pub mod json_path;
/// JSON table-valued functions (array/object expansion)
pub mod json_tvf;
/// JSONB binary format types for JSON UDF evaluation
pub mod json_types;
/// PostgreSQL-compatible JSON aggregate UDAFs
pub mod json_udaf;
/// PostgreSQL-compatible JSON scalar UDFs
pub mod json_udf;
/// Live source provider for streaming execution with plan caching
pub mod live_source;
/// Lookup join plan node for DataFusion.
pub mod lookup_join;
/// Physical execution plan and extension planner for lookup joins.
pub mod lookup_join_exec;
/// Processing-time UDF for `PROCTIME()` support
pub mod proctime_udf;
mod source;
mod table_provider;
/// Dynamic watermark filter for scan-level late-data pruning
/// Watermark UDF for current watermark access
pub mod watermark_udf;
/// Window function UDFs (TUMBLE, HOP, SESSION, CUMULATE)
pub mod window_udf;

pub use ai_udf::{ai_function_markers, AiFunctionMarker};
pub use bridge::{BridgeSendError, BridgeSender, BridgeStream, BridgeTrySendError, StreamBridge};
pub use channel_source::ChannelStreamSource;
pub use complex_type_lambda::{
    register_lambda_functions, ArrayFilter, ArrayReduce, ArrayTransform, MapFilter,
    MapTransformValues,
};
pub use complex_type_udf::{
    register_complex_type_functions, MapContainsKey, MapFromArrays, MapKeys, MapValues, StructDrop,
    StructExtract, StructMerge, StructRename, StructSet,
};
pub use exec::StreamingScanExec;
pub use execute::{execute_streaming_sql, DdlResult, QueryResult, StreamingSqlResult};
pub use format_bridge_udf::{FromJsonUdf, ParseEpochUdf, ParseTimestampUdf, ToJsonUdf};
pub use json_extensions::{
    register_json_extensions, JsonInferSchema, JsonToColumns, JsonbDeepMerge, JsonbExcept,
    JsonbFlatten, JsonbMerge, JsonbPick, JsonbRenameKeys, JsonbStripNulls, JsonbUnflatten,
};
pub use json_path::{CompiledJsonPath, JsonPathStep, JsonbPathExistsUdf, JsonbPathMatchUdf};
pub use json_tvf::{
    register_json_table_functions, JsonbArrayElementsTextTvf, JsonbArrayElementsTvf,
    JsonbEachTextTvf, JsonbEachTvf, JsonbObjectKeysTvf,
};
pub use json_udaf::{JsonAgg, JsonObjectAgg};
pub use json_udf::{
    JsonBuildArray, JsonBuildObject, JsonTypeof, JsonbContainedBy, JsonbContains, JsonbExists,
    JsonbExistsAll, JsonbExistsAny, JsonbGet, JsonbGetIdx, JsonbGetPath, JsonbGetPathText,
    JsonbGetText, JsonbGetTextIdx, ToJsonb,
};
pub use live_source::{LiveSourceHandle, LiveSourceProvider};
pub use lookup_join_exec::{
    LookupJoinExec, LookupJoinExtensionPlanner, LookupSnapshot, LookupTableRegistry,
    PartialLookupJoinExec, PartialLookupState, RegisteredLookup,
};
pub use proctime_udf::ProcTimeUdf;
pub use source::{SortColumn, StreamSource, StreamSourceRef};
pub use table_provider::StreamingTableProvider;
pub use watermark_udf::WatermarkUdf;
pub use window_udf::{
    CumulateWindowEnd, CumulateWindowStart, HopWindowEnd, HopWindowStart, SessionWindowStart,
    TumbleWindowEnd, TumbleWindowStart,
};

use std::sync::atomic::AtomicI64;
use std::sync::Arc;

use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::*;
use datafusion_expr::ScalarUDF;

use crate::planner::streaming_optimizer::{StreamingPhysicalValidator, StreamingValidatorMode};

/// Returns a base `SessionConfig` with identifier normalization disabled.
///
/// DataFusion's default behaviour lowercases all unquoted SQL identifiers
/// (per the SQL standard). LaminarDB disables this so that mixed-case
/// column names from external sources (Kafka, CDC, WebSocket) can be
/// referenced without double-quoting.
#[must_use]
pub fn base_session_config() -> SessionConfig {
    let mut config = SessionConfig::new();
    config.options_mut().sql_parser.enable_ident_normalization = false;
    // Single partition for streaming micro-batch execution. Multi-partition
    // plans contain stateful operators (RepartitionExec) that cannot be
    // reused across cycles, causing panics on cached physical plans.
    config = config.with_target_partitions(1);
    config
}

/// Creates a `DataFusion` session context with identifier normalization
/// disabled.
///
/// Suitable for ad-hoc / non-streaming queries (filters, lookups).
/// For streaming workloads prefer [`create_streaming_context`].
#[must_use]
pub fn create_session_context() -> SessionContext {
    SessionContext::new_with_config(base_session_config())
}

/// Creates a `DataFusion` session context configured for streaming queries.
///
/// The context is configured with:
/// - Batch size of 8192 (balanced for streaming throughput)
/// - Single partition (streaming sources are typically not partitioned)
/// - Identifier normalization disabled (mixed-case columns work unquoted)
/// - All streaming UDFs registered (TUMBLE, HOP, SESSION, WATERMARK)
/// - `StreamingPhysicalValidator` in `Reject` mode (blocks unsafe plans)
///
/// The watermark UDF is initialized with no watermark set (returns NULL).
/// Use [`register_streaming_functions_with_watermark`] to provide a live
/// watermark source.
///
/// # Example
///
/// ```rust,ignore
/// let ctx = create_streaming_context();
/// ctx.register_table("events", provider)?;
/// let df = ctx.sql("SELECT * FROM events").await?;
/// ```
#[must_use]
pub fn create_streaming_context() -> SessionContext {
    create_streaming_context_with_validator(StreamingValidatorMode::Reject)
}

/// Creates a streaming context with a configurable validator mode.
///
/// Same as [`create_streaming_context`] but allows choosing how the
/// [`StreamingPhysicalValidator`] handles plan violations.
///
/// Use [`StreamingValidatorMode::Off`] to get the previous behaviour
/// (no plan-time validation).
#[must_use]
pub fn create_streaming_context_with_validator(mode: StreamingValidatorMode) -> SessionContext {
    let config = base_session_config().with_batch_size(8192);

    let ctx = if matches!(mode, StreamingValidatorMode::Off) {
        SessionContext::new_with_config(config)
    } else {
        // Build a default state to get the standard optimizer rules, then
        // prepend our streaming validator so it fires before DataFusion's
        // built-in SanityCheckPlan (which produces generic error messages).
        let default_state = SessionStateBuilder::new()
            .with_config(config.clone())
            .with_default_features()
            .build();
        let mut rules: Vec<
            Arc<dyn datafusion::physical_optimizer::PhysicalOptimizerRule + Send + Sync>,
        > = vec![Arc::new(StreamingPhysicalValidator::new(mode))];
        rules.extend(default_state.physical_optimizers().iter().cloned());

        let state = SessionStateBuilder::new()
            .with_config(config)
            .with_default_features()
            .with_physical_optimizer_rules(rules)
            .build();
        SessionContext::new_with_state(state)
    };

    register_streaming_functions(&ctx);
    ctx
}

/// Window-time, JSON, complex-type, lambda, and `proctime()` UDFs —
/// every streaming UDF except `watermark()`. Pulled out of the public
/// `register_streaming_functions*` entry points so they share a single
/// list and stay in sync.
fn register_non_watermark_udfs(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::new_from_impl(TumbleWindowStart::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(TumbleWindowEnd::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(HopWindowStart::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(HopWindowEnd::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(SessionWindowStart::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(CumulateWindowStart::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(CumulateWindowEnd::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(ProcTimeUdf::new()));
    for marker in ai_function_markers() {
        ctx.register_udf(marker);
    }
    register_json_functions(ctx);
    register_json_extensions(ctx);
    register_complex_type_functions(ctx);
    register_lambda_functions(ctx);
}

/// Registers `LaminarDB` streaming UDFs with a session context. The
/// `watermark()` UDF is registered in unset mode (always returns NULL);
/// use [`register_streaming_functions_with_watermark`] to provide a
/// live watermark source from Ring 0.
pub fn register_streaming_functions(ctx: &SessionContext) {
    register_non_watermark_udfs(ctx);
    ctx.register_udf(ScalarUDF::new_from_impl(WatermarkUdf::unset()));
}

/// Registers streaming UDFs with a live watermark source — same as
/// [`register_streaming_functions`] but `watermark()` reads
/// `watermark_ms` (in milliseconds since epoch; values < 0 mean "no
/// watermark", returning NULL).
pub fn register_streaming_functions_with_watermark(
    ctx: &SessionContext,
    watermark_ms: Arc<AtomicI64>,
) {
    register_non_watermark_udfs(ctx);
    ctx.register_udf(ScalarUDF::new_from_impl(WatermarkUdf::new(watermark_ms)));
}

/// Registers all PostgreSQL-compatible JSON UDFs and UDAFs
/// with the given `SessionContext`.
pub fn register_json_functions(ctx: &SessionContext) {
    // Extraction operators
    ctx.register_udf(ScalarUDF::new_from_impl(JsonbGet::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(JsonbGetIdx::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(JsonbGetText::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(JsonbGetTextIdx::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(JsonbGetPath::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(JsonbGetPathText::new()));

    // Existence operators
    ctx.register_udf(ScalarUDF::new_from_impl(JsonbExists::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(JsonbExistsAny::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(JsonbExistsAll::new()));

    // Containment operators
    ctx.register_udf(ScalarUDF::new_from_impl(JsonbContains::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(JsonbContainedBy::new()));

    // Interrogation / construction
    ctx.register_udf(ScalarUDF::new_from_impl(JsonTypeof::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(JsonBuildObject::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(JsonBuildArray::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(ToJsonb::new()));

    // Aggregates
    ctx.register_udaf(datafusion_expr::AggregateUDF::new_from_impl(JsonAgg::new()));
    ctx.register_udaf(datafusion_expr::AggregateUDF::new_from_impl(
        JsonObjectAgg::new(),
    ));

    // Format bridge functions
    ctx.register_udf(ScalarUDF::new_from_impl(ParseEpochUdf::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(ParseTimestampUdf::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(ToJsonUdf::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(FromJsonUdf::new()));

    // JSON path query functions (scalar)
    ctx.register_udf(ScalarUDF::new_from_impl(JsonbPathExistsUdf::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(JsonbPathMatchUdf::new()));

    // JSON table-valued functions
    register_json_table_functions(ctx);
}

#[cfg(test)]
mod tests;
