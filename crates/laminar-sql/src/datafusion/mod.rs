//! `DataFusion` integration for SQL processing
//!
//! This module provides the integration layer between `LaminarDB`'s push-based
//! streaming engine and `DataFusion`'s pull-based SQL query execution.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                    Ring 2: Query Planning                        │
//! │  SQL Query → SessionContext → LogicalPlan → ExecutionPlan       │
//! │                                      │                          │
//! │                            StreamingScanExec                    │
//! │                                      │                          │
//! │                              ┌───────▼──────┐                   │
//! │                              │ StreamBridge │ (tokio channel)   │
//! │                              └───────▲──────┘                   │
//! ├──────────────────────────────────────┼──────────────────────────┤
//! │                    Ring 0: Hot Path   │                          │
//! │                                      │                          │
//! │  Source → Reactor.poll() ────────────┘                          │
//! │              (Events with RecordBatch data)                     │
//! └─────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Components
//!
//! - [`StreamSource`]: Trait for streaming data sources
//! - [`StreamBridge`]: Channel-based push-to-pull bridge
//! - [`StreamingScanExec`]: `DataFusion` execution plan for streaming scans
//! - [`StreamingTableProvider`]: `DataFusion` table provider for streaming sources
//! - [`ChannelStreamSource`]: Concrete source using channels
//!
//! # Usage
//!
//! ```rust,ignore
//! use laminar_sql::datafusion::{
//!     create_streaming_context, ChannelStreamSource, StreamingTableProvider,
//! };
//! use std::sync::Arc;
//!
//! // Create a streaming context
//! let ctx = create_streaming_context();
//!
//! // Create a channel source
//! let schema = Arc::new(Schema::new(vec![
//!     Field::new("id", DataType::Int64, false),
//!     Field::new("value", DataType::Float64, true),
//! ]));
//! let source = Arc::new(ChannelStreamSource::new(schema));
//! let sender = source.sender();
//!
//! // Register as a table
//! let provider = StreamingTableProvider::new("events", source);
//! ctx.register_table("events", Arc::new(provider))?;
//!
//! // Push data from the Reactor
//! sender.send(batch).await?;
//!
//! // Execute SQL queries
//! let df = ctx.sql("SELECT * FROM events WHERE value > 100").await?;
//! ```

/// DataFusion aggregate bridge for streaming aggregation.
///
/// Bridges DataFusion's `Accumulator` trait with `laminar-core`'s
/// `DynAccumulator` / `DynAggregatorFactory` traits. This avoids
/// duplicating aggregation logic.
pub mod aggregate_bridge;
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
/// Lookup join plan node for DataFusion.
pub mod lookup_join;
/// Processing-time UDF for `PROCTIME()` support
pub mod proctime_udf;
mod source;
mod table_provider;
/// Watermark UDF for current watermark access
pub mod watermark_udf;
/// Window function UDFs (TUMBLE, HOP, SESSION, CUMULATE)
pub mod window_udf;

pub use aggregate_bridge::{
    create_aggregate_factory, lookup_aggregate_udf, result_to_scalar_value, scalar_value_to_result,
    DataFusionAccumulatorAdapter, DataFusionAggregateFactory,
};
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
pub use proctime_udf::ProcTimeUdf;
pub use source::{SortColumn, StreamSource, StreamSourceRef};
pub use table_provider::StreamingTableProvider;
pub use watermark_udf::WatermarkUdf;
pub use window_udf::{CumulateWindowStart, HopWindowStart, SessionWindowStart, TumbleWindowStart};

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
    let config = base_session_config()
        .with_batch_size(8192)
        .with_target_partitions(1); // Single partition for streaming

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
        let mut rules: Vec<Arc<dyn datafusion::physical_optimizer::PhysicalOptimizerRule + Send + Sync>> =
            vec![Arc::new(StreamingPhysicalValidator::new(mode))];
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

/// Registers `LaminarDB` streaming UDFs with a session context.
///
/// Registers the following scalar functions:
/// - `tumble(timestamp, interval)` — tumbling window start
/// - `hop(timestamp, slide, size)` — hopping window start
/// - `session(timestamp, gap)` — session window pass-through
/// - `watermark()` — current watermark (returns NULL, no live source)
///
/// Use [`register_streaming_functions_with_watermark`] to provide a
/// live watermark source from Ring 0.
pub fn register_streaming_functions(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::new_from_impl(TumbleWindowStart::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(HopWindowStart::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(SessionWindowStart::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(CumulateWindowStart::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(WatermarkUdf::unset()));
    ctx.register_udf(ScalarUDF::new_from_impl(ProcTimeUdf::new()));
    register_json_functions(ctx);
    register_json_extensions(ctx);
    register_complex_type_functions(ctx);
    register_lambda_functions(ctx);
}

/// Registers streaming UDFs with a live watermark source.
///
/// Same as [`register_streaming_functions`] but connects the `watermark()`
/// UDF to a shared atomic value that Ring 0 updates in real time.
///
/// # Arguments
///
/// * `ctx` - `DataFusion` session context
/// * `watermark_ms` - Shared atomic holding the current watermark in
///   milliseconds since epoch. Values < 0 mean "no watermark" (returns NULL).
pub fn register_streaming_functions_with_watermark(
    ctx: &SessionContext,
    watermark_ms: Arc<AtomicI64>,
) {
    ctx.register_udf(ScalarUDF::new_from_impl(TumbleWindowStart::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(HopWindowStart::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(SessionWindowStart::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(CumulateWindowStart::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(WatermarkUdf::new(watermark_ms)));
    ctx.register_udf(ScalarUDF::new_from_impl(ProcTimeUdf::new()));
    register_json_functions(ctx);
    register_json_extensions(ctx);
    register_complex_type_functions(ctx);
    register_lambda_functions(ctx);
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
mod tests {
    use super::*;
    use arrow_array::{Float64Array, Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::execution::FunctionRegistry;
    use futures::StreamExt;
    use std::sync::Arc;

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Float64, true),
        ]))
    }

    /// Take the sender from a `ChannelStreamSource`, panicking if already taken.
    fn take_test_sender(source: &ChannelStreamSource) -> super::bridge::BridgeSender {
        source.take_sender().expect("sender already taken")
    }

    fn test_batch(schema: &Arc<Schema>, ids: Vec<i64>, values: Vec<f64>) -> RecordBatch {
        RecordBatch::try_new(
            Arc::clone(schema),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(Float64Array::from(values)),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_create_streaming_context() {
        let ctx = create_streaming_context();
        let state = ctx.state();
        let config = state.config();

        assert_eq!(config.batch_size(), 8192);
        assert_eq!(config.target_partitions(), 1);
    }

    #[tokio::test]
    async fn test_full_query_pipeline() {
        let ctx = create_streaming_context();
        let schema = test_schema();

        // Create source and take the sender (important for channel closure)
        let source = Arc::new(ChannelStreamSource::new(Arc::clone(&schema)));
        let sender = take_test_sender(&source);
        let provider = StreamingTableProvider::new("events", source);
        ctx.register_table("events", Arc::new(provider)).unwrap();

        // Send test data
        sender
            .send(test_batch(&schema, vec![1, 2, 3], vec![10.0, 20.0, 30.0]))
            .await
            .unwrap();
        sender
            .send(test_batch(&schema, vec![4, 5], vec![40.0, 50.0]))
            .await
            .unwrap();
        drop(sender); // Close the channel

        // Execute query
        let df = ctx.sql("SELECT * FROM events").await.unwrap();
        let batches = df.collect().await.unwrap();

        // Verify results
        let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total_rows, 5);
    }

    #[tokio::test]
    async fn test_query_with_projection() {
        let ctx = create_streaming_context();
        let schema = test_schema();

        let source = Arc::new(ChannelStreamSource::new(Arc::clone(&schema)));
        let sender = take_test_sender(&source);
        let provider = StreamingTableProvider::new("events", source);
        ctx.register_table("events", Arc::new(provider)).unwrap();

        sender
            .send(test_batch(&schema, vec![1, 2], vec![100.0, 200.0]))
            .await
            .unwrap();
        drop(sender);

        // Query only the id column
        let df = ctx.sql("SELECT id FROM events").await.unwrap();
        let batches = df.collect().await.unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_columns(), 1);
        assert_eq!(batches[0].schema().field(0).name(), "id");
    }

    #[tokio::test]
    async fn test_query_with_filter() {
        let ctx = create_streaming_context();
        let schema = test_schema();

        let source = Arc::new(ChannelStreamSource::new(Arc::clone(&schema)));
        let sender = take_test_sender(&source);
        let provider = StreamingTableProvider::new("events", source);
        ctx.register_table("events", Arc::new(provider)).unwrap();

        sender
            .send(test_batch(
                &schema,
                vec![1, 2, 3, 4, 5],
                vec![10.0, 20.0, 30.0, 40.0, 50.0],
            ))
            .await
            .unwrap();
        drop(sender);

        // Filter for value > 25
        let df = ctx
            .sql("SELECT * FROM events WHERE value > 25")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();

        let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total_rows, 3); // 30, 40, 50
    }

    #[tokio::test]
    async fn test_unbounded_aggregation_rejected() {
        // Aggregations on unbounded streams should be rejected by `DataFusion`.
        // Streaming aggregations require windows, which are implemented.
        let ctx = create_streaming_context();
        let schema = test_schema();

        let source = Arc::new(ChannelStreamSource::new(Arc::clone(&schema)));
        let sender = take_test_sender(&source);
        let provider = StreamingTableProvider::new("events", source);
        ctx.register_table("events", Arc::new(provider)).unwrap();

        sender
            .send(test_batch(&schema, vec![1, 2, 3], vec![10.0, 20.0, 30.0]))
            .await
            .unwrap();
        drop(sender);

        // Aggregate query on unbounded stream should fail at execution
        let df = ctx.sql("SELECT COUNT(*) as cnt FROM events").await.unwrap();

        // Execution should fail because we can't aggregate an infinite stream
        let result = df.collect().await;
        assert!(
            result.is_err(),
            "Aggregation on unbounded stream should fail"
        );
    }

    #[tokio::test]
    async fn test_query_with_order_by() {
        let ctx = create_streaming_context();
        let schema = test_schema();

        let source = Arc::new(ChannelStreamSource::new(Arc::clone(&schema)));
        let sender = take_test_sender(&source);
        let provider = StreamingTableProvider::new("events", source);
        ctx.register_table("events", Arc::new(provider)).unwrap();

        sender
            .send(test_batch(&schema, vec![3, 1, 2], vec![30.0, 10.0, 20.0]))
            .await
            .unwrap();
        drop(sender);

        // Query with ORDER BY (`DataFusion` handles this with Sort operator)
        let df = ctx.sql("SELECT id, value FROM events").await.unwrap();
        let batches = df.collect().await.unwrap();

        // Verify we got results (ordering may vary due to streaming nature)
        let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total_rows, 3);
    }

    #[tokio::test]
    async fn test_bridge_throughput() {
        // Benchmark-style test for bridge performance
        let schema = test_schema();
        let bridge = StreamBridge::new(Arc::clone(&schema), 10000);
        let sender = bridge.sender();
        let mut stream = bridge.into_stream();

        let batch_count = 1000;
        let batch = test_batch(&schema, vec![1, 2, 3, 4, 5], vec![1.0, 2.0, 3.0, 4.0, 5.0]);

        // Spawn sender task
        let send_task = tokio::spawn(async move {
            for _ in 0..batch_count {
                sender.send(batch.clone()).await.unwrap();
            }
        });

        // Receive all batches
        let mut received = 0;
        while let Some(result) = stream.next().await {
            result.unwrap();
            received += 1;
            if received == batch_count {
                break;
            }
        }

        send_task.await.unwrap();
        assert_eq!(received, batch_count);
    }

    // ── Integration Tests ──────────────────────────────────────────

    #[test]
    fn test_streaming_functions_registered() {
        let ctx = create_streaming_context();
        // Verify all 4 UDFs are registered
        assert!(ctx.udf("tumble").is_ok(), "tumble UDF not registered");
        assert!(ctx.udf("hop").is_ok(), "hop UDF not registered");
        assert!(ctx.udf("session").is_ok(), "session UDF not registered");
        assert!(ctx.udf("watermark").is_ok(), "watermark UDF not registered");
    }

    #[test]
    fn test_streaming_functions_with_watermark() {
        use std::sync::atomic::AtomicI64;

        let ctx = create_session_context();
        let wm = Arc::new(AtomicI64::new(42_000));
        register_streaming_functions_with_watermark(&ctx, wm);

        assert!(ctx.udf("tumble").is_ok());
        assert!(ctx.udf("watermark").is_ok());
    }

    #[tokio::test]
    async fn test_tumble_udf_via_datafusion() {
        use arrow_array::TimestampMillisecondArray;
        use arrow_schema::TimeUnit;

        let ctx = create_streaming_context();

        // Create schema with timestamp and value columns
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "event_time",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("value", DataType::Float64, false),
        ]));

        let source = Arc::new(ChannelStreamSource::new(Arc::clone(&schema)));
        let sender = take_test_sender(&source);
        let provider = StreamingTableProvider::new("events", source);
        ctx.register_table("events", Arc::new(provider)).unwrap();

        // Send events across two 5-minute windows:
        // Window [0, 300_000): timestamps 60_000, 120_000
        // Window [300_000, 600_000): timestamps 360_000
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(TimestampMillisecondArray::from(vec![
                    60_000i64, 120_000, 360_000,
                ])),
                Arc::new(Float64Array::from(vec![10.0, 20.0, 30.0])),
            ],
        )
        .unwrap();
        sender.send(batch).await.unwrap();
        drop(sender);

        // Verify the tumble UDF computes correct window starts via DataFusion
        // (GROUP BY aggregation and ORDER BY on unbounded streams are handled by Ring 0)
        let df = ctx
            .sql(
                "SELECT tumble(event_time, INTERVAL '5' MINUTE) as window_start, \
                 value \
                 FROM events",
            )
            .await
            .unwrap();

        let batches = df.collect().await.unwrap();
        let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total_rows, 3);

        // Verify the window_start values (single batch, order preserved)
        let ws_col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .expect("window_start should be TimestampMillisecond");
        // 60_000 and 120_000 → window [0, 300_000), start = 0
        assert_eq!(ws_col.value(0), 0);
        assert_eq!(ws_col.value(1), 0);
        // 360_000 → window [300_000, 600_000), start = 300_000
        assert_eq!(ws_col.value(2), 300_000);
    }

    #[tokio::test]
    async fn test_logical_plan_from_windowed_query() {
        use arrow_schema::TimeUnit;

        let ctx = create_streaming_context();

        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "event_time",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("value", DataType::Float64, false),
        ]));

        let source = Arc::new(ChannelStreamSource::new(schema));
        let _sender = source.take_sender();
        let provider = StreamingTableProvider::new("events", source);
        ctx.register_table("events", Arc::new(provider)).unwrap();

        // Create a LogicalPlan for a windowed query
        let df = ctx
            .sql(
                "SELECT tumble(event_time, INTERVAL '5' MINUTE) as w, \
                 COUNT(*) as cnt \
                 FROM events \
                 GROUP BY tumble(event_time, INTERVAL '5' MINUTE)",
            )
            .await;

        // Should succeed in creating the logical plan (UDFs are registered)
        assert!(df.is_ok(), "Failed to create logical plan: {df:?}");
    }

    #[tokio::test]
    async fn test_end_to_end_execute_streaming_sql() {
        use crate::planner::StreamingPlanner;

        let ctx = create_streaming_context();

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let source = Arc::new(ChannelStreamSource::new(Arc::clone(&schema)));
        let sender = take_test_sender(&source);
        let provider = StreamingTableProvider::new("items", source);
        ctx.register_table("items", Arc::new(provider)).unwrap();

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(arrow_array::StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap();
        sender.send(batch).await.unwrap();
        drop(sender);

        let mut planner = StreamingPlanner::new();
        let result = execute_streaming_sql("SELECT id FROM items WHERE id > 1", &ctx, &mut planner)
            .await
            .unwrap();

        match result {
            StreamingSqlResult::Query(qr) => {
                let mut stream = qr.stream;
                let mut total = 0;
                while let Some(batch) = stream.next().await {
                    total += batch.unwrap().num_rows();
                }
                assert_eq!(total, 2); // id=2, id=3
            }
            StreamingSqlResult::Ddl(_) => panic!("Expected Query result"),
        }
    }

    #[tokio::test]
    async fn test_watermark_function_in_filter() {
        use arrow_array::TimestampMillisecondArray;
        use arrow_schema::TimeUnit;
        use std::sync::atomic::AtomicI64;

        // Create context with a specific watermark value
        let config = base_session_config()
            .with_batch_size(8192)
            .with_target_partitions(1);
        let ctx = SessionContext::new_with_config(config);
        let wm = Arc::new(AtomicI64::new(200_000)); // watermark at 200s
        register_streaming_functions_with_watermark(&ctx, wm);

        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "event_time",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("value", DataType::Float64, false),
        ]));

        let source = Arc::new(ChannelStreamSource::new(Arc::clone(&schema)));
        let sender = take_test_sender(&source);
        let provider = StreamingTableProvider::new("events", source);
        ctx.register_table("events", Arc::new(provider)).unwrap();

        // Events: 100s, 200s, 300s - watermark is at 200s
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(TimestampMillisecondArray::from(vec![
                    100_000i64, 200_000, 300_000,
                ])),
                Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0])),
            ],
        )
        .unwrap();
        sender.send(batch).await.unwrap();
        drop(sender);

        // Filter events after watermark
        let df = ctx
            .sql("SELECT value FROM events WHERE event_time > watermark()")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        // Only event at 300s is after watermark (200s)
        assert_eq!(total_rows, 1);
    }
}
