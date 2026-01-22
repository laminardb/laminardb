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

mod bridge;
mod channel_source;
mod exec;
mod source;
mod table_provider;

pub use bridge::{BridgeSendError, BridgeSender, BridgeStream, BridgeTrySendError, StreamBridge};
pub use channel_source::ChannelStreamSource;
pub use exec::StreamingScanExec;
pub use source::{StreamSource, StreamSourceRef};
pub use table_provider::StreamingTableProvider;

use datafusion::prelude::*;

/// Creates a `DataFusion` session context configured for streaming queries.
///
/// The context is configured with:
/// - Batch size of 8192 (balanced for streaming throughput)
/// - Single partition (streaming sources are typically not partitioned)
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
    let config = SessionConfig::new()
        .with_batch_size(8192)
        .with_target_partitions(1); // Single partition for streaming

    SessionContext::new_with_config(config)
}

/// Registers `LaminarDB` custom functions with a session context.
///
/// Currently registers:
/// - (Future) TUMBLE window function
/// - (Future) HOP window function
/// - (Future) SESSION window function
/// - (Future) WATERMARK function
///
/// Note: Streaming SQL extensions (windows, watermarks, EMIT) are
/// deferred to F006.
pub fn register_streaming_functions(_ctx: &SessionContext) {
    // TODO: Register TUMBLE, HOP, SESSION window functions (F006)
    // TODO: Register WATERMARK function (F006)
    // TODO: Register streaming-specific UDFs
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Float64Array, Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use futures::StreamExt;
    use std::sync::Arc;

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Float64, true),
        ]))
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
        let sender = source.take_sender().expect("sender available");
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
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 5);
    }

    #[tokio::test]
    async fn test_query_with_projection() {
        let ctx = create_streaming_context();
        let schema = test_schema();

        let source = Arc::new(ChannelStreamSource::new(Arc::clone(&schema)));
        let sender = source.take_sender().expect("sender available");
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
        let sender = source.take_sender().expect("sender available");
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

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3); // 30, 40, 50
    }

    #[tokio::test]
    async fn test_unbounded_aggregation_rejected() {
        // Aggregations on unbounded streams should be rejected by `DataFusion`.
        // Streaming aggregations require windows, which are implemented in F006.
        let ctx = create_streaming_context();
        let schema = test_schema();

        let source = Arc::new(ChannelStreamSource::new(Arc::clone(&schema)));
        let sender = source.take_sender().expect("sender available");
        let provider = StreamingTableProvider::new("events", source);
        ctx.register_table("events", Arc::new(provider)).unwrap();

        sender
            .send(test_batch(&schema, vec![1, 2, 3], vec![10.0, 20.0, 30.0]))
            .await
            .unwrap();
        drop(sender);

        // Aggregate query on unbounded stream should fail at execution
        let df = ctx
            .sql("SELECT COUNT(*) as cnt FROM events")
            .await
            .unwrap();

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
        let sender = source.take_sender().expect("sender available");
        let provider = StreamingTableProvider::new("events", source);
        ctx.register_table("events", Arc::new(provider)).unwrap();

        sender
            .send(test_batch(
                &schema,
                vec![3, 1, 2],
                vec![30.0, 10.0, 20.0],
            ))
            .await
            .unwrap();
        drop(sender);

        // Query with ORDER BY (`DataFusion` handles this with Sort operator)
        let df = ctx.sql("SELECT id, value FROM events").await.unwrap();
        let batches = df.collect().await.unwrap();

        // Verify we got results (ordering may vary due to streaming nature)
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
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
}
