//! End-to-end streaming SQL execution (F005B)
//!
//! Provides [`execute_streaming_sql`] for parsing, planning, and executing
//! streaming SQL statements through the `DataFusion` engine.
//!
//! # Architecture
//!
//! ```text
//! SQL string
//!     │
//!     ▼
//! parse_streaming_sql()  →  StreamingStatement
//!     │
//!     ▼
//! StreamingPlanner::plan()  →  StreamingPlan
//!     │
//!     ├─ DDL (CREATE SOURCE/SINK)  →  DdlResult
//!     │
//!     ├─ Query (windows/joins)     →  LogicalPlan → DataFrame → stream
//!     │                                + QueryPlan metadata
//!     │
//!     └─ Standard SQL              →  DataFusion ctx.sql() → stream
//! ```

use datafusion::execution::SendableRecordBatchStream;
use datafusion::prelude::SessionContext;

use crate::parser::parse_streaming_sql;
use crate::planner::{QueryPlan, StreamingPlan, StreamingPlanner};
use crate::Error;

/// Result of executing a streaming SQL statement.
#[derive(Debug)]
pub enum StreamingSqlResult {
    /// DDL statement result (CREATE SOURCE, CREATE SINK)
    Ddl(DdlResult),
    /// Query execution result with optional streaming metadata
    Query(QueryResult),
}

/// Result of a DDL statement execution.
#[derive(Debug)]
pub struct DdlResult {
    /// The streaming plan describing what was created or registered
    pub plan: StreamingPlan,
}

/// Result of a query execution.
///
/// Contains both the `DataFusion` record batch stream and optional
/// streaming metadata (window config, join config, emit clause) from
/// the `QueryPlan`. Ring 0 operators use the `query_plan` to configure
/// windowing and join behavior.
pub struct QueryResult {
    /// Record batch stream from `DataFusion` execution
    pub stream: SendableRecordBatchStream,
    /// Streaming query metadata (window config, join config, etc.)
    ///
    /// `None` for standard SQL pass-through queries.
    /// `Some` for queries with streaming features (windows, joins).
    pub query_plan: Option<QueryPlan>,
}

impl std::fmt::Debug for QueryResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryResult")
            .field("query_plan", &self.query_plan)
            .field("stream", &"<SendableRecordBatchStream>")
            .finish()
    }
}

/// Executes a streaming SQL statement end-to-end.
///
/// This function performs the full pipeline:
/// 1. Parse SQL with streaming extensions (CREATE SOURCE/SINK, windows, etc.)
/// 2. Plan via [`StreamingPlanner`]
/// 3. For DDL: return the streaming plan as [`DdlResult`]
/// 4. For queries with streaming features: create `LogicalPlan` via
///    `DataFusion`, execute, and return stream + [`QueryPlan`] metadata
/// 5. For standard SQL: pass through to `DataFusion` directly
///
/// # Arguments
///
/// * `sql` - The SQL statement to execute
/// * `ctx` - `DataFusion` session context (should have streaming functions registered)
/// * `planner` - Streaming planner with registered sources/sinks
///
/// # Errors
///
/// Returns [`Error`] if parsing, planning, or execution fails.
pub async fn execute_streaming_sql(
    sql: &str,
    ctx: &SessionContext,
    planner: &mut StreamingPlanner,
) -> std::result::Result<StreamingSqlResult, Error> {
    let statements = parse_streaming_sql(sql)?;

    if statements.is_empty() {
        return Err(Error::ParseError(crate::parser::ParseError::StreamingError(
            "Empty SQL statement".to_string(),
        )));
    }

    // Process the first statement
    let statement = &statements[0];
    let plan = planner.plan(statement)?;

    match plan {
        StreamingPlan::RegisterSource(_) | StreamingPlan::RegisterSink(_) => {
            Ok(StreamingSqlResult::Ddl(DdlResult { plan }))
        }
        StreamingPlan::Query(query_plan) => {
            let logical_plan = planner.to_logical_plan(&query_plan, ctx).await?;
            let df = ctx.execute_logical_plan(logical_plan).await?;
            let stream = df.execute_stream().await?;

            Ok(StreamingSqlResult::Query(QueryResult {
                stream,
                query_plan: Some(query_plan),
            }))
        }
        StreamingPlan::Standard(stmt) => {
            let sql_str = stmt.to_string();
            let df = ctx.sql(&sql_str).await?;
            let stream = df.execute_stream().await?;

            Ok(StreamingSqlResult::Query(QueryResult {
                stream,
                query_plan: None,
            }))
        }
        StreamingPlan::DagExplain(_) => Ok(StreamingSqlResult::Ddl(DdlResult { plan })),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datafusion::create_streaming_context;

    #[tokio::test]
    async fn test_execute_ddl_source() {
        let ctx = create_streaming_context();
        crate::datafusion::register_streaming_functions(&ctx);
        let mut planner = StreamingPlanner::new();

        let result = execute_streaming_sql(
            "CREATE SOURCE events (id INT, name VARCHAR)",
            &ctx,
            &mut planner,
        )
        .await
        .unwrap();

        assert!(matches!(result, StreamingSqlResult::Ddl(_)));
    }

    #[tokio::test]
    async fn test_execute_ddl_sink() {
        let ctx = create_streaming_context();
        crate::datafusion::register_streaming_functions(&ctx);
        let mut planner = StreamingPlanner::new();

        // Register source first
        execute_streaming_sql(
            "CREATE SOURCE events (id INT, name VARCHAR)",
            &ctx,
            &mut planner,
        )
        .await
        .unwrap();

        let result =
            execute_streaming_sql("CREATE SINK output FROM events", &ctx, &mut planner)
                .await
                .unwrap();

        assert!(matches!(result, StreamingSqlResult::Ddl(_)));
    }

    #[tokio::test]
    async fn test_execute_empty_sql_error() {
        let ctx = create_streaming_context();
        let mut planner = StreamingPlanner::new();

        let result = execute_streaming_sql("", &ctx, &mut planner).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_execute_standard_passthrough() {
        use futures::StreamExt;

        let ctx = create_streaming_context();
        crate::datafusion::register_streaming_functions(&ctx);
        let mut planner = StreamingPlanner::new();

        // Simple SELECT 1 goes through DataFusion directly
        let result = execute_streaming_sql("SELECT 1 as value", &ctx, &mut planner)
            .await
            .unwrap();

        match result {
            StreamingSqlResult::Query(qr) => {
                assert!(qr.query_plan.is_none());
                let mut stream = qr.stream;
                let batch = stream.next().await.unwrap().unwrap();
                assert_eq!(batch.num_rows(), 1);
            }
            _ => panic!("Expected Query result"),
        }
    }

    #[tokio::test]
    async fn test_execute_standard_query_with_table() {
        use arrow_array::{Int64Array, RecordBatch, StringArray};
        use arrow_schema::{DataType, Field, Schema};
        use futures::StreamExt;
        use std::sync::Arc;

        let ctx = create_streaming_context();
        crate::datafusion::register_streaming_functions(&ctx);

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let source =
            Arc::new(crate::datafusion::ChannelStreamSource::new(Arc::clone(&schema)));
        let sender = source.take_sender().expect("sender available");
        let provider = crate::datafusion::StreamingTableProvider::new("users", source);
        ctx.register_table("users", Arc::new(provider)).unwrap();

        // Send data and close channel
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["alice", "bob"])),
            ],
        )
        .unwrap();
        sender.send(batch).await.unwrap();
        drop(sender);

        let mut planner = StreamingPlanner::new();
        let result =
            execute_streaming_sql("SELECT id, name FROM users", &ctx, &mut planner)
                .await
                .unwrap();

        match result {
            StreamingSqlResult::Query(qr) => {
                assert!(qr.query_plan.is_none()); // Standard query
                let mut stream = qr.stream;
                let mut total = 0;
                while let Some(batch) = stream.next().await {
                    total += batch.unwrap().num_rows();
                }
                assert_eq!(total, 2);
            }
            _ => panic!("Expected Query result"),
        }
    }
}
