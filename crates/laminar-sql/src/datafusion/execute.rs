//! Streaming SQL execution via DataFusion.

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
        return Err(Error::ParseError(
            crate::parser::ParseError::StreamingError("Empty SQL statement".to_string()),
        ));
    }

    // Process the first statement
    let statement = &statements[0];
    let plan = planner.plan(statement)?;

    match plan {
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
        StreamingPlan::RegisterSource(_)
        | StreamingPlan::RegisterSink(_)
        | StreamingPlan::RegisterLookupTable(_)
        | StreamingPlan::DropLookupTable { .. } => Ok(StreamingSqlResult::Ddl(DdlResult { plan })),
    }
}

#[cfg(test)]
mod tests;
