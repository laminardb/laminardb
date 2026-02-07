//! `DataFusion` micro-batch stream executor.
//!
//! Executes registered streaming queries against source data using `DataFusion`'s
//! SQL engine. Each processing cycle:
//!
//! 1. Source batches are registered as temporary `MemTable` tables
//! 2. Each stream query is executed via `ctx.sql()`
//! 3. Results are collected as `RecordBatch` vectors
//! 4. Temporary tables are cleared for the next cycle

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::RecordBatch;
use datafusion::prelude::SessionContext;

use crate::error::DbError;

/// A registered stream query for execution.
#[derive(Debug, Clone)]
pub(crate) struct StreamQuery {
    /// Stream name.
    pub name: String,
    /// SQL query text.
    pub sql: String,
}

/// DataFusion-based micro-batch stream executor.
///
/// Holds a `SessionContext` and registered stream queries. Each execution
/// cycle registers source data as `MemTable`, runs queries, and returns
/// named results.
pub(crate) struct StreamExecutor {
    ctx: SessionContext,
    queries: Vec<StreamQuery>,
    /// Tracks which temporary source tables are registered (for cleanup).
    registered_sources: Vec<String>,
}

impl StreamExecutor {
    /// Create a new executor with the given `SessionContext`.
    pub fn new(ctx: SessionContext) -> Self {
        Self {
            ctx,
            queries: Vec::new(),
            registered_sources: Vec::new(),
        }
    }

    /// Register a stream query for execution.
    pub fn add_query(&mut self, name: String, sql: String) {
        self.queries.push(StreamQuery { name, sql });
    }

    /// Register a static reference table (e.g., from `CREATE TABLE`).
    ///
    /// Unlike source tables, these persist across cycles.
    #[allow(dead_code)] // Public API for Phase 3 CREATE TABLE support
    pub fn register_table(&self, name: &str, batch: RecordBatch) -> Result<(), DbError> {
        let schema = batch.schema();
        let mem_table = datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]])
            .map_err(|e| DbError::Pipeline(format!("Failed to create table '{name}': {e}")))?;

        self.ctx
            .register_table(name, Arc::new(mem_table))
            .map_err(|e| DbError::Pipeline(format!("Failed to register table '{name}': {e}")))?;
        Ok(())
    }

    /// Execute one processing cycle.
    ///
    /// Registers `source_batches` as temporary tables, runs all stream queries,
    /// and returns a map from stream name to result batches.
    pub async fn execute_cycle(
        &mut self,
        source_batches: &HashMap<String, Vec<RecordBatch>>,
    ) -> Result<HashMap<String, Vec<RecordBatch>>, DbError> {
        // 1. Register source data as temporary MemTables
        self.register_source_tables(source_batches)?;

        // 2. Execute each stream query (in dependency order).
        //    After each query, register its results as a temp table so
        //    downstream queries can reference it (cascading MVs, issue #35).
        let mut results = HashMap::new();
        for query in &self.queries {
            match self.ctx.sql(&query.sql).await {
                Ok(df) => match df.collect().await {
                    Ok(batches) => {
                        if !batches.is_empty() {
                            // Register intermediate results so downstream queries
                            // can reference this stream's output within the same cycle.
                            let schema = batches[0].schema();
                            if let Ok(mem_table) =
                                datafusion::datasource::MemTable::try_new(
                                    schema,
                                    vec![batches.clone()],
                                )
                            {
                                let _ = self.ctx.deregister_table(&query.name);
                                if self
                                    .ctx
                                    .register_table(&query.name, Arc::new(mem_table))
                                    .is_ok()
                                {
                                    self.registered_sources.push(query.name.clone());
                                }
                            }

                            results.insert(query.name.clone(), batches);
                        }
                    }
                    Err(e) => {
                        tracing::warn!(
                            stream = %query.name,
                            error = %e,
                            "Stream query execution failed"
                        );
                    }
                },
                Err(e) => {
                    tracing::warn!(
                        stream = %query.name,
                        error = %e,
                        "Stream query planning failed"
                    );
                }
            }
        }

        // 3. Cleanup temporary source tables
        self.cleanup_source_tables();

        Ok(results)
    }

    /// Register source batches as temporary `MemTable` providers.
    fn register_source_tables(
        &mut self,
        source_batches: &HashMap<String, Vec<RecordBatch>>,
    ) -> Result<(), DbError> {
        for (name, batches) in source_batches {
            if batches.is_empty() {
                continue;
            }

            let schema = batches[0].schema();
            let mem_table =
                datafusion::datasource::MemTable::try_new(schema, vec![batches.clone()]).map_err(
                    |e| DbError::Pipeline(format!("Failed to create temp table '{name}': {e}")),
                )?;

            // Deregister first if it exists (from previous cycle)
            let _ = self.ctx.deregister_table(name);

            self.ctx
                .register_table(name, Arc::new(mem_table))
                .map_err(|e| {
                    DbError::Pipeline(format!("Failed to register temp table '{name}': {e}"))
                })?;

            self.registered_sources.push(name.clone());
        }
        Ok(())
    }

    /// Remove temporary source tables from the context.
    fn cleanup_source_tables(&mut self) {
        for name in self.registered_sources.drain(..) {
            let _ = self.ctx.deregister_table(&name);
        }
    }

    /// Get the number of registered queries.
    #[allow(dead_code)] // Public API for admin/observability queries
    pub fn query_count(&self) -> usize {
        self.queries.len()
    }
}

#[cfg(test)]
#[allow(clippy::redundant_closure_for_method_calls)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use laminar_sql::register_streaming_functions;

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]))
    }

    fn test_batch() -> RecordBatch {
        let schema = test_schema();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
                Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0])),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_executor_basic_query() {
        let ctx = SessionContext::new();
        register_streaming_functions(&ctx);
        let mut executor = StreamExecutor::new(ctx);

        executor.add_query(
            "test_stream".to_string(),
            "SELECT name, SUM(value) as total FROM events GROUP BY name".to_string(),
        );

        let mut source_batches = HashMap::new();
        source_batches.insert("events".to_string(), vec![test_batch()]);

        let results = executor.execute_cycle(&source_batches).await.unwrap();
        assert!(results.contains_key("test_stream"));
        let batches = &results["test_stream"];
        assert!(!batches.is_empty());
        // Each name should have one row
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);
    }

    #[tokio::test]
    async fn test_executor_empty_source() {
        let ctx = SessionContext::new();
        register_streaming_functions(&ctx);
        let mut executor = StreamExecutor::new(ctx);

        executor.add_query(
            "test_stream".to_string(),
            "SELECT * FROM events".to_string(),
        );

        let source_batches = HashMap::new();
        let results = executor.execute_cycle(&source_batches).await.unwrap();
        // Query should fail gracefully (table not registered)
        assert!(!results.contains_key("test_stream"));
    }

    #[tokio::test]
    async fn test_executor_multiple_queries() {
        let ctx = SessionContext::new();
        register_streaming_functions(&ctx);
        let mut executor = StreamExecutor::new(ctx);

        executor.add_query(
            "count_stream".to_string(),
            "SELECT COUNT(*) as cnt FROM events".to_string(),
        );
        executor.add_query(
            "sum_stream".to_string(),
            "SELECT SUM(value) as total FROM events".to_string(),
        );

        let mut source_batches = HashMap::new();
        source_batches.insert("events".to_string(), vec![test_batch()]);

        let results = executor.execute_cycle(&source_batches).await.unwrap();
        assert!(results.contains_key("count_stream"));
        assert!(results.contains_key("sum_stream"));
    }

    #[tokio::test]
    async fn test_executor_with_filter() {
        let ctx = SessionContext::new();
        register_streaming_functions(&ctx);
        let mut executor = StreamExecutor::new(ctx);

        executor.add_query(
            "filtered".to_string(),
            "SELECT * FROM events WHERE value > 1.5".to_string(),
        );

        let mut source_batches = HashMap::new();
        source_batches.insert("events".to_string(), vec![test_batch()]);

        let results = executor.execute_cycle(&source_batches).await.unwrap();
        assert!(results.contains_key("filtered"));
        let total_rows: usize = results["filtered"].iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2); // value 2.0 and 3.0
    }

    #[tokio::test]
    async fn test_executor_cleanup_between_cycles() {
        let ctx = SessionContext::new();
        register_streaming_functions(&ctx);
        let mut executor = StreamExecutor::new(ctx);

        executor.add_query("pass".to_string(), "SELECT * FROM events".to_string());

        // First cycle
        let mut source_batches = HashMap::new();
        source_batches.insert("events".to_string(), vec![test_batch()]);
        let r1 = executor.execute_cycle(&source_batches).await.unwrap();
        assert!(r1.contains_key("pass"));

        // Second cycle with no data — table should have been cleaned up
        let empty = HashMap::new();
        let r2 = executor.execute_cycle(&empty).await.unwrap();
        assert!(!r2.contains_key("pass"));
    }

    #[tokio::test]
    async fn test_executor_register_table() {
        let ctx = SessionContext::new();
        register_streaming_functions(&ctx);
        let mut executor = StreamExecutor::new(ctx);

        let dim_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("label", DataType::Utf8, false),
        ]));
        let dim_batch = RecordBatch::try_new(
            dim_schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["one", "two"])),
            ],
        )
        .unwrap();

        executor.register_table("dim", dim_batch).unwrap();

        executor.add_query(
            "joined".to_string(),
            "SELECT e.name, d.label FROM events e JOIN dim d ON e.id = d.id".to_string(),
        );

        let mut source_batches = HashMap::new();
        source_batches.insert("events".to_string(), vec![test_batch()]);

        let results = executor.execute_cycle(&source_batches).await.unwrap();
        assert!(results.contains_key("joined"));
        let total_rows: usize = results["joined"].iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2); // ids 1 and 2 match
    }

    #[tokio::test]
    async fn test_executor_cascading_queries() {
        let ctx = SessionContext::new();
        register_streaming_functions(&ctx);
        let mut executor = StreamExecutor::new(ctx);

        // Level 1: aggregate from source
        executor.add_query(
            "level1".to_string(),
            "SELECT name, SUM(value) as total FROM events GROUP BY name".to_string(),
        );
        // Level 2: read from level1 output (cascading)
        executor.add_query(
            "level2".to_string(),
            "SELECT name, total * 2 as doubled FROM level1".to_string(),
        );

        let mut source_batches = HashMap::new();
        source_batches.insert("events".to_string(), vec![test_batch()]);

        let results = executor.execute_cycle(&source_batches).await.unwrap();

        // Level 1 should produce results
        assert!(results.contains_key("level1"), "level1 should have results");
        let l1_rows: usize = results["level1"].iter().map(|b| b.num_rows()).sum();
        assert_eq!(l1_rows, 3); // 3 distinct names

        // Level 2 should also produce results (reading from level1)
        assert!(
            results.contains_key("level2"),
            "level2 should have results (cascading from level1)"
        );
        let l2_rows: usize = results["level2"].iter().map(|b| b.num_rows()).sum();
        assert_eq!(l2_rows, 3); // same 3 names, doubled values
    }

    #[tokio::test]
    async fn test_executor_cascading_three_levels() {
        let ctx = SessionContext::new();
        register_streaming_functions(&ctx);
        let mut executor = StreamExecutor::new(ctx);

        // Level 1: passthrough
        executor.add_query(
            "l1".to_string(),
            "SELECT id, name, value FROM events".to_string(),
        );
        // Level 2: filter from l1
        executor.add_query(
            "l2".to_string(),
            "SELECT id, name, value FROM l1 WHERE value > 1.5".to_string(),
        );
        // Level 3: aggregate from l2
        executor.add_query(
            "l3".to_string(),
            "SELECT COUNT(*) as cnt FROM l2".to_string(),
        );

        let mut source_batches = HashMap::new();
        source_batches.insert("events".to_string(), vec![test_batch()]);

        let results = executor.execute_cycle(&source_batches).await.unwrap();

        assert!(results.contains_key("l1"));
        assert!(results.contains_key("l2"));
        assert!(results.contains_key("l3"), "l3 should cascade through l1 → l2 → l3");

        let l2_rows: usize = results["l2"].iter().map(|b| b.num_rows()).sum();
        assert_eq!(l2_rows, 2); // value 2.0 and 3.0
    }
}
