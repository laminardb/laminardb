//! HAVING clause translator
//!
//! Translates a parsed HAVING expression into a configuration that can
//! filter aggregated `RecordBatch` results. The filter is applied after
//! window aggregation emission, before downstream consumption.

use std::sync::Arc;

use crate::datafusion::create_session_context;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion::datasource::MemTable;

/// Configuration for a post-aggregation HAVING filter.
#[derive(Debug, Clone)]
pub struct HavingFilterConfig {
    /// The HAVING predicate as a SQL expression string.
    predicate: String,
}

impl HavingFilterConfig {
    /// Creates a new HAVING filter configuration.
    #[must_use]
    pub fn new(predicate: String) -> Self {
        Self { predicate }
    }

    /// Returns the predicate SQL string.
    #[must_use]
    pub fn predicate(&self) -> &str {
        &self.predicate
    }

    /// Filters a `RecordBatch` by evaluating the HAVING predicate.
    ///
    /// Registers the batch as a temporary table in a DataFusion context,
    /// applies the predicate as a WHERE clause, and returns matching rows.
    ///
    /// # Errors
    ///
    /// Returns an error if the predicate cannot be parsed, the schema
    /// doesn't match, or evaluation fails.
    pub async fn filter_batch(
        &self,
        batch: &RecordBatch,
    ) -> Result<RecordBatch, HavingFilterError> {
        if batch.num_rows() == 0 {
            return Ok(batch.clone());
        }

        let schema = batch.schema();

        // Register the batch as a temporary MemTable so column names resolve
        let ctx = create_session_context();
        let mem_table = MemTable::try_new(schema.clone(), vec![vec![batch.clone()]])
            .map_err(|e| HavingFilterError::SchemaError(e.to_string()))?;
        ctx.register_table("_having_input", Arc::new(mem_table))
            .map_err(|e| HavingFilterError::SchemaError(e.to_string()))?;

        // Execute: SELECT * FROM _having_input WHERE <predicate>
        let sql = format!("SELECT * FROM _having_input WHERE {}", self.predicate);
        let df = ctx
            .sql(&sql)
            .await
            .map_err(|e| HavingFilterError::ParseError(e.to_string()))?;

        let batches = df
            .collect()
            .await
            .map_err(|e| HavingFilterError::EvaluationError(e.to_string()))?;

        if batches.is_empty() {
            return Ok(RecordBatch::new_empty(schema));
        }

        arrow::compute::concat_batches(&schema, &batches)
            .map_err(|e| HavingFilterError::EvaluationError(e.to_string()))
    }

    /// Returns the output schema (same as input — HAVING only filters rows).
    #[must_use]
    pub fn output_schema(&self, input_schema: &SchemaRef) -> SchemaRef {
        Arc::clone(input_schema)
    }
}

/// Errors from HAVING filter operations.
#[derive(Debug, thiserror::Error)]
pub enum HavingFilterError {
    /// Failed to parse the HAVING expression
    #[error("HAVING parse error: {0}")]
    ParseError(String),

    /// Schema mismatch or missing columns
    #[error("HAVING schema error: {0}")]
    SchemaError(String),

    /// Evaluation failed
    #[error("HAVING evaluation error: {0}")]
    EvaluationError(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Float64Array, Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    fn test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new("count_star", DataType::Int64, false),
            Field::new("sum_volume", DataType::Float64, false),
        ]));

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["AAPL", "GOOG", "MSFT", "TSLA"])),
                Arc::new(Int64Array::from(vec![100, 5, 50, 200])),
                Arc::new(Float64Array::from(vec![50000.0, 1000.0, 25000.0, 80000.0])),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_filter_count_greater_than() {
        let config = HavingFilterConfig::new("count_star > 10".to_string());
        let batch = test_batch();
        let result = config.filter_batch(&batch).await.unwrap();

        assert_eq!(result.num_rows(), 3); // AAPL(100), MSFT(50), TSLA(200)
        let symbols: Vec<&str> = result
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .map(|v| v.unwrap())
            .collect();
        assert_eq!(symbols, vec!["AAPL", "MSFT", "TSLA"]);
    }

    #[tokio::test]
    async fn test_filter_all_pass() {
        let config = HavingFilterConfig::new("count_star > 0".to_string());
        let batch = test_batch();
        let result = config.filter_batch(&batch).await.unwrap();

        assert_eq!(result.num_rows(), 4);
    }

    #[tokio::test]
    async fn test_filter_none_pass() {
        let config = HavingFilterConfig::new("count_star > 1000".to_string());
        let batch = test_batch();
        let result = config.filter_batch(&batch).await.unwrap();

        assert_eq!(result.num_rows(), 0);
    }

    #[tokio::test]
    async fn test_filter_compound_predicate() {
        let config = HavingFilterConfig::new("count_star >= 50 AND sum_volume > 30000".to_string());
        let batch = test_batch();
        let result = config.filter_batch(&batch).await.unwrap();

        // AAPL: 100 >= 50 AND 50000 > 30000 → yes
        // GOOG: 5 >= 50 → no
        // MSFT: 50 >= 50 AND 25000 > 30000 → no
        // TSLA: 200 >= 50 AND 80000 > 30000 → yes
        assert_eq!(result.num_rows(), 2);
    }

    #[tokio::test]
    async fn test_filter_or_predicate() {
        let config = HavingFilterConfig::new("count_star > 150 OR sum_volume < 2000".to_string());
        let batch = test_batch();
        let result = config.filter_batch(&batch).await.unwrap();

        // AAPL: 100 > 150 OR 50000 < 2000 → no
        // GOOG: 5 > 150 OR 1000 < 2000 → yes
        // MSFT: 50 > 150 OR 25000 < 2000 → no
        // TSLA: 200 > 150 OR 80000 < 2000 → yes
        assert_eq!(result.num_rows(), 2);
    }

    #[tokio::test]
    async fn test_filter_empty_batch() {
        let config = HavingFilterConfig::new("count_star > 10".to_string());
        let schema = Arc::new(Schema::new(vec![Field::new(
            "count_star",
            DataType::Int64,
            false,
        )]));
        let batch = RecordBatch::new_empty(schema);
        let result = config.filter_batch(&batch).await.unwrap();

        assert_eq!(result.num_rows(), 0);
    }

    #[tokio::test]
    async fn test_filter_invalid_column() {
        let config = HavingFilterConfig::new("nonexistent_col > 10".to_string());
        let batch = test_batch();
        let result = config.filter_batch(&batch).await;

        assert!(result.is_err());
    }

    #[test]
    fn test_predicate_accessor() {
        let config = HavingFilterConfig::new("SUM(volume) > 1000".to_string());
        assert_eq!(config.predicate(), "SUM(volume) > 1000");
    }

    #[test]
    fn test_output_schema_unchanged() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let config = HavingFilterConfig::new("a > 0".to_string());
        let output = config.output_schema(&schema);
        assert_eq!(*output, *schema);
    }
}
