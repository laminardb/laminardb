//! Query result types for FFI.

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;

use super::error::ApiError;
use crate::catalog::ArrowRecord;

/// Materialized query result containing all batches.
///
/// This is the result of collecting all streaming results into memory.
/// Use [`QueryStream`] for streaming consumption.
#[derive(Debug, Clone)]
pub struct QueryResult {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
}

impl QueryResult {
    /// Create from a single batch.
    #[must_use]
    pub fn from_batch(batch: RecordBatch) -> Self {
        let schema = batch.schema();
        Self {
            schema,
            batches: vec![batch],
        }
    }

    /// Create from multiple batches.
    #[must_use]
    pub fn from_batches(schema: SchemaRef, batches: Vec<RecordBatch>) -> Self {
        Self { schema, batches }
    }

    /// Get the schema.
    #[must_use]
    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    /// Get all batches by reference.
    #[must_use]
    pub fn batches(&self) -> &[RecordBatch] {
        &self.batches
    }

    /// Consume and return all batches.
    #[must_use]
    pub fn into_batches(self) -> Vec<RecordBatch> {
        self.batches
    }

    /// Total row count across all batches.
    #[must_use]
    pub fn num_rows(&self) -> usize {
        self.batches.iter().map(RecordBatch::num_rows).sum()
    }

    /// Number of batches.
    #[must_use]
    pub fn num_batches(&self) -> usize {
        self.batches.len()
    }

    /// Get a batch by index.
    #[must_use]
    pub fn batch(&self, index: usize) -> Option<&RecordBatch> {
        self.batches.get(index)
    }

    /// Total column count.
    #[must_use]
    pub fn num_columns(&self) -> usize {
        self.schema.fields().len()
    }
}

/// Streaming query result.
///
/// Provides access to query results as they become available.
/// Use [`QueryResult`] if you need all results at once.
#[derive(Debug)]
pub struct QueryStream {
    schema: SchemaRef,
    handle: Option<crate::QueryHandle>,
    subscription: Option<laminar_core::streaming::Subscription<ArrowRecord>>,
}

impl QueryStream {
    /// Create from a `QueryHandle`.
    pub(crate) fn from_handle(mut handle: crate::QueryHandle) -> Self {
        let schema = handle.schema().clone();
        let subscription = handle.subscribe_raw().ok();
        Self {
            schema,
            handle: Some(handle),
            subscription,
        }
    }

    /// Get the schema.
    #[must_use]
    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    /// Get the next batch (blocking).
    ///
    /// Returns `Ok(None)` when the stream is exhausted.
    ///
    /// # Errors
    ///
    /// Returns `ApiError` if receiving fails.
    #[allow(clippy::should_implement_trait)] // FFI method name, not Iterator
    pub fn next(&mut self) -> Result<Option<RecordBatch>, ApiError> {
        match &self.subscription {
            Some(sub) => match sub.recv() {
                Ok(batch) => Ok(Some(batch)),
                Err(laminar_core::streaming::RecvError::Disconnected) => Ok(None),
                Err(e) => Err(ApiError::subscription(e.to_string())),
            },
            None => Ok(None),
        }
    }

    /// Try to get the next batch (non-blocking).
    ///
    /// Returns `Ok(None)` if no batch is currently available.
    ///
    /// # Errors
    ///
    /// Returns `ApiError` if receiving fails.
    pub fn try_next(&mut self) -> Result<Option<RecordBatch>, ApiError> {
        match &self.subscription {
            Some(sub) => Ok(sub.poll()),
            None => Ok(None),
        }
    }

    /// Collect all results into a `QueryResult`.
    ///
    /// # Errors
    ///
    /// Returns `ApiError` if receiving fails.
    pub fn collect(mut self) -> Result<QueryResult, ApiError> {
        let mut batches = Vec::new();
        while let Some(batch) = self.try_next()? {
            batches.push(batch);
        }
        Ok(QueryResult::from_batches(self.schema, batches))
    }

    /// Check if the stream is still active.
    #[must_use]
    pub fn is_active(&self) -> bool {
        self.handle
            .as_ref()
            .is_some_and(crate::QueryHandle::is_active)
    }

    /// Cancel the query.
    pub fn cancel(&mut self) {
        if let Some(ref mut handle) = self.handle {
            handle.cancel();
        }
        self.subscription = None;
    }
}

// SAFETY: QueryStream uses Arc and Mutex internally for thread safety.
// The subscription is based on lock-free channels.
unsafe impl Send for QueryStream {}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    fn test_batch() -> RecordBatch {
        RecordBatch::try_new(
            test_schema(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_query_result_from_batch() {
        let batch = test_batch();
        let result = QueryResult::from_batch(batch);

        assert_eq!(result.num_rows(), 3);
        assert_eq!(result.num_columns(), 2);
        assert_eq!(result.batches().len(), 1);
    }

    #[test]
    fn test_query_result_schema() {
        let result = QueryResult::from_batch(test_batch());
        let schema = result.schema();
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "id");
    }

    #[test]
    fn test_query_result_into_batches() {
        let result = QueryResult::from_batch(test_batch());
        let batches = result.into_batches();
        assert_eq!(batches.len(), 1);
    }
}
