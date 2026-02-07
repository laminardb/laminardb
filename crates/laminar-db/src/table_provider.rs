//! Live `DataFusion` table provider backed by the `TableStore`.
//!
//! Unlike `MemTable` (which is a static snapshot), [`ReferenceTableProvider`]
//! reads directly from the `TableStore` on each `scan()` call, so queries
//! always see the latest data without needing to deregister/re-register.

use std::any::Any;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;

use crate::table_store::TableStore;

/// A `DataFusion` table provider that reads live data from `TableStore`.
///
/// Registered once at CREATE TABLE time and never needs re-registration —
/// each `scan()` fetches the current snapshot from the backing store.
pub(crate) struct ReferenceTableProvider {
    table_name: String,
    schema: SchemaRef,
    table_store: Arc<parking_lot::Mutex<TableStore>>,
}

impl ReferenceTableProvider {
    /// Create a new provider for the given table.
    pub fn new(
        table_name: String,
        schema: SchemaRef,
        table_store: Arc<parking_lot::Mutex<TableStore>>,
    ) -> Self {
        Self {
            table_name,
            schema,
            table_store,
        }
    }
}

#[async_trait]
impl TableProvider for ReferenceTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let batch = self
            .table_store
            .lock()
            .to_record_batch(&self.table_name)
            .unwrap_or_else(|| arrow::array::RecordBatch::new_empty(self.schema.clone()));

        let schema = batch.schema();
        let data = if batch.num_rows() > 0 {
            vec![vec![batch]]
        } else {
            vec![vec![]]
        };

        let mem_table = datafusion::datasource::MemTable::try_new(schema, data)?;
        mem_table.scan(_state, _projection, _filters, _limit).await
    }
}

impl std::fmt::Debug for ReferenceTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReferenceTableProvider")
            .field("table_name", &self.table_name)
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use arrow::array::{Float64Array, Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::prelude::SessionContext;

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("price", DataType::Float64, true),
        ]))
    }

    fn make_batch(ids: &[i32], names: &[&str], prices: &[f64]) -> arrow::array::RecordBatch {
        arrow::array::RecordBatch::try_new(
            test_schema(),
            vec![
                Arc::new(Int32Array::from(ids.to_vec())),
                Arc::new(StringArray::from(names.to_vec())),
                Arc::new(Float64Array::from(prices.to_vec())),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_provider_schema() {
        let ts = Arc::new(parking_lot::Mutex::new(TableStore::new()));
        let provider = ReferenceTableProvider::new("test".to_string(), test_schema(), ts);
        assert_eq!(provider.schema(), test_schema());
    }

    #[test]
    fn test_provider_table_type() {
        let ts = Arc::new(parking_lot::Mutex::new(TableStore::new()));
        let provider = ReferenceTableProvider::new("test".to_string(), test_schema(), ts);
        assert_eq!(provider.table_type(), TableType::Base);
    }

    #[tokio::test]
    async fn test_provider_scan_empty() {
        let ts = Arc::new(parking_lot::Mutex::new(TableStore::new()));
        {
            let mut store = ts.lock();
            store.create_table("test", test_schema(), "id").unwrap();
        }

        let provider = ReferenceTableProvider::new("test".to_string(), test_schema(), ts);
        let ctx = SessionContext::new();
        ctx.register_table("test", Arc::new(provider)).unwrap();

        let df = ctx.sql("SELECT * FROM test").await.unwrap();
        let batches = df.collect().await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 0);
    }

    #[tokio::test]
    async fn test_provider_scan_with_data() {
        let ts = Arc::new(parking_lot::Mutex::new(TableStore::new()));
        {
            let mut store = ts.lock();
            store.create_table("test", test_schema(), "id").unwrap();
            store
                .upsert("test", &make_batch(&[1, 2], &["A", "B"], &[1.0, 2.0]))
                .unwrap();
        }

        let provider = ReferenceTableProvider::new("test".to_string(), test_schema(), ts);
        let ctx = SessionContext::new();
        ctx.register_table("test", Arc::new(provider)).unwrap();

        let df = ctx.sql("SELECT * FROM test").await.unwrap();
        let batches = df.collect().await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }

    #[tokio::test]
    async fn test_provider_reads_live_data() {
        let ts = Arc::new(parking_lot::Mutex::new(TableStore::new()));
        {
            let mut store = ts.lock();
            store.create_table("test", test_schema(), "id").unwrap();
        }

        let provider = ReferenceTableProvider::new("test".to_string(), test_schema(), ts.clone());
        let ctx = SessionContext::new();
        ctx.register_table("test", Arc::new(provider)).unwrap();

        // First query: empty
        let df = ctx.sql("SELECT count(*) AS cnt FROM test").await.unwrap();
        let batches = df.collect().await.unwrap();
        let cnt_col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(cnt_col.value(0), 0);

        // Insert data
        {
            let mut store = ts.lock();
            store
                .upsert("test", &make_batch(&[1], &["A"], &[1.0]))
                .unwrap();
        }

        // Second query: should see the new row without re-registration
        let df = ctx.sql("SELECT count(*) AS cnt FROM test").await.unwrap();
        let batches = df.collect().await.unwrap();
        let cnt_col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(cnt_col.value(0), 1);
    }

    #[test]
    fn test_provider_debug() {
        let ts = Arc::new(parking_lot::Mutex::new(TableStore::new()));
        let provider = ReferenceTableProvider::new("test".to_string(), test_schema(), ts);
        let debug = format!("{provider:?}");
        assert!(debug.contains("ReferenceTableProvider"));
        assert!(debug.contains("test"));
    }
}
