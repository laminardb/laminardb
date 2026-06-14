//! `DataFusion` table providers for `TableStore`, streaming sources, and materialized views.

use std::any::Any;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;

use crate::catalog::SourceEntry;
use crate::mv_store::MvStore;
use crate::table_store::TableStore;

/// Live `DataFusion` table provider over `TableStore`; each `scan()` sees the current snapshot.
pub(crate) struct ReferenceTableProvider {
    table_name: String,
    schema: SchemaRef,
    table_store: Arc<parking_lot::RwLock<TableStore>>,
}

impl ReferenceTableProvider {
    pub fn new(
        table_name: String,
        schema: SchemaRef,
        table_store: Arc<parking_lot::RwLock<TableStore>>,
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
            .read()
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

/// Point-in-time snapshot provider for a streaming source's buffered data.
pub(crate) struct SourceSnapshotProvider {
    source_entry: Arc<SourceEntry>,
    num_partitions: usize,
}

impl SourceSnapshotProvider {
    /// `num_partitions` is clamped to `1..=256`.
    pub fn new(source_entry: Arc<SourceEntry>, num_partitions: usize) -> Self {
        Self {
            source_entry,
            num_partitions: num_partitions.clamp(1, 256),
        }
    }
}

#[async_trait]
impl TableProvider for SourceSnapshotProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.source_entry.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let batches = self.source_entry.snapshot();
        let schema = self.source_entry.schema.clone();
        let data = if batches.is_empty() {
            (0..self.num_partitions).map(|_| Vec::new()).collect()
        } else {
            // Round-robin distribution; skew is fine for ad-hoc snapshot queries.
            let mut partitions: Vec<Vec<_>> =
                (0..self.num_partitions).map(|_| Vec::new()).collect();
            for (i, batch) in batches.into_iter().enumerate() {
                partitions[i % self.num_partitions].push(batch);
            }
            partitions
        };
        let mem_table = datafusion::datasource::MemTable::try_new(schema, data)?;
        mem_table.scan(state, projection, filters, limit).await
    }
}

impl std::fmt::Debug for SourceSnapshotProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SourceSnapshotProvider")
            .field("source", &self.source_entry.name)
            .field("num_partitions", &self.num_partitions)
            .finish_non_exhaustive()
    }
}

/// `DataFusion` table provider for materialized view results.
pub(crate) struct MvTableProvider {
    mv_name: String,
    schema: SchemaRef,
    mv_store: Arc<parking_lot::RwLock<MvStore>>,
}

impl MvTableProvider {
    pub fn new(
        mv_name: String,
        schema: SchemaRef,
        mv_store: Arc<parking_lot::RwLock<MvStore>>,
    ) -> Self {
        Self {
            mv_name,
            schema,
            mv_store,
        }
    }
}

#[async_trait]
impl TableProvider for MvTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let batch = self
            .mv_store
            .read()
            .to_record_batch(&self.mv_name)
            .unwrap_or_else(|| arrow::array::RecordBatch::new_empty(self.schema.clone()));

        let schema = batch.schema();
        let data = if batch.num_rows() > 0 {
            vec![vec![batch]]
        } else {
            vec![vec![]]
        };

        let mem_table = datafusion::datasource::MemTable::try_new(schema, data)?;
        mem_table.scan(state, projection, filters, limit).await
    }
}

impl std::fmt::Debug for MvTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MvTableProvider")
            .field("mv_name", &self.mv_name)
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use arrow::array::{Float64Array, Int32Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use laminar_sql::create_session_context;

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
        let ts = Arc::new(parking_lot::RwLock::new(TableStore::new()));
        let provider = ReferenceTableProvider::new("test".to_string(), test_schema(), ts);
        assert_eq!(provider.schema(), test_schema());
    }

    #[test]
    fn test_provider_table_type() {
        let ts = Arc::new(parking_lot::RwLock::new(TableStore::new()));
        let provider = ReferenceTableProvider::new("test".to_string(), test_schema(), ts);
        assert_eq!(provider.table_type(), TableType::Base);
    }

    #[tokio::test]
    async fn test_provider_scan_empty() {
        let ts = Arc::new(parking_lot::RwLock::new(TableStore::new()));
        {
            let mut store = ts.write();
            store.create_table("test", test_schema(), "id").unwrap();
        }

        let provider = ReferenceTableProvider::new("test".to_string(), test_schema(), ts);
        let ctx = create_session_context();
        ctx.register_table("test", Arc::new(provider)).unwrap();

        let df = ctx.sql("SELECT * FROM test").await.unwrap();
        let batches = df.collect().await.unwrap();
        let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total_rows, 0);
    }

    #[tokio::test]
    async fn test_provider_scan_with_data() {
        let ts = Arc::new(parking_lot::RwLock::new(TableStore::new()));
        {
            let mut store = ts.write();
            store.create_table("test", test_schema(), "id").unwrap();
            store
                .upsert("test", &make_batch(&[1, 2], &["A", "B"], &[1.0, 2.0]))
                .unwrap();
        }

        let provider = ReferenceTableProvider::new("test".to_string(), test_schema(), ts);
        let ctx = create_session_context();
        ctx.register_table("test", Arc::new(provider)).unwrap();

        let df = ctx.sql("SELECT * FROM test").await.unwrap();
        let batches = df.collect().await.unwrap();
        let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total_rows, 2);
    }

    #[tokio::test]
    async fn test_provider_reads_live_data() {
        let ts = Arc::new(parking_lot::RwLock::new(TableStore::new()));
        {
            let mut store = ts.write();
            store.create_table("test", test_schema(), "id").unwrap();
        }

        let provider = ReferenceTableProvider::new("test".to_string(), test_schema(), ts.clone());
        let ctx = create_session_context();
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
            let mut store = ts.write();
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
        let ts = Arc::new(parking_lot::RwLock::new(TableStore::new()));
        let provider = ReferenceTableProvider::new("test".to_string(), test_schema(), ts);
        let debug = format!("{provider:?}");
        assert!(debug.contains("ReferenceTableProvider"));
        assert!(debug.contains("test"));
    }
}
