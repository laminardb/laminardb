//! Temporal join operator for the `OperatorGraph`.
//!
//! Wraps the versioned temporal join logic from `laminar-sql`. The stream
//! side arrives via `inputs[0]`, while the lookup table comes from the
//! `LookupTableRegistry` (pre-registered by the pipeline lifecycle).
//! The operator is stateless (no cross-cycle state).

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::prelude::SessionContext;

use laminar_sql::datafusion::lookup_join::LookupJoinType;
use laminar_sql::datafusion::lookup_join_exec::{
    LookupTableRegistry, RegisteredLookup, VersionedLookupJoinExec,
};
use laminar_sql::translator::TemporalJoinTranslatorConfig;

use crate::error::DbError;
use crate::operator_graph::{GraphOperator, OperatorCheckpoint};
use crate::stream_executor::CompiledPostProjection;

pub(crate) struct TemporalJoinOperator {
    op_name: Arc<str>,
    config: TemporalJoinTranslatorConfig,
    projection_sql: Option<Arc<str>>,
    ctx: SessionContext,
    lookup_registry: Option<Arc<LookupTableRegistry>>,
    last_temporal_row_count: usize,
    compiled_post_proj: Option<CompiledPostProjection>,
    post_proj_compile_failed: bool,
}

impl TemporalJoinOperator {
    pub(crate) fn new(
        name: &str,
        config: TemporalJoinTranslatorConfig,
        projection_sql: Option<Arc<str>>,
        ctx: SessionContext,
        lookup_registry: Option<Arc<LookupTableRegistry>>,
    ) -> Self {
        Self {
            op_name: Arc::from(name),
            config,
            projection_sql,
            ctx,
            lookup_registry,
            last_temporal_row_count: 0,
            compiled_post_proj: None,
            post_proj_compile_failed: false,
        }
    }

    /// Execute the versioned temporal join using the `VersionedLookupJoinExec`.
    #[allow(clippy::too_many_lines)]
    async fn execute_versioned_join(
        &mut self,
        stream_batches: Vec<RecordBatch>,
    ) -> Result<Vec<RecordBatch>, DbError> {
        use datafusion::catalog::TableProvider;
        use datafusion::physical_plan::ExecutionPlan as _;
        use futures::TryStreamExt as _;

        let registry = self.lookup_registry.as_ref().ok_or_else(|| {
            DbError::Pipeline(format!(
                "temporal join [{}]: lookup registry not set",
                self.op_name
            ))
        })?;

        let entry = registry.get_entry(&self.config.table_name).ok_or_else(|| {
            DbError::Pipeline(format!(
                "temporal join [{}]: table '{}' not registered",
                self.op_name, self.config.table_name
            ))
        })?;

        let RegisteredLookup::Versioned(versioned) = entry else {
            return Err(DbError::Pipeline(format!(
                "temporal join [{}]: table '{}' not registered as versioned",
                self.op_name, self.config.table_name
            )));
        };

        // Warn once per query when the temporal table shrinks (non-append-only)
        let current_rows = versioned.batch.num_rows();
        if current_rows < self.last_temporal_row_count {
            tracing::warn!(
                query = %self.op_name,
                table = %self.config.table_name,
                previous_rows = self.last_temporal_row_count,
                current_rows = current_rows,
                "Temporal join table has been modified. \
                 Retractions for previously-joined events are NOT emitted. \
                 Use append-only tables for correct temporal join semantics."
            );
        }
        self.last_temporal_row_count = current_rows;

        if stream_batches.iter().all(|b| b.num_rows() == 0) {
            return Ok(Vec::new());
        }

        let table_schema = versioned.batch.schema();
        let stream_schema = stream_batches[0].schema();

        let stream_key_idx = stream_schema
            .index_of(&self.config.stream_key_column)
            .map_err(|_| {
                DbError::Pipeline(format!(
                    "temporal join [{}]: stream key column '{}' not found",
                    self.op_name, self.config.stream_key_column
                ))
            })?;
        let stream_time_idx = stream_schema
            .index_of(&self.config.stream_time_column)
            .map_err(|_| {
                DbError::Pipeline(format!(
                    "temporal join [{}]: stream time column '{}' not found",
                    self.op_name, self.config.stream_time_column
                ))
            })?;

        let join_type = if self.config.join_type == "left" {
            LookupJoinType::LeftOuter
        } else {
            LookupJoinType::Inner
        };

        let key_sort_fields: Vec<arrow::row::SortField> = versioned
            .key_columns
            .iter()
            .filter_map(|k| {
                table_schema
                    .index_of(k)
                    .ok()
                    .map(|i| arrow::row::SortField::new(table_schema.field(i).data_type().clone()))
            })
            .collect();

        let output_schema = build_temporal_output_schema(&stream_schema, &table_schema);

        // Build a MemTable from stream batches as the input plan
        let mem_table = datafusion::datasource::MemTable::try_new(
            Arc::clone(&stream_schema),
            vec![stream_batches],
        )
        .map_err(|e| {
            DbError::Pipeline(format!(
                "temporal join [{}]: memory table: {e}",
                self.op_name
            ))
        })?;

        let input = mem_table
            .scan(&self.ctx.state(), None, &[], None)
            .await
            .map_err(|e| {
                DbError::Pipeline(format!("temporal join [{}]: scan: {e}", self.op_name))
            })?;

        let exec = VersionedLookupJoinExec::try_new(
            input,
            versioned.batch.clone(),
            Arc::clone(&versioned.index),
            vec![stream_key_idx],
            stream_time_idx,
            join_type,
            output_schema,
            key_sort_fields,
        )
        .map_err(|e| {
            DbError::Pipeline(format!(
                "temporal join [{}]: exec build error: {e}",
                self.op_name
            ))
        })?;

        let task_ctx = self.ctx.state().task_ctx();
        let stream = exec
            .execute(0, task_ctx)
            .map_err(|e| DbError::Pipeline(format!("temporal join [{}]: {e}", self.op_name)))?;

        let batches: Vec<RecordBatch> = stream
            .try_collect()
            .await
            .map_err(|e| DbError::Pipeline(format!("temporal join [{}]: {e}", self.op_name)))?;

        Ok(batches)
    }

    async fn apply_projection(
        &mut self,
        batches: Vec<RecordBatch>,
    ) -> Result<Vec<RecordBatch>, DbError> {
        super::apply_post_projection(
            &self.ctx,
            &self.op_name,
            "__temporal_tmp",
            self.projection_sql.as_deref(),
            &mut self.compiled_post_proj,
            &mut self.post_proj_compile_failed,
            batches,
        )
        .await
    }
}

/// Build the merged output schema: stream fields followed by table fields.
fn build_temporal_output_schema(stream_schema: &SchemaRef, table_schema: &SchemaRef) -> SchemaRef {
    let mut fields = stream_schema.fields().to_vec();
    fields.extend(table_schema.fields().iter().cloned());
    Arc::new(arrow::datatypes::Schema::new(fields))
}

#[async_trait]
impl GraphOperator for TemporalJoinOperator {
    async fn process(
        &mut self,
        inputs: &[Vec<RecordBatch>],
        _watermark: i64,
    ) -> Result<Vec<RecordBatch>, DbError> {
        let stream_batches = inputs.first().cloned().unwrap_or_default();

        if stream_batches.is_empty() || stream_batches.iter().all(|b| b.num_rows() == 0) {
            return Ok(Vec::new());
        }

        let joined = self.execute_versioned_join(stream_batches).await?;
        self.apply_projection(joined).await
    }

    fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
        // Stateless — no checkpoint needed.
        Ok(None)
    }

    fn restore(&mut self, _checkpoint: OperatorCheckpoint) -> Result<(), DbError> {
        // Stateless — nothing to restore.
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> TemporalJoinTranslatorConfig {
        TemporalJoinTranslatorConfig {
            stream_table: "orders".to_string(),
            table_name: "rates".to_string(),
            stream_key_column: "currency".to_string(),
            table_key_column: "currency".to_string(),
            stream_time_column: "order_ts".to_string(),
            table_version_column: "valid_from".to_string(),
            semantics: "event_time".to_string(),
            join_type: "inner".to_string(),
        }
    }

    #[tokio::test]
    async fn test_empty_input() {
        let ctx = laminar_sql::create_session_context();
        let mut op = TemporalJoinOperator::new("test_temporal", test_config(), None, ctx, None);

        let result = op.process(&[vec![]], 0).await.unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_no_registry_returns_error() {
        let ctx = laminar_sql::create_session_context();
        let mut op = TemporalJoinOperator::new("test_temporal", test_config(), None, ctx, None);

        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("currency", arrow::datatypes::DataType::Utf8, false),
            arrow::datatypes::Field::new("order_ts", arrow::datatypes::DataType::Int64, false),
            arrow::datatypes::Field::new("amount", arrow::datatypes::DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["USD"])),
                Arc::new(arrow::array::Int64Array::from(vec![1000])),
                Arc::new(arrow::array::Float64Array::from(vec![100.0])),
            ],
        )
        .unwrap();

        let result = op.process(&[vec![batch]], 0).await;
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("lookup registry not set"),
            "unexpected error: {err_msg}"
        );
    }

    #[test]
    fn test_checkpoint_returns_none() {
        let ctx = laminar_sql::create_session_context();
        let mut op = TemporalJoinOperator::new("test_temporal", test_config(), None, ctx, None);
        assert!(op.checkpoint().unwrap().is_none());
    }

    #[test]
    fn test_name() {
        let ctx = laminar_sql::create_session_context();
        let op = TemporalJoinOperator::new("my_temporal_query", test_config(), None, ctx, None);
        assert_eq!(&*op.op_name, "my_temporal_query");
    }
}
