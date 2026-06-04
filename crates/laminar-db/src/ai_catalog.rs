//! Read-only `laminar.*` AI catalog views.
//!
//! - `laminar.models` — the model registry: each model's kind, tasks, source,
//!   and determinism/cost properties.
//! - `laminar.ai_calls` — the inference call log: one row per batch call, with
//!   tokens, cost, latency, and status.
//!
//! Both are a single [`SystemView`] parameterized by a column-builder, registered
//! as a `laminar` schema on the query context. Each `scan()` reads the live
//! [`AiRuntime`], so the views always reflect current state.

use std::any::Any;
use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::{MemorySchemaProvider, SchemaProvider, Session};
use datafusion::datasource::{MemTable, TableProvider, TableType};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;

use crate::ai::{AiRuntime, BackendKind, ModelBackend};

use crate::error::DbError;

/// Reserved schema for AI catalog views.
pub(crate) const LAMINAR_SCHEMA: &str = "laminar";

/// Snapshots the live runtime into a view's columns (schema order).
type ColumnBuilder = fn(&AiRuntime) -> Vec<ArrayRef>;

/// Register `laminar.models` and `laminar.ai_calls` on `ctx`.
///
/// # Errors
///
/// Returns [`DbError::InvalidOperation`] if the default catalog is missing or
/// the schema/tables cannot be registered.
pub(crate) fn register_ai_catalog(
    ctx: &SessionContext,
    runtime: &Arc<AiRuntime>,
) -> Result<(), DbError> {
    let schema = Arc::new(MemorySchemaProvider::new());
    let views: [(&str, SchemaRef, ColumnBuilder); 2] = [
        ("models", models_schema(), models_columns),
        ("ai_calls", ai_calls_schema(), ai_calls_columns),
    ];
    for (name, view_schema, columns) in views {
        let view = SystemView {
            schema: view_schema,
            columns,
            runtime: Arc::clone(runtime),
        };
        schema
            .register_table(name.to_string(), Arc::new(view))
            .map_err(|e| DbError::InvalidOperation(format!("register laminar.{name}: {e}")))?;
    }

    let catalog = ctx.catalog("datafusion").ok_or_else(|| {
        DbError::InvalidOperation("default catalog 'datafusion' is not present".to_string())
    })?;
    catalog
        .register_schema(LAMINAR_SCHEMA, schema)
        .map_err(|e| DbError::InvalidOperation(format!("register laminar schema: {e}")))?;
    Ok(())
}

/// A read-only catalog view: a fixed schema plus a function that snapshots the
/// live [`AiRuntime`] into its columns on each scan.
struct SystemView {
    schema: SchemaRef,
    columns: ColumnBuilder,
    runtime: Arc<AiRuntime>,
}

impl std::fmt::Debug for SystemView {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SystemView").finish_non_exhaustive()
    }
}

#[async_trait]
impl TableProvider for SystemView {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
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
        let batch = RecordBatch::try_new(Arc::clone(&self.schema), (self.columns)(&self.runtime))
            .expect("system view columns match its schema");
        MemTable::try_new(Arc::clone(&self.schema), vec![vec![batch]])?
            .scan(state, projection, filters, limit)
            .await
    }
}

fn kind_str(kind: BackendKind) -> &'static str {
    match kind {
        BackendKind::Local => "local",
        BackendKind::Remote => "remote",
    }
}

fn to_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn models_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("model", DataType::Utf8, false),
        Field::new("kind", DataType::Utf8, false),
        Field::new("tasks", DataType::Utf8, false),
        Field::new("source", DataType::Utf8, false),
        Field::new("deterministic", DataType::Boolean, false),
        Field::new("costed", DataType::Boolean, false),
    ]))
}

fn models_columns(runtime: &AiRuntime) -> Vec<ArrayRef> {
    let mut model = Vec::new();
    let mut kind = Vec::new();
    let mut tasks = Vec::new();
    let mut source = Vec::new();
    let mut deterministic = Vec::new();
    let mut costed = Vec::new();
    for entry in runtime.registry().iter() {
        model.push(entry.id.clone());
        kind.push(kind_str(entry.kind()).to_string());
        tasks.push(
            entry
                .tasks
                .iter()
                .map(|t| t.as_str())
                .collect::<Vec<_>>()
                .join(","),
        );
        source.push(match &entry.backend {
            ModelBackend::Local { source, .. } => source.clone(),
            ModelBackend::Remote { provider, model } => format!("{provider}:{model}"),
        });
        deterministic.push(entry.is_deterministic());
        costed.push(entry.is_costed());
    }
    vec![
        Arc::new(StringArray::from(model)),
        Arc::new(StringArray::from(kind)),
        Arc::new(StringArray::from(tasks)),
        Arc::new(StringArray::from(source)),
        Arc::new(BooleanArray::from(deterministic)),
        Arc::new(BooleanArray::from(costed)),
    ]
}

fn ai_calls_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("timestamp_ms", DataType::Int64, false),
        Field::new("model", DataType::Utf8, false),
        Field::new("provider", DataType::Utf8, false),
        Field::new("task", DataType::Utf8, false),
        Field::new("kind", DataType::Utf8, false),
        Field::new("batch_size", DataType::Int64, false),
        Field::new("input_tokens", DataType::Int64, false),
        Field::new("output_tokens", DataType::Int64, false),
        Field::new("cost_micros", DataType::Int64, false),
        Field::new("latency_ms", DataType::Int64, false),
        Field::new("status", DataType::Utf8, false),
    ]))
}

fn ai_calls_columns(runtime: &AiRuntime) -> Vec<ArrayRef> {
    let records = runtime.call_log().snapshot();
    let mut ts = Vec::with_capacity(records.len());
    let mut model = Vec::with_capacity(records.len());
    let mut provider = Vec::with_capacity(records.len());
    let mut task = Vec::with_capacity(records.len());
    let mut kind = Vec::with_capacity(records.len());
    let mut batch_size = Vec::with_capacity(records.len());
    let mut input_tokens = Vec::with_capacity(records.len());
    let mut output_tokens = Vec::with_capacity(records.len());
    let mut cost = Vec::with_capacity(records.len());
    let mut latency = Vec::with_capacity(records.len());
    let mut status = Vec::with_capacity(records.len());
    for record in &records {
        ts.push(record.timestamp_ms);
        model.push(record.model.clone());
        provider.push(record.provider.to_string());
        task.push(record.task.to_string());
        kind.push(kind_str(record.kind).to_string());
        batch_size.push(i64::from(record.batch_size));
        input_tokens.push(to_i64(record.usage.input_tokens));
        output_tokens.push(to_i64(record.usage.output_tokens));
        cost.push(to_i64(record.usage.cost_micros));
        latency.push(to_i64(record.latency_ms));
        status.push(record.outcome.status().to_string());
    }
    vec![
        Arc::new(Int64Array::from(ts)),
        Arc::new(StringArray::from(model)),
        Arc::new(StringArray::from(provider)),
        Arc::new(StringArray::from(task)),
        Arc::new(StringArray::from(kind)),
        Arc::new(Int64Array::from(batch_size)),
        Arc::new(Int64Array::from(input_tokens)),
        Arc::new(Int64Array::from(output_tokens)),
        Arc::new(Int64Array::from(cost)),
        Arc::new(Int64Array::from(latency)),
        Arc::new(StringArray::from(status)),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ai::{
        AiCallLog, AiResultCache, InferenceProvider, ModelBackend, ModelEntry, ModelRegistry, Task,
    };

    struct Stub;
    #[async_trait]
    impl InferenceProvider for Stub {
        async fn infer_batch(
            &self,
            request: crate::ai::InferenceRequest,
        ) -> Result<crate::ai::InferenceResponse, crate::ai::ProviderError> {
            Ok(crate::ai::InferenceResponse {
                outputs: crate::ai::InferenceOutputs::Text(vec![
                    String::new();
                    request.inputs.len()
                ]),
                usage: crate::ai::Usage::ZERO,
            })
        }
        fn name(&self) -> &'static str {
            "stub"
        }
    }

    fn runtime() -> Arc<AiRuntime> {
        let mut registry = ModelRegistry::new();
        registry
            .register(ModelEntry {
                id: "finbert".into(),
                tasks: vec![Task::Classify, Task::Sentiment],
                backend: ModelBackend::Local {
                    source: "hf:onnx-community/finbert".into(),
                    labels: None,
                },
            })
            .unwrap();
        registry
            .register(ModelEntry {
                id: "haiku".into(),
                tasks: vec![Task::Complete],
                backend: ModelBackend::Remote {
                    provider: "anthropic".into(),
                    model: "claude-haiku-4-5-20251001".into(),
                },
            })
            .unwrap();
        let providers = [(
            "anthropic".to_string(),
            Arc::new(Stub) as Arc<dyn InferenceProvider>,
        )];
        Arc::new(AiRuntime::new(
            registry,
            providers,
            None,
            Arc::new(AiResultCache::with_defaults()),
            Arc::new(AiCallLog::with_defaults()),
        ))
    }

    #[tokio::test]
    async fn models_view_is_queryable() {
        let ctx = laminar_sql::create_session_context();
        register_ai_catalog(&ctx, &runtime()).unwrap();

        let batches = ctx
            .sql("SELECT model, kind, deterministic, costed FROM laminar.models ORDER BY model")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(rows, 2);
    }

    #[tokio::test]
    async fn ai_calls_view_starts_empty_and_is_queryable() {
        let ctx = laminar_sql::create_session_context();
        register_ai_catalog(&ctx, &runtime()).unwrap();

        let batches = ctx
            .sql("SELECT count(*) AS n FROM laminar.ai_calls")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert!(!batches.is_empty());
    }
}
