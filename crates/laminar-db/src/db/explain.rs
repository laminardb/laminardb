//! `EXPLAIN [ANALYZE]`: planner metadata and optional execution metrics, as
//! `plan_key`/`plan_value` rows.

use std::sync::Arc;

use arrow::array::{Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use laminar_sql::parser::StreamingStatement;
use laminar_sql::planner::StreamingPlan;

use super::LaminarDB;
use crate::error::DbError;
use crate::handle::ExecuteResult;

impl LaminarDB {
    pub(super) fn handle_explain(
        &self,
        statement: &StreamingStatement,
    ) -> Result<ExecuteResult, DbError> {
        metadata(&self.planner_rows(statement), "explain metadata")
    }

    pub(super) async fn handle_explain_analyze(
        &self,
        statement: &StreamingStatement,
        original_sql: &str,
    ) -> Result<ExecuteResult, DbError> {
        let explain_result = self.handle_explain(statement)?;
        let mut rows: Vec<(String, String)> = Vec::new();

        if let ExecuteResult::Metadata(explain_batch) = &explain_result {
            let keys_col = explain_batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>();
            let vals_col = explain_batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>();
            if let (Some(keys), Some(vals)) = (keys_col, vals_col) {
                for i in 0..explain_batch.num_rows() {
                    rows.push((keys.value(i).to_string(), vals.value(i).to_string()));
                }
            }
        }

        let upper = original_sql.to_uppercase();
        let inner_start = upper.find("ANALYZE").map_or(0, |pos| pos + "ANALYZE".len());
        let inner_sql = original_sql[inner_start..].trim();

        let start = std::time::Instant::now();
        match self.ctx.sql(inner_sql).await {
            Ok(df) => match df.collect().await {
                Ok(batches) => {
                    let elapsed = start.elapsed();
                    let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
                    rows.push(("rows_produced".into(), total_rows.to_string()));
                    rows.push(("execution_time_ms".into(), elapsed.as_millis().to_string()));
                    rows.push(("batches_processed".into(), batches.len().to_string()));
                }
                Err(e) => {
                    let elapsed = start.elapsed();
                    rows.push(("execution_time_ms".into(), elapsed.as_millis().to_string()));
                    rows.push(("analyze_error".into(), format!("{e}")));
                }
            },
            Err(e) => {
                rows.push(("analyze_error".into(), format!("{e}")));
            }
        }

        metadata(&rows, "explain analyze metadata")
    }

    fn planner_rows(&self, statement: &StreamingStatement) -> Vec<(String, String)> {
        let mut planner = self.planner.lock();
        let plan = match planner.plan(statement) {
            Ok(plan) => plan,
            Err(e) => {
                return vec![
                    ("error".into(), format!("{e}")),
                    (
                        "statement".into(),
                        format!("{:?}", std::mem::discriminant(statement)),
                    ),
                ];
            }
        };
        let mut rows: Vec<(String, String)> = vec![(
            "plan_type".into(),
            match &plan {
                StreamingPlan::Query(_) => "Query",
                StreamingPlan::RegisterSource(_) => "RegisterSource",
                StreamingPlan::RegisterSink(_) => "RegisterSink",
                StreamingPlan::Standard(_) => "Standard",
                StreamingPlan::RegisterLookupTable(_) => "RegisterLookupTable",
                StreamingPlan::DropLookupTable { .. } => "DropLookupTable",
            }
            .into(),
        )];
        match &plan {
            StreamingPlan::Query(qp) => {
                if let Some(name) = &qp.name {
                    rows.push(("query_name".into(), name.clone()));
                }
                if let Some(wc) = &qp.window_config {
                    rows.push(("window".into(), format!("{wc}")));
                }
                if let Some(jcs) = &qp.join_config {
                    if jcs.len() == 1 {
                        rows.push(("join".into(), format!("{}", jcs[0])));
                    } else {
                        for (i, jc) in jcs.iter().enumerate() {
                            rows.push((format!("join_step_{}", i + 1), format!("{jc}")));
                        }
                    }
                }
                if let Some(oc) = &qp.order_config {
                    rows.push(("order_by".into(), format!("{oc:?}")));
                }
                if let Some(fc) = &qp.frame_config {
                    rows.push(("frame_functions".into(), format!("{}", fc.functions.len())));
                }
                if let Some(ec) = &qp.emit_clause {
                    rows.push(("emit".into(), format!("{ec}")));
                }
            }
            StreamingPlan::RegisterSource(info) => {
                rows.push(("source".into(), info.name.clone()));
            }
            StreamingPlan::RegisterSink(info) => {
                rows.push(("sink".into(), info.name.clone()));
            }
            StreamingPlan::Standard(_) => {
                rows.push(("execution".into(), "DataFusion pass-through".into()));
            }
            StreamingPlan::RegisterLookupTable(info) => {
                rows.push(("lookup_table".into(), info.name.clone()));
            }
            StreamingPlan::DropLookupTable { name } => {
                rows.push(("drop_lookup_table".into(), name.clone()));
            }
        }
        rows
    }
}

fn metadata(rows: &[(String, String)], context: &str) -> Result<ExecuteResult, DbError> {
    let keys: Vec<&str> = rows.iter().map(|(k, _)| k.as_str()).collect();
    let values: Vec<&str> = rows.iter().map(|(_, v)| v.as_str()).collect();
    let schema = Arc::new(Schema::new(vec![
        Field::new("plan_key", DataType::Utf8, false),
        Field::new("plan_value", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(keys)),
            Arc::new(StringArray::from(values)),
        ],
    )
    .map_err(|e| DbError::InvalidOperation(format!("{context}: {e}")))?;
    Ok(ExecuteResult::Metadata(batch))
}
