//! `DataFusion` plan inspection and result-batch shaping.
//!
//! Extracts the projection/filter/scan shape a managed operator must reproduce outside
//! `DataFusion`, certifies plan function volatility, and applies global Top-K limits that
//! `DataFusion`'s per-batch `LIMIT` cannot express.

use std::sync::Arc;

use arrow::array::RecordBatch;
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_expr::{LogicalPlan, Volatility};

pub(crate) fn planned_functions_are_immutable(plan: &LogicalPlan) -> bool {
    let mut immutable = true;
    let _ = plan.apply(|node| {
        for expression in node.expressions() {
            let _ = expression.apply(|expression| {
                let volatility = match expression {
                    datafusion_expr::Expr::ScalarFunction(function) => {
                        Some(function.func.signature().volatility)
                    }
                    datafusion_expr::Expr::AggregateFunction(function) => {
                        Some(function.func.signature().volatility)
                    }
                    datafusion_expr::Expr::WindowFunction(function) => {
                        Some(function.fun.signature().volatility)
                    }
                    _ => None,
                };
                if volatility.is_some_and(|volatility| volatility != Volatility::Immutable) {
                    immutable = false;
                    return Ok(TreeNodeRecursion::Stop);
                }
                Ok(TreeNodeRecursion::Continue)
            });
            if !immutable {
                return Ok(TreeNodeRecursion::Stop);
            }
        }
        Ok(TreeNodeRecursion::Continue)
    });
    immutable
}

pub(crate) struct ProjectionFilterInfo {
    pub(crate) proj_exprs: Vec<datafusion_expr::Expr>,
    pub(crate) filter_predicate: Option<datafusion_expr::Expr>,
    pub(crate) input_df_schema: Arc<datafusion_common::DFSchema>,
}

/// Returns `Some` only for `Projection? -> Filter? -> TableScan` plans.
pub(crate) fn extract_projection_filter(plan: &LogicalPlan) -> Option<ProjectionFilterInfo> {
    match plan {
        LogicalPlan::Projection(proj) => {
            let proj_exprs = proj.expr.clone();
            extract_filter_or_scan(&proj.input).map(|(filter_pred, input_schema, _)| {
                ProjectionFilterInfo {
                    proj_exprs,
                    filter_predicate: filter_pred,
                    input_df_schema: input_schema,
                }
            })
        }
        _ => match extract_filter_or_scan(plan) {
            Some((filter_pred, input_schema, _)) => {
                let proj_exprs: Vec<datafusion_expr::Expr> = input_schema
                    .fields()
                    .iter()
                    .map(|f| {
                        datafusion_expr::Expr::Column(datafusion_common::Column::new_unqualified(
                            f.name(),
                        ))
                    })
                    .collect();
                Some(ProjectionFilterInfo {
                    proj_exprs,
                    filter_predicate: filter_pred,
                    input_df_schema: input_schema,
                })
            }
            None => None,
        },
    }
}

fn extract_filter_or_scan(
    plan: &LogicalPlan,
) -> Option<(
    Option<datafusion_expr::Expr>,
    Arc<datafusion_common::DFSchema>,
    String,
)> {
    match plan {
        LogicalPlan::Filter(filter) => match &*filter.input {
            LogicalPlan::TableScan(scan) => Some((
                Some(filter.predicate.clone()),
                Arc::clone(filter.input.schema()),
                scan.table_name.to_string(),
            )),
            LogicalPlan::SubqueryAlias(alias) => {
                if let LogicalPlan::TableScan(scan) = &*alias.input {
                    Some((
                        Some(filter.predicate.clone()),
                        Arc::clone(filter.input.schema()),
                        scan.table_name.to_string(),
                    ))
                } else {
                    None
                }
            }
            _ => None,
        },
        LogicalPlan::TableScan(scan) => {
            Some((None, Arc::clone(plan.schema()), scan.table_name.to_string()))
        }
        LogicalPlan::SubqueryAlias(alias) => extract_filter_or_scan(&alias.input),
        _ => None,
    }
}

/// Apply a global Top-K limit across all batches.
///
/// `DataFusion`'s `LIMIT N` is per micro-batch; this slices the combined result to `k` rows.
pub(crate) fn apply_topk_filter(batches: &[RecordBatch], k: usize) -> Vec<RecordBatch> {
    if batches.is_empty() || k == 0 {
        return Vec::new();
    }

    let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    if total_rows <= k {
        return batches.to_vec();
    }

    let mut remaining = k;
    let mut result = Vec::new();
    for batch in batches {
        if remaining == 0 {
            break;
        }
        let take = remaining.min(batch.num_rows());
        result.push(batch.slice(0, take));
        remaining -= take;
    }
    result
}
