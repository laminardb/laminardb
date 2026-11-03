//! The window operator configuration of a streaming query: its `GROUP BY` window, its EMIT
//! clause, and `ALLOW LATENESS`.

use sqlparser::ast::{GroupByExpr, Select};

use super::PlanningError;
use crate::parser::{EmitClause, LateDataClause, WindowFunction, WindowRewriter};
use crate::translator::WindowOperatorConfig;

/// The first window function in `select`'s `GROUP BY`.
pub(super) fn window_function(select: &Select) -> Option<WindowFunction> {
    let GroupByExpr::Expressions(exprs, _modifiers) = &select.group_by else {
        return None;
    };
    exprs
        .iter()
        .find_map(|expr| WindowRewriter::extract_window_function(expr).ok().flatten())
}

/// `select`'s window configuration. Lateness applies only to a window that emits on close: other
/// EMIT modes never read it, and `EMIT FINAL` drops late rows.
pub(super) fn plan_window_config(
    select: &Select,
    emit_clause: Option<&EmitClause>,
    late_data: Option<&LateDataClause>,
) -> Result<Option<WindowOperatorConfig>, PlanningError> {
    let invalid = |error: crate::parser::ParseError| PlanningError::InvalidQuery(error.to_string());
    let Some(window) = window_function(select) else {
        return Ok(None);
    };
    let mut config = WindowOperatorConfig::from_window_function(&window).map_err(invalid)?;
    if let Some(emit) = emit_clause {
        config = config.with_emit_clause(emit).map_err(invalid)?;
    }
    if let Some(late_data) = late_data {
        if !matches!(emit_clause, Some(EmitClause::OnWindowClose)) {
            return Err(PlanningError::InvalidQuery(
                "ALLOW LATENESS requires EMIT ON WINDOW CLOSE".to_string(),
            ));
        }
        config = config.with_late_data_clause(late_data).map_err(invalid)?;
    }
    Ok(Some(config))
}
