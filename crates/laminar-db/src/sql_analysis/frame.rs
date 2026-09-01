//! Window-frame moment query routing.
//!
//! Plans bivariate moment queries (`CORR`/`COVAR_SAMP`/`COVAR_POP OVER … ROWS N PRECEDING`)
//! for the managed window-frame operator and renders their residual projection.

use sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, FunctionArguments, Ident, ObjectName, ObjectNamePart,
    SelectItem, SetExpr, Statement, TableFactor,
};

use crate::operator::window_frame::MomentFn;

/// Temp table a window-frame operator writes enriched batches to; the residual projection reads from it.
pub(crate) const FRAME_TMP_TABLE: &str = "__frame_tmp";

/// A routable bivariate moment frame query (`CORR`/`COVAR_SAMP`/`COVAR_POP OVER …`).
pub(crate) struct FrameQueryPlan {
    pub func: MomentFn,
    pub x_column: String,
    pub y_column: String,
    pub output_alias: String,
    /// Residual SELECT over [`FRAME_TMP_TABLE`] with the stat call replaced by its alias.
    pub projection_sql: String,
    pub source_table: String,
    /// Rows of preceding history to retain per new row (`max(PRECEDING)`).
    pub retain: usize,
}

/// Plan a query for window-frame routing.
///
/// Returns `Some` only for: single un-joined source, one `CORR`/`COVAR_SAMP`/`COVAR_POP`
/// `OVER (ORDER BY … ROWS N PRECEDING) AS alias`, no `PARTITION BY` or `FOLLOWING`.
pub(crate) fn plan_frame_query(sql: &str) -> Option<FrameQueryPlan> {
    let statements = laminar_sql::parse_streaming_sql(sql).ok()?;
    let mut statement = match statements.into_iter().next()? {
        laminar_sql::parser::StreamingStatement::Standard(boxed) => *boxed,
        _ => return None,
    };

    let analysis = laminar_sql::parser::analytic_parser::analyze_window_frames(&statement)?;
    if !analysis.partition_columns.is_empty() || analysis.has_following() {
        return None;
    }
    let retain = usize::try_from(analysis.max_preceding()).ok()?;
    if retain == 0 {
        return None;
    }

    let Statement::Query(query) = &mut statement else {
        return None;
    };
    let SetExpr::Select(select) = query.body.as_mut() else {
        return None;
    };
    if select.from.len() != 1 || !select.from[0].joins.is_empty() {
        return None;
    }
    let source_table = match &select.from[0].relation {
        TableFactor::Table { name, .. } => name.to_string(),
        _ => return None,
    };

    let mut found: Option<(usize, MomentFn, String, String, String)> = None;
    for (index, item) in select.projection.iter().enumerate() {
        let SelectItem::ExprWithAlias { expr, alias } = item else {
            continue;
        };
        if let Some((func, x, y)) = moment_call(expr) {
            if found.is_some() {
                return None;
            }
            found = Some((index, func, x, y, alias.value.clone()));
        }
    }
    let (index, func, x_column, y_column, output_alias) = found?;

    // Rewrite: stat call → bare alias column; FROM → temp table.
    select.projection[index] =
        SelectItem::UnnamedExpr(Expr::Identifier(Ident::new(output_alias.clone())));
    if let TableFactor::Table { name, .. } = &mut select.from[0].relation {
        *name = ObjectName(vec![ObjectNamePart::Identifier(Ident::new(
            FRAME_TMP_TABLE,
        ))]);
    }

    Some(FrameQueryPlan {
        func,
        x_column,
        y_column,
        output_alias,
        projection_sql: statement.to_string(),
        source_table,
        retain,
    })
}

fn moment_call(expr: &Expr) -> Option<(MomentFn, String, String)> {
    let Expr::Function(func) = expr else {
        return None;
    };
    func.over.as_ref()?;
    let kind = match func.name.to_string().to_ascii_uppercase().as_str() {
        "CORR" => MomentFn::Corr,
        "COVAR_SAMP" | "COVAR" => MomentFn::CovarSamp,
        "COVAR_POP" => MomentFn::CovarPop,
        _ => return None,
    };
    let (x, y) = bivariate_column_args(func)?;
    Some((kind, x, y))
}

fn bivariate_column_args(func: &sqlparser::ast::Function) -> Option<(String, String)> {
    let FunctionArguments::List(list) = &func.args else {
        return None;
    };
    let cols: Vec<String> = list
        .args
        .iter()
        .filter_map(|arg| match arg {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Identifier(id))) => {
                Some(id.value.clone())
            }
            _ => None,
        })
        .collect();
    match cols.as_slice() {
        [x, y] => Some((x.clone(), y.clone())),
        _ => None,
    }
}
