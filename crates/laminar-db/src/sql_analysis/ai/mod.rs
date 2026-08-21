//! AI-function query routing: call detection, plan-time model validation, and residual
//! projection planning for `ai_*` calls over a single source table.

use sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, FunctionArguments, Ident, ObjectName, ObjectNamePart,
    SelectItem, SetExpr, Statement, TableFactor,
};

use crate::ai::{BackendKind, ModelRegistry, Task};

use crate::error::DbError;

pub(crate) const AI_TMP_TABLE: &str = "__ai_tmp";

/// A query routable to the AI operator: exactly one `ai_*` call plus residual projection.
pub(crate) struct AiQueryPlan {
    pub call: AiCallSpec,
    /// Residual SQL over [`AI_TMP_TABLE`] with the `ai_*` item rewritten to its alias column.
    pub projection_sql: String,
    pub source_table: String,
}

/// Plan a query for AI routing: exactly one aliased `ai_*` call over a single source table.
///
/// Returns `None` for multiple AI calls, missing alias, or any join.
pub(crate) fn plan_ai_query(sql: &str) -> Option<AiQueryPlan> {
    let statements = laminar_sql::parse_streaming_sql(sql).ok()?;
    let mut statement = match statements.into_iter().next()? {
        laminar_sql::parser::StreamingStatement::Standard(boxed) => *boxed,
        _ => return None,
    };
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

    let mut found: Option<(usize, AiCallSpec)> = None;
    for (index, item) in select.projection.iter().enumerate() {
        let (expr, alias) = match item {
            SelectItem::UnnamedExpr(expr) => (expr, None),
            SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias.value.clone())),
            _ => continue,
        };
        if let Some(spec) = ai_call_from_expr(expr, alias) {
            if found.is_some() {
                return None;
            }
            found = Some((index, spec));
        }
    }
    let (index, call) = found?;

    // Rewrite: AI item → bare alias column; FROM → temp table.
    // Reuse the original alias Ident so quoted aliases stay quoted.
    let SelectItem::ExprWithAlias { alias, .. } = &select.projection[index] else {
        return None;
    };
    let alias = alias.clone();
    select.projection[index] = SelectItem::UnnamedExpr(Expr::Identifier(alias));
    if let TableFactor::Table { name, .. } = &mut select.from[0].relation {
        *name = ObjectName(vec![ObjectNamePart::Identifier(Ident::new(AI_TMP_TABLE))]);
    }
    let projection_sql = statement.to_string();

    Some(AiQueryPlan {
        call,
        projection_sql,
        source_table,
    })
}

/// One detected `ai_*(...)` call in a query's SELECT projection.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct AiCallSpec {
    pub task: Task,
    pub model: Option<String>,
    pub labels: Option<Vec<String>>,
    /// Empty when the first argument was missing or not a plain column.
    pub input: String,
    pub output_alias: Option<String>,
    /// Non-empty means the call is malformed; surfaced by [`validate_ai_calls`].
    pub parse_errors: Vec<String>,
}

/// Detect `ai_*` calls in the top-level SELECT projection of `sql`.
///
/// Calls nested in expressions or `WHERE` are not recognised here; the marker UDF
/// rejects them. Returns an empty vec if the SQL does not parse or has no AI calls.
pub(crate) fn detect_ai_functions(sql: &str) -> Vec<AiCallSpec> {
    let Ok(statements) = laminar_sql::parse_streaming_sql(sql) else {
        return Vec::new();
    };
    let Some(laminar_sql::parser::StreamingStatement::Standard(stmt)) = statements.first() else {
        return Vec::new();
    };
    let Statement::Query(query) = stmt.as_ref() else {
        return Vec::new();
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Vec::new();
    };

    let mut calls = Vec::new();
    for item in &select.projection {
        let (expr, alias) = match item {
            SelectItem::UnnamedExpr(expr) => (expr, None),
            SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias.value.clone())),
            _ => continue,
        };
        if let Some(spec) = ai_call_from_expr(expr, alias) {
            calls.push(spec);
        }
    }
    calls
}

fn ai_call_from_expr(expr: &Expr, alias: Option<String>) -> Option<AiCallSpec> {
    let Expr::Function(func) = expr else {
        return None;
    };
    let [ObjectNamePart::Identifier(name)] = func.name.0.as_slice() else {
        return None;
    };
    let task = task_from_ai_function(&name.value.to_ascii_lowercase())?;
    let FunctionArguments::List(list) = &func.args else {
        return None;
    };

    let mut input: Option<String> = None;
    let mut seen_input = false;
    let mut model: Option<String> = None;
    let mut labels: Option<Vec<String>> = None;
    let mut parse_errors: Vec<String> = Vec::new();

    for arg in &list.args {
        match arg {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(value)) => {
                if seen_input {
                    parse_errors
                        .push("AI functions take a single positional input column".to_string());
                } else {
                    seen_input = true;
                    // Only a plain column reference — the operator does a name-based lookup.
                    match column_name(value) {
                        Some(col) => input = Some(col),
                        None => parse_errors.push(format!(
                            "AI function input must be a simple column reference, got `{value}`"
                        )),
                    }
                }
            }
            FunctionArg::Named {
                name,
                arg: FunctionArgExpr::Expr(value),
                ..
            } => match name.value.to_ascii_lowercase().as_str() {
                "model" => match string_literal(value) {
                    Some(s) => model = Some(s),
                    None => {
                        parse_errors.push("`model` argument must be a string literal".to_string());
                    }
                },
                "labels" => match string_array_literal(value) {
                    Some(v) => labels = Some(v),
                    None => parse_errors
                        .push("`labels` argument must be an array of string literals".to_string()),
                },
                other => {
                    parse_errors.push(format!("unsupported AI function argument `{other}`"));
                }
            },
            other => {
                parse_errors.push(format!("unsupported AI function argument: {other}"));
            }
        }
    }

    if !seen_input {
        parse_errors
            .push("AI function requires a column reference as its first argument".to_string());
    }

    Some(AiCallSpec {
        task,
        model,
        labels,
        input: input.unwrap_or_default(),
        output_alias: alias,
        parse_errors,
    })
}

fn column_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Identifier(ident) => Some(ident.value.clone()),
        _ => None,
    }
}

// Must stay in step with the marker list in laminar-sql's ai_udf.
pub(crate) fn is_ai_function_name(name: &str) -> bool {
    task_from_ai_function(&name.to_ascii_lowercase()).is_some()
}

fn task_from_ai_function(name: &str) -> Option<Task> {
    match name {
        "ai_classify" => Some(Task::Classify),
        "ai_sentiment" => Some(Task::Sentiment),
        "ai_embed" => Some(Task::Embed),
        "ai_extract" => Some(Task::Extract),
        "ai_complete" => Some(Task::Complete),
        "ai_summarize" => Some(Task::Summarize),
        "ai_translate" => Some(Task::Translate),
        "ai_gen" => Some(Task::Gen),
        _ => None,
    }
}

fn string_literal(expr: &Expr) -> Option<String> {
    let Expr::Value(value) = expr else {
        return None;
    };
    match &value.value {
        sqlparser::ast::Value::SingleQuotedString(s)
        | sqlparser::ast::Value::DoubleQuotedString(s) => Some(s.clone()),
        _ => None,
    }
}

fn string_array_literal(expr: &Expr) -> Option<Vec<String>> {
    let Expr::Array(array) = expr else {
        return None;
    };
    array.elem.iter().map(string_literal).collect()
}

/// Validate every detected AI call against the model registry.
///
/// Fails at plan time for an unknown model, unsupported task, or a labels mismatch.
pub(crate) fn validate_ai_calls(
    registry: &ModelRegistry,
    calls: &[AiCallSpec],
) -> Result<(), DbError> {
    for call in calls {
        validate_ai_call(registry, call)?;
    }
    Ok(())
}

fn validate_ai_call(registry: &ModelRegistry, call: &AiCallSpec) -> Result<(), DbError> {
    if let Some(err) = call.parse_errors.first() {
        return Err(DbError::InvalidOperation(err.clone()));
    }

    let model_name = match &call.model {
        Some(name) => name.clone(),
        None => registry
            .default_for(call.task)
            .map(str::to_string)
            .ok_or_else(|| {
                DbError::InvalidOperation(format!(
                    "no model given for task '{}' and no [ai.defaults] default is configured",
                    call.task
                ))
            })?,
    };

    let entry = registry
        .validate(&model_name, call.task)
        .map_err(|e| DbError::InvalidOperation(e.to_string()))?;

    match entry.kind() {
        BackendKind::Local => {
            if let Some(requested) = &call.labels {
                let model_labels = entry.labels().ok_or_else(|| {
                    DbError::InvalidOperation(format!(
                        "local model '{model_name}' exposes no labels to validate against"
                    ))
                })?;
                if let Some(unknown) = requested
                    .iter()
                    .find(|label| !model_labels.iter().any(|known| known == *label))
                {
                    return Err(DbError::InvalidOperation(format!(
                        "label '{unknown}' is not among local model '{model_name}' labels \
                         {model_labels:?}"
                    )));
                }
            }
        }
        BackendKind::Remote => {
            // Remote sentiment returns a numeric score, so no candidate set needed.
            if call.task == Task::Classify && call.labels.is_none() {
                return Err(DbError::InvalidOperation(format!(
                    "remote classification with model '{model_name}' requires a 'labels' argument"
                )));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod ai_detection_tests;
