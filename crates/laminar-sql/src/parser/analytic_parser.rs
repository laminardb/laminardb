//! Analytic window function detection and extraction
//!
//! Analyzes SQL queries for analytic functions like LAG, LEAD, FIRST_VALUE,
//! LAST_VALUE, and NTH_VALUE with OVER clauses. These are per-row window
//! functions (distinct from GROUP BY aggregate windows like TUMBLE/HOP/SESSION).

use sqlparser::ast::{Expr, SelectItem, SetExpr, Statement};

/// Types of analytic window functions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AnalyticFunctionType {
    /// LAG(col, offset, default) — look back `offset` rows in partition.
    Lag,
    /// LEAD(col, offset, default) — look ahead `offset` rows in partition.
    Lead,
    /// FIRST_VALUE(col) OVER (...) — first value in window frame.
    FirstValue,
    /// LAST_VALUE(col) OVER (...) — last value in window frame.
    LastValue,
    /// NTH_VALUE(col, n) OVER (...) — n-th value in window frame.
    NthValue,
}

impl AnalyticFunctionType {
    /// Returns the function name as used in SQL.
    #[must_use]
    pub fn sql_name(&self) -> &'static str {
        match self {
            Self::Lag => "LAG",
            Self::Lead => "LEAD",
            Self::FirstValue => "FIRST_VALUE",
            Self::LastValue => "LAST_VALUE",
            Self::NthValue => "NTH_VALUE",
        }
    }

    /// Returns true if this function requires buffering future events.
    #[must_use]
    pub fn requires_lookahead(&self) -> bool {
        matches!(self, Self::Lead)
    }
}

/// Information about a single analytic function call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnalyticFunctionInfo {
    /// Type of analytic function
    pub function_type: AnalyticFunctionType,
    /// Column being referenced (first argument)
    pub column: String,
    /// Offset for LAG/LEAD (default 1), or N for NTH_VALUE
    pub offset: usize,
    /// Default value expression as string (for LAG/LEAD third argument)
    pub default_value: Option<String>,
    /// Output alias (AS name)
    pub alias: Option<String>,
}

/// Result of analyzing analytic functions in a query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnalyticWindowAnalysis {
    /// Analytic functions found in the query
    pub functions: Vec<AnalyticFunctionInfo>,
    /// PARTITION BY columns from the OVER clause
    pub partition_columns: Vec<String>,
    /// ORDER BY columns from the OVER clause
    pub order_columns: Vec<String>,
}

impl AnalyticWindowAnalysis {
    /// Returns true if any function requires lookahead (LEAD).
    #[must_use]
    pub fn has_lookahead(&self) -> bool {
        self.functions
            .iter()
            .any(|f| f.function_type.requires_lookahead())
    }

    /// Returns the maximum offset across all functions.
    #[must_use]
    pub fn max_offset(&self) -> usize {
        self.functions.iter().map(|f| f.offset).max().unwrap_or(0)
    }
}

/// Analyzes a SQL statement for analytic window functions.
///
/// Walks SELECT items looking for functions with OVER clauses that match
/// LAG, LEAD, FIRST_VALUE, LAST_VALUE, or NTH_VALUE. Returns `None` if
/// no analytic functions are found.
///
/// # Arguments
///
/// * `stmt` - The SQL statement to analyze
///
/// # Returns
///
/// An `AnalyticWindowAnalysis` if analytic functions are found, or `None`.
#[must_use]
pub fn analyze_analytic_functions(stmt: &Statement) -> Option<AnalyticWindowAnalysis> {
    let Statement::Query(query) = stmt else {
        return None;
    };

    let SetExpr::Select(select) = query.body.as_ref() else {
        return None;
    };

    let mut functions = Vec::new();
    let mut partition_columns = Vec::new();
    let mut order_columns = Vec::new();
    let mut first_window = true;

    for item in &select.projection {
        let (expr, alias) = match item {
            SelectItem::UnnamedExpr(expr) => (expr, None),
            SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias.value.clone())),
            _ => continue,
        };

        if let Some(info) = extract_analytic_function(expr, alias, &mut |spec| {
            if first_window {
                partition_columns = spec
                    .partition_by
                    .iter()
                    .filter_map(extract_column_name)
                    .collect();
                order_columns = spec
                    .order_by
                    .iter()
                    .filter_map(|ob| extract_column_name(&ob.expr))
                    .collect();
                first_window = false;
            }
        }) {
            functions.push(info);
        }
    }

    if functions.is_empty() {
        return None;
    }

    Some(AnalyticWindowAnalysis {
        functions,
        partition_columns,
        order_columns,
    })
}

/// Extracts an analytic function from an expression.
///
/// Returns function info if the expression is a recognized analytic function
/// with an OVER clause. Calls `on_window_spec` with the window spec from the
/// first function found so the caller can extract partition/order columns.
fn extract_analytic_function(
    expr: &Expr,
    alias: Option<String>,
    on_window_spec: &mut dyn FnMut(&sqlparser::ast::WindowSpec),
) -> Option<AnalyticFunctionInfo> {
    let Expr::Function(func) = expr else {
        return None;
    };

    let name = func.name.to_string().to_uppercase();
    let function_type = match name.as_str() {
        "LAG" => AnalyticFunctionType::Lag,
        "LEAD" => AnalyticFunctionType::Lead,
        "FIRST_VALUE" => AnalyticFunctionType::FirstValue,
        "LAST_VALUE" => AnalyticFunctionType::LastValue,
        "NTH_VALUE" => AnalyticFunctionType::NthValue,
        _ => return None,
    };

    // Must have an OVER clause to be an analytic function
    let window_spec = func.over.as_ref()?;
    match window_spec {
        sqlparser::ast::WindowType::WindowSpec(spec) => {
            on_window_spec(spec);
        }
        sqlparser::ast::WindowType::NamedWindow(_) => {}
    }

    // Extract arguments
    let args = extract_function_args(func);

    // First arg is the column
    let column = args.first().cloned().unwrap_or_default();

    // Second arg is offset (for LAG/LEAD) or N (for NTH_VALUE), default 1
    let offset = args
        .get(1)
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(1);

    // Third arg is default value (for LAG/LEAD only)
    let default_value = if matches!(
        function_type,
        AnalyticFunctionType::Lag | AnalyticFunctionType::Lead
    ) {
        args.get(2).cloned()
    } else {
        None
    };

    Some(AnalyticFunctionInfo {
        function_type,
        column,
        offset,
        default_value,
        alias,
    })
}

/// Extracts function argument expressions as strings.
fn extract_function_args(func: &sqlparser::ast::Function) -> Vec<String> {
    match &func.args {
        sqlparser::ast::FunctionArguments::List(list) => list
            .args
            .iter()
            .filter_map(|arg| match arg {
                sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(
                    expr,
                )) => Some(expr_to_string(expr)),
                _ => None,
            })
            .collect(),
        _ => vec![],
    }
}

/// Converts an expression to its string representation.
fn expr_to_string(expr: &Expr) -> String {
    match expr {
        Expr::Identifier(ident) => ident.value.clone(),
        Expr::CompoundIdentifier(parts) => parts.last().map_or(String::new(), |p| p.value.clone()),
        Expr::Value(value_with_span) => match &value_with_span.value {
            sqlparser::ast::Value::Number(n, _) => n.clone(),
            sqlparser::ast::Value::SingleQuotedString(s) => s.clone(),
            sqlparser::ast::Value::Null => "NULL".to_string(),
            _ => format!("{}", value_with_span.value),
        },
        Expr::UnaryOp {
            op: sqlparser::ast::UnaryOperator::Minus,
            expr: inner,
        } => format!("-{}", expr_to_string(inner)),
        _ => expr.to_string(),
    }
}

/// Extracts a simple column name from an expression.
fn extract_column_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Identifier(ident) => Some(ident.value.clone()),
        Expr::CompoundIdentifier(parts) => parts.last().map(|p| p.value.clone()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    fn parse_stmt(sql: &str) -> Statement {
        let dialect = GenericDialect {};
        let mut stmts = Parser::parse_sql(&dialect, sql).unwrap();
        stmts.remove(0)
    }

    #[test]
    fn test_lag_basic() {
        let sql = "SELECT price, LAG(price) OVER (ORDER BY ts) AS prev_price FROM trades";
        let stmt = parse_stmt(sql);
        let analysis = analyze_analytic_functions(&stmt).unwrap();
        assert_eq!(analysis.functions.len(), 1);
        assert_eq!(
            analysis.functions[0].function_type,
            AnalyticFunctionType::Lag
        );
        assert_eq!(analysis.functions[0].column, "price");
        assert_eq!(analysis.functions[0].offset, 1);
        assert_eq!(analysis.functions[0].alias.as_deref(), Some("prev_price"));
    }

    #[test]
    fn test_lag_with_offset() {
        let sql = "SELECT LAG(price, 3) OVER (ORDER BY ts) AS prev3 FROM trades";
        let stmt = parse_stmt(sql);
        let analysis = analyze_analytic_functions(&stmt).unwrap();
        assert_eq!(analysis.functions[0].offset, 3);
    }

    #[test]
    fn test_lag_with_default() {
        let sql = "SELECT LAG(price, 1, 0) OVER (ORDER BY ts) AS prev FROM trades";
        let stmt = parse_stmt(sql);
        let analysis = analyze_analytic_functions(&stmt).unwrap();
        assert_eq!(analysis.functions[0].offset, 1);
        assert_eq!(analysis.functions[0].default_value.as_deref(), Some("0"));
    }

    #[test]
    fn test_lead_basic() {
        let sql = "SELECT LEAD(price) OVER (ORDER BY ts) AS next_price FROM trades";
        let stmt = parse_stmt(sql);
        let analysis = analyze_analytic_functions(&stmt).unwrap();
        assert_eq!(
            analysis.functions[0].function_type,
            AnalyticFunctionType::Lead
        );
        assert!(analysis.has_lookahead());
    }

    #[test]
    fn test_lead_with_offset_and_default() {
        let sql = "SELECT LEAD(price, 2, -1) OVER (ORDER BY ts) AS next2 FROM trades";
        let stmt = parse_stmt(sql);
        let analysis = analyze_analytic_functions(&stmt).unwrap();
        assert_eq!(analysis.functions[0].offset, 2);
        assert_eq!(analysis.functions[0].default_value.as_deref(), Some("-1"));
    }

    #[test]
    fn test_partition_by_extraction() {
        let sql = "SELECT symbol, LAG(price) OVER (PARTITION BY symbol ORDER BY ts) FROM trades";
        let stmt = parse_stmt(sql);
        let analysis = analyze_analytic_functions(&stmt).unwrap();
        assert_eq!(analysis.partition_columns, vec!["symbol".to_string()]);
        assert_eq!(analysis.order_columns, vec!["ts".to_string()]);
    }

    #[test]
    fn test_multiple_analytic_functions() {
        let sql = "SELECT
            LAG(price) OVER (ORDER BY ts) AS prev,
            LEAD(price) OVER (ORDER BY ts) AS next
            FROM trades";
        let stmt = parse_stmt(sql);
        let analysis = analyze_analytic_functions(&stmt).unwrap();
        assert_eq!(analysis.functions.len(), 2);
        assert_eq!(
            analysis.functions[0].function_type,
            AnalyticFunctionType::Lag
        );
        assert_eq!(
            analysis.functions[1].function_type,
            AnalyticFunctionType::Lead
        );
    }

    #[test]
    fn test_first_value() {
        let sql =
            "SELECT FIRST_VALUE(price) OVER (PARTITION BY symbol ORDER BY ts) AS first FROM trades";
        let stmt = parse_stmt(sql);
        let analysis = analyze_analytic_functions(&stmt).unwrap();
        assert_eq!(
            analysis.functions[0].function_type,
            AnalyticFunctionType::FirstValue
        );
        assert_eq!(analysis.functions[0].column, "price");
    }

    #[test]
    fn test_last_value() {
        let sql = "SELECT LAST_VALUE(price) OVER (ORDER BY ts) FROM trades";
        let stmt = parse_stmt(sql);
        let analysis = analyze_analytic_functions(&stmt).unwrap();
        assert_eq!(
            analysis.functions[0].function_type,
            AnalyticFunctionType::LastValue
        );
    }

    #[test]
    fn test_no_analytic_functions() {
        let sql = "SELECT price, volume FROM trades WHERE price > 100";
        let stmt = parse_stmt(sql);
        assert!(analyze_analytic_functions(&stmt).is_none());
    }

    #[test]
    fn test_max_offset() {
        let sql = "SELECT
            LAG(price, 1) OVER (ORDER BY ts) AS p1,
            LAG(price, 5) OVER (ORDER BY ts) AS p5,
            LEAD(price, 3) OVER (ORDER BY ts) AS n3
            FROM trades";
        let stmt = parse_stmt(sql);
        let analysis = analyze_analytic_functions(&stmt).unwrap();
        assert_eq!(analysis.max_offset(), 5);
    }
}
