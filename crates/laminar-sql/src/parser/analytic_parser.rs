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

// --- Window Frame types (F-SQL-006) ---

/// Types of aggregate functions used with window frames.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WindowFrameFunction {
    /// AVG(col) OVER (... ROWS BETWEEN ...)
    Avg,
    /// SUM(col) OVER (... ROWS BETWEEN ...)
    Sum,
    /// MIN(col) OVER (... ROWS BETWEEN ...)
    Min,
    /// MAX(col) OVER (... ROWS BETWEEN ...)
    Max,
    /// COUNT(*) OVER (... ROWS BETWEEN ...)
    Count,
    /// FIRST_VALUE(col) OVER (... ROWS BETWEEN ...)
    FirstValue,
    /// LAST_VALUE(col) OVER (... ROWS BETWEEN ...)
    LastValue,
}

impl WindowFrameFunction {
    /// Returns the function name as used in SQL.
    #[must_use]
    pub fn sql_name(&self) -> &'static str {
        match self {
            Self::Avg => "AVG",
            Self::Sum => "SUM",
            Self::Min => "MIN",
            Self::Max => "MAX",
            Self::Count => "COUNT",
            Self::FirstValue => "FIRST_VALUE",
            Self::LastValue => "LAST_VALUE",
        }
    }
}

/// Window frame unit type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FrameUnits {
    /// ROWS BETWEEN — physical row offsets
    Rows,
    /// RANGE BETWEEN — logical value range
    Range,
}

/// A single bound in a window frame specification.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum FrameBound {
    /// UNBOUNDED PRECEDING
    UnboundedPreceding,
    /// N PRECEDING
    Preceding(u64),
    /// CURRENT ROW
    CurrentRow,
    /// N FOLLOWING
    Following(u64),
    /// UNBOUNDED FOLLOWING
    UnboundedFollowing,
}

/// Information about a single window frame function call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WindowFrameInfo {
    /// Type of aggregate function
    pub function_type: WindowFrameFunction,
    /// Column being aggregated (or "*" for COUNT(*))
    pub column: String,
    /// Frame unit type (ROWS or RANGE)
    pub units: FrameUnits,
    /// Start bound of the frame
    pub start_bound: FrameBound,
    /// End bound of the frame
    pub end_bound: FrameBound,
    /// Output alias (AS name)
    pub alias: Option<String>,
}

/// Result of analyzing window frame functions in a query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WindowFrameAnalysis {
    /// Window frame functions found in the query
    pub functions: Vec<WindowFrameInfo>,
    /// PARTITION BY columns from the OVER clause
    pub partition_columns: Vec<String>,
    /// ORDER BY columns from the OVER clause
    pub order_columns: Vec<String>,
}

impl WindowFrameAnalysis {
    /// Returns true if any frame uses FOLLOWING bounds.
    #[must_use]
    pub fn has_following(&self) -> bool {
        self.functions.iter().any(|f| {
            matches!(
                f.end_bound,
                FrameBound::Following(_) | FrameBound::UnboundedFollowing
            ) || matches!(
                f.start_bound,
                FrameBound::Following(_) | FrameBound::UnboundedFollowing
            )
        })
    }

    /// Returns the maximum preceding offset across all functions.
    #[must_use]
    pub fn max_preceding(&self) -> u64 {
        self.functions
            .iter()
            .filter_map(|f| match &f.start_bound {
                FrameBound::Preceding(n) => Some(*n),
                _ => None,
            })
            .max()
            .unwrap_or(0)
    }
}

/// Analyzes a SQL statement for window frame aggregate functions.
///
/// Walks SELECT items looking for aggregate functions (AVG, SUM, MIN, MAX,
/// COUNT, FIRST_VALUE, LAST_VALUE) with OVER clauses that contain explicit
/// ROWS/RANGE frame specifications. Returns `None` if no such functions
/// are found.
///
/// This is distinct from `analyze_analytic_functions()` which handles
/// per-row offset functions (LAG/LEAD). Window frame functions compute
/// aggregates over a sliding frame of rows.
#[must_use]
pub fn analyze_window_frames(stmt: &Statement) -> Option<WindowFrameAnalysis> {
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

        if let Some(info) = extract_window_frame_function(expr, alias, &mut |spec| {
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

    Some(WindowFrameAnalysis {
        functions,
        partition_columns,
        order_columns,
    })
}

/// Extracts a window frame aggregate function from an expression.
fn extract_window_frame_function(
    expr: &Expr,
    alias: Option<String>,
    on_window_spec: &mut dyn FnMut(&sqlparser::ast::WindowSpec),
) -> Option<WindowFrameInfo> {
    let Expr::Function(func) = expr else {
        return None;
    };

    let name = func.name.to_string().to_uppercase();
    let function_type = match name.as_str() {
        "AVG" => WindowFrameFunction::Avg,
        "SUM" => WindowFrameFunction::Sum,
        "MIN" => WindowFrameFunction::Min,
        "MAX" => WindowFrameFunction::Max,
        "COUNT" => WindowFrameFunction::Count,
        "FIRST_VALUE" => WindowFrameFunction::FirstValue,
        "LAST_VALUE" => WindowFrameFunction::LastValue,
        _ => return None,
    };

    // Must have an OVER clause with an explicit window frame
    let window_type = func.over.as_ref()?;
    let spec = match window_type {
        sqlparser::ast::WindowType::WindowSpec(spec) => spec,
        sqlparser::ast::WindowType::NamedWindow(_) => return None,
    };

    // Only match functions with explicit ROWS/RANGE frame specs
    let frame = spec.window_frame.as_ref()?;

    on_window_spec(spec);

    let units = match frame.units {
        sqlparser::ast::WindowFrameUnits::Rows => FrameUnits::Rows,
        sqlparser::ast::WindowFrameUnits::Range => FrameUnits::Range,
        sqlparser::ast::WindowFrameUnits::Groups => return None,
    };

    let start_bound = convert_frame_bound(&frame.start_bound);
    let end_bound = frame
        .end_bound
        .as_ref()
        .map_or(FrameBound::CurrentRow, convert_frame_bound);

    // Extract the column argument
    let args = extract_function_args(func);
    let column = args.first().cloned().unwrap_or_else(|| "*".to_string());

    Some(WindowFrameInfo {
        function_type,
        column,
        units,
        start_bound,
        end_bound,
        alias,
    })
}

/// Converts a sqlparser `WindowFrameBound` to our `FrameBound`.
fn convert_frame_bound(bound: &sqlparser::ast::WindowFrameBound) -> FrameBound {
    match bound {
        sqlparser::ast::WindowFrameBound::CurrentRow => FrameBound::CurrentRow,
        sqlparser::ast::WindowFrameBound::Preceding(None) => FrameBound::UnboundedPreceding,
        sqlparser::ast::WindowFrameBound::Preceding(Some(expr)) => {
            let n = expr_to_u64(expr).unwrap_or(0);
            FrameBound::Preceding(n)
        }
        sqlparser::ast::WindowFrameBound::Following(None) => FrameBound::UnboundedFollowing,
        sqlparser::ast::WindowFrameBound::Following(Some(expr)) => {
            let n = expr_to_u64(expr).unwrap_or(0);
            FrameBound::Following(n)
        }
    }
}

/// Extracts a u64 value from an expression (numeric literal).
fn expr_to_u64(expr: &Expr) -> Option<u64> {
    match expr {
        Expr::Value(value_with_span) => match &value_with_span.value {
            sqlparser::ast::Value::Number(n, _) => n.parse().ok(),
            _ => None,
        },
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

    // --- Window Frame tests (F-SQL-006) ---

    #[test]
    fn test_frame_rows_preceding_current() {
        let sql = "SELECT AVG(price) OVER (ORDER BY ts \
                    ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS ma \
                    FROM trades";
        let stmt = parse_stmt(sql);
        let analysis = analyze_window_frames(&stmt).unwrap();
        assert_eq!(analysis.functions.len(), 1);
        assert_eq!(
            analysis.functions[0].function_type,
            WindowFrameFunction::Avg
        );
        assert_eq!(analysis.functions[0].column, "price");
        assert_eq!(analysis.functions[0].units, FrameUnits::Rows);
        assert_eq!(analysis.functions[0].start_bound, FrameBound::Preceding(9));
        assert_eq!(analysis.functions[0].end_bound, FrameBound::CurrentRow);
        assert_eq!(analysis.functions[0].alias.as_deref(), Some("ma"));
    }

    #[test]
    fn test_frame_rows_preceding_following() {
        let sql = "SELECT SUM(amount) OVER (ORDER BY id \
                    ROWS BETWEEN 5 PRECEDING AND 3 FOLLOWING) AS total \
                    FROM orders";
        let stmt = parse_stmt(sql);
        let analysis = analyze_window_frames(&stmt).unwrap();
        assert_eq!(
            analysis.functions[0].function_type,
            WindowFrameFunction::Sum
        );
        assert_eq!(analysis.functions[0].start_bound, FrameBound::Preceding(5));
        assert_eq!(analysis.functions[0].end_bound, FrameBound::Following(3));
    }

    #[test]
    fn test_frame_unbounded_preceding_running_sum() {
        let sql = "SELECT SUM(amount) OVER (ORDER BY id \
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running \
                    FROM orders";
        let stmt = parse_stmt(sql);
        let analysis = analyze_window_frames(&stmt).unwrap();
        assert_eq!(
            analysis.functions[0].start_bound,
            FrameBound::UnboundedPreceding
        );
        assert_eq!(analysis.functions[0].end_bound, FrameBound::CurrentRow);
    }

    #[test]
    fn test_frame_range_units() {
        let sql = "SELECT AVG(price) OVER (ORDER BY ts \
                    RANGE BETWEEN 10 PRECEDING AND CURRENT ROW) AS ra \
                    FROM trades";
        let stmt = parse_stmt(sql);
        let analysis = analyze_window_frames(&stmt).unwrap();
        assert_eq!(analysis.functions[0].units, FrameUnits::Range);
        assert_eq!(analysis.functions[0].start_bound, FrameBound::Preceding(10));
    }

    #[test]
    fn test_frame_partition_order_columns() {
        let sql = "SELECT AVG(price) OVER (PARTITION BY symbol ORDER BY ts \
                    ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS ma \
                    FROM trades";
        let stmt = parse_stmt(sql);
        let analysis = analyze_window_frames(&stmt).unwrap();
        assert_eq!(analysis.partition_columns, vec!["symbol".to_string()]);
        assert_eq!(analysis.order_columns, vec!["ts".to_string()]);
    }

    #[test]
    fn test_frame_multiple_functions() {
        let sql = "SELECT \
                    AVG(price) OVER (ORDER BY ts ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS ma, \
                    SUM(volume) OVER (ORDER BY ts ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS sv \
                    FROM trades";
        let stmt = parse_stmt(sql);
        let analysis = analyze_window_frames(&stmt).unwrap();
        assert_eq!(analysis.functions.len(), 2);
        assert_eq!(
            analysis.functions[0].function_type,
            WindowFrameFunction::Avg
        );
        assert_eq!(analysis.functions[0].column, "price");
        assert_eq!(
            analysis.functions[1].function_type,
            WindowFrameFunction::Sum
        );
        assert_eq!(analysis.functions[1].column, "volume");
    }

    #[test]
    fn test_frame_no_frame_returns_none() {
        // AVG with OVER but no explicit frame → None
        let sql = "SELECT AVG(price) OVER (ORDER BY ts) FROM trades";
        let stmt = parse_stmt(sql);
        assert!(analyze_window_frames(&stmt).is_none());
    }

    #[test]
    fn test_frame_unbounded_following() {
        let sql = "SELECT SUM(amount) OVER (ORDER BY id \
                    ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS rest \
                    FROM orders";
        let stmt = parse_stmt(sql);
        let analysis = analyze_window_frames(&stmt).unwrap();
        assert_eq!(analysis.functions[0].start_bound, FrameBound::CurrentRow);
        assert_eq!(
            analysis.functions[0].end_bound,
            FrameBound::UnboundedFollowing
        );
        assert!(analysis.has_following());
    }

    #[test]
    fn test_frame_all_function_types() {
        let sql = "SELECT \
                    AVG(a) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS f1, \
                    SUM(b) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS f2, \
                    MIN(c) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS f3, \
                    MAX(d) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS f4, \
                    COUNT(e) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS f5 \
                    FROM t";
        let stmt = parse_stmt(sql);
        let analysis = analyze_window_frames(&stmt).unwrap();
        assert_eq!(analysis.functions.len(), 5);
        assert_eq!(
            analysis.functions[0].function_type,
            WindowFrameFunction::Avg
        );
        assert_eq!(
            analysis.functions[1].function_type,
            WindowFrameFunction::Sum
        );
        assert_eq!(
            analysis.functions[2].function_type,
            WindowFrameFunction::Min
        );
        assert_eq!(
            analysis.functions[3].function_type,
            WindowFrameFunction::Max
        );
        assert_eq!(
            analysis.functions[4].function_type,
            WindowFrameFunction::Count
        );
    }

    #[test]
    fn test_frame_max_preceding_helper() {
        let sql = "SELECT \
                    AVG(a) OVER (ORDER BY id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS f1, \
                    SUM(b) OVER (ORDER BY id ROWS BETWEEN 10 PRECEDING AND CURRENT ROW) AS f2 \
                    FROM t";
        let stmt = parse_stmt(sql);
        let analysis = analyze_window_frames(&stmt).unwrap();
        assert_eq!(analysis.max_preceding(), 10);
        assert!(!analysis.has_following());
    }
}
