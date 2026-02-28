//! Subscription filtering with Ring 0 predicate push-down.
//!
//! Provides predicate expressions that filter events before they reach
//! subscribers. Lightweight predicates (column equality, range checks) are
//! pushed to Ring 0 for zero-allocation evaluation. Complex predicates are
//! evaluated in Ring 1.
//!
//! # Ring 0 Predicates
//!
//! Simple column-vs-literal comparisons that can be evaluated with no heap
//! allocation in < 50ns:
//! - Equality / inequality: `price = 100.0`, `symbol != 'AAPL'`
//! - Range: `price > 100.0`, `qty BETWEEN 10 AND 20`
//! - Null checks: `name IS NOT NULL`
//! - Small IN sets: `status IN ('A', 'B', 'C')` (max 8 values)
//!
//! # Ring 1 Predicates
//!
//! Complex expressions that may allocate, evaluated in the Ring 1 dispatcher:
//! - Multi-column: `price > min_price`
//! - Function calls: `UPPER(name) = 'FOO'`
//! - OR/NOT: `price > 100 OR qty > 50`
//! - Pattern matching: `name LIKE '%foo%'`
#![deny(clippy::disallowed_types)]

use arrow_array::{BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::SchemaRef;
use rustc_hash::FxHashMap;
use smallvec::SmallVec;

// ---------------------------------------------------------------------------
// ScalarValue
// ---------------------------------------------------------------------------

/// Scalar value for Ring 0 predicate comparison.
///
/// Fixed-size, stack-allocated, no heap. The `StringIndex` variant
/// uses a pre-interned index into the [`StringInternTable`], avoiding
/// heap allocation during evaluation.
#[derive(Debug, Clone, PartialEq)]
pub enum ScalarValue {
    /// 64-bit signed integer.
    Int64(i64),
    /// 64-bit floating point.
    Float64(f64),
    /// Boolean.
    Bool(bool),
    /// Interned string index (not the string itself).
    ///
    /// At filter compile time, string literals are interned into a
    /// [`StringInternTable`]. At evaluation time, the column's string
    /// value is compared against the resolved string. Comparison is
    /// O(n) on string length but avoids allocation.
    StringIndex(u32),
}

// ---------------------------------------------------------------------------
// StringInternTable
// ---------------------------------------------------------------------------

/// Intern table for string values used in Ring 0 predicates.
///
/// Built at filter compilation time (Ring 2), immutable during evaluation.
/// Shared across all predicates for a subscription.
#[derive(Debug, Clone)]
pub struct StringInternTable {
    /// String to index mapping (used at compile time).
    forward: FxHashMap<String, u32>,
    /// Index to string mapping (used at evaluation time).
    reverse: Vec<String>,
}

impl StringInternTable {
    /// Creates a new empty intern table.
    #[must_use]
    pub fn new() -> Self {
        Self {
            forward: FxHashMap::default(),
            reverse: Vec::new(),
        }
    }

    /// Interns a string, returning its index.
    ///
    /// Returns the existing index if the string was already interned.
    pub fn intern(&mut self, s: &str) -> u32 {
        if let Some(&idx) = self.forward.get(s) {
            return idx;
        }
        #[allow(clippy::cast_possible_truncation)]
        let idx = self.reverse.len() as u32;
        self.forward.insert(s.to_string(), idx);
        self.reverse.push(s.to_string());
        idx
    }

    /// Looks up a string by its intern index.
    #[must_use]
    pub fn resolve(&self, idx: u32) -> Option<&str> {
        self.reverse.get(idx as usize).map(String::as_str)
    }

    /// Returns the number of interned strings.
    #[must_use]
    pub fn len(&self) -> usize {
        self.reverse.len()
    }

    /// Returns `true` if no strings have been interned.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.reverse.is_empty()
    }
}

impl Default for StringInternTable {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Ring0Predicate
// ---------------------------------------------------------------------------

/// Ring 0 predicate (must be zero-allocation, < 50ns).
///
/// Supports column-level checks that can be evaluated with no heap allocation.
/// Complex expressions (AND/OR, multi-column, UDFs) are deferred to Ring 1.
#[derive(Debug, Clone)]
pub enum Ring0Predicate {
    /// Column equals a constant value: `column = value`
    Eq {
        /// Column index in the `RecordBatch` schema.
        column_index: usize,
        /// The constant value to compare against.
        value: ScalarValue,
    },
    /// Column not equal: `column != value` / `column <> value`
    NotEq {
        /// Column index in the `RecordBatch` schema.
        column_index: usize,
        /// The constant value to compare against.
        value: ScalarValue,
    },
    /// Column greater than: `column > value`
    Gt {
        /// Column index in the `RecordBatch` schema.
        column_index: usize,
        /// The constant value to compare against.
        value: ScalarValue,
    },
    /// Column greater than or equal: `column >= value`
    GtEq {
        /// Column index in the `RecordBatch` schema.
        column_index: usize,
        /// The constant value to compare against.
        value: ScalarValue,
    },
    /// Column less than: `column < value`
    Lt {
        /// Column index in the `RecordBatch` schema.
        column_index: usize,
        /// The constant value to compare against.
        value: ScalarValue,
    },
    /// Column less than or equal: `column <= value`
    LtEq {
        /// Column index in the `RecordBatch` schema.
        column_index: usize,
        /// The constant value to compare against.
        value: ScalarValue,
    },
    /// Column is between two values (inclusive): `column BETWEEN low AND high`
    Between {
        /// Column index in the `RecordBatch` schema.
        column_index: usize,
        /// Lower bound (inclusive).
        low: ScalarValue,
        /// Upper bound (inclusive).
        high: ScalarValue,
    },
    /// Column is null: `column IS NULL`
    IsNull {
        /// Column index in the `RecordBatch` schema.
        column_index: usize,
    },
    /// Column is not null: `column IS NOT NULL`
    IsNotNull {
        /// Column index in the `RecordBatch` schema.
        column_index: usize,
    },
    /// Column value is in a set: `column IN (v1, v2, ...)`
    ///
    /// Limited to 8 values to stay on the stack via [`SmallVec`].
    In {
        /// Column index in the `RecordBatch` schema.
        column_index: usize,
        /// Fixed-capacity inline set. Max 8 values to stay on stack.
        values: SmallVec<[ScalarValue; 8]>,
    },
}

impl Ring0Predicate {
    /// Evaluates the predicate against a single row in a `RecordBatch`.
    ///
    /// Returns `true` if the row matches the predicate.
    /// This runs on the Ring 0 hot path.
    #[inline]
    #[must_use]
    pub fn evaluate(&self, batch: &RecordBatch, row: usize, intern: &StringInternTable) -> bool {
        match self {
            Ring0Predicate::Eq {
                column_index,
                value,
            } => compare_eq(batch, *column_index, row, value, intern),
            Ring0Predicate::NotEq {
                column_index,
                value,
            } => !compare_eq(batch, *column_index, row, value, intern),
            Ring0Predicate::Gt {
                column_index,
                value,
            } => compare_ord(
                batch,
                *column_index,
                row,
                value,
                std::cmp::Ordering::Greater,
            ),
            Ring0Predicate::GtEq {
                column_index,
                value,
            } => {
                let ord = compare_ord_raw(batch, *column_index, row, value);
                matches!(
                    ord,
                    Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
                )
            }
            Ring0Predicate::Lt {
                column_index,
                value,
            } => compare_ord(batch, *column_index, row, value, std::cmp::Ordering::Less),
            Ring0Predicate::LtEq {
                column_index,
                value,
            } => {
                let ord = compare_ord_raw(batch, *column_index, row, value);
                matches!(
                    ord,
                    Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
                )
            }
            Ring0Predicate::Between {
                column_index,
                low,
                high,
            } => {
                let ge_low = matches!(
                    compare_ord_raw(batch, *column_index, row, low),
                    Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
                );
                let le_high = matches!(
                    compare_ord_raw(batch, *column_index, row, high),
                    Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
                );
                ge_low && le_high
            }
            Ring0Predicate::IsNull { column_index } => batch.column(*column_index).is_null(row),
            Ring0Predicate::IsNotNull { column_index } => batch.column(*column_index).is_valid(row),
            Ring0Predicate::In {
                column_index,
                values,
            } => values
                .iter()
                .any(|v| compare_eq(batch, *column_index, row, v, intern)),
        }
    }
}

/// Compares a batch column value at `row` for equality with a `ScalarValue`.
#[inline]
fn compare_eq(
    batch: &RecordBatch,
    col: usize,
    row: usize,
    value: &ScalarValue,
    intern: &StringInternTable,
) -> bool {
    let column = batch.column(col);
    match value {
        ScalarValue::Int64(v) => column
            .as_any()
            .downcast_ref::<Int64Array>()
            .is_some_and(|a| a.value(row) == *v),
        ScalarValue::Float64(v) => column
            .as_any()
            .downcast_ref::<Float64Array>()
            .is_some_and(|a| (a.value(row) - *v).abs() < f64::EPSILON),
        ScalarValue::Bool(v) => column
            .as_any()
            .downcast_ref::<BooleanArray>()
            .is_some_and(|a| a.value(row) == *v),
        ScalarValue::StringIndex(idx) => {
            if let Some(expected) = intern.resolve(*idx) {
                column
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .is_some_and(|a| a.value(row) == expected)
            } else {
                false
            }
        }
    }
}

/// Returns the ordering of a batch column value at `row` relative to a `ScalarValue`.
#[inline]
fn compare_ord_raw(
    batch: &RecordBatch,
    col: usize,
    row: usize,
    value: &ScalarValue,
) -> Option<std::cmp::Ordering> {
    let column = batch.column(col);
    match value {
        ScalarValue::Int64(v) => column
            .as_any()
            .downcast_ref::<Int64Array>()
            .map(|a| a.value(row).cmp(v)),
        ScalarValue::Float64(v) => column
            .as_any()
            .downcast_ref::<Float64Array>()
            .and_then(|a| a.value(row).partial_cmp(v)),
        ScalarValue::Bool(_) | ScalarValue::StringIndex(_) => None,
    }
}

/// Checks if a batch column value at `row` has the expected ordering vs a `ScalarValue`.
#[inline]
fn compare_ord(
    batch: &RecordBatch,
    col: usize,
    row: usize,
    value: &ScalarValue,
    expected: std::cmp::Ordering,
) -> bool {
    compare_ord_raw(batch, col, row, value) == Some(expected)
}

// ---------------------------------------------------------------------------
// Ring1Predicate
// ---------------------------------------------------------------------------

/// Ring 1 predicate (can allocate, evaluated in Ring 1 dispatcher).
#[derive(Debug, Clone)]
pub enum Ring1Predicate {
    /// SQL expression string to be evaluated against a `RecordBatch`.
    Expression(String),
}

// ---------------------------------------------------------------------------
// SubscriptionFilter
// ---------------------------------------------------------------------------

/// Compiled subscription filter.
///
/// Simple predicates are pushed to Ring 0 for zero-overhead filtering.
/// Complex predicates are evaluated in Ring 1.
#[derive(Debug, Clone)]
pub struct SubscriptionFilter {
    /// Original filter expression.
    pub expression: String,
    /// Ring 0 predicates (simple, zero-allocation checks).
    pub ring0_predicates: Vec<Ring0Predicate>,
    /// Ring 1 predicates (complex, may allocate).
    pub ring1_predicates: Vec<Ring1Predicate>,
    /// Interned string table for Ring 0 string comparisons.
    pub intern_table: StringInternTable,
}

impl SubscriptionFilter {
    /// Evaluates all Ring 0 predicates against a single row.
    ///
    /// Returns `true` if all predicates match (AND semantics).
    /// Returns `true` if there are no Ring 0 predicates.
    #[inline]
    #[must_use]
    pub fn evaluate_ring0(&self, batch: &RecordBatch, row: usize) -> bool {
        self.ring0_predicates
            .iter()
            .all(|p| p.evaluate(batch, row, &self.intern_table))
    }

    /// Returns `true` if this filter has Ring 1 predicates that need evaluation.
    #[must_use]
    pub fn has_ring1_predicates(&self) -> bool {
        !self.ring1_predicates.is_empty()
    }

    /// Returns `true` if this filter has any predicates at all.
    #[must_use]
    pub fn has_predicates(&self) -> bool {
        !self.ring0_predicates.is_empty() || !self.ring1_predicates.is_empty()
    }
}

// ---------------------------------------------------------------------------
// FilterCompileError
// ---------------------------------------------------------------------------

/// Filter compilation errors.
#[derive(Debug, thiserror::Error)]
pub enum FilterCompileError {
    /// The filter expression could not be parsed.
    #[error("invalid filter expression: {0}")]
    InvalidExpression(String),
    /// A referenced column does not exist in the schema.
    #[error("column not found: {0}")]
    ColumnNotFound(String),
    /// A literal value does not match the column's data type.
    #[error("type mismatch: column {column} is {actual}, expected {expected}")]
    TypeMismatch {
        /// Column name.
        column: String,
        /// Actual value type.
        actual: String,
        /// Expected column type.
        expected: String,
    },
}

// ---------------------------------------------------------------------------
// compile_filter
// ---------------------------------------------------------------------------

/// Compiles a filter expression into Ring 0 and Ring 1 predicates.
///
/// Uses the SQL parser to parse the expression, then classifies each
/// conjunct (AND-separated term) as Ring 0 or Ring 1:
///
/// **Ring 0 eligible** (zero-alloc, < 50ns):
/// - Single column comparison: `price > 100.0`, `symbol = 'AAPL'`
/// - IS NULL / IS NOT NULL: `name IS NOT NULL`
/// - IN with <= 8 literal values: `status IN ('A', 'B', 'C')`
/// - BETWEEN with literal bounds: `price BETWEEN 10.0 AND 20.0`
///
/// **Ring 1 fallback** (may allocate):
/// - Multi-column expressions: `price > min_price`
/// - Function calls: `UPPER(name) = 'FOO'`
/// - OR/NOT expressions: `price > 100 OR qty > 50`
/// - LIKE/regex patterns: `name LIKE '%foo%'`
///
/// # Errors
///
/// Returns [`FilterCompileError::InvalidExpression`] if the expression cannot
/// be parsed by the SQL parser.
pub fn compile_filter(
    expression: &str,
    schema: &SchemaRef,
) -> Result<SubscriptionFilter, FilterCompileError> {
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    let dialect = GenericDialect {};
    let mut parser = Parser::new(&dialect)
        .try_with_sql(expression)
        .map_err(|e| FilterCompileError::InvalidExpression(e.to_string()))?;
    let expr = parser
        .parse_expr()
        .map_err(|e| FilterCompileError::InvalidExpression(e.to_string()))?;

    let mut ring0 = Vec::new();
    let mut ring1 = Vec::new();
    let mut intern_table = StringInternTable::new();

    let conjuncts = flatten_and(expr);

    for conjunct in &conjuncts {
        match classify_predicate(conjunct, schema, &mut intern_table) {
            Some(Classification::Ring0(pred)) => ring0.push(pred),
            Some(Classification::Ring1(pred)) => ring1.push(pred),
            None => {
                ring1.push(Ring1Predicate::Expression(format!("{conjunct}")));
            }
        }
    }

    Ok(SubscriptionFilter {
        expression: expression.to_string(),
        ring0_predicates: ring0,
        ring1_predicates: ring1,
        intern_table,
    })
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

enum Classification {
    Ring0(Ring0Predicate),
    Ring1(Ring1Predicate),
}

/// Flattens a top-level AND expression into a list of conjuncts.
fn flatten_and(expr: sqlparser::ast::Expr) -> Vec<sqlparser::ast::Expr> {
    use sqlparser::ast::{BinaryOperator, Expr};

    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            let mut result = flatten_and(*left);
            result.extend(flatten_and(*right));
            result
        }
        Expr::Nested(inner) => flatten_and(*inner),
        other => vec![other],
    }
}

/// Attempts to classify a predicate expression as Ring 0 or Ring 1.
fn classify_predicate(
    expr: &sqlparser::ast::Expr,
    schema: &SchemaRef,
    intern: &mut StringInternTable,
) -> Option<Classification> {
    use sqlparser::ast::{BinaryOperator, Expr};

    match expr {
        Expr::BinaryOp { left, op, right } => {
            let bin_op = match op {
                BinaryOperator::Eq => Some(BinOp::Eq),
                BinaryOperator::NotEq => Some(BinOp::NotEq),
                BinaryOperator::Gt => Some(BinOp::Gt),
                BinaryOperator::GtEq => Some(BinOp::GtEq),
                BinaryOperator::Lt => Some(BinOp::Lt),
                BinaryOperator::LtEq => Some(BinOp::LtEq),
                _ => None,
            };
            let bin_op = bin_op?;

            // Try column op literal
            if let Some(pred) = try_column_op_literal(left, bin_op, right, schema, intern) {
                return Some(Classification::Ring0(pred));
            }
            // Try literal op column (swap)
            if let Some(pred) = try_column_op_literal(right, bin_op.swap(), left, schema, intern) {
                return Some(Classification::Ring0(pred));
            }
            Some(Classification::Ring1(Ring1Predicate::Expression(format!(
                "{expr}"
            ))))
        }
        Expr::IsNull(inner) => {
            let col_name = extract_column_name(inner)?;
            let col_idx = schema.index_of(&col_name).ok()?;
            Some(Classification::Ring0(Ring0Predicate::IsNull {
                column_index: col_idx,
            }))
        }
        Expr::IsNotNull(inner) => {
            let col_name = extract_column_name(inner)?;
            let col_idx = schema.index_of(&col_name).ok()?;
            Some(Classification::Ring0(Ring0Predicate::IsNotNull {
                column_index: col_idx,
            }))
        }
        Expr::InList {
            expr: inner,
            list,
            negated,
        } => {
            if *negated || list.len() > 8 {
                return Some(Classification::Ring1(Ring1Predicate::Expression(format!(
                    "{expr}"
                ))));
            }
            let col_name = extract_column_name(inner)?;
            let col_idx = schema.index_of(&col_name).ok()?;

            let mut values = SmallVec::new();
            for item in list {
                if let Some(scalar) = expr_to_scalar(item, intern) {
                    values.push(scalar);
                } else {
                    return Some(Classification::Ring1(Ring1Predicate::Expression(format!(
                        "{expr}"
                    ))));
                }
            }
            Some(Classification::Ring0(Ring0Predicate::In {
                column_index: col_idx,
                values,
            }))
        }
        Expr::Between {
            expr: inner,
            negated,
            low,
            high,
        } => {
            if *negated {
                return Some(Classification::Ring1(Ring1Predicate::Expression(format!(
                    "{expr}"
                ))));
            }
            let col_name = extract_column_name(inner)?;
            let col_idx = schema.index_of(&col_name).ok()?;
            let low_scalar = expr_to_scalar(low, intern)?;
            let high_scalar = expr_to_scalar(high, intern)?;
            Some(Classification::Ring0(Ring0Predicate::Between {
                column_index: col_idx,
                low: low_scalar,
                high: high_scalar,
            }))
        }
        _ => Some(Classification::Ring1(Ring1Predicate::Expression(format!(
            "{expr}"
        )))),
    }
}

/// Extracts a column name from an Identifier expression.
fn extract_column_name(expr: &sqlparser::ast::Expr) -> Option<String> {
    use sqlparser::ast::Expr;

    match expr {
        Expr::Identifier(ident) => Some(ident.value.clone()),
        Expr::Nested(inner) => extract_column_name(inner),
        _ => None,
    }
}

/// Converts a literal expression to a `ScalarValue`.
fn expr_to_scalar(
    expr: &sqlparser::ast::Expr,
    intern: &mut StringInternTable,
) -> Option<ScalarValue> {
    use sqlparser::ast::{Expr, Value};

    match expr {
        Expr::Value(value_with_span) => value_to_scalar(&value_with_span.value, intern),
        Expr::UnaryOp {
            op: sqlparser::ast::UnaryOperator::Minus,
            expr: inner,
        } => {
            if let Expr::Value(value_with_span) = inner.as_ref() {
                match &value_with_span.value {
                    Value::Number(n, _) => {
                        if let Ok(v) = n.parse::<i64>() {
                            return Some(ScalarValue::Int64(-v));
                        }
                        if let Ok(v) = n.parse::<f64>() {
                            return Some(ScalarValue::Float64(-v));
                        }
                        None
                    }
                    _ => None,
                }
            } else {
                None
            }
        }
        Expr::Nested(inner) => expr_to_scalar(inner, intern),
        _ => None,
    }
}

/// Converts a SQL `Value` to a `ScalarValue`.
fn value_to_scalar(
    value: &sqlparser::ast::Value,
    intern: &mut StringInternTable,
) -> Option<ScalarValue> {
    use sqlparser::ast::Value;

    match value {
        Value::Number(n, _) => {
            if let Ok(v) = n.parse::<i64>() {
                Some(ScalarValue::Int64(v))
            } else if let Ok(v) = n.parse::<f64>() {
                Some(ScalarValue::Float64(v))
            } else {
                None
            }
        }
        Value::SingleQuotedString(s) => {
            let idx = intern.intern(s);
            Some(ScalarValue::StringIndex(idx))
        }
        Value::Boolean(b) => Some(ScalarValue::Bool(*b)),
        _ => None,
    }
}

/// Internal binary operator classification.
#[derive(Debug, Clone, Copy)]
enum BinOp {
    Eq,
    NotEq,
    Gt,
    GtEq,
    Lt,
    LtEq,
}

impl BinOp {
    /// Swaps the operator for reversed operands (e.g., `100 < price` -> `price > 100`).
    fn swap(self) -> Self {
        match self {
            BinOp::Eq => BinOp::Eq,
            BinOp::NotEq => BinOp::NotEq,
            BinOp::Gt => BinOp::Lt,
            BinOp::GtEq => BinOp::LtEq,
            BinOp::Lt => BinOp::Gt,
            BinOp::LtEq => BinOp::GtEq,
        }
    }
}

/// Tries to build a Ring 0 predicate from `column op literal`.
fn try_column_op_literal(
    col_expr: &sqlparser::ast::Expr,
    op: BinOp,
    val_expr: &sqlparser::ast::Expr,
    schema: &SchemaRef,
    intern: &mut StringInternTable,
) -> Option<Ring0Predicate> {
    let col_name = extract_column_name(col_expr)?;
    let col_idx = schema.index_of(&col_name).ok()?;
    let scalar = expr_to_scalar(val_expr, intern)?;

    Some(match op {
        BinOp::Eq => Ring0Predicate::Eq {
            column_index: col_idx,
            value: scalar,
        },
        BinOp::NotEq => Ring0Predicate::NotEq {
            column_index: col_idx,
            value: scalar,
        },
        BinOp::Gt => Ring0Predicate::Gt {
            column_index: col_idx,
            value: scalar,
        },
        BinOp::GtEq => Ring0Predicate::GtEq {
            column_index: col_idx,
            value: scalar,
        },
        BinOp::Lt => Ring0Predicate::Lt {
            column_index: col_idx,
            value: scalar,
        },
        BinOp::LtEq => Ring0Predicate::LtEq {
            column_index: col_idx,
            value: scalar,
        },
    })
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, true),
            Field::new("price", DataType::Float64, false),
            Field::new("qty", DataType::Int64, false),
        ]))
    }

    fn make_batch(symbol: &str, price: f64, qty: i64) -> RecordBatch {
        let schema = test_schema();
        let sym_arr = Arc::new(StringArray::from(vec![symbol]));
        let price_arr = Arc::new(Float64Array::from(vec![price]));
        let qty_arr = Arc::new(Int64Array::from(vec![qty]));
        RecordBatch::try_new(schema, vec![sym_arr, price_arr, qty_arr]).unwrap()
    }

    fn make_nullable_batch(symbol: Option<&str>, price: f64, qty: i64) -> RecordBatch {
        let schema = test_schema();
        let sym_arr = Arc::new(StringArray::from(vec![symbol]));
        let price_arr = Arc::new(Float64Array::from(vec![price]));
        let qty_arr = Arc::new(Int64Array::from(vec![qty]));
        RecordBatch::try_new(schema, vec![sym_arr, price_arr, qty_arr]).unwrap()
    }

    // --- String intern tests ---

    #[test]
    fn test_string_intern_basic() {
        let mut table = StringInternTable::new();
        let idx = table.intern("hello");
        assert_eq!(idx, 0);
        assert_eq!(table.resolve(idx), Some("hello"));
        assert_eq!(table.len(), 1);
    }

    #[test]
    fn test_string_intern_dedup() {
        let mut table = StringInternTable::new();
        let idx1 = table.intern("hello");
        let idx2 = table.intern("hello");
        assert_eq!(idx1, idx2);
        assert_eq!(table.len(), 1);
    }

    #[test]
    fn test_string_intern_multiple() {
        let mut table = StringInternTable::new();
        let a = table.intern("a");
        let b = table.intern("b");
        let c = table.intern("c");
        assert_eq!(a, 0);
        assert_eq!(b, 1);
        assert_eq!(c, 2);
        assert_eq!(table.len(), 3);
        assert_eq!(table.resolve(0), Some("a"));
        assert_eq!(table.resolve(1), Some("b"));
        assert_eq!(table.resolve(2), Some("c"));
        assert_eq!(table.resolve(3), None);
    }

    // --- Ring 0 predicate tests ---

    #[test]
    fn test_ring0_predicate_eq_int64() {
        let intern = StringInternTable::new();
        let pred = Ring0Predicate::Eq {
            column_index: 2,
            value: ScalarValue::Int64(100),
        };
        assert!(pred.evaluate(&make_batch("AAPL", 150.0, 100), 0, &intern));
        assert!(!pred.evaluate(&make_batch("AAPL", 150.0, 99), 0, &intern));
    }

    #[test]
    fn test_ring0_predicate_eq_float64() {
        let intern = StringInternTable::new();
        let pred = Ring0Predicate::Eq {
            column_index: 1,
            value: ScalarValue::Float64(150.5),
        };
        assert!(pred.evaluate(&make_batch("AAPL", 150.5, 10), 0, &intern));
        assert!(!pred.evaluate(&make_batch("AAPL", 150.6, 10), 0, &intern));
    }

    #[test]
    fn test_ring0_predicate_eq_string() {
        let mut intern = StringInternTable::new();
        let idx = intern.intern("AAPL");
        let pred = Ring0Predicate::Eq {
            column_index: 0,
            value: ScalarValue::StringIndex(idx),
        };
        assert!(pred.evaluate(&make_batch("AAPL", 150.0, 10), 0, &intern));
        assert!(!pred.evaluate(&make_batch("GOOG", 150.0, 10), 0, &intern));
    }

    #[test]
    fn test_ring0_predicate_not_eq() {
        let intern = StringInternTable::new();
        let pred = Ring0Predicate::NotEq {
            column_index: 2,
            value: ScalarValue::Int64(100),
        };
        assert!(pred.evaluate(&make_batch("AAPL", 150.0, 99), 0, &intern));
        assert!(!pred.evaluate(&make_batch("AAPL", 150.0, 100), 0, &intern));
    }

    #[test]
    fn test_ring0_predicate_gt() {
        let intern = StringInternTable::new();
        let pred = Ring0Predicate::Gt {
            column_index: 1,
            value: ScalarValue::Float64(100.0),
        };
        assert!(pred.evaluate(&make_batch("AAPL", 150.0, 10), 0, &intern));
        assert!(!pred.evaluate(&make_batch("AAPL", 100.0, 10), 0, &intern));
        assert!(!pred.evaluate(&make_batch("AAPL", 50.0, 10), 0, &intern));
    }

    #[test]
    fn test_ring0_predicate_gt_eq() {
        let intern = StringInternTable::new();
        let pred = Ring0Predicate::GtEq {
            column_index: 2,
            value: ScalarValue::Int64(10),
        };
        assert!(pred.evaluate(&make_batch("AAPL", 150.0, 10), 0, &intern));
        assert!(pred.evaluate(&make_batch("AAPL", 150.0, 11), 0, &intern));
        assert!(!pred.evaluate(&make_batch("AAPL", 150.0, 9), 0, &intern));
    }

    #[test]
    fn test_ring0_predicate_lt() {
        let intern = StringInternTable::new();
        let pred = Ring0Predicate::Lt {
            column_index: 1,
            value: ScalarValue::Float64(100.0),
        };
        assert!(pred.evaluate(&make_batch("AAPL", 50.0, 10), 0, &intern));
        assert!(!pred.evaluate(&make_batch("AAPL", 100.0, 10), 0, &intern));
    }

    #[test]
    fn test_ring0_predicate_lt_eq() {
        let intern = StringInternTable::new();
        let pred = Ring0Predicate::LtEq {
            column_index: 2,
            value: ScalarValue::Int64(10),
        };
        assert!(pred.evaluate(&make_batch("AAPL", 150.0, 10), 0, &intern));
        assert!(pred.evaluate(&make_batch("AAPL", 150.0, 9), 0, &intern));
        assert!(!pred.evaluate(&make_batch("AAPL", 150.0, 11), 0, &intern));
    }

    #[test]
    fn test_ring0_predicate_between() {
        let intern = StringInternTable::new();
        let pred = Ring0Predicate::Between {
            column_index: 1,
            low: ScalarValue::Float64(100.0),
            high: ScalarValue::Float64(200.0),
        };
        // In range
        assert!(pred.evaluate(&make_batch("AAPL", 150.0, 10), 0, &intern));
        // Inclusive lower bound
        assert!(pred.evaluate(&make_batch("AAPL", 100.0, 10), 0, &intern));
        // Inclusive upper bound
        assert!(pred.evaluate(&make_batch("AAPL", 200.0, 10), 0, &intern));
        // Below range
        assert!(!pred.evaluate(&make_batch("AAPL", 99.9, 10), 0, &intern));
        // Above range
        assert!(!pred.evaluate(&make_batch("AAPL", 200.1, 10), 0, &intern));
    }

    #[test]
    fn test_ring0_predicate_is_null() {
        let intern = StringInternTable::new();
        let pred = Ring0Predicate::IsNull { column_index: 0 };
        assert!(pred.evaluate(&make_nullable_batch(None, 100.0, 10), 0, &intern));
        assert!(!pred.evaluate(&make_nullable_batch(Some("AAPL"), 100.0, 10), 0, &intern));
    }

    #[test]
    fn test_ring0_predicate_is_not_null() {
        let intern = StringInternTable::new();
        let pred = Ring0Predicate::IsNotNull { column_index: 0 };
        assert!(pred.evaluate(&make_nullable_batch(Some("AAPL"), 100.0, 10), 0, &intern));
        assert!(!pred.evaluate(&make_nullable_batch(None, 100.0, 10), 0, &intern));
    }

    #[test]
    fn test_ring0_predicate_in_set() {
        let mut intern = StringInternTable::new();
        let aapl = intern.intern("AAPL");
        let goog = intern.intern("GOOG");
        let msft = intern.intern("MSFT");

        let pred = Ring0Predicate::In {
            column_index: 0,
            values: SmallVec::from_vec(vec![
                ScalarValue::StringIndex(aapl),
                ScalarValue::StringIndex(goog),
                ScalarValue::StringIndex(msft),
            ]),
        };

        assert!(pred.evaluate(&make_batch("AAPL", 150.0, 10), 0, &intern));
        assert!(pred.evaluate(&make_batch("GOOG", 150.0, 10), 0, &intern));
        assert!(!pred.evaluate(&make_batch("TSLA", 150.0, 10), 0, &intern));
    }

    // --- SubscriptionFilter tests ---

    #[test]
    fn test_subscription_filter_evaluate_ring0() {
        let mut intern = StringInternTable::new();
        let aapl = intern.intern("AAPL");

        let filter = SubscriptionFilter {
            expression: "symbol = 'AAPL' AND price > 100.0".to_string(),
            ring0_predicates: vec![
                Ring0Predicate::Eq {
                    column_index: 0,
                    value: ScalarValue::StringIndex(aapl),
                },
                Ring0Predicate::Gt {
                    column_index: 1,
                    value: ScalarValue::Float64(100.0),
                },
            ],
            ring1_predicates: vec![],
            intern_table: intern,
        };

        assert!(filter.evaluate_ring0(&make_batch("AAPL", 150.0, 10), 0));
        assert!(!filter.evaluate_ring0(&make_batch("GOOG", 150.0, 10), 0));
        assert!(!filter.evaluate_ring0(&make_batch("AAPL", 50.0, 10), 0));
    }

    // --- compile_filter tests ---

    #[test]
    fn test_filter_compile_simple_eq() {
        let schema = test_schema();
        let filter = compile_filter("price = 100.0", &schema).unwrap();
        assert_eq!(filter.ring0_predicates.len(), 1);
        assert!(filter.ring1_predicates.is_empty());
        assert!(matches!(
            &filter.ring0_predicates[0],
            Ring0Predicate::Eq {
                column_index: 1,
                ..
            }
        ));
    }

    #[test]
    fn test_filter_compile_and_split() {
        let schema = test_schema();
        let filter = compile_filter("price > 100.0 AND UPPER(symbol) = 'AAPL'", &schema).unwrap();
        assert_eq!(filter.ring0_predicates.len(), 1);
        assert_eq!(filter.ring1_predicates.len(), 1);
    }

    #[test]
    fn test_filter_compile_column_not_found() {
        let schema = test_schema();
        let filter = compile_filter("nonexistent = 100", &schema).unwrap();
        // Column not in schema falls through to Ring 1
        assert_eq!(filter.ring0_predicates.len(), 0);
        assert_eq!(filter.ring1_predicates.len(), 1);
    }

    #[test]
    fn test_filter_compile_invalid_expression() {
        let schema = test_schema();
        let result = compile_filter("price >>>> 100", &schema);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            FilterCompileError::InvalidExpression(_)
        ));
    }

    #[test]
    fn test_filter_compile_complex_to_ring1() {
        let schema = test_schema();
        let filter = compile_filter("price > 100 OR qty > 50", &schema).unwrap();
        assert_eq!(filter.ring0_predicates.len(), 0);
        assert_eq!(filter.ring1_predicates.len(), 1);
    }

    #[test]
    fn test_filter_compile_between() {
        let schema = test_schema();
        let filter = compile_filter("price BETWEEN 100.0 AND 200.0", &schema).unwrap();
        assert_eq!(filter.ring0_predicates.len(), 1);
        assert!(matches!(
            &filter.ring0_predicates[0],
            Ring0Predicate::Between {
                column_index: 1,
                ..
            }
        ));
    }

    #[test]
    fn test_filter_compile_in_list() {
        let schema = test_schema();
        let filter = compile_filter("symbol IN ('AAPL', 'GOOG', 'MSFT')", &schema).unwrap();
        assert_eq!(filter.ring0_predicates.len(), 1);
        assert!(matches!(
            &filter.ring0_predicates[0],
            Ring0Predicate::In { column_index: 0, values } if values.len() == 3
        ));
        assert_eq!(filter.intern_table.len(), 3);
    }

    #[test]
    fn test_filter_compile_is_null() {
        let schema = test_schema();
        let filter = compile_filter("symbol IS NULL", &schema).unwrap();
        assert_eq!(filter.ring0_predicates.len(), 1);
        assert!(matches!(
            &filter.ring0_predicates[0],
            Ring0Predicate::IsNull { column_index: 0 }
        ));
    }

    #[test]
    fn test_filter_compile_is_not_null() {
        let schema = test_schema();
        let filter = compile_filter("symbol IS NOT NULL", &schema).unwrap();
        assert_eq!(filter.ring0_predicates.len(), 1);
        assert!(matches!(
            &filter.ring0_predicates[0],
            Ring0Predicate::IsNotNull { column_index: 0 }
        ));
    }

    #[test]
    fn test_filter_compile_end_to_end() {
        let schema = test_schema();
        let filter =
            compile_filter("symbol = 'AAPL' AND price > 100.0 AND qty >= 10", &schema).unwrap();
        assert_eq!(filter.ring0_predicates.len(), 3);
        assert!(filter.ring1_predicates.is_empty());

        assert!(filter.evaluate_ring0(&make_batch("AAPL", 150.0, 20), 0));
        assert!(!filter.evaluate_ring0(&make_batch("GOOG", 150.0, 20), 0));
    }
}
