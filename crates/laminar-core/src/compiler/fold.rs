//! Constant folding pre-pass for expression compilation.
//!
//! [`fold_constants`] rewrites an expression tree, evaluating constant
//! sub-expressions at compile time before Cranelift code generation.

use datafusion_common::ScalarValue;
use datafusion_expr::{BinaryExpr, Expr, Operator};

/// Folds constant sub-expressions in a `DataFusion` [`Expr`] tree.
///
/// Handles:
/// - Arithmetic on literal pairs (`Literal op Literal`)
/// - Boolean identity rules (`true AND x` → `x`, `false OR x` → `x`, etc.)
/// - Recursive descent into `BinaryExpr`, `Not` children
///
/// Returns a (possibly simplified) clone of the input expression.
#[must_use]
pub fn fold_constants(expr: &Expr) -> Expr {
    match expr {
        Expr::BinaryExpr(binary) => fold_binary(binary),
        Expr::Not(inner) => fold_not(inner),
        _ => expr.clone(),
    }
}

/// Folds a binary expression, first recursing into children.
fn fold_binary(binary: &BinaryExpr) -> Expr {
    let left = fold_constants(&binary.left);
    let right = fold_constants(&binary.right);

    // Try boolean identity rules first.
    if let Some(simplified) = try_boolean_identity(&left, &right, binary.op) {
        return simplified;
    }

    // Try literal-literal folding.
    if let (Expr::Literal(lv, _), Expr::Literal(rv, _)) = (&left, &right) {
        if let Some(result) = fold_literal_pair(lv, rv, binary.op) {
            return Expr::Literal(result, None);
        }
    }

    Expr::BinaryExpr(BinaryExpr::new(Box::new(left), binary.op, Box::new(right)))
}

/// Folds `NOT` expressions: `NOT true` → `false`, `NOT false` → `true`.
fn fold_not(inner: &Expr) -> Expr {
    let folded = fold_constants(inner);
    match &folded {
        Expr::Literal(ScalarValue::Boolean(Some(b)), _) => {
            Expr::Literal(ScalarValue::Boolean(Some(!b)), None)
        }
        _ => Expr::Not(Box::new(folded)),
    }
}

/// Applies boolean identity simplifications.
///
/// - `true AND x` → `x`
/// - `false AND x` → `false`
/// - `x AND true` → `x`
/// - `x AND false` → `false`
/// - `true OR x` → `true`
/// - `false OR x` → `x`
/// - `x OR true` → `true`
/// - `x OR false` → `x`
fn try_boolean_identity(left: &Expr, right: &Expr, op: Operator) -> Option<Expr> {
    match op {
        Operator::And => {
            if let Expr::Literal(ScalarValue::Boolean(Some(b)), _) = left {
                return Some(if *b { right.clone() } else { left.clone() });
            }
            if let Expr::Literal(ScalarValue::Boolean(Some(b)), _) = right {
                return Some(if *b { left.clone() } else { right.clone() });
            }
            None
        }
        Operator::Or => {
            if let Expr::Literal(ScalarValue::Boolean(Some(b)), _) = left {
                return Some(if *b { left.clone() } else { right.clone() });
            }
            if let Expr::Literal(ScalarValue::Boolean(Some(b)), _) = right {
                return Some(if *b { right.clone() } else { left.clone() });
            }
            None
        }
        _ => None,
    }
}

/// Evaluates `lhs op rhs` for two scalar literals.
fn fold_literal_pair(lhs: &ScalarValue, rhs: &ScalarValue, op: Operator) -> Option<ScalarValue> {
    // i64 arithmetic
    if let (ScalarValue::Int64(Some(l)), ScalarValue::Int64(Some(r))) = (lhs, rhs) {
        return fold_i64(*l, *r, op);
    }
    // f64 arithmetic
    if let (ScalarValue::Float64(Some(l)), ScalarValue::Float64(Some(r))) = (lhs, rhs) {
        return fold_f64(*l, *r, op);
    }
    // Boolean logic
    if let (ScalarValue::Boolean(Some(l)), ScalarValue::Boolean(Some(r))) = (lhs, rhs) {
        return fold_bool(*l, *r, op);
    }
    None
}

fn fold_i64(l: i64, r: i64, op: Operator) -> Option<ScalarValue> {
    match op {
        Operator::Plus => l.checked_add(r).map(|v| ScalarValue::Int64(Some(v))),
        Operator::Minus => l.checked_sub(r).map(|v| ScalarValue::Int64(Some(v))),
        Operator::Multiply => l.checked_mul(r).map(|v| ScalarValue::Int64(Some(v))),
        Operator::Divide if r != 0 => l.checked_div(r).map(|v| ScalarValue::Int64(Some(v))),
        Operator::Modulo if r != 0 => l.checked_rem(r).map(|v| ScalarValue::Int64(Some(v))),
        Operator::Eq => Some(ScalarValue::Boolean(Some(l == r))),
        Operator::NotEq => Some(ScalarValue::Boolean(Some(l != r))),
        Operator::Lt => Some(ScalarValue::Boolean(Some(l < r))),
        Operator::LtEq => Some(ScalarValue::Boolean(Some(l <= r))),
        Operator::Gt => Some(ScalarValue::Boolean(Some(l > r))),
        Operator::GtEq => Some(ScalarValue::Boolean(Some(l >= r))),
        _ => None,
    }
}

fn fold_f64(l: f64, r: f64, op: Operator) -> Option<ScalarValue> {
    match op {
        Operator::Plus => Some(ScalarValue::Float64(Some(l + r))),
        Operator::Minus => Some(ScalarValue::Float64(Some(l - r))),
        Operator::Multiply => Some(ScalarValue::Float64(Some(l * r))),
        Operator::Divide if r != 0.0 => Some(ScalarValue::Float64(Some(l / r))),
        _ => None,
    }
}

fn fold_bool(l: bool, r: bool, op: Operator) -> Option<ScalarValue> {
    match op {
        Operator::And => Some(ScalarValue::Boolean(Some(l && r))),
        Operator::Or => Some(ScalarValue::Boolean(Some(l || r))),
        Operator::Eq => Some(ScalarValue::Boolean(Some(l == r))),
        Operator::NotEq => Some(ScalarValue::Boolean(Some(l != r))),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion_expr::col;

    fn lit_i64(v: i64) -> Expr {
        Expr::Literal(ScalarValue::Int64(Some(v)), None)
    }

    fn lit_f64(v: f64) -> Expr {
        Expr::Literal(ScalarValue::Float64(Some(v)), None)
    }

    fn lit_bool(v: bool) -> Expr {
        Expr::Literal(ScalarValue::Boolean(Some(v)), None)
    }

    #[test]
    fn fold_i64_addition() {
        let expr = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(lit_i64(10)),
            Operator::Plus,
            Box::new(lit_i64(20)),
        ));
        let folded = fold_constants(&expr);
        assert_eq!(folded, lit_i64(30));
    }

    #[test]
    fn fold_i64_subtraction() {
        let expr = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(lit_i64(50)),
            Operator::Minus,
            Box::new(lit_i64(8)),
        ));
        assert_eq!(fold_constants(&expr), lit_i64(42));
    }

    #[test]
    fn fold_i64_multiplication() {
        let expr = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(lit_i64(6)),
            Operator::Multiply,
            Box::new(lit_i64(7)),
        ));
        assert_eq!(fold_constants(&expr), lit_i64(42));
    }

    #[test]
    fn fold_f64_arithmetic() {
        let expr = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(lit_f64(1.5)),
            Operator::Plus,
            Box::new(lit_f64(2.5)),
        ));
        let folded = fold_constants(&expr);
        assert_eq!(folded, lit_f64(4.0));
    }

    #[test]
    fn fold_nested_constants() {
        // (2 + 3) * 4 → 5 * 4 → 20
        let inner = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(lit_i64(2)),
            Operator::Plus,
            Box::new(lit_i64(3)),
        ));
        let expr = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(inner),
            Operator::Multiply,
            Box::new(lit_i64(4)),
        ));
        assert_eq!(fold_constants(&expr), lit_i64(20));
    }

    #[test]
    fn fold_not_literal() {
        assert_eq!(fold_constants(&Expr::Not(Box::new(lit_bool(true)))), lit_bool(false));
        assert_eq!(fold_constants(&Expr::Not(Box::new(lit_bool(false)))), lit_bool(true));
    }

    #[test]
    fn fold_boolean_identity_and() {
        let x = col("x");
        // true AND x → x
        let expr = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(lit_bool(true)),
            Operator::And,
            Box::new(x.clone()),
        ));
        assert_eq!(fold_constants(&expr), x);

        // false AND x → false
        let expr = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(lit_bool(false)),
            Operator::And,
            Box::new(col("x")),
        ));
        assert_eq!(fold_constants(&expr), lit_bool(false));
    }

    #[test]
    fn fold_boolean_identity_or() {
        let x = col("x");
        // false OR x → x
        let expr = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(lit_bool(false)),
            Operator::Or,
            Box::new(x.clone()),
        ));
        assert_eq!(fold_constants(&expr), x);

        // true OR x → true
        let expr = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(lit_bool(true)),
            Operator::Or,
            Box::new(col("x")),
        ));
        assert_eq!(fold_constants(&expr), lit_bool(true));
    }

    #[test]
    fn no_fold_on_column_expr() {
        let expr = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(col("a")),
            Operator::Plus,
            Box::new(lit_i64(1)),
        ));
        let folded = fold_constants(&expr);
        // Should remain a BinaryExpr (column can't be folded).
        assert!(matches!(folded, Expr::BinaryExpr(_)));
    }

    #[test]
    fn fold_division_by_zero_no_fold() {
        let expr = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(lit_i64(10)),
            Operator::Divide,
            Box::new(lit_i64(0)),
        ));
        // Division by zero — don't fold, leave as binary expr.
        let folded = fold_constants(&expr);
        assert!(matches!(folded, Expr::BinaryExpr(_)));
    }

    #[test]
    fn fold_comparison() {
        let expr = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(lit_i64(5)),
            Operator::Gt,
            Box::new(lit_i64(3)),
        ));
        assert_eq!(fold_constants(&expr), lit_bool(true));
    }
}
