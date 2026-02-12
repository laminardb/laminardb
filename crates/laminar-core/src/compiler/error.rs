//! Error types and compiled expression wrappers for the JIT compiler.
//!
//! Defines [`CompileError`] for compilation failures, function pointer types
//! for compiled filters and scalars, and the [`CompiledExpr`] / [`MaybeCompiledExpr`]
//! wrappers used by downstream pipeline stages.

use std::fmt;

use super::row::FieldType;

/// Errors that can occur during expression compilation.
#[derive(Debug)]
pub enum CompileError {
    /// An expression node is not supported by the JIT compiler.
    UnsupportedExpr(String),
    /// A field type is not supported in the requested context.
    UnsupportedType(FieldType),
    /// A binary operation is not supported for the given type.
    UnsupportedBinaryOp(FieldType, datafusion_expr::Operator),
    /// A literal value type is not supported.
    UnsupportedLiteral,
    /// A referenced column was not found in the schema.
    ColumnNotFound(String),
    /// Cranelift module-level error during compilation or linking.
    Cranelift(Box<cranelift_module::ModuleError>),
}

impl fmt::Display for CompileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedExpr(desc) => write!(f, "unsupported expression: {desc}"),
            Self::UnsupportedType(ft) => write!(f, "unsupported field type: {ft:?}"),
            Self::UnsupportedBinaryOp(ft, op) => {
                write!(f, "unsupported binary op {op} for type {ft:?}")
            }
            Self::UnsupportedLiteral => write!(f, "unsupported literal value"),
            Self::ColumnNotFound(name) => write!(f, "column not found: {name}"),
            Self::Cranelift(e) => write!(f, "cranelift error: {e}"),
        }
    }
}

impl std::error::Error for CompileError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Cranelift(e) => Some(e.as_ref()),
            _ => None,
        }
    }
}

impl From<cranelift_module::ModuleError> for CompileError {
    fn from(e: cranelift_module::ModuleError) -> Self {
        Self::Cranelift(Box::new(e))
    }
}

/// Errors that can occur during pipeline extraction from a logical plan.
#[derive(Debug)]
pub enum ExtractError {
    /// The logical plan contains a node that cannot be decomposed into pipelines.
    UnsupportedPlan(String),
    /// A schema-related error during extraction (e.g., unsupported column type).
    SchemaError(String),
}

impl fmt::Display for ExtractError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedPlan(desc) => write!(f, "unsupported plan node: {desc}"),
            Self::SchemaError(desc) => write!(f, "schema error during extraction: {desc}"),
        }
    }
}

impl std::error::Error for ExtractError {}

/// A compiled filter function: `fn(row_ptr: *const u8) -> u8`.
///
/// Returns 1 if the row passes the filter, 0 otherwise.
/// The caller must ensure the pointer points to a valid `EventRow` byte buffer
/// matching the schema used during compilation.
pub type FilterFn = unsafe extern "C" fn(*const u8) -> u8;

/// A compiled scalar function: `fn(row_ptr: *const u8, out_ptr: *mut u8) -> u8`.
///
/// Evaluates the expression and writes the result to `out_ptr`.
/// Returns 1 if the result is null, 0 if valid.
pub type ScalarFn = unsafe extern "C" fn(*const u8, *mut u8) -> u8;

/// A successfully compiled expression, either a filter or a scalar projection.
pub enum CompiledExpr {
    /// A filter expression returning a boolean.
    Filter(FilterFn),
    /// A scalar expression writing its result to an output pointer.
    Scalar(ScalarFn),
}

/// Result of attempting to compile an expression.
///
/// Falls back to interpretation when the JIT compiler encounters an
/// unsupported expression node, preserving the reason for diagnostics.
pub enum MaybeCompiledExpr {
    /// Successfully compiled to native code.
    Compiled(CompiledExpr),
    /// Compilation failed; caller should use interpreted evaluation.
    Interpreted {
        /// The reason compilation failed.
        reason: CompileError,
    },
}
