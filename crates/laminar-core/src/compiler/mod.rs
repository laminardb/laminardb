//! Plan compiler infrastructure for Ring 0 event processing.
//!
//! This module provides the foundation for compiling `DataFusion` logical plans
//! into native functions that operate on a fixed-layout row format.
//!
//! # Components
//!
//! - [`row`]: Fixed-layout [`EventRow`] format with zero-copy field access
//! - [`bridge`]: [`RowBatchBridge`] for converting rows back to Arrow `RecordBatch`
//!
//! ## JIT Compilation (requires `jit` feature)
//!
//! - [`error`]: Error types and compiled function pointer wrappers
//! - [`expr`]: Cranelift-based expression compiler
//! - [`fold`]: Constant folding pre-pass
//! - [`jit`]: Cranelift JIT context management

pub mod bridge;
pub mod row;

#[cfg(feature = "jit")]
pub mod error;
#[cfg(feature = "jit")]
pub mod expr;
#[cfg(feature = "jit")]
pub mod fold;
#[cfg(feature = "jit")]
pub mod jit;

pub use bridge::{BridgeError, RowBatchBridge};
pub use row::{EventRow, FieldLayout, FieldType, MutableEventRow, RowError, RowSchema};

#[cfg(feature = "jit")]
pub use error::{CompileError, CompiledExpr, FilterFn, MaybeCompiledExpr, ScalarFn};
#[cfg(feature = "jit")]
pub use expr::ExprCompiler;
#[cfg(feature = "jit")]
pub use jit::JitContext;
