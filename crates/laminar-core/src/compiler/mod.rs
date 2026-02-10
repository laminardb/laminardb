//! Plan compiler infrastructure for Ring 0 event processing.
//!
//! This module provides the foundation for compiling `DataFusion` logical plans
//! into native functions that operate on a fixed-layout row format.
//!
//! # Components
//!
//! - [`row`]: Fixed-layout [`EventRow`] format with zero-copy field access
//! - [`bridge`]: [`RowBatchBridge`] for converting rows back to Arrow `RecordBatch`

pub mod bridge;
pub mod row;

pub use bridge::{BridgeError, RowBatchBridge};
pub use row::{EventRow, FieldLayout, FieldType, MutableEventRow, RowError, RowSchema};
