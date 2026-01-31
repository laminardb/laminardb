//! SQL to operator configuration translation
//!
//! This module translates parsed SQL AST into Ring 0 operator configurations
//! that can be instantiated and executed.

/// DAG EXPLAIN formatter
pub mod dag_planner;
mod join_translator;
/// ORDER BY operator configuration builder
pub mod order_translator;
/// Streaming DDL (CREATE SOURCE/SINK) translator
pub mod streaming_ddl;
mod window_translator;

pub use join_translator::{
    JoinOperatorConfig, LookupJoinConfig, LookupJoinType, StreamJoinConfig, StreamJoinType,
};
pub use order_translator::{
    OrderOperatorConfig, PerGroupTopKConfig, TopKConfig, WatermarkSortConfig,
    WindowLocalSortConfig,
};
pub use streaming_ddl::{
    BackpressureStrategy as StreamingBackpressure, ColumnDefinition, SinkDefinition,
    SourceConfigOptions, SourceDefinition, WaitStrategy as StreamingWaitStrategy, WatermarkSpec,
    sql_type_to_arrow,
};
pub use window_translator::{WindowOperatorConfig, WindowType};
pub use dag_planner::{format_dag_explain, DagExplainOutput};
