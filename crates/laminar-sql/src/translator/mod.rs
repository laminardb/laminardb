//! SQL to operator configuration translation
//!
//! This module translates parsed SQL AST into Ring 0 operator configurations
//! that can be instantiated and executed.

/// Analytic window function operator configuration builder
pub mod analytic_translator;
/// HAVING clause filter configuration
pub mod having_translator;
mod join_translator;
/// ORDER BY operator configuration builder
pub mod order_translator;
/// Streaming DDL (CREATE SOURCE/SINK) translator
pub mod streaming_ddl;
mod window_translator;

pub use crate::parser::order_analyzer::RankType;
pub use analytic_translator::{
    AnalyticFunctionConfig, AnalyticWindowConfig, WindowFrameConfig, WindowFrameFunctionConfig,
};
pub use having_translator::HavingFilterConfig;
pub use join_translator::{
    JoinOperatorConfig, LookupJoinConfig, LookupJoinType, StreamJoinConfig,
    TemporalJoinTranslatorConfig,
};
pub use order_translator::{
    OrderOperatorConfig, PerGroupTopKConfig, TopKConfig, WatermarkSortConfig, WindowLocalSortConfig,
};
pub use streaming_ddl::{
    sql_type_to_arrow, ColumnDefinition, SourceConfigOptions, SourceDefinition, WatermarkSpec,
};
pub use window_translator::{WindowOperatorConfig, WindowType};
