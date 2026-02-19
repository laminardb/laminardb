//! Analytic window function operator configuration builder
//!
//! Translates parsed analytic function analysis into Ring 0 operator
//! configurations for LAG/LEAD/FIRST_VALUE/LAST_VALUE/NTH_VALUE execution.

use crate::parser::analytic_parser::{AnalyticFunctionType, AnalyticWindowAnalysis};

/// Configuration for a streaming analytic window operator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnalyticWindowConfig {
    /// Individual function configurations
    pub functions: Vec<AnalyticFunctionConfig>,
    /// PARTITION BY columns
    pub partition_columns: Vec<String>,
    /// ORDER BY columns
    pub order_columns: Vec<String>,
    /// Maximum number of partitions (memory safety)
    pub max_partitions: usize,
}

/// Configuration for a single analytic function within the operator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnalyticFunctionConfig {
    /// Type of analytic function
    pub function_type: AnalyticFunctionType,
    /// Source column name
    pub source_column: String,
    /// Offset (for LAG/LEAD) or N (for NTH_VALUE)
    pub offset: usize,
    /// Default value as string (for LAG/LEAD)
    pub default_value: Option<String>,
    /// Output column alias
    pub output_alias: Option<String>,
}

/// Default maximum partitions for analytic window operators.
const DEFAULT_MAX_PARTITIONS: usize = 10_000;

impl AnalyticWindowConfig {
    /// Creates an operator configuration from an analytic window analysis.
    #[must_use]
    pub fn from_analysis(analysis: &AnalyticWindowAnalysis) -> Self {
        let functions = analysis
            .functions
            .iter()
            .map(|f| AnalyticFunctionConfig {
                function_type: f.function_type,
                source_column: f.column.clone(),
                offset: f.offset,
                default_value: f.default_value.clone(),
                output_alias: f.alias.clone(),
            })
            .collect();

        Self {
            functions,
            partition_columns: analysis.partition_columns.clone(),
            order_columns: analysis.order_columns.clone(),
            max_partitions: DEFAULT_MAX_PARTITIONS,
        }
    }

    /// Sets the maximum number of partitions.
    #[must_use]
    pub fn with_max_partitions(mut self, max_partitions: usize) -> Self {
        self.max_partitions = max_partitions;
        self
    }

    /// Returns true if any function requires lookahead buffering.
    #[must_use]
    pub fn has_lookahead(&self) -> bool {
        self.functions
            .iter()
            .any(|f| f.function_type.requires_lookahead())
    }
}

// --- Window Frame operator configuration ---

use crate::parser::analytic_parser::{
    FrameBound, FrameUnits, WindowFrameAnalysis, WindowFrameFunction,
};

/// Configuration for a single window frame aggregate function.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WindowFrameFunctionConfig {
    /// Type of aggregate function
    pub function_type: WindowFrameFunction,
    /// Source column name
    pub source_column: String,
    /// Frame unit type (ROWS or RANGE)
    pub units: FrameUnits,
    /// Start bound of the frame
    pub start_bound: FrameBound,
    /// End bound of the frame
    pub end_bound: FrameBound,
    /// Output column alias
    pub output_alias: Option<String>,
}

/// Configuration for a streaming window frame operator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WindowFrameConfig {
    /// Individual function configurations
    pub functions: Vec<WindowFrameFunctionConfig>,
    /// PARTITION BY columns
    pub partition_columns: Vec<String>,
    /// ORDER BY columns
    pub order_columns: Vec<String>,
    /// Maximum number of partitions (memory safety)
    pub max_partitions: usize,
}

impl WindowFrameConfig {
    /// Creates an operator configuration from a window frame analysis.
    #[must_use]
    pub fn from_analysis(analysis: &WindowFrameAnalysis) -> Self {
        let functions = analysis
            .functions
            .iter()
            .map(|f| WindowFrameFunctionConfig {
                function_type: f.function_type,
                source_column: f.column.clone(),
                units: f.units,
                start_bound: f.start_bound.clone(),
                end_bound: f.end_bound.clone(),
                output_alias: f.alias.clone(),
            })
            .collect();

        Self {
            functions,
            partition_columns: analysis.partition_columns.clone(),
            order_columns: analysis.order_columns.clone(),
            max_partitions: DEFAULT_MAX_PARTITIONS,
        }
    }

    /// Sets the maximum number of partitions.
    #[must_use]
    pub fn with_max_partitions(mut self, max_partitions: usize) -> Self {
        self.max_partitions = max_partitions;
        self
    }

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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::analytic_parser::{AnalyticFunctionInfo, AnalyticWindowAnalysis};

    fn make_lag_analysis() -> AnalyticWindowAnalysis {
        AnalyticWindowAnalysis {
            functions: vec![AnalyticFunctionInfo {
                function_type: AnalyticFunctionType::Lag,
                column: "price".to_string(),
                offset: 1,
                default_value: None,
                alias: Some("prev_price".to_string()),
            }],
            partition_columns: vec!["symbol".to_string()],
            order_columns: vec!["ts".to_string()],
        }
    }

    #[test]
    fn test_from_analysis_lag() {
        let analysis = make_lag_analysis();
        let config = AnalyticWindowConfig::from_analysis(&analysis);
        assert_eq!(config.functions.len(), 1);
        assert_eq!(config.functions[0].function_type, AnalyticFunctionType::Lag);
        assert_eq!(config.functions[0].source_column, "price");
        assert_eq!(config.functions[0].offset, 1);
        assert_eq!(
            config.functions[0].output_alias.as_deref(),
            Some("prev_price")
        );
        assert_eq!(config.partition_columns, vec!["symbol".to_string()]);
        assert_eq!(config.order_columns, vec!["ts".to_string()]);
        assert!(!config.has_lookahead());
    }

    #[test]
    fn test_from_analysis_lead() {
        let analysis = AnalyticWindowAnalysis {
            functions: vec![AnalyticFunctionInfo {
                function_type: AnalyticFunctionType::Lead,
                column: "price".to_string(),
                offset: 2,
                default_value: Some("0".to_string()),
                alias: Some("next_price".to_string()),
            }],
            partition_columns: vec![],
            order_columns: vec!["ts".to_string()],
        };
        let config = AnalyticWindowConfig::from_analysis(&analysis);
        assert!(config.has_lookahead());
        assert_eq!(config.functions[0].offset, 2);
        assert_eq!(config.functions[0].default_value.as_deref(), Some("0"));
    }

    #[test]
    fn test_max_partitions() {
        let analysis = make_lag_analysis();
        let config = AnalyticWindowConfig::from_analysis(&analysis).with_max_partitions(500);
        assert_eq!(config.max_partitions, 500);
    }

    #[test]
    fn test_multiple_functions() {
        let analysis = AnalyticWindowAnalysis {
            functions: vec![
                AnalyticFunctionInfo {
                    function_type: AnalyticFunctionType::Lag,
                    column: "price".to_string(),
                    offset: 1,
                    default_value: None,
                    alias: Some("prev".to_string()),
                },
                AnalyticFunctionInfo {
                    function_type: AnalyticFunctionType::Lead,
                    column: "price".to_string(),
                    offset: 1,
                    default_value: None,
                    alias: Some("next".to_string()),
                },
            ],
            partition_columns: vec!["sym".to_string()],
            order_columns: vec!["ts".to_string()],
        };
        let config = AnalyticWindowConfig::from_analysis(&analysis);
        assert_eq!(config.functions.len(), 2);
        assert!(config.has_lookahead());
    }

    #[test]
    fn test_default_max_partitions() {
        let analysis = make_lag_analysis();
        let config = AnalyticWindowConfig::from_analysis(&analysis);
        assert_eq!(config.max_partitions, DEFAULT_MAX_PARTITIONS);
    }

    // --- Window Frame translator tests ---

    use crate::parser::analytic_parser::WindowFrameInfo;

    fn make_frame_analysis() -> WindowFrameAnalysis {
        WindowFrameAnalysis {
            functions: vec![WindowFrameInfo {
                function_type: WindowFrameFunction::Avg,
                column: "price".to_string(),
                units: FrameUnits::Rows,
                start_bound: FrameBound::Preceding(9),
                end_bound: FrameBound::CurrentRow,
                alias: Some("ma".to_string()),
            }],
            partition_columns: vec!["symbol".to_string()],
            order_columns: vec!["ts".to_string()],
        }
    }

    #[test]
    fn test_frame_from_analysis_basic() {
        let analysis = make_frame_analysis();
        let config = WindowFrameConfig::from_analysis(&analysis);
        assert_eq!(config.functions.len(), 1);
        assert_eq!(config.functions[0].function_type, WindowFrameFunction::Avg);
        assert_eq!(config.functions[0].source_column, "price");
        assert_eq!(config.functions[0].units, FrameUnits::Rows);
        assert_eq!(config.functions[0].start_bound, FrameBound::Preceding(9));
        assert_eq!(config.functions[0].end_bound, FrameBound::CurrentRow);
        assert_eq!(config.functions[0].output_alias.as_deref(), Some("ma"));
        assert_eq!(config.partition_columns, vec!["symbol".to_string()]);
        assert_eq!(config.order_columns, vec!["ts".to_string()]);
        assert!(!config.has_following());
    }

    #[test]
    fn test_frame_max_partitions_builder() {
        let analysis = make_frame_analysis();
        let config = WindowFrameConfig::from_analysis(&analysis).with_max_partitions(500);
        assert_eq!(config.max_partitions, 500);
    }

    #[test]
    fn test_frame_has_following() {
        let analysis = WindowFrameAnalysis {
            functions: vec![WindowFrameInfo {
                function_type: WindowFrameFunction::Sum,
                column: "amount".to_string(),
                units: FrameUnits::Rows,
                start_bound: FrameBound::Preceding(5),
                end_bound: FrameBound::Following(3),
                alias: None,
            }],
            partition_columns: vec![],
            order_columns: vec!["id".to_string()],
        };
        let config = WindowFrameConfig::from_analysis(&analysis);
        assert!(config.has_following());
    }

    #[test]
    fn test_frame_multiple_functions_config() {
        let analysis = WindowFrameAnalysis {
            functions: vec![
                WindowFrameInfo {
                    function_type: WindowFrameFunction::Avg,
                    column: "price".to_string(),
                    units: FrameUnits::Rows,
                    start_bound: FrameBound::Preceding(9),
                    end_bound: FrameBound::CurrentRow,
                    alias: Some("ma".to_string()),
                },
                WindowFrameInfo {
                    function_type: WindowFrameFunction::Max,
                    column: "price".to_string(),
                    units: FrameUnits::Rows,
                    start_bound: FrameBound::Preceding(4),
                    end_bound: FrameBound::CurrentRow,
                    alias: Some("hi".to_string()),
                },
            ],
            partition_columns: vec!["symbol".to_string()],
            order_columns: vec!["ts".to_string()],
        };
        let config = WindowFrameConfig::from_analysis(&analysis);
        assert_eq!(config.functions.len(), 2);
        assert_eq!(config.functions[0].function_type, WindowFrameFunction::Avg);
        assert_eq!(config.functions[1].function_type, WindowFrameFunction::Max);
    }
}
