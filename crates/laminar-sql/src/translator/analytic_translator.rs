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
}
