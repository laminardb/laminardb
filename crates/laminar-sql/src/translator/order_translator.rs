//! ORDER BY operator configuration builder
//!
//! Translates parsed ORDER BY analysis into Ring 0 operator configurations
//! that can be instantiated for streaming ORDER BY execution.

use crate::parser::order_analyzer::{OrderAnalysis, OrderColumn, OrderPattern};

/// Configuration for a streaming ORDER BY operator.
///
/// Each variant corresponds to a different bounded streaming sort strategy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrderOperatorConfig {
    /// Source already satisfies the ordering — no operator needed.
    SourceSatisfied,

    /// Streaming top-K: ORDER BY ... LIMIT N.
    ///
    /// Maintains a bounded heap of K entries with retraction-based emission.
    TopK(TopKConfig),

    /// Window-local sort: ORDER BY inside a windowed aggregation.
    ///
    /// Buffers per-window output, sorts on window close.
    WindowLocalSort(WindowLocalSortConfig),

    /// Watermark-bounded sort: ORDER BY event_time on out-of-order input.
    ///
    /// Buffers events between watermarks, emits sorted on watermark advance.
    WatermarkBoundedSort(WatermarkSortConfig),

    /// Per-group top-K: ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...) WHERE rn <= N.
    ///
    /// Independent top-K heaps per partition key.
    PerGroupTopK(PerGroupTopKConfig),
}

/// Configuration for streaming top-K operator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopKConfig {
    /// Number of top entries to maintain
    pub k: usize,
    /// Sort columns and directions
    pub sort_columns: Vec<OrderColumn>,
}

/// Configuration for window-local sort operator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WindowLocalSortConfig {
    /// Sort columns and directions
    pub sort_columns: Vec<OrderColumn>,
    /// Optional LIMIT for top-N within window
    pub limit: Option<usize>,
}

/// Configuration for watermark-bounded sort operator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatermarkSortConfig {
    /// Sort columns and directions
    pub sort_columns: Vec<OrderColumn>,
    /// Maximum buffer size (events)
    pub max_buffer_size: usize,
}

/// Configuration for per-group (partitioned) top-K operator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PerGroupTopKConfig {
    /// Number of top entries per partition
    pub k: usize,
    /// Partition key columns
    pub partition_columns: Vec<String>,
    /// Sort columns and directions
    pub sort_columns: Vec<OrderColumn>,
    /// Maximum number of partitions (memory safety)
    pub max_partitions: usize,
}

/// Default maximum buffer size for watermark-bounded sort.
const DEFAULT_MAX_BUFFER_SIZE: usize = 100_000;

/// Default maximum partitions for per-group top-K.
const DEFAULT_MAX_PARTITIONS: usize = 10_000;

impl OrderOperatorConfig {
    /// Creates an operator configuration from an ORDER BY analysis.
    ///
    /// Maps the classified pattern to the appropriate operator config.
    /// Returns `None` for `OrderPattern::None` (no ORDER BY).
    ///
    /// # Errors
    ///
    /// Returns error string for `OrderPattern::Unbounded`.
    pub fn from_analysis(
        analysis: &OrderAnalysis,
    ) -> Result<Option<Self>, String> {
        match &analysis.pattern {
            OrderPattern::None => Ok(None),
            OrderPattern::SourceSatisfied => Ok(Some(Self::SourceSatisfied)),
            OrderPattern::TopK { k } => Ok(Some(Self::TopK(TopKConfig {
                k: *k,
                sort_columns: analysis.order_columns.clone(),
            }))),
            OrderPattern::WindowLocal => Ok(Some(Self::WindowLocalSort(WindowLocalSortConfig {
                sort_columns: analysis.order_columns.clone(),
                limit: analysis.limit,
            }))),
            OrderPattern::PerGroupTopK {
                k,
                partition_columns,
            } => Ok(Some(Self::PerGroupTopK(PerGroupTopKConfig {
                k: *k,
                partition_columns: partition_columns.clone(),
                sort_columns: analysis.order_columns.clone(),
                max_partitions: DEFAULT_MAX_PARTITIONS,
            }))),
            OrderPattern::Unbounded => Err(
                "ORDER BY without LIMIT is not supported on unbounded streams. \
                 Add LIMIT N or use ORDER BY within a windowed aggregation."
                    .to_string(),
            ),
        }
    }

    /// Creates a watermark-bounded sort config for event time ordering.
    #[must_use]
    pub fn watermark_bounded(sort_columns: Vec<OrderColumn>) -> Self {
        Self::WatermarkBoundedSort(WatermarkSortConfig {
            sort_columns,
            max_buffer_size: DEFAULT_MAX_BUFFER_SIZE,
        })
    }
}

impl TopKConfig {
    /// Creates a new top-K configuration.
    #[must_use]
    pub fn new(k: usize, sort_columns: Vec<OrderColumn>) -> Self {
        Self { k, sort_columns }
    }
}

impl PerGroupTopKConfig {
    /// Sets the maximum number of partitions.
    #[must_use]
    pub fn with_max_partitions(mut self, max_partitions: usize) -> Self {
        self.max_partitions = max_partitions;
        self
    }
}

impl WatermarkSortConfig {
    /// Sets the maximum buffer size.
    #[must_use]
    pub fn with_max_buffer_size(mut self, max_buffer_size: usize) -> Self {
        self.max_buffer_size = max_buffer_size;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_sort_columns() -> Vec<OrderColumn> {
        vec![OrderColumn {
            column: "price".to_string(),
            descending: true,
            nulls_first: false,
        }]
    }

    #[test]
    fn test_topk_config_from_analysis() {
        let analysis = OrderAnalysis {
            order_columns: make_sort_columns(),
            limit: Some(10),
            is_windowed: false,
            pattern: OrderPattern::TopK { k: 10 },
        };
        let config = OrderOperatorConfig::from_analysis(&analysis).unwrap().unwrap();
        match config {
            OrderOperatorConfig::TopK(cfg) => {
                assert_eq!(cfg.k, 10);
                assert_eq!(cfg.sort_columns.len(), 1);
                assert_eq!(cfg.sort_columns[0].column, "price");
            }
            _ => panic!("Expected TopK config"),
        }
    }

    #[test]
    fn test_per_group_topk_config() {
        let analysis = OrderAnalysis {
            order_columns: make_sort_columns(),
            limit: Some(5),
            is_windowed: false,
            pattern: OrderPattern::PerGroupTopK {
                k: 5,
                partition_columns: vec!["category".to_string()],
            },
        };
        let config = OrderOperatorConfig::from_analysis(&analysis).unwrap().unwrap();
        match config {
            OrderOperatorConfig::PerGroupTopK(cfg) => {
                assert_eq!(cfg.k, 5);
                assert_eq!(cfg.partition_columns, vec!["category".to_string()]);
                assert_eq!(cfg.max_partitions, DEFAULT_MAX_PARTITIONS);
            }
            _ => panic!("Expected PerGroupTopK config"),
        }
    }

    #[test]
    fn test_window_local_sort_config() {
        let analysis = OrderAnalysis {
            order_columns: make_sort_columns(),
            limit: None,
            is_windowed: true,
            pattern: OrderPattern::WindowLocal,
        };
        let config = OrderOperatorConfig::from_analysis(&analysis).unwrap().unwrap();
        match config {
            OrderOperatorConfig::WindowLocalSort(cfg) => {
                assert_eq!(cfg.sort_columns.len(), 1);
                assert!(cfg.limit.is_none());
            }
            _ => panic!("Expected WindowLocalSort config"),
        }
    }

    #[test]
    fn test_source_satisfied_config() {
        let analysis = OrderAnalysis {
            order_columns: make_sort_columns(),
            limit: None,
            is_windowed: false,
            pattern: OrderPattern::SourceSatisfied,
        };
        let config = OrderOperatorConfig::from_analysis(&analysis).unwrap().unwrap();
        assert_eq!(config, OrderOperatorConfig::SourceSatisfied);
    }

    #[test]
    fn test_unbounded_rejected() {
        let analysis = OrderAnalysis {
            order_columns: make_sort_columns(),
            limit: None,
            is_windowed: false,
            pattern: OrderPattern::Unbounded,
        };
        let result = OrderOperatorConfig::from_analysis(&analysis);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("ORDER BY without LIMIT"));
    }

    #[test]
    fn test_no_order_by_returns_none() {
        let analysis = OrderAnalysis {
            order_columns: vec![],
            limit: None,
            is_windowed: false,
            pattern: OrderPattern::None,
        };
        let config = OrderOperatorConfig::from_analysis(&analysis).unwrap();
        assert!(config.is_none());
    }

    #[test]
    fn test_watermark_bounded_config() {
        let sort_cols = make_sort_columns();
        let config = OrderOperatorConfig::watermark_bounded(sort_cols);
        match config {
            OrderOperatorConfig::WatermarkBoundedSort(cfg) => {
                assert_eq!(cfg.max_buffer_size, DEFAULT_MAX_BUFFER_SIZE);
            }
            _ => panic!("Expected WatermarkBoundedSort config"),
        }
    }

    #[test]
    fn test_per_group_topk_with_max_partitions() {
        let cfg = PerGroupTopKConfig {
            k: 5,
            partition_columns: vec!["cat".to_string()],
            sort_columns: make_sort_columns(),
            max_partitions: DEFAULT_MAX_PARTITIONS,
        }
        .with_max_partitions(500);
        assert_eq!(cfg.max_partitions, 500);
    }

    #[test]
    fn test_watermark_sort_with_max_buffer() {
        let cfg = WatermarkSortConfig {
            sort_columns: make_sort_columns(),
            max_buffer_size: DEFAULT_MAX_BUFFER_SIZE,
        }
        .with_max_buffer_size(50_000);
        assert_eq!(cfg.max_buffer_size, 50_000);
    }
}
