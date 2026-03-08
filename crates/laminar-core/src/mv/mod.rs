//! # Cascading Materialized Views
//!
//! Materialized views that can read from other materialized views, forming a DAG.
//! Essential for multi-resolution time-series aggregation (e.g., 1s → 1m → 1h OHLC bars).
//!
//! ## Key Components
//!
//! - [`MvRegistry`] - Manages view definitions with dependency tracking
//! - [`MaterializedView`] - View definition with SQL, dependencies, and schema
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                      MV Pipeline                            │
//! ├─────────────────────────────────────────────────────────────┤
//! │                                                             │
//! │   Base Table         MV Level 1         MV Level 2          │
//! │  ┌─────────┐       ┌─────────┐        ┌─────────┐          │
//! │  │ trades  │──────▶│ ohlc_1s │───────▶│ ohlc_1m │──────▶   │
//! │  └─────────┘       └─────────┘        └─────────┘          │
//! │       │                 │                  │                │
//! │       ▼                 ▼                  ▼                │
//! │   Watermark ─────▶ Watermark ───────▶ Watermark            │
//! │   (source)         (min of sources)  (min of sources)      │
//! │                                                             │
//! └─────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Example: Cascading OHLC Bars
//!
//! ```rust,ignore
//! use laminar_core::mv::{MvRegistry, MaterializedView, MvPipelineExecutor};
//! use arrow_schema::{Schema, Field, DataType};
//! use std::sync::Arc;
//!
//! // Create registry
//! let mut registry = MvRegistry::new();
//! registry.register_base_table("trades");
//!
//! // Define schema for OHLC bars
//! let schema = Arc::new(Schema::new(vec![
//!     Field::new("symbol", DataType::Utf8, false),
//!     Field::new("open", DataType::Float64, false),
//!     Field::new("high", DataType::Float64, false),
//!     Field::new("low", DataType::Float64, false),
//!     Field::new("close", DataType::Float64, false),
//!     Field::new("volume", DataType::Int64, false),
//! ]));
//!
//! // Register cascading views: trades -> ohlc_1s -> ohlc_1m -> ohlc_1h
//! let ohlc_1s = MaterializedView::new(
//!     "ohlc_1s",
//!     "SELECT symbol, FIRST_VALUE(price), MAX(price), MIN(price), LAST_VALUE(price), SUM(qty) FROM trades GROUP BY symbol, TUMBLE(ts, '1 second')",
//!     vec!["trades".into()],
//!     schema.clone(),
//! );
//! registry.register(ohlc_1s).unwrap();
//!
//! let ohlc_1m = MaterializedView::new(
//!     "ohlc_1m",
//!     "SELECT symbol, FIRST_VALUE(open), MAX(high), MIN(low), LAST_VALUE(close), SUM(volume) FROM ohlc_1s GROUP BY symbol, TUMBLE(bar_time, '1 minute')",
//!     vec!["ohlc_1s".into()],
//!     schema.clone(),
//! );
//! registry.register(ohlc_1m).unwrap();
//!
//! // Views are processed in topological order
//! assert_eq!(registry.topo_order(), &["ohlc_1s", "ohlc_1m"]);
//!
//! // Create executor
//! let registry = Arc::new(registry);
//! let mut executor = MvPipelineExecutor::new(Arc::clone(&registry));
//!
//! // Register operators for each view
//! executor.register_operator("ohlc_1s", Box::new(PassThroughOperator::new("ohlc_1s"))).unwrap();
//! executor.register_operator("ohlc_1m", Box::new(PassThroughOperator::new("ohlc_1m"))).unwrap();
//!
//! assert!(executor.is_ready());
//! ```
//!
//! ## Watermark Propagation
//!
//! Watermarks flow through the DAG using min semantics:
//! - A view's watermark = minimum of all source watermarks
//! - Updates propagate automatically to all dependents
//!
//! ```rust
//! use laminar_core::mv::{MvRegistry, MaterializedView, CascadingWatermarkTracker};
//! use arrow_schema::{Schema, Field, DataType};
//! use std::sync::Arc;
//!
//! let mut registry = MvRegistry::new();
//! registry.register_base_table("orders");
//! registry.register_base_table("payments");
//!
//! let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
//!
//! // View with multiple sources
//! let order_payments = MaterializedView::new(
//!     "order_payments",
//!     "SELECT * FROM orders JOIN payments ON orders.id = payments.order_id",
//!     vec!["orders".into(), "payments".into()],
//!     schema,
//! );
//! registry.register(order_payments).unwrap();
//!
//! let registry = Arc::new(registry);
//! let mut tracker = CascadingWatermarkTracker::new(registry);
//!
//! // Update source watermarks
//! tracker.update_watermark("orders", 100);
//! tracker.update_watermark("payments", 80);
//!
//! // View watermark is the minimum
//! assert_eq!(tracker.get_watermark("order_payments"), Some(80));
//! ```
//!
//! ## Cycle Detection
//!
//! The registry prevents cycles during registration:
//!
//! ```rust
//! use laminar_core::mv::{MvRegistry, MaterializedView, MvError};
//! use arrow_schema::{Schema, Field, DataType};
//! use std::sync::Arc;
//!
//! let mut registry = MvRegistry::new();
//! registry.register_base_table("base");
//!
//! let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
//! let mv = |n: &str, s: Vec<&str>| {
//!     MaterializedView::new(n, "", s.into_iter().map(String::from).collect(), schema.clone())
//! };
//!
//! registry.register(mv("a", vec!["base"])).unwrap();
//! registry.register(mv("b", vec!["a"])).unwrap();
//!
//! // Can't create a cycle back to a
//! // (Note: cycles through existing views require modification, not new registration)
//! ```
//!
//! ## Cascade Drops
//!
//! Use `unregister_cascade` to drop a view and all its dependents:
//!
//! ```rust
//! use laminar_core::mv::{MvRegistry, MaterializedView};
//! use arrow_schema::{Schema, Field, DataType};
//! use std::sync::Arc;
//!
//! let mut registry = MvRegistry::new();
//! registry.register_base_table("trades");
//!
//! let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
//! let mv = |n: &str, s: Vec<&str>| {
//!     MaterializedView::new(n, "", s.into_iter().map(String::from).collect(), schema.clone())
//! };
//!
//! registry.register(mv("ohlc_1s", vec!["trades"])).unwrap();
//! registry.register(mv("ohlc_1m", vec!["ohlc_1s"])).unwrap();
//! registry.register(mv("ohlc_1h", vec!["ohlc_1m"])).unwrap();
//!
//! // Drop ohlc_1s and all dependents
//! let removed = registry.unregister_cascade("ohlc_1s").unwrap();
//! assert_eq!(removed.len(), 3);
//! assert!(registry.is_empty());
//! ```

mod error;
mod registry;

pub use error::{MvError, MvState};
pub use registry::{MaterializedView, MvRegistry};

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn test_dependency_chain() {
        let mut registry = MvRegistry::new();
        registry.register_base_table("trades");

        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let mv = |n: &str, s: Vec<&str>| {
            MaterializedView::new(
                n,
                "",
                s.into_iter().map(String::from).collect(),
                schema.clone(),
            )
        };

        registry.register(mv("a", vec!["trades"])).unwrap();
        registry.register(mv("b", vec!["a"])).unwrap();
        registry.register(mv("c", vec!["b"])).unwrap();

        let chain = registry.dependency_chain("c");
        assert_eq!(chain, vec!["a", "b", "c"]);
    }

    #[test]
    fn test_show_dependencies_equivalent() {
        let mut registry = MvRegistry::new();
        registry.register_base_table("trades");

        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let mv = |n: &str, s: Vec<&str>| {
            MaterializedView::new(
                n,
                "",
                s.into_iter().map(String::from).collect(),
                schema.clone(),
            )
        };

        registry.register(mv("ohlc_1s", vec!["trades"])).unwrap();
        registry.register(mv("ohlc_1m", vec!["ohlc_1s"])).unwrap();
        registry.register(mv("ohlc_1h", vec!["ohlc_1m"])).unwrap();

        let chain = registry.dependency_chain("ohlc_1h");
        assert_eq!(chain, vec!["ohlc_1s", "ohlc_1m", "ohlc_1h"]);

        let direct: Vec<_> = registry.get_dependencies("ohlc_1h").collect();
        assert_eq!(direct, vec!["ohlc_1m"]);
    }
}
