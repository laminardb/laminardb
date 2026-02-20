//! # Cascading Materialized Views
//!
//! Materialized views that can read from other materialized views, forming a DAG.
//! Essential for multi-resolution time-series aggregation (e.g., 1s → 1m → 1h OHLC bars).
//!
//! ## Key Components
//!
//! - [`MvRegistry`] - Manages view definitions with dependency tracking
//! - [`CascadingWatermarkTracker`] - Propagates watermarks through the MV DAG
//! - [`MvPipelineExecutor`] - Processes events in topological order
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
//! ```rust
//! use laminar_core::mv::{MvRegistry, MaterializedView, MvPipelineExecutor, PassThroughOperator};
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
mod executor;
mod registry;

pub mod watermark;

pub use error::{MvError, MvState};
pub use executor::{
    MvPipelineCheckpoint, MvPipelineExecutor, PassThroughOperator, PipelineMetrics,
};
pub use registry::{MaterializedView, MvRegistry};
pub use watermark::{CascadingWatermarkTracker, WatermarkTrackerCheckpoint};

#[cfg(test)]
#[allow(clippy::similar_names)]
mod tests {
    use super::*;
    use crate::operator::{Event, OperatorContext};
    use crate::state::InMemoryStore;
    use crate::time::{BoundedOutOfOrdernessGenerator, TimerService};
    use arrow_array::{Float64Array, Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    fn create_ohlc_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new("open", DataType::Float64, false),
            Field::new("high", DataType::Float64, false),
            Field::new("low", DataType::Float64, false),
            Field::new("close", DataType::Float64, false),
            Field::new("volume", DataType::Int64, false),
        ]))
    }

    fn create_trade_event(symbol: &str, price: f64, qty: i64, timestamp: i64) -> Event {
        let symbol_array = Arc::new(StringArray::from(vec![symbol]));
        let price_array = Arc::new(Float64Array::from(vec![price]));
        let qty_array = Arc::new(Int64Array::from(vec![qty]));

        let batch = RecordBatch::try_from_iter(vec![
            ("symbol", symbol_array as _),
            ("price", price_array as _),
            ("quantity", qty_array as _),
        ])
        .unwrap();

        Event::new(timestamp, batch)
    }

    #[test]
    fn test_full_ohlc_pipeline() {
        // Setup cascading OHLC views
        let mut registry = MvRegistry::new();
        registry.register_base_table("trades");

        let schema = create_ohlc_schema();

        let ohlc_1s = MaterializedView::new(
            "ohlc_1s",
            "SELECT symbol, FIRST_VALUE(price), MAX(price), MIN(price), LAST_VALUE(price), SUM(qty) FROM trades GROUP BY symbol, TUMBLE(ts, '1 second')",
            vec!["trades".into()],
            schema.clone(),
        );
        registry.register(ohlc_1s).unwrap();

        let ohlc_1m = MaterializedView::new(
            "ohlc_1m",
            "SELECT symbol, FIRST_VALUE(open), MAX(high), MIN(low), LAST_VALUE(close), SUM(volume) FROM ohlc_1s GROUP BY symbol, TUMBLE(bar_time, '1 minute')",
            vec!["ohlc_1s".into()],
            schema.clone(),
        );
        registry.register(ohlc_1m).unwrap();

        let ohlc_1h = MaterializedView::new(
            "ohlc_1h",
            "SELECT symbol, FIRST_VALUE(open), MAX(high), MIN(low), LAST_VALUE(close), SUM(volume) FROM ohlc_1m GROUP BY symbol, TUMBLE(bar_time, '1 hour')",
            vec!["ohlc_1m".into()],
            schema,
        );
        registry.register(ohlc_1h).unwrap();

        // Verify topological order
        assert_eq!(registry.topo_order(), &["ohlc_1s", "ohlc_1m", "ohlc_1h"]);

        // Create executor
        let registry = Arc::new(registry);
        let mut executor = MvPipelineExecutor::new(Arc::clone(&registry));

        // Register operators
        executor
            .register_operator("ohlc_1s", Box::new(PassThroughOperator::new("ohlc_1s")))
            .unwrap();
        executor
            .register_operator("ohlc_1m", Box::new(PassThroughOperator::new("ohlc_1m")))
            .unwrap();
        executor
            .register_operator("ohlc_1h", Box::new(PassThroughOperator::new("ohlc_1h")))
            .unwrap();

        assert!(executor.is_ready());

        // Process trade events
        let mut state = InMemoryStore::new();
        let mut timers = TimerService::new();
        let mut wm_gen = BoundedOutOfOrdernessGenerator::new(1000);
        let mut ctx = OperatorContext {
            event_time: 0,
            processing_time: 0,
            timers: &mut timers,
            state: &mut state,
            watermark_generator: &mut wm_gen,
            operator_index: 0,
        };

        // Send trades
        let trades = vec![
            create_trade_event("AAPL", 150.0, 100, 1000),
            create_trade_event("AAPL", 151.5, 200, 1500),
            create_trade_event("AAPL", 149.0, 150, 2000),
        ];

        for trade in trades {
            executor
                .process_source_event("trades", trade, &mut ctx)
                .unwrap();
        }

        // Verify events propagated through all levels
        assert_eq!(executor.metrics().events_processed, 3);
        assert_eq!(executor.metrics().events_per_mv.get("ohlc_1s"), Some(&3));
        assert_eq!(executor.metrics().events_per_mv.get("ohlc_1m"), Some(&3));
        assert_eq!(executor.metrics().events_per_mv.get("ohlc_1h"), Some(&3));

        // Advance watermark and verify propagation
        executor.advance_watermark("trades", 60_000);
        assert_eq!(executor.get_watermark("ohlc_1s"), Some(60_000));
        assert_eq!(executor.get_watermark("ohlc_1m"), Some(60_000));
        assert_eq!(executor.get_watermark("ohlc_1h"), Some(60_000));
    }

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
        // Equivalent to SQL: SHOW DEPENDENCIES FOR ohlc_1h
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

        // Get full dependency chain
        let chain = registry.dependency_chain("ohlc_1h");
        assert_eq!(chain, vec!["ohlc_1s", "ohlc_1m", "ohlc_1h"]);

        // Direct dependencies
        let direct: Vec<_> = registry.get_dependencies("ohlc_1h").collect();
        assert_eq!(direct, vec!["ohlc_1m"]);
    }
}
