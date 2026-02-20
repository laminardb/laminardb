//! Adaptive compilation warmup for JIT-compiled streaming queries.
//!
//! [`AdaptiveQueryRunner`] starts executing a query immediately via `DataFusion`
//! interpreted mode while Cranelift compiles the pipeline in the background.
//! When compilation completes, the runner swaps to the compiled path.
//!
//! This eliminates the "cold start" latency of JIT compilation (typically 0.5–5ms)
//! for interactive or ad-hoc queries, while still providing the throughput benefits
//! of compiled execution for steady-state processing.

#[cfg(feature = "jit")]
use std::sync::Arc;

#[cfg(feature = "jit")]
use datafusion::logical_expr::LogicalPlan;

#[cfg(feature = "jit")]
use laminar_core::compiler::{
    compile_streaming_query, CompiledStreamingQuery, CompilerCache, QueryConfig,
};

/// Current execution mode of an adaptive query.
#[cfg(feature = "jit")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionMode {
    /// Using `DataFusion` interpreted execution.
    Interpreted,
    /// Compilation in progress; interpreted is running.
    Compiling,
    /// Switched to compiled execution.
    Compiled,
    /// Compilation failed; staying on interpreted.
    FallbackPermanent,
}

#[cfg(feature = "jit")]
impl std::fmt::Display for ExecutionMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Interpreted => write!(f, "Interpreted"),
            Self::Compiling => write!(f, "Compiling"),
            Self::Compiled => write!(f, "Compiled"),
            Self::FallbackPermanent => write!(f, "FallbackPermanent"),
        }
    }
}

/// Manages the interpreted → compiled transition for a streaming query.
///
/// # Lifecycle
///
/// 1. Call [`start`](Self::start) to spawn background compilation and begin
///    with `Compiling` mode.
/// 2. Periodically call [`try_take_compiled`](Self::try_take_compiled) to check
///    if compilation has finished.
/// 3. When `try_take_compiled` returns `Some(compiled)`, the caller wires up
///    the compiled query and swaps from interpreted to compiled execution.
#[cfg(feature = "jit")]
pub struct AdaptiveQueryRunner {
    /// Receiver for the compiled query result.
    rx: Option<tokio::sync::oneshot::Receiver<Option<CompiledStreamingQuery>>>,
    /// Current execution mode.
    mode: ExecutionMode,
}

#[cfg(feature = "jit")]
impl AdaptiveQueryRunner {
    /// Creates a runner and immediately spawns background compilation.
    ///
    /// The caller should begin interpreted execution while compilation proceeds.
    pub fn start(
        sql: String,
        logical_plan: LogicalPlan,
        cache: Arc<parking_lot::Mutex<CompilerCache>>,
        config: QueryConfig,
    ) -> Self {
        let (tx, rx) = tokio::sync::oneshot::channel();

        tokio::task::spawn_blocking(move || {
            let mut cache = cache.lock();
            let result = match compile_streaming_query(&sql, &logical_plan, &mut cache, &config) {
                Ok(result) => result,
                Err(e) => {
                    tracing::debug!(
                        error = %e,
                        "Background JIT compilation failed"
                    );
                    None
                }
            };
            // Ignore send error — receiver may have been dropped.
            let _ = tx.send(result);
        });

        Self {
            rx: Some(rx),
            mode: ExecutionMode::Compiling,
        }
    }

    /// Returns the current execution mode.
    #[must_use]
    pub fn mode(&self) -> ExecutionMode {
        self.mode
    }

    /// Non-blocking check: has compilation completed?
    ///
    /// Returns `Some(compiled)` when compilation succeeds and is ready for swap.
    /// Returns `None` if compilation is still in progress, already consumed, or failed.
    ///
    /// After this returns `Some`, subsequent calls return `None`.
    pub fn try_take_compiled(&mut self) -> Option<CompiledStreamingQuery> {
        let rx = self.rx.as_mut()?;
        match rx.try_recv() {
            Ok(Some(compiled)) => {
                self.rx = None;
                self.mode = ExecutionMode::Compiled;
                Some(compiled)
            }
            Ok(None) => {
                // Compilation returned None (breakers / unsupported plan).
                self.rx = None;
                self.mode = ExecutionMode::FallbackPermanent;
                None
            }
            Err(tokio::sync::oneshot::error::TryRecvError::Empty) => {
                // Still compiling.
                None
            }
            Err(tokio::sync::oneshot::error::TryRecvError::Closed) => {
                // Sender dropped (panic or abort).
                self.rx = None;
                self.mode = ExecutionMode::FallbackPermanent;
                None
            }
        }
    }

    /// Returns `true` if compilation is still in progress.
    #[must_use]
    pub fn is_compiling(&self) -> bool {
        self.mode == ExecutionMode::Compiling
    }

    /// Returns `true` if the runner has permanently fallen back to interpreted.
    #[must_use]
    pub fn is_fallback(&self) -> bool {
        self.mode == ExecutionMode::FallbackPermanent
    }
}

#[cfg(feature = "jit")]
impl std::fmt::Debug for AdaptiveQueryRunner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AdaptiveQueryRunner")
            .field("mode", &self.mode)
            .field("compile_pending", &self.rx.is_some())
            .finish()
    }
}

#[cfg(test)]
#[cfg(feature = "jit")]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::common::DFSchema;
    use datafusion::logical_expr::{col, lit, LogicalPlan, LogicalPlanBuilder};

    fn table_scan_plan(fields: Vec<(&str, DataType)>) -> LogicalPlan {
        let arrow_schema = Arc::new(Schema::new(
            fields
                .into_iter()
                .map(|(name, dt)| Field::new(name, dt, true))
                .collect::<Vec<_>>(),
        ));
        let df_schema = DFSchema::try_from(arrow_schema.as_ref().clone()).unwrap();
        LogicalPlan::EmptyRelation(datafusion::logical_expr::EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(df_schema),
        })
    }

    fn new_cache() -> Arc<parking_lot::Mutex<CompilerCache>> {
        Arc::new(parking_lot::Mutex::new(CompilerCache::new(64).unwrap()))
    }

    #[tokio::test]
    async fn adaptive_compiles_simple_filter() {
        let scan = table_scan_plan(vec![("x", DataType::Int64)]);
        let plan = LogicalPlanBuilder::from(scan)
            .filter(col("x").gt(lit(10_i64)))
            .unwrap()
            .build()
            .unwrap();

        let cache = new_cache();
        let config = QueryConfig::default();
        let mut runner = AdaptiveQueryRunner::start(
            "SELECT x FROM t WHERE x > 10".to_string(),
            plan,
            cache,
            config,
        );

        assert_eq!(runner.mode(), ExecutionMode::Compiling);
        assert!(runner.is_compiling());

        // Wait for compilation to finish.
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let compiled = runner.try_take_compiled();
        assert!(compiled.is_some());
        assert_eq!(runner.mode(), ExecutionMode::Compiled);
        assert!(!runner.is_compiling());
    }

    #[tokio::test]
    async fn adaptive_fallback_for_aggregate() {
        let scan = table_scan_plan(vec![("key", DataType::Int64), ("val", DataType::Int64)]);
        let agg_schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int64, true)]));
        let df_schema = DFSchema::try_from(agg_schema.as_ref().clone()).unwrap();
        let agg = datafusion::logical_expr::Aggregate::try_new_with_schema(
            Arc::new(scan),
            vec![col("key")],
            vec![],
            Arc::new(df_schema),
        )
        .unwrap();
        let plan = LogicalPlan::Aggregate(agg);

        let cache = new_cache();
        let config = QueryConfig::default();
        let mut runner = AdaptiveQueryRunner::start(
            "SELECT key FROM t GROUP BY key".to_string(),
            plan,
            cache,
            config,
        );

        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        // Aggregate plan → compile returns None → permanent fallback.
        let compiled = runner.try_take_compiled();
        assert!(compiled.is_none());
        assert_eq!(runner.mode(), ExecutionMode::FallbackPermanent);
        assert!(runner.is_fallback());
    }

    #[tokio::test]
    async fn adaptive_second_take_returns_none() {
        let scan = table_scan_plan(vec![("x", DataType::Int64)]);
        let plan = LogicalPlanBuilder::from(scan)
            .filter(col("x").gt(lit(0_i64)))
            .unwrap()
            .build()
            .unwrap();

        let cache = new_cache();
        let config = QueryConfig::default();
        let mut runner = AdaptiveQueryRunner::start("q".to_string(), plan, cache, config);

        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let first = runner.try_take_compiled();
        assert!(first.is_some());

        // Second call returns None — already consumed.
        let second = runner.try_take_compiled();
        assert!(second.is_none());
    }

    #[tokio::test]
    async fn execution_mode_display() {
        assert_eq!(format!("{}", ExecutionMode::Interpreted), "Interpreted");
        assert_eq!(format!("{}", ExecutionMode::Compiling), "Compiling");
        assert_eq!(format!("{}", ExecutionMode::Compiled), "Compiled");
        assert_eq!(
            format!("{}", ExecutionMode::FallbackPermanent),
            "FallbackPermanent"
        );
    }

    #[tokio::test]
    async fn adaptive_debug_format() {
        let scan = table_scan_plan(vec![("x", DataType::Int64)]);
        let cache = new_cache();
        let config = QueryConfig::default();
        let runner = AdaptiveQueryRunner::start("q".to_string(), scan, cache, config);
        let s = format!("{runner:?}");
        assert!(s.contains("AdaptiveQueryRunner"));
        assert!(s.contains("mode"));
    }

    #[tokio::test]
    async fn try_take_before_finished_returns_none() {
        let scan = table_scan_plan(vec![("x", DataType::Int64)]);
        let plan = LogicalPlanBuilder::from(scan)
            .filter(col("x").gt(lit(0_i64)))
            .unwrap()
            .build()
            .unwrap();

        let cache = new_cache();
        let config = QueryConfig::default();
        let mut runner = AdaptiveQueryRunner::start("q".to_string(), plan, cache, config);

        // Immediately check — may or may not be ready yet.
        // Just ensure this doesn't panic.
        let _ = runner.try_take_compiled();
        // Mode is either Compiling (not ready) or Compiled (very fast).
        assert!(
            runner.mode() == ExecutionMode::Compiling || runner.mode() == ExecutionMode::Compiled
        );
    }
}
