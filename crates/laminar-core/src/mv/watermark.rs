//! Watermark propagation for cascading materialized views.
//!
//! Watermarks flow through the MV DAG following the min semantics:
//! a view's watermark is the minimum of all its source watermarks.

use super::registry::MvRegistry;
use fxhash::FxHashMap;
use std::sync::Arc;

/// Tracks watermarks for cascading materialized views.
///
/// When a source watermark advances, the tracker propagates the change
/// to all dependent views using min semantics: a view's watermark is
/// the minimum of all its source watermarks.
///
/// # Example
///
/// ```rust
/// use laminar_core::mv::{MvRegistry, CascadingWatermarkTracker, MaterializedView};
/// use std::sync::Arc;
///
/// let mut registry = MvRegistry::new();
/// registry.register_base_table("trades");
/// # use arrow_schema::{Schema, Field, DataType};
/// # let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
/// # let mv = |n: &str, s: Vec<&str>| MaterializedView::new(n, "", s.into_iter().map(String::from).collect(), schema.clone());
/// registry.register(mv("ohlc_1s", vec!["trades"])).unwrap();
/// registry.register(mv("ohlc_1m", vec!["ohlc_1s"])).unwrap();
///
/// let registry = Arc::new(registry);
/// let mut tracker = CascadingWatermarkTracker::new(registry);
///
/// // Advance base watermark
/// let updated = tracker.update_watermark("trades", 60_000);
///
/// // All downstream views get updated
/// assert!(updated.iter().any(|(n, _)| n == "ohlc_1s"));
/// assert!(updated.iter().any(|(n, _)| n == "ohlc_1m"));
/// ```
#[derive(Debug)]
pub struct CascadingWatermarkTracker {
    /// Current watermark per source/MV.
    watermarks: FxHashMap<String, i64>,
    /// Registry reference for dependency lookup.
    registry: Arc<MvRegistry>,
}

impl CascadingWatermarkTracker {
    /// Creates a new tracker with the given registry.
    #[must_use]
    pub fn new(registry: Arc<MvRegistry>) -> Self {
        Self {
            watermarks: FxHashMap::default(),
            registry,
        }
    }

    /// Returns the current watermark for a source or MV.
    #[must_use]
    pub fn get_watermark(&self, name: &str) -> Option<i64> {
        self.watermarks.get(name).copied()
    }

    /// Updates the watermark for a source and propagates to dependents.
    ///
    /// Returns a list of (name, watermark) pairs for all sources/MVs that
    /// were updated, in dependency order.
    pub fn update_watermark(&mut self, source: &str, watermark: i64) -> Vec<(String, i64)> {
        let mut updated = Vec::new();

        // Update this source's watermark if it advanced
        let old = self.watermarks.get(source).copied();
        if old.is_none_or(|w| watermark > w) {
            self.watermarks.insert(source.to_string(), watermark);
            updated.push((source.to_string(), watermark));
        }

        // Propagate to dependents
        self.propagate_watermarks(source, &mut updated);

        updated
    }

    /// Updates watermarks for multiple sources atomically.
    ///
    /// Useful for updating all partitions of a source at once.
    pub fn update_watermarks_batch(
        &mut self,
        updates: impl IntoIterator<Item = (String, i64)>,
    ) -> Vec<(String, i64)> {
        let mut all_updated = Vec::new();
        let mut sources_updated = Vec::new();

        // First pass: update all source watermarks
        for (source, watermark) in updates {
            let old = self.watermarks.get(&source).copied();
            if old.is_none_or(|w| watermark > w) {
                self.watermarks.insert(source.clone(), watermark);
                all_updated.push((source.clone(), watermark));
                sources_updated.push(source);
            }
        }

        // Second pass: propagate from all updated sources
        for source in sources_updated {
            self.propagate_watermarks(&source, &mut all_updated);
        }

        all_updated
    }

    /// Gets the effective watermark for an MV based on its sources.
    ///
    /// Returns `None` if any source watermark is unknown.
    #[must_use]
    pub fn effective_watermark(&self, mv_name: &str) -> Option<i64> {
        let view = self.registry.get(mv_name)?;

        // Find minimum of all source watermarks
        let mut min_watermark = None;
        for source in &view.sources {
            match self.watermarks.get(source) {
                Some(&wm) => {
                    min_watermark = Some(min_watermark.map_or(wm, |m: i64| m.min(wm)));
                }
                None => return None, // Unknown source watermark
            }
        }

        min_watermark
    }

    /// Resets all watermarks.
    pub fn reset(&mut self) {
        self.watermarks.clear();
    }

    /// Returns all current watermarks.
    pub fn all_watermarks(&self) -> impl Iterator<Item = (&str, i64)> {
        self.watermarks.iter().map(|(k, v)| (k.as_str(), *v))
    }

    fn propagate_watermarks(&mut self, source: &str, updated: &mut Vec<(String, i64)>) {
        // Get dependents in topological order
        let dependents: Vec<_> = self
            .registry
            .get_dependents(source)
            .map(String::from)
            .collect();

        for dependent in dependents {
            if let Some(new_watermark) = self.compute_min_watermark(&dependent) {
                let old = self.watermarks.get(&dependent).copied();
                if old.is_none_or(|w| new_watermark > w) {
                    self.watermarks.insert(dependent.clone(), new_watermark);

                    // Only add if not already in updated list
                    if !updated.iter().any(|(n, _)| n == &dependent) {
                        updated.push((dependent.clone(), new_watermark));
                    }

                    // Recursively propagate
                    self.propagate_watermarks(&dependent, updated);
                }
            }
        }
    }

    fn compute_min_watermark(&self, mv_name: &str) -> Option<i64> {
        let view = self.registry.get(mv_name)?;

        let mut min_wm: Option<i64> = None;
        for source in &view.sources {
            if let Some(&wm) = self.watermarks.get(source) {
                min_wm = Some(min_wm.map_or(wm, |m| m.min(wm)));
            } else {
                // If any source has no watermark, we can't compute the min
                return None;
            }
        }

        min_wm
    }
}

/// Checkpoint data for watermark tracker.
#[derive(Debug, Clone)]
pub struct WatermarkTrackerCheckpoint {
    /// Watermarks at checkpoint time.
    pub watermarks: Vec<(String, i64)>,
}

impl CascadingWatermarkTracker {
    /// Creates a checkpoint of the current watermark state.
    #[must_use]
    pub fn checkpoint(&self) -> WatermarkTrackerCheckpoint {
        WatermarkTrackerCheckpoint {
            watermarks: self
                .watermarks
                .iter()
                .map(|(k, v)| (k.clone(), *v))
                .collect(),
        }
    }

    /// Restores watermark state from a checkpoint.
    pub fn restore(&mut self, checkpoint: WatermarkTrackerCheckpoint) {
        self.watermarks.clear();
        for (name, watermark) in checkpoint.watermarks {
            self.watermarks.insert(name, watermark);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mv::registry::MaterializedView;
    use arrow_schema::{DataType, Field, Schema};

    fn setup_registry() -> Arc<MvRegistry> {
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

        Arc::new(registry)
    }

    #[test]
    fn test_watermark_update_and_propagation() {
        let registry = setup_registry();
        let mut tracker = CascadingWatermarkTracker::new(registry);

        let updated = tracker.update_watermark("trades", 60_000);

        // All MVs should be updated
        assert!(updated.iter().any(|(n, _)| n == "trades"));
        assert!(updated.iter().any(|(n, _)| n == "ohlc_1s"));
        assert!(updated.iter().any(|(n, _)| n == "ohlc_1m"));
        assert!(updated.iter().any(|(n, _)| n == "ohlc_1h"));

        // Check watermark values
        assert_eq!(tracker.get_watermark("trades"), Some(60_000));
        assert_eq!(tracker.get_watermark("ohlc_1s"), Some(60_000));
        assert_eq!(tracker.get_watermark("ohlc_1m"), Some(60_000));
        assert_eq!(tracker.get_watermark("ohlc_1h"), Some(60_000));
    }

    #[test]
    fn test_watermark_no_regression() {
        let registry = setup_registry();
        let mut tracker = CascadingWatermarkTracker::new(registry);

        tracker.update_watermark("trades", 60_000);
        let updated = tracker.update_watermark("trades", 30_000);

        // Watermark should not regress
        assert!(updated.is_empty());
        assert_eq!(tracker.get_watermark("trades"), Some(60_000));
    }

    #[test]
    fn test_multi_source_min_semantics() {
        let mut registry = MvRegistry::new();
        registry.register_base_table("source_a");
        registry.register_base_table("source_b");

        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        registry
            .register(MaterializedView::new(
                "combined",
                "",
                vec!["source_a".into(), "source_b".into()],
                schema,
            ))
            .unwrap();

        let registry = Arc::new(registry);
        let mut tracker = CascadingWatermarkTracker::new(registry);

        // Update source_a first
        tracker.update_watermark("source_a", 100);
        assert_eq!(tracker.get_watermark("combined"), None); // source_b not set

        // Update source_b with lower value
        tracker.update_watermark("source_b", 80);
        assert_eq!(tracker.get_watermark("combined"), Some(80)); // min(100, 80)

        // Update source_b with higher value
        tracker.update_watermark("source_b", 120);
        assert_eq!(tracker.get_watermark("combined"), Some(100)); // min(100, 120)

        // Update source_a to advance
        tracker.update_watermark("source_a", 150);
        assert_eq!(tracker.get_watermark("combined"), Some(120)); // min(150, 120)
    }

    #[test]
    fn test_effective_watermark() {
        let mut registry = MvRegistry::new();
        registry.register_base_table("a");
        registry.register_base_table("b");

        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        registry
            .register(MaterializedView::new(
                "join",
                "",
                vec!["a".into(), "b".into()],
                schema,
            ))
            .unwrap();

        let registry = Arc::new(registry);
        let mut tracker = CascadingWatermarkTracker::new(registry);

        // Neither source set
        assert_eq!(tracker.effective_watermark("join"), None);

        // Only a set
        tracker.update_watermark("a", 100);
        assert_eq!(tracker.effective_watermark("join"), None);

        // Both set
        tracker.update_watermark("b", 50);
        assert_eq!(tracker.effective_watermark("join"), Some(50));
    }

    #[test]
    fn test_batch_update() {
        let registry = setup_registry();
        let mut tracker = CascadingWatermarkTracker::new(registry);

        let updates = vec![("trades".to_string(), 1000)];
        let new_watermarks = tracker.update_watermarks_batch(updates);
        assert_eq!(new_watermarks.len(), 1);
        assert_eq!(tracker.get_watermark("trades"), Some(1000));
        assert_eq!(tracker.get_watermark("ohlc_1s"), Some(1000));
    }

    #[test]
    fn test_checkpoint_restore() {
        let registry = setup_registry();
        let mut tracker = CascadingWatermarkTracker::new(Arc::clone(&registry));

        tracker.update_watermark("trades", 5000);
        let checkpoint = tracker.checkpoint();

        // Create new tracker and restore
        let mut tracker2 = CascadingWatermarkTracker::new(registry);
        tracker2.restore(checkpoint);

        assert_eq!(tracker2.get_watermark("trades"), Some(5000));
        assert_eq!(tracker2.get_watermark("ohlc_1s"), Some(5000));
    }

    #[test]
    fn test_diamond_dependency_watermarks() {
        let mut registry = MvRegistry::new();
        registry.register_base_table("source");

        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let mv = |n: &str, s: Vec<&str>| {
            MaterializedView::new(
                n,
                "",
                s.into_iter().map(String::from).collect(),
                schema.clone(),
            )
        };

        //       source
        //       /    \
        //      a      b
        //       \    /
        //         c
        registry.register(mv("a", vec!["source"])).unwrap();
        registry.register(mv("b", vec!["source"])).unwrap();
        registry.register(mv("c", vec!["a", "b"])).unwrap();

        let registry = Arc::new(registry);
        let mut tracker = CascadingWatermarkTracker::new(registry);

        tracker.update_watermark("source", 100);

        // All should have same watermark
        assert_eq!(tracker.get_watermark("a"), Some(100));
        assert_eq!(tracker.get_watermark("b"), Some(100));
        assert_eq!(tracker.get_watermark("c"), Some(100));
    }

    #[test]
    fn test_all_watermarks() {
        let registry = setup_registry();
        let mut tracker = CascadingWatermarkTracker::new(registry);

        tracker.update_watermark("trades", 1000);

        let all: Vec<_> = tracker.all_watermarks().collect();
        assert_eq!(all.len(), 4); // trades + 3 MVs
    }

    #[test]
    fn test_reset() {
        let registry = setup_registry();
        let mut tracker = CascadingWatermarkTracker::new(registry);

        tracker.update_watermark("trades", 1000);
        tracker.reset();

        assert!(tracker.get_watermark("trades").is_none());
        assert!(tracker.get_watermark("ohlc_1s").is_none());
    }
}
