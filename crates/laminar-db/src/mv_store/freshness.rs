//! Declared freshness of materialized views: the most a committed read of a view may lag. A
//! monitor compares the age of the newest committed cut with each declaration and raises an
//! alarm when a view's committed reads become staler than it declared.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use crate::checkpoint_coordinator::committed_reads::CommittedCuts;
use crate::engine_metrics::EngineMetrics;

/// How often the monitor compares committed staleness with declared freshness.
const MONITOR_PERIOD: Duration = Duration::from_millis(100);

/// Freshness each view declared.
#[derive(Debug, Default)]
pub(crate) struct FreshnessContracts {
    declared: parking_lot::Mutex<BTreeMap<String, Duration>>,
}

impl FreshnessContracts {
    pub(crate) fn declare(&self, view: &str, freshness: Duration) {
        self.declared.lock().insert(view.to_owned(), freshness);
    }

    pub(super) fn remove(&self, view: &str) {
        self.declared.lock().remove(view);
    }

    #[cfg(test)]
    pub(crate) fn declared_freshness(&self, view: &str) -> Option<Duration> {
        self.declared.lock().get(view).copied()
    }

    /// Views whose declared freshness is below `staleness`.
    fn breached(&self, staleness: Duration) -> BTreeSet<String> {
        self.declared
            .lock()
            .iter()
            .filter(|(_, freshness)| staleness > **freshness)
            .map(|(view, _)| view.clone())
            .collect()
    }
}

/// Views that entered and left breach since the previous evaluation.
#[derive(Debug, Default, PartialEq, Eq)]
struct Transitions {
    entered: Vec<String>,
    left: Vec<String>,
}

/// Replace `current` with `breached`, reporting the views that changed.
fn transition(current: &mut BTreeSet<String>, breached: BTreeSet<String>) -> Transitions {
    let transitions = Transitions {
        entered: breached.difference(current).cloned().collect(),
        left: current.difference(&breached).cloned().collect(),
    };
    *current = breached;
    transitions
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| {
            u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
        })
}

/// Compare committed staleness with declared freshness until the coordinator publishing `cuts`
/// is dropped. Before the first cut commits, staleness counts from the monitor's start.
pub(crate) fn spawn_monitor(
    contracts: Arc<FreshnessContracts>,
    mut cuts: CommittedCuts,
    metrics: Option<Arc<EngineMetrics>>,
) {
    tokio::spawn(async move {
        let started_ms = now_ms();
        let mut breached = BTreeSet::new();
        let mut tick = tokio::time::interval(MONITOR_PERIOD);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                changed = cuts.changed() => {
                    if changed.is_err() {
                        return;
                    }
                }
                _ = tick.tick() => {}
            }
            let taken_at_ms = cuts
                .borrow()
                .as_ref()
                .map_or(started_ms, |cut| cut.taken_at_ms);
            let staleness_ms = now_ms().saturating_sub(taken_at_ms);
            let transitions = transition(
                &mut breached,
                contracts.breached(Duration::from_millis(staleness_ms)),
            );
            if let Some(metrics) = &metrics {
                metrics
                    .views
                    .committed_staleness_ms
                    .set(i64::try_from(staleness_ms).unwrap_or(i64::MAX));
                for view in &transitions.entered {
                    metrics
                        .views
                        .freshness_breaches_total
                        .with_label_values(&[view.as_str()])
                        .inc();
                }
            }
            for view in transitions.entered {
                tracing::warn!(
                    view,
                    staleness_ms,
                    "committed reads of the materialized view are staler than its declared freshness"
                );
            }
            for view in transitions.left {
                tracing::info!(
                    view,
                    "committed reads of the materialized view are fresh again"
                );
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn views(names: &[&str]) -> BTreeSet<String> {
        names.iter().map(|name| (*name).to_owned()).collect()
    }

    #[test]
    fn a_view_breaches_once_while_staler_than_it_declared() {
        let contracts = FreshnessContracts::default();
        contracts.declare("fast", Duration::from_secs(1));
        contracts.declare("slow", Duration::from_secs(10));
        assert!(contracts.breached(Duration::from_secs(1)).is_empty());
        assert_eq!(contracts.breached(Duration::from_secs(2)), views(&["fast"]));

        let mut current = BTreeSet::new();
        let entered = transition(&mut current, views(&["fast"]));
        assert_eq!(entered.entered, ["fast"]);
        let held = transition(&mut current, views(&["fast", "slow"]));
        assert_eq!(held.entered, ["slow"], "a held breach is not raised again");
        let cleared = transition(&mut current, BTreeSet::new());
        assert_eq!(cleared.left, ["fast", "slow"]);

        contracts.remove("fast");
        assert!(contracts.breached(Duration::from_secs(5)).is_empty());
    }

    #[tokio::test]
    async fn the_monitor_raises_one_alarm_per_breach_and_stops_with_its_coordinator() {
        let contracts = Arc::new(FreshnessContracts::default());
        contracts.declare("v", Duration::from_millis(1));
        let metrics = Arc::new(EngineMetrics::new(&prometheus::Registry::new()));
        let (cuts, receiver) = tokio::sync::watch::channel(None);
        spawn_monitor(Arc::clone(&contracts), receiver, Some(Arc::clone(&metrics)));
        tokio::time::sleep(Duration::from_millis(350)).await;
        let breaches = metrics
            .views
            .freshness_breaches_total
            .with_label_values(&["v"]);
        assert_eq!(breaches.get(), 1);
        assert!(metrics.views.committed_staleness_ms.get() >= 300);

        drop(cuts);
        tokio::time::sleep(Duration::from_millis(50)).await;
        let stopped = metrics.views.committed_staleness_ms.get();
        tokio::time::sleep(Duration::from_millis(250)).await;
        assert_eq!(metrics.views.committed_staleness_ms.get(), stopped);
    }
}
