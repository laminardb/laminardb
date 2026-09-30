//! Scrape-time observation of the current connector FIFO's byte reservations.

use std::sync::{Arc, Weak};

use parking_lot::Mutex;
use prometheus::{PullingGauge, Registry};
use tokio::sync::Semaphore;

use super::SourceMsgRx;

struct ObservedBudget {
    max_bytes: usize,
    semaphore: Weak<Semaphore>,
}

pub(crate) struct SourceQueueMetrics {
    current: Arc<Mutex<Option<ObservedBudget>>>,
}

impl SourceQueueMetrics {
    pub(crate) fn register(registry: &Registry) -> Result<Self, prometheus::Error> {
        let current: Arc<Mutex<Option<ObservedBudget>>> = Arc::new(Mutex::new(None));
        let observed = Arc::clone(&current);
        let gauge = PullingGauge::new(
            "source_queue_reserved_bytes",
            "Current connector FIFO byte reservations, including parked messages and partial send reservations; excludes unreserved producer batches and decode scratch",
            Box::new(move || {
                let current = observed.lock();
                let Some(budget) = current.as_ref() else {
                    return 0.0;
                };
                budget.semaphore.upgrade().map_or(0.0, |semaphore| {
                    let bytes = budget.max_bytes.saturating_sub(semaphore.available_permits());
                    // The validated cap fits u32; every charge is exactly representable in f64.
                    f64::from(u32::try_from(bytes).unwrap_or(u32::MAX))
                })
            }),
        )?;
        registry.register(Box::new(gauge))?;
        Ok(Self { current })
    }

    pub(in crate::pipeline::streaming_coordinator) fn observe(
        &self,
        rx: &SourceMsgRx,
        max_bytes: usize,
    ) {
        // A restart replaces the observation. A scrape must not keep a stopped queue alive.
        *self.current.lock() = Some(ObservedBudget {
            max_bytes,
            semaphore: Arc::downgrade(&rx.budget),
        });
    }
}
