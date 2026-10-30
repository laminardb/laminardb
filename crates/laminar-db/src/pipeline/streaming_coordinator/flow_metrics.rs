//! Scrape-time observation of the coordinator's source queue and intake gates.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use prometheus::core::{Collector, Desc};
use prometheus::proto::MetricFamily;
use prometheus::{CounterVec, Gauge, IntGauge, Opts, Registry};
use rustc_hash::FxHashMap;

use super::source_channel::QueueObservation;

/// Why the coordinator is not draining its source queue at full rate. Reasons are exclusive:
/// a fence takes precedence over output pressure, which takes precedence over port pressure.
#[derive(Clone, Copy)]
pub(super) enum IntakePause {
    /// A recovery, vnode-transition or handoff fence closed intake.
    Fence,
    /// Cluster subscription output is above its pause threshold.
    Output,
    /// A source message waits in the parked slot for room on its source's graph ports.
    PortBackpressure,
}

const PAUSE_REASONS: [IntakePause; 3] = [
    IntakePause::Fence,
    IntakePause::Output,
    IntakePause::PortBackpressure,
];

impl IntakePause {
    fn label(self) -> &'static str {
        match self {
            Self::Fence => "fence",
            Self::Output => "output",
            Self::PortBackpressure => "port_backpressure",
        }
    }
}

#[derive(Clone, Copy, Default)]
struct PauseClock {
    total: Duration,
    since: Option<Instant>,
}

/// Intake observations of one coordinator: edge-timed pause clocks and the FIFO depth at its
/// latest wake. Only a pause starting or ending reads the clock or takes the lock; the lock
/// makes a scrape see each pause either open or closed, never both.
#[derive(Default)]
pub(super) struct IntakeStats {
    paused: [AtomicBool; 3],
    clocks: Mutex<[PauseClock; 3]>,
    queued_messages: AtomicUsize,
}

impl IntakeStats {
    pub(super) fn observe_queue(&self, messages: usize) {
        self.queued_messages.store(messages, Ordering::Relaxed);
    }

    pub(super) fn observe(&self, reason: IntakePause, paused: bool) {
        let index = reason as usize;
        if self.paused[index].load(Ordering::Relaxed) == paused {
            return;
        }
        self.paused[index].store(paused, Ordering::Relaxed);
        let now = Instant::now();
        let clock = &mut self.clocks.lock()[index];
        if paused {
            clock.since = Some(now);
        } else if let Some(since) = clock.since.take() {
            clock.total += now.saturating_duration_since(since);
        }
    }

    pub(super) fn close(&self) {
        for reason in PAUSE_REASONS {
            self.observe(reason, false);
        }
        self.observe_queue(0);
    }

    fn paused(&self, reason: IntakePause, now: Instant) -> Duration {
        let clock = self.clocks.lock()[reason as usize];
        clock.total + clock.since.map_or(Duration::ZERO, |since| now - since)
    }
}

#[derive(Default)]
struct Observed {
    queue: Option<QueueObservation>,
    intake: Option<Arc<IntakeStats>>,
    // Totals of replaced pipelines, so the counters never decrease within the process.
    retired_waits: FxHashMap<Arc<str>, Duration>,
    retired_paused: [Duration; 3],
}

impl Observed {
    fn retire(&mut self, now: Instant) {
        if let Some(queue) = self.queue.take() {
            for (source, waited_ns) in queue.waits.totals() {
                *self.retired_waits.entry(source).or_default() += Duration::from_nanos(waited_ns);
            }
        }
        if let Some(intake) = self.intake.take() {
            for reason in PAUSE_REASONS {
                self.retired_paused[reason as usize] += intake.paused(reason, now);
            }
        }
    }
}

struct Families {
    observed: Mutex<Observed>,
    reserved_bytes: Gauge,
    messages: IntGauge,
    source_wait: CounterVec,
    intake_paused: CounterVec,
}

impl Families {
    fn refresh(&self, observed: &Observed) {
        let now = Instant::now();
        let queue = observed.queue.as_ref();
        // The validated cap fits u32; every charge is exactly representable in f64.
        let reserved = queue.map_or(0, QueueObservation::reserved_bytes);
        self.reserved_bytes
            .set(f64::from(u32::try_from(reserved).unwrap_or(u32::MAX)));
        let messages = observed
            .intake
            .as_ref()
            .map_or(0, |intake| intake.queued_messages.load(Ordering::Relaxed));
        self.messages
            .set(i64::try_from(messages).unwrap_or(i64::MAX));
        let mut waits = observed.retired_waits.clone();
        for (source, waited_ns) in queue.map(|queue| queue.waits.totals()).unwrap_or_default() {
            *waits.entry(source).or_default() += Duration::from_nanos(waited_ns);
        }
        self.source_wait.reset();
        for (source, waited) in waits {
            self.source_wait
                .with_label_values(&[&*source])
                .inc_by(waited.as_secs_f64());
        }
        self.intake_paused.reset();
        for reason in PAUSE_REASONS {
            let current = observed
                .intake
                .as_ref()
                .map_or(Duration::ZERO, |intake| intake.paused(reason, now));
            self.intake_paused
                .with_label_values(&[reason.label()])
                .inc_by((observed.retired_paused[reason as usize] + current).as_secs_f64());
        }
    }
}

struct FamiliesCollector(Arc<Families>);

impl Collector for FamiliesCollector {
    fn desc(&self) -> Vec<&Desc> {
        let families = &*self.0;
        let mut descs = families.reserved_bytes.desc();
        descs.extend(families.messages.desc());
        descs.extend(families.source_wait.desc());
        descs.extend(families.intake_paused.desc());
        descs
    }

    fn collect(&self) -> Vec<MetricFamily> {
        let families = &*self.0;
        // Held until collected: a concurrent scrape's reset must not empty these counters.
        let observed = families.observed.lock();
        families.refresh(&observed);
        let mut collected = families.reserved_bytes.collect();
        collected.extend(families.messages.collect());
        collected.extend(families.source_wait.collect());
        collected.extend(families.intake_paused.collect());
        collected
    }
}

/// Source queue and intake series of the running coordinator, read when Prometheus scrapes.
pub(crate) struct CoordinatorFlowMetrics {
    families: Arc<Families>,
}

impl CoordinatorFlowMetrics {
    pub(crate) fn register(registry: &Registry) -> Result<Self, prometheus::Error> {
        let families = Arc::new(Families {
            observed: Mutex::new(Observed::default()),
            reserved_bytes: Gauge::new(
                "source_queue_reserved_bytes",
                "Current connector FIFO byte reservations, including parked messages and partial send reservations; excludes unreserved producer batches and decode scratch",
            )?,
            messages: IntGauge::new(
                "source_queue_messages",
                "Messages in the shared connector FIFO at the coordinator's latest wake, excluding its parked message",
            )?,
            source_wait: CounterVec::new(
                Opts::new(
                    "source_queue_wait_seconds_total",
                    "Time each source waited for connector FIFO bytes or a FIFO slot",
                ),
                &["source"],
            )?,
            intake_paused: CounterVec::new(
                Opts::new(
                    "intake_paused_seconds_total",
                    "Time the coordinator held source intake back: fence, output or port_backpressure",
                ),
                &["reason"],
            )?,
        });
        registry.register(Box::new(FamiliesCollector(Arc::clone(&families))))?;
        Ok(Self { families })
    }

    pub(super) fn observe(&self, queue: QueueObservation, intake: Arc<IntakeStats>) {
        // A restart replaces the observation. A scrape must not keep a stopped queue alive.
        let mut observed = self.families.observed.lock();
        observed.retire(Instant::now());
        observed.queue = Some(queue);
        observed.intake = Some(intake);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn paused_seconds(registry: &Registry, reason: &str) -> f64 {
        registry
            .gather()
            .into_iter()
            .find(|family| family.name() == "intake_paused_seconds_total")
            .unwrap()
            .get_metric()
            .iter()
            .find(|metric| metric.get_label()[0].value() == reason)
            .unwrap()
            .get_counter()
            .value()
    }

    #[test]
    fn pauses_count_while_open_stop_when_closed_and_survive_restart() {
        let registry = Registry::new();
        let metrics = CoordinatorFlowMetrics::register(&registry).unwrap();
        let (_tx, rx) = super::super::source_channel::channel(1, 1024);
        let intake = Arc::new(IntakeStats::default());
        metrics.observe(rx.observation(1024), Arc::clone(&intake));
        intake.observe(IntakePause::Fence, true);
        std::thread::sleep(Duration::from_millis(5));
        let open = paused_seconds(&registry, "fence");
        assert!(
            open >= 0.005,
            "an open pause is visible before it ends: {open}"
        );
        intake.observe(IntakePause::Fence, true);
        intake.observe(IntakePause::Fence, false);
        let closed = paused_seconds(&registry, "fence");
        std::thread::sleep(Duration::from_millis(5));
        assert_eq!(paused_seconds(&registry, "fence"), closed);
        assert_eq!(paused_seconds(&registry, "output"), 0.0);

        intake.observe(IntakePause::PortBackpressure, true);
        intake.close();
        let backpressured = paused_seconds(&registry, "port_backpressure");
        metrics.observe(rx.observation(1024), Arc::default());
        assert_eq!(paused_seconds(&registry, "fence"), closed);
        assert_eq!(
            paused_seconds(&registry, "port_backpressure"),
            backpressured
        );
    }
}
