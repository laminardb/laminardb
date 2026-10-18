//! Shared Prometheus counter/gauge construction for per-connector
//! metric structs. Replaces the per-file `let local; let reg = …`
//! boilerplate and `reg_c!` macros that used to be duplicated in
//! eleven places.

use prometheus::{IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Opts, Registry};

/// Borrowed registry handle used during metric-struct construction.
pub struct RegHandle<'a> {
    registry: &'a Registry,
    _local: Option<Registry>,
}

impl RegHandle<'_> {
    /// Borrow the inner registry for non-helper registrations (e.g. `Histogram`).
    #[must_use]
    pub fn registry(&self) -> &Registry {
        self.registry
    }

    /// Register an `IntCounter`.
    ///
    /// # Panics
    ///
    /// Panics if `name`/`help` is not a valid Prometheus identifier.
    #[must_use]
    pub fn counter(&self, name: &str, help: &str) -> IntCounter {
        let c =
            IntCounter::new(name, help).unwrap_or_else(|e| panic!("invalid counter '{name}': {e}"));
        self.registry.register(Box::new(c.clone())).ok();
        c
    }

    /// Register an `IntGauge`.
    ///
    /// # Panics
    ///
    /// Panics if `name`/`help` is not a valid Prometheus identifier.
    #[must_use]
    pub fn gauge(&self, name: &str, help: &str) -> IntGauge {
        let g = IntGauge::new(name, help).unwrap_or_else(|e| panic!("invalid gauge '{name}': {e}"));
        self.registry.register(Box::new(g.clone())).ok();
        g
    }

    /// Register an `IntCounterVec`.
    ///
    /// # Panics
    ///
    /// Panics if `name`/`help`/`labels` are not valid Prometheus identifiers.
    #[must_use]
    pub fn counter_vec(&self, name: &str, help: &str, labels: &[&str]) -> IntCounterVec {
        let v = IntCounterVec::new(Opts::new(name, help), labels)
            .unwrap_or_else(|e| panic!("invalid counter_vec '{name}': {e}"));
        self.registry.register(Box::new(v.clone())).ok();
        v
    }

    /// Register an `IntGaugeVec`.
    ///
    /// # Panics
    ///
    /// Panics if `name`/`help`/`labels` are not valid Prometheus identifiers.
    #[must_use]
    pub fn gauge_vec(&self, name: &str, help: &str, labels: &[&str]) -> IntGaugeVec {
        let v = IntGaugeVec::new(Opts::new(name, help), labels)
            .unwrap_or_else(|e| panic!("invalid gauge_vec '{name}': {e}"));
        self.registry.register(Box::new(v.clone())).ok();
        v
    }
}

/// `RegHandle` over `registry`, falling back to a fresh registry parked
/// in `local` (which must outlive construction). Pattern:
/// `let mut local = None; let reg = reg_or_local(registry, &mut local);`
#[must_use]
#[allow(clippy::missing_panics_doc)] // `expect` follows an unconditional `*local = Some(..)`
pub fn reg_or_local<'a>(
    registry: Option<&'a Registry>,
    local: &'a mut Option<Registry>,
) -> RegHandle<'a> {
    if let Some(r) = registry {
        RegHandle {
            registry: r,
            _local: None,
        }
    } else {
        *local = Some(Registry::new());
        RegHandle {
            registry: local.as_ref().expect("just initialized"),
            _local: None,
        }
    }
}

/// Metric families of one connector instance, labelled by one bounded constant label and
/// unregistered on drop so a restarted generation can register again.
///
/// A family another live instance already exports keeps this instance's metrics unexported
/// rather than failing connector startup.
pub struct LabelledFamilies {
    label: (&'static str, String),
    counters: Vec<IntCounter>,
    gauges: Vec<IntGauge>,
    registry: Option<Registry>,
}

impl LabelledFamilies {
    /// Start a set labelled `key = value`.
    #[must_use]
    pub fn new(key: &'static str, value: &str) -> Self {
        Self {
            label: (key, value.to_string()),
            counters: Vec::new(),
            gauges: Vec::new(),
            registry: None,
        }
    }

    /// A labelled counter added to this set.
    ///
    /// # Errors
    /// Returns an error when `name` or `help` is not a valid descriptor.
    pub fn counter(&mut self, name: &str, help: &str) -> Result<IntCounter, prometheus::Error> {
        let counter =
            IntCounter::with_opts(Opts::new(name, help).const_label(self.label.0, &self.label.1))?;
        self.counters.push(counter.clone());
        Ok(counter)
    }

    /// A labelled gauge added to this set.
    ///
    /// # Errors
    /// Returns an error when `name` or `help` is not a valid descriptor.
    pub fn gauge(&mut self, name: &str, help: &str) -> Result<IntGauge, prometheus::Error> {
        let gauge =
            IntGauge::with_opts(Opts::new(name, help).const_label(self.label.0, &self.label.1))?;
        self.gauges.push(gauge.clone());
        Ok(gauge)
    }

    fn collectors(&self) -> Vec<Box<dyn prometheus::core::Collector>> {
        let counters = self
            .counters
            .iter()
            .map(|counter| Box::new(counter.clone()) as Box<dyn prometheus::core::Collector>);
        let gauges = self
            .gauges
            .iter()
            .map(|gauge| Box::new(gauge.clone()) as Box<dyn prometheus::core::Collector>);
        counters.chain(gauges).collect()
    }

    /// Register every family on `registry`, rolling back on the first failure.
    #[must_use]
    pub fn register(mut self, registry: Option<&Registry>) -> Self {
        let Some(registry) = registry else {
            return self;
        };
        for (index, collector) in self.collectors().into_iter().enumerate() {
            if let Err(error) = registry.register(collector) {
                for previous in self.collectors().into_iter().take(index) {
                    let _ = registry.unregister(previous);
                }
                tracing::warn!(%error, "connector metrics are not exported for this instance");
                return self;
            }
        }
        self.registry = Some(registry.clone());
        self
    }
}

impl Drop for LabelledFamilies {
    fn drop(&mut self) {
        if let Some(registry) = self.registry.take() {
            for collector in self.collectors() {
                let _ = registry.unregister(collector);
            }
        }
    }
}

#[cfg(test)]
mod tests;
