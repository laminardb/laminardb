//! Shared Prometheus counter/gauge construction for per-connector
//! metric structs. Replaces the per-file `let local; let reg = …`
//! boilerplate and `reg_c!` macros that used to be duplicated in
//! eleven places.

use std::cell::RefCell;
use std::sync::Arc;

use prometheus::core::Collector;
use prometheus::{IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Opts, Registry};

/// Borrowed registry handle used during metric-struct construction.
pub struct RegHandle<'a> {
    registry: &'a Registry,
    _local: Option<Registry>,
    /// Collectors this handle registered; a name another live instance holds is not among them.
    registered: RefCell<Vec<Box<dyn Collector>>>,
}

impl RegHandle<'_> {
    /// Register `collector`, keeping it for [`Self::into_registration`] when this handle holds
    /// the name.
    ///
    /// # Errors
    /// Returns the registry's error, such as another live instance holding the name.
    pub fn register<C: Collector + Clone + 'static>(
        &self,
        collector: &C,
    ) -> prometheus::Result<()> {
        self.registry.register(Box::new(collector.clone()))?;
        self.registered
            .borrow_mut()
            .push(Box::new(collector.clone()));
        Ok(())
    }

    /// Keep this handle's registrations until the returned guard and all its clones drop.
    ///
    /// A connector instance re-created in the same process, such as after a recovery round,
    /// can only export its own metrics once the previous instance has released the names.
    #[must_use]
    pub fn into_registration(self) -> Registration {
        Registration(Arc::new(Exported {
            registry: self.registry.clone(),
            collectors: self.registered.into_inner(),
        }))
    }

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
        let _ = self.register(&c);
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
        let _ = self.register(&g);
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
        let _ = self.register(&v);
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
        let _ = self.register(&v);
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
            registered: RefCell::default(),
        }
    } else {
        *local = Some(Registry::new());
        RegHandle {
            registry: local.as_ref().expect("just initialized"),
            _local: None,
            registered: RefCell::default(),
        }
    }
}

/// Metrics a connector instance exports, unregistered when the last clone drops.
#[derive(Clone)]
pub struct Registration(Arc<Exported>);

impl std::fmt::Debug for Registration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Registration")
            .field("collectors", &self.0.collectors.len())
            .finish()
    }
}

struct Exported {
    registry: Registry,
    collectors: Vec<Box<dyn Collector>>,
}

impl Drop for Exported {
    fn drop(&mut self) {
        for collector in self.collectors.drain(..) {
            let _ = self.registry.unregister(collector);
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

/// Assert that metrics re-created on one registry after the previous instance dropped export the
/// new instance's `counter`, bumped through `bump`.
#[cfg(test)]
pub(crate) fn assert_recreated_metrics_export<M>(
    build: impl Fn(Option<&Registry>) -> M,
    counter: &str,
    bump: impl Fn(&M, u64),
) {
    let registry = Registry::new();
    let previous = build(Some(&registry));
    bump(&previous, 3);
    drop(previous);
    let current = build(Some(&registry));
    bump(&current, 5);
    let exported = registry
        .gather()
        .into_iter()
        .find(|family| family.name() == counter)
        .map(|family| family.get_metric()[0].get_counter().value());
    assert_eq!(
        exported,
        Some(5.0),
        "{counter} must export the re-created instance"
    );
}

#[cfg(test)]
mod tests;
