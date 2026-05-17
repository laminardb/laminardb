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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counter_registers_and_counts() {
        let r = Registry::new();
        let h = RegHandle {
            registry: &r,
            _local: None,
        };
        let c = h.counter("test_total", "Test events");
        c.inc();
        c.inc();
        assert_eq!(c.get(), 2);
        // Registry should also see the counter.
        let mfs = r.gather();
        assert!(mfs.iter().any(|m| m.name() == "test_total"));
    }

    #[test]
    fn reg_or_local_uses_provided() {
        let provided = Registry::new();
        let mut local = None;
        let h = reg_or_local(Some(&provided), &mut local);
        let c = h.counter("provided_total", "Counter on provided registry");
        c.inc();
        assert!(local.is_none());
        assert!(provided
            .gather()
            .iter()
            .any(|m| m.name() == "provided_total"));
    }

    #[test]
    fn reg_or_local_falls_back_to_local() {
        let mut local = None;
        let h = reg_or_local(None, &mut local);
        let c = h.counter("local_total", "Counter on local registry");
        c.inc();
        assert!(local.is_some());
    }
}
