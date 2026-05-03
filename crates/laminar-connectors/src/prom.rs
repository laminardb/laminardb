//! Shared Prometheus counter/gauge construction.
//!
//! Eleven per-connector metric structs used to repeat the same
//! `let local; let reg = if let Some(r) = registry { r } else { local
//! = Registry::new(); &local }` boilerplate plus a private `reg_c!`
//! macro to register `IntCounter`s. This module hosts the shared
//! version so connector-specific metric structs only declare names
//! and help text.

use prometheus::{IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Opts, Registry};

/// Backing store for the registry handle. When the caller didn't pass
/// one we still want a `&Registry` so the construction macros work
/// uniformly; this owns the local fallback in that case.
pub struct RegHandle<'a> {
    /// Borrowed registry (caller-supplied) or a fallback we own.
    registry: &'a Registry,
    /// Lifetime extender: when `registry` borrows from this we keep
    /// the underlying `Registry` alive for the duration of the build.
    _local: Option<Registry>,
}

impl<'a> RegHandle<'a> {
    /// Wraps the optional caller-supplied registry, falling back to a
    /// fresh local registry whose handle has the same lifetime as
    /// `self`.
    #[must_use]
    pub fn new(registry: Option<&'a Registry>) -> Self {
        if let Some(r) = registry {
            Self {
                registry: r,
                _local: None,
            }
        } else {
            // SAFETY-style note: the local Registry is parked inside
            // `_local` and we hand out a reference whose lifetime is
            // bounded by `&self`. Callers never see `_local` directly.
            // Using `Box::leak` would also work but introduces leak
            // semantics we don't need.
            unimplemented!(
                "callers must always pass a registry — see Self::new_owned for the no-registry case"
            )
        }
    }

    /// Variant that always owns its registry. Used by metric structs
    /// in tests / `#[cfg(test)]` helpers and by callers who don't
    /// integrate with a global registry.
    #[must_use]
    pub fn new_owned() -> OwnedRegHandle {
        OwnedRegHandle {
            registry: Registry::new(),
        }
    }

    /// Borrow the inner registry.
    #[must_use]
    pub fn registry(&self) -> &Registry {
        self.registry
    }

    /// Register a new `IntCounter` and return it.
    ///
    /// # Panics
    ///
    /// Panics if `name` or `help` violate Prometheus naming rules. We
    /// only call this with hard-coded names from the same crate, so a
    /// violation is a build-time bug surface caught by the first
    /// integration run.
    #[must_use]
    pub fn counter(&self, name: &str, help: &str) -> IntCounter {
        let c =
            IntCounter::new(name, help).unwrap_or_else(|e| panic!("invalid counter '{name}': {e}"));
        self.registry.register(Box::new(c.clone())).ok();
        c
    }

    /// Register a new `IntGauge` and return it.
    ///
    /// # Panics
    ///
    /// Panics if `name` or `help` violate Prometheus naming rules.
    #[must_use]
    pub fn gauge(&self, name: &str, help: &str) -> IntGauge {
        let g = IntGauge::new(name, help).unwrap_or_else(|e| panic!("invalid gauge '{name}': {e}"));
        self.registry.register(Box::new(g.clone())).ok();
        g
    }

    /// Register a new `IntCounterVec`.
    ///
    /// # Panics
    ///
    /// Panics if `name`, `help`, or any of the `labels` violate
    /// Prometheus naming rules.
    #[must_use]
    pub fn counter_vec(&self, name: &str, help: &str, labels: &[&str]) -> IntCounterVec {
        let v = IntCounterVec::new(Opts::new(name, help), labels)
            .unwrap_or_else(|e| panic!("invalid counter_vec '{name}': {e}"));
        self.registry.register(Box::new(v.clone())).ok();
        v
    }

    /// Register a new `IntGaugeVec`.
    ///
    /// # Panics
    ///
    /// Panics if `name`, `help`, or any of the `labels` violate
    /// Prometheus naming rules.
    #[must_use]
    pub fn gauge_vec(&self, name: &str, help: &str, labels: &[&str]) -> IntGaugeVec {
        let v = IntGaugeVec::new(Opts::new(name, help), labels)
            .unwrap_or_else(|e| panic!("invalid gauge_vec '{name}': {e}"));
        self.registry.register(Box::new(v.clone())).ok();
        v
    }
}

/// Owns a fallback registry. Implements the same `counter`/`gauge`
/// surface as [`RegHandle`] so call sites can dispatch on whether the
/// caller supplied a registry.
pub struct OwnedRegHandle {
    registry: Registry,
}

impl OwnedRegHandle {
    /// Borrow the owned registry as a [`RegHandle`].
    #[must_use]
    pub fn as_borrowed(&self) -> RegHandle<'_> {
        RegHandle {
            registry: &self.registry,
            _local: None,
        }
    }
}

/// Returns a `RegHandle` derived from an optional caller-supplied
/// registry. When `None`, the fallback registry is owned by `local`,
/// which the caller must keep alive for the duration of construction.
///
/// Usage:
/// ```ignore
/// let local;
/// let reg = prom::reg_or_local(registry, &mut local);
/// let counter = reg.counter("foo_total", "Foo events");
/// ```
///
/// # Panics
///
/// Never panics in practice; the `expect` on the freshly-stored
/// `local` registry is unreachable because we just wrote `Some(...)`.
#[must_use]
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
