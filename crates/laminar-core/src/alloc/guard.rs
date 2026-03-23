//! RAII guards for hot path sections and priority class enforcement.
//!
//! This module provides two guards:
//! - [`HotPathGuard`]: No-op marker documenting latency-critical sections.
//! - [`PriorityGuard`]: Debug-build enforcement of priority class contracts.

use std::marker::PhantomData;

/// Priority class for the current execution context.
///
/// Used in debug builds to verify functions run in the correct priority class.
/// In release builds, all checks are compiled away.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PriorityClass {
    /// Event processing — <10ms cycle budget, no blocking I/O.
    EventProcessing,
    /// Background I/O — checkpoint, WAL, table polling.
    BackgroundIo,
    /// Control plane — DDL, config changes, metrics.
    Control,
}

#[cfg(debug_assertions)]
std::thread_local! {
    static CURRENT_PRIORITY: std::cell::Cell<Option<PriorityClass>> = const { std::cell::Cell::new(None) };
}

/// RAII guard that sets the thread-local priority class in debug builds.
///
/// On creation, records the current priority class. On drop, restores
/// the previous value. In release builds this is a zero-size no-op.
///
/// Unlike [`HotPathGuard`], this is `Send` so it can be held across
/// `.await` points in single-threaded async runtimes.
pub struct PriorityGuard {
    #[cfg(debug_assertions)]
    previous: Option<PriorityClass>,
}

impl PriorityGuard {
    /// Enter a priority class section.
    #[inline]
    #[must_use]
    pub fn enter(#[allow(unused_variables)] class: PriorityClass) -> Self {
        #[cfg(debug_assertions)]
        {
            let previous = CURRENT_PRIORITY.with(|c| {
                let prev = c.get();
                c.set(Some(class));
                prev
            });
            Self { previous }
        }
        #[cfg(not(debug_assertions))]
        {
            Self {}
        }
    }

    /// Returns the current thread's priority class (debug builds only).
    #[inline]
    #[must_use]
    pub fn current() -> Option<PriorityClass> {
        #[cfg(debug_assertions)]
        {
            CURRENT_PRIORITY.with(std::cell::Cell::get)
        }
        #[cfg(not(debug_assertions))]
        {
            None
        }
    }
}

impl Drop for PriorityGuard {
    #[inline]
    fn drop(&mut self) {
        #[cfg(debug_assertions)]
        {
            CURRENT_PRIORITY.with(|c| c.set(self.previous));
        }
    }
}

impl std::fmt::Debug for PriorityGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PriorityGuard").finish()
    }
}

/// Assert that the current thread is running in the expected priority class.
///
/// In debug builds, panics if the current priority class does not match.
/// In release builds, this is a no-op.
#[macro_export]
macro_rules! debug_assert_priority {
    ($class:expr) => {
        #[cfg(debug_assertions)]
        {
            let current = $crate::alloc::PriorityGuard::current();
            debug_assert!(
                current == Some($class),
                "priority class mismatch: expected {:?}, got {:?}",
                $class,
                current,
            );
        }
    };
}

/// RAII guard for hot path sections.
///
/// This guard is a no-op used to document code sections that must remain
/// allocation-free. It is `!Send` and `!Sync` to prevent accidental
/// cross-thread movement.
///
/// # Example
///
/// ```rust,ignore
/// use laminar_core::alloc::HotPathGuard;
///
/// fn process_event(event: &Event) {
///     let _guard = HotPathGuard::enter("process_event");
///     // Hot path code here
/// }
/// ```
pub struct HotPathGuard {
    /// Marker to make the guard !Send and !Sync
    _marker: PhantomData<*const ()>,
}

impl HotPathGuard {
    /// Enter a hot path section (no-op).
    #[inline]
    #[must_use]
    pub fn enter(#[allow(unused_variables)] section: &'static str) -> Self {
        Self {
            _marker: PhantomData,
        }
    }

    /// Check if currently in a hot path section. Always returns false.
    #[inline]
    #[must_use]
    pub fn is_active() -> bool {
        false
    }
}

impl Drop for HotPathGuard {
    #[inline]
    fn drop(&mut self) {}
}

impl std::fmt::Debug for HotPathGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HotPathGuard").finish()
    }
}

/// Macro to mark a function as hot path.
///
/// This is a convenience macro that creates a `HotPathGuard` at the start
/// of the function with the function name as the section.
///
/// # Example
///
/// ```rust,ignore
/// use laminar_core::hot_path;
///
/// fn process_event(event: &Event) {
///     hot_path!();
///     // Hot path code...
/// }
///
/// // Or with a custom section name:
/// fn custom_section() {
///     hot_path!("custom::section::name");
///     // Hot path code...
/// }
/// ```
#[macro_export]
macro_rules! hot_path {
    () => {
        let _hot_path_guard =
            $crate::alloc::HotPathGuard::enter(concat!(module_path!(), "::", stringify!(fn)));
    };
    ($section:expr) => {
        let _hot_path_guard = $crate::alloc::HotPathGuard::enter($section);
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_guard_enter_exit() {
        assert!(!HotPathGuard::is_active());

        {
            let _guard = HotPathGuard::enter("test");
        }

        assert!(!HotPathGuard::is_active());
    }

    #[test]
    fn test_guard_debug() {
        let guard = HotPathGuard::enter("test_section");
        let debug_str = format!("{guard:?}");
        assert!(debug_str.contains("HotPathGuard"));
    }

    #[test]
    fn test_nested_guards() {
        let _outer = HotPathGuard::enter("outer");

        {
            let _inner = HotPathGuard::enter("inner");
        }
    }

    #[test]
    fn test_priority_guard_enter_exit() {
        assert_eq!(PriorityGuard::current(), None);

        {
            let _guard = PriorityGuard::enter(PriorityClass::EventProcessing);
            assert_eq!(
                PriorityGuard::current(),
                Some(PriorityClass::EventProcessing)
            );
        }

        assert_eq!(PriorityGuard::current(), None);
    }

    #[test]
    fn test_priority_guard_nesting() {
        let _outer = PriorityGuard::enter(PriorityClass::EventProcessing);
        assert_eq!(
            PriorityGuard::current(),
            Some(PriorityClass::EventProcessing)
        );

        {
            let _inner = PriorityGuard::enter(PriorityClass::BackgroundIo);
            assert_eq!(PriorityGuard::current(), Some(PriorityClass::BackgroundIo));
        }

        assert_eq!(
            PriorityGuard::current(),
            Some(PriorityClass::EventProcessing)
        );
    }

    #[test]
    fn test_debug_assert_priority() {
        let _guard = PriorityGuard::enter(PriorityClass::Control);
        debug_assert_priority!(PriorityClass::Control);
    }
}
