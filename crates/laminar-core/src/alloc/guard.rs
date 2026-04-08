//! RAII guard for priority class enforcement (debug builds only).

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
/// `Send` so it can be held across `.await` points in single-threaded async runtimes.
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

#[cfg(test)]
mod tests {
    use super::*;

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
