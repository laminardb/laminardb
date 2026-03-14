//! RAII guard for hot path sections.
//!
//! This guard is a no-op marker that documents latency-critical sections.
//! It is `!Send` and `!Sync` to prevent accidental cross-thread movement.

use std::marker::PhantomData;

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
}
