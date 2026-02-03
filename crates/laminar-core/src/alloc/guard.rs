//! RAII guard for hot path sections.
//!
//! Provides automatic enable/disable of allocation detection.

use std::marker::PhantomData;

#[cfg(feature = "allocation-tracking")]
use super::detector;

/// RAII guard for hot path sections.
///
/// When created, enables allocation detection for the current thread.
/// When dropped, disables allocation detection.
///
/// # Example
///
/// ```rust,ignore
/// use laminar_core::alloc::HotPathGuard;
///
/// fn process_event(event: &Event) {
///     // Any allocation after this will panic (with allocation-tracking feature)
///     let _guard = HotPathGuard::enter("process_event");
///
///     // Do hot path processing...
///     // Vec::new() here would panic!
///
/// } // Guard dropped here, allocation detection disabled
/// ```
///
/// # Zero-Cost in Release Builds
///
/// Without the `allocation-tracking` feature, this guard compiles to a no-op.
/// The enter/drop methods are inlined to nothing.
pub struct HotPathGuard {
    /// Section name for error messages
    #[cfg(feature = "allocation-tracking")]
    section: &'static str,

    /// Marker to make the guard !Send and !Sync
    /// Hot path detection is thread-local, so guards shouldn't be moved between threads
    _marker: PhantomData<*const ()>,
}

impl HotPathGuard {
    /// Enter a hot path section.
    ///
    /// After calling this, any heap allocation on the current thread will panic
    /// (when `allocation-tracking` feature is enabled).
    ///
    /// # Arguments
    ///
    /// * `section` - Name of the hot path section (used in error messages)
    ///
    /// # Returns
    ///
    /// A guard that will disable detection when dropped.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let _guard = HotPathGuard::enter("reactor::poll");
    /// // Hot path code here
    /// ```
    #[inline]
    #[must_use]
    pub fn enter(#[allow(unused_variables)] section: &'static str) -> Self {
        #[cfg(feature = "allocation-tracking")]
        detector::enable_hot_path(section);

        Self {
            #[cfg(feature = "allocation-tracking")]
            section,
            _marker: PhantomData,
        }
    }

    /// Check if currently in a hot path section.
    #[inline]
    #[must_use]
    pub fn is_active() -> bool {
        #[cfg(feature = "allocation-tracking")]
        {
            detector::is_hot_path_enabled()
        }
        #[cfg(not(feature = "allocation-tracking"))]
        {
            false
        }
    }

    /// Get the current section name, if in hot path.
    #[inline]
    #[must_use]
    #[cfg(feature = "allocation-tracking")]
    pub fn current_section(&self) -> &'static str {
        self.section
    }
}

impl Drop for HotPathGuard {
    #[inline]
    fn drop(&mut self) {
        #[cfg(feature = "allocation-tracking")]
        detector::disable_hot_path();
    }
}

// Implement Debug for better error messages
impl std::fmt::Debug for HotPathGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        #[cfg(feature = "allocation-tracking")]
        {
            f.debug_struct("HotPathGuard")
                .field("section", &self.section)
                .finish()
        }
        #[cfg(not(feature = "allocation-tracking"))]
        {
            f.debug_struct("HotPathGuard").finish()
        }
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
        // Initially not in hot path
        assert!(!HotPathGuard::is_active());

        {
            let _guard = HotPathGuard::enter("test");

            #[cfg(feature = "allocation-tracking")]
            assert!(HotPathGuard::is_active());
        }

        // After guard dropped
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

        #[cfg(feature = "allocation-tracking")]
        assert!(HotPathGuard::is_active());

        {
            let _inner = HotPathGuard::enter("inner");

            #[cfg(feature = "allocation-tracking")]
            assert!(HotPathGuard::is_active());
        }

        // Outer guard still active after inner dropped
        // Note: This is expected behavior - the outer guard keeps detection enabled
        #[cfg(feature = "allocation-tracking")]
        assert!(HotPathGuard::is_active());
    }
}
