//! Hot path allocation detector.
//!
//! Provides a custom global allocator that tracks allocations and can panic
//! when allocations occur in marked hot path sections.

#[cfg(feature = "allocation-tracking")]
use std::alloc::{GlobalAlloc, Layout, System};
#[cfg(feature = "allocation-tracking")]
use std::cell::Cell;
use std::sync::atomic::{AtomicU64, Ordering};

/// Statistics about allocations during hot path detection.
#[derive(Debug, Default, Clone, Copy)]
pub struct AllocationStats {
    /// Total allocations detected in hot path
    pub hot_path_allocations: u64,
    /// Total bytes allocated in hot path
    pub hot_path_bytes: u64,
    /// Total allocations outside hot path
    pub normal_allocations: u64,
    /// Total bytes allocated outside hot path
    pub normal_bytes: u64,
}

// Global counters for allocation statistics
static HOT_PATH_ALLOC_COUNT: AtomicU64 = AtomicU64::new(0);
static HOT_PATH_ALLOC_BYTES: AtomicU64 = AtomicU64::new(0);
static NORMAL_ALLOC_COUNT: AtomicU64 = AtomicU64::new(0);
static NORMAL_ALLOC_BYTES: AtomicU64 = AtomicU64::new(0);

impl AllocationStats {
    /// Get current allocation statistics.
    #[must_use]
    pub fn current() -> Self {
        Self {
            hot_path_allocations: HOT_PATH_ALLOC_COUNT.load(Ordering::Relaxed),
            hot_path_bytes: HOT_PATH_ALLOC_BYTES.load(Ordering::Relaxed),
            normal_allocations: NORMAL_ALLOC_COUNT.load(Ordering::Relaxed),
            normal_bytes: NORMAL_ALLOC_BYTES.load(Ordering::Relaxed),
        }
    }

    /// Reset all counters to zero.
    pub fn reset() {
        HOT_PATH_ALLOC_COUNT.store(0, Ordering::Relaxed);
        HOT_PATH_ALLOC_BYTES.store(0, Ordering::Relaxed);
        NORMAL_ALLOC_COUNT.store(0, Ordering::Relaxed);
        NORMAL_ALLOC_BYTES.store(0, Ordering::Relaxed);
    }
}

// ============================================================================
// Feature-gated hot path detection
// ============================================================================

#[cfg(feature = "allocation-tracking")]
thread_local! {
    /// Nesting level for hot path detection (0 = not in hot path).
    static HOT_PATH_DEPTH: Cell<usize> = const { Cell::new(0) };

    /// Name of the current hot path section for error messages.
    static HOT_PATH_SECTION: Cell<Option<&'static str>> = const { Cell::new(None) };

    /// Whether to panic on allocation (vs just count).
    static PANIC_ON_ALLOC: Cell<bool> = const { Cell::new(true) };
}

/// Enable hot path detection for current thread.
///
/// Supports nesting - each call increments the depth counter.
///
/// # Safety
///
/// This should only be called through `HotPathGuard`.
#[cfg(feature = "allocation-tracking")]
pub(crate) fn enable_hot_path(section: &'static str) {
    HOT_PATH_DEPTH.with(|d| {
        let depth = d.get();
        d.set(depth + 1);
        // Only update section name on first entry
        if depth == 0 {
            HOT_PATH_SECTION.with(|s| s.set(Some(section)));
        }
    });
}

/// Disable hot path detection for current thread.
///
/// Supports nesting - only disables when all guards have been dropped.
#[cfg(feature = "allocation-tracking")]
pub(crate) fn disable_hot_path() {
    HOT_PATH_DEPTH.with(|d| {
        let depth = d.get();
        if depth > 0 {
            d.set(depth - 1);
            if depth == 1 {
                // Last guard dropped, clear section
                HOT_PATH_SECTION.with(|s| s.set(None));
            }
        }
    });
}

/// Check if hot path is currently enabled.
#[cfg(feature = "allocation-tracking")]
#[must_use]
pub fn is_hot_path_enabled() -> bool {
    HOT_PATH_DEPTH.with(|d| d.get() > 0)
}

/// Set whether to panic on hot path allocation.
///
/// When false, allocations are counted but don't panic.
/// Useful for collecting stats without crashing.
#[cfg(feature = "allocation-tracking")]
pub fn set_panic_on_alloc(panic: bool) {
    PANIC_ON_ALLOC.with(|p| p.set(panic));
}

/// Check allocation in hot path and optionally panic.
#[cfg(feature = "allocation-tracking")]
#[cold]
#[inline(never)]
fn check_hot_path_allocation(op: &str, size: usize) {
    let is_hot = HOT_PATH_DEPTH.with(|d| d.get() > 0);

    if is_hot {
        // Record the allocation
        HOT_PATH_ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
        HOT_PATH_ALLOC_BYTES.fetch_add(size as u64, Ordering::Relaxed);

        // Check if we should panic
        let should_panic = PANIC_ON_ALLOC.with(Cell::get);
        if should_panic {
            let section = HOT_PATH_SECTION.with(|s| s.get().unwrap_or("unknown"));
            panic!(
                "\n\
                 ╔══════════════════════════════════════════════════════════════╗\n\
                 ║              ALLOCATION IN HOT PATH DETECTED!                ║\n\
                 ╠══════════════════════════════════════════════════════════════╣\n\
                 ║ Operation: {op:50} ║\n\
                 ║ Size:      {size:50} ║\n\
                 ║ Section:   {section:50} ║\n\
                 ╠══════════════════════════════════════════════════════════════╣\n\
                 ║ Hot path code must be zero-allocation.                       ║\n\
                 ║ Use pre-allocated buffers, ArrayVec, or ObjectPool instead.  ║\n\
                 ╚══════════════════════════════════════════════════════════════╝\n",
                op = op,
                size = format!("{size} bytes"),
                section = section
            );
        }
    } else {
        NORMAL_ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
        NORMAL_ALLOC_BYTES.fetch_add(size as u64, Ordering::Relaxed);
    }
}

// ============================================================================
// Global allocator (only with feature enabled)
// ============================================================================

/// Global allocator that detects hot path allocations.
///
/// When the `allocation-tracking` feature is enabled and hot path detection
/// is active for a thread, this allocator will panic on any heap allocation.
///
/// # Example
///
/// To enable this allocator in your binary:
///
/// ```rust,ignore
/// use laminar_core::alloc::HotPathDetectingAlloc;
///
/// #[global_allocator]
/// static ALLOC: HotPathDetectingAlloc = HotPathDetectingAlloc::new();
/// ```
#[cfg(feature = "allocation-tracking")]
pub struct HotPathDetectingAlloc {
    inner: System,
}

#[cfg(feature = "allocation-tracking")]
impl HotPathDetectingAlloc {
    /// Create a new hot path detecting allocator.
    #[must_use]
    pub const fn new() -> Self {
        Self { inner: System }
    }
}

#[cfg(feature = "allocation-tracking")]
impl Default for HotPathDetectingAlloc {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(feature = "allocation-tracking")]
// SAFETY: We delegate all allocation operations to the System allocator,
// which is safe. We only add tracking/panic logic around the calls.
unsafe impl GlobalAlloc for HotPathDetectingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        check_hot_path_allocation("alloc", layout.size());
        // SAFETY: Delegating to System allocator which is safe
        self.inner.alloc(layout)
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // Dealloc is allowed in hot path - we're freeing memory, not allocating
        // SAFETY: Delegating to System allocator which is safe
        self.inner.dealloc(ptr, layout);
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // Realloc may allocate more memory
        if new_size > layout.size() {
            check_hot_path_allocation("realloc", new_size);
        }
        // SAFETY: Delegating to System allocator which is safe
        self.inner.realloc(ptr, layout, new_size)
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        check_hot_path_allocation("alloc_zeroed", layout.size());
        // SAFETY: Delegating to System allocator which is safe
        self.inner.alloc_zeroed(layout)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stats_current_and_reset() {
        AllocationStats::reset();

        let stats = AllocationStats::current();
        assert_eq!(stats.hot_path_allocations, 0);
        assert_eq!(stats.normal_allocations, 0);
    }

    #[test]
    #[cfg(feature = "allocation-tracking")]
    fn test_hot_path_enable_disable() {
        assert!(!is_hot_path_enabled());

        enable_hot_path("test_section");
        assert!(is_hot_path_enabled());

        disable_hot_path();
        assert!(!is_hot_path_enabled());
    }
}
