//! Separate allocation-count run; instrumentation is absent from the timing build.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

static ACTIVE: AtomicBool = AtomicBool::new(false);
static REQUESTS: AtomicU64 = AtomicU64::new(0);
static BYTES: AtomicU64 = AtomicU64::new(0);

struct AllocationProbe;

// SAFETY: every allocation/deallocation forwards the original pointer and layout to System.
// The counters neither access allocated memory nor change allocator ownership.
unsafe impl GlobalAlloc for AllocationProbe {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: the caller supplies the valid allocation layout required by GlobalAlloc.
        let pointer = unsafe { System.alloc(layout) };
        if ACTIVE.load(Ordering::Relaxed) && !pointer.is_null() {
            REQUESTS.fetch_add(1, Ordering::Relaxed);
            BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: System owns this pointer and receives its unchanged original layout.
        unsafe { System.dealloc(pointer, layout) };
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        // SAFETY: original pointer/layout and requested size are forwarded unchanged.
        let pointer = unsafe { System.realloc(pointer, layout, size) };
        if ACTIVE.load(Ordering::Relaxed) && !pointer.is_null() {
            REQUESTS.fetch_add(1, Ordering::Relaxed);
            BYTES.fetch_add(size as u64, Ordering::Relaxed);
        }
        pointer
    }
}

#[global_allocator]
static ALLOCATOR: AllocationProbe = AllocationProbe;

pub(super) fn probe<T>(label: &str, mut work: impl FnMut() -> T) {
    REQUESTS.store(0, Ordering::Relaxed);
    BYTES.store(0, Ordering::Relaxed);
    ACTIVE.store(true, Ordering::Relaxed);
    let output = work();
    ACTIVE.store(false, Ordering::Relaxed);
    let requests = REQUESTS.load(Ordering::Relaxed);
    let bytes = BYTES.load(Ordering::Relaxed);
    std::hint::black_box(output);
    println!("schema_contract_allocations {label}: requests={requests}; requested_bytes={bytes}; one_operation; excludes fixture construction; bytes are cumulative requests, not peak memory");
}
