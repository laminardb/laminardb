# F071: Zero-Allocation Enforcement

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F071 |
| **Status** | ✅ Done |
| **Priority** | P0 |
| **Phase** | 2 |
| **Effort** | M (3-5 days) |
| **Dependencies** | F013 |
| **Owner** | TBD |
| **Research** | [Thread-Per-Core 2026 Research](../../research/laminardb-thread-per-core-2026-research.md) |

## Summary

Implement zero-allocation enforcement for Ring 0 hot path code. In debug/test builds, a custom allocator detects and panics on any heap allocation within marked hot path sections. In release builds, pre-allocation patterns ensure allocations happen only at startup.

## Motivation

### Why Zero-Allocation Matters

| Issue | Impact on Latency |
|-------|-------------------|
| Single `Vec::push()` with resize | +1-10μs |
| `HashMap::insert()` with rehash | +5-50μs |
| `Box::new()` | +100-500ns |
| `String::from()` | +200-1000ns |

For Ring 0 target of <500ns, a single allocation can blow the budget.

### Common Hidden Allocations

```rust
// Hidden allocations that violate Ring 0 constraints

fn process_event_bad(event: &Event) {
    // Allocates! (Vec clone)
    let key = event.key.to_vec();

    // Allocates! (String format)
    let msg = format!("Processing {}", event.id);

    // Allocates! (Vec collect)
    let results: Vec<_> = items.iter().collect();

    // May allocate! (HashMap growth)
    map.insert(key, value);

    // Allocates! (Box)
    let boxed = Box::new(result);
}
```

### Current Gap

| Aspect | Current | Target |
|--------|---------|--------|
| Allocation detection | None | Debug-mode panics |
| Pre-allocation | Partial (Vec::with_capacity) | Comprehensive |
| CI enforcement | None | Test fails on allocation |
| Object pools | None | For common types |

## Goals

1. Implement allocation detector for debug builds
2. Add `#[hot_path]` attribute macro for marking sections
3. Provide pre-allocation patterns (ObjectPool, ArrayVec)
4. CI check that fails on hot path allocation
5. Audit and fix existing Ring 0 code

## Non-Goals

- Zero-allocation in Ring 1/2 (allowed to allocate)
- Stack size limits (separate concern)
- Custom allocator for release builds

## Technical Design

### Allocation Detector

```rust
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

/// Global allocator that detects hot path allocations.
///
/// In debug builds, panics if allocation occurs during hot path.
/// In release builds, delegates directly to system allocator.
#[cfg(debug_assertions)]
pub struct HotPathDetectingAlloc {
    inner: System,
}

#[cfg(debug_assertions)]
thread_local! {
    /// Whether hot path detection is enabled for this thread.
    static HOT_PATH_ENABLED: Cell<bool> = Cell::new(false);
    /// Stack of hot path sections for better error messages.
    static HOT_PATH_STACK: Cell<Option<&'static str>> = Cell::new(None);
}

#[cfg(debug_assertions)]
impl HotPathDetectingAlloc {
    pub const fn new() -> Self {
        Self { inner: System }
    }
}

#[cfg(debug_assertions)]
unsafe impl GlobalAlloc for HotPathDetectingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        check_no_alloc("alloc", layout.size());
        self.inner.alloc(layout)
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // Dealloc allowed in hot path (for now)
        self.inner.dealloc(ptr, layout)
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        check_no_alloc("realloc", new_size);
        self.inner.realloc(ptr, layout, new_size)
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        check_no_alloc("alloc_zeroed", layout.size());
        self.inner.alloc_zeroed(layout)
    }
}

#[cfg(debug_assertions)]
fn check_no_alloc(op: &str, size: usize) {
    HOT_PATH_ENABLED.with(|enabled| {
        if enabled.get() {
            let section = HOT_PATH_STACK.with(|s| s.get().unwrap_or("unknown"));
            panic!(
                "ALLOCATION IN HOT PATH!\n\
                 Operation: {} ({} bytes)\n\
                 Section: {}\n\
                 \n\
                 Hot path code must be zero-allocation.\n\
                 Use pre-allocated buffers or ArrayVec instead.",
                op, size, section
            );
        }
    });
}

// Register as global allocator
#[cfg(debug_assertions)]
#[global_allocator]
static ALLOC: HotPathDetectingAlloc = HotPathDetectingAlloc::new();
```

### Hot Path Guard

```rust
/// RAII guard for hot path sections.
///
/// Enables allocation detection on creation, disables on drop.
#[cfg(debug_assertions)]
pub struct HotPathGuard {
    section: &'static str,
    _marker: std::marker::PhantomData<*const ()>, // !Send + !Sync
}

#[cfg(debug_assertions)]
impl HotPathGuard {
    /// Enter hot path section. Any allocation will panic.
    #[inline]
    pub fn enter(section: &'static str) -> Self {
        HOT_PATH_ENABLED.with(|e| e.set(true));
        HOT_PATH_STACK.with(|s| s.set(Some(section)));
        Self {
            section,
            _marker: std::marker::PhantomData,
        }
    }
}

#[cfg(debug_assertions)]
impl Drop for HotPathGuard {
    fn drop(&mut self) {
        HOT_PATH_ENABLED.with(|e| e.set(false));
        HOT_PATH_STACK.with(|s| s.set(None));
    }
}

// No-op in release builds
#[cfg(not(debug_assertions))]
pub struct HotPathGuard;

#[cfg(not(debug_assertions))]
impl HotPathGuard {
    #[inline(always)]
    pub fn enter(_section: &'static str) -> Self {
        Self
    }
}
```

### Hot Path Attribute Macro

```rust
/// Attribute macro to mark function as hot path.
///
/// In debug builds, enables allocation detection for function body.
/// In release builds, no-op.
///
/// # Example
/// ```
/// #[hot_path]
/// fn process_event(event: &Event) {
///     // Any allocation here will panic in debug builds
/// }
/// ```
#[proc_macro_attribute]
pub fn hot_path(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemFn);
    let name = &input.sig.ident;
    let block = &input.block;
    let attrs = &input.attrs;
    let vis = &input.vis;
    let sig = &input.sig;

    let section_name = format!("{}::{}", module_path!(), name);

    quote! {
        #(#attrs)*
        #vis #sig {
            let _guard = crate::alloc::HotPathGuard::enter(#section_name);
            #block
        }
    }.into()
}
```

### Pre-Allocation Patterns

#### Object Pool

```rust
use arrayvec::ArrayVec;

/// Fixed-size object pool for frequently allocated types.
///
/// Pre-allocates objects at creation, provides O(1) acquire/release.
/// Used in Ring 0 to avoid heap allocation.
pub struct ObjectPool<T, const N: usize> {
    /// Free objects (stack-allocated array)
    free_list: ArrayVec<T, N>,
    /// Number of objects currently in use
    in_use: usize,
}

impl<T: Default, const N: usize> ObjectPool<T, N> {
    /// Create pool with N pre-allocated objects.
    pub fn new() -> Self {
        let mut free_list = ArrayVec::new();
        for _ in 0..N {
            free_list.push(T::default());
        }
        Self { free_list, in_use: 0 }
    }

    /// Acquire object from pool (O(1), no allocation).
    #[inline]
    pub fn acquire(&mut self) -> Option<T> {
        self.free_list.pop().map(|obj| {
            self.in_use += 1;
            obj
        })
    }

    /// Release object back to pool (O(1), no allocation).
    #[inline]
    pub fn release(&mut self, obj: T) {
        if !self.free_list.is_full() {
            self.free_list.push(obj);
            self.in_use -= 1;
        }
        // Else: pool full, object is dropped
    }

    /// Check if pool has available objects.
    #[inline]
    pub fn available(&self) -> usize {
        self.free_list.len()
    }
}
```

#### Pre-allocated Ring Buffer

```rust
use std::mem::MaybeUninit;

/// Fixed-capacity ring buffer (no allocation on push/pop).
///
/// Used for event queues in Ring 0.
pub struct EventRingBuffer<T, const N: usize> {
    buffer: [MaybeUninit<T>; N],
    head: usize,
    tail: usize,
}

impl<T, const N: usize> EventRingBuffer<T, N> {
    /// Create new ring buffer (one-time allocation at startup).
    pub fn new() -> Self {
        Self {
            // SAFETY: MaybeUninit doesn't require initialization
            buffer: unsafe { MaybeUninit::uninit().assume_init() },
            head: 0,
            tail: 0,
        }
    }

    /// Push item (O(1), no allocation).
    #[inline]
    pub fn push(&mut self, item: T) -> Result<(), T> {
        let next_head = (self.head + 1) % N;
        if next_head == self.tail {
            return Err(item); // Full
        }

        // SAFETY: Writing to valid index
        unsafe {
            self.buffer[self.head].as_mut_ptr().write(item);
        }
        self.head = next_head;
        Ok(())
    }

    /// Pop item (O(1), no allocation).
    #[inline]
    pub fn pop(&mut self) -> Option<T> {
        if self.head == self.tail {
            return None; // Empty
        }

        // SAFETY: Reading from valid, initialized index
        let item = unsafe {
            self.buffer[self.tail].as_ptr().read()
        };
        self.tail = (self.tail + 1) % N;
        Some(item)
    }

    /// Check if buffer is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.head == self.tail
    }

    /// Check if buffer is full.
    #[inline]
    pub fn is_full(&self) -> bool {
        (self.head + 1) % N == self.tail
    }
}
```

#### Scratch Buffer

```rust
/// Thread-local scratch buffer for temporary allocations.
///
/// Use instead of Vec for temporary storage in hot path.
pub struct ScratchBuffer {
    buffer: [u8; 64 * 1024], // 64KB scratch
    position: usize,
}

impl ScratchBuffer {
    thread_local! {
        static INSTANCE: std::cell::UnsafeCell<ScratchBuffer> =
            std::cell::UnsafeCell::new(ScratchBuffer::new());
    }

    const fn new() -> Self {
        Self {
            buffer: [0; 64 * 1024],
            position: 0,
        }
    }

    /// Allocate temporary slice from scratch buffer.
    ///
    /// Automatically reset at end of event processing.
    #[inline]
    pub fn alloc(size: usize) -> &'static mut [u8] {
        INSTANCE.with(|scratch| unsafe {
            let scratch = &mut *scratch.get();
            let start = scratch.position;
            scratch.position += size;

            if scratch.position > scratch.buffer.len() {
                panic!("Scratch buffer overflow: needed {}, have {}",
                       size, scratch.buffer.len() - start);
            }

            &mut scratch.buffer[start..scratch.position]
        })
    }

    /// Reset scratch buffer (call at end of each event).
    #[inline]
    pub fn reset() {
        INSTANCE.with(|scratch| unsafe {
            (*scratch.get()).position = 0;
        });
    }
}
```

### Safe Hot Path Code Patterns

```rust
// GOOD: Zero-allocation Ring 0 code

#[hot_path]
fn process_event_good(
    event: &Event,
    // Pre-allocated buffers passed in
    key_buf: &mut [u8; 256],
    result_buf: &mut ArrayVec<Result, 64>,
    // Pre-sized HashMap (won't grow)
    map: &mut HashMap<Key, Value>,
) {
    // Copy to pre-allocated buffer (no allocation)
    let key_len = event.key.len().min(256);
    key_buf[..key_len].copy_from_slice(&event.key[..key_len]);

    // Use ArrayVec (stack-allocated, fixed capacity)
    result_buf.push(result);

    // HashMap pre-reserved, won't grow (assuming key exists)
    map.insert(key, value);

    // Use scratch buffer for temporary data
    let temp = ScratchBuffer::alloc(128);
    // ... use temp ...

    // Reset scratch at end
    ScratchBuffer::reset();
}
```

### CI Integration

```yaml
# .github/workflows/bench.yml
name: Hot Path Allocation Check

on: [push, pull_request]

jobs:
  allocation-check:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Build with allocation tracking
        run: |
          cargo build --features allocation-tracking

      - name: Run tests with allocation detection
        run: |
          cargo test --features allocation-tracking 2>&1 | tee test.log

          # Fail if any hot path allocations detected
          if grep -q "ALLOCATION IN HOT PATH" test.log; then
            echo "Allocation detected in hot path!"
            exit 1
          fi

      - name: Run benchmarks with allocation tracking
        run: |
          cargo bench --features allocation-tracking 2>&1 | tee bench.log

          if grep -q "ALLOCATION IN HOT PATH" bench.log; then
            echo "Allocation detected in hot path during benchmarks!"
            exit 1
          fi
```

### Feature Flag

```toml
# Cargo.toml
[features]
default = []
allocation-tracking = []  # Enable hot path allocation detection
```

## Implementation Phases

### Phase 1: Allocation Detector (1-2 days)

1. Implement `HotPathDetectingAlloc`
2. Implement `HotPathGuard`
3. Thread-local state management
4. Unit tests

### Phase 2: Pre-Allocation Patterns (1-2 days)

1. Implement `ObjectPool`
2. Implement `EventRingBuffer`
3. Implement `ScratchBuffer`
4. Tests and documentation

### Phase 3: Ring 0 Audit (1-2 days)

1. Add `#[hot_path]` to Ring 0 functions
2. Fix allocation violations found
3. Update to use pre-allocation patterns
4. Verify all tests pass

### Phase 4: CI Integration (1 day)

1. Add `allocation-tracking` feature
2. GitHub Actions workflow
3. Documentation

## Test Cases

```rust
#[test]
#[should_panic(expected = "ALLOCATION IN HOT PATH")]
#[cfg(debug_assertions)]
fn test_detects_vec_allocation() {
    let _guard = HotPathGuard::enter("test");
    let _v: Vec<u8> = Vec::new(); // Should panic
}

#[test]
#[should_panic(expected = "ALLOCATION IN HOT PATH")]
#[cfg(debug_assertions)]
fn test_detects_string_allocation() {
    let _guard = HotPathGuard::enter("test");
    let _s = String::from("hello"); // Should panic
}

#[test]
#[cfg(debug_assertions)]
fn test_allows_outside_hot_path() {
    let _v: Vec<u8> = Vec::new(); // Should not panic
}

#[test]
fn test_object_pool_no_allocation() {
    let mut pool: ObjectPool<u64, 10> = ObjectPool::new();

    // All operations below are zero-allocation
    let _guard = HotPathGuard::enter("test");

    let obj = pool.acquire().unwrap();
    pool.release(obj);
}

#[test]
fn test_ring_buffer_no_allocation() {
    let mut buffer: EventRingBuffer<u64, 1024> = EventRingBuffer::new();

    let _guard = HotPathGuard::enter("test");

    buffer.push(42).unwrap();
    let _ = buffer.pop();
}

#[test]
fn test_scratch_buffer() {
    let _guard = HotPathGuard::enter("test");

    let buf = ScratchBuffer::alloc(128);
    buf[0] = 42;

    ScratchBuffer::reset();
}
```

## Acceptance Criteria

- [x] `HotPathDetectingAlloc` panics on allocation in debug builds
- [x] `HotPathGuard` RAII for section marking (with nesting support)
- [x] `hot_path!` macro for convenience
- [x] `ObjectPool` implemented
- [x] `RingBuffer` implemented (renamed from EventRingBuffer)
- [x] `ScratchBuffer` implemented (with thread-local support)
- [ ] All Ring 0 code audited and marked (future work)
- [ ] CI check for hot path allocations (future work - needs GitHub Actions)
- [x] 33 unit tests passing
- [x] Documentation for patterns

## Audit Checklist

Files to audit for allocation violations:

- [ ] `crates/laminar-core/src/reactor/mod.rs`
- [ ] `crates/laminar-core/src/tpc/core_handle.rs`
- [ ] `crates/laminar-core/src/operator/*.rs`
- [ ] `crates/laminar-core/src/state/*.rs`

## References

- [Thread-Per-Core 2026 Research](../../research/laminardb-thread-per-core-2026-research.md)
- [Rust Custom Allocators](https://doc.rust-lang.org/std/alloc/trait.GlobalAlloc.html)
- [ArrayVec Crate](https://docs.rs/arrayvec/)
- [TigerBeetle Allocation Strategy](https://tigerbeetle.com/blog/2023-07-26-allocation-budget/)
