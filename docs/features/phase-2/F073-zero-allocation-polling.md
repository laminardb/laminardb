# F073: Zero-Allocation Polling

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F073 |
| **Status** | ✅ Done |
| **Priority** | P1 |
| **Phase** | 2 |
| **Effort** | M (3-5 days) |
| **Dependencies** | F013, F071 |
| **Owner** | Completed 2026-01-25 |
| **Audit Source** | Performance Auditor (2026-01-24) |

## Summary

Eliminate heap allocations in the TPC polling hot path by replacing `Vec`-returning APIs with pre-allocated buffers and callback-based patterns. Currently, every `poll()` call allocates memory, violating Ring 0's zero-allocation constraint.

## Motivation

### Current Problem

The performance auditor identified these allocations on every poll cycle:

```rust
// spsc.rs:307 - Allocates on EVERY pop_batch call
pub fn pop_batch(&self, max_count: usize) -> Vec<T> {
    let mut items = Vec::with_capacity(max_count.min(self.len()));
    // ...
}

// runtime.rs:371-372 - Allocates on EVERY poll call
pub fn poll(&self) -> Vec<Output> {
    let mut outputs = Vec::new();
    // ...
}
```

### Impact

| Operation | Current | Target |
|-----------|---------|--------|
| `poll()` | ~50-100ns (allocation) | <10ns (no allocation) |
| `pop_batch()` | ~30-50ns (allocation) | <5ns (no allocation) |
| Events/sec/core | ~500K | ~1M+ |

At 500K events/sec, allocation overhead consumes 25-50μs per second per core - significant for latency-sensitive workloads.

### Ring 0 Constraint Violation

```
Ring 0 Hot Path Requirements:
├── Zero allocations .............. ❌ VIOLATED (Vec allocations)
├── No locks ...................... ✅ OK (lock-free SPSC)
├── < 1μs latency ................. ⚠️ AT RISK (allocation jitter)
└── No syscalls ................... ✅ OK
```

## Goals

1. Eliminate all heap allocations in `poll()` and `pop_batch()` paths
2. Provide pre-allocated buffer APIs for output collection
3. Add callback-based APIs as zero-copy alternatives
4. Replace `format!()` error messages with static variants
5. Maintain backward compatibility with existing APIs (deprecated)
6. Verify with `HotPathGuard` + `allocation-tracking` feature

## Non-Goals

- Changing the SPSC queue internal storage (already allocation-free)
- Modifying operator `process()` signatures (separate concern)
- Eliminating allocations in initialization/setup code

## Technical Design

### 1. Pre-Allocated Buffer API for SPSC Queue

```rust
/// Zero-allocation batch pop into caller-provided buffer.
///
/// Returns the number of items written to `buffer`.
/// Items are written starting at index 0.
impl<T> SpscQueue<T> {
    /// Pop up to `buffer.len()` items into the provided slice.
    ///
    /// # Returns
    /// Number of items actually popped (0 if queue empty).
    #[inline]
    pub fn pop_batch_into(&self, buffer: &mut [MaybeUninit<T>]) -> usize {
        let head = self.head.load(Ordering::Acquire);
        let tail = self.tail.load(Ordering::Relaxed);

        let available = tail.wrapping_sub(head);
        let count = available.min(buffer.len());

        if count == 0 {
            return 0;
        }

        for i in 0..count {
            let idx = (head + i) & self.mask;
            // SAFETY: We have exclusive read access between head and tail
            unsafe {
                let src = self.buffer.add(idx);
                buffer[i].write(ptr::read(src));
            }
        }

        self.head.store(head.wrapping_add(count), Ordering::Release);
        count
    }

    /// Pop items and call `f` for each one (zero-allocation).
    ///
    /// Processing stops if `f` returns `false`.
    #[inline]
    pub fn pop_each<F>(&self, max_count: usize, mut f: F) -> usize
    where
        F: FnMut(T) -> bool,
    {
        let head = self.head.load(Ordering::Acquire);
        let tail = self.tail.load(Ordering::Relaxed);

        let available = tail.wrapping_sub(head);
        let to_pop = available.min(max_count);

        let mut popped = 0;
        for i in 0..to_pop {
            let idx = (head + i) & self.mask;
            // SAFETY: We have exclusive read access
            let item = unsafe { ptr::read(self.buffer.add(idx)) };

            if !f(item) {
                popped = i + 1;
                break;
            }
            popped = i + 1;
        }

        if popped > 0 {
            self.head.store(head.wrapping_add(popped), Ordering::Release);
        }
        popped
    }

    /// Deprecated: Use `pop_batch_into` or `pop_each` instead.
    #[deprecated(since = "0.2.0", note = "Use pop_batch_into or pop_each for zero-allocation polling")]
    pub fn pop_batch(&self, max_count: usize) -> Vec<T> {
        // Keep existing implementation for backward compatibility
        let mut items = Vec::with_capacity(max_count.min(self.len()));
        // ...
    }
}
```

### 2. Pre-Allocated Buffer API for Runtime

```rust
/// Output buffer for zero-allocation polling.
pub struct OutputBuffer {
    /// Pre-allocated storage
    items: Vec<Output>,
    /// Number of valid items (0..len)
    len: usize,
}

impl OutputBuffer {
    /// Create buffer with given capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            items: Vec::with_capacity(capacity),
            len: 0,
        }
    }

    /// Clear the buffer for reuse (no deallocation).
    #[inline]
    pub fn clear(&mut self) {
        self.items.clear();
        self.len = 0;
    }

    /// Get the collected outputs as a slice.
    #[inline]
    pub fn as_slice(&self) -> &[Output] {
        &self.items[..self.len]
    }

    /// Remaining capacity.
    #[inline]
    pub fn remaining(&self) -> usize {
        self.items.capacity() - self.len
    }
}

impl ThreadPerCoreRuntime {
    /// Poll outputs into a pre-allocated buffer (zero-allocation).
    ///
    /// # Arguments
    /// * `buffer` - Pre-allocated buffer to receive outputs
    /// * `max_per_core` - Maximum outputs to collect per core
    ///
    /// # Returns
    /// Total number of outputs collected.
    pub fn poll_into(&self, buffer: &mut OutputBuffer, max_per_core: usize) -> usize {
        buffer.clear();

        for core in &self.cores {
            if buffer.remaining() == 0 {
                break;
            }

            let space = buffer.remaining().min(max_per_core);
            // Use internal pre-allocated scratch or callback
            core.poll_outputs_into(&mut buffer.items, space);
        }

        buffer.len = buffer.items.len();
        buffer.len
    }

    /// Poll with callback for each output (zero-allocation).
    ///
    /// Processing continues until callback returns `false` or all outputs consumed.
    pub fn poll_each<F>(&self, max_per_core: usize, mut f: F) -> usize
    where
        F: FnMut(Output) -> bool,
    {
        let mut total = 0;

        for core in &self.cores {
            let count = core.poll_each(max_per_core, &mut f);
            total += count;

            // Check if callback requested stop
            // (would need to track this in the callback pattern)
        }

        total
    }

    /// Deprecated: Use `poll_into` or `poll_each` instead.
    #[deprecated(since = "0.2.0", note = "Use poll_into or poll_each for zero-allocation polling")]
    pub fn poll(&self) -> Vec<Output> {
        // Keep existing implementation
    }
}
```

### 3. CoreHandle Zero-Allocation Polling

```rust
impl CoreHandle {
    /// Poll outputs into caller's buffer slice.
    ///
    /// Returns number of outputs written.
    pub fn poll_outputs_into(&self, buffer: &mut Vec<Output>, max: usize) -> usize {
        // Reserve space without allocation if capacity exists
        let start_len = buffer.len();

        self.outbox.pop_each(max, |msg| {
            if let CoreMessage::Output(output) = msg {
                buffer.push(output); // May allocate if over capacity
                true
            } else {
                true
            }
        });

        buffer.len() - start_len
    }

    /// Poll with zero-allocation callback.
    pub fn poll_each<F>(&self, max: usize, mut f: F) -> usize
    where
        F: FnMut(Output) -> bool,
    {
        self.outbox.pop_each(max, |msg| {
            match msg {
                CoreMessage::Output(output) => f(output),
                _ => true, // Skip non-output messages
            }
        })
    }
}
```

### 4. Static Error Variants for Router

```rust
/// Routing errors with no heap allocation.
#[derive(Debug, Clone, Copy, thiserror::Error)]
pub enum RouterError {
    #[error("column not found by name")]
    ColumnNotFoundByName,

    #[error("column index out of range")]
    ColumnIndexOutOfRange,

    #[error("row index out of range")]
    RowIndexOutOfRange,

    #[error("unsupported data type for routing")]
    UnsupportedDataType,

    #[error("empty batch")]
    EmptyBatch,
}

// Replace format!() calls:
// Before:
//   format!("Column '{col_name}' not found in schema")
// After:
//   RouterError::ColumnNotFoundByName
```

### 5. SmallVec for Small Batches (Optional Optimization)

```rust
use smallvec::SmallVec;

/// Inline storage for up to 8 outputs (no heap for small batches).
pub type SmallOutputVec = SmallVec<[Output; 8]>;

impl CoreHandle {
    /// Poll with inline storage for small batches.
    ///
    /// Avoids heap allocation for <= 8 outputs.
    pub fn poll_outputs_small(&self, max: usize) -> SmallOutputVec {
        let mut outputs = SmallOutputVec::new();

        self.outbox.pop_each(max.min(8), |msg| {
            if let CoreMessage::Output(output) = msg {
                outputs.push(output);
            }
            true
        });

        outputs
    }
}
```

### 6. Thread-Local Scratch Buffer

```rust
use std::cell::RefCell;

thread_local! {
    static POLL_BUFFER: RefCell<OutputBuffer> = RefCell::new(
        OutputBuffer::with_capacity(1024)
    );
}

impl ThreadPerCoreRuntime {
    /// Poll using thread-local scratch buffer.
    ///
    /// Returns a reference that must be consumed before next poll.
    pub fn poll_scratch(&self, max_per_core: usize) -> impl Deref<Target = [Output]> + '_ {
        POLL_BUFFER.with(|buf| {
            let mut buffer = buf.borrow_mut();
            self.poll_into(&mut buffer, max_per_core);
            // Return guard that provides slice access
            ScratchGuard { buffer }
        })
    }
}
```

## Implementation Phases

### Phase 1: SPSC Queue APIs (1 day)

1. Add `pop_batch_into()` method
2. Add `pop_each()` callback method
3. Deprecate `pop_batch()` (keep for compatibility)
4. Unit tests for new APIs
5. Verify zero-allocation with `HotPathGuard`

### Phase 2: CoreHandle APIs (1 day)

1. Add `poll_outputs_into()` method
2. Add `poll_each()` callback method
3. Update internal polling to use new SPSC APIs
4. Unit tests

### Phase 3: Runtime APIs (1 day)

1. Add `OutputBuffer` type
2. Add `poll_into()` method
3. Add `poll_each()` callback method
4. Deprecate `poll()`
5. Integration tests

### Phase 4: Router Static Errors (0.5 day)

1. Create `RouterError` enum with static variants
2. Replace `format!()` calls
3. Update error handling in callers
4. Unit tests

### Phase 5: Verification & Benchmarks (0.5 day)

1. Enable `allocation-tracking` feature
2. Add benchmark comparing old vs new APIs
3. Verify `HotPathGuard` passes in full poll cycle
4. Update documentation

## Test Cases

```rust
#[test]
fn test_pop_batch_into_zero_allocation() {
    let _guard = HotPathGuard::enter("test");

    let queue: SpscQueue<u64> = SpscQueue::new(16);
    queue.push(1).unwrap();
    queue.push(2).unwrap();

    let mut buffer = [MaybeUninit::uninit(); 8];
    let count = queue.pop_batch_into(&mut buffer);

    assert_eq!(count, 2);
    // No panic from HotPathGuard = no allocation
}

#[test]
fn test_poll_each_zero_allocation() {
    let _guard = HotPathGuard::enter("test");

    let runtime = ThreadPerCoreRuntime::new(config)?;
    // ... submit some events ...

    let mut results = SmallVec::<[Output; 16]>::new();
    runtime.poll_each(100, |output| {
        results.push(output);
        true
    });

    // No panic = no allocation
}

#[test]
fn test_poll_into_reuses_buffer() {
    let mut buffer = OutputBuffer::with_capacity(1024);
    let runtime = ThreadPerCoreRuntime::new(config)?;

    // First poll
    runtime.poll_into(&mut buffer, 100);
    let cap1 = buffer.items.capacity();

    // Second poll - should reuse same allocation
    buffer.clear();
    runtime.poll_into(&mut buffer, 100);
    let cap2 = buffer.items.capacity();

    assert_eq!(cap1, cap2); // No reallocation
}

#[test]
fn test_router_error_no_allocation() {
    let _guard = HotPathGuard::enter("test");

    let router = KeyRouter::new(4, KeySpec::ColumnName("missing".into()));
    let result = router.route(&batch);

    assert!(matches!(result, Err(RouterError::ColumnNotFoundByName)));
    // No panic = no allocation for error
}
```

## Acceptance Criteria

- [ ] `SpscQueue::pop_batch_into()` implemented and tested
- [ ] `SpscQueue::pop_each()` implemented and tested
- [ ] `CoreHandle::poll_outputs_into()` implemented
- [ ] `CoreHandle::poll_each()` implemented
- [ ] `ThreadPerCoreRuntime::poll_into()` implemented
- [ ] `ThreadPerCoreRuntime::poll_each()` implemented
- [ ] `OutputBuffer` type with reusable allocation
- [ ] `RouterError` static variants (no format!())
- [ ] Old APIs deprecated with migration guidance
- [ ] All tests pass with `allocation-tracking` feature
- [ ] Benchmark showing improvement
- [ ] Documentation updated

## Performance Targets

| API | Current | Target | Improvement |
|-----|---------|--------|-------------|
| `pop_batch(8)` | ~40ns | <10ns | 4x |
| `poll()` (4 cores) | ~150ns | <30ns | 5x |
| `route()` error path | ~80ns | <5ns | 16x |

## Migration Guide

```rust
// Before (allocates):
let outputs = runtime.poll();
for output in outputs {
    process(output);
}

// After Option 1 (pre-allocated buffer):
let mut buffer = OutputBuffer::with_capacity(1024);
loop {
    runtime.poll_into(&mut buffer, 256);
    for output in buffer.as_slice() {
        process(output);
    }
    buffer.clear();
}

// After Option 2 (callback, zero-copy):
runtime.poll_each(256, |output| {
    process(output);
    true // continue polling
});
```

## Dependencies

- **F013**: Thread-Per-Core Architecture (provides SPSC, CoreHandle)
- **F071**: Zero-Allocation Enforcement (provides HotPathGuard for verification)

## References

- [Performance Audit Report (2026-01-24)](../../audits/performance-audit-2026-01-24.md)
- [F071: Zero-Allocation Enforcement](F071-zero-allocation-enforcement.md)
- [Ring Architecture](../../ARCHITECTURE.md)
