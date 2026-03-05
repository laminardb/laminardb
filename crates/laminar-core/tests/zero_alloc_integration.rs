//! Integration tests for zero-allocation hot path verification.
//!
//! These tests use `HotPathDetectingAlloc` as the global allocator
//! so that `HotPathGuard` zones actually panic on heap allocation.
//!
//! Run with: `cargo test --features allocation-tracking -p laminar-core --test zero_alloc_integration`

#![cfg(feature = "allocation-tracking")]

use laminar_core::alloc::HotPathDetectingAlloc;

#[global_allocator]
static ALLOC: HotPathDetectingAlloc = HotPathDetectingAlloc::new();

use laminar_core::alloc::HotPathGuard;
use laminar_core::tpc::SpscQueue;
use std::mem::MaybeUninit;

#[test]
fn zero_alloc_spsc_pop_batch_into() {
    let queue: SpscQueue<i32> = SpscQueue::new(16);
    queue.push(1).unwrap();
    queue.push(2).unwrap();
    queue.push(3).unwrap();

    let mut buffer: [MaybeUninit<i32>; 8] = [MaybeUninit::uninit(); 8];

    let _guard = HotPathGuard::enter("pop_batch_into");
    let count = queue.pop_batch_into(&mut buffer);
    drop(_guard);

    assert_eq!(count, 3);
    // SAFETY: We just initialized these elements
    #[allow(unsafe_code)]
    unsafe {
        assert_eq!(buffer[0].assume_init(), 1);
        assert_eq!(buffer[1].assume_init(), 2);
        assert_eq!(buffer[2].assume_init(), 3);
    }
}

#[test]
fn zero_alloc_spsc_pop_each() {
    let queue: SpscQueue<i32> = SpscQueue::new(16);
    queue.push(10).unwrap();
    queue.push(20).unwrap();
    queue.push(30).unwrap();

    let mut sum = 0i32;

    let _guard = HotPathGuard::enter("pop_each");
    let count = queue.pop_each(10, |item| {
        sum += item;
        true
    });
    drop(_guard);

    assert_eq!(count, 3);
    assert_eq!(sum, 60);
}

#[test]
fn zero_alloc_output_buffer_reuse() {
    use laminar_core::operator::Output;
    use laminar_core::tpc::OutputBuffer;

    let mut buffer = OutputBuffer::with_capacity(16);

    // Pre-warm the buffer
    for i in 0..8i64 {
        buffer.push(Output::Watermark(i));
    }
    buffer.clear();

    // Hot path: reuse should not allocate
    let _guard = HotPathGuard::enter("buffer_reuse");
    for i in 0..8i64 {
        buffer.push(Output::Watermark(i));
    }
    buffer.clear();
    drop(_guard);
}
