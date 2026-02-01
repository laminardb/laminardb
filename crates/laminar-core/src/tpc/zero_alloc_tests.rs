//! Zero-allocation verification tests for F073.
//!
//! These tests verify that the new zero-allocation APIs work correctly
//! and don't allocate when used properly.

#[cfg(test)]
mod tests {
    use crate::alloc::HotPathGuard;
    use crate::tpc::{OutputBuffer, SpscQueue};
    use std::mem::MaybeUninit;

    #[test]
    fn test_spsc_pop_batch_into_zero_alloc() {
        // Setup outside of hot path
        let queue: SpscQueue<i32> = SpscQueue::new(16);
        queue.push(1).unwrap();
        queue.push(2).unwrap();
        queue.push(3).unwrap();

        let mut buffer: [MaybeUninit<i32>; 8] = [MaybeUninit::uninit(); 8];

        // Hot path section - no allocation should occur
        let _guard = HotPathGuard::enter("pop_batch_into");
        let count = queue.pop_batch_into(&mut buffer);

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
    fn test_spsc_pop_each_zero_alloc() {
        // Setup outside of hot path
        let queue: SpscQueue<i32> = SpscQueue::new(16);
        queue.push(10).unwrap();
        queue.push(20).unwrap();
        queue.push(30).unwrap();

        let mut sum = 0i32;

        // Hot path section - no allocation should occur
        let _guard = HotPathGuard::enter("pop_each");
        let count = queue.pop_each(10, |item| {
            sum += item;
            true
        });

        assert_eq!(count, 3);
        assert_eq!(sum, 60);
    }

    #[test]
    fn test_spsc_pop_each_early_stop_zero_alloc() {
        // Setup outside of hot path
        let queue: SpscQueue<i32> = SpscQueue::new(16);
        for i in 0..10 {
            queue.push(i).unwrap();
        }

        let mut processed = 0i32;

        // Hot path section
        let _guard = HotPathGuard::enter("pop_each_early_stop");
        let count = queue.pop_each(100, |_| {
            processed += 1;
            processed < 5 // Stop after 5
        });

        assert_eq!(count, 5);
        assert_eq!(processed, 5);
        assert_eq!(queue.len(), 5); // 5 remaining
    }

    #[test]
    fn test_output_buffer_reuse_zero_alloc() {
        // Create buffer with sufficient capacity (outside hot path)
        let mut buffer = OutputBuffer::with_capacity(100);

        // First use - fills buffer (may allocate if over capacity)
        buffer.clear();

        // Verify buffer can be reused without allocation
        let cap_before = buffer.capacity();

        // Hot path section - reusing pre-allocated buffer
        let _guard = HotPathGuard::enter("buffer_reuse");

        // Clear doesn't allocate
        buffer.clear();
        assert_eq!(buffer.capacity(), cap_before);
        assert!(buffer.is_empty());

        // Accessing slice doesn't allocate
        let _slice = buffer.as_slice();
        assert_eq!(buffer.capacity(), cap_before);

        // Iteration doesn't allocate
        for _item in buffer.iter() {
            // No-op
        }
        assert_eq!(buffer.capacity(), cap_before);
    }

    #[test]
    fn test_spsc_batch_operations_perf() {
        // Benchmark-style test to verify no unexpected allocations
        const ITERATIONS: usize = 1000;
        const BATCH_SIZE: usize = 64;

        let queue: SpscQueue<u64> = SpscQueue::new(128);
        let mut buffer: [MaybeUninit<u64>; BATCH_SIZE] = [MaybeUninit::uninit(); BATCH_SIZE];

        for iter in 0..ITERATIONS {
            // Fill queue
            for i in 0..BATCH_SIZE {
                let _ = queue.push((iter * BATCH_SIZE + i) as u64);
            }

            // Hot path: pop using zero-alloc API
            let guard = HotPathGuard::enter("batch_perf");
            let count = queue.pop_batch_into(&mut buffer);
            drop(guard);

            assert_eq!(count, BATCH_SIZE);
        }
    }

    #[test]
    fn test_callback_based_processing() {
        // Test that callback-based processing is truly zero-allocation
        let queue: SpscQueue<(u64, u64)> = SpscQueue::new(32);

        // Fill with tuples
        for i in 0..16 {
            queue.push((i, i * 2)).unwrap();
        }

        let mut key_sum = 0u64;
        let mut value_sum = 0u64;

        // Hot path
        let _guard = HotPathGuard::enter("callback_processing");
        queue.pop_each(100, |(key, value)| {
            key_sum += key;
            value_sum += value;
            true
        });

        assert_eq!(key_sum, (0..16).sum::<u64>());
        assert_eq!(value_sum, (0..16).map(|i| i * 2).sum::<u64>());
    }

    #[test]
    fn test_empty_queue_zero_alloc() {
        let queue: SpscQueue<i32> = SpscQueue::new(16);
        let mut buffer: [MaybeUninit<i32>; 8] = [MaybeUninit::uninit(); 8];

        // Hot path with empty queue
        let _guard = HotPathGuard::enter("empty_queue");

        // pop_batch_into on empty queue
        let count = queue.pop_batch_into(&mut buffer);
        assert_eq!(count, 0);

        // pop_each on empty queue
        let count = queue.pop_each(10, |_| true);
        assert_eq!(count, 0);
    }

    #[test]
    fn test_wrap_around_zero_alloc() {
        let queue: SpscQueue<i32> = SpscQueue::new(4);
        let mut buffer: [MaybeUninit<i32>; 4] = [MaybeUninit::uninit(); 4];

        // Advance indices by filling and emptying multiple times
        for _ in 0..5 {
            queue.push(1).unwrap();
            queue.push(2).unwrap();
            let _ = queue.pop_batch(2);
        }

        // Now indices are wrapped
        queue.push(100).unwrap();
        queue.push(200).unwrap();

        // Hot path with wrapped indices
        let _guard = HotPathGuard::enter("wrap_around");
        let count = queue.pop_batch_into(&mut buffer);

        assert_eq!(count, 2);

        #[allow(unsafe_code)]
        unsafe {
            assert_eq!(buffer[0].assume_init(), 100);
            assert_eq!(buffer[1].assume_init(), 200);
        }
    }
}
