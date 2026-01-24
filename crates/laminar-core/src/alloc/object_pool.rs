//! Fixed-size object pool for zero-allocation acquire/release.
//!
//! Pre-allocates objects at creation time, providing O(1) allocation-free
//! acquire and release operations for use in Ring 0 hot path.

use arrayvec::ArrayVec;

/// Fixed-size object pool for frequently allocated types.
///
/// Pre-allocates objects at creation, provides O(1) acquire/release.
/// Used in Ring 0 to avoid heap allocation during event processing.
///
/// # Type Parameters
///
/// * `T` - The type of objects in the pool. Must implement `Default`.
/// * `N` - Maximum capacity of the pool (compile-time constant).
///
/// # Example
///
/// ```
/// use laminar_core::alloc::ObjectPool;
///
/// // Create a pool of 16 buffers
/// let mut pool: ObjectPool<Vec<u8>, 16> = ObjectPool::with_init(|| Vec::with_capacity(1024));
///
/// // Acquire from pool (O(1), no allocation)
/// let mut buf = pool.acquire().expect("pool not empty");
/// buf.extend_from_slice(b"hello");
///
/// // Return to pool (O(1), no allocation)
/// buf.clear();
/// pool.release(buf);
/// ```
///
/// # Thread Safety
///
/// This pool is NOT thread-safe. Each core in a thread-per-core architecture
/// should have its own pool.
#[derive(Debug)]
pub struct ObjectPool<T, const N: usize> {
    /// Stack of available objects
    free_list: ArrayVec<T, N>,
    /// Number of objects currently in use
    in_use: usize,
}

impl<T, const N: usize> ObjectPool<T, N> {
    /// Create a new empty pool.
    ///
    /// Objects must be added via `release()` or use `with_init()` for pre-population.
    #[must_use]
    pub fn new() -> Self {
        Self {
            free_list: ArrayVec::new(),
            in_use: 0,
        }
    }

    /// Create a pool pre-populated with objects using a factory function.
    ///
    /// The factory is called `N` times to create the initial pool contents.
    /// This allocation happens at startup, not on the hot path.
    ///
    /// # Arguments
    ///
    /// * `factory` - Function to create each object
    ///
    /// # Example
    ///
    /// ```
    /// use laminar_core::alloc::ObjectPool;
    ///
    /// let pool: ObjectPool<Vec<u8>, 8> = ObjectPool::with_init(|| Vec::with_capacity(256));
    /// assert_eq!(pool.available(), 8);
    /// ```
    pub fn with_init<F>(factory: F) -> Self
    where
        F: Fn() -> T,
    {
        let mut free_list = ArrayVec::new();
        for _ in 0..N {
            // This will always succeed since we iterate exactly N times
            // and ArrayVec<T, N> has capacity N
            let _ = free_list.try_push(factory());
        }
        Self { free_list, in_use: 0 }
    }

    /// Acquire an object from the pool.
    ///
    /// Returns `None` if the pool is exhausted.
    ///
    /// # Performance
    ///
    /// O(1), no allocation.
    ///
    /// # Example
    ///
    /// ```
    /// use laminar_core::alloc::ObjectPool;
    ///
    /// let mut pool: ObjectPool<u64, 4> = ObjectPool::with_init(|| 0);
    /// let obj = pool.acquire().unwrap();
    /// assert_eq!(pool.available(), 3);
    /// ```
    #[inline]
    pub fn acquire(&mut self) -> Option<T> {
        let obj = self.free_list.pop();
        if obj.is_some() {
            self.in_use += 1;
        }
        obj
    }

    /// Release an object back to the pool.
    ///
    /// If the pool is full, the object is dropped instead of stored.
    ///
    /// # Performance
    ///
    /// O(1), no allocation.
    ///
    /// # Returns
    ///
    /// `true` if the object was added to the pool, `false` if it was dropped.
    ///
    /// # Example
    ///
    /// ```
    /// use laminar_core::alloc::ObjectPool;
    ///
    /// let mut pool: ObjectPool<u64, 4> = ObjectPool::new();
    /// assert!(pool.release(42)); // Added to pool
    /// assert_eq!(pool.available(), 1);
    /// ```
    #[inline]
    pub fn release(&mut self, obj: T) -> bool {
        if self.free_list.try_push(obj).is_ok() {
            if self.in_use > 0 {
                self.in_use -= 1;
            }
            true
        } else {
            // Pool is full, object is dropped
            false
        }
    }

    /// Get the number of available objects in the pool.
    #[inline]
    #[must_use]
    pub fn available(&self) -> usize {
        self.free_list.len()
    }

    /// Get the number of objects currently in use.
    #[inline]
    #[must_use]
    pub fn in_use(&self) -> usize {
        self.in_use
    }

    /// Get the maximum capacity of the pool.
    #[inline]
    #[must_use]
    pub const fn capacity(&self) -> usize {
        N
    }

    /// Check if the pool is empty (no objects available).
    #[inline]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.free_list.is_empty()
    }

    /// Check if the pool is full (no more room for released objects).
    #[inline]
    #[must_use]
    pub fn is_full(&self) -> bool {
        self.free_list.is_full()
    }

    /// Clear all objects from the pool.
    ///
    /// Drops all pooled objects and resets counters.
    pub fn clear(&mut self) {
        self.free_list.clear();
        self.in_use = 0;
    }
}

impl<T: Default, const N: usize> Default for ObjectPool<T, N> {
    /// Create a pool pre-populated with `N` default objects.
    fn default() -> Self {
        Self::with_init(T::default)
    }
}

impl<T: Clone, const N: usize> Clone for ObjectPool<T, N> {
    fn clone(&self) -> Self {
        Self {
            free_list: self.free_list.clone(),
            in_use: self.in_use,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_pool_empty() {
        let pool: ObjectPool<u64, 10> = ObjectPool::new();
        assert_eq!(pool.available(), 0);
        assert_eq!(pool.capacity(), 10);
        assert!(pool.is_empty());
    }

    #[test]
    fn test_with_init() {
        let pool: ObjectPool<u64, 10> = ObjectPool::with_init(|| 42);
        assert_eq!(pool.available(), 10);
        assert!(!pool.is_empty());
    }

    #[test]
    fn test_acquire_release() {
        let mut pool: ObjectPool<u64, 4> = ObjectPool::with_init(|| 0);

        // Acquire all
        let a = pool.acquire().unwrap();
        let b = pool.acquire().unwrap();
        let c = pool.acquire().unwrap();
        let d = pool.acquire().unwrap();

        assert_eq!(pool.available(), 0);
        assert_eq!(pool.in_use(), 4);
        assert!(pool.acquire().is_none()); // Pool exhausted

        // Release some
        pool.release(a);
        pool.release(b);
        assert_eq!(pool.available(), 2);
        assert_eq!(pool.in_use(), 2);

        // Release rest
        pool.release(c);
        pool.release(d);
        assert_eq!(pool.available(), 4);
        assert_eq!(pool.in_use(), 0);
    }

    #[test]
    fn test_release_to_full_pool() {
        let mut pool: ObjectPool<u64, 2> = ObjectPool::with_init(|| 0);

        // Pool is already full
        assert!(pool.is_full());

        // Try to add another object - should be dropped
        let added = pool.release(99);
        assert!(!added);
        assert_eq!(pool.available(), 2);
    }

    #[test]
    fn test_default_trait() {
        let pool: ObjectPool<u64, 5> = ObjectPool::default();
        assert_eq!(pool.available(), 5);

        // All should be 0 (u64::default())
        let mut pool = pool;
        for _ in 0..5 {
            assert_eq!(pool.acquire(), Some(0));
        }
    }

    #[test]
    fn test_clear() {
        let mut pool: ObjectPool<u64, 10> = ObjectPool::with_init(|| 42);
        pool.acquire();
        pool.acquire();

        pool.clear();

        assert_eq!(pool.available(), 0);
        assert_eq!(pool.in_use(), 0);
    }

    #[test]
    fn test_with_vec_buffers() {
        // Common use case: pool of pre-sized vectors
        let mut pool: ObjectPool<Vec<u8>, 4> = ObjectPool::with_init(|| Vec::with_capacity(1024));

        let mut buf = pool.acquire().unwrap();
        assert!(buf.capacity() >= 1024);

        buf.extend_from_slice(b"test data");
        buf.clear(); // Reset for reuse

        pool.release(buf);
        assert_eq!(pool.available(), 4);
    }

    #[test]
    fn test_clone() {
        let pool: ObjectPool<u64, 4> = ObjectPool::with_init(|| 42);
        let cloned = pool.clone();

        assert_eq!(cloned.available(), pool.available());
        assert_eq!(cloned.capacity(), pool.capacity());
    }
}
