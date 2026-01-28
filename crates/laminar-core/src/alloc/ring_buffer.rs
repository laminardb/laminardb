//! Fixed-capacity ring buffer for zero-allocation queuing.
//!
//! Provides a circular buffer with O(1) push/pop operations without
//! any heap allocation after initialization.

use std::mem::MaybeUninit;

/// Fixed-capacity ring buffer.
///
/// A circular buffer that stores up to `N-1` elements (one slot reserved
/// for distinguishing full from empty). All operations are O(1) and
/// allocation-free after construction.
///
/// # Type Parameters
///
/// * `T` - The element type
/// * `N` - Buffer capacity (must be > 1; usable capacity is N-1)
///
/// # Example
///
/// ```
/// use laminar_core::alloc::RingBuffer;
///
/// let mut buffer: RingBuffer<u64, 4> = RingBuffer::new();
///
/// buffer.push(1).unwrap();
/// buffer.push(2).unwrap();
/// buffer.push(3).unwrap();
/// // buffer.push(4) would fail - only 3 slots usable
///
/// assert_eq!(buffer.pop(), Some(1));
/// assert_eq!(buffer.pop(), Some(2));
/// ```
///
/// # Thread Safety
///
/// This buffer is NOT thread-safe. For multi-threaded scenarios, use the
/// SPSC queue in `tpc::spsc`.
pub struct RingBuffer<T, const N: usize> {
    /// The underlying buffer storage
    buffer: [MaybeUninit<T>; N],
    /// Read position (tail)
    tail: usize,
    /// Write position (head)
    head: usize,
}

impl<T, const N: usize> RingBuffer<T, N> {
    /// Create a new empty ring buffer.
    ///
    /// # Panics
    ///
    /// Panics if `N < 2` (need at least 2 slots for ring buffer semantics).
    #[must_use]
    pub fn new() -> Self {
        assert!(N >= 2, "RingBuffer requires N >= 2");
        Self {
            // SAFETY: MaybeUninit<T> doesn't require initialization
            buffer: unsafe { MaybeUninit::uninit().assume_init() },
            tail: 0,
            head: 0,
        }
    }

    /// Push an item onto the buffer.
    ///
    /// # Performance
    ///
    /// O(1), no allocation.
    ///
    /// # Errors
    ///
    /// Returns `Err(item)` if the buffer is full, giving back ownership
    /// of the item that couldn't be inserted.
    ///
    /// # Example
    ///
    /// ```
    /// use laminar_core::alloc::RingBuffer;
    ///
    /// let mut buf: RingBuffer<i32, 4> = RingBuffer::new();
    /// buf.push(1).unwrap();
    /// buf.push(2).unwrap();
    /// buf.push(3).unwrap();
    /// assert!(buf.push(4).is_err()); // Full
    /// ```
    #[inline]
    pub fn push(&mut self, item: T) -> Result<(), T> {
        let next_head = (self.head + 1) % N;
        if next_head == self.tail {
            return Err(item); // Full
        }

        // SAFETY: We're writing to a valid index within bounds,
        // and we've verified the slot is available (not readable).
        unsafe {
            self.buffer[self.head].as_mut_ptr().write(item);
        }
        self.head = next_head;
        Ok(())
    }

    /// Pop an item from the buffer.
    ///
    /// Returns `None` if the buffer is empty.
    ///
    /// # Performance
    ///
    /// O(1), no allocation.
    ///
    /// # Example
    ///
    /// ```
    /// use laminar_core::alloc::RingBuffer;
    ///
    /// let mut buf: RingBuffer<i32, 4> = RingBuffer::new();
    /// buf.push(42).unwrap();
    /// assert_eq!(buf.pop(), Some(42));
    /// assert_eq!(buf.pop(), None);
    /// ```
    #[inline]
    pub fn pop(&mut self) -> Option<T> {
        if self.head == self.tail {
            return None; // Empty
        }

        // SAFETY: We've verified the slot contains a valid item
        // (head != tail means there's at least one item).
        let item = unsafe { self.buffer[self.tail].as_ptr().read() };
        self.tail = (self.tail + 1) % N;
        Some(item)
    }

    /// Peek at the front item without removing it.
    ///
    /// Returns `None` if the buffer is empty.
    ///
    /// # Performance
    ///
    /// O(1), no allocation.
    #[inline]
    pub fn peek(&self) -> Option<&T> {
        if self.head == self.tail {
            return None;
        }

        // SAFETY: We've verified the slot contains a valid item.
        Some(unsafe { &*self.buffer[self.tail].as_ptr() })
    }

    /// Get mutable reference to the front item without removing it.
    #[inline]
    pub fn peek_mut(&mut self) -> Option<&mut T> {
        if self.head == self.tail {
            return None;
        }

        // SAFETY: We've verified the slot contains a valid item.
        Some(unsafe { &mut *self.buffer[self.tail].as_mut_ptr() })
    }

    /// Check if the buffer is empty.
    #[inline]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.head == self.tail
    }

    /// Check if the buffer is full.
    #[inline]
    #[must_use]
    pub fn is_full(&self) -> bool {
        (self.head + 1) % N == self.tail
    }

    /// Get the number of items in the buffer.
    #[inline]
    #[must_use]
    pub fn len(&self) -> usize {
        if self.head >= self.tail {
            self.head - self.tail
        } else {
            N - self.tail + self.head
        }
    }

    /// Get the maximum usable capacity (N - 1).
    #[inline]
    #[must_use]
    pub const fn capacity(&self) -> usize {
        N - 1
    }

    /// Clear all items from the buffer.
    ///
    /// Drops all contained items and resets to empty state.
    pub fn clear(&mut self) {
        while self.pop().is_some() {}
    }

    /// Iterate over items without consuming them.
    pub fn iter(&self) -> RingBufferIter<'_, T, N> {
        RingBufferIter {
            buffer: self,
            pos: self.tail,
            remaining: self.len(),
        }
    }
}

impl<T, const N: usize> Default for RingBuffer<T, N> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T, const N: usize> Drop for RingBuffer<T, N> {
    fn drop(&mut self) {
        // Drop all remaining items
        self.clear();
    }
}

impl<T: std::fmt::Debug, const N: usize> std::fmt::Debug for RingBuffer<T, N> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RingBuffer")
            .field("len", &self.len())
            .field("capacity", &self.capacity())
            .field("items", &self.iter().collect::<Vec<_>>())
            .finish()
    }
}

/// Iterator over ring buffer items.
pub struct RingBufferIter<'a, T, const N: usize> {
    buffer: &'a RingBuffer<T, N>,
    pos: usize,
    remaining: usize,
}

impl<'a, T, const N: usize> Iterator for RingBufferIter<'a, T, N> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }

        // SAFETY: pos is within the valid range of items
        let item = unsafe { &*self.buffer.buffer[self.pos].as_ptr() };
        self.pos = (self.pos + 1) % N;
        self.remaining -= 1;
        Some(item)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

impl<T, const N: usize> ExactSizeIterator for RingBufferIter<'_, T, N> {}

impl<'a, T, const N: usize> IntoIterator for &'a RingBuffer<T, N> {
    type Item = &'a T;
    type IntoIter = RingBufferIter<'a, T, N>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

// We need to allow unsafe_code for this module due to MaybeUninit usage
#[allow(unsafe_code)]
const _: () = ();

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_buffer() {
        let buf: RingBuffer<u64, 8> = RingBuffer::new();
        assert!(buf.is_empty());
        assert!(!buf.is_full());
        assert_eq!(buf.len(), 0);
        assert_eq!(buf.capacity(), 7); // N-1
    }

    #[test]
    fn test_push_pop() {
        let mut buf: RingBuffer<i32, 4> = RingBuffer::new();

        buf.push(1).unwrap();
        buf.push(2).unwrap();
        buf.push(3).unwrap();

        assert_eq!(buf.len(), 3);
        assert!(buf.is_full());

        assert_eq!(buf.pop(), Some(1));
        assert_eq!(buf.pop(), Some(2));
        assert_eq!(buf.pop(), Some(3));
        assert_eq!(buf.pop(), None);
    }

    #[test]
    fn test_full_buffer() {
        let mut buf: RingBuffer<i32, 4> = RingBuffer::new();

        buf.push(1).unwrap();
        buf.push(2).unwrap();
        buf.push(3).unwrap();

        // Buffer full (capacity is 3 for N=4)
        assert!(buf.push(4).is_err());
    }

    #[test]
    fn test_wraparound() {
        let mut buf: RingBuffer<i32, 4> = RingBuffer::new();

        // Fill and drain multiple times to test wraparound
        for round in 0..5 {
            let base = round * 10;
            buf.push(base + 1).unwrap();
            buf.push(base + 2).unwrap();
            buf.push(base + 3).unwrap();

            assert_eq!(buf.pop(), Some(base + 1));
            assert_eq!(buf.pop(), Some(base + 2));
            assert_eq!(buf.pop(), Some(base + 3));
        }
    }

    #[test]
    fn test_peek() {
        let mut buf: RingBuffer<i32, 4> = RingBuffer::new();

        assert_eq!(buf.peek(), None);

        buf.push(42).unwrap();
        assert_eq!(buf.peek(), Some(&42));
        assert_eq!(buf.len(), 1); // Peek doesn't remove

        buf.pop();
        assert_eq!(buf.peek(), None);
    }

    #[test]
    fn test_peek_mut() {
        let mut buf: RingBuffer<i32, 4> = RingBuffer::new();
        buf.push(10).unwrap();

        if let Some(val) = buf.peek_mut() {
            *val = 20;
        }

        assert_eq!(buf.pop(), Some(20));
    }

    #[test]
    fn test_clear() {
        let mut buf: RingBuffer<i32, 8> = RingBuffer::new();
        buf.push(1).unwrap();
        buf.push(2).unwrap();
        buf.push(3).unwrap();

        buf.clear();

        assert!(buf.is_empty());
        assert_eq!(buf.len(), 0);
    }

    #[test]
    fn test_iter() {
        let mut buf: RingBuffer<i32, 8> = RingBuffer::new();
        buf.push(1).unwrap();
        buf.push(2).unwrap();
        buf.push(3).unwrap();

        let items: Vec<_> = buf.iter().copied().collect();
        assert_eq!(items, vec![1, 2, 3]);

        // Iteration doesn't consume
        assert_eq!(buf.len(), 3);
    }

    #[test]
    fn test_debug() {
        let mut buf: RingBuffer<i32, 4> = RingBuffer::new();
        buf.push(1).unwrap();
        buf.push(2).unwrap();

        let debug_str = format!("{buf:?}");
        assert!(debug_str.contains("RingBuffer"));
        assert!(debug_str.contains("len"));
    }

    #[test]
    fn test_drop_items() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        static DROP_COUNT: AtomicUsize = AtomicUsize::new(0);

        #[derive(Debug)]
        struct DropCounter;
        impl Drop for DropCounter {
            fn drop(&mut self) {
                DROP_COUNT.fetch_add(1, Ordering::SeqCst);
            }
        }

        DROP_COUNT.store(0, Ordering::SeqCst);

        {
            let mut buf: RingBuffer<DropCounter, 8> = RingBuffer::new();
            buf.push(DropCounter).unwrap();
            buf.push(DropCounter).unwrap();
            buf.push(DropCounter).unwrap();
        }

        // All items should be dropped when buffer is dropped
        assert_eq!(DROP_COUNT.load(Ordering::SeqCst), 3);
    }

    #[test]
    #[should_panic(expected = "N >= 2")]
    fn test_invalid_size() {
        let _: RingBuffer<u8, 1> = RingBuffer::new();
    }
}
