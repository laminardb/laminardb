//! Thread-local scratch buffer for temporary allocations.
//!
//! Provides fast, allocation-free temporary storage for hot path code.
//! Each thread gets its own buffer that can be reset between events.

use std::cell::UnsafeCell;

/// Default scratch buffer size (64KB).
pub const DEFAULT_SCRATCH_SIZE: usize = 64 * 1024;

/// Thread-local scratch buffer for temporary allocations.
///
/// Provides fast, allocation-free temporary storage that can be used
/// in Ring 0 hot path code. The buffer is automatically reset after
/// each event is processed.
///
/// # Usage Pattern
///
/// ```rust,ignore
/// use laminar_core::alloc::ScratchBuffer;
///
/// fn process_event(event: &Event) {
///     // Allocate temporary space from scratch buffer
///     let temp = ScratchBuffer::alloc(256);
///     temp.copy_from_slice(&event.data[..256]);
///
///     // Use temp...
///
///     // Reset at end of event processing
///     ScratchBuffer::reset();
/// }
/// ```
///
/// # Thread Safety
///
/// Each thread has its own scratch buffer. The buffer is NOT thread-safe
/// and should only be accessed from a single thread.
///
/// # Panics
///
/// Panics if allocation request exceeds buffer capacity.
pub struct ScratchBuffer<const SIZE: usize = DEFAULT_SCRATCH_SIZE> {
    /// The underlying buffer
    buffer: [u8; SIZE],
    /// Current allocation position
    position: usize,
}

impl<const SIZE: usize> ScratchBuffer<SIZE> {
    /// Create a new scratch buffer.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            buffer: [0; SIZE],
            position: 0,
        }
    }

    /// Get the total capacity of the scratch buffer.
    #[inline]
    #[must_use]
    pub const fn capacity(&self) -> usize {
        SIZE
    }

    /// Get the number of bytes remaining.
    #[inline]
    #[must_use]
    pub fn remaining(&self) -> usize {
        SIZE - self.position
    }

    /// Get the number of bytes used.
    #[inline]
    #[must_use]
    pub fn used(&self) -> usize {
        self.position
    }

    /// Reset the scratch buffer for reuse.
    ///
    /// Does NOT zero the memory - just resets the position.
    #[inline]
    pub fn reset(&mut self) {
        self.position = 0;
    }

    /// Allocate a slice from the scratch buffer.
    ///
    /// # Arguments
    ///
    /// * `size` - Number of bytes to allocate
    ///
    /// # Returns
    ///
    /// Mutable slice of the requested size.
    ///
    /// # Panics
    ///
    /// Panics if there isn't enough space remaining.
    #[inline]
    pub fn alloc_slice(&mut self, size: usize) -> &mut [u8] {
        let start = self.position;
        let end = start + size;

        assert!(
            end <= SIZE,
            "Scratch buffer overflow: requested {size} bytes, only {} remaining",
            self.remaining()
        );

        self.position = end;
        &mut self.buffer[start..end]
    }

    /// Try to allocate a slice, returning None if not enough space.
    #[inline]
    pub fn try_alloc_slice(&mut self, size: usize) -> Option<&mut [u8]> {
        let start = self.position;
        let end = start + size;

        if end > SIZE {
            return None;
        }

        self.position = end;
        Some(&mut self.buffer[start..end])
    }

    /// Allocate space for a value and return a mutable reference.
    ///
    /// The space is properly aligned for the type.
    ///
    /// # Panics
    ///
    /// Panics if there isn't enough space.
    #[inline]
    pub fn alloc<T: Copy + Default>(&mut self) -> &mut T {
        let align = std::mem::align_of::<T>();
        let size = std::mem::size_of::<T>();

        // Align position
        let aligned_pos = (self.position + align - 1) & !(align - 1);
        let end = aligned_pos + size;

        assert!(
            end <= SIZE,
            "Scratch buffer overflow: requested {} bytes (aligned), only {} remaining",
            size,
            SIZE - aligned_pos
        );

        self.position = end;

        // SAFETY: We've ensured the memory is properly aligned and within bounds.
        // The memory is initialized to default.
        #[allow(unsafe_code)]
        unsafe {
            let ptr = self.buffer.as_mut_ptr().add(aligned_pos).cast::<T>();
            ptr.write(T::default());
            &mut *ptr
        }
    }

    /// Allocate space for a value with initialization.
    ///
    /// # Panics
    ///
    /// Panics if there isn't enough space remaining in the buffer.
    #[inline]
    pub fn alloc_with<T: Copy>(&mut self, value: T) -> &mut T {
        let align = std::mem::align_of::<T>();
        let size = std::mem::size_of::<T>();

        // Align position
        let aligned_pos = (self.position + align - 1) & !(align - 1);
        let end = aligned_pos + size;

        assert!(
            end <= SIZE,
            "Scratch buffer overflow: requested {} bytes (aligned), only {} remaining",
            size,
            SIZE - aligned_pos
        );

        self.position = end;

        // SAFETY: We've ensured the memory is properly aligned and within bounds.
        #[allow(unsafe_code)]
        unsafe {
            let ptr = self.buffer.as_mut_ptr().add(aligned_pos).cast::<T>();
            ptr.write(value);
            &mut *ptr
        }
    }
}

impl<const SIZE: usize> Default for ScratchBuffer<SIZE> {
    fn default() -> Self {
        Self::new()
    }
}

// Thread-local scratch buffer instance
thread_local! {
    static THREAD_SCRATCH: UnsafeCell<ScratchBuffer<DEFAULT_SCRATCH_SIZE>> =
        const { UnsafeCell::new(ScratchBuffer::new()) };
}

/// Static methods for accessing thread-local scratch buffer.
impl ScratchBuffer<DEFAULT_SCRATCH_SIZE> {
    /// Allocate from the thread-local scratch buffer.
    ///
    /// # Arguments
    ///
    /// * `size` - Number of bytes to allocate
    ///
    /// # Returns
    ///
    /// Mutable slice that is valid until `reset()` is called.
    ///
    /// # Panics
    ///
    /// Panics if the requested size exceeds remaining capacity.
    ///
    /// # Safety Note
    ///
    /// The returned reference is only valid until the next `reset()` call.
    /// It's the caller's responsibility to ensure the slice is not used
    /// after reset.
    #[inline]
    #[must_use]
    pub fn thread_local_alloc(size: usize) -> &'static mut [u8] {
        THREAD_SCRATCH.with(|scratch| {
            // SAFETY: We're the only accessor on this thread, and the
            // returned reference is valid until reset() is called.
            #[allow(unsafe_code)]
            unsafe {
                (*scratch.get()).alloc_slice(size)
            }
        })
    }

    /// Reset the thread-local scratch buffer.
    ///
    /// Call this at the end of each event to reclaim space.
    #[inline]
    pub fn thread_local_reset() {
        THREAD_SCRATCH.with(|scratch| {
            // SAFETY: We're the only accessor on this thread.
            #[allow(unsafe_code)]
            unsafe {
                (*scratch.get()).reset();
            }
        });
    }

    /// Get remaining capacity in thread-local buffer.
    #[inline]
    #[must_use]
    pub fn thread_local_remaining() -> usize {
        THREAD_SCRATCH.with(|scratch| {
            // SAFETY: We're only reading.
            #[allow(unsafe_code)]
            unsafe { (*scratch.get()).remaining() }
        })
    }

    /// Get used bytes in thread-local buffer.
    #[inline]
    #[must_use]
    pub fn thread_local_used() -> usize {
        THREAD_SCRATCH.with(|scratch| {
            // SAFETY: We're only reading.
            #[allow(unsafe_code)]
            unsafe { (*scratch.get()).used() }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_buffer() {
        let buf: ScratchBuffer<1024> = ScratchBuffer::new();
        assert_eq!(buf.capacity(), 1024);
        assert_eq!(buf.remaining(), 1024);
        assert_eq!(buf.used(), 0);
    }

    #[test]
    fn test_alloc_slice() {
        let mut buf: ScratchBuffer<1024> = ScratchBuffer::new();

        let slice1 = buf.alloc_slice(100);
        assert_eq!(slice1.len(), 100);
        assert_eq!(buf.used(), 100);
        assert_eq!(buf.remaining(), 924);

        let slice2 = buf.alloc_slice(200);
        assert_eq!(slice2.len(), 200);
        assert_eq!(buf.used(), 300);
    }

    #[test]
    fn test_try_alloc_slice() {
        let mut buf: ScratchBuffer<100> = ScratchBuffer::new();

        assert!(buf.try_alloc_slice(50).is_some());
        assert!(buf.try_alloc_slice(50).is_some());
        assert!(buf.try_alloc_slice(1).is_none()); // No space left
    }

    #[test]
    fn test_reset() {
        let mut buf: ScratchBuffer<1024> = ScratchBuffer::new();

        buf.alloc_slice(500);
        assert_eq!(buf.used(), 500);

        buf.reset();
        assert_eq!(buf.used(), 0);
        assert_eq!(buf.remaining(), 1024);
    }

    #[test]
    fn test_alloc_typed() {
        let mut buf: ScratchBuffer<1024> = ScratchBuffer::new();

        let val: &mut u64 = buf.alloc();
        *val = 42;
        assert_eq!(*val, 42);

        let val2: &mut u32 = buf.alloc_with(100);
        assert_eq!(*val2, 100);
    }

    #[test]
    fn test_alignment() {
        let mut buf: ScratchBuffer<1024> = ScratchBuffer::new();

        // Allocate 1 byte to misalign
        buf.alloc_slice(1);

        // u64 requires 8-byte alignment
        let val: &mut u64 = buf.alloc();
        let ptr = val as *const u64 as usize;
        assert_eq!(ptr % 8, 0, "u64 should be 8-byte aligned");
    }

    #[test]
    #[should_panic(expected = "Scratch buffer overflow")]
    fn test_overflow_panic() {
        let mut buf: ScratchBuffer<100> = ScratchBuffer::new();
        buf.alloc_slice(101); // Should panic
    }

    #[test]
    fn test_thread_local() {
        // Reset first
        ScratchBuffer::thread_local_reset();

        assert_eq!(ScratchBuffer::thread_local_used(), 0);

        let slice = ScratchBuffer::thread_local_alloc(128);
        slice[0] = 42;
        assert_eq!(ScratchBuffer::thread_local_used(), 128);

        ScratchBuffer::thread_local_reset();
        assert_eq!(ScratchBuffer::thread_local_used(), 0);
    }

    #[test]
    fn test_multiple_allocs() {
        let mut buf: ScratchBuffer<1024> = ScratchBuffer::new();

        for i in 0..10 {
            let slice = buf.alloc_slice(50);
            slice[0] = i as u8;
        }

        assert_eq!(buf.used(), 500);
    }

    #[test]
    fn test_default() {
        let buf: ScratchBuffer<256> = ScratchBuffer::default();
        assert_eq!(buf.capacity(), 256);
    }
}
