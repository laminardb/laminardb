//! # NUMA-Aware Memory Allocator
//!
//! Provides NUMA-local memory allocation using raw libc syscalls.
//! This module uses `mmap` + `mbind` syscalls directly instead of
//! depending on libnuma.

use super::{NumaError, NumaTopology};
use std::alloc::Layout;
use std::ptr;

/// Recommended placement strategy for data structures.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NumaPlacement {
    /// Allocate on NUMA node local to specific core.
    /// Use for: per-core state stores, WAL buffers.
    Local(usize),

    /// Allocate on producer's NUMA node.
    /// Use for: SPSC queues (producer writes more frequently).
    ProducerLocal {
        /// Core ID of the producer
        producer_core: usize,
    },

    /// Interleave across all NUMA nodes.
    /// Use for: shared lookup tables (read-only, accessed by all cores equally).
    Interleaved,

    /// No NUMA preference (for non-latency-critical data).
    /// Use for: checkpoints, background buffers.
    Any,
}

impl NumaPlacement {
    /// Get placement recommendation for a per-core state store.
    #[must_use]
    pub fn for_state_store(core_id: usize) -> Self {
        Self::Local(core_id)
    }

    /// Get placement recommendation for a per-core WAL buffer.
    #[must_use]
    pub fn for_wal_buffer(core_id: usize) -> Self {
        Self::Local(core_id)
    }

    /// Get placement recommendation for an SPSC queue.
    #[must_use]
    pub fn for_spsc_queue(producer_core: usize) -> Self {
        Self::ProducerLocal { producer_core }
    }

    /// Get placement recommendation for a shared lookup table.
    #[must_use]
    pub fn for_lookup_table() -> Self {
        Self::Interleaved
    }

    /// Get placement recommendation for checkpoint data.
    #[must_use]
    pub fn for_checkpoint() -> Self {
        Self::Any
    }
}

/// NUMA-aware memory allocator.
///
/// Uses raw libc syscalls (`mmap` + `mbind`) for NUMA-local allocation.
/// Falls back to standard allocation on non-NUMA systems.
#[derive(Debug)]
pub struct NumaAllocator {
    /// Reference topology
    topology: NumaTopology,
}

impl NumaAllocator {
    /// Create a new NUMA allocator with the given topology.
    #[must_use]
    pub fn new(topology: &NumaTopology) -> Self {
        Self {
            topology: topology.clone(),
        }
    }

    /// Allocate memory on the NUMA node local to the current CPU.
    ///
    /// # Errors
    ///
    /// Returns error if allocation fails.
    pub fn alloc_local(&self, size: usize, align: usize) -> Result<*mut u8, NumaError> {
        let node = self.topology.current_node();
        self.alloc_on_node(node, size, align)
    }

    /// Allocate memory on a specific NUMA node.
    ///
    /// # Errors
    ///
    /// Returns error if the node is invalid or allocation fails.
    pub fn alloc_on_node(
        &self,
        node: usize,
        size: usize,
        align: usize,
    ) -> Result<*mut u8, NumaError> {
        if node >= self.topology.num_nodes() {
            return Err(NumaError::InvalidNode {
                node,
                available: self.topology.num_nodes(),
            });
        }

        // Allocate via mmap
        let ptr = self.mmap_alloc(size, align)?;

        // Bind to NUMA node on Linux
        #[cfg(target_os = "linux")]
        {
            if self.topology.is_numa() {
                self.bind_to_node(ptr, size, node)?;
            }
        }

        Ok(ptr)
    }

    /// Allocate memory interleaved across all NUMA nodes.
    ///
    /// Best for shared read-only data accessed equally by all cores.
    ///
    /// # Errors
    ///
    /// Returns error if allocation fails.
    pub fn alloc_interleaved(&self, size: usize, align: usize) -> Result<*mut u8, NumaError> {
        // Allocate via mmap
        let ptr = self.mmap_alloc(size, align)?;

        // Bind interleaved on Linux
        #[cfg(target_os = "linux")]
        {
            if self.topology.is_numa() {
                self.bind_interleaved(ptr, size)?;
            }
        }

        Ok(ptr)
    }

    /// Allocate memory with a placement strategy.
    ///
    /// # Errors
    ///
    /// Returns error if allocation fails.
    pub fn alloc_with_placement(
        &self,
        placement: NumaPlacement,
        size: usize,
        align: usize,
    ) -> Result<*mut u8, NumaError> {
        match placement {
            NumaPlacement::Local(core_id) => {
                let node = self.topology.node_for_cpu(core_id);
                self.alloc_on_node(node, size, align)
            }
            NumaPlacement::ProducerLocal { producer_core } => {
                let node = self.topology.node_for_cpu(producer_core);
                self.alloc_on_node(node, size, align)
            }
            NumaPlacement::Interleaved => self.alloc_interleaved(size, align),
            NumaPlacement::Any => self.mmap_alloc(size, align),
        }
    }

    /// Allocate with Layout.
    ///
    /// # Errors
    ///
    /// Returns error if allocation fails.
    pub fn alloc_layout(&self, layout: Layout) -> Result<*mut u8, NumaError> {
        self.alloc_local(layout.size(), layout.align())
    }

    /// Allocate with Layout on a specific node.
    ///
    /// # Errors
    ///
    /// Returns error if allocation fails.
    pub fn alloc_layout_on_node(&self, layout: Layout, node: usize) -> Result<*mut u8, NumaError> {
        self.alloc_on_node(node, layout.size(), layout.align())
    }

    /// Deallocate NUMA-allocated memory.
    ///
    /// # Safety
    ///
    /// The pointer must have been allocated by this allocator with the same size
    /// and alignment.
    ///
    /// # Note
    ///
    /// On non-Linux platforms, you must pass the same alignment used during
    /// allocation. Use the overload with alignment parameter for non-Linux,
    /// or ensure you only use this on Linux.
    pub unsafe fn dealloc(&self, ptr: *mut u8, size: usize) {
        self.dealloc_with_align(ptr, size, 64);
    }

    /// Deallocate NUMA-allocated memory with explicit alignment.
    ///
    /// # Safety
    ///
    /// The pointer must have been allocated by this allocator with the same
    /// size and alignment.
    pub unsafe fn dealloc_with_align(&self, ptr: *mut u8, size: usize, align: usize) {
        if ptr.is_null() || size == 0 {
            return;
        }

        #[cfg(target_os = "linux")]
        {
            // SAFETY: Caller guarantees ptr was allocated by mmap with this size
            let _ = align; // Not needed for munmap
            libc::munmap(ptr.cast(), size);
        }

        #[cfg(not(target_os = "linux"))]
        {
            // On non-Linux, we used std alloc with matching alignment
            if let Ok(layout) =
                Layout::from_size_align(size, align.max(std::mem::align_of::<usize>()))
            {
                std::alloc::dealloc(ptr, layout);
            }
        }
    }

    /// Allocate memory using mmap.
    #[allow(clippy::unused_self, unused_variables)]
    fn mmap_alloc(&self, size: usize, align: usize) -> Result<*mut u8, NumaError> {
        if size == 0 {
            return Ok(ptr::null_mut());
        }

        #[cfg(target_os = "linux")]
        {
            // SAFETY: mmap with MAP_ANONYMOUS | MAP_PRIVATE is safe
            let ptr = unsafe {
                libc::mmap(
                    ptr::null_mut(),
                    size,
                    libc::PROT_READ | libc::PROT_WRITE,
                    libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
                    -1,
                    0,
                )
            };

            if ptr == libc::MAP_FAILED {
                return Err(NumaError::AllocationFailed(format!(
                    "mmap failed: {}",
                    std::io::Error::last_os_error()
                )));
            }

            // Try to enable huge pages (non-fatal if fails)
            #[cfg(target_os = "linux")]
            // SAFETY: `ptr` was just returned successfully from mmap (not MAP_FAILED),
            // `size` matches the mmap region size, and MADV_HUGEPAGE is an advisory hint
            // that cannot cause memory unsafety even if it fails.
            unsafe {
                libc::madvise(ptr, size, libc::MADV_HUGEPAGE);
            }

            Ok(ptr.cast())
        }

        #[cfg(not(target_os = "linux"))]
        {
            // Fallback to standard allocation
            let layout = Layout::from_size_align(size, align.max(std::mem::align_of::<usize>()))
                .map_err(|e| NumaError::AllocationFailed(e.to_string()))?;

            // SAFETY: layout is valid
            let ptr = unsafe { std::alloc::alloc(layout) };
            if ptr.is_null() {
                return Err(NumaError::AllocationFailed(
                    "std::alloc::alloc failed".to_string(),
                ));
            }

            Ok(ptr)
        }
    }

    /// Bind memory to a specific NUMA node using mbind syscall.
    #[cfg(target_os = "linux")]
    fn bind_to_node(&self, ptr: *mut u8, size: usize, node: usize) -> Result<(), NumaError> {
        // Build nodemask - a bitmask where bit N is set if node N should be used
        let mut nodemask: u64 = 0;
        if node < 64 {
            nodemask = 1u64 << node;
        }

        // MPOL_BIND = 2 - strictly bind to the specified nodes
        const MPOL_BIND: i32 = 2;
        // MPOL_MF_MOVE = 2 - move pages to the node if they're already faulted
        const MPOL_MF_MOVE: u32 = 2;

        // SAFETY: mbind is a valid syscall when called with proper arguments
        let result = unsafe {
            libc::syscall(
                libc::SYS_mbind,
                ptr,
                size,
                MPOL_BIND,
                &nodemask as *const u64,
                64usize, // maxnode
                MPOL_MF_MOVE,
            )
        };

        if result < 0 {
            // mbind failure is non-fatal on single-node systems
            let err = std::io::Error::last_os_error();
            if err.raw_os_error() != Some(libc::ENOSYS) {
                tracing::warn!("mbind to node {} failed (non-fatal): {}", node, err);
            }
        }

        Ok(())
    }

    /// Bind memory interleaved across all nodes using mbind syscall.
    #[cfg(target_os = "linux")]
    fn bind_interleaved(&self, ptr: *mut u8, size: usize) -> Result<(), NumaError> {
        // Build nodemask with all nodes enabled
        let mut nodemask: u64 = 0;
        for node in 0..self.topology.num_nodes().min(64) {
            nodemask |= 1u64 << node;
        }

        // MPOL_INTERLEAVE = 3 - interleave pages across nodes
        const MPOL_INTERLEAVE: i32 = 3;

        // SAFETY: mbind is a valid syscall when called with proper arguments
        let result = unsafe {
            libc::syscall(
                libc::SYS_mbind,
                ptr,
                size,
                MPOL_INTERLEAVE,
                &nodemask as *const u64,
                64usize, // maxnode
                0u32,    // flags
            )
        };

        if result < 0 {
            let err = std::io::Error::last_os_error();
            if err.raw_os_error() != Some(libc::ENOSYS) {
                tracing::warn!("mbind interleaved failed (non-fatal): {}", err);
            }
        }

        Ok(())
    }

    /// Get the NUMA node where memory is located.
    ///
    /// Returns the NUMA node for the page containing the given address.
    #[cfg(target_os = "linux")]
    #[must_use]
    pub fn get_memory_node(&self, ptr: *const u8) -> Option<usize> {
        if ptr.is_null() {
            return None;
        }

        let mut status: i32 = -1;
        let page_ptr = ptr as *mut libc::c_void;

        // SAFETY: move_pages with a single page query is safe
        let result = unsafe {
            libc::syscall(
                libc::SYS_move_pages,
                0i32, // self
                1usize,
                &page_ptr as *const *mut libc::c_void,
                ptr::null::<i32>(), // NULL = query only
                &mut status as *mut i32,
                0i32, // flags
            )
        };

        if result == 0 && status >= 0 {
            Some(status as usize)
        } else {
            None
        }
    }

    /// Get the NUMA node where memory is located (non-Linux fallback).
    ///
    /// On non-Linux platforms, always returns `Some(0)` as NUMA is not supported.
    #[cfg(not(target_os = "linux"))]
    #[must_use]
    pub fn get_memory_node(&self, _ptr: *const u8) -> Option<usize> {
        // Not available on non-Linux
        Some(0)
    }

    /// Get the topology reference.
    #[must_use]
    pub fn topology(&self) -> &NumaTopology {
        &self.topology
    }
}

/// A NUMA-local memory region.
///
/// Wraps a NUMA-allocated buffer with automatic cleanup.
#[derive(Debug)]
#[allow(dead_code)] // Public API for NUMA-local allocations in thread-per-core runtime
pub struct NumaBuffer {
    ptr: *mut u8,
    size: usize,
    align: usize,
    node: usize,
}

// SAFETY: NumaBuffer can be sent between threads
unsafe impl Send for NumaBuffer {}

#[allow(dead_code)] // Public API for NUMA-local allocations in thread-per-core runtime
impl NumaBuffer {
    /// Default alignment for NUMA buffers (cache line size).
    const DEFAULT_ALIGN: usize = 64;

    /// Allocate a new NUMA-local buffer on the specified node.
    ///
    /// # Errors
    ///
    /// Returns error if allocation fails.
    pub fn new(allocator: &NumaAllocator, node: usize, size: usize) -> Result<Self, NumaError> {
        let align = Self::DEFAULT_ALIGN;
        let ptr = allocator.alloc_on_node(node, size, align)?;
        Ok(Self {
            ptr,
            size,
            align,
            node,
        })
    }

    /// Allocate a buffer local to the current CPU's NUMA node.
    ///
    /// # Errors
    ///
    /// Returns error if allocation fails.
    pub fn local(allocator: &NumaAllocator, size: usize) -> Result<Self, NumaError> {
        let node = allocator.topology().current_node();
        Self::new(allocator, node, size)
    }

    /// Get a pointer to the buffer.
    #[must_use]
    pub fn as_ptr(&self) -> *const u8 {
        self.ptr
    }

    /// Get a mutable pointer to the buffer.
    #[must_use]
    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.ptr
    }

    /// Get the buffer size.
    #[must_use]
    pub fn size(&self) -> usize {
        self.size
    }

    /// Get the NUMA node this buffer is allocated on.
    #[must_use]
    pub fn node(&self) -> usize {
        self.node
    }

    /// Get as a byte slice.
    ///
    /// # Safety
    ///
    /// The buffer must be properly initialized.
    #[must_use]
    pub unsafe fn as_slice(&self) -> &[u8] {
        std::slice::from_raw_parts(self.ptr, self.size)
    }

    /// Get as a mutable byte slice.
    ///
    /// # Safety
    ///
    /// The buffer must be properly initialized.
    #[must_use]
    pub unsafe fn as_mut_slice(&mut self) -> &mut [u8] {
        std::slice::from_raw_parts_mut(self.ptr, self.size)
    }
}

impl Drop for NumaBuffer {
    fn drop(&mut self) {
        if !self.ptr.is_null() && self.size > 0 {
            #[cfg(target_os = "linux")]
            unsafe {
                libc::munmap(self.ptr.cast(), self.size);
            }

            #[cfg(not(target_os = "linux"))]
            unsafe {
                // Use the same alignment that was used during allocation
                if let Ok(layout) = std::alloc::Layout::from_size_align(self.size, self.align) {
                    std::alloc::dealloc(self.ptr, layout);
                }
            }
        }
    }
}

/// A NUMA-aware vector that stores elements on a specific NUMA node.
///
/// Unlike `Vec`, this allocates memory on a specified NUMA node for
/// better memory locality in thread-per-core architectures.
#[derive(Debug)]
#[allow(dead_code)] // Public API for NUMA-aware typed collections
pub struct NumaVec<T> {
    ptr: *mut T,
    len: usize,
    capacity: usize,
    node: usize,
}

// SAFETY: NumaVec is Send if T is Send
unsafe impl<T: Send> Send for NumaVec<T> {}

#[allow(dead_code)] // Public API for NUMA-aware typed collections
impl<T> NumaVec<T> {
    /// Create a new empty NUMA-local vector.
    #[must_use]
    pub fn new() -> Self {
        Self {
            ptr: ptr::null_mut(),
            len: 0,
            capacity: 0,
            node: 0,
        }
    }

    /// Create a new NUMA-local vector with capacity on a specific node.
    ///
    /// # Errors
    ///
    /// Returns error if allocation fails.
    pub fn with_capacity_on_node(
        allocator: &NumaAllocator,
        capacity: usize,
        node: usize,
    ) -> Result<Self, NumaError> {
        if capacity == 0 {
            return Ok(Self::new());
        }

        let layout =
            Layout::array::<T>(capacity).map_err(|e| NumaError::AllocationFailed(e.to_string()))?;

        let ptr = allocator.alloc_on_node(node, layout.size(), layout.align())?;

        Ok(Self {
            ptr: ptr.cast(),
            len: 0,
            capacity,
            node,
        })
    }

    /// Get the length.
    #[must_use]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Check if empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Get the capacity.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Get the NUMA node.
    #[must_use]
    pub fn node(&self) -> usize {
        self.node
    }

    /// Push an element (if capacity allows).
    ///
    /// # Panics
    ///
    /// Panics if the vector is at capacity (no reallocation for now).
    pub fn push(&mut self, value: T) {
        assert!(self.len < self.capacity, "NumaVec at capacity");

        // SAFETY: We have capacity and ptr is valid
        unsafe {
            self.ptr.add(self.len).write(value);
        }
        self.len += 1;
    }

    /// Pop an element.
    pub fn pop(&mut self) -> Option<T> {
        if self.len == 0 {
            return None;
        }

        self.len -= 1;
        // SAFETY: len was > 0 so this slot is valid
        Some(unsafe { self.ptr.add(self.len).read() })
    }

    /// Get a reference to an element.
    #[must_use]
    pub fn get(&self, index: usize) -> Option<&T> {
        if index >= self.len {
            return None;
        }
        // SAFETY: index is in bounds
        Some(unsafe { &*self.ptr.add(index) })
    }

    /// Get a mutable reference to an element.
    pub fn get_mut(&mut self, index: usize) -> Option<&mut T> {
        if index >= self.len {
            return None;
        }
        // SAFETY: index is in bounds
        Some(unsafe { &mut *self.ptr.add(index) })
    }

    /// Clear the vector.
    pub fn clear(&mut self) {
        // Drop all elements
        for i in 0..self.len {
            // SAFETY: i < len so slot is valid
            unsafe {
                ptr::drop_in_place(self.ptr.add(i));
            }
        }
        self.len = 0;
    }

    /// Get as a slice.
    #[must_use]
    pub fn as_slice(&self) -> &[T] {
        if self.len == 0 {
            return &[];
        }
        // SAFETY: ptr is valid for len elements
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }

    /// Get as a mutable slice.
    #[must_use]
    pub fn as_mut_slice(&mut self) -> &mut [T] {
        if self.len == 0 {
            return &mut [];
        }
        // SAFETY: ptr is valid for len elements
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) }
    }
}

impl<T> Default for NumaVec<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Drop for NumaVec<T> {
    #[allow(unused_variables)]
    fn drop(&mut self) {
        // Drop all elements first
        self.clear();

        // Free memory
        if !self.ptr.is_null() && self.capacity > 0 {
            let size = self.capacity * std::mem::size_of::<T>();

            #[cfg(target_os = "linux")]
            unsafe {
                libc::munmap(self.ptr.cast(), size);
            }

            #[cfg(not(target_os = "linux"))]
            unsafe {
                if let Ok(layout) = Layout::array::<T>(self.capacity) {
                    std::alloc::dealloc(self.ptr.cast(), layout);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_allocator_new() {
        let topo = NumaTopology::detect();
        let allocator = NumaAllocator::new(&topo);
        assert_eq!(allocator.topology().num_nodes(), topo.num_nodes());
    }

    #[test]
    fn test_alloc_local() {
        let topo = NumaTopology::detect();
        let allocator = NumaAllocator::new(&topo);

        let ptr = allocator.alloc_local(4096, 64).unwrap();
        assert!(!ptr.is_null());

        unsafe { allocator.dealloc(ptr, 4096) };
    }

    #[test]
    fn test_alloc_on_node() {
        let topo = NumaTopology::detect();
        let allocator = NumaAllocator::new(&topo);

        let ptr = allocator.alloc_on_node(0, 4096, 64).unwrap();
        assert!(!ptr.is_null());

        unsafe { allocator.dealloc(ptr, 4096) };
    }

    #[test]
    fn test_alloc_invalid_node() {
        let topo = NumaTopology::detect();
        let allocator = NumaAllocator::new(&topo);

        let result = allocator.alloc_on_node(999, 4096, 64);
        assert!(result.is_err());
    }

    #[test]
    fn test_alloc_interleaved() {
        let topo = NumaTopology::detect();
        let allocator = NumaAllocator::new(&topo);

        let ptr = allocator.alloc_interleaved(64 * 1024, 64).unwrap();
        assert!(!ptr.is_null());

        unsafe { allocator.dealloc(ptr, 64 * 1024) };
    }

    #[test]
    fn test_alloc_with_placement() {
        let topo = NumaTopology::detect();
        let allocator = NumaAllocator::new(&topo);

        // Local
        let ptr = allocator
            .alloc_with_placement(NumaPlacement::Local(0), 4096, 64)
            .unwrap();
        assert!(!ptr.is_null());
        unsafe { allocator.dealloc(ptr, 4096) };

        // Interleaved
        let ptr = allocator
            .alloc_with_placement(NumaPlacement::Interleaved, 4096, 64)
            .unwrap();
        assert!(!ptr.is_null());
        unsafe { allocator.dealloc(ptr, 4096) };

        // Any
        let ptr = allocator
            .alloc_with_placement(NumaPlacement::Any, 4096, 64)
            .unwrap();
        assert!(!ptr.is_null());
        unsafe { allocator.dealloc(ptr, 4096) };
    }

    #[test]
    fn test_numa_buffer() {
        let topo = NumaTopology::detect();
        let allocator = NumaAllocator::new(&topo);

        let mut buf = NumaBuffer::new(&allocator, 0, 4096).unwrap();
        assert!(!buf.as_ptr().is_null());
        assert_eq!(buf.size(), 4096);
        assert_eq!(buf.node(), 0);

        // Write to buffer
        unsafe {
            let slice = buf.as_mut_slice();
            slice[0] = 42;
            assert_eq!(slice[0], 42);
        }
    }

    #[test]
    fn test_numa_buffer_local() {
        let topo = NumaTopology::detect();
        let allocator = NumaAllocator::new(&topo);

        let buf = NumaBuffer::local(&allocator, 4096).unwrap();
        assert!(!buf.as_ptr().is_null());
        assert_eq!(buf.size(), 4096);
    }

    #[test]
    fn test_numa_vec() {
        let topo = NumaTopology::detect();
        let allocator = NumaAllocator::new(&topo);

        let mut vec: NumaVec<i32> = NumaVec::with_capacity_on_node(&allocator, 100, 0).unwrap();

        assert!(vec.is_empty());
        assert_eq!(vec.capacity(), 100);

        vec.push(1);
        vec.push(2);
        vec.push(3);

        assert_eq!(vec.len(), 3);
        assert_eq!(vec.get(0), Some(&1));
        assert_eq!(vec.get(1), Some(&2));
        assert_eq!(vec.get(2), Some(&3));

        assert_eq!(vec.pop(), Some(3));
        assert_eq!(vec.len(), 2);

        vec.clear();
        assert!(vec.is_empty());
    }

    #[test]
    fn test_numa_vec_slice() {
        let topo = NumaTopology::detect();
        let allocator = NumaAllocator::new(&topo);

        let mut vec: NumaVec<i32> = NumaVec::with_capacity_on_node(&allocator, 10, 0).unwrap();

        vec.push(10);
        vec.push(20);
        vec.push(30);

        let slice = vec.as_slice();
        assert_eq!(slice, &[10, 20, 30]);

        let mut_slice = vec.as_mut_slice();
        mut_slice[1] = 25;
        assert_eq!(vec.as_slice(), &[10, 25, 30]);
    }

    #[test]
    fn test_placement_helpers() {
        assert!(matches!(
            NumaPlacement::for_state_store(5),
            NumaPlacement::Local(5)
        ));

        assert!(matches!(
            NumaPlacement::for_wal_buffer(3),
            NumaPlacement::Local(3)
        ));

        assert!(matches!(
            NumaPlacement::for_spsc_queue(2),
            NumaPlacement::ProducerLocal { producer_core: 2 }
        ));

        assert!(matches!(
            NumaPlacement::for_lookup_table(),
            NumaPlacement::Interleaved
        ));

        assert!(matches!(
            NumaPlacement::for_checkpoint(),
            NumaPlacement::Any
        ));
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn test_get_memory_node() {
        let topo = NumaTopology::detect();
        let allocator = NumaAllocator::new(&topo);

        let ptr = allocator.alloc_on_node(0, 4096, 64).unwrap();

        // Touch the memory to fault in the page
        unsafe {
            ptr.write(42);
        }

        // Query should return node 0 (or None if NUMA not available)
        let node = allocator.get_memory_node(ptr);
        if topo.is_numa() {
            // On NUMA systems, should return a valid node
            assert!(node.is_some());
        }

        unsafe { allocator.dealloc(ptr, 4096) };
    }
}
