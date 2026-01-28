# F068: NUMA-Aware Memory Allocation

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F068 |
| **Status** | ✅ Done |
| **Priority** | P0 |
| **Phase** | 2 |
| **Effort** | L (1-2 weeks) |
| **Dependencies** | F013 |
| **Owner** | TBD |
| **Research** | [Thread-Per-Core 2026 Research](../../research/laminardb-thread-per-core-2026-research.md) |

## Implementation Notes

**Completed**: 2026-01-24

### Files Created
- `crates/laminar-core/src/numa/mod.rs` - Module exports and integration tests
- `crates/laminar-core/src/numa/error.rs` - NumaError enum
- `crates/laminar-core/src/numa/topology.rs` - NumaTopology detection (sysfs + hwlocality)
- `crates/laminar-core/src/numa/allocator.rs` - NumaAllocator, NumaPlacement, NumaBuffer, NumaVec

### Key Design Decisions
1. **Raw libc syscalls** - Uses `mmap` + `mbind` (SYS_mbind) instead of `libnuma` crate for lighter dependencies
2. **Optional hwlocality** - Feature flag `hwloc` for enhanced topology discovery, falls back to sysfs
3. **Platform fallback** - Single-node topology on non-Linux (Windows, macOS)
4. **Cache-line alignment** - All allocations aligned to 64 bytes
5. **Huge page hints** - Uses `MADV_HUGEPAGE` for large allocations

### API Usage
```rust
use laminar_core::numa::{NumaTopology, NumaAllocator, NumaPlacement};

// Detect topology
let topo = NumaTopology::detect();
println!("{}", topo.summary());

// Create allocator
let alloc = NumaAllocator::new();

// Allocate NUMA-local memory
let buf = alloc.alloc_local(4096, 64)?;

// Allocate on specific node
let buf = alloc.alloc_on_node(0, 4096, 64)?;

// Allocate interleaved across nodes
let buf = alloc.alloc_interleaved(4096, 64)?;

// Use NumaPlacement enum
let buf = alloc.alloc_with_placement(4096, 64, NumaPlacement::Local(0))?;
```

### Integration Points
- `CoreConfig.numa_aware` - Enable NUMA-aware allocation per core
- `CoreHandle.numa_node` - Reports NUMA node for the core
- `TpcConfig.numa_aware()` - Builder method for runtime config
- `CoreStats.numa_node` - NUMA node in stats output

### Tests
- 11 unit tests in numa module
- Integration tests for topology detection and allocation

## Summary

Implement NUMA-aware memory allocation strategy for thread-per-core architecture. On multi-socket systems, memory access latency varies by 2-3x depending on whether memory is local or remote to the CPU. This spec ensures all per-core state is allocated on the NUMA node local to that core.

## Motivation

### NUMA Topology Impact

```
NUMA Topology (2-socket example):

┌─────────────────────┐         ┌─────────────────────┐
│      Socket 0       │         │      Socket 1       │
│  ┌───────────────┐  │         │  ┌───────────────┐  │
│  │  Cores 0-15   │  │         │  │  Cores 16-31  │  │
│  └───────┬───────┘  │         │  └───────┬───────┘  │
│          │          │         │          │          │
│  ┌───────▼───────┐  │  QPI    │  ┌───────▼───────┐  │
│  │  Local DRAM   │◄─┼─────────┼─►│  Local DRAM   │  │
│  │  (~100ns)     │  │ (~150ns)│  │  (~100ns)     │  │
│  └───────────────┘  │         │  └───────────────┘  │
└─────────────────────┘         └─────────────────────┘
```

### Current Gap

| Component | Current | NUMA-Aware Target |
|-----------|---------|-------------------|
| State stores | Generic allocation | NUMA-local per core |
| WAL buffers | Generic allocation | NUMA-local per core |
| SPSC queues | Generic allocation | Producer's NUMA node |
| Shared tables | Generic allocation | Interleaved across nodes |

### Performance Impact

- **Local DRAM access**: ~100ns
- **Remote DRAM access**: ~150ns (1.5x penalty)
- **Cache miss to remote**: ~200-300ns (2-3x penalty)

For Ring 0 operations targeting <500ns, remote NUMA access can consume 20-60% of budget.

## Goals

1. Implement `NumaAllocator` with per-node allocation
2. Update `StateStore` to use NUMA-local allocation
3. Update WAL buffers to use NUMA-local allocation
4. Add NUMA node detection per core
5. Interleaved allocation for shared read-only data
6. Benchmark cross-NUMA vs local access

## Non-Goals

- CXL memory tier support (see F072 future)
- Dynamic NUMA rebalancing
- Non-Linux platforms (NUMA APIs are Linux-specific)

## Technical Design

### NUMA Allocator

```rust
use std::alloc::{Layout, GlobalAlloc};

/// NUMA-aware memory allocator for LaminarDB.
///
/// Provides per-node allocation pools and interleaved allocation
/// for shared data structures.
pub struct NumaAllocator {
    /// Pre-allocated pools per NUMA node
    pools: Vec<NumaPool>,
    /// Number of NUMA nodes in system
    num_nodes: usize,
}

/// Per-NUMA-node memory pool.
pub struct NumaPool {
    node_id: usize,
    /// Arena allocator for small allocations (<2MB)
    arena: bumpalo::Bump,
    /// Large allocations tracking for cleanup
    large_allocs: Vec<(*mut u8, usize)>,
}

impl NumaAllocator {
    /// Create new NUMA allocator, detecting system topology.
    pub fn new() -> Self {
        let num_nodes = unsafe {
            libc::numa_num_configured_nodes() as usize
        };

        let pools = (0..num_nodes)
            .map(|node_id| NumaPool::new(node_id))
            .collect();

        Self { pools, num_nodes }
    }

    /// Allocate on the NUMA node local to current CPU.
    ///
    /// Use this for per-core data structures (state stores, buffers).
    #[inline]
    pub fn alloc_local(&self, layout: Layout) -> *mut u8 {
        let cpu = unsafe { libc::sched_getcpu() } as usize;
        let node = unsafe {
            libc::numa_node_of_cpu(cpu as i32) as usize
        };
        self.alloc_on_node(node, layout)
    }

    /// Allocate on specific NUMA node.
    ///
    /// Use this when you know the target core at allocation time.
    pub fn alloc_on_node(&self, node: usize, layout: Layout) -> *mut u8 {
        if layout.size() < 2 * 1024 * 1024 {
            // Small allocation: use arena
            self.pools[node].arena.alloc_layout(layout).as_ptr()
        } else {
            // Large allocation: direct NUMA alloc
            let ptr = unsafe {
                libc::numa_alloc_onnode(layout.size(), node as i32) as *mut u8
            };
            // Track for cleanup (in production, use proper tracking)
            ptr
        }
    }

    /// Allocate interleaved across all NUMA nodes.
    ///
    /// Use for shared read-only data accessed by all cores equally
    /// (e.g., lookup tables, configuration).
    pub fn alloc_interleaved(&self, layout: Layout) -> *mut u8 {
        unsafe {
            libc::numa_alloc_interleaved(layout.size()) as *mut u8
        }
    }

    /// Free NUMA-allocated memory.
    pub unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        if layout.size() >= 2 * 1024 * 1024 {
            libc::numa_free(ptr as *mut libc::c_void, layout.size());
        }
        // Small allocations freed when arena is dropped
    }

    /// Get NUMA node for a given CPU core.
    pub fn node_for_cpu(&self, cpu: usize) -> usize {
        unsafe { libc::numa_node_of_cpu(cpu as i32) as usize }
    }
}

impl NumaPool {
    fn new(node_id: usize) -> Self {
        // Pre-allocate arena memory on this NUMA node
        let arena_size = 64 * 1024 * 1024; // 64MB per node
        let arena_mem = unsafe {
            libc::numa_alloc_onnode(arena_size, node_id as i32)
        };

        // Create bumpalo arena backed by NUMA-local memory
        // Note: In production, use custom arena that uses NUMA memory

        Self {
            node_id,
            arena: bumpalo::Bump::with_capacity(arena_size),
            large_allocs: Vec::new(),
        }
    }
}
```

### NUMA-Aware State Store

```rust
impl StateStore {
    /// Create state store with memory on correct NUMA node.
    ///
    /// All state for this store is allocated on the NUMA node
    /// local to the owning core.
    pub fn new_numa_aware(core_id: usize, size: usize) -> Self {
        let numa_node = unsafe {
            libc::numa_node_of_cpu(core_id as i32) as usize
        };

        // Allocate state store memory on local NUMA node
        let memory = unsafe {
            let ptr = libc::numa_alloc_onnode(size, numa_node as i32);
            if ptr.is_null() {
                panic!("NUMA allocation failed for {} bytes on node {}",
                       size, numa_node);
            }

            // Enable huge pages if available (reduces TLB pressure)
            libc::madvise(ptr, size, libc::MADV_HUGEPAGE);

            ptr as *mut u8
        };

        Self {
            memory,
            size,
            numa_node,
            core_id,
        }
    }
}
```

### NUMA-Aware WAL Buffers

```rust
/// Per-core WAL buffer allocated on NUMA-local memory.
pub struct NumaWalBuffer {
    /// Core this buffer belongs to
    core_id: usize,
    /// NUMA node for this buffer
    numa_node: usize,
    /// Buffer memory (NUMA-local)
    buffer: *mut u8,
    /// Buffer capacity
    capacity: usize,
    /// Current write position
    position: usize,
}

impl NumaWalBuffer {
    pub fn new(core_id: usize, capacity: usize) -> Self {
        let numa_node = unsafe {
            libc::numa_node_of_cpu(core_id as i32) as usize
        };

        let buffer = unsafe {
            let ptr = libc::numa_alloc_onnode(capacity, numa_node as i32);
            if ptr.is_null() {
                panic!("NUMA allocation failed for WAL buffer");
            }
            ptr as *mut u8
        };

        Self {
            core_id,
            numa_node,
            buffer,
            capacity,
            position: 0,
        }
    }
}
```

### NUMA-Aware SPSC Queues

```rust
impl<T> SpscQueue<T> {
    /// Create SPSC queue with buffer on producer's NUMA node.
    ///
    /// Data flows producer → consumer, so buffer should be
    /// NUMA-local to producer (writer) for best write performance.
    /// Consumer reads are slightly slower but less frequent.
    pub fn new_numa_aware(capacity: usize, producer_core: usize) -> Self {
        let numa_node = unsafe {
            libc::numa_node_of_cpu(producer_core as i32) as usize
        };

        let layout = Layout::array::<T>(capacity).unwrap();
        let buffer = unsafe {
            libc::numa_alloc_onnode(layout.size(), numa_node as i32) as *mut T
        };

        Self {
            buffer,
            capacity,
            head: CachePadded::new(AtomicUsize::new(0)),
            tail: CachePadded::new(AtomicUsize::new(0)),
            numa_node,
        }
    }
}
```

### Data Placement Strategy

```rust
/// Recommended placement for LaminarDB data structures.
pub enum NumaPlacement {
    /// Allocate on NUMA node local to specific core.
    /// Use for: per-core state stores, WAL buffers.
    Local(usize),

    /// Allocate on producer's NUMA node.
    /// Use for: SPSC queues (producer writes, consumer reads).
    ProducerLocal { producer_core: usize },

    /// Interleave across all NUMA nodes.
    /// Use for: shared lookup tables (read-only, accessed by all).
    Interleaved,

    /// Any node (for non-latency-critical data).
    /// Use for: checkpoints, background buffers.
    Any,
}

impl NumaPlacement {
    /// Get placement recommendation for data type.
    pub fn for_data_type(data_type: &str) -> Self {
        match data_type {
            "state_store" => NumaPlacement::Local(0),  // Core-specific
            "wal_buffer" => NumaPlacement::Local(0),   // Core-specific
            "spsc_queue" => NumaPlacement::ProducerLocal { producer_core: 0 },
            "lookup_table" => NumaPlacement::Interleaved,
            "checkpoint" => NumaPlacement::Any,
            _ => NumaPlacement::Any,
        }
    }
}
```

### Integration with F013 CoreHandle

```rust
impl CoreHandle {
    /// Create CoreHandle with NUMA-aware allocations.
    pub fn new_numa_aware(core_id: usize, config: &CoreConfig) -> Self {
        let numa_node = unsafe {
            libc::numa_node_of_cpu(core_id as i32) as usize
        };

        // Allocate state store on NUMA-local memory
        let state_store = StateStore::new_numa_aware(
            core_id,
            config.state_store_size,
        );

        // Create SPSC queues with NUMA awareness
        // Inbox: main thread → core (main thread is producer)
        let inbox = SpscQueue::new_numa_aware(
            config.inbox_capacity,
            0, // Main thread on core 0
        );

        // Outbox: core → main thread (this core is producer)
        let outbox = SpscQueue::new_numa_aware(
            config.outbox_capacity,
            core_id,
        );

        Self {
            core_id,
            numa_node,
            state_store,
            inbox,
            outbox,
            // ...
        }
    }
}
```

### NUMA Topology Detection

```rust
/// Detect and report NUMA topology.
pub struct NumaTopology {
    /// Number of NUMA nodes
    pub num_nodes: usize,
    /// CPUs per node
    pub cpus_per_node: Vec<Vec<usize>>,
    /// Memory per node (bytes)
    pub memory_per_node: Vec<u64>,
}

impl NumaTopology {
    pub fn detect() -> Self {
        let num_nodes = unsafe {
            libc::numa_num_configured_nodes() as usize
        };

        let mut cpus_per_node = vec![Vec::new(); num_nodes];
        let num_cpus = unsafe { libc::sysconf(libc::_SC_NPROCESSORS_ONLN) } as usize;

        for cpu in 0..num_cpus {
            let node = unsafe {
                libc::numa_node_of_cpu(cpu as i32) as usize
            };
            cpus_per_node[node].push(cpu);
        }

        // Get memory per node
        let memory_per_node = (0..num_nodes)
            .map(|node| {
                unsafe {
                    let mut size: libc::c_long = 0;
                    libc::numa_node_size(node as i32, &mut size);
                    size as u64
                }
            })
            .collect();

        Self {
            num_nodes,
            cpus_per_node,
            memory_per_node,
        }
    }

    /// Log topology for debugging.
    pub fn log_topology(&self) {
        tracing::info!("NUMA Topology: {} nodes", self.num_nodes);
        for (node, cpus) in self.cpus_per_node.iter().enumerate() {
            tracing::info!(
                "  Node {}: {} CPUs ({:?}), {} GB memory",
                node,
                cpus.len(),
                cpus,
                self.memory_per_node[node] / (1024 * 1024 * 1024)
            );
        }
    }
}
```

## Implementation Phases

### Phase 1: NUMA Detection (1-2 days)

1. Add libnuma dependency
2. Implement `NumaTopology::detect()`
3. Log topology at startup
4. Unit tests for detection

### Phase 2: NumaAllocator (2-3 days)

1. Implement `NumaAllocator`
2. Per-node allocation pools
3. Interleaved allocation
4. Tests for allocation placement

### Phase 3: State Store Integration (2-3 days)

1. Update `StateStore::new()` to `new_numa_aware()`
2. Add NUMA node tracking
3. Huge page hints
4. Benchmark local vs remote access

### Phase 4: WAL & Queue Integration (2-3 days)

1. NUMA-aware WAL buffers
2. NUMA-aware SPSC queues
3. Integration with F013 CoreHandle
4. End-to-end tests

## Test Cases

```rust
#[test]
fn test_numa_topology_detection() {
    let topo = NumaTopology::detect();
    assert!(topo.num_nodes >= 1);
    assert!(!topo.cpus_per_node[0].is_empty());
}

#[test]
fn test_numa_local_allocation() {
    let allocator = NumaAllocator::new();
    let layout = Layout::from_size_align(4096, 64).unwrap();

    // Pin to specific CPU
    unsafe { libc::sched_setaffinity(/* core 0 */); }

    let ptr = allocator.alloc_local(layout);

    // Verify memory is on expected node
    let node = get_memory_node(ptr);
    let expected = allocator.node_for_cpu(0);
    assert_eq!(node, expected);
}

#[test]
fn test_numa_state_store_placement() {
    let store = StateStore::new_numa_aware(4, 64 * 1024 * 1024);

    // Memory should be on NUMA node for core 4
    assert_eq!(store.numa_node, NumaTopology::detect().node_for_cpu(4));
}

#[test]
fn test_numa_local_vs_remote_latency() {
    let local_store = StateStore::new_numa_aware(0, 1024 * 1024);
    let remote_store = StateStore::new_numa_aware(16, 1024 * 1024); // Different node

    // Pin to core 0
    pin_to_core(0);

    // Local access should be faster
    let local_latency = measure_access_latency(&local_store);
    let remote_latency = measure_access_latency(&remote_store);

    // Remote should be ~1.5x slower
    assert!(remote_latency > local_latency * 1.3);
}

#[test]
fn test_interleaved_allocation() {
    let allocator = NumaAllocator::new();
    let layout = Layout::from_size_align(64 * 1024 * 1024, 4096).unwrap();

    let ptr = allocator.alloc_interleaved(layout);

    // Memory should be spread across nodes (verify with numa_move_pages)
}
```

## Acceptance Criteria

- [x] NUMA topology detection working
- [x] NumaAllocator with per-node pools
- [x] State stores allocated NUMA-local (via CoreConfig.numa_aware)
- [ ] WAL buffers allocated NUMA-local (future: F062 per-core WAL)
- [ ] SPSC queues on producer's node (future: separate feature)
- [x] Interleaved allocation for shared data
- [ ] Benchmark showing local vs remote difference (to add)
- [x] Graceful degradation on single-node systems
- [x] 10+ unit tests passing (11 tests)

## Performance Targets

| Operation | Local NUMA | Remote NUMA | Target |
|-----------|------------|-------------|--------|
| State lookup | ~100ns | ~150ns | Use local |
| WAL append | ~50ns | ~75ns | Use local |
| SPSC push | ~5ns | ~8ns | Use producer-local |

## Platform Support

| Platform | Support |
|----------|---------|
| Linux (libnuma) | Full |
| macOS | Degraded (single NUMA node) |
| Windows | Degraded (single NUMA node) |

## Dependencies

- `libc` crate for NUMA system calls
- `libnuma` library on Linux
- Optional: `numa` crate for safer Rust bindings

## References

- [Thread-Per-Core 2026 Research](../../research/laminardb-thread-per-core-2026-research.md)
- [NUMA Best Practices (Intel)](https://www.intel.com/content/www/us/en/developer/articles/technical/numa-aware-programming.html)
- [libnuma Documentation](https://man7.org/linux/man-pages/man3/numa.3.html)
- [ScyllaDB NUMA Design](https://www.scylladb.com/2018/03/29/scylla-and-seastar-numa-support/)
