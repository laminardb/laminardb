# Thread-Per-Core Architecture Research: 2026 State of the Art

> **Purpose**: Research document for LaminarDB development. Use this when implementing or optimizing the thread-per-core reactor, io_uring integration, NUMA awareness, or any Ring 0 hot path code.

---

## Quick Reference: Key Findings

| Area | Current Best Practice | LaminarDB Gap | Priority |
|------|----------------------|---------------|----------|
| io_uring | SQPOLL + registered buffers + passthrough | Basic io_uring only | P0 |
| NUMA | First-class allocation strategy | Optional/basic | P0 |
| Ring separation | 3 io_uring rings (main/latency/poll) | Single ring | P1 |
| Network I/O | eBPF/XDP for pre-filtering | Standard sockets | P1 |
| Memory tiering | CXL-aware data placement | Not addressed | P2 |
| Kernel bypass | SPDK/DPDK option for extreme perf | io_uring only | P2 |

---

## 1. io_uring Optimization

### 1.1 The Problem with Basic io_uring

Research from TU Munich (Dec 2024) demonstrates that simply replacing traditional I/O with io_uring yields only **1.06-1.10x** improvement. However, careful optimization achieves **2.05x** or more.

### 1.2 Advanced Features to Implement

#### SQPOLL Mode (Priority: Critical)

Eliminates syscalls by using a dedicated kernel polling thread.

```rust
use io_uring::{IoUring, Builder};

pub fn create_optimized_ring(entries: u32, sqpoll_cpu: u32) -> io_uring::IoUring {
    Builder::default()
        // Kernel thread polls submission queue - no syscalls needed
        .setup_sqpoll(1000)  // Idle timeout in ms before kernel thread sleeps
        // Pin SQPOLL thread to dedicated CPU (should be on same NUMA node)
        .setup_sqpoll_cpu(sqpoll_cpu)
        // Reduce kernel-userspace transitions
        .setup_coop_taskrun()
        // Optimize for single-threaded submission (our thread-per-core model)
        .setup_single_issuer()
        .build(entries)
        .expect("failed to create io_uring")
}
```

**Expected improvement**: +32% throughput, syscalls reduced to near-zero under load.

**Measurement**: Use `strace -c` to verify syscall reduction. Target: <10 syscalls/sec under sustained load.

#### Registered Buffers (Priority: Critical)

Pre-register I/O buffers to avoid per-operation mapping overhead.

```rust
use io_uring::types;

pub struct RegisteredBufferPool {
    ring: IoUring,
    buffers: Vec<Vec<u8>>,
}

impl RegisteredBufferPool {
    pub fn new(ring: IoUring, buf_size: usize, buf_count: usize) -> Self {
        let buffers: Vec<Vec<u8>> = (0..buf_count)
            .map(|_| vec![0u8; buf_size])
            .collect();
        
        // Register buffers once at startup
        ring.submitter()
            .register_buffers(&buffers)
            .expect("failed to register buffers");
        
        Self { ring, buffers }
    }
    
    /// Use registered buffer for read operation
    pub fn submit_read_fixed(
        &mut self,
        fd: types::Fd,
        buf_index: u16,
        offset: u64,
        len: u32,
    ) -> u64 {
        let entry = io_uring::opcode::ReadFixed::new(fd, buf_index, len)
            .offset(offset)
            .build()
            .user_data(self.next_user_data());
        
        unsafe {
            self.ring.submission().push(&entry).expect("sq full");
        }
        
        self.ring.submitter().submit().expect("submit failed");
        entry.get_user_data()
    }
}
```

**Expected improvement**: Eliminates buffer registration overhead per I/O operation.

#### Passthrough I/O (Priority: High)

Issues native NVMe commands, bypassing generic storage stack.

```rust
use io_uring::opcode;

/// Submit NVMe passthrough command for direct device access
/// Requires: O_DIRECT file descriptor, NVMe device
pub fn submit_nvme_passthrough(
    ring: &mut IoUring,
    fd: types::Fd,
    nvme_cmd: &NvmeCommand,
) -> u64 {
    let entry = opcode::UringCmd::new(fd, nvme_cmd.opcode())
        .cmd_op(nvme_cmd.cdw10())
        .build()
        .user_data(next_id());
    
    unsafe {
        ring.submission().push(&entry).expect("sq full");
    }
    
    ring.submitter().submit().unwrap()
}
```

**Expected improvement**: +20% throughput by bypassing filesystem layer.

**Requirements**: Device must be opened with appropriate flags, NVMe drive required.

#### IOPOLL Mode (Priority: High)

Polls completions directly from NVMe device queue instead of interrupts.

```rust
pub fn create_iopoll_ring(entries: u32) -> IoUring {
    Builder::default()
        .setup_iopoll()  // Poll completions from device
        .setup_sqpoll(1000)
        .build(entries)
        .expect("failed to create iopoll ring")
}

// Note: IOPOLL disables interrupt-based completion
// Cannot mix with socket I/O on same ring
// Must use separate ring for network operations
```

**Expected improvement**: +21% throughput by eliminating interrupt overhead.

**Constraint**: Cannot use sockets on IOPOLL ring. Need separate rings for storage vs network.

### 1.3 io_uring Benchmark Targets

| Operation | Before Optimization | After Optimization | How to Measure |
|-----------|--------------------|--------------------|----------------|
| WAL append (p99) | ~30μs | <10μs | Criterion benchmark |
| Syscalls under load | 100K+/sec | <10/sec | `strace -c` |
| Single-thread TPC-C | ~270K tx/s | ~550K tx/s | Custom benchmark |

---

## 2. Three-Ring I/O Architecture

### 2.1 Pattern from Seastar/Glommio

High-performance frameworks use multiple io_uring instances per thread, each serving different latency requirements.

```
┌─────────────────────────────────────────────────────────────┐
│                    Per-Core I/O Architecture                │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│   ┌─────────────────┐                                       │
│   │  Latency Ring   │  ← Network receives, urgent ops       │
│   │  (non-blocking) │    Always polled first                │
│   └────────┬────────┘                                       │
│            │ fd registered on main ring for wake-up         │
│            ▼                                                │
│   ┌─────────────────┐                                       │
│   │   Main Ring     │  ← WAL writes, normal I/O             │
│   │  (can block)    │    Blocks when idle                   │
│   └────────┬────────┘                                       │
│            │                                                │
│            ▼                                                │
│   ┌─────────────────┐                                       │
│   │   Poll Ring     │  ← IOPOLL for NVMe (optional)         │
│   │  (storage only) │    Cannot have sockets                │
│   └─────────────────┘                                       │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### 2.2 Implementation Pattern

```rust
pub struct ThreeRingReactor {
    /// Latency-critical operations (network, urgent)
    /// Always checked first, never blocks
    latency_ring: IoUring,
    
    /// Normal operations (WAL, background reads)
    /// Can block when waiting for work
    main_ring: IoUring,
    
    /// Storage polling (optional, for NVMe passthrough)
    /// Uses IOPOLL, cannot have sockets
    poll_ring: Option<IoUring>,
    
    /// Latency ring's fd, registered on main ring for wake-up
    latency_eventfd: RawFd,
}

impl ThreeRingReactor {
    pub fn new(core_id: usize) -> io::Result<Self> {
        let latency_ring = Builder::default()
            .setup_sqpoll(100)  // Short timeout for latency ring
            .setup_single_issuer()
            .build(256)?;
        
        let main_ring = Builder::default()
            .setup_sqpoll(1000)
            .setup_single_issuer()
            .build(1024)?;
        
        // Get latency ring's fd and register it on main ring
        let latency_fd = latency_ring.as_raw_fd();
        
        // Register poll on latency ring's fd so main ring wakes up
        // when latency ring has completions
        let poll_entry = opcode::PollAdd::new(
            types::Fd(latency_fd),
            libc::POLLIN as _,
        ).build();
        
        unsafe {
            main_ring.submission().push(&poll_entry)?;
        }
        
        Ok(Self {
            latency_ring,
            main_ring,
            poll_ring: None,  // Enable for NVMe passthrough
            latency_eventfd: latency_fd,
        })
    }
    
    /// Main event loop - integrate with Ring 0/1/2 architecture
    pub fn run(&mut self) {
        loop {
            // 1. Always drain latency ring first (non-blocking)
            while let Some(cqe) = self.latency_ring.completion().next() {
                self.handle_latency_completion(cqe);
            }
            
            // 2. Drain main ring completions
            while let Some(cqe) = self.main_ring.completion().next() {
                self.handle_main_completion(cqe);
            }
            
            // 3. Ring 0: Process application events
            self.process_ring0_events();
            
            // 4. Ring 1: Background work (if Ring 0 idle)
            if self.ring0_idle() {
                self.process_ring1_chunk();
            }
            
            // 5. Ring 2: Control plane (rare)
            if self.has_control_message() {
                self.process_ring2();
            }
            
            // 6. Block on main ring if nothing to do
            //    Will wake up when latency ring has activity
            if self.should_sleep() {
                self.main_ring.submit_and_wait(1).ok();
            }
        }
    }
}
```

### 2.3 Why Three Rings?

| Ring | Purpose | Blocking | Use Cases |
|------|---------|----------|-----------|
| Latency | Urgent I/O | Never | Network receives, time-critical reads |
| Main | Normal I/O | Yes (when idle) | WAL writes, checkpointing |
| Poll | NVMe polling | Busy-poll | High-throughput storage (optional) |

The key insight: when the main ring blocks waiting for I/O, the latency ring's fd is registered for polling, so any latency-critical completion immediately wakes the thread.

---

## 3. NUMA-Aware Memory Architecture

### 3.1 Why NUMA Matters

On multi-socket systems, memory access latency varies by 2-3x depending on whether memory is local or remote to the CPU.

```
NUMA Topology Example (2-socket):

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

### 3.2 NUMA-Aware Allocator Design

```rust
use libc::{numa_alloc_onnode, numa_node_of_cpu, numa_free};
use std::alloc::{Layout, GlobalAlloc};

/// NUMA-aware memory pool for LaminarDB
pub struct NumaAllocator {
    /// Pre-allocated pools per NUMA node
    pools: Vec<NumaPool>,
}

pub struct NumaPool {
    node_id: usize,
    /// Arena allocator for this node
    arena: bumpalo::Bump,
    /// Large allocations tracking
    large_allocs: Vec<(*mut u8, usize)>,
}

impl NumaAllocator {
    pub fn new() -> Self {
        let num_nodes = unsafe { libc::numa_num_configured_nodes() } as usize;
        let pools = (0..num_nodes)
            .map(|node_id| NumaPool::new(node_id))
            .collect();
        
        Self { pools }
    }
    
    /// Allocate on the NUMA node local to current CPU
    #[inline]
    pub fn alloc_local(&self, layout: Layout) -> *mut u8 {
        let cpu = unsafe { libc::sched_getcpu() } as usize;
        let node = unsafe { numa_node_of_cpu(cpu as i32) } as usize;
        self.alloc_on_node(node, layout)
    }
    
    /// Allocate on specific NUMA node
    pub fn alloc_on_node(&self, node: usize, layout: Layout) -> *mut u8 {
        if layout.size() < 2 * 1024 * 1024 {
            // Small allocation: use arena
            self.pools[node].arena.alloc_layout(layout).as_ptr()
        } else {
            // Large allocation: direct NUMA alloc
            let ptr = unsafe {
                numa_alloc_onnode(layout.size(), node as i32) as *mut u8
            };
            self.pools[node].large_allocs.push((ptr, layout.size()));
            ptr
        }
    }
    
    /// Allocate interleaved across all NUMA nodes
    /// Use for shared data structures accessed by all cores
    pub fn alloc_interleaved(&self, layout: Layout) -> *mut u8 {
        unsafe {
            libc::numa_alloc_interleaved(layout.size()) as *mut u8
        }
    }
}

impl NumaPool {
    fn new(node_id: usize) -> Self {
        // Pre-allocate arena memory on this NUMA node
        let arena_size = 64 * 1024 * 1024; // 64MB per node
        let arena_mem = unsafe {
            numa_alloc_onnode(arena_size, node_id as i32)
        };
        
        Self {
            node_id,
            arena: bumpalo::Bump::with_capacity(arena_size),
            large_allocs: Vec::new(),
        }
    }
}
```

### 3.3 NUMA-Aware State Store Initialization

```rust
impl StateStore {
    /// Create state store with memory on correct NUMA node
    pub fn new(core_id: usize, size: usize) -> Self {
        let numa_node = unsafe {
            libc::numa_node_of_cpu(core_id as i32) as usize
        };
        
        // Allocate state store memory on local NUMA node
        let memory = unsafe {
            let ptr = libc::numa_alloc_onnode(size, numa_node as i32);
            if ptr.is_null() {
                panic!("NUMA allocation failed");
            }
            // Use huge pages if available
            libc::madvise(ptr, size, libc::MADV_HUGEPAGE);
            ptr as *mut u8
        };
        
        Self {
            memory,
            size,
            numa_node,
        }
    }
}
```

### 3.4 Data Placement Strategy

| Data Type | Placement | Reason |
|-----------|-----------|--------|
| Per-core state stores | NUMA-local | Always accessed by owning core |
| Per-core WAL buffers | NUMA-local | Written by owning core |
| Cross-core message queues | Producer's NUMA node | Producer writes, consumer reads |
| Shared lookup tables (read-only) | Interleaved | Accessed by all cores equally |
| Checkpoint data | Any/CXL | Background access, not latency-critical |

---

## 4. Zero-Allocation Hot Path

### 4.1 Allocation Detection

```rust
/// Custom allocator that panics on hot path allocation
/// Use in debug/test builds to detect violations
#[cfg(debug_assertions)]
pub struct AllocationDetector {
    enabled: std::cell::Cell<bool>,
}

#[cfg(debug_assertions)]
thread_local! {
    static DETECTOR: AllocationDetector = AllocationDetector {
        enabled: std::cell::Cell::new(false),
    };
}

#[cfg(debug_assertions)]
pub fn enter_hot_path() {
    DETECTOR.with(|d| d.enabled.set(true));
}

#[cfg(debug_assertions)]
pub fn exit_hot_path() {
    DETECTOR.with(|d| d.enabled.set(false));
}

#[cfg(debug_assertions)]
pub fn check_no_alloc() {
    DETECTOR.with(|d| {
        if d.enabled.get() {
            panic!("Allocation detected in hot path!");
        }
    });
}

// In global allocator wrapper:
#[cfg(debug_assertions)]
unsafe impl GlobalAlloc for HotPathDetectingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        check_no_alloc();
        System.alloc(layout)
    }
    // ...
}
```

### 4.2 Common Allocation Pitfalls

```rust
// ❌ BAD: Hidden allocations in Ring 0

fn process_event_bad(event: &Event) {
    // Allocates!
    let key = event.key.to_vec();
    
    // Allocates!
    let msg = format!("Processing {}", event.id);
    
    // Allocates!
    let results: Vec<_> = items.iter().collect();
    
    // May allocate (HashMap growth)!
    map.insert(key, value);
    
    // Allocates (Box)!
    let boxed = Box::new(result);
}

// ✅ GOOD: Zero-allocation Ring 0

fn process_event_good(
    event: &Event,
    // Pre-allocated buffers passed in
    key_buf: &mut [u8],
    result_buf: &mut ArrayVec<Result, 64>,
    // Pre-sized HashMap
    map: &mut HashMap<Key, Value>,
) {
    // Copy to pre-allocated buffer
    key_buf[..event.key.len()].copy_from_slice(&event.key);
    
    // Use ArrayVec (stack-allocated)
    result_buf.push(result);
    
    // HashMap pre-reserved, won't grow
    map.insert(key, value);
}
```

### 4.3 Pre-allocation Patterns

```rust
/// Object pool for frequently allocated types
pub struct ObjectPool<T> {
    free_list: ArrayVec<T, 1024>,
}

impl<T: Default> ObjectPool<T> {
    pub fn new() -> Self {
        let mut free_list = ArrayVec::new();
        // Pre-allocate objects
        for _ in 0..1024 {
            free_list.push(T::default());
        }
        Self { free_list }
    }
    
    #[inline]
    pub fn acquire(&mut self) -> Option<T> {
        self.free_list.pop()
    }
    
    #[inline]
    pub fn release(&mut self, obj: T) {
        if !self.free_list.is_full() {
            self.free_list.push(obj);
        }
    }
}

/// Ring buffer for events (no allocation on push/pop)
pub struct EventRingBuffer {
    buffer: Box<[MaybeUninit<Event>]>,
    head: usize,
    tail: usize,
    mask: usize,
}

impl EventRingBuffer {
    pub fn new(capacity: usize) -> Self {
        assert!(capacity.is_power_of_two());
        let buffer = (0..capacity)
            .map(|_| MaybeUninit::uninit())
            .collect::<Vec<_>>()
            .into_boxed_slice();
        
        Self {
            buffer,
            head: 0,
            tail: 0,
            mask: capacity - 1,
        }
    }
    
    #[inline]
    pub fn push(&mut self, event: Event) -> Result<(), Event> {
        let next_head = (self.head + 1) & self.mask;
        if next_head == self.tail {
            return Err(event); // Full
        }
        
        unsafe {
            self.buffer[self.head].as_mut_ptr().write(event);
        }
        self.head = next_head;
        Ok(())
    }
}
```

---

## 5. eBPF/XDP for Network Optimization

### 5.1 Overview

XDP (eXpress Data Path) processes packets at the NIC driver level, before kernel networking stack allocation. Achieves 26 million packets/sec/core.

### 5.2 Use Cases for LaminarDB

| Use Case | XDP Action | Benefit |
|----------|------------|---------|
| Invalid packet filtering | XDP_DROP | Reduce CPU load |
| Protocol validation | XDP_DROP/PASS | Early rejection |
| Core routing by partition | XDP_REDIRECT | Bypass kernel routing |
| DDoS mitigation | XDP_DROP | Wire-speed filtering |

### 5.3 XDP Program Example

```c
// laminar_xdp.c - Compile with clang to BPF bytecode

#include <linux/bpf.h>
#include <linux/if_ether.h>
#include <linux/ip.h>
#include <linux/udp.h>
#include <bpf/bpf_helpers.h>

// Map to redirect packets to specific CPUs
struct {
    __uint(type, BPF_MAP_TYPE_CPUMAP);
    __uint(key_size, sizeof(__u32));
    __uint(value_size, sizeof(struct bpf_cpumap_val));
    __uint(max_entries, 64);
} cpu_map SEC(".maps");

// LaminarDB magic bytes (first 4 bytes of payload)
#define LAMINAR_MAGIC 0x4C414D49  // "LAMI"

SEC("xdp")
int laminar_ingress(struct xdp_md *ctx) {
    void *data = (void *)(long)ctx->data;
    void *data_end = (void *)(long)ctx->data_end;
    
    // Parse Ethernet header
    struct ethhdr *eth = data;
    if ((void *)(eth + 1) > data_end)
        return XDP_PASS;
    
    // Only handle IPv4
    if (eth->h_proto != __constant_htons(ETH_P_IP))
        return XDP_PASS;
    
    // Parse IP header
    struct iphdr *ip = (void *)(eth + 1);
    if ((void *)(ip + 1) > data_end)
        return XDP_PASS;
    
    // Only handle UDP (LaminarDB ingest protocol)
    if (ip->protocol != IPPROTO_UDP)
        return XDP_PASS;
    
    // Parse UDP header
    struct udphdr *udp = (void *)(ip + 1);
    if ((void *)(udp + 1) > data_end)
        return XDP_PASS;
    
    // Check LaminarDB port
    if (udp->dest != __constant_htons(9999))
        return XDP_PASS;
    
    // Get payload
    void *payload = (void *)(udp + 1);
    if (payload + 8 > data_end)
        return XDP_DROP;  // Too short
    
    // Check magic bytes
    __u32 magic = *(__u32 *)payload;
    if (magic != __constant_htonl(LAMINAR_MAGIC))
        return XDP_DROP;  // Invalid protocol
    
    // Extract partition key (bytes 4-7)
    __u32 partition_key = *((__u32 *)(payload + 4));
    
    // Route to appropriate CPU based on partition
    __u32 target_cpu = partition_key % bpf_num_possible_cpus();
    
    return bpf_redirect_map(&cpu_map, target_cpu, 0);
}

char LICENSE[] SEC("license") = "GPL";
```

### 5.4 Loading XDP from Rust

```rust
use libbpf_rs::{MapFlags, ObjectBuilder, ProgramType};

pub struct XdpLoader {
    link: libbpf_rs::Link,
}

impl XdpLoader {
    pub fn load_and_attach(
        bpf_obj_path: &str,
        interface: &str,
        num_cores: usize,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        // Load BPF object
        let obj = ObjectBuilder::default()
            .open_file(bpf_obj_path)?
            .load()?;
        
        // Get XDP program
        let prog = obj.prog("laminar_ingress")?;
        
        // Configure CPU map
        let cpu_map = obj.map("cpu_map")?;
        for cpu in 0..num_cores {
            let key = cpu as u32;
            let value = libbpf_rs::CpumapValue {
                qsize: 2048,
                ..Default::default()
            };
            cpu_map.update(&key.to_ne_bytes(), &value.to_bytes(), MapFlags::ANY)?;
        }
        
        // Attach to interface
        let ifindex = nix::net::if_::if_nametoindex(interface)?;
        let link = prog.attach_xdp(ifindex as i32)?;
        
        Ok(Self { link })
    }
}
```

---

## 6. CXL Memory Tiering (Future)

### 6.1 CXL Latency Characteristics

| Memory Type | Read Latency | Write Latency | Use Case |
|-------------|--------------|---------------|----------|
| Local DRAM | ~100ns | ~100ns | Ring 0 state |
| CXL Type 3 | ~200-500ns | ~300ns | Cold data, checkpoints |
| Remote NUMA | ~150ns | ~150ns | Cross-socket access |

### 6.2 Data Placement Strategy

```rust
/// Memory tier hints for LaminarDB data structures
pub enum MemoryTier {
    /// Hot path data - must be local DRAM
    /// Ring 0 state stores, active windows
    Hot,
    
    /// Warm data - can be CXL with prefetching
    /// Recently accessed historical data
    Warm,
    
    /// Cold data - CXL or even NVMe
    /// Checkpoints, archived state
    Cold,
}

pub trait TieredAllocation {
    fn memory_tier(&self) -> MemoryTier;
}

impl TieredAllocation for StateStore {
    fn memory_tier(&self) -> MemoryTier {
        MemoryTier::Hot  // Always local DRAM
    }
}

impl TieredAllocation for CheckpointBuffer {
    fn memory_tier(&self) -> MemoryTier {
        MemoryTier::Cold  // Can use CXL pool
    }
}
```

### 6.3 CXL-Aware Access Patterns

Research shows:
- **Sequential access**: CXL overhead hidden by prefetcher (~90% of DRAM performance)
- **Random access**: Significant penalty, keep in local DRAM
- **Writes**: 3x latency of local DRAM, batch when possible

```rust
/// Hint to optimizer about access pattern
pub enum AccessPattern {
    Sequential,  // OK for CXL
    Random,      // Keep in local DRAM
    WriteHeavy,  // Keep in local DRAM
}
```

---

## 7. Task Budget Enforcement

### 7.1 Budget Tracking

```rust
use std::time::Instant;

pub struct TaskBudget {
    start: Instant,
    budget_ns: u64,
    name: &'static str,
}

impl TaskBudget {
    // Ring 0: Single event processing
    pub const RING0_EVENT_NS: u64 = 500;
    
    // Ring 0: Batch of events
    pub const RING0_BATCH_NS: u64 = 5_000;
    
    // Ring 1: Background chunk
    pub const RING1_CHUNK_NS: u64 = 1_000_000;  // 1ms
    
    #[inline]
    pub fn ring0_event() -> Self {
        Self {
            start: Instant::now(),
            budget_ns: Self::RING0_EVENT_NS,
            name: "ring0_event",
        }
    }
    
    #[inline]
    pub fn ring1_chunk() -> Self {
        Self {
            start: Instant::now(),
            budget_ns: Self::RING1_CHUNK_NS,
            name: "ring1_chunk",
        }
    }
    
    #[inline]
    pub fn remaining_ns(&self) -> i64 {
        self.budget_ns as i64 - self.elapsed_ns() as i64
    }
    
    #[inline]
    pub fn exceeded(&self) -> bool {
        self.elapsed_ns() > self.budget_ns
    }
    
    #[inline]
    pub fn elapsed_ns(&self) -> u64 {
        self.start.elapsed().as_nanos() as u64
    }
    
    /// Call on drop to record metrics
    pub fn record_metrics(&self) {
        let elapsed = self.elapsed_ns();
        
        metrics::histogram!(
            "task.duration_ns",
            "task" => self.name
        ).record(elapsed as f64);
        
        if elapsed > self.budget_ns {
            metrics::counter!(
                "task.budget_exceeded",
                "task" => self.name
            ).increment(1);
            
            metrics::histogram!(
                "task.budget_exceeded_by_ns",
                "task" => self.name
            ).record((elapsed - self.budget_ns) as f64);
        }
    }
}

impl Drop for TaskBudget {
    fn drop(&mut self) {
        self.record_metrics();
    }
}

// Usage
fn process_event(&mut self, event: Event) {
    let _budget = TaskBudget::ring0_event();
    
    // Process event...
    // Metrics recorded automatically on drop
}
```

### 7.2 Cooperative Yielding in Ring 1

```rust
impl Ring1Processor {
    pub fn process_background_work(&mut self) {
        let budget = TaskBudget::ring1_chunk();
        
        while !budget.exceeded() {
            if let Some(work) = self.background_queue.pop() {
                self.process_work_item(work);
            } else {
                break;  // No more work
            }
            
            // Check if Ring 0 has pending events
            if self.ring0_has_pending() {
                break;  // Yield to Ring 0
            }
        }
    }
}
```

---

## 8. Benchmarking Requirements

### 8.1 Micro-Benchmarks

```rust
use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId};

fn bench_state_lookup(c: &mut Criterion) {
    let mut store = StateStore::new(0, 1024 * 1024);
    // Pre-populate
    for i in 0..10000 {
        store.insert(&i.to_le_bytes(), &[0u8; 64]);
    }
    
    c.bench_function("state_lookup", |b| {
        let key = 5000u64.to_le_bytes();
        b.iter(|| {
            store.get(&key)
        })
    });
}

fn bench_spsc_roundtrip(c: &mut Criterion) {
    let (mut tx, mut rx) = rtrb::RingBuffer::<u64>::new(1024);
    
    c.bench_function("spsc_roundtrip", |b| {
        b.iter(|| {
            tx.push(42).unwrap();
            rx.pop().unwrap()
        })
    });
}

fn bench_io_uring_submit(c: &mut Criterion) {
    let ring = create_optimized_ring(256, 0);
    let file = std::fs::File::open("/dev/null").unwrap();
    let fd = types::Fd(file.as_raw_fd());
    
    c.bench_function("io_uring_nop", |b| {
        b.iter(|| {
            let entry = opcode::Nop::new().build();
            unsafe { ring.submission().push(&entry).unwrap(); }
            ring.submitter().submit().unwrap();
        })
    });
}

criterion_group!(benches, 
    bench_state_lookup,
    bench_spsc_roundtrip,
    bench_io_uring_submit,
);
criterion_main!(benches);
```

### 8.2 Target Metrics

| Benchmark | Target | Failure Threshold |
|-----------|--------|-------------------|
| state_lookup p50 | < 200ns | > 500ns |
| state_lookup p99 | < 500ns | > 1μs |
| spsc_push p99 | < 50ns | > 100ns |
| spsc_pop p99 | < 50ns | > 100ns |
| cross_core_msg p99 | < 500ns | > 1μs |
| io_uring_submit (SQPOLL) | < 100ns | > 500ns |

### 8.3 Allocation Detection in CI

```yaml
# .github/workflows/bench.yml
- name: Run benchmarks with allocation detection
  run: |
    cargo build --release --features allocation-tracking
    MALLOC_CONF=prof:true cargo bench --features allocation-tracking 2>&1 | tee bench.log
    
    # Fail if any allocations detected in hot path
    if grep -q "ALLOCATION IN HOT PATH" bench.log; then
      echo "❌ Allocations detected in hot path!"
      exit 1
    fi
```

---

## 9. Implementation Checklist

### Phase 1: io_uring Optimization (P0)

- [ ] Implement SQPOLL mode in reactor
- [ ] Add registered buffer pool
- [ ] Benchmark before/after syscall counts
- [ ] Measure WAL write latency improvement
- [ ] Add IOPOLL ring for storage (optional)

### Phase 2: NUMA Awareness (P0)

- [ ] Implement NumaAllocator
- [ ] Update StateStore to use NUMA-local allocation
- [ ] Update WAL buffers to use NUMA-local allocation
- [ ] Add NUMA node detection per core
- [ ] Benchmark cross-NUMA vs local access

### Phase 3: Three-Ring I/O (P1)

- [ ] Implement ThreeRingReactor
- [ ] Separate latency-critical from normal I/O
- [ ] Add eventfd wake-up mechanism
- [ ] Benchmark latency ring response time

### Phase 4: Zero-Allocation Verification (P0)

- [ ] Implement allocation detector
- [ ] Audit all Ring 0 code paths
- [ ] Add pre-allocation for common types
- [ ] Add CI check for hot path allocations

### Phase 5: Task Budgeting (P1)

- [ ] Implement TaskBudget tracking
- [ ] Add metrics for budget violations
- [ ] Add cooperative yielding in Ring 1
- [ ] Alert on sustained budget violations

### Phase 6: XDP Integration (P2)

- [ ] Write XDP program for packet filtering
- [ ] Implement Rust loader
- [ ] Benchmark throughput improvement
- [ ] Add CPU steering for partitioned data

---

## 10. Reference Implementations

### Open Source Projects to Study

| Project | Language | Key Patterns |
|---------|----------|--------------|
| Seastar | C++ | Three rings, NUMA, task scheduling |
| Glommio | Rust | io_uring rings, thread-per-core |
| ScyllaDB | C++ | Full database on Seastar |
| Redpanda | C++ | Streaming on Seastar |
| TigerBeetle | Zig | io_uring, deterministic simulation |

### Papers & Documentation

1. "io_uring for High-Performance DBMSs" (2024) - TU Munich
2. "A Wake-Up Call for Kernel-Bypass" (SIGMOD 2025)
3. Seastar Tutorial: https://seastar.io/tutorial
4. Glommio Book: https://docs.rs/glommio
5. io_uring guide: https://kernel.dk/io_uring.pdf

---

*Last updated: January 2026*
*Use with: Claude Code, LaminarDB development*
