# F067: io_uring Advanced Optimization

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F067 |
| **Status** | ✅ Done |
| **Priority** | P0 |
| **Phase** | 2 |
| **Effort** | XL (2-3 weeks) |
| **Dependencies** | F001, F013, F062 |
| **Owner** | Claude |
| **Research** | [Thread-Per-Core 2026 Research](../../research/laminardb-thread-per-core-2026-research.md) |

## Summary

Implement advanced io_uring optimizations including SQPOLL mode, registered buffers, passthrough I/O, and IOPOLL mode. Research from TU Munich (Dec 2024) shows basic io_uring yields only 1.06-1.10x improvement, while careful optimization achieves 2.05x or more.

## Motivation

### Current State

From research gap analysis:

| Area | Current | Target | Impact |
|------|---------|--------|--------|
| Syscalls under load | 100K+/sec | <10/sec | SQPOLL eliminates syscalls |
| WAL append p99 | ~30μs | <10μs | Registered buffers + SQPOLL |
| Storage throughput | Baseline | +20% | NVMe passthrough |

### Why This Matters

1. **SQPOLL Mode**: Eliminates syscalls by using dedicated kernel polling thread
2. **Registered Buffers**: Avoids per-operation buffer mapping overhead
3. **IOPOLL Mode**: Polls completions directly from NVMe queue (no interrupts)
4. **Passthrough I/O**: Issues native NVMe commands, bypassing generic storage stack

## Goals

1. Implement SQPOLL mode in reactor
2. Add registered buffer pool for I/O operations
3. Enable IOPOLL for storage operations (optional, NVMe only)
4. Add passthrough I/O for NVMe devices
5. Achieve <10 syscalls/sec under sustained load
6. Reduce WAL append p99 from ~30μs to <10μs

## Non-Goals

- Kernel bypass (SPDK/DPDK) - Phase 3+
- User-space NVMe driver
- Windows support (io_uring is Linux-only)

## Technical Design

### SQPOLL Mode Ring Creation

```rust
use io_uring::{IoUring, Builder};

/// Create optimized io_uring with SQPOLL mode.
///
/// SQPOLL eliminates syscalls by using a kernel thread that continuously
/// polls the submission queue. The kernel thread is pinned to the same
/// NUMA node as the reactor core.
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

**Expected Improvement**: +32% throughput, syscalls reduced to near-zero under load.

### Registered Buffer Pool

```rust
use io_uring::types;

/// Pre-registered I/O buffer pool.
///
/// Avoids per-operation buffer mapping overhead by registering buffers
/// once at startup. Uses fixed-size buffers for predictable performance.
pub struct RegisteredBufferPool {
    ring: IoUring,
    /// Pre-allocated buffers (e.g., 256 x 64KB)
    buffers: Vec<Vec<u8>>,
    /// Free buffer indices
    free_list: Vec<u16>,
    /// Next user_data ID for tracking
    next_id: u64,
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

        Self {
            ring,
            buffers,
            free_list: (0..buf_count as u16).collect(),
            next_id: 0,
        }
    }

    /// Acquire a buffer index for I/O operation.
    #[inline]
    pub fn acquire(&mut self) -> Option<u16> {
        self.free_list.pop()
    }

    /// Release a buffer index back to pool.
    #[inline]
    pub fn release(&mut self, buf_index: u16) {
        self.free_list.push(buf_index);
    }

    /// Submit read using registered buffer (zero-copy).
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

    /// Submit write using registered buffer (zero-copy).
    pub fn submit_write_fixed(
        &mut self,
        fd: types::Fd,
        buf_index: u16,
        offset: u64,
        len: u32,
    ) -> u64 {
        let entry = io_uring::opcode::WriteFixed::new(fd, buf_index, len)
            .offset(offset)
            .build()
            .user_data(self.next_user_data());

        unsafe {
            self.ring.submission().push(&entry).expect("sq full");
        }

        self.ring.submitter().submit().expect("submit failed");
        entry.get_user_data()
    }

    fn next_user_data(&mut self) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        id
    }
}
```

**Expected Improvement**: Eliminates buffer registration overhead per I/O operation.

### IOPOLL Mode for Storage

```rust
/// Create IOPOLL ring for NVMe storage operations.
///
/// IOPOLL polls completions directly from NVMe device queue instead of
/// using interrupts. Cannot mix with socket I/O - requires separate ring.
pub fn create_iopoll_ring(entries: u32) -> IoUring {
    Builder::default()
        .setup_iopoll()  // Poll completions from device
        .setup_sqpoll(1000)
        .setup_single_issuer()
        .build(entries)
        .expect("failed to create iopoll ring")
}

// Note: IOPOLL disables interrupt-based completion
// Cannot mix with socket I/O on same ring
// Must use separate ring for network operations
```

**Expected Improvement**: +21% throughput by eliminating interrupt overhead.

**Constraint**: Cannot use sockets on IOPOLL ring. Feeds into F069 (Three-Ring I/O).

### NVMe Passthrough I/O

```rust
use io_uring::opcode;

/// NVMe command structure for passthrough operations.
pub struct NvmeCommand {
    opcode: u8,
    cdw10: u32,
    // ... other NVMe command fields
}

/// Submit NVMe passthrough command for direct device access.
///
/// Bypasses generic storage stack, issues native NVMe commands.
/// Requires: O_DIRECT file descriptor, NVMe device.
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

**Expected Improvement**: +20% throughput by bypassing filesystem layer.

### Per-Core Ring Manager

```rust
/// Per-core io_uring manager for thread-per-core architecture.
///
/// Integrates with F013 CoreHandle to provide per-core I/O rings.
pub struct CoreRingManager {
    /// Core ID for this manager
    core_id: usize,
    /// SQPOLL CPU for this core (usually core_id or dedicated I/O core)
    sqpoll_cpu: u32,
    /// Main ring for general I/O
    main_ring: IoUring,
    /// Registered buffer pool
    buffer_pool: RegisteredBufferPool,
    /// IOPOLL ring for NVMe (optional)
    iopoll_ring: Option<IoUring>,
    /// Pending operations tracking
    pending: HashMap<u64, PendingOp>,
}

impl CoreRingManager {
    pub fn new(core_id: usize, config: &RingConfig) -> io::Result<Self> {
        let sqpoll_cpu = config.sqpoll_cpu_for_core(core_id);
        let main_ring = create_optimized_ring(config.ring_entries, sqpoll_cpu)?;

        let buffer_pool = RegisteredBufferPool::new(
            main_ring.try_clone()?,
            config.buffer_size,
            config.buffer_count,
        );

        let iopoll_ring = if config.enable_iopoll {
            Some(create_iopoll_ring(config.iopoll_entries)?)
        } else {
            None
        };

        Ok(Self {
            core_id,
            sqpoll_cpu,
            main_ring,
            buffer_pool,
            iopoll_ring,
            pending: HashMap::new(),
        })
    }

    /// Process completions from all rings.
    pub fn poll_completions(&mut self) -> Vec<Completion> {
        let mut completions = Vec::new();

        // Poll main ring
        while let Some(cqe) = self.main_ring.completion().next() {
            completions.push(self.handle_completion(cqe));
        }

        // Poll IOPOLL ring if present
        if let Some(ref mut ring) = self.iopoll_ring {
            while let Some(cqe) = ring.completion().next() {
                completions.push(self.handle_completion(cqe));
            }
        }

        completions
    }
}
```

### WAL Integration

```rust
/// Optimized WAL writer using registered buffers.
pub struct IoUringWalWriter {
    ring_manager: CoreRingManager,
    fd: types::Fd,
    current_position: u64,
}

impl IoUringWalWriter {
    /// Append to WAL using registered buffer.
    ///
    /// Zero-copy: data written directly from registered buffer.
    pub fn append(&mut self, data: &[u8]) -> Result<u64, WalError> {
        let buf_idx = self.ring_manager.buffer_pool.acquire()
            .ok_or(WalError::BufferExhausted)?;

        // Copy data to registered buffer
        let buf = &mut self.ring_manager.buffer_pool.buffers[buf_idx as usize];
        buf[..data.len()].copy_from_slice(data);

        // Submit write
        let user_data = self.ring_manager.buffer_pool.submit_write_fixed(
            self.fd,
            buf_idx,
            self.current_position,
            data.len() as u32,
        );

        // Track pending operation
        self.ring_manager.pending.insert(user_data, PendingOp::WalAppend {
            buf_idx,
            len: data.len(),
        });

        self.current_position += data.len() as u64;
        Ok(self.current_position)
    }

    /// Sync WAL (fdatasync via io_uring).
    pub fn sync(&mut self) -> Result<(), WalError> {
        let entry = io_uring::opcode::Fsync::new(self.fd)
            .flags(io_uring::types::FsyncFlags::DATASYNC)
            .build();

        unsafe {
            self.ring_manager.main_ring.submission().push(&entry)?;
        }

        self.ring_manager.main_ring.submitter().submit_and_wait(1)?;
        Ok(())
    }
}
```

## Implementation Phases

### Phase 1: Basic SQPOLL (3-4 days)

1. Add `io-uring` crate dependency
2. Implement `create_optimized_ring()` with SQPOLL
3. Create basic `CoreRingManager`
4. Unit tests for ring creation
5. Benchmark syscall reduction

### Phase 2: Registered Buffers (3-4 days)

1. Implement `RegisteredBufferPool`
2. Add acquire/release API
3. `submit_read_fixed()` and `submit_write_fixed()`
4. Tests for buffer pool operations
5. Benchmark buffer registration overhead elimination

### Phase 3: WAL Integration (4-5 days)

1. Create `IoUringWalWriter`
2. Integrate with existing WAL (F007)
3. Async completion handling
4. Group commit with io_uring
5. Benchmark WAL append latency

### Phase 4: IOPOLL Mode (Optional) (3-4 days)

1. Implement `create_iopoll_ring()`
2. Separate storage vs network rings
3. Detection of NVMe device support
4. Fallback for non-NVMe devices
5. Integration tests with NVMe

### Phase 5: Passthrough I/O (Optional) (2-3 days)

1. Implement NVMe command structures
2. `submit_nvme_passthrough()` function
3. Tests with NVMe device
4. Benchmark throughput improvement

## Test Cases

```rust
#[test]
fn test_sqpoll_syscall_reduction() {
    let ring = create_optimized_ring(256, 0);

    // Under sustained load, should see < 10 syscalls/sec
    // Use strace to verify: strace -c ./test_binary
}

#[test]
fn test_registered_buffer_zero_copy() {
    let mut pool = RegisteredBufferPool::new(ring, 64 * 1024, 256);

    // Acquire buffer
    let idx = pool.acquire().unwrap();

    // Write and read should use registered buffer (no copy)
    pool.buffers[idx as usize][..5].copy_from_slice(b"hello");

    let user_data = pool.submit_write_fixed(fd, idx, 0, 5);

    // Wait for completion
    // ...

    pool.release(idx);
}

#[test]
fn test_wal_append_latency() {
    let mut writer = IoUringWalWriter::new(config);

    // Measure p99 latency
    let latencies: Vec<u64> = (0..10_000)
        .map(|_| {
            let start = Instant::now();
            writer.append(b"test data").unwrap();
            start.elapsed().as_nanos() as u64
        })
        .collect();

    latencies.sort();
    let p99 = latencies[(latencies.len() * 99) / 100];

    assert!(p99 < 10_000, "WAL append p99 should be < 10μs, got {}ns", p99);
}

#[test]
fn test_iopoll_throughput() {
    let ring = create_iopoll_ring(256);

    // Benchmark storage throughput with IOPOLL
    // Should see +21% improvement over interrupt-based
}
```

## Acceptance Criteria

- [ ] SQPOLL mode working (syscalls < 10/sec under load)
- [ ] Registered buffer pool implemented
- [ ] WAL append p99 < 10μs
- [ ] IOPOLL mode for NVMe (optional)
- [ ] Passthrough I/O for NVMe (optional)
- [ ] Per-core ring manager integrated with F013
- [ ] 10+ unit tests passing
- [ ] Benchmarks documented with before/after

## Performance Targets

| Metric | Before | After | How to Measure |
|--------|--------|-------|----------------|
| WAL append p99 | ~30μs | <10μs | Criterion benchmark |
| Syscalls/sec | 100K+ | <10 | `strace -c` |
| Storage throughput | Baseline | +20% | fio benchmark |
| Single-thread TPC-C | ~270K tx/s | ~550K tx/s | Custom benchmark |

## Platform Support

| Platform | Support |
|----------|---------|
| Linux 5.10+ | Full (SQPOLL, registered buffers) |
| Linux 5.19+ | Full (IOPOLL, passthrough) |
| macOS | None (fallback to tokio) |
| Windows | None (fallback to tokio) |

## Dependencies

- `io-uring` crate (Rust bindings)
- Linux kernel 5.10+ for basic features
- Linux kernel 5.19+ for passthrough
- NVMe device for IOPOLL/passthrough (optional)

## References

- [Thread-Per-Core 2026 Research](../../research/laminardb-thread-per-core-2026-research.md)
- [io_uring for High-Performance DBMSs (TU Munich 2024)](https://www.cidrdb.org/cidr2024/papers/p43-haas.pdf)
- [io_uring Guide (kernel.dk)](https://kernel.dk/io_uring.pdf)
- [Glommio io_uring Implementation](https://github.com/DataDog/glommio)
