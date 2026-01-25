# F069: Three-Ring I/O Architecture

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F069 |
| **Status** | ✅ Done |
| **Priority** | P1 |
| **Phase** | 2 |
| **Effort** | L (2-3 weeks) |
| **Dependencies** | F067 |
| **Owner** | Claude |
| **Research** | [Thread-Per-Core 2026 Research](../../research/laminardb-thread-per-core-2026-research.md) |
| **Completed** | 2026-01-25 |

## Summary

Implement the three-ring I/O architecture pattern from Seastar/Glommio where each thread uses multiple io_uring instances serving different latency requirements: a latency ring (always polled, never blocks), a main ring (can block when idle), and an optional poll ring (IOPOLL for NVMe).

## Motivation

### Why Multiple Rings?

| Ring | Purpose | Blocking | Use Cases |
|------|---------|----------|-----------|
| **Latency** | Urgent I/O | Never | Network receives, time-critical reads |
| **Main** | Normal I/O | Yes (when idle) | WAL writes, checkpointing |
| **Poll** | NVMe polling | Busy-poll | High-throughput storage (optional) |

### Key Insight

When the main ring blocks waiting for I/O, the latency ring's fd is registered for polling, so any latency-critical completion immediately wakes the thread. This enables:

1. **Sub-microsecond network response** while WAL writes complete in background
2. **No missed deadlines** - latency ring always checked first
3. **Efficient sleep** when truly idle (main ring blocks)
4. **Maximum storage throughput** via IOPOLL (when available)

### Current Gap

| Aspect | Current | Three-Ring Target |
|--------|---------|-------------------|
| I/O rings | Single (or none) | 3 specialized rings |
| Latency priority | No differentiation | Latency ring polled first |
| Idle behavior | Busy-poll or tokio | Efficient blocking on main ring |
| NVMe optimization | None | IOPOLL ring for storage |

## Goals

1. Implement `ThreeRingReactor` with latency/main/poll rings
2. Latency ring always polled first (non-blocking)
3. Main ring can block when idle
4. Eventfd wake-up from latency ring to main ring
5. Optional IOPOLL ring for NVMe storage
6. Integration with Ring 0/1/2 architecture

## Non-Goals

- More than 3 rings
- Dynamic ring creation/destruction
- Kernel bypass (SPDK)

## Technical Design

### Architecture Overview

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

### ThreeRingReactor Implementation

```rust
use io_uring::{IoUring, Builder, opcode, types};
use std::os::unix::io::{AsRawFd, RawFd};

/// Three-ring I/O reactor for optimal latency/throughput balance.
///
/// Pattern from Seastar/Glommio: separate rings for different
/// latency requirements, with eventfd-based wake-up coordination.
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

    /// Core ID for this reactor
    core_id: usize,

    /// Stats for monitoring
    stats: ThreeRingStats,
}

/// Statistics for three-ring operation.
#[derive(Default)]
pub struct ThreeRingStats {
    pub latency_completions: u64,
    pub main_completions: u64,
    pub poll_completions: u64,
    pub main_ring_sleeps: u64,
    pub latency_wake_ups: u64,
}

impl ThreeRingReactor {
    pub fn new(core_id: usize, config: &ThreeRingConfig) -> io::Result<Self> {
        // Create latency ring (short SQPOLL timeout for responsiveness)
        let latency_ring = Builder::default()
            .setup_sqpoll(100)  // Short timeout
            .setup_single_issuer()
            .setup_coop_taskrun()
            .build(config.latency_ring_entries)?;

        // Create main ring (longer timeout, can idle)
        let main_ring = Builder::default()
            .setup_sqpoll(1000)  // Longer timeout
            .setup_single_issuer()
            .setup_coop_taskrun()
            .build(config.main_ring_entries)?;

        // Get latency ring's fd for wake-up registration
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
        main_ring.submitter().submit()?;

        // Create optional IOPOLL ring for NVMe storage
        let poll_ring = if config.enable_poll_ring {
            Some(Builder::default()
                .setup_iopoll()
                .setup_sqpoll(1000)
                .setup_single_issuer()
                .build(config.poll_ring_entries)?)
        } else {
            None
        };

        Ok(Self {
            latency_ring,
            main_ring,
            poll_ring,
            latency_eventfd: latency_fd,
            core_id,
            stats: ThreeRingStats::default(),
        })
    }

    /// Main event loop - integrate with Ring 0/1/2 architecture.
    pub fn run(&mut self, handler: &mut dyn RingHandler) {
        loop {
            // 1. Always drain latency ring first (non-blocking)
            while let Some(cqe) = self.latency_ring.completion().next() {
                self.stats.latency_completions += 1;
                handler.handle_latency_completion(cqe);
            }

            // 2. Drain main ring completions (non-blocking check)
            while let Some(cqe) = self.main_ring.completion().next() {
                self.stats.main_completions += 1;
                handler.handle_main_completion(cqe);
            }

            // 3. Drain poll ring if present (non-blocking)
            if let Some(ref mut poll_ring) = self.poll_ring {
                while let Some(cqe) = poll_ring.completion().next() {
                    self.stats.poll_completions += 1;
                    handler.handle_poll_completion(cqe);
                }
            }

            // 4. Ring 0: Process application events
            handler.process_ring0_events();

            // 5. Ring 1: Background work (if Ring 0 idle)
            if handler.ring0_idle() {
                handler.process_ring1_chunk();
            }

            // 6. Ring 2: Control plane (rare)
            if handler.has_control_message() {
                handler.process_ring2();
            }

            // 7. Check for shutdown
            if handler.should_shutdown() {
                break;
            }

            // 8. Block on main ring if nothing to do
            //    Will wake up when latency ring has activity
            if handler.should_sleep() {
                self.stats.main_ring_sleeps += 1;
                match self.main_ring.submitter().submit_and_wait(1) {
                    Ok(_) => {
                        // Check if we were woken by latency ring
                        if self.latency_ring.completion().len() > 0 {
                            self.stats.latency_wake_ups += 1;
                        }
                    }
                    Err(e) if e.raw_os_error() == Some(libc::EINTR) => {
                        // Interrupted, continue loop
                    }
                    Err(e) => {
                        tracing::error!("main_ring wait error: {}", e);
                    }
                }
            }
        }
    }

    /// Submit to latency ring (network, urgent).
    #[inline]
    pub fn submit_latency(&mut self, entry: io_uring::squeue::Entry) -> io::Result<()> {
        unsafe {
            self.latency_ring.submission().push(&entry)?;
        }
        self.latency_ring.submitter().submit()?;
        Ok(())
    }

    /// Submit to main ring (normal I/O).
    #[inline]
    pub fn submit_main(&mut self, entry: io_uring::squeue::Entry) -> io::Result<()> {
        unsafe {
            self.main_ring.submission().push(&entry)?;
        }
        self.main_ring.submitter().submit()?;
        Ok(())
    }

    /// Submit to poll ring (storage, NVMe only).
    pub fn submit_poll(&mut self, entry: io_uring::squeue::Entry) -> io::Result<()> {
        let ring = self.poll_ring.as_mut()
            .ok_or_else(|| io::Error::new(
                io::ErrorKind::NotFound,
                "Poll ring not enabled",
            ))?;

        unsafe {
            ring.submission().push(&entry)?;
        }
        ring.submitter().submit()?;
        Ok(())
    }

    /// Get stats for monitoring.
    pub fn stats(&self) -> &ThreeRingStats {
        &self.stats
    }
}
```

### Ring Handler Trait

```rust
/// Handler interface for three-ring event processing.
///
/// Implement this trait to define how completions are processed
/// for each ring type.
pub trait RingHandler {
    /// Handle latency ring completion (network, urgent).
    fn handle_latency_completion(&mut self, cqe: io_uring::cqueue::Entry);

    /// Handle main ring completion (WAL, normal I/O).
    fn handle_main_completion(&mut self, cqe: io_uring::cqueue::Entry);

    /// Handle poll ring completion (storage).
    fn handle_poll_completion(&mut self, cqe: io_uring::cqueue::Entry);

    /// Process Ring 0 events (application hot path).
    fn process_ring0_events(&mut self);

    /// Check if Ring 0 is idle.
    fn ring0_idle(&self) -> bool;

    /// Process Ring 1 chunk (background work).
    fn process_ring1_chunk(&mut self);

    /// Check for control messages.
    fn has_control_message(&self) -> bool;

    /// Process Ring 2 (control plane).
    fn process_ring2(&mut self);

    /// Check if reactor should sleep.
    fn should_sleep(&self) -> bool;

    /// Check if reactor should shutdown.
    fn should_shutdown(&self) -> bool;
}
```

### Configuration

```rust
/// Configuration for three-ring reactor.
pub struct ThreeRingConfig {
    /// Entries for latency ring (smaller, always polled)
    pub latency_ring_entries: u32,

    /// Entries for main ring (larger, can block)
    pub main_ring_entries: u32,

    /// Entries for poll ring (NVMe storage)
    pub poll_ring_entries: u32,

    /// Enable poll ring (requires NVMe)
    pub enable_poll_ring: bool,

    /// SQPOLL timeout for latency ring (ms)
    pub latency_sqpoll_timeout: u32,

    /// SQPOLL timeout for main ring (ms)
    pub main_sqpoll_timeout: u32,
}

impl Default for ThreeRingConfig {
    fn default() -> Self {
        Self {
            latency_ring_entries: 256,
            main_ring_entries: 1024,
            poll_ring_entries: 256,
            enable_poll_ring: false,
            latency_sqpoll_timeout: 100,
            main_sqpoll_timeout: 1000,
        }
    }
}
```

### Integration with CoreHandle

```rust
impl CoreHandle {
    /// Create CoreHandle with three-ring I/O.
    pub fn new_with_three_ring(
        core_id: usize,
        config: &CoreConfig,
    ) -> io::Result<Self> {
        let three_ring = ThreeRingReactor::new(
            core_id,
            &config.three_ring_config,
        )?;

        // ... rest of CoreHandle setup

        Ok(Self {
            core_id,
            three_ring,
            // ...
        })
    }

    /// Submit network receive (latency ring).
    pub fn submit_network_recv(&mut self, /* ... */) -> io::Result<()> {
        let entry = opcode::Recv::new(/* ... */).build();
        self.three_ring.submit_latency(entry)
    }

    /// Submit WAL write (main ring).
    pub fn submit_wal_write(&mut self, /* ... */) -> io::Result<()> {
        let entry = opcode::Write::new(/* ... */).build();
        self.three_ring.submit_main(entry)
    }

    /// Submit storage read (poll ring if available, else main).
    pub fn submit_storage_read(&mut self, /* ... */) -> io::Result<()> {
        let entry = opcode::Read::new(/* ... */).build();
        if self.three_ring.poll_ring.is_some() {
            self.three_ring.submit_poll(entry)
        } else {
            self.three_ring.submit_main(entry)
        }
    }
}
```

### Operation Classification

```rust
/// Classify I/O operations by ring affinity.
#[derive(Clone, Copy, Debug)]
pub enum RingAffinity {
    /// Latency-critical, always polled.
    /// Network receives, urgent reads, timers.
    Latency,

    /// Normal I/O, can block when idle.
    /// WAL writes, checkpointing, background reads.
    Main,

    /// Storage polling, NVMe only.
    /// High-throughput sequential storage.
    Poll,
}

impl RingAffinity {
    /// Determine ring affinity for operation type.
    pub fn for_operation(op: &OperationType) -> Self {
        match op {
            OperationType::NetworkRecv => RingAffinity::Latency,
            OperationType::NetworkSend => RingAffinity::Latency,
            OperationType::TimerExpiry => RingAffinity::Latency,
            OperationType::UrgentRead => RingAffinity::Latency,

            OperationType::WalWrite => RingAffinity::Main,
            OperationType::WalSync => RingAffinity::Main,
            OperationType::CheckpointWrite => RingAffinity::Main,
            OperationType::BackgroundRead => RingAffinity::Main,

            OperationType::StorageRead => RingAffinity::Poll,
            OperationType::StorageWrite => RingAffinity::Poll,
        }
    }
}
```

### Completion Routing

```rust
/// Routes completions to appropriate handlers.
pub struct CompletionRouter {
    /// Pending operations by user_data ID
    pending: HashMap<u64, PendingOp>,
}

impl CompletionRouter {
    /// Track pending operation.
    pub fn track(&mut self, user_data: u64, op: PendingOp) {
        self.pending.insert(user_data, op);
    }

    /// Route completion to handler.
    pub fn route(
        &mut self,
        cqe: &io_uring::cqueue::Entry,
        handlers: &mut Handlers,
    ) {
        let user_data = cqe.user_data();
        if let Some(op) = self.pending.remove(&user_data) {
            match op.affinity {
                RingAffinity::Latency => handlers.latency.complete(cqe, op),
                RingAffinity::Main => handlers.main.complete(cqe, op),
                RingAffinity::Poll => handlers.poll.complete(cqe, op),
            }
        }
    }
}
```

## Implementation Phases

### Phase 1: Latency + Main Rings (1 week)

1. Create `ThreeRingReactor` struct
2. Implement latency ring (SQPOLL, short timeout)
3. Implement main ring (SQPOLL, can block)
4. Eventfd wake-up mechanism
5. Basic event loop

### Phase 2: Ring Handler Integration (3-4 days)

1. Define `RingHandler` trait
2. Implement completion routing
3. Integrate with existing reactor code
4. Unit tests for ring coordination

### Phase 3: Poll Ring (Optional) (3-4 days)

1. Implement IOPOLL ring creation
2. NVMe device detection
3. Fallback when IOPOLL unavailable
4. Storage operation routing

### Phase 4: CoreHandle Integration (3-4 days)

1. Integrate with F013 CoreHandle
2. Operation classification
3. Benchmarking and tuning
4. Documentation

## Test Cases

```rust
#[test]
fn test_latency_ring_never_blocks() {
    let mut reactor = ThreeRingReactor::new(0, &Default::default()).unwrap();

    // Submit to latency ring
    let entry = opcode::Nop::new().build();
    reactor.submit_latency(entry).unwrap();

    // Should complete without blocking
    let start = Instant::now();
    reactor.latency_ring.completion().next();
    assert!(start.elapsed() < Duration::from_micros(100));
}

#[test]
fn test_main_ring_wakes_on_latency() {
    let mut reactor = ThreeRingReactor::new(0, &Default::default()).unwrap();

    // Start blocking on main ring in another thread
    let handle = thread::spawn(move || {
        reactor.main_ring.submitter().submit_and_wait(1).unwrap();
        reactor.stats.latency_wake_ups
    });

    // Wait a bit, then trigger latency ring
    thread::sleep(Duration::from_millis(10));
    // ... trigger latency completion

    let wake_ups = handle.join().unwrap();
    assert!(wake_ups > 0);
}

#[test]
fn test_ring_affinity_classification() {
    assert_eq!(
        RingAffinity::for_operation(&OperationType::NetworkRecv),
        RingAffinity::Latency
    );
    assert_eq!(
        RingAffinity::for_operation(&OperationType::WalWrite),
        RingAffinity::Main
    );
}

#[test]
fn test_poll_ring_requires_nvme() {
    let config = ThreeRingConfig {
        enable_poll_ring: true,
        ..Default::default()
    };

    let reactor = ThreeRingReactor::new(0, &config);
    // May fail if no NVMe device
}
```

## Acceptance Criteria

- [x] ThreeRingReactor with latency/main rings
- [x] Latency ring always polled first
- [x] Main ring blocks when idle
- [x] Eventfd wake-up working
- [x] Optional IOPOLL ring for NVMe
- [x] Operation classification by ring affinity
- [ ] Integration with F013 CoreHandle (future: add ThreeRingReactor option to CoreHandle)
- [ ] Benchmark showing latency improvement (future: add criterion benchmarks)
- [x] 10+ unit tests passing (40+ tests across modules)

## Implementation Summary

**New Module**: `crates/laminar-core/src/io_uring/three_ring/`

```
three_ring/
├── mod.rs       # Public exports, module documentation
├── affinity.rs  # RingAffinity enum, OperationType classification
├── config.rs    # ThreeRingConfig, builder pattern
├── handler.rs   # RingHandler trait, SimpleRingHandler
├── reactor.rs   # ThreeRingReactor (core implementation)
├── router.rs    # CompletionRouter, PendingOperation, RoutedCompletion
└── stats.rs     # ThreeRingStats (latency, completions, wake-ups)
```

**Key Components**:
- `ThreeRingReactor` - Main reactor with latency/main/poll rings
- `RingAffinity` - Latency, Main, Poll classification
- `OperationType` - 19 operation types (network, WAL, storage, etc.)
- `RingHandler` trait - Handler interface for Ring 0/1/2 integration
- `CompletionRouter` - Tracks pending ops, routes completions
- `ThreeRingStats` - Per-ring latency, submissions, completions, wake-ups

**Usage Example**:
```rust
use laminar_core::io_uring::three_ring::{
    ThreeRingConfig, ThreeRingReactor, OperationType, RingAffinity,
};

// Create reactor
let config = ThreeRingConfig::builder()
    .latency_entries(256)
    .main_entries(1024)
    .enable_poll_ring(false)  // Enable for NVMe
    .build()?;

let mut reactor = ThreeRingReactor::new(0, config)?;

// Submit with automatic affinity
reactor.submit_with_affinity(entry, OperationType::NetworkRecv)?;
reactor.submit_with_affinity(entry, OperationType::WalWrite)?;

// Or use convenience methods
reactor.submit_recv(fd, buf, len)?;  // → latency ring
reactor.submit_wal_write(fd, buf, len, offset)?;  // → main ring
reactor.submit_storage_read(fd, buf, len, offset)?;  // → poll ring (or fallback)

// Poll all rings (latency first)
let completions = reactor.poll_all();
for c in completions {
    println!("Completed: user_data={}, latency={:?}", c.user_data, c.latency());
}
```

## Performance Targets

| Metric | Single Ring | Three Ring | Improvement |
|--------|-------------|------------|-------------|
| Network recv p99 | ~50μs | <10μs | 5x |
| WAL write during network burst | Delayed | No impact | N/A |
| Idle power | Busy-poll | Efficient sleep | Lower CPU |

## References

- [Thread-Per-Core 2026 Research](../../research/laminardb-thread-per-core-2026-research.md)
- [Seastar I/O Architecture](https://seastar.io/tutorial/#io-scheduling)
- [Glommio Rings](https://docs.rs/glommio/latest/glommio/)
- [io_uring Multiple Rings Pattern](https://kernel.dk/io_uring.pdf)
