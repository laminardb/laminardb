# F072: XDP/eBPF Network Optimization

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F072 |
| **Status** | ✅ Done |
| **Priority** | P2 |
| **Phase** | 2 |
| **Effort** | XL (2-3 weeks) |
| **Dependencies** | F013, F067 |
| **Owner** | TBD |
| **Research** | [Thread-Per-Core 2026 Research](../../research/laminardb-thread-per-core-2026-research.md) |

## Summary

Implement XDP (eXpress Data Path) and eBPF integration for network optimization. XDP processes packets at the NIC driver level before kernel networking stack allocation, achieving 26 million packets/sec/core for filtering and CPU steering by partition key.

## Motivation

### XDP Performance

| Processing Level | Packets/sec/core | Latency |
|------------------|------------------|---------|
| Application (userspace) | ~1M | ~50μs |
| Kernel network stack | ~5M | ~10μs |
| **XDP (driver level)** | **26M** | **<1μs** |

### Use Cases for LaminarDB

| Use Case | XDP Action | Benefit |
|----------|------------|---------|
| Invalid packet filtering | XDP_DROP | Reduce CPU load |
| Protocol validation | XDP_DROP/PASS | Early rejection |
| Core routing by partition | XDP_REDIRECT | Bypass kernel routing |
| DDoS mitigation | XDP_DROP | Wire-speed filtering |

### Current Gap

LaminarDB uses standard sockets, meaning:
- All packets traverse full kernel stack
- No pre-filtering at driver level
- CPU steering requires kernel routing
- DDoS protection at application layer (slow)

## Goals

1. XDP program for LaminarDB protocol filtering
2. CPU steering by partition key (route to correct core)
3. Rust loader for XDP program deployment
4. Fallback to standard sockets when XDP unavailable
5. Performance benchmarks showing improvement

## Non-Goals

- Full kernel bypass (DPDK)
- Custom protocol stack
- Windows/macOS support (XDP is Linux-only)

## Technical Design

### XDP Program (C/BPF)

```c
// laminar_xdp.c - Compile with clang to BPF bytecode
//
// clang -O2 -target bpf -c laminar_xdp.c -o laminar_xdp.o

#include <linux/bpf.h>
#include <linux/if_ether.h>
#include <linux/ip.h>
#include <linux/udp.h>
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_endian.h>

// Map to redirect packets to specific CPUs
struct {
    __uint(type, BPF_MAP_TYPE_CPUMAP);
    __uint(key_size, sizeof(__u32));
    __uint(value_size, sizeof(struct bpf_cpumap_val));
    __uint(max_entries, 64);
} cpu_map SEC(".maps");

// Statistics map
struct {
    __uint(type, BPF_MAP_TYPE_PERCPU_ARRAY);
    __uint(key_size, sizeof(__u32));
    __uint(value_size, sizeof(__u64));
    __uint(max_entries, 4);  // dropped, passed, redirected, invalid
} stats SEC(".maps");

// LaminarDB magic bytes (first 4 bytes of payload)
#define LAMINAR_MAGIC 0x4C414D49  // "LAMI" in network byte order

// LaminarDB port
#define LAMINAR_PORT 9999

// Stats keys
#define STAT_DROPPED    0
#define STAT_PASSED     1
#define STAT_REDIRECTED 2
#define STAT_INVALID    3

static __always_inline void inc_stat(__u32 key) {
    __u64 *value = bpf_map_lookup_elem(&stats, &key);
    if (value)
        __sync_fetch_and_add(value, 1);
}

SEC("xdp")
int laminar_ingress(struct xdp_md *ctx) {
    void *data = (void *)(long)ctx->data;
    void *data_end = (void *)(long)ctx->data_end;

    // Parse Ethernet header
    struct ethhdr *eth = data;
    if ((void *)(eth + 1) > data_end) {
        inc_stat(STAT_INVALID);
        return XDP_PASS;
    }

    // Only handle IPv4
    if (eth->h_proto != bpf_htons(ETH_P_IP)) {
        return XDP_PASS;
    }

    // Parse IP header
    struct iphdr *ip = (void *)(eth + 1);
    if ((void *)(ip + 1) > data_end) {
        inc_stat(STAT_INVALID);
        return XDP_PASS;
    }

    // Only handle UDP (LaminarDB ingest protocol)
    if (ip->protocol != IPPROTO_UDP) {
        return XDP_PASS;
    }

    // Parse UDP header
    struct udphdr *udp = (void *)(ip + 1);
    if ((void *)(udp + 1) > data_end) {
        inc_stat(STAT_INVALID);
        return XDP_PASS;
    }

    // Check LaminarDB port
    if (bpf_ntohs(udp->dest) != LAMINAR_PORT) {
        return XDP_PASS;
    }

    // Get payload
    void *payload = (void *)(udp + 1);
    if (payload + 8 > data_end) {
        inc_stat(STAT_DROPPED);
        return XDP_DROP;  // Too short for LaminarDB header
    }

    // Check magic bytes
    __u32 magic = *(__u32 *)payload;
    if (magic != bpf_htonl(LAMINAR_MAGIC)) {
        inc_stat(STAT_DROPPED);
        return XDP_DROP;  // Invalid protocol
    }

    // Extract partition key (bytes 4-7)
    __u32 partition_key = bpf_ntohl(*((__u32 *)(payload + 4)));

    // Route to appropriate CPU based on partition
    __u32 num_cpus = bpf_num_possible_cpus();
    __u32 target_cpu = partition_key % num_cpus;

    // Redirect to target CPU
    long ret = bpf_redirect_map(&cpu_map, target_cpu, 0);
    if (ret == XDP_REDIRECT) {
        inc_stat(STAT_REDIRECTED);
        return XDP_REDIRECT;
    }

    // Fallback: pass to kernel stack
    inc_stat(STAT_PASSED);
    return XDP_PASS;
}

char LICENSE[] SEC("license") = "GPL";
```

### LaminarDB Packet Header

```rust
/// LaminarDB UDP packet header for XDP processing.
///
/// Wire format:
/// ```
/// +--------+--------+--------+--------+
/// |     MAGIC (4 bytes) = "LAMI"      |
/// +--------+--------+--------+--------+
/// |   PARTITION_KEY (4 bytes, BE)     |
/// +--------+--------+--------+--------+
/// |      PAYLOAD_LENGTH (4 bytes)     |
/// +--------+--------+--------+--------+
/// |           PAYLOAD ...             |
/// +--------+--------+--------+--------+
/// ```
#[repr(C, packed)]
pub struct LaminarHeader {
    /// Magic bytes: 0x4C414D49 ("LAMI")
    pub magic: u32,
    /// Partition key for CPU steering
    pub partition_key: u32,
    /// Payload length
    pub payload_len: u32,
}

impl LaminarHeader {
    pub const MAGIC: u32 = 0x4C414D49;
    pub const SIZE: usize = 12;

    pub fn new(partition_key: u32, payload_len: u32) -> Self {
        Self {
            magic: Self::MAGIC.to_be(),
            partition_key: partition_key.to_be(),
            payload_len: payload_len.to_be(),
        }
    }

    pub fn validate(&self) -> bool {
        u32::from_be(self.magic) == Self::MAGIC
    }
}
```

### Rust XDP Loader

```rust
use libbpf_rs::{MapFlags, ObjectBuilder, Link};
use std::path::Path;

/// XDP program loader for LaminarDB.
pub struct XdpLoader {
    /// BPF link (keeps program attached)
    link: Link,
    /// CPU map for steering configuration
    cpu_map: libbpf_rs::Map,
    /// Stats map for monitoring
    stats_map: libbpf_rs::Map,
}

impl XdpLoader {
    /// Load and attach XDP program to network interface.
    pub fn load_and_attach(
        bpf_obj_path: &Path,
        interface: &str,
        num_cores: usize,
    ) -> Result<Self, XdpError> {
        // Load BPF object
        let mut obj = ObjectBuilder::default()
            .open_file(bpf_obj_path)?
            .load()?;

        // Get XDP program
        let prog = obj.prog_mut("laminar_ingress")
            .ok_or(XdpError::ProgramNotFound)?;

        // Configure CPU map
        let cpu_map = obj.map("cpu_map")
            .ok_or(XdpError::MapNotFound("cpu_map"))?;

        for cpu in 0..num_cores {
            let key = (cpu as u32).to_ne_bytes();
            let value = libbpf_rs::CpumapValue {
                qsize: 2048,  // Queue size per CPU
                ..Default::default()
            };
            cpu_map.update(&key, &value.to_bytes(), MapFlags::ANY)?;
        }

        // Get stats map
        let stats_map = obj.map("stats")
            .ok_or(XdpError::MapNotFound("stats"))?;

        // Get interface index
        let ifindex = nix::net::if_::if_nametoindex(interface)
            .map_err(|_| XdpError::InterfaceNotFound(interface.to_string()))?;

        // Attach to interface
        let link = prog.attach_xdp(ifindex as i32)?;

        Ok(Self {
            link,
            cpu_map,
            stats_map,
        })
    }

    /// Get XDP statistics.
    pub fn stats(&self) -> Result<XdpStats, XdpError> {
        let mut stats = XdpStats::default();

        // Read percpu stats and sum
        for key in 0..4u32 {
            let key_bytes = key.to_ne_bytes();
            if let Some(value) = self.stats_map.lookup_percpu(&key_bytes, MapFlags::ANY)? {
                let sum: u64 = value.iter()
                    .map(|v| u64::from_ne_bytes(v.try_into().unwrap_or([0; 8])))
                    .sum();

                match key {
                    0 => stats.dropped = sum,
                    1 => stats.passed = sum,
                    2 => stats.redirected = sum,
                    3 => stats.invalid = sum,
                    _ => {}
                }
            }
        }

        Ok(stats)
    }

    /// Update CPU steering for partition.
    pub fn update_cpu_steering(&mut self, partition: u32, cpu: u32) -> Result<(), XdpError> {
        let key = partition.to_ne_bytes();
        let value = libbpf_rs::CpumapValue {
            qsize: 2048,
            ..Default::default()
        };
        // Note: actual steering is done by hash in XDP program
        // This is for future per-partition mapping
        Ok(())
    }
}

impl Drop for XdpLoader {
    fn drop(&mut self) {
        // Link is automatically detached on drop
    }
}

#[derive(Debug, Default)]
pub struct XdpStats {
    pub dropped: u64,
    pub passed: u64,
    pub redirected: u64,
    pub invalid: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum XdpError {
    #[error("XDP program not found")]
    ProgramNotFound,
    #[error("BPF map not found: {0}")]
    MapNotFound(&'static str),
    #[error("Interface not found: {0}")]
    InterfaceNotFound(String),
    #[error("BPF error: {0}")]
    Bpf(#[from] libbpf_rs::Error),
}
```

### Integration with ThreadPerCoreRuntime

```rust
impl ThreadPerCoreRuntime {
    /// Create runtime with XDP network optimization.
    pub fn with_xdp(
        config: TpcConfig,
        xdp_config: XdpConfig,
    ) -> Result<Self, TpcError> {
        let mut runtime = Self::new(config)?;

        // Load XDP program
        if xdp_config.enabled {
            match XdpLoader::load_and_attach(
                &xdp_config.bpf_object_path,
                &xdp_config.interface,
                runtime.num_cores(),
            ) {
                Ok(loader) => {
                    runtime.xdp_loader = Some(loader);
                    tracing::info!(
                        "XDP loaded on interface {}",
                        xdp_config.interface
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        "XDP load failed, falling back to standard sockets: {}",
                        e
                    );
                }
            }
        }

        Ok(runtime)
    }

    /// Get XDP stats for monitoring.
    pub fn xdp_stats(&self) -> Option<XdpStats> {
        self.xdp_loader.as_ref().and_then(|l| l.stats().ok())
    }
}

/// XDP configuration.
#[derive(Clone)]
pub struct XdpConfig {
    /// Enable XDP (requires root)
    pub enabled: bool,
    /// Path to compiled BPF object
    pub bpf_object_path: PathBuf,
    /// Network interface to attach to
    pub interface: String,
}

impl Default for XdpConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            bpf_object_path: PathBuf::from("/usr/share/laminardb/laminar_xdp.o"),
            interface: "eth0".to_string(),
        }
    }
}
```

### Build System Integration

```makefile
# Makefile for XDP program

CLANG := clang
LLVM_STRIP := llvm-strip
BPF_CFLAGS := -O2 -target bpf -g

XDP_SRC := src/xdp/laminar_xdp.c
XDP_OBJ := target/bpf/laminar_xdp.o

.PHONY: all clean install

all: $(XDP_OBJ)

$(XDP_OBJ): $(XDP_SRC)
	@mkdir -p $(dir $@)
	$(CLANG) $(BPF_CFLAGS) -c $< -o $@
	$(LLVM_STRIP) -g $@

install: $(XDP_OBJ)
	install -D -m 644 $< /usr/share/laminardb/laminar_xdp.o

clean:
	rm -f $(XDP_OBJ)
```

## Implementation Phases

### Phase 1: XDP Program (1 week)

1. Write XDP C program
2. Compile with clang/LLVM
3. Test with xdp-loader tool
4. Verify packet filtering

### Phase 2: Rust Loader (3-4 days)

1. Add libbpf-rs dependency
2. Implement XdpLoader
3. CPU map configuration
4. Stats collection

### Phase 3: Integration (3-4 days)

1. Integrate with ThreadPerCoreRuntime
2. UDP ingest path modifications
3. Fallback when XDP unavailable
4. Configuration options

### Phase 4: Testing & Benchmarks (3-4 days)

1. Integration tests
2. Performance benchmarks
3. Documentation
4. Packaging

## Test Cases

```rust
#[test]
fn test_laminar_header_format() {
    let header = LaminarHeader::new(42, 1024);
    assert_eq!(header.magic, 0x4C414D49u32.to_be());
    assert!(header.validate());
}

#[test]
#[ignore] // Requires root
fn test_xdp_load_unload() {
    let loader = XdpLoader::load_and_attach(
        Path::new("target/bpf/laminar_xdp.o"),
        "lo", // Use loopback for testing
        4,
    ).unwrap();

    let stats = loader.stats().unwrap();
    // Stats should be zero initially
    assert_eq!(stats.dropped, 0);

    // Loader dropped, XDP detached
}

#[test]
#[ignore] // Requires root
fn test_xdp_filtering() {
    let loader = XdpLoader::load_and_attach(/* ... */).unwrap();

    // Send invalid packet
    send_udp_packet(9999, b"invalid");

    // Should be dropped
    let stats = loader.stats().unwrap();
    assert!(stats.dropped > 0);
}

#[test]
#[ignore] // Requires root
fn test_xdp_cpu_steering() {
    let loader = XdpLoader::load_and_attach(/* ... */).unwrap();

    // Send packets with different partition keys
    for partition in 0..4 {
        send_laminar_packet(partition, b"data");
    }

    // Should be redirected to correct CPUs
    let stats = loader.stats().unwrap();
    assert!(stats.redirected > 0);
}
```

## Acceptance Criteria

- [ ] XDP program compiles with clang
- [ ] Rust loader implemented
- [ ] CPU steering by partition key working
- [ ] Fallback to standard sockets
- [ ] Stats collection working
- [ ] Integration with TPC runtime
- [ ] Performance benchmark showing improvement
- [ ] Documentation complete
- [ ] 5+ integration tests passing

## Performance Targets

| Metric | Without XDP | With XDP | Improvement |
|--------|-------------|----------|-------------|
| Packet filtering | ~1M/sec | ~26M/sec | 26x |
| CPU steering latency | ~10μs | <1μs | 10x |
| DDoS packet drop | Application | NIC driver | Wire-speed |

## Platform Support

| Platform | Support |
|----------|---------|
| Linux 4.15+ | XDP generic (SKB mode) |
| Linux 5.3+ | XDP native (driver mode) |
| macOS | Not supported |
| Windows | Not supported |

## Dependencies

- `libbpf-rs` crate for BPF loading
- `clang` and `llvm` for BPF compilation
- Linux kernel with XDP support
- Root privileges for XDP attachment
- Network driver with XDP support (for best performance)

## References

- [Thread-Per-Core 2026 Research](../../research/laminardb-thread-per-core-2026-research.md)
- [XDP Tutorial](https://github.com/xdp-project/xdp-tutorial)
- [libbpf-rs Documentation](https://docs.rs/libbpf-rs/)
- [eBPF and XDP Reference Guide](https://docs.cilium.io/en/latest/bpf/)
