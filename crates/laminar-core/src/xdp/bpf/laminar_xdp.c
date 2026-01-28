// SPDX-License-Identifier: GPL-2.0 OR Apache-2.0
//
// LaminarDB XDP Program for Network Optimization (F072)
//
// This XDP program provides:
// - Invalid packet filtering at NIC driver level
// - CPU steering by partition key for thread-per-core
// - Protocol validation before kernel stack
// - Statistics collection
//
// Compilation:
//   clang -O2 -target bpf -g -c laminar_xdp.c -o laminar_xdp.o
//
// Installation:
//   sudo ip link set dev eth0 xdpgeneric obj laminar_xdp.o sec xdp
//
// Or use the Rust XdpLoader for programmatic control.
//

#include <linux/bpf.h>
#include <linux/if_ether.h>
#include <linux/ip.h>
#include <linux/udp.h>
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_endian.h>

// ============================================================================
// Configuration
// ============================================================================

// LaminarDB magic bytes (first 4 bytes of payload): "LAMI" = 0x4C414D49
#define LAMINAR_MAGIC 0x4C414D49

// LaminarDB default UDP port (can be overridden via map)
#define LAMINAR_PORT 9999

// Maximum number of CPUs supported
#define MAX_CPUS 64

// ============================================================================
// BPF Maps
// ============================================================================

// CPU map for redirecting packets to specific CPUs
struct {
    __uint(type, BPF_MAP_TYPE_CPUMAP);
    __uint(key_size, sizeof(__u32));
    __uint(value_size, sizeof(struct bpf_cpumap_val));
    __uint(max_entries, MAX_CPUS);
} cpu_map SEC(".maps");

// Statistics map (per-CPU for lock-free updates)
// Keys: 0=dropped, 1=passed, 2=redirected, 3=invalid
struct {
    __uint(type, BPF_MAP_TYPE_PERCPU_ARRAY);
    __uint(key_size, sizeof(__u32));
    __uint(value_size, sizeof(__u64));
    __uint(max_entries, 4);
} stats SEC(".maps");

// Port configuration map (allows runtime port configuration)
struct {
    __uint(type, BPF_MAP_TYPE_ARRAY);
    __uint(key_size, sizeof(__u32));
    __uint(value_size, sizeof(__u16));
    __uint(max_entries, 1);
} port_config SEC(".maps");

// ============================================================================
// Statistics Keys
// ============================================================================

#define STAT_DROPPED    0
#define STAT_PASSED     1
#define STAT_REDIRECTED 2
#define STAT_INVALID    3

// ============================================================================
// Helper Functions
// ============================================================================

// Increment a per-CPU statistic counter
static __always_inline void inc_stat(__u32 key) {
    __u64 *value = bpf_map_lookup_elem(&stats, &key);
    if (value)
        __sync_fetch_and_add(value, 1);
}

// Get the configured port (or default)
static __always_inline __u16 get_laminar_port(void) {
    __u32 key = 0;
    __u16 *port = bpf_map_lookup_elem(&port_config, &key);
    if (port && *port != 0)
        return *port;
    return LAMINAR_PORT;
}

// ============================================================================
// LaminarDB Header (must match Rust LaminarHeader)
// ============================================================================

// LaminarDB packet header (20 bytes)
// Wire format:
//   +--------+--------+--------+--------+
//   |     MAGIC (4 bytes) = "LAMI"      |
//   +--------+--------+--------+--------+
//   |   PARTITION_KEY (4 bytes, BE)     |
//   +--------+--------+--------+--------+
//   |     PAYLOAD_LENGTH (4 bytes, BE)  |
//   +--------+--------+--------+--------+
//   |          SEQUENCE (8 bytes, BE)   |
//   |                                   |
//   +--------+--------+--------+--------+
//   |           PAYLOAD ...             |
//   +--------+--------+--------+--------+

struct laminar_header {
    __u32 magic;
    __u32 partition_key;
    __u32 payload_len;
    __u64 sequence;
} __attribute__((packed));

#define LAMINAR_HEADER_SIZE 20

// ============================================================================
// XDP Program Entry Point
// ============================================================================

SEC("xdp")
int laminar_ingress(struct xdp_md *ctx) {
    void *data = (void *)(long)ctx->data;
    void *data_end = (void *)(long)ctx->data_end;

    // ========================================================================
    // Parse Ethernet Header
    // ========================================================================
    struct ethhdr *eth = data;
    if ((void *)(eth + 1) > data_end) {
        inc_stat(STAT_INVALID);
        return XDP_PASS;  // Let kernel handle malformed packets
    }

    // Only process IPv4 packets
    if (eth->h_proto != bpf_htons(ETH_P_IP)) {
        return XDP_PASS;
    }

    // ========================================================================
    // Parse IP Header
    // ========================================================================
    struct iphdr *ip = (void *)(eth + 1);
    if ((void *)(ip + 1) > data_end) {
        inc_stat(STAT_INVALID);
        return XDP_PASS;
    }

    // Check IP header length (variable due to options)
    __u8 ip_hdr_len = ip->ihl * 4;
    if (ip_hdr_len < sizeof(struct iphdr)) {
        inc_stat(STAT_INVALID);
        return XDP_PASS;
    }

    // Only handle UDP (LaminarDB uses UDP for high-throughput ingest)
    if (ip->protocol != IPPROTO_UDP) {
        return XDP_PASS;
    }

    // ========================================================================
    // Parse UDP Header
    // ========================================================================
    struct udphdr *udp = (void *)((char *)ip + ip_hdr_len);
    if ((void *)(udp + 1) > data_end) {
        inc_stat(STAT_INVALID);
        return XDP_PASS;
    }

    // Check if this is a LaminarDB packet (by port)
    __u16 laminar_port = get_laminar_port();
    if (bpf_ntohs(udp->dest) != laminar_port) {
        return XDP_PASS;  // Not for us
    }

    // ========================================================================
    // Parse LaminarDB Header
    // ========================================================================
    struct laminar_header *lam = (void *)(udp + 1);
    if ((void *)lam + LAMINAR_HEADER_SIZE > data_end) {
        // Packet too short for LaminarDB header
        inc_stat(STAT_DROPPED);
        return XDP_DROP;
    }

    // Verify magic bytes
    if (bpf_ntohl(lam->magic) != LAMINAR_MAGIC) {
        // Not a valid LaminarDB packet
        inc_stat(STAT_DROPPED);
        return XDP_DROP;
    }

    // ========================================================================
    // CPU Steering by Partition Key
    // ========================================================================
    __u32 partition_key = bpf_ntohl(lam->partition_key);
    __u32 num_cpus = bpf_num_possible_cpus();

    // Bound check for safety
    if (num_cpus == 0 || num_cpus > MAX_CPUS) {
        num_cpus = 1;
    }

    __u32 target_cpu = partition_key % num_cpus;

    // Try to redirect to target CPU
    long ret = bpf_redirect_map(&cpu_map, target_cpu, 0);
    if (ret == XDP_REDIRECT) {
        inc_stat(STAT_REDIRECTED);
        return XDP_REDIRECT;
    }

    // Redirect failed (CPU not in map), pass to kernel stack
    inc_stat(STAT_PASSED);
    return XDP_PASS;
}

// ============================================================================
// License (required for BPF programs that use certain helpers)
// ============================================================================

char LICENSE[] SEC("license") = "Dual BSD/GPL";

// ============================================================================
// BTF Type Information (for libbpf-rs CO-RE)
// ============================================================================

// Version info for debugging
__u32 _version SEC("version") = 1;
