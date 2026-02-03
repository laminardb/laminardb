//! # Auto-Configuration Builder
//!
//! Generates optimal configuration based on detected system capabilities.
//!
//! ## Usage
//!
//! ```rust,ignore
//! use laminar_core::detect::{SystemCapabilities, RecommendedConfig};
//!
//! let caps = SystemCapabilities::detect();
//! let config = caps.recommended_config();
//!
//! println!("Recommended cores: {}", config.num_cores);
//! println!("Use io_uring: {}", config.use_io_uring);
//! ```

use super::{logical_cpu_count, physical_cpu_count};

/// Recommended configuration based on detected capabilities.
#[derive(Debug, Clone)]
#[allow(clippy::struct_excessive_bools)]
pub struct RecommendedConfig {
    // ===== Thread-Per-Core =====
    /// Recommended number of cores to use.
    pub num_cores: usize,
    /// Whether to enable CPU pinning.
    pub cpu_pinning: bool,
    /// Whether NUMA-aware allocation should be enabled.
    pub numa_aware: bool,

    // ===== I/O =====
    /// Whether to use `io_uring` for I/O.
    pub use_io_uring: bool,
    /// Whether to use SQPOLL mode.
    pub io_uring_sqpoll: bool,
    /// Whether to use IOPOLL mode (for `NVMe`).
    pub io_uring_iopoll: bool,

    // ===== Network =====
    /// Whether to use XDP for network processing.
    pub use_xdp: bool,

    // ===== Memory =====
    /// Whether to use huge pages.
    pub use_huge_pages: bool,
    /// Recommended arena size per core.
    pub arena_size: usize,
    /// Recommended state store size per core.
    pub state_store_size: usize,
    /// Recommended queue capacity.
    pub queue_capacity: usize,

    // ===== Performance =====
    /// Detected cache line size.
    pub cache_line_size: usize,
}

impl RecommendedConfig {
    /// Generate recommended configuration from system capabilities.
    #[must_use]
    pub fn from_capabilities(caps: &super::SystemCapabilities) -> Self {
        Self {
            // Thread-per-core
            num_cores: Self::calculate_num_cores(caps),
            cpu_pinning: caps.cpu_count > 1,
            numa_aware: caps.numa_nodes > 1,

            // I/O
            use_io_uring: caps.io_uring.is_usable(),
            io_uring_sqpoll: caps.io_uring.sqpoll_supported,
            io_uring_iopoll: caps.io_uring.iopoll_supported
                && caps.storage.device_type.supports_iopoll(),

            // Network
            use_xdp: caps.xdp.is_usable(),

            // Memory
            use_huge_pages: caps.memory.huge_pages_available && caps.memory.huge_pages_free > 0,
            arena_size: Self::calculate_arena_size(caps),
            state_store_size: Self::calculate_state_store_size(caps),
            queue_capacity: Self::calculate_queue_capacity(caps),

            // Performance
            cache_line_size: caps.cache_line_size,
        }
    }

    /// Calculate the recommended number of cores.
    fn calculate_num_cores(_caps: &super::SystemCapabilities) -> usize {
        let physical = physical_cpu_count();
        let logical = logical_cpu_count();

        // For thread-per-core, prefer physical cores to avoid SMT contention
        // But if only 1-2 physical cores, use logical to get some parallelism
        if physical <= 2 {
            logical.min(4)
        } else {
            // Leave 1 core for OS/background if we have many cores
            if physical > 8 {
                physical - 1
            } else {
                physical
            }
        }
    }

    /// Calculate the recommended arena size per core.
    fn calculate_arena_size(caps: &super::SystemCapabilities) -> usize {
        let memory_per_core = caps.memory.available_memory / caps.cpu_count as u64;

        // Use 1/8 of per-core memory for arena, capped at 64MB
        let arena = (memory_per_core / 8) as usize;
        arena.clamp(1024 * 1024, 64 * 1024 * 1024) // 1MB - 64MB
    }

    /// Calculate the recommended state store size per core.
    fn calculate_state_store_size(caps: &super::SystemCapabilities) -> usize {
        let memory_per_core = caps.memory.available_memory / caps.cpu_count as u64;

        // Use 1/4 of per-core memory for state, capped at 1GB
        let state_size = (memory_per_core / 4) as usize;
        state_size.clamp(16 * 1024 * 1024, 1024 * 1024 * 1024) // 16MB - 1GB
    }

    /// Calculate the recommended queue capacity.
    fn calculate_queue_capacity(caps: &super::SystemCapabilities) -> usize {
        // Higher capacity for more memory
        if caps.memory.total_memory > 32 * 1024 * 1024 * 1024 {
            131_072 // 128K entries
        } else if caps.memory.total_memory > 8 * 1024 * 1024 * 1024 {
            65_536 // 64K entries
        } else {
            16_384 // 16K entries
        }
    }

    /// Generate a human-readable summary.
    #[must_use]
    pub fn summary(&self) -> String {
        use std::fmt::Write;

        let mut s = String::new();

        let _ = writeln!(s, "Recommended Configuration:");
        let _ = writeln!(
            s,
            "  Cores: {} (pinned: {})",
            self.num_cores, self.cpu_pinning
        );
        let _ = writeln!(s, "  NUMA-aware: {}", self.numa_aware);

        let _ = writeln!(
            s,
            "  io_uring: {} (SQPOLL: {}, IOPOLL: {})",
            self.use_io_uring, self.io_uring_sqpoll, self.io_uring_iopoll
        );
        let _ = writeln!(s, "  XDP: {}", self.use_xdp);

        let _ = writeln!(s, "  Huge pages: {}", self.use_huge_pages);
        let _ = writeln!(s, "  Arena size: {} MB", self.arena_size / (1024 * 1024));
        let _ = writeln!(
            s,
            "  State store: {} MB",
            self.state_store_size / (1024 * 1024)
        );
        let _ = writeln!(s, "  Queue capacity: {}", self.queue_capacity);
        let _ = writeln!(s, "  Cache line: {} bytes", self.cache_line_size);

        s
    }

    /// Check if this configuration uses all advanced features.
    #[must_use]
    pub fn is_optimal(&self) -> bool {
        self.use_io_uring && self.io_uring_sqpoll && self.cpu_pinning
    }

    /// Get a performance tier based on capabilities.
    #[must_use]
    pub fn performance_tier(&self) -> PerformanceTier {
        if self.use_io_uring && self.io_uring_sqpoll && self.io_uring_iopoll && self.numa_aware {
            PerformanceTier::Maximum
        } else if self.use_io_uring && self.io_uring_sqpoll {
            PerformanceTier::High
        } else if self.use_io_uring {
            PerformanceTier::Good
        } else {
            PerformanceTier::Basic
        }
    }
}

impl Default for RecommendedConfig {
    fn default() -> Self {
        Self {
            num_cores: logical_cpu_count(),
            cpu_pinning: false,
            numa_aware: false,
            use_io_uring: false,
            io_uring_sqpoll: false,
            io_uring_iopoll: false,
            use_xdp: false,
            use_huge_pages: false,
            arena_size: 16 * 1024 * 1024,
            state_store_size: 256 * 1024 * 1024,
            queue_capacity: 65_536,
            cache_line_size: 64,
        }
    }
}

/// Performance tier based on available features.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum PerformanceTier {
    /// Standard syscall-based I/O
    Basic,
    /// `io_uring` basic mode
    Good,
    /// `io_uring` with SQPOLL
    High,
    /// Full optimization (`io_uring` SQPOLL+IOPOLL, NUMA, XDP)
    Maximum,
}

impl std::fmt::Display for PerformanceTier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PerformanceTier::Basic => write!(f, "Basic"),
            PerformanceTier::Good => write!(f, "Good"),
            PerformanceTier::High => write!(f, "High"),
            PerformanceTier::Maximum => write!(f, "Maximum"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::SystemCapabilities;
    use super::*;

    #[test]
    fn test_recommended_config_default() {
        let config = RecommendedConfig::default();
        assert!(config.num_cores >= 1);
        assert!(!config.cpu_pinning);
        assert!(!config.use_io_uring);
    }

    #[test]
    fn test_recommended_config_from_capabilities() {
        let caps = SystemCapabilities::detect();
        let config = RecommendedConfig::from_capabilities(caps);

        assert!(config.num_cores >= 1);
        assert!(config.arena_size >= 1024 * 1024);
        assert!(config.state_store_size >= 16 * 1024 * 1024);
        assert!(config.cache_line_size >= 32);
    }

    #[test]
    fn test_recommended_config_summary() {
        let config = RecommendedConfig::default();
        let summary = config.summary();

        assert!(summary.contains("Cores:"));
        assert!(summary.contains("io_uring:"));
        assert!(summary.contains("NUMA-aware:"));
    }

    #[test]
    fn test_performance_tier_ordering() {
        assert!(PerformanceTier::Basic < PerformanceTier::Good);
        assert!(PerformanceTier::Good < PerformanceTier::High);
        assert!(PerformanceTier::High < PerformanceTier::Maximum);
    }

    #[test]
    fn test_performance_tier_display() {
        assert_eq!(format!("{}", PerformanceTier::Basic), "Basic");
        assert_eq!(format!("{}", PerformanceTier::Maximum), "Maximum");
    }

    #[test]
    fn test_is_optimal() {
        let mut config = RecommendedConfig::default();
        assert!(!config.is_optimal());

        config.use_io_uring = true;
        config.io_uring_sqpoll = true;
        config.cpu_pinning = true;
        assert!(config.is_optimal());
    }

    #[test]
    fn test_performance_tier_basic() {
        let config = RecommendedConfig::default();
        assert_eq!(config.performance_tier(), PerformanceTier::Basic);
    }

    #[test]
    fn test_performance_tier_good() {
        let mut config = RecommendedConfig::default();
        config.use_io_uring = true;
        assert_eq!(config.performance_tier(), PerformanceTier::Good);
    }

    #[test]
    fn test_performance_tier_high() {
        let mut config = RecommendedConfig::default();
        config.use_io_uring = true;
        config.io_uring_sqpoll = true;
        assert_eq!(config.performance_tier(), PerformanceTier::High);
    }

    #[test]
    fn test_performance_tier_maximum() {
        let mut config = RecommendedConfig::default();
        config.use_io_uring = true;
        config.io_uring_sqpoll = true;
        config.io_uring_iopoll = true;
        config.numa_aware = true;
        assert_eq!(config.performance_tier(), PerformanceTier::Maximum);
    }
}
