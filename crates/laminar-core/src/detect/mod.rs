//! # Automatic Hardware/Software Feature Detection
//!
//! Runtime detection and automatic configuration of platform-specific features.
//!
//! ## Overview
//!
//! This module provides unified detection of system capabilities including:
//!
//! - **Kernel Version**: Linux kernel features (`io_uring`, XDP requirements)
//! - **CPU Features**: SIMD capabilities (AVX2, AVX-512, NEON), cache configuration
//! - **I/O Capabilities**: `io_uring` support levels, storage type detection
//! - **Network**: XDP/eBPF availability
//! - **Memory**: NUMA topology, huge pages, total memory
//!
//! ## Usage
//!
//! ```rust
//! use laminar_core::detect::SystemCapabilities;
//!
//! // Detect all system capabilities (cached after first call)
//! let caps = SystemCapabilities::detect();
//!
//! // Print system summary
//! println!("{}", caps.summary());
//!
//! // Get recommended configuration
//! let config = caps.recommended_config();
//! println!("Recommended cores: {}", config.num_cores);
//! println!("Use io_uring: {}", config.use_io_uring);
//! ```
//!
//! ## Auto-Configuration
//!
//! Use [`SystemCapabilities::recommended_config()`] to get an optimized configuration
//! based on detected hardware:
//!
//! ```rust,ignore
//! use laminar_core::detect::SystemCapabilities;
//! use laminar_core::tpc::TpcConfig;
//! use laminar_core::io_uring::IoUringConfig;
//!
//! let caps = SystemCapabilities::detect();
//! let recommended = caps.recommended_config();
//!
//! // Apply to TPC runtime
//! let tpc_config = TpcConfig::builder()
//!     .num_cores(recommended.num_cores)
//!     .cpu_pinning(recommended.cpu_pinning)
//!     .numa_aware(recommended.numa_aware)
//!     .build()?;
//! ```
//!
//! ## Platform Support
//!
//! | Feature | Linux | Windows | macOS |
//! |---------|-------|---------|-------|
//! | Kernel version | Full | N/A | Darwin |
//! | CPU features | Full | Full | Full |
//! | NUMA nodes | Full | Single | Single |
//! | io_uring | Full | N/A | N/A |
//! | XDP | Full | N/A | N/A |
//! | Storage detection | Full | Limited | Limited |
//! | Huge pages | Full | Limited | Limited |

mod config;
mod cpu;
mod io;
mod kernel;

pub use config::{PerformanceTier, RecommendedConfig};
pub use cpu::{
    cache_line_size, is_smt_enabled, logical_cpu_count, physical_cpu_count, CpuFeatures, SimdLevel,
};
pub use io::{IoUringCapabilities, MemoryInfo, StorageInfo, StorageType, XdpCapabilities};
pub use kernel::KernelVersion;

use crate::numa::NumaTopology;
use std::path::Path;
use std::sync::OnceLock;

/// Detected system capabilities.
///
/// This struct aggregates all detected hardware and software capabilities
/// into a single view, making it easy to configure `LaminarDB` optimally.
///
/// Detection is performed lazily and cached for efficiency.
#[derive(Debug, Clone)]
pub struct SystemCapabilities {
    // ===== Platform =====
    /// Detected platform.
    pub platform: Platform,
    /// Detected kernel version (Linux/macOS only).
    pub kernel_version: Option<KernelVersion>,

    // ===== CPU =====
    /// Number of logical CPUs.
    pub cpu_count: usize,
    /// Number of physical CPU cores.
    pub physical_cores: usize,
    /// Number of NUMA nodes.
    pub numa_nodes: usize,
    /// Cache line size in bytes.
    pub cache_line_size: usize,
    /// CPU feature flags.
    pub cpu_features: CpuFeatures,

    // ===== I/O =====
    /// `io_uring` capabilities.
    pub io_uring: IoUringCapabilities,
    /// XDP capabilities.
    pub xdp: XdpCapabilities,
    /// Storage information (for data directory).
    pub storage: StorageInfo,

    // ===== Memory =====
    /// Memory information.
    pub memory: MemoryInfo,
}

/// Global cached capabilities.
static CACHED_CAPABILITIES: OnceLock<SystemCapabilities> = OnceLock::new();

impl SystemCapabilities {
    /// Detect all system capabilities.
    ///
    /// This method is safe to call multiple times - results are cached
    /// after the first detection.
    #[must_use]
    pub fn detect() -> &'static Self {
        CACHED_CAPABILITIES.get_or_init(Self::detect_uncached)
    }

    /// Detect capabilities without caching.
    ///
    /// Use [`detect()`](Self::detect) for normal usage.
    #[must_use]
    pub fn detect_uncached() -> Self {
        let kernel_version = KernelVersion::detect();
        let cpu_features = CpuFeatures::detect();
        let numa_topology = NumaTopology::detect();

        Self {
            platform: Platform::detect(),
            kernel_version,

            cpu_count: logical_cpu_count(),
            physical_cores: physical_cpu_count(),
            numa_nodes: numa_topology.num_nodes(),
            cache_line_size: cache_line_size(),
            cpu_features,

            io_uring: IoUringCapabilities::from_kernel_version(kernel_version.as_ref()),
            xdp: XdpCapabilities::from_kernel_version(kernel_version.as_ref()),
            storage: StorageInfo::default(), // Detect lazily with detect_storage()

            memory: MemoryInfo::detect(),
        }
    }

    /// Detect capabilities with storage detection for a specific path.
    #[must_use]
    pub fn detect_with_storage<P: AsRef<Path>>(data_path: P) -> Self {
        let mut caps = Self::detect_uncached();
        caps.storage = StorageInfo::detect(data_path);
        caps
    }

    /// Update storage detection for a new path.
    pub fn update_storage<P: AsRef<Path>>(&mut self, data_path: P) {
        self.storage = StorageInfo::detect(data_path);
    }

    /// Get recommended configuration based on detected capabilities.
    #[must_use]
    pub fn recommended_config(&self) -> RecommendedConfig {
        RecommendedConfig::from_capabilities(self)
    }

    /// Generate a human-readable summary of all capabilities.
    #[must_use]
    pub fn summary(&self) -> String {
        use std::fmt::Write;

        let mut s = String::new();

        let _ = writeln!(s, "System Capabilities:");
        let _ = writeln!(s, "  Platform: {}", self.platform);
        if let Some(ref kv) = self.kernel_version {
            let _ = writeln!(s, "  Kernel: {kv}");
        }

        let _ = writeln!(s);
        let _ = writeln!(s, "CPU:");
        let _ = writeln!(s, "  Logical CPUs: {}", self.cpu_count);
        let _ = writeln!(s, "  Physical cores: {}", self.physical_cores);
        let _ = writeln!(s, "  NUMA nodes: {}", self.numa_nodes);
        let _ = writeln!(s, "  Cache line: {} bytes", self.cache_line_size);
        let _ = writeln!(s, "  SIMD: {}", self.cpu_features.simd_level());
        let _ = writeln!(s, "  Features: {}", self.cpu_features.summary());

        let _ = writeln!(s);
        let _ = writeln!(s, "I/O:");
        let _ = writeln!(s, "  {}", self.io_uring.summary());
        let _ = writeln!(s, "  {}", self.xdp.summary());
        let _ = writeln!(s, "  Storage: {}", self.storage.summary());

        let _ = writeln!(s);
        let _ = writeln!(s, "Memory:");
        let _ = writeln!(s, "  {}", self.memory.summary());

        let recommended = self.recommended_config();
        let _ = writeln!(s);
        let _ = writeln!(s, "Performance Tier: {}", recommended.performance_tier());

        s
    }

    /// Check if all advanced features are available.
    #[must_use]
    pub fn is_fully_optimized(&self) -> bool {
        self.io_uring.is_usable() && self.io_uring.sqpoll_supported && self.cpu_count > 1
    }

    /// Check if the system meets minimum requirements for `LaminarDB`.
    #[must_use]
    pub fn meets_minimum_requirements(&self) -> bool {
        // Minimum: 1 CPU, 512MB RAM
        self.cpu_count >= 1 && self.memory.total_memory >= 512 * 1024 * 1024
    }

    /// Get a list of missing features that would improve performance.
    #[must_use]
    pub fn missing_features(&self) -> Vec<&'static str> {
        let mut missing = Vec::new();

        if !self.io_uring.feature_enabled {
            missing.push("io_uring feature not enabled (compile with --features io-uring)");
        } else if !self.io_uring.available {
            missing.push("io_uring not available (requires Linux 5.1+)");
        }

        if !self.io_uring.sqpoll_supported && self.io_uring.available {
            missing.push("io_uring SQPOLL not supported (requires Linux 5.11+)");
        }

        if !self.io_uring.iopoll_supported && self.io_uring.available {
            missing.push("io_uring IOPOLL not supported (requires Linux 5.19+ and NVMe)");
        }

        if !self.xdp.feature_enabled {
            missing.push("XDP feature not enabled (compile with --features xdp)");
        }

        if self.numa_nodes <= 1 && self.cpu_count > 8 {
            missing.push("NUMA topology not detected (may need hwloc feature)");
        }

        if !self.memory.huge_pages_available {
            missing.push("Huge pages not available");
        }

        missing
    }

    /// Log the detected capabilities.
    pub fn log_summary(&self) {
        tracing::info!("Platform: {}", self.platform);
        if let Some(ref kv) = self.kernel_version {
            tracing::info!("Kernel: {kv}");
        }
        tracing::info!(
            "CPU: {} logical, {} physical, {} NUMA nodes",
            self.cpu_count,
            self.physical_cores,
            self.numa_nodes
        );
        tracing::info!("SIMD: {}", self.cpu_features.simd_level());
        tracing::info!("io_uring: {}", self.io_uring.summary());
        tracing::info!("XDP: {}", self.xdp.summary());
        tracing::info!("Memory: {:.1} GB", self.memory.total_memory_gb());
        tracing::info!(
            "Performance tier: {}",
            self.recommended_config().performance_tier()
        );
    }
}

/// Detected platform.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Platform {
    /// Linux
    Linux,
    /// macOS
    MacOS,
    /// Windows
    Windows,
    /// FreeBSD
    FreeBSD,
    /// Other/unknown
    Other,
}

impl Platform {
    /// Detect the current platform.
    #[must_use]
    pub const fn detect() -> Self {
        #[cfg(target_os = "linux")]
        {
            Self::Linux
        }
        #[cfg(target_os = "macos")]
        {
            Self::MacOS
        }
        #[cfg(target_os = "windows")]
        {
            Self::Windows
        }
        #[cfg(target_os = "freebsd")]
        {
            Self::FreeBSD
        }
        #[cfg(not(any(
            target_os = "linux",
            target_os = "macos",
            target_os = "windows",
            target_os = "freebsd"
        )))]
        {
            Self::Other
        }
    }

    /// Check if this is a Unix-like platform.
    #[must_use]
    pub const fn is_unix(&self) -> bool {
        matches!(self, Self::Linux | Self::MacOS | Self::FreeBSD)
    }

    /// Check if `io_uring` is potentially available.
    #[must_use]
    pub const fn supports_io_uring(&self) -> bool {
        matches!(self, Self::Linux)
    }

    /// Check if XDP is potentially available.
    #[must_use]
    pub const fn supports_xdp(&self) -> bool {
        matches!(self, Self::Linux)
    }
}

impl std::fmt::Display for Platform {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Linux => write!(f, "Linux"),
            Self::MacOS => write!(f, "macOS"),
            Self::Windows => write!(f, "Windows"),
            Self::FreeBSD => write!(f, "FreeBSD"),
            Self::Other => write!(f, "Other"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_system_capabilities_detect() {
        let caps = SystemCapabilities::detect();

        assert!(caps.cpu_count >= 1);
        assert!(caps.physical_cores >= 1);
        assert!(caps.numa_nodes >= 1);
        assert!(caps.cache_line_size >= 32);
        assert!(caps.memory.total_memory > 0);
    }

    #[test]
    fn test_system_capabilities_cached() {
        // Multiple calls should return the same instance
        let caps1 = SystemCapabilities::detect();
        let caps2 = SystemCapabilities::detect();

        assert_eq!(caps1.cpu_count, caps2.cpu_count);
        assert_eq!(caps1.numa_nodes, caps2.numa_nodes);
    }

    #[test]
    fn test_system_capabilities_summary() {
        let caps = SystemCapabilities::detect();
        let summary = caps.summary();

        assert!(summary.contains("System Capabilities"));
        assert!(summary.contains("CPU:"));
        assert!(summary.contains("I/O:"));
        assert!(summary.contains("Memory:"));
    }

    #[test]
    fn test_system_capabilities_recommended_config() {
        let caps = SystemCapabilities::detect();
        let config = caps.recommended_config();

        assert!(config.num_cores >= 1);
        assert!(config.arena_size > 0);
        assert!(config.state_store_size > 0);
    }

    #[test]
    fn test_system_capabilities_meets_minimum() {
        let caps = SystemCapabilities::detect();
        assert!(caps.meets_minimum_requirements());
    }

    #[test]
    fn test_system_capabilities_missing_features() {
        let caps = SystemCapabilities::detect();
        let missing = caps.missing_features();

        // Just verify it returns something (may or may not be empty)
        // The test won't fail either way
        let _ = missing;
    }

    #[test]
    fn test_platform_detect() {
        let platform = Platform::detect();

        #[cfg(target_os = "linux")]
        assert_eq!(platform, Platform::Linux);

        #[cfg(target_os = "macos")]
        assert_eq!(platform, Platform::MacOS);

        #[cfg(target_os = "windows")]
        assert_eq!(platform, Platform::Windows);
    }

    #[test]
    fn test_platform_is_unix() {
        assert!(Platform::Linux.is_unix());
        assert!(Platform::MacOS.is_unix());
        assert!(!Platform::Windows.is_unix());
    }

    #[test]
    fn test_platform_supports_io_uring() {
        assert!(Platform::Linux.supports_io_uring());
        assert!(!Platform::MacOS.supports_io_uring());
        assert!(!Platform::Windows.supports_io_uring());
    }

    #[test]
    fn test_platform_display() {
        assert_eq!(format!("{}", Platform::Linux), "Linux");
        assert_eq!(format!("{}", Platform::MacOS), "macOS");
        assert_eq!(format!("{}", Platform::Windows), "Windows");
    }

    #[test]
    fn test_detect_with_storage() {
        // This test just verifies the method works
        let caps = SystemCapabilities::detect_with_storage("/");

        assert!(caps.cpu_count >= 1);
        // Storage type should be detected (or Unknown)
    }

    #[test]
    fn test_update_storage() {
        let mut caps = SystemCapabilities::detect_uncached();
        caps.update_storage("/");

        // Verify update completed without panic
    }
}
