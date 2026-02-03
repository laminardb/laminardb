//! # I/O Capabilities Detection
//!
//! Detects I/O subsystem capabilities including `io_uring`, XDP, and storage type.
//!
//! ## Usage
//!
//! ```rust,ignore
//! use laminar_core::detect::{IoUringCapabilities, XdpCapabilities, StorageInfo};
//!
//! let io_uring = IoUringCapabilities::detect();
//! if io_uring.sqpoll_supported {
//!     println!("SQPOLL is available!");
//! }
//!
//! let storage = StorageInfo::detect("/var/lib/laminardb");
//! match storage.device_type {
//!     StorageType::NVMe => println!("Fast NVMe storage!"),
//!     _ => {}
//! }
//! ```

use std::path::Path;

use super::KernelVersion;

/// `io_uring` capabilities.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct IoUringCapabilities {
    /// Whether `io_uring` is available at all.
    pub available: bool,
    /// Whether SQPOLL mode is supported (kernel polling thread).
    pub sqpoll_supported: bool,
    /// Whether IOPOLL mode is supported (polling completions from device).
    pub iopoll_supported: bool,
    /// Whether registered buffers are supported.
    pub registered_buffers: bool,
    /// Whether multishot operations are supported.
    pub multishot_supported: bool,
    /// Whether `COOP_TASKRUN` is supported.
    pub coop_taskrun: bool,
    /// Whether `SINGLE_ISSUER` optimization is supported.
    pub single_issuer: bool,
    /// Whether the `io_uring` Cargo feature is enabled.
    pub feature_enabled: bool,
}

impl IoUringCapabilities {
    /// Detect `io_uring` capabilities.
    #[must_use]
    pub fn detect() -> Self {
        let kernel = KernelVersion::detect();
        Self::from_kernel_version(kernel.as_ref())
    }

    /// Determine capabilities from kernel version.
    #[must_use]
    pub fn from_kernel_version(kernel: Option<&KernelVersion>) -> Self {
        // io_uring is only available on Linux
        #[cfg(not(target_os = "linux"))]
        {
            let _ = kernel;
            Self {
                feature_enabled: false,
                ..Default::default()
            }
        }

        #[cfg(target_os = "linux")]
        {
            let feature_enabled = cfg!(feature = "io-uring");

            let Some(kv) = kernel else {
                return Self {
                    feature_enabled,
                    ..Default::default()
                };
            };

            let available = kv.supports_io_uring() && feature_enabled;

            Self {
                available,
                sqpoll_supported: available && kv.supports_io_uring_sqpoll(),
                iopoll_supported: available && kv.supports_io_uring_iopoll(),
                registered_buffers: available && kv.supports_io_uring_registered_buffers(),
                multishot_supported: available && kv.supports_io_uring_multishot(),
                coop_taskrun: available && kv.supports_io_uring_coop_taskrun(),
                single_issuer: available && kv.supports_io_uring_single_issuer(),
                feature_enabled,
            }
        }
    }

    /// Check if `io_uring` is usable for file I/O.
    #[must_use]
    pub fn is_usable(&self) -> bool {
        self.available && self.feature_enabled
    }

    /// Check if advanced features are available.
    #[must_use]
    pub fn has_advanced_features(&self) -> bool {
        self.sqpoll_supported && self.iopoll_supported
    }

    /// Get a summary string.
    #[must_use]
    pub fn summary(&self) -> String {
        if !self.feature_enabled {
            return "io_uring feature not enabled".to_string();
        }

        if !self.available {
            return "io_uring not available".to_string();
        }

        let mut features = vec!["basic"];

        if self.sqpoll_supported {
            features.push("SQPOLL");
        }
        if self.iopoll_supported {
            features.push("IOPOLL");
        }
        if self.registered_buffers {
            features.push("registered_buffers");
        }
        if self.multishot_supported {
            features.push("multishot");
        }
        if self.coop_taskrun {
            features.push("coop_taskrun");
        }
        if self.single_issuer {
            features.push("single_issuer");
        }

        format!("io_uring: {}", features.join(", "))
    }
}

/// XDP/eBPF capabilities.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct XdpCapabilities {
    /// Whether XDP is available.
    pub available: bool,
    /// Whether XDP generic mode (SKB) is supported.
    pub generic_supported: bool,
    /// Whether XDP native mode is widely supported.
    pub native_supported: bool,
    /// Whether XDP CPU map redirect is supported.
    pub cpumap_supported: bool,
    /// Whether the XDP Cargo feature is enabled.
    pub feature_enabled: bool,
}

impl XdpCapabilities {
    /// Detect XDP capabilities.
    #[must_use]
    pub fn detect() -> Self {
        let kernel = KernelVersion::detect();
        Self::from_kernel_version(kernel.as_ref())
    }

    /// Determine capabilities from kernel version.
    #[must_use]
    pub fn from_kernel_version(kernel: Option<&KernelVersion>) -> Self {
        // XDP is only available on Linux
        #[cfg(not(target_os = "linux"))]
        {
            let _ = kernel;
            Self {
                feature_enabled: false,
                ..Default::default()
            }
        }

        #[cfg(target_os = "linux")]
        {
            let feature_enabled = cfg!(feature = "xdp");

            let Some(kv) = kernel else {
                return Self {
                    feature_enabled,
                    ..Default::default()
                };
            };

            let available = kv.supports_xdp() && feature_enabled;

            Self {
                available,
                generic_supported: available && kv.supports_xdp_generic(),
                native_supported: available && kv.supports_xdp_native(),
                cpumap_supported: available && kv.supports_xdp_cpumap(),
                feature_enabled,
            }
        }
    }

    /// Check if XDP is usable.
    #[must_use]
    pub fn is_usable(&self) -> bool {
        self.available && self.feature_enabled
    }

    /// Get a summary string.
    #[must_use]
    pub fn summary(&self) -> String {
        if !self.feature_enabled {
            return "XDP feature not enabled".to_string();
        }

        if !self.available {
            return "XDP not available".to_string();
        }

        let mut modes = Vec::new();

        if self.generic_supported {
            modes.push("generic");
        }
        if self.native_supported {
            modes.push("native");
        }
        if self.cpumap_supported {
            modes.push("cpumap");
        }

        format!("XDP: {}", modes.join(", "))
    }
}

/// Storage device type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StorageType {
    /// `NVMe` SSD (fastest, supports IOPOLL)
    NVMe,
    /// SATA/SAS SSD
    Ssd,
    /// Spinning hard drive
    Hdd,
    /// Network-attached storage
    Network,
    /// RAM disk / tmpfs
    RamDisk,
    /// Unknown or undetected
    #[default]
    Unknown,
}

impl StorageType {
    /// Check if this storage type supports `io_uring` IOPOLL.
    #[must_use]
    pub fn supports_iopoll(&self) -> bool {
        matches!(self, Self::NVMe)
    }

    /// Check if this is fast storage (SSD or `NVMe`).
    #[must_use]
    pub fn is_fast(&self) -> bool {
        matches!(self, Self::NVMe | Self::Ssd | Self::RamDisk)
    }

    /// Get a description of the storage type.
    #[must_use]
    pub fn description(&self) -> &'static str {
        match self {
            Self::NVMe => "NVMe SSD",
            Self::Ssd => "SSD",
            Self::Hdd => "HDD",
            Self::Network => "Network storage",
            Self::RamDisk => "RAM disk",
            Self::Unknown => "Unknown",
        }
    }
}

impl std::fmt::Display for StorageType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.description())
    }
}

/// Storage device information.
#[derive(Debug, Clone, Default)]
pub struct StorageInfo {
    /// Detected storage type.
    pub device_type: StorageType,
    /// Whether `O_DIRECT` is supported.
    pub supports_direct_io: bool,
    /// Detected block device name (e.g., "nvme0n1").
    pub device_name: Option<String>,
    /// Detected filesystem type (e.g., "ext4", "xfs").
    pub filesystem: Option<String>,
}

impl StorageInfo {
    /// Detect storage information for a path.
    #[must_use]
    pub fn detect<P: AsRef<Path>>(path: P) -> Self {
        #[cfg(target_os = "linux")]
        {
            Self::detect_linux(path.as_ref())
        }

        #[cfg(not(target_os = "linux"))]
        {
            let _ = path;
            Self::default()
        }
    }

    /// Detect storage info on Linux.
    #[cfg(target_os = "linux")]
    fn detect_linux(path: &Path) -> Self {
        let mut info = Self::default();

        // Get the device from the path
        let device = match Self::get_device_for_path(path) {
            Some(d) => d,
            None => return info,
        };

        info.device_name = Some(device.clone());

        // Detect storage type from sysfs
        info.device_type = Self::detect_storage_type_sysfs(&device);

        // Detect filesystem type
        info.filesystem = Self::detect_filesystem_type(path);

        // Check O_DIRECT support
        info.supports_direct_io = Self::check_direct_io_support(path);

        info
    }

    /// Get the block device for a path.
    #[cfg(target_os = "linux")]
    fn get_device_for_path(path: &Path) -> Option<String> {
        use std::fs;
        use std::os::unix::fs::MetadataExt;

        // Get the device ID from the path
        let metadata = fs::metadata(path).ok()?;
        let dev = metadata.dev();

        // Major/minor device numbers
        let major = (dev >> 8) & 0xFF;
        let minor = dev & 0xFF;

        // Read /proc/diskstats or /sys/dev/block to find the device name
        let _block_path = format!("/sys/dev/block/{major}:{minor}/device/../");

        if let Ok(entries) = fs::read_dir("/sys/block") {
            for entry in entries.flatten() {
                let name = entry.file_name();
                let dev_path = format!("/sys/block/{}/dev", name.to_string_lossy());
                if let Ok(content) = fs::read_to_string(&dev_path) {
                    let content = content.trim();
                    if content == format!("{major}:{minor}") {
                        return Some(name.to_string_lossy().to_string());
                    }
                    // Check for partitions
                    if content.starts_with(&format!("{major}:")) {
                        let base_name = name.to_string_lossy();
                        // Strip partition number for the base device
                        let base = base_name.trim_end_matches(|c: char| c.is_ascii_digit());
                        if !base.is_empty() {
                            return Some(base.to_string());
                        }
                    }
                }
            }
        }

        // Fallback: try to find from /proc/mounts
        if let Ok(mounts) = fs::read_to_string("/proc/mounts") {
            let path_str = path.to_string_lossy();
            for line in mounts.lines() {
                let parts: Vec<&str> = line.split_whitespace().collect();
                if parts.len() >= 2 && path_str.starts_with(parts[1]) {
                    if let Some(device) = parts[0].strip_prefix("/dev/") {
                        let base = device.trim_end_matches(|c: char| c.is_ascii_digit());
                        return Some(base.to_string());
                    }
                }
            }
        }

        None
    }

    /// Detect storage type from sysfs.
    #[cfg(target_os = "linux")]
    fn detect_storage_type_sysfs(device: &str) -> StorageType {
        use std::fs;

        // Check for NVMe
        if device.starts_with("nvme") {
            return StorageType::NVMe;
        }

        // Check rotational flag
        let rotational_path = format!("/sys/block/{device}/queue/rotational");
        if let Ok(content) = fs::read_to_string(&rotational_path) {
            if content.trim() == "0" {
                return StorageType::Ssd;
            } else if content.trim() == "1" {
                return StorageType::Hdd;
            }
        }

        // Check for RAM disk
        if device.starts_with("ram") || device.starts_with("zram") {
            return StorageType::RamDisk;
        }

        // Check for network devices
        if device.starts_with("nbd") || device.starts_with("rbd") {
            return StorageType::Network;
        }

        StorageType::Unknown
    }

    /// Detect filesystem type.
    #[cfg(target_os = "linux")]
    fn detect_filesystem_type(path: &Path) -> Option<String> {
        use std::fs;

        let path_str = path.to_string_lossy();

        if let Ok(mounts) = fs::read_to_string("/proc/mounts") {
            // Find the most specific mount point for this path
            let mut best_match: Option<(&str, &str)> = None;

            for line in mounts.lines() {
                let parts: Vec<&str> = line.split_whitespace().collect();
                if parts.len() >= 3 {
                    let mount_point = parts[1];
                    let fs_type = parts[2];

                    if path_str.starts_with(mount_point) {
                        match best_match {
                            None => best_match = Some((mount_point, fs_type)),
                            Some((prev_mount, _)) => {
                                if mount_point.len() > prev_mount.len() {
                                    best_match = Some((mount_point, fs_type));
                                }
                            }
                        }
                    }
                }
            }

            return best_match.map(|(_, fs)| fs.to_string());
        }

        None
    }

    /// Check if O_DIRECT is supported.
    #[cfg(target_os = "linux")]
    fn check_direct_io_support(path: &Path) -> bool {
        use std::fs::OpenOptions;
        use std::os::unix::fs::OpenOptionsExt;

        // Try to open a test file with O_DIRECT
        let test_path = if path.is_dir() {
            path.join(".laminardb_direct_test")
        } else {
            path.parent()
                .map(|p| p.join(".laminardb_direct_test"))
                .unwrap_or_else(|| path.to_path_buf())
        };

        let result = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .custom_flags(libc::O_DIRECT)
            .open(&test_path);

        // Clean up test file
        let _ = std::fs::remove_file(&test_path);

        result.is_ok()
    }

    /// Get a summary string.
    #[must_use]
    pub fn summary(&self) -> String {
        let mut parts = vec![self.device_type.description().to_string()];

        if let Some(ref device) = self.device_name {
            parts.push(format!("({device})"));
        }

        if let Some(ref fs) = self.filesystem {
            parts.push(format!("[{fs}]"));
        }

        if self.supports_direct_io {
            parts.push("O_DIRECT".to_string());
        }

        parts.join(" ")
    }
}

/// Memory information.
#[derive(Debug, Clone, Copy, Default)]
pub struct MemoryInfo {
    /// Total system memory in bytes.
    pub total_memory: u64,
    /// Available memory in bytes.
    pub available_memory: u64,
    /// Whether huge pages are available.
    pub huge_pages_available: bool,
    /// Huge page size in bytes (usually 2MB or 1GB).
    pub huge_page_size: usize,
    /// Number of free huge pages.
    pub huge_pages_free: usize,
    /// Whether transparent huge pages are enabled.
    pub thp_enabled: bool,
}

impl MemoryInfo {
    /// Detect memory information.
    #[must_use]
    pub fn detect() -> Self {
        #[cfg(target_os = "linux")]
        {
            Self::detect_linux()
        }

        #[cfg(not(target_os = "linux"))]
        {
            Self::detect_fallback()
        }
    }

    /// Detect memory info on Linux.
    #[cfg(target_os = "linux")]
    fn detect_linux() -> Self {
        use std::fs;

        let mut info = Self::default();

        // Parse /proc/meminfo
        if let Ok(meminfo) = fs::read_to_string("/proc/meminfo") {
            for line in meminfo.lines() {
                let parts: Vec<&str> = line.split_whitespace().collect();
                if parts.len() >= 2 {
                    let value: u64 = parts[1].parse().unwrap_or(0);
                    match parts[0].trim_end_matches(':') {
                        "MemTotal" => info.total_memory = value * 1024,
                        "MemAvailable" => info.available_memory = value * 1024,
                        "Hugepagesize" => info.huge_page_size = (value * 1024) as usize,
                        "HugePages_Free" => info.huge_pages_free = value as usize,
                        _ => {}
                    }
                }
            }
        }

        // Check if huge pages are available
        info.huge_pages_available = info.huge_page_size > 0;

        // Check THP status
        if let Ok(thp_enabled) = fs::read_to_string("/sys/kernel/mm/transparent_hugepage/enabled") {
            // Format is: "[always] madvise never" where brackets indicate current setting
            info.thp_enabled =
                thp_enabled.contains("[always]") || thp_enabled.contains("[madvise]");
        }

        info
    }

    /// Fallback detection for non-Linux platforms.
    #[cfg(not(target_os = "linux"))]
    fn detect_fallback() -> Self {
        // Estimate 16GB as a reasonable default
        Self {
            total_memory: 16 * 1024 * 1024 * 1024,
            available_memory: 8 * 1024 * 1024 * 1024,
            huge_pages_available: false,
            huge_page_size: 0,
            huge_pages_free: 0,
            thp_enabled: false,
        }
    }

    /// Get total memory in gigabytes.
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn total_memory_gb(&self) -> f64 {
        self.total_memory as f64 / (1024.0 * 1024.0 * 1024.0)
    }

    /// Get available memory in gigabytes.
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn available_memory_gb(&self) -> f64 {
        self.available_memory as f64 / (1024.0 * 1024.0 * 1024.0)
    }

    /// Get a summary string.
    #[must_use]
    pub fn summary(&self) -> String {
        let mut parts = vec![format!("{:.1} GB total", self.total_memory_gb())];

        if self.huge_pages_available {
            parts.push(format!(
                "{} huge pages ({} KB each)",
                self.huge_pages_free,
                self.huge_page_size / 1024
            ));
        }

        if self.thp_enabled {
            parts.push("THP enabled".to_string());
        }

        parts.join(", ")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_io_uring_capabilities_default() {
        let caps = IoUringCapabilities::default();
        assert!(!caps.available);
        assert!(!caps.sqpoll_supported);
        assert!(!caps.iopoll_supported);
    }

    #[test]
    fn test_io_uring_capabilities_from_kernel() {
        // Test with kernel 5.1 (basic io_uring)
        let kv = KernelVersion::new(5, 1, 0);
        let caps = IoUringCapabilities::from_kernel_version(Some(&kv));

        // Feature enabled depends on build flags
        #[cfg(all(target_os = "linux", feature = "io-uring"))]
        {
            assert!(caps.available);
            assert!(caps.registered_buffers);
            assert!(!caps.sqpoll_supported); // Needs 5.11+
        }

        #[cfg(not(all(target_os = "linux", feature = "io-uring")))]
        {
            assert!(!caps.available);
        }
    }

    #[test]
    fn test_io_uring_capabilities_advanced() {
        let kv = KernelVersion::new(6, 0, 0);
        let _caps = IoUringCapabilities::from_kernel_version(Some(&kv));

        #[cfg(all(target_os = "linux", feature = "io-uring"))]
        {
            assert!(_caps.sqpoll_supported);
            assert!(_caps.iopoll_supported);
            assert!(_caps.coop_taskrun);
            assert!(_caps.single_issuer);
        }
    }

    #[test]
    fn test_io_uring_summary() {
        let caps = IoUringCapabilities::default();
        let summary = caps.summary();
        assert!(!summary.is_empty());
    }

    #[test]
    fn test_xdp_capabilities_default() {
        let caps = XdpCapabilities::default();
        assert!(!caps.available);
        assert!(!caps.generic_supported);
        assert!(!caps.native_supported);
    }

    #[test]
    fn test_xdp_capabilities_from_kernel() {
        let kv = KernelVersion::new(5, 3, 0);
        let _caps = XdpCapabilities::from_kernel_version(Some(&kv));

        #[cfg(all(target_os = "linux", feature = "xdp"))]
        {
            assert!(_caps.available);
            assert!(_caps.generic_supported);
            assert!(_caps.native_supported);
            assert!(_caps.cpumap_supported);
        }
    }

    #[test]
    fn test_xdp_summary() {
        let caps = XdpCapabilities::default();
        let summary = caps.summary();
        assert!(!summary.is_empty());
    }

    #[test]
    fn test_storage_type_properties() {
        assert!(StorageType::NVMe.supports_iopoll());
        assert!(!StorageType::Ssd.supports_iopoll());
        assert!(!StorageType::Hdd.supports_iopoll());

        assert!(StorageType::NVMe.is_fast());
        assert!(StorageType::Ssd.is_fast());
        assert!(!StorageType::Hdd.is_fast());
        assert!(StorageType::RamDisk.is_fast());
    }

    #[test]
    fn test_storage_type_display() {
        assert_eq!(format!("{}", StorageType::NVMe), "NVMe SSD");
        assert_eq!(format!("{}", StorageType::Ssd), "SSD");
        assert_eq!(format!("{}", StorageType::Hdd), "HDD");
    }

    #[test]
    fn test_storage_info_default() {
        let info = StorageInfo::default();
        assert_eq!(info.device_type, StorageType::Unknown);
        assert!(!info.supports_direct_io);
    }

    #[test]
    fn test_storage_info_detect() {
        // This should not panic on any platform
        let info = StorageInfo::detect("/");

        // Summary should work
        let summary = info.summary();
        assert!(!summary.is_empty());
    }

    #[test]
    fn test_memory_info_detect() {
        let info = MemoryInfo::detect();

        // Should have some memory
        assert!(info.total_memory > 0);

        // Summary should work
        let summary = info.summary();
        assert!(!summary.is_empty());
    }

    #[test]
    fn test_memory_info_gb_conversion() {
        let info = MemoryInfo {
            total_memory: 16 * 1024 * 1024 * 1024,    // 16 GB
            available_memory: 8 * 1024 * 1024 * 1024, // 8 GB
            ..Default::default()
        };

        assert!((info.total_memory_gb() - 16.0).abs() < 0.01);
        assert!((info.available_memory_gb() - 8.0).abs() < 0.01);
    }
}
