//! # Kernel Version Detection
//!
//! Detects the Linux kernel version to determine which features are available.
//!
//! ## Usage
//!
//! ```rust,ignore
//! use laminar_core::detect::KernelVersion;
//!
//! if let Some(version) = KernelVersion::detect() {
//!     println!("Kernel: {}", version);
//!     if version.supports_io_uring_sqpoll() {
//!         println!("SQPOLL is supported!");
//!     }
//! }
//! ```

use std::cmp::Ordering;
use std::fmt;

/// Linux kernel version.
///
/// Parsed from `/proc/version` or `uname -r`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct KernelVersion {
    /// Major version (e.g., 5 in 5.15.0)
    pub major: u32,
    /// Minor version (e.g., 15 in 5.15.0)
    pub minor: u32,
    /// Patch version (e.g., 0 in 5.15.0)
    pub patch: u32,
}

impl KernelVersion {
    /// Creates a new kernel version.
    #[must_use]
    pub const fn new(major: u32, minor: u32, patch: u32) -> Self {
        Self { major, minor, patch }
    }

    /// Detects the current kernel version.
    ///
    /// Returns `None` on non-Linux platforms or if detection fails.
    #[must_use]
    pub fn detect() -> Option<Self> {
        #[cfg(target_os = "linux")]
        {
            Self::detect_linux()
        }

        #[cfg(target_os = "macos")]
        {
            Self::detect_macos()
        }

        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        {
            None
        }
    }

    /// Detect kernel version on Linux.
    #[cfg(target_os = "linux")]
    fn detect_linux() -> Option<Self> {
        // Try /proc/version first (most reliable)
        if let Ok(version) = std::fs::read_to_string("/proc/version") {
            if let Some(v) = Self::parse_proc_version(&version) {
                return Some(v);
            }
        }

        // Fallback to uname
        Self::detect_uname()
    }

    /// Detect kernel version on macOS (Darwin kernel).
    #[cfg(target_os = "macos")]
    fn detect_macos() -> Option<Self> {
        Self::detect_uname()
    }

    /// Detect kernel version using libc uname.
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    fn detect_uname() -> Option<Self> {
        // SAFETY: uname is a safe syscall that fills a buffer
        unsafe {
            let mut info: libc::utsname = std::mem::zeroed();
            if libc::uname(&mut info) != 0 {
                return None;
            }

            // Convert release field to string
            let release = std::ffi::CStr::from_ptr(info.release.as_ptr())
                .to_str()
                .ok()?;

            Self::parse_version_string(release)
        }
    }

    /// Parse `/proc/version` content.
    ///
    /// Format: "Linux version 5.15.0-91-generic ..."
    #[allow(dead_code)]
    fn parse_proc_version(content: &str) -> Option<Self> {
        // Find "Linux version X.Y.Z"
        let parts: Vec<&str> = content.split_whitespace().collect();
        for (i, part) in parts.iter().enumerate() {
            if *part == "version" {
                if let Some(version_str) = parts.get(i + 1) {
                    return Self::parse_version_string(version_str);
                }
            }
        }
        None
    }

    /// Parse a version string like "5.15.0-91-generic" or "5.15.0".
    #[allow(dead_code)]
    fn parse_version_string(s: &str) -> Option<Self> {
        // Take only the numeric prefix (before any dash or other suffix)
        let version_part = s.split(|c: char| !c.is_ascii_digit() && c != '.').next()?;

        let mut parts = version_part.split('.');
        let major: u32 = parts.next()?.parse().ok()?;
        let minor: u32 = parts.next().and_then(|s| s.parse().ok()).unwrap_or(0);
        let patch: u32 = parts.next().and_then(|s| s.parse().ok()).unwrap_or(0);

        Some(Self { major, minor, patch })
    }

    // ===== io_uring Feature Checks =====

    /// Check if basic `io_uring` is supported (Linux 5.1+).
    #[must_use]
    pub fn supports_io_uring(&self) -> bool {
        *self >= Self::new(5, 1, 0)
    }

    /// Check if `io_uring` with fixed files is supported (Linux 5.1+).
    #[must_use]
    pub fn supports_io_uring_fixed_files(&self) -> bool {
        *self >= Self::new(5, 1, 0)
    }

    /// Check if `io_uring` with registered buffers is supported (Linux 5.1+).
    #[must_use]
    pub fn supports_io_uring_registered_buffers(&self) -> bool {
        *self >= Self::new(5, 1, 0)
    }

    /// Check if `io_uring` SQPOLL mode is supported (Linux 5.11+).
    ///
    /// SQPOLL uses a dedicated kernel thread to poll the submission queue,
    /// eliminating syscalls under load.
    #[must_use]
    pub fn supports_io_uring_sqpoll(&self) -> bool {
        *self >= Self::new(5, 11, 0)
    }

    /// Check if `io_uring` IOPOLL mode is supported for `NVMe` (Linux 5.19+).
    ///
    /// IOPOLL polls completions directly from the `NVMe` queue without interrupts.
    #[must_use]
    pub fn supports_io_uring_iopoll(&self) -> bool {
        *self >= Self::new(5, 19, 0)
    }

    /// Check if `io_uring` multishot operations are supported (Linux 5.19+).
    #[must_use]
    pub fn supports_io_uring_multishot(&self) -> bool {
        *self >= Self::new(5, 19, 0)
    }

    /// Check if `io_uring` `COOP_TASKRUN` is supported (Linux 5.19+).
    #[must_use]
    pub fn supports_io_uring_coop_taskrun(&self) -> bool {
        *self >= Self::new(5, 19, 0)
    }

    /// Check if `io_uring` `SINGLE_ISSUER` is supported (Linux 6.0+).
    #[must_use]
    pub fn supports_io_uring_single_issuer(&self) -> bool {
        *self >= Self::new(6, 0, 0)
    }

    // ===== XDP/eBPF Feature Checks =====

    /// Check if XDP is supported (Linux 4.8+).
    #[must_use]
    pub fn supports_xdp(&self) -> bool {
        *self >= Self::new(4, 8, 0)
    }

    /// Check if XDP generic mode (SKB) is supported (Linux 4.12+).
    #[must_use]
    pub fn supports_xdp_generic(&self) -> bool {
        *self >= Self::new(4, 12, 0)
    }

    /// Check if XDP native mode is widely supported (Linux 5.3+).
    #[must_use]
    pub fn supports_xdp_native(&self) -> bool {
        *self >= Self::new(5, 3, 0)
    }

    /// Check if XDP redirect to CPU map is supported (Linux 4.15+).
    #[must_use]
    pub fn supports_xdp_cpumap(&self) -> bool {
        *self >= Self::new(4, 15, 0)
    }

    // ===== Other Feature Checks =====

    /// Check if transparent huge pages are available (Linux 2.6.38+).
    #[must_use]
    pub fn supports_thp(&self) -> bool {
        *self >= Self::new(2, 6, 38)
    }

    /// Check if madvise `MADV_HUGEPAGE` is supported (Linux 2.6.38+).
    #[must_use]
    pub fn supports_madv_hugepage(&self) -> bool {
        *self >= Self::new(2, 6, 38)
    }

    /// Check if CPU affinity syscalls are supported (Linux 2.5.8+).
    #[must_use]
    pub fn supports_cpu_affinity(&self) -> bool {
        *self >= Self::new(2, 5, 8)
    }

    /// Check if NUMA memory policy is supported (Linux 2.6.7+).
    #[must_use]
    pub fn supports_numa_policy(&self) -> bool {
        *self >= Self::new(2, 6, 7)
    }

    /// Returns the minimum kernel version for basic `LaminarDB` operation.
    #[must_use]
    pub const fn minimum_recommended() -> Self {
        Self::new(5, 10, 0)
    }

    /// Returns the recommended kernel version for optimal performance.
    #[must_use]
    pub const fn optimal_recommended() -> Self {
        Self::new(5, 19, 0)
    }

    /// Check if this version meets the minimum requirements for `LaminarDB`.
    #[must_use]
    pub fn meets_minimum(&self) -> bool {
        *self >= Self::minimum_recommended()
    }

    /// Check if this version is optimal for `LaminarDB` performance.
    #[must_use]
    pub fn is_optimal(&self) -> bool {
        *self >= Self::optimal_recommended()
    }
}

impl PartialOrd for KernelVersion {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for KernelVersion {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.major.cmp(&other.major) {
            Ordering::Equal => match self.minor.cmp(&other.minor) {
                Ordering::Equal => self.patch.cmp(&other.patch),
                ord => ord,
            },
            ord => ord,
        }
    }
}

impl fmt::Display for KernelVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

impl From<(u32, u32, u32)> for KernelVersion {
    fn from((major, minor, patch): (u32, u32, u32)) -> Self {
        Self::new(major, minor, patch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kernel_version_new() {
        let v = KernelVersion::new(5, 15, 0);
        assert_eq!(v.major, 5);
        assert_eq!(v.minor, 15);
        assert_eq!(v.patch, 0);
    }

    #[test]
    fn test_kernel_version_from_tuple() {
        let v: KernelVersion = (5, 15, 0).into();
        assert_eq!(v, KernelVersion::new(5, 15, 0));
    }

    #[test]
    fn test_kernel_version_display() {
        let v = KernelVersion::new(5, 15, 91);
        assert_eq!(format!("{v}"), "5.15.91");
    }

    #[test]
    fn test_kernel_version_ordering() {
        let v4 = KernelVersion::new(4, 15, 0);
        let v5_10 = KernelVersion::new(5, 10, 0);
        let v5_15 = KernelVersion::new(5, 15, 0);
        let v5_15_1 = KernelVersion::new(5, 15, 1);
        let v6 = KernelVersion::new(6, 0, 0);

        assert!(v4 < v5_10);
        assert!(v5_10 < v5_15);
        assert!(v5_15 < v5_15_1);
        assert!(v5_15_1 < v6);

        assert!(v5_15 == KernelVersion::new(5, 15, 0));
    }

    #[test]
    fn test_parse_version_string() {
        assert_eq!(
            KernelVersion::parse_version_string("5.15.0"),
            Some(KernelVersion::new(5, 15, 0))
        );

        assert_eq!(
            KernelVersion::parse_version_string("5.15.0-91-generic"),
            Some(KernelVersion::new(5, 15, 0))
        );

        assert_eq!(
            KernelVersion::parse_version_string("6.2.0-39-generic"),
            Some(KernelVersion::new(6, 2, 0))
        );

        assert_eq!(
            KernelVersion::parse_version_string("5.4"),
            Some(KernelVersion::new(5, 4, 0))
        );

        assert_eq!(
            KernelVersion::parse_version_string("5"),
            Some(KernelVersion::new(5, 0, 0))
        );
    }

    #[test]
    fn test_parse_proc_version() {
        let content = "Linux version 5.15.0-91-generic (buildd@lcy02-amd64-047) \
                       (gcc (Ubuntu 11.4.0-1ubuntu1~22.04) 11.4.0, GNU ld (GNU Binutils for Ubuntu) 2.38) \
                       #101-Ubuntu SMP Tue Nov 14 13:30:08 UTC 2023";

        let version = KernelVersion::parse_proc_version(content).unwrap();
        assert_eq!(version, KernelVersion::new(5, 15, 0));
    }

    #[test]
    fn test_supports_io_uring() {
        let v4 = KernelVersion::new(4, 19, 0);
        let v5_1 = KernelVersion::new(5, 1, 0);
        let v5_11 = KernelVersion::new(5, 11, 0);
        let v5_19 = KernelVersion::new(5, 19, 0);
        let v6 = KernelVersion::new(6, 0, 0);

        // Basic io_uring
        assert!(!v4.supports_io_uring());
        assert!(v5_1.supports_io_uring());
        assert!(v5_11.supports_io_uring());

        // SQPOLL
        assert!(!v4.supports_io_uring_sqpoll());
        assert!(!v5_1.supports_io_uring_sqpoll());
        assert!(v5_11.supports_io_uring_sqpoll());

        // IOPOLL
        assert!(!v5_11.supports_io_uring_iopoll());
        assert!(v5_19.supports_io_uring_iopoll());

        // SINGLE_ISSUER
        assert!(!v5_19.supports_io_uring_single_issuer());
        assert!(v6.supports_io_uring_single_issuer());
    }

    #[test]
    fn test_supports_xdp() {
        let v4_8 = KernelVersion::new(4, 8, 0);
        let v4_12 = KernelVersion::new(4, 12, 0);
        let v4_15 = KernelVersion::new(4, 15, 0);
        let v5_3 = KernelVersion::new(5, 3, 0);

        assert!(v4_8.supports_xdp());
        assert!(!v4_8.supports_xdp_generic());

        assert!(v4_12.supports_xdp_generic());
        assert!(!v4_12.supports_xdp_cpumap());

        assert!(v4_15.supports_xdp_cpumap());
        assert!(!v4_15.supports_xdp_native());

        assert!(v5_3.supports_xdp_native());
    }

    #[test]
    fn test_minimum_recommended() {
        let min = KernelVersion::minimum_recommended();
        assert_eq!(min, KernelVersion::new(5, 10, 0));

        let v4 = KernelVersion::new(4, 19, 0);
        let v5_15 = KernelVersion::new(5, 15, 0);

        assert!(!v4.meets_minimum());
        assert!(v5_15.meets_minimum());
    }

    #[test]
    fn test_optimal_recommended() {
        let opt = KernelVersion::optimal_recommended();
        assert_eq!(opt, KernelVersion::new(5, 19, 0));

        let v5_15 = KernelVersion::new(5, 15, 0);
        let v5_19 = KernelVersion::new(5, 19, 0);
        let v6 = KernelVersion::new(6, 2, 0);

        assert!(!v5_15.is_optimal());
        assert!(v5_19.is_optimal());
        assert!(v6.is_optimal());
    }

    #[test]
    fn test_detect() {
        // This test always passes - detection may or may not succeed
        // depending on the platform
        let version = KernelVersion::detect();

        // On Linux, we should get a version
        #[cfg(target_os = "linux")]
        {
            assert!(version.is_some(), "Failed to detect Linux kernel version");
            let v = version.unwrap();
            assert!(v.major >= 2, "Kernel version too old: {v}");
        }

        // On macOS, we should get a Darwin version
        #[cfg(target_os = "macos")]
        {
            assert!(version.is_some(), "Failed to detect macOS kernel version");
            let v = version.unwrap();
            assert!(v.major >= 10, "Darwin version too old: {v}");
        }

        // On Windows, we get None
        #[cfg(target_os = "windows")]
        {
            assert!(version.is_none());
        }
    }
}
