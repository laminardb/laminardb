//! # CPU Feature Detection
//!
//! Detects CPU capabilities including SIMD instructions and cache configuration.
//!
//! ## Usage
//!
//! ```rust,ignore
//! use laminar_core::detect::{CpuFeatures, cache_line_size};
//!
//! let features = CpuFeatures::detect();
//! if features.avx2 {
//!     println!("AVX2 is available!");
//! }
//!
//! let cache_line = cache_line_size();
//! println!("Cache line size: {} bytes", cache_line);
//! ```

/// CPU feature flags.
///
/// Detected using CPUID on x86/x86\_64 or equivalent on other architectures.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct CpuFeatures {
    /// SSE4.2 support (CRC32, string compare)
    pub sse4_2: bool,
    /// AVX2 support (256-bit SIMD)
    pub avx2: bool,
    /// AVX-512 Foundation support (512-bit SIMD)
    pub avx512f: bool,
    /// AVX-512 VBMI2 support (byte/word manipulation)
    pub avx512vbmi2: bool,
    /// POPCNT support (population count)
    pub popcnt: bool,
    /// AES-NI support (hardware AES)
    pub aes_ni: bool,
    /// CLMUL support (carryless multiply, used for CRC)
    pub clmul: bool,
    /// BMI1 support (bit manipulation)
    pub bmi1: bool,
    /// BMI2 support (bit manipulation)
    pub bmi2: bool,
    /// LZCNT support (leading zero count)
    pub lzcnt: bool,
    /// NEON support (ARM SIMD)
    pub neon: bool,
    /// CRC32 hardware support (ARM)
    pub arm_crc32: bool,
}

impl CpuFeatures {
    /// Detect CPU features for the current processor.
    #[must_use]
    pub fn detect() -> Self {
        let mut features = Self::default();

        #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
        {
            features.detect_x86();
        }

        #[cfg(target_arch = "aarch64")]
        {
            features.detect_aarch64();
        }

        features
    }

    /// Detect features on x86/x86\_64 using CPUID.
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    fn detect_x86(&mut self) {
        // Use std::arch::is_x86_feature_detected! macro for reliable detection
        self.sse4_2 = std::arch::is_x86_feature_detected!("sse4.2");
        self.avx2 = std::arch::is_x86_feature_detected!("avx2");
        self.avx512f = std::arch::is_x86_feature_detected!("avx512f");
        self.avx512vbmi2 = std::arch::is_x86_feature_detected!("avx512vbmi2");
        self.popcnt = std::arch::is_x86_feature_detected!("popcnt");
        self.aes_ni = std::arch::is_x86_feature_detected!("aes");
        self.clmul = std::arch::is_x86_feature_detected!("pclmulqdq");
        self.bmi1 = std::arch::is_x86_feature_detected!("bmi1");
        self.bmi2 = std::arch::is_x86_feature_detected!("bmi2");
        self.lzcnt = std::arch::is_x86_feature_detected!("lzcnt");
    }

    /// Detect features on AArch64 (ARM64).
    #[cfg(target_arch = "aarch64")]
    fn detect_aarch64(&mut self) {
        // NEON is mandatory on AArch64
        self.neon = true;
        // CRC32 is common but not universal
        self.arm_crc32 = std::arch::is_aarch64_feature_detected!("crc");
    }

    /// Check if SIMD acceleration is available.
    #[must_use]
    pub fn has_simd(&self) -> bool {
        self.avx2 || self.avx512f || self.neon
    }

    /// Check if hardware CRC32 is available.
    #[must_use]
    pub fn has_hw_crc32(&self) -> bool {
        self.sse4_2 || self.arm_crc32
    }

    /// Check if hardware AES is available.
    #[must_use]
    pub fn has_hw_aes(&self) -> bool {
        self.aes_ni
    }

    /// Get a summary of SIMD capabilities.
    #[must_use]
    pub fn simd_level(&self) -> SimdLevel {
        if self.avx512f {
            SimdLevel::Avx512
        } else if self.avx2 {
            SimdLevel::Avx2
        } else if self.sse4_2 {
            SimdLevel::Sse42
        } else if self.neon {
            SimdLevel::Neon
        } else {
            SimdLevel::None
        }
    }

    /// Get a summary string.
    #[must_use]
    pub fn summary(&self) -> String {
        let mut features = Vec::new();

        if self.avx512f {
            features.push("AVX-512");
        } else if self.avx2 {
            features.push("AVX2");
        } else if self.sse4_2 {
            features.push("SSE4.2");
        }

        if self.neon {
            features.push("NEON");
        }

        if self.aes_ni {
            features.push("AES-NI");
        }

        if self.popcnt {
            features.push("POPCNT");
        }

        if features.is_empty() {
            "None".to_string()
        } else {
            features.join(", ")
        }
    }
}

/// SIMD capability level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum SimdLevel {
    /// No SIMD support
    None,
    /// ARM NEON (128-bit)
    Neon,
    /// x86 SSE4.2 (128-bit)
    Sse42,
    /// x86 AVX2 (256-bit)
    Avx2,
    /// x86 AVX-512 (512-bit)
    Avx512,
}

impl std::fmt::Display for SimdLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SimdLevel::None => write!(f, "None"),
            SimdLevel::Neon => write!(f, "NEON"),
            SimdLevel::Sse42 => write!(f, "SSE4.2"),
            SimdLevel::Avx2 => write!(f, "AVX2"),
            SimdLevel::Avx512 => write!(f, "AVX-512"),
        }
    }
}

/// Detect the cache line size.
///
/// Returns 64 bytes as the default, which is correct for most modern processors.
#[must_use]
pub fn cache_line_size() -> usize {
    detect_cache_line_size().unwrap_or(64)
}

/// Attempt to detect the cache line size from system information.
fn detect_cache_line_size() -> Option<usize> {
    // Try Linux sysfs first
    #[cfg(target_os = "linux")]
    {
        if let Some(size) = detect_cache_line_sysfs() {
            return Some(size);
        }
    }

    // Try CPUID on x86
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    {
        if let Some(size) = detect_cache_line_cpuid() {
            return Some(size);
        }
    }

    None
}

/// Detect cache line size from Linux sysfs.
#[cfg(target_os = "linux")]
fn detect_cache_line_sysfs() -> Option<usize> {
    // Try L1 data cache first
    let paths = [
        "/sys/devices/system/cpu/cpu0/cache/index0/coherency_line_size",
        "/sys/devices/system/cpu/cpu0/cache/index1/coherency_line_size",
        "/sys/devices/system/cpu/cpu0/cache/index2/coherency_line_size",
    ];

    for path in &paths {
        if let Ok(content) = std::fs::read_to_string(path) {
            if let Ok(size) = content.trim().parse::<usize>() {
                return Some(size);
            }
        }
    }

    None
}

/// Detect cache line size using CPUID.
#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
fn detect_cache_line_cpuid() -> Option<usize> {
    // CPUID leaf 0x80000006 (AMD-style cache info)
    // On Intel, this also typically works

    #[cfg(target_arch = "x86")]
    use std::arch::x86::__cpuid;
    #[cfg(target_arch = "x86_64")]
    use std::arch::x86_64::__cpuid;

    // Check if extended CPUID is supported
    // SAFETY: CPUID is a safe instruction on x86
    let max_extended = unsafe { __cpuid(0x8000_0000) }.eax;

    if max_extended >= 0x8000_0006 {
        // SAFETY: CPUID is a safe instruction on x86
        let result = unsafe { __cpuid(0x8000_0006) };
        // ECX bits 0-7 contain L2 cache line size
        let line_size = (result.ecx & 0xFF) as usize;
        if line_size > 0 {
            return Some(line_size);
        }
    }

    None
}

/// Get the number of logical CPUs.
#[must_use]
pub fn logical_cpu_count() -> usize {
    num_cpus::get()
}

/// Get the number of physical CPU cores.
#[must_use]
pub fn physical_cpu_count() -> usize {
    num_cpus::get_physical()
}

/// Check if SMT (Hyper-Threading) is enabled.
#[must_use]
pub fn is_smt_enabled() -> bool {
    logical_cpu_count() > physical_cpu_count()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cpu_features_detect() {
        let features = CpuFeatures::detect();

        // At minimum, we should have some features on x86
        #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
        {
            // SSE4.2 and POPCNT are available on any CPU from the last 15+ years
            // This test should pass on any reasonably modern system
            assert!(
                features.sse4_2 || features.popcnt || features.avx2,
                "Expected at least one common x86 feature to be available"
            );
        }

        // On ARM64, NEON is mandatory
        #[cfg(target_arch = "aarch64")]
        {
            assert!(features.neon);
        }
    }

    #[test]
    fn test_cpu_features_simd_level() {
        let features = CpuFeatures::detect();
        let level = features.simd_level();

        // simd_level should return a valid level
        match level {
            SimdLevel::None
            | SimdLevel::Neon
            | SimdLevel::Sse42
            | SimdLevel::Avx2
            | SimdLevel::Avx512 => {}
        }

        // Display should work
        let level_str = format!("{level}");
        assert!(!level_str.is_empty());
    }

    #[test]
    fn test_cpu_features_summary() {
        let features = CpuFeatures::detect();
        let summary = features.summary();

        // Summary should be non-empty (at least "None")
        assert!(!summary.is_empty());
    }

    #[test]
    fn test_cpu_features_default() {
        let features = CpuFeatures::default();
        assert!(!features.sse4_2);
        assert!(!features.avx2);
        assert!(!features.avx512f);
        assert!(!features.neon);
    }

    #[test]
    fn test_cache_line_size() {
        let size = cache_line_size();
        // Cache line size should be a reasonable power of 2
        assert!(size >= 32);
        assert!(size <= 256);
        assert!(size.is_power_of_two());
    }

    #[test]
    fn test_logical_cpu_count() {
        let count = logical_cpu_count();
        assert!(count >= 1);
    }

    #[test]
    fn test_physical_cpu_count() {
        let count = physical_cpu_count();
        assert!(count >= 1);
        // Physical count should not exceed logical
        assert!(count <= logical_cpu_count());
    }

    #[test]
    fn test_is_smt_enabled() {
        // Just ensure this doesn't panic
        let _ = is_smt_enabled();
    }

    #[test]
    fn test_simd_level_ordering() {
        assert!(SimdLevel::None < SimdLevel::Neon);
        assert!(SimdLevel::Neon < SimdLevel::Sse42);
        assert!(SimdLevel::Sse42 < SimdLevel::Avx2);
        assert!(SimdLevel::Avx2 < SimdLevel::Avx512);
    }

    #[test]
    fn test_has_simd() {
        let mut features = CpuFeatures::default();
        assert!(!features.has_simd());

        features.avx2 = true;
        assert!(features.has_simd());

        features = CpuFeatures::default();
        features.neon = true;
        assert!(features.has_simd());
    }

    #[test]
    fn test_has_hw_crc32() {
        let mut features = CpuFeatures::default();
        assert!(!features.has_hw_crc32());

        features.sse4_2 = true;
        assert!(features.has_hw_crc32());

        features = CpuFeatures::default();
        features.arm_crc32 = true;
        assert!(features.has_hw_crc32());
    }
}
