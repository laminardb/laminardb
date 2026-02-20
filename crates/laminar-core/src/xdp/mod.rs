//! # XDP/eBPF Network Optimization
//!
//! Implements XDP (eXpress Data Path) integration for wire-speed packet filtering
//! and CPU steering. XDP processes packets at the NIC driver level before kernel
//! networking stack allocation.
//!
//! ## Performance
//!
//! | Processing Level | Packets/sec/core | Latency |
//! |------------------|------------------|---------|
//! | Application (userspace) | ~1M | ~50μs |
//! | Kernel network stack | ~5M | ~10μs |
//! | **XDP (driver level)** | **26M** | **<1μs** |
//!
//! ## Use Cases
//!
//! - Invalid packet filtering (`XDP_DROP`)
//! - Protocol validation (`XDP_DROP`/`XDP_PASS`)
//! - Core routing by partition key (`XDP_REDIRECT`)
//! - Denial-of-service mitigation (wire-speed filtering)
//!
//! ## Platform Support
//!
//! - Linux 4.15+: XDP generic (SKB mode)
//! - Linux 5.3+: XDP native (driver mode)
//! - Windows/macOS: Not supported (graceful fallback)
//!
//! ## Example
//!
//! ```rust,ignore
//! use laminar_core::xdp::{XdpConfig, XdpLoader, XdpStats};
//!
//! // Configure XDP (Linux only)
//! let config = XdpConfig {
//!     enabled: true,
//!     bpf_object_path: PathBuf::from("/usr/share/laminardb/laminar_xdp.o"),
//!     interface: "eth0".to_string(),
//!     port: 9999,
//! };
//!
//! // Load and attach (returns fallback stub on non-Linux)
//! let loader = XdpLoader::load_and_attach(&config, 4)?;
//!
//! // Get statistics
//! let stats = loader.stats()?;
//! println!("Redirected: {}", stats.redirected);
//! ```

mod config;
mod error;
mod header;
#[cfg(target_os = "linux")]
mod loader_linux;
#[cfg(not(target_os = "linux"))]
mod loader_stub;
mod stats;

pub use config::XdpConfig;
pub use error::XdpError;
pub use header::LaminarHeader;
pub use stats::XdpStats;

#[cfg(target_os = "linux")]
pub use loader_linux::XdpLoader;

#[cfg(not(target_os = "linux"))]
pub use loader_stub::XdpLoader;

/// XDP action types (mirrors kernel definitions).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum XdpAction {
    /// Abort processing (kernel error path)
    Aborted = 0,
    /// Drop the packet
    Drop = 1,
    /// Pass to kernel network stack
    Pass = 2,
    /// Send back out the same interface
    Tx = 3,
    /// Redirect to another interface/CPU
    Redirect = 4,
}

impl From<u32> for XdpAction {
    fn from(value: u32) -> Self {
        match value {
            1 => XdpAction::Drop,
            2 => XdpAction::Pass,
            3 => XdpAction::Tx,
            4 => XdpAction::Redirect,
            // 0 and any unknown value map to Aborted
            _ => XdpAction::Aborted,
        }
    }
}

/// XDP attach mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum XdpAttachMode {
    /// Auto-detect best mode
    #[default]
    Auto,
    /// Generic mode (SKB, works on all interfaces)
    Generic,
    /// Native mode (driver support required, best performance)
    Native,
    /// Offload mode (NIC hardware, requires specific NICs)
    Offload,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_xdp_action_from_u32() {
        assert_eq!(XdpAction::from(0), XdpAction::Aborted); // 0 maps to Aborted
        assert_eq!(XdpAction::from(1), XdpAction::Drop);
        assert_eq!(XdpAction::from(2), XdpAction::Pass);
        assert_eq!(XdpAction::from(3), XdpAction::Tx);
        assert_eq!(XdpAction::from(4), XdpAction::Redirect);
        assert_eq!(XdpAction::from(99), XdpAction::Aborted); // Unknown maps to Aborted
    }

    #[test]
    fn test_attach_mode_default() {
        assert_eq!(XdpAttachMode::default(), XdpAttachMode::Auto);
    }
}
