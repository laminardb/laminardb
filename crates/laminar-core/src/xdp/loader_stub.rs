//! Stub XDP loader for non-Linux platforms.
//!
//! This provides a graceful fallback when XDP is not available.

use super::{XdpConfig, XdpError, XdpStats};

/// XDP program loader (stub for non-Linux platforms).
///
/// This stub implementation always fails to load but provides a consistent
/// API across platforms. Use `fallback_on_error` in config to gracefully
/// handle unavailability.
#[derive(Debug)]
pub struct XdpLoader {
    /// Local stats (not from XDP)
    stats: super::stats::AtomicXdpStats,
}

impl XdpLoader {
    /// Attempts to load and attach XDP program.
    ///
    /// On non-Linux platforms, this always returns `NotAvailable` error.
    ///
    /// # Errors
    ///
    /// Always returns `XdpError::NotAvailable` on non-Linux.
    pub fn load_and_attach(config: &XdpConfig, _num_cores: usize) -> Result<Self, XdpError> {
        if config.fallback_on_error {
            tracing::warn!("XDP not available on this platform, falling back to standard sockets");
            Ok(Self {
                stats: super::stats::AtomicXdpStats::new(),
            })
        } else {
            Err(XdpError::NotAvailable)
        }
    }

    /// Returns true if XDP is actually active (always false for stub).
    #[must_use]
    pub const fn is_active(&self) -> bool {
        false
    }

    /// Returns XDP statistics.
    ///
    /// On non-Linux, returns empty stats.
    #[must_use]
    pub fn stats(&self) -> XdpStats {
        self.stats.snapshot()
    }

    /// Updates CPU steering for a partition (no-op on non-Linux).
    ///
    /// # Errors
    ///
    /// Never returns an error on non-Linux (stub implementation).
    pub fn update_cpu_steering(&self, _partition: u32, _cpu: u32) -> Result<(), XdpError> {
        Ok(())
    }

    /// Detaches the XDP program (no-op on non-Linux).
    ///
    /// # Errors
    ///
    /// Never returns an error on non-Linux (stub implementation).
    pub fn detach(&self) -> Result<(), XdpError> {
        Ok(())
    }

    /// Returns the interface name (empty for stub).
    #[must_use]
    pub fn interface(&self) -> &'static str {
        ""
    }

    /// Records a packet being processed (for tracking without XDP).
    pub fn record_packet(&self, was_valid: bool, bytes: u64) {
        if was_valid {
            self.stats.inc_passed();
        } else {
            self.stats.inc_invalid();
        }
        self.stats.add_bytes(bytes);
    }
}

impl Drop for XdpLoader {
    fn drop(&mut self) {
        // No cleanup needed for stub
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stub_with_fallback() {
        let config = XdpConfig {
            fallback_on_error: true,
            ..Default::default()
        };

        let loader = XdpLoader::load_and_attach(&config, 4).unwrap();
        assert!(!loader.is_active());
        assert_eq!(loader.stats().total_packets(), 0);
    }

    #[test]
    fn test_stub_without_fallback() {
        let config = XdpConfig {
            enabled: true,
            fallback_on_error: false,
            ..Default::default()
        };

        let result = XdpLoader::load_and_attach(&config, 4);
        assert!(result.is_err());
        assert!(result.unwrap_err().is_not_available());
    }

    #[test]
    fn test_stub_record_packet() {
        let config = XdpConfig {
            fallback_on_error: true,
            ..Default::default()
        };

        let loader = XdpLoader::load_and_attach(&config, 4).unwrap();
        loader.record_packet(true, 100);
        loader.record_packet(false, 50);

        let stats = loader.stats();
        assert_eq!(stats.passed, 1);
        assert_eq!(stats.invalid, 1);
        assert_eq!(stats.bytes_processed, 150);
    }

    #[test]
    fn test_stub_operations() {
        let config = XdpConfig {
            fallback_on_error: true,
            ..Default::default()
        };

        let loader = XdpLoader::load_and_attach(&config, 4).unwrap();

        // All operations should succeed as no-ops
        loader.update_cpu_steering(0, 0).unwrap();
        loader.detach().unwrap();
        assert_eq!(loader.interface(), "");
    }
}
