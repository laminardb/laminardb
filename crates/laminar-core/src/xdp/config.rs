//! XDP configuration.

use std::path::PathBuf;

use super::XdpAttachMode;

/// Configuration for XDP network optimization.
#[derive(Debug, Clone)]
pub struct XdpConfig {
    /// Enable XDP (requires root or `CAP_NET_ADMIN`).
    pub enabled: bool,

    /// Path to compiled BPF object file.
    pub bpf_object_path: PathBuf,

    /// Network interface to attach to (e.g., "eth0").
    pub interface: String,

    /// UDP port for filtering.
    pub port: u16,

    /// XDP attach mode.
    pub attach_mode: XdpAttachMode,

    /// Queue size per CPU for XDP redirect.
    pub cpu_queue_size: u32,

    /// Enable XDP statistics collection.
    pub collect_stats: bool,

    /// Fallback gracefully if XDP unavailable.
    pub fallback_on_error: bool,
}

impl Default for XdpConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            bpf_object_path: PathBuf::from("/usr/share/laminardb/laminar_xdp.o"),
            interface: "eth0".to_string(),
            port: 9999,
            attach_mode: XdpAttachMode::Auto,
            cpu_queue_size: 2048,
            collect_stats: true,
            fallback_on_error: true,
        }
    }
}

impl XdpConfig {
    /// Creates a new configuration builder.
    #[must_use]
    pub fn builder() -> XdpConfigBuilder {
        XdpConfigBuilder::default()
    }

    /// Validates the configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if the configuration is invalid.
    pub fn validate(&self) -> Result<(), super::XdpError> {
        if self.enabled {
            if self.interface.is_empty() {
                return Err(super::XdpError::InvalidConfig(
                    "interface cannot be empty".to_string(),
                ));
            }
            if self.port == 0 {
                return Err(super::XdpError::InvalidConfig(
                    "port must be non-zero".to_string(),
                ));
            }
            if self.cpu_queue_size == 0 {
                return Err(super::XdpError::InvalidConfig(
                    "cpu_queue_size must be non-zero".to_string(),
                ));
            }
        }
        Ok(())
    }
}

/// Builder for [`XdpConfig`].
#[derive(Debug, Default)]
pub struct XdpConfigBuilder {
    enabled: Option<bool>,
    bpf_object_path: Option<PathBuf>,
    interface: Option<String>,
    port: Option<u16>,
    attach_mode: Option<XdpAttachMode>,
    cpu_queue_size: Option<u32>,
    collect_stats: Option<bool>,
    fallback_on_error: Option<bool>,
}

impl XdpConfigBuilder {
    /// Enables or disables XDP.
    #[must_use]
    pub fn enabled(mut self, enabled: bool) -> Self {
        self.enabled = Some(enabled);
        self
    }

    /// Sets the path to the BPF object file.
    #[must_use]
    pub fn bpf_object_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.bpf_object_path = Some(path.into());
        self
    }

    /// Sets the network interface.
    #[must_use]
    pub fn interface(mut self, interface: impl Into<String>) -> Self {
        self.interface = Some(interface.into());
        self
    }

    /// Sets the UDP port for filtering.
    #[must_use]
    pub fn port(mut self, port: u16) -> Self {
        self.port = Some(port);
        self
    }

    /// Sets the XDP attach mode.
    #[must_use]
    pub fn attach_mode(mut self, mode: XdpAttachMode) -> Self {
        self.attach_mode = Some(mode);
        self
    }

    /// Sets the CPU queue size for XDP redirect.
    #[must_use]
    pub fn cpu_queue_size(mut self, size: u32) -> Self {
        self.cpu_queue_size = Some(size);
        self
    }

    /// Enables or disables statistics collection.
    #[must_use]
    pub fn collect_stats(mut self, enabled: bool) -> Self {
        self.collect_stats = Some(enabled);
        self
    }

    /// Enables or disables fallback on error.
    #[must_use]
    pub fn fallback_on_error(mut self, enabled: bool) -> Self {
        self.fallback_on_error = Some(enabled);
        self
    }

    /// Builds the configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if the configuration is invalid.
    pub fn build(self) -> Result<XdpConfig, super::XdpError> {
        let config = XdpConfig {
            enabled: self.enabled.unwrap_or(false),
            bpf_object_path: self.bpf_object_path.unwrap_or_else(|| {
                PathBuf::from("/usr/share/laminardb/laminar_xdp.o")
            }),
            interface: self.interface.unwrap_or_else(|| "eth0".to_string()),
            port: self.port.unwrap_or(9999),
            attach_mode: self.attach_mode.unwrap_or_default(),
            cpu_queue_size: self.cpu_queue_size.unwrap_or(2048),
            collect_stats: self.collect_stats.unwrap_or(true),
            fallback_on_error: self.fallback_on_error.unwrap_or(true),
        };
        config.validate()?;
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = XdpConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.port, 9999);
        assert_eq!(config.interface, "eth0");
        assert_eq!(config.attach_mode, XdpAttachMode::Auto);
    }

    #[test]
    fn test_builder() {
        let config = XdpConfig::builder()
            .enabled(true)
            .interface("lo")
            .port(8080)
            .cpu_queue_size(4096)
            .build()
            .unwrap();

        assert!(config.enabled);
        assert_eq!(config.interface, "lo");
        assert_eq!(config.port, 8080);
        assert_eq!(config.cpu_queue_size, 4096);
    }

    #[test]
    fn test_validation_empty_interface() {
        let result = XdpConfig::builder()
            .enabled(true)
            .interface("")
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn test_validation_zero_port() {
        let result = XdpConfig::builder()
            .enabled(true)
            .port(0)
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn test_disabled_skips_validation() {
        // Empty interface is OK when disabled
        let config = XdpConfig::builder()
            .enabled(false)
            .interface("")
            .build()
            .unwrap();
        assert!(!config.enabled);
    }
}
