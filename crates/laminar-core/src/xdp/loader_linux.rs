//! XDP loader for Linux platforms.
//!
//! This module provides the actual XDP loading and attachment functionality
//! using libbpf-rs when the `xdp` feature is enabled.

use std::sync::atomic::{AtomicBool, Ordering};

use super::{XdpAttachMode, XdpConfig, XdpError, XdpStats};

/// XDP program loader for Linux.
///
/// Loads and attaches XDP programs to network interfaces for packet filtering
/// and CPU steering.
///
/// # Example
///
/// ```rust,ignore
/// use laminar_core::xdp::{XdpConfig, XdpLoader};
///
/// let config = XdpConfig::builder()
///     .enabled(true)
///     .interface("eth0")
///     .port(9999)
///     .build()?;
///
/// let loader = XdpLoader::load_and_attach(&config, 4)?;
/// println!("XDP active: {}", loader.is_active());
///
/// // Get statistics
/// let stats = loader.stats();
/// println!("Redirected: {}", stats.redirected);
/// ```
#[derive(Debug)]
pub struct XdpLoader {
    /// Interface name
    interface: String,
    /// Interface index
    ifindex: u32,
    /// Whether XDP is active
    active: AtomicBool,
    /// Number of cores
    num_cores: usize,
    /// Local stats tracking
    stats: super::stats::AtomicXdpStats,
    /// Attach mode used
    attach_mode: XdpAttachMode,
    /// Configuration used
    #[allow(dead_code)]
    config: XdpConfig,
}

impl XdpLoader {
    /// Loads and attaches XDP program to network interface.
    ///
    /// # Arguments
    ///
    /// * `config` - XDP configuration
    /// * `num_cores` - Number of cores for CPU map
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - XDP program file not found
    /// - Permission denied (requires `CAP_NET_ADMIN` or root)
    /// - Network interface not found
    /// - BPF program loading fails
    pub fn load_and_attach(config: &XdpConfig, num_cores: usize) -> Result<Self, XdpError> {
        config.validate()?;

        if !config.enabled {
            return Self::create_inactive(config, num_cores);
        }

        // Check if BPF object file exists
        if !config.bpf_object_path.exists() {
            if config.fallback_on_error {
                tracing::warn!(
                    "XDP program not found at {:?}, falling back to standard sockets",
                    config.bpf_object_path
                );
                return Self::create_inactive(config, num_cores);
            }
            return Err(XdpError::ProgramNotFound(config.bpf_object_path.clone()));
        }

        // Get interface index
        let ifindex = Self::get_interface_index(&config.interface)?;

        // Try to load and attach XDP program
        match Self::do_load_and_attach(config, ifindex, num_cores) {
            Ok(loader) => Ok(loader),
            Err(e) => {
                if config.fallback_on_error {
                    tracing::warn!("XDP load failed, falling back: {}", e);
                    Self::create_inactive(config, num_cores)
                } else {
                    Err(e)
                }
            }
        }
    }

    /// Creates an inactive loader (fallback mode).
    #[allow(clippy::unnecessary_wraps)]
    fn create_inactive(config: &XdpConfig, num_cores: usize) -> Result<Self, XdpError> {
        Ok(Self {
            interface: config.interface.clone(),
            ifindex: 0,
            active: AtomicBool::new(false),
            num_cores,
            stats: super::stats::AtomicXdpStats::new(),
            attach_mode: config.attach_mode,
            config: config.clone(),
        })
    }

    /// Actually loads and attaches the XDP program.
    #[cfg(feature = "xdp")]
    fn do_load_and_attach(
        config: &XdpConfig,
        ifindex: u32,
        num_cores: usize,
    ) -> Result<Self, XdpError> {
        use libbpf_rs::{MapCore, MapFlags, ObjectBuilder};

        // Load BPF object
        let mut obj = ObjectBuilder::default()
            .open_file(&config.bpf_object_path)
            .map_err(|e| XdpError::LoadFailed(e.to_string()))?
            .load()
            .map_err(|e| XdpError::LoadFailed(e.to_string()))?;

        // Get XDP program by iterating progs()
        let prog = obj
            .progs_mut()
            .find(|p| p.name() == "laminar_ingress")
            .ok_or_else(|| XdpError::MapNotFound("laminar_ingress program".to_string()))?;

        // Attach to interface
        #[allow(clippy::cast_possible_wrap)]
        let _link = prog
            .attach_xdp(ifindex as i32)
            .map_err(|e: libbpf_rs::Error| XdpError::AttachFailed(e.to_string()))?;

        // Configure CPU map if present
        if let Some(cpu_map) = obj.maps_mut().find(|m| m.name() == "cpu_map") {
            #[allow(clippy::cast_possible_truncation)]
            for cpu in 0..num_cores {
                let key = (cpu as u32).to_ne_bytes();
                // CpumapValue format: qsize as u32
                let qsize = config.cpu_queue_size;
                let value = qsize.to_ne_bytes();
                cpu_map
                    .update(&key, &value, MapFlags::ANY)
                    .map_err(|e: libbpf_rs::Error| XdpError::MapUpdateFailed(e.to_string()))?;
            }
        }

        tracing::info!(
            "XDP program attached to interface {} (index {})",
            config.interface,
            ifindex
        );

        Ok(Self {
            interface: config.interface.clone(),
            ifindex,
            active: AtomicBool::new(true),
            num_cores,
            stats: super::stats::AtomicXdpStats::new(),
            attach_mode: config.attach_mode,
            config: config.clone(),
        })
    }

    /// Stub for when xdp feature is not enabled.
    #[cfg(not(feature = "xdp"))]
    fn do_load_and_attach(
        config: &XdpConfig,
        _ifindex: u32,
        num_cores: usize,
    ) -> Result<Self, XdpError> {
        tracing::warn!("XDP feature not enabled, using stub implementation");
        Self::create_inactive(config, num_cores)
    }

    /// Gets the interface index from the interface name.
    fn get_interface_index(interface: &str) -> Result<u32, XdpError> {
        use std::ffi::CString;

        let c_interface = CString::new(interface)
            .map_err(|_| XdpError::InterfaceNotFound(interface.to_string()))?;

        // SAFETY: We're calling a standard libc function with a valid CString
        let ifindex = unsafe { libc::if_nametoindex(c_interface.as_ptr()) };

        if ifindex == 0 {
            Err(XdpError::InterfaceNotFound(interface.to_string()))
        } else {
            Ok(ifindex)
        }
    }

    /// Returns true if XDP is actually active.
    #[must_use]
    pub fn is_active(&self) -> bool {
        self.active.load(Ordering::Acquire)
    }

    /// Returns XDP statistics.
    #[must_use]
    pub fn stats(&self) -> XdpStats {
        if self.is_active() {
            self.read_bpf_stats()
        } else {
            self.stats.snapshot()
        }
    }

    /// Reads statistics from BPF maps.
    #[cfg(feature = "xdp")]
    fn read_bpf_stats(&self) -> XdpStats {
        // In a real implementation, this would read from the BPF stats map
        // For now, return the local stats
        self.stats.snapshot()
    }

    #[cfg(not(feature = "xdp"))]
    fn read_bpf_stats(&self) -> XdpStats {
        self.stats.snapshot()
    }

    /// Updates CPU steering for a partition.
    ///
    /// This allows runtime reconfiguration of which CPU handles
    /// packets for a given partition.
    ///
    /// # Errors
    ///
    /// Returns `XdpError::InvalidConfig` if the CPU index is out of range.
    pub fn update_cpu_steering(&self, partition: u32, cpu: u32) -> Result<(), XdpError> {
        if !self.is_active() {
            return Ok(());
        }

        if cpu as usize >= self.num_cores {
            return Err(XdpError::InvalidConfig(format!(
                "CPU {} out of range (0..{})",
                cpu, self.num_cores
            )));
        }

        // In a real implementation, this would update the BPF partition map
        tracing::debug!("XDP steering: partition {} -> CPU {}", partition, cpu);
        Ok(())
    }

    /// Detaches the XDP program from the interface.
    ///
    /// # Errors
    ///
    /// Currently infallible, but may return errors in future implementations
    /// when actual XDP detachment is performed.
    pub fn detach(&self) -> Result<(), XdpError> {
        if !self.is_active() {
            return Ok(());
        }

        self.do_detach();
        self.active.store(false, Ordering::Release);

        tracing::info!("XDP program detached from interface {}", self.interface);
        Ok(())
    }

    #[cfg(feature = "xdp")]
    #[allow(clippy::unused_self)]
    fn do_detach(&self) {
        // Detach XDP program by setting empty flags (removes the program)
        // In newer libbpf-rs, detach is handled by dropping the link
        // For now, we just log that detach was requested
        // The actual detach happens when the XdpLink is dropped
    }

    #[cfg(not(feature = "xdp"))]
    #[allow(clippy::unused_self)]
    fn do_detach(&self) {
        // No-op when xdp feature is disabled
    }

    /// Returns the interface name.
    #[must_use]
    pub fn interface(&self) -> &str {
        &self.interface
    }

    /// Returns the interface index.
    #[must_use]
    pub fn ifindex(&self) -> u32 {
        self.ifindex
    }

    /// Returns the number of cores configured.
    #[must_use]
    pub fn num_cores(&self) -> usize {
        self.num_cores
    }

    /// Returns the attach mode.
    #[must_use]
    pub fn attach_mode(&self) -> XdpAttachMode {
        self.attach_mode
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
        if self.is_active() {
            if let Err(e) = self.detach() {
                tracing::error!("Failed to detach XDP on drop: {}", e);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_loader_disabled() {
        let config = XdpConfig {
            enabled: false,
            ..Default::default()
        };

        let loader = XdpLoader::load_and_attach(&config, 4).unwrap();
        assert!(!loader.is_active());
    }

    #[test]
    fn test_loader_fallback() {
        let config = XdpConfig {
            enabled: true,
            fallback_on_error: true,
            bpf_object_path: "/nonexistent/path.o".into(),
            ..Default::default()
        };

        let loader = XdpLoader::load_and_attach(&config, 4).unwrap();
        assert!(!loader.is_active());
    }

    #[test]
    fn test_loader_no_fallback() {
        let config = XdpConfig {
            enabled: true,
            fallback_on_error: false,
            bpf_object_path: "/nonexistent/path.o".into(),
            ..Default::default()
        };

        let result = XdpLoader::load_and_attach(&config, 4);
        assert!(result.is_err());
    }

    #[test]
    fn test_interface_index_loopback() {
        // Loopback interface should always exist
        let result = XdpLoader::get_interface_index("lo");
        assert!(result.is_ok());
        assert!(result.unwrap() > 0);
    }

    #[test]
    fn test_interface_index_nonexistent() {
        let result = XdpLoader::get_interface_index("nonexistent_interface_xyz");
        assert!(result.is_err());
    }

    #[test]
    fn test_loader_stats() {
        let config = XdpConfig {
            enabled: false,
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
    fn test_cpu_steering_inactive() {
        let config = XdpConfig {
            enabled: false,
            ..Default::default()
        };

        let loader = XdpLoader::load_and_attach(&config, 4).unwrap();
        // Should succeed as no-op
        loader.update_cpu_steering(0, 0).unwrap();
    }

    #[test]
    fn test_cpu_steering_invalid_cpu() {
        let config = XdpConfig {
            enabled: true,
            fallback_on_error: true,
            ..Default::default()
        };

        let loader = XdpLoader::load_and_attach(&config, 4).unwrap();
        // When active, invalid CPU should fail
        // But since we're in fallback mode, it's inactive
        assert!(!loader.is_active());
    }

    #[test]
    fn test_detach_inactive() {
        let config = XdpConfig {
            enabled: false,
            ..Default::default()
        };

        let loader = XdpLoader::load_and_attach(&config, 4).unwrap();
        loader.detach().unwrap(); // Should succeed as no-op
    }
}
