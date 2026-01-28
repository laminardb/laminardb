//! XDP error types.

use std::path::PathBuf;

/// Errors that can occur during XDP operations.
#[derive(Debug, thiserror::Error)]
pub enum XdpError {
    /// XDP is not available on this platform.
    #[error("XDP not available on this platform")]
    NotAvailable,

    /// XDP program file not found.
    #[error("XDP program not found: {0}")]
    ProgramNotFound(PathBuf),

    /// BPF map not found in XDP program.
    #[error("BPF map not found: {0}")]
    MapNotFound(String),

    /// Network interface not found.
    #[error("Network interface not found: {0}")]
    InterfaceNotFound(String),

    /// Failed to load BPF program.
    #[error("Failed to load BPF program: {0}")]
    LoadFailed(String),

    /// Failed to attach XDP program.
    #[error("Failed to attach XDP program: {0}")]
    AttachFailed(String),

    /// Failed to detach XDP program.
    #[error("Failed to detach XDP program: {0}")]
    DetachFailed(String),

    /// Failed to update BPF map.
    #[error("Failed to update BPF map: {0}")]
    MapUpdateFailed(String),

    /// Failed to read BPF map.
    #[error("Failed to read BPF map: {0}")]
    MapReadFailed(String),

    /// Permission denied (requires `CAP_NET_ADMIN` or root).
    #[error("Permission denied: XDP requires CAP_NET_ADMIN or root")]
    PermissionDenied,

    /// Invalid configuration.
    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),

    /// I/O error.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

impl XdpError {
    /// Returns true if this is a permission error.
    #[must_use]
    pub fn is_permission_error(&self) -> bool {
        matches!(self, XdpError::PermissionDenied)
    }

    /// Returns true if XDP is not available.
    #[must_use]
    pub fn is_not_available(&self) -> bool {
        matches!(self, XdpError::NotAvailable)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = XdpError::NotAvailable;
        assert_eq!(err.to_string(), "XDP not available on this platform");

        let err = XdpError::InterfaceNotFound("eth0".to_string());
        assert_eq!(err.to_string(), "Network interface not found: eth0");
    }

    #[test]
    fn test_is_permission_error() {
        assert!(XdpError::PermissionDenied.is_permission_error());
        assert!(!XdpError::NotAvailable.is_permission_error());
    }

    #[test]
    fn test_is_not_available() {
        assert!(XdpError::NotAvailable.is_not_available());
        assert!(!XdpError::PermissionDenied.is_not_available());
    }
}
