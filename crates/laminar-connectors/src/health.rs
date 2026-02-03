//! Connector health status types.
//!
//! Provides health reporting for connectors used by the runtime
//! and admin dashboard.

use std::fmt;

/// Health status of a connector.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum HealthStatus {
    /// Connector is healthy and operating normally.
    Healthy,

    /// Connector is degraded but still operational.
    /// Contains a description of the degradation.
    Degraded(String),

    /// Connector is unhealthy and not processing data.
    /// Contains a description of the failure.
    Unhealthy(String),

    /// Health status is unknown (e.g., connector not yet started).
    #[default]
    Unknown,
}

impl HealthStatus {
    /// Returns `true` if the connector is healthy.
    #[must_use]
    pub fn is_healthy(&self) -> bool {
        matches!(self, HealthStatus::Healthy)
    }

    /// Returns `true` if the connector can still process data.
    #[must_use]
    pub fn is_operational(&self) -> bool {
        matches!(self, HealthStatus::Healthy | HealthStatus::Degraded(_))
    }
}

impl fmt::Display for HealthStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HealthStatus::Healthy => write!(f, "Healthy"),
            HealthStatus::Degraded(msg) => write!(f, "Degraded: {msg}"),
            HealthStatus::Unhealthy(msg) => write!(f, "Unhealthy: {msg}"),
            HealthStatus::Unknown => write!(f, "Unknown"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_health_status_checks() {
        assert!(HealthStatus::Healthy.is_healthy());
        assert!(HealthStatus::Healthy.is_operational());

        let degraded = HealthStatus::Degraded("high latency".into());
        assert!(!degraded.is_healthy());
        assert!(degraded.is_operational());

        let unhealthy = HealthStatus::Unhealthy("connection lost".into());
        assert!(!unhealthy.is_healthy());
        assert!(!unhealthy.is_operational());

        assert!(!HealthStatus::Unknown.is_healthy());
        assert!(!HealthStatus::Unknown.is_operational());
    }

    #[test]
    fn test_health_status_display() {
        assert_eq!(HealthStatus::Healthy.to_string(), "Healthy");
        assert!(HealthStatus::Degraded("slow".into())
            .to_string()
            .contains("slow"));
        assert!(HealthStatus::Unhealthy("down".into())
            .to_string()
            .contains("down"));
    }
}
