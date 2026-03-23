//! Delta mode configuration extraction and validation.
//!
//! Extracts delta-specific settings from `ServerConfig` and validates
//! them before the delta startup orchestrator runs. This module is
//! always compiled (not feature-gated) so that non-delta builds can
//! still detect when delta mode is requested and produce a clear error.

use std::fmt;
use std::time::Duration;

use crate::config::{CoordinationSection, DiscoverySection, ServerConfig};

/// Node identity for delta mode.
///
/// Wraps a string identifier that must be non-empty and at most 64
/// characters. Used as the human-readable node name in discovery and
/// coordination protocols.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
pub struct DeltaNodeId(String);

#[allow(dead_code)]
impl DeltaNodeId {
    /// Maximum length for a node ID (bytes).
    const MAX_LEN: usize = 64;

    /// Create a `DeltaNodeId` from an explicit config string.
    ///
    /// # Errors
    ///
    /// Returns `DeltaConfigError::InvalidNodeId` if the string is empty.
    pub fn from_config(id: String) -> Result<Self, DeltaConfigError> {
        if id.is_empty() {
            return Err(DeltaConfigError::InvalidNodeId(
                "node_id must not be empty".to_string(),
            ));
        }
        let truncated = if id.len() > Self::MAX_LEN {
            id[..Self::MAX_LEN].to_string()
        } else {
            id
        };
        Ok(Self(truncated))
    }

    /// Auto-generate a node ID from the bind address.
    ///
    /// Uses `gethostname` to build `{hostname}-{port}`. Falls back to
    /// a UUID v4 if hostname resolution fails.
    pub fn auto_generate(bind_addr: &str) -> Self {
        let port = bind_addr.rsplit(':').next().unwrap_or("8080");

        let hostname = gethostname::gethostname();
        let hostname_str = hostname.to_string_lossy();

        let candidate = if hostname_str.is_empty() {
            format!("{}", uuid::Uuid::new_v4())
        } else {
            format!("{hostname_str}-{port}")
        };

        let truncated = if candidate.len() > Self::MAX_LEN {
            candidate[..Self::MAX_LEN].to_string()
        } else {
            candidate
        };

        Self(truncated)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for DeltaNodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Extracted and validated delta configuration.
///
/// Built from `ServerConfig` fields that are only relevant in delta
/// mode. Holds owned copies so the original config can be dropped.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct DeltaConfig {
    /// Unique node identity.
    pub node_id: DeltaNodeId,
    /// Discovery settings.
    pub discovery: DiscoverySection,
    /// Coordination settings.
    pub coordination: CoordinationSection,
    /// Timeout for initial peer discovery and formation.
    pub formation_timeout: Duration,
    /// Timeout for Raft quorum establishment.
    pub quorum_timeout: Duration,
    /// Timeout for initial partition assignment.
    pub assignment_timeout: Duration,
}

impl DeltaConfig {
    /// Default formation timeout (60 seconds).
    const DEFAULT_FORMATION_TIMEOUT: Duration = Duration::from_secs(60);
    /// Default quorum timeout (30 seconds).
    const DEFAULT_QUORUM_TIMEOUT: Duration = Duration::from_secs(30);
    /// Default assignment timeout (30 seconds).
    const DEFAULT_ASSIGNMENT_TIMEOUT: Duration = Duration::from_secs(30);

    /// Extract delta config from a `ServerConfig`.
    ///
    /// Returns `Ok(None)` for embedded mode (no delta sections needed).
    /// Returns `Ok(Some(...))` for valid delta mode.
    /// Returns `Err(...)` for invalid delta config.
    pub fn from_server_config(config: &ServerConfig) -> Result<Option<Self>, DeltaConfigError> {
        if config.server.mode != "delta" {
            return Ok(None);
        }

        let discovery = config
            .discovery
            .clone()
            .ok_or_else(|| DeltaConfigError::MissingSection("[discovery]".to_string()))?;

        let coordination = config
            .coordination
            .clone()
            .ok_or_else(|| DeltaConfigError::MissingSection("[coordination]".to_string()))?;

        if discovery.seeds.is_empty() && discovery.strategy == "static" {
            return Err(DeltaConfigError::EmptySeeds);
        }

        let node_id = match &config.node_id {
            Some(id) => DeltaNodeId::from_config(id.clone())?,
            None => DeltaNodeId::auto_generate(&config.server.bind),
        };

        Ok(Some(Self {
            node_id,
            discovery,
            coordination,
            formation_timeout: Self::DEFAULT_FORMATION_TIMEOUT,
            quorum_timeout: Self::DEFAULT_QUORUM_TIMEOUT,
            assignment_timeout: Self::DEFAULT_ASSIGNMENT_TIMEOUT,
        }))
    }
}

/// Errors from delta configuration extraction.
#[derive(Debug, thiserror::Error)]
pub enum DeltaConfigError {
    /// A required TOML section is missing.
    #[error("delta mode requires {0} section in config")]
    MissingSection(String),

    /// The node ID is invalid.
    #[error("invalid node_id: {0}")]
    InvalidNodeId(String),

    /// Static discovery has no seed addresses.
    #[error("static discovery requires at least one seed address")]
    EmptySeeds,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::*;

    fn base_config() -> ServerConfig {
        ServerConfig {
            server: ServerSection::default(),
            state: StateSection::default(),
            checkpoint: CheckpointSection::default(),
            sources: vec![],
            lookups: vec![],
            pipelines: vec![],
            sinks: vec![],
            discovery: None,
            coordination: None,
            node_id: None,
            sql: None,
        }
    }

    fn delta_config() -> ServerConfig {
        let mut config = base_config();
        config.server.mode = "delta".to_string();
        config.node_id = Some("test-node-1".to_string());
        config.discovery = Some(DiscoverySection {
            strategy: "static".to_string(),
            seeds: vec!["node-1:7946".to_string(), "node-2:7946".to_string()],
            gossip_port: 7946,
        });
        config.coordination = Some(CoordinationSection {
            strategy: "raft".to_string(),
            raft_port: 7947,
            election_timeout: Duration::from_millis(1500),
            heartbeat_interval: Duration::from_millis(300),
        });
        config
    }

    #[test]
    fn test_delta_config_from_server_config_valid() {
        let config = delta_config();
        let result = DeltaConfig::from_server_config(&config).unwrap();
        let delta_cfg = result.expect("should return Some for delta mode");
        assert_eq!(delta_cfg.node_id.as_str(), "test-node-1");
        assert_eq!(delta_cfg.discovery.strategy, "static");
        assert_eq!(delta_cfg.coordination.raft_port, 7947);
        assert_eq!(delta_cfg.formation_timeout, Duration::from_secs(60));
        assert_eq!(delta_cfg.quorum_timeout, Duration::from_secs(30));
        assert_eq!(delta_cfg.assignment_timeout, Duration::from_secs(30));
    }

    #[test]
    fn test_delta_config_embedded_mode_returns_none() {
        let config = base_config();
        let result = DeltaConfig::from_server_config(&config).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_delta_config_missing_discovery() {
        let mut config = delta_config();
        config.discovery = None;
        let err = DeltaConfig::from_server_config(&config).unwrap_err();
        assert!(err.to_string().contains("[discovery]"));
    }

    #[test]
    fn test_delta_config_missing_coordination() {
        let mut config = delta_config();
        config.coordination = None;
        let err = DeltaConfig::from_server_config(&config).unwrap_err();
        assert!(err.to_string().contains("[coordination]"));
    }

    #[test]
    fn test_node_id_from_config() {
        let node_id = DeltaNodeId::from_config("star-1".to_string()).unwrap();
        assert_eq!(node_id.as_str(), "star-1");
    }

    #[test]
    fn test_node_id_auto_generate() {
        let node_id = DeltaNodeId::auto_generate("0.0.0.0:8080");
        let s = node_id.as_str();
        assert!(!s.is_empty());
        assert!(s.ends_with("-8080"), "expected suffix -8080, got: {s}");
    }

    #[test]
    fn test_node_id_auto_generate_truncation() {
        // A very long hostname won't exceed 64 chars
        let node_id = DeltaNodeId::from_config("a".repeat(100)).unwrap();
        assert_eq!(node_id.as_str().len(), 64);
    }

    #[test]
    fn test_delta_startup_error_display() {
        let err = DeltaConfigError::MissingSection("[discovery]".to_string());
        assert_eq!(
            err.to_string(),
            "delta mode requires [discovery] section in config"
        );

        let err = DeltaConfigError::InvalidNodeId("empty".to_string());
        assert_eq!(err.to_string(), "invalid node_id: empty");

        let err = DeltaConfigError::EmptySeeds;
        assert!(err.to_string().contains("at least one seed"));
    }

    #[test]
    fn test_empty_seeds_with_static_strategy() {
        let mut config = delta_config();
        config.discovery.as_mut().unwrap().seeds.clear();
        let err = DeltaConfig::from_server_config(&config).unwrap_err();
        match err {
            DeltaConfigError::EmptySeeds => {}
            other => panic!("expected EmptySeeds, got: {other}"),
        }
    }
}
