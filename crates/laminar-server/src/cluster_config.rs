//! Cluster mode configuration extraction and validation.

use std::fmt;
use std::time::Duration;

use crate::config::{cluster_tls_server_name_is_valid, DiscoverySection, ServerConfig, ServerMode};

/// Node identity for cluster mode (non-empty, max 64 chars).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterNodeId(String);

impl ClusterNodeId {
    const MAX_LEN: usize = 64;

    pub fn from_config(id: String) -> Result<Self, ClusterConfigError> {
        if id.is_empty() {
            return Err(ClusterConfigError::InvalidNodeId(
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

    /// Auto-generate from bind address: `{hostname}-{port}`, or UUID v4 fallback.
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

impl fmt::Display for ClusterNodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Extracted and validated cluster configuration.
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    pub node_id: ClusterNodeId,
    pub discovery: DiscoverySection,
    pub formation_timeout: Duration,
}

impl ClusterConfig {
    const DEFAULT_FORMATION_TIMEOUT: Duration = Duration::from_secs(60);

    pub fn from_server_config(config: &ServerConfig) -> Result<Option<Self>, ClusterConfigError> {
        if config.server.mode != ServerMode::Cluster {
            return Ok(None);
        }

        let discovery = config
            .discovery
            .clone()
            .ok_or_else(|| ClusterConfigError::MissingSection("[discovery]".to_string()))?;

        if !matches!(discovery.strategy.as_str(), "gossip" | "static") {
            return Err(ClusterConfigError::InvalidDiscoveryStrategy(
                discovery.strategy,
            ));
        }

        if discovery.seeds.is_empty() && discovery.strategy == "static" {
            return Err(ClusterConfigError::EmptySeeds);
        }

        let tls_configured = [
            discovery.cluster_tls_cert.is_some(),
            discovery.cluster_tls_key.is_some(),
            discovery.cluster_tls_client_ca.is_some(),
            discovery.cluster_tls_server_name.is_some(),
        ];
        let tls_complete = tls_configured.iter().all(|is_set| *is_set)
            && discovery
                .cluster_tls_server_name
                .as_ref()
                .is_some_and(|name| cluster_tls_server_name_is_valid(name));
        if tls_configured.iter().any(|is_set| *is_set) && !tls_complete {
            return Err(ClusterConfigError::IncompleteControlPlaneTls);
        }

        let node_id = match &config.node_id {
            Some(id) => ClusterNodeId::from_config(id.clone())?,
            None => ClusterNodeId::auto_generate(&config.server.bind),
        };

        Ok(Some(Self {
            node_id,
            discovery,
            formation_timeout: Self::DEFAULT_FORMATION_TIMEOUT,
        }))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ClusterConfigError {
    #[error("cluster mode requires {0} section in config")]
    MissingSection(String),
    #[error("invalid node_id: {0}")]
    InvalidNodeId(String),
    #[error("static discovery requires at least one seed address")]
    EmptySeeds,
    #[error("unsupported discovery strategy {0:?}; expected \"gossip\" or \"static\"")]
    InvalidDiscoveryStrategy(String),
    #[error("cluster mutual TLS configuration must be complete and use a valid server name")]
    IncompleteControlPlaneTls,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::*;

    fn base_config() -> ServerConfig {
        ServerConfig {
            server: ServerSection::default(),
            state: laminar_core::state::StateBackendConfig::default(),
            checkpoint: CheckpointSection::default(),
            supervision: Default::default(),
            sources: vec![],
            lookups: vec![],
            pipelines: vec![],
            sinks: vec![],
            discovery: None,
            node_id: None,
            sql: None,
            ai: Default::default(),
            models: Default::default(),
        }
    }

    fn cluster_config() -> ServerConfig {
        let mut config = base_config();
        config.server.mode = ServerMode::Cluster;
        config.node_id = Some("test-node-1".to_string());
        config.discovery = Some(DiscoverySection {
            strategy: "static".to_string(),
            seeds: vec!["node-1:7946".to_string(), "node-2:7946".to_string()],
            gossip_port: 7946,
            advertise_host: None,
            failure_domain: None,
            placement_isolation_tier: 0,
            cluster_tls_cert: None,
            cluster_tls_key: None,
            cluster_tls_client_ca: None,
            cluster_tls_server_name: None,
        });
        config
    }

    #[test]
    fn test_cluster_config_from_server_config_valid() {
        let config = cluster_config();
        let result = ClusterConfig::from_server_config(&config).unwrap();
        let cluster_cfg = result.expect("should return Some for cluster mode");
        assert_eq!(cluster_cfg.node_id.as_str(), "test-node-1");
        assert_eq!(cluster_cfg.discovery.strategy, "static");
        assert_eq!(cluster_cfg.formation_timeout, Duration::from_secs(60));
    }

    #[test]
    fn test_cluster_config_single_mode_returns_none() {
        let config = base_config();
        let result = ClusterConfig::from_server_config(&config).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_cluster_config_missing_discovery() {
        let mut config = cluster_config();
        config.discovery = None;
        let err = ClusterConfig::from_server_config(&config).unwrap_err();
        assert!(err.to_string().contains("[discovery]"));
    }

    #[test]
    fn test_node_id_from_config() {
        let node_id = ClusterNodeId::from_config("star-1".to_string()).unwrap();
        assert_eq!(node_id.as_str(), "star-1");
    }

    #[test]
    fn test_node_id_auto_generate() {
        let node_id = ClusterNodeId::auto_generate("0.0.0.0:8080");
        let s = node_id.as_str();
        assert!(!s.is_empty());
        assert!(s.ends_with("-8080"), "expected suffix -8080, got: {s}");
    }

    #[test]
    fn test_node_id_auto_generate_truncation() {
        // A very long hostname won't exceed 64 chars
        let node_id = ClusterNodeId::from_config("a".repeat(100)).unwrap();
        assert_eq!(node_id.as_str().len(), 64);
    }

    #[test]
    fn test_cluster_config_error_display() {
        assert!(ClusterConfigError::MissingSection("[discovery]".into())
            .to_string()
            .contains("[discovery]"));
        assert!(ClusterConfigError::EmptySeeds
            .to_string()
            .contains("at least one seed"));
        assert!(ClusterConfigError::IncompleteControlPlaneTls
            .to_string()
            .contains("complete"));
    }

    #[test]
    fn programmatic_remote_cluster_config_accepts_plaintext() {
        let mut config = cluster_config();
        config.server.bind = "0.0.0.0:8080".into();
        let discovery = config.discovery.as_mut().unwrap();
        discovery.advertise_host = Some("10.0.0.7".into());
        discovery.seeds = vec!["10.0.0.8:7946".into()];

        assert!(ClusterConfig::from_server_config(&config).is_ok());
    }

    #[test]
    fn programmatic_partial_cluster_tls_fails() {
        let mut config = cluster_config();
        config.discovery.as_mut().unwrap().cluster_tls_cert = Some("node.pem".into());

        assert!(matches!(
            ClusterConfig::from_server_config(&config),
            Err(ClusterConfigError::IncompleteControlPlaneTls)
        ));

        for invalid_name in ["", " bad.example", "bad name"] {
            let mut config = cluster_config();
            config.discovery.as_mut().unwrap().cluster_tls_server_name = Some(invalid_name.into());
            assert!(matches!(
                ClusterConfig::from_server_config(&config),
                Err(ClusterConfigError::IncompleteControlPlaneTls)
            ));
        }
    }

    #[test]
    fn test_empty_seeds_with_static_strategy() {
        let mut config = cluster_config();
        config.discovery.as_mut().unwrap().seeds.clear();
        let err = ClusterConfig::from_server_config(&config).unwrap_err();
        match err {
            ClusterConfigError::EmptySeeds => {}
            other => panic!("expected EmptySeeds, got: {other}"),
        }
    }

    #[test]
    fn test_unknown_discovery_strategy_is_rejected() {
        let mut config = cluster_config();
        config.discovery.as_mut().unwrap().strategy = "typo".into();

        let err = ClusterConfig::from_server_config(&config).unwrap_err();
        assert!(matches!(
            err,
            ClusterConfigError::InvalidDiscoveryStrategy(strategy) if strategy == "typo"
        ));
    }
}
