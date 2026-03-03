//! Deployment profiles for `LaminarDB`.
//!
//! A [`Profile`] determines which subsystems are activated at startup.
//! Profiles form a hierarchy: each tier includes all capabilities of
//! the tiers below it.
//!
//! ```text
//! BareMetal ⊂ Embedded ⊂ Durable ⊂ Delta
//! ```
//!
//! ## Usage
//!
//! ```rust,ignore
//! use laminar_db::{LaminarDB, Profile};
//!
//! let db = LaminarDB::builder()
//!     .profile(Profile::Durable)
//!     .object_store_url("s3://my-bucket/checkpoints")
//!     .build()
//!     .await?;
//! ```

use std::fmt;
use std::str::FromStr;

use crate::config::LaminarConfig;

/// Deployment profile — determines which subsystems are activated.
///
/// Profiles are ordered by capability: each tier includes everything
/// from the tiers below it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Profile {
    /// In-memory only, no persistence. Fastest startup.
    #[default]
    BareMetal,
    /// Local WAL persistence (embedded single-node).
    Embedded,
    /// Object-store checkpoints + rkyv snapshots.
    Durable,
    /// Full distributed: Durable + gRPC + gossip + Raft.
    Delta,
}

impl Profile {
    /// Auto-detect the appropriate profile from configuration.
    ///
    /// Uses orthogonal signals (checkpoint URL scheme, presence of
    /// discovery config) rather than requiring an explicit profile choice.
    ///
    /// | Signal | Detected Profile |
    /// |--------|-----------------|
    /// | `has_discovery` = true | `Delta` |
    /// | `object_store_url` is `s3://`/`gs://`/`az://` | `Durable` |
    /// | `object_store_url` is `file://` or `storage_dir` set | `Embedded` |
    /// | None of the above | `BareMetal` |
    #[must_use]
    pub fn from_config(config: &LaminarConfig, has_discovery: bool) -> Self {
        if has_discovery {
            return Self::Delta;
        }
        if let Some(url) = &config.object_store_url {
            if url.starts_with("s3://")
                || url.starts_with("gs://")
                || url.starts_with("az://")
                || url.starts_with("abfs://")
            {
                return Self::Durable;
            }
            if url.starts_with("file://") {
                return Self::Embedded;
            }
        }
        if config.storage_dir.is_some() {
            return Self::Embedded;
        }
        Self::BareMetal
    }

    /// Validate that the compiled feature flags satisfy this profile's
    /// requirements. Returns an error if a required feature was not
    /// compiled in.
    ///
    /// # Errors
    ///
    /// Returns [`ProfileError::FeatureNotCompiled`] if a required Cargo
    /// feature is missing.
    pub fn validate_features(self) -> Result<(), ProfileError> {
        // Feature gates for durable/delta were removed — all profiles are
        // always available. Heavy distributed deps (tonic, openraft, chitchat)
        // are gated on laminar-core's `delta` feature, which the server binary
        // enables unconditionally. Library users of laminar-db get lightweight
        // builds without distributed infrastructure.
        match self {
            Self::BareMetal | Self::Embedded | Self::Durable | Self::Delta => Ok(()),
        }
    }

    /// Validate that the given configuration satisfies this profile's
    /// runtime requirements (e.g., a storage directory for Embedded,
    /// an object store URL for Durable).
    ///
    /// # Errors
    ///
    /// Returns [`ProfileError::RequirementNotMet`] if a required config
    /// field is missing.
    pub fn validate_config(
        self,
        config: &LaminarConfig,
        object_store_url: Option<&str>,
    ) -> Result<(), ProfileError> {
        match self {
            Self::BareMetal => Ok(()),
            Self::Embedded => {
                if config.storage_dir.is_none() {
                    return Err(ProfileError::RequirementNotMet(
                        "Embedded profile requires a storage_dir".into(),
                    ));
                }
                Ok(())
            }
            Self::Durable | Self::Delta => {
                if object_store_url.is_none() {
                    return Err(ProfileError::RequirementNotMet(
                        "Durable/Delta profile requires an \
                         object_store_url"
                            .into(),
                    ));
                }
                Ok(())
            }
        }
    }

    /// Apply sensible defaults to a [`LaminarConfig`] for this profile.
    ///
    /// Does not override fields that the user has already set.
    pub fn apply_defaults(self, config: &mut LaminarConfig) {
        match self {
            Self::BareMetal => {
                // No persistence — nothing to configure.
            }
            Self::Embedded => {
                // Ensure a reasonable buffer size for local workloads.
                if config.default_buffer_size == LaminarConfig::default().default_buffer_size {
                    config.default_buffer_size = 32_768;
                }
            }
            Self::Durable => {
                // Larger buffers for durable workloads.
                if config.default_buffer_size == LaminarConfig::default().default_buffer_size {
                    config.default_buffer_size = 131_072;
                }
            }
            Self::Delta => {
                // Largest buffers for distributed workloads.
                if config.default_buffer_size == LaminarConfig::default().default_buffer_size {
                    config.default_buffer_size = 262_144;
                }
            }
        }
    }
}

impl FromStr for Profile {
    type Err = ProfileError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "bare_metal" | "baremetal" | "bare-metal" => Ok(Self::BareMetal),
            "embedded" => Ok(Self::Embedded),
            "durable" => Ok(Self::Durable),
            "delta" => Ok(Self::Delta),
            _ => Err(ProfileError::UnknownProfileName(s.into())),
        }
    }
}

impl fmt::Display for Profile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BareMetal => write!(f, "bare_metal"),
            Self::Embedded => write!(f, "embedded"),
            Self::Durable => write!(f, "durable"),
            Self::Delta => write!(f, "delta"),
        }
    }
}

/// Errors from profile validation.
#[derive(Debug, thiserror::Error)]
pub enum ProfileError {
    /// A runtime requirement (e.g., config field) was not satisfied.
    #[error("profile requirement not met: {0}")]
    RequirementNotMet(String),

    /// A required Cargo feature was not compiled in.
    #[error("feature `{0}` not compiled — enable it in Cargo.toml")]
    FeatureNotCompiled(String),

    /// The profile name could not be parsed.
    #[error("unknown profile name: {0}")]
    UnknownProfileName(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bare_metal_zero_config() {
        let config = LaminarConfig::default();
        let profile = Profile::BareMetal;

        // BareMetal needs no features and no config
        assert!(profile.validate_features().is_ok());
        assert!(profile.validate_config(&config, None).is_ok());
    }

    #[test]
    fn test_embedded_requires_storage_dir() {
        let config = LaminarConfig::default();
        let result = Profile::Embedded.validate_config(&config, None);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            ProfileError::RequirementNotMet(_)
        ));
    }

    #[test]
    fn test_durable_fails_without_object_store_url() {
        let config = LaminarConfig::default();
        let result = Profile::Durable.validate_config(&config, None);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            ProfileError::RequirementNotMet(_)
        ));
    }

    #[test]
    fn test_profile_from_str() {
        assert_eq!(Profile::from_str("bare_metal").unwrap(), Profile::BareMetal);
        assert_eq!(Profile::from_str("baremetal").unwrap(), Profile::BareMetal);
        assert_eq!(Profile::from_str("bare-metal").unwrap(), Profile::BareMetal);
        assert_eq!(Profile::from_str("embedded").unwrap(), Profile::Embedded);
        assert_eq!(Profile::from_str("durable").unwrap(), Profile::Durable);
        assert_eq!(Profile::from_str("delta").unwrap(), Profile::Delta);
        // Case insensitive
        assert_eq!(Profile::from_str("DURABLE").unwrap(), Profile::Durable);
        // Unknown name
        assert!(Profile::from_str("quantum").is_err());
        assert!(matches!(
            Profile::from_str("quantum").unwrap_err(),
            ProfileError::UnknownProfileName(_)
        ));
    }

    #[test]
    fn test_all_profiles_validate_features() {
        // Feature gates removed — all profiles always pass validation.
        assert!(Profile::BareMetal.validate_features().is_ok());
        assert!(Profile::Embedded.validate_features().is_ok());
        assert!(Profile::Durable.validate_features().is_ok());
        assert!(Profile::Delta.validate_features().is_ok());
    }

    #[test]
    fn test_profile_display() {
        assert_eq!(Profile::BareMetal.to_string(), "bare_metal");
        assert_eq!(Profile::Embedded.to_string(), "embedded");
        assert_eq!(Profile::Durable.to_string(), "durable");
        assert_eq!(Profile::Delta.to_string(), "delta");
    }

    #[test]
    fn test_profile_default() {
        assert_eq!(Profile::default(), Profile::BareMetal);
    }

    #[test]
    fn test_apply_defaults_bare_metal_noop() {
        let mut config = LaminarConfig::default();
        let original_buffer = config.default_buffer_size;
        Profile::BareMetal.apply_defaults(&mut config);
        assert_eq!(config.default_buffer_size, original_buffer);
    }

    #[test]
    fn test_apply_defaults_does_not_override_user_values() {
        let mut config = LaminarConfig {
            default_buffer_size: 999,
            ..LaminarConfig::default()
        };
        Profile::Durable.apply_defaults(&mut config);
        // User explicitly set 999 — should not be overridden
        assert_eq!(config.default_buffer_size, 999);
    }

    #[test]
    fn test_from_config_bare_metal() {
        let config = LaminarConfig::default();
        assert_eq!(Profile::from_config(&config, false), Profile::BareMetal);
    }

    #[test]
    fn test_from_config_embedded_storage_dir() {
        let config = LaminarConfig {
            storage_dir: Some(std::path::PathBuf::from("/tmp/data")),
            ..LaminarConfig::default()
        };
        assert_eq!(Profile::from_config(&config, false), Profile::Embedded);
    }

    #[test]
    fn test_from_config_embedded_file_url() {
        let config = LaminarConfig {
            object_store_url: Some("file:///tmp/checkpoints".to_string()),
            ..LaminarConfig::default()
        };
        assert_eq!(Profile::from_config(&config, false), Profile::Embedded);
    }

    #[test]
    fn test_from_config_durable_s3() {
        let config = LaminarConfig {
            object_store_url: Some("s3://my-bucket/prefix".to_string()),
            ..LaminarConfig::default()
        };
        assert_eq!(Profile::from_config(&config, false), Profile::Durable);
    }

    #[test]
    fn test_from_config_durable_gs() {
        let config = LaminarConfig {
            object_store_url: Some("gs://my-bucket/prefix".to_string()),
            ..LaminarConfig::default()
        };
        assert_eq!(Profile::from_config(&config, false), Profile::Durable);
    }

    #[test]
    fn test_from_config_durable_az() {
        let config = LaminarConfig {
            object_store_url: Some("az://container/prefix".to_string()),
            ..LaminarConfig::default()
        };
        assert_eq!(Profile::from_config(&config, false), Profile::Durable);
    }

    #[test]
    fn test_from_config_durable_abfs() {
        let config = LaminarConfig {
            object_store_url: Some("abfs://container/prefix".to_string()),
            ..LaminarConfig::default()
        };
        assert_eq!(Profile::from_config(&config, false), Profile::Durable);
    }

    #[test]
    fn test_from_config_delta() {
        let config = LaminarConfig::default();
        assert_eq!(Profile::from_config(&config, true), Profile::Delta);
    }

    #[test]
    fn test_from_config_delta_overrides_url() {
        let config = LaminarConfig {
            object_store_url: Some("s3://bucket/prefix".to_string()),
            ..LaminarConfig::default()
        };
        // Discovery takes priority over URL-based detection
        assert_eq!(Profile::from_config(&config, true), Profile::Delta);
    }
}
