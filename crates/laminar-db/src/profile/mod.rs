//! Deployment profiles for `LaminarDB`.
//!
//! A [`Profile`] determines which subsystems are activated at startup.
//! Profiles form a hierarchy: each tier includes all capabilities of
//! the tiers below it.
//!
//! ```text
//! BareMetal ⊂ Embedded ⊂ Durable ⊂ Cluster
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

use laminar_core::storage_location::{StorageLocation, StorageProvider};

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
    /// Full distributed: Durable + gRPC + gossip + cluster primitives.
    Cluster,
}

impl Profile {
    /// Auto-detect the appropriate profile from configuration.
    ///
    /// Uses orthogonal signals (checkpoint URL scheme, presence of
    /// discovery config) rather than requiring an explicit profile choice.
    ///
    /// | Signal | Detected Profile |
    /// |--------|-----------------|
    /// | `has_discovery` = true | `Cluster` |
    /// | `object_store_url` uses a supported cloud scheme | `Durable` |
    /// | `object_store_url` is `file://` or `storage_dir` set | `Embedded` |
    /// | None of the above | `BareMetal` |
    #[must_use]
    pub fn from_config(config: &LaminarConfig, has_discovery: bool) -> Self {
        if has_discovery {
            return Self::Cluster;
        }
        if let Some(url) = &config.object_store_url {
            if let Ok(location) = StorageLocation::parse(url) {
                return if location.provider == StorageProvider::Local {
                    Self::Embedded
                } else {
                    Self::Durable
                };
            }
        }
        if config.storage_dir.is_some()
            || config
                .checkpoint
                .as_ref()
                .and_then(|checkpoint| checkpoint.data_dir.as_ref())
                .is_some()
        {
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
        // Feature gates for durable/cluster were removed — all profiles are
        // always available. Heavy distributed deps (tonic, chitchat)
        // are gated on laminar-core's `cluster` feature, which the
        // server binary enables unconditionally. Library users of laminar-db
        // get lightweight builds without distributed infrastructure.
        match self {
            Self::BareMetal | Self::Embedded | Self::Durable | Self::Cluster => Ok(()),
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
                if let Some(url) = object_store_url.filter(|url| url.starts_with("file://")) {
                    laminar_core::checkpoint::object_store_builder::file_url_path(url)
                        .map_err(|error| ProfileError::RequirementNotMet(error.to_string()))?;
                    return Ok(());
                }
                if config.storage_dir.is_some()
                    || config
                        .checkpoint
                        .as_ref()
                        .and_then(|checkpoint| checkpoint.data_dir.as_ref())
                        .is_some()
                {
                    return Ok(());
                }
                Err(ProfileError::RequirementNotMet(
                    "Embedded profile requires an absolute file:// checkpoint URL or local storage directory"
                        .into(),
                ))
            }
            Self::Durable | Self::Cluster => {
                if object_store_url.is_none() {
                    return Err(ProfileError::RequirementNotMet(
                        "Durable/Cluster profile requires an \
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
            Self::Cluster => {
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
            "cluster" => Ok(Self::Cluster),
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
            Self::Cluster => write!(f, "cluster"),
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
mod tests;
