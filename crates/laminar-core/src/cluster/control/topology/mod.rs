//! Control-path identities and explicit adoption of a legacy sealed catalog.
//!
//! Adoption changes the authority encoding, not the processing graph. Runtime migrations must
//! still establish a checkpoint cut, validate state mappings and commit coordinated release.

use std::num::NonZeroU64;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{CatalogManifestError, CatalogManifestRef, LeaseError};
use crate::error_codes;

mod admission;
pub(crate) use admission::MAX_TOPOLOGY_PLAN_BYTES;
pub use admission::{
    TopologyAbortReason, TopologyAdmissionPhase, TopologyAdmissionPlan, TopologyAdmissionStatus,
    TopologyPlanRef, MAX_TOPOLOGY_OPERATIONS,
};

/// Version of the topology adoption protocol understood by this binary.
pub const TOPOLOGY_PROTOCOL_VERSION: u16 = 1;

/// Logical topology version, independent of catalog/checkpoint serialization versions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TopologyVersion(NonZeroU64);

impl TopologyVersion {
    /// The explicitly adopted legacy inventory is topology one.
    pub const LEGACY_BASELINE: Self = Self(NonZeroU64::MIN);

    /// Construct a nonzero logical version.
    ///
    /// # Errors
    /// Zero never identifies a topology.
    pub fn new(value: u64) -> Result<Self, TopologyError> {
        NonZeroU64::new(value)
            .map(Self)
            .ok_or_else(|| TopologyError::Invalid("topology version must be nonzero".into()))
    }

    /// Numeric logical version.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0.get()
    }

    /// Allocate the exact successor without wrapping.
    ///
    /// # Errors
    /// Fails when the topology version domain is exhausted.
    pub fn successor(self) -> Result<Self, TopologyError> {
        self.get()
            .checked_add(1)
            .ok_or_else(|| TopologyError::Invalid("topology version exhausted".into()))
            .and_then(Self::new)
    }
}

/// Durable request identity for a topology operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "Uuid")]
pub struct TopologyOperationId(Uuid);

impl TryFrom<Uuid> for TopologyOperationId {
    type Error = TopologyError;

    fn try_from(value: Uuid) -> Result<Self, Self::Error> {
        if value.is_nil() {
            return Err(TopologyError::Invalid(
                "topology operation identity must be nonzero".into(),
            ));
        }
        Ok(Self(value))
    }
}

impl TopologyOperationId {
    /// UUID supplied by the request owner and reused on every retry.
    #[must_use]
    pub const fn get(self) -> Uuid {
        self.0
    }
}

/// Immutable identity of the existing inventory at the authority format upgrade.
///
/// This record deliberately cannot represent topology two: no target may become authoritative
/// before the checkpoint-bound migration protocol is installed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LegacyTopologyBaseline {
    /// Protocol the upgrading binary must understand.
    pub protocol_version: u16,
    /// Explicitly assigned baseline version, always one.
    pub topology_version: TopologyVersion,
    /// Original manifest reference; bytes, hash and object generations are not rewritten.
    pub manifest: CatalogManifestRef,
    /// Existing checkpoint/control deployment identity.
    pub deployment_id: String,
    /// Idempotency identity of the successful upgrade request.
    pub operation_id: TopologyOperationId,
    /// The exact shared authority append that admitted this baseline.
    pub authority_sequence: u64,
}

impl LegacyTopologyBaseline {
    pub(super) fn validate(&self) -> Result<(), TopologyError> {
        if self.protocol_version != TOPOLOGY_PROTOCOL_VERSION {
            return Err(TopologyError::Protocol(format!(
                "unsupported topology protocol {}",
                self.protocol_version
            )));
        }
        if self.topology_version != TopologyVersion::LEGACY_BASELINE || self.authority_sequence == 0
        {
            return Err(TopologyError::Invalid(
                "legacy adoption must bind topology one to a nonzero authority sequence".into(),
            ));
        }
        let deployment = Uuid::parse_str(&self.deployment_id)
            .map_err(|_| TopologyError::Invalid("invalid topology deployment identity".into()))?;
        if deployment.is_nil() || deployment.to_string() != self.deployment_id {
            return Err(TopologyError::Invalid(
                "topology deployment identity must be a canonical nonzero UUID".into(),
            ));
        }
        self.manifest.validate()?;
        Ok(())
    }
}

/// Durable catalog state; missing version metadata is never an implicit current version.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum TopologyCatalogState {
    /// No catalog inventory has been sealed.
    Uninitialized,
    /// A sealed inventory exists, but the format upgrade has not been admitted.
    LegacySealed {
        /// Exact existing inventory, with no inferred logical topology version.
        manifest: CatalogManifestRef,
    },
    /// Explicitly adopted inventory. Runtime activation is separate evidence.
    Versioned {
        /// Durable baseline identity.
        baseline: LegacyTopologyBaseline,
    },
}

/// Result of one explicit format upgrade, including the original winner on an identical retry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TopologyAdoptionOutcome {
    /// This request admitted the baseline.
    Created(LegacyTopologyBaseline),
    /// The same baseline was already admitted, possibly by another request.
    Existing(LegacyTopologyBaseline),
}

/// Typed, stable control-path failures. No error authorizes a topology or state fallback.
#[derive(Debug, thiserror::Error)]
pub enum TopologyError {
    /// Shared authority I/O or validation failed.
    #[error("[{code}] topology authority: {0}", code = error_codes::TOPOLOGY_AUTHORITY_FAILED)]
    Authority(#[from] LeaseError),
    /// Catalog content failed validation.
    #[error("[{code}] topology catalog: {0}", code = error_codes::TOPOLOGY_INVALID)]
    Catalog(CatalogManifestError),
    /// Malformed proposal, durable state or exhausted identity domain.
    #[error("[{code}] invalid topology evidence: {0}", code = error_codes::TOPOLOGY_INVALID)]
    Invalid(String),
    /// Expected parent/reference/deployment does not match durable authority.
    #[error("[{code}] topology parent conflict: {0}", code = error_codes::TOPOLOGY_PARENT_CONFLICT)]
    Conflict(String),
    /// Supplied leader proof is stale.
    #[error("[{code}] topology operation was fenced by current leader authority", code = error_codes::TOPOLOGY_FENCED)]
    Fenced,
    /// A required protocol/storage capability is not available.
    #[error("[{code}] topology protocol unavailable: {0}", code = error_codes::TOPOLOGY_PROTOCOL_UNSUPPORTED)]
    Protocol(String),
    /// The bounded CAS/deadline budget was exhausted; authoritative status must be reread.
    #[error("[{code}] topology authority is contended or its outcome is uncertain; reread status and retry the same operation identity", code = error_codes::TOPOLOGY_AUTHORITY_CONTENDED)]
    Contended,
    /// A status read exceeded its control-path deadline.
    #[error("[{code}] topology status read exceeded its 15 second deadline; retry this read", code = error_codes::TOPOLOGY_AUTHORITY_CONTENDED)]
    ReadTimedOut,
}

impl From<CatalogManifestError> for TopologyError {
    fn from(error: CatalogManifestError) -> Self {
        match error {
            CatalogManifestError::Authority(error) => Self::Authority(error),
            error => Self::Catalog(error),
        }
    }
}

impl TopologyError {
    /// Stable registry code for this typed failure.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::Authority(_) => error_codes::TOPOLOGY_AUTHORITY_FAILED,
            Self::Catalog(_) | Self::Invalid(_) => error_codes::TOPOLOGY_INVALID,
            Self::Conflict(_) => error_codes::TOPOLOGY_PARENT_CONFLICT,
            Self::Fenced => error_codes::TOPOLOGY_FENCED,
            Self::Protocol(_) => error_codes::TOPOLOGY_PROTOCOL_UNSUPPORTED,
            Self::Contended | Self::ReadTimedOut => error_codes::TOPOLOGY_AUTHORITY_CONTENDED,
        }
    }
}

#[cfg(test)]
mod tests;
