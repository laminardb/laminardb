//! Compact inventory records for unresolved prepared checkpoints.

use super::{PipelineIdentity, PIPELINE_IDENTITY_VERSION};
use crate::state::CheckpointAttempt;

/// Maximum unresolved prepared attempts one process may include in a stopped report.
///
/// The inventory is recovery evidence, not history. Retaining a bounded recent set keeps the
/// control-plane record small and makes an unexpectedly growing backlog fail closed.
pub const MAX_PREPARED_CHECKPOINT_WITNESSES: usize = 64;

/// One process's compact evidence that an exact checkpoint attempt reached `Prepared`.
///
/// This is deliberately not a commit signal. A recovery driver must resolve every witness through
/// the immutable checkpoint-outcome authority before allowing the cluster to restart.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedCheckpointWitness {
    /// Exact epoch and globally unique checkpoint-attempt identifier.
    pub attempt: CheckpointAttempt,
    /// Stable logical participant recorded by the prepared manifest.
    pub participant_id: u64,
    /// Canonical non-nil deployment UUID recorded by the prepared manifest.
    pub deployment_id: String,
    /// Exact logical pipeline and state-ABI identity recorded by the prepared manifest.
    pub pipeline_identity: PipelineIdentity,
}

impl PreparedCheckpointWitness {
    /// Construct and validate a compact prepared-checkpoint witness.
    ///
    /// # Errors
    /// Returns an error when any persisted identity is zero, non-canonical, or unsupported.
    pub fn new(
        attempt: CheckpointAttempt,
        participant_id: u64,
        deployment_id: String,
        pipeline_identity: PipelineIdentity,
    ) -> Result<Self, String> {
        let witness = Self {
            attempt,
            participant_id,
            deployment_id,
            pipeline_identity,
        };
        witness.validate()?;
        Ok(witness)
    }

    /// Validate the persisted identities carried by this witness.
    ///
    /// # Errors
    /// Returns an error describing the first non-canonical field.
    pub(crate) fn validate(&self) -> Result<(), String> {
        if self.attempt.epoch == 0 || self.attempt.checkpoint_id == 0 {
            return Err(
                "prepared checkpoint attempt must have nonzero epoch and checkpoint ID".into(),
            );
        }
        if self.participant_id == 0 {
            return Err("prepared checkpoint participant must be nonzero".into());
        }
        let deployment = uuid::Uuid::parse_str(&self.deployment_id)
            .map_err(|error| format!("prepared checkpoint deployment ID is invalid: {error}"))?;
        if deployment.is_nil() || deployment.to_string() != self.deployment_id {
            return Err(
                "prepared checkpoint deployment ID must be a canonical non-nil UUID".into(),
            );
        }
        if !self.pipeline_identity.is_canonical() {
            return Err(format!(
                "prepared checkpoint pipeline identity must use canonical version {PIPELINE_IDENTITY_VERSION} and a lowercase SHA-256 digest"
            ));
        }
        Ok(())
    }

    /// Whether all persisted identities have their canonical production shape.
    #[must_use]
    pub fn is_canonical(&self) -> bool {
        self.validate().is_ok()
    }

    pub(crate) const fn ordering_key(&self) -> (u64, u64, u64) {
        (
            self.attempt.epoch,
            self.attempt.checkpoint_id,
            self.participant_id,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn witness() -> PreparedCheckpointWitness {
        PreparedCheckpointWitness::new(
            CheckpointAttempt::new(7, 42),
            3,
            uuid::Uuid::from_u128(9).to_string(),
            PipelineIdentity::empty(),
        )
        .unwrap()
    }

    #[test]
    fn witness_rejects_zero_and_noncanonical_identities() {
        let valid = witness();
        assert!(valid.is_canonical());

        let mut invalid = valid.clone();
        invalid.attempt.epoch = 0;
        assert!(!invalid.is_canonical());

        let mut invalid = valid.clone();
        invalid.attempt.checkpoint_id = 0;
        assert!(!invalid.is_canonical());

        let mut invalid = valid.clone();
        invalid.participant_id = 0;
        assert!(!invalid.is_canonical());

        let mut invalid = valid.clone();
        invalid.deployment_id = uuid::Uuid::nil().to_string();
        assert!(!invalid.is_canonical());

        let mut invalid = valid;
        invalid.pipeline_identity.canonical_version = PIPELINE_IDENTITY_VERSION + 1;
        assert!(!invalid.is_canonical());
    }

    #[test]
    fn witness_wire_rejects_unknown_fields() {
        let mut value = serde_json::to_value(witness()).unwrap();
        value["unknown"] = serde_json::json!(true);
        assert!(serde_json::from_value::<PreparedCheckpointWitness>(value).is_err());
    }
}
