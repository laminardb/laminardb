//! Connector-owned, globally sealed initial cursors. These are requirements, not intake permits.

use super::TopologyError;
use crate::checkpoint::ConnectorCheckpoint;
use serde::{Deserialize, Serialize};

/// Maximum physical input channels in one new source's initial inventory.
pub const MAX_TOPOLOGY_SOURCE_CHANNELS: usize = 4096;

/// One new catalog incarnation's exact position before any participant consumes input.
/// The existing connector cursor encoding preserves never-read partitions without inventing a
/// consumed offset, checkpoint attempt or assignment ownership. Target installation must validate
/// this cursor through the connector and adopt only its assigned channels before releasing intake.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TopologySourceInitialization {
    /// Canonical source identity from the independently certified candidate.
    pub name: String,
    /// New catalog incarnation, distinct from the source's connector cursor format.
    pub catalog_generation: u64,
    /// Exact definition/schema/connector/dependency contract certified by every participant.
    pub compatibility_sha256: String,
    /// Complete global input inventory and numeric initial cursors supplied by the connector.
    pub checkpoint: ConnectorCheckpoint,
}

impl TopologySourceInitialization {
    pub(crate) fn validate(&self) -> Result<(), TopologyError> {
        let checkpoint = &self.checkpoint;
        let channels = checkpoint.input_channels.as_deref().ok_or_else(|| {
            TopologyError::Invalid("source initialization has no complete input inventory".into())
        })?;
        if self.name.is_empty()
            || self.catalog_generation == 0
            || self.compatibility_sha256.len() != 64
            || !self
                .compatibility_sha256
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            || checkpoint.source_assignment_version.is_some()
            || channels.is_empty()
            || channels.len() > MAX_TOPOLOGY_SOURCE_CHANNELS
            || !channels.windows(2).all(|p| p[0] < p[1])
            || channels.iter().any(|c| {
                c.is_empty()
                    || c.len() > 1024
                    || c == crate::checkpoint::SINGLETON_WATERMARK_CHANNEL
            })
            || checkpoint.offsets.is_empty()
            || checkpoint.offsets.len() > MAX_TOPOLOGY_SOURCE_CHANNELS
            || checkpoint.metadata.len() > 64
            || checkpoint
                .metadata
                .get("connector")
                .is_none_or(String::is_empty)
            || checkpoint
                .offsets
                .iter()
                .chain(&checkpoint.metadata)
                .any(|(key, value)| key.is_empty() || key.len() > 1024 || value.len() > 4096)
        {
            return Err(TopologyError::Invalid(
                "source initialization requires a bounded, unowned, complete connector cursor"
                    .into(),
            ));
        }
        Ok(())
    }
}
