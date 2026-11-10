//! Real-time recency: how far a source's upstream has been written, so a committed read can wait
//! for a checkpoint that holds everything the source could see when the read arrived.

use std::fmt::Debug;
use std::time::Instant;

use async_trait::async_trait;

use crate::checkpoint::SourceCheckpoint;
use crate::error::ConnectorError;

/// Reports how far a source's upstream has been written. It is built from the source's
/// configuration on any node and never touches the running reader or its position.
#[async_trait]
pub trait SourceRecency: Send + Sync + Debug {
    /// The upstream's end now: every record a reader of this source could see.
    ///
    /// # Errors
    ///
    /// Returns an error when the upstream cannot be reached or described before `deadline`.
    async fn upstream_end(
        &self,
        deadline: Instant,
    ) -> Result<Box<dyn RecencyTarget>, ConnectorError>;
}

/// The upstream's end at one instant.
#[async_trait]
pub trait RecencyTarget: Send + Sync + Debug {
    /// Whether `checkpoint`, a committed checkpoint of this source, holds every record before
    /// this end. Positions only grow, so a later checkpoint of a covering one also covers it.
    ///
    /// # Errors
    ///
    /// Returns an error when `checkpoint` is not this connector's, or coverage cannot be decided
    /// before `deadline`.
    async fn covered_by(
        &self,
        checkpoint: &SourceCheckpoint,
        deadline: Instant,
    ) -> Result<bool, ConnectorError>;
}
