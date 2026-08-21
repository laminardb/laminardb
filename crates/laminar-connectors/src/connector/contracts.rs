//! Source and sink delivery, topology, and changelog contracts.

use std::str::FromStr;

/// Delivery guarantee level for the pipeline.
///
/// Configures the expected end-to-end delivery semantics. The pipeline
/// validates at startup that all sources and sinks meet the requirements
/// for the chosen guarantee level.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Default, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryGuarantee {
    /// Best effort: no replay contract. Intended for bare-metal/embedded
    /// low-latency pipelines that explicitly accept loss on failure.
    #[default]
    BestEffort,
    /// At-least-once: records may be replayed on recovery. Requires
    /// checkpointing and replayable sources.
    AtLeastOnce,
    /// Exactly-once: no duplicates or losses. Requires all sources to
    /// support replay, all sinks to support exactly-once, and checkpoint
    /// to be enabled.
    ExactlyOnce,
}

impl std::fmt::Display for DeliveryGuarantee {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DeliveryGuarantee::BestEffort => write!(f, "best-effort"),
            DeliveryGuarantee::AtLeastOnce => write!(f, "at-least-once"),
            DeliveryGuarantee::ExactlyOnce => write!(f, "exactly-once"),
        }
    }
}

impl FromStr for DeliveryGuarantee {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().replace('-', "_").as_str() {
            "best_effort" | "besteffort" | "none" => Ok(Self::BestEffort),
            "at_least_once" | "atleastonce" => Ok(Self::AtLeastOnce),
            "exactly_once" | "exactlyonce" => Ok(Self::ExactlyOnce),
            other => Err(format!("unknown delivery guarantee: '{other}'")),
        }
    }
}

/// Recovery semantics provided by a source.
///
/// This is deliberately a small, ordered set of operational contracts rather
/// than a collection of independent capability flags. A source must advertise
/// the strongest contract its implementation can actually uphold.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum SourceConsistency {
    /// Events cannot be reconstructed after the runtime has accepted them.
    #[default]
    Ephemeral,
    /// A persisted source position can be used to reproduce accepted events.
    Replayable,
    /// Replay is supported, and upstream progress/resources advance only when
    /// the corresponding `LaminarDB` checkpoint is durably committed.
    CommitCoupled,
}

impl SourceConsistency {
    /// Whether a persisted source position can be replayed after recovery.
    #[must_use]
    pub const fn supports_replay(self) -> bool {
        !matches!(self, Self::Ephemeral)
    }

    /// Whether checkpoint commits are required for safe upstream progress.
    #[must_use]
    pub const fn requires_checkpointing(self) -> bool {
        matches!(self, Self::CommitCoupled)
    }
}

/// How a source may be placed across runtime nodes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum SourceTopology {
    /// Exactly one runtime instance owns the source.
    #[default]
    Singleton,
    /// Input partitions can be assigned independently across runtime nodes.
    Splittable,
    /// Each runtime node receives a distinct, node-local input stream.
    NodeLocalIngress,
}

/// Update model emitted by a configured source.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum SourceInputMode {
    /// Every row is an insertion.
    #[default]
    AppendOnly,
    /// Current row images and deletes are reconciled by the declared primary key.
    KeyedUpsert,
    /// Decoded rows carry a non-null, non-zero `Int64` `__weight` column.
    FullChangelog,
}

/// Whether a source emits an ordered deterministic position for every decoded row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum SourceRowPositionCapability {
    /// The source does not provide row positions.
    #[default]
    Unavailable,
    /// Every emitted row carries a replay position. Within one source run, `(order_key,
    /// sub_offset)` is nondecreasing per partition across batches; recovery may restart from an
    /// earlier position. Replaying an equal position must produce the same logical row and
    /// mutation.
    OrderedDeterministic,
}

/// Complete source admission contract for a concrete connector configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct SourceContract {
    /// Recovery and external-progress semantics.
    pub consistency: SourceConsistency,
    /// Valid runtime placement model.
    pub topology: SourceTopology,
    /// Update model produced after connector decoding.
    pub input_mode: SourceInputMode,
    /// Deterministic per-row position support.
    pub row_positions: SourceRowPositionCapability,
    exact_delivery_certified: bool,
}

impl SourceContract {
    /// Construct a source contract from its recovery, placement, and update dimensions.
    /// Exactly-once certification defaults to fail-closed.
    #[must_use]
    pub const fn new(
        consistency: SourceConsistency,
        topology: SourceTopology,
        input_mode: SourceInputMode,
    ) -> Self {
        Self {
            consistency,
            topology,
            input_mode,
            row_positions: SourceRowPositionCapability::Unavailable,
            exact_delivery_certified: false,
        }
    }

    /// Declare the source's per-row position contract.
    #[must_use]
    pub const fn with_row_positions(mut self, capability: SourceRowPositionCapability) -> Self {
        self.row_positions = capability;
        self
    }

    /// Mark a built-in connector whose exact-delivery suite is an engine release gate.
    #[must_use]
    pub(crate) const fn with_exact_delivery_certification(mut self) -> Self {
        self.exact_delivery_certified = true;
        self
    }

    /// Whether this source is certified for exactly-once delivery.
    #[doc(hidden)]
    #[must_use]
    pub const fn is_exact_delivery_certified(self) -> bool {
        self.exact_delivery_certified
    }

    /// Whether a persisted source position can be replayed after recovery.
    #[must_use]
    pub const fn supports_replay(self) -> bool {
        self.consistency.supports_replay()
    }

    /// Whether checkpoint commits are required for safe upstream progress.
    #[must_use]
    pub const fn requires_checkpointing(self) -> bool {
        self.consistency.requires_checkpointing()
    }
}

/// Durability protocol provided by a sink.
///
/// This describes externally observable behaviour, not an implementation
/// detail such as whether the client library buffers or retries writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum SinkConsistency {
    /// An accepted write can be lost when the connector or peer fails.
    #[default]
    Ephemeral,
    /// Successful writes are durably acknowledged, but replay may duplicate
    /// them because visibility is not coupled to a `LaminarDB` checkpoint.
    DurableAtLeastOnce,
    /// Output can be staged and made visible by the checkpoint commit
    /// protocol. This is necessary, but not sufficient, for exactly-once
    /// certification: namespaces and recovery cursors must also be fenced.
    CheckpointCommittable,
}

/// How a sink may be placed across runtime nodes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum SinkTopology {
    /// Only one fenced runtime writer may target the configured destination.
    #[default]
    Singleton,
    /// Independent runtime writers can safely target the destination.
    MultiWriter,
    /// Each runtime node owns a distinct local egress endpoint or audience.
    NodeLocalEgress,
}

/// The strongest input update model a configured sink understands.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum SinkInputMode {
    /// Inserts only; retractions or deletes would be lost.
    #[default]
    AppendOnly,
    /// Rows are reconciled by a configured key, but the connector does not
    /// consume a native full-changelog envelope.
    KeyedUpsert,
    /// Inserts, updates, and deletes/retractions are represented faithfully.
    FullChangelog,
}

impl SinkInputMode {
    /// Whether this mode can faithfully consume a full Z-set changelog,
    /// including deletes/retractions.
    #[must_use]
    pub const fn accepts_full_changelog(self) -> bool {
        matches!(self, Self::FullChangelog)
    }
}

/// Complete sink admission contract for a concrete connector configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct SinkContract {
    /// Durability and checkpoint-commit semantics.
    pub consistency: SinkConsistency,
    /// Valid runtime placement model.
    pub topology: SinkTopology,
    /// Strongest supported input update model.
    pub input_mode: SinkInputMode,
    /// True only for a built-in sink whose immutable phase-one and fenced
    /// external cursor protocol is certified for multi-node exact delivery.
    cluster_exact_delivery_certified: bool,
}

impl SinkContract {
    /// Construct a sink contract from its three explicit dimensions.
    #[must_use]
    pub const fn new(
        consistency: SinkConsistency,
        topology: SinkTopology,
        input_mode: SinkInputMode,
    ) -> Self {
        Self {
            consistency,
            topology,
            input_mode,
            cluster_exact_delivery_certified: false,
        }
    }

    /// Mark a built-in sink whose cluster exact-delivery protocol is a release gate.
    #[must_use]
    pub(crate) const fn with_cluster_exact_delivery_certification(mut self) -> Self {
        self.cluster_exact_delivery_certified = true;
        self
    }

    /// Whether this sink's complete multi-node exact-delivery protocol is certified.
    #[doc(hidden)]
    #[must_use]
    pub const fn is_cluster_exact_delivery_certified(self) -> bool {
        self.cluster_exact_delivery_certified
    }

    /// Whether this contract participates in checkpoint-owned external commit.
    #[must_use]
    pub const fn is_checkpoint_committable(self) -> bool {
        matches!(self.consistency, SinkConsistency::CheckpointCommittable)
    }

    /// Whether this contract faithfully consumes inserts, updates, and retractions.
    #[must_use]
    pub const fn accepts_full_changelog(self) -> bool {
        self.input_mode.accepts_full_changelog()
    }
}
