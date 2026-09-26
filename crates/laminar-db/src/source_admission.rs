//! Native managed-source admission identity and committed checkpoint correlation.
//!
//! Purpose: expose a typed admission receipt for a batch accepted into a managed
//! push source, plus the committed source barrier that proves an existing native
//! checkpoint captured that source instance's input progress. `LaminarDB` owns
//! the checkpoint; this module owns only the identity/coordinate vocabulary.
//!
//! Boundary: no payload body is opened, buffered, chunked, proxied, parsed, or
//! transformed here. The Arrow batch moves through the existing source channel
//! unchanged and this module records only its native admission coordinate.
//!
//! Users: [`crate::UntypedSourceHandle::push_arrow_receipted`],
//! [`crate::LaminarDB::checkpoint_through`], [`crate::LaminarDB::checkpoint_current`]
//! and downstream native live-edge adapters.
//!
//! Do not use these values to derive progress from output rows, queue length,
//! epochs, wall clocks, or append positions. The ordered input offset is the
//! native admission ordinal of a pushed batch.
//!
//! Limitation / drift trigger: the committed coordinate is captured through the
//! existing checkpoint manifest `source_offsets[*].metadata`. If checkpoint
//! capture stops carrying that metadata, `checkpoint_through`/`checkpoint_current`
//! fail closed with [`SourceAdmissionError::CheckpointUnavailable`] rather than
//! inventing a coordinate.

use std::sync::atomic::{AtomicU64, Ordering};

/// Checkpoint metadata key carrying the managed source instance identity.
pub const SOURCE_INSTANCE_METADATA_KEY: &str = "laminar.managed_source.source_instance";

/// Checkpoint metadata key carrying the committed ordered input offset.
///
/// The value is the decimal native admission ordinal captured at barrier time.
pub const ORDERED_INPUT_OFFSET_METADATA_KEY: &str = "laminar.managed_source.ordered_input_offset";

/// Native-issued identity of one managed source instance.
///
/// The identity is stable for one process generation and always fresh after a
/// restart: a new process issues a new instance for the same catalog name. It is
/// deliberately opaque; never parse it to recover the source name or ordinal.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SourceInstance(String);

impl SourceInstance {
    /// Issue a native instance identity for a catalog source.
    ///
    /// `boot_nonce` distinguishes catalog/process generations; `ordinal` distinguishes
    /// registrations within one catalog. Both are supplied by the catalog owner
    /// so this constructor stays deterministic and side-effect free.
    #[must_use]
    pub(crate) fn issue(catalog_name: &str, boot_nonce: uuid::Uuid, ordinal: u64) -> Self {
        Self(format!("{catalog_name}#{boot_nonce}.{ordinal:016x}"))
    }

    /// Opaque string form used in checkpoint metadata and wire contracts.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for SourceInstance {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Monotonic native input coordinate: the ordinal of an admitted batch.
///
/// Offset `0` is the empty-source baseline. A nonzero offset is only produced by
/// a real successful native admission.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OrderedInputOffset(u64);

impl OrderedInputOffset {
    /// The empty-source baseline offset.
    pub const ZERO: Self = Self(0);

    #[must_use]
    pub(crate) const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Raw ordinal value.
    #[must_use]
    pub const fn value(self) -> u64 {
        self.0
    }

    /// Whether this committed offset includes the `required` admission.
    #[must_use]
    pub const fn covers(self, required: Self) -> bool {
        self.0 >= required.0
    }
}

/// Receipt for a batch admitted into a managed native source.
///
/// Created only after the native source accepted the batch. A failed or
/// backpressured enqueue produces no receipt.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceAdmissionReceipt {
    source_instance: SourceInstance,
    ordered_input_offset: OrderedInputOffset,
}

impl SourceAdmissionReceipt {
    /// Construct a receipt from a proven native admission.
    ///
    /// Private to the crate: callers receive receipts from
    /// [`crate::UntypedSourceHandle::push_arrow_receipted`], never by fabricating
    /// an instance/offset pair.
    #[must_use]
    pub(crate) const fn from_native_admission(
        source_instance: SourceInstance,
        ordered_input_offset: OrderedInputOffset,
    ) -> Self {
        Self {
            source_instance,
            ordered_input_offset,
        }
    }

    /// The exact native source instance that admitted the batch.
    #[must_use]
    pub const fn source_instance(&self) -> &SourceInstance {
        &self.source_instance
    }

    /// The exact native admission ordinal for the batch.
    #[must_use]
    pub const fn ordered_input_offset(&self) -> OrderedInputOffset {
        self.ordered_input_offset
    }
}

/// A committed native checkpoint's captured progress for one source instance.
///
/// Carries the real committed checkpoint identity (`checkpoint_id`, `epoch`)
/// together with the captured source instance and its ordered input offset.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommittedSourceBarrier {
    source_instance: SourceInstance,
    ordered_input_offset: OrderedInputOffset,
    checkpoint_id: u64,
    epoch: u64,
}

impl CommittedSourceBarrier {
    #[must_use]
    pub(crate) const fn new(
        source_instance: SourceInstance,
        ordered_input_offset: OrderedInputOffset,
        checkpoint_id: u64,
        epoch: u64,
    ) -> Self {
        Self {
            source_instance,
            ordered_input_offset,
            checkpoint_id,
            epoch,
        }
    }

    /// Captured native source instance.
    #[must_use]
    pub const fn source_instance(&self) -> &SourceInstance {
        &self.source_instance
    }

    /// Captured native ordered input offset.
    #[must_use]
    pub const fn ordered_input_offset(&self) -> OrderedInputOffset {
        self.ordered_input_offset
    }

    /// Committed native checkpoint id.
    #[must_use]
    pub const fn checkpoint_id(&self) -> u64 {
        self.checkpoint_id
    }

    /// Committed native checkpoint epoch.
    #[must_use]
    pub const fn epoch(&self) -> u64 {
        self.epoch
    }
}

/// Typed managed-source admission failures.
///
/// Branch on these variants, never on message text.
#[derive(Debug, thiserror::Error)]
pub enum SourceAdmissionError {
    /// No committed checkpoint captured the requested native source instance.
    #[error("managed source instance '{0}' is not present in the committed checkpoint roster")]
    SourceNotManaged(String),
    /// No committed native checkpoint is available to correlate against.
    #[error("native checkpoint unavailable: {0}")]
    CheckpointUnavailable(String),
    /// The committed checkpoint captured a smaller offset than the receipt.
    ///
    /// The caller must retry the exact idempotent checkpoint request; this is not
    /// a fabricated success.
    #[error("committed checkpoint offset {committed} does not cover required offset {required}")]
    OffsetNotCovered {
        /// Admission ordinal the caller required to be covered.
        required: u64,
        /// Admission ordinal actually captured by the committed checkpoint.
        committed: u64,
    },
}

/// Per-source native admission counter shared by the push handle and the pipeline.
///
/// Kept as a distinct type so an admitted ordinal cannot be confused with a
/// checkpoint id, epoch, or reservation value.
#[derive(Debug, Default)]
pub(crate) struct AdmittedInputCounter {
    next: AtomicU64,
}

impl AdmittedInputCounter {
    /// Reserve the next ordinal for a successfully admitted batch.
    pub(crate) fn next_offset(&self) -> OrderedInputOffset {
        OrderedInputOffset::new(self.next.fetch_add(1, Ordering::AcqRel) + 1)
    }

    /// Current committed admission ordinal.
    pub(crate) fn current(&self) -> OrderedInputOffset {
        OrderedInputOffset::new(self.next.load(Ordering::Acquire))
    }
}
