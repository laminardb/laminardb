//! Connector traits — async `SourceConnector` / `SinkConnector`.

use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Weak};

use arrow_array::{
    Array, ArrayRef, BinaryArray, RecordBatch, RecordBatchOptions, UInt32Array, UInt8Array,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use sha2::{Digest, Sha256};
use tokio::sync::Notify;

use crate::checkpoint::SourceCheckpoint;
use crate::config::ConnectorConfig;
use crate::error::ConnectorError;

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

/// Reserved column carrying the source partition bytes.
pub const SOURCE_PARTITION_COLUMN: &str = "__source_partition";
/// Reserved column carrying an order-preserving source cursor.
pub const SOURCE_ORDER_KEY_COLUMN: &str = "__source_order_key";
/// Reserved column carrying a row ordinal within one source cursor.
pub const SOURCE_SUB_OFFSET_COLUMN: &str = "__source_sub_offset";
/// Reserved column carrying row-level source mutations for stateful operators.
#[doc(hidden)]
pub const SOURCE_MUTATION_COLUMN: &str = "__source_mutation";

const SOURCE_POSITION_COLUMNS: [&str; 3] = [
    SOURCE_PARTITION_COLUMN,
    SOURCE_ORDER_KEY_COLUMN,
    SOURCE_SUB_OFFSET_COLUMN,
];
const SOURCE_METADATA_COLUMNS: [&str; 4] = [
    SOURCE_MUTATION_COLUMN,
    SOURCE_PARTITION_COLUMN,
    SOURCE_ORDER_KEY_COLUMN,
    SOURCE_SUB_OFFSET_COLUMN,
];

/// Append the reserved row-position fields to a connector's declared schema.
///
/// # Errors
/// Returns an error when the declared schema already uses a reserved field name.
pub fn schema_with_source_row_positions(schema: &SchemaRef) -> Result<SchemaRef, ConnectorError> {
    schema_with_source_metadata(schema, false)
}

/// Append the mixed-mutation field and trailing row-position fields to a declared schema.
///
/// # Errors
/// Returns an error when the declared schema already uses a reserved field name.
pub fn schema_with_source_mutations_and_row_positions(
    schema: &SchemaRef,
) -> Result<SchemaRef, ConnectorError> {
    schema_with_source_metadata(schema, true)
}

fn schema_with_source_metadata(
    schema: &SchemaRef,
    include_mutations: bool,
) -> Result<SchemaRef, ConnectorError> {
    if let Some(field) = schema.fields().iter().find(|field| {
        SOURCE_METADATA_COLUMNS
            .iter()
            .any(|reserved| field.name().eq_ignore_ascii_case(reserved))
    }) {
        return Err(ConnectorError::SchemaMismatch(format!(
            "source schema contains reserved metadata column '{}'",
            field.name()
        )));
    }

    let mut fields = schema.fields().to_vec();
    if include_mutations {
        fields.push(Arc::new(Field::new(
            SOURCE_MUTATION_COLUMN,
            DataType::UInt8,
            false,
        )));
    }
    fields.extend([
        Arc::new(Field::new(SOURCE_PARTITION_COLUMN, DataType::Binary, false)),
        Arc::new(Field::new(SOURCE_ORDER_KEY_COLUMN, DataType::Binary, false)),
        Arc::new(Field::new(
            SOURCE_SUB_OFFSET_COLUMN,
            DataType::UInt32,
            false,
        )),
    ]);
    Ok(Arc::new(Schema::new_with_metadata(
        fields,
        schema.metadata().clone(),
    )))
}

#[derive(Clone, Copy)]
struct SourceMetadataLayout {
    visible_columns: usize,
    has_mutations: bool,
}

fn source_metadata_layout(schema: &Schema) -> Result<Option<SourceMetadataLayout>, ConnectorError> {
    let fields = schema.fields();
    if !fields.iter().any(|field| {
        SOURCE_METADATA_COLUMNS
            .iter()
            .any(|reserved| field.name().eq_ignore_ascii_case(reserved))
    }) {
        return Ok(None);
    }

    let position_start = fields
        .len()
        .checked_sub(SOURCE_POSITION_COLUMNS.len())
        .ok_or_else(|| {
            ConnectorError::SchemaMismatch(
                "source metadata must end with the exact three row-position fields".into(),
            )
        })?;
    let expected_positions = [
        (SOURCE_PARTITION_COLUMN, DataType::Binary),
        (SOURCE_ORDER_KEY_COLUMN, DataType::Binary),
        (SOURCE_SUB_OFFSET_COLUMN, DataType::UInt32),
    ];
    for (field, (name, data_type)) in fields[position_start..].iter().zip(expected_positions) {
        if field.name() != name || field.data_type() != &data_type || field.is_nullable() {
            return Err(ConnectorError::SchemaMismatch(
                "source metadata must end with the exact three typed row-position fields".into(),
            ));
        }
    }

    let has_mutations = position_start != 0
        && fields[position_start - 1]
            .name()
            .eq_ignore_ascii_case(SOURCE_MUTATION_COLUMN);
    let visible_columns = position_start - usize::from(has_mutations);
    if has_mutations {
        let field = &fields[visible_columns];
        if field.name() != SOURCE_MUTATION_COLUMN
            || field.data_type() != &DataType::UInt8
            || field.is_nullable()
        {
            return Err(ConnectorError::SchemaMismatch(
                "source mutation metadata must be the exact non-null UInt8 field immediately before row positions"
                    .into(),
            ));
        }
    }
    if fields[..visible_columns].iter().any(|field| {
        SOURCE_METADATA_COLUMNS
            .iter()
            .any(|reserved| field.name().eq_ignore_ascii_case(reserved))
    }) {
        return Err(ConnectorError::SchemaMismatch(
            "source metadata fields are reserved and must use their exact trailing positions"
                .into(),
        ));
    }

    Ok(Some(SourceMetadataLayout {
        visible_columns,
        has_mutations,
    }))
}

fn validate_source_position_arrays(
    records: &RecordBatch,
    layout: SourceMetadataLayout,
) -> Result<(), ConnectorError> {
    let position_start = layout.visible_columns + usize::from(layout.has_mutations);
    let partition = records.columns()[position_start]
        .as_any()
        .downcast_ref::<BinaryArray>();
    let order = records.columns()[position_start + 1]
        .as_any()
        .downcast_ref::<BinaryArray>();
    let sub_offset = records.columns()[position_start + 2]
        .as_any()
        .downcast_ref::<UInt32Array>();
    let (Some(partition), Some(order), Some(sub_offset)) = (partition, order, sub_offset) else {
        return Err(ConnectorError::SchemaMismatch(
            "source row-position metadata arrays have invalid types".into(),
        ));
    };
    if partition.len() != records.num_rows()
        || order.len() != records.num_rows()
        || sub_offset.len() != records.num_rows()
    {
        return Err(ConnectorError::SchemaMismatch(
            "source row-position metadata is not row-aligned".into(),
        ));
    }
    if partition.null_count() != 0 || order.null_count() != 0 || sub_offset.null_count() != 0 {
        return Err(ConnectorError::SchemaMismatch(
            "source row-position metadata must not contain nulls".into(),
        ));
    }
    Ok(())
}

fn validate_encoded_source_schema(
    visible: &Schema,
    encoded: &Schema,
    expect_mutations: bool,
) -> Result<(), ConnectorError> {
    let layout = source_metadata_layout(encoded)?.ok_or_else(|| {
        ConnectorError::SchemaMismatch("encoded source schema is missing row positions".into())
    })?;
    if layout.has_mutations != expect_mutations
        || encoded.fields()[..layout.visible_columns] != visible.fields()[..]
        || encoded.metadata() != visible.metadata()
    {
        return Err(ConnectorError::SchemaMismatch(
            "encoded source schema does not match its visible schema and metadata contract".into(),
        ));
    }
    Ok(())
}

/// Validated borrowed access to row-aligned source mutations.
#[derive(Debug, Clone, Copy)]
pub struct SourceMutationView<'a> {
    values: &'a UInt8Array,
}

impl<'a> SourceMutationView<'a> {
    /// Number of row mutations.
    #[must_use]
    pub fn len(self) -> usize {
        self.values.len()
    }

    /// Whether the batch has no row mutations.
    #[must_use]
    pub fn is_empty(self) -> bool {
        self.values.is_empty()
    }

    /// Mutation for `row`, if it is in bounds.
    #[must_use]
    pub fn get(self, row: usize) -> Option<SourceMutation> {
        self.values.values().get(row).map(|value| match *value {
            0 => SourceMutation::Put,
            1 => SourceMutation::Tombstone,
            _ => unreachable!("SourceMutationView is validated when constructed"),
        })
    }
}

/// Borrow the optional row-aligned mutation metadata after validating it.
///
/// # Errors
/// Returns an error for malformed metadata, nulls, unknown values, or a noncanonical all-put
/// mutation column.
pub fn source_mutations(
    records: &RecordBatch,
) -> Result<Option<SourceMutationView<'_>>, ConnectorError> {
    source_mutations_validated(records, false)
}

/// Borrow mutations from a slice derived from a strictly validated routed batch.
///
/// Unlike ingress validation, this permits a retained mutation column whose slice contains only
/// puts. Layout, alignment, types, nulls, and values remain validated.
///
/// # Errors
/// Returns an error for malformed mutation or row-position metadata.
pub fn source_mutations_routed(
    records: &RecordBatch,
) -> Result<Option<SourceMutationView<'_>>, ConnectorError> {
    source_mutations_validated(records, true)
}

fn source_mutations_validated(
    records: &RecordBatch,
    allow_all_put: bool,
) -> Result<Option<SourceMutationView<'_>>, ConnectorError> {
    let Some(layout) = source_metadata_layout(records.schema().as_ref())? else {
        return Ok(None);
    };
    validate_source_position_arrays(records, layout)?;
    if !layout.has_mutations {
        return Ok(None);
    }
    let mutations = records
        .column(layout.visible_columns)
        .as_any()
        .downcast_ref::<UInt8Array>()
        .ok_or_else(|| {
            ConnectorError::SchemaMismatch("source mutation metadata array is not UInt8".into())
        })?;
    if mutations.len() != records.num_rows() {
        return Err(ConnectorError::SchemaMismatch(format!(
            "source mutation count {} does not match decoded row count {}",
            mutations.len(),
            records.num_rows()
        )));
    }
    if mutations.null_count() != 0 {
        return Err(ConnectorError::SchemaMismatch(
            "source mutation metadata must not contain nulls".into(),
        ));
    }
    let mut has_tombstone = false;
    for &value in mutations.values() {
        match value {
            0 => {}
            1 => has_tombstone = true,
            value => {
                return Err(ConnectorError::SchemaMismatch(format!(
                    "source mutation metadata contains unknown value {value}"
                )));
            }
        }
    }
    if !has_tombstone && !allow_all_put {
        return Err(ConnectorError::SchemaMismatch(
            "all-put source mutations must omit the mutation metadata field".into(),
        ));
    }
    Ok(Some(SourceMutationView { values: mutations }))
}

/// Remove only mutation metadata while retaining the exact trailing row positions.
///
/// # Errors
/// Returns an error when any source metadata field is malformed.
pub fn strip_source_mutations(records: &RecordBatch) -> Result<RecordBatch, ConnectorError> {
    strip_source_mutations_validated(records, false)
}

/// Remove mutation metadata from a slice derived from a strictly validated routed batch.
///
/// # Errors
/// Returns an error when any source metadata field is malformed.
pub fn strip_source_mutations_routed(records: &RecordBatch) -> Result<RecordBatch, ConnectorError> {
    strip_source_mutations_validated(records, true)
}

fn strip_source_mutations_validated(
    records: &RecordBatch,
    allow_all_put: bool,
) -> Result<RecordBatch, ConnectorError> {
    let schema = records.schema();
    let Some(layout) = source_metadata_layout(schema.as_ref())? else {
        return Ok(records.clone());
    };
    source_mutations_validated(records, allow_all_put)?;
    if !layout.has_mutations {
        return Ok(records.clone());
    }
    let mut fields = schema.fields()[..layout.visible_columns].to_vec();
    fields.extend(
        schema.fields()[layout.visible_columns + 1..]
            .iter()
            .cloned(),
    );
    let mut columns = records.columns()[..layout.visible_columns].to_vec();
    columns.extend(
        records.columns()[layout.visible_columns + 1..]
            .iter()
            .cloned(),
    );
    let options = RecordBatchOptions::new().with_row_count(Some(records.num_rows()));
    RecordBatch::try_new_with_options(
        Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone())),
        columns,
        &options,
    )
    .map_err(|error| {
        ConnectorError::SchemaMismatch(format!("failed to strip source mutation metadata: {error}"))
    })
}

/// Remove all connector metadata without copying visible Arrow buffers.
///
/// Batches without reserved fields are returned unchanged. Mutation metadata, when present, must
/// be immediately before the exact trailing row-position fields.
///
/// # Errors
/// Returns an error for partial, misplaced, nullable, incorrectly typed, or invalid metadata.
pub fn strip_source_row_positions(records: &RecordBatch) -> Result<RecordBatch, ConnectorError> {
    let schema = records.schema();
    let Some(layout) = source_metadata_layout(schema.as_ref())? else {
        return Ok(records.clone());
    };
    source_mutations(records)?;
    let visible_schema = Arc::new(Schema::new_with_metadata(
        schema.fields()[..layout.visible_columns].to_vec(),
        schema.metadata().clone(),
    ));
    let options = RecordBatchOptions::new().with_row_count(Some(records.num_rows()));
    RecordBatch::try_new_with_options(
        visible_schema,
        records.columns()[..layout.visible_columns].to_vec(),
        &options,
    )
    .map_err(|error| {
        ConnectorError::SchemaMismatch(format!("failed to strip source metadata: {error}"))
    })
}

/// Deterministic source coordinates aligned one-for-one with decoded rows.
#[derive(Debug, Clone)]
pub struct SourceRowPositions {
    partition: BinaryArray,
    order_key: BinaryArray,
    sub_offset: UInt32Array,
}

impl SourceRowPositions {
    /// Construct a validated row-position sidecar.
    ///
    /// # Errors
    /// Returns an error when arrays differ in length or contain nulls.
    pub fn try_new(
        partition: BinaryArray,
        order_key: BinaryArray,
        sub_offset: UInt32Array,
    ) -> Result<Self, ConnectorError> {
        let len = partition.len();
        if order_key.len() != len || sub_offset.len() != len {
            return Err(ConnectorError::SchemaMismatch(format!(
                "source row-position arrays have different lengths: partition={len}, order={}, sub_offset={}",
                order_key.len(),
                sub_offset.len()
            )));
        }
        if partition.null_count() != 0
            || order_key.null_count() != 0
            || sub_offset.null_count() != 0
        {
            return Err(ConnectorError::SchemaMismatch(
                "source row-position arrays must not contain nulls".into(),
            ));
        }
        Ok(Self {
            partition,
            order_key,
            sub_offset,
        })
    }

    fn len(&self) -> usize {
        self.partition.len()
    }

    /// Partition coordinate for each row.
    #[must_use]
    pub const fn partition(&self) -> &BinaryArray {
        &self.partition
    }

    /// Order-preserving cursor for each row.
    #[must_use]
    pub const fn order_key(&self) -> &BinaryArray {
        &self.order_key
    }

    /// Row ordinal within one source cursor.
    #[must_use]
    pub const fn sub_offset(&self) -> &UInt32Array {
        &self.sub_offset
    }

    fn validate_row_count(&self, rows: usize) -> Result<(), ConnectorError> {
        if self.len() == rows {
            Ok(())
        } else {
            Err(ConnectorError::SchemaMismatch(format!(
                "source row-position count {} does not match decoded row count {rows}",
                self.len()
            )))
        }
    }

    fn append_metadata(
        self,
        records: &RecordBatch,
        encoded_schema: &SchemaRef,
        mutations: Option<&[SourceMutation]>,
    ) -> Result<RecordBatch, ConnectorError> {
        self.validate_row_count(records.num_rows())?;
        validate_encoded_source_schema(
            records.schema().as_ref(),
            encoded_schema.as_ref(),
            mutations.is_some(),
        )?;
        if let Some(mutations) = mutations {
            if mutations.len() != records.num_rows() {
                return Err(ConnectorError::SchemaMismatch(format!(
                    "source mutation count {} does not match decoded row count {}",
                    mutations.len(),
                    records.num_rows()
                )));
            }
            if !mutations.contains(&SourceMutation::Tombstone) {
                return Err(ConnectorError::SchemaMismatch(
                    "all-put source mutations must omit the mutation metadata field".into(),
                ));
            }
        }
        let mut columns = records.columns().to_vec();
        if let Some(mutations) = mutations {
            columns.push(Arc::new(UInt8Array::from_iter_values(
                mutations.iter().map(|mutation| match mutation {
                    SourceMutation::Put => 0,
                    SourceMutation::Tombstone => 1,
                }),
            )));
        }
        columns.extend([
            Arc::new(self.partition) as ArrayRef,
            Arc::new(self.order_key) as ArrayRef,
            Arc::new(self.sub_offset) as ArrayRef,
        ]);
        RecordBatch::try_new(Arc::clone(encoded_schema), columns).map_err(|error| {
            ConnectorError::SchemaMismatch(format!("failed to append source metadata: {error}"))
        })
    }
}

/// Canonical mutation applied by a stateful operator for one source row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceMutation {
    /// Insert or replace the keyed value.
    Put,
    /// Remove the keyed value.
    Tombstone,
}

/// A batch of records read from a source connector.
#[derive(Debug, Clone)]
pub struct SourceBatch {
    /// Arrow batch carrying the records.
    pub records: RecordBatch,
    row_positions: Option<SourceRowPositions>,
    mutations: Option<Box<[SourceMutation]>>,
}

impl SourceBatch {
    /// Construct a source batch.
    #[must_use]
    pub fn new(records: RecordBatch) -> Self {
        Self {
            records,
            row_positions: None,
            mutations: None,
        }
    }

    /// Construct a batch with deterministic positions for every decoded row.
    ///
    /// # Errors
    /// Returns an error when the sidecar is not row-aligned with `records`.
    pub fn positioned(
        records: RecordBatch,
        row_positions: SourceRowPositions,
    ) -> Result<Self, ConnectorError> {
        row_positions.validate_row_count(records.num_rows())?;
        Ok(Self {
            records,
            row_positions: Some(row_positions),
            mutations: None,
        })
    }

    /// Attach row-aligned mutations to a batch.
    ///
    /// All-`Put` input is canonicalized to the default. Connectors should call this only for
    /// batches that may contain tombstones.
    ///
    /// # Errors
    /// Returns an error when the mutation count differs from the decoded row count.
    pub fn with_mutations(
        mut self,
        mutations: impl Into<Box<[SourceMutation]>>,
    ) -> Result<Self, ConnectorError> {
        let mutations = mutations.into();
        if mutations.len() != self.records.num_rows() {
            return Err(ConnectorError::SchemaMismatch(format!(
                "source mutation count {} does not match decoded row count {}",
                mutations.len(),
                self.records.num_rows()
            )));
        }
        self.mutations = mutations
            .contains(&SourceMutation::Tombstone)
            .then_some(mutations);
        Ok(self)
    }

    /// Deterministic row positions, when supplied by the connector.
    #[must_use]
    pub const fn row_positions(&self) -> Option<&SourceRowPositions> {
        self.row_positions.as_ref()
    }

    /// Mixed row mutations, or `None` when every row is a [`SourceMutation::Put`].
    #[must_use]
    pub fn mutations(&self) -> Option<&[SourceMutation]> {
        self.mutations.as_deref()
    }

    /// Validate and append optional mixed mutations plus deterministic row positions.
    ///
    /// The mutation field is omitted for the canonical all-`Put` case.
    ///
    /// # Errors
    /// Returns an error when capability, schema, or row-aligned metadata is malformed.
    pub fn into_records_with_metadata(
        self,
        capability: SourceRowPositionCapability,
        positioned_schema: &SchemaRef,
        mutation_schema: &SchemaRef,
    ) -> Result<RecordBatch, ConnectorError> {
        let Self {
            records,
            row_positions,
            mutations,
        } = self;
        match (capability, row_positions) {
            (SourceRowPositionCapability::Unavailable, None) if mutations.is_none() => Ok(records),
            (SourceRowPositionCapability::Unavailable, _) => Err(ConnectorError::SchemaMismatch(
                "source emitted state metadata without declaring deterministic row positions"
                    .into(),
            )),
            (SourceRowPositionCapability::OrderedDeterministic, None) => {
                Err(ConnectorError::SchemaMismatch(
                    "source declared deterministic row positions but omitted the sidecar".into(),
                ))
            }
            (SourceRowPositionCapability::OrderedDeterministic, Some(positions)) => {
                let schema = if mutations.is_some() {
                    mutation_schema
                } else {
                    positioned_schema
                };
                positions.append_metadata(&records, schema, mutations.as_deref())
            }
        }
    }

    /// Record count in the batch.
    #[must_use]
    pub fn num_rows(&self) -> usize {
        self.records.num_rows()
    }
}

/// Summary of a successful `write_batch` call.
#[derive(Debug, Clone)]
pub struct WriteResult {
    /// Records accepted by the sink.
    pub records_written: usize,
    /// Bytes written to the underlying transport (may be estimated).
    pub bytes_written: u64,
}

impl WriteResult {
    /// Construct with raw counts.
    #[must_use]
    pub fn new(records_written: usize, bytes_written: u64) -> Self {
        Self {
            records_written,
            bytes_written,
        }
    }
}

/// What the runtime must do when a started connector operation is cancelled.
///
/// This is an internal connector/driver capability, not a deployment option.
/// Cancellation always respects the runtime-owned deadline. A connector may be
/// reused only when dropping the exact future is known to preserve its state;
/// otherwise the runtime retires the complete connector generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectorCancellationPolicy {
    /// Dropping an in-flight future leaves the connector valid for recovery or reuse.
    CancelSafe,
    /// Dropping an in-flight future may leave its external outcome unknown, so
    /// the connector instance must not process another operation.
    RetireConnector,
}

const CONNECTOR_TASK_OWNER_DROPPED: usize = 1usize << (usize::BITS - 1);

struct ConnectorTaskState {
    state: AtomicUsize,
    terminated: Notify,
}

/// Sole admission authority for detached tasks owned by one connector generation.
///
/// The owner must live inside the connector. Dropping it seals the generation so
/// terminal completion can be observed after every admitted task guard is gone.
pub struct ConnectorTaskOwner {
    inner: Arc<ConnectorTaskState>,
}

/// Cloneable, non-owning admission handle for dynamically spawned connector tasks.
///
/// The handle does not keep the task generation open. Admission fails after
/// the sole [`ConnectorTaskOwner`] is dropped, including when existing task
/// guards still keep the generation observable.
#[derive(Clone)]
pub struct ConnectorTaskAdmission {
    inner: Weak<ConnectorTaskState>,
}

/// Cloneable observer for terminal completion of one connector generation.
#[derive(Clone)]
pub struct ConnectorTaskTracker {
    inner: Arc<ConnectorTaskState>,
}

/// RAII proof that one connector-owned task is still active.
///
/// Move the guard into the task before spawning it and retain it for the task's
/// full lifetime.
#[must_use = "dropping the guard marks its connector task complete"]
pub struct ConnectorTaskGuard {
    inner: Arc<ConnectorTaskState>,
}

impl ConnectorTaskOwner {
    /// Create the sole task owner and its cloneable terminal observer.
    #[must_use]
    pub fn new() -> (Self, ConnectorTaskTracker) {
        let inner = Arc::new(ConnectorTaskState {
            state: AtomicUsize::new(0),
            terminated: Notify::new(),
        });
        (
            Self {
                inner: Arc::clone(&inner),
            },
            ConnectorTaskTracker { inner },
        )
    }

    /// Create a non-owning admission handle for tasks discovered by owned work.
    ///
    /// This is intended for accept loops and similar dynamic task producers.
    /// Cloning the handle never extends the connector generation's admission
    /// lifetime.
    #[must_use]
    pub fn admission(&self) -> ConnectorTaskAdmission {
        ConnectorTaskAdmission {
            inner: Arc::downgrade(&self.inner),
        }
    }

    /// Admit one task into this live connector generation.
    ///
    /// The returned guard must be created before the task is spawned and moved
    /// into that task. `None` means the generation can no longer admit work.
    #[must_use]
    pub fn track(&self) -> Option<ConnectorTaskGuard> {
        track_connector_task(&self.inner)
    }
}

impl ConnectorTaskAdmission {
    /// Admit one dynamic task while the connector generation remains open.
    ///
    /// Returns `None` once the sole owner has been dropped. A successful
    /// admission remains tracked until its returned guard is dropped.
    #[must_use]
    pub fn track(&self) -> Option<ConnectorTaskGuard> {
        let inner = self.inner.upgrade()?;
        track_connector_task(&inner)
    }
}

fn track_connector_task(inner: &Arc<ConnectorTaskState>) -> Option<ConnectorTaskGuard> {
    let mut observed = inner.state.load(Ordering::Acquire);
    loop {
        if observed & CONNECTOR_TASK_OWNER_DROPPED != 0 {
            return None;
        }
        let next = observed.checked_add(1)?;
        if next & CONNECTOR_TASK_OWNER_DROPPED != 0 {
            return None;
        }
        match inner
            .state
            .compare_exchange_weak(observed, next, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => {
                return Some(ConnectorTaskGuard {
                    inner: Arc::clone(inner),
                });
            }
            Err(actual) => observed = actual,
        }
    }
}

impl Drop for ConnectorTaskOwner {
    fn drop(&mut self) {
        let previous = self
            .inner
            .state
            .fetch_or(CONNECTOR_TASK_OWNER_DROPPED, Ordering::AcqRel);
        debug_assert_eq!(previous & CONNECTOR_TASK_OWNER_DROPPED, 0);
        if previous == 0 {
            self.inner.terminated.notify_waiters();
        }
    }
}

impl ConnectorTaskTracker {
    /// Whether the owner and all task guards have been dropped.
    #[must_use]
    pub fn is_terminated(&self) -> bool {
        self.inner.state.load(Ordering::Acquire) == CONNECTOR_TASK_OWNER_DROPPED
    }

    /// Wait until the owner and all task guards have been dropped.
    pub async fn wait_terminated(&self) {
        loop {
            let notified = self.inner.terminated.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.is_terminated() {
                return;
            }
            notified.await;
        }
    }
}

impl Drop for ConnectorTaskGuard {
    fn drop(&mut self) {
        let previous = self.inner.state.fetch_sub(1, Ordering::AcqRel);
        debug_assert_ne!(previous & !CONNECTOR_TASK_OWNER_DROPPED, 0);
        if previous == CONNECTOR_TASK_OWNER_DROPPED | 1 {
            self.inner.terminated.notify_waiters();
        }
    }
}

/// Atomic startup position for a source connector.
///
/// A resume request carries both the durable checkpoint attempt and the
/// connector checkpoint captured by that attempt. Connectors must install the
/// position before `start` returns and before `poll_batch` can emit records.
#[derive(Debug, Clone)]
pub enum SourcePosition {
    /// Start from the connector's configured deterministic initial position.
    Initial,
    /// Resume from an exact durable engine checkpoint.
    Resume {
        /// Checkpoint attempt that owns the connector state.
        attempt: laminar_core::checkpoint::CheckpointAttempt,
        /// Connector cursor captured by `attempt`.
        checkpoint: SourceCheckpoint,
    },
}

/// Complete source startup request.
///
/// Startup is intentionally a single operation so a connector cannot become
/// externally active between opening resources and restoring its position.
#[derive(Debug, Clone)]
pub struct SourceStart {
    /// Fully resolved connector configuration.
    config: ConnectorConfig,
    /// Initial or exact recovery position.
    position: SourcePosition,
    /// Pipeline-wide delivery guarantee used for fail-closed cursor policy.
    delivery: DeliveryGuarantee,
}

impl SourceStart {
    /// Construct a source startup request before any connector I/O.
    ///
    /// # Errors
    /// Returns a configuration error when a resume attempt is zero or split across two identities.
    pub fn new(
        config: ConnectorConfig,
        position: SourcePosition,
        delivery: DeliveryGuarantee,
    ) -> Result<Self, ConnectorError> {
        if matches!(
            &position,
            SourcePosition::Resume { attempt, .. } if !attempt.is_canonical()
        ) {
            return Err(ConnectorError::ConfigurationError(
                "source resume must use one nonzero canonical checkpoint ID".into(),
            ));
        }
        Ok(Self {
            config,
            position,
            delivery,
        })
    }

    /// Consume the request into connector-owned startup inputs.
    #[must_use]
    pub fn into_parts(self) -> (ConnectorConfig, SourcePosition, DeliveryGuarantee) {
        (self.config, self.position, self.delivery)
    }
}

/// Exact cluster transition for which a source must stop advancing input.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceDrainRequest {
    /// Compact predecessor/target/leader identity.
    pub round: laminar_core::checkpoint::AssignmentDrainId,
}

impl SourceDrainRequest {
    /// Construct a canonical source drain request.
    ///
    /// # Errors
    /// Returns an error when the transition identity is not canonical.
    pub fn new(round: laminar_core::checkpoint::AssignmentDrainId) -> Result<Self, ConnectorError> {
        if !round.is_canonical() {
            return Err(ConnectorError::ConfigurationError(
                "source drain round is not canonical".into(),
            ));
        }
        Ok(Self { round })
    }
}

/// Terminal resolution of one exact source drain round.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceDrainOutcome {
    /// The target assignment committed and the source may adopt its target input ownership.
    Commit,
    /// The transition aborted and the source must resume from the predecessor cut.
    Abort,
}

/// Exact round resolution delivered to a source connector.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceDrainResolution {
    /// Round being resolved.
    pub round: laminar_core::checkpoint::AssignmentDrainId,
    /// Durable transition outcome.
    pub outcome: SourceDrainOutcome,
}

/// Trait for source connectors that read data from external systems.
///
/// Source connectors operate in Ring 1 and push data into Ring 0 via
/// the streaming `Source<ArrowRecord>::push_arrow()` API.
///
/// # Lifecycle
///
/// 1. `start()` — atomically install the configured or recovered cursor and initialize the reader
/// 2. `poll_batch()` — read batches in a loop
/// 3. `checkpoint()` — capture the current connector cursor
/// 4. `close()` — clean shutdown
#[async_trait]
pub trait SourceConnector: Send {
    /// Deadline behavior required by the underlying client implementation.
    ///
    /// Retirement is the conservative default: a new connector must not be
    /// reused after cancellation until every lifecycle future has been audited.
    fn cancellation_policy(&self) -> ConnectorCancellationPolicy {
        ConnectorCancellationPolicy::RetireConnector
    }

    /// Observe detached tasks whose lifetime may outlast this connector value.
    ///
    /// A connector that spawns detached work must retain the matching
    /// [`ConnectorTaskOwner`] and move a guard into every task. The runtime can
    /// then wait for true terminal completion after dropping the connector.
    fn terminal_task_tracker(&self) -> Option<ConnectorTaskTracker> {
        None
    }

    /// Opens the source and establishes its initial or resumed position as one
    /// indivisible lifecycle transition.
    ///
    /// Implementations must not emit records or expose an externally active
    /// consumer before the requested position has been applied successfully.
    async fn start(&mut self, request: SourceStart) -> Result<(), ConnectorError>;

    /// `Ok(None)` = no data currently available; runtime retries after a delay.
    /// `max_records` is the normal batching target. A source may exceed it only
    /// when one upstream atomic replay unit cannot be split without making its
    /// checkpoint cursor invalid. Such sources must enforce independent hard
    /// record and byte limits and fail before retained data can grow unbounded.
    ///
    /// The runtime may cancel this future at a shutdown or authority deadline.
    /// [`ConnectorCancellationPolicy::CancelSafe`] implementations must not
    /// advance external or checkpoint-visible position across an `.await`
    /// unless dropping the future there preserves replay of every
    /// not-yet-returned record. Stage work privately, then advance the cursor
    /// and return without another cancellation point. The conservative
    /// [`ConnectorCancellationPolicy::RetireConnector`] policy instead makes
    /// the complete connector generation terminal after cancellation.
    async fn poll_batch(
        &mut self,
        max_records: usize,
    ) -> Result<Option<SourceBatch>, ConnectorError>;

    /// Resolve the source schema from the connector and format properties before
    /// DDL reaches the planner. Implementations that perform network I/O must
    /// bound it with a timeout. Return `Err(ConnectorError::…)` on failure so
    /// the runtime can surface the cause to DDL — do not log and swallow.
    async fn discover_schema(
        &mut self,
        _properties: &std::collections::HashMap<String, String>,
    ) -> Result<(), ConnectorError> {
        Ok(())
    }

    /// Arrow schema of records this source produces.
    fn schema(&self) -> SchemaRef;

    /// Returned checkpoint must contain enough info to resume from the
    /// current position after a restart.
    fn checkpoint(&self) -> SourceCheckpoint;

    /// Atomically attempt to capture a source cursor. `Ok(None)` means a
    /// transient control-plane publication is not reconciled yet; the runtime
    /// retains the barrier and retries without advancing it.
    ///
    /// # Errors
    /// Returns a connector error when the source cannot produce a valid cursor for the current
    /// ownership/configuration state.
    fn try_checkpoint(&self) -> Result<Option<SourceCheckpoint>, ConnectorError> {
        Ok(Some(self.checkpoint()))
    }

    /// Whether this source's data-plane cursor is reconciled with the current
    /// control-plane ownership publication. The runtime does not poll records
    /// or admit checkpoint barriers while this is false.
    ///
    /// Non-partitioned and local sources are always ready. Cluster-aware
    /// sources override this with a lock-free version fence.
    ///
    /// # Errors
    /// Returns a connector error when control-plane reconciliation detects invalid or lost
    /// ownership state that cannot be retried safely.
    fn checkpoint_ready(&self) -> Result<bool, ConnectorError> {
        Ok(true)
    }

    /// Start or advance connector-side control-plane work needed to become
    /// [`checkpoint_ready`](Self::checkpoint_ready). Called even while data
    /// polling and barriers are fenced.
    fn drive_control_plane(&mut self) {}

    /// Stop advancing external input for an exact cluster transition.
    ///
    /// Implementations start the provider operation without blocking and later expose readiness
    /// through [`Self::poll_drain_ready`]. `deadline` is the engine-owned absolute deadline for
    /// this drain attempt; provider retries must not continue beyond it. Actor-only sources are
    /// fenced by the engine and never call this hook; assignment-scoped sources must implement it
    /// explicitly.
    ///
    /// # Errors
    /// Returns an error when the provider cannot start this exact drain round.
    fn begin_drain(
        &mut self,
        _request: &SourceDrainRequest,
        _deadline: tokio::time::Instant,
    ) -> Result<(), ConnectorError> {
        Err(ConnectorError::ConfigurationError(
            "assignment-scoped source does not implement provider drain".into(),
        ))
    }

    /// Whether the exact provider FIFO boundary has been consumed.
    ///
    /// Returns `false` while the reader is still pausing or while pre-boundary payloads remain.
    ///
    /// # Errors
    /// Returns an error when drain progress cannot be observed safely.
    fn poll_drain_ready(
        &mut self,
        _round: laminar_core::checkpoint::AssignmentDrainId,
    ) -> Result<bool, ConnectorError> {
        Err(ConnectorError::ConfigurationError(
            "assignment-scoped source does not expose provider drain readiness".into(),
        ))
    }

    /// Resolve an exact drain after target commit or abort.
    ///
    /// An abort must rewind any client-delivered but engine-unaccepted records before resuming.
    /// A commit must reconcile target ownership before clearing its post-cut filter.
    /// `deadline` is the engine-owned absolute deadline for the complete resolution; provider
    /// retries and blocking client calls must be bounded by its remaining time.
    async fn finish_drain(
        &mut self,
        _resolution: SourceDrainResolution,
        _deadline: tokio::time::Instant,
    ) -> Result<(), ConnectorError> {
        Err(ConnectorError::ConfigurationError(
            "assignment-scoped source does not implement provider drain resolution".into(),
        ))
    }

    /// Install the cluster vnode assignment for a source that advertises
    /// [`SourceTopology::Splittable`]. The source identity is the stable,
    /// canonical catalog object name and must be part of any external split
    /// mapping ABI.
    ///
    /// Embedded, single-node, singleton, and node-local sources are not sent
    /// this hook. The default fails closed so an extension cannot advertise
    /// splittable placement while every cluster node reads the full input.
    ///
    /// # Errors
    /// Returns a configuration error unless the connector implements exact
    /// vnode-scoped input ownership.
    fn set_vnode_assignment(
        &mut self,
        _source_identity: &str,
        _registry: Arc<laminar_core::state::VnodeRegistry>,
        _self_id: laminar_core::state::NodeId,
    ) -> Result<(), ConnectorError> {
        Err(ConnectorError::ConfigurationError(
            "source advertises splittable placement but does not implement vnode assignment".into(),
        ))
    }

    /// Close the connection and release resources.
    async fn close(&mut self) -> Result<(), ConnectorError>;

    /// Returns a [`Notify`] handle that is signalled when new data is available.
    ///
    /// When `Some`, the pipeline coordinator awaits the notification instead of
    /// polling on a timer, eliminating idle CPU usage. Push-driven sources should
    /// return `Some` and call `notify.notify_one()` when data arrives.
    ///
    /// The default implementation returns `None`, which causes the pipeline to
    /// fall back to timer-based polling (suitable for batch/file sources).
    fn data_ready_notify(&self) -> Option<Arc<Notify>> {
        None
    }

    /// Declare recovery and placement semantics for this exact configuration.
    ///
    /// # Errors
    ///
    /// Returns an error when the concrete configuration cannot provide a valid
    /// recovery, placement, or input contract.
    fn contract(&self, config: &ConnectorConfig) -> Result<SourceContract, ConnectorError>;

    /// Return connector-owned semantic options for durable recovery identity.
    ///
    /// The hook must be deterministic, configuration-only, and free of external
    /// I/O. `Some` replaces the raw property map in the pipeline identity;
    /// `None` asks the runtime to use its conservative sanitized-property
    /// fallback. A connector may omit operational endpoints or credentials only
    /// when its checkpoint independently binds the exact external object.
    ///
    /// # Errors
    ///
    /// Returns a configuration error when the semantic identity cannot be
    /// derived from the supplied source configuration.
    fn recovery_identity_options(
        &self,
        _config: &ConnectorConfig,
    ) -> Result<Option<std::collections::BTreeMap<String, String>>, ConnectorError> {
        Ok(None)
    }

    /// Acknowledge that `epoch` has been durably committed.
    ///
    /// Called after the manifest and exact engine commit decision are durable. Coordinated
    /// external sink publication may complete asynchronously afterward. The `checkpoint` is the exact
    /// per-source `SourceCheckpoint` that was persisted into the manifest
    /// for this epoch — sources can rely on it to advance external offset
    /// state (broker group offsets, lookup-DB cursors, ack tokens) using
    /// values that match what's durable.
    ///
    /// May be called with an empty `checkpoint` for timer-driven commits
    /// where no per-source state was captured; implementations should
    /// treat that as a no-op for any externally-visible advancement.
    ///
    /// Idempotent — a retry after cancellation is legal.
    ///
    /// # Errors
    ///
    /// The epoch is already durable and cannot be rolled back. During normal processing an error
    /// faults replay-capable pipelines so recovery retries the advisory upstream commit; during
    /// shutdown it is logged and the durable `LaminarDB` checkpoint remains authoritative.
    async fn notify_epoch_committed(
        &mut self,
        _epoch: u64,
        _checkpoint: &SourceCheckpoint,
    ) -> Result<(), ConnectorError> {
        Ok(())
    }
}

/// Trait for sink connectors that write data to external systems.
///
/// Sink connectors operate in Ring 1, receiving data from Ring 0 and
/// writing to external systems. Implementations whose contract is
/// [`SinkConsistency::CheckpointCommittable`] prepare checkpoint-owned
/// committables with `begin_epoch`/`pre_commit`, expose a
/// [`CoordinatedCommitter`] for the single external commit, and implement
/// `rollback_epoch`; the runtime drives them via the checkpoint coordinator.
///
/// All sinks follow `open()` → `write_batch()`/`flush()` → `close()`.
/// Checkpoint-committable sinks additionally loop over `begin_epoch()`, staged
/// writes, `pre_commit()`, and coordinated commit (or `rollback_epoch()` on a
/// proven pre-decision failure).
#[async_trait]
pub trait SinkConnector: Send {
    /// Deadline behavior required by the underlying client implementation.
    ///
    /// Retirement is the conservative default: a new connector must not be
    /// reused after cancellation until every lifecycle future has been audited.
    fn cancellation_policy(&self) -> ConnectorCancellationPolicy {
        ConnectorCancellationPolicy::RetireConnector
    }

    /// Observe detached tasks whose lifetime may outlast this connector value.
    ///
    /// A connector that spawns detached work must retain the matching
    /// [`ConnectorTaskOwner`] and move a guard into every task. The runtime can
    /// then wait for true terminal completion after dropping the connector.
    fn terminal_task_tracker(&self) -> Option<ConnectorTaskTracker> {
        None
    }

    /// Declare durability, placement, and input semantics for this exact
    /// configuration without opening files, sockets, clients, or transactions.
    ///
    /// The fail-closed default is an ephemeral append-only singleton. Durable
    /// or distributed behaviour must be opted into explicitly.
    ///
    /// # Errors
    ///
    /// Returns an error when the concrete configuration cannot provide a valid
    /// durability, placement, or input contract. The default implementation never fails.
    fn contract(&self, _config: &ConnectorConfig) -> Result<SinkContract, ConnectorError> {
        Ok(SinkContract::default())
    }

    /// Open the connection and prepare to accept writes.
    async fn open(&mut self, config: &ConnectorConfig) -> Result<(), ConnectorError>;

    /// Implementations using [`ConnectorCancellationPolicy::CancelSafe`] must
    /// remain valid when this future is dropped at a deadline. For
    /// [`ConnectorCancellationPolicy::RetireConnector`], cancellation makes the
    /// complete connector instance terminal before later work can be processed.
    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<WriteResult, ConnectorError>;

    /// Expected Arrow schema of input batches.
    fn schema(&self) -> SchemaRef;

    /// Begin checkpoint-owned staging. Called only for an admitted
    /// checkpoint-committable contract; weaker sinks use the no-op default.
    async fn begin_epoch(&mut self, _epoch: u64) -> Result<(), ConnectorError> {
        Ok(())
    }

    /// Flush + prepare, but do not finalize externally. The runtime persists
    /// the checkpoint decision before a designated committer finalizes the
    /// collected descriptors; on failure it calls `rollback_epoch`.
    ///
    /// Returns an opaque commit descriptor for checkpoint-committable sinks (the
    /// committables the designated committer will aggregate), else `None`.
    /// Default delegates to `flush()` and returns `None`.
    ///
    /// # Errors
    /// Returns `ConfigurationError` if the sink exposes a coordinated committer
    /// yet relies on this default — it would finalize epochs with no external commit.
    async fn pre_commit(&mut self, _epoch: u64) -> Result<Option<Vec<u8>>, ConnectorError> {
        if self.as_coordinated_committer().is_some() {
            return Err(ConnectorError::ConfigurationError(
                "sink exposes a coordinated committer but does not override pre_commit".into(),
            ));
        }
        self.flush().await?;
        Ok(None)
    }

    /// Must be idempotent. The runtime calls this on every
    /// checkpoint-committable sink after proving a pre-decision failure,
    /// including sinks that never completed `pre_commit`.
    async fn rollback_epoch(&mut self, _epoch: u64) -> Result<(), ConnectorError> {
        Ok(())
    }

    /// Default per-call `write_batch` I/O timeout. Users can override this via
    /// the `sink.write.timeout.ms` connector property.
    fn suggested_write_timeout(&self) -> std::time::Duration;

    /// Maximum residence time for a non-empty sink buffer before the runtime
    /// invokes [`flush`](Self::flush). Checkpoint-committable sinks ignore the
    /// periodic timer and flush only through their checkpoint protocol.
    fn flush_interval(&self) -> std::time::Duration {
        std::time::Duration::from_secs(5)
    }

    /// Must be internally bounded — the sink task's periodic timer
    /// calls this on every tick. Thorough drains belong in `pre_commit`
    /// / coordinated commit / `close`, not here.
    async fn flush(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }

    /// Close the sink and release resources.
    async fn close(&mut self) -> Result<(), ConnectorError>;

    /// Leader-side committer for a checkpoint-committable contract; `None`
    /// for every weaker contract.
    fn as_coordinated_committer(&self) -> Option<&dyn CoordinatedCommitter> {
        None
    }
}

/// Fixed control-plane bound for one connector's coordinated-commit payload.
///
/// Connectors must keep prepared metadata at or below this limit before
/// returning it to the checkpoint runtime. Bulk records belong in the sink's
/// data plane, referenced by the bounded payload.
pub const MAX_COORDINATED_COMMIT_PAYLOAD_BYTES: usize = 16 * 1024 * 1024;

/// Fixed aggregate control-plane bound for one designated commit call.
pub const MAX_COORDINATED_COMMIT_BATCH_BYTES: usize = 64 * 1024 * 1024;

/// Fixed participant-marker bound for one designated commit call.
pub const MAX_COORDINATED_COMMIT_BATCH_ENTRIES: usize = 4_096;

/// Stable external commit namespace for one deployment incarnation of a logical pipeline sink.
///
/// The configured external target already scopes its metadata. The create-once deployment id
/// prevents checkpoint-store resets or two
/// identically configured deployments from sharing a cursor. Pipeline identity plus sink id then
/// binds that deployment to one recovery-compatible logical writer.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct CoordinatedCommitNamespace {
    /// Canonical logical-pipeline identity used by checkpoint recovery.
    pub pipeline_identity: laminar_core::checkpoint::checkpoint_manifest::PipelineIdentity,
    /// Create-once UUID stored with checkpoint decisions and shared by every cluster member.
    pub deployment_id: String,
    /// Stable sink registration id within the pipeline.
    pub sink_id: String,
}

impl CoordinatedCommitNamespace {
    /// Construct and validate a namespace before any external metadata lookup.
    ///
    /// # Errors
    /// Returns a configuration error for a malformed pipeline digest or empty
    /// sink id.
    pub fn try_new(
        pipeline_identity: laminar_core::checkpoint::checkpoint_manifest::PipelineIdentity,
        deployment_id: impl Into<String>,
        sink_id: impl Into<String>,
    ) -> Result<Self, ConnectorError> {
        let deployment_id = deployment_id.into();
        let sink_id = sink_id.into();
        if pipeline_identity.sha256.len() != 64
            || !pipeline_identity
                .sha256
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(ConnectorError::ConfigurationError(
                "coordinated commit requires a canonical lowercase SHA-256 pipeline identity"
                    .into(),
            ));
        }
        if sink_id.is_empty() {
            return Err(ConnectorError::ConfigurationError(
                "coordinated commit sink id cannot be empty".into(),
            ));
        }
        let parsed_deployment = uuid::Uuid::parse_str(&deployment_id).map_err(|error| {
            ConnectorError::ConfigurationError(format!(
                "coordinated commit deployment id is not a UUID: {error}"
            ))
        })?;
        if parsed_deployment.is_nil() || parsed_deployment.to_string() != deployment_id {
            return Err(ConnectorError::ConfigurationError(
                "coordinated commit deployment id must be a canonical non-nil UUID".into(),
            ));
        }
        Ok(Self {
            pipeline_identity,
            deployment_id,
            sink_id,
        })
    }

    /// Bounded, filesystem/catalog-safe key for external transaction metadata.
    #[must_use]
    pub fn external_key(&self) -> String {
        let mut digest = Sha256::new();
        digest.update(self.pipeline_identity.canonical_version.to_be_bytes());
        digest.update(self.pipeline_identity.sha256.as_bytes());
        digest.update([0]);
        digest.update(self.deployment_id.as_bytes());
        digest.update([0]);
        digest.update(self.sink_id.as_bytes());
        let digest = digest.finalize();
        format!("ldb-c3-{digest:x}")
    }
}

/// Exact external commit position and the authority that published it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CoordinatedCommitCursor {
    /// Highest globally unique checkpoint id atomically reflected by the sink.
    pub checkpoint_id: u64,
    /// Monotonic authority token that fenced earlier designated committers.
    pub fencing_token: u64,
}

/// One participant's validated prepared marker for one exact attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoordinatedCommitPayload {
    /// Exact checkpoint attempt that admitted this marker.
    pub attempt: laminar_core::checkpoint::CheckpointAttempt,
    /// Stable nonzero runtime participant ID.
    pub participant_id: u64,
    /// Connector-specific committable, or `None` for an explicitly empty cut.
    pub payload: Option<Vec<u8>>,
}

/// Exact batch submitted to a designated external-sink committer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoordinatedCommitBatch {
    /// External cursor namespace.
    pub namespace: CoordinatedCommitNamespace,
    /// Exact external cursor that must precede this batch. The zero cursor names
    /// an empty target. A different authority at the predecessor checkpoint is
    /// a conflicting history and must fail closed.
    pub expected_predecessor: CoordinatedCommitCursor,
    /// Non-zero authority token that the external commit must persist atomically.
    pub fencing_token: u64,
    /// Highest exact attempt atomically covered by this commit.
    pub target: laminar_core::checkpoint::CheckpointAttempt,
    /// Every prepared participant marker through `target`, including empty ones.
    pub entries: Vec<CoordinatedCommitPayload>,
}

/// Runtime-owned deadline for one designated external publication.
///
/// The deadline is created before the command enters the sink actor, so a
/// connector sees the actual budget left after queueing rather than a second,
/// connector-local timeout window.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CoordinatedCommitContext {
    deadline: tokio::time::Instant,
}

impl CoordinatedCommitContext {
    /// Create a context from the sink actor's absolute end-to-end deadline.
    #[must_use]
    pub const fn new(deadline: tokio::time::Instant) -> Self {
        Self { deadline }
    }

    /// Absolute monotonic publication deadline.
    #[must_use]
    pub const fn deadline(self) -> tokio::time::Instant {
        self.deadline
    }

    /// Budget still available at the point the connector starts publication.
    #[must_use]
    pub fn remaining(self) -> std::time::Duration {
        self.deadline
            .saturating_duration_since(tokio::time::Instant::now())
    }
}

impl CoordinatedCommitBatch {
    /// Collision-resistant identity for one exact ordered publication cut.
    /// Every variable-length field is length framed so distinct batches cannot
    /// share an input byte stream before hashing.
    #[must_use]
    pub fn exact_fingerprint(&self) -> [u8; 32] {
        fn update_length(hasher: &mut Sha256, length: usize) {
            let source = length.to_be_bytes();
            let mut encoded = [0_u8; 16];
            let start = encoded.len() - source.len();
            encoded[start..].copy_from_slice(&source);
            hasher.update(encoded);
        }

        fn update_framed(hasher: &mut Sha256, bytes: &[u8]) {
            update_length(hasher, bytes.len());
            hasher.update(bytes);
        }

        let mut hasher = Sha256::new();
        update_framed(&mut hasher, b"laminardb/coordinated-commit-batch/v1");
        update_framed(&mut hasher, self.namespace.external_key().as_bytes());
        hasher.update(self.expected_predecessor.checkpoint_id.to_be_bytes());
        hasher.update(self.expected_predecessor.fencing_token.to_be_bytes());
        hasher.update(self.fencing_token.to_be_bytes());
        hasher.update(self.target.epoch.to_be_bytes());
        hasher.update(self.target.checkpoint_id.to_be_bytes());
        update_length(&mut hasher, self.entries.len());
        for entry in &self.entries {
            hasher.update(entry.attempt.epoch.to_be_bytes());
            hasher.update(entry.attempt.checkpoint_id.to_be_bytes());
            hasher.update(entry.participant_id.to_be_bytes());
            match &entry.payload {
                Some(payload) => {
                    hasher.update([1]);
                    update_framed(&mut hasher, payload);
                }
                None => hasher.update([0]),
            }
        }
        hasher.finalize().into()
    }

    /// Validate canonical attempt/participant order and all fixed control-plane bounds.
    /// This check is independent of external state and must run before connector I/O.
    ///
    /// # Errors
    /// Returns a diagnostic when the batch is malformed or exceeds a fixed bound.
    pub fn validate_shape(&self) -> Result<(), String> {
        use laminar_core::checkpoint::CheckpointAttemptRelation;

        if !self.target.is_canonical() {
            return Err(
                "coordinated batch target must use one nonzero canonical checkpoint ID".into(),
            );
        }
        if let Some(entry) = self
            .entries
            .iter()
            .find(|entry| entry.participant_id == 0 || !entry.attempt.is_canonical())
        {
            return Err(format!(
                "coordinated batch entry must use a nonzero participant and canonical checkpoint ID; got participant {}",
                entry.participant_id
            ));
        }
        if self.expected_predecessor.checkpoint_id >= self.target.checkpoint_id {
            return Err(format!(
                "invalid coordinated batch predecessor {} for target {}",
                self.expected_predecessor.checkpoint_id, self.target.checkpoint_id
            ));
        }
        if (self.expected_predecessor.checkpoint_id == 0)
            != (self.expected_predecessor.fencing_token == 0)
        {
            return Err(
                "coordinated batch predecessor must be either an exact non-zero cursor or the zero cursor"
                    .into(),
            );
        }
        if self.fencing_token == 0 {
            return Err("coordinated batch fencing token must be non-zero".into());
        }
        if self.entries.is_empty() || self.entries.len() > MAX_COORDINATED_COMMIT_BATCH_ENTRIES {
            return Err(format!(
                "coordinated batch entry count must be in 1..={MAX_COORDINATED_COMMIT_BATCH_ENTRIES}"
            ));
        }

        let mut total_payload_bytes = 0usize;
        let mut previous: Option<&CoordinatedCommitPayload> = None;
        for entry in &self.entries {
            if entry.attempt.checkpoint_id <= self.expected_predecessor.checkpoint_id
                || entry.attempt.checkpoint_id > self.target.checkpoint_id
            {
                return Err(
                    "coordinated batch entries do not cover the predecessor-to-target interval"
                        .into(),
                );
            }
            if let Some(payload) = &entry.payload {
                if payload.len() > MAX_COORDINATED_COMMIT_PAYLOAD_BYTES {
                    return Err(format!(
                        "coordinated participant payload exceeds the fixed {MAX_COORDINATED_COMMIT_PAYLOAD_BYTES} byte limit"
                    ));
                }
                total_payload_bytes = total_payload_bytes
                    .checked_add(payload.len())
                    .ok_or_else(|| "coordinated batch payload byte count overflow".to_owned())?;
                if total_payload_bytes > MAX_COORDINATED_COMMIT_BATCH_BYTES {
                    return Err(format!(
                        "coordinated batch payloads exceed the fixed {MAX_COORDINATED_COMMIT_BATCH_BYTES} byte limit"
                    ));
                }
            }

            if let Some(previous) = previous {
                match entry.attempt.relation_to(previous.attempt) {
                    CheckpointAttemptRelation::Exact
                        if entry.participant_id > previous.participant_id => {}
                    CheckpointAttemptRelation::Newer => {}
                    CheckpointAttemptRelation::Exact => {
                        return Err(
                            "coordinated batch contains a duplicate or out-of-order attempt/participant key"
                                .into(),
                        );
                    }
                    CheckpointAttemptRelation::Older | CheckpointAttemptRelation::Conflict => {
                        return Err(
                            "coordinated batch attempts are not in coherent epoch/checkpoint order"
                                .into(),
                        );
                    }
                }
            }
            previous = Some(entry);
        }
        if previous.map(|entry| entry.attempt) != Some(self.target) {
            return Err("coordinated batch target is not its final exact attempt".into());
        }
        Ok(())
    }

    /// Validate a cursor freshly read from the external target against this
    /// exact batch. Advancing overlap is safe only at an attempt named by the
    /// batch; rollback or an unproven gap would skip output.
    ///
    /// # Errors
    ///
    /// Returns a diagnostic when the batch is malformed, the observed cursor
    /// proves rollback, or an overlap cannot be tied to an exact batch entry.
    pub fn validate_observed_cursor(
        &self,
        observed: Option<CoordinatedCommitCursor>,
    ) -> Result<(), String> {
        self.validate_shape()?;
        let Some(observed) = observed else {
            return if self.expected_predecessor.checkpoint_id == 0 {
                Ok(())
            } else {
                Err(format!(
                    "external cursor is absent below expected predecessor {}",
                    self.expected_predecessor.checkpoint_id
                ))
            };
        };
        if observed.fencing_token == 0 {
            return Err("external cursor contains a zero fencing token".into());
        }
        if observed.fencing_token > self.fencing_token {
            return Err(format!(
                "external fencing token {} is newer than designated committer token {}",
                observed.fencing_token, self.fencing_token
            ));
        }
        if observed.checkpoint_id >= self.target.checkpoint_id
            && observed.fencing_token != self.fencing_token
        {
            return Err(format!(
                "external cursor at or above target {} has fencing token {}, expected {}",
                self.target.checkpoint_id, observed.fencing_token, self.fencing_token
            ));
        }
        if observed.checkpoint_id < self.expected_predecessor.checkpoint_id {
            return Err(format!(
                "external cursor rolled back from expected predecessor {} to {}",
                self.expected_predecessor.checkpoint_id, observed.checkpoint_id
            ));
        }
        if observed.checkpoint_id == self.expected_predecessor.checkpoint_id
            && observed != self.expected_predecessor
        {
            return Err(format!(
                "external cursor checkpoint {} has fencing token {}, expected predecessor token {}",
                observed.checkpoint_id,
                observed.fencing_token,
                self.expected_predecessor.fencing_token
            ));
        }
        if observed.checkpoint_id > self.expected_predecessor.checkpoint_id
            && observed.fencing_token < self.expected_predecessor.fencing_token
        {
            return Err(format!(
                "external cursor advanced past predecessor {} while fencing token regressed from {} to {}",
                self.expected_predecessor.checkpoint_id,
                self.expected_predecessor.fencing_token,
                observed.fencing_token
            ));
        }
        if observed.checkpoint_id < self.target.checkpoint_id
            && observed.checkpoint_id != self.expected_predecessor.checkpoint_id
            && !self
                .entries
                .iter()
                .any(|entry| entry.attempt.checkpoint_id == observed.checkpoint_id)
        {
            return Err(format!(
                "external cursor {} is not an exact attempt in batch {}..={}",
                observed.checkpoint_id,
                self.expected_predecessor.checkpoint_id,
                self.target.checkpoint_id
            ));
        }
        Ok(())
    }
}

/// Leader-side commit for checkpoint-committable sinks.
///
/// The designated committer aggregates every writer's `pre_commit` descriptor
/// for an epoch into one external commit. Must be idempotent: re-running with
/// the same inputs after a leader failover is a no-op once the target already
/// reflects the epoch.
#[async_trait]
pub trait CoordinatedCommitter: Send + Sync {
    /// Atomically commit the validated participant markers and advance the
    /// namespaced external cursor to the batch's exact target. Empty markers
    /// still advance the cursor.
    async fn commit_aggregated(
        &self,
        batch: CoordinatedCommitBatch,
        context: CoordinatedCommitContext,
    ) -> Result<(), ConnectorError>;

    /// Highest checkpoint and fencing authority committed in `namespace`.
    /// A metadata read error must be returned, never converted to an absent
    /// cursor, because that could duplicate a previously committed batch.
    async fn committed_cursor(
        &self,
        namespace: &CoordinatedCommitNamespace,
    ) -> Result<Option<CoordinatedCommitCursor>, ConnectorError>;
}

#[cfg(test)]
#[allow(clippy::cast_possible_wrap)]
mod tests {
    use super::*;
    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
    }

    fn test_batch(n: usize) -> RecordBatch {
        #[allow(clippy::cast_possible_wrap)]
        let ids: Vec<i64> = (0..n as i64).collect();
        RecordBatch::try_new(test_schema(), vec![Arc::new(Int64Array::from(ids))]).unwrap()
    }

    #[test]
    fn connector_task_generation_terminates_after_owner_and_every_guard() {
        let (owner, tracker) = ConnectorTaskOwner::new();
        let first = owner.track().expect("live generation");
        let second = owner.track().expect("live generation");

        assert!(!tracker.is_terminated());
        drop(owner);
        assert!(!tracker.is_terminated());
        drop(first);
        assert!(!tracker.is_terminated());
        drop(second);
        assert!(tracker.is_terminated());
    }

    #[test]
    fn connector_task_admission_is_sealed_by_owner_drop() {
        let (owner, tracker) = ConnectorTaskOwner::new();
        let admission = owner.admission();
        let admission_clone = admission.clone();
        let admitted = admission.track().expect("live generation");

        drop(owner);

        assert!(admission.track().is_none());
        assert!(admission_clone.track().is_none());
        assert!(!tracker.is_terminated());
        drop(admitted);
        assert!(tracker.is_terminated());
    }

    #[test]
    fn connector_task_admission_does_not_retain_generation_state() {
        let (owner, tracker) = ConnectorTaskOwner::new();
        let admission = owner.admission();

        drop(owner);
        drop(tracker);

        assert!(admission.inner.upgrade().is_none());
        assert!(admission.track().is_none());
    }

    #[tokio::test]
    async fn connector_task_wait_wakes_every_tracker_clone() {
        let (owner, tracker) = ConnectorTaskOwner::new();
        let guard = owner.track().expect("live generation");
        let first = tokio::spawn({
            let tracker = tracker.clone();
            async move { tracker.wait_terminated().await }
        });
        let second = tokio::spawn({
            let tracker = tracker.clone();
            async move { tracker.wait_terminated().await }
        });

        drop(owner);
        assert!(!tracker.is_terminated());
        drop(guard);

        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            first.await.expect("first waiter task");
            second.await.expect("second waiter task");
        })
        .await
        .expect("tracker waiters must wake");
        tracker.wait_terminated().await;
    }

    #[test]
    fn test_source_batch() {
        let batch = SourceBatch::new(test_batch(10));
        assert_eq!(batch.num_rows(), 10);
        assert!(batch.row_positions().is_none());
        assert!(batch.mutations().is_none());
    }

    #[test]
    fn source_row_positions_reject_nulls_and_misalignment() {
        let null_partition = BinaryArray::from(vec![Some(&b"p0"[..]), None]);
        let order = BinaryArray::from(vec![&b"0"[..], &b"1"[..]]);
        let sub_offset = UInt32Array::from(vec![0, 0]);
        assert!(
            SourceRowPositions::try_new(null_partition, order.clone(), sub_offset.clone()).is_err()
        );

        let positions = SourceRowPositions::try_new(
            BinaryArray::from(vec![&b"p0"[..], &b"p0"[..]]),
            order,
            sub_offset,
        )
        .unwrap();
        assert!(SourceBatch::positioned(test_batch(1), positions).is_err());
    }

    #[test]
    fn source_batch_validates_and_canonicalizes_mutations() {
        let mixed = SourceBatch::new(test_batch(2))
            .with_mutations(vec![SourceMutation::Put, SourceMutation::Tombstone])
            .unwrap();
        assert_eq!(
            mixed.mutations(),
            Some(&[SourceMutation::Put, SourceMutation::Tombstone][..])
        );

        let puts = SourceBatch::new(test_batch(2))
            .with_mutations(vec![SourceMutation::Put; 2])
            .unwrap();
        assert!(puts.mutations().is_none());
        assert!(SourceBatch::new(test_batch(2))
            .with_mutations(vec![SourceMutation::Tombstone])
            .is_err());
    }

    #[test]
    fn source_metadata_round_trip_is_sparse_and_zero_copy() {
        let records = test_batch(2);
        let positioned_schema = schema_with_source_row_positions(&records.schema()).unwrap();
        let mutation_schema =
            schema_with_source_mutations_and_row_positions(&records.schema()).unwrap();
        let positions = SourceRowPositions::try_new(
            BinaryArray::from(vec![&b"p0"[..], &b"p0"[..]]),
            BinaryArray::from(vec![&b"0"[..], &b"1"[..]]),
            UInt32Array::from(vec![0, 0]),
        )
        .unwrap();
        let encoded = SourceBatch::positioned(records.clone(), positions.clone())
            .unwrap()
            .with_mutations(vec![SourceMutation::Put, SourceMutation::Tombstone])
            .unwrap()
            .into_records_with_metadata(
                SourceRowPositionCapability::OrderedDeterministic,
                &positioned_schema,
                &mutation_schema,
            )
            .unwrap();
        let mutations = source_mutations(&encoded).unwrap().unwrap();
        assert_eq!(mutations.len(), 2);
        assert!(!mutations.is_empty());
        assert_eq!(mutations.get(0), Some(SourceMutation::Put));
        assert_eq!(mutations.get(1), Some(SourceMutation::Tombstone));
        assert_eq!(
            encoded.schema().field(records.num_columns()).name(),
            SOURCE_MUTATION_COLUMN
        );

        let positioned = strip_source_mutations(&encoded).unwrap();
        assert_eq!(positioned.schema(), positioned_schema);
        assert!(Arc::ptr_eq(positioned.column(0), records.column(0)));

        let routed_put = encoded.slice(0, 1);
        assert!(source_mutations(&routed_put).is_err());
        assert_eq!(
            source_mutations_routed(&routed_put)
                .unwrap()
                .unwrap()
                .get(0),
            Some(SourceMutation::Put)
        );
        let routed_visible = Arc::clone(routed_put.column(0));
        let routed_positioned = strip_source_mutations_routed(&routed_put).unwrap();
        assert!(Arc::ptr_eq(&routed_visible, routed_positioned.column(0)));

        let stripped = strip_source_row_positions(&encoded).unwrap();
        assert_eq!(stripped.schema(), records.schema());
        assert_eq!(stripped.num_rows(), records.num_rows());
        assert!(Arc::ptr_eq(stripped.column(0), records.column(0)));

        let puts = SourceBatch::positioned(records.clone(), positions)
            .unwrap()
            .with_mutations(vec![SourceMutation::Put; 2])
            .unwrap()
            .into_records_with_metadata(
                SourceRowPositionCapability::OrderedDeterministic,
                &positioned_schema,
                &mutation_schema,
            )
            .unwrap();
        assert!(Arc::ptr_eq(&puts.schema(), &positioned_schema));
        assert!(puts.column_by_name(SOURCE_MUTATION_COLUMN).is_none());
        assert!(source_mutations(&puts).unwrap().is_none());
    }

    #[test]
    fn source_metadata_rejects_collisions_and_malformed_batches() {
        let collision = Arc::new(Schema::new(vec![Field::new(
            "__SOURCE_MUTATION",
            DataType::UInt8,
            false,
        )]));
        assert!(schema_with_source_row_positions(&collision).is_err());
        assert!(schema_with_source_mutations_and_row_positions(&collision).is_err());

        let records = test_batch(2);
        let positioned_schema = schema_with_source_row_positions(&records.schema()).unwrap();
        let mutation_schema =
            schema_with_source_mutations_and_row_positions(&records.schema()).unwrap();
        let positions = SourceRowPositions::try_new(
            BinaryArray::from(vec![&b"p0"[..], &b"p0"[..]]),
            BinaryArray::from(vec![&b"0"[..], &b"1"[..]]),
            UInt32Array::from(vec![0, 0]),
        )
        .unwrap();
        let encoded = SourceBatch::positioned(records.clone(), positions)
            .unwrap()
            .with_mutations(vec![SourceMutation::Put, SourceMutation::Tombstone])
            .unwrap()
            .into_records_with_metadata(
                SourceRowPositionCapability::OrderedDeterministic,
                &positioned_schema,
                &mutation_schema,
            )
            .unwrap();
        let mutation_index = records.num_columns();

        let malformed = |field: Field, array: ArrayRef| {
            let mut fields = encoded.schema().fields().to_vec();
            fields[mutation_index] = Arc::new(field);
            let mut columns = encoded.columns().to_vec();
            columns[mutation_index] = array;
            RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
        };
        let wrong_type = malformed(
            Field::new(SOURCE_MUTATION_COLUMN, DataType::Int64, false),
            Arc::new(Int64Array::from(vec![0, 1])),
        );
        assert!(source_mutations(&wrong_type).is_err());

        let null = malformed(
            Field::new(SOURCE_MUTATION_COLUMN, DataType::UInt8, true),
            Arc::new(UInt8Array::from(vec![Some(0), None])),
        );
        assert!(strip_source_mutations(&null).is_err());

        let unknown = malformed(
            Field::new(SOURCE_MUTATION_COLUMN, DataType::UInt8, false),
            Arc::new(UInt8Array::from(vec![0, 2])),
        );
        assert!(source_mutations(&unknown).is_err());
        assert!(strip_source_mutations(&unknown).is_err());

        let all_put = malformed(
            Field::new(SOURCE_MUTATION_COLUMN, DataType::UInt8, false),
            Arc::new(UInt8Array::from(vec![0, 0])),
        );
        assert!(strip_source_mutations(&all_put).is_err());

        let mut fields = encoded.schema().fields().to_vec();
        let mutation_field = fields.remove(mutation_index);
        fields.push(mutation_field);
        let mut columns = encoded.columns().to_vec();
        let mutation_column = columns.remove(mutation_index);
        columns.push(mutation_column);
        let misplaced = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
        assert!(source_mutations(&misplaced).is_err());
    }

    #[test]
    fn test_write_result() {
        let result = WriteResult::new(100, 5000);
        assert_eq!(result.records_written, 100);
        assert_eq!(result.bytes_written, 5000);
    }

    #[test]
    fn source_drain_request_requires_canonical_round() {
        let round = laminar_core::checkpoint::AssignmentDrainId {
            predecessor_version: 7,
            target_version: 8,
            digest: [9; 32],
        };
        assert_eq!(SourceDrainRequest::new(round).unwrap().round, round);
        assert!(
            SourceDrainRequest::new(laminar_core::checkpoint::AssignmentDrainId {
                predecessor_version: 8,
                target_version: 8,
                digest: [9; 32],
            })
            .is_err()
        );
    }

    #[test]
    fn source_contract_defaults_fail_closed() {
        let contract = SourceContract::default();
        assert_eq!(contract.consistency, SourceConsistency::Ephemeral);
        assert_eq!(contract.topology, SourceTopology::Singleton);
        assert_eq!(contract.input_mode, SourceInputMode::AppendOnly);
        assert_eq!(
            contract.row_positions,
            SourceRowPositionCapability::Unavailable
        );
        assert!(!contract.supports_replay());
        assert!(!contract.requires_checkpointing());
        assert!(!contract.is_exact_delivery_certified());
    }

    #[test]
    fn commit_coupled_sources_are_replayable_and_require_checkpoints() {
        let contract = SourceContract::new(
            SourceConsistency::CommitCoupled,
            SourceTopology::NodeLocalIngress,
            SourceInputMode::FullChangelog,
        );
        assert!(contract.supports_replay());
        assert!(contract.requires_checkpointing());
        assert_eq!(contract.input_mode, SourceInputMode::FullChangelog);
    }

    #[test]
    fn source_start_rejects_split_and_zero_resume_before_connector_start() {
        use laminar_core::checkpoint::CheckpointAttempt;

        for attempt in [CheckpointAttempt::new(7, 8), CheckpointAttempt::new(0, 0)] {
            let error = SourceStart::new(
                ConnectorConfig::new("test"),
                SourcePosition::Resume {
                    attempt,
                    checkpoint: SourceCheckpoint::new(),
                },
                DeliveryGuarantee::AtLeastOnce,
            )
            .unwrap_err();
            assert!(matches!(
                error,
                ConnectorError::ConfigurationError(message)
                    if message.contains("one nonzero canonical checkpoint ID")
            ));
        }
    }

    #[test]
    fn source_start_accepts_initial_and_exposes_validated_parts() {
        let mut config = ConnectorConfig::new("test");
        config.set("endpoint", "local");
        let request = SourceStart::new(
            config,
            SourcePosition::Initial,
            DeliveryGuarantee::BestEffort,
        )
        .unwrap();

        let (config, position, delivery) = request.into_parts();
        assert_eq!(config.get("endpoint"), Some("local"));
        assert!(matches!(position, SourcePosition::Initial));
        assert_eq!(delivery, DeliveryGuarantee::BestEffort);
    }

    #[test]
    fn sink_contract_defaults_fail_closed() {
        let contract = SinkContract::default();
        assert_eq!(contract.consistency, SinkConsistency::Ephemeral);
        assert_eq!(contract.topology, SinkTopology::Singleton);
        assert_eq!(contract.input_mode, SinkInputMode::AppendOnly);
        assert!(!contract.input_mode.accepts_full_changelog());
    }

    #[test]
    fn coordinated_namespace_is_bounded_stable_and_sink_scoped() {
        use laminar_core::checkpoint::checkpoint_manifest::PipelineIdentity;
        const DEPLOYMENT: &str = "018f0000-0000-7000-8000-000000000001";

        let first =
            CoordinatedCommitNamespace::try_new(PipelineIdentity::empty(), DEPLOYMENT, "orders")
                .unwrap();
        let same =
            CoordinatedCommitNamespace::try_new(PipelineIdentity::empty(), DEPLOYMENT, "orders")
                .unwrap();
        let other =
            CoordinatedCommitNamespace::try_new(PipelineIdentity::empty(), DEPLOYMENT, "audit")
                .unwrap();
        let other_deployment = CoordinatedCommitNamespace::try_new(
            PipelineIdentity::empty(),
            "018f0000-0000-7000-8000-000000000002",
            "orders",
        )
        .unwrap();

        assert_eq!(first.external_key(), same.external_key());
        assert_ne!(first.external_key(), other.external_key());
        assert_ne!(first.external_key(), other_deployment.external_key());
        assert_eq!(first.external_key().len(), "ldb-c3-".len() + 64);
        assert!(first
            .external_key()
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-'));
    }

    #[test]
    fn coordinated_namespace_rejects_ambiguous_identity() {
        const DEPLOYMENT: &str = "018f0000-0000-7000-8000-000000000001";

        use laminar_core::checkpoint::checkpoint_manifest::{
            PipelineIdentity, PIPELINE_IDENTITY_VERSION,
        };
        let malformed = PipelineIdentity {
            canonical_version: PIPELINE_IDENTITY_VERSION,
            sha256: "NOT-A-DIGEST".into(),
        };
        assert!(CoordinatedCommitNamespace::try_new(malformed, DEPLOYMENT, "orders").is_err());
        assert!(
            CoordinatedCommitNamespace::try_new(PipelineIdentity::empty(), DEPLOYMENT, "").is_err()
        );
        assert!(CoordinatedCommitNamespace::try_new(
            PipelineIdentity::empty(),
            "not-a-uuid",
            "orders"
        )
        .is_err());
    }

    #[test]
    fn coordinated_batch_fingerprint_covers_the_exact_ordered_cut() {
        use laminar_core::checkpoint::checkpoint_manifest::PipelineIdentity;
        use laminar_core::checkpoint::CheckpointAttempt;

        let namespace = CoordinatedCommitNamespace::try_new(
            PipelineIdentity::empty(),
            "018f0000-0000-7000-8000-000000000001",
            "orders",
        )
        .unwrap();
        let attempt = CheckpointAttempt::new(8, 108);
        let batch = CoordinatedCommitBatch {
            namespace,
            expected_predecessor: CoordinatedCommitCursor {
                checkpoint_id: 107,
                fencing_token: 3,
            },
            fencing_token: 4,
            target: attempt,
            entries: vec![CoordinatedCommitPayload {
                attempt,
                participant_id: 7,
                payload: None,
            }],
        };
        let expected = batch.exact_fingerprint();
        assert_eq!(expected, batch.clone().exact_fingerprint());

        let mut variants = Vec::new();
        let mut variant = batch.clone();
        variant.namespace = CoordinatedCommitNamespace::try_new(
            PipelineIdentity::empty(),
            "018f0000-0000-7000-8000-000000000001",
            "audit",
        )
        .unwrap();
        variants.push(variant);
        let mut variant = batch.clone();
        variant.expected_predecessor.checkpoint_id -= 1;
        variants.push(variant);
        let mut variant = batch.clone();
        variant.fencing_token += 1;
        variants.push(variant);
        let mut variant = batch.clone();
        variant.target.epoch += 1;
        variants.push(variant);
        let mut variant = batch.clone();
        variant.entries[0].attempt.checkpoint_id += 1;
        variants.push(variant);
        let mut variant = batch.clone();
        variant.entries[0].participant_id += 1;
        variants.push(variant);
        let mut variant = batch;
        variant.entries[0].payload = Some(Vec::new());
        variants.push(variant);

        assert!(variants
            .into_iter()
            .all(|variant| variant.exact_fingerprint() != expected));
    }

    fn valid_coordinated_batch() -> CoordinatedCommitBatch {
        use laminar_core::checkpoint::checkpoint_manifest::PipelineIdentity;
        use laminar_core::checkpoint::CheckpointAttempt;

        let target = CheckpointAttempt::canonical(102);
        CoordinatedCommitBatch {
            namespace: CoordinatedCommitNamespace::try_new(
                PipelineIdentity::empty(),
                "018f0000-0000-7000-8000-000000000001",
                "orders",
            )
            .unwrap(),
            expected_predecessor: CoordinatedCommitCursor {
                checkpoint_id: 101,
                fencing_token: 1,
            },
            fencing_token: 2,
            target,
            entries: vec![CoordinatedCommitPayload {
                attempt: target,
                participant_id: 1,
                payload: None,
            }],
        }
    }

    #[test]
    fn coordinated_batch_rejects_noncanonical_target_before_other_shape_checks() {
        use laminar_core::checkpoint::CheckpointAttempt;

        for target in [
            CheckpointAttempt::new(102, 103),
            CheckpointAttempt::new(0, 0),
        ] {
            let mut batch = valid_coordinated_batch();
            batch.target = target;
            let error = batch.validate_shape().unwrap_err();
            assert!(
                error.contains("target must use one nonzero canonical checkpoint ID"),
                "unexpected validation error: {error}"
            );
        }
    }

    #[test]
    fn coordinated_batch_rejects_noncanonical_entry_before_other_shape_checks() {
        use laminar_core::checkpoint::CheckpointAttempt;

        for attempt in [
            CheckpointAttempt::new(101, 102),
            CheckpointAttempt::new(0, 0),
        ] {
            let mut batch = valid_coordinated_batch();
            batch.entries[0].attempt = attempt;
            let error = batch.validate_shape().unwrap_err();
            assert!(
                error.contains("canonical checkpoint ID"),
                "unexpected validation error: {error}"
            );
        }
        let mut batch = valid_coordinated_batch();
        batch.entries[0].participant_id = 0;
        assert!(batch
            .validate_shape()
            .unwrap_err()
            .contains("nonzero participant"));
    }

    #[test]
    fn coordinated_batch_rejects_cursor_rollback_and_unproven_overlap() {
        use laminar_core::checkpoint::checkpoint_manifest::PipelineIdentity;
        use laminar_core::checkpoint::CheckpointAttempt;

        let first = CheckpointAttempt::canonical(108);
        let target = CheckpointAttempt::canonical(110);
        let batch = CoordinatedCommitBatch {
            namespace: CoordinatedCommitNamespace::try_new(
                PipelineIdentity::empty(),
                "018f0000-0000-7000-8000-000000000001",
                "orders",
            )
            .unwrap(),
            expected_predecessor: CoordinatedCommitCursor {
                checkpoint_id: 107,
                fencing_token: 3,
            },
            fencing_token: 4,
            target,
            entries: vec![
                CoordinatedCommitPayload {
                    attempt: first,
                    participant_id: 1,
                    payload: None,
                },
                CoordinatedCommitPayload {
                    attempt: target,
                    participant_id: 1,
                    payload: None,
                },
            ],
        };

        let cursor = |checkpoint_id, fencing_token| {
            Some(CoordinatedCommitCursor {
                checkpoint_id,
                fencing_token,
            })
        };
        assert!(batch.validate_observed_cursor(cursor(106, 3)).is_err());
        assert!(batch.validate_observed_cursor(cursor(109, 3)).is_err());
        assert!(batch.validate_observed_cursor(cursor(107, 2)).is_err());
        assert!(batch.validate_observed_cursor(cursor(107, 3)).is_ok());
        assert!(batch.validate_observed_cursor(cursor(108, 3)).is_ok());
        assert!(batch.validate_observed_cursor(cursor(110, 4)).is_ok());
        assert!(batch.validate_observed_cursor(cursor(110, 3)).is_err());
        assert!(batch.validate_observed_cursor(cursor(108, 5)).is_err());
    }

    #[test]
    fn coordinated_batch_requires_unique_canonical_attempt_participants() {
        use laminar_core::checkpoint::checkpoint_manifest::PipelineIdentity;
        use laminar_core::checkpoint::CheckpointAttempt;

        let namespace = CoordinatedCommitNamespace::try_new(
            PipelineIdentity::empty(),
            "018f0000-0000-7000-8000-000000000001",
            "orders",
        )
        .unwrap();
        let target = CheckpointAttempt::canonical(102);
        let batch = |entries| CoordinatedCommitBatch {
            namespace: namespace.clone(),
            expected_predecessor: CoordinatedCommitCursor {
                checkpoint_id: 100,
                fencing_token: 1,
            },
            fencing_token: 2,
            target,
            entries,
        };
        let payload = |attempt, participant_id| CoordinatedCommitPayload {
            attempt,
            participant_id,
            payload: None,
        };

        let duplicate = batch(vec![payload(target, 1), payload(target, 1)]);
        assert!(duplicate
            .validate_shape()
            .unwrap_err()
            .contains("duplicate"));

        let out_of_order = batch(vec![payload(target, 2), payload(target, 1)]);
        assert!(out_of_order
            .validate_shape()
            .unwrap_err()
            .contains("out-of-order"));

        let noncanonical = batch(vec![
            payload(CheckpointAttempt::new(3, 101), 1),
            payload(target, 2),
        ]);
        assert!(noncanonical
            .validate_shape()
            .unwrap_err()
            .contains("canonical checkpoint ID"));
    }

    #[test]
    fn coordinated_batch_entry_limit_accepts_max_and_rejects_max_plus_one() {
        use laminar_core::checkpoint::checkpoint_manifest::PipelineIdentity;
        use laminar_core::checkpoint::CheckpointAttempt;

        let namespace = CoordinatedCommitNamespace::try_new(
            PipelineIdentity::empty(),
            "018f0000-0000-7000-8000-000000000001",
            "orders",
        )
        .unwrap();
        let target = CheckpointAttempt::canonical(101);
        let make_batch = |count: usize| CoordinatedCommitBatch {
            namespace: namespace.clone(),
            expected_predecessor: CoordinatedCommitCursor {
                checkpoint_id: 0,
                fencing_token: 0,
            },
            fencing_token: 1,
            target,
            entries: (1..=count)
                .map(|participant_id| CoordinatedCommitPayload {
                    attempt: target,
                    participant_id: participant_id as u64,
                    payload: None,
                })
                .collect(),
        };

        assert!(make_batch(MAX_COORDINATED_COMMIT_BATCH_ENTRIES - 1)
            .validate_shape()
            .is_ok());
        assert!(make_batch(MAX_COORDINATED_COMMIT_BATCH_ENTRIES)
            .validate_shape()
            .is_ok());
        assert!(make_batch(MAX_COORDINATED_COMMIT_BATCH_ENTRIES + 1)
            .validate_shape()
            .is_err());
    }

    struct DefaultPreCommitSink {
        coordinated: bool,
    }

    #[async_trait]
    impl SinkConnector for DefaultPreCommitSink {
        async fn open(&mut self, _config: &ConnectorConfig) -> Result<(), ConnectorError> {
            Ok(())
        }
        async fn write_batch(
            &mut self,
            _batch: &RecordBatch,
        ) -> Result<WriteResult, ConnectorError> {
            Ok(WriteResult::new(0, 0))
        }
        fn schema(&self) -> SchemaRef {
            test_schema()
        }
        fn suggested_write_timeout(&self) -> std::time::Duration {
            std::time::Duration::from_secs(5)
        }
        fn as_coordinated_committer(&self) -> Option<&dyn CoordinatedCommitter> {
            self.coordinated
                .then_some(self as &dyn CoordinatedCommitter)
        }
        async fn close(&mut self) -> Result<(), ConnectorError> {
            Ok(())
        }
    }

    #[async_trait]
    impl CoordinatedCommitter for DefaultPreCommitSink {
        async fn commit_aggregated(
            &self,
            _batch: CoordinatedCommitBatch,
            _context: CoordinatedCommitContext,
        ) -> Result<(), ConnectorError> {
            Ok(())
        }

        async fn committed_cursor(
            &self,
            _namespace: &CoordinatedCommitNamespace,
        ) -> Result<Option<CoordinatedCommitCursor>, ConnectorError> {
            Ok(None)
        }
    }

    #[tokio::test]
    async fn default_pre_commit_rejects_coordinated_sink() {
        let mut sink = DefaultPreCommitSink { coordinated: true };
        assert!(matches!(
            sink.pre_commit(1).await,
            Err(ConnectorError::ConfigurationError(_))
        ));
    }

    #[tokio::test]
    async fn default_pre_commit_ok_for_non_coordinated_sink() {
        let mut sink = DefaultPreCommitSink { coordinated: false };
        assert!(matches!(sink.pre_commit(1).await, Ok(None)));
    }
}
