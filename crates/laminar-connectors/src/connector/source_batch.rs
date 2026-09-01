//! Source-batch metadata encoding, validation, and ergonomic views.

use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, BinaryArray, RecordBatch, RecordBatchOptions, UInt32Array, UInt8Array,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};

use crate::checkpoint::{SourceCheckpoint, SourceCheckpointDelta};
use crate::error::ConnectorError;

use super::SourceRowPositionCapability;

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

/// One borrowed deterministic source coordinate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceRowPositionRef<'a> {
    /// Connector-defined partition identity.
    pub partition: &'a [u8],
    /// Connector-defined order-preserving cursor.
    pub order_key: &'a [u8],
    /// Row ordinal within one source cursor.
    pub sub_offset: u32,
}

/// Validated borrowed access to row-aligned deterministic source coordinates.
#[derive(Debug, Clone, Copy)]
pub struct SourceRowPositionView<'a> {
    partition: &'a BinaryArray,
    order_key: &'a BinaryArray,
    sub_offset: &'a UInt32Array,
}

impl<'a> SourceRowPositionView<'a> {
    /// Number of row positions.
    #[must_use]
    pub fn len(self) -> usize {
        self.partition.len()
    }

    /// Whether the batch has no row positions.
    #[must_use]
    pub fn is_empty(self) -> bool {
        self.partition.is_empty()
    }

    /// Position for `row`, if it is in bounds.
    #[must_use]
    pub fn get(self, row: usize) -> Option<SourceRowPositionRef<'a>> {
        (row < self.len()).then(|| SourceRowPositionRef {
            partition: self.partition.value(row),
            order_key: self.order_key.value(row),
            sub_offset: self.sub_offset.value(row),
        })
    }
}

/// Borrow optional row-aligned source positions after validating their canonical layout.
///
/// # Errors
/// Returns an error for partial, misplaced, nullable, incorrectly typed, or misaligned metadata.
pub fn source_row_positions(
    records: &RecordBatch,
) -> Result<Option<SourceRowPositionView<'_>>, ConnectorError> {
    let Some(layout) = source_metadata_layout(records.schema().as_ref())? else {
        return Ok(None);
    };
    validate_source_position_arrays(records, layout)?;
    let position_start = layout.visible_columns + usize::from(layout.has_mutations);
    let (Some(partition), Some(order_key), Some(sub_offset)) = (
        records.columns()[position_start]
            .as_any()
            .downcast_ref::<BinaryArray>(),
        records.columns()[position_start + 1]
            .as_any()
            .downcast_ref::<BinaryArray>(),
        records.columns()[position_start + 2]
            .as_any()
            .downcast_ref::<UInt32Array>(),
    ) else {
        return Err(ConnectorError::SchemaMismatch(
            "source row-position metadata arrays have invalid types".into(),
        ));
    };
    Ok(Some(SourceRowPositionView {
        partition,
        order_key,
        sub_offset,
    }))
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

impl SourceMutationView<'_> {
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
    cursor: Option<SourceBatchCursor>,
}

/// Source progress captured with one emitted batch.
#[derive(Debug, Clone)]
pub enum SourceBatchCursor {
    /// Complete recovery cursor captured with the batch.
    Complete(SourceCheckpoint),
    /// Changed offsets extending a complete cursor emitted earlier.
    Incremental(SourceCheckpointDelta),
}

impl SourceBatch {
    /// Construct a source batch.
    #[must_use]
    pub fn new(records: RecordBatch) -> Self {
        Self {
            records,
            row_positions: None,
            mutations: None,
            cursor: None,
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
            cursor: None,
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

    /// Bind the exact source cursor captured with this batch.
    #[must_use]
    pub fn with_checkpoint(mut self, checkpoint: SourceCheckpoint) -> Self {
        self.cursor = Some(SourceBatchCursor::Complete(checkpoint));
        self
    }

    /// Bind changed offsets from the assignment checkpoint already published by an earlier batch.
    #[must_use]
    pub fn with_checkpoint_delta(mut self, delta: SourceCheckpointDelta) -> Self {
        self.cursor = Some(SourceBatchCursor::Incremental(delta));
        self
    }

    /// Remove the source cursor bound to this batch, when present.
    pub fn take_cursor(&mut self) -> Option<SourceBatchCursor> {
        self.cursor.take()
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
            cursor: _,
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
