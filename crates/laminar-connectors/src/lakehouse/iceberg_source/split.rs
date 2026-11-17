//! `read.splits = n` in append mode: each data file belongs to one of `n` input channels by a hash
//! of its path, and each channel keeps its own position in the ref's append lineage. On one node
//! the source reads every channel; in a cluster each channel is read by the owner of the vnode the
//! engine's channel route gives it.

use std::collections::BTreeMap;
use std::num::NonZeroU32;
use std::time::Instant;

use iceberg::table::Table;
use laminar_core::checkpoint::AssignmentDrainId;
use laminar_core::state::key_hash;

use super::append_lineage::{plan_split_appends, validate_cursor_lineage};
use super::cursor::{
    cursor_error, durable_metadata_location, IcebergSourceCursorOriginV1, IcebergSourceCursorV1,
    CURSOR_VERSION,
};
use super::{planner, IcebergSource};
use crate::checkpoint::SourceCheckpoint;
use crate::connector::split_ownership::{
    source_input_channel, OwnershipChange, SplitAssignment, SplitOwnership,
};
use crate::connector::SourceDrainSettlement;
use crate::error::ConnectorError;
use crate::lakehouse::iceberg_config::{IcebergReadBootstrap, IcebergSourceConfig};

/// Domain separator of the data-file hash. Changing the hash moves files between channels, so it
/// needs a new version rather than an edit.
const DATA_FILE_CHANNEL_HASH_V1: &[u8] = b"laminardb.iceberg-data-file-channel.v1\0";
const SPLIT_CURSOR_VERSION: &str = "2";
const CURSOR_VERSION_METADATA: &str = "iceberg.cursor.version";
const SPLITS_METADATA: &str = "iceberg.splits";
const CATALOG_IDENTITY_METADATA: &str = "iceberg.catalog_identity";
const TABLE_UUID_METADATA: &str = "iceberg.table_uuid";
const TABLE_IDENTIFIER_METADATA: &str = "iceberg.table_identifier";
const TABLE_REF_METADATA: &str = "iceberg.table_ref";
const READ_SCHEMA_ID_METADATA: &str = "iceberg.read_schema_id";

pub(super) fn data_file_channel(path: &str, splits: NonZeroU32) -> u32 {
    let mut key = Vec::with_capacity(DATA_FILE_CHANNEL_HASH_V1.len() + path.len());
    key.extend_from_slice(DATA_FILE_CHANNEL_HASH_V1);
    key.extend_from_slice(path.as_bytes());
    // The remainder of a division by a u32 count fits in u32.
    #[allow(clippy::cast_possible_truncation)]
    let channel = (key_hash(&key) % u64::from(splits.get())) as u32;
    channel
}

/// A snapshot of the followed ref.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct SnapshotPosition {
    pub snapshot_id: i64,
    pub sequence_number: i64,
}

/// Position of one channel in the ref's append lineage.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum ChannelPosition {
    /// Before the table's first snapshot, observed in this metadata file.
    EmptyTable { metadata_location: String },
    /// The channel's files of this whole snapshot are still to be read, then later appends.
    Bootstrap(SnapshotPosition),
    /// The channel's files up to and including this snapshot are read.
    After(SnapshotPosition),
}

impl ChannelPosition {
    /// Where an unsplit source with the same settings would start.
    fn start(cursor: &IcebergSourceCursorV1, bootstrap: IcebergReadBootstrap) -> Self {
        let snapshot = SnapshotPosition {
            snapshot_id: cursor.snapshot_id,
            sequence_number: cursor.sequence_number,
        };
        match (&cursor.origin, bootstrap) {
            (IcebergSourceCursorOriginV1::EmptyTable, _) => Self::EmptyTable {
                metadata_location: cursor.metadata_location.clone(),
            },
            (IcebergSourceCursorOriginV1::Snapshot, IcebergReadBootstrap::Initial) => {
                Self::Bootstrap(snapshot)
            }
            (IcebergSourceCursorOriginV1::Snapshot, IcebergReadBootstrap::None) => {
                Self::After(snapshot)
            }
        }
    }

    fn encode(&self) -> String {
        match self {
            Self::EmptyTable { metadata_location } => format!("empty:{metadata_location}"),
            Self::Bootstrap(snapshot) => {
                format!(
                    "bootstrap:{}:{}",
                    snapshot.snapshot_id, snapshot.sequence_number
                )
            }
            Self::After(snapshot) => {
                format!(
                    "after:{}:{}",
                    snapshot.snapshot_id, snapshot.sequence_number
                )
            }
        }
    }

    fn decode(encoded: &str) -> Result<Self, ConnectorError> {
        let malformed = || {
            cursor_error(
                "LDB-ICEBERG-CURSOR-DECODE",
                "malformed split channel position",
            )
        };
        let (kind, rest) = encoded.split_once(':').ok_or_else(malformed)?;
        if kind == "empty" {
            return Ok(Self::EmptyTable {
                metadata_location: rest.to_string(),
            });
        }
        let (snapshot_id, sequence_number) = rest.split_once(':').ok_or_else(malformed)?;
        let snapshot = SnapshotPosition {
            snapshot_id: snapshot_id.parse().map_err(|_| malformed())?,
            sequence_number: sequence_number.parse().map_err(|_| malformed())?,
        };
        match kind {
            "bootstrap" => Ok(Self::Bootstrap(snapshot)),
            "after" => Ok(Self::After(snapshot)),
            _ => Err(malformed()),
        }
    }
}

fn offset_key(channel: u32) -> String {
    format!("iceberg.split.{channel}")
}

/// What every channel's position is bound to: the same on every node, so it lives in checkpoint
/// metadata, which the commit merge requires to be identical across participants.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct SplitBinding {
    catalog_identity: String,
    table_uuid: String,
    table_identifier: String,
    table_ref: String,
    read_schema_id: i32,
}

impl SplitBinding {
    fn from_cursor(cursor: &IcebergSourceCursorV1) -> Self {
        Self {
            catalog_identity: cursor.catalog_identity.clone(),
            table_uuid: cursor.table_uuid.clone(),
            table_identifier: cursor.table_identifier.clone(),
            table_ref: cursor.table_ref.clone(),
            read_schema_id: cursor.read_schema_id,
        }
    }

    fn from_checkpoint(checkpoint: &SourceCheckpoint) -> Result<Self, ConnectorError> {
        let field = |key: &str| {
            checkpoint
                .get_metadata(key)
                .map(str::to_string)
                .ok_or_else(|| {
                    cursor_error(
                        "LDB-ICEBERG-CURSOR-MISSING",
                        &format!("split cursor has no {key}"),
                    )
                })
        };
        Ok(Self {
            catalog_identity: field(CATALOG_IDENTITY_METADATA)?,
            table_uuid: field(TABLE_UUID_METADATA)?,
            table_identifier: field(TABLE_IDENTIFIER_METADATA)?,
            table_ref: field(TABLE_REF_METADATA)?,
            read_schema_id: field(READ_SCHEMA_ID_METADATA)?.parse().map_err(|_| {
                cursor_error(
                    "LDB-ICEBERG-CURSOR-DECODE",
                    &format!("malformed {READ_SCHEMA_ID_METADATA}"),
                )
            })?,
        })
    }

    fn write_to(&self, checkpoint: &mut SourceCheckpoint) {
        checkpoint.set_metadata(CATALOG_IDENTITY_METADATA, self.catalog_identity.as_str());
        checkpoint.set_metadata(TABLE_UUID_METADATA, self.table_uuid.as_str());
        checkpoint.set_metadata(TABLE_IDENTIFIER_METADATA, self.table_identifier.as_str());
        checkpoint.set_metadata(TABLE_REF_METADATA, self.table_ref.as_str());
        checkpoint.set_metadata(READ_SCHEMA_ID_METADATA, self.read_schema_id.to_string());
    }

    /// The unsplit cursor of a channel at `position`, which validation and lineage planning read.
    /// A snapshot position records the metadata file of `table`, the table it is checked against.
    pub fn channel_cursor(
        &self,
        position: &ChannelPosition,
        table: &Table,
    ) -> Result<IcebergSourceCursorV1, ConnectorError> {
        let (origin, snapshot, metadata_location) = match position {
            ChannelPosition::EmptyTable { metadata_location } => (
                IcebergSourceCursorOriginV1::EmptyTable,
                SnapshotPosition {
                    snapshot_id: 0,
                    sequence_number: 0,
                },
                metadata_location.clone(),
            ),
            ChannelPosition::Bootstrap(snapshot) | ChannelPosition::After(snapshot) => (
                IcebergSourceCursorOriginV1::Snapshot,
                *snapshot,
                durable_metadata_location(table)?.to_string(),
            ),
        };
        let cursor = IcebergSourceCursorV1 {
            version: CURSOR_VERSION,
            catalog_identity: self.catalog_identity.clone(),
            table_uuid: self.table_uuid.clone(),
            table_identifier: self.table_identifier.clone(),
            table_ref: self.table_ref.clone(),
            origin,
            snapshot_id: snapshot.snapshot_id,
            sequence_number: snapshot.sequence_number,
            read_schema_id: self.read_schema_id,
            metadata_location,
        };
        cursor.validate_payload()?;
        Ok(cursor)
    }
}

/// A cut that covers every channel: a single-node checkpoint or a cluster cut merged from every
/// participant.
pub(super) struct SplitCut {
    pub binding: SplitBinding,
    /// One position per channel, in channel order.
    pub positions: Vec<ChannelPosition>,
}

impl SplitCut {
    /// The cut a source named `source_name` with `splits` channels resumes from.
    pub fn from_resume_checkpoint(
        checkpoint: &SourceCheckpoint,
        source_name: &str,
        splits: NonZeroU32,
    ) -> Result<Self, ConnectorError> {
        let channels = (0..splits.get())
            .map(|channel| source_input_channel(source_name, channel))
            .collect::<Result<Vec<_>, _>>()?;
        Self::from_checkpoint(checkpoint, &channels)
    }

    fn from_checkpoint(
        checkpoint: &SourceCheckpoint,
        channels: &[Vec<u8>],
    ) -> Result<Self, ConnectorError> {
        if checkpoint.get_metadata("connector_type") != Some("iceberg")
            || checkpoint.get_metadata(CURSOR_VERSION_METADATA) != Some(SPLIT_CURSOR_VERSION)
        {
            return Err(cursor_error(
                "LDB-ICEBERG-CURSOR-VERSION",
                "resume checkpoint was not written by an Iceberg source with read.splits",
            ));
        }
        let splits = channels.len().to_string();
        if checkpoint.get_metadata(SPLITS_METADATA) != Some(splits.as_str()) {
            return Err(cursor_error(
                "LDB-ICEBERG-CURSOR-SPLITS",
                &format!(
                    "checkpoint has {} splits, the source is configured with {splits}",
                    checkpoint.get_metadata(SPLITS_METADATA).unwrap_or("no")
                ),
            ));
        }
        if checkpoint.input_channels() != Some(channels)
            || checkpoint.offsets().len() != channels.len()
        {
            return Err(cursor_error(
                "LDB-ICEBERG-CURSOR-SPLITS",
                "checkpoint does not cover every split channel",
            ));
        }
        let positions = (0..)
            .zip(channels)
            .map(|(channel, _)| {
                let key = offset_key(channel);
                let encoded = checkpoint.get_offset(&key).ok_or_else(|| {
                    cursor_error(
                        "LDB-ICEBERG-CURSOR-MISSING",
                        &format!("split cursor has no {key}"),
                    )
                })?;
                ChannelPosition::decode(encoded)
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            binding: SplitBinding::from_checkpoint(checkpoint)?,
            positions,
        })
    }
}

/// The split input: the binding every position shares, and the positions of the owned channels.
pub(super) struct IcebergSplitInput {
    binding: SplitBinding,
    ownership: SplitOwnership<ChannelPosition>,
}

impl IcebergSplitInput {
    /// Start every channel at its position in `start`, after checking each against `table`.
    fn new(
        source_name: &str,
        start: SplitCut,
        config: &IcebergSourceConfig,
        table: &Table,
        assignment: Option<SplitAssignment>,
    ) -> Result<Self, ConnectorError> {
        for position in &start.positions {
            let cursor = start.binding.channel_cursor(position, table)?;
            cursor.validate_binding(config, table)?;
            validate_cursor_lineage(table, &cursor, config.max_snapshots_per_poll)?;
        }
        Ok(Self {
            binding: start.binding,
            ownership: SplitOwnership::new(source_name, start.positions, assignment)?,
        })
    }

    pub fn ready(&self) -> Result<bool, ConnectorError> {
        self.ownership.ready()
    }

    pub fn is_draining(&self) -> bool {
        self.ownership.is_draining()
    }

    fn checkpoint(&self) -> SourceCheckpoint {
        let mut checkpoint = SourceCheckpoint::new();
        for (channel, position) in self.ownership.owned() {
            checkpoint.set_offset(offset_key(*channel), position.encode());
        }
        checkpoint.set_metadata("connector_type", "iceberg");
        checkpoint.set_metadata(CURSOR_VERSION_METADATA, SPLIT_CURSOR_VERSION);
        checkpoint.set_metadata(SPLITS_METADATA, self.ownership.count().to_string());
        self.binding.write_to(&mut checkpoint);
        self.ownership.bind_checkpoint(&mut checkpoint);
        checkpoint
    }

    pub fn drive(&mut self) -> OwnershipChange {
        let binding = &self.binding;
        self.ownership
            .drive(|checkpoint, channels| handoff_positions(checkpoint, channels, binding))
    }

    pub fn begin_drain(&mut self, round: AssignmentDrainId) -> Result<(), ConnectorError> {
        self.ownership.begin_drain(round)
    }

    pub fn ensure_active_drain(&self, round: AssignmentDrainId) -> Result<(), ConnectorError> {
        self.ownership.ensure_active_drain(round)
    }

    pub fn finish_drain(
        &mut self,
        settlement: &SourceDrainSettlement,
    ) -> Result<OwnershipChange, ConnectorError> {
        let binding = &self.binding;
        self.ownership
            .finish_drain(settlement, |checkpoint, channels| {
                handoff_positions(checkpoint, channels, binding)
            })
    }
}

/// Positions from a committed cut, which must share this source's binding.
fn handoff_positions(
    checkpoint: &SourceCheckpoint,
    channels: &[Vec<u8>],
    binding: &SplitBinding,
) -> Result<Vec<ChannelPosition>, ConnectorError> {
    let cut = SplitCut::from_checkpoint(checkpoint, channels)?;
    if &cut.binding != binding {
        return Err(cursor_error(
            "LDB-ICEBERG-CURSOR-TABLE",
            "committed split cursor is bound to another table, ref or read schema",
        ));
    }
    Ok(cut.positions)
}

impl IcebergSource {
    /// Start every channel at its committed position, or where an unsplit source would start.
    pub(super) fn start_split(
        &mut self,
        source_name: &str,
        resumed: Option<SplitCut>,
    ) -> Result<(), ConnectorError> {
        let splits = self
            .config
            .splits
            .ok_or_else(|| ConnectorError::InvalidState {
                expected: "Iceberg source with read.splits".into(),
                actual: "read.splits is not set".into(),
            })?;
        let table = self.loaded_table()?.clone();
        let read_schema_id = self.read_schema()?.schema_id();
        let start = match resumed {
            Some(cut) if cut.binding.read_schema_id != read_schema_id => {
                return Err(cursor_error(
                    "LDB-ICEBERG-CURSOR-SCHEMA",
                    &format!(
                        "split cursor reads schema {}, the source binds schema {read_schema_id}",
                        cut.binding.read_schema_id
                    ),
                ));
            }
            Some(cut) => cut,
            None => {
                let cursor = match Self::selected_snapshot(&table, &self.config)? {
                    Some(snapshot) => IcebergSourceCursorV1::from_snapshot(
                        &self.config,
                        &table,
                        &snapshot,
                        read_schema_id,
                    )?,
                    None => IcebergSourceCursorV1::from_empty_table(
                        &self.config,
                        &table,
                        read_schema_id,
                    )?,
                };
                let position = ChannelPosition::start(&cursor, self.config.bootstrap);
                SplitCut {
                    binding: SplitBinding::from_cursor(&cursor),
                    positions: vec![position; splits.get() as usize],
                }
            }
        };
        let split = IcebergSplitInput::new(
            source_name,
            start,
            &self.config,
            &table,
            self.assignment.clone(),
        )?;
        self.checkpoint = split.checkpoint();
        self.split = Some(split);
        Ok(())
    }

    /// Whether a split source may read on: not before its channels follow the assignment, and
    /// not past the end of its current replay unit while a drain holds it.
    pub(super) fn split_may_read(&self) -> Result<bool, ConnectorError> {
        let Some(split) = &self.split else {
            return Ok(true);
        };
        if !split.ready()? {
            return Ok(false);
        }
        Ok(self.replay_unit_in_progress || !split.is_draining())
    }

    pub(super) fn split_mut(&mut self) -> Result<&mut IcebergSplitInput, ConnectorError> {
        self.split.as_mut().ok_or_else(|| {
            ConnectorError::ConfigurationError(
                "only an Iceberg source with read.splits drains".into(),
            )
        })
    }

    pub(super) fn install_channel_position(
        &mut self,
        channel: u32,
        position: ChannelPosition,
    ) -> Result<(), ConnectorError> {
        if let ChannelPosition::After(snapshot) = &position {
            self.observe_processed(snapshot.snapshot_id, snapshot.sequence_number);
        }
        let split = self.split_mut()?;
        split.ownership.advance(channel, position)?;
        self.checkpoint = split.checkpoint();
        self.replay_unit_in_progress = false;
        Ok(())
    }

    /// Follow an ownership change. A scan planned for other channels is dropped with its unread
    /// output. A change inside a replay unit would split a partly emitted unit between owners, so
    /// it stops the source for recovery from the committed cut.
    pub(super) fn follow_split_ownership(&mut self, change: OwnershipChange) {
        let in_unit = self.replay_unit_in_progress;
        let Some(split) = self.split.as_mut() else {
            return;
        };
        match change {
            OwnershipChange::Unchanged => {}
            OwnershipChange::Rebound => self.checkpoint = split.checkpoint(),
            OwnershipChange::Moved if in_unit => split.ownership.fail(
                "split channel ownership changed inside a replay unit; recovery from the \
                 committed cut is required"
                    .into(),
            ),
            OwnershipChange::Moved => {
                self.checkpoint = split.checkpoint();
                if let Some(scan) = self.scan.take() {
                    drop(scan.receiver);
                    scan.handle.abort();
                }
                self.pending = None;
                self.last_poll_time = None;
            }
        }
    }

    /// Plan the next replay units of the owned channels: the channels still to bootstrap from
    /// one snapshot, else every channel's appends.
    pub(super) async fn refresh_split(&mut self) -> Result<(), ConnectorError> {
        if self
            .last_poll_time
            .is_some_and(|last| last.elapsed() < self.config.poll_interval)
            || self
                .split
                .as_ref()
                .is_none_or(|split| split.ownership.owned().is_empty())
        {
            return Ok(());
        }
        let poll_started = Instant::now();
        // An acquired channel may bootstrap from a snapshot newer than the table loaded at start.
        let table = self.reload_table().await?;
        self.table = Some(table.clone());
        if self.start_split_bootstrap_scan(&table)? {
            return Ok(());
        }
        self.start_split_append_scan(table).await?;
        self.last_poll_time = Some(Instant::now());
        self.metrics
            .poll_duration
            .observe(poll_started.elapsed().as_secs_f64());
        Ok(())
    }

    /// Scan the whole snapshot the first channels still to bootstrap wait for. Returns whether
    /// any owned channel bootstraps.
    fn start_split_bootstrap_scan(&mut self, table: &Table) -> Result<bool, ConnectorError> {
        let Some(split) = &self.split else {
            return Ok(false);
        };
        let bootstrap: Vec<(u32, SnapshotPosition)> = split
            .ownership
            .owned()
            .iter()
            .filter_map(|(channel, position)| match position {
                ChannelPosition::Bootstrap(snapshot) => Some((*channel, *snapshot)),
                _ => None,
            })
            .collect();
        let Some(&(_, snapshot)) = bootstrap.first() else {
            return Ok(false);
        };
        let channels = bootstrap
            .iter()
            .filter(|(_, waiting_for)| *waiting_for == snapshot)
            .map(|(channel, _)| *channel)
            .collect();
        let splits = split.ownership.count();
        self.set_pending_snapshots(1);
        self.scan = Some(planner::split_bootstrap_task(
            table.clone(),
            &self.config,
            self.read_schema()?,
            self.filter_predicate.clone(),
            snapshot,
            splits,
            channels,
        ));
        Ok(true)
    }

    async fn start_split_append_scan(&mut self, table: Table) -> Result<(), ConnectorError> {
        let Some(split) = &self.split else {
            return Ok(());
        };
        let cursors = split
            .ownership
            .owned()
            .iter()
            .map(|(channel, position)| {
                let cursor = split.binding.channel_cursor(position, &table)?;
                cursor.validate_binding(&self.config, &table)?;
                Ok((*channel, cursor))
            })
            .collect::<Result<BTreeMap<_, _>, ConnectorError>>()?;
        let splits = split.ownership.count();
        let plans = self
            .plan_within_request_timeout(|deadline| {
                plan_split_appends(&table, &cursors, splits, &self.config, deadline)
            })
            .await?;
        let files = plans
            .iter()
            .flat_map(|plan| &plan.channels)
            .map(|(_, files)| files.len())
            .sum();
        let manifests = plans.iter().map(|plan| plan.manifest_count).sum();
        self.observe_planned(files, manifests, plans.len());
        self.scan = planner::split_append_task(
            table,
            &self.config,
            self.read_schema()?,
            self.filter_predicate.clone(),
            plans,
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;
    use std::sync::Arc;
    use std::time::Duration;

    use arrow_array::Int64Array;
    use laminar_core::checkpoint::CheckpointAttempt;
    use laminar_core::state::{NodeId, VnodeRegistry};

    use super::*;
    use crate::config::ConnectorConfig;
    use crate::connector::split_ownership::test_support::{merge, publish, round, routed};
    use crate::connector::{SourceConnector, SourceDrainRequest, SourceTopology};
    use crate::lakehouse::iceberg::test_support::{append_rows, create_test_table, TestTable};

    const VNODES: u32 = 4;

    fn connector_config(splits: &str) -> ConnectorConfig {
        let mut config = ConnectorConfig::new("iceberg");
        config.set("catalog.uri", "http://localhost:8181");
        config.set("catalog.warehouse", "s3://test/wh");
        config.set("namespace", "test");
        config.set("table.name", "events");
        config.set("read.mode", "append");
        config.set("read.splits", splits);
        config
    }

    fn source_config(splits: u32, bootstrap: IcebergReadBootstrap) -> IcebergSourceConfig {
        let mut config =
            IcebergSourceConfig::from_config(&connector_config(&splits.to_string())).unwrap();
        config.bootstrap = bootstrap;
        config.poll_interval = Duration::from_millis(1);
        config
    }

    /// Append one snapshot holding `ids`, each in its own partition and so its own data file.
    async fn append(fixture: &TestTable, table: &Table, epoch: u64, ids: &[i64]) -> Table {
        let categories: Vec<String> = ids.iter().map(|id| format!("c{id}")).collect();
        let values: Vec<(i64, Option<&str>)> = ids
            .iter()
            .zip(&categories)
            .map(|(id, category)| (*id, Some(category.as_str())))
            .collect();
        append_rows(fixture, table, epoch, &values).await.0
    }

    fn start(
        fixture: &TestTable,
        table: &Table,
        config: IcebergSourceConfig,
        node: Option<(&Arc<VnodeRegistry>, u64)>,
        resume: Option<&SourceCheckpoint>,
    ) -> IcebergSource {
        let splits = config.splits.unwrap();
        let mut source = IcebergSource::new(config, None);
        if let Some((registry, node)) = node {
            source
                .set_vnode_assignment("ice", Arc::clone(registry), NodeId(node))
                .unwrap();
        }
        source.catalog = Some(Arc::clone(&fixture.catalog));
        source.table = Some(table.clone());
        source
            .bind_read_schema(&table.current_schema_ref(), None)
            .unwrap();
        let resumed = resume
            .map(|checkpoint| SplitCut::from_resume_checkpoint(checkpoint, "ice", splits).unwrap());
        source.start_split("ice", resumed).unwrap();
        source.drive_control_plane();
        source
    }

    /// Poll until the source stays empty, `max_records` rows at a time.
    async fn read(source: &mut IcebergSource, max_records: usize) -> Vec<i64> {
        let mut ids = Vec::new();
        let mut idle = 0;
        while idle < 10 {
            let Some(batch) = source.poll_batch(max_records).await.unwrap() else {
                idle += 1;
                tokio::time::sleep(Duration::from_millis(2)).await;
                continue;
            };
            idle = 0;
            let values = batch
                .records
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            ids.extend(values.values());
        }
        ids.sort_unstable();
        ids
    }

    fn positions(checkpoint: &SourceCheckpoint) -> Vec<(u32, ChannelPosition)> {
        let mut positions: Vec<(u32, ChannelPosition)> = checkpoint
            .offsets()
            .iter()
            .map(|(key, value)| {
                let channel = key.strip_prefix("iceberg.split.").unwrap().parse().unwrap();
                (channel, ChannelPosition::decode(value).unwrap())
            })
            .collect();
        positions.sort_by_key(|(channel, _)| *channel);
        positions
    }

    fn after(table: &Table) -> ChannelPosition {
        let head = table.metadata().current_snapshot().unwrap();
        ChannelPosition::After(SnapshotPosition {
            snapshot_id: head.snapshot_id(),
            sequence_number: head.sequence_number(),
        })
    }

    fn deadline() -> tokio::time::Instant {
        tokio::time::Instant::now() + Duration::from_secs(5)
    }

    const PINNED_CHANNELS: [u32; 4] = [2, 1, 1, 3];

    #[test]
    fn data_file_channels_are_pinned_and_spread() {
        let four = NonZeroU32::new(4).unwrap();
        let pinned: Vec<u32> = ["a.parquet", "b.parquet", "c.parquet", "d.parquet"]
            .iter()
            .map(|name| data_file_channel(&format!("s3://wh/t/data/{name}"), four))
            .collect();
        assert_eq!(pinned, PINNED_CHANNELS);
        let mut counts = [0_u32; 4];
        for file in 0..1_000 {
            counts[data_file_channel(&format!("s3://wh/t/data/{file}.parquet"), four) as usize] +=
                1;
        }
        assert!(counts.iter().all(|count| *count > 200), "{counts:?}");
    }

    #[test]
    fn channel_positions_round_trip_and_reject_malformed_values() {
        for position in [
            ChannelPosition::EmptyTable {
                metadata_location: "s3://wh/t/metadata/v1.json".into(),
            },
            ChannelPosition::Bootstrap(SnapshotPosition {
                snapshot_id: 7,
                sequence_number: 2,
            }),
            ChannelPosition::After(SnapshotPosition {
                snapshot_id: -9,
                sequence_number: 3,
            }),
        ] {
            assert_eq!(
                ChannelPosition::decode(&position.encode()).unwrap(),
                position
            );
        }
        for malformed in ["after:x:1", "later:1:2", "after:1", "bootstrap"] {
            assert!(ChannelPosition::decode(malformed).is_err(), "{malformed}");
        }
    }

    #[test]
    fn splits_need_append_mode_and_place_the_source() {
        let contract = IcebergSource::new(source_config(4, IcebergReadBootstrap::Initial), None)
            .contract(&connector_config("4"))
            .unwrap();
        assert_eq!(contract.topology, SourceTopology::Splittable);
        assert_eq!(
            contract.placement.map(|placement| placement.channels.get()),
            Some(4)
        );
        assert!(!contract.is_exact_delivery_certified());

        let mut snapshot = connector_config("4");
        snapshot.set("read.mode", "snapshot");
        for (config, expected) in [
            (snapshot, "only valid when read.mode=append"),
            (connector_config("0"), "read.splits must be > 0"),
            (connector_config("65536"), "at most 65535"),
        ] {
            let error = IcebergSourceConfig::from_config(&config).unwrap_err();
            assert!(error.to_string().contains(expected), "{error}");
        }
    }

    #[tokio::test]
    async fn every_row_is_read_once_through_a_bootstrap_and_a_resume() {
        let fixture = create_test_table(true).await;
        let seeded = append(&fixture, &fixture.table, 1, &(0..8).collect::<Vec<_>>()).await;
        let config = source_config(3, IcebergReadBootstrap::Initial);
        let mut first = start(&fixture, &seeded, config.clone(), None, None);
        assert_eq!(read(&mut first, 2).await, (0..8).collect::<Vec<_>>());
        let checkpoint = first.try_checkpoint().unwrap().unwrap();
        assert_eq!(checkpoint.assignment_version(), None);
        assert!(positions(&checkpoint)
            .iter()
            .all(|(_, position)| *position == after(&seeded)));
        first.close().await.unwrap();

        let second = append(&fixture, &seeded, 2, &[8, 9, 10]).await;
        let third = append(&fixture, &second, 3, &[11, 12]).await;
        let mut resumed = start(&fixture, &third, config, None, Some(&checkpoint));
        assert_eq!(read(&mut resumed, 2).await, (8..13).collect::<Vec<_>>());
        let end = resumed.try_checkpoint().unwrap().unwrap();
        assert_eq!(positions(&end).len(), 3);
        assert!(positions(&end)
            .iter()
            .all(|(_, position)| *position == after(&third)));
    }

    #[tokio::test]
    async fn channels_without_files_still_advance_with_the_lineage() {
        let fixture = create_test_table(false).await;
        let first = append(&fixture, &fixture.table, 1, &[1]).await;
        let mut source = start(
            &fixture,
            &first,
            source_config(16, IcebergReadBootstrap::None),
            None,
            None,
        );
        let second = append(&fixture, &first, 2, &[2]).await;
        let third = append(&fixture, &second, 3, &[3]).await;
        assert_eq!(read(&mut source, 64).await, [2, 3]);
        let checkpoint = source.try_checkpoint().unwrap().unwrap();
        assert_eq!(positions(&checkpoint).len(), 16);
        assert!(positions(&checkpoint)
            .iter()
            .all(|(_, position)| *position == after(&third)));
    }

    #[tokio::test]
    async fn resume_refuses_another_split_count_or_an_unsplit_cursor() {
        let fixture = create_test_table(false).await;
        let table = append(&fixture, &fixture.table, 1, &[1]).await;
        let three = start(
            &fixture,
            &table,
            source_config(3, IcebergReadBootstrap::None),
            None,
            None,
        );
        let split_checkpoint = three.try_checkpoint().unwrap().unwrap();
        let error =
            SplitCut::from_resume_checkpoint(&split_checkpoint, "ice", NonZeroU32::new(4).unwrap())
                .err()
                .unwrap();
        assert!(
            error
                .to_string()
                .contains("has 3 splits, the source is configured with 4"),
            "{error}"
        );
        let unsplit = IcebergSourceCursorV1::from_snapshot(
            &source_config(3, IcebergReadBootstrap::None),
            &table,
            table.metadata().current_snapshot().unwrap(),
            table.metadata().current_schema_id(),
        )
        .unwrap()
        .to_checkpoint()
        .unwrap();
        let error = SplitCut::from_resume_checkpoint(&unsplit, "ice", NonZeroU32::new(3).unwrap())
            .err()
            .unwrap();
        assert!(error.to_string().contains("with read.splits"), "{error}");
        let error = IcebergSourceCursorV1::from_checkpoint(&split_checkpoint).unwrap_err();
        assert!(
            error.to_string().contains("LDB-ICEBERG-CURSOR-MISSING"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn cluster_sources_read_disjoint_channels_that_cover_the_table() {
        let fixture = create_test_table(true).await;
        let table = append(&fixture, &fixture.table, 1, &(0..24).collect::<Vec<_>>()).await;
        let registry = Arc::new(VnodeRegistry::new_unassigned(VNODES));
        let config = source_config(4, IcebergReadBootstrap::Initial);
        let mut a = start(&fixture, &table, config.clone(), Some((&registry, 1)), None);
        let mut b = start(&fixture, &table, config, Some((&registry, 2)), None);
        assert!(!a.checkpoint_ready().unwrap());
        assert!(a.try_checkpoint().unwrap().is_none());
        assert!(a.poll_batch(64).await.unwrap().is_none());

        let owners = [1, 2, 1, 2];
        publish(&registry, &owners, 1);
        a.drive_control_plane();
        b.drive_control_plane();
        let mut rows = read(&mut a, 64).await;
        let from_b = read(&mut b, 64).await;
        assert!(!rows.is_empty() && !from_b.is_empty());
        assert!(rows.iter().all(|id| !from_b.contains(id)));
        rows.extend(from_b);
        rows.sort_unstable();
        assert_eq!(rows, (0..24).collect::<Vec<_>>());

        for (source, node) in [(&a, 1), (&b, 2)] {
            let checkpoint = source.try_checkpoint().unwrap().unwrap();
            assert_eq!(
                checkpoint.assignment_version().map(NonZeroU64::get),
                Some(1)
            );
            let owned: Vec<u32> = positions(&checkpoint).iter().map(|(c, _)| *c).collect();
            assert_eq!(owned, routed("ice", &owners, node, 4));
        }
    }

    #[tokio::test]
    async fn a_committed_drain_hands_channels_over_at_the_handoff_cursor() {
        let fixture = create_test_table(true).await;
        let first = append(&fixture, &fixture.table, 1, &(0..12).collect::<Vec<_>>()).await;
        let config = source_config(4, IcebergReadBootstrap::Initial);
        let (left, right) = (
            Arc::new(VnodeRegistry::new_unassigned(VNODES)),
            Arc::new(VnodeRegistry::new_unassigned(VNODES)),
        );
        publish(&left, &[1; 4], 1);
        publish(&right, &[1; 4], 1);
        let mut a = start(&fixture, &first, config.clone(), Some((&left, 1)), None);
        let mut b = start(&fixture, &first, config, Some((&right, 2)), None);
        assert_eq!(read(&mut a, 64).await, (0..12).collect::<Vec<_>>());
        assert!(read(&mut b, 64).await.is_empty());

        let second = append(&fixture, &first, 2, &[12, 13, 14]).await;
        let request = SourceDrainRequest::new(round(1)).unwrap();
        for source in [&mut a, &mut b] {
            source.begin_drain(&request, deadline()).unwrap();
            assert!(source.poll_drain_ready(round(1)).unwrap());
            assert!(source.poll_batch(64).await.unwrap().is_none());
        }
        let handoff = merge(
            &[
                a.try_checkpoint().unwrap().unwrap(),
                b.try_checkpoint().unwrap().unwrap(),
            ],
            1,
        );
        publish(&left, &[2; 4], 2);
        publish(&right, &[2; 4], 2);
        for source in [&mut a, &mut b] {
            source
                .finish_drain(
                    SourceDrainSettlement::commit(
                        round(1),
                        CheckpointAttempt::canonical(1),
                        Arc::clone(&handoff),
                    ),
                    deadline(),
                )
                .await
                .unwrap();
        }
        // Node 2 started at the first snapshot; it reads only what node 1 had not.
        assert_eq!(read(&mut b, 64).await, [12, 13, 14]);
        assert!(read(&mut a, 64).await.is_empty());
        let end = b.try_checkpoint().unwrap().unwrap();
        assert_eq!(end.assignment_version().map(NonZeroU64::get), Some(2));
        assert!(positions(&end)
            .iter()
            .all(|(_, position)| *position == after(&second)));
    }

    #[tokio::test]
    async fn a_drain_finishes_the_replay_unit_in_progress_first() {
        let fixture = create_test_table(false).await;
        let first = append(&fixture, &fixture.table, 1, &[1, 2, 3]).await;
        let registry = Arc::new(VnodeRegistry::new_unassigned(VNODES));
        publish(&registry, &[1; 4], 1);
        let mut source = start(
            &fixture,
            &first,
            source_config(1, IcebergReadBootstrap::Initial),
            Some((&registry, 1)),
            None,
        );
        assert!(source.poll_batch(1).await.unwrap().is_some());
        assert!(source.try_checkpoint().unwrap().is_none());

        let request = SourceDrainRequest::new(round(1)).unwrap();
        source.begin_drain(&request, deadline()).unwrap();
        assert!(!source.poll_drain_ready(round(1)).unwrap());
        let mut rest = 0;
        while let Some(batch) = source.poll_batch(1).await.unwrap() {
            rest += batch.records.num_rows();
        }
        assert_eq!(rest, 2);
        assert!(source.poll_drain_ready(round(1)).unwrap());
        assert!(source.try_checkpoint().unwrap().is_some());
    }

    #[tokio::test]
    async fn an_ownership_change_inside_a_replay_unit_stops_the_source() {
        let fixture = create_test_table(false).await;
        let first = append(&fixture, &fixture.table, 1, &[1, 2, 3]).await;
        let registry = Arc::new(VnodeRegistry::new_unassigned(VNODES));
        publish(&registry, &[1; 4], 1);
        let mut source = start(
            &fixture,
            &first,
            source_config(4, IcebergReadBootstrap::Initial),
            Some((&registry, 1)),
            None,
        );
        while source.try_checkpoint().unwrap().is_some() {
            assert!(source.poll_batch(1).await.unwrap().is_some());
        }
        publish(&registry, &[2; 4], 2);
        source.drive_control_plane();
        let error = source.checkpoint_ready().unwrap_err();
        assert!(
            error.to_string().contains("inside a replay unit"),
            "{error}"
        );
    }
}
