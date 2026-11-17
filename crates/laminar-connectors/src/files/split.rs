//! `splits = n` over an object-store directory: each object belongs to one of `n` input channels by
//! a hash of its key, and each channel keeps the keys it has read whole and the object it is part
//! way through. In a cluster each channel is read by the owner of the vnode the engine's channel
//! route gives it.

use std::collections::BTreeSet;
use std::num::NonZeroU32;
use std::sync::Arc;

use laminar_core::checkpoint::AssignmentDrainId;
use tokio::sync::watch;

use super::ingestion_position::{
    file_checkpoint, validate_file_checkpoint, validate_object_key, FileProgress,
};
use super::manifest::manifest_fragment;
use crate::checkpoint::{PersistentOffset, SourceCheckpoint};
use crate::connector::split_ownership::{
    path_channel, split_input_channels, OwnershipChange, SplitAssignment, SplitOwnership,
};
use crate::connector::{SourceDrainSettlement, SourcePosition};
use crate::error::ConnectorError;

/// Domain separator of the object-key hash. Changing the hash moves objects between channels, so
/// it needs a new version rather than an edit.
const OBJECT_CHANNEL_HASH_V1: &[u8] = b"laminardb.files-object-channel.v1\0";
const SPLIT_CHECKPOINT_VERSION: &str = "2";
const SPLITS_METADATA: &str = "splits";

/// The channel an object belongs to, by its key relative to the source directory.
pub(super) fn object_channel(key: &str, splits: NonZeroU32) -> u32 {
    path_channel(OBJECT_CHANNEL_HASH_V1, key, splits)
}

fn manifest_key(channel: u32) -> String {
    format!("manifest.{channel}")
}

fn progress_key(channel: u32) -> String {
    format!("file_progress.{channel}")
}

fn invalid(message: impl Into<String>) -> ConnectorError {
    ConnectorError::ConfigurationError(message.into())
}

/// What one channel has read.
#[derive(Clone)]
struct ChannelCursor {
    /// Keys of the objects read whole. A clone shares the set until its first insert.
    processed: Arc<BTreeSet<String>>,
    /// `processed` in read order, as the checkpoint stores it.
    manifest: PersistentOffset,
    /// An object read part way that this node has not finished.
    progress: Option<FileProgress>,
}

impl ChannelCursor {
    fn empty() -> Self {
        Self {
            processed: Arc::new(BTreeSet::new()),
            manifest: PersistentOffset::new("[", ",", "]"),
            progress: None,
        }
    }

    fn complete(&mut self, key: String) {
        let fragment = manifest_fragment(&key);
        if Arc::make_mut(&mut self.processed).insert(key) {
            self.manifest.push_fragment(fragment);
        }
        self.progress = None;
    }
}

/// The channels this node reads and what each has read.
pub(super) struct SplitObjects {
    ownership: SplitOwnership<ChannelCursor>,
    /// The owned channels, for discovery to list only their objects.
    owned_channels: watch::Sender<Arc<BTreeSet<u32>>>,
}

impl SplitObjects {
    /// Start every channel where `position` puts it. Without a cluster assignment this node reads
    /// every channel.
    pub fn restore(
        source_name: &str,
        count: NonZeroU32,
        position: &SourcePosition,
        assignment: Option<SplitAssignment>,
    ) -> Result<Self, ConnectorError> {
        let start = match position {
            SourcePosition::Initial => (0..count.get()).map(|_| ChannelCursor::empty()).collect(),
            SourcePosition::Initialized { .. } => {
                return Err(invalid(
                    "file source has no sealed topology startup contract",
                ));
            }
            SourcePosition::Resume { checkpoint, .. } => cut_cursors(
                checkpoint,
                count,
                &split_input_channels(source_name, count)?,
            )?,
        };
        let ownership = SplitOwnership::new(source_name, start, assignment)?;
        let (owned_channels, _) = watch::channel(owned_set(&ownership));
        Ok(Self {
            ownership,
            owned_channels,
        })
    }

    pub fn count(&self) -> NonZeroU32 {
        self.ownership.count()
    }

    /// The owned channels as they change, for discovery.
    pub fn watch_owned_channels(&self) -> watch::Receiver<Arc<BTreeSet<u32>>> {
        self.owned_channels.subscribe()
    }

    pub fn is_cluster(&self) -> bool {
        self.ownership.is_cluster()
    }

    pub fn ready(&self) -> Result<bool, ConnectorError> {
        self.ownership.ready()
    }

    pub fn is_draining(&self) -> bool {
        self.ownership.is_draining()
    }

    /// Whether this node reads the object `key`.
    pub fn owns(&self, key: &str) -> bool {
        self.ownership
            .owned()
            .contains_key(&object_channel(key, self.count()))
    }

    /// Whether this node reads the object `key` and has not read it whole.
    pub fn is_unread(&self, key: &str) -> bool {
        self.ownership
            .owned()
            .get(&object_channel(key, self.count()))
            .is_some_and(|cursor| !cursor.processed.contains(key))
    }

    /// Objects of owned channels read part way, to finish before any new object.
    pub fn unfinished(&self) -> impl Iterator<Item = &FileProgress> {
        self.ownership
            .owned()
            .values()
            .filter_map(|cursor| cursor.progress.as_ref())
    }

    /// Record that the object `key` was read whole.
    pub fn complete(&mut self, key: String) -> Result<(), ConnectorError> {
        let channel = object_channel(&key, self.count());
        self.ownership.position_mut(channel)?.complete(key);
        Ok(())
    }

    /// The owned channels' cursor, with `reading` as the progress of its channel.
    pub fn checkpoint(
        &self,
        reading: Option<&FileProgress>,
        schema_contract: Option<&str>,
    ) -> SourceCheckpoint {
        let count = self.count();
        let reading_channel = reading.map(|progress| object_channel(&progress.path, count));
        let mut checkpoint = file_checkpoint(SPLIT_CHECKPOINT_VERSION, schema_contract);
        checkpoint.set_metadata(SPLITS_METADATA, count.to_string());
        for (channel, cursor) in self.ownership.owned() {
            checkpoint.set_persistent_offset(manifest_key(*channel), cursor.manifest.clone());
            let progress = reading
                .filter(|_| reading_channel == Some(*channel))
                .or(cursor.progress.as_ref());
            if let Some(progress) = progress {
                checkpoint.set_offset(progress_key(*channel), progress.encode());
            }
        }
        self.ownership.bind_checkpoint(&mut checkpoint);
        checkpoint
    }

    /// Follow a new assignment publication.
    pub fn drive(&mut self) -> OwnershipChange {
        let count = self.count();
        let change = self
            .ownership
            .drive(|checkpoint, channels| cut_cursors(checkpoint, count, channels));
        self.publish(change);
        change
    }

    pub fn begin_drain(&mut self, round: AssignmentDrainId) -> Result<(), ConnectorError> {
        self.ownership.begin_drain(round)
    }

    // Every batch boundary has an exact cursor, so a paused source holds nothing past it.
    pub fn drain_ready(&self, round: AssignmentDrainId) -> Result<bool, ConnectorError> {
        self.ownership.ensure_active_drain(round).map(|()| true)
    }

    /// Resolve the drain. A commit moves acquired channels to the committed handoff cursor once
    /// the target assignment is published.
    pub fn finish_drain(
        &mut self,
        settlement: &SourceDrainSettlement,
    ) -> Result<OwnershipChange, ConnectorError> {
        let count = self.count();
        let change = self
            .ownership
            .finish_drain(settlement, |checkpoint, channels| {
                cut_cursors(checkpoint, count, channels)
            })?;
        self.publish(change);
        Ok(change)
    }

    fn publish(&self, change: OwnershipChange) {
        if change == OwnershipChange::Moved {
            self.owned_channels.send_replace(owned_set(&self.ownership));
        }
    }
}

fn owned_set(ownership: &SplitOwnership<ChannelCursor>) -> Arc<BTreeSet<u32>> {
    Arc::new(ownership.owned().keys().copied().collect())
}

/// Every channel's cursor in a checkpoint that covers all of them: a single-node checkpoint or a
/// cluster cut merged from every participant.
fn cut_cursors(
    checkpoint: &SourceCheckpoint,
    count: NonZeroU32,
    channels: &[Vec<u8>],
) -> Result<Vec<ChannelCursor>, ConnectorError> {
    validate_file_checkpoint(checkpoint, SPLIT_CHECKPOINT_VERSION)?;
    if checkpoint.get_metadata(SPLITS_METADATA) != Some(count.to_string().as_str()) {
        return Err(invalid(format!(
            "file checkpoint has {} splits, the source is configured with {count}",
            checkpoint.get_metadata(SPLITS_METADATA).unwrap_or("no")
        )));
    }
    if checkpoint.input_channels() != Some(channels) {
        return Err(invalid(
            "file checkpoint does not cover every split channel",
        ));
    }
    let cursors = (0..count.get())
        .map(|channel| channel_cursor(checkpoint, channel, count))
        .collect::<Result<Vec<_>, ConnectorError>>()?;
    let offsets = cursors.len()
        + cursors
            .iter()
            .filter(|cursor| cursor.progress.is_some())
            .count();
    if checkpoint.offsets().len() != offsets {
        return Err(invalid("file checkpoint holds offsets of no split channel"));
    }
    Ok(cursors)
}

fn channel_cursor(
    checkpoint: &SourceCheckpoint,
    channel: u32,
    count: NonZeroU32,
) -> Result<ChannelCursor, ConnectorError> {
    let belongs = |key: &str| {
        if object_channel(key, count) == channel {
            Ok(())
        } else {
            Err(invalid(format!(
                "file checkpoint lists object '{key}' under channel {channel}, not its own"
            )))
        }
    };
    let manifest = checkpoint
        .get_offset(&manifest_key(channel))
        .ok_or_else(|| {
            invalid(format!(
                "file checkpoint is missing '{}'",
                manifest_key(channel)
            ))
        })?;
    let keys: Vec<String> = serde_json::from_str(manifest).map_err(|error| {
        invalid(format!(
            "invalid file manifest of channel {channel}: {error}"
        ))
    })?;
    let mut cursor = ChannelCursor::empty();
    for key in keys {
        belongs(&key)?;
        cursor.complete(key);
    }
    cursor.progress = checkpoint
        .get_offset(&progress_key(channel))
        .map(serde_json::from_str::<FileProgress>)
        .transpose()
        .map_err(|error| {
            invalid(format!(
                "invalid file progress of channel {channel}: {error}"
            ))
        })?;
    if let Some(progress) = &cursor.progress {
        belongs(&progress.path)?;
        validate_object_key(progress)?;
        if cursor.processed.contains(&progress.path) || progress.next_row == 0 {
            return Err(invalid(format!(
                "file checkpoint holds contradictory progress for channel {channel}"
            )));
        }
    }
    Ok(cursor)
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;
    use std::time::Duration;

    use laminar_core::state::{NodeId, VnodeRegistry};
    use object_store::memory::InMemory;
    use object_store::ObjectStore;

    use super::*;
    use crate::config::ConnectorConfig;
    use crate::connector::split_ownership::source_input_channel;
    use crate::connector::split_ownership::test_support::{merge, publish, round, routed};
    use crate::connector::{
        DeliveryGuarantee, SourceConnector, SourceDrainRequest, SourceLocality, SourceStart,
        SourceTopology,
    };
    use crate::files::source::FileSource;
    use crate::files::test_support::{next_lines, put_object, read_lines, resume_from};
    use laminar_core::checkpoint::CheckpointAttempt;

    const VNODES: u32 = 4;
    const SOURCE: &str = "landing";

    fn config(splits: Option<u32>) -> ConnectorConfig {
        let mut config = ConnectorConfig::new("files");
        config.set("path", "s3://bucket/landing");
        config.set("format", "text");
        config.set("poll_interval", "5ms");
        config.set("laminar.source.name", SOURCE);
        if let Some(splits) = splits {
            config.set("splits", splits.to_string());
        }
        config
    }

    /// Objects `o00.txt`.. of `rows` lines `o<i>-r<j>` each.
    async fn put_objects(store: &InMemory, objects: std::ops::Range<u32>, rows: u32) {
        for object in objects {
            let body: String = (0..rows)
                .map(|row| format!("o{object:02}-r{row}"))
                .fold(String::new(), |body, line| body + &line + "\n");
            put_object(store, &format!("o{object:02}.txt"), body.as_bytes()).await;
        }
    }

    fn lines(objects: std::ops::Range<u32>, rows: u32) -> Vec<String> {
        objects
            .flat_map(|object| (0..rows).map(move |row| format!("o{object:02}-r{row}")))
            .collect()
    }

    async fn start(
        store: &Arc<InMemory>,
        splits: Option<u32>,
        position: SourcePosition,
        node: Option<(&Arc<VnodeRegistry>, u64)>,
    ) -> Result<FileSource, ConnectorError> {
        let mut source = FileSource::with_object_store(Arc::clone(store) as Arc<dyn ObjectStore>);
        if let Some((registry, node)) = node {
            source.set_vnode_assignment(SOURCE, Arc::clone(registry), NodeId(node))?;
        }
        let request =
            SourceStart::new(config(splits), position, DeliveryGuarantee::AtLeastOnce).unwrap();
        source.start(request).await?;
        Ok(source)
    }

    /// Lines read until the source stays empty, sorted.
    async fn read(source: &mut FileSource) -> Vec<String> {
        let mut lines = read_lines(source).await;
        lines.sort_unstable();
        lines
    }

    fn deadline() -> tokio::time::Instant {
        tokio::time::Instant::now() + Duration::from_secs(5)
    }

    #[test]
    fn the_object_hash_is_pinned_and_spreads_keys() {
        let four = NonZeroU32::new(4).unwrap();
        let pinned: Vec<u32> = ["a.json", "b.json", "2026/11/17/c.json", "d.csv"]
            .iter()
            .map(|key| object_channel(key, four))
            .collect();
        assert_eq!(pinned, [3, 2, 2, 1]);
        let mut counts = [0_u32; 4];
        for object in 0..1_000 {
            counts[object_channel(&format!("part-{object:05}.json"), four) as usize] += 1;
        }
        assert!(
            counts.iter().all(|count| (200..300).contains(count)),
            "{counts:?}"
        );
    }

    #[test]
    fn splits_make_an_object_store_source_splittable() {
        let source = FileSource::new();
        let contract = source.contract(&config(Some(4))).unwrap();
        assert_eq!(contract.topology, SourceTopology::Splittable);
        assert_eq!(contract.locality, SourceLocality::Portable);
        assert_eq!(
            contract.placement.map(|placement| placement.channels.get()),
            Some(4)
        );
        assert!(!contract.is_exact_delivery_certified());

        let mut local = config(Some(4));
        local.set("path", "/data/landing");
        let error = source.contract(&local).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("needs an object-store directory"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn every_object_is_read_once_across_a_resume() {
        let store = Arc::new(InMemory::new());
        put_objects(&store, 0..12, 3).await;
        let mut first = start(&store, Some(4), SourcePosition::Initial, None)
            .await
            .unwrap();
        assert_eq!(read(&mut first).await, lines(0..12, 3));
        let checkpoint = first.checkpoint();
        assert_eq!(checkpoint.get_metadata("splits"), Some("4"));
        assert_eq!(checkpoint.input_channels().map(<[Vec<u8>]>::len), Some(4));
        first.close().await.unwrap();

        put_objects(&store, 12..16, 3).await;
        let mut resumed = start(&store, Some(4), resume_from(checkpoint), None)
            .await
            .unwrap();
        assert_eq!(read(&mut resumed).await, lines(12..16, 3));
        resumed.close().await.unwrap();
    }

    #[tokio::test]
    async fn a_checkpoint_of_another_shape_is_refused() {
        let store = Arc::new(InMemory::new());
        put_objects(&store, 0..2, 1).await;
        let mut split = start(&store, Some(4), SourcePosition::Initial, None)
            .await
            .unwrap();
        read(&mut split).await;
        let split_checkpoint = split.checkpoint();
        split.close().await.unwrap();
        let mut unsplit = start(&store, None, SourcePosition::Initial, None)
            .await
            .unwrap();
        read(&mut unsplit).await;
        let unsplit_checkpoint = unsplit.checkpoint();
        unsplit.close().await.unwrap();

        for (splits, checkpoint, expected) in [
            (Some(2), &split_checkpoint, "has 4 splits"),
            (Some(4), &unsplit_checkpoint, "checkpoint.version=2"),
            (None, &split_checkpoint, "checkpoint.version=1"),
        ] {
            let error = start(&store, splits, resume_from(checkpoint.clone()), None)
                .await
                .expect_err("an incompatible checkpoint must be refused");
            assert!(error.to_string().contains(expected), "{error}");
        }
    }

    #[tokio::test]
    async fn a_checkpoint_with_misplaced_or_contradictory_state_is_refused() {
        let store = Arc::new(InMemory::new());
        put_objects(&store, 0..1, 1).await;
        let mut source = start(&store, Some(4), SourcePosition::Initial, None)
            .await
            .unwrap();
        read(&mut source).await;
        let checkpoint = source.checkpoint();
        source.close().await.unwrap();
        let four = NonZeroU32::new(4).unwrap();
        let channel = object_channel("o00.txt", four);
        let manifest = checkpoint
            .get_offset(&manifest_key(channel))
            .unwrap()
            .to_owned();
        let progress = FileProgress {
            path: "o00.txt".into(),
            size: 7,
            modified_ms: 0,
            content_sha256: String::new(),
            next_row: 1,
        };

        let mut misplaced = checkpoint.clone();
        misplaced.set_offset(manifest_key((channel + 1) % 4), manifest);
        misplaced.set_offset(manifest_key(channel), "[]");
        let mut stray = checkpoint.clone();
        stray.set_offset("unrelated", "1");
        let mut contradictory = checkpoint.clone();
        contradictory.set_offset(progress_key(channel), progress.encode());
        for (checkpoint, expected) in [
            (misplaced, "not its own"),
            (stray, "offsets of no split channel"),
            (contradictory, "contradictory progress"),
        ] {
            let error = start(&store, Some(4), resume_from(checkpoint), None)
                .await
                .expect_err("a malformed checkpoint must be refused");
            assert!(error.to_string().contains(expected), "{error}");
        }
    }

    #[tokio::test]
    async fn a_vnode_assignment_needs_splits() {
        let store = Arc::new(InMemory::new());
        let registry = Arc::new(VnodeRegistry::new_unassigned(VNODES));
        let error = start(&store, None, SourcePosition::Initial, Some((&registry, 1)))
            .await
            .expect_err("an unsplit source must refuse a vnode assignment");
        assert!(error.to_string().contains("without splits"), "{error}");
    }

    #[tokio::test]
    async fn cluster_sources_read_disjoint_objects_that_cover_the_directory() {
        let store = Arc::new(InMemory::new());
        put_objects(&store, 0..24, 2).await;
        let registry = Arc::new(VnodeRegistry::new_unassigned(VNODES));
        let mut a = start(
            &store,
            Some(4),
            SourcePosition::Initial,
            Some((&registry, 1)),
        )
        .await
        .unwrap();
        let mut b = start(
            &store,
            Some(4),
            SourcePosition::Initial,
            Some((&registry, 2)),
        )
        .await
        .unwrap();
        assert!(!a.checkpoint_ready().unwrap());
        assert!(a.try_checkpoint().unwrap().is_none());
        assert!(a.poll_batch(64).await.unwrap().is_none());

        let owners = [1, 2, 1, 2];
        publish(&registry, &owners, 1);
        let mut from_a = read(&mut a).await;
        let from_b = read(&mut b).await;
        assert!(!from_a.is_empty() && !from_b.is_empty());
        assert!(from_a.iter().all(|line| !from_b.contains(line)));
        from_a.extend(from_b);
        from_a.sort_unstable();
        assert_eq!(from_a, lines(0..24, 2));

        for (source, node) in [(&a, 1), (&b, 2)] {
            let checkpoint = source.try_checkpoint().unwrap().unwrap();
            assert_eq!(
                checkpoint.assignment_version().map(NonZeroU64::get),
                Some(1)
            );
            let channels: Vec<Vec<u8>> = routed(SOURCE, &owners, node, 4)
                .into_iter()
                .map(|channel| source_input_channel(SOURCE, channel).unwrap())
                .collect();
            assert_eq!(checkpoint.input_channels(), Some(channels.as_slice()));
        }
    }

    #[tokio::test]
    async fn a_committed_drain_hands_a_partly_read_object_over_at_its_next_row() {
        let store = Arc::new(InMemory::new());
        put_objects(&store, 0..8, 3).await;
        let (left, right) = (
            Arc::new(VnodeRegistry::new_unassigned(VNODES)),
            Arc::new(VnodeRegistry::new_unassigned(VNODES)),
        );
        publish(&left, &[1; 4], 1);
        publish(&right, &[1; 4], 1);
        let mut a = start(&store, Some(4), SourcePosition::Initial, Some((&left, 1)))
            .await
            .unwrap();
        let mut b = start(&store, Some(4), SourcePosition::Initial, Some((&right, 2)))
            .await
            .unwrap();
        let first = next_lines(&mut a, 1).await;
        assert_eq!(first.len(), 1);
        assert!(read(&mut b).await.is_empty());

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
        // Node 2 finishes the object node 1 was part way through, without its first row.
        let mut expected = lines(0..8, 3);
        expected.retain(|line| *line != first[0]);
        assert_eq!(read(&mut b).await, expected);
        assert!(read(&mut a).await.is_empty());
    }

    #[tokio::test]
    async fn a_resumed_node_keeps_every_partly_read_object_of_its_channels() {
        let store = Arc::new(InMemory::new());
        put_objects(&store, 0..12, 3).await;
        let registry = Arc::new(VnodeRegistry::new_unassigned(VNODES));
        publish(&registry, &[1, 2, 1, 2], 1);
        let mut a = start(
            &store,
            Some(4),
            SourcePosition::Initial,
            Some((&registry, 1)),
        )
        .await
        .unwrap();
        let mut b = start(
            &store,
            Some(4),
            SourcePosition::Initial,
            Some((&registry, 2)),
        )
        .await
        .unwrap();
        let mut first_rows = next_lines(&mut a, 1).await;
        first_rows.extend(next_lines(&mut b, 1).await);
        let cut = merge(
            &[
                a.try_checkpoint().unwrap().unwrap(),
                b.try_checkpoint().unwrap().unwrap(),
            ],
            1,
        );

        // One node now owns both partly read objects; its first checkpoint keeps both.
        let mut resumed = start(&store, Some(4), resume_from((*cut).clone()), None)
            .await
            .unwrap();
        let progress = resumed
            .checkpoint()
            .offsets()
            .keys()
            .filter(|key| key.starts_with("file_progress."))
            .count();
        assert_eq!(progress, 2);
        let mut expected = lines(0..12, 3);
        expected.retain(|line| !first_rows.contains(line));
        assert_eq!(read(&mut resumed).await, expected);
    }
}
