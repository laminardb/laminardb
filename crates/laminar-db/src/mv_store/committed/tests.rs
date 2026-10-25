//! Reads of a synthesized two-participant committed cut, the shape a cluster checkpoint has:
//! each participant lists the vnode frames it owns and its own view-log segment.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Array, ArrayRef, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use bytes::Bytes;
use datafusion::prelude::{col, lit};
use laminar_core::checkpoint::{
    checkpoint_sha256, ByteRange, CheckpointAssignmentFence, CheckpointAttempt, CheckpointManifest,
    CheckpointParticipant, CheckpointScope, CheckpointStore, NodeDataObject,
    ObjectStoreCheckpointStore, PipelineIdentity, StateChunkId, StateFrame, StateFrameKey, ViewLog,
    ViewLogSegment,
};
use laminar_core::state::KeyGroupCount;
use object_store::memory::InMemory;
use object_store::ObjectStore;

use super::*;
use crate::checkpoint_coordinator::{CheckpointConfig, CheckpointCoordinator};
use crate::mv_store::checkpoint::batches_to_ipc;
use crate::mv_store::{MvStore, ViewKey};

const KEY_GROUPS: u16 = 4;
/// Participant, the vnodes it owns, and the keys its `events` segment holds.
const PARTICIPANTS: [(u64, [u16; 2], &[i64]); 2] = [(1, [0, 1], &[1, 2]), (2, [2, 3], &[3])];

fn key_groups() -> KeyGroupCount {
    KeyGroupCount::try_from(KEY_GROUPS).unwrap()
}

fn int_schema(names: &[&str]) -> SchemaRef {
    Arc::new(Schema::new(
        names
            .iter()
            .map(|name| Field::new(*name, DataType::Int64, true))
            .collect::<Vec<_>>(),
    ))
}

fn int_column(values: Vec<i64>) -> ArrayRef {
    Arc::new(Int64Array::from(values))
}

/// `counts` holds keys 1..=20 with n = 10 × k; `events` is an append view.
fn view_store() -> MvStore {
    let mut store = MvStore::new();
    store
        .create_mv(
            "counts",
            int_schema(&["k", "n"]),
            MvStorageMode::Changelog {
                key: ViewKey::Keyed(vec![0]),
                key_groups: key_groups(),
            },
        )
        .unwrap();
    store
        .create_mv(
            "events",
            int_schema(&["k"]),
            MvStorageMode::append_rows(100),
        )
        .unwrap();
    let keys: Vec<i64> = (1..=20).collect();
    let changes = RecordBatch::try_from_iter(vec![
        ("k", int_column(keys.clone())),
        ("n", int_column(keys.iter().map(|k| k * 10).collect())),
        (
            laminar_core::changelog::WEIGHT_COLUMN,
            int_column(vec![1; keys.len()]),
        ),
    ])
    .unwrap();
    store.update("counts", &changes);
    store
}

fn participant_manifest(
    participant: u64,
    vnodes: [u16; 2],
    events: &[i64],
    frames: &[(u32, Bytes)],
    deployment_id: &str,
    fence: &CheckpointAssignmentFence,
) -> (CheckpointManifest, Bytes, Bytes) {
    let mut manifest = CheckpointManifest::new_with_key_group_count(2, 2, key_groups());
    manifest.bind_participant(participant);
    manifest.deployment_id = deployment_id.into();
    manifest.assignment_fence = Some(fence.clone());
    manifest.reassignment_portable = true;
    manifest.owned_vnodes = vnodes.to_vec();
    let chunk = manifest.node_data.chunk;
    let mut node_data = Vec::new();
    for vnode in vnodes {
        let (_, frame) = frames
            .iter()
            .find(|(captured, _)| *captured == u32::from(vnode))
            .unwrap();
        manifest.state_frames.push(StateFrame {
            key: StateFrameKey::Vnode {
                operator_id: "graph:counts::view".into(),
                vnode,
            },
            chunk,
            range: ByteRange {
                offset: node_data.len() as u64,
                length: frame.len() as u64,
            },
            sha256: checkpoint_sha256(frame),
        });
        node_data.extend_from_slice(frame);
    }
    manifest.node_data.object_length = node_data.len() as u64;
    manifest.node_data.sha256 = checkpoint_sha256(&node_data);

    let rows = RecordBatch::try_from_iter(vec![("k", int_column(events.to_vec()))]).unwrap();
    let segment = batches_to_ipc(&int_schema(&["k"]), [&rows]).unwrap();
    manifest.view_log = Some(NodeDataObject {
        chunk,
        object_length: segment.len() as u64,
        sha256: checkpoint_sha256(&segment),
    });
    manifest.view_logs = vec![ViewLog {
        view: "events".into(),
        next_sequence: 1,
        frontier: None,
        segments: vec![ViewLogSegment {
            partition: vnodes[0],
            sequence: 0,
            chunk,
            range: ByteRange {
                offset: 0,
                length: segment.len() as u64,
            },
            sha256: checkpoint_sha256(&segment),
            rows: events.len() as u64,
            event_time: None,
        }],
    }];
    (manifest, node_data.into(), segment.into())
}

fn checkpoint_store(
    objects: &Arc<dyn ObjectStore>,
    participant: u64,
) -> ObjectStoreCheckpointStore {
    ObjectStoreCheckpointStore::new(Arc::clone(objects), "")
        .with_key_group_count(key_groups())
        .with_participant_id(participant)
}

struct SynthesizedCut {
    objects: Arc<dyn ObjectStore>,
    views: CommittedViews,
    store: MvStore,
    _cuts: tokio::sync::watch::Sender<Option<Arc<CommittedCut>>>,
}

/// One committed cut of two participants, written to an in-memory object store.
async fn synthesized_cut() -> SynthesizedCut {
    let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let decisions = Arc::new(CheckpointDecisionStore::new(Arc::clone(&objects)));
    let deployment_id = decisions.load_or_create_deployment_id().await.unwrap();
    let participants = PARTICIPANTS
        .iter()
        .map(|(id, _, _)| CheckpointParticipant {
            node_id: *id,
            boot_incarnation: uuid::Uuid::from_u128(u128::from(*id)),
        })
        .collect();
    let fence = CheckpointAssignmentFence::from_owner_map(9, &[1, 1, 2, 2], participants).unwrap();
    let mut store = view_store();
    let frames = store.full_view_frames("counts");
    let mut prepared = Vec::new();
    for (participant, vnodes, events) in PARTICIPANTS {
        let (manifest, node_data, segment) =
            participant_manifest(participant, vnodes, events, &frames, &deployment_id, &fence);
        let writer = checkpoint_store(&objects, participant);
        writer
            .save_view_log(manifest.view_log.as_ref().unwrap(), &[segment])
            .await
            .unwrap();
        let encoded = writer
            .save_checkpoint(&manifest, &[node_data])
            .await
            .unwrap();
        prepared.push((manifest, encoded));
    }
    let mut coordinator = CheckpointCoordinator::new(
        CheckpointConfig::default(),
        Box::new(checkpoint_store(&objects, 1)),
    )
    .unwrap();
    coordinator
        .bind_pipeline_identity(PipelineIdentity::empty())
        .unwrap();
    coordinator.bind_deployment_id(deployment_id).unwrap();
    let index = coordinator
        .build_committed_index(
            CheckpointAttempt::canonical(2),
            CheckpointScope::Cluster,
            Some(fence),
            None,
            &BTreeMap::new(),
            &prepared,
            None,
        )
        .unwrap();
    let (_, reference) = index.encode_and_reference().unwrap();
    let cut = CommittedCut::new(
        index,
        reference,
        prepared.iter().map(|(manifest, _)| manifest),
    );
    let (sender, cuts) = tokio::sync::watch::channel(Some(Arc::new(cut)));
    let views = CommittedViews::new(
        Arc::new(checkpoint_store(&objects, 1)),
        decisions,
        (cuts, Arc::default()),
        1 << 20,
    );
    SynthesizedCut {
        objects,
        views,
        store,
        _cuts: sender,
    }
}

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(30)
}

fn values(batches: &[RecordBatch], column: usize) -> Vec<i64> {
    let mut values: Vec<i64> = batches
        .iter()
        .flat_map(|batch| {
            let array = batch
                .column(column)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            (0..array.len())
                .map(|row| array.value(row))
                .collect::<Vec<_>>()
        })
        .collect();
    values.sort_unstable();
    values
}

#[tokio::test]
async fn a_cut_reads_each_view_from_every_participant() {
    let synthesized = synthesized_cut().await;
    let cut = synthesized
        .views
        .pin(CommittedRead::Latest, deadline())
        .await
        .unwrap();
    assert_eq!((cut.epoch, cut.checkpoint_id), (2, 2));

    let counts = synthesized.store.view_shape("counts").unwrap();
    let rows = cut
        .read_view(&synthesized.views, &counts, &[])
        .await
        .unwrap();
    assert_eq!(values(&rows, 0), (1..=20).collect::<Vec<_>>());
    assert_eq!(
        values(&rows, 1),
        (1..=20).map(|k| k * 10).collect::<Vec<_>>()
    );

    let events = synthesized.store.view_shape("events").unwrap();
    let rows = cut
        .read_view(&synthesized.views, &events, &[])
        .await
        .unwrap();
    assert_eq!(values(&rows, 0), [1, 2, 3]);
}

#[tokio::test]
async fn a_full_key_equality_reads_only_the_frame_of_its_vnode() {
    let synthesized = synthesized_cut().await;
    let cut = synthesized
        .views
        .pin(CommittedRead::Latest, deadline())
        .await
        .unwrap();
    let counts = synthesized.store.view_shape("counts").unwrap();
    let rows = cut
        .read_view(&synthesized.views, &counts, &[col("k").eq(lit(7_i64))])
        .await
        .unwrap();
    let keys = values(&rows, 0);
    assert!(keys.contains(&7) && keys.len() < 20, "{keys:?}");
    assert_eq!(synthesized.views.artifacts.len(), 1);
}

#[tokio::test]
async fn a_cached_frame_is_not_read_again() {
    let synthesized = synthesized_cut().await;
    let cut = synthesized
        .views
        .pin(CommittedRead::Latest, deadline())
        .await
        .unwrap();
    let counts = synthesized.store.view_shape("counts").unwrap();
    let first = cut
        .read_view(&synthesized.views, &counts, &[])
        .await
        .unwrap();
    for (participant, _, _) in PARTICIPANTS {
        checkpoint_store(&synthesized.objects, participant)
            .delete_node_data(StateChunkId {
                participant_id: participant,
                checkpoint_id: 2,
            })
            .await
            .unwrap();
    }
    let second = cut
        .read_view(&synthesized.views, &counts, &[])
        .await
        .unwrap();
    assert_eq!(values(&first, 0), values(&second, 0));
}

#[tokio::test]
async fn a_view_created_after_the_cut_has_no_committed_state() {
    let mut synthesized = synthesized_cut().await;
    synthesized
        .store
        .create_mv("later", int_schema(&["k"]), MvStorageMode::append_rows(10))
        .unwrap();
    let cut = synthesized
        .views
        .pin(CommittedRead::Latest, deadline())
        .await
        .unwrap();
    let later = synthesized.store.view_shape("later").unwrap();
    let error = cut
        .read_view(&synthesized.views, &later, &[])
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        DbError::CommittedRead(CommittedReadError::ViewNotCommitted { epoch: 2, .. })
    ));
}

#[test]
fn the_cache_admits_a_merged_segment() {
    let cache = super::cache::ArtifactCache::new(256 << 20);
    let rows = 2 << 20;
    let batch = RecordBatch::try_from_iter(vec![("k", int_column(vec![7; rows]))]).unwrap();
    assert!(batch.get_array_memory_size() >= 16 << 20);
    cache.insert("merged", vec![batch.clone()].into());
    assert_eq!(cache.len(), 1);
    assert!(cache.get("merged", &batch.schema()).is_some());
}
