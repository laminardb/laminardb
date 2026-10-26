//! Reads of a synthesized two-participant committed cut, the shape a cluster checkpoint has:
//! each participant lists the vnode frames it owns and its own view-log segment.

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Array, ArrayRef, Int64Array, RecordBatch, TimestampMillisecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use bytes::Bytes;
use datafusion::prelude::{col, lit};
use laminar_core::checkpoint::{
    checkpoint_sha256, ByteRange, CheckpointAssignmentFence, CheckpointAttempt, CheckpointManifest,
    CheckpointParticipant, CheckpointScope, CheckpointStore, EventTimeBounds, NodeDataObject,
    ObjectStoreCheckpointStore, PipelineIdentity, StateChunkId, StateFrame, StateFrameKey, ViewLog,
    ViewLogSegment,
};
use laminar_core::state::KeyGroupCount;
use object_store::memory::InMemory;
use object_store::ObjectStore;

use super::*;
use crate::checkpoint_coordinator::{CheckpointConfig, CheckpointCoordinator};
use crate::mv_store::checkpoint::batches_to_ipc;
use crate::mv_store::{MvStore, ViewKey, ViewRetention};

const KEY_GROUPS: u16 = 4;
/// Participant, the vnodes it owns, and the keys its `events` segment holds. Key `k` happened at
/// `k × 100` ms.
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

fn events_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, true),
        Field::new("ts", DataType::Timestamp(TimeUnit::Millisecond, None), true),
    ]))
}

fn events_rows(keys: &[i64]) -> RecordBatch {
    RecordBatch::try_new(
        events_schema(),
        vec![
            int_column(keys.to_vec()),
            Arc::new(TimestampMillisecondArray::from(
                keys.iter().map(|k| k * 100).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

/// `counts` holds keys 1..=20 with n = 10 × k; `events` is an append view stored as `events`.
fn view_store(events: MvStorageMode) -> MvStore {
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
    store.create_mv("events", events_schema(), events).unwrap();
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
    (participant, vnodes, events): (u64, &[u16], &[i64]),
    checkpoint: u64,
    frontier: Option<i64>,
    frames: &[(u32, Bytes)],
    deployment_id: &str,
    fence: &CheckpointAssignmentFence,
) -> (CheckpointManifest, Bytes, Bytes) {
    let mut manifest =
        CheckpointManifest::new_with_key_group_count(checkpoint, checkpoint, key_groups());
    manifest.bind_participant(participant);
    manifest.deployment_id = deployment_id.into();
    manifest.assignment_fence = Some(fence.clone());
    manifest.reassignment_portable = true;
    manifest.owned_vnodes = vnodes.to_vec();
    let chunk = manifest.node_data.chunk;
    let mut node_data = Vec::new();
    for &vnode in vnodes {
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

    let rows = events_rows(events);
    let segment = batches_to_ipc(&events_schema(), [&rows]).unwrap();
    manifest.view_log = Some(NodeDataObject {
        chunk,
        object_length: segment.len() as u64,
        sha256: checkpoint_sha256(&segment),
    });
    manifest.view_logs = vec![ViewLog {
        view: "events".into(),
        next_sequence: 1,
        frontier,
        idle: false,
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
            event_time: Some(EventTimeBounds {
                min: events[0] * 100,
                max: events[events.len() - 1] * 100,
            }),
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
    decisions: Arc<CheckpointDecisionStore>,
    deployment_id: String,
    _cuts: tokio::sync::watch::Sender<Option<Arc<CommittedCut>>>,
}

async fn synthesized_cut() -> SynthesizedCut {
    synthesized_cut_with(MvStorageMode::append_rows(100), [None, None]).await
}

/// One committed cut of two participants, written to an in-memory object store, where each
/// participant logged `events` with its frontier in `frontiers`.
async fn synthesized_cut_with(
    events: MvStorageMode,
    frontiers: [Option<i64>; 2],
) -> SynthesizedCut {
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
    let mut store = view_store(events);
    let frames = store.full_view_frames("counts");
    let mut prepared = Vec::new();
    for ((participant, vnodes, keys), frontier) in PARTICIPANTS.into_iter().zip(frontiers) {
        let (manifest, node_data, segment) = participant_manifest(
            (participant, &vnodes, keys),
            2,
            frontier,
            &frames,
            &deployment_id,
            &fence,
        );
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
    coordinator
        .bind_deployment_id(deployment_id.clone())
        .unwrap();
    let index = coordinator
        .build_committed_index(
            CheckpointAttempt::canonical(2),
            CheckpointScope::Cluster,
            Some(fence),
            None,
            &crate::checkpoint_coordinator::CommittedFrontiers::default(),
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
        Arc::clone(&decisions),
        (cuts, Arc::default()),
        1 << 20,
    );
    SynthesizedCut {
        objects,
        views,
        store,
        decisions,
        deployment_id,
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
async fn a_cluster_cut_keeps_append_rows_against_its_committed_frontier() {
    let interval = MvStorageMode::AppendLog {
        retention: ViewRetention::EventTime {
            column: 1,
            interval_ms: 150,
        },
    };
    // Participant 1 alone would keep rows from 150 ms and drop key 1. The cut's frontier is the
    // participants' minimum, 200, so every participant keeps rows from 50 ms.
    let synthesized = synthesized_cut_with(interval.clone(), [Some(300), Some(200)]).await;
    let cut = synthesized
        .views
        .pin(CommittedRead::Latest, deadline())
        .await
        .unwrap();
    let events = synthesized.store.view_shape("events").unwrap();
    let rows = cut
        .read_view(&synthesized.views, &events, &[])
        .await
        .unwrap();
    assert_eq!(values(&rows, 0), [1, 2, 3]);

    // At frontier 400 the horizon is 250 on both participants: only key 3 remains.
    let synthesized = synthesized_cut_with(interval, [Some(400), Some(400)]).await;
    let cut = synthesized
        .views
        .pin(CommittedRead::Latest, deadline())
        .await
        .unwrap();
    let rows = cut
        .read_view(&synthesized.views, &events, &[])
        .await
        .unwrap();
    assert_eq!(values(&rows, 0), [3]);
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn a_cluster_store_keeps_live_rows_against_the_newest_committed_frontier() {
    let interval = MvStorageMode::AppendLog {
        retention: ViewRetention::EventTime {
            column: 1,
            interval_ms: 150,
        },
    };
    let mut synthesized = synthesized_cut_with(interval, [Some(300), Some(200)]).await;
    let store = &mut synthesized.store;
    store.follow_committed_frontiers(synthesized._cuts.subscribe());
    store.update("events", &events_rows(&[1, 2, 3]));
    // This node's frontier alone would keep only rows from 850 ms; the cut's is 200.
    store.advance_view_frontiers(|_| {
        Some(crate::operator_graph::InputFrontier {
            watermark: Some(1_000),
            idle: false,
        })
    });
    let rows = store.to_record_batch("events").unwrap().unwrap();
    assert_eq!(values(&[rows], 0), [1, 2, 3]);
}

/// Participant 2 leaves after the synthesized cut. Participant 1 takes every vnode, writes key 4
/// and, as an adopter would, lists participant 2's `events` segment by reference.
#[tokio::test]
async fn a_cut_after_a_participant_leaves_lists_its_view_log_segments() {
    let mut synthesized = synthesized_cut().await;
    let predecessor = synthesized._cuts.borrow().clone().unwrap();
    let reference = synthesized
        .decisions
        .create_committed_checkpoint(&predecessor.index)
        .await
        .unwrap();
    let leaver = crate::checkpoint_coordinator::load_index_manifests(
        &checkpoint_store(&synthesized.objects, 1),
        &predecessor.index,
    )
    .await
    .unwrap()
    .into_iter()
    .find(|manifest| manifest.participant_id == 2)
    .unwrap();

    let fence = CheckpointAssignmentFence::from_owner_map(
        10,
        &[1, 1, 1, 1],
        vec![CheckpointParticipant {
            node_id: 1,
            boot_incarnation: uuid::Uuid::from_u128(1),
        }],
    )
    .unwrap();
    let frames = synthesized.store.full_view_frames("counts");
    let (mut manifest, node_data, segment) = participant_manifest(
        (1, &[0, 1, 2, 3], &[4]),
        3,
        None,
        &frames,
        &synthesized.deployment_id,
        &fence,
    );
    let leaver_object = leaver.view_log.clone().unwrap();
    let mut adopted = leaver.view_logs[0].segments[0].clone();
    adopted.partition = 0;
    adopted.sequence = 1;
    let log = &mut manifest.view_logs[0];
    log.segments.push(adopted);
    log.next_sequence = 2;
    manifest.referenced_view_logs = vec![laminar_core::checkpoint::ReferencedStateChunk {
        chunk: leaver_object.chunk,
        object_length: leaver_object.object_length,
        sha256: leaver_object.sha256,
        ref_count: std::num::NonZeroU32::new(1).unwrap(),
    }];
    let writer = checkpoint_store(&synthesized.objects, 1);
    writer
        .save_view_log(manifest.view_log.as_ref().unwrap(), &[segment])
        .await
        .unwrap();

    let mut coordinator = CheckpointCoordinator::new(
        CheckpointConfig::default(),
        Box::new(checkpoint_store(&synthesized.objects, 1)),
    )
    .unwrap();
    coordinator
        .bind_pipeline_identity(PipelineIdentity::empty())
        .unwrap();
    coordinator
        .bind_deployment_id(synthesized.deployment_id.clone())
        .unwrap();
    coordinator
        .set_decision_store(Arc::clone(&synthesized.decisions))
        .unwrap();
    let frontiers =
        crate::checkpoint_coordinator::CommittedFrontiers::of(&predecessor.index).unwrap();
    let build = |manifest: CheckpointManifest, encoded: Bytes| {
        let prepared = vec![(manifest, encoded)];
        let coordinator = &coordinator;
        let reference = reference.clone();
        let fence = fence.clone();
        let frontiers = frontiers.clone();
        async move {
            coordinator
                .build_validated_committed_index_until(
                    CheckpointAttempt::canonical(3),
                    CheckpointScope::Cluster,
                    Some(fence),
                    Some(reference),
                    &frontiers,
                    &prepared,
                    None,
                    tokio::time::Instant::now() + Duration::from_secs(30),
                )
                .await
                .map(|index| (index, prepared))
        }
    };

    // Without participant 2's segment the cut would lose its rows.
    let mut lost = manifest.clone();
    lost.view_logs[0].segments.pop();
    lost.referenced_view_logs.clear();
    let encoded = Bytes::from(laminar_core::checkpoint::checkpoint_manifest_bytes(&lost).unwrap());
    let error = build(lost, encoded).await.unwrap_err();
    assert!(error.to_string().contains("0 times"), "{error}");

    let encoded = writer
        .save_checkpoint(&manifest, &[node_data])
        .await
        .unwrap();
    let (index, prepared) = build(manifest, encoded).await.unwrap();
    let (_, successor) = index.encode_and_reference().unwrap();
    synthesized
        ._cuts
        .send_replace(Some(Arc::new(CommittedCut::new(
            index,
            successor,
            prepared.iter().map(|(manifest, _)| manifest),
        ))));
    let cut = synthesized
        .views
        .pin(CommittedRead::Latest, deadline())
        .await
        .unwrap();
    let events = synthesized.store.view_shape("events").unwrap();
    let rows = cut
        .read_view(&synthesized.views, &events, &[])
        .await
        .unwrap();
    assert_eq!(values(&rows, 0), [3, 4]);
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
