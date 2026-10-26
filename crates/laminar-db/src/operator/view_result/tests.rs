use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use laminar_core::checkpoint::{CheckpointAssignmentFence, CheckpointParticipant};
use laminar_core::state::{KeyGroupCount, NodeId};
use rustc_hash::FxHashSet;

use super::*;
use crate::mv_store::{MvStorageMode, ViewKey};
use crate::operator_graph::{
    ManagedVnodeRestore, ManagedVnodeTransition, ManagedVnodeTransitionMode,
};

const VNODES: u32 = 4;

/// A store whose view `totals` places rows by their full row and holds `values`.
fn totals(values: &[i64]) -> MvStore {
    let mut store = MvStore::new();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "total",
        DataType::Int64,
        true,
    )]));
    store
        .create_mv(
            "totals",
            schema,
            MvStorageMode::Changelog {
                key: ViewKey::MultisetByRow,
                key_groups: KeyGroupCount::try_from(4_u16).unwrap(),
            },
        )
        .unwrap();
    let column = |values: Vec<i64>| Arc::new(Int64Array::from(values)) as ArrayRef;
    let changes = RecordBatch::try_from_iter(vec![
        ("total", column(values.to_vec())),
        (
            laminar_core::changelog::WEIGHT_COLUMN,
            column(vec![1; values.len()]),
        ),
    ])
    .unwrap();
    store.update("totals", &changes);
    store
}

fn fence(version: u64, owners: &[u64]) -> CheckpointAssignmentFence {
    let participants = [7, 8]
        .into_iter()
        .filter(|node| owners.contains(node))
        .map(|node_id| CheckpointParticipant {
            node_id,
            boot_incarnation: uuid::Uuid::from_u128(u128::from(node_id)),
        })
        .collect();
    CheckpointAssignmentFence::from_owner_map(version, owners, participants).unwrap()
}

/// A survivor restoring a cut taken before a failed owner's vnodes moved to it rebuilds the
/// view from every frame, through the recovery image that replaces its store.
#[test]
fn a_checkpoint_bootstrap_restores_the_view_through_the_recovery_image() {
    let values: Vec<i64> = (1..=40).collect();
    let frames = totals(&values).full_view_frames("totals");
    let store = Arc::new(parking_lot::RwLock::new(totals(&[])));
    let mut node = ViewResultOperator::new(
        "totals".into(),
        OperatorStateClass::VnodeKeyed,
        Arc::clone(&store),
    );
    let predecessor_owners = [NodeId(7), NodeId(8), NodeId(7), NodeId(8)];
    let restores: Vec<_> = frames
        .iter()
        .map(|(vnode, state)| ManagedVnodeRestore {
            participant_id: if vnode % 2 == 0 { 7 } else { 8 },
            vnode: *vnode,
            state: state.as_ref(),
        })
        .collect();
    node.prepare_vnode_transition(ManagedVnodeTransition {
        predecessor: &fence(1, &[7, 8, 7, 8]),
        target: &fence(2, &[7; VNODES as usize]),
        revoked: &FxHashSet::default(),
        restores: &restores,
        whole_restores: &[],
        mode: ManagedVnodeTransitionMode::CheckpointBootstrap {
            predecessor_owners: &predecessor_owners,
        },
    })
    .unwrap();
    node.publish_vnode_transition();

    let image = store
        .read()
        .recovery_image(node.take_restored_view().into_iter().collect(), None, &[])
        .unwrap();
    let batch = image.to_record_batch("totals").unwrap().unwrap();
    let mut restored = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .values()
        .to_vec();
    restored.sort_unstable();
    assert_eq!(restored, values);
}
