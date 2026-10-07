//! Public process bindings owned by database startup, checkpoint and recovery rounds.
//! Test control and shared storage are in memory; owner-local output observation is private.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{BooleanArray, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use futures::FutureExt as _;
use laminar_core::checkpoint::{CommittedCheckpointIndex, CommittedCheckpointRef};
use laminar_core::cluster::control::{
    AssignmentSnapshot, AssignmentSnapshotStore, ClusterController, RecoverPhase,
    RecoveryAnnouncement,
};
use laminar_core::cluster::discovery::NodeId;
use laminar_core::state::{NodeId as StateNodeId, VnodeRegistry};

use super::tests::{descriptor, input_batch, AccountActivity};
#[cfg(feature = "process-remote")]
use super::ProcessRuntime;
use super::{
    NativeProcessFunction, ProcessActivation, ProcessActivationResult, ProcessCallback,
    ProcessFunctionDescriptor, ProcessHandler, ValueState,
};
use crate::{DbError, LaminarDB};

mod runtime;
mod source;

const DEADLINE: Duration = Duration::from_secs(40);
const LEASE_TTL: Duration = Duration::from_secs(120);
type ActivityRow = (String, String, i64, bool, i64);

#[derive(Clone, Debug, PartialEq, Eq)]
struct CallbackStamp {
    id: u64,
    key: String,
    timestamp: i64,
    timer: bool,
    state: ValueState,
}

struct ObservedActivity {
    callbacks: Arc<parking_lot::Mutex<Vec<CallbackStamp>>>,
    fail: Arc<AtomicBool>,
}

impl NativeProcessFunction for ObservedActivity {
    fn invoke(
        &self,
        activations: &[ProcessActivation],
    ) -> Result<Vec<ProcessActivationResult>, DbError> {
        let mut observed = self.callbacks.lock();
        assert!(observed.len() + activations.len() <= 64);
        for activation in activations {
            observed.push(CallbackStamp {
                id: activation.id,
                key: activation.key_text.clone(),
                timestamp: activation.event_time_us,
                timer: matches!(activation.callback, ProcessCallback::Timer { .. }),
                state: activation.state,
            });
            if let ProcessCallback::Input(batch) = &activation.callback {
                let amount = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(0);
                if amount == 1 && self.fail.swap(false, Ordering::AcqRel) {
                    return Err(DbError::StatefulOperatorPartialApply(
                        "cluster process qualification fault".into(),
                    ));
                }
            }
        }
        drop(observed);
        AccountActivity.invoke(activations)
    }
}

fn key_for(vnode: u32, label: &str) -> String {
    (0..1_000)
        .map(|index| format!("{label}-{index}"))
        .find(|key| {
            laminar_core::shuffle::row_vnodes(&input_batch(&[(key, 0, 100_000)]), &[0], 2).unwrap()
                [0]
                == vnode
        })
        .unwrap()
}

fn script() -> Arc<[RecordBatch]> {
    let threshold_account = key_for(0, "a");
    let peer_account = key_for(1, "b");
    let fault_account = key_for(0, "c");
    let later_account = key_for(1, "d");
    let watermark_account = key_for(0, "e");
    Arc::from(vec![
        input_batch(&[(&threshold_account, 60, 100_000)]),
        input_batch(&[(&peer_account, 7, 105_000)]),
        input_batch(&[(&threshold_account, 50, 108_000)]),
        input_batch(&[(&peer_account, 13, 107_000)]),
        input_batch(&[(&fault_account, 1, 125_000)]),
        input_batch(&[(&later_account, 2, 145_000)]),
        input_batch(&[(&watermark_account, 0, 165_000)]),
    ])
}

fn activity_rows(batch: &RecordBatch) -> Vec<ActivityRow> {
    let key = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let kind = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let total = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let crossed = batch
        .column(3)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    let time = batch
        .column(4)
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap();
    (0..batch.num_rows())
        .map(|row| {
            (
                key.value(row).into(),
                kind.value(row).into(),
                total.value(row),
                crossed.value(row),
                time.value(row),
            )
        })
        .collect()
}

async fn wait_until(mut predicate: impl FnMut() -> bool) {
    tokio::time::timeout(DEADLINE, async {
        while !predicate() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("database process qualification did not reach its bounded condition");
}

impl runtime::Rig {
    fn available(&self, prefix: u64) {
        for peer in &self.peers {
            peer.probe.available.store(prefix, Ordering::Release);
        }
    }

    async fn commit_prefix(&mut self) -> (CommittedCheckpointRef, CommittedCheckpointIndex) {
        self.available(2);
        wait_until(|| self.output_ready(2)).await;
        let committed = self.commit_source_cut("2", 104).await;
        assert_eq!(committed.1.checkpoint_id, 1);
        self.callbacks.lock().clear();
        self.output.clear();
        committed
    }

    async fn commit_source_cut(
        &self,
        cursor: &str,
        watermark: i64,
    ) -> (CommittedCheckpointRef, CommittedCheckpointIndex) {
        let result = self.peers[0]
            .db
            .as_ref()
            .unwrap()
            .checkpoint_with_timeout(DEADLINE)
            .await
            .unwrap();
        assert!(result.success, "{result:?}");
        let authority = self.peers[0].controller.checkpoint_authority().unwrap();
        let (outcome, index) = authority
            .cluster_outcome_with_committed_checkpoint(result.epoch)
            .await
            .unwrap()
            .unwrap();
        let index = index.unwrap();
        let reference = outcome.committed_checkpoint.unwrap();
        assert_eq!(index.source_offsets["events"].offsets["cursor"], cursor);
        assert_eq!(
            index.source_offsets["events"].input_channels.as_deref(),
            Some([source::CHANNEL.to_vec()].as_slice())
        );
        let physical = index
            .channel_progress
            .iter()
            .filter(|progress| {
                progress.input_channel != laminar_core::checkpoint::SINGLETON_WATERMARK_CHANNEL
            })
            .collect::<Vec<_>>();
        assert_eq!(physical.len(), 1);
        assert_eq!(physical[0].source_name, "events");
        assert_eq!(physical[0].participant_id, 7);
        assert_eq!(physical[0].input_channel, source::CHANNEL);
        assert_eq!(physical[0].watermark, Some(watermark));
        assert!(!physical[0].idle);
        assert_eq!(index.channel_progress.len(), self.peers.len());
        for marker in index
            .channel_progress
            .iter()
            .filter(|progress| progress.participant_id != 7)
        {
            assert_eq!(marker.source_name, "events");
            assert_eq!(
                marker.input_channel,
                laminar_core::checkpoint::SINGLETON_WATERMARK_CHANNEL
            );
            assert!(marker.idle);
        }
        assert_eq!(index.participants.len(), self.peers.len());
        assert_eq!(
            index.assignment_fence.as_ref(),
            Some(&self.assignment.assignment_fence().unwrap())
        );
        (reference, index)
    }

    async fn finish_suffix(&mut self) {
        self.available(7);
        // Timers remain capped by the committed prefix until this source decision is durable.
        wait_until(|| self.output_ready(5)).await;
        let (_, index) = self.commit_source_cut("7", 164).await;
        assert_eq!(index.checkpoint_id, 2);
        wait_until(|| self.output_ready(9)).await;
    }

    async fn replay_after_fault(
        &mut self,
        reference: &CommittedCheckpointRef,
        index: &CommittedCheckpointIndex,
    ) {
        for peer in &self.peers {
            peer.db
                .as_ref()
                .unwrap()
                .enable_coordinated_recovery()
                .unwrap();
        }
        self.available(4);
        wait_until(|| self.output_ready(2)).await;
        for peer in &self.peers {
            peer.probe.hold.store(true, Ordering::Release);
        }
        self.fail.store(true, Ordering::Release);
        self.available(7);
        wait_until(|| {
            self.peers
                .iter()
                .all(|peer| peer.probe.starts.lock().len() == 2)
        })
        .await;
        let start = self.validate_held_restore(reference, index).await;
        for peer in &self.peers {
            peer.probe.hold.store(false, Ordering::Release);
        }
        wait_until(|| {
            self.peers
                .iter()
                .all(|peer| !peer.db.as_ref().unwrap().cluster_intake_fenced())
        })
        .await;
        let release = self.peers[0]
            .controller
            .latest_committed_recover_release()
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            release,
            RecoveryAnnouncement {
                round: start.round,
                phase: RecoverPhase::ReleaseCommitted { epoch: index.epoch }
            }
        );
        for peer in &self.peers {
            let installed = peer
                .db
                .as_ref()
                .unwrap()
                .installed_vnode_state
                .lock()
                .clone()
                .unwrap();
            assert_eq!(
                installed.assignment(),
                index.assignment_fence.as_ref().unwrap()
            );
            assert!(installed.matches(
                index.assignment_fence.as_ref().unwrap(),
                &index.pipeline_identity
            ));
        }
        self.finish_suffix().await;
    }

    async fn validate_held_restore(
        &mut self,
        reference: &CommittedCheckpointRef,
        index: &CommittedCheckpointIndex,
    ) -> RecoveryAnnouncement {
        let start = self.peers[0]
            .controller
            .observe_recover_control()
            .await
            .unwrap()
            .unwrap();
        assert_eq!(start.phase, RecoverPhase::Start { epoch: index.epoch });
        assert_eq!(
            start.round.assignment_fence,
            self.assignment.assignment_fence().unwrap()
        );
        assert!(start
            .round
            .faults
            .iter()
            .any(|fault| fault.reporter == NodeId(7)));
        for peer in &self.peers {
            let resume = peer.probe.starts.lock()[1].clone();
            assert_eq!(resume.cursor, 2);
            assert_eq!(resume.assignment, self.assignment.version);
            assert_eq!(resume.channels, vec![source::CHANNEL.to_vec()]);
            assert!(peer.db.as_ref().unwrap().cluster_intake_fenced());
            assert!(peer.controller.is_recovering());
        }
        let authority = self.peers[0].controller.checkpoint_authority().unwrap();
        let selected = authority
            .highest_cluster_committed_outcome()
            .await
            .unwrap()
            .unwrap();
        assert_eq!(selected.committed_checkpoint.as_ref(), Some(reference));
        let polls = self
            .peers
            .iter()
            .map(|peer| peer.probe.polls.load(Ordering::Acquire))
            .collect::<Vec<_>>();
        self.callbacks.lock().clear();
        self.output.clear();
        self.open_observers();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(self.callbacks.lock().is_empty());
        assert!(self.output_ready(0));
        assert_eq!(
            self.peers
                .iter()
                .map(|peer| peer.probe.polls.load(Ordering::Acquire))
                .collect::<Vec<_>>(),
            polls
        );
        assert!(self.peers[0]
            .controller
            .latest_committed_recover_release()
            .await
            .unwrap()
            .is_none());
        start
    }
}

async fn capture(
    remote: bool,
    owners: &[u64],
    fault: bool,
) -> (Vec<CallbackStamp>, Vec<ActivityRow>) {
    let mut rig = runtime::Rig::new(owners);
    let result = std::panic::AssertUnwindSafe(async {
        rig.setup(remote).await;
        let (reference, index) = rig.commit_prefix().await;
        if fault {
            rig.replay_after_fault(&reference, &index).await;
        } else {
            rig.finish_suffix().await;
        }
        let mut callbacks = rig.callbacks.lock().clone();
        let mut output = rig.output.clone();
        // Callback IDs bind engine order; independent-key RPC arrival and output are concurrent.
        callbacks.sort_by_key(|callback| callback.id);
        assert!(callbacks.windows(2).all(|pair| pair[0].id < pair[1].id));
        output.sort_by(|left, right| left.0.cmp(&right.0));
        (callbacks, output)
    })
    .catch_unwind()
    .await;
    if result.is_err() {
        eprintln!(
            "callbacks={:?}, output={:?}",
            rig.callbacks.lock(),
            rig.output
        );
        for peer in &rig.peers {
            eprintln!(
                "owner {}: fault={:?}, starts={:?}, polls={}",
                peer.controller.instance_id().0,
                peer.db.as_ref().and_then(|db| db.last_fault()),
                peer.probe.starts.lock(),
                peer.probe.polls.load(Ordering::Relaxed)
            );
        }
    }
    let cleanup = rig.close().await;
    match result {
        Ok(captured) => {
            assert!(cleanup.is_empty(), "{cleanup:?}");
            captured
        }
        Err(primary) => {
            if !cleanup.is_empty() {
                eprintln!("cluster recovery cleanup also failed: {cleanup:?}");
            }
            std::panic::resume_unwind(primary)
        }
    }
}

async fn qualify(remote: bool, owners: &[u64]) {
    let reference = capture(remote, owners, false).await;
    let mut expected = vec![
        (key_for(0, "a"), "running".into(), 110, true, 108_000),
        (key_for(0, "a"), "inactive".into(), 110, false, 118_000),
        (key_for(1, "b"), "running".into(), 20, false, 107_000),
        (key_for(1, "b"), "inactive".into(), 20, false, 117_000),
        (key_for(0, "c"), "running".into(), 1, false, 125_000),
        (key_for(0, "c"), "inactive".into(), 1, false, 135_000),
        (key_for(1, "d"), "running".into(), 2, false, 145_000),
        (key_for(1, "d"), "inactive".into(), 2, false, 155_000),
        (key_for(0, "e"), "running".into(), 0, false, 165_000),
    ];
    expected.sort_by(|left, right| left.0.cmp(&right.0));
    assert_eq!(reference.1, expected);
    assert_eq!(reference.0.len(), 9);
    assert_eq!(
        reference.0.iter().filter(|callback| callback.timer).count(),
        4
    );
    assert!(reference
        .0
        .iter()
        .any(|callback| callback.key == key_for(0, "a")
            && callback.timer
            && callback.timestamp == 118_000
            && callback.state == ValueState::Value(110)));
    assert!(!reference
        .0
        .iter()
        .any(|callback| callback.timer && callback.timestamp == 110_000));
    let recovered = capture(remote, owners, true).await;
    assert_eq!(recovered, reference);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_owner_database_round_restores_process_state_and_timer_cuts() {
    qualify(false, &[7, 7]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn distributed_database_round_restores_process_state_and_timer_cuts() {
    qualify(false, &[7, 8]).await;
}

#[cfg(feature = "process-remote")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn remote_single_owner_database_round_restores_process_state_and_timer_cuts() {
    qualify(true, &[7, 7]).await;
}

#[cfg(feature = "process-remote")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn remote_distributed_database_round_restores_process_state_and_timer_cuts() {
    qualify(true, &[7, 8]).await;
}
