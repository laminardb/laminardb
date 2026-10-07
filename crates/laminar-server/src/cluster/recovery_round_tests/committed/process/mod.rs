//! A single physical replay channel drives the public process binding over live S3 authority.

use super::connectors::ReplayProbe;
use super::*;
use laminar_db::process_function::{
    NativeProcessFunction, ProcessActivation, ProcessActivationResult, ProcessCallback,
    ProcessFunctionDescriptor, ProcessFunctionLimits, ProcessRuntime, TimerOperation,
    ValueMutation, ValueState,
};

mod activity;
mod scenario;
mod source;

pub(super) use activity::{descriptor, Activity, ActivityRow, Callback};
pub(super) use scenario::qualify;
pub(super) use source::register;
pub(super) const CHANNEL: &[u8] = b"global-process-channel";
pub(super) type Transcript = (Vec<Callback>, Vec<ActivityRow>);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires loopback MinIO; set LAMINAR_PROCESS_TEST_S3_ENDPOINT and LAMINAR_PROCESS_TEST_S3_BUCKET"]
async fn native_database_node_loss_restores_process_state_and_matching_timer_cuts() {
    let reference = super::scenario::run_process(Runtime::Native, 0)
        .await
        .unwrap();
    for failed in [7, 8] {
        let restored = super::scenario::run_process(Runtime::Native, failed)
            .await
            .unwrap();
        assert_eq!(restored, reference);
    }
}

#[cfg(feature = "process-remote")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires loopback MinIO; set LAMINAR_PROCESS_TEST_S3_ENDPOINT and LAMINAR_PROCESS_TEST_S3_BUCKET"]
async fn remote_database_node_loss_restores_process_state_and_matching_timer_cuts() {
    let reference = super::scenario::run_process(Runtime::RemoteRust, 0)
        .await
        .unwrap();
    for failed in [7, 8] {
        let restored = super::scenario::run_process(Runtime::RemoteRust, failed)
            .await
            .unwrap();
        assert_eq!(restored, reference);
    }
}
