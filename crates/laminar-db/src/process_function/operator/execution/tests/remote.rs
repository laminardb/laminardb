use super::*;
use crate::process_function::remote::{wire, RemoteProcessClient, RustReferenceWorker};
use crate::process_function::ProcessRuntime;
use tokio_util::sync::CancellationToken;

pub(super) struct Worker {
    pub(super) client: Arc<RemoteProcessClient>,
    pub(super) scopes: tokio::sync::mpsc::Receiver<wire::Open>,
    shutdown: CancellationToken,
    task: tokio::task::JoinHandle<Result<(), DbError>>,
}

impl Worker {
    pub(super) async fn new(
        binding: ProcessFunctionDescriptor,
        handler: Arc<dyn NativeProcessFunction>,
    ) -> Self {
        let (observer, scopes) = tokio::sync::mpsc::channel(32);
        let worker = RustReferenceWorker::new(binding.clone(), handler, 4)
            .unwrap()
            .with_scope_observer(observer);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(worker.serve_loopback(listener, shutdown.clone()));
        let client = Arc::new(
            RemoteProcessClient::connect_loopback(&endpoint, binding, 4, Duration::from_secs(5))
                .await
                .unwrap(),
        );
        Self {
            client,
            scopes,
            shutdown,
            task,
        }
    }

    pub(super) async fn stop(self) {
        self.shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(3), self.task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
}

pub(super) fn remote_operator(
    fixture: &Fixture,
    worker: &Worker,
    binding: ProcessFunctionDescriptor,
    wake: &Arc<tokio::sync::Notify>,
) -> ProcessFunctionOperator {
    let mut operator = ProcessFunctionOperator::new_remote(
        binding,
        &worker.client,
        tokio::runtime::Handle::current(),
        Arc::clone(wake),
        "activity".into(),
        4,
    )
    .unwrap();
    fixture.bind_operator(&mut operator);
    operator
}

async fn drain(
    operator: &mut ProcessFunctionOperator,
    wake: &Arc<tokio::sync::Notify>,
    watermark: i64,
) -> Vec<RecordBatch> {
    tokio::time::timeout(Duration::from_secs(3), async {
        let mut output = Vec::new();
        while operator.checkpoint_drain_pending() {
            wake.notified().await;
            output.extend(
                operator
                    .process_with_frontiers(&[], &frontier(watermark))
                    .await
                    .unwrap(),
            );
        }
        output
    })
    .await
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_worker_receives_assignment_and_recovery_for_data_and_timers() {
    let fixture = Fixture::new(Uuid::from_u128(7), 7, 3, TTL).await;
    let mut binding = descriptor();
    binding.runtime = ProcessRuntime::RemoteRust;
    let mut worker = Worker::new(binding.clone(), Arc::new(AccountActivity)).await;
    let wake = Arc::new(tokio::sync::Notify::new());
    let mut operator = remote_operator(&fixture, &worker, binding, &wake);
    let input = input_batch(&[("a", 7, 100_000)]);
    let expected_vnode = laminar_core::shuffle::row_vnodes(&input, &[0], 4).unwrap()[0];
    assert!(operator
        .process_with_frontiers(&[vec![input]], &frontier(100))
        .await
        .unwrap()
        .is_empty());
    let rows = activity_rows(&drain(&mut operator, &wake, 100).await);
    assert_eq!(rows[0].2, 7);
    let data = worker.scopes.recv().await.unwrap();
    assert_eq!(
        (data.owner_generation, data.recovery_generation, data.vnode),
        (7, 3, expected_vnode)
    );
    assert!(operator
        .process_with_frontiers(&[], &frontier(120))
        .await
        .unwrap()
        .is_empty());
    let rows = activity_rows(&drain(&mut operator, &wake, 120).await);
    assert_eq!(rows[0].1, "inactive");
    let timer = worker.scopes.recv().await.unwrap();
    assert_eq!(
        (
            timer.owner_generation,
            timer.recovery_generation,
            timer.vnode
        ),
        (7, 3, expected_vnode)
    );
    assert_ne!(data.attempt_id, timer.attempt_id);
    assert_ne!(data.batch_id, timer.batch_id);
    worker.stop().await;
}

pub(super) struct DelayedActivity {
    pub(super) entered: tokio::sync::Notify,
    pub(super) release: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
}

impl NativeProcessFunction for DelayedActivity {
    fn invoke(
        &self,
        activations: &[ProcessActivation],
    ) -> Result<Vec<ProcessActivationResult>, DbError> {
        self.entered.notify_one();
        self.release
            .lock()
            .unwrap()
            .recv_timeout(Duration::from_secs(8))
            .unwrap();
        AccountActivity.invoke(activations)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn late_worker_result_cannot_apply_after_recovery_or_assignment_changes() {
    for recovery_changed in [false, true] {
        let fixture = Fixture::new(Uuid::from_u128(7), 7, 3, TTL).await;
        let mut binding = descriptor();
        binding.runtime = ProcessRuntime::RemoteRust;
        let (release, receiver) = std::sync::mpsc::channel();
        let handler = Arc::new(DelayedActivity {
            entered: tokio::sync::Notify::new(),
            release: std::sync::Mutex::new(receiver),
        });
        let mut worker = Worker::new(binding.clone(), handler.clone()).await;
        let wake = Arc::new(tokio::sync::Notify::new());
        let mut operator = remote_operator(&fixture, &worker, binding, &wake);
        let before = state_image(&operator);
        operator
            .process_with_frontiers(&[vec![input_batch(&[("a", 999, 100_000)])]], &frontier(100))
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(3), handler.entered.notified())
            .await
            .unwrap();
        let scope = worker.scopes.recv().await.unwrap();
        assert_eq!((scope.owner_generation, scope.recovery_generation), (7, 3));
        if recovery_changed {
            fixture.scope.receiver.set_recovery_gen(4);
            fixture.scope.sender.set_recovery_gen(4);
        } else {
            let target = CheckpointAssignmentFence::from_owner_map(
                8,
                &[7; 4],
                fixture.binding.assignment().participants.clone(),
            )
            .unwrap();
            fixture
                .scope
                .registry
                .set_assignment_and_version(Arc::from([NodeId(7); 4]), 8);
            fixture
                .scope
                .sender
                .install_assignment_fence(&target, &[7; 4])
                .unwrap();
            fixture
                .scope
                .receiver
                .install_assignment_fence(&target, &[7; 4])
                .unwrap();
        }
        release.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(3), wake.notified())
            .await
            .unwrap();
        assert!(operator
            .process_with_frontiers(&[], &frontier(120))
            .await
            .unwrap_err()
            .requires_pipeline_recovery());
        assert_eq!(state_image(&operator), before);
        drop(operator);
        worker.stop().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lost_owner_reply_is_rejected_and_new_boot_restores_the_selected_state_and_timer_cut() {
    let fixture = Fixture::new(Uuid::from_u128(7), 7, 3, TTL).await;
    let mut binding = descriptor();
    binding.runtime = ProcessRuntime::RemoteRust;
    let wake = Arc::new(tokio::sync::Notify::new());
    let mut initial = Worker::new(binding.clone(), Arc::new(AccountActivity)).await;
    let mut operator = remote_operator(&fixture, &initial, binding.clone(), &wake);
    operator
        .process_with_frontiers(&[vec![input_batch(&[("a", 7, 100_000)])]], &frontier(100))
        .await
        .unwrap();
    drain(&mut operator, &wake, 100).await;
    initial.scopes.recv().await.unwrap();
    let metadata = operator.checkpoint().unwrap().unwrap().data;
    let frames = operator
        .checkpoint_vnodes(&[0, 1, 2, 3], 4, u64::MAX)
        .unwrap()
        .unwrap();
    let before = state_image(&operator);
    drop(operator);
    initial.stop().await;
    let (release, receiver) = std::sync::mpsc::channel();
    let handler = Arc::new(DelayedActivity {
        entered: tokio::sync::Notify::new(),
        release: std::sync::Mutex::new(receiver),
    });
    let mut worker = Worker::new(binding.clone(), handler.clone()).await;
    // Restore first, then bind: the live operator intentionally has no raw-restore path.
    let frame_bytes = frames
        .into_iter()
        .map(|frame| {
            (
                frame.vnode,
                frame.state.unwrap().materialize(&mut 0, u64::MAX).unwrap(),
            )
        })
        .collect::<Vec<_>>();
    let mut operator = ProcessFunctionOperator::new_remote(
        binding.clone(),
        &worker.client,
        tokio::runtime::Handle::current(),
        Arc::clone(&wake),
        "activity".into(),
        4,
    )
    .unwrap();
    operator
        .require_cluster_execution("activity", tokio::runtime::Handle::current(), NodeId(7))
        .unwrap();
    operator
        .restore(OperatorCheckpoint {
            data: metadata.clone(),
        })
        .unwrap();
    for (vnode, bytes) in &frame_bytes {
        operator.restore_vnode(*vnode, 4, bytes).unwrap();
    }
    fixture.bind_operator(&mut operator);
    operator
        .process_with_frontiers(&[vec![input_batch(&[("a", 999, 101_000)])]], &frontier(101))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), handler.entered.notified())
        .await
        .unwrap();
    assert_eq!(worker.scopes.recv().await.unwrap().owner_generation, 7);
    fixture.controller.fence_process_lease();
    let replacement = fixture.takeover(Uuid::from_u128(8), 8, 4).await;
    assert_eq!(
        replacement
            .controller
            .try_live_local_process_authority_identity()
            .unwrap()
            .process_term,
        2
    );
    release.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(3), wake.notified())
        .await
        .unwrap();
    assert!(operator
        .process_with_frontiers(&[], &frontier(120))
        .await
        .unwrap_err()
        .requires_pipeline_recovery());
    assert_eq!(state_image(&operator), before);
    drop(operator);
    worker.stop().await;
    let mut worker = Worker::new(binding.clone(), Arc::new(AccountActivity)).await;
    let mut restored = ProcessFunctionOperator::new_remote(
        binding,
        &worker.client,
        tokio::runtime::Handle::current(),
        Arc::clone(&wake),
        "activity".into(),
        4,
    )
    .unwrap();
    restored
        .require_cluster_execution("activity", tokio::runtime::Handle::current(), NodeId(7))
        .unwrap();
    restored
        .restore(OperatorCheckpoint {
            data: metadata.clone(),
        })
        .unwrap();
    for (vnode, bytes) in &frame_bytes {
        restored.restore_vnode(*vnode, 4, bytes).unwrap();
    }
    assert!(restored
        .bind_startup_assignment(fixture.binding.assignment(), &[0, 1, 2, 3])
        .and_then(|()| restored.bind_process_execution_authority(
            &replacement.scope,
            replacement.controller.process_lease_deadline().unwrap()
        ))
        .is_err());
    // The rejected boot binding never opened intake; rebuild the private restore image.
    drop(restored);
    let mut restored = ProcessFunctionOperator::new_remote(
        worker.client.descriptor().clone(),
        &worker.client,
        tokio::runtime::Handle::current(),
        Arc::clone(&wake),
        "activity".into(),
        4,
    )
    .unwrap();
    restored
        .require_cluster_execution("activity", tokio::runtime::Handle::current(), NodeId(7))
        .unwrap();
    restored
        .restore(OperatorCheckpoint { data: metadata })
        .unwrap();
    for (vnode, bytes) in &frame_bytes {
        restored.restore_vnode(*vnode, 4, bytes).unwrap();
    }
    replacement.bind_operator(&mut restored);
    assert!(restored
        .process_with_frontiers(&[], &frontier(110))
        .await
        .unwrap()
        .is_empty());
    let timer = activity_rows(&drain(&mut restored, &wake, 110).await);
    assert_eq!(
        (timer[0].1.as_str(), timer[0].2, timer[0].4),
        ("inactive", 7, 110_000)
    );
    let scope = worker.scopes.recv().await.unwrap();
    assert_eq!((scope.owner_generation, scope.recovery_generation), (8, 4));
    restored
        .process_with_frontiers(&[vec![input_batch(&[("a", 1, 111_000)])]], &frontier(111))
        .await
        .unwrap();
    let result = activity_rows(&drain(&mut restored, &wake, 111).await);
    assert_eq!(result[0].2, 8);
    let scope = worker.scopes.recv().await.unwrap();
    assert_eq!((scope.owner_generation, scope.recovery_generation), (8, 4));
    drop(restored);
    worker.stop().await;
}
