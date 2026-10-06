use super::super::super::remote::{DelayedActivity, Worker};
use super::*;
use crate::process_function::{ProcessCallback, ProcessRuntime};

struct DelayUncommitted(DelayedActivity);

impl NativeProcessFunction for DelayUncommitted {
    fn invoke(
        &self,
        activations: &[ProcessActivation],
    ) -> Result<Vec<ProcessActivationResult>, DbError> {
        let delayed = activations.iter().any(|activation| {
            let ProcessCallback::Input(batch) = &activation.callback else {
                return false;
            };
            batch
                .column(1)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap()
                .value(0)
                == 999
        });
        if delayed {
            self.0.invoke(activations)
        } else {
            AccountActivity.invoke(activations)
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delayed_lost_owner_reply_cannot_apply_after_committed_shared_checkpoint_transfer() {
    let pair = Pair::new().await;
    let mut binding = descriptor();
    binding.runtime = ProcessRuntime::RemoteRust;
    let (release, receiver) = std::sync::mpsc::channel();
    let handler = Arc::new(DelayUncommitted(DelayedActivity {
        entered: tokio::sync::Notify::new(),
        release: std::sync::Mutex::new(receiver),
    }));
    let mut old_worker = Worker::new(binding.clone(), handler.clone()).await;
    let mut original = populated(
        &pair,
        binding.clone(),
        ProcessHandler::Remote(Arc::clone(&old_worker.client)),
    )
    .await;
    let cut = SharedCut::persist(&pair, &mut original).await;
    let key = key_for(0);
    original[0]
        .execute_cycle(&source_batch(&[(&key, 999, 106_000)]), 106, None)
        .await
        .unwrap();
    tokio::time::timeout(DEADLINE, async {
        let entered = handler.0.entered.notified();
        tokio::pin!(entered);
        loop {
            tokio::select! {
                () = &mut entered => break,
                () = async {
                    for graph in &mut original {
                        graph.execute_cycle(&rustc_hash::FxHashMap::default(), 106, None).await.unwrap();
                    }
                    tokio::task::yield_now().await;
                } => {}
            }
        }
    }).await.unwrap();
    assert!(!original[0].checkpoint_is_quiescent());
    assert!(original[0].capture_state(u64::MAX).is_err());
    let mut scopes = Vec::new();
    while let Ok(scope) = old_worker.scopes.try_recv() {
        scopes.push(scope);
    }
    assert!(scopes.iter().any(|scope| scope.vnode == 0
        && scope.owner_generation == 7
        && scope.recovery_generation == 3));

    let owners = [9; 4];
    let target = target_fence(8, owners);
    let nodes = target_nodes(&pair, &target, owners).await;
    cut.publish_target([7, 8, 7, 8], &target, owners).await;
    for node in &pair.nodes {
        node.controller.fence_process_lease();
    }
    let recovered = cut.recover(&nodes[0]).await.unwrap();
    assert_eq!(recovered.checkpoint_watermark(), Some(105));
    let mut worker = Worker::new(binding.clone(), Arc::new(AccountActivity)).await;
    let mut restored = restore_graph(
        &nodes[0],
        &recovered,
        binding,
        ProcessHandler::Remote(Arc::clone(&worker.client)),
    )
    .unwrap();
    restored
        .execute_cycle(&source_batch(&[(&key, 1, 106_000)]), 106, None)
        .await
        .unwrap();
    let output = progress(std::slice::from_mut(&mut restored), 106).await;
    assert_eq!(activity_rows(&output[0])[0].2, 8);
    let scope = worker.scopes.recv().await.unwrap();
    assert_eq!(
        (
            scope.owner_generation,
            scope.recovery_generation,
            scope.vnode
        ),
        (8, 4, 0)
    );
    release.send(()).unwrap();
    tokio::time::timeout(DEADLINE, async {
        let wake = original[0].process_work_wake().unwrap();
        wake.notified().await;
    })
    .await
    .unwrap();
    assert!(original[0]
        .execute_cycle(&rustc_hash::FxHashMap::default(), 120, None)
        .await
        .unwrap_err()
        .requires_pipeline_recovery());
    drop(original);
    let output = progress(std::slice::from_mut(&mut restored), 115).await;
    let mut totals = activity_rows(&output[0])
        .into_iter()
        .map(|row| row.2)
        .collect::<Vec<_>>();
    totals.sort_unstable();
    assert_eq!(totals, [11, 13, 17]);
    let output = progress(std::slice::from_mut(&mut restored), 116).await;
    assert_eq!(activity_rows(&output[0])[0].2, 8);
    assert!(progress(std::slice::from_mut(&mut restored), 116).await[0].is_empty());
    drop(restored);
    old_worker.stop().await;
    worker.stop().await;
}
