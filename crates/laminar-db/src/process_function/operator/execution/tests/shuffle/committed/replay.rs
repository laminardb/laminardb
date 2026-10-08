use super::*;
use crate::process_function::ProcessCallback;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
pub(super) struct CallbackIdentity {
    pub(super) key: String,
    pub(super) event_time_us: i64,
    pub(super) timer: bool,
    pub(super) id: u64,
}

#[derive(Default)]
pub(super) struct RecordingActivity {
    pub(super) callbacks: parking_lot::Mutex<Vec<CallbackIdentity>>,
}

impl NativeProcessFunction for RecordingActivity {
    fn invoke(
        &self,
        activations: &[ProcessActivation],
    ) -> Result<Vec<ProcessActivationResult>, DbError> {
        let result = AccountActivity.invoke(activations)?;
        let mut callbacks = self.callbacks.lock();
        assert!(callbacks.len() + activations.len() <= 64);
        callbacks.extend(activations.iter().map(|activation| CallbackIdentity {
            key: activation.key_text.clone(),
            event_time_us: activation.event_time_us,
            timer: matches!(activation.callback, ProcessCallback::Timer { .. }),
            id: activation.id,
        }));
        Ok(result)
    }
}

pub(super) async fn replay_input(
    graphs: &mut [OperatorGraph],
    batch_rows: usize,
) -> Vec<RecordBatch> {
    let keys = [key_for(0), key_for(1), key_for(2), key_for(3)];
    let rows = (0..8)
        .map(|index| {
            let key = &keys[index % keys.len()];
            let index = i64::try_from(index).unwrap();
            (key.as_str(), index + 1, 106_000 + index * 1_000)
        })
        .collect::<Vec<_>>();
    let mut output = Vec::new();
    for chunk in rows.chunks(batch_rows) {
        let emitted = graphs[0]
            .execute_cycle(&source_batch(chunk), 105, None)
            .await
            .unwrap();
        output.extend(emitted.get("activity").into_iter().flatten().cloned());
        output.extend(progress(graphs, 105).await.into_iter().flatten());
    }
    output.extend(progress(graphs, 125).await.into_iter().flatten());
    output
}

async fn qualify_committed_replay(
    binding: ProcessFunctionDescriptor,
    handler: ProcessHandler,
    observed: &RecordingActivity,
) {
    let pair = Pair::new().await;
    let mut original = populated(&pair, binding.clone(), handler.clone()).await;
    let cut = SharedCut::persist(&pair, &mut original).await;
    observed.callbacks.lock().clear();
    let reference_output = replay_input(&mut original, 4).await;
    let mut expected = observed.callbacks.lock().clone();
    expected.sort();
    assert_eq!(expected.len(), 12);
    observed.callbacks.lock().clear();
    for node in &pair.nodes {
        node.controller.fence_process_lease();
    }
    drop(original);

    let owners = [7, 9, 8, 9];
    let fence = target_fence(8, owners);
    let nodes = target_nodes(&pair, &fence, owners).await;
    cut.publish_target([7, 8, 7, 8], &fence, owners).await;
    let mut graphs = Vec::new();
    for node in &nodes {
        let recovered = cut.recover(node).await.unwrap();
        graphs.push(restore_graph(node, &recovered, binding.clone(), handler.clone()).unwrap());
    }
    progress(&mut graphs, 105).await;
    let recovered_output = replay_input(&mut graphs, 1).await;
    let mut expected_rows = activity_rows(&reference_output);
    let mut recovered_rows = activity_rows(&recovered_output);
    expected_rows.sort();
    recovered_rows.sort();
    assert_eq!(recovered_rows, expected_rows);
    let mut actual = observed.callbacks.lock().clone();
    actual.sort();
    assert_eq!(actual, expected);
}

#[tokio::test]
async fn committed_replay_keeps_callback_ids_after_rescale_and_batch_splitting() {
    let observed = Arc::new(RecordingActivity::default());
    qualify_committed_replay(
        descriptor(),
        ProcessHandler::Native(observed.clone()),
        &observed,
    )
    .await;
}

#[cfg(feature = "process-remote")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn remote_committed_replay_keeps_callback_ids_after_rescale_and_batch_splitting() {
    use super::super::super::remote::Worker;
    use crate::process_function::ProcessRuntime;

    let observed = Arc::new(RecordingActivity::default());
    let mut binding = descriptor();
    binding.runtime = ProcessRuntime::RemoteRust;
    let worker = Worker::new(binding.clone(), observed.clone()).await;
    qualify_committed_replay(
        binding,
        ProcessHandler::Remote(Arc::clone(&worker.client)),
        &observed,
    )
    .await;
    worker.stop().await;
}
