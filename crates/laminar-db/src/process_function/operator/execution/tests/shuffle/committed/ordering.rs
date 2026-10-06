use super::replay::CallbackIdentity;
use super::*;
use crate::process_function::{ProcessCallback, TimerOperation, ValueMutation, ValueState};

#[derive(Default)]
struct ReschedulingActivity(parking_lot::Mutex<Vec<CallbackIdentity>>);

impl NativeProcessFunction for ReschedulingActivity {
    fn invoke(
        &self,
        activations: &[ProcessActivation],
    ) -> Result<Vec<ProcessActivationResult>, DbError> {
        let mut results = AccountActivity.invoke(activations)?;
        let mut callbacks = self.0.lock();
        assert!(callbacks.len() + activations.len() <= 64);
        for (activation, result) in activations.iter().zip(&mut results) {
            callbacks.push(CallbackIdentity {
                key: activation.key_text.clone(),
                event_time_us: activation.event_time_us,
                timer: matches!(activation.callback, ProcessCallback::Timer { .. }),
                id: activation.id,
            });
            match &activation.callback {
                ProcessCallback::Input(batch) => {
                    let amount = batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<arrow::array::Int64Array>()
                        .unwrap()
                        .value(0);
                    if amount == 0 {
                        result.timers = vec![TimerOperation::Cancel {
                            name: "inactive".into(),
                        }];
                    }
                }
                ProcessCallback::Timer { .. } => {
                    let ValueState::Value(total) = activation.state else {
                        panic!("fixture timers have a nonnull account total");
                    };
                    if total < 1_000 {
                        result.value = ValueMutation::Set(total + 1_000);
                        result.timers = vec![TimerOperation::Set {
                            name: "inactive".into(),
                            at_us: activation.event_time_us + 20_000,
                        }];
                    }
                }
            }
        }
        Ok(results)
    }
}

fn second_key_for(vnode: u32) -> String {
    let first = key_for(vnode);
    (0..10_000)
        .map(|index| format!("second-{index}"))
        .find(|key| {
            key != &first
                && laminar_core::shuffle::row_vnodes(&input_batch(&[(key, 1, 100_000)]), &[0], 4)
                    .unwrap()[0]
                    == vnode
        })
        .expect("bounded fixture search must find another key in this vnode")
}

async fn ordered_replay(graphs: &mut [OperatorGraph], width: usize) -> Vec<RecordBatch> {
    let keys = [
        key_for(0),
        second_key_for(0),
        key_for(1),
        key_for(2),
        key_for(3),
    ];
    // This fixture admits one total source order and retains explicit watermark cuts. Splitting
    // batches must preserve that order; per-partition cursors alone do not certify a channel merge.
    let rows = [
        (keys[0].as_str(), 1, 106_000),
        (keys[1].as_str(), 2, 107_000),
        (keys[2].as_str(), 0, 108_000),
        (keys[3].as_str(), 3, 109_000),
        (keys[4].as_str(), 4, 110_000),
        // Still above the accepted watermark: arrival order wins over event-time order.
        (keys[0].as_str(), 5, 105_500),
    ];
    let mut output = Vec::new();
    for rows in rows.chunks(width) {
        let expected = output.iter().map(RecordBatch::num_rows).sum::<usize>() + rows.len();
        let emitted = graphs[0]
            .execute_cycle(&source_batch(rows), 105, None)
            .await
            .unwrap();
        output.extend(emitted.get("activity").into_iter().flatten().cloned());
        // The unchanged watermark does not prove that a peer has received this batch. Require
        // every input output before submitting another chunk or advancing timer progress.
        tokio::time::timeout(DEADLINE, async {
            loop {
                output.extend(progress(graphs, 105).await.into_iter().flatten());
                let actual = output.iter().map(RecordBatch::num_rows).sum::<usize>();
                assert!(actual <= expected, "duplicate input output");
                if actual == expected {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("owners did not apply the ordered input prefix");
    }
    // Replaced and cancelled firings are absent at 115. Each live timer then fires once and
    // re-registers strictly beyond this progress step, followed by one terminal firing.
    let early = progress(graphs, 115)
        .await
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    assert!(
        early.is_empty(),
        "unexpected timers: {:?}",
        activity_rows(&early)
    );
    output.extend(progress(graphs, 125).await.into_iter().flatten());
    output.extend(progress(graphs, 145).await.into_iter().flatten());
    output
}

fn expected_callbacks() -> Vec<CallbackIdentity> {
    let key0 = key_for(0);
    let second = second_key_for(0);
    let mut expected = vec![
        (key0.clone(), 106_000, false, 4),
        (second.clone(), 107_000, false, 8),
        (key_for(1), 108_000, false, 5),
        (key_for(2), 109_000, false, 6),
        (key_for(3), 110_000, false, 7),
        (key0.clone(), 105_500, false, 12),
        (key0.clone(), 115_500, true, 16),
        (second.clone(), 117_000, true, 20),
        (key_for(2), 119_000, true, 10),
        (key_for(3), 120_000, true, 11),
        (key0, 135_500, true, 24),
        (second, 137_000, true, 28),
        (key_for(2), 139_000, true, 14),
        (key_for(3), 140_000, true, 15),
    ]
    .into_iter()
    .map(|(key, event_time_us, timer, id)| CallbackIdentity {
        key,
        event_time_us,
        timer,
        id,
    })
    .collect::<Vec<_>>();
    expected.sort();
    expected
}

fn expected_rows() -> Vec<(String, String, i64, bool, i64)> {
    let first = key_for(0);
    let second = second_key_for(0);
    let mut rows = vec![
        (first.clone(), "running".into(), 8, false, 106_000),
        (second.clone(), "running".into(), 2, false, 107_000),
        (key_for(1), "running".into(), 11, false, 108_000),
        (key_for(2), "running".into(), 16, false, 109_000),
        (key_for(3), "running".into(), 21, false, 110_000),
        (first.clone(), "running".into(), 13, false, 105_500),
    ];
    for (key, total, at_us) in [
        (second, 2, 117_000),
        (key_for(2), 16, 119_000),
        (key_for(3), 21, 120_000),
        (first, 13, 115_500),
    ] {
        rows.push((key.clone(), "inactive".into(), total, false, at_us));
        rows.push((key, "inactive".into(), total + 1_000, false, at_us + 20_000));
    }
    rows.sort();
    rows
}

async fn qualify_ordered_timers(
    binding: ProcessFunctionDescriptor,
    handler: ProcessHandler,
    observed: &ReschedulingActivity,
) {
    let pair = Pair::new().await;
    let mut original = populated(&pair, binding.clone(), handler.clone()).await;
    let cut = SharedCut::persist(&pair, &mut original).await;
    observed.0.lock().clear();
    let mut reference = activity_rows(&ordered_replay(&mut original, 6).await);
    let mut callbacks = observed.0.lock().clone();
    callbacks.sort();
    assert_eq!(callbacks, expected_callbacks());
    reference.sort();
    assert_eq!(reference, expected_rows());
    observed.0.lock().clear();
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
    let mut actual = activity_rows(&ordered_replay(&mut graphs, 1).await);
    actual.sort();
    assert_eq!(actual, reference);
    let mut callbacks = observed.0.lock().clone();
    callbacks.sort();
    assert_eq!(callbacks, expected_callbacks());
}

#[tokio::test]
async fn committed_source_order_preserves_replaced_cancelled_and_rescheduled_timers() {
    let observed = Arc::new(ReschedulingActivity::default());
    qualify_ordered_timers(
        descriptor(),
        ProcessHandler::Native(observed.clone()),
        &observed,
    )
    .await;
}

#[cfg(feature = "process-remote")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn remote_committed_source_order_preserves_replaced_cancelled_and_rescheduled_timers() {
    use super::super::super::remote::Worker;
    use futures::FutureExt as _;

    let observed = Arc::new(ReschedulingActivity::default());
    let mut binding = descriptor();
    binding.runtime = crate::process_function::ProcessRuntime::RemoteRust;
    let worker = Worker::new(binding.clone(), observed.clone()).await;
    let outcome = std::panic::AssertUnwindSafe(qualify_ordered_timers(
        binding,
        ProcessHandler::Remote(Arc::clone(&worker.client)),
        &observed,
    ))
    .catch_unwind()
    .await;
    let cleanup = std::panic::AssertUnwindSafe(worker.stop())
        .catch_unwind()
        .await;
    if let Err(primary) = outcome {
        if cleanup.is_err() {
            eprintln!("ordered replay worker cleanup also failed");
        }
        std::panic::resume_unwind(primary);
    }
    if let Err(cleanup) = cleanup {
        std::panic::resume_unwind(cleanup);
    }
}

#[tokio::test]
async fn independent_channel_permutation_does_not_certify_vnode_callback_identity() {
    let fixture = Fixture::new(Uuid::from_u128(7), 7, 3, TTL).await;
    let observed = Arc::new(ReschedulingActivity::default());
    let first = key_for(0);
    let second = second_key_for(0);
    let mut identities = Vec::new();
    let mut output = Vec::new();
    for rows in [
        [(first.as_str(), 1, 100_000), (second.as_str(), 2, 100_000)],
        [(second.as_str(), 2, 100_000), (first.as_str(), 1, 100_000)],
    ] {
        let mut operator = ProcessFunctionOperator::new(descriptor(), observed.clone(), 4).unwrap();
        fixture.bind_operator(&mut operator);
        let mut rows = activity_rows(
            &operator
                .process_with_frontiers(&[vec![input_batch(&rows)]], &frontier(100))
                .await
                .unwrap(),
        );
        rows.sort();
        output.push(rows);
        let mut callbacks = std::mem::take(&mut *observed.0.lock());
        callbacks.sort();
        identities.push(callbacks);
    }
    assert_eq!(output[0], output[1]);
    assert_ne!(identities[0], identities[1]);
    assert_eq!(
        identities[0]
            .iter()
            .map(|callback| callback.id)
            .collect::<Vec<_>>(),
        identities[1]
            .iter()
            .rev()
            .map(|callback| callback.id)
            .collect::<Vec<_>>()
    );
}
