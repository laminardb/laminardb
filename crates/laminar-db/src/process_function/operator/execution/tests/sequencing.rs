use super::*;
use crate::process_function::ValueMutation;

#[derive(Default)]
struct ClearingActivity(parking_lot::Mutex<Vec<u64>>);

impl NativeProcessFunction for ClearingActivity {
    fn invoke(
        &self,
        activations: &[ProcessActivation],
    ) -> Result<Vec<ProcessActivationResult>, DbError> {
        let mut ids = self.0.lock();
        assert!(ids.len() + activations.len() <= 4);
        ids.extend(activations.iter().map(|activation| activation.id));
        Ok(activations
            .iter()
            .map(|activation| ProcessActivationResult {
                activation_id: activation.id,
                output: Vec::new(),
                value: ValueMutation::Clear,
                timers: Vec::new(),
            })
            .collect())
    }
}

#[tokio::test]
async fn cleared_keys_keep_callback_identity_across_capture_and_restore() {
    let fixture = Fixture::new(Uuid::from_u128(7), 7, 3, TTL).await;
    let handler = Arc::new(ClearingActivity::default());
    let mut original = ProcessFunctionOperator::new(descriptor(), handler.clone(), 4).unwrap();
    fixture.bind_operator(&mut original);
    original
        .process_with_frontiers(&[vec![input_batch(&[("a", 1, 100_000)])]], &frontier(100))
        .await
        .unwrap();
    assert_eq!(original.key_count, 0);
    let metadata = original.checkpoint().unwrap().unwrap();
    let frames = original
        .checkpoint_vnodes(&[0, 1, 2, 3], 4, 4096)
        .unwrap()
        .unwrap()
        .into_iter()
        .map(|frame| {
            (
                frame.vnode,
                frame.state.unwrap().materialize(&mut 0, u64::MAX).unwrap(),
            )
        })
        .collect::<Vec<_>>();
    let first_id = handler.0.lock()[0];
    let vnode = u32::try_from(first_id % 4).unwrap();
    let mut restored = ProcessFunctionOperator::new(descriptor(), handler.clone(), 4).unwrap();
    restored
        .require_cluster_execution("activity", tokio::runtime::Handle::current(), NodeId(7))
        .unwrap();
    restored.restore(metadata).unwrap();
    for (vnode, bytes) in &frames {
        restored.restore_vnode(*vnode, 4, bytes).unwrap();
    }
    let (_, bytes) = frames.iter().find(|(slot, _)| *slot == vnode).unwrap();
    assert!(restored.restore_vnode(vnode, 4, bytes).is_err());
    fixture.bind_operator(&mut restored);
    restored
        .process_with_frontiers(&[vec![input_batch(&[("a", 2, 101_000)])]], &frontier(101))
        .await
        .unwrap();
    assert_eq!(*handler.0.lock(), [first_id, first_id + 4]);
    assert_eq!(restored.key_count, 0);
}

#[tokio::test]
async fn exhausted_vnode_rolls_back_earlier_reservations_before_calling_handler() {
    let fixture = Fixture::new(Uuid::from_u128(7), 7, 3, TTL).await;
    let handler = Arc::new(ClearingActivity::default());
    let mut operator = ProcessFunctionOperator::new(descriptor(), handler.clone(), 4).unwrap();
    fixture.bind_operator(&mut operator);
    let input = input_batch(&[("a", 1, 100_000), ("b", 2, 100_000)]);
    let vnodes = laminar_core::shuffle::row_vnodes(&input, &[0], 4).unwrap();
    assert_ne!(vnodes[0], vnodes[1]);
    operator.activation_sequences[vnodes[1] as usize] = u64::MAX;
    let before = operator.activation_sequences.clone();
    assert!(operator
        .process_with_frontiers(&[vec![input]], &frontier(100))
        .await
        .unwrap_err()
        .requires_pipeline_halt());
    assert_eq!(operator.activation_sequences, before);
    assert_eq!(operator.next_activation_id, 0);
    assert_eq!(operator.key_count, 0);
    assert!(handler.0.lock().is_empty());
}

#[tokio::test]
async fn incompatible_cluster_callback_frames_reject_before_state_installation() {
    let mut operator =
        ProcessFunctionOperator::new(descriptor(), Arc::new(AccountActivity), 4).unwrap();
    operator
        .require_cluster_execution("activity", tokio::runtime::Handle::current(), NodeId(7))
        .unwrap();
    let before = state_image(&operator);
    for abi in [None, Some(2)] {
        let mut frame = operator.checkpoint_frame();
        frame.activation_id_abi = abi;
        assert!(operator
            .restore(OperatorCheckpoint {
                data: serde_json::to_vec(&frame).unwrap()
            })
            .is_err());
        assert!(!operator.metadata_restored);
        assert_eq!(state_image(&operator), before);
        assert!(operator
            .activation_sequences
            .iter()
            .all(|sequence| *sequence == 0));
    }
    for sequence in [None, Some(u64::MAX)] {
        let frame = super::super::super::VnodeFrame {
            codec: crate::process_function::STATE_CODEC_VERSION,
            vnode: 0,
            activation_sequence: sequence,
            entries: Vec::new(),
        };
        assert!(operator
            .restore_vnode(0, 4, &serde_json::to_vec(&frame).unwrap())
            .is_err());
        assert_eq!(state_image(&operator), before);
    }
    let mut local =
        ProcessFunctionOperator::new(descriptor(), Arc::new(AccountActivity), 4).unwrap();
    let checkpoint = local.checkpoint().unwrap().unwrap();
    local.restore(checkpoint).unwrap();
    assert!(local
        .require_cluster_execution("activity", tokio::runtime::Handle::current(), NodeId(7))
        .is_err());
}
