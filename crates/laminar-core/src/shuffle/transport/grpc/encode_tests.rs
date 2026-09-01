use super::*;
use arrow::array::ArrayData;
use arrow::buffer::Buffer;
use arrow_array::Int64Array;
use arrow_schema::{DataType, Field, Schema};

async fn frame(msg: ShuffleMessage) -> Encoded {
    let workspace = outbound_workspace_bytes(&msg).unwrap();
    let permits = u32::try_from(workspace).unwrap();
    let peer = Arc::new(Semaphore::new(workspace))
        .try_acquire_many_owned(permits)
        .expect("test byte permit");
    let node = Arc::new(Semaphore::new(workspace))
        .try_acquire_many_owned(permits)
        .expect("test node byte permit");
    let budget = OutboundReservation { peer, node };
    let (prepared, budget) = prepare_outbound_message(&msg, budget).await.unwrap();
    frame_message(Outbound {
        gen: 0,
        assignment_version: 1,
        seq: 0,
        msg: prepared,
        assignment_digest: None,
        _budget: budget,
    })
    .unwrap()
}

fn payload(encoded: Encoded) -> Vec<u8> {
    encoded
        .frames
        .into_iter()
        .flat_map(|frame| match frame.kind.unwrap() {
            shuffle_frame::Kind::Data(fragment) => fragment.arrow_ipc,
            _ => panic!("expected data fragment"),
        })
        .collect()
}

#[tokio::test]
async fn frontier_encoding_preserves_initialized_and_idle_state() {
    for (watermark, idle) in [
        (Some(-7), false),
        (Some(42), true),
        (None, false),
        (None, true),
    ] {
        let encoded = frame(ShuffleMessage::Frontier {
            stage: "right-input".into(),
            watermark,
            idle,
        })
        .await;
        assert_eq!(encoded.frames.len(), 1);
        let shuffle_frame::Kind::Frontier(frontier) =
            encoded.frames.into_iter().next().unwrap().kind.unwrap()
        else {
            panic!("expected frontier frame");
        };
        assert_eq!(frontier.stage, "right-input");
        assert_eq!(frontier.watermark, watermark);
        assert_eq!(frontier.idle, idle);
        assert_eq!(frontier.recovery_gen, 0);
        assert_eq!(frontier.seq, 0);
    }
}

#[test]
fn frontier_rejects_noncanonical_progress() {
    for message in [
        ShuffleMessage::Frontier {
            stage: String::new(),
            watermark: None,
            idle: false,
        },
        ShuffleMessage::Frontier {
            stage: "stage".into(),
            watermark: Some(i64::MIN),
            idle: true,
        },
    ] {
        assert_eq!(
            outbound_workspace_bytes(&message).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
    }
}

#[tokio::test]
async fn each_logical_batch_is_self_contained_without_stage_codec_state() {
    let batch = |name: &str| {
        let schema = Arc::new(Schema::new(vec![Field::new(name, DataType::Int64, false)]));
        arrow_array::RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1]))])
            .unwrap()
    };
    for name in ["a", "b"] {
        let encoded = frame(ShuffleMessage::checkpointed("s".into(), 0, batch(name))).await;
        let decoded = decode_ipc_payload(&mut BatchStreamDecoder::new(), payload(encoded)).unwrap();
        assert_eq!(decoded.schema().field(0).name(), name);
    }
}

#[tokio::test]
async fn large_batch_is_offloaded_and_split_into_bounded_wire_frames() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Int64, false),
        Field::new("c", DataType::Int64, false),
    ]));
    let column = || Arc::new(Int64Array::from(vec![7; 65_536])) as arrow_array::ArrayRef;
    let batch = RecordBatch::try_new(schema, vec![column(), column(), column()]).unwrap();
    let msg = ShuffleMessage::checkpointed("stage".into(), 9, batch);
    let encoded = frame(msg).await;

    assert!(encoded.frames.len() > 1);
    let mut previous_end = None;
    for (index, frame) in encoded.frames.into_iter().enumerate() {
        let shuffle_frame::Kind::Data(fragment) = frame.kind.unwrap() else {
            panic!("expected data fragment");
        };
        assert!(fragment.arrow_ipc.len() <= MAX_WIRE_PAYLOAD_BYTES);
        assert_eq!(fragment.seq, 0);
        if index == 0 {
            assert_eq!(fragment.stage, "stage");
            assert_eq!(fragment.routed_vnodes, vec![9]);
        } else {
            assert!(fragment.stage.is_empty());
            assert!(fragment.routed_vnodes.is_empty());
        }
        if let Some(end) = previous_end {
            assert_eq!(fragment.arrow_ipc.as_ptr(), end);
        }
        previous_end = Some(
            fragment
                .arrow_ipc
                .as_ptr()
                .wrapping_add(fragment.arrow_ipc.len()),
        );
    }
}

#[tokio::test]
async fn cancelled_blocking_encode_retains_admission_until_worker_exits() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![7; 65_536]))]).unwrap();
    let msg = ShuffleMessage::checkpointed("stage".into(), 9, batch);
    let workspace = outbound_workspace_bytes(&msg).unwrap();
    let permits = u32::try_from(workspace).unwrap();
    let peer = Arc::new(Semaphore::new(workspace));
    let node = Arc::new(Semaphore::new(workspace));
    let budget = OutboundReservation {
        peer: Arc::clone(&peer).try_acquire_many_owned(permits).unwrap(),
        node: Arc::clone(&node).try_acquire_many_owned(permits).unwrap(),
    };
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let task = tokio::spawn(async move {
        prepare_outbound_message_with_hook(&msg, budget, move || {
            let _ = started_tx.send(());
            let _ = release_rx.recv();
        })
        .await
    });

    started_rx.await.unwrap();
    task.abort();
    assert_eq!(peer.available_permits(), 0);
    assert_eq!(node.available_permits(), 0);
    release_tx.send(()).unwrap();
    let _ = task.await;
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while peer.available_permits() != workspace || node.available_permits() != workspace {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("cancelled encoder retained its admission after the worker exited");
}

#[test]
fn oversized_decoded_batch_is_rejected_before_enqueue() {
    let rows = crate::shuffle::ROUTE_MAX_BATCH_BYTES / std::mem::size_of::<i64>() + 1;
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![0; rows]))]).unwrap();
    let msg = ShuffleMessage::checkpointed("stage".into(), 0, batch);

    assert_eq!(
        outbound_workspace_bytes(&msg).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
}

#[test]
fn externally_owned_buffers_cannot_bypass_the_logical_batch_bound() {
    let rows = crate::shuffle::ROUTE_MAX_BATCH_BYTES / std::mem::size_of::<i64>() + 1;
    let bytes = Bytes::from(vec![0; rows * std::mem::size_of::<i64>()]);
    let data = ArrayData::builder(DataType::Int64)
        .len(rows)
        .add_buffer(Buffer::from(bytes))
        .build()
        .unwrap();
    let array = Int64Array::from(data);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)])),
        vec![Arc::new(array)],
    )
    .unwrap();
    let msg = ShuffleMessage::checkpointed("stage".into(), 0, batch);

    assert_eq!(
        outbound_workspace_bytes(&msg).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
}

#[test]
fn oversized_schema_is_rejected_before_ipc_allocation() {
    let field_name = "x".repeat(MAX_SOURCE_SCHEMA_MEMORY_BYTES);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            field_name,
            DataType::Int64,
            false,
        )])),
        vec![Arc::new(Int64Array::from(Vec::<i64>::new()))],
    )
    .unwrap();

    let error = outbound_workspace_bytes(&ShuffleMessage::checkpointed("stage".into(), 0, batch))
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    assert!(error.to_string().contains("schema"));
}

#[tokio::test]
async fn accepted_near_limit_schema_round_trips_with_decode_headroom() {
    let field_name = "x".repeat(MAX_SOURCE_SCHEMA_MEMORY_BYTES - 4096);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            field_name.clone(),
            DataType::Int64,
            false,
        )])),
        vec![Arc::new(Int64Array::from(Vec::<i64>::new()))],
    )
    .unwrap();
    assert!(schema_memory_size(batch.schema().as_ref()) < MAX_SOURCE_SCHEMA_MEMORY_BYTES);

    let encoded = frame(ShuffleMessage::checkpointed("stage".into(), 0, batch)).await;
    let decoded = decode_ipc_payload(&mut BatchStreamDecoder::new(), payload(encoded)).unwrap();

    assert_eq!(decoded.schema().field(0).name(), &field_name);
    assert!(schema_memory_size(decoded.schema().as_ref()) < MAX_DECODED_SCHEMA_MEMORY_BYTES);
}
