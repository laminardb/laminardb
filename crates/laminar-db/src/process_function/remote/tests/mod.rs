use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{
    Array, BinaryArray, BooleanArray, Decimal128Array, Float32Array, Float64Array, Int16Array,
    Int32Array, Int64Array, Int8Array, RecordBatch, StringArray, TimestampMicrosecondArray,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use laminar_core::state::PartitionKeyCodecV1;
use prost::Message;
use sha2::{Digest, Sha256};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use super::codec::{decode_batch, encode_batch};
use super::wire::{self, host_frame};
use super::{RemoteInvocationScope, RemoteProcessClient, RustReferenceWorker, MAX_FRAME_BYTES};
use crate::error::DbError;
use crate::process_function::{
    NativeProcessFunction, ProcessActivation, ProcessActivationResult, ProcessCallback,
    ProcessFunctionDescriptor, ProcessFunctionLimits, ProcessRuntime, TimerOperation,
    ValueMutation, ValueState,
};

fn input_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("key", DataType::Utf8, false),
        Field::new("amount", DataType::Int64, false),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
    ]))
}

fn output_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("key", DataType::Utf8, false),
        Field::new("total", DataType::Int64, false),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
    ]))
}

fn descriptor() -> ProcessFunctionDescriptor {
    ProcessFunctionDescriptor {
        version: 1,
        runtime: ProcessRuntime::RemoteRust,
        function_id: "remote_reference".into(),
        pipeline_state_id: "remote_test_pipeline".into(),
        implementation_digest: "b".repeat(64),
        python_environment: None,
        determinism: crate::process_function::ProcessDeterminism::Undeclared,
        input_schema: input_schema(),
        output_schema: output_schema(),
        key_columns: vec!["key".into()],
        event_time_column: "ts".into(),
        output_event_time_column: "ts".into(),
        value_state_name: "total".into(),
        timer_names: vec!["flush".into()],
        limits: ProcessFunctionLimits::default(),
    }
}

#[tokio::test]
async fn supervised_binding_is_revoked_for_every_client_clone() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let shutdown = CancellationToken::new();
    let worker = RustReferenceWorker::new(descriptor(), Arc::new(ReferenceHandler), 1).unwrap();
    let server = tokio::spawn(worker.serve_loopback(listener, shutdown.clone()));
    let mut client =
        RemoteProcessClient::connect_loopback(&endpoint, descriptor(), 1, Duration::from_secs(1))
            .await
            .unwrap();
    assert!(!client.has_python_replay_binding());
    let exited = CancellationToken::new();
    client.bind_python_replay_lifetime(exited.clone());
    let clone = client.clone();
    assert!(clone.has_python_replay_binding());
    exited.cancel();
    assert!(!client.has_python_replay_binding());
    assert!(!clone.has_python_replay_binding());
    assert!(clone
        .invoke(&scope(), &[])
        .await
        .unwrap_err()
        .to_string()
        .contains("exited"));
    shutdown.cancel();
    server.await.unwrap().unwrap();
}

fn activation(id: u64, key: &str, amount: i64, state: ValueState) -> ProcessActivation {
    let key_codec = PartitionKeyCodecV1::try_new([DataType::Utf8]).unwrap();
    let encoded = key_codec
        .encode_columns(&[Arc::new(StringArray::from(vec![key]))])
        .unwrap();
    let ts = 1_000_000;
    let batch = RecordBatch::try_new(
        input_schema(),
        vec![
            Arc::new(StringArray::from(vec![key])),
            Arc::new(Int64Array::from(vec![amount])),
            Arc::new(TimestampMicrosecondArray::from(vec![ts])),
        ],
    )
    .unwrap();
    ProcessActivation {
        id,
        key: Arc::from(encoded.row(0).data()),
        key_text: key.into(),
        event_time_us: ts,
        callback: ProcessCallback::Input(batch),
        state,
    }
}

fn scope() -> RemoteInvocationScope {
    RemoteInvocationScope {
        operator_id: "operator_1".into(),
        vnode: 0,
        vnode_count: 1,
        owner_generation: 0,
        recovery_generation: 0,
        batch_id: uuid::Uuid::new_v4(),
        attempt_id: uuid::Uuid::new_v4(),
        input_watermark_us: Some(900_000),
    }
}

struct ReferenceHandler;

impl NativeProcessFunction for ReferenceHandler {
    fn invoke(
        &self,
        activations: &[ProcessActivation],
    ) -> Result<Vec<ProcessActivationResult>, DbError> {
        activations
            .iter()
            .map(|activation| {
                let (value, output, timers) = match &activation.callback {
                    ProcessCallback::Input(batch) => {
                        let amount = batch
                            .column(1)
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap()
                            .value(0);
                        let prior = match activation.state {
                            ValueState::Value(value) => value,
                            ValueState::Absent | ValueState::Null => 0,
                        };
                        let total = prior + amount;
                        let output = if amount == 0 {
                            Vec::new()
                        } else {
                            let row = RecordBatch::try_new(
                                output_schema(),
                                vec![
                                    Arc::new(StringArray::from(vec![activation.key_text.as_str()])),
                                    Arc::new(Int64Array::from(vec![total])),
                                    Arc::new(TimestampMicrosecondArray::from(vec![
                                        activation.event_time_us,
                                    ])),
                                ],
                            )
                            .unwrap();
                            vec![row.clone(), row]
                        };
                        (
                            ValueMutation::Set(total),
                            output,
                            vec![TimerOperation::Set {
                                name: "flush".into(),
                                at_us: activation.event_time_us + 100,
                            }],
                        )
                    }
                    ProcessCallback::Timer { .. } => (
                        ValueMutation::Clear,
                        Vec::new(),
                        vec![TimerOperation::Cancel {
                            name: "flush".into(),
                        }],
                    ),
                };
                Ok(ProcessActivationResult {
                    activation_id: activation.id,
                    output,
                    value,
                    timers,
                })
            })
            .collect()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rust_reference_worker_exchanges_state_outputs_and_timer_callbacks() {
    let descriptor = descriptor();
    let worker =
        RustReferenceWorker::new(descriptor.clone(), Arc::new(ReferenceHandler), 2).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let shutdown = CancellationToken::new();
    let server = tokio::spawn(worker.serve_loopback(listener, shutdown.clone()));
    let client = RemoteProcessClient::connect_loopback(
        &format!("http://{address}"),
        descriptor.clone(),
        2,
        Duration::from_secs(5),
    )
    .await
    .unwrap();

    let results = client
        .invoke(
            &scope(),
            &[
                activation(7, "alpha", 3, ValueState::Absent),
                activation(8, "beta", 0, ValueState::Null),
            ],
        )
        .await
        .unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].activation_id, 7);
    assert_eq!(results[0].value, ValueMutation::Set(3));
    assert_eq!(results[0].output.len(), 2);
    assert_eq!(
        results[0].timers,
        vec![TimerOperation::Set {
            name: "flush".into(),
            at_us: 1_000_100
        }]
    );
    assert_eq!(results[1].value, ValueMutation::Set(0));
    assert!(results[1].output.is_empty());

    let mut timer = activation(9, "alpha", 0, ValueState::Value(3));
    timer.callback = ProcessCallback::Timer {
        name: "flush".into(),
    };
    timer.event_time_us = 1_000_100;
    let results = client.invoke(&scope(), &[timer]).await.unwrap();
    assert_eq!(results[0].value, ValueMutation::Clear);
    assert!(results[0].output.is_empty());
    assert_eq!(
        results[0].timers,
        vec![TimerOperation::Cancel {
            name: "flush".into()
        }]
    );

    shutdown.cancel();
    server.await.unwrap().unwrap();
}

#[test]
fn arrow_ipc_round_trips_supported_types_and_rejects_truncation() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("bool", DataType::Boolean, true),
        Field::new("i8", DataType::Int8, true),
        Field::new("i16", DataType::Int16, true),
        Field::new("i32", DataType::Int32, true),
        Field::new("i64", DataType::Int64, true),
        Field::new("f32", DataType::Float32, true),
        Field::new("f64", DataType::Float64, true),
        Field::new("text", DataType::Utf8, true),
        Field::new("bin", DataType::Binary, true),
        Field::new("decimal", DataType::Decimal128(20, 4), true),
        Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, None), true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(BooleanArray::from(vec![Some(true), None])),
            Arc::new(Int8Array::from(vec![Some(-8), None])),
            Arc::new(Int16Array::from(vec![Some(-16), None])),
            Arc::new(Int32Array::from(vec![Some(-32), None])),
            Arc::new(Int64Array::from(vec![Some(-64), None])),
            Arc::new(Float32Array::from(vec![Some(f32::NAN), None])),
            Arc::new(Float64Array::from(vec![Some(f64::INFINITY), None])),
            Arc::new(StringArray::from(vec![Some("é"), None])),
            Arc::new(BinaryArray::from(vec![Some(&b"\0\xff"[..]), None])),
            Arc::new(
                Decimal128Array::from(vec![Some(123_456), None])
                    .with_precision_and_scale(20, 4)
                    .unwrap(),
            ),
            Arc::new(TimestampMicrosecondArray::from(vec![Some(1_234_567), None])),
        ],
    )
    .unwrap();
    let ipc = encode_batch(&batch, &schema).unwrap();
    let decoded = decode_batch(&ipc, &schema, 2, MAX_FRAME_BYTES).unwrap();
    assert_eq!(decoded.num_rows(), 2);
    assert!(decoded
        .column(5)
        .as_any()
        .downcast_ref::<Float32Array>()
        .unwrap()
        .value(0)
        .is_nan());
    assert_eq!(
        decoded
            .column(9)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap()
            .value(0),
        123_456
    );
    assert_eq!(
        decoded
            .column(10)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap()
            .value(0),
        1_234_567
    );
    assert!(decoded.columns().iter().all(|column| column.is_null(1)));
    assert!(decode_batch(&ipc[..ipc.len() / 2], &schema, 2, MAX_FRAME_BYTES).is_err());
    assert!(decode_batch(&ipc, &input_schema(), 2, MAX_FRAME_BYTES).is_err());
}

#[tokio::test]
async fn remote_client_rejects_nonloopback_plaintext() {
    let result = RemoteProcessClient::connect_loopback(
        "http://192.0.2.1:5000",
        descriptor(),
        1,
        Duration::from_secs(1),
    )
    .await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn worker_rejects_manifest_mismatch_and_truncated_ipc() {
    let descriptor = descriptor();
    let worker =
        RustReferenceWorker::new(descriptor.clone(), Arc::new(ReferenceHandler), 1).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let shutdown = CancellationToken::new();
    let server = tokio::spawn(worker.serve_loopback(listener, shutdown.clone()));
    let endpoint = format!("http://{address}");

    let mut wrong = descriptor.clone();
    wrong.implementation_digest = "c".repeat(64);
    let wrong_client =
        RemoteProcessClient::connect_loopback(&endpoint, wrong, 1, Duration::from_secs(5))
            .await
            .unwrap();
    assert!(wrong_client
        .invoke(&scope(), &[activation(1, "alpha", 1, ValueState::Absent)])
        .await
        .is_err());

    let channel = tonic::transport::Endpoint::from_shared(endpoint)
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut raw = wire::process_worker_client::ProcessWorkerClient::new(channel);
    let digest: [u8; 32] = Sha256::digest(descriptor.to_manifest_json().unwrap()).into();
    let invocation = scope();
    let deadline_unix_ms = u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
        + 5_000;
    let open = wire::HostFrame {
        kind: Some(host_frame::Kind::Open(wire::Open {
            protocol_version: 1,
            descriptor_sha256: digest.to_vec(),
            operator_id: invocation.operator_id,
            vnode: 0,
            owner_generation: 0,
            recovery_generation: 0,
            batch_id: invocation.batch_id.as_bytes().to_vec(),
            attempt_id: invocation.attempt_id.as_bytes().to_vec(),
            input_watermark_us: None,
            deadline_unix_ms,
            vnode_count: 1,
        })),
    };
    let mut invalid = super::codec::encode_activation(
        &activation(2, "alpha", 1, ValueState::Absent),
        &descriptor,
    )
    .unwrap();
    invalid.callback = Some(wire::activation::Callback::InputIpc(vec![1, 2, 3]));
    let frames = vec![
        open,
        wire::HostFrame {
            kind: Some(host_frame::Kind::Activation(invalid)),
        },
        wire::HostFrame {
            kind: Some(host_frame::Kind::End(wire::End { count: 1 })),
        },
    ];
    assert!(frames
        .iter()
        .all(|frame| frame.encoded_len() < super::MAX_FRAME_BYTES));
    let exchange = raw
        .exchange(tonic::Request::new(tokio_stream::iter(frames)))
        .await;
    assert!(exchange.is_err());

    shutdown.cancel();
    server.await.unwrap().unwrap();
}

struct BlockingOnceHandler {
    started: std::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    release: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
    calls: AtomicUsize,
}

impl NativeProcessFunction for BlockingOnceHandler {
    fn invoke(
        &self,
        activations: &[ProcessActivation],
    ) -> Result<Vec<ProcessActivationResult>, DbError> {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            if let Some(started) = self.started.lock().unwrap().take() {
                let _ = started.send(());
            }
            self.release.lock().unwrap().recv().unwrap();
        }
        ReferenceHandler.invoke(activations)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_call_keeps_worker_credit_until_handler_finishes() {
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let handler = Arc::new(BlockingOnceHandler {
        started: std::sync::Mutex::new(Some(started_tx)),
        release: std::sync::Mutex::new(release_rx),
        calls: AtomicUsize::new(0),
    });
    let worker = RustReferenceWorker::new(descriptor(), handler.clone(), 1).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let shutdown = CancellationToken::new();
    let server = tokio::spawn(worker.serve_loopback(listener, shutdown.clone()));
    let client =
        RemoteProcessClient::connect_loopback(&endpoint, descriptor(), 1, Duration::from_secs(5))
            .await
            .unwrap();
    let first = tokio::spawn({
        let client = client.clone();
        async move {
            client
                .invoke(&scope(), &[activation(1, "alpha", 1, ValueState::Absent)])
                .await
        }
    });
    started_rx.await.unwrap();
    first.abort();
    let _ = first.await;

    let second_client =
        RemoteProcessClient::connect_loopback(&endpoint, descriptor(), 1, Duration::from_secs(5))
            .await
            .unwrap();
    let denied = second_client
        .invoke(&scope(), &[activation(2, "beta", 1, ValueState::Absent)])
        .await;
    assert!(denied.unwrap_err().to_string().contains("slots full"));
    assert_eq!(handler.calls.load(Ordering::SeqCst), 1);
    release_tx.send(()).unwrap();
    shutdown.cancel();
    server.await.unwrap().unwrap();
}

proptest::proptest! {
    #[test]
    fn malformed_ipc_is_rejected_without_panic(bytes in proptest::collection::vec(proptest::num::u8::ANY, 0..256)) {
        let _ = decode_batch(&bytes, &output_schema(), 2, 1024);
    }
}

mod python;
