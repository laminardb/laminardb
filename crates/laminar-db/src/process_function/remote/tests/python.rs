use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{
    Array, BinaryArray, BooleanArray, Decimal128Array, Float32Array, Float64Array, Int16Array,
    Int32Array, Int64Array, Int8Array, RecordBatch, StringArray, TimestampMicrosecondArray,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use sha2::{Digest, Sha256};
use tokio::io::AsyncBufReadExt;
use tokio::process::Command;

use super::{activation, descriptor, scope};
use crate::process_function::remote::wire::{self, host_frame};
use crate::process_function::remote::RemoteProcessClient;
use crate::process_function::{
    ProcessCallback, ProcessFunctionDescriptor, ProcessRuntime, TimerOperation, ValueMutation,
    ValueState,
};

struct PythonWorker {
    _directory: tempfile::TempDir,
    child: tokio::process::Child,
    endpoint: String,
}

impl PythonWorker {
    async fn start(
        python: &str,
        descriptor: &ProcessFunctionDescriptor,
        handler: &str,
        max_in_flight: usize,
        marker: Option<&std::path::Path>,
    ) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let manifest = directory.path().join("manifest.json");
        std::fs::write(&manifest, descriptor.to_manifest_json().unwrap()).unwrap();
        let package = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../python/laminardb_process")
            .canonicalize()
            .unwrap();
        let mut paths = vec![package.clone(), package.join("tests")];
        if let Some(dependencies) = std::env::var_os("LAMINAR_PROCESS_PYTHON_DEPS") {
            paths.push(PathBuf::from(dependencies));
        }
        if let Some(existing) = std::env::var_os("PYTHONPATH") {
            paths.extend(std::env::split_paths(&existing));
        }
        let python_path = std::env::join_paths(paths).unwrap();
        let mut command = Command::new(python);
        command
            .args(["-m", "laminardb_process.worker", "--manifest"])
            .arg(&manifest)
            .args([
                "--handler",
                handler,
                "--bind",
                "127.0.0.1:0",
                "--max-in-flight",
            ])
            .arg(max_in_flight.to_string())
            .env("PYTHONPATH", python_path)
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true);
        if let Some(marker) = marker {
            command.env("LAMINAR_PROCESS_TEST_BLOCK_MARKER", marker);
        }
        let mut child = command.spawn().unwrap();
        let stdout = child.stdout.take().unwrap();
        let line = tokio::time::timeout(
            Duration::from_secs(15),
            tokio::io::BufReader::new(stdout).lines().next_line(),
        )
        .await
        .unwrap()
        .unwrap()
        .expect("Python worker exited before readiness");
        let port = line.strip_prefix("READY ").unwrap().parse::<u16>().unwrap();
        Self {
            _directory: directory,
            child,
            endpoint: format!("http://127.0.0.1:{port}"),
        }
    }

    async fn stop(mut self) {
        let _ = self.child.start_kill();
        let _ = self.child.wait().await;
    }
}

fn python_descriptor() -> ProcessFunctionDescriptor {
    let mut descriptor = descriptor();
    descriptor.runtime = ProcessRuntime::RemotePython;
    descriptor
}

#[test]
fn quickstart_manifest_is_canonical_and_binds_example_handler() {
    let example = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../examples/process_python");
    let raw = std::fs::read(example.join("manifest.json")).unwrap();
    let canonical = raw.strip_suffix(b"\n").unwrap_or(&raw);
    let descriptor = ProcessFunctionDescriptor::from_manifest_json(canonical).unwrap();
    assert_eq!(descriptor.runtime, ProcessRuntime::RemotePython);
    assert_eq!(descriptor.to_manifest_json().unwrap(), canonical);
    let handler = std::fs::read(example.join("handler.py")).unwrap();
    assert_eq!(
        descriptor.implementation_digest,
        format!("{:x}", Sha256::digest(handler))
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn python_worker_exchanges_state_outputs_and_timer_callbacks() {
    let Ok(python) = std::env::var("LAMINAR_PROCESS_PYTHON") else {
        return;
    };
    let descriptor = python_descriptor();
    let worker = PythonWorker::start(
        &python,
        &descriptor,
        "conformance_handlers:accumulate",
        2,
        None,
    )
    .await;
    let client = RemoteProcessClient::connect_loopback(
        &worker.endpoint,
        descriptor,
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
    assert_eq!(results[0].value, ValueMutation::Set(3));
    assert_eq!(results[0].output.len(), 2);
    assert_eq!(results[0].output[0], results[0].output[1]);
    assert_eq!(
        results[0].timers,
        vec![TimerOperation::Set {
            name: "flush".into(),
            at_us: 1_000_100,
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
    assert_eq!(
        results[0].timers,
        vec![TimerOperation::Cancel {
            name: "flush".into()
        }]
    );
    worker.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn python_worker_is_invariant_to_distinct_key_batching_and_order() {
    let Ok(python) = std::env::var("LAMINAR_PROCESS_PYTHON") else {
        return;
    };
    let descriptor = python_descriptor();
    let worker = PythonWorker::start(
        &python,
        &descriptor,
        "conformance_handlers:accumulate",
        2,
        None,
    )
    .await;
    let client = RemoteProcessClient::connect_loopback(
        &worker.endpoint,
        descriptor,
        2,
        Duration::from_secs(5),
    )
    .await
    .unwrap();
    let alpha = activation(11, "alpha", 3, ValueState::Value(2));
    let beta = activation(12, "beta", 4, ValueState::Absent);
    let together = client
        .invoke(&scope(), &[alpha.clone(), beta.clone()])
        .await
        .unwrap();
    let beta_alone = client.invoke(&scope(), &[beta]).await.unwrap();
    let alpha_alone = client.invoke(&scope(), &[alpha]).await.unwrap();
    assert_eq!(together[0].value, alpha_alone[0].value);
    assert_eq!(together[0].output, alpha_alone[0].output);
    assert_eq!(together[0].timers, alpha_alone[0].timers);
    assert_eq!(together[1].value, beta_alone[0].value);
    assert_eq!(together[1].output, beta_alone[0].output);
    assert_eq!(together[1].timers, beta_alone[0].timers);
    worker.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn python_worker_distinguishes_absent_null_and_present_state() {
    let Ok(python) = std::env::var("LAMINAR_PROCESS_PYTHON") else {
        return;
    };
    let descriptor = python_descriptor();
    let worker = PythonWorker::start(
        &python,
        &descriptor,
        "conformance_handlers:state_variants",
        2,
        None,
    )
    .await;
    let client = RemoteProcessClient::connect_loopback(
        &worker.endpoint,
        descriptor,
        2,
        Duration::from_secs(5),
    )
    .await
    .unwrap();
    let result = client
        .invoke(
            &scope(),
            &[
                activation(21, "absent", 0, ValueState::Absent),
                activation(22, "null", 0, ValueState::Null),
                activation(23, "value", 0, ValueState::Value(7)),
            ],
        )
        .await
        .unwrap();
    assert_eq!(
        result.iter().map(|item| item.value).collect::<Vec<_>>(),
        [
            ValueMutation::SetNull,
            ValueMutation::Clear,
            ValueMutation::Set(8),
        ]
    );
    worker.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn python_worker_round_trips_scalar_types_and_nulls() {
    let Ok(python) = std::env::var("LAMINAR_PROCESS_PYTHON") else {
        return;
    };
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Utf8, false),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
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
        Field::new(
            "other_ts",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
    ]));
    let mut descriptor = python_descriptor();
    descriptor.input_schema = schema.clone();
    descriptor.output_schema = schema.clone();
    let worker = PythonWorker::start(
        &python,
        &descriptor,
        "conformance_handlers:echo_types",
        2,
        None,
    )
    .await;
    let client = RemoteProcessClient::connect_loopback(
        &worker.endpoint,
        descriptor,
        2,
        Duration::from_secs(5),
    )
    .await
    .unwrap();
    let mut input = activation(1, "alpha", 0, ValueState::Absent);
    input.callback = ProcessCallback::Input(
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["alpha"])),
                Arc::new(TimestampMicrosecondArray::from(vec![1_000_000])),
                Arc::new(BooleanArray::from(vec![Some(true)])),
                Arc::new(Int8Array::from(vec![Some(-8)])),
                Arc::new(Int16Array::from(vec![Some(-16)])),
                Arc::new(Int32Array::from(vec![Some(-32)])),
                Arc::new(Int64Array::from(vec![Some(-64)])),
                Arc::new(Float32Array::from(vec![Some(f32::NAN)])),
                Arc::new(Float64Array::from(vec![Some(f64::INFINITY)])),
                Arc::new(StringArray::from(vec![Some("é")])),
                Arc::new(BinaryArray::from(vec![Some(&b"\0\xff"[..])])),
                Arc::new(
                    Decimal128Array::from(vec![Some(123_456)])
                        .with_precision_and_scale(20, 4)
                        .unwrap(),
                ),
                Arc::new(TimestampMicrosecondArray::from(vec![Some(1_234_567)])),
            ],
        )
        .unwrap(),
    );
    let result = client.invoke(&scope(), &[input]).await.unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].value, ValueMutation::Unchanged);
    let output = &result[0].output[0];
    assert_eq!(output.num_rows(), 1);
    assert!(output
        .column(2)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap()
        .value(0));
    assert_eq!(
        output
            .column(3)
            .as_any()
            .downcast_ref::<Int8Array>()
            .unwrap()
            .value(0),
        -8
    );
    assert_eq!(
        output
            .column(4)
            .as_any()
            .downcast_ref::<Int16Array>()
            .unwrap()
            .value(0),
        -16
    );
    assert_eq!(
        output
            .column(5)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(0),
        -32
    );
    assert_eq!(
        output
            .column(6)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        -64
    );
    assert!(output
        .column(7)
        .as_any()
        .downcast_ref::<Float32Array>()
        .unwrap()
        .value(0)
        .is_nan());
    assert_eq!(
        output
            .column(9)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "é"
    );
    assert_eq!(
        output
            .column(10)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap()
            .value(0),
        b"\0\xff"
    );
    assert_eq!(
        output
            .column(11)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap()
            .value(0),
        123_456
    );
    assert_eq!(
        output
            .column(12)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap()
            .value(0),
        1_234_567
    );
    assert!(output
        .column(8)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .value(0)
        .is_infinite());

    let mut nulls = activation(2, "beta", 0, ValueState::Null);
    nulls.callback = ProcessCallback::Input(
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["beta"])),
                Arc::new(TimestampMicrosecondArray::from(vec![1_000_000])),
                Arc::new(BooleanArray::from(vec![None])),
                Arc::new(Int8Array::from(vec![None])),
                Arc::new(Int16Array::from(vec![None])),
                Arc::new(Int32Array::from(vec![None])),
                Arc::new(Int64Array::from(vec![None])),
                Arc::new(Float32Array::from(vec![None])),
                Arc::new(Float64Array::from(vec![None])),
                Arc::new(StringArray::from(vec![None::<&str>])),
                Arc::new(BinaryArray::from(vec![None::<&[u8]>])),
                Arc::new(
                    Decimal128Array::from(vec![None])
                        .with_precision_and_scale(20, 4)
                        .unwrap(),
                ),
                Arc::new(TimestampMicrosecondArray::from(vec![None])),
            ],
        )
        .unwrap(),
    );
    let result = client.invoke(&scope(), &[nulls]).await.unwrap();
    assert!(result[0].output[0].columns()[2..]
        .iter()
        .all(|column| column.is_null(0)));
    worker.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn python_worker_rejects_manifest_mismatch_and_truncated_ipc() {
    let Ok(python) = std::env::var("LAMINAR_PROCESS_PYTHON") else {
        return;
    };
    let descriptor = python_descriptor();
    let worker = PythonWorker::start(
        &python,
        &descriptor,
        "conformance_handlers:accumulate",
        2,
        None,
    )
    .await;
    let mut wrong = descriptor.clone();
    wrong.implementation_digest = "c".repeat(64);
    let wrong_client =
        RemoteProcessClient::connect_loopback(&worker.endpoint, wrong, 1, Duration::from_secs(5))
            .await
            .unwrap();
    assert!(wrong_client
        .invoke(&scope(), &[activation(1, "alpha", 1, ValueState::Absent)])
        .await
        .is_err());

    let channel = tonic::transport::Endpoint::from_shared(worker.endpoint.clone())
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut raw = wire::process_worker_client::ProcessWorkerClient::new(channel);
    let missing_deadline = raw
        .exchange(tonic::Request::new(tokio_stream::iter(Vec::<
            wire::HostFrame,
        >::new())))
        .await
        .unwrap_err();
    assert_eq!(missing_deadline.code(), tonic::Code::DeadlineExceeded);
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
    let mut invalid = crate::process_function::remote::codec::encode_activation(
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
    let mut request = tonic::Request::new(tokio_stream::iter(frames));
    request.set_timeout(Duration::from_secs(5));
    let error = raw.exchange(request).await.unwrap_err();
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    worker.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn python_cancelled_call_keeps_worker_credit_until_handler_finishes() {
    let Ok(python) = std::env::var("LAMINAR_PROCESS_PYTHON") else {
        return;
    };
    let descriptor = python_descriptor();
    let marker_dir = tempfile::tempdir().unwrap();
    let marker = marker_dir.path().join("started.txt");
    let worker = PythonWorker::start(
        &python,
        &descriptor,
        "conformance_handlers:blocking",
        1,
        Some(&marker),
    )
    .await;
    let client = RemoteProcessClient::connect_loopback(
        &worker.endpoint,
        descriptor.clone(),
        1,
        Duration::from_secs(5),
    )
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
    tokio::time::timeout(Duration::from_secs(5), async {
        while !marker.exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    first.abort();
    let _ = first.await;

    let second_client = RemoteProcessClient::connect_loopback(
        &worker.endpoint,
        descriptor,
        1,
        Duration::from_secs(5),
    )
    .await
    .unwrap();
    let second = tokio::spawn(async move {
        second_client
            .invoke(&scope(), &[activation(2, "beta", 1, ValueState::Absent)])
            .await
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        std::fs::read_to_string(&marker)
            .unwrap()
            .lines()
            .collect::<Vec<_>>(),
        ["1"]
    );
    second.abort();
    let _ = second.await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(
        std::fs::read_to_string(&marker)
            .unwrap()
            .lines()
            .collect::<Vec<_>>(),
        ["1"]
    );

    let client = RemoteProcessClient::connect_loopback(
        &worker.endpoint,
        python_descriptor(),
        1,
        Duration::from_secs(5),
    )
    .await
    .unwrap();
    let result = client
        .invoke(&scope(), &[activation(3, "gamma", 1, ValueState::Absent)])
        .await
        .unwrap();
    assert_eq!(result[0].activation_id, 3);
    worker.stop().await;
}
