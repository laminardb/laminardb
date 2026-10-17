//! IBM MQ source and sink against a real queue manager (`tests/docker/compose.yml` service
//! `ibmmq`, IBM MQ Advanced for Developers `9.4.0.26-r2`, queue manager `QM1`).
//!
//! ```text
//! docker compose -f tests/docker/compose.yml up -d --wait ibmmq
//! LAMINAR_TEST_IBMMQ_LIBRARY=/opt/mqm/lib64/libmqm_r.so \
//! LAMINAR_TEST_IBMMQ_CONTAINER=laminardb-ibmmq \
//!   cargo test -p laminar-connectors --features ibmmq --test ibmmq_integration
//! ```
//!
//! The IBM MQ client is not bundled: `LAMINAR_TEST_IBMMQ_LIBRARY` names an installed
//! `libmqm_r`. Tests skip without it and without the container unless
//! `LAMINAR_REQUIRE_IBMMQ=1`. Queue state is provisioned and inspected with `runmqsc`.

#![cfg(feature = "ibmmq")]

use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use arrow_array::cast::AsArray;
use arrow_array::types::Int32Type;
use arrow_array::{Array, BinaryArray, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};

use laminar_connectors::config::ConnectorConfig;
use laminar_connectors::connector::{
    DeliveryGuarantee, SinkConnector, SourceBatchCursor, SourceConnector, SourcePosition,
    SourceStart,
};
use laminar_connectors::error::ConnectorError;
use laminar_connectors::ibmmq::metadata::MetadataColumns;
use laminar_connectors::ibmmq::{IbmMqSink, IbmMqSource};

static NEXT: AtomicU64 = AtomicU64::new(0);

struct Fixture {
    library: String,
    container: String,
}

fn fixture() -> Option<Fixture> {
    let library = std::env::var("LAMINAR_TEST_IBMMQ_LIBRARY").ok();
    let container = std::env::var("LAMINAR_TEST_IBMMQ_CONTAINER").ok();
    match (library, container) {
        (Some(library), Some(container)) => Some(Fixture { library, container }),
        _ if std::env::var("LAMINAR_REQUIRE_IBMMQ").is_ok_and(|v| v == "1") => {
            panic!("IBM MQ is required: set LAMINAR_TEST_IBMMQ_LIBRARY and LAMINAR_TEST_IBMMQ_CONTAINER")
        }
        _ => {
            eprintln!("skipping: IBM MQ client library or container not configured");
            None
        }
    }
}

impl Fixture {
    fn mqsc(&self, commands: &str) -> String {
        let output = Command::new("docker")
            .args(["exec", "-i", &self.container, "runmqsc", "QM1"])
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .spawn()
            .and_then(|mut child| {
                use std::io::Write;
                child.stdin.take().unwrap().write_all(commands.as_bytes())?;
                child.wait_with_output()
            })
            .expect("runmqsc");
        String::from_utf8_lossy(&output.stdout).into_owned()
    }

    fn queue(&self, name: &str, attributes: &str) -> String {
        let queue = format!(
            "DEV.LAMINAR.{name}.{}.{}",
            std::process::id() % 100_000,
            NEXT.fetch_add(1, Ordering::Relaxed)
        );
        let output = self.mqsc(&format!("DEFINE QLOCAL('{queue}') REPLACE {attributes}\n"));
        assert!(output.contains("AMQ8006I"), "define {queue}: {output}");
        queue
    }

    fn delete(&self, queues: &[&str]) {
        for queue in queues {
            self.mqsc(&format!("DELETE QLOCAL('{queue}') PURGE\n"));
        }
    }

    /// `CURDEPTH` counts messages got under an uncommitted unit of work.
    fn depth(&self, queue: &str) -> u32 {
        let output = self.mqsc(&format!("DISPLAY QLOCAL('{queue}') CURDEPTH\n"));
        let value = output
            .split("CURDEPTH(")
            .nth(1)
            .and_then(|rest| rest.split(')').next())
            .unwrap_or_else(|| panic!("no CURDEPTH for {queue}: {output}"));
        value.trim().parse().unwrap()
    }

    /// Whether a unit of work holds uncommitted puts or gets on `queue`.
    fn uncommitted(&self, queue: &str) -> bool {
        let output = self.mqsc(&format!("DISPLAY QSTATUS('{queue}') TYPE(QUEUE) UNCOM\n"));
        !output.contains("UNCOM(NO)")
    }

    fn wait_depth(&self, queue: &str, expected: u32) {
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        loop {
            let depth = self.depth(queue);
            if depth == expected {
                return;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "{queue} depth {depth}, expected {expected}"
            );
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    fn config(&self, pairs: &[(&str, &str)]) -> ConnectorConfig {
        let mut config = ConnectorConfig::new("ibmmq");
        let password = std::env::var("LAMINAR_TEST_IBMMQ_PASSWORD")
            .unwrap_or_else(|_| "laminar-test-app".into());
        for (key, value) in [
            ("client.library", self.library.as_str()),
            ("queue.manager", "QM1"),
            ("channel", "DEV.APP.SVRCONN"),
            ("connection.name", "127.0.0.1(1414)"),
            ("username", "app"),
            ("password", password.as_str()),
        ] {
            config.set(key, value);
        }
        for (key, value) in pairs {
            config.set(*key, *value);
        }
        config
    }

    async fn source(
        &self,
        queue: &str,
        payload: SchemaRef,
        pairs: &[(&str, &str)],
    ) -> Result<IbmMqSource, ConnectorError> {
        let mut all = vec![
            ("queue", queue),
            ("format", "json"),
            ("wait.interval.ms", "50"),
        ];
        all.extend_from_slice(pairs);
        let config = self.config(&all);
        let output = MetadataColumns::parse(config.get("metadata.columns"))
            .unwrap()
            .output_schema(&payload);
        let mut source = IbmMqSource::new(output, None);
        source
            .start(
                SourceStart::new(
                    config,
                    SourcePosition::Initial,
                    DeliveryGuarantee::AtLeastOnce,
                )
                .unwrap(),
            )
            .await?;
        Ok(source)
    }

    async fn sink(
        &self,
        schema: SchemaRef,
        queue: &str,
        pairs: &[(&str, &str)],
    ) -> Result<IbmMqSink, ConnectorError> {
        let mut all = vec![("queue", queue)];
        all.extend_from_slice(pairs);
        let mut sink = IbmMqSink::new(schema, None);
        sink.open(&self.config(&all)).await?;
        Ok(sink)
    }

    async fn put_json(&self, queue: &str, ids: std::ops::Range<i64>) {
        let schema = ids_schema();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(ids.collect::<Vec<_>>()))],
        )
        .unwrap();
        let mut sink = self
            .sink(schema, queue, &[("format", "json")])
            .await
            .unwrap();
        sink.write_batch(&batch).await.unwrap();
        sink.close().await.unwrap();
    }

    async fn put_raw(&self, queue: &str, payloads: &[&[u8]], pairs: &[(&str, &str)]) {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Binary,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(BinaryArray::from(payloads.to_vec()))],
        )
        .unwrap();
        let mut all = vec![("format", "raw")];
        all.extend_from_slice(pairs);
        let mut sink = self.sink(schema, queue, &all).await.unwrap();
        sink.write_batch(&batch).await.unwrap();
        sink.close().await.unwrap();
    }
}

fn ids_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
}

async fn poll_rows(source: &mut IbmMqSource, rows: usize, wait: Duration) -> Vec<RecordBatch> {
    let deadline = tokio::time::Instant::now() + wait;
    let mut batches = Vec::new();
    let mut seen = 0;
    while seen < rows && tokio::time::Instant::now() < deadline {
        match source.poll_batch(1_000).await.expect("poll") {
            Some(batch) => {
                seen += batch.num_rows();
                batches.push(batch.records);
            }
            None => tokio::time::sleep(Duration::from_millis(10)).await,
        }
    }
    batches
}

async fn poll_error(source: &mut IbmMqSource) -> ConnectorError {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        assert!(tokio::time::Instant::now() < deadline, "no poll error");
        match source.poll_batch(100).await {
            Err(error) => return error,
            Ok(Some(_)) => panic!("a batch was handed off"),
            Ok(None) => tokio::time::sleep(Duration::from_millis(10)).await,
        }
    }
}

fn ids(batches: &[RecordBatch]) -> Vec<i64> {
    batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_primitive::<arrow_array::types::Int64Type>()
                .values()
                .to_vec()
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn messages_commit_only_with_a_durable_checkpoint() {
    let Some(mq) = fixture() else { return };
    let queue = mq.queue("COMMIT", "");
    mq.put_json(&queue, 0..10).await;
    mq.wait_depth(&queue, 10);

    let mut source = mq.source(&queue, ids_schema(), &[]).await.unwrap();
    // Each batch binds its own cursor, so polls between barriers are never sealed.
    let mut batches = Vec::new();
    tokio::time::timeout(Duration::from_secs(10), async {
        while batches.iter().map(RecordBatch::num_rows).sum::<usize>() < 10 {
            if let Some(mut batch) = source.poll_batch(5).await.unwrap() {
                assert!(matches!(
                    batch.take_cursor(),
                    Some(SourceBatchCursor::Complete(_))
                ));
                batches.push(batch.records);
            }
        }
    })
    .await
    .expect("two polls of five without a barrier");
    assert_eq!(ids(&batches), (0..10).collect::<Vec<_>>());
    // Got under syncpoint: the gets stay uncommitted until the checkpoint is durable.
    assert!(mq.uncommitted(&queue));
    let cut = source.checkpoint();
    mq.put_json(&queue, 10..12).await;
    // Intake is sealed between barrier capture and the durable-commit notification.
    assert!(poll_rows(&mut source, 1, Duration::from_millis(300))
        .await
        .is_empty());
    source.notify_epoch_committed(1, &cut).await.unwrap();
    assert!(!mq.uncommitted(&queue));
    mq.wait_depth(&queue, 2);
    assert_eq!(
        ids(&poll_rows(&mut source, 2, Duration::from_secs(10)).await),
        vec![10, 11]
    );
    assert!(mq.uncommitted(&queue));
    source.close().await.unwrap();
    // Closing backs out the uncommitted unit of work instead of letting MQDISC commit it.
    assert!(!mq.uncommitted(&queue));
    mq.wait_depth(&queue, 2);
    mq.delete(&[&queue]);
}

#[tokio::test(flavor = "multi_thread")]
async fn backed_out_messages_return_with_incremented_backout_count() {
    let Some(mq) = fixture() else { return };
    let queue = mq.queue("BACKOUT", "");
    mq.put_json(&queue, 0..3).await;
    let mut first = mq.source(&queue, ids_schema(), &[]).await.unwrap();
    assert_eq!(
        ids(&poll_rows(&mut first, 3, Duration::from_secs(10)).await).len(),
        3
    );
    drop(first);
    mq.wait_depth(&queue, 3);
    let mut second = mq
        .source(
            &queue,
            ids_schema(),
            &[("metadata.columns", "backout_count,persistence,format")],
        )
        .await
        .unwrap();
    let batches = poll_rows(&mut second, 3, Duration::from_secs(10)).await;
    let batch = &batches[0];
    // Metadata columns follow canonical order: format, persistence, backout_count.
    assert_eq!(batch.column(1).as_string::<i32>().value(0), "MQSTR");
    assert!(batch
        .column(2)
        .as_primitive::<Int32Type>()
        .iter()
        .all(|p| p == Some(1)));
    let backout = batch.column(3).as_primitive::<Int32Type>();
    assert!(backout.iter().all(|count| count == Some(1)), "{backout:?}");
    second.close().await.unwrap();
    mq.delete(&[&queue]);
}

#[tokio::test(flavor = "multi_thread")]
async fn poison_messages_move_atomically_to_the_local_backout_queue() {
    let Some(mq) = fixture() else { return };
    let backout = mq.queue("BOQ", "");
    let queue = mq.queue("POISON", &format!("BOTHRESH(2) BOQNAME('{backout}')"));
    mq.put_raw(&queue, &[b"not json"], &[]).await;

    // Two failed generations back the message out twice; the third moves it. A backout counts
    // against every message of the unit of work, so the good message arrives afterwards.
    for _ in 0..2 {
        let mut source = mq.source(&queue, ids_schema(), &[]).await.unwrap();
        assert!(!poll_error(&mut source).await.is_transient());
        drop(source);
        mq.wait_depth(&queue, 1);
    }
    mq.put_json(&queue, 1..2).await;
    let mut source = mq.source(&queue, ids_schema(), &[]).await.unwrap();
    assert_eq!(
        ids(&poll_rows(&mut source, 1, Duration::from_secs(10)).await),
        vec![1]
    );
    // Uncommitted: the move is part of the checkpoint's unit of work.
    assert!(mq.uncommitted(&backout));
    let cut = source.checkpoint();
    source.notify_epoch_committed(1, &cut).await.unwrap();
    mq.wait_depth(&backout, 1);
    mq.wait_depth(&queue, 0);
    source.close().await.unwrap();
    let mut moved = mq
        .source(
            &backout,
            Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Binary,
                false,
            )])),
            &[("format", "raw")],
        )
        .await
        .unwrap();
    let batch = &poll_rows(&mut moved, 1, Duration::from_secs(10)).await[0];
    assert_eq!(batch.column(0).as_binary::<i32>().value(0), b"not json");
    moved.close().await.unwrap();
    mq.delete(&[&queue, &backout]);
}

#[tokio::test(flavor = "multi_thread")]
async fn moved_messages_keep_their_descriptor_and_properties() {
    let Some(mq) = fixture() else { return };
    let backout = mq.queue("BOQ3", "");
    let queue = mq.queue("PROPS", &format!("BOTHRESH(5) BOQNAME('{backout}')"));
    let schema = Arc::new(Schema::new(vec![
        Field::new("value", DataType::Binary, false),
        Field::new("correl", DataType::Utf8, true),
        Field::new("tenant", DataType::Int64, true),
        Field::new("region", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(BinaryArray::from(vec![&b"\xffnot json"[..]])),
            Arc::new(StringArray::from(vec![Some("trace-9")])),
            Arc::new(Int64Array::from(vec![Some(42)])),
            Arc::new(StringArray::from(vec![Some("emea")])),
        ],
    )
    .unwrap();
    let mut sink = mq
        .sink(
            schema,
            &queue,
            &[
                ("format", "raw"),
                ("correlation.id.column", "correl"),
                ("property.columns", "tenant,region"),
                ("priority", "6"),
            ],
        )
        .await
        .unwrap();
    sink.write_batch(&batch).await.unwrap();
    sink.close().await.unwrap();
    let mut source = mq
        .source(
            &queue,
            ids_schema(),
            &[("on.decode.error", "backout-queue")],
        )
        .await
        .unwrap();
    assert!(poll_rows(&mut source, 1, Duration::from_millis(500))
        .await
        .is_empty());
    let cut = source.checkpoint();
    source.notify_epoch_committed(1, &cut).await.unwrap();
    source.close().await.unwrap();
    mq.wait_depth(&backout, 1);
    let payload = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Binary,
        false,
    )]));
    let mut moved = mq
        .source(
            &backout,
            payload,
            &[
                ("format", "raw"),
                ("metadata.columns", "correl_id,priority,properties"),
            ],
        )
        .await
        .unwrap();
    let batch = &poll_rows(&mut moved, 1, Duration::from_secs(10)).await[0];
    assert_eq!(batch.column(0).as_binary::<i32>().value(0), b"\xffnot json");
    assert_eq!(
        &batch.column(1).as_fixed_size_binary().value(0)[..7],
        b"trace-9"
    );
    assert_eq!(batch.column(2).as_primitive::<Int32Type>().value(0), 6);
    let entries = batch.column(3).as_list::<i32>().value(0);
    let names: Vec<&str> = entries
        .as_struct()
        .column(0)
        .as_string::<i32>()
        .iter()
        .flatten()
        .collect();
    assert!(
        names.contains(&"tenant") && names.contains(&"region"),
        "{names:?}"
    );
    let cut = moved.checkpoint();
    moved.notify_epoch_committed(1, &cut).await.unwrap();
    moved.close().await.unwrap();
    mq.delete(&[&queue, &backout]);
}

#[tokio::test(flavor = "multi_thread")]
async fn undecodable_and_oversized_messages_follow_the_configured_policy() {
    let Some(mq) = fixture() else { return };
    let backout = mq.queue("BOQ2", "");
    let queue = mq.queue("DECODE", &format!("BOTHRESH(5) BOQNAME('{backout}')"));
    mq.put_json(&queue, 1..2).await;
    mq.put_raw(&queue, &[b"\xff\xfe"], &[]).await;
    mq.put_raw(&queue, &[&[b'{'; 200]], &[]).await;
    mq.put_json(&queue, 4..5).await;

    // fail: an oversized message is never truncated; it stays on the queue untouched.
    let mut strict = mq
        .source(
            &queue,
            ids_schema(),
            &[
                ("max.message.bytes", "64"),
                ("max.batch.bytes", "64"),
                ("max.uow.bytes", "64"),
            ],
        )
        .await
        .unwrap();
    let error = poll_error(&mut strict).await;
    assert!(!error.is_transient(), "{error}");
    drop(strict);
    mq.wait_depth(&queue, 4);

    let mut lenient = mq
        .source(
            &queue,
            ids_schema(),
            &[
                ("on.decode.error", "backout-queue"),
                ("max.message.bytes", "64"),
                ("max.batch.bytes", "64"),
                ("max.uow.bytes", "4096"),
            ],
        )
        .await
        .unwrap();
    assert_eq!(
        ids(&poll_rows(&mut lenient, 2, Duration::from_secs(10)).await),
        vec![1, 4]
    );
    let cut = lenient.checkpoint();
    lenient.notify_epoch_committed(1, &cut).await.unwrap();
    mq.wait_depth(&queue, 0);
    mq.wait_depth(&backout, 2);
    lenient.close().await.unwrap();
    mq.delete(&[&queue, &backout]);
}

#[tokio::test(flavor = "multi_thread")]
async fn unusable_backout_settings_fail_before_any_get() {
    let Some(mq) = fixture() else { return };
    let no_target = mq.queue("NOBOQ", "BOTHRESH(3)");
    let error = mq
        .source(&no_target, ids_schema(), &[])
        .await
        .err()
        .expect("BOTHRESH without BOQNAME");
    assert!(error.to_string().contains("without BOQNAME"), "{error}");
    let remote = format!("DEV.LAMINAR.REMOTE.{}", std::process::id() % 100_000);
    let xmit = mq.queue("XMIT", "USAGE(XMITQ)");
    mq.mqsc(&format!(
        "DEFINE QREMOTE('{remote}') RNAME('X') RQMNAME('OTHERQM') XMITQ('{xmit}') REPLACE\n"
    ));
    let remote_target = mq.queue("REMOTEBOQ", &format!("BOTHRESH(3) BOQNAME('{remote}')"));
    let error = mq
        .source(&remote_target, ids_schema(), &[])
        .await
        .err()
        .expect("remote BOQNAME");
    assert!(error.to_string().contains("local queue"), "{error}");
    let missing = mq
        .source("DEV.LAMINAR.DOES.NOT.EXIST", ids_schema(), &[])
        .await
        .err()
        .unwrap();
    assert!(!missing.is_transient(), "{missing}");
    mq.mqsc(&format!("DELETE QREMOTE('{remote}')\n"));
    mq.delete(&[&no_target, &remote_target, &xmit]);
}

#[tokio::test(flavor = "multi_thread")]
async fn descriptor_fields_and_properties_round_trip_byte_exact() {
    let Some(mq) = fixture() else { return };
    let queue = mq.queue("MQMD", "");
    let schema = Arc::new(Schema::new(vec![
        Field::new("value", DataType::Binary, false),
        Field::new("msg", DataType::Binary, true),
        Field::new("correl", DataType::Utf8, true),
        Field::new("prio", DataType::Int64, true),
        Field::new("tenant", DataType::Int64, true),
        Field::new("region", DataType::Utf8, true),
    ]));
    let message_id = [7_u8; 24];
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(BinaryArray::from(vec![&[0xff_u8, 0x00, 0xc3, 0x28][..]])),
            Arc::new(BinaryArray::from(vec![Some(&message_id[..])])),
            Arc::new(StringArray::from(vec![Some("order-42")])),
            Arc::new(Int64Array::from(vec![Some(7)])),
            Arc::new(Int64Array::from(vec![Some(42)])),
            Arc::new(StringArray::from(vec![Some("emea")])),
        ],
    )
    .unwrap();
    let mut sink = mq
        .sink(
            schema,
            &queue,
            &[
                ("format", "raw"),
                ("message.id.column", "msg"),
                ("correlation.id.column", "correl"),
                ("priority.column", "prio"),
                ("property.columns", "tenant,region"),
                ("reply.to.queue", "DEV.QUEUE.2"),
            ],
        )
        .await
        .unwrap();
    sink.write_batch(&batch).await.unwrap();
    sink.close().await.unwrap();

    let payload = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Binary,
        false,
    )]));
    let mut source = mq
        .source(
            &queue,
            payload,
            &[("format", "raw"), ("metadata.columns", "msg_id,correl_id,format,priority,persistence,reply_to_queue,properties,put_timestamp")],
        )
        .await
        .unwrap();
    let batch = &poll_rows(&mut source, 1, Duration::from_secs(10)).await[0];
    assert_eq!(
        batch.column(0).as_binary::<i32>().value(0),
        &[0xff, 0x00, 0xc3, 0x28]
    );
    assert_eq!(batch.column(1).as_fixed_size_binary().value(0), &message_id);
    let mut correl = [0_u8; 24];
    correl[..8].copy_from_slice(b"order-42");
    assert_eq!(batch.column(2).as_fixed_size_binary().value(0), &correl);
    assert_eq!(
        batch.column(3).as_string::<i32>().value(0),
        "",
        "binary payloads use MQFMT_NONE"
    );
    assert_eq!(
        batch.column(4).as_primitive::<Int32Type>().value(0),
        1,
        "persistent"
    );
    assert_eq!(
        batch.column(5).as_primitive::<Int32Type>().value(0),
        7,
        "priority"
    );
    assert_eq!(batch.column(6).as_string::<i32>().value(0), "DEV.QUEUE.2");
    assert!(
        !batch.column(7).is_null(0),
        "PutDate/PutTime parse to a timestamp"
    );
    let properties = batch.column(8).as_list::<i32>().value(0);
    let entries = properties.as_struct();
    let names: Vec<&str> = entries
        .column(0)
        .as_string::<i32>()
        .iter()
        .flatten()
        .collect();
    let values: Vec<&[u8]> = entries
        .column(1)
        .as_binary::<i32>()
        .iter()
        .flatten()
        .collect();
    assert!(
        names.contains(&"tenant") && names.contains(&"region"),
        "{names:?}"
    );
    let tenant = names.iter().position(|name| *name == "tenant").unwrap();
    assert_eq!(values[tenant], b"42");
    let cut = source.checkpoint();
    source.notify_epoch_committed(1, &cut).await.unwrap();
    source.close().await.unwrap();
    mq.wait_depth(&queue, 0);
    mq.delete(&[&queue]);
}

#[tokio::test(flavor = "multi_thread")]
async fn conversion_is_explicit_and_bytes_are_preserved_by_default() {
    let Some(mq) = fixture() else { return };
    let queue = mq.queue("CONVERT", "");
    // "café" in ISO-8859-1 as an MQSTR message.
    let latin1: &[u8] = b"caf\xe9";
    mq.put_raw(
        &queue,
        &[latin1, latin1],
        &[("message.format", "MQSTR"), ("ccsid", "819")],
    )
    .await;
    let payload = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Binary,
        false,
    )]));
    let mut raw = mq
        .source(
            &queue,
            payload.clone(),
            &[("format", "raw"), ("metadata.columns", "ccsid")],
        )
        .await
        .unwrap();
    let batch = &poll_rows(&mut raw, 2, Duration::from_secs(10)).await[0];
    assert_eq!(batch.column(0).as_binary::<i32>().value(0), latin1);
    assert_eq!(batch.column(1).as_primitive::<Int32Type>().value(0), 819);
    drop(raw);
    mq.wait_depth(&queue, 2);
    let mut converted = mq
        .source(
            &queue,
            payload,
            &[
                ("format", "raw"),
                ("convert", "true"),
                ("metadata.columns", "ccsid"),
            ],
        )
        .await
        .unwrap();
    let batch = &poll_rows(&mut converted, 2, Duration::from_secs(10)).await[0];
    assert_eq!(
        batch.column(0).as_binary::<i32>().value(0),
        "café".as_bytes()
    );
    assert_eq!(batch.column(1).as_primitive::<Int32Type>().value(0), 1208);
    let cut = converted.checkpoint();
    converted.notify_epoch_committed(1, &cut).await.unwrap();
    converted.close().await.unwrap();
    mq.delete(&[&queue]);
}

#[tokio::test(flavor = "multi_thread")]
async fn sink_commits_bounded_units_and_rejects_bad_identities() {
    let Some(mq) = fixture() else { return };
    let queue = mq.queue("UNITS", "MAXMSGL(1024)");
    let schema = ids_schema();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from((0..7).collect::<Vec<_>>()))],
    )
    .unwrap();
    let mut sink = mq
        .sink(schema.clone(), &queue, &[("max.uow.messages", "3")])
        .await
        .unwrap();
    sink.write_batch(&batch).await.unwrap();
    mq.wait_depth(&queue, 7);
    sink.close().await.unwrap();

    // A message over MAXMSGL is refused and its unit of work backed out.
    let big = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Binary,
        false,
    )]));
    let payload = vec![b'x'; 2048];
    let rows = RecordBatch::try_new(
        big.clone(),
        vec![Arc::new(BinaryArray::from(vec![&payload[..]]))],
    )
    .unwrap();
    let mut oversized = mq.sink(big, &queue, &[("format", "raw")]).await.unwrap();
    let error = oversized.write_batch(&rows).await.unwrap_err();
    assert!(error.to_string().contains("MAXMSGL"), "{error}");
    assert!(
        oversized.write_batch(&rows).await.is_err(),
        "a failed sink generation stays failed"
    );
    mq.wait_depth(&queue, 7);

    let error = mq
        .sink(schema, "DEV.LAMINAR.NO.SUCH.QUEUE", &[])
        .await
        .err()
        .expect("unknown queue");
    assert!(!error.is_transient(), "{error}");
    let mut bad_password = mq.config(&[("queue", &queue)]);
    bad_password.set("password", "wrong");
    let error = IbmMqSink::new(ids_schema(), None)
        .open(&bad_password)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("NOT_AUTHORIZED"), "{error}");
    assert!(!error.is_transient(), "{error}");
    mq.delete(&[&queue]);
}

#[tokio::test(flavor = "multi_thread")]
async fn ccdt_selects_the_channel() {
    let Some(mq) = fixture() else { return };
    let queue = mq.queue("CCDT", "");
    let directory = tempfile::tempdir().unwrap();
    let ccdt = directory.path().join("ccdt.json");
    std::fs::write(
        &ccdt,
        r#"{"channel":[{"name":"DEV.APP.SVRCONN","clientConnection":{"connection":[{"host":"127.0.0.1","port":1414}],"queueManager":"QM1"},"type":"clientConnection"}]}"#,
    )
    .unwrap();
    let url = format!("file://{}", ccdt.display());
    let mut config = mq.config(&[("queue", &queue), ("ccdt.url", &url)]);
    let properties = config.properties().clone();
    config = ConnectorConfig::new("ibmmq");
    for (key, value) in properties {
        if key != "channel" && key != "connection.name" {
            config.set(key, value);
        }
    }
    let mut sink = IbmMqSink::new(ids_schema(), None);
    sink.open(&config).await.unwrap();
    let batch =
        RecordBatch::try_new(ids_schema(), vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();
    sink.write_batch(&batch).await.unwrap();
    sink.close().await.unwrap();
    mq.wait_depth(&queue, 1);
    mq.delete(&[&queue]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_poll_cancelled_after_getting_messages_can_never_commit_them() {
    let Some(mq) = fixture() else { return };
    let queue = mq.queue("CANCEL", "");
    mq.put_json(&queue, 0..5).await;
    let mut source = mq.source(&queue, ids_schema(), &[]).await.unwrap();
    // The first poll of the future queues the get; the zero deadline then drops the future, so
    // the worker's result (five messages in the unit of work) has nobody to hand it to.
    assert!(tokio::time::timeout(Duration::ZERO, source.poll_batch(100))
        .await
        .is_err());
    let error = poll_error(&mut source).await;
    assert!(error.to_string().contains("cancelled poll"), "{error}");
    let cut = source.checkpoint();
    assert!(
        source.notify_epoch_committed(1, &cut).await.is_ok(),
        "nothing handed off to commit"
    );
    source.close().await.unwrap();
    // Backed out, not committed: every message is still available.
    mq.wait_depth(&queue, 5);
    mq.delete(&[&queue]);
}
