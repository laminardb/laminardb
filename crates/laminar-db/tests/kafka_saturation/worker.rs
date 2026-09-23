use std::fs;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion_expr::{create_udf, ColumnarValue, Volatility};
use laminar_db::{BackpressurePolicy, DeliveryGuarantee, EngineMetrics, LaminarDB};
use parking_lot::Mutex;
use serde_json::json;

use super::{record, unique, wait_for_required_ids, BatchObservation, Case, DEADLINE};

#[derive(Default)]
struct Gate {
    entered: AtomicBool,
    released: AtomicBool,
    batches: Mutex<Vec<BatchObservation>>,
}

fn recording_udf(gate: Arc<Gate>) -> datafusion_expr::ScalarUDF {
    create_udf(
        "record_batch",
        vec![DataType::Int64, DataType::Int64],
        DataType::Int64,
        Volatility::Volatile,
        Arc::new(move |args| {
            let mut columns = ColumnarValue::values_to_arrays(args)?;
            let ids = columns[0].as_any().downcast_ref::<Int64Array>().unwrap();
            let rows = ids.len();
            let mut padded: Vec<_> = ids.iter().collect();
            padded.resize(rows.max(1024), None);
            // The identity result retains a bounded backing allocation. This lets the byte
            // case fill the downstream port while both source batches still fit on intake.
            columns[0] = Arc::new(Int64Array::from(padded).slice(0, rows));
            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, true),
                Field::new("value", DataType::Int64, true),
            ]));
            let batch = RecordBatch::try_new(schema, columns)?;
            gate.batches.lock().push(BatchObservation {
                rows: batch.num_rows(),
                retained_bytes: laminar_core::streaming::retained_arrow_bytes(&batch),
            });
            Ok(ColumnarValue::Array(Arc::clone(batch.column(0))))
        }),
    )
}

fn holding_udf(gate: Arc<Gate>, enabled: bool) -> datafusion_expr::ScalarUDF {
    create_udf(
        "hold_batch",
        vec![DataType::Int64, DataType::Int64],
        DataType::Int64,
        Volatility::Volatile,
        Arc::new(move |args| {
            if enabled && !gate.released.load(Ordering::Acquire) {
                gate.entered.store(true, Ordering::Release);
                // Test-only scheduling gate, bounded even if the controller dies. Production
                // graph, checkpoint and connector code run unchanged in this child process.
                let deadline = std::time::Instant::now() + DEADLINE;
                while !gate.released.load(Ordering::Acquire) {
                    assert!(std::time::Instant::now() < deadline, "test gate timed out");
                    std::thread::sleep(Duration::from_millis(1));
                }
            }
            Ok(args[0].clone())
        }),
    )
}

async fn database(
    case: &Case,
    phase: &str,
    gate: &Arc<Gate>,
) -> (Arc<LaminarDB>, Arc<EngineMetrics>) {
    let pressure = matches!(phase, "pressure" | "crash");
    let mut builder = LaminarDB::builder()
        .storage_dir(case.directory.join("checkpoints"))
        .checkpoint(laminar_core::streaming::StreamCheckpointConfig {
            interval_ms: None,
            ..Default::default()
        })
        .delivery_guarantee(DeliveryGuarantee::AtLeastOnce)
        .pipeline_backpressure_policy(if case.fail {
            BackpressurePolicy::Fail
        } else {
            BackpressurePolicy::Backpressure
        })
        .pipeline_max_input_buf_batches(if case.byte_capacity.is_some() {
            0
        } else if case.fail && !pressure {
            256
        } else {
            2
        })
        .pipeline_batch_window(if pressure {
            Duration::from_millis(100)
        } else {
            Duration::ZERO
        })
        .pipeline_drain_budget_ns(if pressure { 100_000_000 } else { 1 })
        .pipeline_query_budget_ns(if pressure { 1 } else { 8_000_000 })
        .register_udf(recording_udf(Arc::clone(gate)))
        .register_udf(holding_udf(Arc::clone(gate), pressure));
    if let Some(capacity) = case.byte_capacity {
        builder = builder.pipeline_max_input_buf_bytes(if case.fail && !pressure {
            capacity * 128
        } else {
            capacity
        });
    }
    let db = builder.build().await.unwrap();
    let metrics = Arc::new(EngineMetrics::new(&prometheus::Registry::new()));
    db.set_engine_metrics(Arc::clone(&metrics));
    let brokers = &case.brokers;
    db.execute(&format!(
        "CREATE SOURCE input (id BIGINT, value BIGINT) FROM KAFKA (\
        'bootstrap.servers' = '{brokers}', 'topic' = '{}', 'group.id' = '{}', \
        'startup.mode' = 'earliest', 'max.poll.records' = '1') FORMAT JSON",
        case.input, case.input
    ))
    .await
    .unwrap();
    for (name, query) in [
        (
            "stage0",
            "SELECT record_batch(id, value) AS id, value FROM input",
        ),
        (
            "stage1",
            "SELECT hold_batch(id, value) AS id, value FROM stage0",
        ),
        ("stage2", "SELECT id, value FROM stage1"),
    ] {
        db.execute(&format!("CREATE STREAM {name} AS {query}"))
            .await
            .unwrap();
    }
    db.execute(&format!(
        "CREATE SINK output FROM stage2 INTO KAFKA \
        ('bootstrap.servers' = '{brokers}', 'topic' = '{}') FORMAT JSON",
        case.output
    ))
    .await
    .unwrap();
    (db, metrics)
}

async fn pressure_worker(
    case: &Case,
    phase: &str,
    db: &Arc<LaminarDB>,
    metrics: &EngineMetrics,
    gate: &Gate,
) {
    tokio::time::timeout(DEADLINE, async {
        while !gate.entered.load(Ordering::Acquire) && db.last_fault().is_none() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("the fixture never reached saturation");
    let batches = gate.batches.lock().clone();
    let completed_producers = metrics
        .operator_process_duration
        .with_label_values(&["stage0", "normal"])
        .get_sample_count();
    record(
        case.directory.join(format!("{phase}.pressure.json")),
        json!({
            "batches":batches, "fault":db.last_fault(), "events_ingested":metrics.events_ingested.get(),
            "events_dropped":metrics.events_dropped.get(), "query_budget_ns":1, "byte_capacity":case.byte_capacity,
            "completed_producer_invocations":completed_producers
        }),
    );
    assert_eq!(
        batches.len(),
        2,
        "must fill the port with two separate valid batches"
    );
    assert!(batches.iter().all(|batch| batch.rows == 1));
    if let Some(capacity) = case.byte_capacity {
        assert!(batches.iter().all(|batch| batch.retained_bytes < capacity));
        assert_eq!(
            batches
                .iter()
                .map(|batch| batch.retained_bytes)
                .sum::<usize>(),
            capacity
        );
    }
    assert_eq!(
        completed_producers, 1,
        "both batches must occupy one port before its consumer runs"
    );
    assert_eq!(metrics.events_ingested.get(), 2);
    assert_eq!(metrics.events_dropped.get(), 0);
    let checkpoint_db = Arc::clone(db);
    let checkpoint = tokio::spawn(async move { checkpoint_db.checkpoint().await });
    if case.fail {
        let fault = db.last_fault().expect("Fail must halt at the full port");
        assert!(
            fault.contains("input buffer at capacity downstream of 'stage0'"),
            "{fault}"
        );
        assert!(!checkpoint.await.unwrap().is_ok_and(|result| result.success));
        fs::write(
            case.directory.join(format!("{phase}.checkpoint")),
            "rejected",
        )
        .unwrap();
        return;
    }
    assert!(db.last_fault().is_none());
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !checkpoint.is_finished(),
        "checkpoint passed retained graph work"
    );
    fs::write(
        case.directory.join(format!("{phase}.checkpoint")),
        "pending",
    )
    .unwrap();
    tokio::time::timeout(DEADLINE, async {
        while !case.directory.join(format!("{phase}.release")).exists() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        metrics.events_ingested.get(),
        2,
        "queued successors cannot replace replay cursors"
    );
    gate.released.store(true, Ordering::Release);
    let result = checkpoint.await.unwrap().unwrap();
    assert!(result.success);
    record(
        case.directory.join(format!("{phase}.checkpoint.json")),
        json!({"result":format!("{result:?}")}),
    );
    wait_for_required_ids(
        &case.brokers,
        &case.output,
        &unique("drained"),
        0..6,
        DEADLINE,
    )
    .await;
    assert!(db.checkpoint().await.unwrap().success);
    assert_eq!(metrics.events_dropped.get(), 0);
    assert!(db.last_fault().is_none());
    fs::write(case.directory.join(format!("{phase}.done")), []).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "child worker, launched only by durable_saturation_checkpoint_restart_ledger"]
async fn run() {
    let case: Case =
        serde_json::from_str(&std::env::var("LAMINAR_SATURATION_CASE").unwrap()).unwrap();
    let phase = std::env::var("LAMINAR_SATURATION_PHASE").unwrap();
    let gate = Arc::new(Gate::default());
    let (db, metrics) = database(&case, &phase, &gate).await;
    db.start().await.unwrap();
    fs::write(case.directory.join(format!("{phase}.ready")), []).unwrap();
    if matches!(phase.as_str(), "pressure" | "crash") {
        pressure_worker(&case, &phase, &db, &metrics, &gate).await;
    } else {
        if phase == "seed" {
            wait_for_required_ids(&case.brokers, &case.output, &unique("seed"), 0..2, DEADLINE)
                .await;
        } else {
            tokio::time::timeout(DEADLINE, async {
                while !case.directory.join("recovery.finish").exists() {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .unwrap();
        }
        assert!(db.checkpoint().await.unwrap().success);
        db.shutdown().await.unwrap();
        assert_eq!(metrics.events_dropped.get(), 0);
        record(
            case.directory.join(format!("{phase}-batches.json")),
            json!(*gate.batches.lock()),
        );
        fs::write(case.directory.join(format!("{phase}.done")), []).unwrap();
    }
    // The controller always kills and reaps this process, never a simulated reopen.
    tokio::time::sleep(DEADLINE).await;
    panic!("controller failed to terminate worker");
}
