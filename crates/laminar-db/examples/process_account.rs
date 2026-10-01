//! Account activity with engine-owned totals, threshold alerts and event-time timers.
//! See `examples/process_account/README.md` for the fixed reference and recovery commands.

use std::error::Error;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Array, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use arrow_schema::SchemaRef;
use laminar_db::process_function::{
    NativeProcessFunction, ProcessActivation, ProcessActivationResult, ProcessCallback,
    ProcessFunctionDescriptor, ProcessRuntime, TimerOperation, ValueMutation, ValueState,
};
use laminar_db::subscription::{PortalFrame, SubscribeStart, SubscriptionPortal};
use laminar_db::{DbError, LaminarDB};
use sha2::{Digest, Sha256};

#[cfg(feature = "process-remote")]
use laminar_db::process_function::remote::{
    LocalPythonWorker, LocalPythonWorkerConfig, RemoteProcessClient,
};

const THRESHOLD: i64 = 100;
const INACTIVITY_US: i64 = 10_000;
const DEADLINE: Duration = Duration::from_secs(5);
type ExpectedRow = (&'static str, &'static str, i64, i64);

const INITIAL: &[(&str, i64, i64)] = &[
    ("alice", 60, 100_000),
    ("bob", 20, 100_040),
    ("alice", 50, 100_080),
    ("bob", 20, 100_040),
    ("alice", -30, 100_090),
];
// Hand-calculated reference; neither implementation computes these expectations.
const INITIAL_OUTPUT: &[ExpectedRow] = &[
    ("alice", "running", 60, 100_000),
    ("bob", "running", 20, 100_040),
    ("alice", "threshold", 110, 100_080),
    ("bob", "running", 40, 100_040),
    ("alice", "running", 80, 100_090),
];

enum DemoMode {
    Full,
    Checkpoint(PathBuf),
    Resume(PathBuf),
}

enum DemoHandler {
    Native,
    #[cfg(feature = "process-remote")]
    Python(Arc<RemoteProcessClient>),
}

struct AccountActivity {
    output_schema: SchemaRef,
}

impl NativeProcessFunction for AccountActivity {
    fn invoke(
        &self,
        activations: &[ProcessActivation],
    ) -> Result<Vec<ProcessActivationResult>, DbError> {
        activations
            .iter()
            .map(|activation| {
                let prior = match activation.state {
                    ValueState::Absent => 0,
                    ValueState::Value(value) => value,
                    ValueState::Null => {
                        return Err(DbError::InvalidOperation("account total is null".into()));
                    }
                };
                let (kind, total, value, timers) = match &activation.callback {
                    ProcessCallback::Input(batch) => {
                        let amount = batch
                            .column_by_name("amount")
                            .and_then(|column| column.as_any().downcast_ref::<Int64Array>())
                            .filter(|column| column.len() == 1 && column.null_count() == 0)
                            .ok_or_else(|| DbError::InvalidOperation("invalid amount row".into()))?
                            .value(0);
                        let total = prior.checked_add(amount).ok_or_else(|| {
                            DbError::InvalidOperation("account total overflow".into())
                        })?;
                        let at_us = activation
                            .event_time_us
                            .checked_add(INACTIVITY_US)
                            .ok_or_else(|| {
                                DbError::InvalidOperation("inactivity timer overflow".into())
                            })?;
                        let kind = if prior < THRESHOLD && total >= THRESHOLD {
                            "threshold"
                        } else {
                            "running"
                        };
                        (
                            kind,
                            total,
                            ValueMutation::Set(total),
                            vec![TimerOperation::Set {
                                name: "inactive".into(),
                                at_us,
                            }],
                        )
                    }
                    ProcessCallback::Timer { name } if name == "inactive" => {
                        ("inactive", prior, ValueMutation::Unchanged, Vec::new())
                    }
                    ProcessCallback::Timer { .. } => {
                        return Err(DbError::InvalidOperation("unknown account timer".into()));
                    }
                };
                let output = RecordBatch::try_new(
                    Arc::clone(&self.output_schema),
                    vec![
                        Arc::new(StringArray::from(vec![activation.key_text.as_str()])),
                        Arc::new(StringArray::from(vec![kind])),
                        Arc::new(Int64Array::from(vec![total])),
                        Arc::new(TimestampMicrosecondArray::from(vec![
                            activation.event_time_us,
                        ])),
                    ],
                )
                .map_err(|error| DbError::InvalidOperation(error.to_string()))?;
                Ok(ProcessActivationResult {
                    activation_id: activation.id,
                    output: vec![output],
                    value,
                    timers,
                })
            })
            .collect()
    }
}

fn descriptor() -> Result<ProcessFunctionDescriptor, DbError> {
    ProcessFunctionDescriptor::from_manifest_json(include_bytes!(
        "../../../examples/process_account/manifest.json"
    ))
}

fn demo_mode(args: &[String]) -> Result<DemoMode, Box<dyn Error>> {
    match args {
        [] => Ok(DemoMode::Full),
        [command, path] if command == "checkpoint" => Ok(DemoMode::Checkpoint(PathBuf::from(path))),
        [command, path] if command == "resume" => Ok(DemoMode::Resume(PathBuf::from(path))),
        _ => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "usage: process_account native|python [checkpoint|resume STORAGE_DIR]",
        )
        .into()),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let (runtime, args) = args.split_first().ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "select native or python")
    })?;
    let mode = demo_mode(args)?;
    match runtime.as_str() {
        "native" => {
            let mut descriptor = descriptor()?;
            descriptor.runtime = ProcessRuntime::NativeRust;
            descriptor.implementation_digest =
                format!("{:x}", Sha256::digest(include_bytes!("process_account.rs")));
            run_database(&descriptor, &DemoHandler::Native, &mode).await
        }
        #[cfg(feature = "process-remote")]
        "python" => run_python(&mode).await,
        _ => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "select native, or python with --features process-remote",
        )
        .into()),
    }
}

#[cfg(feature = "process-remote")]
async fn run_python(mode: &DemoMode) -> Result<(), Box<dyn Error>> {
    let descriptor = descriptor()?;
    if let Some(endpoint) = std::env::var_os("LAMINAR_PROCESS_ENDPOINT") {
        let endpoint = endpoint.to_str().ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "worker endpoint must be UTF-8",
            )
        })?;
        let client =
            RemoteProcessClient::connect_loopback(endpoint, descriptor.clone(), 2, DEADLINE)
                .await?;
        return run_database(&descriptor, &DemoHandler::Python(Arc::new(client)), mode).await;
    }
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let example = root.join("examples/process_account");
    let mut python_paths = vec![root.join("python/laminardb_process")];
    if let Some(dependencies) = std::env::var_os("LAMINAR_PROCESS_PYTHON_DEPS") {
        python_paths.push(PathBuf::from(dependencies));
    }
    let worker = LocalPythonWorker::start(LocalPythonWorkerConfig {
        python: std::env::var_os("LAMINAR_PROCESS_PYTHON")
            .map_or_else(|| PathBuf::from("python"), PathBuf::from),
        runtime_root: None,
        manifest: example.join("manifest.json"),
        handler_file: example.join("handler.py"),
        function: "handle".into(),
        python_paths,
        max_in_flight: 2,
        timeout: DEADLINE,
    })
    .await?;
    let outcome = run_database(&descriptor, &DemoHandler::Python(worker.client()), mode).await;
    let cleanup = worker.shutdown().await;
    match (outcome, cleanup) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) => Err(error),
        (Ok(()), Err(error)) => Err(Box::new(error)),
        (Err(error), Err(cleanup)) => {
            Err(std::io::Error::other(format!("{error}; worker cleanup: {cleanup}")).into())
        }
    }
}

async fn run_database(
    descriptor: &ProcessFunctionDescriptor,
    handler: &DemoHandler,
    mode: &DemoMode,
) -> Result<(), Box<dyn Error>> {
    let db = match mode {
        DemoMode::Full => LaminarDB::open()?,
        DemoMode::Checkpoint(path) | DemoMode::Resume(path) => {
            LaminarDB::builder()
                .storage_dir(path)
                .checkpoint(laminar_core::streaming::StreamCheckpointConfig::default())
                .build()
                .await?
        }
    };
    let outcome = run_pipeline(&db, descriptor, handler, mode).await;
    let cleanup = db.shutdown().await;
    match (outcome, cleanup) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) => Err(error),
        (Ok(()), Err(error)) => Err(Box::new(error)),
        (Err(error), Err(cleanup)) => {
            Err(std::io::Error::other(format!("{error}; database cleanup: {cleanup}")).into())
        }
    }
}

async fn run_pipeline(
    db: &Arc<LaminarDB>,
    descriptor: &ProcessFunctionDescriptor,
    handler: &DemoHandler,
    mode: &DemoMode,
) -> Result<(), Box<dyn Error>> {
    // Hold automatic progress behind the fixed out-of-order input. Explicit watermarks are ms.
    db.execute(
        "CREATE SOURCE events (account VARCHAR NOT NULL, amount BIGINT NOT NULL, \
        ts TIMESTAMP NOT NULL, WATERMARK FOR ts AS ts - INTERVAL '1' SECOND)",
    )
    .await?;
    match handler {
        DemoHandler::Native => {
            db.register_native_process_function(
                "activity",
                "events",
                descriptor.clone(),
                Arc::new(AccountActivity {
                    output_schema: Arc::clone(&descriptor.output_schema),
                }),
            )
            .await?
        }
        #[cfg(feature = "process-remote")]
        DemoHandler::Python(client) => {
            db.register_remote_process_function(
                "activity",
                "events",
                descriptor.clone(),
                Arc::clone(client),
            )
            .await?
        }
    }
    db.start().await?;
    let mut portal = db
        .open_subscription("activity", None, SubscribeStart::Tail)
        .await?;
    if !matches!(mode, DemoMode::Resume(_)) {
        push_events(db, descriptor, INITIAL)?;
        check_output(&mut portal, INITIAL_OUTPUT).await?;
    }
    if !matches!(mode, DemoMode::Checkpoint(_)) {
        push_events(
            db,
            descriptor,
            &[("alice", 25, 101_000), ("charlie", 7, 100_500)],
        )?;
        check_output(
            &mut portal,
            &[
                ("alice", "threshold", 105, 101_000),
                ("charlie", "running", 7, 100_500),
            ],
        )
        .await?;
        advance_watermark(db, descriptor, 110, ("dana", 1, 110_001))?;
        check_output(&mut portal, &[("dana", "running", 1, 110_001)]).await?;
        check_no_output(&mut portal)?;
        advance_watermark(db, descriptor, 111, ("dana", 2, 111_001))?;
        check_output(
            &mut portal,
            &[
                ("dana", "running", 3, 111_001),
                ("bob", "inactive", 40, 110_040),
                ("charlie", "inactive", 7, 110_500),
                ("alice", "inactive", 105, 111_000),
            ],
        )
        .await?;
        push_events(
            db,
            descriptor,
            &[
                ("bob", 10, 112_000),
                ("alice", -10, 112_001),
                ("alice", 5, 112_002),
            ],
        )?;
        check_output(
            &mut portal,
            &[
                ("bob", "running", 50, 112_000),
                ("alice", "running", 95, 112_001),
                ("alice", "threshold", 100, 112_002),
            ],
        )
        .await?;
        advance_watermark(db, descriptor, 123, ("dana", 3, 123_001))?;
        check_output(
            &mut portal,
            &[
                ("dana", "running", 6, 123_001),
                ("bob", "inactive", 50, 122_000),
                ("alice", "inactive", 100, 122_002),
            ],
        )
        .await?;
        advance_watermark(db, descriptor, 130, ("dana", 4, 130_001))?;
        check_output(&mut portal, &[("dana", "running", 10, 130_001)]).await?;
    }
    if !matches!(mode, DemoMode::Full) {
        db.checkpoint().await?;
    }
    check_no_output(&mut portal)?;
    println!("reference matched");
    Ok(())
}

fn push_events(
    db: &LaminarDB,
    descriptor: &ProcessFunctionDescriptor,
    rows: &[(&str, i64, i64)],
) -> Result<(), Box<dyn Error>> {
    let batch = RecordBatch::try_new(
        Arc::clone(&descriptor.input_schema),
        vec![
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.0).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|row| row.1).collect::<Vec<_>>(),
            )),
            Arc::new(TimestampMicrosecondArray::from(
                rows.iter().map(|row| row.2).collect::<Vec<_>>(),
            )),
        ],
    )?;
    db.source_untyped("events")?.push_arrow(batch)?;
    Ok(())
}

fn advance_watermark(
    db: &LaminarDB,
    descriptor: &ProcessFunctionDescriptor,
    at_ms: i64,
    event: (&str, i64, i64),
) -> Result<(), Box<dyn Error>> {
    db.source_untyped("events")?.watermark(at_ms);
    // A real fixture event drives the input cycle that forwards this frontier to operators.
    push_events(db, descriptor, &[event])
}

async fn check_output(
    portal: &mut SubscriptionPortal,
    expected: &[ExpectedRow],
) -> Result<(), Box<dyn Error>> {
    tokio::time::timeout(DEADLINE, async {
        let mut remaining = expected.to_vec();
        while !remaining.is_empty() {
            let batch = match portal.next_frame().await {
                Some(PortalFrame::Batch { batch, .. }) => batch,
                Some(PortalFrame::Barrier { .. }) => continue,
                other => {
                    return Err(std::io::Error::other(format!(
                        "output unavailable: {other:?}"
                    )))
                }
            };
            let accounts = batch.column(0).as_any().downcast_ref::<StringArray>();
            let kinds = batch.column(1).as_any().downcast_ref::<StringArray>();
            let totals = batch.column(2).as_any().downcast_ref::<Int64Array>();
            let times = batch
                .column(3)
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>();
            let (Some(accounts), Some(kinds), Some(totals), Some(times)) =
                (accounts, kinds, totals, times)
            else {
                return Err(std::io::Error::other("unexpected output schema"));
            };
            for row in 0..batch.num_rows() {
                let actual = (
                    accounts.value(row),
                    kinds.value(row),
                    totals.value(row),
                    times.value(row),
                );
                let index = remaining
                    .iter()
                    .position(|expected| *expected == actual)
                    .ok_or_else(|| {
                        std::io::Error::other(format!(
                            "unexpected output {actual:?}; checkpoint may be missing or stale"
                        ))
                    })?;
                remaining.remove(index);
                println!(
                    "account={} kind={} total={} ts_us={}",
                    actual.0, actual.1, actual.2, actual.3
                );
            }
        }
        Ok::<(), std::io::Error>(())
    })
    .await??;
    Ok(())
}

fn check_no_output(portal: &mut SubscriptionPortal) -> Result<(), Box<dyn Error>> {
    for _ in 0..64 {
        match portal.try_next_frame() {
            Some(PortalFrame::Barrier { .. }) => {}
            None => return Ok(()),
            other => {
                return Err(
                    std::io::Error::other(format!("unexpected extra output: {other:?}")).into(),
                )
            }
        }
    }
    Err(std::io::Error::other("too many checkpoint barriers").into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn native_reference_and_checkpoint_recovery() {
        let mut descriptor = descriptor().unwrap();
        descriptor.runtime = ProcessRuntime::NativeRust;
        descriptor.implementation_digest =
            format!("{:x}", Sha256::digest(include_bytes!("process_account.rs")));
        let handler = DemoHandler::Native;
        run_database(&descriptor, &handler, &DemoMode::Full)
            .await
            .unwrap();
        let directory = tempfile::tempdir().unwrap();
        run_database(
            &descriptor,
            &handler,
            &DemoMode::Checkpoint(directory.path().into()),
        )
        .await
        .unwrap();
        run_database(
            &descriptor,
            &handler,
            &DemoMode::Resume(directory.path().into()),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    #[cfg(feature = "process-remote")]
    async fn python_reference_and_checkpoint_recovery() {
        if std::env::var_os("LAMINAR_PROCESS_PYTHON").is_none() {
            return;
        }
        run_python(&DemoMode::Full).await.unwrap();
        let directory = tempfile::tempdir().unwrap();
        run_python(&DemoMode::Checkpoint(directory.path().into()))
            .await
            .unwrap();
        run_python(&DemoMode::Resume(directory.path().into()))
            .await
            .unwrap();
    }

    #[test]
    fn native_rejects_overflow_null_state_and_unknown_timer() {
        let descriptor = descriptor().unwrap();
        let handler = AccountActivity {
            output_schema: descriptor.output_schema,
        };
        let input = RecordBatch::try_new(
            descriptor.input_schema,
            vec![
                Arc::new(StringArray::from(vec!["alice"])),
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(TimestampMicrosecondArray::from(vec![100_000])),
            ],
        )
        .unwrap();
        let mut activation = ProcessActivation {
            id: 1,
            key: Arc::from(&b"host-key"[..]),
            key_text: "alice".into(),
            event_time_us: 100_000,
            callback: ProcessCallback::Input(input),
            state: ValueState::Value(i64::MAX),
        };
        assert!(matches!(
            handler.invoke(&[activation.clone()]),
            Err(DbError::InvalidOperation(_))
        ));
        activation.state = ValueState::Value(0);
        activation.event_time_us = i64::MAX - INACTIVITY_US + 1;
        assert!(matches!(
            handler.invoke(&[activation.clone()]),
            Err(DbError::InvalidOperation(_))
        ));
        activation.callback = ProcessCallback::Timer {
            name: "inactive".into(),
        };
        activation.state = ValueState::Null;
        assert!(matches!(
            handler.invoke(&[activation.clone()]),
            Err(DbError::InvalidOperation(_))
        ));
        activation.state = ValueState::Value(7);
        activation.callback = ProcessCallback::Timer {
            name: "other".into(),
        };
        assert!(matches!(
            handler.invoke(&[activation.clone()]),
            Err(DbError::InvalidOperation(_))
        ));
        activation.callback = ProcessCallback::Timer {
            name: "inactive".into(),
        };
        activation.event_time_us = i64::MAX;
        let result = handler.invoke(&[activation]).unwrap();
        assert_eq!(result[0].value, ValueMutation::Unchanged);
        assert!(result[0].timers.is_empty());
    }
}
