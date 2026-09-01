//! Provider-native Delta Lake append/read/restart integration.

#![cfg(feature = "delta-lake")]
#![allow(clippy::disallowed_types)]

mod cloud_test_support;

use std::collections::HashMap;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use laminar_connectors::config::ConnectorConfig;
use laminar_connectors::connector::{DeliveryGuarantee, SinkConnector};
use laminar_connectors::lakehouse::delta_table_provider::register_delta_table;
use laminar_connectors::lakehouse::{DeltaLakeSink, DeltaLakeSinkConfig};
use laminar_connectors::storage::StorageCredentialResolver;
use object_store::ObjectStoreExt as _;

use cloud_test_support::{DependencyVersions, EvidenceOutcome, NativeCloudContext};

const CLEANUP_TIMEOUT: Duration = Duration::from_secs(120);
const MAX_CLEANUP_OBJECTS: usize = 10_000;

fn batch(ids: Vec<i64>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(ids))]).unwrap()
}

fn config(url: &str) -> DeltaLakeSinkConfig {
    let mut config = DeltaLakeSinkConfig::new(url);
    config.delivery_guarantee = DeliveryGuarantee::AtLeastOnce;
    config
}

async fn append(url: &str, ids: Vec<i64>) -> Result<(), ()> {
    let mut sink = DeltaLakeSink::new(config(url), None);
    sink.open(&ConnectorConfig::new("delta-lake"))
        .await
        .map_err(|_| ())?;
    sink.write_batch(&batch(ids)).await.map_err(|_| ())?;
    sink.flush().await.map_err(|_| ())?;
    sink.close().await.map_err(|_| ())
}

async fn read_ids(url: &str) -> Result<Vec<i64>, ()> {
    let options = StorageCredentialResolver::resolve(url, &HashMap::new()).options;
    let session = datafusion::prelude::SessionContext::new();
    register_delta_table(&session, "native_delta", url, options)
        .await
        .map_err(|_| ())?;
    let batches = session
        .sql("SELECT id FROM native_delta ORDER BY id")
        .await
        .map_err(|_| ())?
        .collect()
        .await
        .map_err(|_| ())?;
    let mut ids = Vec::new();
    for batch in batches {
        let values = arrow_cast::cast(batch.column(0), &DataType::Int64).map_err(|_| ())?;
        let values = values.as_any().downcast_ref::<Int64Array>().ok_or(())?;
        ids.extend(values.values());
    }
    Ok(ids)
}

async fn cleanup(url: &str) -> Result<(), ()> {
    tokio::time::timeout(CLEANUP_TIMEOUT, cleanup_inner(url))
        .await
        .map_err(|_| ())?
}

async fn cleanup_inner(url: &str) -> Result<(), ()> {
    let options = StorageCredentialResolver::resolve(url, &HashMap::new()).options;
    let table = laminar_connectors::lakehouse::delta_io::open_or_create_table(url, options, None)
        .await
        .map_err(|_| ())?;
    let store = table.object_store();
    let mut listing = store.list(None);
    let mut objects = Vec::new();
    while let Some(object) = tokio_stream::StreamExt::next(&mut listing).await {
        if objects.len() == MAX_CLEANUP_OBJECTS {
            return Err(());
        }
        objects.push(object.map_err(|_| ())?);
    }
    for object in objects {
        store.delete(&object.location).await.map_err(|_| ())?;
    }
    Ok(())
}

async fn run(url: &str) -> Result<(), ()> {
    append(url, vec![0, 1, 2]).await?;
    if read_ids(url).await? != vec![0, 1, 2] {
        return Err(());
    }
    append(url, vec![3, 4, 5]).await?;

    let (left, right) = tokio::join!(append(url, vec![6]), append(url, vec![7]));
    left?;
    right?;
    if read_ids(url).await? != (0..=7).collect::<Vec<_>>() {
        return Err(());
    }
    // A new DataFusion session and Delta client must discover the same committed state.
    if read_ids(url).await? != (0..=7).collect::<Vec<_>>() {
        return Err(());
    }
    Ok(())
}

struct FaultRun {
    kills: u64,
    retry_after_prepublication_kill: u64,
    reconciled_after_publication_kill: u64,
    records: u64,
}

async fn run_process_faults(url: &str, kills: u64) -> Result<FaultRun, ()> {
    append(url, vec![0]).await?;
    let signals = tempfile::tempdir().map_err(|_| ())?;
    let mut expected = vec![0];
    let mut retries = 0;
    let mut reconciled = 0;
    for round in 0..kills {
        let id = i64::try_from(round).map_err(|_| ())? + 1;
        let before_publication = round % 2 == 0;
        kill_delta_worker(
            url,
            id,
            before_publication,
            &signals.path().join(format!("round-{round}")),
        )
        .await?;

        let observed = read_ids(url).await?;
        if before_publication {
            if observed.contains(&id) {
                return Err(());
            }
            append(url, vec![id]).await?;
            retries += 1;
        } else {
            if observed
                .iter()
                .filter(|candidate| **candidate == id)
                .count()
                != 1
            {
                return Err(());
            }
            reconciled += 1;
        }
        expected.push(id);
        if read_ids(url).await? != expected {
            return Err(());
        }
    }
    Ok(FaultRun {
        kills,
        retry_after_prepublication_kill: retries,
        reconciled_after_publication_kill: reconciled,
        records: u64::try_from(expected.len()).map_err(|_| ())?,
    })
}

async fn kill_delta_worker(
    url: &str,
    id: i64,
    before_publication: bool,
    signal_directory: &Path,
) -> Result<(), ()> {
    std::fs::create_dir_all(signal_directory).map_err(|_| ())?;
    let mode = if before_publication {
        "before-publication"
    } else {
        "after-publication"
    };
    let expected_signal = signal_directory.join("ready");
    let mut child = Command::new(std::env::current_exe().map_err(|_| ())?)
        .args([
            "--ignored",
            "--exact",
            "native_delta_fault_worker",
            "--nocapture",
        ])
        .env("LAMINAR_DELTA_FAULT_WORKER_MODE", mode)
        .env("LAMINAR_DELTA_FAULT_TABLE_URL", url)
        .env("LAMINAR_DELTA_FAULT_RECORD_ID", id.to_string())
        .env("LAMINAR_DELTA_FAULT_SIGNAL_DIR", signal_directory)
        .stdin(Stdio::null())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .map_err(|_| ())?;

    let signal = wait_for_path(&expected_signal, Duration::from_secs(120)).await;
    let publication = if before_publication {
        Ok(())
    } else {
        match std::fs::write(signal_directory.join("release"), b"release") {
            Ok(()) => wait_for_delta_record(url, id, Duration::from_secs(120)).await,
            Err(_) => Err(()),
        }
    };
    let kill = child.kill().map_err(|_| ());
    let exit = wait_for_child(&mut child, Duration::from_secs(30)).await;
    signal?;
    publication?;
    kill?;
    let status = exit?;
    if status.success() {
        return Err(());
    }
    Ok(())
}

async fn wait_for_delta_record(url: &str, id: i64, timeout: Duration) -> Result<(), ()> {
    let deadline = Instant::now() + timeout;
    loop {
        if read_ids(url)
            .await
            .is_ok_and(|records| records.contains(&id))
        {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_for_path(path: &Path, timeout: Duration) -> Result<(), ()> {
    let deadline = Instant::now() + timeout;
    loop {
        if path.is_file() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn wait_for_child(
    child: &mut Child,
    timeout: Duration,
) -> Result<std::process::ExitStatus, ()> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child.try_wait().map_err(|_| ())? {
            return Ok(status);
        }
        if Instant::now() >= deadline {
            return Err(());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
#[ignore = "invoked as a child by native_delta_process_fault_soak"]
async fn native_delta_fault_worker() {
    let Ok(mode) = std::env::var("LAMINAR_DELTA_FAULT_WORKER_MODE") else {
        return;
    };
    let url = std::env::var("LAMINAR_DELTA_FAULT_TABLE_URL")
        .expect("fault worker requires a prevalidated table URL");
    let id = std::env::var("LAMINAR_DELTA_FAULT_RECORD_ID")
        .expect("fault worker requires a record ID")
        .parse::<i64>()
        .expect("fault worker record ID must be an i64");
    let signals = std::path::PathBuf::from(
        std::env::var("LAMINAR_DELTA_FAULT_SIGNAL_DIR")
            .expect("fault worker requires a signal directory"),
    );
    std::fs::write(signals.join("ready"), b"ready")
        .expect("fault worker must publish its pre-publication boundary");
    match mode.as_str() {
        "before-publication" => {}
        "after-publication" => {
            wait_for_path(&signals.join("release"), Duration::from_secs(120))
                .await
                .expect("fault worker must receive the publication release");
            append(&url, vec![id])
                .await
                .expect("fault worker Delta publication must succeed");
        }
        _ => panic!("unsupported Delta fault worker mode"),
    }
    tokio::time::sleep(Duration::from_secs(600)).await;
}

// The arms differ in provider-specific builds; all-features makes each `cfg!` true.
#[allow(clippy::match_like_matches_macro)]
fn native_delta_feature_enabled() -> bool {
    match std::env::var("LAMINAR_NATIVE_CLOUD_PROVIDER").as_deref() {
        Ok("aws") => cfg!(feature = "delta-lake-s3"),
        Ok("azure") => cfg!(feature = "delta-lake-azure"),
        Ok("gcs") => cfg!(feature = "delta-lake-gcs"),
        _ => false,
    }
}

#[tokio::test]
#[ignore = "requires an explicit native-cloud marker and pre-provisioned location"]
async fn native_delta_append_read_restart() {
    let feature_enabled = native_delta_feature_enabled();
    let context = NativeCloudContext::load(
        "delta-native-integration",
        "native_delta_append_read_restart",
        feature_enabled,
    )
    .unwrap_or_else(|reason| panic!("required native Delta setup is incomplete: {reason}"));
    let Some(context) = context else {
        return;
    };

    let result = run(&context.test_url).await;
    let cleanup = cleanup(&context.test_url).await;
    let passed = result.is_ok() && cleanup.is_ok();
    let capabilities = serde_json::json!({
        "create_or_open": result.is_ok(),
        "append": result.is_ok(),
        "read": result.is_ok(),
        "fresh_client_reopen": result.is_ok(),
        "concurrent_writers": result.is_ok(),
        "fault_soak": false
    });
    let evidence = context.evidence(
        DependencyVersions {
            deltalake: Some("0.32.4"),
            iceberg: None,
            opendal: None,
        },
        capabilities,
        EvidenceOutcome {
            iterations: 4,
            process_kill_count: 0,
            recovery_bound_ms: 60_000,
            conditional_create: None,
            stale_cas: None,
            restart: result.is_ok(),
            delivery_contract: "at-least-once-native-integration",
            records_produced: 8,
            records_committed: u64::from(result.is_ok()) * 8,
            records_recovered: u64::from(result.is_ok()) * 8,
            duplicates: result.is_ok().then_some(0),
            losses: result.is_ok().then_some(0),
            passed,
            cleanup_result: if cleanup.is_ok() { "passed" } else { "failed" }.into(),
            failure: (!passed).then_some("native Delta integration or cleanup failed"),
        },
    );
    context
        .write_evidence(&evidence)
        .expect("native Delta evidence artifact must be written");
    assert!(passed, "native Delta integration failed");
}

#[tokio::test]
#[ignore = "requires native cloud storage and spawns killable child processes"]
async fn native_delta_process_fault_soak() {
    let feature_enabled = native_delta_feature_enabled();
    let context = NativeCloudContext::load(
        "delta-native-fault-soak",
        "native_delta_process_fault_soak",
        feature_enabled,
    )
    .unwrap_or_else(|reason| panic!("required native Delta fault setup is incomplete: {reason}"));
    let Some(context) = context else {
        return;
    };
    let kills = std::env::var("LAMINAR_SOAK_KILLS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(4);
    assert!(
        (2..=16).contains(&kills),
        "LAMINAR_SOAK_KILLS must be between 2 and 16"
    );

    let result = tokio::time::timeout(
        Duration::from_secs(300 * kills),
        run_process_faults(&context.test_url, kills),
    )
    .await
    .map_err(|_| ())
    .and_then(|result| result);
    let cleanup = cleanup(&context.test_url).await;
    let passed = result.is_ok() && cleanup.is_ok();
    let capabilities = serde_json::json!({
        "real_process_kills": result.as_ref().map_or(0, |run| run.kills),
        "kill_before_commit_publication": result.is_ok(),
        "kill_after_commit_before_parent_acknowledgement": result.is_ok(),
        "retry_after_prepublication_kill": result.as_ref().map_or(0, |run| run.retry_after_prepublication_kill),
        "outcome_unknown_reconciled_by_fresh_client": result.as_ref().map_or(0, |run| run.reconciled_after_publication_kill),
        "fresh_client_restart": result.is_ok(),
        "fault_soak": true
    });
    let records = result.as_ref().map_or(0, |run| run.records);
    let evidence = context.evidence(
        DependencyVersions {
            deltalake: Some("0.32.4"),
            iceberg: None,
            opendal: None,
        },
        capabilities,
        EvidenceOutcome {
            iterations: kills,
            process_kill_count: result.as_ref().map_or(0, |run| run.kills),
            recovery_bound_ms: 120_000,
            conditional_create: None,
            stale_cas: None,
            restart: result.is_ok(),
            delivery_contract: "delta-at-least-once-process-fault",
            records_produced: records,
            records_committed: u64::from(result.is_ok()) * records,
            records_recovered: u64::from(result.is_ok()) * records,
            duplicates: result.is_ok().then_some(0),
            losses: result.is_ok().then_some(0),
            passed,
            cleanup_result: if cleanup.is_ok() { "passed" } else { "failed" }.into(),
            failure: (!passed).then_some("native Delta process fault soak or cleanup failed"),
        },
    );
    context
        .write_evidence(&evidence)
        .expect("native Delta fault evidence artifact must be written");
    assert!(passed, "native Delta process fault soak failed");
}
