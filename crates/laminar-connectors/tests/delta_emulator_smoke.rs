//! Delta protocol smoke coverage against Azure and GCS emulators.
//!
//! Azure exercises coordinated publication and recovery. The pinned GCS emulator cannot enforce
//! generation preconditions, so GCS is limited to at-least-once append/read/reopen coverage. The
//! production cluster-admission predicate remains native-evidence gated.

#![cfg(feature = "delta-lake")]
#![allow(clippy::disallowed_types)]

mod cloud_test_support;

use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use laminar_connectors::config::ConnectorConfig;
use laminar_connectors::connector::{
    CoordinatedCommitBatch, CoordinatedCommitContext, CoordinatedCommitCursor,
    CoordinatedCommitNamespace, CoordinatedCommitPayload, CoordinatedCommitter, DeliveryGuarantee,
    SinkConnector,
};
use laminar_connectors::lakehouse::delta_table_provider::register_delta_table;
use laminar_connectors::lakehouse::{DeltaLakeSink, DeltaLakeSinkConfig, DeltaWriteMode};
use laminar_connectors::storage::StorageProvider;
use laminar_core::checkpoint::checkpoint_manifest::PipelineIdentity;
use laminar_core::checkpoint::CheckpointAttempt;
use object_store::ObjectStoreExt as _;
use tokio_stream::StreamExt as _;

use cloud_test_support::EmulatorCloudContext;

const CLEANUP_TIMEOUT: Duration = Duration::from_secs(120);
const PROTOCOL_TIMEOUT: Duration = Duration::from_secs(300);
const MAX_CLEANUP_OBJECTS: usize = 10_000;

fn provider_feature_enabled() -> bool {
    let Ok(provider) = std::env::var("LAMINAR_CLOUD_EMULATOR_PROVIDER") else {
        return false;
    };
    (provider == "azure" && cfg!(feature = "delta-lake-azure"))
        || (provider == "gcs" && cfg!(feature = "delta-lake-gcs"))
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
}

fn batch(ids: Vec<i64>) -> Result<RecordBatch, String> {
    RecordBatch::try_new(schema(), vec![Arc::new(Int64Array::from(ids))])
        .map_err(|_| "cannot build the Delta emulator input batch".to_string())
}

fn sink_config(
    context: &EmulatorCloudContext,
    delivery_guarantee: DeliveryGuarantee,
) -> DeltaLakeSinkConfig {
    let mut config = DeltaLakeSinkConfig::new(&context.test_url);
    config.write_mode = DeltaWriteMode::Append;
    config.delivery_guarantee = delivery_guarantee;
    config.storage_options.clone_from(&context.options);
    config
}

fn namespace() -> Result<CoordinatedCommitNamespace, String> {
    CoordinatedCommitNamespace::try_new(
        PipelineIdentity::empty(),
        "018f0000-0000-7000-8000-000000000001",
        "delta-emulator-output",
    )
    .map_err(|_| "cannot build the coordinated Delta namespace".to_string())
}

fn commit_batch(
    namespace: &CoordinatedCommitNamespace,
    descriptor: Vec<u8>,
    fencing_token: u64,
) -> CoordinatedCommitBatch {
    let target = CheckpointAttempt::canonical(101);
    CoordinatedCommitBatch {
        namespace: namespace.clone(),
        expected_predecessor: CoordinatedCommitCursor {
            checkpoint_id: 0,
            fencing_token: 0,
        },
        fencing_token,
        target,
        entries: vec![CoordinatedCommitPayload {
            attempt: target,
            participant_id: 1,
            payload: Some(descriptor),
        }],
    }
}

fn commit_context() -> CoordinatedCommitContext {
    CoordinatedCommitContext::new(tokio::time::Instant::now() + Duration::from_secs(60))
}

async fn prepared_sink(
    context: &EmulatorCloudContext,
    ids: Vec<i64>,
) -> Result<(DeltaLakeSink, Vec<u8>), String> {
    let mut sink = DeltaLakeSink::with_schema(
        sink_config(context, DeliveryGuarantee::ExactlyOnce),
        schema(),
    );
    let contract = sink
        .contract(&ConnectorConfig::new("delta-lake"))
        .map_err(|_| "Delta emulator contract validation failed".to_string())?;
    if contract.is_cluster_exact_delivery_certified() {
        return Err("emulator storage unexpectedly granted cluster certification".into());
    }
    sink.open(&ConnectorConfig::new("delta-lake"))
        .await
        .map_err(|_| "Delta emulator sink open failed".to_string())?;
    sink.begin_epoch(1)
        .await
        .map_err(|_| "Delta emulator epoch start failed".to_string())?;
    sink.write_batch(&batch(ids)?)
        .await
        .map_err(|_| "Delta emulator stage write failed".to_string())?;
    let descriptor = sink
        .pre_commit(1)
        .await
        .map_err(|_| "Delta emulator descriptor preparation failed".to_string())?
        .ok_or_else(|| "Delta emulator prepared an empty descriptor".to_string())?;
    Ok((sink, descriptor))
}

async fn read_ids(context: &EmulatorCloudContext) -> Result<Vec<i64>, String> {
    let session = datafusion::prelude::SessionContext::new();
    register_delta_table(
        &session,
        "delta_emulator",
        &context.test_url,
        context.options.clone(),
    )
    .await
    .map_err(|_| "Delta emulator table registration failed".to_string())?;
    let batches = session
        .sql("SELECT id FROM delta_emulator ORDER BY id")
        .await
        .map_err(|_| "Delta emulator query planning failed".to_string())?
        .collect()
        .await
        .map_err(|_| "Delta emulator query execution failed".to_string())?;
    let mut ids = Vec::new();
    for batch in batches {
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| "Delta emulator query returned the wrong type".to_string())?;
        ids.extend(values.values());
    }
    Ok(ids)
}

async fn run_coordinated_protocol(context: &EmulatorCloudContext) -> Result<(), String> {
    let namespace = namespace()?;
    let (mut first, first_descriptor) = prepared_sink(context, vec![1, 2]).await?;
    let (mut second, second_descriptor) = prepared_sink(context, vec![99]).await?;
    if !read_ids(context).await?.is_empty() {
        return Err("prepared Delta files became visible before publication".into());
    }

    let first_batch = commit_batch(&namespace, first_descriptor, 1);
    let second_batch = commit_batch(&namespace, second_descriptor, 2);
    let (first_result, second_result) = tokio::join!(
        first.commit_aggregated(first_batch.clone(), commit_context()),
        second.commit_aggregated(second_batch.clone(), commit_context())
    );
    let (winner_batch, expected_ids, expected_fence, loser_error) =
        match (first_result, second_result) {
            (Ok(()), Err(error)) => (first_batch, vec![1, 2], 1, error),
            (Err(error), Ok(())) => (second_batch, vec![99], 2, error),
            (Ok(()), Ok(())) => {
                return Err("both conflicting Delta checkpoint publications succeeded".into());
            }
            (Err(_), Err(_)) => {
                return Err("both conflicting Delta checkpoint publications failed".into());
            }
        };
    if loser_error.is_outcome_unknown() {
        return Err(
            "the losing Delta publication was not classified as a definite conflict".into(),
        );
    }

    let mut fresh = DeltaLakeSink::with_schema(
        sink_config(context, DeliveryGuarantee::ExactlyOnce),
        schema(),
    );
    fresh
        .open(&ConnectorConfig::new("delta-lake"))
        .await
        .map_err(|_| "fresh Delta emulator client open failed".to_string())?;
    fresh
        .commit_aggregated(winner_batch, commit_context())
        .await
        .map_err(|_| "idempotent Delta emulator retry failed".to_string())?;
    let cursor = fresh
        .committed_cursor(&namespace)
        .await
        .map_err(|_| "fresh Delta emulator cursor read failed".to_string())?;
    if cursor
        != Some(CoordinatedCommitCursor {
            checkpoint_id: 101,
            fencing_token: expected_fence,
        })
    {
        return Err("fresh Delta emulator client recovered the wrong cursor".into());
    }
    if read_ids(context).await? != expected_ids {
        return Err("Delta emulator retry produced duplicate or lost records".into());
    }

    first
        .close()
        .await
        .map_err(|_| "first Delta emulator sink close failed".to_string())?;
    second
        .close()
        .await
        .map_err(|_| "second Delta emulator sink close failed".to_string())?;
    fresh
        .close()
        .await
        .map_err(|_| "fresh Delta emulator sink close failed".to_string())
}

async fn run_basic_protocol(context: &EmulatorCloudContext) -> Result<(), String> {
    let config = ConnectorConfig::new("delta-lake");
    let mut sink = DeltaLakeSink::with_schema(
        sink_config(context, DeliveryGuarantee::AtLeastOnce),
        schema(),
    );
    let contract = sink
        .contract(&config)
        .map_err(|_| "GCS emulator Delta contract validation failed".to_string())?;
    if contract.is_cluster_exact_delivery_certified() {
        return Err("GCS emulator storage unexpectedly granted cluster certification".into());
    }
    sink.open(&config)
        .await
        .map_err(|_| "GCS emulator Delta sink open failed".to_string())?;
    sink.write_batch(&batch(vec![1, 2])?)
        .await
        .map_err(|_| "GCS emulator Delta append failed".to_string())?;
    sink.flush()
        .await
        .map_err(|_| "GCS emulator Delta flush failed".to_string())?;
    sink.close()
        .await
        .map_err(|_| "GCS emulator Delta sink close failed".to_string())?;

    let mut fresh = DeltaLakeSink::with_schema(
        sink_config(context, DeliveryGuarantee::AtLeastOnce),
        schema(),
    );
    fresh
        .open(&config)
        .await
        .map_err(|_| "fresh GCS emulator Delta client open failed".to_string())?;
    if read_ids(context).await? != vec![1, 2] {
        return Err("fresh GCS emulator Delta client read different records".into());
    }
    fresh
        .close()
        .await
        .map_err(|_| "fresh GCS emulator Delta client close failed".to_string())
}

async fn run_protocol(context: &EmulatorCloudContext) -> Result<(), String> {
    match context.provider {
        StorageProvider::AzureAdls => run_coordinated_protocol(context).await,
        StorageProvider::Gcs => run_basic_protocol(context).await,
        StorageProvider::AwsS3 | StorageProvider::Local => {
            Err("cloud emulator provider was not Azure or GCS".into())
        }
    }
}

async fn cleanup(context: &EmulatorCloudContext) -> Result<(), String> {
    tokio::time::timeout(CLEANUP_TIMEOUT, cleanup_inner(context))
        .await
        .map_err(|_| "Delta emulator cleanup timed out".to_string())?
}

async fn cleanup_inner(context: &EmulatorCloudContext) -> Result<(), String> {
    let table = laminar_connectors::lakehouse::delta_io::open_or_create_table(
        &context.test_url,
        context.options.clone(),
        None,
    )
    .await
    .map_err(|_| "Delta emulator cleanup client construction failed".to_string())?;
    let store = table.object_store();
    let mut listing = store.list(None);
    let mut objects = Vec::new();
    while let Some(object) = listing.next().await {
        if objects.len() == MAX_CLEANUP_OBJECTS {
            return Err("Delta emulator cleanup exceeded its object bound".into());
        }
        objects.push(object.map_err(|_| "Delta emulator cleanup listing failed".to_string())?);
    }
    for object in objects {
        store
            .delete(&object.location)
            .await
            .map_err(|_| "Delta emulator cleanup delete failed".to_string())?;
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires an explicitly marked Azure or GCS emulator"]
async fn delta_emulator_protocol_smoke() {
    let context = EmulatorCloudContext::load(provider_feature_enabled())
        .unwrap_or_else(|reason| panic!("Delta emulator setup is incomplete: {reason}"));
    let result = tokio::time::timeout(PROTOCOL_TIMEOUT, run_protocol(&context))
        .await
        .map_err(|_| "Delta emulator protocol timed out".to_string())
        .and_then(|result| result);
    let cleanup = cleanup(&context).await;
    assert!(result.is_ok(), "{}", result.unwrap_err());
    assert!(cleanup.is_ok(), "{}", cleanup.unwrap_err());
}
