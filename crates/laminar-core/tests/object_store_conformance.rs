//! Provider-neutral object-store capability contract.
//!
//! The local tests always run. Ignored cloud entry points require explicit native or emulator
//! markers so an absent external service cannot be mistaken for evidence.

#![allow(clippy::disallowed_types)] // Mirrors the checkpoint builder's public options type.

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::future::join_all;
use futures::TryStreamExt as _;
use laminar_core::checkpoint::object_store_builder::build_object_store;
use laminar_core::storage_location::{StorageEndpointClass, StorageLocation, StorageProvider};
use object_store::path::Path;
use object_store::{
    ObjectMeta, ObjectStore, ObjectStoreExt as _, PutMode, PutOptions, UpdateVersion,
};
use serde::{Deserialize, Serialize};

const OPERATION_TIMEOUT: Duration = Duration::from_secs(60);
const MULTIPART_PART_BYTES: usize = 6 * 1024 * 1024;
const MAX_LISTED_OBJECTS: usize = 10_000;
const RANDOM_FAULT_FIRST_EPOCH: u64 = 4;
const RANDOM_FAULT_LAST_EPOCH: u64 = 12;
const RANDOM_FAULT_ITERATIONS: u64 = RANDOM_FAULT_LAST_EPOCH - RANDOM_FAULT_FIRST_EPOCH + 1;

#[derive(Clone)]
struct StoreConfig {
    url: String,
    options: HashMap<String, String>,
    test_store: Option<Arc<dyn ObjectStore>>,
}

impl StoreConfig {
    fn build(&self) -> Result<Arc<dyn ObjectStore>, String> {
        if let Some(store) = &self.test_store {
            return Ok(Arc::clone(store));
        }
        build_object_store(&self.url, &self.options)
            .map_err(|_| "object-store client construction failed".to_string())
    }
}

#[derive(Clone)]
struct CloudStoreTestContext {
    provider: StorageProvider,
    native_or_emulator: &'static str,
    base_url: String,
    unique_prefix: String,
    run_id: String,
    base_sha: String,
    tested_sha: String,
    endpoint_class: StorageEndpointClass,
    auth_source: &'static str,
}

impl std::fmt::Debug for CloudStoreTestContext {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CloudStoreTestContext")
            .field("provider", &self.provider)
            .field("native_or_emulator", &self.native_or_emulator)
            .field("base_url", &"<redacted-location>")
            .field("unique_prefix", &"<isolated-prefix>")
            .field("run_id", &self.run_id)
            .field("base_sha", &self.base_sha)
            .field("tested_sha", &self.tested_sha)
            .field("endpoint_class", &self.endpoint_class)
            .field("auth_source", &self.auth_source)
            .finish()
    }
}

#[derive(Default, Serialize)]
struct CapabilityResults {
    put_get: bool,
    range_get: bool,
    head_version: bool,
    list: bool,
    delete: bool,
    multipart: bool,
    conditional_create: bool,
    create_race_single_winner: bool,
    conditional_update: bool,
    stale_update_rejected: bool,
    fresh_client: bool,
    prefix_isolation: bool,
    cleanup_after_failure: bool,
}

#[derive(Default, Serialize)]
struct FaultResults {
    interruption_before_data_publication: bool,
    interruption_after_data_before_metadata: bool,
    interruption_before_pointer_cas: bool,
    interruption_after_cas_before_acknowledgement: bool,
    recovery_reconstructed_client: bool,
    contending_writers_single_winner: bool,
    stale_writer_rejected: bool,
    fresh_client_recovery: bool,
    randomized_epoch_failures: bool,
    committed_epochs_monotonic: bool,
    recovered_checksum_matches: bool,
}

#[derive(Clone, Copy)]
enum CapabilityScope {
    Full,
    Basic,
}

#[derive(Serialize)]
struct CloudEvidence {
    schema_version: u32,
    repository: &'static str,
    base_sha: String,
    tested_sha: String,
    workflow_run_id: String,
    provider: &'static str,
    native_or_emulator: &'static str,
    redacted_endpoint_classification: &'static str,
    region_or_cloud_location: Option<String>,
    url_scheme: String,
    enabled_cargo_features: Vec<String>,
    object_store_version: String,
    deltalake_version: Option<&'static str>,
    iceberg_version: Option<&'static str>,
    opendal_version: Option<&'static str>,
    auth_source: &'static str,
    test_suite: &'static str,
    test_name: &'static str,
    started_at: String,
    finished_at: String,
    duration_ms: u128,
    iterations: u64,
    process_kill_count: u64,
    recovery_bound_ms: u64,
    capability_results: CapabilityResults,
    fault_results: Option<FaultResults>,
    conditional_create_result: bool,
    stale_cas_result: bool,
    restart_result: bool,
    delivery_contract_tested: &'static str,
    records_produced: u64,
    records_committed: u64,
    records_recovered: u64,
    duplicates: u64,
    losses: u64,
    passed: bool,
    skip_count: u64,
    skip_reasons: Vec<String>,
    cleanup_result: String,
    failure: Option<String>,
}

#[tokio::test]
async fn in_memory_object_store_satisfies_the_conformance_contract() {
    let context = CloudStoreTestContext {
        provider: StorageProvider::Local,
        native_or_emulator: "in-memory",
        base_url: "memory:///".into(),
        unique_prefix: format!("laminardb-tests/memory/{}/", uuid::Uuid::new_v4()),
        run_id: "memory".into(),
        base_sha: "memory".into(),
        tested_sha: "memory".into(),
        endpoint_class: StorageEndpointClass::Local,
        auth_source: "anonymous-in-memory",
    };
    let config = StoreConfig {
        url: "memory:///".into(),
        options: HashMap::new(),
        test_store: Some(Arc::new(object_store::memory::InMemory::new())),
    };
    let mut capabilities = CapabilityResults::default();
    let result = run_contract(&context, &config, &mut capabilities).await;
    let cleanup = cleanup_prefixes(&config, &context.unique_prefix).await;
    assert!(result.is_ok(), "{}", result.unwrap_err());
    assert!(cleanup.is_ok(), "{}", cleanup.unwrap_err());
}

#[tokio::test]
async fn in_memory_object_store_satisfies_the_checkpoint_fault_contract() {
    let config = StoreConfig {
        url: "memory:///".into(),
        options: HashMap::new(),
        test_store: Some(Arc::new(object_store::memory::InMemory::new())),
    };
    let prefix = format!("laminardb-tests/memory-fault/{}/", uuid::Uuid::new_v4());
    let mut faults = FaultResults::default();
    let result = run_checkpoint_fault_contract(&prefix, "memory", &config, &mut faults).await;
    let cleanup = cleanup_prefixes(&config, &prefix).await;
    assert!(result.is_ok(), "{}", result.unwrap_err());
    assert!(cleanup.is_ok(), "{}", cleanup.unwrap_err());
}

#[tokio::test]
#[ignore = "requires an explicit native-cloud marker and pre-provisioned location"]
async fn native_object_store_conformance() {
    let required = env_truthy("LAMINAR_NATIVE_CLOUD_REQUIRED");
    let setup = native_context();
    let (context, config) = match setup {
        Ok(value) => value,
        Err(reason) if !required => {
            eprintln!("native object-store conformance not run: {reason}");
            return;
        }
        Err(reason) => panic!("required native object-store setup is incomplete: {reason}"),
    };
    let evidence = capability_evidence(
        &context,
        &config,
        "checkpoint-object-store-conformance",
        "native_object_store_conformance",
        "storage-capability-only",
        CapabilityScope::Full,
    )
    .await;
    write_evidence(&evidence).expect("native evidence artifact must be written");
    assert!(evidence.passed, "native object-store conformance failed");
}

#[tokio::test]
#[ignore = "requires an explicit local cloud-emulator marker"]
async fn emulator_object_store_conformance() {
    let (context, config) = emulator_context()
        .unwrap_or_else(|reason| panic!("cloud-emulator setup is incomplete: {reason}"));
    let (test_suite, delivery_contract, scope) = match context.provider {
        StorageProvider::Gcs => (
            "checkpoint-object-store-emulator-basic-smoke",
            "storage-basic-emulator-smoke",
            CapabilityScope::Basic,
        ),
        StorageProvider::AzureAdls => (
            "checkpoint-object-store-emulator-conformance",
            "storage-capability-emulator-smoke",
            CapabilityScope::Full,
        ),
        StorageProvider::AwsS3 | StorageProvider::Local => {
            panic!("cloud emulator provider was prevalidated as Azure or GCS")
        }
    };
    let evidence = capability_evidence(
        &context,
        &config,
        test_suite,
        "emulator_object_store_conformance",
        delivery_contract,
        scope,
    )
    .await;
    write_evidence(&evidence).expect("emulator evidence artifact must be written");
    assert!(
        evidence.passed,
        "emulator object-store conformance failed: {}",
        evidence.failure.as_deref().unwrap_or("unknown failure")
    );
}

#[tokio::test]
#[ignore = "requires an explicit native-cloud marker and pre-provisioned location"]
async fn native_checkpoint_store_fault_contract() {
    let required = env_truthy("LAMINAR_NATIVE_CLOUD_REQUIRED");
    let setup = native_context();
    let (context, config) = match setup {
        Ok(value) => value,
        Err(reason) if !required => {
            eprintln!("native checkpoint-store fault contract not run: {reason}");
            return;
        }
        Err(reason) => panic!("required native checkpoint-store setup is incomplete: {reason}"),
    };
    let evidence = fault_evidence(
        &context,
        &config,
        "checkpoint-store-fault-contract",
        "native_checkpoint_store_fault_contract",
        "checkpoint-state-integrity",
    )
    .await;
    write_evidence(&evidence).expect("native fault evidence artifact must be written");
    assert!(
        evidence.passed,
        "native checkpoint-store fault contract failed"
    );
}

#[tokio::test]
#[ignore = "requires an explicit local cloud-emulator marker"]
async fn emulator_checkpoint_store_fault_contract() {
    let (context, config) = emulator_context()
        .unwrap_or_else(|reason| panic!("cloud-emulator setup is incomplete: {reason}"));
    let evidence = fault_evidence(
        &context,
        &config,
        "checkpoint-store-emulator-fault-contract",
        "emulator_checkpoint_store_fault_contract",
        "checkpoint-protocol-emulator-smoke",
    )
    .await;
    write_evidence(&evidence).expect("emulator fault evidence artifact must be written");
    assert!(
        evidence.passed,
        "emulator checkpoint-store fault contract failed: {}",
        evidence.failure.as_deref().unwrap_or("unknown failure")
    );
}

async fn capability_evidence(
    context: &CloudStoreTestContext,
    config: &StoreConfig,
    test_suite: &'static str,
    test_name: &'static str,
    delivery_contract: &'static str,
    scope: CapabilityScope,
) -> CloudEvidence {
    let started_at = chrono::Utc::now();
    let started = Instant::now();
    let mut capabilities = CapabilityResults::default();
    let result = match scope {
        CapabilityScope::Full => run_contract(context, config, &mut capabilities).await,
        CapabilityScope::Basic => run_basic_contract(context, config, &mut capabilities).await,
    };
    let cleanup = cleanup_prefixes(config, &context.unique_prefix).await;
    let cleanup_result = cleanup_result(&cleanup);
    let failure = result.as_ref().err().cloned().or_else(|| cleanup.err());
    let conditional_create =
        capabilities.conditional_create && capabilities.create_race_single_winner;
    let stale_cas = capabilities.stale_update_rejected;
    let restart = capabilities.fresh_client;
    CloudEvidence {
        schema_version: 1,
        repository: "laminardb/laminardb",
        base_sha: context.base_sha.clone(),
        tested_sha: context.tested_sha.clone(),
        workflow_run_id: context.run_id.clone(),
        provider: provider_id(context.provider),
        native_or_emulator: context.native_or_emulator,
        redacted_endpoint_classification: endpoint_class_id(context.endpoint_class),
        region_or_cloud_location: non_empty_env("LAMINAR_CLOUD_LOCATION"),
        url_scheme: context_scheme(context),
        enabled_cargo_features: enabled_cargo_features(),
        object_store_version: locked_dependency_version("object_store")
            .expect("object_store dependency version must match Cargo.lock"),
        deltalake_version: None,
        iceberg_version: None,
        opendal_version: None,
        auth_source: context.auth_source,
        test_suite,
        test_name,
        started_at: started_at.to_rfc3339(),
        finished_at: chrono::Utc::now().to_rfc3339(),
        duration_ms: started.elapsed().as_millis(),
        iterations: 1,
        process_kill_count: 0,
        recovery_bound_ms: OPERATION_TIMEOUT.as_millis() as u64,
        capability_results: capabilities,
        fault_results: None,
        conditional_create_result: conditional_create,
        stale_cas_result: stale_cas,
        restart_result: restart,
        delivery_contract_tested: delivery_contract,
        records_produced: 0,
        records_committed: 0,
        records_recovered: 0,
        duplicates: 0,
        losses: 0,
        passed: failure.is_none(),
        skip_count: 0,
        skip_reasons: Vec::new(),
        cleanup_result,
        failure,
    }
}

async fn fault_evidence(
    context: &CloudStoreTestContext,
    config: &StoreConfig,
    test_suite: &'static str,
    test_name: &'static str,
    delivery_contract: &'static str,
) -> CloudEvidence {
    let started_at = chrono::Utc::now();
    let started = Instant::now();
    let mut faults = FaultResults::default();
    let result =
        run_checkpoint_fault_contract(&context.unique_prefix, &context.run_id, config, &mut faults)
            .await;
    let cleanup = cleanup_prefixes(config, &context.unique_prefix).await;
    let cleanup_result = cleanup_result(&cleanup);
    let failure = result.as_ref().err().cloned().or_else(|| cleanup.err());
    let conditional_create = faults.contending_writers_single_winner;
    let stale_cas = faults.stale_writer_rejected;
    let restart = faults.fresh_client_recovery;
    CloudEvidence {
        schema_version: 1,
        repository: "laminardb/laminardb",
        base_sha: context.base_sha.clone(),
        tested_sha: context.tested_sha.clone(),
        workflow_run_id: context.run_id.clone(),
        provider: provider_id(context.provider),
        native_or_emulator: context.native_or_emulator,
        redacted_endpoint_classification: endpoint_class_id(context.endpoint_class),
        region_or_cloud_location: non_empty_env("LAMINAR_CLOUD_LOCATION"),
        url_scheme: context_scheme(context),
        enabled_cargo_features: enabled_cargo_features(),
        object_store_version: locked_dependency_version("object_store")
            .expect("object_store dependency version must match Cargo.lock"),
        deltalake_version: None,
        iceberg_version: None,
        opendal_version: None,
        auth_source: context.auth_source,
        test_suite,
        test_name,
        started_at: started_at.to_rfc3339(),
        finished_at: chrono::Utc::now().to_rfc3339(),
        duration_ms: started.elapsed().as_millis(),
        iterations: RANDOM_FAULT_ITERATIONS,
        process_kill_count: 0,
        recovery_bound_ms: OPERATION_TIMEOUT.as_millis() as u64,
        capability_results: CapabilityResults::default(),
        fault_results: Some(faults),
        conditional_create_result: conditional_create,
        stale_cas_result: stale_cas,
        restart_result: restart,
        delivery_contract_tested: delivery_contract,
        records_produced: 0,
        records_committed: 0,
        records_recovered: 0,
        duplicates: 0,
        losses: 0,
        passed: failure.is_none(),
        skip_count: 0,
        skip_reasons: Vec::new(),
        cleanup_result,
        failure,
    }
}

fn context_scheme(context: &CloudStoreTestContext) -> String {
    StorageLocation::parse(&context.base_url)
        .expect("cloud test URL was prevalidated")
        .original_scheme
}

fn enabled_cargo_features() -> Vec<String> {
    non_empty_env("LAMINAR_ENABLED_CARGO_FEATURES")
        .map(|features| features.split(',').map(str::to_string).collect())
        .unwrap_or_default()
}

fn cleanup_result(cleanup: &Result<(), String>) -> String {
    match cleanup {
        Ok(()) => "passed".to_string(),
        Err(error) => error.clone(),
    }
}

#[tokio::test]
#[ignore = "requires an explicit native-cloud marker and isolated cleanup URL"]
async fn native_cleanup_prefix() {
    let required = env_truthy("LAMINAR_NATIVE_CLOUD_REQUIRED");
    let setup = native_cleanup_config();
    let config = match setup {
        Ok(config) => config,
        Err(reason) if !required => {
            eprintln!("native cleanup not run: {reason}");
            return;
        }
        Err(reason) => panic!("required native cleanup setup is incomplete: {reason}"),
    };
    let store = config.build().expect("native cleanup client construction");
    cleanup_listed_prefix(&store, &Path::from(""))
        .await
        .expect("native isolated prefix cleanup");
}

async fn run_contract(
    context: &CloudStoreTestContext,
    config: &StoreConfig,
    results: &mut CapabilityResults,
) -> Result<(), String> {
    run_basic_contract(context, config, results).await?;

    let store = config.build()?;
    let prefix = Path::from(context.unique_prefix.trim_end_matches('/'));
    multipart_roundtrip(&store, &child(&prefix, "multipart/large.bin")).await?;
    results.multipart = true;

    let race_path = child(&prefix, "conditional/create-race");
    let writers = (0_u8..8).map(|writer| {
        let store = Arc::clone(&store);
        let path = race_path.clone();
        async move {
            store
                .put_opts(
                    &path,
                    Bytes::from(vec![writer]).into(),
                    PutOptions::from(PutMode::Create),
                )
                .await
        }
    });
    let outcomes = bounded_value("conditional create race", join_all(writers)).await?;
    let winners = outcomes.iter().filter(|outcome| outcome.is_ok()).count();
    require(
        winners == 1,
        "conditional create race did not have exactly one winner",
    )?;
    require(
        outcomes
            .iter()
            .filter(|outcome| outcome.is_err())
            .all(|outcome| {
                matches!(
                    outcome,
                    Err(object_store::Error::AlreadyExists { .. }
                        | object_store::Error::Precondition { .. })
                )
            }),
        "conditional create losers were not classified as atomic conflicts",
    )?;
    results.conditional_create = true;
    results.create_race_single_winner = true;

    let cas_path = child(&prefix, "conditional/cas");
    let created = bounded(
        "conditional create",
        store.put_opts(
            &cas_path,
            Bytes::from_static(b"v1").into(),
            PutOptions::from(PutMode::Create),
        ),
    )
    .await?;
    let stale_version: UpdateVersion = created.into();
    let updated = bounded(
        "conditional update",
        store.put_opts(
            &cas_path,
            Bytes::from_static(b"v2").into(),
            PutOptions::from(PutMode::Update(stale_version.clone())),
        ),
    )
    .await?;
    results.conditional_update = true;
    let stale = bounded_value(
        "stale conditional update",
        store.put_opts(
            &cas_path,
            Bytes::from_static(b"stale").into(),
            PutOptions::from(PutMode::Update(stale_version)),
        ),
    )
    .await?;
    require(
        matches!(stale, Err(object_store::Error::Precondition { .. })),
        "stale conditional update was not rejected",
    )?;
    results.stale_update_rejected = true;

    let fresh_store = config.build()?;
    let fresh_read = bounded("fresh-client get", fresh_store.get(&cas_path)).await?;
    require(
        bounded("fresh-client body", fresh_read.bytes()).await? == Bytes::from_static(b"v2"),
        "fresh client did not observe the committed object version",
    )?;
    let current_version: UpdateVersion = updated.into();
    bounded(
        "fresh-client update",
        fresh_store.put_opts(
            &cas_path,
            Bytes::from_static(b"v3").into(),
            PutOptions::from(PutMode::Update(current_version)),
        ),
    )
    .await?;
    results.fresh_client = true;
    Ok(())
}

async fn run_basic_contract(
    context: &CloudStoreTestContext,
    config: &StoreConfig,
    results: &mut CapabilityResults,
) -> Result<(), String> {
    let store = config.build()?;
    let prefix = Path::from(context.unique_prefix.trim_end_matches('/'));
    let object = child(&prefix, "basic/object.bin");
    let payload = Bytes::from_static(b"laminardb-object-store-conformance");

    bounded("put", store.put(&object, payload.clone().into())).await?;
    let read = bounded("get", store.get(&object)).await?;
    let read = bounded("get body", read.bytes()).await?;
    require(read == payload, "full object read did not match the write")?;
    results.put_get = true;

    let range = bounded("range get", store.get_range(&object, 2..10)).await?;
    require(
        range == payload.slice(2..10),
        "range read returned different bytes",
    )?;
    results.range_get = true;

    let metadata = bounded("head", store.head(&object)).await?;
    require(
        metadata.e_tag.is_some() || metadata.version.is_some(),
        "head returned neither ETag nor provider version metadata",
    )?;
    results.head_version = true;

    let listed = list_prefix(&store, Some(&prefix), "list").await?;
    require(
        listed.iter().any(|entry| entry.location == object),
        "prefix listing did not contain the written object",
    )?;
    results.list = true;

    bounded("delete", store.delete(&object)).await?;
    require(
        bounded("head after delete", store.head(&object))
            .await
            .is_err(),
        "deleted object remained visible",
    )?;
    results.delete = true;

    let restart_path = child(&prefix, "restart/object.bin");
    let restart_payload = Bytes::from_static(b"fresh-client-restart");
    bounded(
        "restart put",
        store.put(&restart_path, restart_payload.clone().into()),
    )
    .await?;
    let fresh_store = config.build()?;
    let fresh_read = bounded("fresh-client get", fresh_store.get(&restart_path)).await?;
    require(
        bounded("fresh-client body", fresh_read.bytes()).await? == restart_payload,
        "fresh client did not observe the previously written object",
    )?;
    results.fresh_client = true;

    let sibling_prefix = Path::from(sibling_prefix(&context.unique_prefix));
    let sibling = child(&sibling_prefix, "object");
    bounded(
        "sibling put",
        store.put(&sibling, Bytes::from_static(b"sibling").into()),
    )
    .await?;
    let isolated = list_prefix(&store, Some(&prefix), "isolated list").await?;
    require(
        isolated.iter().all(|entry| entry.location != sibling),
        "a concurrent run's sibling prefix contaminated this run",
    )?;
    results.prefix_isolation = true;

    let cleanup_probe = child(&prefix, "cleanup/failure-probe");
    bounded(
        "cleanup probe put",
        store.put(&cleanup_probe, Bytes::from_static(b"probe").into()),
    )
    .await?;
    cleanup_path(&store, &cleanup_probe).await?;
    require(
        bounded("cleanup probe head", store.head(&cleanup_probe))
            .await
            .is_err(),
        "cleanup did not remove a partially completed probe",
    )?;
    results.cleanup_after_failure = true;
    Ok(())
}

#[derive(Clone, Serialize, Deserialize)]
struct TestCheckpointPointer {
    epoch: u64,
    manifest_path: String,
}

#[derive(Serialize, Deserialize)]
struct TestCheckpointManifest {
    epoch: u64,
    data_path: String,
    sha256: String,
}

async fn run_checkpoint_fault_contract(
    unique_prefix: &str,
    run_id: &str,
    config: &StoreConfig,
    results: &mut FaultResults,
) -> Result<(), String> {
    let store = config.build()?;
    let prefix = Path::from(unique_prefix.trim_end_matches('/'));
    let pointer_path = child(&prefix, "checkpoint/current.json");
    let initial = TestCheckpointPointer {
        epoch: 0,
        manifest_path: String::new(),
    };
    let initial_put = bounded(
        "initial pointer create",
        store.put_opts(
            &pointer_path,
            json_payload(&initial)?,
            PutOptions::from(PutMode::Create),
        ),
    )
    .await?;
    let mut current_version: UpdateVersion = initial_put.into();

    let recovered = recover_checkpoint(config, &pointer_path).await?;
    require(
        recovered.0 == 0,
        "initial pointer did not recover at epoch zero",
    )?;

    let before_data = checkpoint_paths(&prefix, 1);
    simulate_interrupted_data_publication(&store, &before_data.0).await?;
    let partial = bounded_value(
        "interrupted checkpoint visibility probe",
        store.head(&before_data.0),
    )
    .await?;
    require(
        matches!(partial, Err(object_store::Error::NotFound { .. })),
        "an incomplete checkpoint multipart upload became visible",
    )?;
    require(
        recover_checkpoint(config, &pointer_path).await?.0 == 0,
        "a checkpoint killed before data publication became visible",
    )?;
    results.interruption_before_data_publication = true;

    let data_epoch_1 = checkpoint_data(1);
    let paths_epoch_1 = checkpoint_paths(&prefix, 1);
    put_create(&store, &paths_epoch_1.0, data_epoch_1.clone()).await?;
    require(
        recover_checkpoint(config, &pointer_path).await?.0 == 0,
        "data without metadata was selected as committed",
    )?;
    results.interruption_after_data_before_metadata = true;

    write_manifest_for_existing_data(&store, 1, &paths_epoch_1, &data_epoch_1).await?;
    require(
        recover_checkpoint(config, &pointer_path).await?.0 == 0,
        "metadata without pointer CAS was selected as committed",
    )?;
    results.interruption_before_pointer_cas = true;

    let pointer_epoch_1 = TestCheckpointPointer {
        epoch: 1,
        manifest_path: paths_epoch_1.1.to_string(),
    };
    let committed_epoch_1 = bounded(
        "pointer CAS before acknowledgement",
        store.put_opts(
            &pointer_path,
            json_payload(&pointer_epoch_1)?,
            PutOptions::from(PutMode::Update(current_version.clone())),
        ),
    )
    .await?;
    current_version = committed_epoch_1.into();
    let recovered = recover_checkpoint(config, &pointer_path).await?;
    require(
        recovered.0 == 1 && recovered.1 == sha256_hex(&data_epoch_1),
        "a successful pointer CAS was not recoverable after acknowledgement loss",
    )?;
    results.interruption_after_cas_before_acknowledgement = true;
    results.recovery_reconstructed_client = true;
    results.recovered_checksum_matches = true;

    let data_epoch_2 = write_checkpoint_objects(&store, &prefix, 2).await?;
    let data_epoch_3 = write_checkpoint_objects(&store, &prefix, 3).await?;
    let pointer_epoch_2 = pointer_for(&prefix, 2);
    let pointer_epoch_3 = pointer_for(&prefix, 3);
    let stale_version = current_version.clone();
    let contenders = [pointer_epoch_2, pointer_epoch_3]
        .into_iter()
        .map(|pointer| {
            let store = Arc::clone(&store);
            let pointer_path = pointer_path.clone();
            let version = current_version.clone();
            async move {
                let payload = json_payload(&pointer).expect("bounded test pointer serialization");
                let outcome = store
                    .put_opts(
                        &pointer_path,
                        payload,
                        PutOptions::from(PutMode::Update(version)),
                    )
                    .await;
                (pointer.epoch, outcome)
            }
        });
    let contenders = bounded_value("contending pointer CAS", join_all(contenders)).await?;
    let winners = contenders
        .iter()
        .filter(|(_, outcome)| outcome.is_ok())
        .count();
    require(
        winners == 1,
        "contending pointer writers did not have one winner",
    )?;
    require(
        contenders
            .iter()
            .filter(|(_, outcome)| outcome.is_err())
            .all(|(_, outcome)| matches!(outcome, Err(object_store::Error::Precondition { .. }))),
        "contending pointer losers were not classified as conditional conflicts",
    )?;
    let (winning_epoch, winning_result) = contenders
        .into_iter()
        .find(|(_, outcome)| outcome.is_ok())
        .ok_or_else(|| "contending pointer writers had no winner".to_string())?;
    current_version = winning_result
        .map(UpdateVersion::from)
        .map_err(|_| "winning pointer result disappeared".to_string())?;
    let expected_digest = if winning_epoch == 2 {
        sha256_hex(&data_epoch_2)
    } else {
        sha256_hex(&data_epoch_3)
    };
    let recovered = recover_checkpoint(config, &pointer_path).await?;
    require(
        recovered == (winning_epoch, expected_digest),
        "recovery did not select the single CAS winner",
    )?;
    results.contending_writers_single_winner = true;

    let stale = bounded_value(
        "stale pointer CAS",
        store.put_opts(
            &pointer_path,
            json_payload(&pointer_for(&prefix, 1))?,
            PutOptions::from(PutMode::Update(stale_version)),
        ),
    )
    .await?;
    require(
        matches!(stale, Err(object_store::Error::Precondition { .. })),
        "stale pointer writer was not rejected",
    )?;
    results.stale_writer_rejected = true;

    let mut committed_epoch = winning_epoch;
    let mut previous_recovered = winning_epoch;
    for epoch in RANDOM_FAULT_FIRST_EPOCH..=RANDOM_FAULT_LAST_EPOCH {
        let phase = deterministic_fault_phase(run_id, epoch);
        let paths = checkpoint_paths(&prefix, epoch);
        let data = checkpoint_data(epoch);
        if phase >= 1 {
            put_create(&store, &paths.0, data.clone()).await?;
        }
        if phase >= 2 {
            write_manifest_for_existing_data(&store, epoch, &paths, &data).await?;
        }
        if phase == 3 {
            let committed = bounded(
                "randomized pointer CAS",
                store.put_opts(
                    &pointer_path,
                    json_payload(&pointer_for(&prefix, epoch))?,
                    PutOptions::from(PutMode::Update(current_version.clone())),
                ),
            )
            .await?;
            current_version = committed.into();
            committed_epoch = epoch;
        }
        let fresh_recovery = recover_checkpoint(config, &pointer_path).await?;
        require(
            fresh_recovery.0 == committed_epoch,
            "an incomplete randomized checkpoint was selected",
        )?;
        require(
            fresh_recovery.0 >= previous_recovered,
            "committed checkpoint epochs regressed",
        )?;
        previous_recovered = fresh_recovery.0;
    }
    let _ = current_version;
    results.randomized_epoch_failures = true;
    results.committed_epochs_monotonic = true;
    results.fresh_client_recovery = true;
    Ok(())
}

async fn simulate_interrupted_data_publication(
    store: &Arc<dyn ObjectStore>,
    final_path: &Path,
) -> Result<(), String> {
    let mut upload = bounded(
        "interrupted checkpoint multipart create",
        store.put_multipart(final_path),
    )
    .await?;
    bounded(
        "interrupted checkpoint multipart part",
        upload.put_part(Bytes::from(vec![b'i'; MULTIPART_PART_BYTES]).into()),
    )
    .await?;
    bounded("interrupted checkpoint multipart cleanup", upload.abort()).await
}

async fn recover_checkpoint(
    config: &StoreConfig,
    pointer_path: &Path,
) -> Result<(u64, String), String> {
    let store = config.build()?;
    let pointer = bounded("recovery pointer read", store.get(pointer_path)).await?;
    let pointer: TestCheckpointPointer =
        serde_json::from_slice(&bounded("recovery pointer body", pointer.bytes()).await?)
            .map_err(|_| "recovery pointer was invalid".to_string())?;
    if pointer.epoch == 0 {
        return Ok((0, String::new()));
    }
    let manifest_path = Path::from(pointer.manifest_path);
    let manifest = bounded("recovery manifest read", store.get(&manifest_path)).await?;
    let manifest: TestCheckpointManifest =
        serde_json::from_slice(&bounded("recovery manifest body", manifest.bytes()).await?)
            .map_err(|_| "recovery manifest was invalid".to_string())?;
    require(
        manifest.epoch == pointer.epoch,
        "recovery pointer and manifest epochs differed",
    )?;
    let data = bounded(
        "recovery data read",
        store.get(&Path::from(manifest.data_path)),
    )
    .await?;
    let data = bounded("recovery data body", data.bytes()).await?;
    let digest = sha256_hex(&data);
    require(
        digest == manifest.sha256,
        "recovered state checksum did not match",
    )?;
    Ok((pointer.epoch, digest))
}

async fn write_checkpoint_objects(
    store: &Arc<dyn ObjectStore>,
    prefix: &Path,
    epoch: u64,
) -> Result<Bytes, String> {
    let paths = checkpoint_paths(prefix, epoch);
    let data = checkpoint_data(epoch);
    put_create(store, &paths.0, data.clone()).await?;
    write_manifest_for_existing_data(store, epoch, &paths, &data).await?;
    Ok(data)
}

async fn write_manifest_for_existing_data(
    store: &Arc<dyn ObjectStore>,
    epoch: u64,
    paths: &(Path, Path),
    data: &Bytes,
) -> Result<(), String> {
    let manifest = TestCheckpointManifest {
        epoch,
        data_path: paths.0.to_string(),
        sha256: sha256_hex(data),
    };
    put_create(store, &paths.1, json_bytes(&manifest)?).await
}

async fn put_create(store: &Arc<dyn ObjectStore>, path: &Path, data: Bytes) -> Result<(), String> {
    bounded(
        "immutable checkpoint create",
        store.put_opts(path, data.into(), PutOptions::from(PutMode::Create)),
    )
    .await
    .map(|_| ())
}

fn pointer_for(prefix: &Path, epoch: u64) -> TestCheckpointPointer {
    TestCheckpointPointer {
        epoch,
        manifest_path: checkpoint_paths(prefix, epoch).1.to_string(),
    }
}

fn checkpoint_paths(prefix: &Path, epoch: u64) -> (Path, Path) {
    (
        child(prefix, &format!("checkpoint/epochs/{epoch}/state.bin")),
        child(prefix, &format!("checkpoint/epochs/{epoch}/manifest.json")),
    )
}

fn checkpoint_data(epoch: u64) -> Bytes {
    Bytes::from(format!("checkpoint-state-epoch-{epoch}"))
}

fn json_payload(value: &impl Serialize) -> Result<object_store::PutPayload, String> {
    json_bytes(value).map(Into::into)
}

fn json_bytes(value: &impl Serialize) -> Result<Bytes, String> {
    serde_json::to_vec(value)
        .map(Bytes::from)
        .map_err(|_| "test checkpoint metadata serialization failed".to_string())
}

fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest as _, Sha256};
    format!("{:x}", Sha256::digest(bytes))
}

async fn multipart_roundtrip(store: &Arc<dyn ObjectStore>, path: &Path) -> Result<(), String> {
    let mut upload = bounded("multipart create", store.put_multipart(path)).await?;
    let first = upload.put_part(Bytes::from(vec![b'a'; MULTIPART_PART_BYTES]).into());
    if bounded("multipart first part", first).await.is_err() {
        let _ = bounded("multipart abort", upload.abort()).await;
        return Err("multipart first part failed".into());
    }
    let second = upload.put_part(Bytes::from_static(b"tail").into());
    if bounded("multipart final part", second).await.is_err() {
        let _ = bounded("multipart abort", upload.abort()).await;
        return Err("multipart final part failed".into());
    }
    bounded("multipart complete", upload.complete()).await?;
    let metadata = bounded("multipart head", store.head(path)).await?;
    require(
        metadata.size == MULTIPART_PART_BYTES as u64 + 4,
        "multipart upload size did not match",
    )
}

async fn cleanup_prefixes(config: &StoreConfig, unique_prefix: &str) -> Result<(), String> {
    let store = config.build()?;
    let prefix = Path::from(unique_prefix.trim_end_matches('/'));
    cleanup_listed_prefix(&store, &prefix).await?;
    let sibling = Path::from(sibling_prefix(unique_prefix));
    cleanup_listed_prefix(&store, &sibling).await
}

async fn cleanup_listed_prefix(store: &Arc<dyn ObjectStore>, prefix: &Path) -> Result<(), String> {
    let objects = list_prefix(store, Some(prefix), "cleanup list").await?;
    for object in objects {
        cleanup_path(store, &object.location).await?;
    }
    let remaining = list_prefix(store, Some(prefix), "cleanup verify").await?;
    require(
        remaining.is_empty(),
        "cleanup left objects under the isolated prefix",
    )
}

async fn cleanup_path(store: &Arc<dyn ObjectStore>, path: &Path) -> Result<(), String> {
    bounded("cleanup delete", store.delete(path)).await
}

async fn list_prefix(
    store: &Arc<dyn ObjectStore>,
    prefix: Option<&Path>,
    operation: &'static str,
) -> Result<Vec<ObjectMeta>, String> {
    tokio::time::timeout(OPERATION_TIMEOUT, async {
        let mut stream = store.list(prefix);
        let mut objects = Vec::new();
        while let Some(object) = stream
            .try_next()
            .await
            .map_err(|_| format!("{operation} failed"))?
        {
            if objects.len() == MAX_LISTED_OBJECTS {
                return Err(format!(
                    "{operation} exceeded the {MAX_LISTED_OBJECTS}-object safety bound"
                ));
            }
            objects.push(object);
        }
        Ok(objects)
    })
    .await
    .map_err(|_| format!("{operation} timed out"))?
}

async fn bounded<T, E>(
    operation: &'static str,
    future: impl Future<Output = Result<T, E>>,
) -> Result<T, String> {
    tokio::time::timeout(OPERATION_TIMEOUT, future)
        .await
        .map_err(|_| format!("{operation} timed out"))?
        .map_err(|_| format!("{operation} failed"))
}

async fn bounded_value<T>(
    operation: &'static str,
    future: impl Future<Output = T>,
) -> Result<T, String> {
    tokio::time::timeout(OPERATION_TIMEOUT, future)
        .await
        .map_err(|_| format!("{operation} timed out"))
}

fn require(condition: bool, message: &'static str) -> Result<(), String> {
    condition.then_some(()).ok_or_else(|| message.to_string())
}

fn child(prefix: &Path, child: &str) -> Path {
    Path::from(format!("{prefix}/{child}"))
}

fn sibling_prefix(unique_prefix: &str) -> String {
    let trimmed = unique_prefix.trim_end_matches('/');
    let (parent, run) = trimmed.rsplit_once('/').unwrap_or(("", trimmed));
    if parent.is_empty() {
        format!("sibling-{run}")
    } else {
        format!("{parent}/sibling-{run}")
    }
}

fn deterministic_fault_phase(run_id: &str, epoch: u64) -> u8 {
    use sha2::{Digest as _, Sha256};

    let digest = Sha256::digest(format!("{run_id}:{epoch}"));
    digest[0] % 4
}

fn native_context() -> Result<(CloudStoreTestContext, StoreConfig), String> {
    if !env_truthy("LAMINAR_NATIVE_CLOUD") {
        return Err("LAMINAR_NATIVE_CLOUD=1 is required".into());
    }
    for endpoint in [
        "AWS_ENDPOINT",
        "AWS_ENDPOINT_URL",
        "AWS_ENDPOINT_URL_S3",
        "AZURITE_BLOB_STORAGE_URL",
        "AZURE_STORAGE_ENDPOINT",
        "GOOGLE_BASE_URL",
        "GOOGLE_ENDPOINT_URL",
        "STORAGE_EMULATOR_HOST",
        "GOOGLE_STORAGE_EMULATOR_HOST",
    ] {
        if non_empty_env(endpoint).is_some() {
            return Err(format!("{endpoint} must be unset in native mode"));
        }
    }
    let provider_name = required_env("LAMINAR_NATIVE_CLOUD_PROVIDER")?;
    let (provider, location_variable, feature_enabled) = match provider_name.as_str() {
        "aws" => (
            StorageProvider::AwsS3,
            "LAMINAR_AWS_TEST_URL",
            cfg!(feature = "aws"),
        ),
        "azure" => (
            StorageProvider::AzureAdls,
            "LAMINAR_AZURE_TEST_URL",
            cfg!(feature = "azure"),
        ),
        "gcs" => (
            StorageProvider::Gcs,
            "LAMINAR_GCS_TEST_URL",
            cfg!(feature = "gcs"),
        ),
        _ => return Err("LAMINAR_NATIVE_CLOUD_PROVIDER must be aws, azure, or gcs".into()),
    };
    if !feature_enabled {
        return Err(format!("the {provider_name} Cargo feature is not enabled"));
    }
    validate_native_object_store_auth(provider)?;
    let base_url = required_env(location_variable)?;
    let parsed = StorageLocation::parse(&base_url)
        .map_err(|error| format!("{location_variable} is invalid: {error}"))?;
    if parsed.provider != provider || parsed.endpoint_class() != StorageEndpointClass::Native {
        return Err(format!(
            "{location_variable} does not select the required native provider"
        ));
    }
    let run_id = required_env("GITHUB_RUN_ID").or_else(|_| required_env("LAMINAR_RUN_ID"))?;
    let base_sha = required_env("LAMINAR_BASE_SHA")?;
    let tested_sha = required_env("GITHUB_SHA").or_else(|_| required_env("LAMINAR_TESTED_SHA"))?;
    let run_component = safe_component(&run_id);
    let sha_component = safe_component(&base_sha.chars().take(12).collect::<String>());
    let unique_prefix = format!(
        "laminardb-tests/{sha_component}/{run_component}/object-store-conformance/{}/",
        uuid::Uuid::new_v4()
    );
    let auth_source = native_auth_source(provider)?;
    Ok((
        CloudStoreTestContext {
            provider,
            native_or_emulator: "native",
            base_url: base_url.clone(),
            unique_prefix,
            run_id,
            base_sha,
            tested_sha,
            endpoint_class: StorageEndpointClass::Native,
            auth_source,
        },
        StoreConfig {
            url: base_url,
            options: HashMap::new(),
            test_store: None,
        },
    ))
}

fn emulator_context() -> Result<(CloudStoreTestContext, StoreConfig), String> {
    if !env_truthy("LAMINAR_CLOUD_EMULATOR") {
        return Err("LAMINAR_CLOUD_EMULATOR=1 is required".into());
    }
    if env_truthy("LAMINAR_NATIVE_CLOUD") {
        return Err("native and emulator cloud markers are mutually exclusive".into());
    }
    let provider_name = required_env("LAMINAR_CLOUD_EMULATOR_PROVIDER")?;
    let (provider, feature_enabled, auth_source) = match provider_name.as_str() {
        "azure" => (
            StorageProvider::AzureAdls,
            cfg!(feature = "azure"),
            "explicit-static-emulator",
        ),
        "gcs" => (
            StorageProvider::Gcs,
            cfg!(feature = "gcs"),
            "anonymous-emulator",
        ),
        _ => return Err("LAMINAR_CLOUD_EMULATOR_PROVIDER must be azure or gcs".into()),
    };
    if !feature_enabled {
        return Err(format!("the {provider_name} Cargo feature is not enabled"));
    }
    let base_url = required_env("LAMINAR_CLOUD_EMULATOR_TEST_URL")?;
    let location = StorageLocation::parse(&base_url)
        .map_err(|error| format!("LAMINAR_CLOUD_EMULATOR_TEST_URL is invalid: {error}"))?;
    if location.provider != provider || location.endpoint_class() != StorageEndpointClass::Native {
        return Err("emulator test URL must use the selected provider's direct scheme".into());
    }
    let endpoint = loopback_emulator_endpoint(&required_env("LAMINAR_CLOUD_EMULATOR_ENDPOINT")?)?;
    let run_id = required_env("GITHUB_RUN_ID").or_else(|_| required_env("LAMINAR_RUN_ID"))?;
    let base_sha = required_env("LAMINAR_BASE_SHA")?;
    let tested_sha = required_env("GITHUB_SHA").or_else(|_| required_env("LAMINAR_TESTED_SHA"))?;
    let unique_prefix = format!(
        "laminardb-tests/{}/{}/object-store-emulator/{}/",
        safe_component(&base_sha.chars().take(12).collect::<String>()),
        safe_component(&run_id),
        uuid::Uuid::new_v4()
    );
    let options = emulator_options(provider, &endpoint);
    Ok((
        CloudStoreTestContext {
            provider,
            native_or_emulator: "emulator",
            base_url: base_url.clone(),
            unique_prefix,
            run_id,
            base_sha,
            tested_sha,
            endpoint_class: StorageEndpointClass::CustomOrEmulator,
            auth_source,
        },
        StoreConfig {
            url: base_url,
            options,
            test_store: None,
        },
    ))
}

fn loopback_emulator_endpoint(raw: &str) -> Result<String, String> {
    let parsed = url::Url::parse(raw)
        .map_err(|_| "LAMINAR_CLOUD_EMULATOR_ENDPOINT must be an absolute URL".to_string())?;
    if parsed.scheme() != "http"
        || !matches!(parsed.host_str(), Some("127.0.0.1" | "localhost" | "::1"))
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
    {
        return Err(
            "LAMINAR_CLOUD_EMULATOR_ENDPOINT must be a credential-free loopback HTTP URL".into(),
        );
    }
    Ok(raw.trim_end_matches('/').to_string())
}

fn emulator_options(provider: StorageProvider, endpoint: &str) -> HashMap<String, String> {
    const AZURITE_ACCOUNT: &str = "devstoreaccount1";
    const AZURITE_KEY: &str =
        "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

    match provider {
        StorageProvider::AzureAdls => HashMap::from([
            ("azure_storage_account_name".into(), AZURITE_ACCOUNT.into()),
            ("azure_storage_account_key".into(), AZURITE_KEY.into()),
            ("azure_storage_endpoint".into(), endpoint.into()),
            ("azure_allow_http".into(), "true".into()),
        ]),
        StorageProvider::Gcs => HashMap::from([
            ("google_allow_http".into(), "true".into()),
            (
                "google_service_account_key".into(),
                serde_json::json!({
                    "client_email": "",
                    "disable_oauth": true,
                    "gcs_base_url": endpoint,
                    "private_key": "",
                    "private_key_id": ""
                })
                .to_string(),
            ),
        ]),
        StorageProvider::AwsS3 | StorageProvider::Local => HashMap::new(),
    }
}

fn validate_native_object_store_auth(provider: StorageProvider) -> Result<(), String> {
    if provider != StorageProvider::Gcs {
        return Ok(());
    }
    validate_pinned_gcs_adc()
}

fn validate_pinned_gcs_adc() -> Result<(), String> {
    let Some(path) = non_empty_env("GOOGLE_APPLICATION_CREDENTIALS") else {
        return Ok(());
    };
    let bytes = std::fs::read(path)
        .map_err(|_| "GOOGLE_APPLICATION_CREDENTIALS cannot be read".to_string())?;
    let document: serde_json::Value = serde_json::from_slice(&bytes)
        .map_err(|_| "GOOGLE_APPLICATION_CREDENTIALS is not valid JSON".to_string())?;
    match document.get("type").and_then(serde_json::Value::as_str) {
        Some("service_account" | "authorized_user") => Ok(()),
        Some("external_account") => Err(
            "pinned object_store 0.13.2 cannot load external_account GCS credentials; native checkpoint certification requires an upstream refreshable WIF implementation"
                .into(),
        ),
        Some(_) => Err(
            "GOOGLE_APPLICATION_CREDENTIALS uses a credential type unsupported by pinned object_store 0.13.2"
                .into(),
        ),
        None => Err("GOOGLE_APPLICATION_CREDENTIALS has no credential type".into()),
    }
}

fn native_cleanup_config() -> Result<StoreConfig, String> {
    if !env_truthy("LAMINAR_NATIVE_CLOUD") {
        return Err("LAMINAR_NATIVE_CLOUD=1 is required".into());
    }
    let cleanup_url = required_env("LAMINAR_NATIVE_CLEANUP_URL")?;
    let location = StorageLocation::parse(&cleanup_url)
        .map_err(|error| format!("LAMINAR_NATIVE_CLEANUP_URL is invalid: {error}"))?;
    if location.endpoint_class() != StorageEndpointClass::Native {
        return Err("LAMINAR_NATIVE_CLEANUP_URL must select a native provider endpoint".into());
    }
    let provider = required_env("LAMINAR_NATIVE_CLOUD_PROVIDER")?;
    let expected_provider = match provider.as_str() {
        "aws" if cfg!(feature = "aws") => StorageProvider::AwsS3,
        "azure" if cfg!(feature = "azure") => StorageProvider::AzureAdls,
        "gcs" if cfg!(feature = "gcs") => StorageProvider::Gcs,
        "aws" | "azure" | "gcs" => {
            return Err(format!("the {provider} Cargo feature is not enabled"));
        }
        _ => return Err("LAMINAR_NATIVE_CLOUD_PROVIDER must be aws, azure, or gcs".into()),
    };
    if location.provider != expected_provider {
        return Err("cleanup URL provider does not match the selected provider".into());
    }
    let base_sha = safe_component(&required_env("LAMINAR_BASE_SHA")?);
    let run_id =
        safe_component(&required_env("GITHUB_RUN_ID").or_else(|_| required_env("LAMINAR_RUN_ID"))?);
    let expected_marker = format!("laminardb-tests/{base_sha}/{run_id}/");
    let scoped_prefix = location.prefix.trim_end_matches('/');
    if !scoped_prefix.starts_with(&expected_marker)
        && !scoped_prefix.contains(&format!("/{expected_marker}"))
    {
        return Err(
            "cleanup URL is outside the current run's isolated laminardb-tests prefix".into(),
        );
    }
    Ok(StoreConfig {
        url: cleanup_url,
        options: HashMap::new(),
        test_store: None,
    })
}

fn native_auth_source(provider: StorageProvider) -> Result<&'static str, String> {
    if let Some(source) = non_empty_env("LAMINAR_NATIVE_AUTH_SOURCE") {
        return match source.as_str() {
            "oidc-workload-identity" => Ok("oidc-workload-identity"),
            "web-identity" => Ok("web-identity"),
            "workload-identity" => Ok("workload-identity"),
            "azure-cli" => Ok("azure-cli"),
            "managed-identity-or-metadata" => Ok("managed-identity-or-metadata"),
            "application-default" => Ok("application-default"),
            "downstream-default" => Ok("downstream-default"),
            _ => Err(
                "LAMINAR_NATIVE_AUTH_SOURCE must name a non-secret ambient identity category"
                    .into(),
            ),
        };
    }
    Ok(match provider {
        StorageProvider::AwsS3 if non_empty_env("AWS_WEB_IDENTITY_TOKEN_FILE").is_some() => {
            "web-identity"
        }
        StorageProvider::AwsS3 => "downstream-default",
        StorageProvider::AzureAdls if non_empty_env("AZURE_FEDERATED_TOKEN_FILE").is_some() => {
            "workload-identity"
        }
        StorageProvider::AzureAdls => "managed-identity-or-metadata",
        StorageProvider::Gcs => "application-default",
        StorageProvider::Local => "anonymous-local",
    })
}

fn write_evidence(evidence: &CloudEvidence) -> Result<(), String> {
    let directory = non_empty_env("LAMINAR_CLOUD_EVIDENCE_DIR")
        .unwrap_or_else(|| "target/cloud-evidence".into());
    std::fs::create_dir_all(&directory)
        .map_err(|_| "cannot create cloud evidence directory".to_string())?;
    let filename = format!(
        "{}-{}-{}.json",
        safe_component(evidence.test_suite),
        evidence.provider,
        safe_component(&evidence.workflow_run_id)
    );
    let path = std::path::Path::new(&directory).join(filename);
    let bytes = serde_json::to_vec_pretty(evidence)
        .map_err(|_| "cannot serialize cloud evidence".to_string())?;
    std::fs::write(path, bytes).map_err(|_| "cannot write cloud evidence artifact".to_string())
}

fn locked_dependency_version(package: &str) -> Result<String, String> {
    let lockfile = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("Cargo.lock");
    let contents = std::fs::read_to_string(lockfile)
        .map_err(|_| "workspace Cargo.lock cannot be read".to_string())?;
    let expected_name = format!("name = \"{package}\"");
    let mut versions = contents
        .split("[[package]]")
        .filter(|entry| entry.lines().any(|line| line.trim() == expected_name))
        .filter_map(|entry| {
            entry.lines().find_map(|line| {
                line.trim()
                    .strip_prefix("version = \"")
                    .and_then(|value| value.strip_suffix('"'))
            })
        })
        .collect::<Vec<_>>();
    versions.sort_unstable();
    versions.dedup();
    match versions.as_slice() {
        [version] => Ok((*version).to_string()),
        [] => Err(format!("{package} is absent from workspace Cargo.lock")),
        _ => Err(format!(
            "{package} has multiple versions in workspace Cargo.lock"
        )),
    }
}

fn required_env(name: &str) -> Result<String, String> {
    non_empty_env(name).ok_or_else(|| format!("{name} is required"))
}

fn non_empty_env(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
}

fn env_truthy(name: &str) -> bool {
    non_empty_env(name).is_some_and(|value| matches!(value.as_str(), "1" | "true" | "TRUE"))
}

fn safe_component(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_') {
                character
            } else {
                '_'
            }
        })
        .take(80)
        .collect()
}

const fn provider_id(provider: StorageProvider) -> &'static str {
    match provider {
        StorageProvider::AwsS3 => "aws",
        StorageProvider::AzureAdls => "azure",
        StorageProvider::Gcs => "gcs",
        StorageProvider::Local => "local",
    }
}

const fn endpoint_class_id(classification: StorageEndpointClass) -> &'static str {
    match classification {
        StorageEndpointClass::Native => "native-provider-default",
        StorageEndpointClass::S3Compatible => "s3-compatible-custom-endpoint",
        StorageEndpointClass::CustomOrEmulator => "custom-or-emulator-endpoint",
        StorageEndpointClass::Local => "local-filesystem",
    }
}
