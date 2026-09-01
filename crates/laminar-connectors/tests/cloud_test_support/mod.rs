#![allow(dead_code)]

use std::time::Instant;

use laminar_connectors::storage::{
    StorageCredentialResolver, StorageEndpointClass, StorageLocation, StorageProvider,
};
use serde::Serialize;

#[allow(clippy::disallowed_types)]
type StorageOptions = std::collections::HashMap<String, String>;

pub struct NativeCloudContext {
    pub provider: StorageProvider,
    pub provider_id: &'static str,
    pub base_url: String,
    pub test_url: String,
    pub unique_prefix: String,
    pub run_id: String,
    pub base_sha: String,
    pub tested_sha: String,
    pub auth_source: String,
    pub started_at: chrono::DateTime<chrono::Utc>,
    pub started: Instant,
    pub suite: &'static str,
    pub test_name: &'static str,
}

impl std::fmt::Debug for NativeCloudContext {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NativeCloudContext")
            .field("provider", &self.provider)
            .field("base_url", &"<redacted-location>")
            .field("test_url", &"<redacted-location>")
            .field("unique_prefix", &"<isolated-prefix>")
            .field("run_id", &self.run_id)
            .field("base_sha", &self.base_sha)
            .field("tested_sha", &self.tested_sha)
            .field("auth_source", &self.auth_source)
            .field("suite", &self.suite)
            .field("test_name", &self.test_name)
            .finish()
    }
}

impl NativeCloudContext {
    pub fn load(
        suite: &'static str,
        test_name: &'static str,
        feature_enabled: bool,
    ) -> Result<Option<Self>, String> {
        let required = env_truthy("LAMINAR_NATIVE_CLOUD_REQUIRED");
        match Self::load_required(suite, test_name, feature_enabled) {
            Ok(context) => Ok(Some(context)),
            Err(reason) if !required => {
                eprintln!("native cloud integration not run: {reason}");
                Ok(None)
            }
            Err(reason) => Err(reason),
        }
    }

    fn load_required(
        suite: &'static str,
        test_name: &'static str,
        feature_enabled: bool,
    ) -> Result<Self, String> {
        if !env_truthy("LAMINAR_NATIVE_CLOUD") {
            return Err("LAMINAR_NATIVE_CLOUD=1 is required".into());
        }
        if !feature_enabled {
            return Err("the selected provider Cargo feature is not enabled".into());
        }
        reject_custom_endpoints()?;
        let provider_id = required_env("LAMINAR_NATIVE_CLOUD_PROVIDER")?;
        let (provider, location_env) = match provider_id.as_str() {
            "aws" => (StorageProvider::AwsS3, "LAMINAR_AWS_TEST_URL"),
            "azure" => (StorageProvider::AzureAdls, "LAMINAR_AZURE_TEST_URL"),
            "gcs" => (StorageProvider::Gcs, "LAMINAR_GCS_TEST_URL"),
            _ => return Err("LAMINAR_NATIVE_CLOUD_PROVIDER must be aws, azure, or gcs".into()),
        };
        if provider == StorageProvider::Gcs && suite.starts_with("delta-") {
            validate_pinned_gcs_adc()?;
        }
        let base_url = required_env(location_env)?;
        let location = StorageLocation::parse(&base_url)
            .map_err(|error| format!("{location_env} is invalid: {error}"))?;
        if location.provider != provider
            || location.endpoint_class() != StorageEndpointClass::Native
        {
            return Err(format!(
                "{location_env} does not select the required native provider"
            ));
        }
        let run_id = required_env("GITHUB_RUN_ID").or_else(|_| required_env("LAMINAR_RUN_ID"))?;
        let base_sha = required_env("LAMINAR_BASE_SHA")?;
        let tested_sha =
            required_env("GITHUB_SHA").or_else(|_| required_env("LAMINAR_TESTED_SHA"))?;
        let prefix = format!(
            "laminardb-tests/{}/{}/{suite}/{}/",
            safe_component(&base_sha.chars().take(12).collect::<String>()),
            safe_component(&run_id),
            uuid::Uuid::new_v4()
        );
        let test_url = format!("{}/{}", base_url.trim_end_matches('/'), prefix);
        let auth_source = native_auth_source(&test_url)?;
        Ok(Self {
            provider,
            provider_id: match provider {
                StorageProvider::AwsS3 => "aws",
                StorageProvider::AzureAdls => "azure",
                StorageProvider::Gcs => "gcs",
                StorageProvider::Local => unreachable!("native context excludes local storage"),
            },
            base_url,
            test_url,
            unique_prefix: prefix,
            run_id,
            base_sha,
            tested_sha,
            auth_source,
            started_at: chrono::Utc::now(),
            started: Instant::now(),
            suite,
            test_name,
        })
    }

    pub fn evidence(
        &self,
        dependencies: DependencyVersions,
        capabilities: serde_json::Value,
        outcome: EvidenceOutcome,
    ) -> NativeEvidence {
        let scheme = StorageLocation::parse(&self.base_url)
            .expect("native cloud URL was prevalidated")
            .original_scheme;
        NativeEvidence {
            schema_version: 1,
            repository: "laminardb/laminardb",
            base_sha: self.base_sha.clone(),
            tested_sha: self.tested_sha.clone(),
            workflow_run_id: self.run_id.clone(),
            provider: self.provider_id,
            native_or_emulator: "native",
            redacted_endpoint_classification: "native-provider-default",
            region_or_cloud_location: non_empty_env("LAMINAR_CLOUD_LOCATION"),
            url_scheme: scheme,
            enabled_cargo_features: non_empty_env("LAMINAR_ENABLED_CARGO_FEATURES")
                .map(|features| features.split(',').map(str::to_owned).collect())
                .unwrap_or_default(),
            object_store_version: "0.13.2",
            deltalake_version: dependencies.deltalake,
            iceberg_version: dependencies.iceberg,
            opendal_version: dependencies.opendal,
            auth_source: self.auth_source.clone(),
            test_suite: self.suite,
            test_name: self.test_name,
            started_at: self.started_at.to_rfc3339(),
            finished_at: chrono::Utc::now().to_rfc3339(),
            duration_ms: self.started.elapsed().as_millis(),
            iterations: outcome.iterations,
            process_kill_count: outcome.process_kill_count,
            recovery_bound_ms: outcome.recovery_bound_ms,
            capability_results: capabilities,
            conditional_create_result: outcome.conditional_create,
            stale_cas_result: outcome.stale_cas,
            restart_result: outcome.restart,
            delivery_contract_tested: outcome.delivery_contract,
            records_produced: outcome.records_produced,
            records_committed: outcome.records_committed,
            records_recovered: outcome.records_recovered,
            duplicates: outcome.duplicates,
            losses: outcome.losses,
            passed: outcome.passed,
            skip_count: 0,
            skip_reasons: Vec::new(),
            cleanup_result: outcome.cleanup_result,
            failure: outcome.failure,
        }
    }

    pub fn write_evidence(&self, evidence: &NativeEvidence) -> Result<(), String> {
        let directory = non_empty_env("LAMINAR_CLOUD_EVIDENCE_DIR")
            .unwrap_or_else(|| "target/cloud-evidence".into());
        std::fs::create_dir_all(&directory)
            .map_err(|_| "cannot create native evidence directory".to_string())?;
        let filename = format!(
            "{}-{}-{}.json",
            safe_component(self.suite),
            self.provider_id,
            safe_component(&self.run_id)
        );
        let bytes = serde_json::to_vec_pretty(evidence)
            .map_err(|_| "cannot serialize native evidence".to_string())?;
        std::fs::write(std::path::Path::new(&directory).join(filename), bytes)
            .map_err(|_| "cannot write native evidence artifact".to_string())
    }
}

pub struct EmulatorCloudContext {
    pub provider: StorageProvider,
    pub test_url: String,
    pub options: StorageOptions,
}

impl std::fmt::Debug for EmulatorCloudContext {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EmulatorCloudContext")
            .field("provider", &self.provider)
            .field("test_url", &"<redacted-location>")
            .field("options", &"<redacted-options>")
            .finish()
    }
}

impl EmulatorCloudContext {
    pub fn load(feature_enabled: bool) -> Result<Self, String> {
        if !env_truthy("LAMINAR_CLOUD_EMULATOR") {
            return Err("LAMINAR_CLOUD_EMULATOR=1 is required".into());
        }
        if env_truthy("LAMINAR_NATIVE_CLOUD") {
            return Err("native and emulator cloud markers are mutually exclusive".into());
        }
        if !feature_enabled {
            return Err("the selected provider Cargo feature is not enabled".into());
        }
        let provider = match required_env("LAMINAR_CLOUD_EMULATOR_PROVIDER")?.as_str() {
            "azure" => StorageProvider::AzureAdls,
            "gcs" => StorageProvider::Gcs,
            _ => return Err("LAMINAR_CLOUD_EMULATOR_PROVIDER must be azure or gcs".into()),
        };
        let base_url = required_env("LAMINAR_CLOUD_EMULATOR_TEST_URL")?;
        let location = StorageLocation::parse(&base_url)
            .map_err(|error| format!("LAMINAR_CLOUD_EMULATOR_TEST_URL is invalid: {error}"))?;
        if location.provider != provider
            || location.endpoint_class() != StorageEndpointClass::Native
        {
            return Err("emulator test URL must use the selected provider's direct scheme".into());
        }
        let endpoint =
            loopback_emulator_endpoint(&required_env("LAMINAR_CLOUD_EMULATOR_ENDPOINT")?)?;
        let run_id = required_env("GITHUB_RUN_ID").or_else(|_| required_env("LAMINAR_RUN_ID"))?;
        let base_sha = required_env("LAMINAR_BASE_SHA")?;
        let prefix = format!(
            "laminardb-tests/{}/{}/delta-emulator/{}/",
            safe_component(&base_sha.chars().take(12).collect::<String>()),
            safe_component(&run_id),
            uuid::Uuid::new_v4()
        );
        Ok(Self {
            provider,
            test_url: format!("{}/{}", base_url.trim_end_matches('/'), prefix),
            options: emulator_options(provider, &endpoint),
        })
    }
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

fn emulator_options(provider: StorageProvider, endpoint: &str) -> StorageOptions {
    const AZURITE_ACCOUNT: &str = "devstoreaccount1";
    const AZURITE_KEY: &str =
        "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

    match provider {
        StorageProvider::AzureAdls => StorageOptions::from([
            ("azure_storage_account_name".into(), AZURITE_ACCOUNT.into()),
            ("azure_storage_account_key".into(), AZURITE_KEY.into()),
            ("azure_storage_endpoint".into(), endpoint.into()),
            ("azure_allow_http".into(), "true".into()),
        ]),
        StorageProvider::Gcs => StorageOptions::from([
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
        StorageProvider::AwsS3 | StorageProvider::Local => StorageOptions::new(),
    }
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
            "pinned object_store 0.13.2 cannot load external_account GCS credentials; native Delta certification requires an upstream refreshable WIF implementation"
                .into(),
        ),
        Some(_) => Err(
            "GOOGLE_APPLICATION_CREDENTIALS uses a credential type unsupported by pinned object_store 0.13.2"
                .into(),
        ),
        None => Err("GOOGLE_APPLICATION_CREDENTIALS has no credential type".into()),
    }
}

pub struct DependencyVersions {
    pub deltalake: Option<&'static str>,
    pub iceberg: Option<&'static str>,
    pub opendal: Option<&'static str>,
}

pub struct EvidenceOutcome {
    pub iterations: u64,
    pub process_kill_count: u64,
    pub recovery_bound_ms: u64,
    pub conditional_create: Option<bool>,
    pub stale_cas: Option<bool>,
    pub restart: bool,
    pub delivery_contract: &'static str,
    pub records_produced: u64,
    pub records_committed: u64,
    pub records_recovered: u64,
    pub duplicates: Option<u64>,
    pub losses: Option<u64>,
    pub passed: bool,
    pub cleanup_result: String,
    pub failure: Option<&'static str>,
}

#[derive(Serialize)]
pub struct NativeEvidence {
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
    object_store_version: &'static str,
    deltalake_version: Option<&'static str>,
    iceberg_version: Option<&'static str>,
    opendal_version: Option<&'static str>,
    auth_source: String,
    test_suite: &'static str,
    test_name: &'static str,
    started_at: String,
    finished_at: String,
    duration_ms: u128,
    iterations: u64,
    process_kill_count: u64,
    recovery_bound_ms: u64,
    capability_results: serde_json::Value,
    conditional_create_result: Option<bool>,
    stale_cas_result: Option<bool>,
    restart_result: bool,
    delivery_contract_tested: &'static str,
    records_produced: u64,
    records_committed: u64,
    records_recovered: u64,
    duplicates: Option<u64>,
    losses: Option<u64>,
    passed: bool,
    skip_count: u64,
    skip_reasons: Vec<String>,
    cleanup_result: String,
    failure: Option<&'static str>,
}

fn reject_custom_endpoints() -> Result<(), String> {
    for name in [
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
        if non_empty_env(name).is_some() {
            return Err(format!("{name} must be unset in native mode"));
        }
    }
    Ok(())
}

fn native_auth_source(test_url: &str) -> Result<String, String> {
    let source = non_empty_env("LAMINAR_NATIVE_AUTH_SOURCE").unwrap_or_else(|| {
        StorageCredentialResolver::resolve(test_url, &Default::default())
            .auth_source
            .to_string()
    });
    if matches!(
        source.as_str(),
        "oidc-workload-identity"
            | "web-identity"
            | "workload-identity"
            | "azure-cli"
            | "managed-identity-or-metadata"
            | "application-default"
            | "downstream-default"
    ) {
        return Ok(source);
    }
    Err("LAMINAR_NATIVE_AUTH_SOURCE must name a non-secret ambient identity category".into())
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
