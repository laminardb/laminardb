//! Fail-closed coordinated-storage configuration and log-store certification.

use super::{
    ConnectorError, DeltaTable, HashMap, StorageProvider, COORDINATED_CONNECT_TIMEOUT,
    COORDINATED_HTTP_MAX_RETRIES, COORDINATED_MAX_BACKOFF, COORDINATED_REQUEST_TIMEOUT,
    COORDINATED_RETRY_TIMEOUT,
};

#[cfg(feature = "delta-lake")]
pub(in crate::lakehouse) fn bound_coordinated_storage_options(
    mut options: HashMap<String, String>,
) -> HashMap<String, String> {
    options.retain(|key, _| {
        !matches!(
            key.to_ascii_lowercase().as_str(),
            "timeout"
                | "aws_timeout"
                | "azure_timeout"
                | "google_timeout"
                | "connect_timeout"
                | "aws_connect_timeout"
                | "azure_connect_timeout"
                | "google_connect_timeout"
                | "max_retries"
                | "retry_timeout"
                | "max_backoff"
                | "backoff.max_backoff"
                | "backoff_config.max_backoff"
        )
    });
    options.insert("timeout".into(), COORDINATED_REQUEST_TIMEOUT.into());
    options.insert("connect_timeout".into(), COORDINATED_CONNECT_TIMEOUT.into());
    options.insert("retry_timeout".into(), COORDINATED_RETRY_TIMEOUT.into());
    options.insert("max_retries".into(), COORDINATED_HTTP_MAX_RETRIES.into());
    options.insert("max_backoff".into(), COORDINATED_MAX_BACKOFF.into());
    options
}

#[cfg(feature = "delta-lake")]
fn effective_values<F>(
    options: &HashMap<String, String>,
    aliases: &[&str],
    environment_keys: &[&str],
    environment: &F,
) -> Vec<String>
where
    F: Fn(&str) -> Option<String>,
{
    let explicit: Vec<String> = options
        .iter()
        .filter(|(key, _)| aliases.iter().any(|alias| key.eq_ignore_ascii_case(alias)))
        .map(|(_, value)| value.clone())
        .collect();
    if !explicit.is_empty() {
        return explicit;
    }
    environment_keys
        .iter()
        .filter_map(|key| environment(key))
        .collect()
}

#[cfg(feature = "delta-lake")]
fn has_effective_value<F>(
    options: &HashMap<String, String>,
    aliases: &[&str],
    environment_keys: &[&str],
    environment: &F,
) -> bool
where
    F: Fn(&str) -> Option<String>,
{
    effective_values(options, aliases, environment_keys, environment)
        .iter()
        .any(|value| !value.trim().is_empty())
}

#[cfg(feature = "delta-lake")]
fn is_truthy(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "y" | "on"
    )
}

#[cfg(feature = "delta-lake")]
fn validate_coordinated_s3_options<F>(
    options: &HashMap<String, String>,
    environment: &F,
) -> Result<(), ConnectorError>
where
    F: Fn(&str) -> Option<String>,
{
    let custom_endpoint = has_effective_value(
        options,
        &[
            "endpoint",
            "endpoint_url",
            "aws_endpoint",
            "aws_endpoint_url",
        ],
        &["AWS_ENDPOINT", "AWS_ENDPOINT_URL"],
        environment,
    );
    let soak_emulator = cfg!(debug_assertions)
        && environment("LAMINAR_SOAK_ALLOW_S3_EMULATOR")
            .as_deref()
            .is_some_and(is_truthy);
    if custom_endpoint && !soak_emulator {
        return Err(ConnectorError::ConfigurationError(
            "Delta exactly-once does not admit custom S3 endpoints until their atomic-create behavior passes the release fault suite"
                .into(),
        ));
    }
    let conditional_put = effective_values(
        options,
        &["conditional_put", "aws_conditional_put"],
        &["AWS_CONDITIONAL_PUT"],
        environment,
    );
    if conditional_put
        .iter()
        .any(|value| !value.trim().eq_ignore_ascii_case("etag"))
    {
        return Err(ConnectorError::ConfigurationError(
            "Delta exactly-once requires native S3 ETag conditional put; Dynamo and disabled conditional-put modes are not certified"
                .into(),
        ));
    }
    if has_effective_value(
        options,
        &["s3_locking_provider", "aws_s3_locking_provider"],
        &["AWS_S3_LOCKING_PROVIDER"],
        environment,
    ) || effective_values(
        options,
        &["allow_unsafe_rename", "aws_s3_allow_unsafe_rename"],
        &["AWS_S3_ALLOW_UNSAFE_RENAME"],
        environment,
    )
    .iter()
    .any(|value| is_truthy(value))
    {
        return Err(ConnectorError::ConfigurationError(
            "Delta exactly-once requires the native conditional-put log store; locking-provider and unsafe-rename modes are not certified"
                .into(),
        ));
    }
    Ok(())
}

#[cfg(feature = "delta-lake")]
fn validate_coordinated_azure_options<F>(
    options: &HashMap<String, String>,
    environment: &F,
) -> Result<(), ConnectorError>
where
    F: Fn(&str) -> Option<String>,
{
    if has_effective_value(
        options,
        &["endpoint", "azure_endpoint", "azure_storage_endpoint"],
        &["AZURE_ENDPOINT", "AZURE_STORAGE_ENDPOINT"],
        environment,
    ) || effective_values(
        options,
        &[
            "use_emulator",
            "azure_use_emulator",
            "azure_storage_use_emulator",
        ],
        &["AZURE_USE_EMULATOR", "AZURE_STORAGE_USE_EMULATOR"],
        environment,
    )
    .iter()
    .any(|value| is_truthy(value))
    {
        return Err(ConnectorError::ConfigurationError(
            "Delta exactly-once does not admit custom Azure endpoints or emulators until their atomic-create behavior passes the release fault suite"
                .into(),
        ));
    }
    Ok(())
}

#[cfg(feature = "delta-lake")]
fn validate_coordinated_gcs_options<F>(
    options: &HashMap<String, String>,
    environment: &F,
) -> Result<(), ConnectorError>
where
    F: Fn(&str) -> Option<String>,
{
    if has_effective_value(
        options,
        &[
            "google_service_account",
            "google_service_account_path",
            "service_account",
            "service_account_path",
        ],
        &["GOOGLE_SERVICE_ACCOUNT", "GOOGLE_SERVICE_ACCOUNT_PATH"],
        environment,
    ) {
        return Err(ConnectorError::ConfigurationError(
            "Delta exactly-once does not admit GCS service-account path files because they can override the storage endpoint; use workload identity, application-default credentials, or an inline key"
                .into(),
        ));
    }
    for key in effective_values(
        options,
        &["google_service_account_key", "service_account_key"],
        &["GOOGLE_SERVICE_ACCOUNT_KEY"],
        environment,
    ) {
        let document: serde_json::Value = serde_json::from_str(&key).map_err(|error| {
            ConnectorError::ConfigurationError(format!(
                "invalid GCS service-account key for Delta exactly-once: {error}"
            ))
        })?;
        if document.get("gcs_base_url").is_some() {
            return Err(ConnectorError::ConfigurationError(
                "Delta exactly-once does not admit a custom gcs_base_url until its atomic-create behavior passes the release fault suite"
                    .into(),
            ));
        }
    }
    Ok(())
}

#[cfg(feature = "delta-lake")]
pub(super) fn validate_coordinated_storage_preflight_with_env<F>(
    table_path: &str,
    options: &HashMap<String, String>,
    environment: &F,
) -> Result<(), ConnectorError>
where
    F: Fn(&str) -> Option<String>,
{
    match StorageProvider::detect_uri(table_path) {
        Some(StorageProvider::AwsS3) => validate_coordinated_s3_options(options, environment),
        Some(StorageProvider::AzureAdls) => {
            validate_coordinated_azure_options(options, environment)
        }
        Some(StorageProvider::Gcs) => validate_coordinated_gcs_options(options, environment),
        Some(StorageProvider::Local) => Ok(()),
        None => match table_path.split_once("://") {
            None => Ok(()),
            Some((scheme, _)) if scheme.eq_ignore_ascii_case("uc") => Ok(()),
            Some((scheme, _)) => Err(ConnectorError::ConfigurationError(format!(
                "Delta exactly-once does not admit unknown storage URI scheme '{scheme}'"
            ))),
        },
    }
}

#[cfg(feature = "delta-lake")]
pub(in crate::lakehouse) fn validate_coordinated_storage_preflight(
    table_path: &str,
    options: &HashMap<String, String>,
) -> Result<(), ConnectorError> {
    validate_coordinated_storage_preflight_with_env(table_path, options, &|key| {
        std::env::var(key).ok()
    })
}

#[cfg(feature = "delta-lake")]
pub(super) fn is_certified_coordinated_log_store(name: &str) -> bool {
    name == "DefaultLogStore"
}

#[cfg(feature = "delta-lake")]
pub(super) fn validate_coordinated_log_store(table: &DeltaTable) -> Result<(), ConnectorError> {
    let log_store = table.log_store();
    if !is_certified_coordinated_log_store(&log_store.name()) {
        return Err(ConnectorError::ConfigurationError(format!(
            "Delta exactly-once requires the single-step atomic-create DefaultLogStore; '{}' is not certified",
            log_store.name()
        )));
    }
    validate_coordinated_storage_preflight(
        log_store.config().location().as_str(),
        &log_store.config().options().raw,
    )
}
