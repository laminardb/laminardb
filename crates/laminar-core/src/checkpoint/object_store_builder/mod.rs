//! Factory for building `ObjectStore` instances from URL schemes.
//!
//! Provider selection, credentials, retries, and client construction are
//! delegated to `object_store`. Explicit options override environment values.

#![allow(clippy::disallowed_types)] // cold path: object store setup

use std::collections::HashMap;
#[cfg(any(feature = "aws", feature = "gcs", feature = "azure"))]
use std::hash::Hash;
use std::path::PathBuf;
#[cfg(any(feature = "aws", feature = "gcs", feature = "azure"))]
use std::str::FromStr;
use std::sync::Arc;

use crate::storage_auth::{classify_storage_auth_source, AuthSource};
use crate::storage_location::{StorageConsumer, StorageLocation, StorageProvider};
use object_store::ObjectStore;

/// Errors from object store construction.
#[derive(Debug, thiserror::Error)]
pub enum ObjectStoreBuilderError {
    /// The URL could not be parsed.
    #[error("invalid object store URL: {0}")]
    InvalidUrl(String),

    /// Backend construction failed.
    #[error("object store build error: {0}")]
    Build(String),
}

impl From<object_store::Error> for ObjectStoreBuilderError {
    fn from(e: object_store::Error) -> Self {
        Self::Build(e.to_string())
    }
}

/// Failure domain of a checkpoint object-store URL.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum CheckpointStorageScope {
    /// No durable checkpoint namespace can be proven from the URL.
    Volatile,
    /// A local filesystem namespace survives process restart on one node.
    NodeDurable,
    /// A remote object-store namespace is shared by cluster participants.
    ClusterShared,
}

impl CheckpointStorageScope {
    /// Classify only URL schemes accepted by the object-store builder.
    #[must_use]
    pub fn for_url(url: &str) -> Self {
        let Ok(location) = StorageLocation::parse(url) else {
            return Self::Volatile;
        };
        match location.provider {
            StorageProvider::Local => Self::NodeDurable,
            StorageProvider::AwsS3 | StorageProvider::AzureAdls | StorageProvider::Gcs => {
                Self::ClusterShared
            }
        }
    }

    /// Whether this failure domain is at least as durable as the requirement.
    #[must_use]
    pub const fn satisfies(self, required: Self) -> bool {
        self as u8 >= required as u8
    }
}

/// Build an [`ObjectStore`] from a URL and optional configuration overrides.
///
/// # Supported schemes
///
/// | Scheme | Feature |
/// |--------|---------|
/// | `file://` | (always) |
/// | `s3://`, `s3a://` | `aws` |
/// | `gs://`, `gcs://` | `gcs` |
/// | `az://`, `abfs://`, `abfss://`, `wasb://`, `wasbs://` | `azure` |
///
/// The URL path is applied as a key prefix on the returned store. R2 and
/// `MinIO` use the S3 scheme with the endpoint option supported by
/// `object_store`.
///
/// # Errors
///
/// Returns [`ObjectStoreBuilderError`] if the scheme is unsupported, requires
/// an uncompiled feature, or the backend fails to build.
#[allow(clippy::implicit_hasher)]
pub fn build_object_store(
    url: &str,
    options: &HashMap<String, String>,
) -> Result<Arc<dyn ObjectStore>, ObjectStoreBuilderError> {
    let mut location = StorageLocation::parse(url)
        .map_err(|error| ObjectStoreBuilderError::InvalidUrl(error.to_string()))?;
    if location.provider == StorageProvider::Local {
        return build_local_file_system(url);
    }
    let mut environment = provider_environment(location.provider);
    if let Some(endpoint) = configured_endpoint(location.provider, options, &environment) {
        location = location
            .with_endpoint_override(endpoint)
            .map_err(|error| ObjectStoreBuilderError::Build(error.to_string()))?;
    }
    let adapted = location
        .adapt(StorageConsumer::ObjectStore)
        .map_err(|error| ObjectStoreBuilderError::InvalidUrl(error.to_string()))?;
    let parsed = url::Url::parse(&adapted.url)
        .map_err(|error| ObjectStoreBuilderError::InvalidUrl(error.to_string()))?;
    #[cfg(any(feature = "aws", feature = "gcs", feature = "azure"))]
    validate_explicit_options(location.provider, options)?;
    #[cfg(feature = "azure")]
    validate_url_derived_azure_options(options, &adapted.derived_options)?;

    environment.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
    let auth_source = checkpoint_auth_source(location.provider, options);
    let endpoint_transport = location
        .endpoint_override
        .as_ref()
        .map_or("default", |endpoint| endpoint.scheme());
    tracing::debug!(
        operation_class = "client-build",
        provider = location.provider.name(),
        endpoint_class = location.endpoint_class().name(),
        endpoint_transport,
        allow_http = configured_allow_http(location.provider, options, &environment),
        auth_source = %auth_source,
        "building checkpoint object-store client"
    );

    // `parse_url_opts` applies options in iteration order. Keep explicit settings after the
    // environment so aliases that resolve to the same provider key have deterministic precedence.
    let resolved = environment
        .into_iter()
        .chain(
            options
                .iter()
                .map(|(key, value)| (key.clone(), value.clone())),
        )
        .chain(adapted.derived_options);
    let (store, prefix) = object_store::parse_url_opts(&parsed, resolved).map_err(|_| {
        ObjectStoreBuilderError::Build(format!(
            "failed to construct the {} client; verify the configured option values and downstream credential chain",
            location.provider.name()
        ))
    })?;
    let store: Arc<dyn ObjectStore> = Arc::from(store);
    if prefix.as_ref().is_empty() {
        Ok(store)
    } else {
        Ok(Arc::new(object_store::prefix::PrefixStore::new(
            store, prefix,
        )))
    }
}

/// Open the crash-durable local object store used by checkpoint data and metadata.
///
/// # Errors
///
/// Returns an object-store error when the root cannot be created, synchronized, or opened.
pub fn durable_local_object_store(
    root: impl AsRef<std::path::Path>,
) -> object_store::Result<Arc<dyn ObjectStore>> {
    Ok(Arc::new(
        crate::durable_local_store::DurableLocalObjectStore::new(root)?,
    ))
}

fn provider_environment_key(provider: StorageProvider, key: &str) -> bool {
    match provider {
        StorageProvider::AwsS3 => key.starts_with("AWS_"),
        StorageProvider::Gcs => key.starts_with("GOOGLE_") || key == "SERVICE_ACCOUNT",
        StorageProvider::AzureAdls => key.starts_with("AZURE_") || key == "IDENTITY_ENDPOINT",
        StorageProvider::Local => false,
    }
}

fn provider_environment(provider: StorageProvider) -> Vec<(String, String)> {
    std::env::vars_os()
        .filter_map(|(key, value)| Some((key.into_string().ok()?, value.into_string().ok()?)))
        .filter(|(key, _)| provider_environment_key(provider, key))
        .collect()
}

fn checkpoint_auth_source(
    provider: StorageProvider,
    options: &HashMap<String, String>,
) -> AuthSource {
    let classified =
        classify_storage_auth_source(provider, options, &|name| std::env::var(name).ok());
    if provider == StorageProvider::AwsS3 && classified == AuthSource::Profile {
        // COMPAT: object_store 0.13 does not load the AWS shared profile files. Delta's AWS
        // client does, so the shared classifier retains Profile for connector diagnostics.
        AuthSource::Unknown
    } else {
        classified
    }
}

fn configured_endpoint<'a>(
    provider: StorageProvider,
    options: &'a HashMap<String, String>,
    environment: &'a [(String, String)],
) -> Option<&'a str> {
    let (option_keys, environment_keys): (&[&str], &[&str]) = match provider {
        StorageProvider::AwsS3 => (
            &[
                "aws_endpoint_url_s3",
                "aws_endpoint",
                "aws_endpoint_url",
                "endpoint",
                "endpoint_url",
            ],
            &["AWS_ENDPOINT_URL_S3", "AWS_ENDPOINT_URL", "AWS_ENDPOINT"],
        ),
        StorageProvider::AzureAdls => (
            &["azure_storage_endpoint", "azure_endpoint", "endpoint"],
            &["AZURE_STORAGE_ENDPOINT"],
        ),
        StorageProvider::Gcs => (
            &["google_base_url", "base_url"],
            &["GOOGLE_BASE_URL", "GOOGLE_ENDPOINT_URL"],
        ),
        StorageProvider::Local => (&[], &[]),
    };
    configured_option(options, option_keys)
        .or_else(|| configured_environment(environment, environment_keys))
}

fn configured_allow_http(
    provider: StorageProvider,
    options: &HashMap<String, String>,
    environment: &[(String, String)],
) -> bool {
    let (option_keys, environment_keys): (&[&str], &[&str]) = match provider {
        StorageProvider::AwsS3 => (&["aws_allow_http", "allow_http"], &["AWS_ALLOW_HTTP"]),
        StorageProvider::AzureAdls => (&["azure_allow_http", "allow_http"], &["AZURE_ALLOW_HTTP"]),
        StorageProvider::Gcs => (&["google_allow_http", "allow_http"], &["GOOGLE_ALLOW_HTTP"]),
        StorageProvider::Local => (&[], &[]),
    };
    configured_option(options, option_keys)
        .or_else(|| configured_environment(environment, environment_keys))
        .is_some_and(|value| value.trim().eq_ignore_ascii_case("true"))
}

fn configured_option<'a>(options: &'a HashMap<String, String>, keys: &[&str]) -> Option<&'a str> {
    keys.iter().find_map(|key| {
        options.iter().find_map(|(candidate, value)| {
            (candidate.eq_ignore_ascii_case(key) && !value.trim().is_empty())
                .then_some(value.as_str())
        })
    })
}

fn configured_environment<'a>(
    environment: &'a [(String, String)],
    keys: &[&str],
) -> Option<&'a str> {
    keys.iter().find_map(|key| {
        environment.iter().find_map(|(candidate, value)| {
            (candidate == key && !value.trim().is_empty()).then_some(value.as_str())
        })
    })
}

#[cfg(any(feature = "aws", feature = "gcs", feature = "azure"))]
fn validate_explicit_options(
    provider: StorageProvider,
    options: &HashMap<String, String>,
) -> Result<(), ObjectStoreBuilderError> {
    match provider {
        #[cfg(feature = "aws")]
        StorageProvider::AwsS3 => {
            validate_typed_options::<object_store::aws::AmazonS3ConfigKey>("S3", options)
        }
        #[cfg(feature = "gcs")]
        StorageProvider::Gcs => {
            validate_typed_options::<object_store::gcp::GoogleConfigKey>("GCS", options)
        }
        #[cfg(feature = "azure")]
        StorageProvider::AzureAdls => {
            validate_typed_options::<object_store::azure::AzureConfigKey>("Azure", options)
        }
        _ => Ok(()),
    }
}

#[cfg(any(feature = "aws", feature = "gcs", feature = "azure"))]
fn validate_typed_options<K>(
    provider: &str,
    options: &HashMap<String, String>,
) -> Result<(), ObjectStoreBuilderError>
where
    K: FromStr + Eq + Hash,
    K::Err: std::fmt::Display,
{
    let mut canonical = HashMap::<K, &str>::with_capacity(options.len());
    for key in options.keys() {
        let parsed = key.to_ascii_lowercase().parse::<K>().map_err(|error| {
            ObjectStoreBuilderError::Build(format!(
                "invalid {provider} storage option '{key}': {error}"
            ))
        })?;
        if let Some(previous) = canonical.insert(parsed, key) {
            return Err(ObjectStoreBuilderError::Build(format!(
                "{provider} storage options '{previous}' and '{key}' configure the same setting"
            )));
        }
    }
    Ok(())
}

#[cfg(feature = "azure")]
fn validate_url_derived_azure_options(
    explicit: &HashMap<String, String>,
    derived: &[(String, String)],
) -> Result<(), ObjectStoreBuilderError> {
    use object_store::azure::AzureConfigKey;

    for (derived_key, derived_value) in derived {
        let canonical = derived_key
            .parse::<AzureConfigKey>()
            .map_err(|error| ObjectStoreBuilderError::Build(error.to_string()))?;
        for (explicit_key, explicit_value) in explicit {
            let explicit_canonical = explicit_key
                .to_ascii_lowercase()
                .parse::<AzureConfigKey>()
                .map_err(|error| ObjectStoreBuilderError::Build(error.to_string()))?;
            if explicit_canonical == canonical && explicit_value != derived_value {
                return Err(ObjectStoreBuilderError::Build(format!(
                    "Azure storage option '{explicit_key}' conflicts with the URL authority"
                )));
            }
        }
    }
    Ok(())
}

/// Absolute local filesystem path from a canonical lowercase `file://` URL.
///
/// # Errors
/// Returns [`ObjectStoreBuilderError::InvalidUrl`] for a non-file scheme,
/// remote authority, relative path, query, fragment, or invalid encoding.
pub fn file_url_path(url: &str) -> Result<PathBuf, ObjectStoreBuilderError> {
    let location = StorageLocation::parse(url)
        .map_err(|error| ObjectStoreBuilderError::InvalidUrl(error.to_string()))?;
    if location.provider != StorageProvider::Local {
        return Err(ObjectStoreBuilderError::InvalidUrl(
            "expected a file URL".to_string(),
        ));
    }
    let adapted = location
        .adapt(StorageConsumer::ObjectStore)
        .map_err(|error| ObjectStoreBuilderError::InvalidUrl(error.to_string()))?;
    let parsed = url::Url::parse(&adapted.url)
        .map_err(|error| ObjectStoreBuilderError::InvalidUrl(error.to_string()))?;
    let path = parsed.to_file_path().map_err(|()| {
        ObjectStoreBuilderError::InvalidUrl("file URL is not a local filesystem path".to_string())
    })?;
    if !path.is_absolute() {
        return Err(ObjectStoreBuilderError::InvalidUrl(
            "file URL path must be absolute".to_string(),
        ));
    }
    Ok(path)
}

/// Whether a file URL syntactically names an absolute local namespace.
///
/// This failure-domain classification is deliberately independent of the current host's path
/// syntax. Actual store construction still calls [`file_url_path`] and rejects paths the current
/// operating system cannot represent before any runtime starts.
#[must_use]
pub fn is_absolute_local_file_url(url: &str) -> bool {
    StorageLocation::parse(url).is_ok_and(|location| location.provider == StorageProvider::Local)
}

/// Extract the local path from a `file://` URL and create a crash-durable local store.
fn build_local_file_system(url: &str) -> Result<Arc<dyn ObjectStore>, ObjectStoreBuilderError> {
    let path = file_url_path(url)?;
    durable_local_object_store(path).map_err(Into::into)
}

#[cfg(test)]
mod tests;
