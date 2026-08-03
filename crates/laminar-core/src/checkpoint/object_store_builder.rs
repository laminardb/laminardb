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
        if is_absolute_local_file_url(url) {
            return Self::NodeDurable;
        }
        let Ok(parsed) = url::Url::parse(url) else {
            return Self::Volatile;
        };
        match parsed.scheme() {
            "s3" | "s3a" | "gs" | "az" | "abfs" | "abfss" => Self::ClusterShared,
            _ => Self::Volatile,
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
/// | `gs://` | `gcs` |
/// | `az://`, `abfs://`, `abfss://` | `azure` |
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
    let parsed = url::Url::parse(url)
        .map_err(|error| ObjectStoreBuilderError::InvalidUrl(error.to_string()))?;
    match parsed.scheme() {
        "file" => return build_local_file_system(url),
        "s3" | "s3a" | "gs" | "az" | "abfs" | "abfss" => {}
        scheme => {
            return Err(ObjectStoreBuilderError::InvalidUrl(format!(
                "unsupported scheme '{scheme}'"
            )));
        }
    }
    #[cfg(any(feature = "aws", feature = "gcs", feature = "azure"))]
    validate_explicit_options(parsed.scheme(), options)?;

    // `parse_url_opts` applies options in iteration order. Keep explicit settings after the
    // environment so aliases that resolve to the same provider key have deterministic precedence.
    let environment = std::env::vars_os()
        .filter_map(|(key, value)| Some((key.into_string().ok()?, value.into_string().ok()?)));
    let mut environment = environment
        .filter(|(key, _)| provider_environment_key(parsed.scheme(), key))
        .collect::<Vec<_>>();
    environment.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
    let resolved = environment.into_iter().chain(
        options
            .iter()
            .map(|(key, value)| (key.clone(), value.clone())),
    );
    let (store, prefix) = object_store::parse_url_opts(&parsed, resolved)
        .map_err(|error| ObjectStoreBuilderError::Build(error.to_string()))?;
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

fn provider_environment_key(scheme: &str, key: &str) -> bool {
    match scheme {
        "s3" | "s3a" => key.starts_with("AWS_"),
        "gs" => key.starts_with("GOOGLE_") || key == "SERVICE_ACCOUNT",
        "az" | "abfs" | "abfss" => key.starts_with("AZURE_") || key == "IDENTITY_ENDPOINT",
        _ => false,
    }
}

#[cfg(any(feature = "aws", feature = "gcs", feature = "azure"))]
fn validate_explicit_options(
    scheme: &str,
    options: &HashMap<String, String>,
) -> Result<(), ObjectStoreBuilderError> {
    match scheme {
        #[cfg(feature = "aws")]
        "s3" | "s3a" => {
            validate_typed_options::<object_store::aws::AmazonS3ConfigKey>("S3", options)
        }
        #[cfg(feature = "gcs")]
        "gs" => validate_typed_options::<object_store::gcp::GoogleConfigKey>("GCS", options),
        #[cfg(feature = "azure")]
        "az" | "abfs" | "abfss" => {
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

/// Absolute local filesystem path from a canonical lowercase `file://` URL.
///
/// # Errors
/// Returns [`ObjectStoreBuilderError::InvalidUrl`] for a non-file scheme,
/// remote authority, relative path, query, fragment, or invalid encoding.
pub fn file_url_path(url: &str) -> Result<PathBuf, ObjectStoreBuilderError> {
    let parsed = parse_absolute_local_file_url(url)?;
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
    parse_absolute_local_file_url(url).is_ok()
}

fn parse_absolute_local_file_url(url: &str) -> Result<url::Url, ObjectStoreBuilderError> {
    if !url.starts_with("file://") {
        return Err(ObjectStoreBuilderError::InvalidUrl(
            "file URL scheme must be lowercase 'file://'".to_string(),
        ));
    }
    if url == "file://" {
        return Err(ObjectStoreBuilderError::InvalidUrl(
            "file URL path must not be empty".to_string(),
        ));
    }
    let parsed = url::Url::parse(url)
        .map_err(|error| ObjectStoreBuilderError::InvalidUrl(error.to_string()))?;
    if parsed.scheme() != "file" {
        return Err(ObjectStoreBuilderError::InvalidUrl(format!(
            "expected file URL, found scheme '{}'",
            parsed.scheme()
        )));
    }
    if parsed.query().is_some() || parsed.fragment().is_some() {
        return Err(ObjectStoreBuilderError::InvalidUrl(
            "file URL must not contain a query or fragment".to_string(),
        ));
    }
    if parsed
        .host_str()
        .is_some_and(|host| !host.eq_ignore_ascii_case("localhost"))
    {
        return Err(ObjectStoreBuilderError::InvalidUrl(
            "file URL must not name a remote host".to_string(),
        ));
    }
    if !parsed.path().starts_with('/') {
        return Err(ObjectStoreBuilderError::InvalidUrl(
            "file URL path must be absolute".to_string(),
        ));
    }
    Ok(parsed)
}

/// Extract the local path from a `file://` URL and create a crash-durable local store.
fn build_local_file_system(url: &str) -> Result<Arc<dyn ObjectStore>, ObjectStoreBuilderError> {
    let path = file_url_path(url)?;
    durable_local_object_store(path).map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_file_scheme_creates_durable_local_store() {
        let dir = tempfile::tempdir().unwrap();
        let url = url::Url::from_directory_path(dir.path()).unwrap();
        let store = build_object_store(url.as_str(), &HashMap::new()).unwrap();
        assert!(store.to_string().starts_with("DurableLocalObjectStore("));
    }

    #[test]
    fn test_file_scheme_empty_path_errors() {
        let result = build_object_store("file://", &HashMap::new());
        assert!(result.is_err());
    }

    #[test]
    fn file_url_requires_an_absolute_local_path() {
        assert!(file_url_path("file://checkpoint-host/path").is_err());
        assert!(file_url_path("file://./relative").is_err());
        assert!(file_url_path("FILE:///tmp/path").is_err());
        assert!(file_url_path("file:///tmp/path?version=1").is_err());
        assert!(file_url_path("file:///tmp/path#fragment").is_err());
    }

    #[test]
    fn absolute_local_file_url_classification_is_platform_independent() {
        assert!(is_absolute_local_file_url("file:///tmp/checkpoints"));

        let directory = tempfile::tempdir().unwrap();
        let local_url = url::Url::from_directory_path(directory.path()).unwrap();
        assert!(is_absolute_local_file_url(local_url.as_str()));

        assert!(!is_absolute_local_file_url("file://checkpoint-host/path"));
        assert!(!is_absolute_local_file_url("file://./relative"));
        assert!(!is_absolute_local_file_url("FILE:///tmp/path"));
        assert!(!is_absolute_local_file_url("file:///tmp/path?version=1"));
        assert!(!is_absolute_local_file_url("file:///tmp/path#fragment"));
    }

    #[test]
    fn checkpoint_storage_scope_matches_supported_schemes() {
        assert_eq!(
            CheckpointStorageScope::for_url("file:///tmp/checkpoints"),
            CheckpointStorageScope::NodeDurable
        );
        for url in [
            "s3://bucket/prefix",
            "s3a://bucket/prefix",
            "gs://bucket/prefix",
            "az://container/prefix",
            "abfs://container/prefix",
            "abfss://container/prefix",
        ] {
            assert_eq!(
                CheckpointStorageScope::for_url(url),
                CheckpointStorageScope::ClusterShared
            );
        }
        for url in ["memory://", "file://relative", "gcs://bucket", "ftp://host"] {
            assert_eq!(
                CheckpointStorageScope::for_url(url),
                CheckpointStorageScope::Volatile
            );
        }
    }

    #[test]
    fn file_url_decodes_escaped_path_segments() {
        let directory = tempfile::tempdir().unwrap();
        let expected = directory.path().join("laminar checkpoint");
        let encoded = url::Url::from_directory_path(&expected).unwrap();
        assert!(encoded.as_str().contains("laminar%20checkpoint"));
        assert_eq!(file_url_path(encoded.as_str()).unwrap(), expected);
    }

    #[test]
    fn test_unknown_scheme_errors() {
        let result = build_object_store("ftp://bucket/prefix", &HashMap::new());
        assert!(result.is_err());
    }

    #[test]
    fn test_no_scheme_errors() {
        let result = build_object_store("/just/a/path", &HashMap::new());
        assert!(result.is_err());
    }

    #[test]
    fn ambient_options_are_scoped_to_the_selected_provider() {
        assert!(provider_environment_key("s3", "AWS_REGION"));
        assert!(!provider_environment_key("s3", "REGION"));
        assert!(!provider_environment_key("s3", "AZURE_STORAGE_TOKEN"));
        assert!(provider_environment_key("gs", "SERVICE_ACCOUNT"));
        assert!(provider_environment_key("az", "IDENTITY_ENDPOINT"));
        assert!(!provider_environment_key("az", "ENDPOINT"));
    }

    #[cfg(feature = "aws")]
    #[test]
    fn explicit_provider_options_fail_closed() {
        let mut options = HashMap::from([("aws_regoin".to_string(), "us-east-1".to_string())]);
        let error = validate_explicit_options("s3", &options).unwrap_err();
        assert!(error.to_string().contains("aws_regoin"), "{error}");

        options = HashMap::from([
            ("aws_region".to_string(), "us-east-1".to_string()),
            ("region".to_string(), "us-west-2".to_string()),
        ]);
        assert!(validate_explicit_options("s3", &options).is_err());
    }
}
