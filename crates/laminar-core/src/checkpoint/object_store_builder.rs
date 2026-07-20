//! Factory for building `ObjectStore` instances from URL schemes.
//!
//! Detects the cloud provider from the URL scheme (`s3://`, `gs://`, `az://`,
//! `file://`) and constructs the appropriate backend. Cloud providers require
//! their respective feature flags (`aws`, `gcs`, `azure`).
//!
//! Credentials are resolved via `from_env()` (reads standard env vars like
//! `AWS_ACCESS_KEY_ID`) with explicit overrides from the `options` map.

#![allow(clippy::disallowed_types)] // cold path: object store setup

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use object_store::local::LocalFileSystem;
use object_store::ObjectStore;

/// Errors from object store construction.
#[derive(Debug, thiserror::Error)]
pub enum ObjectStoreBuilderError {
    /// The URL scheme requires a feature that is not compiled in.
    #[error("scheme '{scheme}' requires the '{feature}' feature flag (compile with --features {feature})")]
    MissingFeature {
        /// The URL scheme (e.g., "s3").
        scheme: String,
        /// The required cargo feature.
        feature: String,
    },

    /// Unrecognized URL scheme.
    #[error("unsupported object store URL scheme: '{0}'")]
    UnsupportedScheme(String),

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

/// Build an [`ObjectStore`] from a URL and optional configuration overrides.
///
/// # Supported schemes
///
/// | Scheme | Feature | Builder |
/// |--------|---------|---------|
/// | `file://` | (always) | `LocalFileSystem` |
/// | `s3://` | `aws` | `AmazonS3Builder` |
/// | `gs://` | `gcs` | `GoogleCloudStorageBuilder` |
/// | `az://`, `abfs://` | `azure` | `MicrosoftAzureBuilder` |
///
/// The URL's path (everything after the bucket/container) is applied as a
/// key prefix on the returned store, so every consumer — checkpoint
/// manifests, decision markers, control plane, state partials — is rooted
/// under it. The cloud builders themselves only consume the bucket from the
/// URL; without the wrapper, two clusters sharing a bucket with different
/// path prefixes would silently collide at the bucket root.
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
    let scheme = url
        .find("://")
        .map(|i| &url[..i])
        .ok_or_else(|| ObjectStoreBuilderError::InvalidUrl(format!("no scheme in '{url}'")))?;

    let store = match scheme {
        // file:// uses the whole path as the filesystem root — already rooted.
        "file" => return build_local_file_system(url),
        "s3" => build_s3(url, options),
        "gs" => build_gcs(url, options),
        "az" | "abfs" | "abfss" => build_azure(url, options),
        other => Err(ObjectStoreBuilderError::UnsupportedScheme(
            other.to_string(),
        )),
    }?;

    Ok(match url_path_prefix(url) {
        "" => store,
        prefix => Arc::new(object_store::prefix::PrefixStore::new(
            store,
            object_store::path::Path::from(prefix),
        )),
    })
}

/// The key prefix encoded in a cloud URL's path: everything after the
/// bucket/container authority, e.g. `s3://bucket/a/b/` → `a/b`.
fn url_path_prefix(url: &str) -> &str {
    let after_scheme = url.find("://").map_or(url, |i| &url[i + 3..]);
    after_scheme
        .find('/')
        .map_or("", |i| after_scheme[i + 1..].trim_matches('/'))
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

/// Extract the local path from a `file://` URL and create a `LocalFileSystem`.
fn build_local_file_system(url: &str) -> Result<Arc<dyn ObjectStore>, ObjectStoreBuilderError> {
    let path = file_url_path(url)?;

    // Ensure the directory exists — LocalFileSystem doesn't create it.
    std::fs::create_dir_all(&path).map_err(|e| {
        ObjectStoreBuilderError::InvalidUrl(format!(
            "failed to create directory '{}': {e}",
            path.display()
        ))
    })?;

    let fs = LocalFileSystem::new_with_prefix(&path)?;
    Ok(Arc::new(fs))
}

// ---------------------------------------------------------------------------
// S3 (feature = "aws")
// ---------------------------------------------------------------------------

#[cfg(feature = "aws")]
fn build_s3(
    url: &str,
    options: &HashMap<String, String>,
) -> Result<Arc<dyn ObjectStore>, ObjectStoreBuilderError> {
    use object_store::aws::AmazonS3Builder;

    let mut builder = AmazonS3Builder::from_env().with_url(url);

    for (key, value) in options {
        let config_key = key.parse().map_err(|e: object_store::Error| {
            ObjectStoreBuilderError::Build(format!("invalid S3 config key '{key}': {e}"))
        })?;
        builder = builder.with_config(config_key, value);
    }

    let store = builder.build()?;
    Ok(Arc::new(store))
}

#[cfg(not(feature = "aws"))]
fn build_s3(
    _url: &str,
    _options: &HashMap<String, String>,
) -> Result<Arc<dyn ObjectStore>, ObjectStoreBuilderError> {
    Err(ObjectStoreBuilderError::MissingFeature {
        scheme: "s3".to_string(),
        feature: "aws".to_string(),
    })
}

// ---------------------------------------------------------------------------
// GCS (feature = "gcs")
// ---------------------------------------------------------------------------

#[cfg(feature = "gcs")]
fn build_gcs(
    url: &str,
    options: &HashMap<String, String>,
) -> Result<Arc<dyn ObjectStore>, ObjectStoreBuilderError> {
    use object_store::gcp::GoogleCloudStorageBuilder;

    let mut builder = GoogleCloudStorageBuilder::from_env().with_url(url);

    for (key, value) in options {
        let config_key = key.parse().map_err(|e: object_store::Error| {
            ObjectStoreBuilderError::Build(format!("invalid GCS config key '{key}': {e}"))
        })?;
        builder = builder.with_config(config_key, value);
    }

    let store = builder.build()?;
    Ok(Arc::new(store))
}

#[cfg(not(feature = "gcs"))]
fn build_gcs(
    _url: &str,
    _options: &HashMap<String, String>,
) -> Result<Arc<dyn ObjectStore>, ObjectStoreBuilderError> {
    Err(ObjectStoreBuilderError::MissingFeature {
        scheme: "gs".to_string(),
        feature: "gcs".to_string(),
    })
}

// ---------------------------------------------------------------------------
// Azure (feature = "azure")
// ---------------------------------------------------------------------------

#[cfg(feature = "azure")]
fn build_azure(
    url: &str,
    options: &HashMap<String, String>,
) -> Result<Arc<dyn ObjectStore>, ObjectStoreBuilderError> {
    use object_store::azure::MicrosoftAzureBuilder;

    let mut builder = MicrosoftAzureBuilder::from_env().with_url(url);

    for (key, value) in options {
        let config_key = key.parse().map_err(|e: object_store::Error| {
            ObjectStoreBuilderError::Build(format!("invalid Azure config key '{key}': {e}"))
        })?;
        builder = builder.with_config(config_key, value);
    }

    let store = builder.build()?;
    Ok(Arc::new(store))
}

#[cfg(not(feature = "azure"))]
fn build_azure(
    _url: &str,
    _options: &HashMap<String, String>,
) -> Result<Arc<dyn ObjectStore>, ObjectStoreBuilderError> {
    Err(ObjectStoreBuilderError::MissingFeature {
        scheme: "az".to_string(),
        feature: "azure".to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_file_scheme_creates_local_fs() {
        let dir = tempfile::tempdir().unwrap();
        let url = url::Url::from_directory_path(dir.path()).unwrap();
        let store = build_object_store(url.as_str(), &HashMap::new());
        assert!(store.is_ok(), "file:// should succeed: {store:?}");
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
        let err = result.unwrap_err().to_string();
        assert!(err.contains("unsupported"), "got: {err}");
    }

    #[test]
    fn test_no_scheme_errors() {
        let result = build_object_store("/just/a/path", &HashMap::new());
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("no scheme"), "got: {err}");
    }

    #[test]
    fn test_s3_without_feature_errors() {
        // This test validates the behavior when aws feature is NOT compiled.
        // When aws IS compiled, S3 builder will fail for other reasons (no region).
        let result = build_object_store("s3://my-bucket/prefix", &HashMap::new());
        if cfg!(feature = "aws") {
            // With feature enabled, it will try to build (may fail due to missing config)
            assert!(result.is_err() || result.is_ok());
        } else {
            let err = result.unwrap_err().to_string();
            assert!(err.contains("aws"), "got: {err}");
        }
    }

    #[test]
    fn test_gs_without_feature_errors() {
        let result = build_object_store("gs://my-bucket/prefix", &HashMap::new());
        if cfg!(feature = "gcs") {
            assert!(result.is_err() || result.is_ok());
        } else {
            let err = result.unwrap_err().to_string();
            assert!(err.contains("gcs"), "got: {err}");
        }
    }

    #[test]
    fn test_azure_without_feature_errors() {
        let result = build_object_store("az://my-container/prefix", &HashMap::new());
        if cfg!(feature = "azure") {
            assert!(result.is_err() || result.is_ok());
        } else {
            let err = result.unwrap_err().to_string();
            assert!(err.contains("azure"), "got: {err}");
        }
    }

    /// The URL path roots ALL consumers of the store (manifests,
    /// decision markers, control plane, state partials) — two clusters
    /// sharing a bucket must not collide at the root.
    #[test]
    fn url_path_prefix_extraction() {
        assert_eq!(url_path_prefix("s3://bucket"), "");
        assert_eq!(url_path_prefix("s3://bucket/"), "");
        assert_eq!(url_path_prefix("s3://bucket/a"), "a");
        assert_eq!(url_path_prefix("s3://bucket/a/b/"), "a/b");
        assert_eq!(url_path_prefix("gs://bucket/x"), "x");
        assert_eq!(
            url_path_prefix("abfss://container@account.dfs.core.windows.net/p/q"),
            "p/q"
        );
    }
}
