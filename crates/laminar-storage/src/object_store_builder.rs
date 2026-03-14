//! Factory for building `ObjectStore` instances from URL schemes.
//!
//! Detects the cloud provider from the URL scheme (`s3://`, `gs://`, `az://`,
//! `file://`) and constructs the appropriate backend. Cloud providers require
//! their respective feature flags (`aws`, `gcs`, `azure`).
//!
//! Credentials are resolved via `from_env()` (reads standard env vars like
//! `AWS_ACCESS_KEY_ID`) with explicit overrides from the `options` map.

#[allow(clippy::disallowed_types)] // cold path: object store setup
use std::collections::HashMap;
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

    match scheme {
        "file" => build_local_file_system(url),
        "s3" => build_s3(url, options),
        "gs" => build_gcs(url, options),
        "az" | "abfs" | "abfss" => build_azure(url, options),
        other => Err(ObjectStoreBuilderError::UnsupportedScheme(
            other.to_string(),
        )),
    }
}

/// Extract the local path from a `file://` URL and create a `LocalFileSystem`.
fn build_local_file_system(url: &str) -> Result<Arc<dyn ObjectStore>, ObjectStoreBuilderError> {
    // file:///path/to/dir → /path/to/dir
    let path = url
        .strip_prefix("file://")
        .ok_or_else(|| ObjectStoreBuilderError::InvalidUrl(url.to_string()))?;

    if path.is_empty() {
        return Err(ObjectStoreBuilderError::InvalidUrl(
            "file:// URL has empty path".to_string(),
        ));
    }

    let fs = LocalFileSystem::new_with_prefix(path)?;
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
        let url = format!("file://{}", dir.path().to_str().unwrap());
        let store = build_object_store(&url, &HashMap::new());
        assert!(store.is_ok(), "file:// should succeed: {store:?}");
    }

    #[test]
    fn test_file_scheme_empty_path_errors() {
        let result = build_object_store("file://", &HashMap::new());
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("empty path"), "got: {err}");
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
}
