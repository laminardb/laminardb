//! Delta GCS factory with refreshable external-account credentials.

#![allow(clippy::disallowed_types)] // cold path: connector storage setup

use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use deltalake::logstore::{
    client_options_from_certificate, object_store_factories, ObjectStoreFactory, ObjectStoreRef,
    StorageConfig,
};
use deltalake::{DeltaResult, DeltaTableError};
use futures_util::stream::BoxStream;
use laminar_core::gcs_credentials::{
    configure_gcs_workload_identity, gcs_workload_identity_provider,
};
use object_store::client::SpawnedReqwestConnector;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
    RenameTargetMode,
};
use url::Url;

use crate::error::ConnectorError;

/// Install a delegating factory immediately before a Laminar-owned GCS open.
///
/// Delta's registry is process-global and public. Replacing the entry on every
/// open makes this resilient to another caller re-registering Delta's stock
/// factory, while all non-WIF configurations still delegate to that factory.
pub(super) fn register_laminar_gcs_factory() -> Result<(), ConnectorError> {
    let scheme = Url::parse("gs://").map_err(|_| {
        ConnectorError::ConfigurationError(
            "internal Delta GCS factory scheme is invalid".to_string(),
        )
    })?;
    object_store_factories().insert(scheme, Arc::new(LaminarGcsFactory));
    Ok(())
}

#[derive(Debug)]
struct LaminarGcsFactory;

impl ObjectStoreFactory for LaminarGcsFactory {
    fn parse_url_opts(
        &self,
        url: &Url,
        config: &StorageConfig,
    ) -> DeltaResult<(ObjectStoreRef, Path)> {
        let credentials =
            gcs_workload_identity_provider(&config.raw, &|name| std::env::var(name).ok())
                .map_err(|error| DeltaTableError::Generic(error.to_string()))?;
        let Some(credentials) = credentials else {
            return deltalake::gcp::GcpFactory::default().parse_url_opts(url, config);
        };

        tracing::debug!(
            provider = "gcs",
            operation_class = "client-build",
            auth_source = "workload-identity",
            "building Delta GCS client"
        );
        build_workload_identity_store(url, config, credentials)
    }
}

fn build_workload_identity_store(
    url: &Url,
    config: &StorageConfig,
    credentials: object_store::gcp::GcpCredentialProvider,
) -> DeltaResult<(ObjectStoreRef, Path)> {
    let mut builder = GoogleCloudStorageBuilder::new()
        .with_url(url.to_string())
        .with_retry(config.retry.clone());
    if let Some(runtime) = &config.runtime {
        builder = builder.with_http_connector(SpawnedReqwestConnector::new(runtime.get_handle()));
    }
    if let Some(path) = config
        .certificate
        .as_ref()
        .and_then(|certificate| certificate.certificate_path.as_ref())
    {
        let client_options = client_options_from_certificate(path).map_err(|_| {
            DeltaTableError::Generic(
                "failed to load the configured Delta GCS trust certificate".to_string(),
            )
        })?;
        builder = builder.with_client_options(client_options);
    }

    let mut environment = std::env::vars_os()
        .filter_map(|(key, value)| Some((key.into_string().ok()?, value.into_string().ok()?)))
        .filter(|(key, _)| key.starts_with("GOOGLE_"))
        .collect::<Vec<_>>();
    environment.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
    let options = environment.into_iter().chain(
        config
            .raw
            .iter()
            .map(|(key, value)| (key.clone(), value.clone())),
    );
    let builder = configure_gcs_workload_identity(builder, options, credentials);
    let store = builder.build().map_err(|_| {
        DeltaTableError::Generic(
            "failed to construct the Delta GCS workload-identity client".to_string(),
        )
    })?;
    let (_, prefix) = object_store::ObjectStoreScheme::parse(url).map_err(|_| {
        DeltaTableError::Generic("failed to parse the canonical Delta GCS location".to_string())
    })?;
    let prefix = Path::parse(prefix).map_err(|_| {
        DeltaTableError::Generic("failed to parse the canonical Delta GCS prefix".to_string())
    })?;
    Ok((
        Arc::new(DeltaGcsStorageBackend::new(Arc::new(store))),
        prefix,
    ))
}

/// Preserve the pinned Delta GCS backend's conditional-rename classification.
struct DeltaGcsStorageBackend {
    inner: ObjectStoreRef,
}

impl DeltaGcsStorageBackend {
    fn new(inner: ObjectStoreRef) -> Self {
        Self { inner }
    }
}

impl std::fmt::Debug for DeltaGcsStorageBackend {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("DeltaGcsStorageBackend")
    }
}

impl std::fmt::Display for DeltaGcsStorageBackend {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("DeltaGcsStorageBackend")
    }
}

#[async_trait::async_trait]
#[deny(clippy::missing_trait_methods)]
impl ObjectStore for DeltaGcsStorageBackend {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> object_store::Result<Vec<Bytes>> {
        self.inner.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(
        &self,
        from: &Path,
        to: &Path,
        options: RenameOptions,
    ) -> object_store::Result<()> {
        if options.target_mode == RenameTargetMode::Overwrite {
            return self.inner.rename_opts(from, to, options).await;
        }
        match self.inner.rename_if_not_exists(from, to).await {
            Err(object_store::Error::Generic { store: _, source })
                if format!("{source:?}").contains("429") =>
            {
                Err(object_store::Error::AlreadyExists {
                    path: to.to_string(),
                    source,
                })
            }
            result => result,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn external_account_document(secret: &str) -> serde_json::Value {
        serde_json::json!({
            "type": "external_account",
            "audience": "//iam.googleapis.com/projects/1/locations/global/workloadIdentityPools/test/providers/github",
            "subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
            "token_url": "https://sts.googleapis.com/v1/token",
            "credential_source": { "file": secret }
        })
    }

    #[test]
    fn external_account_builds_the_delta_gcs_backend() {
        let directory = tempfile::tempdir().unwrap();
        let credential_path = directory.path().join("external-account.json");
        std::fs::write(
            &credential_path,
            external_account_document("subject-token").to_string(),
        )
        .unwrap();
        let config = StorageConfig::parse_options([(
            "google_application_credentials".to_string(),
            credential_path.to_string_lossy().into_owned(),
        )])
        .unwrap();

        let (store, prefix) = LaminarGcsFactory
            .parse_url_opts(
                &Url::parse("gs://test-bucket/table/prefix").unwrap(),
                &config,
            )
            .unwrap();
        assert_eq!(store.to_string(), "DeltaGcsStorageBackend");
        assert_eq!(prefix.as_ref(), "table/prefix");
    }

    #[test]
    fn invalid_external_account_error_is_redacted() {
        let directory = tempfile::tempdir().unwrap();
        let secret = "do-not-disclose";
        let credential_path = directory.path().join(secret);
        std::fs::write(
            &credential_path,
            serde_json::json!({
                "type": "external_account",
                "token_url": format!("https://{secret}.example/token"),
                "credential_source": { "file": secret }
            })
            .to_string(),
        )
        .unwrap();
        let config = StorageConfig::parse_options([(
            "google_application_credentials".to_string(),
            credential_path.to_string_lossy().into_owned(),
        )])
        .unwrap();

        let error = LaminarGcsFactory
            .parse_url_opts(&Url::parse("gs://test-bucket/table").unwrap(), &config)
            .unwrap_err()
            .to_string();
        assert!(error.contains("audience"), "{error}");
        assert!(!error.contains(secret), "{error}");
    }
}
