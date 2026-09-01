//! Timeout enforcement around the released `OpenDAL` Iceberg storage factory.

#[cfg(any(test, feature = "iceberg-catalog-rest"))]
use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::ops::Range;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::stream::BoxStream;
use iceberg::io::{
    FileMetadata, FileRead, FileWrite, InputFile, OutputFile, Storage, StorageConfig,
    StorageFactory, ADLS_ACCOUNT_KEY, ADLS_AUTHORITY_HOST, ADLS_CLIENT_ID, ADLS_CLIENT_SECRET,
    ADLS_CONNECTION_STRING, ADLS_SAS_TOKEN, ADLS_TENANT_ID, GCS_CREDENTIALS_JSON, GCS_TOKEN,
    S3_ACCESS_KEY_ID, S3_ASSUME_ROLE_ARN, S3_ASSUME_ROLE_EXTERNAL_ID, S3_ASSUME_ROLE_SESSION_NAME,
    S3_SECRET_ACCESS_KEY, S3_SESSION_TOKEN, S3_SSE_KEY, S3_SSE_MD5,
};
use iceberg::{Error, ErrorKind, Result};
use iceberg_storage_opendal::OpenDalResolvingStorageFactory;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use super::super::iceberg::capabilities::REST_REMOTE_SIGNING_ENABLED;
use crate::storage::{StorageConsumer, StorageLocation};

const MAX_DELETE_STREAM_OBJECTS: usize = 100_000;

#[derive(Clone, Serialize, Deserialize)]
pub(super) struct BoundedStorageFactory {
    inner: OpenDalResolvingStorageFactory,
    connect_timeout: Duration,
    request_timeout: Duration,
    #[serde(default)]
    locally_configured_sensitive: Vec<SensitivePropertyFingerprint>,
}

#[derive(Clone, Serialize, Deserialize)]
struct SensitivePropertyFingerprint {
    property: String,
    digest: [u8; 32],
}

const STORAGE_SENSITIVE_PROPERTIES: &[&str] = &[
    S3_ACCESS_KEY_ID,
    S3_SECRET_ACCESS_KEY,
    S3_SESSION_TOKEN,
    S3_ASSUME_ROLE_ARN,
    S3_ASSUME_ROLE_EXTERNAL_ID,
    S3_ASSUME_ROLE_SESSION_NAME,
    S3_SSE_KEY,
    S3_SSE_MD5,
    GCS_CREDENTIALS_JSON,
    GCS_TOKEN,
    ADLS_CONNECTION_STRING,
    ADLS_ACCOUNT_KEY,
    ADLS_SAS_TOKEN,
    ADLS_TENANT_ID,
    ADLS_CLIENT_ID,
    ADLS_CLIENT_SECRET,
    ADLS_AUTHORITY_HOST,
];

#[cfg(any(test, feature = "iceberg-catalog-rest"))]
pub(super) fn is_sensitive_storage_property(property: &str) -> bool {
    STORAGE_SENSITIVE_PROPERTIES.contains(&property)
}

pub(super) fn requests_remote_signing(property: &str, value: &str) -> bool {
    property == REST_REMOTE_SIGNING_ENABLED && value.eq_ignore_ascii_case("true")
}

impl BoundedStorageFactory {
    #[cfg(any(test, feature = "iceberg-catalog-rest"))]
    pub(super) fn new(
        inner: OpenDalResolvingStorageFactory,
        connect_timeout: Duration,
        request_timeout: Duration,
        configured_properties: &HashMap<String, String>,
    ) -> Self {
        let mut locally_configured_sensitive = configured_properties
            .iter()
            .filter(|(key, _)| is_sensitive_storage_property(key))
            .map(|(property, value)| SensitivePropertyFingerprint {
                property: property.clone(),
                digest: sensitive_property_digest(property, value),
            })
            .collect::<Vec<_>>();
        locally_configured_sensitive
            .sort_unstable_by(|left, right| left.property.cmp(&right.property));
        Self {
            inner,
            connect_timeout,
            request_timeout,
            locally_configured_sensitive,
        }
    }

    fn validate_config(&self, config: &StorageConfig) -> Result<()> {
        if config
            .props()
            .iter()
            .any(|(property, value)| requests_remote_signing(property, value))
        {
            return Err(Error::new(
                ErrorKind::FeatureUnsupported,
                "[LDB-ICEBERG-REMOTE-SIGNING-UNSUPPORTED] iceberg-rust 0.10.1 OpenDAL storage does not support REST remote signing",
            ));
        }
        if STORAGE_SENSITIVE_PROPERTIES.iter().any(|property| {
            config
                .props()
                .get(*property)
                .is_some_and(|value| !self.was_configured_locally(property, value))
        }) {
            return Err(Error::new(
                ErrorKind::FeatureUnsupported,
                "[LDB-ICEBERG-VENDED-CREDENTIALS-UNSUPPORTED] REST table configuration supplies storage access or encryption material that was not configured locally",
            ));
        }
        Ok(())
    }

    fn was_configured_locally(&self, property: &str, value: &str) -> bool {
        let digest = sensitive_property_digest(property, value);
        self.locally_configured_sensitive
            .iter()
            .any(|configured| configured.property == property && configured.digest == digest)
    }
}

impl fmt::Debug for BoundedStorageFactory {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let property_keys = self
            .locally_configured_sensitive
            .iter()
            .map(|configured| configured.property.as_str())
            .collect::<Vec<_>>();
        formatter
            .debug_struct("BoundedStorageFactory")
            .field("inner", &self.inner)
            .field("connect_timeout", &self.connect_timeout)
            .field("request_timeout", &self.request_timeout)
            .field("locally_configured_sensitive_keys", &property_keys)
            .finish()
    }
}

fn sensitive_property_digest(property: &str, value: &str) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(property.len().to_le_bytes());
    digest.update(property.as_bytes());
    digest.update(value.as_bytes());
    digest.finalize().into()
}

#[typetag::serde(name = "laminardb-bounded-opendal-factory-v1")]
impl StorageFactory for BoundedStorageFactory {
    fn build(&self, config: &StorageConfig) -> Result<Arc<dyn Storage>> {
        self.validate_config(config)?;
        Ok(Arc::new(BoundedStorage {
            factory: self.inner.clone(),
            config: config.clone(),
            connect_timeout: self.connect_timeout,
            request_timeout: self.request_timeout,
            inner: Arc::new(OnceLock::new()),
            initial_request: Arc::default(),
        }))
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct BoundedStorage {
    factory: OpenDalResolvingStorageFactory,
    config: StorageConfig,
    connect_timeout: Duration,
    request_timeout: Duration,
    #[serde(skip)]
    inner: Arc<OnceLock<Arc<dyn Storage>>>,
    #[serde(skip)]
    initial_request: Arc<InitialRequestState>,
}

#[derive(Default)]
struct InitialRequestState {
    complete: AtomicBool,
    gate: tokio::sync::Mutex<()>,
}

impl InitialRequestState {
    async fn run<T>(
        &self,
        future: impl Future<Output = Result<T>>,
        connect_timeout: Duration,
        request_timeout: Duration,
    ) -> Result<T> {
        if !self.complete.load(Ordering::Acquire) {
            // INVARIANT: only a successful operation releases later requests from the connect bound.
            let _initial = self.gate.lock().await;
            if !self.complete.load(Ordering::Acquire) {
                let result = bounded(future, connect_timeout, "connection").await;
                if result.is_ok() {
                    self.complete.store(true, Ordering::Release);
                }
                return result;
            }
        }
        bounded(future, request_timeout, "request").await
    }
}

impl fmt::Debug for BoundedStorage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut property_keys = self.config.props().keys().collect::<Vec<_>>();
        property_keys.sort_unstable();
        formatter
            .debug_struct("BoundedStorage")
            .field("factory", &self.factory)
            .field("property_keys", &property_keys)
            .field("connect_timeout", &self.connect_timeout)
            .field("request_timeout", &self.request_timeout)
            .finish_non_exhaustive()
    }
}

impl BoundedStorage {
    fn storage(&self) -> Result<Arc<dyn Storage>> {
        if let Some(storage) = self.inner.get() {
            return Ok(Arc::clone(storage));
        }
        let storage = self.factory.build(&self.config)?;
        let _ = self.inner.set(Arc::clone(&storage));
        self.inner
            .get()
            .cloned()
            .ok_or_else(|| Error::new(ErrorKind::Unexpected, "storage initialization failed"))
    }

    fn initial_timeout(&self) -> Duration {
        self.connect_timeout.min(self.request_timeout)
    }

    async fn initial<T>(&self, future: impl Future<Output = Result<T>>) -> Result<T> {
        self.initial_request
            .run(future, self.initial_timeout(), self.request_timeout)
            .await
    }

    fn canonical_path(path: &str) -> Result<String> {
        if cfg!(test) && path.starts_with("memory://") {
            return Ok(path.to_string());
        }
        StorageLocation::parse(path)
            .and_then(|location| location.adapt(StorageConsumer::Iceberg))
            .map(|adapted| adapted.url)
            .map_err(|error| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("invalid Iceberg storage location: {error}"),
                )
            })
    }
}

#[async_trait]
#[typetag::serde(name = "laminardb-bounded-opendal-storage-v1")]
impl Storage for BoundedStorage {
    async fn exists(&self, path: &str) -> Result<bool> {
        let path = Self::canonical_path(path)?;
        self.initial(self.storage()?.exists(&path)).await
    }

    async fn metadata(&self, path: &str) -> Result<FileMetadata> {
        let path = Self::canonical_path(path)?;
        self.initial(self.storage()?.metadata(&path)).await
    }

    async fn read(&self, path: &str) -> Result<Bytes> {
        let path = Self::canonical_path(path)?;
        self.initial(self.storage()?.read(&path)).await
    }

    async fn reader(&self, path: &str) -> Result<Box<dyn FileRead>> {
        let path = Self::canonical_path(path)?;
        let reader = self.initial(self.storage()?.reader(&path)).await?;
        Ok(Box::new(BoundedFileRead {
            inner: reader,
            initial_timeout: self.initial_timeout(),
            request_timeout: self.request_timeout,
            initial_complete: AtomicBool::new(false),
            initial_gate: tokio::sync::Mutex::new(()),
        }))
    }

    async fn write(&self, path: &str, bytes: Bytes) -> Result<()> {
        let path = Self::canonical_path(path)?;
        self.initial(self.storage()?.write(&path, bytes)).await
    }

    async fn writer(&self, path: &str) -> Result<Box<dyn FileWrite>> {
        let path = Self::canonical_path(path)?;
        let writer = self.initial(self.storage()?.writer(&path)).await?;
        Ok(Box::new(BoundedFileWrite {
            inner: writer,
            initial_timeout: self.initial_timeout(),
            request_timeout: self.request_timeout,
            first_write: true,
        }))
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let path = Self::canonical_path(path)?;
        self.initial(self.storage()?.delete(&path)).await
    }

    async fn delete_prefix(&self, path: &str) -> Result<()> {
        let path = Self::canonical_path(path)?;
        self.initial(self.storage()?.delete_prefix(&path)).await
    }

    async fn delete_stream(&self, mut paths: BoxStream<'static, String>) -> Result<()> {
        let mut deleted = 0_usize;
        while let Some(path) = futures_util::StreamExt::next(&mut paths).await {
            if deleted == MAX_DELETE_STREAM_OBJECTS {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "Iceberg delete stream exceeded its object safety bound",
                ));
            }
            self.delete(&path).await?;
            deleted += 1;
        }
        Ok(())
    }

    fn new_input(&self, path: &str) -> Result<InputFile> {
        Ok(InputFile::new(
            Arc::new(self.clone()),
            Self::canonical_path(path)?,
        ))
    }

    fn new_output(&self, path: &str) -> Result<OutputFile> {
        Ok(OutputFile::new(
            Arc::new(self.clone()),
            Self::canonical_path(path)?,
        ))
    }
}

struct BoundedFileRead {
    inner: Box<dyn FileRead>,
    initial_timeout: Duration,
    request_timeout: Duration,
    initial_complete: AtomicBool,
    initial_gate: tokio::sync::Mutex<()>,
}

#[async_trait]
impl FileRead for BoundedFileRead {
    async fn read(&self, range: Range<u64>) -> Result<Bytes> {
        if !self.initial_complete.load(Ordering::Acquire) {
            // INVARIANT: waiters retain the connect bound until one initial read succeeds.
            let _initial = self.initial_gate.lock().await;
            if !self.initial_complete.load(Ordering::Acquire) {
                let result = bounded(self.inner.read(range), self.initial_timeout, "read").await;
                if result.is_ok() {
                    self.initial_complete.store(true, Ordering::Release);
                }
                return result;
            }
        }
        bounded(self.inner.read(range), self.request_timeout, "read").await
    }
}

struct BoundedFileWrite {
    inner: Box<dyn FileWrite>,
    initial_timeout: Duration,
    request_timeout: Duration,
    first_write: bool,
}

#[async_trait]
impl FileWrite for BoundedFileWrite {
    async fn write(&mut self, bytes: Bytes) -> Result<()> {
        if self.first_write {
            bounded(self.inner.write(bytes), self.initial_timeout, "write").await?;
            self.first_write = false;
            return Ok(());
        }
        bounded(self.inner.write(bytes), self.request_timeout, "write").await
    }

    async fn close(&mut self) -> Result<()> {
        bounded(self.inner.close(), self.request_timeout, "close").await
    }
}

async fn bounded<T>(
    future: impl Future<Output = Result<T>>,
    timeout: Duration,
    operation: &'static str,
) -> Result<T> {
    tokio::time::timeout(timeout, future).await.map_err(|_| {
        Error::new(
            ErrorKind::Unexpected,
            format!("Iceberg storage {operation} timed out"),
        )
        .with_retryable(true)
    })?
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn timeout_is_retryable_without_exposing_a_path() {
        let error = bounded(
            std::future::pending::<Result<()>>(),
            Duration::from_millis(1),
            "read",
        )
        .await
        .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Unexpected);
        assert!(error.retryable());
        assert_eq!(
            error.to_string(),
            "Unexpected => Iceberg storage read timed out"
        );
    }

    #[tokio::test]
    async fn only_success_releases_requests_from_the_connection_bound() {
        let state = InitialRequestState::default();
        let failure = Error::new(ErrorKind::Unexpected, "injected failure");
        assert!(state
            .run(
                std::future::ready(Err::<(), _>(failure)),
                Duration::from_secs(1),
                Duration::from_millis(1),
            )
            .await
            .is_err());
        assert!(!state.complete.load(Ordering::Acquire));

        state
            .run(
                std::future::ready(Ok(())),
                Duration::from_secs(1),
                Duration::from_millis(1),
            )
            .await
            .unwrap();
        assert!(state.complete.load(Ordering::Acquire));
        let error = state
            .run(
                std::future::pending::<Result<()>>(),
                Duration::from_secs(1),
                Duration::from_millis(1),
            )
            .await
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            "Unexpected => Iceberg storage request timed out"
        );
    }

    struct FailOnceWrite(bool);

    #[async_trait]
    impl FileWrite for FailOnceWrite {
        async fn write(&mut self, _bytes: Bytes) -> Result<()> {
            if self.0 {
                self.0 = false;
                return Err(Error::new(ErrorKind::Unexpected, "injected failure"));
            }
            Ok(())
        }

        async fn close(&mut self) -> Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn failed_initial_write_retains_the_connection_bound() {
        let mut writer = BoundedFileWrite {
            inner: Box::new(FailOnceWrite(true)),
            initial_timeout: Duration::from_secs(1),
            request_timeout: Duration::from_secs(2),
            first_write: true,
        };
        assert!(writer.write(Bytes::from_static(b"first")).await.is_err());
        assert!(writer.first_write);
        writer.write(Bytes::from_static(b"retry")).await.unwrap();
        assert!(!writer.first_write);
    }

    #[test]
    fn storage_debug_redacts_property_values() {
        let factory = BoundedStorageFactory::new(
            test_factory(),
            Duration::from_secs(1),
            Duration::from_secs(2),
            &std::collections::HashMap::new(),
        );
        let storage = factory
            .build(&StorageConfig::new().with_prop("secret-access-key", "inline-secret"))
            .unwrap();
        let debug = format!("{storage:?}");
        assert!(debug.contains("secret-access-key"));
        assert!(!debug.contains("inline-secret"));
    }

    #[test]
    fn server_vended_credentials_fail_before_storage_initialization() {
        let factory = BoundedStorageFactory::new(
            test_factory(),
            Duration::from_secs(1),
            Duration::from_secs(2),
            &std::collections::HashMap::new(),
        );
        let error = factory
            .build(&StorageConfig::new().with_prop(S3_SESSION_TOKEN, "server-secret"))
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::FeatureUnsupported);
        assert!(error
            .to_string()
            .contains("LDB-ICEBERG-VENDED-CREDENTIALS-UNSUPPORTED"));
        assert!(!error.to_string().contains("server-secret"));
    }

    #[test]
    fn local_credentials_remain_usable_after_rest_config_merge() {
        let properties = std::collections::HashMap::from([
            (S3_ACCESS_KEY_ID.to_string(), "local-id".to_string()),
            (S3_SECRET_ACCESS_KEY.to_string(), "local-secret".to_string()),
        ]);
        let factory = BoundedStorageFactory::new(
            test_factory(),
            Duration::from_secs(1),
            Duration::from_secs(2),
            &properties,
        );
        let debug = format!("{factory:?}");
        let serialized = serde_json::to_string(&factory).unwrap();
        assert!(!debug.contains("local-secret"));
        assert!(!serialized.contains("local-secret"));
        factory
            .build(
                &StorageConfig::new()
                    .with_prop(S3_ACCESS_KEY_ID, "local-id")
                    .with_prop(S3_SECRET_ACCESS_KEY, "local-secret"),
            )
            .unwrap();
    }

    #[test]
    fn rest_table_config_cannot_replace_a_local_credential_value() {
        let properties = std::collections::HashMap::from([(
            S3_SESSION_TOKEN.to_string(),
            "local-session-token".to_string(),
        )]);
        let factory = BoundedStorageFactory::new(
            test_factory(),
            Duration::from_secs(1),
            Duration::from_secs(2),
            &properties,
        );
        let error = factory
            .build(&StorageConfig::new().with_prop(S3_SESSION_TOKEN, "server-session-token"))
            .unwrap_err();
        let diagnostic = error.to_string();
        assert!(diagnostic.contains("LDB-ICEBERG-VENDED-CREDENTIALS-UNSUPPORTED"));
        assert!(!diagnostic.contains("local-session-token"));
        assert!(!diagnostic.contains("server-session-token"));
    }

    #[test]
    fn remote_signing_fails_closed() {
        let factory = BoundedStorageFactory::new(
            test_factory(),
            Duration::from_secs(1),
            Duration::from_secs(2),
            &std::collections::HashMap::new(),
        );
        let error = factory
            .build(&StorageConfig::new().with_prop(REST_REMOTE_SIGNING_ENABLED, "true"))
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::FeatureUnsupported);
        assert!(error
            .to_string()
            .contains("LDB-ICEBERG-REMOTE-SIGNING-UNSUPPORTED"));
    }

    #[test]
    fn catalog_returned_paths_use_the_shared_provider_rules() {
        let factory = BoundedStorageFactory::new(
            test_factory(),
            Duration::from_secs(1),
            Duration::from_secs(2),
            &std::collections::HashMap::new(),
        );
        let storage = factory.build(&StorageConfig::new()).unwrap();
        let gcs = storage.new_input("gcs://bucket/path/data.parquet").unwrap();
        assert_eq!(gcs.location(), "gs://bucket/path/data.parquet");

        let error = storage
            .new_input("s3n://bucket/path/data.parquet")
            .unwrap_err();
        assert!(error.to_string().contains("unsupported storage URL scheme"));
        assert!(!error.to_string().contains("bucket/path"));
    }

    fn test_factory() -> OpenDalResolvingStorageFactory {
        OpenDalResolvingStorageFactory::new()
    }
}
