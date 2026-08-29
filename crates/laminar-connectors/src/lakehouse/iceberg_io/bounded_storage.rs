//! Timeout enforcement around the released `OpenDAL` Iceberg storage factory.

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
    StorageFactory,
};
use iceberg::{Error, ErrorKind, Result};
use iceberg_storage_opendal::OpenDalStorageFactory;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct BoundedStorageFactory {
    inner: OpenDalStorageFactory,
    connect_timeout: Duration,
    request_timeout: Duration,
}

impl BoundedStorageFactory {
    #[cfg(any(test, feature = "iceberg-catalog-rest"))]
    pub(super) fn new(
        inner: OpenDalStorageFactory,
        connect_timeout: Duration,
        request_timeout: Duration,
    ) -> Self {
        Self {
            inner,
            connect_timeout,
            request_timeout,
        }
    }
}

#[typetag::serde(name = "laminardb-bounded-opendal-factory-v1")]
impl StorageFactory for BoundedStorageFactory {
    fn build(&self, config: &StorageConfig) -> Result<Arc<dyn Storage>> {
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
    factory: OpenDalStorageFactory,
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
}

#[async_trait]
#[typetag::serde(name = "laminardb-bounded-opendal-storage-v1")]
impl Storage for BoundedStorage {
    async fn exists(&self, path: &str) -> Result<bool> {
        self.initial(self.storage()?.exists(path)).await
    }

    async fn metadata(&self, path: &str) -> Result<FileMetadata> {
        self.initial(self.storage()?.metadata(path)).await
    }

    async fn read(&self, path: &str) -> Result<Bytes> {
        self.initial(self.storage()?.read(path)).await
    }

    async fn reader(&self, path: &str) -> Result<Box<dyn FileRead>> {
        let reader = self.initial(self.storage()?.reader(path)).await?;
        Ok(Box::new(BoundedFileRead {
            inner: reader,
            initial_timeout: self.initial_timeout(),
            request_timeout: self.request_timeout,
            initial_complete: AtomicBool::new(false),
            initial_gate: tokio::sync::Mutex::new(()),
        }))
    }

    async fn write(&self, path: &str, bytes: Bytes) -> Result<()> {
        self.initial(self.storage()?.write(path, bytes)).await
    }

    async fn writer(&self, path: &str) -> Result<Box<dyn FileWrite>> {
        let writer = self.initial(self.storage()?.writer(path)).await?;
        Ok(Box::new(BoundedFileWrite {
            inner: writer,
            initial_timeout: self.initial_timeout(),
            request_timeout: self.request_timeout,
            first_write: true,
        }))
    }

    async fn delete(&self, path: &str) -> Result<()> {
        self.initial(self.storage()?.delete(path)).await
    }

    async fn delete_prefix(&self, path: &str) -> Result<()> {
        self.initial(self.storage()?.delete_prefix(path)).await
    }

    async fn delete_stream(&self, paths: BoxStream<'static, String>) -> Result<()> {
        self.initial(self.storage()?.delete_stream(paths)).await
    }

    fn new_input(&self, path: &str) -> Result<InputFile> {
        Ok(InputFile::new(Arc::new(self.clone()), path.to_string()))
    }

    fn new_output(&self, path: &str) -> Result<OutputFile> {
        Ok(OutputFile::new(Arc::new(self.clone()), path.to_string()))
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
        );
        let storage = factory
            .build(&StorageConfig::new().with_prop("secret-access-key", "inline-secret"))
            .unwrap();
        let debug = format!("{storage:?}");
        assert!(debug.contains("secret-access-key"));
        assert!(!debug.contains("inline-secret"));
    }

    fn test_factory() -> OpenDalStorageFactory {
        #[cfg(feature = "iceberg-storage-fs")]
        {
            return OpenDalStorageFactory::Fs;
        }
        #[cfg(all(not(feature = "iceberg-storage-fs"), feature = "iceberg-storage-s3"))]
        {
            return OpenDalStorageFactory::S3 {
                customized_credential_load: None,
            };
        }
        #[cfg(all(
            not(feature = "iceberg-storage-fs"),
            not(feature = "iceberg-storage-s3"),
            feature = "iceberg-storage-gcs"
        ))]
        {
            return OpenDalStorageFactory::Gcs;
        }
        #[cfg(all(
            not(feature = "iceberg-storage-fs"),
            not(feature = "iceberg-storage-s3"),
            not(feature = "iceberg-storage-gcs"),
            feature = "iceberg-storage-azure"
        ))]
        {
            OpenDalStorageFactory::Azdls
        }
    }
}
