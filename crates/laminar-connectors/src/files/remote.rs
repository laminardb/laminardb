//! Object-store source directories (`s3://`, `gs://`, `az://`): the store built from the path and
//! `storage.*` options, the listing that discovers objects, and the reader.
//!
//! The store is rooted at the directory, so an object's key is its path relative to the source
//! directory. That key is what the manifest records, independent of the bucket, endpoint or URL
//! form the path was written in.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use crossfire::{mpsc, MAsyncTx};
use object_store::path::Path as ObjectPath;
use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt as _};
use tokio::sync::watch;
use tracing::warn;

use super::discovery::DiscoveredFile;
use super::manifest::FileInventorySnapshot;
use super::source::FileReader;
use crate::connector::ConnectorTaskGuard;
use crate::error::ConnectorError;
use crate::storage::resolver::StorageCredentialResolver;
use laminar_core::storage_location::{StorageLocation, StorageProvider};

/// A failed listing is retried at the next poll; this many in a row stop the source.
const MAX_CONSECUTIVE_LIST_FAILURES: u32 = 5;
const LIST_TIMEOUT: Duration = Duration::from_secs(60);

/// An object-store directory the source reads, with an optional file-name glob.
#[derive(Debug, Clone)]
pub(super) struct RemoteDirectory {
    url: String,
    glob: Option<globset::GlobMatcher>,
}

impl RemoteDirectory {
    /// The directory `path` names when it is an object-store URL; `None` for a local path. As for
    /// a local directory, `glob_pattern` takes precedence over a glob in the path.
    ///
    /// # Errors
    /// Returns a configuration error for a malformed URL or file-name glob.
    pub(super) fn parse(
        path: &str,
        glob_pattern: Option<&str>,
    ) -> Result<Option<Self>, ConnectorError> {
        if std::path::Path::new(path).is_absolute()
            || !path.contains("://")
            || path.starts_with("file://")
        {
            return Ok(None);
        }
        // Parse the whole path before splitting off a glob: `?` is also a glob character, and a
        // query string can carry signed credentials, which the parser rejects without echoing.
        let location = StorageLocation::parse(path).map_err(|error| {
            ConnectorError::ConfigurationError(format!("invalid file source path: {error}"))
        })?;
        if location.provider == StorageProvider::Local {
            return Ok(None);
        }
        let (directory, glob_from_path) = super::discovery::split_dir_and_glob(path);
        let glob = glob_pattern.map(str::to_owned).or(glob_from_path);
        let glob = glob
            .map(|pattern| {
                globset::Glob::new(&pattern)
                    .map(|glob| glob.compile_matcher())
                    .map_err(|error| {
                        ConnectorError::ConfigurationError(format!(
                            "invalid file name glob in 'path': {error}"
                        ))
                    })
            })
            .transpose()?;
        Ok(Some(Self {
            url: directory,
            glob,
        }))
    }

    /// The store rooted at this directory, with credentials from `storage.*` options or the
    /// environment.
    ///
    /// # Errors
    /// Returns a connection error when the store cannot be built, such as for a provider this
    /// build does not include.
    pub(super) fn connect(
        &self,
        storage_options: &HashMap<String, String>,
    ) -> Result<Arc<dyn ObjectStore>, ConnectorError> {
        let resolved = StorageCredentialResolver::resolve(&self.url, storage_options);
        laminar_core::checkpoint::object_store_builder::build_object_store(
            &self.url,
            &resolved.options,
        )
        .map_err(|error| {
            ConnectorError::ConnectionFailed(format!("file source object store: {error}"))
        })
    }

    fn matches(&self, object: &ObjectMeta) -> bool {
        let Some(name) = object.location.filename() else {
            return false;
        };
        self.glob.as_ref().is_none_or(|glob| glob.is_match(name))
    }
}

/// Which listed objects discovery sends.
pub(super) enum ListingFilter {
    /// Objects the manifest does not hold.
    Unprocessed(Arc<FileInventorySnapshot>),
    /// Objects of the split channels this node owns; the source skips those it has read.
    OwnedChannels {
        splits: NonZeroU32,
        owned: watch::Receiver<Arc<BTreeSet<u32>>>,
    },
}

impl ListingFilter {
    /// Whether the owned channels changed since the last call, so that objects refused or
    /// dropped before may be wanted now.
    fn ownership_moved(&mut self) -> bool {
        match self {
            Self::Unprocessed(_) => false,
            // Observe and acknowledge in one step: every publication bumps the version, so a
            // release and a re-acquisition between two listings still read as a move.
            Self::OwnedChannels { owned, .. } => owned.borrow_and_update().has_changed(),
        }
    }

    fn wants(&self, key: &str) -> bool {
        match self {
            Self::Unprocessed(known) => !known.contains(key),
            Self::OwnedChannels { splits, owned } => owned
                .borrow()
                .contains(&super::split::object_channel(key, *splits)),
        }
    }
}

/// List the directory every `poll_interval` and send each object `filter` wants once, in key
/// order. Only direct children are read, as for a local directory. Objects are complete once
/// listed, so there is no stabilisation delay, and an object deleted before it was read drops out
/// at the next listing.
pub(super) async fn discovery_loop(
    store: Arc<dyn ObjectStore>,
    directory: RemoteDirectory,
    poll_interval: Duration,
    tx: MAsyncTx<mpsc::Array<DiscoveredFile>>,
    mut filter: ListingFilter,
) -> Result<(), ConnectorError> {
    let mut sent: HashSet<String> = HashSet::new();
    let mut failures = 0;
    loop {
        if filter.ownership_moved() {
            sent.clear();
        }
        match list(store.as_ref()).await {
            Ok(objects) => {
                failures = 0;
                let objects = objects
                    .into_iter()
                    .filter(|object| directory.matches(object));
                if !send_new(objects, &filter, &mut sent, &tx).await {
                    return Ok(());
                }
            }
            Err(error) => {
                failures += 1;
                if failures == MAX_CONSECUTIVE_LIST_FAILURES {
                    return Err(error);
                }
                warn!(%error, failures, "file source listing failed; retrying at the next poll");
            }
        }
        tokio::time::sleep(poll_interval).await;
    }
}

/// Send the listed objects `filter` wants that are not in `sent`, in key order, and forget sent
/// keys no longer listed. Returns `false` once the source has stopped receiving.
async fn send_new(
    objects: impl Iterator<Item = ObjectMeta>,
    filter: &ListingFilter,
    sent: &mut HashSet<String>,
    tx: &MAsyncTx<mpsc::Array<DiscoveredFile>>,
) -> bool {
    let mut objects: Vec<ObjectMeta> = objects.collect();
    objects.sort_by(|left, right| left.location.cmp(&right.location));
    let listed: HashSet<String> = objects
        .iter()
        .map(|object| object.location.to_string())
        .collect();
    sent.retain(|key| listed.contains(key));
    for object in objects {
        let key = object.location.to_string();
        if sent.contains(&key) || !filter.wants(&key) {
            continue;
        }
        let file = DiscoveredFile {
            path: key.clone(),
            size: object.size,
            modified_ms: u64::try_from(object.last_modified.timestamp_millis()).unwrap_or_default(),
        };
        if tx.send(file).await.is_err() {
            return false;
        }
        sent.insert(key);
    }
    true
}

async fn list(store: &dyn ObjectStore) -> Result<Vec<ObjectMeta>, ConnectorError> {
    let listing = tokio::time::timeout(LIST_TIMEOUT, store.list_with_delimiter(None))
        .await
        .map_err(|_| ConnectorError::ReadError("file source listing timed out".into()))?
        .map_err(|error| ConnectorError::ReadError(format!("file source listing: {error}")))?;
    Ok(listing.objects)
}

/// Reads whole objects, refusing one that grew past `max_bytes` before downloading it.
pub(super) struct ObjectStoreReader {
    store: Arc<dyn ObjectStore>,
    max_bytes: u64,
}

impl ObjectStoreReader {
    pub(super) fn new(store: Arc<dyn ObjectStore>, max_bytes: u64) -> Self {
        Self { store, max_bytes }
    }
}

#[async_trait]
impl FileReader for ObjectStoreReader {
    async fn read(
        &self,
        path: &str,
        _task_guard: ConnectorTaskGuard,
    ) -> Result<Vec<u8>, ConnectorError> {
        let location = ObjectPath::parse(path).map_err(|error| {
            ConnectorError::ReadError(format!("invalid object key '{path}': {error}"))
        })?;
        let object =
            self.store.get(&location).await.map_err(|error| {
                ConnectorError::ReadError(format!("cannot read '{path}': {error}"))
            })?;
        if object.meta.size > self.max_bytes {
            return Err(ConnectorError::ConfigurationError(format!(
                "file '{path}' size {} exceeds max_file_bytes {}",
                object.meta.size, self.max_bytes
            )));
        }
        let bytes = object
            .bytes()
            .await
            .map_err(|error| ConnectorError::ReadError(format!("cannot read '{path}': {error}")))?;
        Ok(Vec::from(bytes))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use crossfire::{mpsc, AsyncRx};
    use object_store::memory::InMemory;
    use object_store::PutPayload;

    use super::*;
    use crate::connector::ConnectorTaskOwner;
    use crate::files::discovery::FileDiscoveryEngine;
    use crate::files::manifest::FileIngestionManifest;
    use crate::files::test_support::put_object;

    fn directory(path: &str) -> RemoteDirectory {
        RemoteDirectory::parse(path, None).unwrap().unwrap()
    }

    /// Start listing `store`, skipping what `manifest` holds.
    fn listen(
        store: &Arc<InMemory>,
        path: &str,
        manifest: &FileIngestionManifest,
    ) -> (
        AsyncRx<mpsc::Array<DiscoveredFile>>,
        tokio::task::JoinHandle<()>,
    ) {
        let (tx, rx) = mpsc::bounded_async::<DiscoveredFile>(16);
        let store: Arc<dyn ObjectStore> = Arc::clone(store) as Arc<dyn ObjectStore>;
        let known = Arc::new(manifest.snapshot_for_dedup());
        let directory = directory(path);
        let handle = tokio::spawn(async move {
            discovery_loop(
                store,
                directory,
                Duration::from_millis(5),
                tx,
                ListingFilter::Unprocessed(known),
            )
            .await
            .unwrap();
        });
        (rx, handle)
    }

    /// The keys sent once `expected` have arrived (within five seconds), plus any sent within a few
    /// more polls.
    async fn sent(
        rx: &AsyncRx<mpsc::Array<DiscoveredFile>>,
        expected: usize,
    ) -> Vec<(String, u64)> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        let mut files = Vec::new();
        while files.len() < expected {
            match tokio::time::timeout_at(deadline, rx.recv()).await {
                Ok(Ok(file)) => files.push((file.path, file.size)),
                _ => break,
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        while let Ok(file) = rx.try_recv() {
            files.push((file.path, file.size));
        }
        files
    }

    #[test]
    fn only_object_store_urls_are_remote() {
        let remote = directory("s3://bucket/landing/*.json");
        assert_eq!(remote.url, "s3://bucket/landing");
        assert!(remote.glob.is_some());
        for local in ["/data/landing", "landing/*.csv", "file:///data/landing"] {
            assert!(
                RemoteDirectory::parse(local, None).unwrap().is_none(),
                "{local}"
            );
        }
        let error = RemoteDirectory::parse("az://container/path?sv=1&sig=secret", None)
            .unwrap_err()
            .to_string();
        assert!(!error.contains("secret"), "{error}");
    }

    #[tokio::test]
    async fn listing_sends_matching_direct_children_once_in_key_order() {
        let store = Arc::new(InMemory::new());
        put_object(&store, "b.json", b"{}").await;
        put_object(&store, "a.json", b"{}\n{}").await;
        put_object(&store, "empty.json", b"").await;
        put_object(&store, "notes.txt", b"skip").await;
        put_object(&store, "nested/c.json", b"{}").await;
        let (rx, handle) = listen(&store, "s3://bucket/*.json", &FileIngestionManifest::new());

        assert_eq!(
            sent(&rx, 3).await,
            [
                ("a.json".into(), 5),
                ("b.json".into(), 2),
                ("empty.json".into(), 0)
            ]
        );
        put_object(&store, "d.json", b"{}").await;
        assert_eq!(sent(&rx, 1).await, [("d.json".into(), 2)]);
        handle.abort();
    }

    #[tokio::test]
    async fn processed_objects_are_not_sent() {
        let store = Arc::new(InMemory::new());
        put_object(&store, "a.json", b"{}").await;
        put_object(&store, "b.json", b"{}").await;
        let mut manifest = FileIngestionManifest::new();
        manifest.insert("a.json".into());
        let (rx, handle) = listen(&store, "s3://bucket", &manifest);

        assert_eq!(sent(&rx, 1).await, [("b.json".into(), 2)]);
        handle.abort();
    }

    type ObjectStream<T> = std::pin::Pin<
        Box<dyn tokio_stream::Stream<Item = object_store::Result<T>> + Send + 'static>,
    >;

    /// A store whose every request fails, counting listings.
    #[derive(Debug, Default)]
    struct UnreachableStore {
        listings: std::sync::atomic::AtomicU32,
    }

    impl std::fmt::Display for UnreachableStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("UnreachableStore")
        }
    }

    fn unreachable() -> object_store::Error {
        object_store::Error::Generic {
            store: "UnreachableStore",
            source: "connection refused".into(),
        }
    }

    #[async_trait]
    impl ObjectStore for UnreachableStore {
        async fn put_opts(
            &self,
            _location: &ObjectPath,
            _payload: PutPayload,
            _opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            Err(unreachable())
        }

        async fn put_multipart_opts(
            &self,
            _location: &ObjectPath,
            _opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            Err(unreachable())
        }

        async fn get_opts(
            &self,
            _location: &ObjectPath,
            _options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            Err(unreachable())
        }

        fn delete_stream(&self, _locations: ObjectStream<ObjectPath>) -> ObjectStream<ObjectPath> {
            Box::pin(tokio_stream::once(Err(unreachable())))
        }

        fn list(&self, _prefix: Option<&ObjectPath>) -> ObjectStream<ObjectMeta> {
            Box::pin(tokio_stream::once(Err(unreachable())))
        }

        async fn list_with_delimiter(
            &self,
            _prefix: Option<&ObjectPath>,
        ) -> object_store::Result<object_store::ListResult> {
            self.listings
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Err(unreachable())
        }

        async fn copy_opts(
            &self,
            _from: &ObjectPath,
            _to: &ObjectPath,
            _options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            Err(unreachable())
        }
    }

    #[tokio::test]
    async fn repeated_listing_failures_stop_discovery() {
        let store = Arc::new(UnreachableStore::default());
        let (owner, _tracker) = ConnectorTaskOwner::new();
        let mut engine = FileDiscoveryEngine::start_remote(
            Arc::clone(&store) as Arc<dyn ObjectStore>,
            directory("s3://bucket/landing"),
            Duration::from_millis(1),
            ListingFilter::Unprocessed(Arc::new(FileIngestionManifest::new().snapshot_for_dedup())),
            owner.track().unwrap(),
        );

        let error = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Err(error) = engine.drain(16).await {
                    return error;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("discovery kept running after repeated listing failures");
        assert!(error.to_string().contains("connection refused"), "{error}");
        assert_eq!(
            store.listings.load(std::sync::atomic::Ordering::Relaxed),
            MAX_CONSECUTIVE_LIST_FAILURES
        );
    }

    #[tokio::test]
    async fn objects_of_channels_owned_again_are_sent_again() {
        let store = Arc::new(InMemory::new());
        for key in ["a.json", "b.json", "c.json", "d.json"] {
            put_object(&store, key, b"{}").await;
        }
        let splits = NonZeroU32::new(2).unwrap();
        let every_channel = Arc::new(BTreeSet::from([0, 1]));
        let (owned, receiver) = watch::channel(Arc::new(BTreeSet::new()));
        let (tx, rx) = mpsc::bounded_async::<DiscoveredFile>(16);
        let filter = ListingFilter::OwnedChannels {
            splits,
            owned: receiver,
        };
        let handle = tokio::spawn(discovery_loop(
            Arc::clone(&store) as Arc<dyn ObjectStore>,
            directory("s3://bucket"),
            Duration::from_millis(5),
            tx,
            filter,
        ));
        assert!(sent(&rx, 0).await.is_empty());

        owned.send_replace(Arc::clone(&every_channel));
        assert_eq!(sent(&rx, 4).await.len(), 4);
        // Released and taken back between two listings: the source dropped what it had queued.
        owned.send_replace(Arc::new(BTreeSet::new()));
        owned.send_replace(every_channel);
        assert_eq!(sent(&rx, 4).await.len(), 4);
        handle.abort();
    }

    #[tokio::test]
    async fn the_reader_refuses_an_object_larger_than_max_file_bytes() {
        let store = Arc::new(InMemory::new());
        put_object(&store, "small.json", b"{}").await;
        put_object(&store, "large.json", b"0123456789").await;
        let reader = ObjectStoreReader::new(Arc::clone(&store) as Arc<dyn ObjectStore>, 4);
        let (owner, _tracker) = ConnectorTaskOwner::new();

        let small = reader.read("small.json", owner.track().unwrap()).await;
        assert_eq!(small.unwrap(), b"{}");
        let error = reader
            .read("large.json", owner.track().unwrap())
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("exceeds max_file_bytes"),
            "{error}"
        );
    }
}
