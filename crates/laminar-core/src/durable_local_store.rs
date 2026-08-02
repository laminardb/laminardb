use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path as FsPath, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};

#[cfg(test)]
use parking_lot::Condvar;
use parking_lot::Mutex;
use rustc_hash::{FxHashMap, FxHashSet};

use async_trait::async_trait;
use futures::stream::{self, BoxStream};
use futures::StreamExt;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
};

#[cfg(unix)]
use crate::durable_fs::sync_directory;
use crate::durable_fs::{durable_rename, ensure_durable_directory, DurableRenameMode};

const STORE_NAME: &str = "DurableLocalObjectStore";
const TEMP_PREFIX: &str = ".laminardb-object#";
const MAX_CACHED_DIRECTORIES: usize = 4096;
const MAX_RETAINED_POISONED_ROOTS: usize = 4096;
const MAX_EMPTY_PREFIX_CLEANUP_DEPTH: usize = 128;

#[cfg(test)]
#[derive(Debug, Default)]
struct TestPublicationGate {
    state: Mutex<(bool, bool)>,
    changed: Condvar,
}

#[cfg(test)]
impl TestPublicationGate {
    fn block(&self) {
        let mut state = self.state.lock();
        state.0 = true;
        self.changed.notify_all();
        while !state.1 {
            self.changed.wait(&mut state);
        }
    }

    fn wait_until_entered(&self, timeout: std::time::Duration) -> bool {
        let mut state = self.state.lock();
        self.changed
            .wait_while_for(&mut state, |state| !state.0, timeout);
        state.0
    }

    fn release(&self) {
        self.state.lock().1 = true;
        self.changed.notify_all();
    }
}

#[derive(Debug)]
struct RootDomain {
    operation_order: Arc<tokio::sync::Mutex<()>>,
    established_directories: Mutex<FxHashSet<PathBuf>>,
    directory_publication_failed: AtomicBool,
    poisoned_paths: Mutex<FxHashSet<PathBuf>>,
    #[cfg(test)]
    publication_gate: Mutex<Option<Arc<TestPublicationGate>>>,
    #[cfg(test)]
    deletion_gate: Mutex<Option<Arc<TestPublicationGate>>>,
    #[cfg(test)]
    directory_prepare_gate: Mutex<Option<Arc<TestPublicationGate>>>,
}

impl RootDomain {
    fn new(root: &FsPath) -> Self {
        let mut established_directories = FxHashSet::default();
        established_directories.insert(root.to_path_buf());
        Self {
            operation_order: Arc::new(tokio::sync::Mutex::new(())),
            established_directories: Mutex::new(established_directories),
            directory_publication_failed: AtomicBool::new(false),
            poisoned_paths: Mutex::new(FxHashSet::default()),
            #[cfg(test)]
            publication_gate: Mutex::new(None),
            #[cfg(test)]
            deletion_gate: Mutex::new(None),
            #[cfg(test)]
            directory_prepare_gate: Mutex::new(None),
        }
    }

    fn requires_recovery(&self) -> bool {
        self.directory_publication_failed.load(Ordering::Acquire)
            || !self.poisoned_paths.lock().is_empty()
    }
}

fn shared_root_domain(root: &FsPath) -> io::Result<Arc<RootDomain>> {
    static DOMAINS: OnceLock<Mutex<FxHashMap<PathBuf, Arc<RootDomain>>>> = OnceLock::new();
    let mut domains = DOMAINS
        .get_or_init(|| Mutex::new(FxHashMap::default()))
        .lock();
    domains.retain(|_, domain| Arc::strong_count(domain) != 1 || domain.requires_recovery());
    if let Some(domain) = domains.get(root) {
        return Ok(Arc::clone(domain));
    }
    if domains.len() >= MAX_RETAINED_POISONED_ROOTS {
        return Err(io::Error::other(
            "too many unresolved local object roots; restart after storage recovery",
        ));
    }
    let domain = Arc::new(RootDomain::new(root));
    domains.insert(root.to_path_buf(), Arc::clone(&domain));
    Ok(domain)
}

/// Local object store whose successful puts survive a host crash.
#[derive(Clone, Debug)]
pub(crate) struct DurableLocalObjectStore {
    root: Arc<PathBuf>,
    inner: LocalFileSystem,
    domain: Arc<RootDomain>,
    ownership_lock: Option<Arc<File>>,
}

#[async_trait]
pub(crate) trait EmptyPrefixCleanup: Send + Sync {
    async fn cleanup_empty_prefix(&self, prefix: &Path) -> object_store::Result<()>;

    async fn cleanup_retired_empty_epoch_prefixes(
        &self,
        state_root: &Path,
        before_epoch: u64,
        limit: usize,
    ) -> object_store::Result<usize>;
}

impl DurableLocalObjectStore {
    /// Open a store rooted at `root`, creating the root when necessary.
    ///
    /// # Errors
    ///
    /// Returns an object-store error when the root cannot be created, synchronized, or opened.
    pub(crate) fn new(root: impl AsRef<FsPath>) -> object_store::Result<Self> {
        Self::open(root.as_ref(), None)
    }

    /// Open a store with one live process owner for protocols that use local overwrites.
    pub(crate) fn new_exclusive(
        root: impl AsRef<FsPath>,
        lock_name: &str,
    ) -> object_store::Result<Self> {
        Self::open(root.as_ref(), Some(lock_name))
    }

    fn open(root: &FsPath, lock_name: Option<&str>) -> object_store::Result<Self> {
        ensure_durable_directory(root).map_err(generic_io_error)?;
        let root = std::fs::canonicalize(root).map_err(generic_io_error)?;
        let ownership_lock = lock_name
            .map(|name| {
                let lock = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .truncate(false)
                    .open(root.join(name))?;
                lock.try_lock().map_err(|error| {
                    io::Error::new(
                        io::ErrorKind::WouldBlock,
                        format!(
                            "local store {} is already owned by another process: {error}",
                            root.display()
                        ),
                    )
                })?;
                Ok::<_, io::Error>(Arc::new(lock))
            })
            .transpose()
            .map_err(generic_io_error)?;
        let domain = shared_root_domain(&root).map_err(generic_io_error)?;
        let inner = LocalFileSystem::new_with_prefix(&root)?;
        Ok(Self {
            root: Arc::new(root),
            inner,
            domain,
            ownership_lock,
        })
    }

    fn filesystem_path(&self, location: &Path) -> object_store::Result<PathBuf> {
        self.inner.path_to_filesystem(location)
    }
}

impl fmt::Display for DurableLocalObjectStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{STORE_NAME}({})", self.root.display())
    }
}

#[async_trait]
impl ObjectStore for DurableLocalObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        if !options.attributes.is_empty() {
            return Err(not_implemented("put_opts with attributes"));
        }
        let mode = match options.mode {
            PutMode::Create => DurableRenameMode::NoReplace,
            PutMode::Overwrite => DurableRenameMode::Replace,
            PutMode::Update(_) => return Err(not_implemented("put_opts with Update")),
        };
        let destination = self.filesystem_path(location)?;
        let root = Arc::clone(&self.root);
        let domain = Arc::clone(&self.domain);
        ensure_not_poisoned(&domain.poisoned_paths, &destination).map_err(generic_io_error)?;
        let operation_order = if mode == DurableRenameMode::Replace {
            Some(Arc::clone(&domain.operation_order).lock_owned().await)
        } else {
            None
        };
        let ownership_lock = self.ownership_lock.clone();
        let location = location.to_string();
        tokio::task::spawn_blocking(move || {
            let _operation_order = operation_order;
            let _ownership_lock = ownership_lock;
            write_durable(
                root.as_ref(),
                domain.as_ref(),
                &destination,
                payload,
                mode,
                &location,
            )
        })
        .await??;
        Ok(PutResult {
            e_tag: None,
            version: None,
        })
    }

    async fn put_multipart_opts(
        &self,
        _location: &Path,
        _options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        Err(not_implemented("put_multipart_opts"))
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let _operation_order = Arc::clone(&self.domain.operation_order).lock_owned().await;
        let path = self.filesystem_path(location)?;
        ensure_not_poisoned(&self.domain.poisoned_paths, &path).map_err(generic_io_error)?;
        if let Some(path) = canonical_object_path(&self.root, &path).map_err(generic_io_error)? {
            ensure_not_poisoned(&self.domain.poisoned_paths, &path).map_err(generic_io_error)?;
        }
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        let inner = self.inner.clone();
        let root = Arc::clone(&self.root);
        let domain = Arc::clone(&self.domain);
        let ownership_lock = self.ownership_lock.clone();
        stream::once(async move {
            let runtime = match tokio::runtime::Handle::try_current() {
                Ok(runtime) => runtime,
                Err(error) => {
                    return stream::once(async move {
                        Err(generic_io_error(io::Error::other(format!(
                            "delete_stream requires a Tokio runtime: {error}"
                        ))))
                    })
                    .boxed();
                }
            };
            let (results_tx, results_rx) = tokio::sync::mpsc::channel(10);
            drop(runtime.spawn(async move {
                let jobs = locations
                    .map(move |location| {
                        let inner = inner.clone();
                        let root = Arc::clone(&root);
                        let domain = Arc::clone(&domain);
                        let ownership_lock = ownership_lock.clone();
                        async move {
                            let location = location?;
                            let destination = inner.path_to_filesystem(&location)?;
                            let object_path = location.to_string();
                            // Serialize only this durable namespace mutation. Holding the root
                            // order across an entire retention stream can starve foreground
                            // checkpoint publication behind an arbitrarily large GC sweep.
                            let operation_order =
                                Arc::clone(&domain.operation_order).lock_owned().await;
                            tokio::task::spawn_blocking(move || {
                                #[cfg(test)]
                                block_test_deletion(domain.as_ref());
                                let result = delete_local_object(
                                    root.as_ref(),
                                    domain.as_ref(),
                                    &destination,
                                    &object_path,
                                );
                                // The logical deletion is durable before this guard is released.
                                // Empty-directory cleanup is hygiene and must not extend the
                                // foreground read/overwrite exclusion window.
                                drop(operation_order);
                                if let Ok(deleted_path) = &result {
                                    remove_empty_ancestor_directories(
                                        root.as_ref(),
                                        domain.as_ref(),
                                        deleted_path,
                                    );
                                }
                                drop(ownership_lock);
                                result.map(|_| location)
                            })
                            .await?
                        }
                    })
                    // The root order serializes durable object mutations. Polling one job at a
                    // time avoids queueing a batch of GC lock waiters ahead of a live checkpoint
                    // overwrite.
                    .buffered(1);
                futures::pin_mut!(jobs);

                loop {
                    let result = tokio::select! {
                        biased;
                        () = results_tx.closed() => break,
                        result = jobs.next() => result,
                    };
                    let Some(result) = result else {
                        break;
                    };
                    if results_tx.send(result).await.is_err() {
                        break;
                    }
                }
            }));
            stream::unfold(results_rx, |mut receiver| async move {
                receiver.recv().await.map(|result| (result, receiver))
            })
            .boxed()
        })
        .flatten()
        .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let inner = self.inner.clone();
        let path_resolver = self.inner.clone();
        let prefix = prefix.cloned();
        let root = Arc::clone(&self.root);
        let domain = Arc::clone(&self.domain);
        let ownership_lock = self.ownership_lock.clone();
        // Local publication, replacement, and deletion change the visible namespace atomically;
        // object-store LIST does not promise a point-in-time snapshot. Do not hold the mutation
        // order while streaming results: delete_stream is allowed to consume a LIST from this
        // same store and takes that order before polling its input.
        inner
            .list(prefix.as_ref())
            .map(move |result| {
                let _keep_alive = &ownership_lock;
                let metadata = result?;
                let path = path_resolver.path_to_filesystem(&metadata.location)?;
                if let Some(path) =
                    canonical_object_path(root.as_ref(), &path).map_err(generic_io_error)?
                {
                    ensure_not_poisoned(&domain.poisoned_paths, &path).map_err(generic_io_error)?;
                }
                Ok(metadata)
            })
            .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        // Like streaming LIST, delimiter inventory observes an atomic but not point-in-time
        // namespace. A large directory walk must not retain the mutation order needed by live
        // checkpoint publication.
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        _from: &Path,
        _to: &Path,
        _options: CopyOptions,
    ) -> object_store::Result<()> {
        Err(not_implemented("copy_opts"))
    }

    async fn rename_opts(
        &self,
        _from: &Path,
        _to: &Path,
        _options: RenameOptions,
    ) -> object_store::Result<()> {
        Err(not_implemented("rename_opts"))
    }
}

#[async_trait]
impl EmptyPrefixCleanup for DurableLocalObjectStore {
    async fn cleanup_empty_prefix(&self, prefix: &Path) -> object_store::Result<()> {
        let prefix = self.filesystem_path(prefix)?;
        let root = Arc::clone(&self.root);
        let domain = Arc::clone(&self.domain);
        let ownership_lock = self.ownership_lock.clone();
        tokio::task::spawn_blocking(move || {
            let _ownership_lock = ownership_lock;
            cleanup_empty_directory_tree(root.as_ref(), domain.as_ref(), &prefix)
                .map(|_| ())
                .map_err(generic_io_error)
        })
        .await?
    }

    async fn cleanup_retired_empty_epoch_prefixes(
        &self,
        state_root: &Path,
        before_epoch: u64,
        limit: usize,
    ) -> object_store::Result<usize> {
        if limit == 0 {
            return Ok(0);
        }
        let state_root = self.filesystem_path(state_root)?;
        let root = Arc::clone(&self.root);
        let domain = Arc::clone(&self.domain);
        let ownership_lock = self.ownership_lock.clone();
        tokio::task::spawn_blocking(move || {
            let _ownership_lock = ownership_lock;
            cleanup_retired_empty_epoch_directories(
                root.as_ref(),
                domain.as_ref(),
                &state_root,
                before_epoch,
                limit,
            )
            .map_err(generic_io_error)
        })
        .await?
    }
}

#[cfg(unix)]
fn delete_local_object(
    root: &FsPath,
    domain: &RootDomain,
    destination: &FsPath,
    object_path: &str,
) -> object_store::Result<PathBuf> {
    let destination = canonical_object_path(root, destination)
        .map_err(generic_io_error)?
        .ok_or_else(|| object_not_found(object_path, destination))?;
    match std::fs::remove_file(&destination) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Err(object_store::Error::NotFound {
                path: object_path.to_owned(),
                source: Box::new(error),
            });
        }
        Err(error) => return Err(generic_io_error(error)),
    }

    if let Err(error) = sync_deleted_object_parent(&destination) {
        domain.poisoned_paths.lock().insert(destination);
        return Err(generic_io_error(error));
    }
    Ok(destination)
}

#[cfg(unix)]
fn sync_deleted_object_parent(destination: &FsPath) -> io::Result<()> {
    let parent = destination
        .parent()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "local object has no parent"))?;
    sync_directory(parent)
}

#[cfg(windows)]
fn delete_local_object(
    root: &FsPath,
    _domain: &RootDomain,
    destination: &FsPath,
    object_path: &str,
) -> object_store::Result<PathBuf> {
    let destination = canonical_object_path(root, destination)
        .map_err(generic_io_error)?
        .ok_or_else(|| object_not_found(object_path, destination))?;
    let parent = destination.parent().ok_or_else(|| {
        generic_io_error(io::Error::new(
            io::ErrorKind::InvalidInput,
            "local object has no parent",
        ))
    })?;
    // `LocalFileSystem` reserves and suppresses names ending in `#<digits>`. Keep delete
    // tombstones in that namespace so a crash after the durable rename cannot expose one as an
    // object during recovery inventory.
    let tombstone = internal_artifact_path(parent);
    match durable_rename(&destination, &tombstone, DurableRenameMode::NoReplace) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Err(object_store::Error::NotFound {
                path: object_path.to_owned(),
                source: Box::new(error),
            });
        }
        Err(error) => return Err(generic_io_error(error)),
    }

    // The write-through rename is the durable logical deletion. The internal tombstone is not a
    // visible object and can be removed without weakening that result.
    if let Err(error) = std::fs::remove_file(&tombstone) {
        if error.kind() != io::ErrorKind::NotFound {
            tracing::warn!(path = %tombstone.display(), %error, "local object tombstone cleanup failed");
        }
    }
    Ok(destination)
}

#[cfg(not(any(unix, windows)))]
fn delete_local_object(
    _root: &FsPath,
    _domain: &RootDomain,
    destination: &FsPath,
    _object_path: &str,
) -> object_store::Result<PathBuf> {
    Err(generic_io_error(io::Error::new(
        io::ErrorKind::Unsupported,
        format!(
            "no proven crash-durable deletion primitive for {}",
            destination.display()
        ),
    )))
}

fn remove_empty_ancestor_directories(root: &FsPath, domain: &RootDomain, object: &FsPath) {
    let Some(mut directory) = object.parent().map(FsPath::to_path_buf) else {
        return;
    };
    while directory != root && directory.starts_with(root) {
        let Some(parent) = directory.parent().map(FsPath::to_path_buf) else {
            break;
        };
        if !try_remove_empty_directory(root, domain, &directory) {
            break;
        }
        // Directory removal is best-effort metadata hygiene. The file unlink/rename was already
        // synchronized; a crash may only resurrect an empty directory for prefix maintenance.
        directory = parent;
    }
}

fn cleanup_empty_directory_tree(
    root: &FsPath,
    domain: &RootDomain,
    prefix: &FsPath,
) -> io::Result<bool> {
    let prefix = match std::fs::canonicalize(prefix) {
        Ok(prefix) => prefix,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(true),
        Err(error) => return Err(error),
    };
    if prefix == root || !prefix.starts_with(root) {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "empty-prefix cleanup resolves outside its store root",
        ));
    }

    cleanup_empty_directory(root, domain, &prefix, 0)?;
    match std::fs::metadata(&prefix) {
        Ok(_) => Ok(false),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(true),
        Err(error) => Err(error),
    }
}

fn cleanup_empty_directory(
    root: &FsPath,
    domain: &RootDomain,
    directory: &FsPath,
    depth: usize,
) -> io::Result<()> {
    if depth >= MAX_EMPTY_PREFIX_CLEANUP_DEPTH {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "empty-prefix cleanup directory nesting exceeds its fixed bound",
        ));
    }

    let entries = match std::fs::read_dir(directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error),
        };
        let file_type = match entry.file_type() {
            Ok(file_type) => file_type,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error),
        };
        if file_type.is_dir() && !file_type.is_symlink() {
            let child = match std::fs::canonicalize(entry.path()) {
                Ok(child) => child,
                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error),
            };
            if child.parent() != Some(directory) || !child.starts_with(root) {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "empty-prefix cleanup child resolves outside its parent",
                ));
            }
            cleanup_empty_directory(root, domain, &child, depth + 1)?;
        } else if file_type.is_file() && is_internal_artifact_name(&entry.file_name()) {
            match std::fs::remove_file(entry.path()) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
        }
    }
    try_remove_empty_directory(root, domain, directory);
    Ok(())
}

fn cleanup_retired_empty_epoch_directories(
    root: &FsPath,
    domain: &RootDomain,
    state_root: &FsPath,
    before_epoch: u64,
    limit: usize,
) -> io::Result<usize> {
    let metadata = match std::fs::symlink_metadata(state_root) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(0),
        Err(error) => return Err(error),
    };
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "state root for empty-prefix cleanup is not a physical directory",
        ));
    }
    let state_root = std::fs::canonicalize(state_root)?;
    if state_root.parent() != Some(root) || !state_root.starts_with(root) {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "state root for empty-prefix cleanup resolves outside its store root",
        ));
    }

    let mut candidates = Vec::new();
    candidates.try_reserve_exact(limit).map_err(|error| {
        io::Error::other(format!(
            "cannot reserve retired empty-prefix cleanup batch: {error}"
        ))
    })?;
    let entries = std::fs::read_dir(&state_root)?;
    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error),
        };
        let file_type = match entry.file_type() {
            Ok(file_type) => file_type,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error),
        };
        if !file_type.is_dir() || file_type.is_symlink() {
            continue;
        }
        let file_name = entry.file_name();
        let Some(encoded) = file_name
            .to_str()
            .and_then(|name| name.strip_prefix("epoch="))
        else {
            continue;
        };
        if encoded.is_empty()
            || encoded.starts_with('0')
            || !encoded.bytes().all(|byte| byte.is_ascii_digit())
        {
            continue;
        }
        if !encoded
            .parse::<u64>()
            .is_ok_and(|epoch| epoch < before_epoch)
        {
            continue;
        }
        let directory = match std::fs::canonicalize(entry.path()) {
            Ok(directory) => directory,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error),
        };
        if directory.parent() != Some(state_root.as_path()) || !directory.starts_with(root) {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "retired epoch directory resolves outside the state root",
            ));
        }
        candidates.push(directory);
        if candidates.len() == limit {
            break;
        }
    }

    let mut cleaned = 0_usize;
    for directory in candidates {
        if cleanup_empty_directory_tree(root, domain, &directory)? {
            cleaned += 1;
        }
    }
    Ok(cleaned)
}

fn is_internal_artifact_name(name: &std::ffi::OsStr) -> bool {
    name.to_str()
        .and_then(|name| name.strip_prefix(TEMP_PREFIX))
        .is_some_and(|suffix| {
            !suffix.is_empty() && suffix.bytes().all(|byte| byte.is_ascii_digit())
        })
}

fn try_remove_empty_directory(root: &FsPath, domain: &RootDomain, directory: &FsPath) -> bool {
    if directory == root || !directory.starts_with(root) {
        return false;
    }
    // Immutable creates hold this same short critical section until their temporary file exists.
    // Cleanup therefore cannot invalidate a directory prepared by a live writer.
    let mut established = domain.established_directories.lock();
    match std::fs::remove_dir(directory) {
        Ok(()) => {
            established.remove(directory);
            true
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            established.remove(directory);
            true
        }
        Err(_) => false,
    }
}

fn object_not_found(object_path: &str, filesystem_path: &FsPath) -> object_store::Error {
    object_store::Error::NotFound {
        path: object_path.to_owned(),
        source: Box::new(io::Error::new(
            io::ErrorKind::NotFound,
            format!("{} does not exist", filesystem_path.display()),
        )),
    }
}

#[cfg(test)]
fn block_test_deletion(domain: &RootDomain) {
    let gate = domain.deletion_gate.lock().take();
    if let Some(gate) = gate {
        gate.block();
    }
}

fn write_durable(
    root: &FsPath,
    domain: &RootDomain,
    destination: &FsPath,
    payload: PutPayload,
    mode: DurableRenameMode,
    object_path: &str,
) -> object_store::Result<()> {
    let parent = destination.parent().ok_or_else(|| {
        generic_io_error(io::Error::new(
            io::ErrorKind::InvalidInput,
            "local object has no parent",
        ))
    })?;
    let (canonical_parent, mut temporary, temporary_path) =
        establish_parent_and_create_temporary(root, domain, parent).map_err(generic_io_error)?;
    let cleanup = TemporaryFile(temporary_path.clone());
    let file_name = destination.file_name().ok_or_else(|| {
        generic_io_error(io::Error::new(
            io::ErrorKind::InvalidInput,
            "local object has no file name",
        ))
    })?;
    let destination = canonical_parent.join(file_name);
    ensure_not_poisoned(&domain.poisoned_paths, &destination).map_err(generic_io_error)?;
    for chunk in payload {
        temporary.write_all(&chunk).map_err(generic_io_error)?;
    }
    temporary.sync_all().map_err(generic_io_error)?;
    drop(temporary);
    #[cfg(test)]
    {
        let gate = domain.publication_gate.lock().take();
        if let Some(gate) = gate {
            gate.block();
        }
    }
    if let Err(error) = durable_rename(&temporary_path, &destination, mode) {
        if mode == DurableRenameMode::NoReplace && error.kind() == io::ErrorKind::AlreadyExists {
            return Err(object_store::Error::AlreadyExists {
                path: object_path.to_owned(),
                source: Box::new(error),
            });
        }
        domain.poisoned_paths.lock().insert(destination);
        return Err(generic_io_error(error));
    }
    drop(cleanup);
    Ok(())
}

fn establish_parent_and_create_temporary(
    root: &FsPath,
    domain: &RootDomain,
    parent: &FsPath,
) -> io::Result<(PathBuf, File, PathBuf)> {
    if domain.directory_publication_failed.load(Ordering::Acquire) {
        return Err(io::Error::other(
            "local object directory durability is unresolved; restart after storage recovery",
        ));
    }

    let mut established = domain.established_directories.lock();
    if domain.directory_publication_failed.load(Ordering::Acquire) {
        return Err(io::Error::other(
            "local object directory durability is unresolved; restart after storage recovery",
        ));
    }

    let canonical = match std::fs::canonicalize(parent).ok() {
        Some(path) if established.contains(&path) => path,
        _ => match ensure_durable_directory(parent).and_then(|()| std::fs::canonicalize(parent)) {
            Ok(path) => path,
            Err(error) => {
                domain
                    .directory_publication_failed
                    .store(true, Ordering::Release);
                return Err(error);
            }
        },
    };
    if !canonical.starts_with(root) {
        domain
            .directory_publication_failed
            .store(true, Ordering::Release);
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "local object resolves outside its store root",
        ));
    }
    if established.len() >= MAX_CACHED_DIRECTORIES {
        established.clear();
        established.insert(root.to_path_buf());
    }
    established.insert(canonical.clone());
    #[cfg(test)]
    {
        let gate = domain.directory_prepare_gate.lock().take();
        if let Some(gate) = gate {
            gate.block();
        }
    }
    let (temporary, temporary_path) = create_temporary(&canonical)?;
    Ok((canonical, temporary, temporary_path))
}

fn ensure_not_poisoned(
    poisoned_paths: &Mutex<FxHashSet<PathBuf>>,
    path: &FsPath,
) -> io::Result<()> {
    if poisoned_paths.lock().contains(path) {
        return Err(io::Error::other(format!(
            "durability of {} is unresolved; restart after storage recovery",
            path.display()
        )));
    }
    Ok(())
}

fn canonical_object_path(root: &FsPath, path: &FsPath) -> io::Result<Option<PathBuf>> {
    let Some(parent) = path.parent() else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "local object has no parent",
        ));
    };
    let parent = match std::fs::canonicalize(parent) {
        Ok(parent) => parent,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    if !parent.starts_with(root) {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "local object resolves outside its store root",
        ));
    }
    let file_name = path.file_name().ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidInput, "local object has no file name")
    })?;
    Ok(Some(parent.join(file_name)))
}

fn create_temporary(parent: &FsPath) -> io::Result<(File, PathBuf)> {
    loop {
        let path = internal_artifact_path(parent);
        match OpenOptions::new().write(true).create_new(true).open(&path) {
            Ok(file) => return Ok((file, path)),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error),
        }
    }
}

fn internal_artifact_path(parent: &FsPath) -> PathBuf {
    parent.join(format!("{TEMP_PREFIX}{}", uuid::Uuid::new_v4().as_u128()))
}

struct TemporaryFile(PathBuf);

impl Drop for TemporaryFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

fn not_implemented(operation: &str) -> object_store::Error {
    object_store::Error::NotImplemented {
        operation: operation.to_owned(),
        implementer: STORE_NAME.to_owned(),
    }
}

fn generic_io_error(error: io::Error) -> object_store::Error {
    object_store::Error::Generic {
        store: STORE_NAME,
        source: Box::new(error),
    }
}

#[cfg(test)]
mod tests {
    use futures::TryStreamExt;
    use object_store::ObjectStoreExt;

    use super::*;
    use crate::checkpoint_decision::{CheckpointDecisionStore, CheckpointScope, CheckpointVerdict};

    #[tokio::test]
    async fn create_is_exclusive_and_keeps_the_winner() {
        let directory = tempfile::tempdir().unwrap();
        let store = DurableLocalObjectStore::new(directory.path()).unwrap();
        let path = Path::from("checkpoint/id.json");
        let first = store.put_opts(
            &path,
            PutPayload::from_static(b"first"),
            PutMode::Create.into(),
        );
        let second = store.put_opts(
            &path,
            PutPayload::from_static(b"second"),
            PutMode::Create.into(),
        );
        let (first, second) = tokio::join!(first, second);
        assert_ne!(first.is_ok(), second.is_ok());
        let error = first.err().or_else(|| second.err()).unwrap();
        assert!(matches!(error, object_store::Error::AlreadyExists { .. }));
        let bytes = store.get(&path).await.unwrap().bytes().await.unwrap();
        assert!(bytes == b"first".as_slice() || bytes == b"second".as_slice());
    }

    #[tokio::test]
    async fn immutable_create_does_not_wait_for_mutable_operation_order() {
        let directory = tempfile::tempdir().unwrap();
        let store = DurableLocalObjectStore::new(directory.path()).unwrap();
        let guard = Arc::clone(&store.domain.operation_order).lock_owned().await;
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            store.put_opts(
                &Path::from("state/vnode=1/partial.bin"),
                PutPayload::from_static(b"partial"),
                PutMode::Create.into(),
            ),
        )
        .await
        .expect("independent immutable publication was serialized");
        result.unwrap();
        drop(guard);
    }

    #[tokio::test]
    async fn namespace_inventory_does_not_wait_for_mutable_operation_order() {
        let directory = tempfile::tempdir().unwrap();
        let store = DurableLocalObjectStore::new(directory.path()).unwrap();
        store
            .put_opts(
                &Path::from("state/epoch=1/object"),
                PutPayload::from_static(b"state"),
                PutMode::Create.into(),
            )
            .await
            .unwrap();
        let guard = Arc::clone(&store.domain.operation_order).lock_owned().await;

        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let listed = store
                .list(Some(&Path::from("state")))
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
            assert_eq!(listed.len(), 1);
            let delimited = store
                .list_with_delimiter(Some(&Path::from("state")))
                .await
                .unwrap();
            assert_eq!(delimited.common_prefixes, vec![Path::from("state/epoch=1")]);
        })
        .await
        .expect("namespace inventory was serialized behind a mutation");

        drop(guard);
    }

    #[tokio::test]
    async fn overwrite_read_list_and_delete_roundtrip() {
        let directory = tempfile::tempdir().unwrap();
        let store = DurableLocalObjectStore::new(directory.path()).unwrap();
        let path = Path::from("outcomes/epoch=1/outcome");
        store
            .put(&path, PutPayload::from_static(b"old"))
            .await
            .unwrap();
        store
            .put(&path, PutPayload::from_static(b"new"))
            .await
            .unwrap();
        let bytes = store.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(bytes, b"new".as_slice());
        let listed = store
            .list(Some(&Path::from("outcomes")))
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].location, path);
        store.delete(&path).await.unwrap();
        assert!(matches!(
            store.get(&path).await.unwrap_err(),
            object_store::Error::NotFound { .. }
        ));
    }

    #[tokio::test]
    async fn delete_cleans_empty_ancestors_preserves_siblings_and_allows_recreate() {
        let directory = tempfile::tempdir().unwrap();
        let store = DurableLocalObjectStore::new(directory.path()).unwrap();
        let retired = Path::from("state/epoch=1/checkpoint=1/vnode=0/partial.bin");
        let retained = Path::from("state/epoch=2/checkpoint=2/vnode=0/partial.bin");
        for path in [&retired, &retained] {
            store
                .put_opts(
                    path,
                    PutPayload::from_static(b"state"),
                    PutMode::Create.into(),
                )
                .await
                .unwrap();
        }

        store.delete(&retired).await.unwrap();

        assert!(!directory.path().join("state/epoch=1").exists());
        assert!(directory
            .path()
            .join("state/epoch=2/checkpoint=2/vnode=0/partial.bin")
            .is_file());
        store
            .put_opts(
                &retired,
                PutPayload::from_static(b"recreated"),
                PutMode::Create.into(),
            )
            .await
            .unwrap();
        assert_eq!(
            store.get(&retired).await.unwrap().bytes().await.unwrap(),
            bytes::Bytes::from_static(b"recreated")
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn immutable_create_survives_concurrent_empty_ancestor_cleanup() {
        let directory = tempfile::tempdir().unwrap();
        let store = DurableLocalObjectStore::new(directory.path()).unwrap();
        let retired = Path::from("state/epoch=1/retired.bin");
        let live = Path::from("state/epoch=1/live.bin");
        store
            .put_opts(
                &retired,
                PutPayload::from_static(b"retired"),
                PutMode::Create.into(),
            )
            .await
            .unwrap();

        let gate = Arc::new(TestPublicationGate::default());
        *store.domain.directory_prepare_gate.lock() = Some(Arc::clone(&gate));
        let creating_store = store.clone();
        let creating_path = live.clone();
        let creating = tokio::spawn(async move {
            creating_store
                .put_opts(
                    &creating_path,
                    PutPayload::from_static(b"live"),
                    PutMode::Create.into(),
                )
                .await
        });
        let wait_gate = Arc::clone(&gate);
        assert!(
            tokio::task::spawn_blocking(move || {
                wait_gate.wait_until_entered(std::time::Duration::from_secs(5))
            })
            .await
            .unwrap(),
            "create did not reach the prepared-directory boundary"
        );

        let deleting_store = store.clone();
        let deleting_path = retired.clone();
        let deleting = tokio::spawn(async move { deleting_store.delete(&deleting_path).await });
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while directory.path().join("state/epoch=1/retired.bin").exists() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("delete did not reach empty-directory cleanup");

        gate.release();
        creating.await.unwrap().unwrap();
        deleting.await.unwrap().unwrap();
        assert_eq!(
            store.get(&live).await.unwrap().bytes().await.unwrap(),
            bytes::Bytes::from_static(b"live")
        );
    }

    #[tokio::test]
    async fn delete_stream_can_consume_a_listing_from_the_same_store() {
        let directory = tempfile::tempdir().unwrap();
        let store = DurableLocalObjectStore::new(directory.path()).unwrap();
        let prefix = Path::from("state/epoch=1");
        for vnode in 0..3 {
            store
                .put_opts(
                    &Path::from(format!("state/epoch=1/vnode={vnode}")),
                    PutPayload::from_static(b"retired"),
                    PutMode::Create.into(),
                )
                .await
                .unwrap();
        }

        let locations = store
            .list(Some(&prefix))
            .map(|result| result.map(|metadata| metadata.location))
            .boxed();
        let deleted = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            store.delete_stream(locations).try_collect::<Vec<_>>(),
        )
        .await
        .expect("list-to-delete deadlocked")
        .unwrap();

        assert_eq!(deleted.len(), 3);
        assert!(store
            .list(Some(&prefix))
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn delete_stream_releases_mutation_order_between_objects() {
        let directory = tempfile::tempdir().unwrap();
        let store = DurableLocalObjectStore::new(directory.path()).unwrap();
        let retired_a = Path::from("state/epoch=1/a");
        let retired_b = Path::from("state/epoch=1/b");
        let live = Path::from("state/prune-floor");
        for path in [&retired_a, &retired_b, &live] {
            store
                .put_opts(
                    path,
                    PutPayload::from_static(b"old"),
                    PutMode::Create.into(),
                )
                .await
                .unwrap();
        }

        let (locations_tx, locations_rx) =
            futures::channel::mpsc::unbounded::<object_store::Result<Path>>();
        locations_tx.unbounded_send(Ok(retired_a.clone())).unwrap();
        let mut deletes = store.delete_stream(locations_rx.boxed());
        assert_eq!(
            tokio::time::timeout(std::time::Duration::from_secs(5), deletes.next())
                .await
                .expect("first delete stalled")
                .expect("delete stream ended early")
                .unwrap(),
            retired_a
        );

        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            store.put_opts(
                &live,
                PutPayload::from_static(b"new"),
                PutMode::Overwrite.into(),
            ),
        )
        .await
        .expect("idle delete stream retained the mutation order")
        .unwrap();

        locations_tx.unbounded_send(Ok(retired_b.clone())).unwrap();
        drop(locations_tx);
        assert_eq!(deletes.next().await.unwrap().unwrap(), retired_b);
        assert!(deletes.next().await.is_none());
        assert_eq!(
            store.get(&live).await.unwrap().bytes().await.unwrap(),
            bytes::Bytes::from_static(b"new")
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn delete_completes_after_synchronizing_the_parent_directory() {
        let directory = tempfile::tempdir().unwrap();
        let store = DurableLocalObjectStore::new(directory.path()).unwrap();
        let path = Path::from("retention/obsolete");
        store
            .put_opts(
                &path,
                PutPayload::from_static(b"obsolete"),
                PutMode::Create.into(),
            )
            .await
            .unwrap();

        store.delete(&path).await.unwrap();

        assert!(!directory.path().join("retention/obsolete").exists());
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn delete_durably_renames_before_removing_the_internal_tombstone() {
        let directory = tempfile::tempdir().unwrap();
        let store = DurableLocalObjectStore::new(directory.path()).unwrap();
        let path = Path::from("retention/obsolete");
        store
            .put_opts(
                &path,
                PutPayload::from_static(b"obsolete"),
                PutMode::Create.into(),
            )
            .await
            .unwrap();

        store.delete(&path).await.unwrap();

        let parent = directory.path().join("retention");
        assert!(!parent.exists());
    }

    #[tokio::test]
    async fn overwrite_and_reads_share_order_across_reopen() {
        let directory = tempfile::tempdir().unwrap();
        let first = DurableLocalObjectStore::new(directory.path()).unwrap();
        first
            .put_opts(
                &Path::from("floor"),
                PutPayload::from_static(b"one"),
                PutMode::Create.into(),
            )
            .await
            .unwrap();
        let first_domain = Arc::clone(&first.domain);
        let guard = Arc::clone(&first_domain.operation_order).lock_owned().await;
        drop(first);

        let reopened = DurableLocalObjectStore::new(directory.path()).unwrap();
        assert!(Arc::ptr_eq(&first_domain, &reopened.domain));
        let path = Path::from("floor");
        let mut read = Box::pin(reopened.get(&path));
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(25), &mut read)
                .await
                .is_err()
        );
        drop(guard);
        tokio::time::timeout(std::time::Duration::from_secs(5), read)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelled_overwrite_drains_before_reopen_rmw() {
        let directory = tempfile::tempdir().unwrap();
        let store = DurableLocalObjectStore::new(directory.path()).unwrap();
        let path = Path::from("floor");
        store
            .put_opts(&path, PutPayload::from_static(b"1"), PutMode::Create.into())
            .await
            .unwrap();

        let gate = Arc::new(TestPublicationGate::default());
        *store.domain.publication_gate.lock() = Some(Arc::clone(&gate));
        let writer = store.clone();
        let writer_path = path.clone();
        let pending = tokio::spawn(async move {
            writer
                .put_opts(
                    &writer_path,
                    PutPayload::from_static(b"10"),
                    PutMode::Overwrite.into(),
                )
                .await
        });
        let wait_gate = Arc::clone(&gate);
        let entered = tokio::task::spawn_blocking(move || {
            wait_gate.wait_until_entered(std::time::Duration::from_secs(5))
        })
        .await
        .unwrap();
        if !entered {
            gate.release();
            panic!("overwrite did not reach its blocking publication boundary");
        }
        pending.abort();
        assert!(pending.await.unwrap_err().is_cancelled());
        drop(store);

        let reopened = DurableLocalObjectStore::new(directory.path()).unwrap();
        let read_modify_write_path = path.clone();
        let mut read_modify_write = tokio::spawn(async move {
            let bytes = reopened.get(&read_modify_write_path).await?.bytes().await?;
            let current = std::str::from_utf8(&bytes)
                .map_err(|error| object_store::Error::Generic {
                    store: STORE_NAME,
                    source: Box::new(error),
                })?
                .parse::<u64>()
                .map_err(|error| object_store::Error::Generic {
                    store: STORE_NAME,
                    source: Box::new(error),
                })?;
            let next = current.max(5).to_string();
            reopened
                .put_opts(
                    &read_modify_write_path,
                    PutPayload::from(next),
                    PutMode::Overwrite.into(),
                )
                .await?;
            Ok::<_, object_store::Error>(reopened)
        });
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(25), &mut read_modify_write,)
                .await
                .is_err(),
            "reopened read overtook the detached overwrite"
        );

        gate.release();
        let reopened = tokio::time::timeout(std::time::Duration::from_secs(5), read_modify_write)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let bytes = reopened.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(bytes, b"10".as_slice());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelled_delete_holds_ownership_until_the_filesystem_job_finishes() {
        const LOCK: &str = ".test-owner.lock";

        let directory = tempfile::tempdir().unwrap();
        let store = DurableLocalObjectStore::new_exclusive(directory.path(), LOCK).unwrap();
        let path = Path::from("state/retired");
        store
            .put_opts(
                &path,
                PutPayload::from_static(b"old"),
                PutMode::Create.into(),
            )
            .await
            .unwrap();

        let gate = Arc::new(TestPublicationGate::default());
        *store.domain.deletion_gate.lock() = Some(Arc::clone(&gate));
        let deleting_store = store.clone();
        let deleting_path = path.clone();
        let pending = tokio::spawn(async move { deleting_store.delete(&deleting_path).await });
        let wait_gate = Arc::clone(&gate);
        let entered = tokio::task::spawn_blocking(move || {
            wait_gate.wait_until_entered(std::time::Duration::from_secs(5))
        })
        .await
        .unwrap();
        if !entered {
            gate.release();
            panic!("delete did not reach its blocking filesystem boundary");
        }

        pending.abort();
        assert!(pending.await.unwrap_err().is_cancelled());
        drop(store);
        assert!(
            DurableLocalObjectStore::new_exclusive(directory.path(), LOCK).is_err(),
            "a replacement owner overtook the detached delete"
        );

        gate.release();
        let reopened = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                match DurableLocalObjectStore::new_exclusive(directory.path(), LOCK) {
                    Ok(store) => break store,
                    Err(_) => tokio::task::yield_now().await,
                }
            }
        })
        .await
        .expect("detached delete did not release its ownership fence");
        reopened
            .put_opts(
                &path,
                PutPayload::from_static(b"new"),
                PutMode::Create.into(),
            )
            .await
            .unwrap();
        let bytes = reopened.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(bytes, b"new".as_slice());
    }

    #[test]
    fn exclusive_owner_is_held_until_background_jobs_release_it() {
        const LOCK: &str = ".test-owner.lock";

        let directory = tempfile::tempdir().unwrap();
        let store = DurableLocalObjectStore::new_exclusive(directory.path(), LOCK).unwrap();
        let background_owner = store.ownership_lock.clone().unwrap();
        assert!(DurableLocalObjectStore::new_exclusive(directory.path(), LOCK).is_err());
        drop(store);
        assert!(DurableLocalObjectStore::new_exclusive(directory.path(), LOCK).is_err());
        drop(background_owner);
        DurableLocalObjectStore::new_exclusive(directory.path(), LOCK).unwrap();
    }

    #[tokio::test]
    async fn successful_and_failed_puts_leave_no_temporary_files() {
        let directory = tempfile::tempdir().unwrap();
        let store = DurableLocalObjectStore::new(directory.path()).unwrap();
        let path = Path::from("nested/metadata");
        store
            .put_opts(
                &path,
                PutPayload::from_static(b"winner"),
                PutMode::Create.into(),
            )
            .await
            .unwrap();
        store
            .put_opts(
                &path,
                PutPayload::from_static(b"loser"),
                PutMode::Create.into(),
            )
            .await
            .unwrap_err();

        fn visit(directory: &FsPath, temporary: &mut Vec<PathBuf>) {
            for entry in std::fs::read_dir(directory).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    visit(&path, temporary);
                } else if path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.starts_with(TEMP_PREFIX))
                {
                    temporary.push(path);
                }
            }
        }

        let mut temporary = Vec::new();
        visit(directory.path(), &mut temporary);
        assert!(
            temporary.is_empty(),
            "temporary files leaked: {temporary:?}"
        );
    }

    #[tokio::test]
    async fn internal_temporary_files_are_not_visible_in_object_listings() {
        let directory = tempfile::tempdir().unwrap();
        let store = DurableLocalObjectStore::new(directory.path()).unwrap();
        let parent = directory.path().join("nested");
        ensure_durable_directory(&parent).unwrap();
        let (temporary, temporary_path) = create_temporary(&parent).unwrap();
        drop(temporary);
        std::fs::write(parent.join("visible"), b"value").unwrap();

        let prefix = Path::from("nested");
        let listed = store
            .list(Some(&prefix))
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].location, Path::from("nested/visible"));

        let delimited = store.list_with_delimiter(Some(&prefix)).await.unwrap();
        assert_eq!(delimited.objects.len(), 1);
        assert_eq!(delimited.objects[0].location, Path::from("nested/visible"));
        assert!(temporary_path.is_file());
    }

    #[tokio::test]
    async fn reopen_hides_leftover_delete_tombstone_from_outcome_inventory() {
        let directory = tempfile::tempdir().unwrap();
        let store = DurableLocalObjectStore::new(directory.path()).unwrap();
        let object_store: Arc<dyn ObjectStore> = Arc::new(store);
        let decisions = CheckpointDecisionStore::new(Arc::clone(&object_store));
        for epoch in 1..=2 {
            decisions
                .record_outcome(
                    epoch,
                    epoch,
                    CheckpointScope::Local,
                    None,
                    None,
                    CheckpointVerdict::Commit,
                    None,
                )
                .await
                .unwrap();
        }

        let deleted = directory.path().join("checkpoint-outcomes/epoch=1/outcome");
        let tombstone = internal_artifact_path(deleted.parent().unwrap());
        durable_rename(&deleted, &tombstone, DurableRenameMode::NoReplace).unwrap();
        assert!(tombstone.is_file());
        drop(decisions);
        drop(object_store);

        let reopened: Arc<dyn ObjectStore> =
            Arc::new(DurableLocalObjectStore::new(directory.path()).unwrap());
        let outcomes = CheckpointDecisionStore::new(reopened)
            .outcomes()
            .await
            .unwrap();
        assert_eq!(outcomes.len(), 1);
        assert_eq!(outcomes[0].epoch, 2);
        assert!(tombstone.is_file());
    }

    #[tokio::test]
    async fn unsupported_mutations_fail_closed() {
        let directory = tempfile::tempdir().unwrap();
        let store = DurableLocalObjectStore::new(directory.path()).unwrap();
        let path = Path::from("metadata");
        let update = store
            .put_opts(
                &path,
                PutPayload::from_static(b"value"),
                PutMode::Update(object_store::UpdateVersion {
                    e_tag: Some("stale".into()),
                    version: None,
                })
                .into(),
            )
            .await;
        assert!(matches!(
            update,
            Err(object_store::Error::NotImplemented { .. })
        ));
        assert!(matches!(
            store
                .put_multipart_opts(&path, PutMultipartOptions::default())
                .await,
            Err(object_store::Error::NotImplemented { .. })
        ));
        assert!(matches!(
            store
                .copy_opts(&path, &Path::from("copy"), CopyOptions::default())
                .await,
            Err(object_store::Error::NotImplemented { .. })
        ));
    }

    #[tokio::test]
    async fn unresolved_publication_cannot_be_read_or_rewritten() {
        let directory = tempfile::tempdir().unwrap();
        let store = DurableLocalObjectStore::new(directory.path()).unwrap();
        let path = Path::from("metadata");
        let filesystem_path = store.filesystem_path(&path).unwrap();
        store.domain.poisoned_paths.lock().insert(filesystem_path);

        assert!(matches!(
            store.get(&path).await,
            Err(object_store::Error::Generic { .. })
        ));
        assert!(matches!(
            store.put(&path, PutPayload::from_static(b"value")).await,
            Err(object_store::Error::Generic { .. })
        ));
        assert!(std::fs::read_dir(directory.path())
            .unwrap()
            .all(|entry| { !is_internal_artifact_name(&entry.unwrap().file_name()) }));
        drop(store);
        let reopened = DurableLocalObjectStore::new(directory.path()).unwrap();
        assert!(matches!(
            reopened.get(&path).await,
            Err(object_store::Error::Generic { .. })
        ));
    }

    #[tokio::test]
    async fn directory_publication_failure_is_latched() {
        let directory = tempfile::tempdir().unwrap();
        let store = DurableLocalObjectStore::new(directory.path()).unwrap();
        std::fs::write(directory.path().join("not-a-directory"), b"file").unwrap();

        assert!(store
            .put_opts(
                &Path::from("not-a-directory/object"),
                PutPayload::from_static(b"value"),
                PutMode::Create.into(),
            )
            .await
            .is_err());
        drop(store);
        let reopened = DurableLocalObjectStore::new(directory.path()).unwrap();
        let error = reopened
            .put_opts(
                &Path::from("valid/object"),
                PutPayload::from_static(b"value"),
                PutMode::Create.into(),
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("durability is unresolved"));
    }
}
