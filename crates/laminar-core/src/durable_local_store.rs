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
    #[cfg(test)]
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

#[cfg(test)]
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
mod tests;
