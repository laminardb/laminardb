//! [`StateBackendConfig`] — the single tagged enum that selects and
//! configures the state backend.
//!
//! Five deployment shapes are expressible **in config alone**; application
//! code holds `Arc<dyn StateBackend>` and does not need to know which
//! concrete impl was chosen:
//!
//! ```toml
//! # 1. Embedded library, single process (default).
//! [state]
//! backend = "in_process"
//!
//! # 2. Standalone server, single node.
//! [state]
//! backend = "local"
//! path = "/var/laminar"
//!
//! # 3. Distributed-embedded, static assignment.
//! [state]
//! backend = "object_store"
//! url = "s3://bucket/laminar"
//! instance_id = "node-0"
//! vnodes = [0, 1, 2, 3]
//! merger_instance = "node-0"
//!
//! # 4. Distributed-embedded, dynamic (gossip + consensus).
//! [state]
//! backend = "object_store"
//! url = "s3://bucket/laminar"
//! instance_id = "node-0"
//! discovery = "dynamic"
//! seed_peers = ["10.0.0.1:7946", "10.0.0.2:7946"]
//!
//! # 5. Constellation — same object_store backend with a cluster
//! #    manager wired up externally. See [`crate::cluster`].
//! ```
//!
//! Embedded users can build programmatically:
//!
//! ```no_run
//! use laminar_core::state::StateBackendConfig;
//! # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
//! let backend = StateBackendConfig::in_process().build().await?;
//! # Ok(())
//! # }
//! ```

use std::path::PathBuf;
use std::sync::Arc;

use serde::Deserialize;

use super::{
    backend::StateBackend, in_process::InProcessBackend, local::LocalBackend,
    object_store::ObjectStoreBackend,
};

/// Default number of vnodes if the user does not override.
pub const DEFAULT_VNODE_CAPACITY: u32 = 256;

fn default_vnode_capacity() -> u32 {
    DEFAULT_VNODE_CAPACITY
}

/// How nodes discover one another in `object_store` mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiscoveryMode {
    /// Static vnode assignment. `vnodes` and (optionally) `merger_instance`
    /// are required in this mode.
    #[default]
    Static,
    /// Dynamic membership — peers gossip via chitchat; vnode assignment
    /// is chosen by the coordination layer.
    Dynamic,
}

/// Tagged-union config that selects the runtime [`StateBackend`].
///
/// See module docs for the five deployment shapes.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(tag = "backend", rename_all = "snake_case")]
pub enum StateBackendConfig {
    /// Non-durable in-process backend. The default.
    InProcess {
        /// Number of vnodes the backend should size for.
        #[serde(default = "default_vnode_capacity")]
        vnode_capacity: u32,
    },

    /// Durable single-node backend. Watermarks are `mmap`'d; partials
    /// live under `<path>/partials/epoch={n}/vnode={v}/partial.bin`.
    Local {
        /// Filesystem root for state.
        path: PathBuf,
        /// Number of vnodes the backend should size for.
        #[serde(default = "default_vnode_capacity")]
        vnode_capacity: u32,
    },

    /// Durable shared-state backend on S3 / GCS / Azure. Used by all
    /// distributed-embedded and constellation modes.
    ObjectStore {
        /// Object store URL: `s3://bucket/prefix`, `gs://bucket/prefix`,
        /// etc.
        url: String,
        /// This node's identity. Written into epoch manifests and used
        /// by the assignment-version fence to reject stale writes.
        instance_id: String,
        /// Number of vnodes the backend should size for.
        #[serde(default = "default_vnode_capacity")]
        vnode_capacity: u32,
        /// Static vnode subset for this instance. `None` means "all
        /// vnodes" (useful for the merger instance or for dynamic mode).
        #[serde(default)]
        vnodes: Option<Vec<u32>>,
        /// Optional merger instance — the node that fans in partials
        /// for sink emission. Only meaningful in static mode.
        #[serde(default)]
        merger_instance: Option<String>,
        /// Discovery strategy: static assignment or chitchat gossip.
        #[serde(default)]
        discovery: DiscoveryMode,
        /// Seed peers for dynamic discovery.
        #[serde(default)]
        seed_peers: Vec<String>,
    },
}

impl Default for StateBackendConfig {
    fn default() -> Self {
        Self::InProcess {
            vnode_capacity: DEFAULT_VNODE_CAPACITY,
        }
    }
}

/// Failure modes for [`StateBackendConfig::build`].
#[derive(Debug, thiserror::Error)]
pub enum StateBackendBuildError {
    /// The selected backend exists in config but its runtime impl has
    /// not been wired up yet.
    #[error("state backend '{0}' is not yet implemented")]
    NotImplemented(&'static str),

    /// Backend construction failed at the I/O layer.
    #[error("state backend construction failed: {0}")]
    Io(String),
}

impl StateBackendConfig {
    /// Builder: embedded library, single process.
    #[must_use]
    pub fn in_process() -> Self {
        Self::InProcess {
            vnode_capacity: DEFAULT_VNODE_CAPACITY,
        }
    }

    /// Builder: single-node durable state on the local filesystem.
    #[must_use]
    pub fn local(path: impl Into<PathBuf>) -> Self {
        Self::Local {
            path: path.into(),
            vnode_capacity: DEFAULT_VNODE_CAPACITY,
        }
    }

    /// Builder: distributed-embedded over an object store, static mode.
    #[must_use]
    pub fn object_store(url: impl Into<String>, instance_id: impl Into<String>) -> Self {
        Self::ObjectStore {
            url: url.into(),
            instance_id: instance_id.into(),
            vnode_capacity: DEFAULT_VNODE_CAPACITY,
            vnodes: None,
            merger_instance: None,
            discovery: DiscoveryMode::Static,
            seed_peers: Vec::new(),
        }
    }

    /// Instantiate the runtime backend.
    ///
    /// Declared `async` because backends added in later iterations
    /// (object store, distributed) need to perform async setup. The
    /// in-process path completes synchronously today; callers must
    /// still `.await` for forward-compatibility.
    ///
    /// # Errors
    /// - [`StateBackendBuildError::NotImplemented`] if the config selects
    ///   a backend whose impl has not yet landed (currently `local` and
    ///   `object_store`).
    /// - [`StateBackendBuildError::Io`] if the backend's construction
    ///   touches the filesystem / network and that fails.
    #[allow(clippy::unused_async)] // async kept for forward compatibility
    pub async fn build(&self) -> Result<Arc<dyn StateBackend>, StateBackendBuildError> {
        match self {
            Self::InProcess { vnode_capacity } => {
                Ok(Arc::new(InProcessBackend::new(*vnode_capacity)))
            }
            Self::Local {
                path,
                vnode_capacity,
            } => Ok(Arc::new(
                LocalBackend::open(path, *vnode_capacity)
                    .map_err(|e| StateBackendBuildError::Io(e.to_string()))?,
            )),
            Self::ObjectStore {
                url,
                instance_id,
                vnode_capacity,
                ..
            } => {
                let store = build_object_store(url)?;
                Ok(Arc::new(ObjectStoreBackend::new(
                    store,
                    instance_id,
                    *vnode_capacity,
                )))
            }
        }
    }

    /// Filesystem path for durable state, if any. Returns `None` for
    /// non-filesystem backends.
    #[must_use]
    pub fn local_storage_dir(&self) -> Option<&std::path::Path> {
        match self {
            Self::Local { path, .. } => Some(path.as_path()),
            _ => None,
        }
    }

    /// Returns true if this backend persists state across process
    /// restarts.
    #[must_use]
    pub fn is_durable(&self) -> bool {
        !matches!(self, Self::InProcess { .. })
    }

    /// Number of vnodes this backend is sized for.
    #[must_use]
    pub fn vnode_capacity(&self) -> u32 {
        match self {
            Self::InProcess { vnode_capacity }
            | Self::Local { vnode_capacity, .. }
            | Self::ObjectStore { vnode_capacity, .. } => *vnode_capacity,
        }
    }
}

/// Dispatch a URL to the matching `object_store` implementation.
///
/// Supported: `file://<path>`.
/// Returns `NotImplemented` for `s3://`, `gs://`, `az://` — these will
/// be added in a later iteration once the workspace pulls in
/// `url::Url` and the corresponding `object_store` features.
fn build_object_store(
    url: &str,
) -> Result<Arc<dyn ::object_store::ObjectStore>, StateBackendBuildError> {
    if let Some(path) = url.strip_prefix("file://") {
        let path = path.trim_start_matches('/');
        std::fs::create_dir_all(path)
            .map_err(|e| StateBackendBuildError::Io(e.to_string()))?;
        let fs = ::object_store::local::LocalFileSystem::new_with_prefix(path)
            .map_err(|e| StateBackendBuildError::Io(e.to_string()))?;
        Ok(Arc::new(fs))
    } else {
        Err(StateBackendBuildError::NotImplemented("object_store"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_in_process_minimal() {
        let toml = r#"backend = "in_process""#;
        let c: StateBackendConfig = toml::from_str(toml).unwrap();
        assert!(matches!(c, StateBackendConfig::InProcess { vnode_capacity: 256 }));
        assert!(!c.is_durable());
        assert!(c.local_storage_dir().is_none());
    }

    #[test]
    fn parse_local_with_path() {
        let toml = r#"
backend = "local"
path = "/var/laminar"
vnode_capacity = 128
"#;
        let c: StateBackendConfig = toml::from_str(toml).unwrap();
        assert_eq!(c.local_storage_dir(), Some(std::path::Path::new("/var/laminar")));
        assert!(c.is_durable());
        if let StateBackendConfig::Local { vnode_capacity, .. } = c {
            assert_eq!(vnode_capacity, 128);
        } else {
            panic!("expected Local");
        }
    }

    #[test]
    fn parse_object_store_static() {
        let toml = r#"
backend = "object_store"
url = "s3://bucket/laminar"
instance_id = "node-0"
vnodes = [0, 1, 2, 3]
merger_instance = "node-0"
"#;
        let c: StateBackendConfig = toml::from_str(toml).unwrap();
        match c {
            StateBackendConfig::ObjectStore {
                url,
                instance_id,
                vnodes,
                merger_instance,
                discovery,
                ..
            } => {
                assert_eq!(url, "s3://bucket/laminar");
                assert_eq!(instance_id, "node-0");
                assert_eq!(vnodes, Some(vec![0, 1, 2, 3]));
                assert_eq!(merger_instance.as_deref(), Some("node-0"));
                assert_eq!(discovery, DiscoveryMode::Static);
            }
            _ => panic!("expected ObjectStore"),
        }
    }

    #[test]
    fn parse_object_store_dynamic() {
        let toml = r#"
backend = "object_store"
url = "s3://bucket/laminar"
instance_id = "node-0"
discovery = "dynamic"
seed_peers = ["10.0.0.1:7946", "10.0.0.2:7946"]
"#;
        let c: StateBackendConfig = toml::from_str(toml).unwrap();
        match c {
            StateBackendConfig::ObjectStore {
                discovery,
                seed_peers,
                ..
            } => {
                assert_eq!(discovery, DiscoveryMode::Dynamic);
                assert_eq!(seed_peers.len(), 2);
            }
            _ => panic!("expected ObjectStore dynamic"),
        }
    }

    #[tokio::test]
    async fn build_in_process_returns_backend() {
        let c = StateBackendConfig::in_process();
        let backend = c.build().await.unwrap();
        // Smoke test: publish a watermark, read it back.
        backend.publish_watermark(0, 123).await.unwrap();
        assert_eq!(backend.global_watermark(&[0]).await.unwrap(), 123);
    }

    #[tokio::test]
    async fn build_local_instantiates_backend() {
        let dir = tempfile::tempdir().unwrap();
        let c = StateBackendConfig::local(dir.path());
        let backend = c.build().await.unwrap();
        backend.publish_watermark(0, 42).await.unwrap();
        assert_eq!(backend.global_watermark(&[0]).await.unwrap(), 42);
    }

    #[tokio::test]
    async fn build_object_store_file_url_instantiates_backend() {
        let dir = tempfile::tempdir().unwrap();
        let url = format!("file://{}", dir.path().display().to_string().replace('\\', "/"));
        let c = StateBackendConfig::object_store(url, "node-0");
        let backend = c.build().await.unwrap();
        backend
            .write_partial(0, 1, bytes::Bytes::from_static(b"z"))
            .await
            .unwrap();
        let got = backend.read_partial(0, 1).await.unwrap().unwrap();
        assert_eq!(&got[..], b"z");
    }

    #[tokio::test]
    async fn build_object_store_s3_returns_not_implemented() {
        let c = StateBackendConfig::object_store("s3://bucket/path", "node-0");
        assert!(matches!(
            c.build().await,
            Err(StateBackendBuildError::NotImplemented("object_store"))
        ));
    }

    #[test]
    fn default_is_in_process() {
        let c = StateBackendConfig::default();
        assert!(matches!(c, StateBackendConfig::InProcess { .. }));
    }

    #[test]
    fn partial_eq_works() {
        assert_eq!(
            StateBackendConfig::in_process(),
            StateBackendConfig::in_process()
        );
        assert_ne!(
            StateBackendConfig::in_process(),
            StateBackendConfig::local("/tmp/x")
        );
    }
}
