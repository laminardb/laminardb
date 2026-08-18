//! Shared HTTP application state and serving authority.
//!
//! `AppState` owns every handle the endpoints share (DB, config, metrics, auth policy,
//! WebSocket budget, serving gate). `ServingGate` is one-way serving authority: opened once
//! after startup, permanently fenced by the terminal cluster lease fence, never reopened.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;

use prometheus::Registry;

use laminar_db::LaminarDB;

use crate::config::ServerConfig;
use crate::metrics::ServerMetrics;
use crate::reload::ReloadGuard;

use super::auth::{DiagnosticReadGate, HttpAuthPolicy};

/// Cluster control-plane handles backing the `/api/v1/cluster/*` endpoints.
/// Absent in single-node mode, where those endpoints return `404`.
#[cfg(feature = "cluster")]
#[derive(Clone)]
pub struct ClusterComponents {
    /// Leader-election / membership controller.
    pub controller: Arc<laminar_core::cluster::control::ClusterController>,
    /// Durable vnode-assignment snapshot store.
    pub snapshot_store: Arc<laminar_core::cluster::control::AssignmentSnapshotStore>,
    /// Live cluster membership feed.
    pub membership_rx:
        tokio::sync::watch::Receiver<Vec<laminar_core::cluster::discovery::NodeInfo>>,
}

pub struct AppState {
    pub db: Arc<LaminarDB>,
    pub config_path: PathBuf,
    pub current_config: parking_lot::RwLock<ServerConfig>,
    pub reload_guard: ReloadGuard,
    pub registry: Arc<Registry>,
    pub server_metrics: ServerMetrics,
    pub(crate) auth_policy: HttpAuthPolicy,
    #[cfg(feature = "cluster")]
    pub(crate) diagnostic_reads: DiagnosticReadGate,
    pub(crate) ws_slots: Arc<tokio::sync::Semaphore>,
    pub(crate) serving_gate: Arc<ServingGate>,
    /// Cluster control-plane handles (cluster mode only). `None` in
    /// single-node mode; the cluster endpoints 404 when absent.
    #[cfg(feature = "cluster")]
    pub cluster: Option<ClusterComponents>,
}

const SERVING_STARTING: u8 = 0;
const SERVING_READY: u8 = 1;
const SERVING_FENCED: u8 = 2;

/// One-way serving authority shared by startup and the terminal cluster lease fence.
pub(crate) struct ServingGate {
    state: AtomicU8,
    fenced: tokio::sync::Notify,
    #[cfg(feature = "cluster")]
    process_deadline: std::sync::OnceLock<Arc<laminar_core::cluster::control::LeaseDeadline>>,
    #[cfg(feature = "cluster")]
    deadline_watcher: parking_lot::Mutex<Option<tokio::task::AbortHandle>>,
}

impl ServingGate {
    pub(crate) fn starting() -> Self {
        Self {
            state: AtomicU8::new(SERVING_STARTING),
            fenced: tokio::sync::Notify::new(),
            #[cfg(feature = "cluster")]
            process_deadline: std::sync::OnceLock::new(),
            #[cfg(feature = "cluster")]
            deadline_watcher: parking_lot::Mutex::new(None),
        }
    }

    /// Open serving after startup. A terminal fence can never be reopened.
    pub(crate) fn open(&self) -> bool {
        match self.state.compare_exchange(
            SERVING_STARTING,
            SERVING_READY,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) | Err(SERVING_READY) => true,
            Err(SERVING_FENCED) => false,
            Err(_) => unreachable!("serving gate contains an invalid state"),
        }
    }

    /// Permanently revoke serving authority.
    pub(crate) fn fence(&self) {
        self.state.store(SERVING_FENCED, Ordering::Release);
        self.fenced.notify_waiters();
    }

    #[cfg(feature = "cluster")]
    pub(crate) fn install_process_lease_deadline(
        self: &Arc<Self>,
        deadline: Arc<laminar_core::cluster::control::LeaseDeadline>,
    ) -> Result<(), &'static str> {
        if !deadline.is_live() {
            self.fence();
            return Err("HTTP process lease is already expired");
        }
        let mut watcher_slot = self.deadline_watcher.lock();
        if let Some(current) = self.process_deadline.get() {
            return if Arc::ptr_eq(current, &deadline) {
                Ok(())
            } else {
                Err("HTTP process lease deadline is already installed")
            };
        }
        self.process_deadline
            .set(Arc::clone(&deadline))
            .map_err(|_| "HTTP process lease deadline is already installed")?;
        let gate = Arc::downgrade(self);
        let watcher = tokio::spawn(async move {
            deadline.wait_until_expired().await;
            if let Some(gate) = gate.upgrade() {
                gate.fence();
            }
        });
        *watcher_slot = Some(watcher.abort_handle());
        Ok(())
    }

    pub(super) async fn wait_fenced(&self) {
        loop {
            let fenced = self.fenced.notified();
            tokio::pin!(fenced);
            fenced.as_mut().enable();
            if self.state.load(Ordering::Acquire) == SERVING_FENCED {
                return;
            }
            fenced.await;
        }
    }

    pub(crate) fn rejection_message(&self) -> Option<&'static str> {
        match self.state.load(Ordering::Acquire) {
            SERVING_STARTING => Some("server startup is not complete"),
            SERVING_READY => None,
            SERVING_FENCED => Some("server serving authority is fenced"),
            _ => unreachable!("serving gate contains an invalid state"),
        }
    }
}

impl AppState {
    /// Open the startup gate after the runtime has established serving authority.
    pub(crate) fn open_startup_gate(&self) -> bool {
        self.serving_gate.open()
    }

    pub(super) fn serving_rejection(&self) -> Option<&'static str> {
        if let Some(reason) = self.serving_gate.rejection_message() {
            return Some(reason);
        }
        #[cfg(feature = "cluster")]
        if let Some(cluster) = self.cluster.as_ref() {
            if !cluster.controller.process_lease_is_live() {
                return Some("server process lease is no longer live");
            }
            if cluster.controller.is_recovering() || self.db.coordinated_recovery_in_progress() {
                return Some("server is completing coordinated recovery");
            }
        }
        None
    }

    pub(super) async fn wait_for_serving_fence(&self) {
        #[cfg(feature = "cluster")]
        if let Some(cluster) = self.cluster.as_ref() {
            tokio::select! {
                biased;
                () = cluster.controller.wait_for_process_lease_loss() => return,
                () = self.serving_gate.wait_fenced() => return,
            }
        }
        self.serving_gate.wait_fenced().await;
    }
}

impl Drop for ServingGate {
    fn drop(&mut self) {
        #[cfg(feature = "cluster")]
        if let Some(watcher) = self.deadline_watcher.get_mut().take() {
            watcher.abort();
        }
    }
}
