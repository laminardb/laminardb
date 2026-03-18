//! Structured audit logging for security events.
//!
//! Emits structured [`AuditEvent`]s via `tracing` (at INFO level using the
//! `laminardb::audit` target). Events include: who did what, when, from
//! where, and whether it succeeded.
//!
//! An optional [`FileAuditSink`] writes JSON-lines audit logs to a file.

use std::net::IpAddr;
use std::path::PathBuf;
use std::sync::Arc;

use serde::Serialize;
use tokio::sync::mpsc;
use tracing::info;

/// Audit event kinds.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditAction {
    /// User authenticated successfully.
    Login,
    /// Authentication attempt failed.
    LoginFailed,
    /// SQL query executed.
    QueryExecute,
    /// DDL statement executed (CREATE, DROP, ALTER).
    DdlExecute,
    /// Checkpoint triggered.
    CheckpointTrigger,
    /// Configuration reloaded.
    ConfigReload,
    /// Access denied by RBAC.
    AccessDenied,
    /// Server started.
    ServerStart,
    /// Server shutdown.
    ServerShutdown,
}

impl std::fmt::Display for AuditAction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Login => write!(f, "login"),
            Self::LoginFailed => write!(f, "login_failed"),
            Self::QueryExecute => write!(f, "query_execute"),
            Self::DdlExecute => write!(f, "ddl_execute"),
            Self::CheckpointTrigger => write!(f, "checkpoint_trigger"),
            Self::ConfigReload => write!(f, "config_reload"),
            Self::AccessDenied => write!(f, "access_denied"),
            Self::ServerStart => write!(f, "server_start"),
            Self::ServerShutdown => write!(f, "server_shutdown"),
        }
    }
}

/// A structured audit event.
#[derive(Debug, Clone, Serialize)]
pub struct AuditEvent {
    /// ISO-8601 timestamp.
    pub timestamp: String,
    /// The action that occurred.
    pub action: AuditAction,
    /// User ID (or "anonymous" / "system").
    pub user_id: String,
    /// Client IP address, if available.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_ip: Option<IpAddr>,
    /// Whether the action succeeded.
    pub success: bool,
    /// Human-readable detail (e.g., the SQL statement, error message).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

impl AuditEvent {
    /// Create a new audit event with the current timestamp.
    pub fn new(action: AuditAction, user_id: impl Into<String>) -> Self {
        Self {
            timestamp: chrono::Utc::now().to_rfc3339(),
            action,
            user_id: user_id.into(),
            source_ip: None,
            success: true,
            detail: None,
        }
    }

    /// Set the source IP.
    #[must_use]
    pub fn with_source_ip(mut self, ip: IpAddr) -> Self {
        self.source_ip = Some(ip);
        self
    }

    /// Mark the event as a failure.
    #[must_use]
    pub fn failed(mut self) -> Self {
        self.success = false;
        self
    }

    /// Add detail text.
    #[must_use]
    pub fn with_detail(mut self, detail: impl Into<String>) -> Self {
        self.detail = Some(detail.into());
        self
    }

    /// Emit this event via the `tracing` framework (target: `laminardb::audit`).
    pub fn emit(&self) {
        info!(
            target: "laminardb::audit",
            action = %self.action,
            user_id = %self.user_id,
            success = self.success,
            source_ip = ?self.source_ip,
            detail = ?self.detail,
            "audit: {} by {} (success={})",
            self.action,
            self.user_id,
            self.success,
        );
    }
}

/// Handle to send audit events to a [`FileAuditSink`].
#[derive(Debug, Clone)]
pub struct AuditLogger {
    tx: mpsc::Sender<AuditEvent>,
}

impl AuditLogger {
    /// Log an audit event. Also emits via tracing.
    ///
    /// This is non-blocking; if the channel is full the event is dropped
    /// (with a tracing warning).
    pub fn log(&self, event: AuditEvent) {
        event.emit();
        if self.tx.try_send(event).is_err() {
            tracing::warn!(target: "laminardb::audit", "audit log channel full, event dropped");
        }
    }
}

/// File-based audit sink that writes JSON-lines to a file.
///
/// Spawns a background task that reads from a channel and appends
/// newline-delimited JSON to the configured path.
pub struct FileAuditSink {
    _handle: tokio::task::JoinHandle<()>,
}

impl FileAuditSink {
    /// Create a new file audit sink and its corresponding [`AuditLogger`].
    ///
    /// The `buffer_size` controls the channel capacity (events buffered
    /// before backpressure).
    pub fn new(path: PathBuf, buffer_size: usize) -> (AuditLogger, Self) {
        let (tx, rx) = mpsc::channel(buffer_size);
        let handle = tokio::spawn(Self::writer_task(path, rx));

        (AuditLogger { tx }, FileAuditSink { _handle: handle })
    }

    /// Background task: read events from the channel and append to file.
    async fn writer_task(path: PathBuf, mut rx: mpsc::Receiver<AuditEvent>) {
        use std::io::Write;

        let file = match std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
        {
            Ok(f) => f,
            Err(e) => {
                tracing::error!(
                    target: "laminardb::audit",
                    path = %path.display(),
                    error = %e,
                    "failed to open audit log file"
                );
                return;
            }
        };

        let mut writer = std::io::BufWriter::new(file);

        while let Some(event) = rx.recv().await {
            if let Ok(json) = serde_json::to_string(&event) {
                let _ = writeln!(writer, "{json}");
                let _ = writer.flush();
            }
        }
    }
}

/// Create an [`AuditLogger`] that only emits via tracing (no file sink).
///
/// Useful when file-based audit logging is not configured.
pub fn tracing_only_logger() -> AuditLogger {
    // Create a channel but immediately drop the receiver.
    // Events will be emitted via tracing in AuditLogger::log before
    // attempting the channel send.
    let (tx, _rx) = mpsc::channel(1);
    AuditLogger { tx }
}

/// Convenience: emit an audit event and return an [`Arc`]-wrapped logger.
pub fn shared_tracing_logger() -> Arc<AuditLogger> {
    Arc::new(tracing_only_logger())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_audit_event_builder() {
        let event = AuditEvent::new(AuditAction::Login, "alice")
            .with_source_ip("127.0.0.1".parse().unwrap())
            .with_detail("JWT login");

        assert_eq!(event.user_id, "alice");
        assert!(event.success);
        assert_eq!(event.source_ip, Some("127.0.0.1".parse().unwrap()));
        assert_eq!(event.detail.as_deref(), Some("JWT login"));
    }

    #[test]
    fn test_audit_event_failed() {
        let event = AuditEvent::new(AuditAction::LoginFailed, "bob").failed();
        assert!(!event.success);
    }

    #[test]
    fn test_audit_event_serializes_to_json() {
        let event =
            AuditEvent::new(AuditAction::QueryExecute, "carol").with_detail("SELECT * FROM trades");

        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("query_execute"));
        assert!(json.contains("carol"));
        assert!(json.contains("SELECT * FROM trades"));
    }

    #[tokio::test]
    async fn test_file_audit_sink() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("audit.jsonl");

        let (logger, _sink) = FileAuditSink::new(path.clone(), 100);
        logger.log(AuditEvent::new(AuditAction::Login, "alice"));
        logger.log(AuditEvent::new(AuditAction::QueryExecute, "alice").with_detail("SELECT 1"));

        // Give the background task time to write
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let contents = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = contents.lines().collect();
        assert_eq!(lines.len(), 2);
        assert!(lines[0].contains("login"));
        assert!(lines[1].contains("query_execute"));
    }

    #[test]
    fn test_tracing_only_logger_does_not_panic() {
        let logger = tracing_only_logger();
        // Should not panic even though receiver is dropped
        logger.log(AuditEvent::new(AuditAction::ServerStart, "system"));
    }

    #[test]
    fn test_audit_action_display() {
        assert_eq!(AuditAction::Login.to_string(), "login");
        assert_eq!(AuditAction::AccessDenied.to_string(), "access_denied");
        assert_eq!(AuditAction::DdlExecute.to_string(), "ddl_execute");
    }
}
