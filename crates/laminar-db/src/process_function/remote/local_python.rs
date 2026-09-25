use std::path::PathBuf;
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use sha2::{Digest, Sha256};
use tokio::io::AsyncReadExt;
use tokio::process::{Child, Command};
use tokio_util::sync::CancellationToken;

use super::{RemoteProcessClient, MAX_IN_FLIGHT};
use crate::error::DbError;
use crate::process_function::{ProcessFunctionDescriptor, ProcessRuntime};

/// Explicit local Python worker launch. The handler file is the direct digest-bound artifact;
/// callers must pin any imported code or data separately before claiming replay equivalence.
pub struct LocalPythonWorkerConfig {
    /// Python executable or a trusted executable name resolved by the host environment.
    pub python: PathBuf,
    /// Canonical descriptor manifest consumed by the worker.
    pub manifest: PathBuf,
    /// Top-level Python module file containing the handler function.
    pub handler_file: PathBuf,
    /// Function name exported by `handler_file`.
    pub function: String,
    /// Additional import roots, used for a local SDK installation or locked dependencies.
    pub python_paths: Vec<PathBuf>,
    /// Maximum simultaneous worker calls. The process and client use the same limit.
    pub max_in_flight: usize,
    /// Startup and per-call deadline, between 1 millisecond and 30 seconds.
    pub timeout: Duration,
}

/// One child process with an explicit shutdown owner. Unexpected exit is observed by the
/// supervisor and outstanding calls fail through the connected transport.
pub struct LocalPythonWorker {
    client: Arc<RemoteProcessClient>,
    cancel: CancellationToken,
    exited: CancellationToken,
    alive: Arc<AtomicBool>,
    supervisor: Option<tokio::task::JoinHandle<Result<(), DbError>>>,
    #[cfg(test)]
    process_id: u32,
}

struct VerifiedBinding {
    manifest: PathBuf,
    handler_file: PathBuf,
    module: String,
    descriptor: ProcessFunctionDescriptor,
}

impl VerifiedBinding {
    fn verify(config: &LocalPythonWorkerConfig) -> Result<Self, DbError> {
        if config.max_in_flight == 0
            || config.max_in_flight > MAX_IN_FLIGHT
            || config.timeout < Duration::from_millis(1)
            || config.timeout > Duration::from_secs(30)
            || !valid_identifier(&config.function)
        {
            return Err(DbError::InvalidOperation(
                "invalid local process worker limits or handler function".into(),
            ));
        }
        let manifest = config
            .manifest
            .canonicalize()
            .map_err(|error| DbError::Config(format!("resolve process manifest: {error}")))?;
        let handler_file = config
            .handler_file
            .canonicalize()
            .map_err(|error| DbError::Config(format!("resolve process handler: {error}")))?;
        let module = handler_file
            .file_stem()
            .and_then(|name| name.to_str())
            .filter(|name| valid_identifier(name))
            .ok_or_else(|| DbError::InvalidOperation("invalid Python handler filename".into()))?;
        if handler_file.extension().and_then(|ext| ext.to_str()) != Some("py") {
            return Err(DbError::InvalidOperation(
                "local Python handler must be a .py file".into(),
            ));
        }
        let manifest_bytes = std::fs::read(&manifest)
            .map_err(|error| DbError::Config(format!("read process manifest: {error}")))?;
        let canonical = manifest_bytes
            .strip_suffix(b"\r\n")
            .or_else(|| manifest_bytes.strip_suffix(b"\n"))
            .unwrap_or(&manifest_bytes);
        let descriptor = ProcessFunctionDescriptor::from_manifest_json(canonical)?;
        if descriptor.to_manifest_json()? != canonical {
            return Err(DbError::InvalidOperation(
                "Python worker manifest is not canonical".into(),
            ));
        }
        if descriptor.runtime != ProcessRuntime::RemotePython {
            return Err(DbError::Unsupported(
                "local Python worker requires a Python process descriptor".into(),
            ));
        }
        let code = std::fs::read(&handler_file)
            .map_err(|error| DbError::Config(format!("read process handler: {error}")))?;
        if format!("{:x}", Sha256::digest(&code)) != descriptor.implementation_digest {
            return Err(DbError::InvalidOperation(
                "Python handler file digest differs from its manifest".into(),
            ));
        }
        let module = module.to_string();
        Ok(Self {
            manifest,
            handler_file,
            module,
            descriptor,
        })
    }
}

impl LocalPythonWorker {
    /// Verify the local binding, start Python without a shell, and connect over loopback.
    ///
    /// # Errors
    /// Rejects invalid paths, mismatched artifacts, malformed readiness, or startup failure.
    pub async fn start(config: LocalPythonWorkerConfig) -> Result<Self, DbError> {
        let binding = VerifiedBinding::verify(&config)?;
        let handler_directory = binding
            .handler_file
            .parent()
            .ok_or_else(|| DbError::InvalidOperation("Python handler has no parent".into()))?;
        let mut paths = vec![handler_directory.to_path_buf()];
        for path in &config.python_paths {
            paths.push(path.canonicalize().map_err(|error| {
                DbError::Config(format!("resolve Python import root: {error}"))
            })?);
        }
        if let Some(existing) = std::env::var_os("PYTHONPATH") {
            paths.extend(std::env::split_paths(&existing));
        }
        let python_path = std::env::join_paths(paths)
            .map_err(|error| DbError::Config(format!("Python import path: {error}")))?;
        let mut command = Command::new(&config.python);
        command
            .current_dir(handler_directory)
            .args(["-m", "laminardb_process.worker", "--manifest"])
            .arg(&binding.manifest)
            .args([
                "--handler",
                &format!("{}:{}", binding.module, config.function),
                "--handler-file",
            ])
            .arg(&binding.handler_file)
            .args(["--bind", "127.0.0.1:0", "--max-in-flight"])
            .arg(config.max_in_flight.to_string())
            .env("PYTHONPATH", python_path)
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true);
        let mut child = command
            .spawn()
            .map_err(|error| DbError::Pipeline(format!("start Python process worker: {error}")))?;
        let ready = tokio::time::timeout(config.timeout, read_ready(&mut child)).await;
        let port = match ready {
            Ok(Ok(port)) => port,
            Ok(Err(error)) => {
                return Err(stop_after_start_error(&mut child, error).await);
            }
            Err(_) => {
                return Err(stop_after_start_error(
                    &mut child,
                    DbError::Pipeline("Python process worker readiness timed out".into()),
                )
                .await);
            }
        };
        let endpoint = format!("http://127.0.0.1:{port}");
        let client = match RemoteProcessClient::connect_loopback(
            &endpoint,
            binding.descriptor,
            config.max_in_flight,
            config.timeout,
        )
        .await
        {
            Ok(client) => Arc::new(client),
            Err(error) => {
                return Err(stop_after_start_error(&mut child, error).await);
            }
        };
        let cancel = CancellationToken::new();
        let exited = CancellationToken::new();
        let alive = Arc::new(AtomicBool::new(true));
        #[cfg(test)]
        let process_id = child.id().unwrap_or(0);
        let supervisor = tokio::spawn(supervise(
            child,
            cancel.clone(),
            exited.clone(),
            Arc::clone(&alive),
        ));
        Ok(Self {
            client,
            cancel,
            exited,
            alive,
            supervisor: Some(supervisor),
            #[cfg(test)]
            process_id,
        })
    }

    /// Connected client to register with the local database instance.
    #[must_use]
    pub fn client(&self) -> Arc<RemoteProcessClient> {
        Arc::clone(&self.client)
    }

    /// Whether the supervisor still owns a live child process.
    #[must_use]
    pub fn is_alive(&self) -> bool {
        self.alive.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(crate) const fn process_id(&self) -> u32 {
        self.process_id
    }

    /// Wait until the supervisor has observed and reaped the worker process.
    /// A caller that still owns the worker should treat this as a terminal event.
    pub async fn wait_for_exit(&self) {
        self.exited.cancelled().await;
    }

    /// Stop the worker and wait for process exit after the database pipeline has stopped.
    ///
    /// # Errors
    /// Reports a process wait failure or an unexpected worker exit already observed.
    pub async fn shutdown(mut self) -> Result<(), DbError> {
        self.cancel.cancel();
        let Some(supervisor) = self.supervisor.take() else {
            return Ok(());
        };
        supervisor.await.map_err(|error| {
            DbError::Pipeline(format!("Python process supervisor task: {error}"))
        })?
    }
}

impl Drop for LocalPythonWorker {
    fn drop(&mut self) {
        // The supervisor owns the child and always reaps it. Explicit `shutdown` also waits for
        // completion and reports unexpected exits; this signal covers an abandoned local owner.
        self.cancel.cancel();
    }
}

fn valid_identifier(value: &str) -> bool {
    let mut bytes = value.bytes();
    matches!(bytes.next(), Some(b'a'..=b'z' | b'A'..=b'Z' | b'_'))
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

async fn read_ready(child: &mut Child) -> Result<u16, DbError> {
    let stdout = child
        .stdout
        .as_mut()
        .ok_or_else(|| DbError::Pipeline("Python worker stdout is unavailable".into()))?;
    let mut line = Vec::with_capacity(16);
    for _ in 0..32 {
        let mut byte = [0u8];
        stdout.read_exact(&mut byte).await.map_err(|error| {
            DbError::Pipeline(format!("Python worker closed before readiness: {error}"))
        })?;
        if byte[0] == b'\n' {
            let value = std::str::from_utf8(&line)
                .ok()
                .map(|line| line.trim_end_matches('\r'))
                .and_then(|line| line.strip_prefix("READY "))
                .and_then(|port| port.parse::<u16>().ok())
                .filter(|port| *port != 0)
                .ok_or_else(|| DbError::Pipeline("invalid Python worker readiness".into()))?;
            child.stdout.take();
            return Ok(value);
        }
        line.push(byte[0]);
    }
    Err(DbError::Pipeline(
        "Python worker readiness line exceeded 32 bytes".into(),
    ))
}

async fn stop_child(child: &mut Child) -> Result<(), DbError> {
    let _ = child.start_kill();
    tokio::time::timeout(Duration::from_secs(5), child.wait())
        .await
        .map_err(|_| DbError::Pipeline("reap Python process worker timed out".into()))?
        .map_err(|error| DbError::Pipeline(format!("reap Python process worker: {error}")))?;
    Ok(())
}

async fn stop_after_start_error(child: &mut Child, primary: DbError) -> DbError {
    match stop_child(child).await {
        Ok(()) => primary,
        Err(cleanup) => DbError::Pipeline(format!("{primary}; worker cleanup: {cleanup}")),
    }
}

async fn supervise(
    mut child: Child,
    cancel: CancellationToken,
    exited: CancellationToken,
    alive: Arc<AtomicBool>,
) -> Result<(), DbError> {
    let outcome = tokio::select! {
        biased;
        status = child.wait() => {
            match status {
                Ok(status) => Err(DbError::Pipeline(format!(
                    "Python process worker exited unexpectedly with {status}"
                ))),
                Err(error) => Err(DbError::Pipeline(format!(
                    "wait for Python process worker: {error}"
                ))),
            }
        }
        () = cancel.cancelled() => {
            stop_child(&mut child).await
        }
    };
    alive.store(false, Ordering::Release);
    exited.cancel();
    outcome
}
