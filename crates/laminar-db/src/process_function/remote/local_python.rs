use std::ffi::OsString;
use std::io::Read;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::io::AsyncReadExt;
use tokio::process::{Child, Command};
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

use super::python_environment::{self, FileGuards, VerifiedEnvironment};
use super::{RemoteProcessClient, MAX_IN_FLIGHT};
use crate::error::DbError;
use crate::process_function::descriptor::{valid_python_identifier, MAX_MANIFEST_BYTES};
use crate::process_function::{ProcessDeterminism, ProcessFunctionDescriptor, ProcessRuntime};

/// Explicit local Python worker launch. The handler file is the direct digest-bound artifact;
/// an optional environment binding checks deployment drift. A replay-safe descriptor additionally
/// requires Linux, an unprivileged child, and a package on the read-only root filesystem.
/// Bound paths reject links in their ancestry before canonicalization. On Windows, bound launches
/// also retain read-share handles to inventoried files and configured path ancestors.
/// Bound launches compile filesystem source modules without reading their bytecode caches.
#[derive(Clone)]
pub struct LocalPythonWorkerConfig {
    /// Python executable or a trusted executable name resolved by the host environment.
    /// Environment-bound launches require a file path inside `runtime_root`.
    pub python: PathBuf,
    /// Complete interpreter installation to check against the descriptor's environment binding.
    /// Supply this together with `python_environment`; exclude the function manifest from it.
    /// On Windows, existing files and configured path ancestors remain guarded during supervision.
    pub runtime_root: Option<PathBuf>,
    /// Canonical descriptor manifest consumed by the worker.
    pub manifest: PathBuf,
    /// Top-level Python module file containing the handler function.
    pub handler_file: PathBuf,
    /// Function name exported by `handler_file`.
    pub function: String,
    /// Explicit import roots for the SDK and handler dependencies. The child does not inherit
    /// the host's `PYTHONPATH` or Python user site.
    pub python_paths: Vec<PathBuf>,
    /// Maximum simultaneous worker calls. The process and client use the same limit.
    /// The launcher sets `OMP_NUM_THREADS=1` and `OPENBLAS_NUM_THREADS=1` before imports.
    pub max_in_flight: usize,
    /// Worker readiness and per-call deadline, between 1 millisecond and 30 seconds.
    /// Filesystem binding verification precedes the readiness deadline.
    pub timeout: Duration,
}

/// One child process with an explicit shutdown owner. Unexpected exit is observed by the
/// supervisor and outstanding calls fail through the connected transport.
/// Cancelling startup signals the same supervisor to stop and reap the child.
pub struct LocalPythonWorker {
    client: Arc<RemoteProcessClient>,
    cancel: CancellationToken,
    exited: CancellationToken,
    alive: Arc<AtomicBool>,
    supervisor: Option<tokio::task::JoinHandle<Result<(), DbError>>>,
    #[cfg(test)]
    process_id: u32,
    #[cfg(test)]
    endpoint: String,
}

struct VerifiedBinding {
    manifest: PathBuf,
    handler_file: PathBuf,
    module: String,
    descriptor: ProcessFunctionDescriptor,
    environment: VerifiedEnvironment,
}

type StartupResult = Result<(Arc<RemoteProcessClient>, u16), DbError>;

impl VerifiedBinding {
    fn verify(config: &LocalPythonWorkerConfig) -> Result<Self, DbError> {
        if config.max_in_flight == 0
            || config.max_in_flight > MAX_IN_FLIGHT
            || config.timeout < Duration::from_millis(1)
            || config.timeout > Duration::from_secs(30)
            || !valid_python_identifier(&config.function)
        {
            return Err(DbError::InvalidOperation(
                "invalid local process worker limits or handler function".into(),
            ));
        }
        let mut guards = FileGuards::default();
        let (manifest, handler_file) =
            if config.runtime_root.is_some() {
                (
                    guards.canonical_file(&config.manifest)?,
                    guards.canonical_file(&config.handler_file)?,
                )
            } else {
                let manifest = config.manifest.canonicalize().map_err(|error| {
                    DbError::Config(format!("resolve process manifest: {error}"))
                })?;
                let handler_file = config.handler_file.canonicalize().map_err(|error| {
                    DbError::Config(format!("resolve process handler: {error}"))
                })?;
                (manifest, handler_file)
            };
        let module = handler_file
            .file_stem()
            .and_then(|name| name.to_str())
            .filter(|name| valid_python_identifier(name))
            .ok_or_else(|| DbError::InvalidOperation("invalid Python handler filename".into()))?;
        if handler_file.extension().and_then(|ext| ext.to_str()) != Some("py") {
            return Err(DbError::InvalidOperation(
                "local Python handler must be a .py file".into(),
            ));
        }
        let mut manifest_bytes = Vec::new();
        let manifest_limit = u64::try_from(MAX_MANIFEST_BYTES)
            .map_err(|error| DbError::Config(format!("process manifest size limit: {error}")))?;
        let mut manifest_file = FileGuards::open_file(&manifest)?;
        (&mut manifest_file)
            .take(manifest_limit + 3)
            .read_to_end(&mut manifest_bytes)
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
        if descriptor.determinism == ProcessDeterminism::ReplaySafe {
            guards = FileGuards::for_replay()?;
            // Reopen under the stronger filesystem contract before using the binding.
            guards.canonical_file(&manifest)?;
            guards.canonical_file(&handler_file)?;
        }
        if python_environment::file_sha256(&handler_file)? != descriptor.implementation_digest {
            return Err(DbError::InvalidOperation(
                "Python handler file digest differs from its manifest".into(),
            ));
        }
        let module = module.to_string();
        let handler_directory = handler_file
            .parent()
            .ok_or_else(|| DbError::InvalidOperation("Python handler has no parent".into()))?;
        let environment = python_environment::verify(
            config,
            &descriptor,
            handler_directory,
            &manifest,
            &module,
            guards,
        )?;
        Ok(Self {
            manifest,
            handler_file,
            module,
            descriptor,
            environment,
        })
    }

    fn command(&self, config: &LocalPythonWorkerConfig) -> Result<Command, DbError> {
        let mut command = Command::new(&self.environment.python);
        if let Some(root) = &self.environment.runtime_root {
            let paths = serde_json::to_string(&self.environment.import_roots).map_err(|error| {
                DbError::Config(format!("encode bound Python import paths: {error}"))
            })?;
            // A regular-file prefix makes source cache paths unresolvable, including imports
            // before bootstrap. The inventoried executable is already guarded on Windows.
            let mut cache_prefix = OsString::from("pycache_prefix=");
            cache_prefix.push(&self.environment.python);
            command.args(
                if self.descriptor.determinism == ProcessDeterminism::ReplaySafe {
                    ["-s", "-P", "-S", "-B"]
                } else {
                    ["-I", "-S", "-B", "-X"]
                },
            );
            if self.descriptor.determinism == ProcessDeterminism::ReplaySafe {
                command.env_clear().env("PYTHONHASHSEED", "0").arg("-X");
                #[cfg(target_os = "linux")]
                command.env("LD_LIBRARY_PATH", root.join("lib"));
            }
            command
                .arg(cache_prefix)
                .arg("-c")
                .arg(include_str!("python_environment/bootstrap.py"))
                .arg(root)
                .arg(paths)
                .arg("laminardb_process.worker")
                .env_remove("PYTHONPATH");
        } else {
            let paths = std::env::join_paths(&self.environment.import_roots)
                .map_err(|error| DbError::Config(format!("Python import path: {error}")))?;
            command
                .args(["-s", "-P", "-m", "laminardb_process.worker"])
                .env("PYTHONPATH", paths);
        }
        command
            .current_dir(&self.environment.import_roots[0])
            .arg("--manifest")
            .arg(&self.manifest)
            .args([
                "--handler",
                &format!("{}:{}", self.module, config.function),
                "--handler-file",
            ])
            .arg(&self.handler_file)
            .args(["--bind", "127.0.0.1:0", "--max-in-flight"])
            .arg(config.max_in_flight.to_string())
            .env_remove("PYTHONHOME")
            .env("OMP_NUM_THREADS", "1")
            .env("OPENBLAS_NUM_THREADS", "1")
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true);
        #[cfg(target_os = "linux")]
        if self.descriptor.determinism == ProcessDeterminism::ReplaySafe {
            // SAFETY: the child hook performs only the async-signal-safe prctl syscall. All
            // other replay checks run before spawn or in the initialized Python bootstrap.
            unsafe {
                command.pre_exec(|| {
                    if libc::prctl(
                        libc::PR_SET_NO_NEW_PRIVS,
                        1 as libc::c_ulong,
                        0 as libc::c_ulong,
                        0 as libc::c_ulong,
                        0 as libc::c_ulong,
                    ) == 0
                    {
                        Ok(())
                    } else {
                        Err(std::io::Error::last_os_error())
                    }
                });
            }
        }
        Ok(command)
    }
}

impl LocalPythonWorker {
    /// Verify the local binding, start Python without a shell, and connect over loopback.
    ///
    /// # Errors
    /// Rejects invalid paths, mismatched artifacts, malformed readiness, or startup failure.
    pub async fn start(config: LocalPythonWorkerConfig) -> Result<Self, DbError> {
        let (config, binding) = tokio::task::spawn_blocking(move || {
            let binding = VerifiedBinding::verify(&config)?;
            Ok::<_, DbError>((config, binding))
        })
        .await
        .map_err(|error| DbError::Pipeline(format!("verify Python process binding: {error}")))??;
        let child = binding
            .command(&config)?
            .spawn()
            .map_err(|error| DbError::Pipeline(format!("start Python process worker: {error}")))?;
        let cancel = CancellationToken::new();
        let startup_cancel = cancel.clone().drop_guard();
        let exited = CancellationToken::new();
        let alive = Arc::new(AtomicBool::new(true));
        #[cfg(test)]
        let process_id = child.id().unwrap_or(0);
        let (ready_tx, ready_rx) = oneshot::channel();
        // Transfer the child and file handles before the first await after spawn. Cancelling
        // startup signals this same cleanup owner, which retains handles while reaping.
        let supervisor = tokio::spawn(supervise(
            child,
            binding,
            config,
            ready_tx,
            cancel.clone(),
            exited.clone(),
            Arc::clone(&alive),
        ));
        let (client, _port) = match ready_rx
            .await
            .map_err(|error| DbError::Pipeline(format!("Python worker startup channel: {error}")))
            .and_then(|result| result)
        {
            Ok(ready) => ready,
            Err(primary) => {
                return match supervisor.await {
                    Ok(Ok(())) => Err(primary),
                    Ok(Err(cleanup)) => Err(DbError::Pipeline(format!(
                        "{primary}; worker cleanup: {cleanup}"
                    ))),
                    Err(error) => Err(DbError::Pipeline(format!(
                        "{primary}; worker supervisor: {error}"
                    ))),
                };
            }
        };
        startup_cancel.disarm();
        Ok(Self {
            client,
            cancel,
            exited,
            alive,
            supervisor: Some(supervisor),
            #[cfg(test)]
            process_id,
            #[cfg(test)]
            endpoint: format!("http://127.0.0.1:{_port}"),
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

    #[cfg(test)]
    pub(crate) fn loopback_endpoint(&self) -> &str {
        &self.endpoint
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

async fn connect_child(
    child: &mut Child,
    descriptor: ProcessFunctionDescriptor,
    config: &LocalPythonWorkerConfig,
    exited: CancellationToken,
) -> StartupResult {
    let port = tokio::time::timeout(config.timeout, read_ready(child))
        .await
        .map_err(|_| DbError::Pipeline("Python process worker readiness timed out".into()))??;
    let replay_safe = descriptor.determinism == ProcessDeterminism::ReplaySafe;
    let mut client = RemoteProcessClient::connect_loopback(
        &format!("http://127.0.0.1:{port}"),
        descriptor,
        config.max_in_flight,
        config.timeout,
    )
    .await?;
    if replay_safe {
        client.bind_python_replay_lifetime(exited);
    }
    Ok((Arc::new(client), port))
}

async fn supervise(
    mut child: Child,
    binding: VerifiedBinding,
    config: LocalPythonWorkerConfig,
    ready: oneshot::Sender<StartupResult>,
    cancel: CancellationToken,
    exited: CancellationToken,
    alive: Arc<AtomicBool>,
) -> Result<(), DbError> {
    let startup = tokio::select! {
        biased;
        () = cancel.cancelled() => Err(DbError::Pipeline("Python worker startup cancelled".into())),
        result = connect_child(&mut child, binding.descriptor, &config, exited.clone()) => result,
    };
    let outcome = match startup {
        Ok(client) => {
            if ready.send(Ok(client)).is_err() {
                cancel.cancel();
            }
            observe_child(&mut child, &cancel).await
        }
        Err(primary) => {
            let error = stop_after_start_error(&mut child, primary).await;
            let _ = ready.send(Err(error));
            Ok(())
        }
    };
    drop(binding.environment);
    alive.store(false, Ordering::Release);
    exited.cancel();
    outcome
}

async fn observe_child(child: &mut Child, cancel: &CancellationToken) -> Result<(), DbError> {
    tokio::select! {
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
            stop_child(child).await
        }
    }
}
