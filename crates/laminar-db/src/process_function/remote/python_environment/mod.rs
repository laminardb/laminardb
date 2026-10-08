use std::path::{Path, PathBuf};

use super::LocalPythonWorkerConfig;
use crate::error::DbError;
use crate::process_function::descriptor::{valid_python_handler, MAX_PYTHON_IMPORT_ROOTS};
use crate::process_function::{ProcessFunctionDescriptor, PythonEnvironmentBinding};

mod file_guards;
mod tree;

pub(super) use file_guards::FileGuards;
pub(super) use tree::file_sha256;
use tree::{canonical_directory, fingerprint_guarded, InventoryBudget};

#[cfg(all(test, target_os = "linux"))]
pub(crate) fn read_only_fixture_config() -> LocalPythonWorkerConfig {
    let package = std::path::PathBuf::from(
        std::env::var_os("LAMINAR_PROCESS_REPLAY_PACKAGE")
            .expect("set LAMINAR_PROCESS_REPLAY_PACKAGE to the read-only qualification package"),
    );
    LocalPythonWorkerConfig {
        python: package.join("runtime/bin/python3.13"),
        runtime_root: Some(package.join("runtime")),
        manifest: package.join("manifest.json"),
        handler_file: package.join("handler/replay_handler.py"),
        function: "handle".into(),
        python_paths: vec![package.join("runtime/lib/python3.13/site-packages")],
        max_in_flight: 4,
        timeout: std::time::Duration::from_secs(10),
    }
}

pub(super) struct VerifiedEnvironment {
    pub(super) python: PathBuf,
    pub(super) runtime_root: Option<PathBuf>,
    pub(super) import_roots: Vec<PathBuf>,
    pub(super) _guards: FileGuards,
}

impl PythonEnvironmentBinding {
    /// Fingerprint a quiescent runtime installation and ordered import trees, including all
    /// source, bytecode, native libraries and data. The first import root is the handler directory.
    /// `handler` selects the exact `module:function` entry point in that directory.
    /// This performs blocking filesystem work; call it during packaging or on a blocking task.
    /// On Windows, read-share handles exclude writers until capture returns. Directory additions
    /// still require a quiescent deployment, and capture does not retain lifetime protection.
    ///
    /// # Errors
    /// Rejects an interpreter outside the runtime tree, links/reparse points (including path
    /// ancestors), more than 128 ancestors per configured path, non-UTF-8 paths, more than
    /// 16 import roots, 32,768 total entries, 4 GiB total bytes or a 512 MiB file.
    /// Windows also rejects files with an existing incompatible write/delete handle.
    pub fn capture(
        runtime_root: &Path,
        python: &Path,
        handler: &str,
        import_roots: &[PathBuf],
    ) -> Result<Self, DbError> {
        capture(
            runtime_root,
            python,
            handler,
            import_roots,
            &mut FileGuards::default(),
        )
    }
}

fn capture(
    runtime_root: &Path,
    python: &Path,
    handler: &str,
    import_roots: &[PathBuf],
    guards: &mut FileGuards,
) -> Result<PythonEnvironmentBinding, DbError> {
    if import_roots.is_empty() || import_roots.len() > MAX_PYTHON_IMPORT_ROOTS {
        return Err(DbError::Config(
            "Python binding requires 1..=16 import roots".into(),
        ));
    }
    if !valid_python_handler(handler) {
        return Err(DbError::Config("invalid Python environment handler".into()));
    }
    let runtime_root = canonical_directory(runtime_root, guards)?;
    let python = guards.canonical_file(python)?;
    let relative = python
        .strip_prefix(&runtime_root)
        .map_err(|_| DbError::Config("Python executable is outside the runtime root".into()))?;
    let executable = relative
        .to_str()
        .ok_or_else(|| DbError::Config("Python executable path must be UTF-8".into()))?
        .replace(std::path::MAIN_SEPARATOR, "/");
    let mut budget = InventoryBudget::default();
    let runtime_sha256 = fingerprint_guarded(&runtime_root, &mut budget, guards)?;
    let mut import_roots_sha256 = Vec::with_capacity(import_roots.len());
    for root in import_roots {
        import_roots_sha256.push(fingerprint_guarded(
            &canonical_directory(root, guards)?,
            &mut budget,
            guards,
        )?);
    }
    let binding = PythonEnvironmentBinding {
        version: 1,
        executable,
        handler: handler.to_owned(),
        runtime_sha256,
        import_roots_sha256,
    };
    binding.validate()?;
    Ok(binding)
}

pub(super) fn verify(
    config: &LocalPythonWorkerConfig,
    descriptor: &ProcessFunctionDescriptor,
    handler_directory: &Path,
    manifest: &Path,
    module: &str,
    mut guards: FileGuards,
) -> Result<VerifiedEnvironment, DbError> {
    if config.python_paths.len() >= MAX_PYTHON_IMPORT_ROOTS {
        return Err(DbError::Config(
            "Python worker supports at most 15 explicit import roots".into(),
        ));
    }
    let mut import_roots = vec![handler_directory.to_path_buf()];
    for root in &config.python_paths {
        let root = if descriptor.python_environment.is_some() {
            canonical_directory(root, &mut guards)?
        } else {
            root.canonicalize()
                .map_err(|error| DbError::Config(format!("resolve Python import root: {error}")))?
        };
        import_roots.push(root);
    }
    match (&descriptor.python_environment, &config.runtime_root) {
        (None, None) => Ok(VerifiedEnvironment {
            python: config.python.clone(),
            runtime_root: None,
            import_roots,
            _guards: guards,
        }),
        (Some(expected), Some(root)) => {
            let runtime_root = canonical_directory(root, &mut guards)?;
            if manifest.starts_with(&runtime_root)
                || import_roots.iter().any(|root| manifest.starts_with(root))
            {
                return Err(DbError::Config(
                    "bound process manifest must be outside the hashed trees".into(),
                ));
            }
            let actual = capture(
                &runtime_root,
                &config.python,
                &format!("{module}:{}", config.function),
                &import_roots,
                &mut guards,
            )?;
            if &actual != expected {
                return Err(DbError::Config(
                    "Python environment differs from its process manifest".into(),
                ));
            }
            let python = runtime_root.join(&actual.executable);
            Ok(VerifiedEnvironment {
                python,
                runtime_root: Some(runtime_root),
                import_roots,
                _guards: guards,
            })
        }
        _ => Err(DbError::Config(
            "Python environment binding and runtime_root must be supplied together".into(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn deployment(root: &Path) -> (LocalPythonWorkerConfig, ProcessFunctionDescriptor) {
        let root = root.canonicalize().unwrap();
        let runtime = root.join("runtime");
        let handlers = root.join("handlers");
        let sdk = root.join("sdk");
        for path in [&runtime, &handlers, &sdk] {
            std::fs::create_dir(path).unwrap();
        }
        let python = runtime.join("python");
        std::fs::write(&python, b"interpreter").unwrap();
        std::fs::write(runtime.join("stdlib.py"), b"runtime").unwrap();
        let handler_file = handlers.join("handler.py");
        std::fs::write(&handler_file, b"handler").unwrap();
        std::fs::write(sdk.join("worker.py"), b"sdk").unwrap();
        let manifest = root.join("manifest.json");
        std::fs::write(&manifest, []).unwrap();
        let mut descriptor = ProcessFunctionDescriptor::from_manifest_json(
            &std::fs::read(
                PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                    .join("../../examples/process_python/manifest.json"),
            )
            .unwrap(),
        )
        .unwrap();
        descriptor.python_environment = Some(
            PythonEnvironmentBinding::capture(
                &runtime,
                &python,
                "handler:handle",
                &[handlers, sdk.clone()],
            )
            .unwrap(),
        );
        (
            LocalPythonWorkerConfig {
                python,
                runtime_root: Some(runtime),
                manifest,
                handler_file,
                function: "handle".into(),
                python_paths: vec![sdk],
                max_in_flight: 1,
                timeout: Duration::from_secs(5),
            },
            descriptor,
        )
    }

    fn check(
        config: &LocalPythonWorkerConfig,
        descriptor: &ProcessFunctionDescriptor,
    ) -> Result<VerifiedEnvironment, DbError> {
        verify(
            config,
            descriptor,
            config.handler_file.parent().unwrap(),
            &config.manifest,
            "handler",
            FileGuards::default(),
        )
    }

    #[test]
    fn replay_binding_rejects_writable_deployment() {
        let root = tempfile::tempdir().unwrap();
        let (config, _) = deployment(root.path());
        let result = FileGuards::for_replay()
            .and_then(|mut guards| guards.canonical_file(&config.python).map(|_| ()));
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("read-only"));
    }

    #[test]
    fn binding_rejects_runtime_sdk_addition_and_removal_drift() {
        let root = tempfile::tempdir().unwrap();
        let (config, descriptor) = deployment(root.path());
        check(&config, &descriptor).unwrap();
        let stdlib = config.runtime_root.as_ref().unwrap().join("stdlib.py");
        std::fs::write(&stdlib, b"changed").unwrap();
        assert!(check(&config, &descriptor)
            .err()
            .unwrap()
            .to_string()
            .contains("environment differs"));
        std::fs::write(&stdlib, b"runtime").unwrap();
        let sdk = config.python_paths[0].join("worker.py");
        std::fs::write(&sdk, b"SDK").unwrap();
        assert!(check(&config, &descriptor).is_err());
        std::fs::write(&sdk, b"sdk").unwrap();
        let extra = config.python_paths[0].join("new.pyc");
        std::fs::write(&extra, b"new import").unwrap();
        assert!(check(&config, &descriptor).is_err());
        std::fs::remove_file(extra).unwrap();
        check(&config, &descriptor).unwrap();
        std::fs::remove_file(sdk).unwrap();
        assert!(check(&config, &descriptor).is_err());
    }

    #[test]
    fn binding_is_relocatable_and_requires_both_launch_and_manifest_fields() {
        let first = tempfile::tempdir().unwrap();
        let second = tempfile::tempdir().unwrap();
        let (config, descriptor) = deployment(first.path());
        let (mut moved, equivalent) = deployment(second.path());
        assert_eq!(descriptor.python_environment, equivalent.python_environment);
        check(&moved, &descriptor).unwrap();
        moved.runtime_root = None;
        assert!(check(&moved, &descriptor).is_err());
        let mut unbound = descriptor.clone();
        unbound.python_environment = None;
        assert!(check(&config, &unbound).is_err());
        check(&moved, &unbound).unwrap();
        let mut changed_entrypoint = config;
        changed_entrypoint.function = "different_handler".into();
        assert!(check(&changed_entrypoint, &descriptor).is_err());
    }

    #[test]
    fn binding_rejects_uncontained_executable_and_self_referential_manifest() {
        let root = tempfile::tempdir().unwrap();
        let (mut config, descriptor) = deployment(root.path());
        let outside = root.path().join("outside");
        std::fs::write(&outside, b"interpreter").unwrap();
        assert!(PythonEnvironmentBinding::capture(
            config.runtime_root.as_ref().unwrap(),
            &outside,
            "handler:handle",
            &config.python_paths
        )
        .unwrap_err()
        .to_string()
        .contains("outside"));
        config.manifest = config.python_paths[0].join("manifest.json");
        assert!(check(&config, &descriptor)
            .err()
            .unwrap()
            .to_string()
            .contains("outside the hashed trees"));
        assert!(PythonEnvironmentBinding::capture(
            config.runtime_root.as_ref().unwrap(),
            &config.python,
            "handler:handle",
            &vec![config.python_paths[0].clone(); 17]
        )
        .is_err());
    }

    #[cfg(windows)]
    #[test]
    fn verified_environment_retains_file_guards_and_failed_checks_release_them() {
        let root = tempfile::tempdir().unwrap();
        let (config, descriptor) = deployment(root.path());
        let files = [
            config.python.clone(),
            config.runtime_root.as_ref().unwrap().join("stdlib.py"),
            config.handler_file.clone(),
            config.python_paths[0].join("worker.py"),
        ];
        let environment = check(&config, &descriptor).unwrap();
        for file in &files {
            assert_eq!(
                std::fs::write(file, b"changed").unwrap_err().raw_os_error(),
                Some(32)
            );
        }
        drop(environment);
        std::fs::write(&files[3], b"changed").unwrap();
        assert!(check(&config, &descriptor).is_err());
        for file in &files {
            std::fs::OpenOptions::new().write(true).open(file).unwrap();
        }
    }
}
