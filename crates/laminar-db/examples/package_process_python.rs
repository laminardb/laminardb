use std::error::Error;
use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::path::PathBuf;

use laminar_db::process_function::{
    ProcessDeterminism, ProcessFunctionDescriptor, ProcessRuntime, PythonEnvironmentBinding,
};
use sha2::{Digest, Sha256};

fn main() -> Result<(), Box<dyn Error>> {
    let mut arguments = std::env::args_os().skip(1).peekable();
    let replay_safe = arguments.next_if(|arg| arg == "--replay-safe").is_some();
    let args = arguments.map(PathBuf::from).collect::<Vec<_>>();
    let [input, output, runtime, python, handler, function, roots @ ..] = args.as_slice() else {
        return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput,
            "usage: package_process_python [--replay-safe] INPUT_MANIFEST OUTPUT_MANIFEST RUNTIME_ROOT PYTHON HANDLER_FILE FUNCTION [IMPORT_ROOT ...]").into());
    };
    let mut raw = Vec::new();
    std::fs::File::open(input)?
        .take(64 * 1024 + 1)
        .read_to_end(&mut raw)?;
    let mut descriptor = ProcessFunctionDescriptor::from_manifest_json(&raw)?;
    if descriptor.runtime != ProcessRuntime::RemotePython {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "packaging requires a Python descriptor",
        )
        .into());
    }
    let handler = handler.canonicalize()?;
    if handler.extension().and_then(|extension| extension.to_str()) != Some("py") {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "handler must be a .py file",
        )
        .into());
    }
    let handler_directory = handler.parent().ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "handler has no parent")
    })?;
    let mut import_roots = vec![handler_directory.to_path_buf()];
    for root in roots {
        import_roots.push(root.canonicalize()?);
    }
    let output_parent = output
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or(std::path::Path::new("."));
    let output_name = output.file_name().ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "output has no filename")
    })?;
    let output = output_parent.canonicalize()?.join(output_name);
    if output.starts_with(runtime.canonicalize()?)
        || import_roots.iter().any(|root| output.starts_with(root))
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "output manifest must be outside the hashed trees",
        )
        .into());
    }
    descriptor.python_environment = Some(PythonEnvironmentBinding::capture(
        runtime,
        python,
        &format!(
            "{}:{}",
            handler
                .file_stem()
                .and_then(|stem| stem.to_str())
                .ok_or_else(|| std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "handler filename must be UTF-8"
                ))?,
            function.to_str().ok_or_else(|| std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "function must be UTF-8"
            ))?
        ),
        &import_roots,
    )?);
    let mut source = std::fs::File::open(&handler)?.take(512 * 1024 * 1024 + 1);
    let mut digest = Sha256::new();
    if std::io::copy(&mut source, &mut digest)? > 512 * 1024 * 1024 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "handler file exceeds 512 MiB",
        )
        .into());
    }
    descriptor.implementation_digest = format!("{:x}", digest.finalize());
    if replay_safe {
        descriptor.determinism = ProcessDeterminism::ReplaySafe;
    }
    let manifest = descriptor.to_manifest_json()?;
    // A new output path preserves the reviewed input manifest and any earlier package.
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&output)?
        .write_all(&manifest)?;
    println!("Wrote {}", output.display());
    Ok(())
}
