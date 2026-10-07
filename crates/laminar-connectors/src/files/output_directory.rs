//! File sink namespace initialization and monotone output generations.

use crate::error::ConnectorError;
use std::path::Path;

pub(super) fn initialise_output_directory(
    dir: &Path,
    prefix: &str,
    extension: &str,
) -> Result<u64, ConnectorError> {
    std::fs::create_dir_all(dir).map_err(|e| {
        ConnectorError::WriteError(format!(
            "cannot create output directory '{}': {e}",
            dir.display()
        ))
    })?;
    if !dir.is_dir() {
        return Err(ConnectorError::WriteError(format!(
            "file sink output '{}' is not a directory",
            dir.display()
        )));
    }
    scan_next_generation(dir, prefix, extension)
}

/// Finds a generation strictly above every existing final or temporary file
/// for this exact prefix and format. Temporary files are deliberately retained:
/// deleting them during `open` could destroy a still-live writer that was
/// accidentally configured with the same target. They can be garbage-collected
/// only by an operator after establishing exclusive ownership of the target.
/// Malformed unrelated names are ignored; `create_new` and final-path
/// no-overwrite checks remain authoritative at publication time.
pub(super) fn scan_next_generation(
    dir: &Path,
    prefix: &str,
    extension: &str,
) -> Result<u64, ConnectorError> {
    let entries = std::fs::read_dir(dir).map_err(|e| {
        ConnectorError::WriteError(format!(
            "cannot scan output directory '{}': {e}",
            dir.display()
        ))
    })?;
    let name_prefix = format!("{prefix}_");
    let final_suffix = format!(".{extension}");
    let temporary_suffix = format!(".{extension}.tmp");
    let mut highest = None::<u64>;
    for entry in entries {
        let entry = entry.map_err(|e| {
            ConnectorError::WriteError(format!(
                "cannot read an entry in output directory '{}': {e}",
                dir.display()
            ))
        })?;
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        let Some(name) = name.strip_prefix(&name_prefix) else {
            continue;
        };
        let body = name
            .strip_suffix(&temporary_suffix)
            .or_else(|| name.strip_suffix(&final_suffix));
        let Some(body) = body else { continue };
        let Some((generation, segment)) = body.rsplit_once('_') else {
            continue;
        };
        if segment.parse::<usize>().is_err() {
            continue;
        }
        let Ok(generation) = generation.parse::<u64>() else {
            continue;
        };
        highest = Some(highest.map_or(generation, |current| current.max(generation)));
    }
    highest
        .unwrap_or(0)
        .checked_add(1)
        .ok_or_else(|| ConnectorError::WriteError("file sink generation space is exhausted".into()))
}
