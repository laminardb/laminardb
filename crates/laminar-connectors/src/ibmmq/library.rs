//! IBM MQ client library, loaded once per process at run time.
//!
//! `LaminarDB` does not bundle IBM software: the operator installs the IBM MQ client (the
//! redistributable client, the MQ server, or the macOS toolkit) and either points
//! `client.library` at `libmqm_r`, sets `MQ_INSTALLATION_PATH`, or puts the library on the
//! platform loader path.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use dlopen2::wrapper::Container;
use libmqm_sys::dlopen2::MqWrapper;

use crate::error::ConnectorError;

/// The loaded MQI entry points; connections hold a clone.
pub(super) type Library = Arc<Container<MqWrapper>>;

const FILE_NAME: &str = if cfg!(windows) {
    "mqm.dll"
} else if cfg!(target_os = "macos") {
    "libmqm_r.dylib"
} else {
    "libmqm_r.so"
};

/// The loaded library; a failed load is not remembered, so a corrected path can be retried.
static LOADED: parking_lot::Mutex<Option<(PathBuf, Library)>> = parking_lot::const_mutex(None);

/// The library path a configuration selects.
fn resolve(configured: Option<&Path>) -> PathBuf {
    if let Some(path) = configured {
        return path.to_path_buf();
    }
    match std::env::var_os("MQ_INSTALLATION_PATH") {
        Some(root) if cfg!(windows) => PathBuf::from(root).join("bin64").join(FILE_NAME),
        Some(root) => PathBuf::from(root).join("lib64").join(FILE_NAME),
        None => PathBuf::from(FILE_NAME),
    }
}

/// Load the client library, or return the one this process already loaded.
///
/// # Errors
/// Returns a configuration error when the library cannot be loaded or a different library is
/// requested after one was loaded.
pub(super) fn load(configured: Option<&Path>) -> Result<Library, ConnectorError> {
    let wanted = resolve(configured);
    let mut loaded = LOADED.lock();
    if let Some((path, library)) = loaded.as_ref() {
        if *path != wanted {
            return Err(ConnectorError::ConfigurationError(format!(
                "this process already loaded the IBM MQ client from '{}'; one client library per \
                 process is supported, so '{}' cannot also be used",
                path.display(),
                wanted.display()
            )));
        }
        return Ok(Arc::clone(library));
    }
    // SAFETY: this runs the IBM MQ client's library initializers. The library is IBM's supported
    // MQI runtime, loaded once and kept in a process-lifetime static, so its function pointers
    // stay valid for every connection created from it.
    let library = unsafe { Container::<MqWrapper>::load(&wanted) }.map_err(|error| {
        ConnectorError::ConfigurationError(format!(
            "cannot load the IBM MQ client library '{}': {error}; install the IBM MQ client and \
             set 'client.library' or MQ_INSTALLATION_PATH",
            wanted.display()
        ))
    })?;
    let library = Arc::new(library);
    *loaded = Some((wanted, Arc::clone(&library)));
    Ok(library)
}
