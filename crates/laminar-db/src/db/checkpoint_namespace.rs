//! One OS lease for the local checkpoint and schema-contract namespace.

use super::{LaminarDB, RuntimeMode};
use crate::error::DbError;
use laminar_connectors::storage::StorageProvider;

impl LaminarDB {
    pub(crate) fn ensure_local_checkpoint_namespace(&self) -> Result<(), DbError> {
        if self.runtime_mode() != RuntimeMode::Local {
            return Ok(());
        }
        let Some(config) = &self.config.checkpoint else {
            return Ok(());
        };
        let root = match self.config.object_store_url.as_deref() {
            Some(url) if StorageProvider::detect_uri(url) == Some(StorageProvider::Local) => {
                laminar_core::checkpoint::object_store_builder::file_url_path(url)
                    .map_err(|error| DbError::Config(format!("checkpoint directory: {error}")))?
            }
            Some(_) => return Ok(()),
            None => config
                .data_dir
                .clone()
                .or_else(|| self.config.storage_dir.clone())
                .unwrap_or_else(|| std::path::PathBuf::from("./data")),
        };
        let mut namespace = self.checkpoint_namespace_lock.lock();
        if namespace.is_some() {
            return Ok(());
        }
        laminar_core::durable_fs::ensure_durable_directory(&root)
            .map_err(|error| DbError::Config(format!("checkpoint directory: {error}")))?;
        let lock = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(root.join(".laminardb-checkpoint.lock"))
            .map_err(|error| {
                DbError::Config(format!("[LDB-0014] checkpoint namespace lock: {error}"))
            })?;
        lock.try_lock().map_err(|error| {
            DbError::Config(format!(
                "[LDB-0014] checkpoint namespace is already owned by another live process: {error}"
            ))
        })?;
        *namespace = Some(std::sync::Arc::new(lock));
        Ok(())
    }
}
