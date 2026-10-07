//! Local deployment schema authority in the existing checkpoint namespace.

use std::collections::BTreeMap;
use std::sync::Arc;

use futures::StreamExt;
use laminar_core::schema_binding::SchemaBinding;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, UpdateVersion};
use serde::{Deserialize, Serialize};

use crate::db::{LaminarDB, RuntimeMode};
use crate::error::DbError;

const JOURNAL_PATH: &str = "catalog/schema-contracts-v1.json";
const MAX_BYTES: usize = 8 * 1024 * 1024;
const MAX_OBJECTS: usize = 4096;

#[derive(Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Journal {
    version: u16,
    objects: BTreeMap<String, Entry>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Entry {
    generation: u64,
    ddl: String,
    binding: Option<SchemaBinding>,
}

pub(super) struct SchemaJournal {
    store: Arc<dyn ObjectStore>,
    state: Journal,
    revision: Option<UpdateVersion>,
    exclusive_local: bool,
}

impl SchemaJournal {
    pub(super) async fn load(db: &LaminarDB) -> Result<Option<Self>, DbError> {
        if db.runtime_mode() != RuntimeMode::Local || db.config.checkpoint.is_none() {
            return Ok(None);
        }
        db.ensure_local_checkpoint_namespace()?;
        let store = db.checkpoint_object_store()?.ok_or_else(|| {
            DbError::Checkpoint("durable schema authority has no checkpoint store".into())
        })?;
        let mut journal = Self {
            store,
            state: Journal {
                version: 1,
                ..Journal::default()
            },
            revision: None,
            exclusive_local: db.checkpoint_namespace_lock.lock().is_some(),
        };
        let result = match journal.store.get(&JOURNAL_PATH.into()).await {
            Ok(result) => result,
            Err(object_store::Error::NotFound { .. }) => return Ok(Some(journal)),
            Err(error) => return Err(journal_error(&error)),
        };
        if result.meta.size > MAX_BYTES as u64 {
            return Err(DbError::Checkpoint("schema journal exceeds 8 MiB".into()));
        }
        journal.revision = Some(UpdateVersion {
            e_tag: result.meta.e_tag.clone(),
            version: result.meta.version.clone(),
        });
        let mut stream = result.into_stream();
        let mut bytes = Vec::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|error| journal_error(&error))?;
            if bytes.len().saturating_add(chunk.len()) > MAX_BYTES {
                return Err(DbError::Checkpoint("schema journal exceeds 8 MiB".into()));
            }
            bytes.extend_from_slice(&chunk);
        }
        journal.state = serde_json::from_slice(&bytes).map_err(|_| DbError::Checkpoint(
            "invalid schema journal; preserve the checkpoint namespace and perform a controlled migration".into()))?;
        if journal.state.version != 1 || journal.state.objects.len() > MAX_OBJECTS {
            return Err(DbError::Checkpoint(
                "unsupported schema journal version or object limit".into(),
            ));
        }
        for entry in journal.state.objects.values() {
            if entry.generation == 0 {
                return Err(DbError::Checkpoint("zero schema generation".into()));
            }
            if let Some(binding) = &entry.binding {
                binding
                    .canonical_bytes()
                    .map_err(|error| DbError::Checkpoint(error.to_string()))?;
            }
        }
        Ok(Some(journal))
    }

    pub(super) fn generation(&self, name: &str) -> Option<u64> {
        self.state.objects.get(name).map(|entry| entry.generation)
    }

    pub(super) fn replay(&self, name: &str, ddl: &str) -> Result<Option<SchemaBinding>, DbError> {
        let Some(entry) = self.state.objects.get(name) else {
            return Ok(None);
        };
        if entry.binding.is_none() {
            return Ok(None);
        }
        if entry.ddl != ddl {
            return Err(DbError::Checkpoint(format!(
                "durable definition for '{name}' differs from the original DDL; use a controlled DROP/CREATE migration")));
        }
        Ok(entry.binding.clone())
    }

    pub(super) async fn publish(
        &mut self,
        name: &str,
        ddl: &str,
        binding: Option<SchemaBinding>,
    ) -> Result<(), DbError> {
        if self.update_entry(name, ddl, binding)? {
            self.persist().await?;
        }
        Ok(())
    }

    pub(super) async fn retire(&mut self, names: &[String]) -> Result<(), DbError> {
        let mut changed = false;
        for name in names {
            if self
                .state
                .objects
                .get(name)
                .is_some_and(|entry| entry.binding.is_some())
            {
                changed |= self.update_entry(name, "", None)?;
            }
        }
        if changed {
            self.persist().await?;
        }
        Ok(())
    }

    fn update_entry(
        &mut self,
        name: &str,
        ddl: &str,
        binding: Option<SchemaBinding>,
    ) -> Result<bool, DbError> {
        if let Some(existing) = self.state.objects.get(name) {
            if existing.ddl == ddl && existing.binding == binding {
                return Ok(false);
            }
        }
        let generation = self
            .state
            .objects
            .get(name)
            .map_or(Some(1), |entry| entry.generation.checked_add(1))
            .ok_or_else(|| DbError::Checkpoint("schema generation exhausted".into()))?;
        self.state.objects.insert(
            name.into(),
            Entry {
                generation,
                ddl: ddl.into(),
                binding,
            },
        );

        Ok(true)
    }

    async fn persist(&mut self) -> Result<(), DbError> {
        if self.state.objects.len() > MAX_OBJECTS {
            return Err(DbError::Checkpoint(
                "schema journal exceeds 4096 objects".into(),
            ));
        }
        let mut value = serde_json::to_value(&self.state)
            .map_err(|error| DbError::Checkpoint(error.to_string()))?;
        value.sort_all_objects();
        let bytes =
            serde_json::to_vec(&value).map_err(|error| DbError::Checkpoint(error.to_string()))?;
        if bytes.len() > MAX_BYTES {
            return Err(DbError::Checkpoint("schema journal exceeds 8 MiB".into()));
        }
        // Local replacement is fenced by the OS lease, retained by the durable store's worker.
        // Shared stores use their native conditional version token.
        let mode = match (&self.revision, self.exclusive_local) {
            (None, _) => PutMode::Create,
            (Some(_), true) => PutMode::Overwrite,
            (Some(revision), false) => PutMode::Update(revision.clone()),
        };
        let result = self
            .store
            .put_opts(
                &JOURNAL_PATH.into(),
                bytes.into(),
                PutOptions {
                    mode,
                    ..PutOptions::default()
                },
            )
            .await
            .map_err(|error| journal_error(&error))?;
        self.revision = Some(UpdateVersion {
            e_tag: result.e_tag,
            version: result.version,
        });
        Ok(())
    }
}

fn journal_error(error: &object_store::Error) -> DbError {
    DbError::Checkpoint(format!("durable schema authority: {error}"))
}
