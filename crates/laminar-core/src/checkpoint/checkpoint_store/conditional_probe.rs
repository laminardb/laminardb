//! Destructive capability probes for object-store conditional writes.

use std::time::Duration;

use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload, UpdateVersion};

use super::{normalize_prefix, CheckpointStoreError};

/// Prove that conditional create rejects an existing object.
///
/// # Errors
/// Returns an error when conditional create is absent, times out, or cannot be cleaned up.
pub async fn probe_object_store_conditional_create(
    store: &dyn ObjectStore,
    prefix: &str,
    timeout: Duration,
) -> Result<(), CheckpointStoreError> {
    run_conditional_probe(store, prefix, false, timeout).await
}

/// Prove conditional create/update and stale-update rejection.
///
/// # Errors
/// Returns an error when any conditional operation is absent, times out, or cannot be cleaned up.
pub async fn probe_object_store_conditional_update(
    store: &dyn ObjectStore,
    prefix: &str,
    timeout: Duration,
) -> Result<(), CheckpointStoreError> {
    run_conditional_probe(store, prefix, true, timeout).await
}

async fn run_conditional_probe(
    store: &dyn ObjectStore,
    prefix: &str,
    require_update: bool,
    timeout: Duration,
) -> Result<(), CheckpointStoreError> {
    let path = conditional_probe_path(prefix);
    let probe = async {
        let create = PutOptions {
            mode: PutMode::Create,
            ..PutOptions::default()
        };
        store
            .put_opts(
                &path,
                PutPayload::from_static(b"laminardb-conditional-create-v1"),
                create.clone(),
            )
            .await?;
        match store
            .put_opts(
                &path,
                PutPayload::from_static(b"must-not-overwrite"),
                create,
            )
            .await
        {
            Err(
                object_store::Error::AlreadyExists { .. }
                | object_store::Error::Precondition { .. },
            ) => {}
            Ok(_) => {
                return Err(CheckpointStoreError::Invalid(
                    "object store overwrote an existing conditional-create probe".into(),
                ));
            }
            Err(error) => return Err(error.into()),
        }
        if require_update {
            let observed = store.get(&path).await?;
            let stale = UpdateVersion {
                e_tag: observed.meta.e_tag.clone(),
                version: observed.meta.version.clone(),
            };
            if stale.e_tag.is_none() && stale.version.is_none() {
                return Err(CheckpointStoreError::Invalid(
                    "object store GET returned neither ETag nor version for conditional update"
                        .into(),
                ));
            }
            store
                .put_opts(
                    &path,
                    PutPayload::from_static(b"laminardb-conditional-update-v2"),
                    PutOptions {
                        mode: PutMode::Update(stale.clone()),
                        ..PutOptions::default()
                    },
                )
                .await?;
            match store
                .put_opts(
                    &path,
                    PutPayload::from_static(b"must-not-stale-update"),
                    PutOptions {
                        mode: PutMode::Update(stale),
                        ..PutOptions::default()
                    },
                )
                .await
            {
                Err(object_store::Error::Precondition { .. }) => {}
                Ok(_) => {
                    return Err(CheckpointStoreError::Invalid(
                        "object store accepted a stale conditional update".into(),
                    ));
                }
                Err(error) => return Err(error.into()),
            }
            let observed = store.get(&path).await?;
            let current = UpdateVersion {
                e_tag: observed.meta.e_tag.clone(),
                version: observed.meta.version.clone(),
            };
            if current.e_tag.is_none() && current.version.is_none() {
                return Err(CheckpointStoreError::Invalid(
                    "object store GET returned neither ETag nor version after conditional update"
                        .into(),
                ));
            }
            store
                .put_opts(
                    &path,
                    PutPayload::from_static(b"laminardb-conditional-update-v3"),
                    PutOptions {
                        mode: PutMode::Update(current),
                        ..PutOptions::default()
                    },
                )
                .await?;
        }
        Ok::<(), CheckpointStoreError>(())
    };

    let result = match tokio::time::timeout(timeout, probe).await {
        Ok(result) => result,
        Err(_) => Err(CheckpointStoreError::Invalid(
            "conditional-put probe timed out".into(),
        )),
    };
    let cleanup = tokio::time::timeout(timeout, store.delete(&path)).await;
    match cleanup {
        Ok(Ok(()) | Err(object_store::Error::NotFound { .. })) => result,
        Ok(Err(error)) => Err(error.into()),
        Err(_) => Err(CheckpointStoreError::Invalid(
            "conditional-put probe cleanup timed out".into(),
        )),
    }
}

fn conditional_probe_path(prefix: &str) -> object_store::path::Path {
    object_store::path::Path::from(format!(
        "{}capability-probes/{}.bin",
        normalize_prefix(prefix),
        uuid::Uuid::new_v4()
    ))
}
