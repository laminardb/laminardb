use futures::TryStreamExt;
use object_store::ObjectStoreExt;

use super::*;

#[tokio::test]
async fn create_is_exclusive_and_keeps_the_winner() {
    let directory = tempfile::tempdir().unwrap();
    let store = DurableLocalObjectStore::new(directory.path()).unwrap();
    let path = Path::from("checkpoint/id.json");
    let first = store.put_opts(
        &path,
        PutPayload::from_static(b"first"),
        PutMode::Create.into(),
    );
    let second = store.put_opts(
        &path,
        PutPayload::from_static(b"second"),
        PutMode::Create.into(),
    );
    let (first, second) = tokio::join!(first, second);
    assert_ne!(first.is_ok(), second.is_ok());
    let error = first.err().or_else(|| second.err()).unwrap();
    assert!(matches!(error, object_store::Error::AlreadyExists { .. }));
    let bytes = store.get(&path).await.unwrap().bytes().await.unwrap();
    assert!(bytes == b"first".as_slice() || bytes == b"second".as_slice());
}

#[tokio::test]
async fn immutable_create_does_not_wait_for_mutable_operation_order() {
    let directory = tempfile::tempdir().unwrap();
    let store = DurableLocalObjectStore::new(directory.path()).unwrap();
    let guard = Arc::clone(&store.domain.operation_order).lock_owned().await;
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        store.put_opts(
            &Path::from("objects/chunk=1/data.bin"),
            PutPayload::from_static(b"data"),
            PutMode::Create.into(),
        ),
    )
    .await
    .expect("independent immutable publication was serialized");
    result.unwrap();
    drop(guard);
}

#[tokio::test]
async fn namespace_inventory_does_not_wait_for_mutable_operation_order() {
    let directory = tempfile::tempdir().unwrap();
    let store = DurableLocalObjectStore::new(directory.path()).unwrap();
    store
        .put_opts(
            &Path::from("objects/generation=1/object"),
            PutPayload::from_static(b"data"),
            PutMode::Create.into(),
        )
        .await
        .unwrap();
    let guard = Arc::clone(&store.domain.operation_order).lock_owned().await;

    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let listed = store
            .list(Some(&Path::from("objects")))
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(listed.len(), 1);
        let delimited = store
            .list_with_delimiter(Some(&Path::from("objects")))
            .await
            .unwrap();
        assert_eq!(
            delimited.common_prefixes,
            vec![Path::from("objects/generation=1")]
        );
    })
    .await
    .expect("namespace inventory was serialized behind a mutation");

    drop(guard);
}

#[tokio::test]
async fn overwrite_read_list_and_delete_roundtrip() {
    let directory = tempfile::tempdir().unwrap();
    let store = DurableLocalObjectStore::new(directory.path()).unwrap();
    let path = Path::from("outcomes/epoch=1/outcome");
    store
        .put(&path, PutPayload::from_static(b"old"))
        .await
        .unwrap();
    store
        .put(&path, PutPayload::from_static(b"new"))
        .await
        .unwrap();
    let bytes = store.get(&path).await.unwrap().bytes().await.unwrap();
    assert_eq!(bytes, b"new".as_slice());
    let listed = store
        .list(Some(&Path::from("outcomes")))
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].location, path);
    store.delete(&path).await.unwrap();
    assert!(matches!(
        store.get(&path).await.unwrap_err(),
        object_store::Error::NotFound { .. }
    ));
}

#[tokio::test]
async fn delete_cleans_empty_ancestors_preserves_siblings_and_allows_recreate() {
    let directory = tempfile::tempdir().unwrap();
    let store = DurableLocalObjectStore::new(directory.path()).unwrap();
    let retired = Path::from("objects/generation=1/chunk=0/data.bin");
    let retained = Path::from("objects/generation=2/chunk=0/data.bin");
    for path in [&retired, &retained] {
        store
            .put_opts(
                path,
                PutPayload::from_static(b"data"),
                PutMode::Create.into(),
            )
            .await
            .unwrap();
    }

    store.delete(&retired).await.unwrap();

    assert!(!directory.path().join("objects/generation=1").exists());
    assert!(directory
        .path()
        .join("objects/generation=2/chunk=0/data.bin")
        .is_file());
    store
        .put_opts(
            &retired,
            PutPayload::from_static(b"recreated"),
            PutMode::Create.into(),
        )
        .await
        .unwrap();
    assert_eq!(
        store.get(&retired).await.unwrap().bytes().await.unwrap(),
        bytes::Bytes::from_static(b"recreated")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn immutable_create_survives_concurrent_empty_ancestor_cleanup() {
    let directory = tempfile::tempdir().unwrap();
    let store = DurableLocalObjectStore::new(directory.path()).unwrap();
    let retired = Path::from("objects/generation=1/retired.bin");
    let live = Path::from("objects/generation=1/live.bin");
    store
        .put_opts(
            &retired,
            PutPayload::from_static(b"retired"),
            PutMode::Create.into(),
        )
        .await
        .unwrap();

    let gate = Arc::new(TestPublicationGate::default());
    *store.domain.directory_prepare_gate.lock() = Some(Arc::clone(&gate));
    let creating_store = store.clone();
    let creating_path = live.clone();
    let creating = tokio::spawn(async move {
        creating_store
            .put_opts(
                &creating_path,
                PutPayload::from_static(b"live"),
                PutMode::Create.into(),
            )
            .await
    });
    let wait_gate = Arc::clone(&gate);
    assert!(
        tokio::task::spawn_blocking(move || {
            wait_gate.wait_until_entered(std::time::Duration::from_secs(5))
        })
        .await
        .unwrap(),
        "create did not reach the prepared-directory boundary"
    );

    let deleting_store = store.clone();
    let deleting_path = retired.clone();
    let deleting = tokio::spawn(async move { deleting_store.delete(&deleting_path).await });
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while directory
            .path()
            .join("objects/generation=1/retired.bin")
            .exists()
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("delete did not reach empty-directory cleanup");

    gate.release();
    creating.await.unwrap().unwrap();
    deleting.await.unwrap().unwrap();
    assert_eq!(
        store.get(&live).await.unwrap().bytes().await.unwrap(),
        bytes::Bytes::from_static(b"live")
    );
}

#[tokio::test]
async fn delete_stream_can_consume_a_listing_from_the_same_store() {
    let directory = tempfile::tempdir().unwrap();
    let store = DurableLocalObjectStore::new(directory.path()).unwrap();
    let prefix = Path::from("objects/generation=1");
    for chunk in 0..3 {
        store
            .put_opts(
                &Path::from(format!("objects/generation=1/chunk={chunk}")),
                PutPayload::from_static(b"retired"),
                PutMode::Create.into(),
            )
            .await
            .unwrap();
    }

    let locations = store
        .list(Some(&prefix))
        .map(|result| result.map(|metadata| metadata.location))
        .boxed();
    let deleted = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        store.delete_stream(locations).try_collect::<Vec<_>>(),
    )
    .await
    .expect("list-to-delete deadlocked")
    .unwrap();

    assert_eq!(deleted.len(), 3);
    assert!(store
        .list(Some(&prefix))
        .try_collect::<Vec<_>>()
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn delete_stream_releases_mutation_order_between_objects() {
    let directory = tempfile::tempdir().unwrap();
    let store = DurableLocalObjectStore::new(directory.path()).unwrap();
    let retired_a = Path::from("objects/generation=1/a");
    let retired_b = Path::from("objects/generation=1/b");
    let live = Path::from("metadata/prune-floor");
    for path in [&retired_a, &retired_b, &live] {
        store
            .put_opts(
                path,
                PutPayload::from_static(b"old"),
                PutMode::Create.into(),
            )
            .await
            .unwrap();
    }

    let (locations_tx, locations_rx) =
        futures::channel::mpsc::unbounded::<object_store::Result<Path>>();
    locations_tx.unbounded_send(Ok(retired_a.clone())).unwrap();
    let mut deletes = store.delete_stream(locations_rx.boxed());
    assert_eq!(
        tokio::time::timeout(std::time::Duration::from_secs(5), deletes.next())
            .await
            .expect("first delete stalled")
            .expect("delete stream ended early")
            .unwrap(),
        retired_a
    );

    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        store.put_opts(
            &live,
            PutPayload::from_static(b"new"),
            PutMode::Overwrite.into(),
        ),
    )
    .await
    .expect("idle delete stream retained the mutation order")
    .unwrap();

    locations_tx.unbounded_send(Ok(retired_b.clone())).unwrap();
    drop(locations_tx);
    assert_eq!(deletes.next().await.unwrap().unwrap(), retired_b);
    assert!(deletes.next().await.is_none());
    assert_eq!(
        store.get(&live).await.unwrap().bytes().await.unwrap(),
        bytes::Bytes::from_static(b"new")
    );
}

#[cfg(unix)]
#[tokio::test]
async fn delete_completes_after_synchronizing_the_parent_directory() {
    let directory = tempfile::tempdir().unwrap();
    let store = DurableLocalObjectStore::new(directory.path()).unwrap();
    let path = Path::from("retention/obsolete");
    store
        .put_opts(
            &path,
            PutPayload::from_static(b"obsolete"),
            PutMode::Create.into(),
        )
        .await
        .unwrap();

    store.delete(&path).await.unwrap();

    assert!(!directory.path().join("retention/obsolete").exists());
}

#[cfg(windows)]
#[tokio::test]
async fn delete_durably_renames_before_removing_the_internal_tombstone() {
    let directory = tempfile::tempdir().unwrap();
    let store = DurableLocalObjectStore::new(directory.path()).unwrap();
    let path = Path::from("retention/obsolete");
    store
        .put_opts(
            &path,
            PutPayload::from_static(b"obsolete"),
            PutMode::Create.into(),
        )
        .await
        .unwrap();

    store.delete(&path).await.unwrap();

    let parent = directory.path().join("retention");
    assert!(!parent.exists());
}

#[tokio::test]
async fn overwrite_and_reads_share_order_across_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let first = DurableLocalObjectStore::new(directory.path()).unwrap();
    first
        .put_opts(
            &Path::from("floor"),
            PutPayload::from_static(b"one"),
            PutMode::Create.into(),
        )
        .await
        .unwrap();
    let first_domain = Arc::clone(&first.domain);
    let guard = Arc::clone(&first_domain.operation_order).lock_owned().await;
    drop(first);

    let reopened = DurableLocalObjectStore::new(directory.path()).unwrap();
    assert!(Arc::ptr_eq(&first_domain, &reopened.domain));
    let path = Path::from("floor");
    let mut read = Box::pin(reopened.get(&path));
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(25), &mut read)
            .await
            .is_err()
    );
    drop(guard);
    tokio::time::timeout(std::time::Duration::from_secs(5), read)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_overwrite_drains_before_reopen_rmw() {
    let directory = tempfile::tempdir().unwrap();
    let store = DurableLocalObjectStore::new(directory.path()).unwrap();
    let path = Path::from("floor");
    store
        .put_opts(&path, PutPayload::from_static(b"1"), PutMode::Create.into())
        .await
        .unwrap();

    let gate = Arc::new(TestPublicationGate::default());
    *store.domain.publication_gate.lock() = Some(Arc::clone(&gate));
    let writer = store.clone();
    let writer_path = path.clone();
    let pending = tokio::spawn(async move {
        writer
            .put_opts(
                &writer_path,
                PutPayload::from_static(b"10"),
                PutMode::Overwrite.into(),
            )
            .await
    });
    let wait_gate = Arc::clone(&gate);
    let entered = tokio::task::spawn_blocking(move || {
        wait_gate.wait_until_entered(std::time::Duration::from_secs(5))
    })
    .await
    .unwrap();
    if !entered {
        gate.release();
        panic!("overwrite did not reach its blocking publication boundary");
    }
    pending.abort();
    assert!(pending.await.unwrap_err().is_cancelled());
    drop(store);

    let reopened = DurableLocalObjectStore::new(directory.path()).unwrap();
    let read_modify_write_path = path.clone();
    let mut read_modify_write = tokio::spawn(async move {
        let bytes = reopened.get(&read_modify_write_path).await?.bytes().await?;
        let current = std::str::from_utf8(&bytes)
            .map_err(|error| object_store::Error::Generic {
                store: STORE_NAME,
                source: Box::new(error),
            })?
            .parse::<u64>()
            .map_err(|error| object_store::Error::Generic {
                store: STORE_NAME,
                source: Box::new(error),
            })?;
        let next = current.max(5).to_string();
        reopened
            .put_opts(
                &read_modify_write_path,
                PutPayload::from(next),
                PutMode::Overwrite.into(),
            )
            .await?;
        Ok::<_, object_store::Error>(reopened)
    });
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(25), &mut read_modify_write,)
            .await
            .is_err(),
        "reopened read overtook the detached overwrite"
    );

    gate.release();
    let reopened = tokio::time::timeout(std::time::Duration::from_secs(5), read_modify_write)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let bytes = reopened.get(&path).await.unwrap().bytes().await.unwrap();
    assert_eq!(bytes, b"10".as_slice());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_delete_holds_ownership_until_the_filesystem_job_finishes() {
    const LOCK: &str = ".test-owner.lock";

    let directory = tempfile::tempdir().unwrap();
    let store = DurableLocalObjectStore::new_exclusive(directory.path(), LOCK).unwrap();
    let path = Path::from("objects/retired");
    store
        .put_opts(
            &path,
            PutPayload::from_static(b"old"),
            PutMode::Create.into(),
        )
        .await
        .unwrap();

    let gate = Arc::new(TestPublicationGate::default());
    *store.domain.deletion_gate.lock() = Some(Arc::clone(&gate));
    let deleting_store = store.clone();
    let deleting_path = path.clone();
    let pending = tokio::spawn(async move { deleting_store.delete(&deleting_path).await });
    let wait_gate = Arc::clone(&gate);
    let entered = tokio::task::spawn_blocking(move || {
        wait_gate.wait_until_entered(std::time::Duration::from_secs(5))
    })
    .await
    .unwrap();
    if !entered {
        gate.release();
        panic!("delete did not reach its blocking filesystem boundary");
    }

    pending.abort();
    assert!(pending.await.unwrap_err().is_cancelled());
    drop(store);
    assert!(
        DurableLocalObjectStore::new_exclusive(directory.path(), LOCK).is_err(),
        "a replacement owner overtook the detached delete"
    );

    gate.release();
    let reopened = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            match DurableLocalObjectStore::new_exclusive(directory.path(), LOCK) {
                Ok(store) => break store,
                Err(_) => tokio::task::yield_now().await,
            }
        }
    })
    .await
    .expect("detached delete did not release its ownership fence");
    reopened
        .put_opts(
            &path,
            PutPayload::from_static(b"new"),
            PutMode::Create.into(),
        )
        .await
        .unwrap();
    let bytes = reopened.get(&path).await.unwrap().bytes().await.unwrap();
    assert_eq!(bytes, b"new".as_slice());
}

#[test]
fn exclusive_owner_is_held_until_background_jobs_release_it() {
    const LOCK: &str = ".test-owner.lock";

    let directory = tempfile::tempdir().unwrap();
    let store = DurableLocalObjectStore::new_exclusive(directory.path(), LOCK).unwrap();
    let background_owner = store.ownership_lock.clone().unwrap();
    assert!(DurableLocalObjectStore::new_exclusive(directory.path(), LOCK).is_err());
    drop(store);
    assert!(DurableLocalObjectStore::new_exclusive(directory.path(), LOCK).is_err());
    drop(background_owner);
    DurableLocalObjectStore::new_exclusive(directory.path(), LOCK).unwrap();
}

#[tokio::test]
async fn successful_and_failed_puts_leave_no_temporary_files() {
    let directory = tempfile::tempdir().unwrap();
    let store = DurableLocalObjectStore::new(directory.path()).unwrap();
    let path = Path::from("nested/metadata");
    store
        .put_opts(
            &path,
            PutPayload::from_static(b"winner"),
            PutMode::Create.into(),
        )
        .await
        .unwrap();
    store
        .put_opts(
            &path,
            PutPayload::from_static(b"loser"),
            PutMode::Create.into(),
        )
        .await
        .unwrap_err();

    fn visit(directory: &FsPath, temporary: &mut Vec<PathBuf>) {
        for entry in std::fs::read_dir(directory).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                visit(&path, temporary);
            } else if path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with(TEMP_PREFIX))
            {
                temporary.push(path);
            }
        }
    }

    let mut temporary = Vec::new();
    visit(directory.path(), &mut temporary);
    assert!(
        temporary.is_empty(),
        "temporary files leaked: {temporary:?}"
    );
}

#[tokio::test]
async fn internal_temporary_files_are_not_visible_in_object_listings() {
    let directory = tempfile::tempdir().unwrap();
    let store = DurableLocalObjectStore::new(directory.path()).unwrap();
    let parent = directory.path().join("nested");
    ensure_durable_directory(&parent).unwrap();
    let (temporary, temporary_path) = create_temporary(&parent).unwrap();
    drop(temporary);
    std::fs::write(parent.join("visible"), b"value").unwrap();

    let prefix = Path::from("nested");
    let listed = store
        .list(Some(&prefix))
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].location, Path::from("nested/visible"));

    let delimited = store.list_with_delimiter(Some(&prefix)).await.unwrap();
    assert_eq!(delimited.objects.len(), 1);
    assert_eq!(delimited.objects[0].location, Path::from("nested/visible"));
    assert!(temporary_path.is_file());
}

#[tokio::test]
async fn unsupported_mutations_fail_closed() {
    let directory = tempfile::tempdir().unwrap();
    let store = DurableLocalObjectStore::new(directory.path()).unwrap();
    let path = Path::from("metadata");
    let update = store
        .put_opts(
            &path,
            PutPayload::from_static(b"value"),
            PutMode::Update(object_store::UpdateVersion {
                e_tag: Some("stale".into()),
                version: None,
            })
            .into(),
        )
        .await;
    assert!(matches!(
        update,
        Err(object_store::Error::NotImplemented { .. })
    ));
    assert!(matches!(
        store
            .put_multipart_opts(&path, PutMultipartOptions::default())
            .await,
        Err(object_store::Error::NotImplemented { .. })
    ));
    assert!(matches!(
        store
            .copy_opts(&path, &Path::from("copy"), CopyOptions::default())
            .await,
        Err(object_store::Error::NotImplemented { .. })
    ));
}

#[tokio::test]
async fn unresolved_publication_cannot_be_read_or_rewritten() {
    let directory = tempfile::tempdir().unwrap();
    let store = DurableLocalObjectStore::new(directory.path()).unwrap();
    let path = Path::from("metadata");
    let filesystem_path = store.filesystem_path(&path).unwrap();
    store.domain.poisoned_paths.lock().insert(filesystem_path);

    assert!(matches!(
        store.get(&path).await,
        Err(object_store::Error::Generic { .. })
    ));
    assert!(matches!(
        store.put(&path, PutPayload::from_static(b"value")).await,
        Err(object_store::Error::Generic { .. })
    ));
    assert!(std::fs::read_dir(directory.path())
        .unwrap()
        .all(|entry| { !is_internal_artifact_name(&entry.unwrap().file_name()) }));
    drop(store);
    let reopened = DurableLocalObjectStore::new(directory.path()).unwrap();
    assert!(matches!(
        reopened.get(&path).await,
        Err(object_store::Error::Generic { .. })
    ));
}

#[tokio::test]
async fn directory_publication_failure_is_latched() {
    let directory = tempfile::tempdir().unwrap();
    let store = DurableLocalObjectStore::new(directory.path()).unwrap();
    std::fs::write(directory.path().join("not-a-directory"), b"file").unwrap();

    assert!(store
        .put_opts(
            &Path::from("not-a-directory/object"),
            PutPayload::from_static(b"value"),
            PutMode::Create.into(),
        )
        .await
        .is_err());
    drop(store);
    let reopened = DurableLocalObjectStore::new(directory.path()).unwrap();
    let error = reopened
        .put_opts(
            &Path::from("valid/object"),
            PutPayload::from_static(b"value"),
            PutMode::Create.into(),
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("durability is unresolved"));
}
