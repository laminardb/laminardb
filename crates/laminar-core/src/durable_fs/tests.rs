use super::*;

#[test]
fn no_replace_publishes_and_refuses_existing_destination() {
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("source.tmp");
    let destination = directory.path().join("final");
    std::fs::write(&source, b"first").unwrap();
    durable_rename(&source, &destination, DurableRenameMode::NoReplace).unwrap();
    assert_eq!(std::fs::read(&destination).unwrap(), b"first");
    assert!(!source.exists());

    std::fs::write(&source, b"second").unwrap();
    assert!(durable_rename(&source, &destination, DurableRenameMode::NoReplace).is_err());
    assert_eq!(std::fs::read(&destination).unwrap(), b"first");
    assert!(source.exists());
}

#[test]
fn replace_publishes_new_bytes() {
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("source.tmp");
    let destination = directory.path().join("final");
    std::fs::write(&destination, b"old").unwrap();
    std::fs::write(&source, b"new").unwrap();
    durable_rename(&source, &destination, DurableRenameMode::Replace).unwrap();
    assert_eq!(std::fs::read(&destination).unwrap(), b"new");
    assert!(!source.exists());
}

#[test]
fn creates_nested_durable_directories_and_is_idempotent() {
    let directory = tempfile::tempdir().unwrap();
    let nested = directory.path().join("one").join("two").join("three");
    ensure_durable_directory(&nested).unwrap();
    assert!(nested.is_dir());
    ensure_durable_directory(&nested).unwrap();
}

#[test]
fn rejects_a_file_in_the_directory_path() {
    let directory = tempfile::tempdir().unwrap();
    let file = directory.path().join("file");
    std::fs::write(&file, b"not a directory").unwrap();
    let error = ensure_durable_directory(&file.join("child")).unwrap_err();
    assert!(
        matches!(
            error.kind(),
            io::ErrorKind::AlreadyExists | io::ErrorKind::NotADirectory
        ),
        "unexpected error for a file in the directory path: {error}"
    );
}

#[cfg(windows)]
#[test]
fn directory_publication_accepts_a_competing_real_directory() {
    let directory = tempfile::tempdir().unwrap();
    let destination = directory.path().join("winner");
    std::fs::create_dir(&destination).unwrap();

    publish_directory_component(directory.path(), &destination).unwrap();

    assert!(destination.is_dir());
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 1);
}
