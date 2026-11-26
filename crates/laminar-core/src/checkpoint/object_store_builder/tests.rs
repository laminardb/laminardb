use super::*;

#[test]
fn test_file_scheme_creates_durable_local_store() {
    let dir = tempfile::tempdir().unwrap();
    let url = url::Url::from_directory_path(dir.path()).unwrap();
    let store = build_object_store(url.as_str(), &HashMap::new()).unwrap();
    assert!(store.to_string().starts_with("DurableLocalObjectStore("));
}

#[test]
fn test_file_scheme_empty_path_errors() {
    let result = build_object_store("file://", &HashMap::new());
    assert!(result.is_err());
}

#[test]
fn file_url_requires_an_absolute_local_path() {
    assert!(file_url_path("file://checkpoint-host/path").is_err());
    assert!(file_url_path("file://./relative").is_err());
    let directory = tempfile::tempdir().unwrap();
    let file_url = url::Url::from_directory_path(directory.path())
        .unwrap()
        .to_string();
    let uppercase_file_url = file_url.replacen("file", "FILE", 1);
    assert_eq!(
        file_url_path(&uppercase_file_url).unwrap(),
        directory.path()
    );
    assert!(file_url_path("file:///tmp/path?version=1").is_err());
    assert!(file_url_path("file:///tmp/path#fragment").is_err());
}

#[test]
fn absolute_local_file_url_classification_is_platform_independent() {
    assert!(is_absolute_local_file_url("file:///tmp/checkpoints"));

    let directory = tempfile::tempdir().unwrap();
    let local_url = url::Url::from_directory_path(directory.path()).unwrap();
    assert!(is_absolute_local_file_url(local_url.as_str()));

    assert!(!is_absolute_local_file_url("file://checkpoint-host/path"));
    assert!(!is_absolute_local_file_url("file://./relative"));
    assert!(is_absolute_local_file_url("FILE:///tmp/path"));
    assert!(!is_absolute_local_file_url("file:///tmp/path?version=1"));
    assert!(!is_absolute_local_file_url("file:///tmp/path#fragment"));
}

#[test]
fn checkpoint_storage_scope_matches_supported_schemes() {
    assert_eq!(
        CheckpointStorageScope::for_url("file:///tmp/checkpoints"),
        CheckpointStorageScope::NodeDurable
    );
    for url in [
        "s3://bucket/prefix",
        "s3a://bucket/prefix",
        "gs://bucket/prefix",
        "gcs://bucket/prefix",
        "az://container/prefix",
        "abfs://container/prefix",
        "abfss://container/prefix",
        "wasb://container/prefix",
        "wasbs://container/prefix",
    ] {
        assert_eq!(
            CheckpointStorageScope::for_url(url),
            CheckpointStorageScope::ClusterShared
        );
    }
    for url in ["memory://", "file://relative", "s3n://bucket", "ftp://host"] {
        assert_eq!(
            CheckpointStorageScope::for_url(url),
            CheckpointStorageScope::Volatile
        );
    }
}

#[test]
fn file_url_decodes_escaped_path_segments() {
    let directory = tempfile::tempdir().unwrap();
    let expected = directory.path().join("laminar checkpoint");
    let encoded = url::Url::from_directory_path(&expected).unwrap();
    assert!(encoded.as_str().contains("laminar%20checkpoint"));
    assert_eq!(file_url_path(encoded.as_str()).unwrap(), expected);
}

#[test]
fn test_unknown_scheme_errors() {
    let result = build_object_store("ftp://bucket/prefix", &HashMap::new());
    assert!(result.is_err());
}

#[test]
fn test_no_scheme_errors() {
    let result = build_object_store("/just/a/path", &HashMap::new());
    assert!(result.is_err());
}

#[test]
fn ambient_options_are_scoped_to_the_selected_provider() {
    assert!(provider_environment_key(
        StorageProvider::AwsS3,
        "AWS_REGION"
    ));
    assert!(!provider_environment_key(StorageProvider::AwsS3, "REGION"));
    assert!(!provider_environment_key(
        StorageProvider::AwsS3,
        "AZURE_STORAGE_TOKEN"
    ));
    assert!(provider_environment_key(
        StorageProvider::Gcs,
        "SERVICE_ACCOUNT"
    ));
    assert!(provider_environment_key(
        StorageProvider::AzureAdls,
        "IDENTITY_ENDPOINT"
    ));
    assert!(!provider_environment_key(
        StorageProvider::AzureAdls,
        "ENDPOINT"
    ));
}

#[test]
fn endpoint_resolution_is_deterministic_and_explicit_first() {
    let options = HashMap::from([
        (
            "aws_endpoint".to_string(),
            "https://explicit.example".to_string(),
        ),
        (
            "aws_endpoint_url_s3".to_string(),
            "https://service-specific.example".to_string(),
        ),
    ]);
    let environment = vec![
        (
            "AWS_ENDPOINT_URL_S3".to_string(),
            "https://ambient.example".to_string(),
        ),
        ("AWS_ALLOW_HTTP".to_string(), "true".to_string()),
    ];
    assert_eq!(
        configured_endpoint(StorageProvider::AwsS3, &options, &environment),
        Some("https://service-specific.example")
    );
    assert!(configured_allow_http(
        StorageProvider::AwsS3,
        &options,
        &environment
    ));

    let options = HashMap::from([("aws_allow_http".to_string(), "false".to_string())]);
    assert!(!configured_allow_http(
        StorageProvider::AwsS3,
        &options,
        &environment
    ));
}

#[test]
fn signed_endpoint_query_is_rejected_without_disclosure() {
    let options = HashMap::from([(
        "aws_endpoint".to_string(),
        "https://compat.example?X-Amz-Signature=do-not-print".to_string(),
    )]);
    let error = build_object_store("s3://bucket/checkpoints", &options)
        .unwrap_err()
        .to_string();
    assert!(error.contains("query parameters"), "{error}");
    assert!(!error.contains("do-not-print"), "{error}");
    assert!(!error.contains("compat.example"), "{error}");
}

#[cfg(feature = "gcs")]
#[test]
fn external_account_adc_builds_without_static_credentials() {
    let directory = tempfile::tempdir().unwrap();
    let credential_path = directory.path().join("external-account.json");
    std::fs::write(
        &credential_path,
        serde_json::to_vec(&serde_json::json!({
            "type": "external_account",
            "audience": "//iam.googleapis.com/projects/1/locations/global/workloadIdentityPools/test/providers/github",
            "subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
            "token_url": "https://sts.googleapis.com/v1/token",
            "credential_source": { "file": "subject-token" }
        }))
        .unwrap(),
    )
    .unwrap();
    let options = HashMap::from([(
        "google_application_credentials".to_string(),
        credential_path.to_string_lossy().into_owned(),
    )]);

    let store = build_object_store("gs://test-bucket/checkpoints", &options).unwrap();
    assert!(!store.to_string().contains("external-account"));
}

#[cfg(feature = "gcs")]
#[test]
fn external_account_errors_are_redacted() {
    let directory = tempfile::tempdir().unwrap();
    let secret = "do-not-disclose";
    let credential_path = directory.path().join(secret);
    std::fs::write(
        &credential_path,
        serde_json::json!({
            "type": "external_account",
            "token_url": format!("https://{secret}.example/token"),
            "credential_source": { "file": secret }
        })
        .to_string(),
    )
    .unwrap();
    let options = HashMap::from([(
        "google_application_credentials".to_string(),
        credential_path.to_string_lossy().into_owned(),
    )]);

    let error = build_object_store("gs://test-bucket/checkpoints", &options)
        .unwrap_err()
        .to_string();
    assert!(error.contains("audience"), "{error}");
    assert!(!error.contains(secret), "{error}");
}

#[cfg(feature = "aws")]
#[test]
fn explicit_provider_options_fail_closed() {
    let mut options = HashMap::from([("aws_regoin".to_string(), "us-east-1".to_string())]);
    let error = validate_explicit_options(StorageProvider::AwsS3, &options).unwrap_err();
    assert!(error.to_string().contains("aws_regoin"), "{error}");

    options = HashMap::from([
        ("aws_region".to_string(), "us-east-1".to_string()),
        ("region".to_string(), "us-west-2".to_string()),
    ]);
    assert!(validate_explicit_options(StorageProvider::AwsS3, &options).is_err());
}

#[cfg(feature = "azure")]
#[test]
fn qualified_azure_authority_cannot_be_overridden_by_options() {
    let options = HashMap::from([(
        "azure_storage_account_name".to_string(),
        "different-account".to_string(),
    )]);
    let error = build_object_store(
        "abfss://events@account.dfs.core.windows.net/checkpoints",
        &options,
    )
    .unwrap_err()
    .to_string();
    assert!(error.contains("conflicts with the URL authority"));
    assert!(!error.contains("different-account"));
    assert!(!error.contains("events@account"));
}

/// A loopback S3 endpoint that answers every HEAD and holds the second response until released.
#[cfg(feature = "aws")]
#[derive(Default)]
struct HeldResponseEndpoint {
    url: String,
    requests: std::sync::atomic::AtomicUsize,
    held_request_arrived: tokio::sync::Notify,
    release_held_response: tokio::sync::Notify,
}

#[cfg(feature = "aws")]
impl HeldResponseEndpoint {
    const HEAD_RESPONSE: &'static [u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nETag: \"frame\"\r\nLast-Modified: Thu, 26 Nov 2026 00:00:00 GMT\r\n\r\n";

    async fn start() -> Arc<Self> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = Arc::new(Self {
            url: format!("http://{}", listener.local_addr().unwrap()),
            ..Self::default()
        });
        let server = Arc::clone(&endpoint);
        tokio::spawn(async move {
            loop {
                let (socket, _) = listener.accept().await.unwrap();
                tokio::spawn(Arc::clone(&server).serve(socket));
            }
        });
        endpoint
    }

    async fn serve(self: Arc<Self>, mut socket: tokio::net::TcpStream) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let mut buffer = Vec::new();
        let mut chunk = [0_u8; 4096];
        loop {
            while let Some(end) = buffer.windows(4).position(|window| window == b"\r\n\r\n") {
                buffer.drain(..end + 4);
                if self
                    .requests
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                    == 1
                {
                    self.held_request_arrived.notify_one();
                    self.release_held_response.notified().await;
                }
                if socket.write_all(Self::HEAD_RESPONSE).await.is_err() {
                    return;
                }
            }
            match socket.read(&mut chunk).await {
                Ok(0) | Err(_) => return,
                Ok(read) => buffer.extend_from_slice(&chunk[..read]),
            }
        }
    }
}

/// A pipeline's compute runtime reads the shared store and ends with the pipeline. A request
/// another runtime sends on a connection that runtime opened must still complete.
#[cfg(feature = "aws")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_store_on_an_io_runtime_outlives_the_runtime_that_opened_its_connection() {
    use object_store::ObjectStoreExt as _;

    const DEADLINE: std::time::Duration = std::time::Duration::from_secs(10);

    let endpoint = HeldResponseEndpoint::start().await;
    let options = HashMap::from([
        ("aws_endpoint".to_string(), endpoint.url.clone()),
        ("aws_region".to_string(), "us-east-1".to_string()),
        ("aws_access_key_id".to_string(), "test".to_string()),
        ("aws_secret_access_key".to_string(), "test".to_string()),
        ("aws_allow_http".to_string(), "true".to_string()),
    ]);
    let store = build_object_store_on(
        "s3://checkpoints/node",
        &options,
        &tokio::runtime::Handle::current(),
    )
    .unwrap();
    let frame = object_store::path::Path::from("frame");

    // The compute runtime sends the first request and keeps running until stopped.
    let (opened, opened_on_compute) = tokio::sync::oneshot::channel();
    let (stop, stop_compute) = tokio::sync::oneshot::channel::<()>();
    let compute = std::thread::spawn({
        let (store, frame) = (Arc::clone(&store), frame.clone());
        move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async {
                opened.send(store.head(&frame).await.map(|_| ())).unwrap();
                let _ = stop_compute.await;
            });
        }
    });
    tokio::time::timeout(DEADLINE, opened_on_compute)
        .await
        .unwrap()
        .unwrap()
        .unwrap();

    // This runtime reuses that connection; the compute runtime ends while the response is held.
    let held = tokio::spawn({
        let store = Arc::clone(&store);
        async move { store.head(&frame).await }
    });
    tokio::time::timeout(DEADLINE, endpoint.held_request_arrived.notified())
        .await
        .unwrap();
    stop.send(()).unwrap();
    tokio::task::spawn_blocking(move || compute.join())
        .await
        .unwrap()
        .unwrap();
    endpoint.release_held_response.notify_one();
    tokio::time::timeout(DEADLINE, held)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    // No retry hid a failed request.
    assert_eq!(
        endpoint.requests.load(std::sync::atomic::Ordering::SeqCst),
        2
    );
}
