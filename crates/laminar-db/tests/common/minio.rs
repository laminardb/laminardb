use std::net::TcpStream;
use std::process::Command;
use std::time::Duration;

pub const MINIO_ENDPOINT: &str = "http://127.0.0.1:19000";
pub const MINIO_ACCESS_KEY: &str = "laminar";
pub const MINIO_SECRET_KEY: &str = "laminar-test-secret";
const REQUIRE_MINIO_ENV: &str = "LAMINAR_REQUIRE_MINIO";

pub fn minio_endpoint() -> Option<&'static str> {
    let addr: std::net::SocketAddr = "127.0.0.1:19000".parse().ok()?;
    let endpoint = TcpStream::connect_timeout(&addr, Duration::from_millis(500))
        .ok()
        .map(|_| MINIO_ENDPOINT);
    if endpoint.is_none()
        && std::env::var(REQUIRE_MINIO_ENV)
            .is_ok_and(|value| value == "1" || value.eq_ignore_ascii_case("true"))
    {
        panic!("MinIO is required by {REQUIRE_MINIO_ENV} but is not reachable");
    }
    endpoint
}

pub async fn minio_store(bucket: &str) -> std::sync::Arc<dyn object_store::ObjectStore> {
    use object_store::aws::AmazonS3Builder;
    use object_store::ObjectStore;

    let _ = minio_endpoint().expect("MinIO must be up; run `docker compose up -d minio`");
    let root = AmazonS3Builder::new()
        .with_endpoint(MINIO_ENDPOINT)
        .with_access_key_id(MINIO_ACCESS_KEY)
        .with_secret_access_key(MINIO_SECRET_KEY)
        .with_region("us-east-1")
        .with_allow_http(true)
        .with_bucket_name(bucket)
        .build()
        .expect("minio client");
    if root.list_with_delimiter(None).await.is_err() {
        let status = Command::new("docker")
            .args([
                "exec",
                "laminardb-minio",
                "mc",
                "--quiet",
                "alias",
                "set",
                "local",
                "http://127.0.0.1:9000",
                MINIO_ACCESS_KEY,
                MINIO_SECRET_KEY,
            ])
            .status();
        if let Ok(status) = status {
            assert!(status.success(), "mc alias set failed");
        }
        let _ = Command::new("docker")
            .args([
                "exec",
                "laminardb-minio",
                "mc",
                "--quiet",
                "mb",
                format!("local/{bucket}").as_str(),
            ])
            .status();
    }
    std::sync::Arc::new(root) as std::sync::Arc<dyn ObjectStore>
}
