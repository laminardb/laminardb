//! Iceberg leg of the three-node exactly-once soak.
//!
//! The bounded join also writes to the certified REST-catalog Iceberg append sink on direct S3, so
//! every kill-9 and paused-leader round exercises Iceberg publication beside Delta. Once the frozen
//! input is durable, the table's current data files must hold every expected join pair exactly
//! once, and every LaminarDB snapshot must belong to exactly one checkpoint.
//!
//! Defaults target `tests/docker/iceberg-compose.yml`. `LAMINAR_SOAK_ICEBERG_CATALOG_URI`,
//! `LAMINAR_SOAK_ICEBERG_S3_ENDPOINT`, `LAMINAR_SOAK_ICEBERG_S3_BUCKET` and
//! `LAMINAR_SOAK_ICEBERG_S3_ACCESS_KEY` override them; nodes read the S3 secret from
//! `LAMINAR_SOAK_ICEBERG_S3_SECRET_KEY`, which must be set.

use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use arrow_array::{Array as _, Int64Array};
use object_store::aws::AmazonS3Builder;

use super::iceberg_table::{self, TableMetadata};
use super::{assert_running_nodes, Node};

pub(super) const ICEBERG_SINK: &str = "soak_output_iceberg";
const SECRET_ENV: &str = "LAMINAR_SOAK_ICEBERG_S3_SECRET_KEY";
const NAMESPACE: &str = "laminardb_soak";
const REGION: &str = "us-east-1";

pub(super) struct IcebergJoinOutput {
    catalog_uri: String,
    s3_endpoint: String,
    bucket: String,
    access_key: String,
    table: String,
    client: reqwest::Client,
    runtime: tokio::runtime::Runtime,
}

impl IcebergJoinOutput {
    /// Create this run's table before any node starts; the sink binds an existing table.
    pub(super) fn create(run_id: &str) -> Self {
        let setting =
            |name: &str, default: &str| std::env::var(name).unwrap_or_else(|_| default.to_owned());
        let secret = std::env::var(SECRET_ENV)
            .unwrap_or_else(|_| panic!("{SECRET_ENV} must be set for the Iceberg leg"));
        assert!(!secret.is_empty(), "{SECRET_ENV} must not be empty");
        let output = Self {
            catalog_uri: setting("LAMINAR_SOAK_ICEBERG_CATALOG_URI", "http://127.0.0.1:8181"),
            s3_endpoint: setting("LAMINAR_SOAK_ICEBERG_S3_ENDPOINT", "http://127.0.0.1:9000"),
            bucket: setting("LAMINAR_SOAK_ICEBERG_S3_BUCKET", "warehouse"),
            access_key: setting("LAMINAR_SOAK_ICEBERG_S3_ACCESS_KEY", "minioadmin"),
            table: format!("join_{}", run_id.replace('-', "_")),
            client: reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(5))
                .timeout(Duration::from_secs(15))
                .build()
                .expect("Iceberg oracle HTTP client"),
            runtime: tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("Iceberg oracle runtime"),
        };
        output.runtime.block_on(iceberg_table::create_table(
            &output.client,
            &output.catalog_uri,
            |request| request,
            NAMESPACE,
            &output.table,
            serde_json::json!([
                {"id": 1, "name": "left_id", "required": false, "type": "long"},
                {"id": 2, "name": "right_id", "required": false, "type": "long"},
                {"id": 3, "name": "join_key", "required": false, "type": "long"}
            ]),
        ));
        eprintln!("soak: EO Iceberg output table {NAMESPACE}.{}", output.table);
        output
    }

    pub(super) fn sink_config(&self) -> String {
        format!(
            r#"
[[sink]]
name = "{ICEBERG_SINK}"
pipeline = "soak_join"
connector = "iceberg"
[sink.properties]
"catalog.type" = "rest"
"catalog.uri" = "{catalog_uri}"
"catalog.warehouse" = "s3://{bucket}/wh"
"storage.type" = "s3"
"storage.endpoint" = "{endpoint}"
"storage.region" = "{REGION}"
"storage.path_style" = "true"
"storage.property.s3.access-key-id" = "{access_key}"
"storage.property.s3.secret-access-key" = "$${{{SECRET_ENV}}}"
namespace = "{NAMESPACE}"
"table.name" = "{table}"
"write.mode" = "append"
"#,
            catalog_uri = self.catalog_uri,
            bucket = self.bucket,
            endpoint = self.s3_endpoint,
            access_key = self.access_key,
            table = self.table,
        )
    }

    /// Wait for the frozen input's pairs, then require each exactly once in the data files.
    pub(super) fn assert_exact(
        &self,
        nodes: &mut [Node],
        expected: &BTreeSet<(u64, u64)>,
        window: Duration,
        label: &str,
    ) {
        let expected_rows = u64::try_from(expected.len()).expect("pair count fits u64");
        let deadline = Instant::now() + window;
        let metadata = loop {
            assert_running_nodes(nodes);
            if let Ok(metadata) = self.runtime.block_on(self.load()) {
                let records = metadata.total_records();
                assert!(
                    records <= expected_rows,
                    "{label} Iceberg table holds {records} rows for {expected_rows} expected pairs"
                );
                if records == expected_rows {
                    break metadata;
                }
            }
            assert!(
                Instant::now() < deadline,
                "{label} Iceberg table did not reach {expected_rows} rows within {window:?}"
            );
            std::thread::sleep(Duration::from_millis(250));
        };
        metadata.validate_exact_commit_identities();
        let pairs = self.read_pairs(&metadata, label);
        assert!(
            &pairs == expected,
            "{label} Iceberg pairs differ from the frozen input oracle: {} observed, {} expected",
            pairs.len(),
            expected.len()
        );
        eprintln!(
            "soak: {label} Iceberg table holds all {} pairs exactly once across {} checkpoint snapshots",
            pairs.len(),
            metadata.laminar_snapshot_count()
        );
    }

    async fn load(&self) -> Result<TableMetadata, String> {
        iceberg_table::load_table(self.client.get(format!(
            "{}/v1/namespaces/{NAMESPACE}/tables/{}",
            self.catalog_uri, self.table
        )))
        .await
    }

    fn read_pairs(&self, metadata: &TableMetadata, label: &str) -> BTreeSet<(u64, u64)> {
        let store = AmazonS3Builder::new()
            .with_access_key_id(&self.access_key)
            .with_secret_access_key(std::env::var(SECRET_ENV).expect("Iceberg S3 secret"))
            .with_endpoint(&self.s3_endpoint)
            .with_allow_http(true)
            .with_bucket_name(&self.bucket)
            .with_region(REGION)
            .build()
            .expect("build Iceberg data-file reader");
        let batches = self
            .runtime
            .block_on(iceberg_table::read_current_data_files(
                &store,
                &self.bucket,
                metadata,
            ));
        let mut pairs = BTreeSet::new();
        for batch in batches {
            let column = |name: &str| {
                let index = batch.schema().index_of(name).expect("Iceberg join column");
                batch
                    .column(index)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("Iceberg join IDs must be Int64")
                    .clone()
            };
            let (left, right) = (column("left_id"), column("right_id"));
            for row in 0..batch.num_rows() {
                assert!(
                    !left.is_null(row) && !right.is_null(row),
                    "{label} Iceberg output contains a null join ID"
                );
                let pair = (
                    u64::try_from(left.value(row)).expect("nonnegative left_id"),
                    u64::try_from(right.value(row)).expect("nonnegative right_id"),
                );
                assert!(
                    pairs.insert(pair),
                    "{label} Iceberg output contains duplicate pair {pair:?}"
                );
            }
        }
        pairs
    }
}
