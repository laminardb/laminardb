//! Iceberg REST metadata and data-file oracles shared by the real-process soaks.

use std::collections::{BTreeMap, BTreeSet};

use arrow_array::RecordBatch;
use futures::TryStreamExt as _;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt as _};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::Deserialize;

const CATALOG_RESPONSE_LIMIT: usize = 8 * 1024 * 1024;
const DATA_FILE_ORACLE_LIMIT: u64 = 8 * 1024 * 1024;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct LoadTableResponse {
    metadata: TableMetadata,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) struct TableMetadata {
    pub(crate) location: String,
    current_snapshot_id: Option<i64>,
    #[serde(default)]
    snapshots: Vec<Snapshot>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct Snapshot {
    snapshot_id: i64,
    summary: BTreeMap<String, String>,
}

impl TableMetadata {
    pub(crate) fn total_records(&self) -> u64 {
        self.current_summary_count("total-records")
    }

    pub(crate) fn total_data_files(&self) -> u64 {
        self.current_summary_count("total-data-files")
    }

    fn current_summary_count(&self, key: &str) -> u64 {
        self.current_snapshot()
            .and_then(|snapshot| snapshot.summary.get(key))
            .and_then(|value| value.parse().ok())
            .unwrap_or(0)
    }

    fn current_snapshot(&self) -> Option<&Snapshot> {
        let current = self.current_snapshot_id?;
        self.snapshots
            .iter()
            .find(|snapshot| snapshot.snapshot_id == current)
    }

    pub(crate) fn commit_uuid(&self, checkpoint_id: u64) -> Option<&str> {
        self.snapshots.iter().find_map(|snapshot| {
            let committed_checkpoint = snapshot
                .summary
                .get("laminardb.checkpoint.id")
                .and_then(|value| value.parse().ok());
            if committed_checkpoint != Some(checkpoint_id) {
                return None;
            }
            snapshot
                .summary
                .get("laminardb.commit.uuid")
                .map(String::as_str)
        })
    }

    /// Every LaminarDB snapshot belongs to exactly one checkpoint and carries its full identity.
    pub(crate) fn validate_exact_commit_identities(&self) {
        let mut checkpoints = BTreeSet::new();
        let mut commit_uuids = BTreeSet::new();
        for snapshot in &self.snapshots {
            let Some(checkpoint) = snapshot.summary.get("laminardb.checkpoint.id") else {
                continue;
            };
            let checkpoint = checkpoint
                .parse::<u64>()
                .expect("LaminarDB snapshot checkpoint must be a u64");
            let commit_uuid = snapshot
                .summary
                .get("laminardb.commit.uuid")
                .expect("LaminarDB snapshot must carry a commit UUID");
            assert!(
                checkpoints.insert(checkpoint),
                "checkpoint {checkpoint} produced more than one Iceberg snapshot"
            );
            assert!(
                commit_uuids.insert(commit_uuid),
                "Iceberg commit UUID {commit_uuid} was reused across checkpoints"
            );
            for required in [
                "laminardb.commit.namespace",
                "laminardb.deployment.id",
                "laminardb.sink.id",
                "laminardb.fencing.token",
                "laminardb.batch.fingerprint",
                "laminardb.file-set.fingerprint",
            ] {
                assert!(
                    snapshot
                        .summary
                        .get(required)
                        .is_some_and(|value| !value.is_empty()),
                    "Iceberg snapshot {} lacks {required}",
                    snapshot.snapshot_id
                );
            }
        }
    }

    pub(crate) fn laminar_snapshot_count(&self) -> usize {
        self.snapshots
            .iter()
            .filter(|snapshot| snapshot.summary.contains_key("laminardb.checkpoint.id"))
            .count()
    }
}

/// Load table metadata through an already-authorized REST `GET .../tables/{table}` request.
pub(crate) async fn load_table(request: reqwest::RequestBuilder) -> Result<TableMetadata, String> {
    let response = request
        .send()
        .await
        .map_err(|error| error.to_string())?
        .error_for_status()
        .map_err(|error| error.to_string())?;
    let bytes = response.bytes().await.map_err(|error| error.to_string())?;
    if bytes.len() > CATALOG_RESPONSE_LIMIT {
        return Err(format!(
            "Iceberg load-table response exceeds {CATALOG_RESPONSE_LIMIT} bytes"
        ));
    }
    serde_json::from_slice::<LoadTableResponse>(&bytes)
        .map(|response| response.metadata)
        .map_err(|error| error.to_string())
}

/// Create `namespace` (if absent) and a format-v2 `table` whose schema has `fields`.
pub(crate) async fn create_table(
    client: &reqwest::Client,
    catalog_uri: &str,
    authorize: impl Fn(reqwest::RequestBuilder) -> reqwest::RequestBuilder,
    namespace: &str,
    table: &str,
    fields: serde_json::Value,
) {
    let namespace_response = authorize(client.post(format!("{catalog_uri}/v1/namespaces")))
        .json(&serde_json::json!({
            "namespace": [namespace],
            "properties": {}
        }))
        .send()
        .await
        .expect("create Iceberg namespace");
    assert!(
        namespace_response.status().is_success()
            || namespace_response.status() == reqwest::StatusCode::CONFLICT,
        "create Iceberg namespace returned {}",
        namespace_response.status()
    );
    let table_response =
        authorize(client.post(format!("{catalog_uri}/v1/namespaces/{namespace}/tables")))
            .json(&serde_json::json!({
                "name": table,
                "schema": {
                    "type": "struct",
                    "schema-id": 0,
                    "fields": fields,
                    "identifier-field-ids": []
                },
                "stage-create": false,
                "properties": { "format-version": "2" }
            }))
            .send()
            .await
            .expect("create Iceberg table");
    assert!(
        table_response.status().is_success(),
        "create Iceberg table returned {}",
        table_response.status()
    );
}

/// Read every Parquet data file under the table's data prefix in `bucket`.
///
/// Append-only tables never rewrite files, so the listed files must be exactly the current
/// snapshot's data files: a surviving staging object or an extra file fails the oracle.
pub(crate) async fn read_current_data_files(
    store: &dyn ObjectStore,
    bucket: &str,
    metadata: &TableMetadata,
) -> Vec<RecordBatch> {
    let prefix = metadata
        .location
        .strip_prefix(&format!("s3://{bucket}/"))
        .expect("Iceberg table location must use the configured test bucket");
    let expected_files = metadata.total_data_files();
    let data_prefix = ObjectPath::parse(format!("{prefix}/data"))
        .expect("Iceberg table data prefix must be a valid object-store path");
    let mut objects = store.list(Some(&data_prefix));
    let mut parquet_count = 0_u64;
    let mut batches = Vec::new();
    while let Some(object) = objects.try_next().await.expect("list Iceberg data files") {
        assert!(
            !object.location.as_ref().contains("-stage-"),
            "a publish-ineligible staging object survived recovery"
        );
        if !object.location.as_ref().ends_with(".parquet") {
            continue;
        }
        parquet_count = parquet_count
            .checked_add(1)
            .expect("data-file count must not overflow");
        assert!(
            parquet_count <= expected_files,
            "listed Parquet files exceed Iceberg's current data-file count"
        );
        assert!(object.size >= 8, "Iceberg data file is incomplete");
        assert!(
            object.size <= DATA_FILE_ORACLE_LIMIT,
            "Iceberg data file exceeds the bounded soak oracle"
        );
        let object_size = usize::try_from(object.size).expect("bounded data-file size fits usize");
        let bytes = store
            .get(&object.location)
            .await
            .expect("read Iceberg data file")
            .bytes()
            .await
            .expect("collect bounded Iceberg data file");
        assert_eq!(bytes.len(), object_size, "Iceberg data file was truncated");
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .expect("read Iceberg Parquet metadata")
            .with_batch_size(1024)
            .build()
            .expect("build Iceberg Parquet row oracle");
        for batch in reader {
            batches.push(batch.expect("decode Iceberg Parquet rows"));
        }
    }
    assert_eq!(
        parquet_count, expected_files,
        "listed Parquet files differ from Iceberg's current data-file count"
    );
    batches
}
