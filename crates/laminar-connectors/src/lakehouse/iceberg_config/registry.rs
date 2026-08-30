use crate::config::ConfigKeySpec;

pub(crate) fn sink_config_keys() -> Vec<ConfigKeySpec> {
    let mut keys = common_config_keys();
    keys.extend([
        ConfigKeySpec::optional(
            "delivery.guarantee",
            "Delivery guarantee: at-least-once or exactly-once",
            "at-least-once",
        ),
        ConfigKeySpec::optional(
            "write.mode",
            "Write mode: append, merge-on-read, or copy-on-write",
            "append",
        ),
        ConfigKeySpec::optional(
            "target.file.size.bytes",
            "Target Parquet data-file size in bytes",
            "134217728",
        ),
        ConfigKeySpec::optional(
            "parquet.row.group.size.bytes",
            "Maximum Parquet row-group size in bytes",
            "134217728",
        ),
        ConfigKeySpec::optional(
            "parquet.compression",
            "Parquet compression: zstd, snappy, lz4, or uncompressed",
            "zstd",
        ),
        ConfigKeySpec::optional(
            "max.buffer.rows",
            "Maximum rows accepted in one in-flight batch",
            "65536",
        ),
        ConfigKeySpec::optional(
            "max.buffer.bytes",
            "Maximum Arrow bytes accepted in one in-flight batch",
            "67108864",
        ),
        ConfigKeySpec::optional(
            "max.open.partitions",
            "Maximum simultaneously open partition writers",
            "64",
        ),
        ConfigKeySpec::optional(
            "max.files.per.checkpoint",
            "Maximum files emitted by one checkpoint participant",
            "4096",
        ),
        ConfigKeySpec::optional(
            "max.descriptor.bytes",
            "Maximum encoded participant descriptor size",
            "16777216",
        ),
        ConfigKeySpec::optional("max.flush.age", "Maximum age of an open data file", "300s"),
        ConfigKeySpec::optional(
            "write.distribution.mode",
            "Partition writer distribution: fanout or clustered",
            "fanout",
        ),
        ConfigKeySpec::optional(
            "identifier.fields",
            "Comma-separated Iceberg identifier field names",
            "",
        ),
        ConfigKeySpec::optional(
            "schema.evolution.mode",
            "Schema evolution policy: strict or safe",
            "strict",
        ),
        ConfigKeySpec::optional("auto.create", "Create the table when absent", "false"),
        ConfigKeySpec::optional(
            "format.version",
            "Iceberg format version for table creation",
            "2",
        ),
        ConfigKeySpec::optional(
            "partition.spec",
            "JSON partition-field array for table creation",
            "",
        ),
        ConfigKeySpec::optional("sort.order", "JSON sort-field array for table creation", ""),
    ]);
    keys
}

pub(crate) fn source_config_keys() -> Vec<ConfigKeySpec> {
    let mut keys = common_config_keys();
    keys.extend([
        ConfigKeySpec::optional(
            "read.mode",
            "Read mode: snapshot, append, or changelog",
            "snapshot",
        ),
        ConfigKeySpec::optional(
            "read.bootstrap",
            "Append bootstrap policy: initial or none",
            "initial",
        ),
        ConfigKeySpec::optional(
            "start.snapshot.id",
            "Snapshot selected for bounded reads or append lineage",
            "",
        ),
        ConfigKeySpec::optional("poll.interval", "Append metadata polling interval", "60s"),
        ConfigKeySpec::optional("projection", "Comma-separated projected column names", ""),
        ConfigKeySpec::optional("filter", "Iceberg scan filter expression", ""),
        ConfigKeySpec::optional(
            "read.max.snapshots.per.poll",
            "Maximum lineage snapshots processed by one poll",
            "1024",
        ),
        ConfigKeySpec::optional(
            "read.max.planned.files",
            "Maximum files exposed by scan planning",
            "65536",
        ),
        ConfigKeySpec::optional(
            "read.max.manifest.list.bytes",
            "Maximum encoded manifest-list size",
            "67108864",
        ),
        ConfigKeySpec::optional(
            "read.max.manifest.bytes",
            "Maximum encoded size of one manifest",
            "67108864",
        ),
        ConfigKeySpec::optional(
            "read.max.manifests.per.snapshot",
            "Maximum manifests referenced by one snapshot",
            "65536",
        ),
        ConfigKeySpec::optional(
            "read.channel.capacity",
            "Bounded scan-to-ingestion channel capacity",
            "2",
        ),
        ConfigKeySpec::optional(
            "read.scan.concurrency",
            "Maximum concurrent manifest and data-file reads",
            "4",
        ),
    ]);
    keys
}

fn common_config_keys() -> Vec<ConfigKeySpec> {
    vec![
        ConfigKeySpec::required("catalog.uri", "Catalog endpoint URI"),
        ConfigKeySpec::optional(
            "warehouse",
            "Legacy alias for catalog.warehouse; one spelling is required",
            "",
        ),
        ConfigKeySpec::required("namespace", "Iceberg namespace"),
        ConfigKeySpec::required("table.name", "Table name within the namespace"),
        ConfigKeySpec::optional(
            "catalog.warehouse",
            "Catalog warehouse name or location; required unless warehouse is set",
            "",
        ),
        ConfigKeySpec::optional(
            "catalog.type",
            "Catalog type: rest, glue, hms, s3tables, or sql",
            "rest",
        ),
        ConfigKeySpec::optional("catalog.prefix", "Catalog namespace prefix", ""),
        ConfigKeySpec::optional(
            "catalog.auth.type",
            "Catalog authentication: none, bearer, or oauth2",
            "none",
        ),
        ConfigKeySpec::optional("catalog.oauth2.server_uri", "OAuth2 token endpoint", ""),
        ConfigKeySpec::optional("catalog.oauth2.client_id", "OAuth2 client identifier", ""),
        ConfigKeySpec::optional("catalog.oauth2.scope", "OAuth2 requested scope", ""),
        ConfigKeySpec::optional(
            "catalog.access_delegation",
            "Request REST-catalog vended data credentials",
            "false",
        ),
        ConfigKeySpec::optional(
            "catalog.connect_timeout",
            "Catalog connection establishment bound",
            "10s",
        ),
        ConfigKeySpec::optional(
            "catalog.request_timeout",
            "Bound for one catalog request",
            "30s",
        ),
        ConfigKeySpec::optional(
            "catalog.commit_timeout",
            "End-to-end catalog commit bound",
            "120s",
        ),
        ConfigKeySpec::optional("storage.type", "Storage backend: s3, gcs, azure, or fs", ""),
        ConfigKeySpec::optional("storage.endpoint", "Storage service endpoint override", ""),
        ConfigKeySpec::optional("storage.region", "Storage service region", ""),
        ConfigKeySpec::optional(
            "storage.path_style",
            "Use S3 path-style addressing",
            "false",
        ),
        ConfigKeySpec::optional(
            "storage.request_timeout",
            "Bound for one storage request",
            "30s",
        ),
        ConfigKeySpec::optional(
            "storage.connect_timeout",
            "Storage connection establishment bound",
            "10s",
        ),
        ConfigKeySpec::optional(
            "storage.encryption",
            "Storage encryption: none, sse, or kms",
            "none",
        ),
        ConfigKeySpec::optional("storage.kms_key", "Resolved KMS key identifier", ""),
        ConfigKeySpec::optional("table.ref", "Named Iceberg table ref", "main"),
    ]
}
