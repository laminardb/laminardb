use std::fmt;
use std::str::FromStr;

macro_rules! string_enum {
    (
        $(#[$meta:meta])*
        pub enum $name:ident {
            $($(#[$variant_meta:meta])* $variant:ident => [$canonical:literal $(, $alias:literal)*]),+ $(,)?
        }
        default $default:ident;
        error $error:literal;
    ) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub enum $name {
            $($(#[$variant_meta])* $variant),+
        }

        impl Default for $name {
            fn default() -> Self {
                Self::$default
            }
        }

        impl FromStr for $name {
            type Err = String;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                match value.trim().to_ascii_lowercase().as_str() {
                    $($canonical $(| $alias)* => Ok(Self::$variant),)+
                    other => Err(format!(concat!($error, ": '{}'"), other)),
                }
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                match self {
                    $(Self::$variant => formatter.write_str($canonical),)+
                }
            }
        }
    };
}

string_enum! {
    /// Read semantics exposed by the Iceberg source.
    pub enum IcebergReadMode {
        /// Read one immutable snapshot and then complete.
        Snapshot => ["snapshot"],
        /// Emit a bootstrap snapshot and then append-only snapshot additions.
        Append => ["append", "incremental"],
        /// Emit row-level changes when upstream delete semantics are available.
        Changelog => ["changelog", "cdc"]
    }
    default Snapshot;
    error "invalid Iceberg read.mode";
}

string_enum! {
    /// Bootstrap policy for append reads.
    pub enum IcebergReadBootstrap {
        /// Emit the selected starting snapshot before following later appends.
        Initial => ["initial", "snapshot", "current"],
        /// Start after the selected snapshot without emitting its rows.
        None => ["none", "skip"]
    }
    default Initial;
    error "invalid Iceberg read.bootstrap";
}

string_enum! {
    /// Mutation semantics requested from the Iceberg sink.
    pub enum IcebergWriteMode {
        /// Add new data files with an atomic Iceberg `FastAppend`.
        Append => ["append"],
        /// Write data and delete files with an atomic `RowDelta`.
        MergeOnRead => ["merge-on-read", "merge_on_read", "mor"],
        /// Atomically replace affected data files.
        CopyOnWrite => ["copy-on-write", "copy_on_write", "cow"]
    }
    default Append;
    error "invalid Iceberg write.mode";
}

string_enum! {
    /// How input rows are distributed across partition writers.
    pub enum IcebergWriteDistributionMode {
        /// Input is grouped by partition and never returns to a closed partition.
        Clustered => ["clustered"],
        /// Interleaved partitions use a bounded set of active writers.
        Fanout => ["fanout", "hash"]
    }
    default Fanout;
    error "invalid Iceberg write.distribution.mode";
}

string_enum! {
    /// Permitted schema changes while a sink is running.
    pub enum IcebergSchemaEvolutionMode {
        /// Require the schema bound at open for every checkpoint.
        Strict => ["strict", "none", "disabled"],
        /// Permit safe Iceberg promotions and nullable additions.
        Safe => ["safe", "additive"]
    }
    default Strict;
    error "invalid Iceberg schema.evolution.mode";
}

string_enum! {
    /// Iceberg catalog implementation requested by configuration.
    pub enum IcebergCatalogType {
        /// Iceberg REST catalog.
        Rest => ["rest"],
        /// AWS Glue catalog.
        Glue => ["glue"],
        /// Hive Metastore catalog.
        Hms => ["hms", "hive"],
        /// Amazon S3 Tables catalog.
        S3Tables => ["s3tables", "s3-tables"],
        /// SQL-backed Iceberg catalog.
        Sql => ["sql"]
    }
    default Rest;
    error "invalid Iceberg catalog.type";
}

string_enum! {
    /// Catalog authentication mechanism.
    pub enum IcebergCatalogAuthType {
        /// No connector-managed authentication.
        None => ["none"],
        /// Static bearer token resolved by the existing secret layer.
        Bearer => ["bearer", "token"],
        /// `OAuth2` client credentials with refresh.
        OAuth2 => ["oauth2", "oauth"]
    }
    default None;
    error "invalid Iceberg catalog.auth.type";
}

string_enum! {
    /// Storage backend used for Iceberg table data and metadata.
    pub enum IcebergStorageType {
        /// Amazon S3 or an S3-compatible object store.
        S3 => ["s3", "s3a"],
        /// Google Cloud Storage.
        Gcs => ["gcs", "gs"],
        /// Azure Data Lake Storage Gen2.
        Azure => ["azure", "azdls", "adls", "blob", "abfs", "abfss", "wasb", "wasbs"],
        /// Local filesystem storage.
        Fs => ["fs", "file", "filesystem"]
    }
    default S3;
    error "invalid Iceberg storage.type";
}

string_enum! {
    /// Server-side encryption requested for object storage.
    pub enum IcebergStorageEncryption {
        /// Use the storage service default.
        None => ["none"],
        /// Use service-managed encryption keys.
        Sse => ["sse", "sse-s3"],
        /// Use a configured key-management-service key.
        Kms => ["kms", "sse-kms"]
    }
    default None;
    error "invalid Iceberg storage.encryption";
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aliases_parse_to_typed_modes() {
        assert_eq!("incremental".parse(), Ok(IcebergReadMode::Append));
        assert_eq!("mor".parse(), Ok(IcebergWriteMode::MergeOnRead));
        assert_eq!("abfss".parse(), Ok(IcebergStorageType::Azure));
        assert!("overwrite".parse::<IcebergWriteMode>().is_err());
    }
}
