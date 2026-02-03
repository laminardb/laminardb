//! Cloud storage provider detection from URI schemes.
//!
//! [`StorageProvider`] identifies the cloud backend from a table path, enabling
//! provider-specific credential resolution and validation.

use std::fmt;

/// Cloud storage provider detected from URI scheme.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StorageProvider {
    /// Amazon S3 or S3-compatible (`MinIO`, `LocalStack`).
    AwsS3,
    /// Azure Data Lake Storage Gen2 / Azure Blob Storage.
    AzureAdls,
    /// Google Cloud Storage.
    Gcs,
    /// Local filesystem (no credentials needed).
    Local,
}

impl StorageProvider {
    /// Detects the storage provider from a table path URI.
    ///
    /// # Examples
    ///
    /// ```
    /// use laminar_connectors::storage::StorageProvider;
    ///
    /// assert_eq!(StorageProvider::detect("s3://bucket/path"), StorageProvider::AwsS3);
    /// assert_eq!(StorageProvider::detect("az://container/path"), StorageProvider::AzureAdls);
    /// assert_eq!(StorageProvider::detect("gs://bucket/path"), StorageProvider::Gcs);
    /// assert_eq!(StorageProvider::detect("/local/path"), StorageProvider::Local);
    /// ```
    #[must_use]
    pub fn detect(table_path: &str) -> Self {
        let lower = table_path.to_lowercase();

        if lower.starts_with("s3://") || lower.starts_with("s3a://") {
            return Self::AwsS3;
        }

        if lower.starts_with("az://")
            || lower.starts_with("abfs://")
            || lower.starts_with("abfss://")
            || lower.starts_with("wasb://")
            || lower.starts_with("wasbs://")
        {
            return Self::AzureAdls;
        }

        if lower.starts_with("gs://") || lower.starts_with("gcs://") {
            return Self::Gcs;
        }

        Self::Local
    }

    /// Returns true if this provider requires cloud credentials.
    #[must_use]
    pub const fn requires_credentials(self) -> bool {
        !matches!(self, Self::Local)
    }

    /// Returns the display name for this provider.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::AwsS3 => "AWS S3",
            Self::AzureAdls => "Azure ADLS",
            Self::Gcs => "Google Cloud Storage",
            Self::Local => "Local Filesystem",
        }
    }
}

impl fmt::Display for StorageProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── S3 detection ──

    #[test]
    fn test_detect_s3_scheme() {
        assert_eq!(
            StorageProvider::detect("s3://bucket/path"),
            StorageProvider::AwsS3
        );
    }

    #[test]
    fn test_detect_s3a_scheme() {
        assert_eq!(
            StorageProvider::detect("s3a://bucket/path"),
            StorageProvider::AwsS3
        );
    }

    #[test]
    fn test_detect_s3_case_insensitive() {
        assert_eq!(
            StorageProvider::detect("S3://Bucket/Path"),
            StorageProvider::AwsS3
        );
    }

    // ── Azure detection ──

    #[test]
    fn test_detect_az_scheme() {
        assert_eq!(
            StorageProvider::detect("az://container/path"),
            StorageProvider::AzureAdls
        );
    }

    #[test]
    fn test_detect_abfss_scheme() {
        assert_eq!(
            StorageProvider::detect("abfss://container@account.dfs.core.windows.net/path"),
            StorageProvider::AzureAdls
        );
    }

    #[test]
    fn test_detect_abfs_scheme() {
        assert_eq!(
            StorageProvider::detect("abfs://container@account/path"),
            StorageProvider::AzureAdls
        );
    }

    #[test]
    fn test_detect_wasbs_scheme() {
        assert_eq!(
            StorageProvider::detect("wasbs://container@account.blob.core.windows.net/path"),
            StorageProvider::AzureAdls
        );
    }

    // ── GCS detection ──

    #[test]
    fn test_detect_gs_scheme() {
        assert_eq!(
            StorageProvider::detect("gs://bucket/path"),
            StorageProvider::Gcs
        );
    }

    #[test]
    fn test_detect_gcs_scheme() {
        assert_eq!(
            StorageProvider::detect("gcs://bucket/path"),
            StorageProvider::Gcs
        );
    }

    // ── Local detection ──

    #[test]
    fn test_detect_local_absolute() {
        assert_eq!(
            StorageProvider::detect("/data/tables/t1"),
            StorageProvider::Local
        );
    }

    #[test]
    fn test_detect_local_relative() {
        assert_eq!(
            StorageProvider::detect("./data/tables"),
            StorageProvider::Local
        );
    }

    #[test]
    fn test_detect_file_scheme() {
        assert_eq!(
            StorageProvider::detect("file:///data/tables"),
            StorageProvider::Local
        );
    }

    #[test]
    fn test_detect_windows_path() {
        assert_eq!(
            StorageProvider::detect("C:\\data\\tables"),
            StorageProvider::Local
        );
    }

    #[test]
    fn test_detect_windows_forward_slash() {
        assert_eq!(
            StorageProvider::detect("C:/data/tables"),
            StorageProvider::Local
        );
    }

    // ── Properties ──

    #[test]
    fn test_requires_credentials_cloud() {
        assert!(StorageProvider::AwsS3.requires_credentials());
        assert!(StorageProvider::AzureAdls.requires_credentials());
        assert!(StorageProvider::Gcs.requires_credentials());
    }

    #[test]
    fn test_requires_credentials_local() {
        assert!(!StorageProvider::Local.requires_credentials());
    }

    #[test]
    fn test_display() {
        assert_eq!(StorageProvider::AwsS3.to_string(), "AWS S3");
        assert_eq!(StorageProvider::AzureAdls.to_string(), "Azure ADLS");
        assert_eq!(StorageProvider::Gcs.to_string(), "Google Cloud Storage");
        assert_eq!(StorageProvider::Local.to_string(), "Local Filesystem");
    }

    #[test]
    fn test_name() {
        assert_eq!(StorageProvider::AwsS3.name(), "AWS S3");
        assert_eq!(StorageProvider::Local.name(), "Local Filesystem");
    }
}
