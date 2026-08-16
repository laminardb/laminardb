//! Cloud storage provider detection from URI schemes.
//!
//! [`StorageProvider`] identifies the cloud backend from a table path, enabling
//! provider-specific credential resolution and validation.

#[cfg(any(test, feature = "delta-lake"))]
use std::borrow::Cow;
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
    /// Detects a recognized provider URI. Unlike [`Self::detect`], an unknown
    /// URI scheme is not treated as a local path.
    #[must_use]
    pub fn detect_uri(table_path: &str) -> Option<Self> {
        let (scheme, _) = table_path.split_once("://")?;

        if scheme.eq_ignore_ascii_case("s3") || scheme.eq_ignore_ascii_case("s3a") {
            return Some(Self::AwsS3);
        }

        if ["az", "abfs", "abfss", "wasb", "wasbs"]
            .iter()
            .any(|candidate| scheme.eq_ignore_ascii_case(candidate))
        {
            return Some(Self::AzureAdls);
        }

        if scheme.eq_ignore_ascii_case("gs") || scheme.eq_ignore_ascii_case("gcs") {
            return Some(Self::Gcs);
        }

        scheme.eq_ignore_ascii_case("file").then_some(Self::Local)
    }

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
        Self::detect_uri(table_path).unwrap_or(Self::Local)
    }

    /// Whether the path names a shared cloud object store.
    #[must_use]
    pub fn is_shared_uri(table_path: &str) -> bool {
        Self::detect_uri(table_path).is_some_and(|provider| provider != Self::Local)
    }

    /// Whether the path directly names the release-admitted S3 log store.
    #[must_use]
    pub fn is_direct_s3_uri(table_path: &str) -> bool {
        let Some((scheme, _)) = table_path.split_once("://") else {
            return false;
        };
        scheme.eq_ignore_ascii_case("s3") || scheme.eq_ignore_ascii_case("s3a")
    }

    /// Maps legacy Azure Blob schemes to the Azure URI registered by delta-rs.
    #[must_use]
    #[cfg(any(test, feature = "delta-lake"))]
    pub(crate) fn canonical_uri(table_path: &str) -> Cow<'_, str> {
        let Some((scheme, rest)) = table_path.split_once("://") else {
            return Cow::Borrowed(table_path);
        };
        if scheme.eq_ignore_ascii_case("wasb") || scheme.eq_ignore_ascii_case("wasbs") {
            Cow::Owned(format!("az://{rest}"))
        } else {
            Cow::Borrowed(table_path)
        }
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
        assert_eq!(
            StorageProvider::canonical_uri("wasbs://container@account.blob.core.windows.net/path"),
            "az://container@account.blob.core.windows.net/path"
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
