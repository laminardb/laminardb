//! S3 storage class tiering for checkpoint data.
//!
//! Maps checkpoint objects to storage tiers (hot/warm/cold) with appropriate
//! S3 storage classes, object tags for lifecycle rules, and compression
//! strategies (LZ4 for hot, Zstd for warm/cold).

use std::fmt;

use object_store::{Attribute, Attributes, PutOptions, TagSet};

// ---------------------------------------------------------------------------
// StorageClass
// ---------------------------------------------------------------------------

/// S3-compatible storage class for checkpoint objects.
///
/// Maps to the `x-amz-storage-class` header on PUT requests. Provider-specific
/// strings are used because each cloud provider has different class names.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StorageClass {
    /// S3 Standard (default). Multi-AZ durability, low latency.
    Standard,
    /// S3 Express One Zone. Single-digit ms latency, -55% PUT / -85% GET cost.
    ExpressOneZone,
    /// S3 Intelligent-Tiering. Automatic cost optimization by access pattern.
    IntelligentTiering,
    /// S3 Glacier Instant Retrieval. Archive with millisecond access, -68% cost.
    GlacierInstantRetrieval,
}

impl StorageClass {
    /// S3 storage class header value.
    #[must_use]
    pub fn as_s3_str(self) -> &'static str {
        match self {
            Self::Standard => "STANDARD",
            Self::ExpressOneZone => "EXPRESS_ONEZONE",
            Self::IntelligentTiering => "INTELLIGENT_TIERING",
            Self::GlacierInstantRetrieval => "GLACIER_IR",
        }
    }

    /// Parse from a configuration string (case-insensitive, underscores/hyphens tolerated).
    #[must_use]
    pub fn from_config(s: &str) -> Option<Self> {
        match s.to_ascii_uppercase().replace('-', "_").as_str() {
            "STANDARD" => Some(Self::Standard),
            "EXPRESS_ONEZONE" | "EXPRESS_ONE_ZONE" => Some(Self::ExpressOneZone),
            "INTELLIGENT_TIERING" => Some(Self::IntelligentTiering),
            "GLACIER_IR" | "GLACIER_INSTANT_RETRIEVAL" => Some(Self::GlacierInstantRetrieval),
            _ => None,
        }
    }
}

impl fmt::Display for StorageClass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_s3_str())
    }
}

// ---------------------------------------------------------------------------
// StorageTier
// ---------------------------------------------------------------------------

/// Logical tier for a checkpoint object.
///
/// Each tier maps to a storage class, compression strategy, and object tag.
/// S3 Lifecycle rules target objects by the `laminardb-tier` tag to transition
/// them between storage classes over time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StorageTier {
    /// Active checkpoints. Fast access, LZ4 compression.
    Hot,
    /// Historical checkpoints (1-7 days). Zstd compression.
    Warm,
    /// Archive checkpoints. Zstd max compression.
    Cold,
}

impl StorageTier {
    /// Tag value for S3 Lifecycle rule targeting.
    #[must_use]
    pub fn tag_value(self) -> &'static str {
        match self {
            Self::Hot => "hot",
            Self::Warm => "warm",
            Self::Cold => "cold",
        }
    }
}

impl fmt::Display for StorageTier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.tag_value())
    }
}

// ---------------------------------------------------------------------------
// TieringPolicy
// ---------------------------------------------------------------------------

/// Builds [`PutOptions`] and selects compression for each storage tier.
///
/// Constructed from config strings (hot/warm/cold class names). When no
/// tiering is configured, all objects use `Standard` class with LZ4.
#[derive(Debug, Clone)]
#[allow(clippy::struct_field_names)]
pub struct TieringPolicy {
    hot_class: StorageClass,
    warm_class: StorageClass,
    cold_class: Option<StorageClass>,
}

impl TieringPolicy {
    /// Create a tiering policy from config class names.
    ///
    /// Falls back to `Standard` for unrecognized or empty class strings.
    #[must_use]
    pub fn new(hot_class: &str, warm_class: &str, cold_class: &str) -> Self {
        Self {
            hot_class: StorageClass::from_config(hot_class).unwrap_or(StorageClass::Standard),
            warm_class: StorageClass::from_config(warm_class).unwrap_or(StorageClass::Standard),
            cold_class: if cold_class.is_empty() {
                None
            } else {
                StorageClass::from_config(cold_class)
            },
        }
    }

    /// Default policy: all tiers use `Standard`.
    #[must_use]
    pub fn standard() -> Self {
        Self {
            hot_class: StorageClass::Standard,
            warm_class: StorageClass::Standard,
            cold_class: None,
        }
    }

    /// Storage class for the given tier.
    #[must_use]
    pub fn storage_class(&self, tier: StorageTier) -> StorageClass {
        match tier {
            StorageTier::Hot => self.hot_class,
            StorageTier::Warm => self.warm_class,
            StorageTier::Cold => self.cold_class.unwrap_or(self.warm_class),
        }
    }

    /// Whether a cold tier is configured.
    #[must_use]
    pub fn has_cold_tier(&self) -> bool {
        self.cold_class.is_some()
    }

    /// Build [`PutOptions`] for the given tier.
    ///
    /// Sets:
    /// - `Attribute::StorageClass` to the tier's S3 class
    /// - `laminardb-tier` tag for lifecycle rule targeting
    #[must_use]
    pub fn put_options(&self, tier: StorageTier) -> PutOptions {
        let class = self.storage_class(tier);

        let mut attrs = Attributes::new();
        attrs.insert(Attribute::StorageClass, class.as_s3_str().into());

        let mut tags = TagSet::default();
        tags.push("laminardb-tier", tier.tag_value());

        PutOptions {
            attributes: attrs,
            tags,
            ..PutOptions::default()
        }
    }

    /// Build [`PutOptions`] for the given tier with conditional-create mode.
    #[must_use]
    pub fn put_options_create(&self, tier: StorageTier) -> PutOptions {
        let mut opts = self.put_options(tier);
        opts.mode = object_store::PutMode::Create;
        opts
    }
}

impl Default for TieringPolicy {
    fn default() -> Self {
        Self::standard()
    }
}

// ---------------------------------------------------------------------------
// Tier-aware compression
// ---------------------------------------------------------------------------

/// Zstd compression level for warm tier (fast, good ratio).
const ZSTD_WARM_LEVEL: i32 = 3;

/// Zstd compression level for cold tier (max ratio, slower).
const ZSTD_COLD_LEVEL: i32 = 19;

/// Compress data using the appropriate algorithm for the given tier.
///
/// - **Hot**: LZ4 (fastest decompression for active recovery)
/// - **Warm**: Zstd level 3 (good balance of speed and ratio)
/// - **Cold**: Zstd level 19 (maximum compression for archival)
#[must_use]
#[allow(clippy::missing_panics_doc)] // zstd::encode_all on &[u8] is infallible
pub fn compress_for_tier(data: &[u8], tier: StorageTier) -> Vec<u8> {
    match tier {
        StorageTier::Hot => lz4_flex::compress_prepend_size(data),
        StorageTier::Warm => zstd::encode_all(data, ZSTD_WARM_LEVEL).unwrap(),
        StorageTier::Cold => zstd::encode_all(data, ZSTD_COLD_LEVEL).unwrap(),
    }
}

/// Decompress data using the appropriate algorithm for the given tier.
///
/// # Errors
///
/// Returns [`DecompressionError`] if the data is corrupt or not valid
/// for the tier's compression format.
pub fn decompress_for_tier(
    compressed: &[u8],
    tier: StorageTier,
) -> Result<Vec<u8>, DecompressionError> {
    match tier {
        StorageTier::Hot => lz4_flex::decompress_size_prepended(compressed)
            .map_err(|e| DecompressionError(format!("LZ4 decompression failed: {e}"))),
        StorageTier::Warm | StorageTier::Cold => zstd::decode_all(compressed)
            .map_err(|e| DecompressionError(format!("Zstd decompression failed: {e}"))),
    }
}

/// Error from tier-aware decompression.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct DecompressionError(String);

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- StorageClass --

    #[test]
    fn test_storage_class_s3_strings() {
        assert_eq!(StorageClass::Standard.as_s3_str(), "STANDARD");
        assert_eq!(StorageClass::ExpressOneZone.as_s3_str(), "EXPRESS_ONEZONE");
        assert_eq!(
            StorageClass::IntelligentTiering.as_s3_str(),
            "INTELLIGENT_TIERING"
        );
        assert_eq!(
            StorageClass::GlacierInstantRetrieval.as_s3_str(),
            "GLACIER_IR"
        );
    }

    #[test]
    fn test_storage_class_from_config() {
        assert_eq!(
            StorageClass::from_config("STANDARD"),
            Some(StorageClass::Standard)
        );
        assert_eq!(
            StorageClass::from_config("express_one_zone"),
            Some(StorageClass::ExpressOneZone)
        );
        assert_eq!(
            StorageClass::from_config("EXPRESS-ONEZONE"),
            Some(StorageClass::ExpressOneZone)
        );
        assert_eq!(
            StorageClass::from_config("intelligent_tiering"),
            Some(StorageClass::IntelligentTiering)
        );
        assert_eq!(
            StorageClass::from_config("GLACIER_IR"),
            Some(StorageClass::GlacierInstantRetrieval)
        );
        assert_eq!(
            StorageClass::from_config("glacier_instant_retrieval"),
            Some(StorageClass::GlacierInstantRetrieval)
        );
        assert_eq!(StorageClass::from_config("unknown"), None);
        assert_eq!(StorageClass::from_config(""), None);
    }

    #[test]
    fn test_storage_class_display() {
        assert_eq!(format!("{}", StorageClass::Standard), "STANDARD");
        assert_eq!(
            format!("{}", StorageClass::GlacierInstantRetrieval),
            "GLACIER_IR"
        );
    }

    // -- StorageTier --

    #[test]
    fn test_tier_tag_values() {
        assert_eq!(StorageTier::Hot.tag_value(), "hot");
        assert_eq!(StorageTier::Warm.tag_value(), "warm");
        assert_eq!(StorageTier::Cold.tag_value(), "cold");
    }

    // -- TieringPolicy --

    #[test]
    fn test_policy_from_config() {
        let policy = TieringPolicy::new("EXPRESS_ONE_ZONE", "STANDARD", "GLACIER_IR");
        assert_eq!(
            policy.storage_class(StorageTier::Hot),
            StorageClass::ExpressOneZone
        );
        assert_eq!(
            policy.storage_class(StorageTier::Warm),
            StorageClass::Standard
        );
        assert_eq!(
            policy.storage_class(StorageTier::Cold),
            StorageClass::GlacierInstantRetrieval
        );
        assert!(policy.has_cold_tier());
    }

    #[test]
    fn test_policy_no_cold_tier() {
        let policy = TieringPolicy::new("STANDARD", "STANDARD", "");
        assert!(!policy.has_cold_tier());
        // Cold falls back to warm class
        assert_eq!(
            policy.storage_class(StorageTier::Cold),
            StorageClass::Standard
        );
    }

    #[test]
    fn test_policy_standard_default() {
        let policy = TieringPolicy::standard();
        assert_eq!(
            policy.storage_class(StorageTier::Hot),
            StorageClass::Standard
        );
        assert_eq!(
            policy.storage_class(StorageTier::Warm),
            StorageClass::Standard
        );
        assert!(!policy.has_cold_tier());
    }

    #[test]
    fn test_policy_unknown_config_falls_back() {
        let policy = TieringPolicy::new("NONEXISTENT", "ALSO_BAD", "");
        assert_eq!(
            policy.storage_class(StorageTier::Hot),
            StorageClass::Standard
        );
        assert_eq!(
            policy.storage_class(StorageTier::Warm),
            StorageClass::Standard
        );
    }

    #[test]
    fn test_put_options_has_storage_class() {
        let policy = TieringPolicy::new("EXPRESS_ONE_ZONE", "STANDARD", "GLACIER_IR");
        let opts = policy.put_options(StorageTier::Hot);

        let class = opts.attributes.get(&Attribute::StorageClass);
        assert!(class.is_some());
        assert_eq!(class.unwrap().as_ref(), "EXPRESS_ONEZONE");
    }

    #[test]
    fn test_put_options_has_tier_tag() {
        let policy = TieringPolicy::new("STANDARD", "STANDARD", "");
        let opts = policy.put_options(StorageTier::Warm);

        let encoded = opts.tags.encoded();
        assert!(
            encoded.contains("laminardb-tier=warm"),
            "tag encoding: {encoded}"
        );
    }

    #[test]
    fn test_put_options_create_mode() {
        let policy = TieringPolicy::standard();
        let opts = policy.put_options_create(StorageTier::Hot);
        assert!(matches!(opts.mode, object_store::PutMode::Create));
    }

    // -- Compression --

    #[test]
    fn test_lz4_roundtrip_hot() {
        let data = b"checkpoint state data for hot tier recovery";
        let compressed = compress_for_tier(data, StorageTier::Hot);
        let decompressed = decompress_for_tier(&compressed, StorageTier::Hot).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_zstd_roundtrip_warm() {
        let data = b"checkpoint state data for warm tier archival";
        let compressed = compress_for_tier(data, StorageTier::Warm);
        let decompressed = decompress_for_tier(&compressed, StorageTier::Warm).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_zstd_roundtrip_cold() {
        let data = b"checkpoint state data for cold tier archive compliance";
        let compressed = compress_for_tier(data, StorageTier::Cold);
        let decompressed = decompress_for_tier(&compressed, StorageTier::Cold).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_cold_compresses_better_than_warm() {
        // Repetitive data to make compression ratio visible
        let data: Vec<u8> = (0..10_000u16).map(|i| (i % 256) as u8).collect();
        let warm = compress_for_tier(&data, StorageTier::Warm);
        let cold = compress_for_tier(&data, StorageTier::Cold);
        assert!(
            cold.len() <= warm.len(),
            "cold ({}) should be <= warm ({})",
            cold.len(),
            warm.len()
        );
    }

    #[test]
    fn test_decompress_corrupt_data() {
        let bad = b"not valid compressed data";
        assert!(decompress_for_tier(bad, StorageTier::Hot).is_err());
        assert!(decompress_for_tier(bad, StorageTier::Warm).is_err());
        assert!(decompress_for_tier(bad, StorageTier::Cold).is_err());
    }
}
