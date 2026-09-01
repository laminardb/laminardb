use super::*;

#[test]
fn test_granularity_default() {
    assert_eq!(
        TimeSeriesGranularity::default(),
        TimeSeriesGranularity::Seconds
    );
}

#[test]
fn test_custom_granularity_valid() {
    let g = TimeSeriesGranularity::custom(3600, 3600).unwrap();
    assert!(matches!(g, TimeSeriesGranularity::Custom { .. }));
}

#[test]
fn test_custom_granularity_mismatch() {
    let err = TimeSeriesGranularity::custom(3600, 1800).unwrap_err();
    assert!(err.to_string().contains("bucket_max_span_seconds"));
}

#[test]
fn test_custom_granularity_zero() {
    let err = TimeSeriesGranularity::custom(0, 0).unwrap_err();
    assert!(err.to_string().contains("between 1"));
}

#[test]
fn custom_granularity_rejects_server_limit_overflow() {
    let error = TimeSeriesGranularity::custom(31_536_001, 31_536_001).unwrap_err();
    assert!(error.to_string().contains("31536000"));
}

#[test]
fn test_collection_kind_default() {
    assert!(matches!(
        CollectionKind::default(),
        CollectionKind::Standard
    ));
}

#[test]
fn test_granularity_display() {
    assert_eq!(TimeSeriesGranularity::Seconds.to_string(), "seconds");
    assert_eq!(TimeSeriesGranularity::Minutes.to_string(), "minutes");
    assert_eq!(TimeSeriesGranularity::Hours.to_string(), "hours");
    let custom = TimeSeriesGranularity::custom(7200, 7200).unwrap();
    assert_eq!(custom.to_string(), "custom(7200s)");
}

#[test]
fn time_series_config_rejects_conflicting_metadata_and_ttl_overflow() {
    let mut config = TimeSeriesConfig {
        time_field: "ts".into(),
        meta_field: Some("ts".into()),
        granularity: TimeSeriesGranularity::Seconds,
        expire_after_seconds: None,
    };
    assert!(config
        .validate()
        .unwrap_err()
        .to_string()
        .contains("differ"));

    config.meta_field = Some("_id".into());
    assert!(config.validate().unwrap_err().to_string().contains("_id"));

    config.meta_field = None;
    config.expire_after_seconds = Some(u64::try_from(i64::MAX).unwrap() + 1);
    assert!(config
        .validate()
        .unwrap_err()
        .to_string()
        .contains("signed 64-bit"));
}

#[test]
fn config_validation_cannot_bypass_field_and_custom_bucket_invariants() {
    let mut config = TimeSeriesConfig {
        time_field: "bad\0field".into(),
        meta_field: None,
        granularity: TimeSeriesGranularity::Seconds,
        expire_after_seconds: None,
    };
    assert!(config.validate().unwrap_err().to_string().contains("NUL"));

    config.time_field = "ts".into();
    config.granularity = TimeSeriesGranularity::Custom {
        bucket_max_span_seconds: 60,
        bucket_rounding_seconds: 30,
    };
    assert!(config
        .validate()
        .unwrap_err()
        .to_string()
        .contains("bucket_rounding_seconds"));
}
