use super::*;

#[test]
fn test_bare_metal_zero_config() {
    let config = LaminarConfig::default();
    let profile = Profile::BareMetal;

    // BareMetal needs no features and no config
    assert!(profile.validate_features().is_ok());
    assert!(profile.validate_config(&config, None).is_ok());
}

#[test]
fn test_embedded_requires_storage_dir() {
    let config = LaminarConfig::default();
    let result = Profile::Embedded.validate_config(&config, None);
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        ProfileError::RequirementNotMet(_)
    ));
}

#[test]
fn embedded_accepts_an_absolute_file_checkpoint_url() {
    let config = LaminarConfig::default();
    let directory = tempfile::tempdir().unwrap();
    let normalized = directory.path().display().to_string().replace('\\', "/");
    let url = if normalized.starts_with('/') {
        format!("file://{normalized}")
    } else {
        format!("file:///{normalized}")
    };
    assert!(Profile::Embedded
        .validate_config(&config, Some(&url))
        .is_ok());
    let uppercase_url = url.replacen("file", "FILE", 1);
    assert!(Profile::Embedded
        .validate_config(&config, Some(&uppercase_url))
        .is_ok());
    assert!(Profile::Embedded
        .validate_config(&config, Some("file://./relative"))
        .is_err());
    let config_with_fallback = LaminarConfig {
        storage_dir: Some(directory.path().to_path_buf()),
        ..LaminarConfig::default()
    };
    assert!(Profile::Embedded
        .validate_config(&config_with_fallback, Some("file://./relative"))
        .is_err());
}

#[test]
fn test_durable_fails_without_object_store_url() {
    let config = LaminarConfig::default();
    let result = Profile::Durable.validate_config(&config, None);
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        ProfileError::RequirementNotMet(_)
    ));
}

#[test]
fn test_profile_from_str() {
    assert_eq!(Profile::from_str("bare_metal").unwrap(), Profile::BareMetal);
    assert_eq!(Profile::from_str("baremetal").unwrap(), Profile::BareMetal);
    assert_eq!(Profile::from_str("bare-metal").unwrap(), Profile::BareMetal);
    assert_eq!(Profile::from_str("embedded").unwrap(), Profile::Embedded);
    assert_eq!(Profile::from_str("durable").unwrap(), Profile::Durable);
    assert_eq!(Profile::from_str("cluster").unwrap(), Profile::Cluster);
    // Case insensitive
    assert_eq!(Profile::from_str("DURABLE").unwrap(), Profile::Durable);
    // Unknown name
    assert!(Profile::from_str("quantum").is_err());
    assert!(matches!(
        Profile::from_str("quantum").unwrap_err(),
        ProfileError::UnknownProfileName(_)
    ));
}

#[test]
fn test_all_profiles_validate_features() {
    // Feature gates removed — all profiles always pass validation.
    assert!(Profile::BareMetal.validate_features().is_ok());
    assert!(Profile::Embedded.validate_features().is_ok());
    assert!(Profile::Durable.validate_features().is_ok());
    assert!(Profile::Cluster.validate_features().is_ok());
}

#[test]
fn test_profile_display() {
    assert_eq!(Profile::BareMetal.to_string(), "bare_metal");
    assert_eq!(Profile::Embedded.to_string(), "embedded");
    assert_eq!(Profile::Durable.to_string(), "durable");
    assert_eq!(Profile::Cluster.to_string(), "cluster");
}

#[test]
fn test_profile_default() {
    assert_eq!(Profile::default(), Profile::BareMetal);
}

#[test]
fn test_apply_defaults_bare_metal_noop() {
    let mut config = LaminarConfig::default();
    let original_buffer = config.default_buffer_size;
    Profile::BareMetal.apply_defaults(&mut config);
    assert_eq!(config.default_buffer_size, original_buffer);
}

#[test]
fn test_apply_defaults_does_not_override_user_values() {
    let mut config = LaminarConfig {
        default_buffer_size: 999,
        ..LaminarConfig::default()
    };
    Profile::Durable.apply_defaults(&mut config);
    // User explicitly set 999 — should not be overridden
    assert_eq!(config.default_buffer_size, 999);
}

#[test]
fn test_from_config_bare_metal() {
    let config = LaminarConfig::default();
    assert_eq!(Profile::from_config(&config, false), Profile::BareMetal);
}

#[test]
fn test_from_config_embedded_storage_dir() {
    let config = LaminarConfig {
        storage_dir: Some(std::path::PathBuf::from("/tmp/data")),
        ..LaminarConfig::default()
    };
    assert_eq!(Profile::from_config(&config, false), Profile::Embedded);
}

#[test]
fn test_from_config_embedded_file_url() {
    let config = LaminarConfig {
        object_store_url: Some("file:///tmp/checkpoints".to_string()),
        ..LaminarConfig::default()
    };
    assert_eq!(Profile::from_config(&config, false), Profile::Embedded);
}

#[test]
fn test_from_config_durable_s3() {
    let config = LaminarConfig {
        object_store_url: Some("s3://my-bucket/prefix".to_string()),
        ..LaminarConfig::default()
    };
    assert_eq!(Profile::from_config(&config, false), Profile::Durable);
}

#[test]
fn test_from_config_durable_gs() {
    let config = LaminarConfig {
        object_store_url: Some("gs://my-bucket/prefix".to_string()),
        ..LaminarConfig::default()
    };
    assert_eq!(Profile::from_config(&config, false), Profile::Durable);
}

#[test]
fn test_from_config_durable_az() {
    let config = LaminarConfig {
        object_store_url: Some("az://container/prefix".to_string()),
        ..LaminarConfig::default()
    };
    assert_eq!(Profile::from_config(&config, false), Profile::Durable);
}

#[test]
fn test_from_config_durable_abfs() {
    let config = LaminarConfig {
        object_store_url: Some("abfs://container/prefix".to_string()),
        ..LaminarConfig::default()
    };
    assert_eq!(Profile::from_config(&config, false), Profile::Durable);
}

#[test]
fn test_from_config_durable_abfss() {
    let config = LaminarConfig {
        object_store_url: Some("abfss://container/prefix".to_string()),
        ..LaminarConfig::default()
    };
    assert_eq!(Profile::from_config(&config, false), Profile::Durable);
}

#[test]
fn shared_storage_parser_drives_durable_profile_aliases() {
    for location in [
        "S3A://bucket/prefix",
        "GCS://bucket/prefix",
        "wasb://container@account.blob.core.windows.net/prefix",
        "WASBS://container@account.blob.core.windows.net/prefix",
    ] {
        let config = LaminarConfig {
            object_store_url: Some(location.to_string()),
            ..LaminarConfig::default()
        };
        assert_eq!(
            Profile::from_config(&config, false),
            Profile::Durable,
            "{location}"
        );
    }
}

#[test]
fn malformed_or_unsupported_object_store_url_is_not_treated_as_durable() {
    for location in [
        "file://relative/checkpoints",
        "https://example.invalid/checkpoints",
        "s3n://bucket/checkpoints",
    ] {
        let config = LaminarConfig {
            object_store_url: Some(location.to_string()),
            ..LaminarConfig::default()
        };
        assert_eq!(
            Profile::from_config(&config, false),
            Profile::BareMetal,
            "{location}"
        );
    }
}

#[test]
fn test_from_config_cluster() {
    let config = LaminarConfig::default();
    assert_eq!(Profile::from_config(&config, true), Profile::Cluster);
}

#[test]
fn test_from_config_cluster_overrides_url() {
    let config = LaminarConfig {
        object_store_url: Some("s3://bucket/prefix".to_string()),
        ..LaminarConfig::default()
    };
    // Discovery takes priority over URL-based detection
    assert_eq!(Profile::from_config(&config, true), Profile::Cluster);
}
