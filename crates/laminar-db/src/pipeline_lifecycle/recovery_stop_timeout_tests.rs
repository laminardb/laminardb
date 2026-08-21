use super::*;

#[test]
fn coordinated_stop_ceiling_resolves_immutable_checkpoint_config() {
    let defaults = crate::checkpoint_coordinator::CheckpointConfig::default();
    assert_eq!(
        defaults.checkpoint_timeout,
        std::time::Duration::from_secs(120)
    );
    assert_eq!(defaults.cleanup_timeout, std::time::Duration::from_secs(30));
    assert_eq!(
        PUBLIC_PIPELINE_STOP_TIMEOUT,
        std::time::Duration::from_secs(10)
    );

    let mut config = crate::config::LaminarConfig::default();
    assert_eq!(
        configured_checkpoint_timeout(&config),
        defaults.checkpoint_timeout
    );
    assert_eq!(
        coordinated_recovery_stop_ceiling(
            configured_checkpoint_timeout(&config),
            defaults.cleanup_timeout,
        ),
        std::time::Duration::from_secs(210)
    );

    config.checkpoint = Some(laminar_core::streaming::StreamCheckpointConfig {
        timeout_ms: Some(1_234),
        ..Default::default()
    });
    assert_eq!(
        configured_checkpoint_timeout(&config),
        std::time::Duration::from_millis(1_234)
    );
    assert_eq!(
        coordinated_recovery_stop_ceiling(
            configured_checkpoint_timeout(&config),
            defaults.cleanup_timeout,
        ),
        std::time::Duration::from_millis(91_234)
    );

    assert_eq!(
        coordinated_recovery_stop_ceiling(std::time::Duration::MAX, defaults.cleanup_timeout,),
        std::time::Duration::MAX
    );
    assert!(checked_pipeline_deadline(std::time::Duration::ZERO, "test").is_ok());
    assert!(checked_pipeline_deadline(std::time::Duration::MAX, "test").is_err());
}
