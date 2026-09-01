use super::*;

#[test]
fn test_default_config() {
    let config = ChannelConfig::default();
    assert_eq!(config.buffer_size, DEFAULT_BUFFER_SIZE);
}

#[test]
fn test_buffer_size_clamping() {
    let config = ChannelConfig::with_buffer_size(0);
    assert_eq!(config.buffer_size, MIN_BUFFER_SIZE);

    let config = ChannelConfig::with_buffer_size(usize::MAX);
    assert_eq!(config.buffer_size, MAX_BUFFER_SIZE);
}

#[test]
fn test_source_config() {
    let config = SourceConfig::with_buffer_size(512);
    assert_eq!(config.channel.buffer_size, 512);
    assert!(config.name.is_none());

    let config = SourceConfig::named("my_source");
    assert_eq!(config.name.as_deref(), Some("my_source"));
}

#[test]
fn test_backpressure_parse() {
    assert_eq!(
        "block".parse::<BackpressureStrategy>().unwrap(),
        BackpressureStrategy::Block
    );
    assert_eq!(
        "reject".parse::<BackpressureStrategy>().unwrap(),
        BackpressureStrategy::Reject
    );
    assert!("invalid".parse::<BackpressureStrategy>().is_err());
}
