use super::*;

fn test_config() -> ReconnectConfig {
    ReconnectConfig {
        enabled: true,
        initial_delay: Duration::from_millis(100),
        max_delay: Duration::from_secs(30),
        max_retries: None,
    }
}

#[test]
fn test_current_url() {
    let mgr = ConnectionManager::new(vec!["ws://a".into(), "ws://b".into()], test_config());
    assert_eq!(mgr.current_url(), "ws://a");
}

#[test]
fn test_failover_cycles_urls() {
    let mut mgr = ConnectionManager::new(
        vec!["ws://a".into(), "ws://b".into(), "ws://c".into()],
        test_config(),
    );

    mgr.next_backoff();
    assert_eq!(mgr.current_url(), "ws://b");

    mgr.next_backoff();
    assert_eq!(mgr.current_url(), "ws://c");

    mgr.next_backoff();
    assert_eq!(mgr.current_url(), "ws://a");
}

#[test]
fn test_exponential_backoff() {
    let mut mgr = ConnectionManager::new(vec!["ws://a".into()], test_config());

    let d1 = mgr.next_backoff().unwrap();
    assert!((75..=125).contains(&d1.as_millis()));

    let d2 = mgr.next_backoff().unwrap();
    assert!((150..=250).contains(&d2.as_millis()));

    let d3 = mgr.next_backoff().unwrap();
    assert!((300..=500).contains(&d3.as_millis()));
}

#[test]
fn test_max_delay_cap() {
    let config = ReconnectConfig {
        enabled: true,
        initial_delay: Duration::from_secs(20),
        max_delay: Duration::from_secs(30),
        max_retries: None,
    };
    let mut mgr = ConnectionManager::new(vec!["ws://a".into()], config);

    mgr.next_backoff(); // 20s
    let d2 = mgr.next_backoff().unwrap(); // would be 40s, capped to 30s
    assert!((Duration::from_millis(22_500)..=Duration::from_secs(30)).contains(&d2));
}

#[test]
fn test_max_retries() {
    let config = ReconnectConfig {
        max_retries: Some(2),
        ..test_config()
    };
    let mut mgr = ConnectionManager::new(vec!["ws://a".into()], config);

    assert!(mgr.next_backoff().is_some()); // attempt 1
    assert!(mgr.next_backoff().is_some()); // attempt 2
    assert!(mgr.next_backoff().is_none()); // exceeded
}

#[test]
fn test_reset() {
    let mut mgr = ConnectionManager::new(vec!["ws://a".into()], test_config());

    mgr.next_backoff();
    mgr.next_backoff();
    assert_eq!(mgr.attempt(), 2);

    mgr.reset();
    assert_eq!(mgr.attempt(), 0);

    let d = mgr.next_backoff().unwrap();
    assert!((75..=125).contains(&d.as_millis()));
}

#[test]
fn test_disabled_reconnect() {
    let config = ReconnectConfig {
        enabled: false,
        ..test_config()
    };
    let mut mgr = ConnectionManager::new(vec!["ws://a".into()], config);
    assert!(mgr.next_backoff().is_none());
}
