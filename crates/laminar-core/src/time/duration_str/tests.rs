use super::*;

#[test]
fn bare_seconds() {
    assert_eq!(parse_duration_str("5"), Some(Duration::from_secs(5)));
    assert_eq!(parse_duration_str(" 42 "), Some(Duration::from_secs(42)));
}

#[test]
fn suffixed() {
    assert_eq!(
        parse_duration_str("250ms"),
        Some(Duration::from_millis(250))
    );
    assert_eq!(parse_duration_str("5s"), Some(Duration::from_secs(5)));
    assert_eq!(parse_duration_str("10m"), Some(Duration::from_secs(600)));
    assert_eq!(parse_duration_str("2h"), Some(Duration::from_secs(7200)));
    assert_eq!(parse_duration_str("1d"), Some(Duration::from_secs(86_400)));
}

#[test]
fn malformed() {
    assert_eq!(parse_duration_str(""), None);
    assert_eq!(parse_duration_str("abc"), None);
    assert_eq!(parse_duration_str("5x"), None);
    assert_eq!(parse_duration_str("ms"), None);
}
