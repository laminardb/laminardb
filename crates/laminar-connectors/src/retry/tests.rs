use super::*;

#[test]
fn delay_caps_at_max() {
    let b = Backoff::new(Duration::from_secs(1), Duration::from_secs(30), 0.0);
    assert_eq!(b.delay(0), Duration::from_secs(1));
    assert_eq!(b.delay(1), Duration::from_secs(2));
    assert_eq!(b.delay(4), Duration::from_secs(16));
    assert_eq!(b.delay(5), Duration::from_secs(30));
    // Past saturation still capped, no panic.
    assert_eq!(b.delay(100), Duration::from_secs(30));
    assert_eq!(b.delay(u32::MAX), Duration::from_secs(30));
}

#[test]
fn jitter_stays_inside_bounds() {
    let initial = Duration::from_secs(1);
    let max = Duration::from_secs(60);
    let b = Backoff::new(initial, max, 0.25);
    for attempt in 0..7 {
        let d = b.delay(attempt);
        let raw = (initial.saturating_mul(1u32 << attempt)).min(max);
        let lo = raw.as_secs_f64() * 0.74;
        let hi = raw.as_secs_f64() * 1.26;
        let actual = d.as_secs_f64();
        assert!(
            actual >= lo && actual <= hi,
            "attempt {attempt}: {actual} not in [{lo}, {hi}]"
        );
    }
}

#[test]
fn jitter_never_exceeds_hard_cap() {
    let b = Backoff::new(Duration::from_secs(30), Duration::from_secs(30), 0.25);
    for _ in 0..100 {
        assert!(b.delay(0) <= Duration::from_secs(30));
    }
}

#[test]
fn shift_overflow_protected() {
    // The previous hand-rolled `1u64 << consecutive_failures` panicked
    // for attempt >= 64. Backoff must not.
    let b = Backoff::broker_reconnect();
    let _ = b.delay(64);
    let _ = b.delay(u32::MAX);
}
