use super::*;

#[test]
fn test_watermark_creation() {
    let watermark = Watermark::new(1000);
    assert_eq!(watermark.timestamp(), 1000);
}

#[test]
fn test_watermark_late_detection() {
    let watermark = Watermark::new(1000);
    assert!(watermark.is_late(999));
    assert!(!watermark.is_late(1000));
    assert!(!watermark.is_late(1001));
}

#[test]
fn test_watermark_min_max() {
    let w1 = Watermark::new(1000);
    let w2 = Watermark::new(2000);

    assert_eq!(w1.min(w2), Watermark::new(1000));
    assert_eq!(w1.max(w2), Watermark::new(2000));
}

#[test]
fn test_watermark_ordering() {
    let w1 = Watermark::new(1000);
    let w2 = Watermark::new(2000);

    assert!(w1 < w2);
    assert!(w2 > w1);
    assert_eq!(w1, Watermark::new(1000));
}

#[test]
fn test_watermark_conversions() {
    let wm = Watermark::from(1000i64);
    assert_eq!(wm.timestamp(), 1000);

    let ts: i64 = wm.into();
    assert_eq!(ts, 1000);
}

#[test]
fn test_watermark_default() {
    let wm = Watermark::default();
    assert_eq!(wm.timestamp(), i64::MIN);
}

#[test]
fn test_timer_service_creation() {
    let service = TimerService::new();
    assert_eq!(service.pending_count(), 0);
    assert_eq!(service.next_timer_timestamp(), None);
}

#[test]
fn test_timer_registration() {
    let mut service = TimerService::new();

    let id1 = service.register_timer(100, None, None);
    let id2 = service.register_timer(50, Some(TimerKey::from_slice(&[1, 2, 3])), Some(1));

    assert_eq!(service.pending_count(), 2);
    assert_ne!(id1, id2);
}

#[test]
fn test_timer_poll_order() {
    let mut service = TimerService::new();

    let id1 = service.register_timer(100, None, None);
    let id2 = service.register_timer(50, Some(TimerKey::from_slice(&[1, 2, 3])), Some(0));
    let _id3 = service.register_timer(150, None, None);

    // Poll at time 75 - should get timer at t=50
    let fired = service.poll_timers(75);
    assert_eq!(fired.len(), 1);
    assert_eq!(fired[0].id, id2);
    assert_eq!(fired[0].key, Some(TimerKey::from_slice(&[1, 2, 3])));

    // Poll at time 125 - should get timer at t=100
    let fired = service.poll_timers(125);
    assert_eq!(fired.len(), 1);
    assert_eq!(fired[0].id, id1);

    // Poll at time 200 - should get timer at t=150
    let fired = service.poll_timers(200);
    assert_eq!(fired.len(), 1);

    assert_eq!(service.pending_count(), 0);
}

#[test]
fn test_timer_poll_multiple() {
    let mut service = TimerService::new();

    service.register_timer(50, None, None);
    service.register_timer(75, None, None);
    service.register_timer(100, None, None);

    // Poll at time 80 - should get timers at t=50 and t=75
    let fired = service.poll_timers(80);
    assert_eq!(fired.len(), 2);
    // Should be in timestamp order
    assert_eq!(fired[0].timestamp, 50);
    assert_eq!(fired[1].timestamp, 75);
}

#[test]
fn test_timer_cancel() {
    let mut service = TimerService::new();

    let id1 = service.register_timer(100, None, None);
    let id2 = service.register_timer(200, None, None);

    assert!(service.cancel_timer(id1));
    assert_eq!(service.pending_count(), 1);

    // Should not be able to cancel again
    assert!(!service.cancel_timer(id1));

    // Cancel the remaining timer
    assert!(service.cancel_timer(id2));
    assert_eq!(service.pending_count(), 0);
}

#[test]
fn test_timer_next_timestamp() {
    let mut service = TimerService::new();

    assert_eq!(service.next_timer_timestamp(), None);

    service.register_timer(100, None, None);
    assert_eq!(service.next_timer_timestamp(), Some(100));

    service.register_timer(50, None, None);
    assert_eq!(service.next_timer_timestamp(), Some(50));
}

#[test]
fn test_timer_clear() {
    let mut service = TimerService::new();

    service.register_timer(100, None, None);
    service.register_timer(200, None, None);
    service.register_timer(300, None, None);

    service.clear();
    assert_eq!(service.pending_count(), 0);
    assert_eq!(service.next_timer_timestamp(), None);
}

#[test]
fn test_bounded_watermark_generator() {
    let mut generator = BoundedOutOfOrdernessGenerator::new(100);

    // First event
    let wm1 = generator.on_event(1000);
    assert_eq!(wm1, Some(Watermark::new(900)));

    // Out of order event - no new watermark
    let wm2 = generator.on_event(800);
    assert!(wm2.is_none());

    // New max timestamp
    let wm3 = generator.on_event(1200);
    assert_eq!(wm3, Some(Watermark::new(1100)));
}

#[test]
fn test_ascending_watermark_generator() {
    let mut generator = AscendingTimestampsGenerator::new();

    let wm1 = generator.on_event(1000);
    assert_eq!(wm1, Some(Watermark::new(1000)));

    let wm2 = generator.on_event(2000);
    assert_eq!(wm2, Some(Watermark::new(2000)));

    // Out of order - no watermark
    let wm3 = generator.on_event(1500);
    assert_eq!(wm3, None);
}

#[test]
fn test_watermark_tracker_basic() {
    let mut tracker = WatermarkTracker::new(2);

    tracker.update_source(0, 1000);
    let wm = tracker.update_source(1, 500);

    assert_eq!(wm, Some(Watermark::new(500)));
}

#[test]
fn test_watermark_tracker_idle() {
    let mut tracker = WatermarkTracker::new(2);

    tracker.update_source(0, 5000);
    tracker.update_source(1, 1000);

    // Mark slow source as idle
    let wm = tracker.mark_idle(1);
    assert_eq!(wm, Some(Watermark::new(5000)));

    assert!(tracker.is_idle(1));
    assert!(!tracker.is_idle(0));
}
