use super::*;

#[test]
fn test_bounded_generator_first_event() {
    let mut gen = BoundedOutOfOrdernessGenerator::new(100);
    let wm = gen.on_event(1000);
    assert_eq!(wm, Some(Watermark::new(900)));
    assert_eq!(gen.current_watermark(), 900);
}

#[test]
fn processing_time_domain_is_reported_for_late_filter_skip() {
    // Source-side late-filtering keys off this: a wall-clock watermark must
    // not be compared against event-time timestamps, or it drops every row.
    assert!(ProcessingTimeGenerator::new().is_processing_time());
    assert!(!BoundedOutOfOrdernessGenerator::new(100).is_processing_time());
    // The periodic wrapper reports its inner generator's time domain.
    let p = Duration::from_millis(1);
    assert!(PeriodicGenerator::new(ProcessingTimeGenerator::new(), p).is_processing_time());
    assert!(
        !PeriodicGenerator::new(BoundedOutOfOrdernessGenerator::new(100), p).is_processing_time()
    );
}

#[test]
fn test_bounded_generator_out_of_order() {
    let mut gen = BoundedOutOfOrdernessGenerator::new(100);

    // First event
    gen.on_event(1000);

    // Out of order - should not emit new watermark
    let wm = gen.on_event(800);
    assert_eq!(wm, None);
    assert_eq!(gen.current_watermark(), 900); // Still at 1000 - 100
}

#[test]
fn test_bounded_generator_advancement() {
    let mut gen = BoundedOutOfOrdernessGenerator::new(100);

    gen.on_event(1000);
    let wm = gen.on_event(1200);

    assert_eq!(wm, Some(Watermark::new(1100)));
}

#[test]
fn bounded_recovery_restore_lowers_timestamp_baseline() {
    let mut gen = BoundedOutOfOrdernessGenerator::new(100).with_max_future_skew(0);
    assert_eq!(gen.on_event(2_000), Some(Watermark::new(1_900)));

    gen.restore_watermark_for_recovery(500);
    assert_eq!(gen.current_watermark(), 500);
    // This event is below the pre-recovery maximum but above the restored
    // baseline, so it proves stale generator state was not retained.
    assert_eq!(gen.on_event(650), Some(Watermark::new(550)));
}

#[test]
fn test_bounded_generator_from_duration() {
    let gen = BoundedOutOfOrdernessGenerator::from_duration(Duration::from_secs(5));
    assert_eq!(gen.max_out_of_orderness(), 5000);
}

#[test]
fn test_bounded_generator_no_periodic() {
    let mut gen = BoundedOutOfOrdernessGenerator::new(100);
    assert_eq!(gen.on_periodic(), None);
}

#[test]
fn test_ascending_generator_advances_on_each_event() {
    let mut gen = AscendingTimestampsGenerator::new();

    let wm1 = gen.on_event(1000);
    assert_eq!(wm1, Some(Watermark::new(1000)));

    let wm2 = gen.on_event(2000);
    assert_eq!(wm2, Some(Watermark::new(2000)));
}

#[test]
fn test_ascending_generator_ignores_backwards() {
    let mut gen = AscendingTimestampsGenerator::new();

    gen.on_event(2000);
    let wm = gen.on_event(1000); // Earlier timestamp

    assert_eq!(wm, None);
    assert_eq!(gen.current_watermark(), 2000);
}

#[test]
fn ascending_recovery_restore_lowers_then_advances() {
    let mut gen = AscendingTimestampsGenerator::new().with_max_future_skew(0);
    gen.on_event(2_000);

    gen.restore_watermark_for_recovery(500);
    assert_eq!(gen.current_watermark(), 500);
    assert_eq!(gen.on_event(600), Some(Watermark::new(600)));
}

#[test]
fn test_periodic_generator_passes_through() {
    let inner = BoundedOutOfOrdernessGenerator::new(100);
    let mut gen = PeriodicGenerator::new(inner, Duration::from_millis(100));

    let wm = gen.on_event(1000);
    assert_eq!(wm, Some(Watermark::new(900)));
}

#[test]
fn test_periodic_generator_inner_access() {
    let inner = BoundedOutOfOrdernessGenerator::new(100);
    let gen = PeriodicGenerator::new(inner, Duration::from_millis(100));

    assert_eq!(gen.inner().max_out_of_orderness(), 100);
}

#[test]
fn periodic_recovery_restore_resets_inner_and_emission_frontier() {
    let inner = AscendingTimestampsGenerator::new().with_max_future_skew(0);
    let mut gen = PeriodicGenerator::new(inner, Duration::from_millis(100));
    gen.on_event(2_000);

    gen.restore_watermark_for_recovery(500);
    assert_eq!(gen.current_watermark(), 500);
    assert_eq!(gen.last_emitted_watermark, 500);
    assert_eq!(gen.on_event(600), Some(Watermark::new(600)));
}

#[test]
fn test_punctuated_generator_predicate() {
    let mut gen = PunctuatedGenerator::new(|ts| {
        if ts % 1000 == 0 {
            Some(Watermark::new(ts))
        } else {
            None
        }
    });

    assert_eq!(gen.on_event(500), None);
    assert_eq!(gen.on_event(999), None);
    assert_eq!(gen.on_event(1000), Some(Watermark::new(1000)));
    assert_eq!(gen.on_event(1500), None);
    assert_eq!(gen.on_event(2000), Some(Watermark::new(2000)));
}

#[test]
fn test_punctuated_generator_no_regression() {
    let mut gen = PunctuatedGenerator::new(|ts| Some(Watermark::new(ts)));

    gen.on_event(2000);
    let wm = gen.on_event(1000); // Lower watermark

    assert_eq!(wm, None);
    assert_eq!(gen.current_watermark(), 2000);
}

#[test]
fn punctuated_recovery_restore_lowers_then_advances() {
    let mut gen = PunctuatedGenerator::new(|ts| Some(Watermark::new(ts)));
    gen.on_event(2_000);

    gen.restore_watermark_for_recovery(500);
    assert_eq!(gen.current_watermark(), 500);
    assert_eq!(gen.on_event(600), Some(Watermark::new(600)));
}

#[test]
fn test_tracker_single_source() {
    let mut tracker = WatermarkTracker::new(1);

    let wm = tracker.update_source(0, 1000);
    assert_eq!(wm, Some(Watermark::new(1000)));
    assert_eq!(tracker.current_watermark(), Some(Watermark::new(1000)));
}

#[test]
fn test_tracker_multiple_sources() {
    let mut tracker = WatermarkTracker::new(3);

    // All sources need to report before watermark advances
    tracker.update_source(0, 1000);
    tracker.update_source(1, 2000);
    let wm = tracker.update_source(2, 500);

    assert_eq!(wm, Some(Watermark::new(500))); // Minimum
}

#[test]
fn test_tracker_min_watermark() {
    let mut tracker = WatermarkTracker::new(2);

    tracker.update_source(0, 5000);
    tracker.update_source(1, 3000);

    assert_eq!(tracker.current_watermark(), Some(Watermark::new(3000)));

    // Source 1 advances
    tracker.update_source(1, 4000);
    assert_eq!(tracker.current_watermark(), Some(Watermark::new(4000)));
}

#[test]
fn test_tracker_idle_source() {
    let mut tracker = WatermarkTracker::new(2);

    tracker.update_source(0, 5000);
    tracker.update_source(1, 1000);

    // Source 1 is slow, mark it idle
    let wm = tracker.mark_idle(1);

    // Now only source 0's watermark counts
    assert_eq!(wm, Some(Watermark::new(5000)));
}

#[test]
fn check_idle_sources_advances_then_reactivation_is_monotone() {
    // A quiet watermarked source must not pin the combined-min, and a
    // later-reactivating source with an OLD watermark must not regress
    // it. `idle_timeout = 0` makes any source immediately eligible for
    // `check_idle_sources` without sleeping.
    let mut tracker = WatermarkTracker::with_idle_timeout(2, Duration::ZERO);
    tracker.update_source(0, 5_000); // active, fast
    tracker.update_source(1, 1_000); // will go quiet
    assert_eq!(tracker.current_watermark(), Some(Watermark::new(1_000)));

    // Source 1 idle past timeout → excluded → combined jumps to s0.
    let advanced = tracker.check_idle_sources();
    assert_eq!(advanced, Some(Watermark::new(5_000)));
    assert_eq!(tracker.current_watermark(), Some(Watermark::new(5_000)));

    // Source 1 reactivates with a STALE watermark: must not regress.
    let res = tracker.update_source(1, 1_500);
    assert_eq!(res, None, "stale reactivation must not emit a regress");
    assert_eq!(tracker.current_watermark(), Some(Watermark::new(5_000)));

    // Once it catches up past the combined, progress resumes from min.
    tracker.update_source(0, 9_000);
    tracker.update_source(1, 8_000);
    assert_eq!(tracker.current_watermark(), Some(Watermark::new(8_000)));
}

#[test]
fn test_tracker_all_idle() {
    let mut tracker = WatermarkTracker::new(2);

    tracker.update_source(0, 5000);
    tracker.update_source(1, 3000);

    tracker.mark_idle(0);
    let wm = tracker.mark_idle(1);

    // Use max when all idle
    assert_eq!(wm, Some(Watermark::new(5000)));
}

#[test]
fn test_tracker_source_watermark() {
    let mut tracker = WatermarkTracker::new(2);

    tracker.update_source(0, 1000);
    tracker.update_source(1, 2000);

    assert_eq!(tracker.source_watermark(0), Some(1000));
    assert_eq!(tracker.source_watermark(1), Some(2000));
    assert_eq!(tracker.source_watermark(5), None); // Out of bounds
}

#[test]
fn test_tracker_active_source_count() {
    let mut tracker = WatermarkTracker::new(3);

    assert_eq!(tracker.active_source_count(), 3);

    tracker.mark_idle(0);
    assert_eq!(tracker.active_source_count(), 2);

    tracker.mark_idle(2);
    assert_eq!(tracker.active_source_count(), 1);

    // Reactivate by updating
    tracker.update_source(0, 1000);
    assert_eq!(tracker.active_source_count(), 2);
}

#[test]
fn test_tracker_invalid_source() {
    let mut tracker = WatermarkTracker::new(2);

    let wm = tracker.update_source(5, 1000); // Invalid source ID
    assert_eq!(wm, None);

    let wm = tracker.mark_idle(5);
    assert_eq!(wm, None);
}

#[test]
fn tracker_recovery_restore_is_exact_and_runtime_progress_resumes() {
    let mut tracker = WatermarkTracker::new(2);
    tracker.update_source(0, 9_000);
    tracker.update_source(1, 8_000);
    assert_eq!(tracker.current_watermark(), Some(Watermark::new(8_000)));

    tracker
        .restore_for_recovery(&[Some(1_000), None], &[false, true], Some(750))
        .unwrap();

    assert_eq!(tracker.source_watermark(0), Some(1_000));
    assert_eq!(tracker.source_watermark(1), Some(i64::MIN));
    assert!(!tracker.is_idle(0));
    assert!(tracker.is_idle(1));
    assert_eq!(tracker.current_watermark(), Some(Watermark::new(750)));

    assert_eq!(tracker.update_source(0, 1_200), Some(Watermark::new(1_200)));
    assert_eq!(tracker.current_watermark(), Some(Watermark::new(1_200)));
}

#[test]
fn tracker_recovery_restore_rejects_topology_mismatch_without_mutation() {
    let mut tracker = WatermarkTracker::new(2);
    tracker.update_source(0, 2_000);
    tracker.update_source(1, 1_000);

    let error = tracker
        .restore_for_recovery(&[Some(500)], &[false, true], Some(500))
        .unwrap_err();

    assert_eq!(error.expected, 2);
    assert_eq!(error.watermarks, 1);
    assert_eq!(error.idle_statuses, 2);
    assert_eq!(tracker.source_watermark(0), Some(2_000));
    assert_eq!(tracker.source_watermark(1), Some(1_000));
    assert_eq!(tracker.current_watermark(), Some(Watermark::new(1_000)));
}

#[test]
fn test_source_provided_fallback() {
    let mut gen = SourceProvidedGenerator::new(100, false);

    let wm = gen.on_event(1000);
    assert_eq!(wm, Some(Watermark::new(900))); // Fallback behavior
}

#[test]
fn test_source_provided_explicit_watermark() {
    let mut gen = SourceProvidedGenerator::new(100, true);

    let wm = gen.on_source_watermark(500);
    assert_eq!(wm, Some(Watermark::new(500)));
    assert_eq!(gen.current_watermark(), 500);
}

#[test]
fn source_provided_recovery_restore_resets_source_and_fallback() {
    let mut gen = SourceProvidedGenerator::new(100, false);
    gen.on_source_watermark(2_000);
    gen.on_event(2_000);

    gen.restore_watermark_for_recovery(500);
    assert_eq!(gen.source_watermark, 500);
    assert_eq!(gen.fallback.current_watermark(), 500);
    assert_eq!(gen.on_event(700), Some(Watermark::new(600)));
    assert_eq!(gen.current_watermark(), 600);
}

// --- advance_watermark() tests ---

#[test]
fn test_advance_watermark_bounded_generator() {
    let mut gen = BoundedOutOfOrdernessGenerator::new(100);

    // Advance from initial state
    let wm = gen.advance_watermark(500);
    assert_eq!(wm, Some(Watermark::new(500)));
    assert_eq!(gen.current_watermark(), 500);

    // Advance further
    let wm = gen.advance_watermark(800);
    assert_eq!(wm, Some(Watermark::new(800)));
    assert_eq!(gen.current_watermark(), 800);

    // No regression
    let wm = gen.advance_watermark(600);
    assert_eq!(wm, None);
    assert_eq!(gen.current_watermark(), 800);
}

#[test]
fn test_advance_watermark_maintains_invariant() {
    let mut gen = BoundedOutOfOrdernessGenerator::new(100);

    // Process an event to set initial state
    gen.on_event(1000); // wm=900, max_ts=1000

    // Advance watermark beyond current
    gen.advance_watermark(1200);
    assert_eq!(gen.current_watermark(), 1200);

    // Now on_event at 1250 should work correctly: max_ts should be >= 1300
    // wm = 1250 - 100 = 1150 which is < 1200, so no new watermark from on_event
    let wm = gen.on_event(1250);
    assert_eq!(wm, None);
    assert_eq!(gen.current_watermark(), 1200);

    // But event at 1400: max_ts = 1400, wm = 1300 > 1200
    let wm = gen.on_event(1400);
    assert_eq!(wm, Some(Watermark::new(1300)));
}

#[test]
fn test_advance_watermark_ascending_generator() {
    let mut gen = AscendingTimestampsGenerator::new();

    let wm = gen.advance_watermark(500);
    assert_eq!(wm, Some(Watermark::new(500)));
    assert_eq!(gen.current_watermark(), 500);

    // No regression
    let wm = gen.advance_watermark(300);
    assert_eq!(wm, None);
    assert_eq!(gen.current_watermark(), 500);

    // Further advance
    let wm = gen.advance_watermark(1000);
    assert_eq!(wm, Some(Watermark::new(1000)));
}

#[test]
fn test_advance_watermark_periodic_generator() {
    let inner = BoundedOutOfOrdernessGenerator::new(100);
    let mut gen = PeriodicGenerator::new(inner, Duration::from_millis(100));

    let wm = gen.advance_watermark(500);
    assert_eq!(wm, Some(Watermark::new(500)));
    assert_eq!(gen.current_watermark(), 500);

    // No regression
    let wm = gen.advance_watermark(300);
    assert_eq!(wm, None);
}

#[test]
fn test_advance_watermark_punctuated_generator() {
    let mut gen = PunctuatedGenerator::new(|ts| {
        if ts % 1000 == 0 {
            Some(Watermark::new(ts))
        } else {
            None
        }
    });

    // External advance (does not invoke predicate)
    let wm = gen.advance_watermark(500);
    assert_eq!(wm, Some(Watermark::new(500)));
    assert_eq!(gen.current_watermark(), 500);

    // No regression
    let wm = gen.advance_watermark(200);
    assert_eq!(wm, None);
}

#[test]
fn test_advance_watermark_source_provided_generator() {
    let mut gen = SourceProvidedGenerator::new(100, true);

    let wm = gen.advance_watermark(500);
    assert_eq!(wm, Some(Watermark::new(500)));
    assert_eq!(gen.current_watermark(), 500);

    // No regression
    let wm = gen.advance_watermark(300);
    assert_eq!(wm, None);
}

// --- ProcessingTimeGenerator tests ---

#[test]
fn test_processing_time_generator_ignores_events() {
    let mut gen = ProcessingTimeGenerator::new();
    assert_eq!(gen.on_event(1000), None);
    assert_eq!(gen.on_event(2000), None);
    assert_eq!(gen.current_watermark(), i64::MIN);
}

#[test]
fn test_processing_time_generator_periodic() {
    let mut gen = ProcessingTimeGenerator::new();
    let wm = gen.on_periodic();
    assert!(wm.is_some());
    let ts = wm.unwrap().timestamp();
    // Should be a reasonable timestamp (after 2020-01-01)
    assert!(ts > 1_577_836_800_000, "timestamp too old: {ts}");
}

#[test]
fn test_processing_time_generator_advance_watermark() {
    let mut gen = ProcessingTimeGenerator::new();

    let wm = gen.advance_watermark(500);
    assert_eq!(wm, Some(Watermark::new(500)));
    assert_eq!(gen.current_watermark(), 500);

    // No regression
    let wm = gen.advance_watermark(300);
    assert_eq!(wm, None);
    assert_eq!(gen.current_watermark(), 500);

    // Further advance
    let wm = gen.advance_watermark(1000);
    assert_eq!(wm, Some(Watermark::new(1000)));
}

#[test]
fn processing_time_recovery_restore_lowers_then_advances() {
    let mut gen = ProcessingTimeGenerator::new();
    gen.advance_watermark(2_000);

    gen.restore_watermark_for_recovery(500);
    assert_eq!(gen.current_watermark(), 500);
    assert_eq!(gen.advance_watermark(600), Some(Watermark::new(600)));
}

#[test]
fn test_processing_time_generator_default() {
    let gen = ProcessingTimeGenerator::default();
    assert_eq!(gen.current_watermark(), i64::MIN);
}

// --- future-skew guard ---

#[test]
fn future_skew_event_does_not_advance_watermark() {
    let mut gen = BoundedOutOfOrdernessGenerator::new(0);
    let now = crate::time::now_unix_millis();
    // A ~2h-future event must not poison the watermark...
    assert_eq!(gen.on_event(now + 2 * 60 * 60 * 1000), None);
    assert_eq!(gen.current_watermark(), i64::MIN);
    // ...but a normal event still advances it.
    assert_eq!(gen.on_event(now), Some(Watermark::new(now)));
}
