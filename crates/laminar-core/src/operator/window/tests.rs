use super::*;

#[test]
fn test_window_id_basics() {
    let id = WindowId::new(1000, 2000);
    assert_eq!(id.start, 1000);
    assert_eq!(id.end, 2000);
    assert_eq!(id.duration_ms(), 1000);
}

#[test]
fn test_window_id_key_roundtrip() {
    let id = WindowId::new(1000, 2000);
    assert_eq!(WindowId::from_key(&id.to_key_inline()), Some(id));
    assert_eq!(WindowId::from_key(&[0u8; 8]), None);
}

#[test]
fn test_tumbling_assigner_positive() {
    let a = TumblingWindowAssigner::from_millis(1000);
    assert_eq!(a.assign(500), WindowId::new(0, 1000));
    assert_eq!(a.assign(1500), WindowId::new(1000, 2000));
    assert_eq!(a.assign(0), WindowId::new(0, 1000));
    assert_eq!(a.assign(999), WindowId::new(0, 1000));
    assert_eq!(a.assign(1000), WindowId::new(1000, 2000));
}

#[test]
fn test_tumbling_assigner_negative() {
    let a = TumblingWindowAssigner::from_millis(1000);
    assert_eq!(a.assign(-1), WindowId::new(-1000, 0));
    assert_eq!(a.assign(-500), WindowId::new(-1000, 0));
    assert_eq!(a.assign(-1000), WindowId::new(-1000, 0));
    assert_eq!(a.assign(-1001), WindowId::new(-2000, -1000));
}

#[test]
fn test_tumbling_assigner_offset() {
    let a = TumblingWindowAssigner::from_millis(1000).with_offset_ms(500);
    assert_eq!(a.offset_ms(), 500);
    assert_eq!(a.assign(600), WindowId::new(500, 1500));
    assert_eq!(a.assign(400), WindowId::new(-500, 500));
}

#[test]
fn test_window_assigner_trait() {
    let a = TumblingWindowAssigner::from_millis(1000);
    let windows = a.assign_windows(500);
    assert_eq!(windows.len(), 1);
    assert_eq!(windows[0], WindowId::new(0, 1000));
}

#[test]
fn test_emit_strategy_variants() {
    assert_eq!(EmitStrategy::default(), EmitStrategy::OnWatermark);
    assert!(EmitStrategy::Periodic(Duration::from_secs(5)).needs_periodic_timer());
    assert!(EmitStrategy::OnUpdate.emits_on_update());
    assert!(EmitStrategy::OnWindowClose.suppresses_intermediate());
    assert!(EmitStrategy::Final.drops_late_data());
    assert!(EmitStrategy::Changelog.requires_changelog());
    assert!(EmitStrategy::OnWindowClose.is_append_only_compatible());
}

#[test]
fn test_cdc_operation_weights() {
    assert_eq!(CdcOperation::Insert.weight(), 1);
    assert_eq!(CdcOperation::Delete.weight(), -1);
    assert_eq!(CdcOperation::UpdateBefore.weight(), -1);
    assert_eq!(CdcOperation::UpdateAfter.weight(), 1);
}

#[test]
fn test_cdc_operation_u8_roundtrip() {
    for op in [
        CdcOperation::Insert,
        CdcOperation::Delete,
        CdcOperation::UpdateBefore,
        CdcOperation::UpdateAfter,
    ] {
        assert_eq!(CdcOperation::from_u8(op.to_u8()), op);
    }
    assert_eq!(CdcOperation::from_u8(255), CdcOperation::Insert);
}
