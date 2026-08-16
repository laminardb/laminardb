use super::*;
use laminar_core::time::{BoundedOutOfOrdernessGenerator, EventTimeExtractor, ExtractionMode};

fn channel_progress(
    entries: &[(&[u8], Option<i64>, bool)],
) -> FxHashMap<Box<[u8]>, RecoveredInputChannelProgress> {
    entries
        .iter()
        .map(|(channel, watermark, idle)| {
            (
                Box::<[u8]>::from(*channel),
                RecoveredInputChannelProgress {
                    watermark: *watermark,
                    idle: *idle,
                },
            )
        })
        .collect()
}

#[test]
fn empty_owned_inventory_restores_its_exact_committed_source_cut() {
    let progress = FxHashMap::default();
    assert_eq!(
        recovered_source_watermark(Some(&progress), true, Some(900)),
        (Some(900), true)
    );
}

#[test]
fn all_idle_inventory_prefers_the_exact_committed_source_cut() {
    let progress = channel_progress(&[(b"left", Some(700), true), (b"right", Some(800), true)]);
    assert_eq!(
        recovered_source_watermark(Some(&progress), false, Some(900)),
        (Some(900), true)
    );
}

#[test]
fn logical_empty_inventory_marker_is_not_restored_as_a_physical_channel() {
    let progress = channel_progress(&[(SINGLETON_WATERMARK_CHANNEL, Some(900), true)]);
    let physical = physical_recovered_input_channel_progress(Some(&progress));
    let inventory: Arc<[Vec<u8>]> = Arc::from(Vec::<Vec<u8>>::new());

    assert!(physical.is_empty());
    validate_recovered_input_channels("orders", &physical, Some(&inventory)).unwrap();
    assert_eq!(
        recovered_source_watermark(Some(&progress), true, Some(900)),
        (Some(900), true),
        "the unstripped logical marker must still recover the source decision"
    );
}

#[test]
fn active_uninitialized_inventory_ignores_a_retained_committed_cut() {
    let progress = channel_progress(&[
        (b"initialized", Some(700), false),
        (b"uninitialized", None, false),
    ]);
    assert_eq!(
        recovered_source_watermark(Some(&progress), false, Some(900)),
        (None, false)
    );
}

#[test]
fn legacy_idle_inventory_without_a_reconstructable_cut_fails_closed() {
    let legacy_version =
        laminar_core::checkpoint::COMMITTED_CHECKPOINT_INDEX_VERSION.saturating_sub(1);
    let error =
        validate_recovered_source_watermark("orders", None, true, None, Some(legacy_version))
            .unwrap_err();
    assert!(matches!(
        error,
        DbError::Checkpoint(message)
            if message.contains("legacy committed checkpoint index")
                && message.contains("orders")
    ));
}

#[test]
fn current_index_and_active_uninitialized_legacy_source_remain_uninitialized() {
    assert!(validate_recovered_source_watermark(
        "orders",
        None,
        true,
        None,
        Some(laminar_core::checkpoint::COMMITTED_CHECKPOINT_INDEX_VERSION),
    )
    .is_ok());
    assert!(validate_recovered_source_watermark(
        "orders",
        None,
        false,
        None,
        Some(laminar_core::checkpoint::COMMITTED_CHECKPOINT_INDEX_VERSION - 1),
    )
    .is_ok());
}

#[test]
fn committed_idle_cut_becomes_the_recovered_partition_floor() {
    let inventory: Arc<[Vec<u8>]> = Arc::from([b"partition".to_vec()]);
    let progress = channel_progress(&[(b"partition", Some(700), true)]);
    let mut state = SourceWatermarkState::new(
        EventTimeExtractor::from_column("ts").with_mode(ExtractionMode::Max),
        Box::new(BoundedOutOfOrdernessGenerator::new(0).with_max_future_skew(0)),
        "ts".into(),
    )
    .with_input_channels(
        std::time::Duration::ZERO,
        0,
        None,
        progress.clone(),
        Some(Arc::clone(&inventory)),
    );
    let (recovered, idle) = recovered_source_watermark(Some(&progress), false, Some(900));
    restore_source_watermark_state(&mut state, recovered, idle, Some(900));
    state
        .install_input_channels(Some(inventory), i64::MIN)
        .unwrap();

    assert_eq!(state.generator.current_watermark(), 900);
    let restored = state.input_channel_progress().unwrap().unwrap();
    assert_eq!(restored.len(), 1);
    assert_eq!(restored[0].watermark, Some(900));
    assert!(restored[0].idle);
}

#[test]
fn committed_empty_inventory_cut_floors_its_first_recovered_channel() {
    let empty_inventory: Arc<[Vec<u8>]> = Arc::from(Vec::<Vec<u8>>::new());
    let progress = FxHashMap::default();
    let mut state = SourceWatermarkState::new(
        EventTimeExtractor::from_column("ts").with_mode(ExtractionMode::Max),
        Box::new(BoundedOutOfOrdernessGenerator::new(0).with_max_future_skew(0)),
        "ts".into(),
    )
    .with_input_channels(
        std::time::Duration::ZERO,
        0,
        None,
        progress.clone(),
        Some(Arc::clone(&empty_inventory)),
    );
    let (recovered, idle) = recovered_source_watermark(Some(&progress), true, Some(900));
    restore_source_watermark_state(&mut state, recovered, idle, Some(900));
    state
        .install_input_channels(Some(empty_inventory), i64::MIN)
        .unwrap();

    assert_eq!(state.generator.current_watermark(), 900);
    assert_eq!(state.input_channels_all_idle(), Some(true));
    assert!(state.input_channel_progress().unwrap().unwrap().is_empty());

    state
        .install_input_channels(Some(Arc::from([b"new-partition".to_vec()])), i64::MIN)
        .unwrap();
    let restored = state.input_channel_progress().unwrap().unwrap();
    assert_eq!(restored.len(), 1);
    assert_eq!(restored[0].watermark, Some(900));
    assert!(!restored[0].idle);
}
