use super::*;
use crate::state::InMemoryStore;
use crate::time::{BoundedOutOfOrdernessGenerator, TimerService};
use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use std::sync::Arc;

fn create_test_event(timestamp: i64, value: i64) -> Event {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![value]))]).unwrap();
    Event::new(timestamp, batch)
}

fn create_test_context<'a>(
    timers: &'a mut TimerService,
    state: &'a mut dyn StateStore,
    watermark_gen: &'a mut dyn crate::time::WatermarkGenerator,
) -> OperatorContext<'a> {
    OperatorContext {
        event_time: 0,
        processing_time: 0,
        timers,
        state,
        watermark_generator: watermark_gen,
        operator_index: 0,
    }
}

#[test]
fn test_window_id_creation() {
    let window = WindowId::new(1000, 2000);
    assert_eq!(window.start, 1000);
    assert_eq!(window.end, 2000);
    assert_eq!(window.duration_ms(), 1000);
}

#[test]
fn test_window_id_serialization() {
    let window = WindowId::new(1000, 2000);
    let key = window.to_key();
    assert_eq!(key.len(), 16);

    let restored = WindowId::from_key(&key).unwrap();
    assert_eq!(restored, window);
}

#[test]
fn test_tumbling_assigner_positive_timestamps() {
    let assigner = TumblingWindowAssigner::from_millis(1000);

    // Events at different times within same window
    assert_eq!(assigner.assign(0), WindowId::new(0, 1000));
    assert_eq!(assigner.assign(500), WindowId::new(0, 1000));
    assert_eq!(assigner.assign(999), WindowId::new(0, 1000));

    // Event at window boundary goes to next window
    assert_eq!(assigner.assign(1000), WindowId::new(1000, 2000));
    assert_eq!(assigner.assign(1500), WindowId::new(1000, 2000));
}

#[test]
fn test_tumbling_assigner_negative_timestamps() {
    let assigner = TumblingWindowAssigner::from_millis(1000);

    // Negative timestamps
    assert_eq!(assigner.assign(-1), WindowId::new(-1000, 0));
    assert_eq!(assigner.assign(-500), WindowId::new(-1000, 0));
    assert_eq!(assigner.assign(-1000), WindowId::new(-1000, 0));
    assert_eq!(assigner.assign(-1001), WindowId::new(-2000, -1000));
}

#[test]
fn test_count_aggregator() {
    let mut acc = CountAccumulator::default();
    assert!(acc.is_empty());
    assert_eq!(acc.result(), 0);

    acc.add(());
    acc.add(());
    acc.add(());

    assert!(!acc.is_empty());
    assert_eq!(acc.result(), 3);
}

#[test]
fn test_sum_accumulator() {
    let mut acc = SumAccumulator::default();
    acc.add(10);
    acc.add(20);
    acc.add(30);

    assert_eq!(acc.result(), 60);
}

#[test]
fn test_min_accumulator() {
    let mut acc = MinAccumulator::default();
    assert!(acc.is_empty());
    assert_eq!(acc.result(), None);

    acc.add(50);
    acc.add(10);
    acc.add(30);

    assert_eq!(acc.result(), Some(10));
}

#[test]
fn test_max_accumulator() {
    let mut acc = MaxAccumulator::default();
    acc.add(10);
    acc.add(50);
    acc.add(30);

    assert_eq!(acc.result(), Some(50));
}

#[test]
fn test_avg_accumulator() {
    let mut acc = AvgAccumulator::default();
    acc.add(10);
    acc.add(20);
    acc.add(30);

    let result = acc.result().unwrap();
    assert!((result - 20.0).abs() < f64::EPSILON);
}

#[test]
fn test_accumulator_merge() {
    let mut acc1 = SumAccumulator::default();
    acc1.add(10);
    acc1.add(20);

    let mut acc2 = SumAccumulator::default();
    acc2.add(30);
    acc2.add(40);

    acc1.merge(&acc2);
    assert_eq!(acc1.result(), 100);
}

#[test]
fn test_tumbling_window_operator_basic() {
    let assigner = TumblingWindowAssigner::from_millis(1000);
    let aggregator = CountAggregator::new();
    let mut operator = TumblingWindowOperator::with_id(
        assigner,
        aggregator,
        Duration::from_millis(0),
        "test_op".to_string(),
    );

    let mut timers = TimerService::new();
    let mut state = InMemoryStore::new();
    let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

    // Process events in the same window
    let event1 = create_test_event(100, 1);
    let event2 = create_test_event(500, 2);
    let event3 = create_test_event(900, 3);

    {
        let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
        operator.process(&event1, &mut ctx);
    }
    {
        let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
        operator.process(&event2, &mut ctx);
    }
    {
        let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
        operator.process(&event3, &mut ctx);
    }

    // Check that a timer was registered
    assert_eq!(operator.registered_windows.len(), 1);
    assert!(operator
        .registered_windows
        .contains(&WindowId::new(0, 1000)));
}

#[test]
fn test_tumbling_window_operator_trigger() {
    let assigner = TumblingWindowAssigner::from_millis(1000);
    let aggregator = CountAggregator::new();
    let mut operator = TumblingWindowOperator::with_id(
        assigner,
        aggregator,
        Duration::from_millis(0),
        "test_op".to_string(),
    );

    let mut timers = TimerService::new();
    let mut state = InMemoryStore::new();
    let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

    // Process events
    for ts in [100, 500, 900] {
        let event = create_test_event(ts, 1);
        let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
        operator.process(&event, &mut ctx);
    }

    // Trigger the window via timer
    let timer = Timer {
        key: WindowId::new(0, 1000).to_key(),
        timestamp: 1000,
    };

    let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
    let outputs = operator.on_timer(timer, &mut ctx);

    assert_eq!(outputs.len(), 1);
    match &outputs[0] {
        Output::Event(event) => {
            assert_eq!(event.timestamp, 1000); // window end
                                               // Check the result column (count = 3)
            let result_col = event.data.column(2);
            let result_array = result_col.as_any().downcast_ref::<Int64Array>().unwrap();
            assert_eq!(result_array.value(0), 3);
        }
        _ => panic!("Expected Event output"),
    }

    // Window should be cleaned up
    assert!(operator.registered_windows.is_empty());
}

#[test]
fn test_tumbling_window_multiple_windows() {
    let assigner = TumblingWindowAssigner::from_millis(1000);
    let aggregator = CountAggregator::new();
    let mut operator = TumblingWindowOperator::with_id(
        assigner,
        aggregator,
        Duration::from_millis(0),
        "test_op".to_string(),
    );

    let mut timers = TimerService::new();
    let mut state = InMemoryStore::new();
    let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

    // Events in different windows
    let events = [
        create_test_event(100, 1),  // Window [0, 1000)
        create_test_event(500, 2),  // Window [0, 1000)
        create_test_event(1100, 3), // Window [1000, 2000)
        create_test_event(1500, 4), // Window [1000, 2000)
        create_test_event(2500, 5), // Window [2000, 3000)
    ];

    for event in &events {
        let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
        operator.process(event, &mut ctx);
    }

    // Should have 3 windows registered
    assert_eq!(operator.registered_windows.len(), 3);
}

#[test]
fn test_tumbling_window_checkpoint_restore() {
    let assigner = TumblingWindowAssigner::from_millis(1000);
    let aggregator = CountAggregator::new();
    let mut operator = TumblingWindowOperator::with_id(
        assigner.clone(),
        aggregator.clone(),
        Duration::from_millis(0),
        "test_op".to_string(),
    );

    // Register some windows
    operator.registered_windows.insert(WindowId::new(0, 1000));
    operator
        .registered_windows
        .insert(WindowId::new(1000, 2000));

    // Checkpoint
    let checkpoint = operator.checkpoint();

    // Create a new operator and restore
    let mut restored_operator = TumblingWindowOperator::with_id(
        assigner,
        aggregator,
        Duration::from_millis(0),
        "test_op".to_string(),
    );
    restored_operator.restore(checkpoint).unwrap();

    assert_eq!(restored_operator.registered_windows.len(), 2);
    assert!(restored_operator
        .registered_windows
        .contains(&WindowId::new(0, 1000)));
    assert!(restored_operator
        .registered_windows
        .contains(&WindowId::new(1000, 2000)));
}

#[test]
fn test_sum_aggregator_extraction() {
    let aggregator = SumAggregator::new(0);
    let event = create_test_event(100, 42);

    let extracted = aggregator.extract(&event);
    assert_eq!(extracted, Some(42));
}

#[test]
fn test_empty_window_trigger() {
    let assigner = TumblingWindowAssigner::from_millis(1000);
    let aggregator = CountAggregator::new();
    let mut operator = TumblingWindowOperator::with_id(
        assigner,
        aggregator,
        Duration::from_millis(0),
        "test_op".to_string(),
    );

    let mut timers = TimerService::new();
    let mut state = InMemoryStore::new();
    let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

    // Trigger without any events
    let timer = Timer {
        key: WindowId::new(0, 1000).to_key(),
        timestamp: 1000,
    };

    let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
    let outputs = operator.on_timer(timer, &mut ctx);

    // Empty window should produce no output
    assert!(outputs.is_empty());
}

#[test]
fn test_window_assigner_trait() {
    let assigner = TumblingWindowAssigner::from_millis(1000);
    let windows = assigner.assign_windows(500);

    assert_eq!(windows.len(), 1);
    assert_eq!(windows[0], WindowId::new(0, 1000));
}

#[test]
fn test_emit_strategy_default() {
    let strategy = EmitStrategy::default();
    assert_eq!(strategy, EmitStrategy::OnWatermark);
}

#[test]
fn test_emit_strategy_on_watermark() {
    let strategy = EmitStrategy::OnWatermark;
    assert!(!strategy.needs_periodic_timer());
    assert!(strategy.periodic_interval().is_none());
    assert!(!strategy.emits_on_update());
}

#[test]
fn test_emit_strategy_periodic() {
    let interval = Duration::from_secs(10);
    let strategy = EmitStrategy::Periodic(interval);
    assert!(strategy.needs_periodic_timer());
    assert_eq!(strategy.periodic_interval(), Some(interval));
    assert!(!strategy.emits_on_update());
}

#[test]
fn test_emit_strategy_on_update() {
    let strategy = EmitStrategy::OnUpdate;
    assert!(!strategy.needs_periodic_timer());
    assert!(strategy.periodic_interval().is_none());
    assert!(strategy.emits_on_update());
}

#[test]
fn test_window_operator_set_emit_strategy() {
    let assigner = TumblingWindowAssigner::from_millis(1000);
    let aggregator = CountAggregator::new();
    let mut operator = TumblingWindowOperator::with_id(
        assigner,
        aggregator,
        Duration::from_millis(0),
        "test_op".to_string(),
    );

    // Default is OnWatermark
    assert_eq!(*operator.emit_strategy(), EmitStrategy::OnWatermark);

    // Set to Periodic
    operator.set_emit_strategy(EmitStrategy::Periodic(Duration::from_secs(5)));
    assert_eq!(
        *operator.emit_strategy(),
        EmitStrategy::Periodic(Duration::from_secs(5))
    );

    // Set to OnUpdate
    operator.set_emit_strategy(EmitStrategy::OnUpdate);
    assert_eq!(*operator.emit_strategy(), EmitStrategy::OnUpdate);
}

#[test]
fn test_emit_on_update_emits_intermediate_results() {
    let assigner = TumblingWindowAssigner::from_millis(1000);
    let aggregator = CountAggregator::new();
    let mut operator = TumblingWindowOperator::with_id(
        assigner,
        aggregator,
        Duration::from_millis(0),
        "test_op".to_string(),
    );

    // Set emit strategy to OnUpdate
    operator.set_emit_strategy(EmitStrategy::OnUpdate);

    let mut timers = TimerService::new();
    let mut state = InMemoryStore::new();
    let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

    // Process first event - should emit intermediate result
    let event1 = create_test_event(100, 1);
    let outputs1 = {
        let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
        operator.process(&event1, &mut ctx)
    };

    // Should have at least one event output (intermediate result)
    let has_event = outputs1.iter().any(|o| matches!(o, Output::Event(_)));
    assert!(
        has_event,
        "OnUpdate should emit intermediate result after first event"
    );

    // Process second event - should emit another intermediate result
    let event2 = create_test_event(500, 2);
    let outputs2 = {
        let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
        operator.process(&event2, &mut ctx)
    };

    // Should have intermediate result with count = 2
    let event_output = outputs2.iter().find_map(|o| {
        if let Output::Event(e) = o {
            Some(e)
        } else {
            None
        }
    });

    assert!(
        event_output.is_some(),
        "OnUpdate should emit after second event"
    );
    if let Some(event) = event_output {
        let result_col = event.data.column(2);
        let result_array = result_col.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(result_array.value(0), 2, "Intermediate count should be 2");
    }
}

#[test]
fn test_emit_on_watermark_no_intermediate_results() {
    let assigner = TumblingWindowAssigner::from_millis(1000);
    let aggregator = CountAggregator::new();
    let mut operator = TumblingWindowOperator::with_id(
        assigner,
        aggregator,
        Duration::from_millis(0),
        "test_op".to_string(),
    );

    // Default is OnWatermark - no intermediate emissions
    let mut timers = TimerService::new();
    let mut state = InMemoryStore::new();
    let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

    // Process events
    let event1 = create_test_event(100, 1);
    let outputs1 = {
        let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
        operator.process(&event1, &mut ctx)
    };

    // Should NOT have event output (only watermark update if any)
    let has_intermediate_event = outputs1.iter().any(|o| matches!(o, Output::Event(_)));
    assert!(
        !has_intermediate_event,
        "OnWatermark should not emit intermediate results"
    );
}

#[test]
fn test_checkpoint_restore_with_emit_strategy() {
    let assigner = TumblingWindowAssigner::from_millis(1000);
    let aggregator = CountAggregator::new();
    let mut operator = TumblingWindowOperator::with_id(
        assigner.clone(),
        aggregator.clone(),
        Duration::from_millis(0),
        "test_op".to_string(),
    );

    // Set emit strategy and register some windows
    operator.set_emit_strategy(EmitStrategy::Periodic(Duration::from_secs(10)));
    operator.registered_windows.insert(WindowId::new(0, 1000));
    operator
        .periodic_timer_windows
        .insert(WindowId::new(0, 1000));

    // Checkpoint
    let checkpoint = operator.checkpoint();

    // Create a new operator and restore
    let mut restored_operator = TumblingWindowOperator::with_id(
        assigner,
        aggregator,
        Duration::from_millis(0),
        "test_op".to_string(),
    );
    restored_operator.restore(checkpoint).unwrap();

    // Both registered_windows and periodic_timer_windows should be restored
    assert_eq!(restored_operator.registered_windows.len(), 1);
    assert_eq!(restored_operator.periodic_timer_windows.len(), 1);
    assert!(restored_operator
        .registered_windows
        .contains(&WindowId::new(0, 1000)));
    assert!(restored_operator
        .periodic_timer_windows
        .contains(&WindowId::new(0, 1000)));
}

#[test]
fn test_periodic_timer_key_format() {
    // Verify the periodic timer key format
    let window_id = WindowId::new(1000, 2000);

    // Create periodic key using the helper
    let periodic_key = TumblingWindowOperator::<CountAggregator>::periodic_timer_key(&window_id);

    // Periodic key should be 16 bytes (same as window key, but with high bit set)
    assert_eq!(periodic_key.len(), 16);

    // First byte should have high bit set
    assert!(TumblingWindowOperator::<CountAggregator>::is_periodic_timer_key(&periodic_key));

    // Extract window ID from periodic key
    let extracted =
        TumblingWindowOperator::<CountAggregator>::window_id_from_periodic_key(&periodic_key);
    assert_eq!(extracted, Some(window_id));

    // Regular window key should not be detected as periodic
    let regular_key = window_id.to_key();
    assert!(!TumblingWindowOperator::<CountAggregator>::is_periodic_timer_key(&regular_key));
}

#[test]
fn test_late_data_config_default() {
    let config = LateDataConfig::default();
    assert!(config.should_drop());
    assert!(config.side_output().is_none());
}

#[test]
fn test_late_data_config_drop() {
    let config = LateDataConfig::drop();
    assert!(config.should_drop());
    assert!(config.side_output().is_none());
}

#[test]
fn test_late_data_config_with_side_output() {
    let config = LateDataConfig::with_side_output("late_events".to_string());
    assert!(!config.should_drop());
    assert_eq!(config.side_output(), Some("late_events"));
}

#[test]
fn test_late_data_metrics_initial() {
    let metrics = LateDataMetrics::new();
    assert_eq!(metrics.late_events_total(), 0);
    assert_eq!(metrics.late_events_dropped(), 0);
    assert_eq!(metrics.late_events_side_output(), 0);
}

#[test]
fn test_late_data_metrics_tracking() {
    let mut metrics = LateDataMetrics::new();

    metrics.record_dropped();
    metrics.record_dropped();
    metrics.record_side_output();

    assert_eq!(metrics.late_events_total(), 3);
    assert_eq!(metrics.late_events_dropped(), 2);
    assert_eq!(metrics.late_events_side_output(), 1);
}

#[test]
fn test_late_data_metrics_reset() {
    let mut metrics = LateDataMetrics::new();

    metrics.record_dropped();
    metrics.record_side_output();

    assert_eq!(metrics.late_events_total(), 2);

    metrics.reset();

    assert_eq!(metrics.late_events_total(), 0);
    assert_eq!(metrics.late_events_dropped(), 0);
    assert_eq!(metrics.late_events_side_output(), 0);
}

#[test]
fn test_window_operator_set_late_data_config() {
    let assigner = TumblingWindowAssigner::from_millis(1000);
    let aggregator = CountAggregator::new();
    let mut operator = TumblingWindowOperator::with_id(
        assigner,
        aggregator,
        Duration::from_millis(100),
        "test_op".to_string(),
    );

    // Default is drop
    assert!(operator.late_data_config().should_drop());

    // Set to side output
    operator.set_late_data_config(LateDataConfig::with_side_output("late".to_string()));
    assert!(!operator.late_data_config().should_drop());
    assert_eq!(operator.late_data_config().side_output(), Some("late"));
}

#[test]
fn test_late_event_dropped_without_side_output() {
    let assigner = TumblingWindowAssigner::from_millis(1000);
    let aggregator = CountAggregator::new();
    let mut operator = TumblingWindowOperator::with_id(
        assigner,
        aggregator,
        Duration::from_millis(0), // No allowed lateness
        "test_op".to_string(),
    );

    // Default: drop late events
    let mut timers = TimerService::new();
    let mut state = InMemoryStore::new();
    // Use a watermark generator with high max lateness so watermarks advance quickly
    let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(0);

    // Process an event to advance the watermark to 1000
    let event1 = create_test_event(1000, 1);
    {
        let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
        operator.process(&event1, &mut ctx);
    }

    // Process a late event (timestamp 500 when watermark is at 1000)
    let late_event = create_test_event(500, 2);
    let outputs = {
        let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
        operator.process(&late_event, &mut ctx)
    };

    // Should emit a LateEvent (dropped)
    assert!(!outputs.is_empty());
    let is_late_event = outputs.iter().any(|o| matches!(o, Output::LateEvent(_)));
    assert!(is_late_event, "Expected LateEvent output");

    // Metrics should show dropped
    assert_eq!(operator.late_data_metrics().late_events_dropped(), 1);
    assert_eq!(operator.late_data_metrics().late_events_side_output(), 0);
}

#[test]
fn test_late_event_routed_to_side_output() {
    let assigner = TumblingWindowAssigner::from_millis(1000);
    let aggregator = CountAggregator::new();
    let mut operator = TumblingWindowOperator::with_id(
        assigner,
        aggregator,
        Duration::from_millis(0), // No allowed lateness
        "test_op".to_string(),
    );

    // Configure side output for late events
    operator.set_late_data_config(LateDataConfig::with_side_output("late_events".to_string()));

    let mut timers = TimerService::new();
    let mut state = InMemoryStore::new();
    let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(0);

    // Process an event to advance the watermark to 1000
    let event1 = create_test_event(1000, 1);
    {
        let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
        operator.process(&event1, &mut ctx);
    }

    // Process a late event
    let late_event = create_test_event(500, 2);
    let outputs = {
        let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
        operator.process(&late_event, &mut ctx)
    };

    // Should emit a SideOutput
    assert!(!outputs.is_empty());
    let side_output = outputs.iter().find_map(|o| {
        if let Output::SideOutput { name, .. } = o {
            Some(name.clone())
        } else {
            None
        }
    });
    assert_eq!(side_output, Some("late_events".to_string()));

    // Metrics should show side output
    assert_eq!(operator.late_data_metrics().late_events_dropped(), 0);
    assert_eq!(operator.late_data_metrics().late_events_side_output(), 1);
}

#[test]
fn test_event_within_lateness_not_late() {
    let assigner = TumblingWindowAssigner::from_millis(1000);
    let aggregator = CountAggregator::new();
    let mut operator = TumblingWindowOperator::with_id(
        assigner,
        aggregator,
        Duration::from_millis(500), // 500ms allowed lateness
        "test_op".to_string(),
    );

    let mut timers = TimerService::new();
    let mut state = InMemoryStore::new();
    let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(0);

    // Process an event to advance the watermark to 1200
    // This would close window [0, 1000) at time 1000 + 0 (no lateness from watermark gen)
    // But with 500ms allowed lateness, window cleanup is at 1500
    let event1 = create_test_event(1200, 1);
    {
        let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
        operator.process(&event1, &mut ctx);
    }

    // Process an event for window [0, 1000) at timestamp 800
    // Watermark is at 1200, window cleanup time is 1000 + 500 = 1500
    // Since 1200 < 1500, the event should NOT be late
    let event2 = create_test_event(800, 2);
    let outputs = {
        let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
        operator.process(&event2, &mut ctx)
    };

    // Should NOT be a late event - should be processed normally
    let is_late_event = outputs
        .iter()
        .any(|o| matches!(o, Output::LateEvent(_) | Output::SideOutput { .. }));
    assert!(
        !is_late_event,
        "Event within lateness period should not be marked as late"
    );

    // No late events recorded
    assert_eq!(operator.late_data_metrics().late_events_total(), 0);
}

#[test]
fn test_reset_late_data_metrics() {
    let assigner = TumblingWindowAssigner::from_millis(1000);
    let aggregator = CountAggregator::new();
    let mut operator = TumblingWindowOperator::with_id(
        assigner,
        aggregator,
        Duration::from_millis(0),
        "test_op".to_string(),
    );

    let mut timers = TimerService::new();
    let mut state = InMemoryStore::new();
    let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(0);

    // Generate a late event
    let event1 = create_test_event(1000, 1);
    {
        let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
        operator.process(&event1, &mut ctx);
    }
    let late_event = create_test_event(500, 2);
    {
        let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
        operator.process(&late_event, &mut ctx);
    }

    assert_eq!(operator.late_data_metrics().late_events_total(), 1);

    // Reset metrics
    operator.reset_late_data_metrics();

    assert_eq!(operator.late_data_metrics().late_events_total(), 0);
}

#[test]
fn test_emit_strategy_helper_methods() {
    // OnWatermark
    assert!(!EmitStrategy::OnWatermark.emits_intermediate());
    assert!(!EmitStrategy::OnWatermark.requires_changelog());
    assert!(!EmitStrategy::OnWatermark.is_append_only_compatible());
    assert!(EmitStrategy::OnWatermark.generates_retractions());
    assert!(!EmitStrategy::OnWatermark.suppresses_intermediate());
    assert!(!EmitStrategy::OnWatermark.drops_late_data());

    // OnUpdate
    assert!(EmitStrategy::OnUpdate.emits_intermediate());
    assert!(!EmitStrategy::OnUpdate.requires_changelog());
    assert!(!EmitStrategy::OnUpdate.is_append_only_compatible());
    assert!(EmitStrategy::OnUpdate.generates_retractions());
    assert!(!EmitStrategy::OnUpdate.suppresses_intermediate());

    // Periodic
    let periodic = EmitStrategy::Periodic(Duration::from_secs(10));
    assert!(periodic.emits_intermediate());
    assert!(!periodic.requires_changelog());
    assert!(!periodic.is_append_only_compatible());
    assert!(!periodic.generates_retractions());
    assert!(!periodic.suppresses_intermediate());

    // OnWindowClose
    assert!(!EmitStrategy::OnWindowClose.emits_intermediate());
    assert!(!EmitStrategy::OnWindowClose.requires_changelog());
    assert!(EmitStrategy::OnWindowClose.is_append_only_compatible());
    assert!(!EmitStrategy::OnWindowClose.generates_retractions());
    assert!(EmitStrategy::OnWindowClose.suppresses_intermediate());
    assert!(!EmitStrategy::OnWindowClose.drops_late_data());

    // Changelog
    assert!(!EmitStrategy::Changelog.emits_intermediate());
    assert!(EmitStrategy::Changelog.requires_changelog());
    assert!(!EmitStrategy::Changelog.is_append_only_compatible());
    assert!(EmitStrategy::Changelog.generates_retractions());
    assert!(!EmitStrategy::Changelog.suppresses_intermediate());

    // Final
    assert!(!EmitStrategy::Final.emits_intermediate());
    assert!(!EmitStrategy::Final.requires_changelog());
    assert!(EmitStrategy::Final.is_append_only_compatible());
    assert!(!EmitStrategy::Final.generates_retractions());
    assert!(EmitStrategy::Final.suppresses_intermediate());
    assert!(EmitStrategy::Final.drops_late_data());
}

#[test]
fn test_cdc_operation() {
    assert_eq!(CdcOperation::Insert.weight(), 1);
    assert_eq!(CdcOperation::Delete.weight(), -1);
    assert_eq!(CdcOperation::UpdateBefore.weight(), -1);
    assert_eq!(CdcOperation::UpdateAfter.weight(), 1);

    assert!(CdcOperation::Insert.is_insert());
    assert!(CdcOperation::UpdateAfter.is_insert());
    assert!(!CdcOperation::Delete.is_insert());
    assert!(!CdcOperation::UpdateBefore.is_insert());

    assert!(CdcOperation::Delete.is_delete());
    assert!(CdcOperation::UpdateBefore.is_delete());
    assert!(!CdcOperation::Insert.is_delete());
    assert!(!CdcOperation::UpdateAfter.is_delete());

    assert_eq!(CdcOperation::Insert.debezium_op(), 'c');
    assert_eq!(CdcOperation::Delete.debezium_op(), 'd');
    assert_eq!(CdcOperation::UpdateBefore.debezium_op(), 'u');
    assert_eq!(CdcOperation::UpdateAfter.debezium_op(), 'u');
}

#[test]
fn test_changelog_record_insert() {
    let event = create_test_event(1000, 42);
    let record = ChangelogRecord::insert(event.clone(), 2000);

    assert_eq!(record.operation, CdcOperation::Insert);
    assert_eq!(record.weight, 1);
    assert_eq!(record.emit_timestamp, 2000);
    assert_eq!(record.event.timestamp, 1000);
    assert!(record.is_insert());
    assert!(!record.is_delete());
}

#[test]
fn test_changelog_record_delete() {
    let event = create_test_event(1000, 42);
    let record = ChangelogRecord::delete(event.clone(), 2000);

    assert_eq!(record.operation, CdcOperation::Delete);
    assert_eq!(record.weight, -1);
    assert_eq!(record.emit_timestamp, 2000);
    assert!(record.is_delete());
    assert!(!record.is_insert());
}

#[test]
fn test_changelog_record_update() {
    let old_event = create_test_event(1000, 10);
    let new_event = create_test_event(1000, 20);
    let (before, after) = ChangelogRecord::update(old_event, new_event, 2000);

    assert_eq!(before.operation, CdcOperation::UpdateBefore);
    assert_eq!(before.weight, -1);
    assert!(before.is_delete());

    assert_eq!(after.operation, CdcOperation::UpdateAfter);
    assert_eq!(after.weight, 1);
    assert!(after.is_insert());
}

#[test]
fn test_emit_strategy_on_window_close() {
    let assigner = TumblingWindowAssigner::from_millis(1000);
    let aggregator = CountAggregator::new();
    let mut operator = TumblingWindowOperator::with_id(
        assigner,
        aggregator,
        Duration::from_millis(0),
        "test_op".to_string(),
    );

    // Set OnWindowClose strategy
    operator.set_emit_strategy(EmitStrategy::OnWindowClose);

    let mut timers = TimerService::new();
    let mut state = InMemoryStore::new();
    let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

    // Process events - should NOT emit intermediate results
    let event1 = create_test_event(100, 1);
    let event2 = create_test_event(200, 2);

    let outputs1 = {
        let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
        operator.process(&event1, &mut ctx)
    };
    let outputs2 = {
        let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
        operator.process(&event2, &mut ctx)
    };

    // No Event outputs (only watermark updates)
    let event_outputs1: Vec<_> = outputs1
        .iter()
        .filter(|o| matches!(o, Output::Event(_)))
        .collect();
    let event_outputs2: Vec<_> = outputs2
        .iter()
        .filter(|o| matches!(o, Output::Event(_)))
        .collect();

    assert!(
        event_outputs1.is_empty(),
        "OnWindowClose should not emit intermediate results"
    );
    assert!(
        event_outputs2.is_empty(),
        "OnWindowClose should not emit intermediate results"
    );
}

#[test]
fn test_emit_strategy_final_drops_late_data() {
    let assigner = TumblingWindowAssigner::from_millis(1000);
    let aggregator = CountAggregator::new();
    let mut operator = TumblingWindowOperator::with_id(
        assigner,
        aggregator,
        Duration::from_millis(0),
        "test_op".to_string(),
    );

    // Set Final strategy
    operator.set_emit_strategy(EmitStrategy::Final);

    let mut timers = TimerService::new();
    let mut state = InMemoryStore::new();
    let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(0);

    // Advance watermark past first window
    let event1 = create_test_event(1500, 1);
    {
        let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
        operator.process(&event1, &mut ctx);
    }

    // Send late event - should be silently dropped (no LateEvent output)
    let late_event = create_test_event(500, 2);
    let outputs = {
        let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
        operator.process(&late_event, &mut ctx)
    };

    // Should have NO output at all (silently dropped)
    assert!(
        outputs.is_empty(),
        "EMIT FINAL should silently drop late data"
    );
    assert_eq!(
        operator.late_data_metrics().late_events_dropped(),
        1,
        "Late event should be recorded as dropped"
    );
}

#[test]
fn test_emit_strategy_changelog_emits_records() {
    let assigner = TumblingWindowAssigner::from_millis(1000);
    let aggregator = CountAggregator::new();
    let mut operator = TumblingWindowOperator::with_id(
        assigner,
        aggregator,
        Duration::from_millis(0),
        "test_op".to_string(),
    );

    // Set Changelog strategy
    operator.set_emit_strategy(EmitStrategy::Changelog);

    let mut timers = TimerService::new();
    let mut state = InMemoryStore::new();
    let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

    // Process event - Changelog emits on every update
    let event = create_test_event(100, 1);
    let outputs = {
        let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
        operator.process(&event, &mut ctx)
    };

    // Should emit Changelog record
    let changelog_outputs: Vec<_> = outputs
        .iter()
        .filter(|o| matches!(o, Output::Changelog(_)))
        .collect();

    assert_eq!(
        changelog_outputs.len(),
        1,
        "Changelog strategy should emit changelog record on update"
    );

    if let Output::Changelog(record) = &changelog_outputs[0] {
        assert_eq!(record.operation, CdcOperation::Insert);
        assert_eq!(record.weight, 1);
    } else {
        panic!("Expected Changelog output");
    }
}

#[test]
fn test_emit_strategy_changelog_on_timer() {
    let assigner = TumblingWindowAssigner::from_millis(1000);
    let aggregator = CountAggregator::new();
    let mut operator = TumblingWindowOperator::with_id(
        assigner,
        aggregator,
        Duration::from_millis(0),
        "test_op".to_string(),
    );

    // Set Changelog strategy
    operator.set_emit_strategy(EmitStrategy::Changelog);

    let mut timers = TimerService::new();
    let mut state = InMemoryStore::new();
    let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

    // Process events to populate window
    let event = create_test_event(100, 1);
    {
        let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
        operator.process(&event, &mut ctx);
    }

    // Trigger window timer - should emit Changelog
    let timer = Timer {
        key: WindowId::new(0, 1000).to_key(),
        timestamp: 1000,
    };

    let outputs = {
        let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
        operator.on_timer(timer, &mut ctx)
    };

    // Should emit Changelog record on final emission
    let changelog_outputs: Vec<_> = outputs
        .iter()
        .filter(|o| matches!(o, Output::Changelog(_)))
        .collect();

    assert_eq!(
        changelog_outputs.len(),
        1,
        "Changelog strategy should emit changelog record on timer"
    );

    if let Output::Changelog(record) = &changelog_outputs[0] {
        assert_eq!(record.operation, CdcOperation::Insert);
    } else {
        panic!("Expected Changelog output");
    }
}

// FIRST_VALUE / LAST_VALUE Tests

#[test]
fn test_first_value_single_event() {
    let mut acc = FirstValueAccumulator::default();
    assert!(acc.is_empty());
    assert_eq!(acc.result(), None);

    acc.add((100, 1000));
    assert!(!acc.is_empty());
    assert_eq!(acc.result(), Some(100));
}

#[test]
fn test_first_value_multiple_events() {
    let mut acc = FirstValueAccumulator::default();
    acc.add((100, 1000)); // timestamp 1000
    acc.add((200, 500)); // timestamp 500 (earlier)
    acc.add((300, 1500)); // timestamp 1500 (later)

    // Earliest timestamp wins
    assert_eq!(acc.result(), Some(200));
}

#[test]
fn test_first_value_same_timestamp() {
    let mut acc = FirstValueAccumulator::default();
    acc.add((100, 1000));
    acc.add((200, 1000)); // Same timestamp - keep first

    assert_eq!(acc.result(), Some(100));
}

#[test]
fn test_first_value_merge() {
    let mut acc1 = FirstValueAccumulator::default();
    acc1.add((100, 1000));

    let mut acc2 = FirstValueAccumulator::default();
    acc2.add((200, 500)); // Earlier

    acc1.merge(&acc2);
    assert_eq!(acc1.result(), Some(200)); // 500 < 1000
}

#[test]
fn test_first_value_merge_empty() {
    let mut acc1 = FirstValueAccumulator::default();
    acc1.add((100, 1000));

    let acc2 = FirstValueAccumulator::default(); // Empty

    acc1.merge(&acc2);
    assert_eq!(acc1.result(), Some(100)); // Keep acc1

    let mut acc3 = FirstValueAccumulator::default(); // Empty
    acc3.merge(&acc1);
    assert_eq!(acc3.result(), Some(100)); // Take acc1's value
}

#[test]
fn test_last_value_single_event() {
    let mut acc = LastValueAccumulator::default();
    assert!(acc.is_empty());
    assert_eq!(acc.result(), None);

    acc.add((100, 1000));
    assert!(!acc.is_empty());
    assert_eq!(acc.result(), Some(100));
}

#[test]
fn test_last_value_multiple_events() {
    let mut acc = LastValueAccumulator::default();
    acc.add((100, 1000));
    acc.add((200, 500)); // Earlier - ignored
    acc.add((300, 1500)); // Latest timestamp wins

    assert_eq!(acc.result(), Some(300));
}

#[test]
fn test_last_value_same_timestamp() {
    let mut acc = LastValueAccumulator::default();
    acc.add((100, 1000));
    acc.add((200, 1000)); // Same timestamp - keep latest arrival

    assert_eq!(acc.result(), Some(200));
}

#[test]
fn test_last_value_merge() {
    let mut acc1 = LastValueAccumulator::default();
    acc1.add((100, 1000));

    let mut acc2 = LastValueAccumulator::default();
    acc2.add((200, 1500)); // Later

    acc1.merge(&acc2);
    assert_eq!(acc1.result(), Some(200)); // 1500 > 1000
}

#[test]
fn test_last_value_merge_same_timestamp() {
    let mut acc1 = LastValueAccumulator::default();
    acc1.add((100, 1000));

    let mut acc2 = LastValueAccumulator::default();
    acc2.add((200, 1000)); // Same timestamp

    acc1.merge(&acc2);
    assert_eq!(acc1.result(), Some(200)); // Take other on same timestamp
}

#[test]
fn test_first_value_f64_basic() {
    let mut acc = FirstValueF64Accumulator::default();
    acc.add((100.5, 1000));
    acc.add((200.5, 500)); // Earlier
    acc.add((300.5, 1500)); // Later

    let result = acc.result().unwrap();
    assert!((result - 200.5).abs() < f64::EPSILON);
}

#[test]
fn test_last_value_f64_basic() {
    let mut acc = LastValueF64Accumulator::default();
    acc.add((100.5, 1000));
    acc.add((200.5, 500)); // Earlier
    acc.add((300.5, 1500)); // Later

    let result = acc.result().unwrap();
    assert!((result - 300.5).abs() < f64::EPSILON);
}

#[test]
fn test_first_value_aggregator_extract() {
    let aggregator = FirstValueAggregator::new(0, 1);

    // Create event with value and timestamp columns
    let schema = Arc::new(Schema::new(vec![
        Field::new("price", DataType::Int64, false),
        Field::new("ts", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![100])),
            Arc::new(Int64Array::from(vec![1000])),
        ],
    )
    .unwrap();
    let event = Event::new(1000, batch);

    let extracted = aggregator.extract(&event);
    assert_eq!(extracted, Some((100, 1000)));
}

#[test]
fn test_last_value_aggregator_extract() {
    let aggregator = LastValueAggregator::new(0, 1);

    let schema = Arc::new(Schema::new(vec![
        Field::new("price", DataType::Int64, false),
        Field::new("ts", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![100])),
            Arc::new(Int64Array::from(vec![1000])),
        ],
    )
    .unwrap();
    let event = Event::new(1000, batch);

    let extracted = aggregator.extract(&event);
    assert_eq!(extracted, Some((100, 1000)));
}

#[test]
fn test_first_value_aggregator_invalid_column() {
    let aggregator = FirstValueAggregator::new(5, 6); // Out of bounds

    let schema = Arc::new(Schema::new(vec![Field::new(
        "price",
        DataType::Int64,
        false,
    )]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![100]))]).unwrap();
    let event = Event::new(1000, batch);

    assert_eq!(aggregator.extract(&event), None);
}

#[test]
fn test_ohlc_simulation() {
    // Simulate OHLC bar generation:
    // Open = FIRST_VALUE(price)
    // High = MAX(price)
    // Low = MIN(price)
    // Close = LAST_VALUE(price)
    // Volume = SUM(quantity)

    // Trades: (price, timestamp, quantity)
    // t=100: price=100, qty=10
    // t=200: price=105, qty=5
    // t=300: price=98, qty=15
    // t=400: price=102, qty=8

    let mut first = FirstValueAccumulator::default();
    let mut max = MaxAccumulator::default();
    let mut min = MinAccumulator::default();
    let mut last = LastValueAccumulator::default();
    let mut sum = SumAccumulator::default();

    // Process trades
    first.add((100, 100));
    max.add(100);
    min.add(100);
    last.add((100, 100));
    sum.add(10);

    first.add((105, 200));
    max.add(105);
    min.add(105);
    last.add((105, 200));
    sum.add(5);

    first.add((98, 300));
    max.add(98);
    min.add(98);
    last.add((98, 300));
    sum.add(15);

    first.add((102, 400));
    max.add(102);
    min.add(102);
    last.add((102, 400));
    sum.add(8);

    // Expected OHLC: open=100, high=105, low=98, close=102, volume=38
    assert_eq!(first.result(), Some(100), "Open");
    assert_eq!(max.result(), Some(105), "High");
    assert_eq!(min.result(), Some(98), "Low");
    assert_eq!(last.result(), Some(102), "Close");
    assert_eq!(sum.result(), 38, "Volume");
}

#[test]
fn test_first_value_checkpoint_restore() {
    let mut acc = FirstValueAccumulator::default();
    acc.add((100, 1000));
    acc.add((200, 500)); // Earlier - this wins

    // Serialize
    let bytes = rkyv::to_bytes::<RkyvError>(&acc)
        .expect("serialize")
        .to_vec();

    // Deserialize
    let restored =
        rkyv::from_bytes::<FirstValueAccumulator, RkyvError>(&bytes).expect("deserialize");

    assert_eq!(restored.result(), Some(200));
    assert_eq!(restored.timestamp, Some(500));
}

#[test]
fn test_last_value_checkpoint_restore() {
    let mut acc = LastValueAccumulator::default();
    acc.add((100, 1000));
    acc.add((300, 1500)); // Later - this wins

    // Serialize
    let bytes = rkyv::to_bytes::<RkyvError>(&acc)
        .expect("serialize")
        .to_vec();

    // Deserialize
    let restored =
        rkyv::from_bytes::<LastValueAccumulator, RkyvError>(&bytes).expect("deserialize");

    assert_eq!(restored.result(), Some(300));
    assert_eq!(restored.timestamp, Some(1500));
}

// ════════════════════════════════════════════════════════════════════════
// Composite Aggregator & f64 Type Support Tests
// ════════════════════════════════════════════════════════════════════════

// ── ScalarResult tests ──────────────────────────────────────────────────

#[test]
fn test_scalar_result_int64_conversions() {
    let r = ScalarResult::Int64(42);
    assert_eq!(r.to_i64_lossy(), 42);
    assert!((r.to_f64_lossy() - 42.0).abs() < f64::EPSILON);
    assert!(!r.is_null());
    assert_eq!(r.data_type(), DataType::Int64);
}

#[test]
fn test_scalar_result_float64_conversions() {
    let r = ScalarResult::Float64(3.125);
    assert_eq!(r.to_i64_lossy(), 3); // truncated
    assert!((r.to_f64_lossy() - 3.125).abs() < f64::EPSILON);
    assert!(!r.is_null());
    assert_eq!(r.data_type(), DataType::Float64);
}

#[test]
fn test_scalar_result_uint64_conversions() {
    let r = ScalarResult::UInt64(100);
    assert_eq!(r.to_i64_lossy(), 100);
    assert!((r.to_f64_lossy() - 100.0).abs() < f64::EPSILON);
    assert_eq!(r.data_type(), DataType::UInt64);
}

#[test]
fn test_scalar_result_null_variants() {
    assert!(ScalarResult::Null.is_null());
    assert!(ScalarResult::OptionalInt64(None).is_null());
    assert!(ScalarResult::OptionalFloat64(None).is_null());
    assert!(!ScalarResult::OptionalInt64(Some(1)).is_null());
    assert!(!ScalarResult::OptionalFloat64(Some(1.0)).is_null());
}

#[test]
fn test_scalar_result_optional_conversions() {
    let r = ScalarResult::OptionalFloat64(Some(2.5));
    assert_eq!(r.to_i64_lossy(), 2);
    assert!((r.to_f64_lossy() - 2.5).abs() < f64::EPSILON);

    let r2 = ScalarResult::OptionalInt64(None);
    assert_eq!(r2.to_i64_lossy(), 0);
    assert!((r2.to_f64_lossy()).abs() < f64::EPSILON);
}

// ── f64 aggregator tests ────────────────────────────────────────────────

fn make_f64_event(values: &[f64]) -> Event {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Float64,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(arrow_array::Float64Array::from(values.to_vec()))],
    )
    .unwrap();
    Event::new(1000, batch)
}

#[test]
fn test_sum_f64_accumulator_basic() {
    let mut acc = SumF64IndexedAccumulator::new(0);
    let event = make_f64_event(&[1.5, 2.5, 3.0]);
    acc.add_event(&event);
    assert!(!acc.is_empty());
    match acc.result_scalar() {
        ScalarResult::Float64(v) => assert!((v - 7.0).abs() < f64::EPSILON),
        other => panic!("Expected Float64, got {other:?}"),
    }
}

#[test]
fn test_sum_f64_accumulator_empty() {
    let acc = SumF64IndexedAccumulator::new(0);
    assert!(acc.is_empty());
    assert!(matches!(acc.result_scalar(), ScalarResult::Null));
}

#[test]
fn test_min_f64_accumulator_basic() {
    let mut acc = MinF64IndexedAccumulator::new(0);
    let event = make_f64_event(&[3.0, 1.5, 2.5]);
    acc.add_event(&event);
    match acc.result_scalar() {
        ScalarResult::OptionalFloat64(Some(v)) => {
            assert!((v - 1.5).abs() < f64::EPSILON);
        }
        other => panic!("Expected OptionalFloat64(Some), got {other:?}"),
    }
}

#[test]
fn test_max_f64_accumulator_basic() {
    let mut acc = MaxF64IndexedAccumulator::new(0);
    let event = make_f64_event(&[3.0, 1.5, 2.5]);
    acc.add_event(&event);
    match acc.result_scalar() {
        ScalarResult::OptionalFloat64(Some(v)) => {
            assert!((v - 3.0).abs() < f64::EPSILON);
        }
        other => panic!("Expected OptionalFloat64(Some), got {other:?}"),
    }
}

#[test]
fn test_avg_f64_accumulator_basic() {
    let mut acc = AvgF64IndexedAccumulator::new(0);
    let event = make_f64_event(&[1.0, 2.0, 3.0]);
    acc.add_event(&event);
    match acc.result_scalar() {
        ScalarResult::Float64(v) => assert!((v - 2.0).abs() < f64::EPSILON),
        other => panic!("Expected Float64, got {other:?}"),
    }
}

#[test]
fn test_sum_f64_merge() {
    let mut acc1 = SumF64IndexedAccumulator::new(0);
    let mut acc2 = SumF64IndexedAccumulator::new(0);
    acc1.add_event(&make_f64_event(&[1.0, 2.0]));
    acc2.add_event(&make_f64_event(&[3.0, 4.0]));
    acc1.merge_dyn(&acc2);
    match acc1.result_scalar() {
        ScalarResult::Float64(v) => assert!((v - 10.0).abs() < f64::EPSILON),
        other => panic!("Expected Float64, got {other:?}"),
    }
}

#[test]
fn test_min_f64_merge() {
    let mut acc1 = MinF64IndexedAccumulator::new(0);
    let mut acc2 = MinF64IndexedAccumulator::new(0);
    acc1.add_event(&make_f64_event(&[5.0]));
    acc2.add_event(&make_f64_event(&[2.0]));
    acc1.merge_dyn(&acc2);
    match acc1.result_scalar() {
        ScalarResult::OptionalFloat64(Some(v)) => {
            assert!((v - 2.0).abs() < f64::EPSILON);
        }
        other => panic!("Expected OptionalFloat64(Some), got {other:?}"),
    }
}

#[test]
fn test_f64_accumulator_serialization() {
    let mut acc = SumF64IndexedAccumulator::new(0);
    acc.add_event(&make_f64_event(&[1.5, 2.5]));
    let data = acc.serialize();
    assert_eq!(data.len(), 16); // 8 bytes sum + 8 bytes count
}

// ── Count DynAccumulator tests ──────────────────────────────────────────

#[test]
fn test_count_dyn_accumulator() {
    let mut acc = CountDynAccumulator::default();
    let event = make_f64_event(&[1.0, 2.0, 3.0]);
    acc.add_event(&event);
    assert_eq!(acc.result_scalar(), ScalarResult::Int64(3));
}

#[test]
fn test_count_dyn_merge() {
    let mut acc1 = CountDynAccumulator::default();
    let mut acc2 = CountDynAccumulator::default();
    acc1.add_event(&make_f64_event(&[1.0, 2.0]));
    acc2.add_event(&make_f64_event(&[3.0]));
    acc1.merge_dyn(&acc2);
    assert_eq!(acc1.result_scalar(), ScalarResult::Int64(3));
}

// ── FirstValue/LastValue DynAccumulator tests ───────────────────────────

fn make_ts_f64_event(values: &[(f64, i64)]) -> Event {
    let schema = Arc::new(Schema::new(vec![
        Field::new("value", DataType::Float64, false),
        Field::new("timestamp", DataType::Int64, false),
    ]));
    let vals: Vec<f64> = values.iter().map(|(v, _)| *v).collect();
    let tss: Vec<i64> = values.iter().map(|(_, t)| *t).collect();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(arrow_array::Float64Array::from(vals)),
            Arc::new(Int64Array::from(tss)),
        ],
    )
    .unwrap();
    Event::new(1000, batch)
}

#[test]
fn test_first_value_f64_dyn() {
    let mut acc = FirstValueF64DynAccumulator::new(0, 1);
    acc.add_event(&make_ts_f64_event(&[(10.0, 200), (20.0, 100), (30.0, 300)]));
    // Earliest timestamp is 100 → value 20.0
    match acc.result_scalar() {
        ScalarResult::OptionalFloat64(Some(v)) => {
            assert!((v - 20.0).abs() < f64::EPSILON);
        }
        other => panic!("Expected OptionalFloat64(Some), got {other:?}"),
    }
}

#[test]
fn test_last_value_f64_dyn() {
    let mut acc = LastValueF64DynAccumulator::new(0, 1);
    acc.add_event(&make_ts_f64_event(&[(10.0, 200), (20.0, 100), (30.0, 300)]));
    // Latest timestamp is 300 → value 30.0
    match acc.result_scalar() {
        ScalarResult::OptionalFloat64(Some(v)) => {
            assert!((v - 30.0).abs() < f64::EPSILON);
        }
        other => panic!("Expected OptionalFloat64(Some), got {other:?}"),
    }
}

#[test]
fn test_first_value_f64_dyn_merge() {
    let mut acc1 = FirstValueF64DynAccumulator::new(0, 1);
    let mut acc2 = FirstValueF64DynAccumulator::new(0, 1);
    acc1.add_event(&make_ts_f64_event(&[(10.0, 200)]));
    acc2.add_event(&make_ts_f64_event(&[(20.0, 50)]));
    acc1.merge_dyn(&acc2);
    // acc2 has earlier timestamp (50) → value 20.0
    match acc1.result_scalar() {
        ScalarResult::OptionalFloat64(Some(v)) => {
            assert!((v - 20.0).abs() < f64::EPSILON);
        }
        other => panic!("Expected OptionalFloat64(Some), got {other:?}"),
    }
}

// ── CompositeAggregator tests ───────────────────────────────────────────

#[test]
fn test_composite_aggregator_creation() {
    let agg = CompositeAggregator::new(vec![
        Box::new(CountDynFactory::new("cnt")),
        Box::new(SumF64Factory::new(0, "total")),
    ]);
    assert_eq!(agg.num_aggregates(), 2);
}

#[test]
fn test_composite_aggregator_schema() {
    let agg = CompositeAggregator::new(vec![
        Box::new(CountDynFactory::new("cnt")),
        Box::new(MinF64Factory::new(0, "low")),
        Box::new(MaxF64Factory::new(0, "high")),
    ]);
    let schema = agg.output_schema();
    assert_eq!(schema.fields().len(), 5); // window_start, window_end, cnt, low, high
    assert_eq!(schema.field(0).name(), "window_start");
    assert_eq!(schema.field(1).name(), "window_end");
    assert_eq!(schema.field(2).name(), "cnt");
    assert_eq!(schema.field(3).name(), "low");
    assert_eq!(schema.field(4).name(), "high");
}

#[test]
fn test_composite_accumulator_lifecycle() {
    let agg = CompositeAggregator::new(vec![
        Box::new(CountDynFactory::new("cnt")),
        Box::new(SumF64Factory::new(0, "total")),
    ]);
    let mut acc = agg.create_accumulator();
    assert!(acc.is_empty());
    assert_eq!(acc.num_accumulators(), 2);

    let event = make_f64_event(&[1.0, 2.0, 3.0]);
    acc.add_event(&event);
    assert!(!acc.is_empty());

    let results = acc.results();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0], ScalarResult::Int64(3));
    match &results[1] {
        ScalarResult::Float64(v) => assert!((v - 6.0).abs() < f64::EPSILON),
        other => panic!("Expected Float64, got {other:?}"),
    }
}

#[test]
fn test_composite_accumulator_merge() {
    let agg = CompositeAggregator::new(vec![
        Box::new(CountDynFactory::new("cnt")),
        Box::new(SumF64Factory::new(0, "total")),
    ]);
    let mut acc1 = agg.create_accumulator();
    let acc2_holder = agg.create_accumulator();
    // Fill acc1
    acc1.add_event(&make_f64_event(&[1.0, 2.0]));

    // We need a mutable acc2 to add events
    let mut acc2 = acc2_holder;
    acc2.add_event(&make_f64_event(&[3.0, 4.0]));

    acc1.merge(&acc2);
    let results = acc1.results();
    assert_eq!(results[0], ScalarResult::Int64(4));
    match &results[1] {
        ScalarResult::Float64(v) => assert!((v - 10.0).abs() < f64::EPSILON),
        other => panic!("Expected Float64, got {other:?}"),
    }
}

#[test]
fn test_composite_accumulator_serialization() {
    let agg = CompositeAggregator::new(vec![
        Box::new(CountDynFactory::new("cnt")),
        Box::new(SumF64Factory::new(0, "total")),
    ]);
    let mut acc = agg.create_accumulator();
    acc.add_event(&make_f64_event(&[1.0, 2.0]));

    let bytes = acc.serialize();
    // Should be non-empty with header and two accumulator entries
    assert!(bytes.len() > 4);
    // Header: 4 bytes for count
    let n = u32::from_le_bytes(bytes[..4].try_into().unwrap());
    assert_eq!(n, 2);
}

#[test]
fn test_composite_accumulator_record_batch() {
    let agg = CompositeAggregator::new(vec![
        Box::new(CountDynFactory::new("cnt")),
        Box::new(MinF64Factory::new(0, "low")),
        Box::new(MaxF64Factory::new(0, "high")),
    ]);
    let schema = agg.output_schema();
    let mut acc = agg.create_accumulator();
    acc.add_event(&make_f64_event(&[3.0, 1.0, 5.0, 2.0]));

    let window_id = WindowId::new(0, 60000);
    let batch = acc.to_record_batch(&window_id, &schema).unwrap();

    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.num_columns(), 5);

    // window_start = 0
    let ws = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(ws.value(0), 0);

    // window_end = 60000
    let we = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(we.value(0), 60000);

    // count = 4
    let cnt = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(cnt.value(0), 4);

    // min = 1.0
    let low = batch
        .column(3)
        .as_any()
        .downcast_ref::<arrow_array::Float64Array>()
        .unwrap();
    assert!((low.value(0) - 1.0).abs() < f64::EPSILON);

    // max = 5.0
    let high = batch
        .column(4)
        .as_any()
        .downcast_ref::<arrow_array::Float64Array>()
        .unwrap();
    assert!((high.value(0) - 5.0).abs() < f64::EPSILON);
}

#[test]
fn test_ohlc_composite_integration() {
    // Simulate OHLC query: FIRST(price), MAX(price), MIN(price), LAST(price), SUM(qty), COUNT(*)
    let agg = CompositeAggregator::new(vec![
        Box::new(FirstValueF64DynFactory::new(0, 1, "open")),
        Box::new(MaxF64Factory::new(0, "high")),
        Box::new(MinF64Factory::new(0, "low")),
        Box::new(LastValueF64DynFactory::new(0, 1, "close")),
        Box::new(CountDynFactory::new("trade_count")),
    ]);

    let mut acc = agg.create_accumulator();

    // Trades: (price, timestamp)
    let schema = Arc::new(Schema::new(vec![
        Field::new("price", DataType::Float64, false),
        Field::new("ts", DataType::Int64, false),
    ]));

    // Trade 1: price=100.0 at t=1000
    let batch1 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(arrow_array::Float64Array::from(vec![100.0])),
            Arc::new(Int64Array::from(vec![1000])),
        ],
    )
    .unwrap();
    acc.add_event(&Event::new(1000, batch1));

    // Trade 2: price=105.0 at t=2000
    let batch2 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(arrow_array::Float64Array::from(vec![105.0])),
            Arc::new(Int64Array::from(vec![2000])),
        ],
    )
    .unwrap();
    acc.add_event(&Event::new(2000, batch2));

    // Trade 3: price=98.0 at t=3000
    let batch3 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(arrow_array::Float64Array::from(vec![98.0])),
            Arc::new(Int64Array::from(vec![3000])),
        ],
    )
    .unwrap();
    acc.add_event(&Event::new(3000, batch3));

    // Trade 4: price=102.0 at t=4000
    let batch4 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(arrow_array::Float64Array::from(vec![102.0])),
            Arc::new(Int64Array::from(vec![4000])),
        ],
    )
    .unwrap();
    acc.add_event(&Event::new(4000, batch4));

    let results = acc.results();
    // OHLC: Open=100, High=105, Low=98, Close=102, Count=4
    match &results[0] {
        ScalarResult::OptionalFloat64(Some(v)) => {
            assert!((v - 100.0).abs() < f64::EPSILON, "Open should be 100.0");
        }
        other => panic!("Expected Open=100.0, got {other:?}"),
    }
    match &results[1] {
        ScalarResult::OptionalFloat64(Some(v)) => {
            assert!((v - 105.0).abs() < f64::EPSILON, "High should be 105.0");
        }
        other => panic!("Expected High=105.0, got {other:?}"),
    }
    match &results[2] {
        ScalarResult::OptionalFloat64(Some(v)) => {
            assert!((v - 98.0).abs() < f64::EPSILON, "Low should be 98.0");
        }
        other => panic!("Expected Low=98.0, got {other:?}"),
    }
    match &results[3] {
        ScalarResult::OptionalFloat64(Some(v)) => {
            assert!((v - 102.0).abs() < f64::EPSILON, "Close should be 102.0");
        }
        other => panic!("Expected Close=102.0, got {other:?}"),
    }
    assert_eq!(results[4], ScalarResult::Int64(4));
}

#[test]
fn test_composite_aggregator_clone() {
    let agg = CompositeAggregator::new(vec![
        Box::new(CountDynFactory::new("cnt")),
        Box::new(SumF64Factory::new(0, "total")),
    ]);
    let cloned = agg.clone();
    assert_eq!(cloned.num_aggregates(), 2);

    // Create accumulators from both and verify they produce same results
    let mut acc1 = agg.create_accumulator();
    let mut acc2 = cloned.create_accumulator();
    let event = make_f64_event(&[5.0]);
    acc1.add_event(&event);
    acc2.add_event(&event);
    assert_eq!(acc1.results(), acc2.results());
}

#[test]
fn test_composite_accumulator_clone() {
    let agg = CompositeAggregator::new(vec![Box::new(CountDynFactory::new("cnt"))]);
    let mut acc = agg.create_accumulator();
    acc.add_event(&make_f64_event(&[1.0, 2.0]));

    let cloned = acc.clone();
    assert_eq!(acc.results(), cloned.results());
}

#[test]
fn test_backward_compat_existing_aggregators_unchanged() {
    // Verify existing static-dispatch aggregators still work
    let count_agg = CountAggregator::new();
    let mut count_acc = count_agg.create_accumulator();
    count_acc.add(());
    count_acc.add(());
    assert_eq!(count_acc.result(), 2);

    let sum_agg = SumAggregator::new(0);
    let mut sum_acc = sum_agg.create_accumulator();
    sum_acc.add(10);
    sum_acc.add(20);
    assert_eq!(sum_acc.result(), 30);
}

#[test]
fn test_backward_compat_result_to_i64() {
    // Verify ResultToI64 still works for existing types
    assert_eq!(42u64.to_i64(), 42);
    assert_eq!(42i64.to_i64(), 42);
    assert_eq!(Some(42i64).to_i64(), 42);
    assert_eq!(None::<i64>.to_i64(), 0);
}

#[test]
fn test_backward_compat_window_schema_unchanged() {
    let schema = create_window_output_schema();
    assert_eq!(schema.fields().len(), 3);
    assert_eq!(schema.field(0).name(), "window_start");
    assert_eq!(schema.field(1).name(), "window_end");
    assert_eq!(schema.field(2).name(), "result");
}

#[test]
fn test_f64_accumulator_out_of_range_column() {
    // Column index out of range should not panic
    let mut acc = SumF64IndexedAccumulator::new(99);
    acc.add_event(&make_f64_event(&[1.0, 2.0]));
    assert!(acc.is_empty());
}

#[test]
fn test_f64_factory_types() {
    let sum_factory = SumF64Factory::new(0, "total");
    assert_eq!(sum_factory.type_tag(), "sum_f64");
    assert_eq!(sum_factory.result_field().name(), "total");

    let min_factory = MinF64Factory::new(1, "low");
    assert_eq!(min_factory.type_tag(), "min_f64");

    let max_factory = MaxF64Factory::new(1, "high");
    assert_eq!(max_factory.type_tag(), "max_f64");

    let avg_factory = AvgF64Factory::new(0, "average");
    assert_eq!(avg_factory.type_tag(), "avg_f64");
}

// ============================================================================
// Window Close Metrics Tests
// ============================================================================

#[test]
fn test_window_close_metrics_basic() {
    let mut metrics = WindowCloseMetrics::new();
    assert_eq!(metrics.windows_closed_total(), 0);
    assert_eq!(metrics.avg_close_latency_ms(), 0);
    assert_eq!(metrics.max_close_latency_ms(), 0);

    // Record a close with 500ms latency (window_end=1000, processing_time=1500)
    metrics.record_close(1000, 1500);
    assert_eq!(metrics.windows_closed_total(), 1);
    assert_eq!(metrics.avg_close_latency_ms(), 500);
    assert_eq!(metrics.max_close_latency_ms(), 500);

    // Record another with 200ms latency
    metrics.record_close(2000, 2200);
    assert_eq!(metrics.windows_closed_total(), 2);
    assert_eq!(metrics.avg_close_latency_ms(), 350); // (500+200)/2
    assert_eq!(metrics.max_close_latency_ms(), 500);

    // Record another with 1000ms latency (new max)
    metrics.record_close(3000, 4000);
    assert_eq!(metrics.windows_closed_total(), 3);
    assert_eq!(metrics.max_close_latency_ms(), 1000);
}

#[test]
fn test_window_close_metrics_reset() {
    let mut metrics = WindowCloseMetrics::new();
    metrics.record_close(1000, 1500);
    metrics.record_close(2000, 2200);
    assert_eq!(metrics.windows_closed_total(), 2);

    metrics.reset();
    assert_eq!(metrics.windows_closed_total(), 0);
    assert_eq!(metrics.avg_close_latency_ms(), 0);
    assert_eq!(metrics.max_close_latency_ms(), 0);
}

#[test]
fn test_tumbling_window_close_metrics_on_timer() {
    let assigner = TumblingWindowAssigner::from_millis(1000);
    let aggregator = CountAggregator::new();
    let mut operator = TumblingWindowOperator::with_id(
        assigner,
        aggregator,
        Duration::from_millis(0),
        "metrics_test".to_string(),
    );

    let mut timers = TimerService::new();
    let mut state = InMemoryStore::new();
    let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(0);

    // Process events into 2 windows
    for ts in [100, 400, 700] {
        let event = create_test_event(ts, 1);
        let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
        operator.process(&event, &mut ctx);
    }
    for ts in [1100, 1500] {
        let event = create_test_event(ts, 1);
        let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
        operator.process(&event, &mut ctx);
    }

    // Verify active windows count
    assert_eq!(operator.active_windows_count(), 2);

    // Fire window 1
    let win_timer_1 = Timer {
        key: WindowId::new(0, 1000).to_key(),
        timestamp: 1000,
    };
    {
        let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
        operator.on_timer(win_timer_1, &mut ctx);
    }

    assert_eq!(operator.window_close_metrics().windows_closed_total(), 1);
    assert_eq!(operator.active_windows_count(), 1);

    // Fire window 2
    let win_timer_2 = Timer {
        key: WindowId::new(1000, 2000).to_key(),
        timestamp: 2000,
    };
    {
        let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
        operator.on_timer(win_timer_2, &mut ctx);
    }

    assert_eq!(operator.window_close_metrics().windows_closed_total(), 2);
    assert_eq!(operator.active_windows_count(), 0);
}

#[test]
fn test_window_close_metrics_empty_window_not_counted() {
    let assigner = TumblingWindowAssigner::from_millis(1000);
    let aggregator = CountAggregator::new();
    let mut operator = TumblingWindowOperator::with_id(
        assigner,
        aggregator,
        Duration::from_millis(0),
        "metrics_test".to_string(),
    );

    let mut timers = TimerService::new();
    let mut state = InMemoryStore::new();
    let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(0);

    // Fire timer for empty window — should not count as closed
    let timer = Timer {
        key: WindowId::new(0, 1000).to_key(),
        timestamp: 1000,
    };
    {
        let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
        operator.on_timer(timer, &mut ctx);
    }

    assert_eq!(
        operator.window_close_metrics().windows_closed_total(),
        0,
        "Empty window should not increment closed counter"
    );
}

// ============================================================================
// EMIT ON WINDOW CLOSE (EOWC) — End-to-End Tests (Issue #52)
// ============================================================================

#[test]
fn test_eowc_tumbling_single_window_single_emit() {
    // Verify: exactly 1 Output::Event with final count after watermark
    // advances past window_end. No intermediate emissions.
    let assigner = TumblingWindowAssigner::from_millis(60_000); // 1 minute
    let aggregator = CountAggregator::new();
    let mut operator = TumblingWindowOperator::with_id(
        assigner,
        aggregator,
        Duration::from_millis(0),
        "eowc_test".to_string(),
    );
    operator.set_emit_strategy(EmitStrategy::OnWindowClose);

    let mut timers = TimerService::new();
    let mut state = InMemoryStore::new();
    let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(0);

    // Send 10 events in window [0, 60000)
    for i in 0..10 {
        let event = create_test_event(i * 5000, 1); // 0, 5k, 10k, ..., 45k
        let outputs = {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process(&event, &mut ctx)
        };
        // No Event outputs during accumulation
        let event_outputs: Vec<_> = outputs
            .iter()
            .filter(|o| matches!(o, Output::Event(_)))
            .collect();
        assert!(
            event_outputs.is_empty(),
            "EOWC should not emit intermediate results (event #{i})"
        );
    }

    // Now fire the timer at window_end (60000)
    let timer = Timer {
        key: WindowId::new(0, 60_000).to_key(),
        timestamp: 60_000,
    };
    let outputs = {
        let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
        operator.on_timer(timer, &mut ctx)
    };

    // Exactly 1 emission
    assert_eq!(outputs.len(), 1, "EOWC should emit exactly once per window");
    match &outputs[0] {
        Output::Event(event) => {
            assert_eq!(event.timestamp, 60_000);
            let result_col = event.data.column(2);
            let result_array = result_col.as_any().downcast_ref::<Int64Array>().unwrap();
            assert_eq!(result_array.value(0), 10, "Count should be 10");
        }
        other => panic!("Expected Output::Event, got: {other:?}"),
    }

    // Window state should be purged
    assert!(
        operator.registered_windows.is_empty(),
        "Window state should be purged after EOWC emit"
    );
}

#[test]
fn test_eowc_tumbling_no_intermediate_emissions() {
    // Verify that process() NEVER returns Output::Event for OnWindowClose.
    // Only Output::Watermark (and possibly nothing) should come from process().
    let assigner = TumblingWindowAssigner::from_millis(1000);
    let aggregator = CountAggregator::new();
    let mut operator = TumblingWindowOperator::with_id(
        assigner,
        aggregator,
        Duration::from_millis(0),
        "eowc_test".to_string(),
    );
    operator.set_emit_strategy(EmitStrategy::OnWindowClose);

    let mut timers = TimerService::new();
    let mut state = InMemoryStore::new();
    let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

    // Process 20 events across 2 windows
    for ts in (0..20).map(|i| i * 100) {
        let event = create_test_event(ts, 1);
        let outputs = {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process(&event, &mut ctx)
        };
        for output in &outputs {
            assert!(
                !matches!(output, Output::Event(_)),
                "process() must not emit Output::Event with OnWindowClose (ts={ts})"
            );
            assert!(
                !matches!(output, Output::Changelog(_)),
                "process() must not emit Output::Changelog with OnWindowClose (ts={ts})"
            );
        }
    }
}

#[test]
fn test_eowc_tumbling_multiple_windows() {
    // Send events spanning 3 windows, advance watermark past each,
    // verify 3 separate emissions with correct counts.
    let assigner = TumblingWindowAssigner::from_millis(1000);
    let aggregator = CountAggregator::new();
    let mut operator = TumblingWindowOperator::with_id(
        assigner,
        aggregator,
        Duration::from_millis(0),
        "eowc_test".to_string(),
    );
    operator.set_emit_strategy(EmitStrategy::OnWindowClose);

    let mut timers = TimerService::new();
    let mut state = InMemoryStore::new();
    let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(0);

    // Window [0, 1000): 3 events
    for ts in [100, 400, 700] {
        let event = create_test_event(ts, 1);
        let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
        operator.process(&event, &mut ctx);
    }

    // Window [1000, 2000): 2 events
    for ts in [1100, 1500] {
        let event = create_test_event(ts, 1);
        let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
        operator.process(&event, &mut ctx);
    }

    // Window [2000, 3000): 1 event
    {
        let event = create_test_event(2200, 1);
        let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
        operator.process(&event, &mut ctx);
    }

    // Fire window 1: [0, 1000)
    let win_timer_1 = Timer {
        key: WindowId::new(0, 1000).to_key(),
        timestamp: 1000,
    };
    let out1 = {
        let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
        operator.on_timer(win_timer_1, &mut ctx)
    };
    assert_eq!(out1.len(), 1);
    if let Output::Event(e) = &out1[0] {
        let result = e
            .data
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(result.value(0), 3, "Window [0,1000) should have count=3");
    } else {
        panic!("Expected Event output for window 1");
    }

    // Fire window 2: [1000, 2000)
    let win_timer_2 = Timer {
        key: WindowId::new(1000, 2000).to_key(),
        timestamp: 2000,
    };
    let out2 = {
        let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
        operator.on_timer(win_timer_2, &mut ctx)
    };
    assert_eq!(out2.len(), 1);
    if let Output::Event(e) = &out2[0] {
        let result = e
            .data
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(result.value(0), 2, "Window [1000,2000) should have count=2");
    } else {
        panic!("Expected Event output for window 2");
    }

    // Fire window 3: [2000, 3000)
    let win_timer_3 = Timer {
        key: WindowId::new(2000, 3000).to_key(),
        timestamp: 3000,
    };
    let out3 = {
        let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
        operator.on_timer(win_timer_3, &mut ctx)
    };
    assert_eq!(out3.len(), 1);
    if let Output::Event(e) = &out3[0] {
        let result = e
            .data
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(result.value(0), 1, "Window [2000,3000) should have count=1");
    } else {
        panic!("Expected Event output for window 3");
    }

    // All windows purged
    assert!(operator.registered_windows.is_empty());
}

#[test]
fn test_eowc_tumbling_late_data_dropped_after_close() {
    // After window closes and emits, late data should be dropped.
    let assigner = TumblingWindowAssigner::from_millis(1000);
    let aggregator = CountAggregator::new();
    let mut operator = TumblingWindowOperator::with_id(
        assigner,
        aggregator,
        Duration::from_millis(5000), // 5s allowed lateness
        "eowc_test".to_string(),
    );
    operator.set_emit_strategy(EmitStrategy::OnWindowClose);

    let mut timers = TimerService::new();
    let mut state = InMemoryStore::new();
    let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(0);

    // Fill window [0, 1000) with 3 events
    for ts in [100, 400, 700] {
        let event = create_test_event(ts, 1);
        let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
        operator.process(&event, &mut ctx);
    }

    // Fire timer at window_end + allowed_lateness = 1000 + 5000 = 6000
    let timer = Timer {
        key: WindowId::new(0, 1000).to_key(),
        timestamp: 6000,
    };
    let emit_out = {
        let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
        operator.on_timer(timer, &mut ctx)
    };
    assert_eq!(emit_out.len(), 1, "Should emit exactly once");
    assert!(matches!(&emit_out[0], Output::Event(_)));

    // Now advance watermark well past the window close point
    let advance_event = create_test_event(7000, 1);
    {
        let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
        operator.process(&advance_event, &mut ctx);
    }

    // Send late event for the closed window [0, 1000)
    let late_event = create_test_event(500, 99);
    let late_outputs = {
        let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
        operator.process(&late_event, &mut ctx)
    };

    // Should be classified as late (LateEvent output)
    let is_late = late_outputs
        .iter()
        .any(|o| matches!(o, Output::LateEvent(_)));
    assert!(
        is_late,
        "Late event after EOWC close should produce LateEvent output"
    );

    // Should NOT produce a new Event (window already closed)
    let is_event = late_outputs.iter().any(|o| matches!(o, Output::Event(_)));
    assert!(
        !is_event,
        "Late event after EOWC close must not produce a new Event"
    );

    assert_eq!(operator.late_data_metrics().late_events_dropped(), 1);
}

#[test]
fn test_eowc_tumbling_late_data_side_output() {
    // Same as above but with side output configured.
    let assigner = TumblingWindowAssigner::from_millis(1000);
    let aggregator = CountAggregator::new();
    let mut operator = TumblingWindowOperator::with_id(
        assigner,
        aggregator,
        Duration::from_millis(0),
        "eowc_test".to_string(),
    );
    operator.set_emit_strategy(EmitStrategy::OnWindowClose);
    operator.set_late_data_config(LateDataConfig::with_side_output("late_trades".to_string()));

    let mut timers = TimerService::new();
    let mut state = InMemoryStore::new();
    let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(0);

    // Fill window [0, 1000)
    let event = create_test_event(500, 1);
    {
        let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
        operator.process(&event, &mut ctx);
    }

    // Fire timer
    let timer = Timer {
        key: WindowId::new(0, 1000).to_key(),
        timestamp: 1000,
    };
    {
        let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
        operator.on_timer(timer, &mut ctx);
    }

    // Advance watermark
    let advance = create_test_event(2000, 1);
    {
        let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
        operator.process(&advance, &mut ctx);
    }

    // Send late event
    let late_event = create_test_event(300, 99);
    let outputs = {
        let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
        operator.process(&late_event, &mut ctx)
    };

    // Should be routed to side output
    let side_output = outputs.iter().find_map(|o| {
        if let Output::SideOutput { name, .. } = o {
            Some(name.clone())
        } else {
            None
        }
    });
    assert_eq!(
        side_output,
        Some("late_trades".to_string()),
        "Late event should be routed to side output"
    );
    assert_eq!(operator.late_data_metrics().late_events_side_output(), 1);
}

#[test]
fn test_eowc_tumbling_empty_window_no_emission() {
    // Windows that receive no events produce no output when timer fires.
    let assigner = TumblingWindowAssigner::from_millis(1000);
    let aggregator = CountAggregator::new();
    let mut operator = TumblingWindowOperator::with_id(
        assigner,
        aggregator,
        Duration::from_millis(0),
        "eowc_test".to_string(),
    );
    operator.set_emit_strategy(EmitStrategy::OnWindowClose);

    let mut timers = TimerService::new();
    let mut state = InMemoryStore::new();
    let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(0);

    // Fire timer for a window that was never populated
    let timer = Timer {
        key: WindowId::new(0, 1000).to_key(),
        timestamp: 1000,
    };
    let outputs = {
        let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
        operator.on_timer(timer, &mut ctx)
    };

    assert!(
        outputs.is_empty(),
        "Empty window should produce no output on timer fire"
    );
}

#[test]
fn test_eowc_tumbling_checkpoint_restore_then_emit() {
    // Accumulate events, checkpoint, restore, then verify correct emission.
    let assigner = TumblingWindowAssigner::from_millis(1000);
    let aggregator = CountAggregator::new();
    let mut operator = TumblingWindowOperator::with_id(
        assigner.clone(),
        aggregator.clone(),
        Duration::from_millis(0),
        "eowc_test".to_string(),
    );
    operator.set_emit_strategy(EmitStrategy::OnWindowClose);

    let mut timers = TimerService::new();
    let mut state = InMemoryStore::new();
    let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(0);

    // Process events into window [0, 1000)
    for ts in [100, 300, 600] {
        let event = create_test_event(ts, 1);
        let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
        operator.process(&event, &mut ctx);
    }

    // Checkpoint
    let checkpoint = operator.checkpoint();

    // Create a new operator and restore
    let mut restored = TumblingWindowOperator::with_id(
        assigner,
        aggregator,
        Duration::from_millis(0),
        "eowc_test".to_string(),
    );
    restored.set_emit_strategy(EmitStrategy::OnWindowClose);
    restored.restore(checkpoint).unwrap();

    assert_eq!(
        restored.registered_windows.len(),
        1,
        "Should have 1 registered window after restore"
    );

    // Fire the timer on the restored operator
    let timer = Timer {
        key: WindowId::new(0, 1000).to_key(),
        timestamp: 1000,
    };
    let outputs = {
        let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
        restored.on_timer(timer, &mut ctx)
    };

    assert_eq!(outputs.len(), 1, "Restored operator should emit once");
    if let Output::Event(e) = &outputs[0] {
        let result = e
            .data
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(result.value(0), 3, "Restored window should have count=3");
    } else {
        panic!("Expected Event output after restore");
    }

    assert!(restored.registered_windows.is_empty());
}

#[test]
fn test_eowc_vs_on_watermark_same_result() {
    // Property: given the same events, OnWindowClose and OnWatermark
    // should produce the same final result for single-emission cases.
    let events: Vec<(i64, i64)> = vec![(100, 5), (300, 10), (500, 15), (800, 20)];

    let mut results = Vec::new();
    for strategy in [EmitStrategy::OnWindowClose, EmitStrategy::OnWatermark] {
        let assigner = TumblingWindowAssigner::from_millis(1000);
        let aggregator = SumAggregator::new(0);
        let mut operator = TumblingWindowOperator::with_id(
            assigner,
            aggregator,
            Duration::from_millis(0),
            "compare_test".to_string(),
        );
        operator.set_emit_strategy(strategy.clone());

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(0);

        for &(ts, val) in &events {
            let event = create_test_event(ts, val);
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process(&event, &mut ctx);
        }

        // Fire the timer
        let timer = Timer {
            key: WindowId::new(0, 1000).to_key(),
            timestamp: 1000,
        };
        let outputs = {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.on_timer(timer, &mut ctx)
        };

        assert_eq!(outputs.len(), 1, "Both strategies should emit once");
        if let Output::Event(e) = &outputs[0] {
            let result = e
                .data
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            results.push(result.value(0));
        } else {
            panic!("Expected Event output for {strategy:?}");
        }
    }

    assert_eq!(
        results[0], results[1],
        "OnWindowClose and OnWatermark should produce same final result: \
         got {}, {}",
        results[0], results[1]
    );
    assert_eq!(results[0], 50, "Sum should be 5+10+15+20=50");
}
