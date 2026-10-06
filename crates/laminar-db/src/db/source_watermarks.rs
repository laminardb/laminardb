//! Physical source progress and replay-bound watermark cuts.

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Array, BinaryArray, RecordBatch};

#[derive(Clone, Copy, PartialEq, Eq)]
enum WatermarkCutMode {
    Arrival,
    FixedBatch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RecoveredInputChannelProgress {
    pub(crate) watermark: Option<i64>,
    pub(crate) idle: bool,
}

pub(crate) struct InputChannelProgress {
    pub(crate) input_channel: Vec<u8>,
    pub(crate) watermark: Option<i64>,
    pub(crate) idle: bool,
}

struct InputChannelWatermark {
    generator: laminar_core::time::BoundedOutOfOrdernessGenerator,
    idle: bool,
    last_activity: Instant,
}

struct PartitionedSourceWatermarks {
    max_out_of_orderness_ms: i64,
    max_future_skew_ms: i64,
    idle_timeout: Option<Duration>,
    inventory: Option<Arc<[Vec<u8>]>>,
    channels: rustc_hash::FxHashMap<Box<[u8]>, InputChannelWatermark>,
    recovered: rustc_hash::FxHashMap<Box<[u8]>, RecoveredInputChannelProgress>,
    recovered_inventory: Option<Arc<[Vec<u8>]>>,
    external_floor: i64,
}

impl PartitionedSourceWatermarks {
    fn channel(
        &self,
        recovered: Option<RecoveredInputChannelProgress>,
        admission_floor: i64,
    ) -> InputChannelWatermark {
        let mut generator =
            laminar_core::time::BoundedOutOfOrdernessGenerator::new(self.max_out_of_orderness_ms)
                .with_max_future_skew(self.max_future_skew_ms);
        let watermark = recovered
            .and_then(|progress| progress.watermark)
            .map_or(admission_floor, |watermark| watermark.max(admission_floor));
        if watermark > i64::MIN {
            laminar_core::time::WatermarkGenerator::restore_watermark_for_recovery(
                &mut generator,
                watermark,
            );
        }
        InputChannelWatermark {
            generator,
            idle: recovered.is_some_and(|progress| progress.idle),
            last_activity: Instant::now(),
        }
    }

    fn effective_watermark(&self, channel: &InputChannelWatermark) -> i64 {
        laminar_core::time::WatermarkGenerator::current_watermark(&channel.generator)
            .max(self.external_floor)
    }

    fn frontier(&self) -> i64 {
        if self.channels.is_empty() {
            return i64::MIN;
        }
        let mut active = false;
        let mut active_min = i64::MAX;
        let mut idle_max = i64::MIN;
        for channel in self.channels.values() {
            let watermark = self.effective_watermark(channel);
            idle_max = idle_max.max(watermark);
            if !channel.idle {
                active = true;
                active_min = active_min.min(watermark);
            }
        }
        if active {
            active_min
        } else {
            idle_max
        }
    }

    fn all_idle(&self) -> bool {
        self.inventory
            .as_ref()
            .is_some_and(|_| self.channels.values().all(|channel| channel.idle))
    }
}

pub(crate) struct SourceWatermarkState {
    pub(crate) extractor: laminar_core::time::EventTimeExtractor,
    pub(crate) generator: Box<dyn laminar_core::time::WatermarkGenerator>,
    pub(crate) column: String,
    partitioned: Option<PartitionedSourceWatermarks>,
    cut_mode: WatermarkCutMode,
}

impl SourceWatermarkState {
    pub(crate) fn new(
        extractor: laminar_core::time::EventTimeExtractor,
        generator: Box<dyn laminar_core::time::WatermarkGenerator>,
        column: String,
    ) -> Self {
        Self {
            extractor,
            generator,
            column,
            partitioned: None,
            cut_mode: WatermarkCutMode::Arrival,
        }
    }

    pub(crate) fn install_replay_batch_cuts(
        &mut self,
        out_of_orderness: Duration,
    ) -> Result<(), String> {
        use laminar_core::time::{BoundedOutOfOrdernessGenerator, WatermarkGenerator as _};

        let state = self.partitioned.as_ref().ok_or_else(|| {
            "fixed replay batches require physical input-channel progress".to_string()
        })?;
        validate_replay_channel_inventory(state, state.inventory.as_deref().unwrap_or(&[]))?;
        // RECOVERY: preserve the committed cut while removing clock-dependent advancement.
        let watermark = self.generator.current_watermark();
        let mut generator =
            BoundedOutOfOrdernessGenerator::from_duration(out_of_orderness).with_max_future_skew(0);
        generator.restore_watermark_for_recovery(watermark);
        self.generator = Box::new(generator);
        self.cut_mode = WatermarkCutMode::FixedBatch;
        if let Some(state) = self.partitioned.as_mut() {
            state.max_future_skew_ms = 0;
            state.idle_timeout = None;
            for channel in state.channels.values_mut() {
                let watermark = channel.generator.current_watermark();
                channel.generator =
                    BoundedOutOfOrdernessGenerator::new(state.max_out_of_orderness_ms)
                        .with_max_future_skew(0);
                channel.generator.restore_watermark_for_recovery(watermark);
            }
        }
        Ok(())
    }

    pub(crate) fn with_input_channels(
        mut self,
        max_out_of_orderness: Duration,
        max_future_skew_ms: i64,
        idle_timeout: Option<Duration>,
        recovered: rustc_hash::FxHashMap<Box<[u8]>, RecoveredInputChannelProgress>,
        recovered_inventory: Option<Arc<[Vec<u8>]>>,
    ) -> Self {
        self.partitioned = Some(PartitionedSourceWatermarks {
            max_out_of_orderness_ms: i64::try_from(max_out_of_orderness.as_millis())
                .unwrap_or(i64::MAX),
            max_future_skew_ms,
            idle_timeout,
            inventory: None,
            channels: rustc_hash::FxHashMap::default(),
            recovered,
            recovered_inventory,
            external_floor: i64::MIN,
        });
        self
    }

    pub(crate) const fn is_partitioned(&self) -> bool {
        self.partitioned.is_some()
    }

    pub(crate) fn input_channels_all_idle(&self) -> Option<bool> {
        self.partitioned
            .as_ref()
            .map(PartitionedSourceWatermarks::all_idle)
    }

    /// Install a trusted, durable source-decision floor without wall-clock skew rejection.
    ///
    /// Unlike checkpoint restore this is monotonic: a live source can only advance. The normal
    /// `advance_watermark` path intentionally rejects implausibly future event timestamps, but a
    /// committed cluster cut may legitimately originate from a peer clock and must be reproduced
    /// exactly by both aggregate and per-channel checkpoint state.
    pub(crate) fn install_committed_watermark_floor(&mut self, watermark: i64) -> Option<i64> {
        if watermark == i64::MIN {
            return None;
        }
        if let Some(state) = self.partitioned.as_mut() {
            state.external_floor = state.external_floor.max(watermark);
        }
        if watermark <= self.generator.current_watermark() {
            return None;
        }
        self.generator.restore_watermark_for_recovery(watermark);
        Some(watermark)
    }

    pub(crate) fn install_input_channels(
        &mut self,
        inventory: Option<Arc<[Vec<u8>]>>,
        admission_floor: i64,
    ) -> Result<bool, String> {
        let activation_floor = admission_floor.max(self.generator.current_watermark());
        let Some(state) = self.partitioned.as_mut() else {
            return Ok(false);
        };
        let inventory = inventory.ok_or_else(|| {
            "ordered event-time source checkpoint omitted its input-channel inventory".to_string()
        })?;
        if inventory.iter().any(Vec::is_empty)
            || !inventory.windows(2).all(|pair| pair[0] < pair[1])
        {
            return Err(
                "input-channel inventory must contain non-empty, strictly ordered identities"
                    .into(),
            );
        }
        if self.cut_mode == WatermarkCutMode::FixedBatch {
            validate_replay_channel_inventory(state, &inventory)?;
        }
        if state.inventory.as_ref().is_some_and(|installed| {
            Arc::ptr_eq(installed, &inventory) || installed.as_ref() == inventory.as_ref()
        }) {
            return Ok(false);
        }

        let initial_install = state.inventory.is_none();
        if initial_install {
            let recovered_expected = state.recovered_inventory.as_deref();
            if let Some(input_channel) = inventory.iter().find(|input_channel| {
                !state.recovered.contains_key(input_channel.as_slice())
                    && recovered_expected.is_some_and(|expected| {
                        expected
                            .binary_search_by(|candidate| candidate.as_slice().cmp(input_channel))
                            .is_ok()
                    })
            }) {
                return Err(format!(
                    "recovered input channel {input_channel:02x?} has no committed watermark progress"
                ));
            }
        }
        let mut previous = std::mem::take(&mut state.channels);
        let mut channels = rustc_hash::FxHashMap::with_capacity_and_hasher(
            inventory.len(),
            rustc_hash::FxBuildHasher,
        );
        for input_channel in inventory.iter() {
            if let Some(channel) = previous.remove(input_channel.as_slice()) {
                channels.insert(input_channel.clone().into_boxed_slice(), channel);
                continue;
            }
            let recovered = if initial_install {
                state.recovered.remove(input_channel.as_slice())
            } else {
                None
            };
            channels.insert(
                input_channel.clone().into_boxed_slice(),
                state.channel(recovered, activation_floor),
            );
        }
        if initial_install {
            state.recovered.clear();
            if self.cut_mode == WatermarkCutMode::Arrival {
                state.recovered_inventory = None;
            }
        }
        if self.cut_mode == WatermarkCutMode::FixedBatch
            && !inventory.is_empty()
            && state
                .recovered_inventory
                .as_ref()
                .is_none_or(|cut| cut.is_empty())
        {
            // INVARIANT: retain the physical identity through an empty owned inventory.
            state.recovered_inventory = Some(Arc::clone(&inventory));
        }
        state.inventory = Some(inventory);
        state.channels = channels;
        self.generator.advance_watermark(state.frontier());
        Ok(true)
    }

    pub(crate) fn observe_input_channels(
        &mut self,
        batch: &RecordBatch,
        admission_floor: i64,
    ) -> Result<Option<i64>, String> {
        let Some(state) = self.partitioned.as_mut() else {
            let floor = self.generator.advance_watermark(admission_floor);
            let event = match self.extractor.extract(batch) {
                Ok(timestamp) => self.generator.on_event(timestamp).map(|wm| wm.timestamp()),
                Err(laminar_core::time::EventTimeError::NullTimestamp { .. }) => None,
                Err(error) => return Err(error.to_string()),
            };
            return Ok(event.or_else(|| floor.map(|watermark| watermark.timestamp())));
        };
        if state.inventory.is_none() {
            return Err("ordered event-time source emitted a batch before installing its input-channel inventory".into());
        }
        let timestamps = self
            .extractor
            .extract_millis_array(batch)
            .map_err(|error| error.to_string())?;
        let partitions = batch
            .column_by_name(laminar_connectors::connector::SOURCE_PARTITION_COLUMN)
            .ok_or_else(|| "ordered event-time batch omitted __source_partition".to_string())?
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| {
                "ordered event-time batch __source_partition must be Binary".to_string()
            })?;
        if partitions.len() != timestamps.len() {
            return Err("event-time and input-channel column lengths differ".into());
        }

        let observed_at = Instant::now();
        let activation_floor = admission_floor.max(self.generator.current_watermark());
        for row in 0..timestamps.len() {
            if partitions.is_null(row) {
                return Err(format!("null input-channel identity at row {row}"));
            }
            let input_channel = partitions.value(row);
            let channel = state.channels.get_mut(input_channel).ok_or_else(|| {
                format!("row {row} references an input channel outside the installed inventory")
            })?;
            if channel.idle {
                laminar_core::time::WatermarkGenerator::advance_watermark(
                    &mut channel.generator,
                    activation_floor,
                );
            }
            channel.idle = false;
            channel.last_activity = observed_at;
            if !timestamps.is_null(row) {
                laminar_core::time::WatermarkGenerator::on_event(
                    &mut channel.generator,
                    timestamps.value(row),
                );
            }
        }
        let frontier = state.frontier();
        Ok(self
            .generator
            .advance_watermark(frontier)
            .map(|watermark| watermark.timestamp()))
    }

    pub(crate) fn advance_external_watermark(&mut self, watermark: i64) -> Option<i64> {
        if self.cut_mode == WatermarkCutMode::FixedBatch {
            return None;
        }
        if let Some(state) = self.partitioned.as_mut() {
            let candidate_floor = state.external_floor.max(watermark);
            let frontier = state.frontier().max(candidate_floor);
            let current = self.generator.current_watermark();
            let advanced = self.generator.advance_watermark(frontier);
            if frontier > current && advanced.is_none() {
                return None;
            }
            state.external_floor = candidate_floor;
            advanced.map(|watermark| watermark.timestamp())
        } else {
            self.generator
                .advance_watermark(watermark)
                .map(|watermark| watermark.timestamp())
        }
    }

    pub(crate) fn tick_input_channel_idleness(&mut self) -> (Option<i64>, bool) {
        if self.cut_mode == WatermarkCutMode::FixedBatch {
            return (None, self.input_channels_all_idle().unwrap_or(false));
        }
        let Some(state) = self.partitioned.as_mut() else {
            return (
                self.generator
                    .on_periodic()
                    .map(|watermark| watermark.timestamp()),
                false,
            );
        };
        let now = Instant::now();
        let external_floor = state.external_floor;
        let mut has_channel = false;
        let mut has_active = false;
        let mut active_min = i64::MAX;
        let mut idle_max = i64::MIN;
        for channel in state.channels.values_mut() {
            has_channel = true;
            if !channel.idle
                && state.idle_timeout.is_some_and(|timeout| {
                    now.saturating_duration_since(channel.last_activity) >= timeout
                })
            {
                channel.idle = true;
            }
            let watermark =
                laminar_core::time::WatermarkGenerator::current_watermark(&channel.generator)
                    .max(external_floor);
            idle_max = idle_max.max(watermark);
            if !channel.idle {
                has_active = true;
                active_min = active_min.min(watermark);
            }
        }
        let frontier = if !has_channel {
            i64::MIN
        } else if has_active {
            active_min
        } else {
            idle_max
        };
        (
            self.generator
                .advance_watermark(frontier)
                .map(|watermark| watermark.timestamp()),
            state.inventory.is_some() && !has_active,
        )
    }

    pub(crate) fn input_channel_progress(
        &self,
    ) -> Result<Option<Vec<InputChannelProgress>>, String> {
        let Some(state) = self.partitioned.as_ref() else {
            return Ok(None);
        };
        let inventory = state.inventory.as_ref().ok_or_else(|| {
            "ordered event-time source has no installed input-channel inventory".to_string()
        })?;
        let mut progress = Vec::with_capacity(inventory.len());
        for input_channel in inventory.iter() {
            let channel = state
                .channels
                .get(input_channel.as_slice())
                .ok_or_else(|| {
                    "installed input-channel inventory and watermark state diverged".to_string()
                })?;
            let watermark = state.effective_watermark(channel);
            progress.push(InputChannelProgress {
                input_channel: input_channel.clone(),
                watermark: (watermark > i64::MIN).then_some(watermark),
                idle: channel.idle,
            });
        }
        Ok(Some(progress))
    }
}

fn validate_replay_channel_inventory(
    state: &PartitionedSourceWatermarks,
    inventory: &[Vec<u8>],
) -> Result<(), String> {
    if inventory.len() > 1 || state.recovered.len() > 1 {
        return Err("fixed replay batches require one global physical input channel".into());
    }
    if let Some(expected) = state.recovered_inventory.as_deref() {
        if expected.len() > 1 {
            return Err("fixed replay checkpoint contains multiple physical input channels".into());
        }
        if !expected.is_empty() && !inventory.is_empty() && expected != inventory {
            return Err("fixed replay source changed its physical input-channel identity".into());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use laminar_connectors::connector::{
        schema_with_source_mutations_and_row_positions, schema_with_source_row_positions,
        SourceBatch, SourceRowPositionCapability, SourceRowPositions,
    };

    use super::*;
    use crate::db::filter_late_rows;
    use arrow::datatypes::{DataType, Field, Schema};

    fn state(
        recovered: rustc_hash::FxHashMap<Box<[u8]>, RecoveredInputChannelProgress>,
        inventory: Option<Arc<[Vec<u8>]>>,
    ) -> SourceWatermarkState {
        SourceWatermarkState::new(
            laminar_core::time::EventTimeExtractor::from_column("ts"),
            Box::new(laminar_core::time::BoundedOutOfOrdernessGenerator::new(0)),
            "ts".into(),
        )
        .with_input_channels(
            Duration::ZERO,
            300_000,
            Some(Duration::ZERO),
            recovered,
            inventory,
        )
    }

    fn batch(timestamp_ms: i64) -> RecordBatch {
        let records =
            crate::process_function::tests::input_batch(&[("a", 1, timestamp_ms * 1_000)]);
        let positioned = schema_with_source_row_positions(&records.schema()).unwrap();
        let mutations = schema_with_source_mutations_and_row_positions(&records.schema()).unwrap();
        SourceBatch::positioned(
            records,
            SourceRowPositions::try_new(
                BinaryArray::from_vec(vec![b"ordered"]),
                BinaryArray::from_vec(vec![b"1"]),
                arrow::array::UInt32Array::from(vec![0]),
            )
            .unwrap(),
        )
        .unwrap()
        .into_records_with_metadata(
            SourceRowPositionCapability::OrderedDeterministic,
            &positioned,
            &mutations,
        )
        .unwrap()
    }

    #[test]
    fn fixed_batch_cuts_exclude_wall_clock_idleness_and_external_watermarks() {
        let mut state = state(rustc_hash::FxHashMap::default(), None);
        state.install_replay_batch_cuts(Duration::ZERO).unwrap();
        state
            .install_input_channels(Some(Arc::from([b"ordered".to_vec()])), i64::MIN)
            .unwrap();
        let timestamp = laminar_core::time::now_unix_millis() + 86_400_000;
        assert_eq!(
            state
                .observe_input_channels(&batch(timestamp), i64::MIN)
                .unwrap(),
            Some(timestamp)
        );
        assert_eq!(state.advance_external_watermark(timestamp + 1_000), None);
        assert_eq!(state.tick_input_channel_idleness(), (None, false));
        let progress = state.input_channel_progress().unwrap().unwrap();
        assert_eq!(progress[0].watermark, Some(timestamp));
        assert!(!progress[0].idle);
    }

    #[test]
    fn fixed_batch_channel_identity_survives_an_empty_owned_inventory() {
        let mut state = state(rustc_hash::FxHashMap::default(), None);
        state.install_replay_batch_cuts(Duration::ZERO).unwrap();
        let channel = Arc::from([b"ordered".to_vec()]);
        state
            .install_input_channels(Some(Arc::clone(&channel)), i64::MIN)
            .unwrap();
        state
            .install_input_channels(Some(Arc::from([])), i64::MIN)
            .unwrap();
        assert!(state
            .install_input_channels(Some(Arc::from([b"different".to_vec()])), i64::MIN)
            .is_err());
        assert!(state
            .install_input_channels(
                Some(Arc::from([b"ordered".to_vec(), b"other".to_vec()])),
                i64::MIN
            )
            .is_err());
        state
            .install_input_channels(Some(channel), i64::MIN)
            .unwrap();
    }

    #[test]
    fn fixed_batch_recovery_rejects_a_changed_channel_or_multiple_channel_cut() {
        let mut state = state(
            rustc_hash::FxHashMap::from_iter([(
                Box::from(&b"ordered"[..]),
                RecoveredInputChannelProgress {
                    watermark: Some(100),
                    idle: false,
                },
            )]),
            Some(Arc::from([b"ordered".to_vec()])),
        );
        state.generator.restore_watermark_for_recovery(100);
        state.install_replay_batch_cuts(Duration::ZERO).unwrap();
        assert_eq!(state.generator.current_watermark(), 100);
        assert!(state
            .install_input_channels(Some(Arc::from([b"different".to_vec()])), i64::MIN)
            .is_err());
        state.partitioned.as_mut().unwrap().recovered.insert(
            Box::from(&b"other"[..]),
            RecoveredInputChannelProgress {
                watermark: Some(100),
                idle: false,
            },
        );
        assert!(state.install_replay_batch_cuts(Duration::ZERO).is_err());
    }
    #[test]
    fn recovered_input_channels_hold_the_logical_watermark_during_skewed_replay() {
        use arrow::array::{BinaryArray, TimestampMillisecondArray};
        use laminar_core::time::{
            BoundedOutOfOrdernessGenerator, EventTimeExtractor, ExtractionMode,
        };

        let recovered_inventory: Arc<[Vec<u8>]> =
            Arc::from([b"fast".to_vec(), b"slow".to_vec(), b"stale".to_vec()]);
        let recovered = rustc_hash::FxHashMap::from_iter([
            (
                Box::<[u8]>::from(&b"slow"[..]),
                RecoveredInputChannelProgress {
                    watermark: Some(10),
                    idle: false,
                },
            ),
            (
                Box::<[u8]>::from(&b"fast"[..]),
                RecoveredInputChannelProgress {
                    watermark: Some(100),
                    idle: false,
                },
            ),
            (
                Box::<[u8]>::from(&b"stale"[..]),
                RecoveredInputChannelProgress {
                    watermark: Some(1_000),
                    idle: true,
                },
            ),
        ]);
        let mut state = SourceWatermarkState::new(
            EventTimeExtractor::from_column("ts").with_mode(ExtractionMode::Max),
            Box::new(BoundedOutOfOrdernessGenerator::new(0).with_max_future_skew(0)),
            "ts".into(),
        )
        .with_input_channels(
            Duration::ZERO,
            0,
            None,
            recovered,
            Some(recovered_inventory),
        );
        state
            .install_input_channels(
                Some(Arc::from([b"fast".to_vec(), b"slow".to_vec()])),
                i64::MIN,
            )
            .unwrap();
        let partitioned = state.partitioned.as_ref().unwrap();
        assert!(partitioned.recovered.is_empty());
        assert!(partitioned.recovered_inventory.is_none());

        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "ts",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
                false,
            ),
            Field::new(
                laminar_connectors::connector::SOURCE_PARTITION_COLUMN,
                DataType::Binary,
                false,
            ),
        ]));
        let batch = |timestamp, input_channel: &'static [u8]| {
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(TimestampMillisecondArray::from(vec![timestamp])),
                    Arc::new(BinaryArray::from(vec![input_channel])),
                ],
            )
            .unwrap()
        };

        state
            .observe_input_channels(&batch(200, b"fast"), 10)
            .unwrap();
        assert_eq!(state.generator.current_watermark(), 10);

        let slow = filter_late_rows(&batch(12, b"slow"), "ts", 10)
            .unwrap()
            .expect("the slow channel row is still on time");
        state.observe_input_channels(&slow, 10).unwrap();
        assert_eq!(state.generator.current_watermark(), 12);

        state
            .install_input_channels(Some(Arc::from([b"slow".to_vec()])), 1)
            .unwrap();
        let retained = state.input_channel_progress().unwrap().unwrap();
        assert_eq!(retained[0].watermark, Some(12));

        state
            .install_input_channels(
                Some(Arc::from([
                    b"fast".to_vec(),
                    b"slow".to_vec(),
                    b"stale".to_vec(),
                ])),
                1,
            )
            .unwrap();
        let reassigned = state.input_channel_progress().unwrap().unwrap();
        assert_eq!(reassigned[0].watermark, Some(12));
        assert_eq!(reassigned[1].watermark, Some(12));
        assert_eq!(reassigned[2].watermark, Some(12));
        assert!(!reassigned[2].idle);

        {
            let channels = &mut state.partitioned.as_mut().unwrap().channels;
            channels.get_mut(&b"slow"[..]).unwrap().idle = true;
            channels.get_mut(&b"stale"[..]).unwrap().idle = true;
        }
        state
            .observe_input_channels(&batch(200, b"fast"), 1)
            .unwrap();
        assert_eq!(state.generator.current_watermark(), 200);

        state
            .observe_input_channels(&batch(13, b"slow"), 1)
            .unwrap();
        let resumed = state.input_channel_progress().unwrap().unwrap();
        assert_eq!(resumed[1].watermark, Some(200));
        assert!(!resumed[1].idle);
    }

    #[test]
    fn null_event_time_does_not_advance_a_physical_input_channel() {
        use arrow::array::{BinaryArray, TimestampMillisecondArray};
        use laminar_core::time::{BoundedOutOfOrdernessGenerator, EventTimeExtractor};

        let mut state = SourceWatermarkState::new(
            EventTimeExtractor::from_column("ts"),
            Box::new(BoundedOutOfOrdernessGenerator::new(0).with_max_future_skew(0)),
            "ts".into(),
        )
        .with_input_channels(Duration::ZERO, 0, None, Default::default(), None);
        state
            .install_input_channels(Some(Arc::from([b"p0".to_vec()])), i64::MIN)
            .unwrap();

        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new(
                    "ts",
                    DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
                    true,
                ),
                Field::new(
                    laminar_connectors::connector::SOURCE_PARTITION_COLUMN,
                    DataType::Binary,
                    false,
                ),
            ])),
            vec![
                Arc::new(TimestampMillisecondArray::from(vec![None])),
                Arc::new(BinaryArray::from(vec![&b"p0"[..]])),
            ],
        )
        .unwrap();

        assert_eq!(
            state.observe_input_channels(&batch, i64::MIN).unwrap(),
            None
        );
        assert_eq!(state.generator.current_watermark(), i64::MIN);
        assert_eq!(
            state.input_channel_progress().unwrap().unwrap()[0].watermark,
            None
        );
    }

    #[test]
    fn recovered_input_channels_are_not_idle_before_inventory_reconciliation() {
        use laminar_core::time::{
            BoundedOutOfOrdernessGenerator, EventTimeExtractor, ExtractionMode,
        };

        let inventory: Arc<[Vec<u8>]> = Arc::from([b"slow".to_vec()]);
        let recovered = rustc_hash::FxHashMap::from_iter([(
            Box::<[u8]>::from(&b"slow"[..]),
            RecoveredInputChannelProgress {
                watermark: Some(10),
                idle: false,
            },
        )]);
        let mut state = SourceWatermarkState::new(
            EventTimeExtractor::from_column("ts").with_mode(ExtractionMode::Max),
            Box::new(BoundedOutOfOrdernessGenerator::new(0).with_max_future_skew(0)),
            "ts".into(),
        )
        .with_input_channels(
            Duration::ZERO,
            0,
            None,
            recovered,
            Some(Arc::clone(&inventory)),
        );
        state.generator.restore_watermark_for_recovery(10);
        assert_eq!(state.input_channels_all_idle(), Some(false));

        let mut tracker = laminar_core::time::WatermarkTracker::new(2);
        tracker.update_source(0, 10);
        tracker.update_source(1, 100);
        let advanced = tracker.update_source(0, state.generator.current_watermark());
        if state.input_channels_all_idle() == Some(true) {
            let _ = tracker.mark_idle(0).or(advanced);
        }
        assert_eq!(
            tracker
                .current_watermark()
                .map(|watermark| watermark.timestamp()),
            Some(10),
            "an uninstalled recovered source must hold the combined frontier"
        );

        state
            .install_input_channels(Some(Arc::from([])), 10)
            .unwrap();
        assert_eq!(state.input_channels_all_idle(), Some(true));
    }

    #[test]
    fn input_channel_idle_tick_keeps_active_minimum_and_idle_maximum_semantics() {
        use laminar_core::time::{BoundedOutOfOrdernessGenerator, EventTimeExtractor};

        let mut state = SourceWatermarkState::new(
            EventTimeExtractor::from_column("ts"),
            Box::new(BoundedOutOfOrdernessGenerator::new(0).with_max_future_skew(0)),
            "ts".into(),
        )
        .with_input_channels(Duration::ZERO, 0, None, Default::default(), None);
        state
            .install_input_channels(
                Some(Arc::from([b"fast".to_vec(), b"slow".to_vec()])),
                i64::MIN,
            )
            .unwrap();
        let channels = &mut state.partitioned.as_mut().unwrap().channels;
        laminar_core::time::WatermarkGenerator::restore_watermark_for_recovery(
            &mut channels.get_mut(&b"fast"[..]).unwrap().generator,
            20,
        );
        laminar_core::time::WatermarkGenerator::restore_watermark_for_recovery(
            &mut channels.get_mut(&b"slow"[..]).unwrap().generator,
            10,
        );

        assert_eq!(state.tick_input_channel_idleness(), (Some(10), false));
        state
            .partitioned
            .as_mut()
            .unwrap()
            .channels
            .get_mut(&b"slow"[..])
            .unwrap()
            .idle = true;
        assert_eq!(state.tick_input_channel_idleness(), (Some(20), false));
        state
            .partitioned
            .as_mut()
            .unwrap()
            .channels
            .get_mut(&b"fast"[..])
            .unwrap()
            .idle = true;
        assert_eq!(state.tick_input_channel_idleness(), (None, true));
    }

    #[test]
    fn rejected_external_watermark_is_not_checkpointed_as_a_partition_floor() {
        use laminar_core::time::{BoundedOutOfOrdernessGenerator, EventTimeExtractor};

        let mut state = SourceWatermarkState::new(
            EventTimeExtractor::from_column("ts"),
            Box::new(BoundedOutOfOrdernessGenerator::new(0).with_max_future_skew(1)),
            "ts".into(),
        )
        .with_input_channels(
            Duration::ZERO,
            1,
            None,
            rustc_hash::FxHashMap::default(),
            None,
        );
        state
            .install_input_channels(Some(Arc::from([b"partition".to_vec()])), i64::MIN)
            .unwrap();

        assert_eq!(state.advance_external_watermark(i64::MAX), None);
        assert_eq!(state.generator.current_watermark(), i64::MIN);
        assert_eq!(
            state.input_channel_progress().unwrap().unwrap()[0].watermark,
            None
        );
    }
}
