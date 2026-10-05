//! Controlled connector effects for the exact-root installation fixture.

use super::*;
use laminar_connectors::connector::{SourcePosition, SourceRowPositions};

#[derive(Default)]
pub(super) struct InstallationProbe {
    pub allow_parent_initial: std::sync::atomic::AtomicBool,
    pub reject_initialized: std::sync::atomic::AtomicBool,
    pub fail_start: std::sync::atomic::AtomicBool,
    pub block_start: std::sync::atomic::AtomicBool,
    pub start_entered: tokio::sync::Notify,
    pub start_release: tokio::sync::Notify,
    pub starts: parking_lot::Mutex<Vec<(String, SourcePosition)>>,
    pub polls: AtomicUsize,
    pub controls: AtomicUsize,
    pub acknowledgements: parking_lot::Mutex<Vec<(String, u64)>>,
    pub source_closes: AtomicUsize,
    pub sink_closes: AtomicUsize,
    pub sink_opens: AtomicUsize,
    pub sink_epochs: AtomicUsize,
    pub output: parking_lot::Mutex<Vec<(String, RecordBatch)>>,
    pub input: parking_lot::Mutex<HashMap<String, std::collections::VecDeque<SourceBatch>>>,
}

pub(super) struct RuntimeSource {
    pub probe: Arc<InstallationProbe>,
    pub name: String,
    pub checkpoint: SourceCheckpoint,
    pub assignment: Option<std::num::NonZeroU64>,
}

impl RuntimeSource {
    pub fn new(probe: Arc<InstallationProbe>) -> Self {
        Self {
            probe,
            name: String::new(),
            checkpoint: SourceCheckpoint::new(),
            assignment: None,
        }
    }

    pub async fn start(&mut self, request: SourceStart) -> Result<(), ConnectorError> {
        let (_, position, _) = request.into_parts();
        self.probe
            .starts
            .lock()
            .push((self.name.clone(), position.clone()));
        self.probe.start_entered.notify_one();
        if self.probe.block_start.load(Ordering::Acquire) {
            self.probe.start_release.notified().await;
        }
        if self.probe.fail_start.load(Ordering::Acquire) {
            return Err(ConnectorError::ConfigurationError(
                "injected atomic start failure".into(),
            ));
        }
        self.checkpoint = match position {
            SourcePosition::Resume { checkpoint, .. }
            | SourcePosition::Initialized { checkpoint } => checkpoint,
            SourcePosition::Initial => {
                if self.name != "trades" || !self.probe.allow_parent_initial.load(Ordering::Acquire)
                {
                    return Err(ConnectorError::ConfigurationError(
                        "target must use its exact sealed cut".into(),
                    ));
                }
                let mut checkpoint = SourceCheckpoint::with_offsets(HashMap::from([(
                    "old.cursor".into(),
                    "0".into(),
                )]));
                checkpoint.set_metadata("connector", "planning-source");
                checkpoint.set_input_channels(vec![vec![1]])?;
                checkpoint
            }
        };
        if let Some(version) = self.assignment {
            self.checkpoint.bind_assignment_version(version);
        }
        if self.checkpoint.input_channels().is_none() {
            self.checkpoint.set_input_channels(vec![vec![1]])?;
        }
        Ok(())
    }

    pub fn poll(&mut self) -> Result<Option<SourceBatch>, ConnectorError> {
        self.probe.polls.fetch_add(1, Ordering::AcqRel);
        let batch = self
            .probe
            .input
            .lock()
            .get_mut(&self.name)
            .and_then(std::collections::VecDeque::pop_front);
        Ok(batch.map(|batch| {
            let key = if self.checkpoint.get_offset("old.cursor").is_some() {
                "old.cursor"
            } else {
                "partition-0-next"
            };
            let next = self
                .checkpoint
                .get_offset(key)
                .unwrap()
                .parse::<u64>()
                .unwrap()
                + batch.records.num_rows() as u64;
            self.checkpoint.set_offset(key, next.to_string());
            batch.with_checkpoint(self.checkpoint.clone())
        }))
    }
}

pub(super) struct RuntimeSink {
    pub probe: Arc<InstallationProbe>,
    topic: String,
}

impl RuntimeSink {
    pub fn new(probe: Arc<InstallationProbe>) -> Self {
        Self {
            probe,
            topic: String::new(),
        }
    }

    pub fn open(&mut self, config: &ConnectorConfig) -> Result<(), ConnectorError> {
        self.topic = config.require("topic")?.to_owned();
        self.probe.sink_opens.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }

    pub fn write(&self, batch: &RecordBatch) -> Result<WriteResult, ConnectorError> {
        self.probe
            .output
            .lock()
            .push((self.topic.clone(), batch.clone()));
        Ok(WriteResult::new(batch.num_rows(), 0))
    }
}

pub(super) fn positioned(batch: RecordBatch, first: u64) -> SourceBatch {
    positioned_in_channel(batch, first, &[1])
}

pub(super) fn positioned_in_channel(batch: RecordBatch, first: u64, channel: &[u8]) -> SourceBatch {
    let rows = batch.num_rows();
    let orders = (first..first + rows as u64)
        .map(u64::to_be_bytes)
        .collect::<Vec<_>>();
    SourceBatch::positioned(
        batch,
        SourceRowPositions::try_new(
            arrow::array::BinaryArray::from(vec![channel; rows]),
            arrow::array::BinaryArray::from(
                orders.iter().map(<[u8; 8]>::as_slice).collect::<Vec<_>>(),
            ),
            arrow::array::UInt32Array::from(vec![0; rows]),
        )
        .unwrap(),
    )
    .unwrap()
}
