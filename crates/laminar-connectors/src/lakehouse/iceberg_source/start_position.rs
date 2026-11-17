//! The position `start` installs: decoded from the start request before any catalog I/O, then
//! installed once the table is loaded and the read schema is bound.

use iceberg::table::Table;

use super::cursor::IcebergSourceCursorV1;
use super::split::SplitCut;
use super::IcebergSource;
use crate::checkpoint::SourceCheckpoint;
use crate::connector::SourcePosition;
use crate::error::ConnectorError;

pub(super) enum StartPosition {
    Initial,
    Cursor(IcebergSourceCursorV1),
    Split(SplitCut),
}

impl StartPosition {
    /// The cursor whose retained schema the source binds: the resumed one, or a channel's.
    pub fn schema_cursor(
        &self,
        table: &Table,
    ) -> Result<Option<IcebergSourceCursorV1>, ConnectorError> {
        match self {
            Self::Initial => Ok(None),
            Self::Cursor(cursor) => Ok(Some(cursor.clone())),
            Self::Split(cut) => cut
                .binding
                .channel_cursor(&cut.positions[0], table)
                .map(Some),
        }
    }
}

impl IcebergSource {
    /// Forget the read position of an earlier run on this instance.
    pub(super) fn reset_read_state(&mut self) {
        let mut checkpoint = SourceCheckpoint::new();
        checkpoint.set_metadata("connector_type", "iceberg");
        self.checkpoint = checkpoint;
        self.cursor = None;
        self.split = None;
        self.last_poll_time = None;
        self.bounded_snapshot_complete = false;
        self.replay_unit_in_progress = false;
        self.scan_failed = false;
        self.set_pending_snapshots(0);
    }

    pub(super) fn decode_start(
        &self,
        position: &SourcePosition,
        source_name: &str,
    ) -> Result<StartPosition, ConnectorError> {
        if self.assignment.is_some() && self.config.splits.is_none() {
            return Err(ConnectorError::ConfigurationError(
                "an Iceberg source without read.splits cannot take a cluster vnode assignment"
                    .into(),
            ));
        }
        match (position, self.config.splits) {
            (SourcePosition::Initial, _) => Ok(StartPosition::Initial),
            (SourcePosition::Initialized { .. }, _) => Err(ConnectorError::ConfigurationError(
                "Iceberg has no sealed topology startup contract".into(),
            )),
            (SourcePosition::Resume { checkpoint, .. }, None) => Ok(StartPosition::Cursor(
                IcebergSourceCursorV1::from_checkpoint(checkpoint)?,
            )),
            (SourcePosition::Resume { checkpoint, .. }, Some(splits)) => Ok(StartPosition::Split(
                SplitCut::from_resume_checkpoint(checkpoint, source_name, splits)?,
            )),
        }
    }

    pub(super) fn install_start(
        &mut self,
        start: StartPosition,
        source_name: &str,
    ) -> Result<(), ConnectorError> {
        match start {
            StartPosition::Initial if self.config.splits.is_some() => {
                self.start_split(source_name, None)
            }
            StartPosition::Initial => self.start_initial_scan(),
            StartPosition::Cursor(cursor) => self.install_cursor(cursor),
            StartPosition::Split(cut) => self.start_split(source_name, Some(cut)),
        }
    }
}
