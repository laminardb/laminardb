//! Sink task actor: one detached task per sink owning command execution,
//! epoch gating, write ordering, and close/terminal lifecycle.
//!
//! Child modules: `protocol` (commands, events, config, epoch-gate types),
//! `operation` (bounded connector awaits + deadline errors), `actor`
//! (admission/close/terminal state and supervision), `handle` (the public
//! `SinkTaskHandle`), `close` (close driver), `lifecycle` (run loops), and
//! `commands` (command dispatch, epoch transitions, write paths).

mod actor;
mod close;
mod commands;
mod handle;
mod lifecycle;
mod operation;
mod protocol;

pub(crate) use handle::SinkTaskHandle;
pub(crate) use protocol::{
    SinkEpochAdmission, SinkEvent, SinkTaskConfig, DEFAULT_CHANNEL_CAPACITY,
    SINK_EVENT_CHANNEL_CAPACITY,
};

#[cfg(test)]
pub(crate) use protocol::DEFAULT_FLUSH_INTERVAL;

#[cfg(test)]
mod tests;
