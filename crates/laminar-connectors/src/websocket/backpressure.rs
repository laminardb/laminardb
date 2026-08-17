//! Backpressure strategies for WebSocket connectors.
//!
//! When the internal bounded channel is full and cannot accept more
//! messages from a WebSocket source, a `WsBackpressure` strategy determines
//! what happens to incoming data.

/// Strategy applied when the bounded channel is full and cannot accept more messages.
///
/// WebSocket sources produce data at the rate of the remote sender. When the
/// downstream processing pipeline cannot keep up, one of these strategies
/// governs how the connector handles the overflow.
///
#[derive(Debug, Clone, Default)]
pub enum WsBackpressure {
    /// Block WS read -- TCP backpressure propagates to sender.
    ///
    /// This is the safest option: the WebSocket read loop simply stops
    /// reading, which causes the TCP window to fill, eventually slowing
    /// the remote sender. `LaminarDB` does not intentionally drop while blocked,
    /// but an ephemeral upstream can still disconnect or lose data.
    #[default]
    Block,
    /// Drop incoming message when channel full (don't enqueue).
    ///
    /// The newly arriving message is silently discarded. Useful when
    /// freshness of already-buffered data matters more than completeness.
    DropNewest,
}
#[cfg(test)]
mod tests;
