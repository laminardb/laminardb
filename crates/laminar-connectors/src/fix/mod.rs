//! FIX TagValue source built on the QuickFIX C++ session engine.
//!
//! One source owns one initiator session. Inbound application messages are journaled inside the
//! engine callback, and the session's store never persists an inbound sequence number past a
//! record that is not yet durable; records are then projected from the journal into Arrow rows
//! with deterministic `(session, journal index, row)` positions. Restart replays the journal from
//! the committed checkpoint cursor; the session resumes from its store and the counterparty
//! retransmits anything a crash lost.
//!
//! Supported: FIX.4.2/FIX.4.4 and FIXT.1.1 (one FIX 5.0 application version) initiator over
//! loopback TCP or verified TLS, at-least-once or exactly-once with checkpointing, embedded and
//! single-node deployments, and the Binance Spot drop-copy profile (Ed25519-signed Logon, no
//! retransmission). Not supported (rejected before I/O): acceptor sessions, several
//! application versions in one session, cluster placement, and outbound application messages.

// FIX field and message names (MsgSeqNum, BodyLength, ExecutionReport) are protocol
// vocabulary in prose, not code identifiers.
#![allow(clippy::doc_markdown)]

mod capture;
mod config;
mod dictionary;
mod engine;
#[cfg(feature = "fpml")]
mod fpml;
mod journal;
mod metrics;
mod projection;
mod session_store;
mod source;
#[cfg(any(test, feature = "testing"))]
pub mod testing;
#[cfg(test)]
mod tests;
mod tls;
mod venue;

pub use source::FixSource;

use std::sync::Arc;

use crate::config::ConnectorInfo;
use crate::error::ConnectorError;
use crate::registry::ConnectorRegistry;
use crate::schema::resolution::SchemaCapabilities;

/// Native schema format recorded in FIX schema bindings.
const NATIVE_FORMAT: &str = "fix-tagvalue";

/// Register the `fix` source connector.
///
/// # Errors
/// Returns an error if the source is already registered or the registry is frozen.
pub fn register_fix_source(registry: &ConnectorRegistry) -> Result<(), ConnectorError> {
    let info = ConnectorInfo {
        schema_capabilities: SchemaCapabilities::declared(false),
        name: "fix".to_string(),
        display_name: "FIX Session Source".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        is_source: true,
        is_sink: false,
        config_keys: config::config_keys(),
    };
    registry.register_source(
        "fix",
        info,
        Arc::new(|registry| Ok(Box::new(FixSource::new(registry.cloned())))),
    )
}
