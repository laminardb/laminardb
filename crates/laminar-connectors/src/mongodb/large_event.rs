//! Large event fragment reassembly for `MongoDB` change streams.
//!
//! `MongoDB` ≥ 6.0.9 supports the `$changeStreamSplitLargeEvent` pipeline
//! stage, which splits change events exceeding the 16 MiB BSON limit into
//! ordered fragments. Each fragment carries:
//!
//! - `splitEvent.fragment`: 1-based ordinal of this fragment
//! - `splitEvent.of`: total number of fragments
//!
//! This module buffers fragments by their resume token prefix and
//! reassembles them into a single event once all fragments arrive.
//!
//! # Error Handling
//!
//! If fragments arrive out of order or are incomplete after the
//! configured timeout, `LargeEventReassemblyFailed` is emitted.

use std::collections::HashMap;
use std::time::{Duration, Instant};

/// Default timeout for large event fragment reassembly.
const DEFAULT_REASSEMBLY_TIMEOUT: Duration = Duration::from_secs(30);

/// Error when large event reassembly fails.
#[derive(Debug, thiserror::Error)]
pub enum LargeEventError {
    /// Not all fragments arrived before the timeout.
    #[error("large event reassembly failed: expected {expected} fragments, received {received}")]
    ReassemblyFailed {
        /// Total expected fragments.
        expected: u32,
        /// Fragments actually received.
        received: u32,
    },

    /// Fragment ordinal out of range.
    #[error("fragment index {index} out of range (total: {total})")]
    FragmentOutOfRange {
        /// The fragment ordinal received.
        index: u32,
        /// The total fragments expected.
        total: u32,
    },
}

/// Metadata about a split event fragment.
#[derive(Debug, Clone)]
pub struct SplitEventInfo {
    /// 1-based ordinal of this fragment.
    pub fragment: u32,
    /// Total number of fragments.
    pub of: u32,
}

/// A single fragment of a large change event.
#[derive(Debug, Clone)]
pub struct EventFragment {
    /// Fragment metadata.
    pub split_info: SplitEventInfo,
    /// The partial JSON body of this fragment.
    pub body: String,
}

/// In-progress reassembly of a split large event.
#[derive(Debug)]
struct PendingReassembly {
    /// Total number of fragments expected.
    total: u32,
    /// Fragments received, indexed by 1-based ordinal.
    fragments: HashMap<u32, String>,
    /// When the first fragment was received.
    started_at: Instant,
}

/// Buffers and reassembles split large change events.
///
/// Events are keyed by a caller-provided correlation ID (typically the
/// resume token of the first fragment). Completed events are returned
/// as reassembled JSON strings.
#[derive(Debug)]
pub struct LargeEventReassembler {
    /// In-progress reassemblies keyed by correlation ID.
    pending: HashMap<String, PendingReassembly>,
    /// Timeout for fragment completion.
    timeout: Duration,
}

impl LargeEventReassembler {
    /// Creates a new reassembler with the default timeout (30s).
    #[must_use]
    pub fn new() -> Self {
        Self {
            pending: HashMap::new(),
            timeout: DEFAULT_REASSEMBLY_TIMEOUT,
        }
    }

    /// Creates a new reassembler with a custom timeout.
    #[must_use]
    pub fn with_timeout(timeout: Duration) -> Self {
        Self {
            pending: HashMap::new(),
            timeout,
        }
    }

    /// Adds a fragment and returns the reassembled body if all fragments
    /// have arrived.
    ///
    /// # Arguments
    ///
    /// - `correlation_id`: Unique ID to group fragments (e.g., resume token).
    /// - `fragment`: The fragment to add.
    ///
    /// # Errors
    ///
    /// Returns `LargeEventError::FragmentOutOfRange` if the fragment ordinal
    /// is invalid.
    pub fn add_fragment(
        &mut self,
        correlation_id: &str,
        fragment: EventFragment,
    ) -> Result<Option<String>, LargeEventError> {
        let info = &fragment.split_info;

        if info.fragment == 0 || info.fragment > info.of {
            return Err(LargeEventError::FragmentOutOfRange {
                index: info.fragment,
                total: info.of,
            });
        }

        let entry = self
            .pending
            .entry(correlation_id.to_string())
            .or_insert_with(|| PendingReassembly {
                total: info.of,
                fragments: HashMap::new(),
                started_at: Instant::now(),
            });

        entry.fragments.insert(info.fragment, fragment.body);

        #[allow(clippy::cast_possible_truncation)]
        if entry.fragments.len() as u32 == entry.total {
            let reassembly = self.pending.remove(correlation_id).unwrap_or_else(|| {
                // Should be unreachable since we just checked.
                unreachable!("pending entry disappeared during reassembly")
            });
            let mut body = String::new();
            for i in 1..=reassembly.total {
                if let Some(part) = reassembly.fragments.get(&i) {
                    body.push_str(part);
                }
            }
            Ok(Some(body))
        } else {
            Ok(None)
        }
    }

    /// Evicts timed-out pending reassemblies and returns errors for each.
    pub fn evict_expired(&mut self) -> Vec<(String, LargeEventError)> {
        let now = Instant::now();
        let mut expired = Vec::new();

        self.pending.retain(|id, pending| {
            if now.duration_since(pending.started_at) > self.timeout {
                expired.push((
                    id.clone(),
                    LargeEventError::ReassemblyFailed {
                        expected: pending.total,
                        #[allow(clippy::cast_possible_truncation)]
                        received: pending.fragments.len() as u32,
                    },
                ));
                false
            } else {
                true
            }
        });

        expired
    }

    /// Returns the number of in-progress reassemblies.
    #[must_use]
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }
}

impl Default for LargeEventReassembler {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_fragment(index: u32, total: u32, body: &str) -> EventFragment {
        EventFragment {
            split_info: SplitEventInfo {
                fragment: index,
                of: total,
            },
            body: body.to_string(),
        }
    }

    #[test]
    fn test_single_fragment_event() {
        let mut r = LargeEventReassembler::new();
        let result = r
            .add_fragment("tok1", make_fragment(1, 1, "full_body"))
            .unwrap();
        assert_eq!(result.as_deref(), Some("full_body"));
        assert_eq!(r.pending_count(), 0);
    }

    #[test]
    fn test_two_fragment_event() {
        let mut r = LargeEventReassembler::new();

        let result = r
            .add_fragment("tok1", make_fragment(1, 2, "first_"))
            .unwrap();
        assert!(result.is_none());
        assert_eq!(r.pending_count(), 1);

        let result = r
            .add_fragment("tok1", make_fragment(2, 2, "second"))
            .unwrap();
        assert_eq!(result.as_deref(), Some("first_second"));
        assert_eq!(r.pending_count(), 0);
    }

    #[test]
    fn test_out_of_order_fragments() {
        let mut r = LargeEventReassembler::new();

        // Fragment 2 arrives before 1.
        let result = r.add_fragment("tok1", make_fragment(2, 3, "B")).unwrap();
        assert!(result.is_none());

        let result = r.add_fragment("tok1", make_fragment(3, 3, "C")).unwrap();
        assert!(result.is_none());

        let result = r.add_fragment("tok1", make_fragment(1, 3, "A")).unwrap();
        assert_eq!(result.as_deref(), Some("ABC"));
    }

    #[test]
    fn test_fragment_out_of_range() {
        let mut r = LargeEventReassembler::new();
        let err = r
            .add_fragment("tok1", make_fragment(0, 2, "body"))
            .unwrap_err();
        assert!(matches!(err, LargeEventError::FragmentOutOfRange { .. }));

        let err = r
            .add_fragment("tok1", make_fragment(3, 2, "body"))
            .unwrap_err();
        assert!(matches!(err, LargeEventError::FragmentOutOfRange { .. }));
    }

    #[test]
    fn test_evict_expired() {
        let mut r = LargeEventReassembler::with_timeout(Duration::from_millis(0));

        r.add_fragment("tok1", make_fragment(1, 2, "partial"))
            .unwrap();

        // Immediate expiry.
        std::thread::sleep(Duration::from_millis(1));
        let expired = r.evict_expired();
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].0, "tok1");
        assert!(matches!(
            expired[0].1,
            LargeEventError::ReassemblyFailed {
                expected: 2,
                received: 1,
            }
        ));
        assert_eq!(r.pending_count(), 0);
    }

    #[test]
    fn test_multiple_concurrent_events() {
        let mut r = LargeEventReassembler::new();

        r.add_fragment("tok1", make_fragment(1, 2, "A1")).unwrap();
        r.add_fragment("tok2", make_fragment(1, 2, "B1")).unwrap();
        assert_eq!(r.pending_count(), 2);

        let result = r.add_fragment("tok1", make_fragment(2, 2, "A2")).unwrap();
        assert_eq!(result.as_deref(), Some("A1A2"));
        assert_eq!(r.pending_count(), 1);

        let result = r.add_fragment("tok2", make_fragment(2, 2, "B2")).unwrap();
        assert_eq!(result.as_deref(), Some("B1B2"));
        assert_eq!(r.pending_count(), 0);
    }
}
