//! Ring handler trait for processing completions.
//!
//! The `RingHandler` trait defines the interface for handling completions
//! from each ring type, as well as integrating with the Ring 0/1/2 architecture.

use super::router::RoutedCompletion;

/// Handler interface for three-ring event processing.
///
/// Implement this trait to define how completions are processed for each
/// ring type and how to integrate with the Ring 0/1/2 architecture.
///
/// # Ring 0/1/2 Integration
///
/// The three-ring I/O pattern maps to the LaminarDB ring architecture:
///
/// - **Ring 0 (Hot Path)**: Latency ring completions trigger event processing
/// - **Ring 1 (Background)**: Main ring completions for WAL/checkpoints
/// - **Ring 2 (Control)**: Handled separately via control messages
pub trait RingHandler {
    /// Handle a latency ring completion (network, urgent ops).
    ///
    /// This is called for completions from the latency ring, which handles
    /// time-critical operations like network receives.
    fn handle_latency_completion(&mut self, completion: RoutedCompletion);

    /// Handle a main ring completion (WAL, normal I/O).
    ///
    /// This is called for completions from the main ring, which handles
    /// normal I/O operations that can tolerate higher latency.
    fn handle_main_completion(&mut self, completion: RoutedCompletion);

    /// Handle a poll ring completion (storage).
    ///
    /// This is called for completions from the poll ring, which handles
    /// high-throughput storage operations via IOPOLL.
    fn handle_poll_completion(&mut self, completion: RoutedCompletion);

    /// Process Ring 0 events (application hot path).
    ///
    /// Called after draining latency ring completions.
    /// This is where the main application event processing happens.
    fn process_ring0_events(&mut self);

    /// Check if Ring 0 is idle.
    ///
    /// Returns true if there are no pending Ring 0 events.
    /// When idle, Ring 1 background work can be processed.
    fn ring0_idle(&self) -> bool;

    /// Process a chunk of Ring 1 work (background).
    ///
    /// Called when Ring 0 is idle. Should process a bounded amount
    /// of background work to avoid starving Ring 0.
    fn process_ring1_chunk(&mut self);

    /// Check for control messages (Ring 2).
    ///
    /// Returns true if there are pending control messages.
    fn has_control_message(&self) -> bool;

    /// Process Ring 2 (control plane).
    ///
    /// Handle administrative messages like reconfiguration.
    fn process_ring2(&mut self);

    /// Check if the reactor should sleep.
    ///
    /// Returns true if all rings are idle and we should block
    /// on the main ring waiting for activity.
    fn should_sleep(&self) -> bool;

    /// Check if the reactor should shutdown.
    ///
    /// Returns true when the reactor should exit its event loop.
    fn should_shutdown(&self) -> bool;
}

/// A simple ring handler for testing and basic usage.
///
/// This handler collects completions into vectors and provides
/// basic Ring 0/1/2 scaffolding.
#[derive(Debug, Default)]
pub struct SimpleRingHandler {
    /// Latency completions received.
    pub latency_completions: Vec<RoutedCompletion>,
    /// Main completions received.
    pub main_completions: Vec<RoutedCompletion>,
    /// Poll completions received.
    pub poll_completions: Vec<RoutedCompletion>,

    /// Whether Ring 0 is idle.
    pub is_ring0_idle: bool,
    /// Whether there are control messages.
    pub has_control: bool,
    /// Whether to sleep.
    pub should_sleep_flag: bool,
    /// Whether to shutdown.
    pub shutdown: bool,

    /// Ring 0 events processed.
    pub ring0_events_processed: u64,
    /// Ring 1 chunks processed.
    pub ring1_chunks_processed: u64,
    /// Ring 2 messages processed.
    pub ring2_messages_processed: u64,
}

impl SimpleRingHandler {
    /// Create a new simple handler.
    #[must_use]
    pub fn new() -> Self {
        Self {
            is_ring0_idle: true,
            should_sleep_flag: false,
            ..Default::default()
        }
    }

    /// Clear all collected completions.
    pub fn clear(&mut self) {
        self.latency_completions.clear();
        self.main_completions.clear();
        self.poll_completions.clear();
    }

    /// Get total completions collected.
    #[must_use]
    pub fn total_completions(&self) -> usize {
        self.latency_completions.len()
            + self.main_completions.len()
            + self.poll_completions.len()
    }

    /// Request shutdown.
    pub fn request_shutdown(&mut self) {
        self.shutdown = true;
    }
}

impl RingHandler for SimpleRingHandler {
    fn handle_latency_completion(&mut self, completion: RoutedCompletion) {
        self.latency_completions.push(completion);
        self.is_ring0_idle = false;
    }

    fn handle_main_completion(&mut self, completion: RoutedCompletion) {
        self.main_completions.push(completion);
    }

    fn handle_poll_completion(&mut self, completion: RoutedCompletion) {
        self.poll_completions.push(completion);
    }

    fn process_ring0_events(&mut self) {
        self.ring0_events_processed += 1;
        // Mark as idle after processing
        self.is_ring0_idle = true;
    }

    fn ring0_idle(&self) -> bool {
        self.is_ring0_idle
    }

    fn process_ring1_chunk(&mut self) {
        self.ring1_chunks_processed += 1;
    }

    fn has_control_message(&self) -> bool {
        self.has_control
    }

    fn process_ring2(&mut self) {
        self.ring2_messages_processed += 1;
        self.has_control = false;
    }

    fn should_sleep(&self) -> bool {
        self.should_sleep_flag
    }

    fn should_shutdown(&self) -> bool {
        self.shutdown
    }
}

/// Callback-based ring handler.
///
/// Allows providing closures for handling completions.
pub struct CallbackRingHandler<L, M, P>
where
    L: FnMut(RoutedCompletion),
    M: FnMut(RoutedCompletion),
    P: FnMut(RoutedCompletion),
{
    latency_callback: L,
    main_callback: M,
    poll_callback: P,
    shutdown: bool,
}

impl<L, M, P> CallbackRingHandler<L, M, P>
where
    L: FnMut(RoutedCompletion),
    M: FnMut(RoutedCompletion),
    P: FnMut(RoutedCompletion),
{
    /// Create a new callback handler.
    pub fn new(latency: L, main: M, poll: P) -> Self {
        Self {
            latency_callback: latency,
            main_callback: main,
            poll_callback: poll,
            shutdown: false,
        }
    }

    /// Request shutdown.
    pub fn request_shutdown(&mut self) {
        self.shutdown = true;
    }
}

impl<L, M, P> RingHandler for CallbackRingHandler<L, M, P>
where
    L: FnMut(RoutedCompletion),
    M: FnMut(RoutedCompletion),
    P: FnMut(RoutedCompletion),
{
    fn handle_latency_completion(&mut self, completion: RoutedCompletion) {
        (self.latency_callback)(completion);
    }

    fn handle_main_completion(&mut self, completion: RoutedCompletion) {
        (self.main_callback)(completion);
    }

    fn handle_poll_completion(&mut self, completion: RoutedCompletion) {
        (self.poll_callback)(completion);
    }

    fn process_ring0_events(&mut self) {
        // No-op for callback handler
    }

    fn ring0_idle(&self) -> bool {
        true
    }

    fn process_ring1_chunk(&mut self) {
        // No-op for callback handler
    }

    fn has_control_message(&self) -> bool {
        false
    }

    fn process_ring2(&mut self) {
        // No-op for callback handler
    }

    fn should_sleep(&self) -> bool {
        false
    }

    fn should_shutdown(&self) -> bool {
        self.shutdown
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io_uring::three_ring::RingAffinity;
    use std::time::Instant;

    fn make_completion(user_data: u64, affinity: RingAffinity) -> RoutedCompletion {
        RoutedCompletion {
            user_data,
            result: 0,
            flags: 0,
            affinity,
            submitted_at: Some(Instant::now()),
            op_type: None,
        }
    }

    #[test]
    fn test_simple_handler_new() {
        let handler = SimpleRingHandler::new();
        assert!(handler.is_ring0_idle);
        assert!(!handler.shutdown);
        assert!(!handler.has_control);
        assert_eq!(handler.total_completions(), 0);
    }

    #[test]
    fn test_simple_handler_completions() {
        let mut handler = SimpleRingHandler::new();

        handler.handle_latency_completion(make_completion(1, RingAffinity::Latency));
        handler.handle_latency_completion(make_completion(2, RingAffinity::Latency));
        handler.handle_main_completion(make_completion(3, RingAffinity::Main));
        handler.handle_poll_completion(make_completion(4, RingAffinity::Poll));

        assert_eq!(handler.latency_completions.len(), 2);
        assert_eq!(handler.main_completions.len(), 1);
        assert_eq!(handler.poll_completions.len(), 1);
        assert_eq!(handler.total_completions(), 4);

        // Latency completion should mark Ring 0 as not idle
        assert!(!handler.is_ring0_idle);
    }

    #[test]
    fn test_simple_handler_ring_processing() {
        let mut handler = SimpleRingHandler::new();

        // Process Ring 0
        handler.process_ring0_events();
        assert_eq!(handler.ring0_events_processed, 1);
        assert!(handler.ring0_idle());

        // Process Ring 1
        handler.process_ring1_chunk();
        assert_eq!(handler.ring1_chunks_processed, 1);

        // Process Ring 2
        handler.has_control = true;
        assert!(handler.has_control_message());
        handler.process_ring2();
        assert_eq!(handler.ring2_messages_processed, 1);
        assert!(!handler.has_control_message());
    }

    #[test]
    fn test_simple_handler_shutdown() {
        let mut handler = SimpleRingHandler::new();
        assert!(!handler.should_shutdown());

        handler.request_shutdown();
        assert!(handler.should_shutdown());
    }

    #[test]
    fn test_simple_handler_clear() {
        let mut handler = SimpleRingHandler::new();
        handler.handle_latency_completion(make_completion(1, RingAffinity::Latency));
        handler.handle_main_completion(make_completion(2, RingAffinity::Main));
        assert_eq!(handler.total_completions(), 2);

        handler.clear();
        assert_eq!(handler.total_completions(), 0);
    }

    #[test]
    fn test_callback_handler() {
        let mut latency_count = 0;
        let mut main_count = 0;
        let mut poll_count = 0;

        {
            let mut handler = CallbackRingHandler::new(
                |_| latency_count += 1,
                |_| main_count += 1,
                |_| poll_count += 1,
            );

            handler.handle_latency_completion(make_completion(1, RingAffinity::Latency));
            handler.handle_latency_completion(make_completion(2, RingAffinity::Latency));
            handler.handle_main_completion(make_completion(3, RingAffinity::Main));
            handler.handle_poll_completion(make_completion(4, RingAffinity::Poll));
        }

        assert_eq!(latency_count, 2);
        assert_eq!(main_count, 1);
        assert_eq!(poll_count, 1);
    }
}
