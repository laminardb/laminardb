use std::sync::Arc;

use laminar_connectors::connector::ConnectorTaskTracker;

#[derive(Clone)]
pub(crate) struct ConnectorTaskFence {
    name: Arc<str>,
    tracker: ConnectorTaskTracker,
    registration: Arc<()>,
}

pub(crate) type OwnedConnectorTaskFences = Arc<parking_lot::Mutex<Vec<ConnectorTaskFence>>>;

impl ConnectorTaskFence {
    #[cfg(test)]
    pub(crate) fn new(name: impl Into<Arc<str>>, tracker: ConnectorTaskTracker) -> Self {
        Self {
            name: name.into(),
            tracker,
            registration: Arc::new(()),
        }
    }

    fn registered(name: Arc<str>, tracker: ConnectorTaskTracker, registration: Arc<()>) -> Self {
        Self {
            name,
            tracker,
            registration,
        }
    }

    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn is_finished(&self) -> bool {
        self.tracker.is_terminated()
    }

    pub(crate) async fn wait_until(&self, deadline: tokio::time::Instant) -> bool {
        if self.is_finished() {
            return true;
        }
        tokio::time::timeout_at(deadline, self.tracker.wait_terminated())
            .await
            .is_ok()
    }
}

/// Captures one connector generation proof and leaves its DB fence armed on every implicit drop.
pub(crate) struct ConnectorTaskFenceRegistration {
    name: Arc<str>,
    tracker: Option<ConnectorTaskTracker>,
    owned: Option<OwnedConnectorTaskFences>,
    registration: Option<Arc<()>>,
}

impl ConnectorTaskFenceRegistration {
    pub(crate) fn capture(
        name: impl Into<Arc<str>>,
        tracker: Option<ConnectorTaskTracker>,
    ) -> Self {
        Self {
            name: name.into(),
            tracker,
            owned: None,
            registration: None,
        }
    }

    pub(crate) fn capture_registered(
        name: impl Into<Arc<str>>,
        tracker: Option<ConnectorTaskTracker>,
        owned: &OwnedConnectorTaskFences,
    ) -> Self {
        let mut captured = Self::capture(name, tracker);
        captured.register(owned);
        captured
    }

    fn register(&mut self, owned: &OwnedConnectorTaskFences) {
        if self.tracker.is_none() || self.registration.is_some() {
            return;
        }
        let registration = Arc::new(());
        owned.lock().push(ConnectorTaskFence::registered(
            Arc::clone(&self.name),
            self.tracker
                .as_ref()
                .expect("tracker checked above")
                .clone(),
            Arc::clone(&registration),
        ));
        self.owned = Some(Arc::clone(owned));
        self.registration = Some(registration);
    }

    pub(crate) fn tracker(&self) -> Option<ConnectorTaskTracker> {
        self.tracker.clone()
    }

    /// Removes the provisional fence only after a stable supervisor owns the same tracker.
    pub(crate) fn handoff(mut self) {
        let (Some(owned), Some(registration)) = (self.owned.take(), self.registration.take())
        else {
            return;
        };
        owned
            .lock()
            .retain(|fence| !Arc::ptr_eq(&fence.registration, &registration));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use laminar_connectors::connector::ConnectorTaskOwner;

    #[tokio::test(start_paused = true)]
    async fn fence_waits_for_every_connector_child() {
        let (owner, tracker) = ConnectorTaskOwner::new();
        let guard = owner.track().expect("live connector generation");
        let fence = ConnectorTaskFence::new("source:test", tracker);
        drop(owner);

        let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(10);
        assert!(!fence.wait_until(deadline).await);
        assert!(!fence.is_finished());

        drop(guard);
        assert!(fence.is_finished());
    }

    #[test]
    fn implicit_registration_drop_leaves_db_fence_armed() {
        let (owner, tracker) = ConnectorTaskOwner::new();
        let guard = owner.track().expect("live connector generation");
        let owned = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let registration = ConnectorTaskFenceRegistration::capture_registered(
            "source:test",
            Some(tracker),
            &owned,
        );

        drop(registration);
        drop(owner);
        assert_eq!(owned.lock().len(), 1);
        assert!(!owned.lock()[0].is_finished());

        drop(guard);
        assert!(owned.lock()[0].is_finished());
    }

    #[test]
    fn explicit_handoff_removes_only_its_provisional_fence() {
        let (first_owner, first_tracker) = ConnectorTaskOwner::new();
        let (second_owner, second_tracker) = ConnectorTaskOwner::new();
        let owned = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let first = ConnectorTaskFenceRegistration::capture_registered(
            "source:first",
            Some(first_tracker),
            &owned,
        );
        let _second = ConnectorTaskFenceRegistration::capture_registered(
            "source:second",
            Some(second_tracker),
            &owned,
        );

        first.handoff();
        assert_eq!(owned.lock().len(), 1);
        assert_eq!(owned.lock()[0].name(), "source:second");
        drop((first_owner, second_owner));
    }
}
