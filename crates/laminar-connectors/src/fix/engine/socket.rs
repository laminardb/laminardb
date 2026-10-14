//! Engine socket objects: one initiator or acceptor and the engine values it borrows.
//!
//! QuickFIX keeps references to the application, store factory, log factory, and settings for
//! its whole life, so each socket owns them and deletes the engine object first.
//!
//! Both sockets use the engine's threaded variants: `start` spawns engine-owned threads (one
//! connect loop plus one thread per connection) and `stop` joins all of them before returning.
//! The single-threaded `SocketInitiator` is not used: after a session-initiated disconnect its
//! `SocketMonitor::drop` removes the socket before `ConnectorWrapper::onError` calls `drop`
//! again, so `onDisconnect` never fires and the initiator never reconnects.

use std::collections::BTreeSet;

use parking_lot::Mutex;
use quickfix_ffi as ffi;

use super::store::StoreFactory;
use super::{bridge_error, engine_error, Application, EngineError, Settings};

/// Sessions registered with the engine in this process. The engine's session registry is
/// process-global, so a second socket for an active session would share or corrupt its state.
static ACTIVE_SESSIONS: Mutex<BTreeSet<String>> = Mutex::new(BTreeSet::new());

/// Exclusive claim on one session identity; released after the engine object is deleted.
struct SessionClaim(String);

impl SessionClaim {
    fn acquire(session: &str) -> Result<Self, EngineError> {
        if !ACTIVE_SESSIONS.lock().insert(session.to_string()) {
            return Err(EngineError::Call {
                operation: "session registration",
                message: format!("FIX session {session} is already active in this process"),
            });
        }
        Ok(Self(session.to_string()))
    }
}

impl Drop for SessionClaim {
    fn drop(&mut self) {
        ACTIVE_SESSIONS.lock().remove(&self.0);
    }
}

/// A `ThreadedSocketInitiator` for one session.
pub(crate) struct Initiator {
    raw: ffi::FixInitiator_t,
    // Dropped after `raw` is deleted in `Drop`, in declaration order.
    _store: StoreFactory,
    _application: Application,
    _settings: Settings,
    _claim: SessionClaim,
}

// SAFETY: `start` and `stop` are the engine's cross-thread control API, and the owned engine
// values are only freed in `Drop`, after `stop` joined the engine threads.
unsafe impl Send for Initiator {}
// SAFETY: see `Send`.
unsafe impl Sync for Initiator {}

impl Initiator {
    /// Create the initiator and its session. No socket is opened until `start`.
    pub(crate) fn new(
        settings: Settings,
        application: Application,
        store: StoreFactory,
    ) -> Result<Self, EngineError> {
        let claim = SessionClaim::acquire(&settings.session)?;
        // SAFETY: all referenced objects are live and owned by the returned value, which keeps
        // them alive until after the initiator is deleted.
        let raw = unsafe {
            ffi::FixInitiator_new(
                application.raw,
                store.raw,
                settings.raw,
                application.log,
                1,
                0,
            )
        }
        .ok_or_else(|| engine_error("initiator"))?;
        Ok(Self {
            raw,
            _store: store,
            _application: application,
            _settings: settings,
            _claim: claim,
        })
    }

    /// Start the engine threads. Callbacks run on them until `stop` returns.
    pub(crate) fn start(&self) -> Result<(), EngineError> {
        // SAFETY: the initiator is live for the call.
        if unsafe { ffi::FixInitiator_start(self.raw) } != 0 {
            return Err(engine_error("initiator start"));
        }
        Ok(())
    }

    /// Stop the engine and join its threads. With `force == false` the engine first sends
    /// Logout and waits up to ten seconds for the counterparty's response.
    pub(crate) fn stop(&self, force: bool) -> Result<(), EngineError> {
        // SAFETY: the initiator is live; the engine allows stop from another thread.
        if unsafe { ffi::LmFix_initiatorStop(self.raw, i8::from(force)) } != 0 {
            return Err(bridge_error("initiator stop"));
        }
        Ok(())
    }

    /// Whether the engine has stopped.
    #[cfg(test)]
    pub(crate) fn is_stopped(&self) -> bool {
        // SAFETY: the initiator is live.
        unsafe { ffi::FixInitiator_isStopped(self.raw) == 1 }
    }
}

impl Drop for Initiator {
    fn drop(&mut self) {
        // `~Initiator` deletes sessions without stopping engine threads, so stop and join them
        // first; stopping an already stopped initiator returns immediately.
        // SAFETY: the initiator is live and owned; no other reference remains.
        unsafe {
            let _ = ffi::LmFix_initiatorStop(self.raw, 1);
            ffi::FixInitiator_delete(self.raw);
        }
    }
}

/// A threaded `SocketAcceptor` used as a local counterparty in tests.
#[cfg(any(test, feature = "testing"))]
pub(crate) struct Acceptor {
    raw: ffi::FixAcceptor_t,
    _store: StoreFactory,
    _application: Application,
    _settings: Settings,
    _claim: SessionClaim,
}

// SAFETY: as for `Initiator`; the acceptor runs on engine-owned threads after `start`.
#[cfg(any(test, feature = "testing"))]
unsafe impl Send for Acceptor {}
// SAFETY: see `Send`.
#[cfg(any(test, feature = "testing"))]
unsafe impl Sync for Acceptor {}

#[cfg(any(test, feature = "testing"))]
impl Acceptor {
    pub(crate) fn new(settings: Settings, application: Application) -> Result<Self, EngineError> {
        let claim = SessionClaim::acquire(&settings.session)?;
        let store = StoreFactory::file(&settings)?;
        // SAFETY: as for `Initiator::new`.
        let raw = unsafe {
            ffi::FixAcceptor_new(
                application.raw,
                store.raw,
                settings.raw,
                application.log,
                1,
                0,
            )
        }
        .ok_or_else(|| engine_error("acceptor"))?;
        Ok(Self {
            raw,
            _store: store,
            _application: application,
            _settings: settings,
            _claim: claim,
        })
    }

    /// Start listening on engine-owned threads.
    pub(crate) fn start(&self) -> Result<(), EngineError> {
        // SAFETY: the acceptor is live.
        if unsafe { ffi::FixAcceptor_start(self.raw) } != 0 {
            return Err(engine_error("acceptor start"));
        }
        Ok(())
    }

    /// Stop and join the engine threads.
    pub(crate) fn stop(&self, force: bool) -> Result<(), EngineError> {
        // SAFETY: the acceptor is live.
        if unsafe { ffi::LmFix_acceptorStop(self.raw, i8::from(force)) } != 0 {
            return Err(bridge_error("acceptor stop"));
        }
        Ok(())
    }
}

#[cfg(any(test, feature = "testing"))]
impl Drop for Acceptor {
    fn drop(&mut self) {
        // As for `Initiator`: `~Acceptor` does not stop engine threads.
        // SAFETY: the acceptor is live and owned; no other reference remains.
        unsafe {
            let _ = ffi::LmFix_acceptorStop(self.raw, 1);
            ffi::FixAcceptor_delete(self.raw);
        }
    }
}
