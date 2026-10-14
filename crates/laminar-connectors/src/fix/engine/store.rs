//! Engine message-store factories.
//!
//! Sessions of the source use a store whose state lives in Rust (`StoreBackend`): the engine
//! caches sequence numbers and the creation time, and persists every change through the backend
//! before caching it. A backend error surfaces in the engine as an `IOException`: inbound
//! processing disconnects, and an outbound message is not sent.

use std::ffi::c_void;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Arc;

use quickfix_ffi as ffi;

use super::{bridge_error, EngineError};
#[cfg(any(test, feature = "testing"))]
use super::{engine_error, Settings};
use crate::error::ConnectorError;

/// Session store state as the engine sees it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct StoreValues {
    /// Changes exactly when the store is reset.
    pub(crate) creation_unix_micros: i64,
    pub(crate) next_sender: u64,
    pub(crate) next_target: u64,
}

/// Durable owner of one session's store state. Called on engine threads while the engine holds
/// its session-state lock.
pub(crate) trait StoreBackend: Send + Sync {
    fn values(&self) -> StoreValues;
    /// Persist the next outbound sequence number before the engine sends a message with the
    /// previous one.
    fn set_next_sender(&self, next: u64) -> Result<(), ConnectorError>;
    fn set_next_target(&self, next: u64) -> Result<(), ConnectorError>;
    /// Start a new store: both sequence numbers 1 and a creation time after every earlier one.
    /// Returns the creation time.
    fn reset(&self, now_unix_micros: i64) -> Result<i64, ConnectorError>;
}

static CALLBACKS: ffi::LmFixStoreCallbacks = ffi::LmFixStoreCallbacks {
    set_next_sender,
    set_next_target,
    reset,
    load,
};

/// A store factory and the backend its stores call.
pub(crate) struct StoreFactory {
    pub(super) raw: ffi::FixMessageStoreFactory_t,
    // The engine holds a thin pointer to this box until the factory is deleted in `Drop`.
    _backend: Option<Box<Arc<dyn StoreBackend>>>,
}

impl StoreFactory {
    /// Stores backed by `backend`, starting from its current values.
    pub(crate) fn owned(backend: Arc<dyn StoreBackend>) -> Result<Self, EngineError> {
        let values = backend.values();
        let backend = Box::new(backend);
        let ctx = std::ptr::from_ref::<Arc<dyn StoreBackend>>(&backend)
            .cast_mut()
            .cast::<c_void>();
        // SAFETY: the callbacks table is static and `ctx` points into a box owned by the returned
        // factory, which outlives every store it creates.
        let raw = unsafe {
            ffi::LmFix_callbackStoreFactoryNew(
                &raw const CALLBACKS,
                ctx,
                values.creation_unix_micros,
                values.next_sender,
                values.next_target,
            )
        }
        .ok_or_else(|| bridge_error("session store factory"))?;
        Ok(Self {
            raw,
            _backend: Some(backend),
        })
    }

    /// The engine's own `FileStore` under `FileStorePath`, for local counterparties.
    #[cfg(any(test, feature = "testing"))]
    pub(crate) fn file(settings: &Settings) -> Result<Self, EngineError> {
        // SAFETY: `settings` is live; the factory copies what it needs (FileStorePath).
        let raw = unsafe { ffi::FixFileMessageStoreFactory_new(settings.raw) }
            .ok_or_else(|| engine_error("file store factory"))?;
        Ok(Self {
            raw,
            _backend: None,
        })
    }
}

impl Drop for StoreFactory {
    fn drop(&mut self) {
        // SAFETY: created by a factory constructor and deleted after the socket that used it.
        unsafe { ffi::FixMessageStoreFactory_delete(self.raw) };
    }
}

fn guarded(ctx: *mut c_void, body: impl FnOnce(&dyn StoreBackend) -> bool) -> i8 {
    // SAFETY: the engine passes back the pointer registered in `StoreFactory::owned`, whose box
    // is alive while the factory's stores exist.
    let backend = unsafe { &*ctx.cast::<Arc<dyn StoreBackend>>() };
    match catch_unwind(AssertUnwindSafe(|| body(backend.as_ref()))) {
        Ok(true) => 0,
        Ok(false) => -1,
        Err(_) => {
            tracing::error!("FIX session store callback panicked");
            -1
        }
    }
}

extern "C" fn set_next_sender(ctx: *mut c_void, next: u64) -> i8 {
    guarded(ctx, |backend| backend.set_next_sender(next).is_ok())
}

extern "C" fn set_next_target(ctx: *mut c_void, next: u64) -> i8 {
    guarded(ctx, |backend| backend.set_next_target(next).is_ok())
}

extern "C" fn reset(ctx: *mut c_void, now_unix_micros: i64, creation: *mut i64) -> i8 {
    guarded(ctx, |backend| match backend.reset(now_unix_micros) {
        Ok(created) => {
            // SAFETY: the bridge passes a valid writable location.
            unsafe { creation.write(created) };
            true
        }
        Err(_) => false,
    })
}

extern "C" fn load(
    ctx: *mut c_void,
    creation: *mut i64,
    next_sender: *mut u64,
    next_target: *mut u64,
) -> i8 {
    guarded(ctx, |backend| {
        let values = backend.values();
        // SAFETY: the bridge passes valid writable locations.
        unsafe {
            creation.write(values.creation_unix_micros);
            next_sender.write(values.next_sender);
            next_target.write(values.next_target);
        }
        true
    })
}
