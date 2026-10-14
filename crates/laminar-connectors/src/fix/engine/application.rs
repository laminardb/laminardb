//! Engine application and log callbacks.
//!
//! The engine invokes these `extern "C"` functions on the thread that runs the session. Each
//! callback catches panics: a Rust panic must neither unwind into C++ nor be reported to the
//! engine as success, because success lets the engine advance the inbound sequence number past
//! a message that was not durably accepted.

use std::ffi::{c_char, c_void, CStr};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Arc;

use quickfix_ffi as ffi;

use super::{engine_error, set_field_raw, EngineError, MessageRef, Section};

/// Result of an inbound callback.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CallbackOutcome {
    /// The message is accepted; the engine advances the inbound sequence number.
    Accept,
    /// The message was not accepted. The engine sends Logout and disconnects without advancing
    /// the inbound sequence number, so the counterparty retransmits it after reconnecting.
    Disconnect,
}

impl CallbackOutcome {
    const fn code(self) -> i8 {
        match self {
            Self::Accept => ffi::CALLBACK_OK,
            // `quickfix-bind` maps this code to `RejectLogon`, which `Session::next` handles with
            // `generateLogout` + `disconnect` and no `incrNextTargetMsgSeqNum`.
            Self::Disconnect => ffi::CALLBACK_RESULT_REJECT_LOGON,
        }
    }
}

/// Session callbacks for one engine session. Implementations run on the engine session thread
/// and must bound their work.
pub(crate) trait SessionCallbacks: Send + Sync + 'static {
    /// The session completed logon.
    fn on_logon(&self) {}
    /// The session disconnected after a logon.
    fn on_logout(&self) {}
    /// An outbound administrative message is about to be serialized and sent.
    fn to_admin(&self, _message: &mut OutboundMessage<'_>) {}
    /// An inbound administrative message passed session validation.
    fn inbound_admin(&self, message: MessageRef<'_>) -> CallbackOutcome;
    /// An inbound application message passed validation and sequencing.
    fn inbound_app(&self, message: MessageRef<'_>) -> CallbackOutcome;
    /// An engine event line. Never contains message bodies.
    fn on_event(&self, _text: &str) {}
    /// A callback panicked. Inbound callbacks report `Disconnect` after this returns.
    fn on_panic(&self, callback: &'static str);
}

/// Mutable access to an outbound message inside `to_admin`.
pub(crate) struct OutboundMessage<'a> {
    raw: ffi::FixMessage_t,
    _lifetime: std::marker::PhantomData<&'a mut ()>,
}

impl OutboundMessage<'_> {
    /// Borrow the message read-only.
    pub(crate) fn view(&self) -> MessageRef<'_> {
        // SAFETY: the engine keeps the message alive for the callback; the borrow ends with it.
        unsafe { MessageRef::from_raw(self.raw) }
    }

    /// Set a body field.
    pub(crate) fn set_body(&mut self, tag: i32, value: &[u8]) -> Result<(), EngineError> {
        set_field_raw(self.raw, Section::Body, tag, value)
    }
}

/// Owns the engine `Application` and `LogFactory` that call into `SessionCallbacks`.
pub(crate) struct Application {
    pub(super) raw: ffi::FixApplication_t,
    pub(super) log: ffi::FixLogFactory_t,
    // Boxed so the address handed to the engine stays fixed; dropped after the engine objects.
    _host: Box<CallbackHost>,
}

struct CallbackHost {
    callbacks: Arc<dyn SessionCallbacks>,
}

// SAFETY: the engine objects are only used through the socket that owns this value, and the
// callback host is `Send + Sync` (`SessionCallbacks: Send + Sync`).
unsafe impl Send for Application {}
// SAFETY: see `Send`; no `&self` method mutates engine state.
unsafe impl Sync for Application {}

impl Application {
    pub(crate) fn new(callbacks: Arc<dyn SessionCallbacks>) -> Result<Self, EngineError> {
        let host = Box::new(CallbackHost { callbacks });
        let data = std::ptr::from_ref::<CallbackHost>(&host).cast::<c_void>();
        // SAFETY: `data` stays valid until `Application` drops, which happens only after the
        // owning socket deleted the engine; the callback table is `'static`.
        let application =
            unsafe { ffi::FixApplication_new(data, &raw const APPLICATION_CALLBACKS) }
                .ok_or_else(|| engine_error("application"))?;
        // SAFETY: as above for the log callback table.
        let Some(log) = (unsafe { ffi::FixLogFactory_new(data, &raw const LOG_CALLBACKS) }) else {
            // SAFETY: created just above and not shared yet.
            unsafe { ffi::FixApplication_delete(application) };
            return Err(engine_error("log factory"));
        };
        Ok(Self {
            raw: application,
            log,
            _host: host,
        })
    }
}

impl Drop for Application {
    fn drop(&mut self) {
        // SAFETY: both objects were created in `new` and the owning socket deleted the engine
        // before this value drops, so no callback can still reference them.
        unsafe {
            ffi::FixLogFactory_delete(self.log);
            ffi::FixApplication_delete(self.raw);
        }
    }
}

static APPLICATION_CALLBACKS: ffi::FixApplicationCallbacks_t = ffi::FixApplicationCallbacks_t {
    onCreate: on_create,
    onLogon: on_logon,
    onLogout: on_logout,
    toAdmin: to_admin,
    toApp: to_app,
    fromAdmin: from_admin,
    fromApp: from_app,
};

static LOG_CALLBACKS: ffi::FixLogCallbacks_t = ffi::FixLogCallbacks_t {
    onIncoming: log_ignored,
    onOutgoing: log_ignored,
    onEvent: log_event,
};

/// # Safety
/// `data` must be the `CallbackHost` pointer registered by `Application::new`.
unsafe fn host<'a>(data: *const c_void) -> &'a CallbackHost {
    // SAFETY: guaranteed by the caller.
    unsafe { &*data.cast::<CallbackHost>() }
}

fn guarded<T>(
    data: *const c_void,
    callback: &'static str,
    on_panic: T,
    body: impl FnOnce(&CallbackHost) -> T,
) -> T {
    // SAFETY: the engine passes back the pointer registered in `Application::new`.
    let host = unsafe { host(data) };
    catch_unwind(AssertUnwindSafe(|| body(host))).unwrap_or_else(|_| {
        let _ = catch_unwind(AssertUnwindSafe(|| host.callbacks.on_panic(callback)));
        on_panic
    })
}

extern "C" fn on_create(_data: *const c_void, _session: ffi::FixSessionID_t) {}

extern "C" fn on_logon(data: *const c_void, _session: ffi::FixSessionID_t) {
    guarded(data, "on_logon", (), |host| host.callbacks.on_logon());
}

extern "C" fn on_logout(data: *const c_void, _session: ffi::FixSessionID_t) {
    guarded(data, "on_logout", (), |host| host.callbacks.on_logout());
}

extern "C" fn to_admin(
    data: *const c_void,
    message: ffi::FixMessage_t,
    _session: ffi::FixSessionID_t,
) {
    guarded(data, "to_admin", (), |host| {
        let mut outbound = OutboundMessage {
            raw: message,
            _lifetime: std::marker::PhantomData,
        };
        host.callbacks.to_admin(&mut outbound);
    });
}

extern "C" fn to_app(
    _data: *const c_void,
    _message: ffi::FixMessage_t,
    _session: ffi::FixSessionID_t,
) -> i8 {
    ffi::CALLBACK_OK
}

extern "C" fn from_admin(
    data: *const c_void,
    message: ffi::FixMessage_t,
    _session: ffi::FixSessionID_t,
) -> i8 {
    guarded(data, "from_admin", CallbackOutcome::Disconnect, |host| {
        // SAFETY: the engine keeps `message` alive and unmodified for the callback.
        host.callbacks
            .inbound_admin(unsafe { MessageRef::from_raw(message) })
    })
    .code()
}

extern "C" fn from_app(
    data: *const c_void,
    message: ffi::FixMessage_t,
    _session: ffi::FixSessionID_t,
) -> i8 {
    guarded(data, "from_app", CallbackOutcome::Disconnect, |host| {
        // SAFETY: the engine keeps `message` alive and unmodified for the callback.
        host.callbacks
            .inbound_app(unsafe { MessageRef::from_raw(message) })
    })
    .code()
}

extern "C" fn log_ignored(
    _data: *const c_void,
    _session: Option<ffi::FixSessionID_t>,
    _message: *const c_char,
) {
}

extern "C" fn log_event(
    data: *const c_void,
    _session: Option<ffi::FixSessionID_t>,
    message: *const c_char,
) {
    if message.is_null() {
        return;
    }
    guarded(data, "on_event", (), |host| {
        // SAFETY: the engine passes a NUL-terminated string valid for the callback.
        let text = unsafe { CStr::from_ptr(message) }.to_string_lossy();
        host.callbacks.on_event(&text);
    });
}
