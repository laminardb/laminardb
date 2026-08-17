//! Async FFI callbacks for push-based notifications.

use std::ffi::{c_char, c_void, CStr, CString};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};

use super::connection::LaminarConnection;
use super::error::{
    clear_last_error, set_last_error, LAMINAR_ERR_INVALID_UTF8, LAMINAR_ERR_NULL_POINTER,
    LAMINAR_OK,
};
use super::query::LaminarRecordBatch;

/// Event type: insert (+1 weight).
pub const LAMINAR_EVENT_INSERT: i32 = 0;
/// Event type: delete (-1 weight).
pub const LAMINAR_EVENT_DELETE: i32 = 1;
/// Event type: update (delete + insert pair).
pub const LAMINAR_EVENT_UPDATE: i32 = 2;
/// Event type: watermark progress.
pub const LAMINAR_EVENT_WATERMARK: i32 = 3;
/// Event type: initial-state snapshot.
pub const LAMINAR_EVENT_SNAPSHOT: i32 = 4;

/// Callback invoked from the subscription thread with each batch.
///
/// # Safety
///
/// The callback receives ownership of `batch` and must free it with `laminar_batch_free`.
pub type LaminarSubscriptionCallback = Option<
    unsafe extern "C" fn(user_data: *mut c_void, batch: *mut LaminarRecordBatch, event_type: i32),
>;

/// Callback invoked when a subscription error occurs.
pub type LaminarErrorCallback = Option<
    unsafe extern "C" fn(user_data: *mut c_void, error_code: i32, error_message: *const c_char),
>;

/// Opaque handle for a callback-based subscription.
///
/// Created by `laminar_subscribe_callback`, freed by `laminar_subscription_free`.
#[repr(C)]
pub struct LaminarSubscriptionHandle {
    cancelled: Arc<AtomicBool>,
    thread_handle: Option<JoinHandle<()>>,
    // Not owned; caller is responsible for lifetime.
    user_data: *mut c_void,
}

// SAFETY: user_data thread-safety is the caller's responsibility.
unsafe impl Send for LaminarSubscriptionHandle {}

impl LaminarSubscriptionHandle {
    fn new(
        cancelled: Arc<AtomicBool>,
        thread_handle: JoinHandle<()>,
        user_data: *mut c_void,
    ) -> Self {
        Self {
            cancelled,
            thread_handle: Some(thread_handle),
            user_data,
        }
    }

    fn cancel(&mut self) {
        self.cancelled.store(true, Ordering::SeqCst);
        if let Some(handle) = self.thread_handle.take() {
            let _ = handle.join();
        }
    }
}

struct CallbackContext {
    user_data: *mut c_void,
    on_data: LaminarSubscriptionCallback,
    on_error: LaminarErrorCallback,
    cancelled: Arc<AtomicBool>,
}

// SAFETY: only accessed from the subscription thread; user_data is caller's responsibility.
unsafe impl Send for CallbackContext {}

impl CallbackContext {
    fn call_on_data(&self, batch: LaminarRecordBatch, event_type: i32) {
        if let Some(callback) = self.on_data {
            let batch_ptr = Box::into_raw(Box::new(batch));
            unsafe { callback(self.user_data, batch_ptr, event_type) };
        }
    }

    fn call_on_error(&self, error_code: i32, message: &str) {
        if let Some(callback) = self.on_error {
            let c_message = CString::new(message)
                .unwrap_or_else(|_| CString::new("Error message contained null byte").unwrap());
            unsafe { callback(self.user_data, error_code, c_message.as_ptr()) };
        }
    }

    fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }
}

/// Create a callback-based subscription; `on_data` is called from a background thread.
///
/// # Safety
///
/// `conn`, `query`, and `out` must be valid non-null pointers; `query` must be a
/// null-terminated UTF-8 string; callbacks must be thread-safe if `user_data` is shared.
#[no_mangle]
pub unsafe extern "C" fn laminar_subscribe_callback(
    conn: *mut LaminarConnection,
    query: *const c_char,
    on_data: LaminarSubscriptionCallback,
    on_error: LaminarErrorCallback,
    user_data: *mut c_void,
    out: *mut *mut LaminarSubscriptionHandle,
) -> i32 {
    clear_last_error();

    if conn.is_null() || query.is_null() || out.is_null() {
        return LAMINAR_ERR_NULL_POINTER;
    }

    let Ok(query_str) = (unsafe { CStr::from_ptr(query) }).to_str() else {
        return LAMINAR_ERR_INVALID_UTF8;
    };

    let conn_ref = unsafe { &(*conn).inner };

    let stream = match conn_ref.query_stream(query_str) {
        Ok(s) => s,
        Err(e) => {
            let code = e.code();
            set_last_error(e);
            return code;
        }
    };

    let cancelled = Arc::new(AtomicBool::new(false));
    let cancelled_clone = Arc::clone(&cancelled);

    let ctx = CallbackContext {
        user_data,
        on_data,
        on_error,
        cancelled: cancelled_clone,
    };

    let thread_handle = thread::spawn(move || {
        subscription_thread(stream, ctx);
    });

    let handle = Box::new(LaminarSubscriptionHandle::new(
        cancelled,
        thread_handle,
        user_data,
    ));

    unsafe { *out = Box::into_raw(handle) };

    LAMINAR_OK
}

#[allow(clippy::needless_pass_by_value)]
fn subscription_thread(mut stream: crate::api::QueryStream, ctx: CallbackContext) {
    loop {
        if ctx.is_cancelled() {
            break;
        }

        match stream.try_next() {
            Ok(Some(batch)) => {
                ctx.call_on_data(LaminarRecordBatch::new(batch), LAMINAR_EVENT_INSERT);
            }
            Ok(None) => {
                if !stream.is_active() {
                    break;
                }
                // Poll interval — avoid busy spin.
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
            Err(e) => {
                ctx.call_on_error(e.code(), e.message());
                if !stream.is_active() {
                    break;
                }
            }
        }
    }
}

/// Cancel a callback-based subscription; no callbacks will fire after this returns.
///
/// # Safety
///
/// `handle` must be a valid subscription handle.
#[no_mangle]
pub unsafe extern "C" fn laminar_subscription_cancel(
    handle: *mut LaminarSubscriptionHandle,
) -> i32 {
    clear_last_error();

    if handle.is_null() {
        return LAMINAR_ERR_NULL_POINTER;
    }

    let handle_ref = unsafe { &mut *handle };
    handle_ref.cancel();

    LAMINAR_OK
}

/// Check if a subscription is still active.
///
/// # Safety
///
/// `handle` and `out` must be valid non-null pointers.
#[no_mangle]
pub unsafe extern "C" fn laminar_subscription_is_active(
    handle: *mut LaminarSubscriptionHandle,
    out: *mut bool,
) -> i32 {
    clear_last_error();

    if handle.is_null() || out.is_null() {
        return LAMINAR_ERR_NULL_POINTER;
    }

    let handle_ref = unsafe { &*handle };
    let active = !handle_ref.cancelled.load(Ordering::SeqCst) && handle_ref.thread_handle.is_some();

    unsafe { *out = active };

    LAMINAR_OK
}

/// Return the user data pointer from a subscription handle, or NULL.
///
/// # Safety
///
/// `handle` must be a valid subscription handle or NULL.
#[no_mangle]
pub unsafe extern "C" fn laminar_subscription_user_data(
    handle: *mut LaminarSubscriptionHandle,
) -> *mut c_void {
    if handle.is_null() {
        return std::ptr::null_mut();
    }

    unsafe { (*handle).user_data }
}

/// Free a subscription handle, cancelling it first if still active.
///
/// # Safety
///
/// `handle` must be a valid handle from `laminar_subscribe_callback`, or NULL.
#[no_mangle]
pub unsafe extern "C" fn laminar_subscription_free(handle: *mut LaminarSubscriptionHandle) {
    if !handle.is_null() {
        let mut boxed = unsafe { Box::from_raw(handle) };
        boxed.cancel();
        drop(boxed);
    }
}

#[cfg(test)]
#[allow(
    clippy::borrow_as_ptr,
    clippy::manual_c_str_literals,
    clippy::items_after_statements
)]
mod tests;
