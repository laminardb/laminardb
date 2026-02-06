//! Async FFI callbacks for push-based notifications.
//!
//! Provides callback-based APIs for subscriptions where the database notifies
//! the caller when new data arrives, rather than requiring polling.
//!
//! # Thread Safety
//!
//! - Callbacks are invoked from a background thread
//! - Callbacks for a single subscription are serialized (never concurrent)
//! - After `laminar_subscription_cancel()` returns, no more callbacks will fire
//!
//! # Example
//!
//! ```c
//! #include "laminar.h"
//!
//! void on_data(void* ctx, LaminarRecordBatch* batch, int32_t event_type) {
//!     // Process the batch...
//!     laminar_batch_free(batch);  // Must free
//! }
//!
//! void on_error(void* ctx, int32_t code, const char* msg) {
//!     fprintf(stderr, "Error %d: %s\n", code, msg);
//! }
//!
//! int main() {
//!     LaminarConnection* conn;
//!     laminar_open(&conn);
//!
//!     LaminarSubscriptionHandle* sub;
//!     laminar_subscribe_callback(conn, "SELECT * FROM trades",
//!                                on_data, on_error, NULL, &sub);
//!
//!     // Callbacks fire in background...
//!     sleep(60);
//!
//!     laminar_subscription_cancel(sub);
//!     laminar_subscription_free(sub);
//!     laminar_close(conn);
//! }
//! ```

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

// ============================================================================
// Event Type Constants
// ============================================================================

/// Event type: insert (+1 weight).
pub const LAMINAR_EVENT_INSERT: i32 = 0;
/// Event type: delete (-1 weight).
pub const LAMINAR_EVENT_DELETE: i32 = 1;
/// Event type: update (decomposed to delete + insert).
pub const LAMINAR_EVENT_UPDATE: i32 = 2;
/// Event type: watermark progress (no data).
pub const LAMINAR_EVENT_WATERMARK: i32 = 3;
/// Event type: snapshot (initial state load).
pub const LAMINAR_EVENT_SNAPSHOT: i32 = 4;

// ============================================================================
// Callback Function Types
// ============================================================================

/// Callback function type for subscription data.
///
/// # Arguments
///
/// * `user_data` - Opaque pointer passed to `laminar_subscribe_callback`
/// * `batch` - Record batch (ownership transferred to callback, must free)
/// * `event_type` - One of `LAMINAR_EVENT_*` constants
///
/// # Safety
///
/// The callback must free the batch with `laminar_batch_free`.
pub type LaminarSubscriptionCallback = Option<
    unsafe extern "C" fn(user_data: *mut c_void, batch: *mut LaminarRecordBatch, event_type: i32),
>;

/// Callback function type for errors.
///
/// # Arguments
///
/// * `user_data` - Opaque pointer passed to `laminar_subscribe_callback`
/// * `error_code` - One of `LAMINAR_ERR_*` constants
/// * `error_message` - Error message (valid only during callback)
pub type LaminarErrorCallback = Option<
    unsafe extern "C" fn(user_data: *mut c_void, error_code: i32, error_message: *const c_char),
>;

/// Callback function type for completion notifications.
///
/// # Arguments
///
/// * `user_data` - Opaque pointer passed to the async function
/// * `status` - `LAMINAR_OK` on success, or an error code
/// * `result` - Operation-specific result (may be NULL)
///
/// Note: Currently unused but defined for future async operation support.
#[allow(dead_code)]
pub type LaminarCompletionCallback =
    Option<unsafe extern "C" fn(user_data: *mut c_void, status: i32, result: *mut c_void)>;

// ============================================================================
// Subscription Handle
// ============================================================================

/// Opaque handle for a callback-based subscription.
///
/// Created by `laminar_subscribe_callback`, freed by `laminar_subscription_free`.
#[repr(C)]
pub struct LaminarSubscriptionHandle {
    /// Flag to signal cancellation to the background thread.
    cancelled: Arc<AtomicBool>,
    /// Background thread handle (if still running).
    thread_handle: Option<JoinHandle<()>>,
    /// User data pointer (stored for reference, not owned).
    user_data: *mut c_void,
}

// SAFETY: The handle can be sent between threads. The user_data pointer
// is the caller's responsibility to ensure thread safety.
unsafe impl Send for LaminarSubscriptionHandle {}

impl LaminarSubscriptionHandle {
    /// Create a new subscription handle.
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

    /// Cancel the subscription and wait for the thread to stop.
    fn cancel(&mut self) {
        // Signal cancellation
        self.cancelled.store(true, Ordering::SeqCst);

        // Wait for thread to finish
        if let Some(handle) = self.thread_handle.take() {
            let _ = handle.join();
        }
    }
}

// ============================================================================
// Callback Context
// ============================================================================

/// Internal context passed to the background subscription thread.
struct CallbackContext {
    /// User data pointer.
    user_data: *mut c_void,
    /// Data callback.
    on_data: LaminarSubscriptionCallback,
    /// Error callback.
    on_error: LaminarErrorCallback,
    /// Cancellation flag.
    cancelled: Arc<AtomicBool>,
}

// SAFETY: CallbackContext is only accessed from the subscription thread.
// The user_data pointer is the caller's responsibility.
unsafe impl Send for CallbackContext {}

impl CallbackContext {
    /// Invoke the data callback if set.
    fn call_on_data(&self, batch: LaminarRecordBatch, event_type: i32) {
        if let Some(callback) = self.on_data {
            let batch_ptr = Box::into_raw(Box::new(batch));
            // SAFETY: Callback is provided by FFI caller, we pass valid pointers.
            unsafe {
                callback(self.user_data, batch_ptr, event_type);
            }
        }
    }

    /// Invoke the error callback if set.
    fn call_on_error(&self, error_code: i32, message: &str) {
        if let Some(callback) = self.on_error {
            let c_message = CString::new(message)
                .unwrap_or_else(|_| CString::new("Error message contained null byte").unwrap());
            // SAFETY: Callback is provided by FFI caller, we pass valid pointers.
            unsafe {
                callback(self.user_data, error_code, c_message.as_ptr());
            }
        }
    }

    /// Check if cancellation has been requested.
    fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }
}

// ============================================================================
// FFI Functions
// ============================================================================

/// Create a callback-based subscription.
///
/// The `on_data` callback is invoked from a background thread when new data
/// arrives. The callback receives ownership of the batch and must free it.
///
/// # Arguments
///
/// * `conn` - Database connection
/// * `query` - SQL query string (null-terminated)
/// * `on_data` - Callback for data batches (may be NULL to ignore data)
/// * `on_error` - Callback for errors (may be NULL to ignore errors)
/// * `user_data` - Opaque pointer passed to callbacks
/// * `out` - Pointer to receive subscription handle
///
/// # Returns
///
/// `LAMINAR_OK` on success, or an error code.
///
/// # Safety
///
/// * `conn` must be a valid connection handle
/// * `query` must be a valid null-terminated UTF-8 string
/// * `out` must be a valid pointer
/// * Callbacks must be thread-safe if `user_data` is shared
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

    // Parse query string
    let Ok(query_str) = (unsafe { CStr::from_ptr(query) }).to_str() else {
        return LAMINAR_ERR_INVALID_UTF8;
    };

    // SAFETY: conn is non-null (checked above)
    let conn_ref = unsafe { &(*conn).inner };

    // Create the subscription (using query_stream for streaming results)
    let stream = match conn_ref.query_stream(query_str) {
        Ok(s) => s,
        Err(e) => {
            let code = e.code();
            set_last_error(e);
            return code;
        }
    };

    // Set up cancellation flag
    let cancelled = Arc::new(AtomicBool::new(false));
    let cancelled_clone = Arc::clone(&cancelled);

    // Create callback context
    let ctx = CallbackContext {
        user_data,
        on_data,
        on_error,
        cancelled: cancelled_clone,
    };

    // Spawn background thread to drive the subscription
    let thread_handle = thread::spawn(move || {
        subscription_thread(stream, ctx);
    });

    // Create handle
    let handle = Box::new(LaminarSubscriptionHandle::new(
        cancelled,
        thread_handle,
        user_data,
    ));

    // SAFETY: out is non-null (checked above)
    unsafe { *out = Box::into_raw(handle) };

    LAMINAR_OK
}

/// Background thread function that drives the subscription.
#[allow(clippy::needless_pass_by_value)] // ctx is owned by the thread
fn subscription_thread(mut stream: crate::api::QueryStream, ctx: CallbackContext) {
    loop {
        // Check for cancellation
        if ctx.is_cancelled() {
            break;
        }

        // Try to get next batch (non-blocking to allow cancellation checks)
        match stream.try_next() {
            Ok(Some(batch)) => {
                // Determine event type (default to insert for query results)
                let event_type = LAMINAR_EVENT_INSERT;
                ctx.call_on_data(LaminarRecordBatch::new(batch), event_type);
            }
            Ok(None) => {
                // No data available, check if stream is exhausted
                if !stream.is_active() {
                    break;
                }
                // Brief sleep to avoid busy-waiting
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
            Err(e) => {
                ctx.call_on_error(e.code(), e.message());
                // Continue unless fatal
                if !stream.is_active() {
                    break;
                }
            }
        }
    }
}

/// Cancel a callback-based subscription.
///
/// After this function returns, no more callbacks will be invoked.
/// It is safe to free resources referenced by `user_data` after this returns.
///
/// # Arguments
///
/// * `handle` - Subscription handle
///
/// # Returns
///
/// `LAMINAR_OK` on success, or an error code.
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

    // SAFETY: handle is non-null (checked above)
    let handle_ref = unsafe { &mut *handle };
    handle_ref.cancel();

    LAMINAR_OK
}

/// Check if a subscription is still active.
///
/// # Arguments
///
/// * `handle` - Subscription handle
/// * `out` - Pointer to receive result (true if active)
///
/// # Returns
///
/// `LAMINAR_OK` on success, or an error code.
///
/// # Safety
///
/// * `handle` must be a valid subscription handle
/// * `out` must be a valid pointer
#[no_mangle]
pub unsafe extern "C" fn laminar_subscription_is_active(
    handle: *mut LaminarSubscriptionHandle,
    out: *mut bool,
) -> i32 {
    clear_last_error();

    if handle.is_null() || out.is_null() {
        return LAMINAR_ERR_NULL_POINTER;
    }

    // SAFETY: handle and out are non-null (checked above)
    let handle_ref = unsafe { &*handle };
    let active = !handle_ref.cancelled.load(Ordering::SeqCst) && handle_ref.thread_handle.is_some();

    unsafe { *out = active };

    LAMINAR_OK
}

/// Get the user data pointer from a subscription handle.
///
/// # Arguments
///
/// * `handle` - Subscription handle
///
/// # Returns
///
/// The user data pointer passed to `laminar_subscribe_callback`, or NULL.
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

    // SAFETY: handle is non-null (checked above)
    unsafe { (*handle).user_data }
}

/// Free a subscription handle.
///
/// If the subscription is still active, it will be cancelled first.
///
/// # Arguments
///
/// * `handle` - Subscription handle to free
///
/// # Safety
///
/// `handle` must be a valid handle from `laminar_subscribe_callback`, or NULL.
#[no_mangle]
pub unsafe extern "C" fn laminar_subscription_free(handle: *mut LaminarSubscriptionHandle) {
    if !handle.is_null() {
        // SAFETY: handle is non-null and was allocated by Box
        let mut boxed = unsafe { Box::from_raw(handle) };
        // Cancel if still active
        boxed.cancel();
        drop(boxed);
    }
}

// Note: Async write operations (laminar_writer_write_async) are deferred to a
// future release. They require careful synchronization (Arc<Mutex<Writer>>)
// that is beyond the scope of this initial callback implementation.
// For now, use the synchronous laminar_writer_write() API.

#[cfg(test)]
#[allow(
    clippy::borrow_as_ptr,
    clippy::manual_c_str_literals,
    clippy::items_after_statements
)]
mod tests {
    use super::*;
    use crate::ffi::connection::{laminar_close, laminar_execute, laminar_open};
    use std::ptr;
    use std::sync::atomic::AtomicUsize;

    #[test]
    fn test_event_type_constants() {
        assert_eq!(LAMINAR_EVENT_INSERT, 0);
        assert_eq!(LAMINAR_EVENT_DELETE, 1);
        assert_eq!(LAMINAR_EVENT_UPDATE, 2);
        assert_eq!(LAMINAR_EVENT_WATERMARK, 3);
        assert_eq!(LAMINAR_EVENT_SNAPSHOT, 4);
    }

    #[test]
    fn test_subscribe_null_pointer() {
        let mut out: *mut LaminarSubscriptionHandle = ptr::null_mut();

        // Null connection
        let rc = unsafe {
            laminar_subscribe_callback(
                ptr::null_mut(),
                b"SELECT 1\0".as_ptr().cast(),
                None,
                None,
                ptr::null_mut(),
                &mut out,
            )
        };
        assert_eq!(rc, LAMINAR_ERR_NULL_POINTER);

        // Null query
        let mut conn: *mut LaminarConnection = ptr::null_mut();
        unsafe { laminar_open(&mut conn) };

        let rc = unsafe {
            laminar_subscribe_callback(conn, ptr::null(), None, None, ptr::null_mut(), &mut out)
        };
        assert_eq!(rc, LAMINAR_ERR_NULL_POINTER);

        // Null out
        let rc = unsafe {
            laminar_subscribe_callback(
                conn,
                b"SELECT 1\0".as_ptr().cast(),
                None,
                None,
                ptr::null_mut(),
                ptr::null_mut(),
            )
        };
        assert_eq!(rc, LAMINAR_ERR_NULL_POINTER);

        unsafe { laminar_close(conn) };
    }

    #[test]
    fn test_subscription_cancel_null() {
        let rc = unsafe { laminar_subscription_cancel(ptr::null_mut()) };
        assert_eq!(rc, LAMINAR_ERR_NULL_POINTER);
    }

    #[test]
    fn test_subscription_free_null() {
        // Should not crash
        unsafe { laminar_subscription_free(ptr::null_mut()) };
    }

    #[test]
    fn test_subscription_user_data_null() {
        let result = unsafe { laminar_subscription_user_data(ptr::null_mut()) };
        assert!(result.is_null());
    }

    #[test]
    fn test_subscribe_and_cancel() {
        let mut conn: *mut LaminarConnection = ptr::null_mut();
        let mut sub: *mut LaminarSubscriptionHandle = ptr::null_mut();

        unsafe {
            laminar_open(&mut conn);

            // Create a table for querying
            let sql = b"CREATE TABLE callback_test (id BIGINT)\0";
            laminar_execute(conn, sql.as_ptr().cast(), ptr::null_mut());

            // Subscribe (no callbacks, just test lifecycle)
            let query = b"SELECT * FROM callback_test\0";
            let rc = laminar_subscribe_callback(
                conn,
                query.as_ptr().cast(),
                None,
                None,
                ptr::null_mut(),
                &mut sub,
            );
            assert_eq!(rc, LAMINAR_OK);
            assert!(!sub.is_null());

            // Check active
            let mut active = false;
            let rc = laminar_subscription_is_active(sub, &mut active);
            assert_eq!(rc, LAMINAR_OK);
            // Note: may be false if stream completed immediately

            // Cancel
            let rc = laminar_subscription_cancel(sub);
            assert_eq!(rc, LAMINAR_OK);

            // Should no longer be active
            let rc = laminar_subscription_is_active(sub, &mut active);
            assert_eq!(rc, LAMINAR_OK);
            assert!(!active);

            laminar_subscription_free(sub);
            laminar_close(conn);
        }
    }

    #[test]
    fn test_subscribe_with_user_data() {
        let mut conn: *mut LaminarConnection = ptr::null_mut();
        let mut sub: *mut LaminarSubscriptionHandle = ptr::null_mut();

        // Use a counter as user data
        static COUNTER: AtomicUsize = AtomicUsize::new(42);

        unsafe {
            laminar_open(&mut conn);

            let sql = b"CREATE TABLE userdata_test (id BIGINT)\0";
            laminar_execute(conn, sql.as_ptr().cast(), ptr::null_mut());

            let query = b"SELECT * FROM userdata_test\0";
            let user_data = std::ptr::addr_of!(COUNTER) as *mut c_void;

            let rc = laminar_subscribe_callback(
                conn,
                query.as_ptr().cast(),
                None,
                None,
                user_data,
                &mut sub,
            );
            assert_eq!(rc, LAMINAR_OK);

            // Verify user data is preserved
            let retrieved = laminar_subscription_user_data(sub);
            assert_eq!(retrieved, user_data);

            laminar_subscription_cancel(sub);
            laminar_subscription_free(sub);
            laminar_close(conn);
        }
    }

    // Static counters for callback tests
    static DATA_CALLBACK_COUNT: AtomicUsize = AtomicUsize::new(0);
    static ERROR_CALLBACK_COUNT: AtomicUsize = AtomicUsize::new(0);

    unsafe extern "C" fn test_data_callback(
        _user_data: *mut c_void,
        batch: *mut LaminarRecordBatch,
        _event_type: i32,
    ) {
        DATA_CALLBACK_COUNT.fetch_add(1, Ordering::SeqCst);
        // Must free the batch
        if !batch.is_null() {
            crate::ffi::query::laminar_batch_free(batch);
        }
    }

    unsafe extern "C" fn test_error_callback(
        _user_data: *mut c_void,
        _error_code: i32,
        _error_message: *const c_char,
    ) {
        ERROR_CALLBACK_COUNT.fetch_add(1, Ordering::SeqCst);
    }

    #[test]
    fn test_subscribe_with_callbacks() {
        // Reset counters
        DATA_CALLBACK_COUNT.store(0, Ordering::SeqCst);
        ERROR_CALLBACK_COUNT.store(0, Ordering::SeqCst);

        let mut conn: *mut LaminarConnection = ptr::null_mut();
        let mut sub: *mut LaminarSubscriptionHandle = ptr::null_mut();

        unsafe {
            laminar_open(&mut conn);

            let sql = b"CREATE TABLE callback_data_test (id BIGINT)\0";
            laminar_execute(conn, sql.as_ptr().cast(), ptr::null_mut());

            let query = b"SELECT * FROM callback_data_test\0";
            let rc = laminar_subscribe_callback(
                conn,
                query.as_ptr().cast(),
                Some(test_data_callback),
                Some(test_error_callback),
                ptr::null_mut(),
                &mut sub,
            );
            assert_eq!(rc, LAMINAR_OK);

            // Give a moment for the subscription thread to run
            std::thread::sleep(std::time::Duration::from_millis(50));

            laminar_subscription_cancel(sub);
            laminar_subscription_free(sub);
            laminar_close(conn);
        }

        // Note: callbacks may or may not have fired depending on timing
        // The important thing is no crashes occurred
    }

    #[test]
    fn test_subscription_is_active_null_pointer() {
        let mut active = true;
        let rc = unsafe { laminar_subscription_is_active(ptr::null_mut(), &mut active) };
        assert_eq!(rc, LAMINAR_ERR_NULL_POINTER);

        let mut conn: *mut LaminarConnection = ptr::null_mut();
        let mut sub: *mut LaminarSubscriptionHandle = ptr::null_mut();

        unsafe {
            laminar_open(&mut conn);
            let sql = b"CREATE TABLE active_test (id BIGINT)\0";
            laminar_execute(conn, sql.as_ptr().cast(), ptr::null_mut());

            let query = b"SELECT * FROM active_test\0";
            laminar_subscribe_callback(
                conn,
                query.as_ptr().cast(),
                None,
                None,
                ptr::null_mut(),
                &mut sub,
            );

            // Null out pointer
            let rc = laminar_subscription_is_active(sub, ptr::null_mut());
            assert_eq!(rc, LAMINAR_ERR_NULL_POINTER);

            laminar_subscription_cancel(sub);
            laminar_subscription_free(sub);
            laminar_close(conn);
        }
    }
}
