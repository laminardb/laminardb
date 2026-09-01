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
        let sql = b"CREATE TABLE callback_test (id BIGINT PRIMARY KEY)\0";
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

        let sql = b"CREATE TABLE userdata_test (id BIGINT PRIMARY KEY)\0";
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

        let sql = b"CREATE TABLE callback_data_test (id BIGINT PRIMARY KEY)\0";
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
        let sql = b"CREATE TABLE active_test (id BIGINT PRIMARY KEY)\0";
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
