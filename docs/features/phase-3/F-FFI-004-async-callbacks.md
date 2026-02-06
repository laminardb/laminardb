# F-FFI-004: Async FFI Callbacks

> **Status**: Draft
> **Priority**: P0
> **Dependencies**: F-FFI-001, F-FFI-003

## Overview

Provide callback-based async notification APIs for C FFI consumers. This enables
non-blocking subscription patterns where the database notifies the caller when
new data is available, rather than requiring polling.

## Goals

1. **Subscription callbacks** - notify when new data arrives on subscriptions
2. **Completion callbacks** - notify when async operations complete
3. **Thread-safe delivery** - callbacks may be invoked from background threads
4. **Cancellation support** - ability to unregister callbacks

## Non-Goals

- True async/await syntax (not possible in C FFI)
- Callback execution on specific threads (caller must handle thread affinity)

## Design

### Callback Function Types

```c
// Subscription data callback
typedef void (*laminar_subscription_callback)(
    void* user_data,
    LaminarRecordBatch* batch,  // Owned by callee, must free
    int32_t event_type          // 0=insert, 1=delete, 2=update, 3=watermark, 4=snapshot
);

// Error callback
typedef void (*laminar_error_callback)(
    void* user_data,
    int32_t error_code,
    const char* error_message  // Valid only during callback
);

// Completion callback (for async operations)
typedef void (*laminar_completion_callback)(
    void* user_data,
    int32_t status,            // LAMINAR_OK or error code
    void* result               // Operation-specific result (may be NULL)
);
```

### Subscription with Callbacks

```c
// Subscribe with callbacks
int32_t laminar_subscribe_callback(
    LaminarConnection* conn,
    const char* query,
    laminar_subscription_callback on_data,
    laminar_error_callback on_error,
    void* user_data,
    LaminarSubscriptionHandle** out
);

// Unsubscribe and stop callbacks
int32_t laminar_subscription_cancel(
    LaminarSubscriptionHandle* handle
);

// Free subscription handle
void laminar_subscription_free(
    LaminarSubscriptionHandle* handle
);
```

### Usage Example (C)

```c
void on_data(void* ctx, LaminarRecordBatch* batch, int32_t event_type) {
    MyContext* my_ctx = (MyContext*)ctx;

    // Process the batch
    size_t rows;
    laminar_batch_num_rows(batch, &rows);
    printf("Received %zu rows, event_type=%d\n", rows, event_type);

    // Must free the batch
    laminar_batch_free(batch);
}

void on_error(void* ctx, int32_t code, const char* msg) {
    fprintf(stderr, "Subscription error %d: %s\n", code, msg);
}

int main() {
    LaminarConnection* conn;
    laminar_open(&conn);

    MyContext ctx = { .count = 0 };
    LaminarSubscriptionHandle* sub;

    int32_t rc = laminar_subscribe_callback(
        conn,
        "SELECT * FROM trades WHERE symbol = 'AAPL'",
        on_data,
        on_error,
        &ctx,
        &sub
    );

    if (rc == LAMINAR_OK) {
        // Do other work while callbacks fire...
        sleep(60);

        // Stop receiving callbacks
        laminar_subscription_cancel(sub);
        laminar_subscription_free(sub);
    }

    laminar_close(conn);
}
```

## Implementation

### Event Type Constants

```c
#define LAMINAR_EVENT_INSERT    0
#define LAMINAR_EVENT_DELETE    1
#define LAMINAR_EVENT_UPDATE    2
#define LAMINAR_EVENT_WATERMARK 3
#define LAMINAR_EVENT_SNAPSHOT  4
```

### Thread Safety

- Callbacks are invoked from a background thread managed by the subscription
- The callback must be thread-safe if accessing shared state
- The `user_data` pointer is passed through without modification
- Batch ownership transfers to the callback; callee must free

### Synchronization Contract

1. Callbacks are serialized (never called concurrently for the same subscription)
2. After `laminar_subscription_cancel()` returns, no more callbacks will fire
3. It is safe to free `user_data` after cancel returns

## API Reference

| Function | Description |
|----------|-------------|
| `laminar_subscribe_callback` | Create subscription with callbacks |
| `laminar_subscription_cancel` | Stop receiving callbacks |
| `laminar_subscription_free` | Free subscription handle |

## Testing

1. Subscription receives inserts via callback
2. Error callback fires on subscription error
3. Cancel stops callbacks
4. Thread safety under concurrent inserts
5. Memory leak check (valgrind)

## Migration

- Existing polling API (`laminar_stream_*`) remains available
- Callbacks are an additional option for push-based notification
