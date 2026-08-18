//! Subscription cursors: connection-scoped `DECLARE CURSOR FOR SUBSCRIBE`
//! state and strict-PG `FETCH`/`CLOSE` handling.
//!
//! INVARIANT: cursors live only inside an explicit transaction; the exhausted
//! flag is the reap signal so dead cursors never leak past the next command.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use futures::stream;
use pgwire::api::results::{FieldInfo, QueryResponse, Response, Tag};
use pgwire::error::PgWireResult;
use sqlparser::ast::{CloseCursor, FetchDirection, Value as AstValue};
use tokio::sync::Mutex as TokioMutex;

use laminar_db::subscription::{PortalFrame, SubscriptionPortal};
use laminar_db::LaminarDB;

use super::dispatch::user_error;
use super::subscription::{
    encode_subscription_progress_row, open_portal_for_subscribe, subscription_field_infos,
    BatchCursor,
};

pub(crate) const SUBSCRIPTION_FETCH_WAIT: std::time::Duration = std::time::Duration::from_secs(1);
pub(crate) const SUBSCRIPTION_MAX_FETCH_ROWS: u64 = 1024;

/// State that shares the cursor's lifetime: the portal, the leftover-row
/// buffer, and the exhausted flag. Held by `Arc` so a row stream can keep
/// reading after `ConnState::get` returns.
struct CursorInner {
    state: TokioMutex<CursorState>,
    /// Flipped when the portal emits `None`, `Lagged`, or `Error` so the next
    /// command can reap the cursor.
    exhausted: AtomicBool,
}

struct CursorState {
    portal: SubscriptionPortal,
    batch: Option<BatchCursor>,
}

#[derive(Clone)]
pub(super) struct ActiveCursor {
    inner: Arc<CursorInner>,
    schema: arrow_schema::SchemaRef,
}

#[derive(Default)]
pub(super) struct ConnState {
    cursors: parking_lot::Mutex<HashMap<String, ActiveCursor>>,
}

impl ConnState {
    /// Cursor names follow PG identifier folding: unquoted → lowercase. We
    /// don't track `quote_style`, so quoted-mixed-case cursors collapse too —
    /// good enough for the `\set FETCH_COUNT` case this targets.
    fn key(name: &str) -> String {
        name.to_ascii_lowercase()
    }

    fn insert(&self, name: &str, cursor: ActiveCursor) {
        self.cursors.lock().insert(Self::key(name), cursor);
    }

    fn contains(&self, name: &str) -> bool {
        self.cursors.lock().contains_key(&Self::key(name))
    }

    fn get(&self, name: &str) -> Option<ActiveCursor> {
        self.cursors.lock().get(&Self::key(name)).cloned()
    }

    fn remove(&self, name: &str) -> bool {
        self.cursors.lock().remove(&Self::key(name)).is_some()
    }

    pub(super) fn drop_all(&self) {
        self.cursors.lock().clear();
    }

    pub(super) fn prune_dead(&self) {
        let mut cursors = self.cursors.lock();
        cursors.retain(|_, c| !c.inner.exhausted.load(Ordering::Acquire));
    }
}

/// Open a SUBSCRIBE behind a cursor name. Rejects with 42P03 if the name is
/// already in use on this connection (matches PG; user must `CLOSE` first).
pub(super) async fn handle_declare_cursor(
    db: &LaminarDB,
    state: &ConnState,
    cursor_name: &str,
    subscribe: laminar_sql::parser::SubscribeStatement,
) -> PgWireResult<Response> {
    if state.contains(cursor_name) {
        return Err(user_error(
            "42P03",
            format!("cursor \"{cursor_name}\" already exists"),
        ));
    }
    let portal = open_portal_for_subscribe(db, &subscribe).await?;
    let schema = portal.schema();
    state.insert(
        cursor_name,
        ActiveCursor {
            inner: Arc::new(CursorInner {
                state: TokioMutex::new(CursorState {
                    portal,
                    batch: None,
                }),
                exhausted: AtomicBool::new(false),
            }),
            schema,
        },
    );
    Ok(Response::Execution(Tag::new("DECLARE CURSOR")))
}

/// `FETCH NEXT` and bare `FETCH FORWARD` map to a single row, matching PG.
pub(super) fn fetch_direction_count(dir: &FetchDirection) -> PgWireResult<FetchTarget> {
    match dir {
        FetchDirection::Next | FetchDirection::Forward { limit: None } => Ok(FetchTarget::Count(1)),
        FetchDirection::Count { limit } | FetchDirection::Forward { limit: Some(limit) } => {
            let count = value_to_u64(limit)?;
            if count > SUBSCRIPTION_MAX_FETCH_ROWS {
                return Err(user_error(
                    "22023",
                    format!(
                        "FETCH count exceeds the bounded subscription limit of {SUBSCRIPTION_MAX_FETCH_ROWS} rows"
                    ),
                ));
            }
            Ok(FetchTarget::Count(count))
        }
        FetchDirection::All | FetchDirection::ForwardAll => Err(user_error(
            "0A000",
            "FETCH ALL is not supported for subscriptions; request a positive bounded row count",
        )),
        FetchDirection::Prior
        | FetchDirection::First
        | FetchDirection::Last
        | FetchDirection::Absolute { .. }
        | FetchDirection::Relative { .. }
        | FetchDirection::Backward { .. }
        | FetchDirection::BackwardAll => Err(user_error(
            "0A000",
            "FETCH direction not supported (SUBSCRIBE cursors are forward-only): use FORWARD or NEXT",
        )),
    }
}

#[derive(Copy, Clone)]
pub(super) enum FetchTarget {
    Count(u64),
}

fn value_to_u64(v: &AstValue) -> PgWireResult<u64> {
    match v {
        AstValue::Number(n, _) => n
            .parse::<u64>()
            .map_err(|_| user_error("22023", format!("invalid FETCH count: {n}"))),
        other => Err(user_error(
            "22023",
            format!("FETCH count must be an integer, got {other}"),
        )),
    }
}

pub(super) fn handle_fetch(
    state: &ConnState,
    cursor_name: &str,
    target: FetchTarget,
) -> PgWireResult<Response> {
    let cursor = state
        .get(cursor_name)
        .ok_or_else(|| user_error("34000", format!("cursor \"{cursor_name}\" does not exist")))?;
    Ok(fetch_response(cursor, target))
}

pub(super) fn handle_close(state: &ConnState, cursor: &CloseCursor) -> PgWireResult<Response> {
    match cursor {
        CloseCursor::All => {
            state.drop_all();
            Ok(Response::Execution(Tag::new("CLOSE CURSOR ALL")))
        }
        CloseCursor::Specific { name } => {
            if state.remove(&name.value) {
                Ok(Response::Execution(Tag::new("CLOSE CURSOR")))
            } else {
                Err(user_error(
                    "34000",
                    format!("cursor \"{}\" does not exist", name.value),
                ))
            }
        }
    }
}

/// Strict-PG FETCH: blocks until `target` rows are produced, the portal exits,
/// or the subscription faults. Text format only; SimpleQuery has no binary.
/// A partially consumed Arrow batch remains on the cursor for the next FETCH.
pub(super) fn fetch_response(cursor: ActiveCursor, target: FetchTarget) -> Response {
    let fields = Arc::new(subscription_field_infos(&cursor.schema, None));
    let data_columns = cursor.schema.fields().len();
    let FetchTarget::Count(remaining) = target;

    struct State {
        cursor: ActiveCursor,
        fields: Arc<Vec<FieldInfo>>,
        remaining: u64,
    }

    let init = State {
        cursor,
        fields: Arc::clone(&fields),
        remaining,
    };

    let row_stream = stream::unfold(init, move |mut s| async move {
        loop {
            if s.remaining == 0 {
                return None;
            }
            if s.cursor.inner.exhausted.load(Ordering::Acquire) {
                return None;
            }

            let mut cursor_state = s.cursor.inner.state.lock().await;
            if let Some(batch) = cursor_state.batch.as_mut() {
                if let Some(row) = batch.next_row(&s.fields) {
                    let failed = row.is_err();
                    if failed || batch.is_exhausted() {
                        cursor_state.batch = None;
                    }
                    if failed {
                        s.cursor.inner.exhausted.store(true, Ordering::Release);
                    }
                    drop(cursor_state);
                    if !failed {
                        s.remaining = s.remaining.saturating_sub(1);
                    }
                    return Some((row, s));
                }
                cursor_state.batch = None;
            }

            let next = match tokio::time::timeout(
                SUBSCRIPTION_FETCH_WAIT,
                cursor_state.portal.next_frame(),
            )
            .await
            {
                Ok(next) => next,
                Err(_) => {
                    drop(cursor_state);
                    return None;
                }
            };
            match next {
                None => {
                    drop(cursor_state);
                    s.cursor.inner.exhausted.store(true, Ordering::Release);
                    return None;
                }
                Some(PortalFrame::Batch {
                    batch,
                    sequence,
                    lease,
                }) if batch.num_rows() > 0 => {
                    cursor_state.batch = Some(BatchCursor::new(batch, sequence, lease));
                }
                Some(PortalFrame::Batch { .. }) => {}
                Some(PortalFrame::Barrier {
                    sequence,
                    epoch,
                    checkpoint_id,
                    through_sequence,
                }) => {
                    let row = encode_subscription_progress_row(
                        &s.fields,
                        data_columns,
                        sequence,
                        epoch,
                        checkpoint_id,
                        through_sequence,
                    );
                    let failed = row.is_err();
                    drop(cursor_state);
                    if failed {
                        s.cursor.inner.exhausted.store(true, Ordering::Release);
                    } else {
                        s.remaining = s.remaining.saturating_sub(1);
                    }
                    return Some((row, s));
                }
                Some(PortalFrame::Lagged(n)) => {
                    drop(cursor_state);
                    s.cursor.inner.exhausted.store(true, Ordering::Release);
                    let err = user_error(
                        "54000",
                        format!("subscription lagged: skipped {n} messages, terminating cursor"),
                    );
                    return Some((Err(err), s));
                }
                Some(PortalFrame::Error { message }) => {
                    drop(cursor_state);
                    s.cursor.inner.exhausted.store(true, Ordering::Release);
                    let err = user_error(
                        "XX000",
                        format!("subscription failed: {message}; terminating cursor"),
                    );
                    return Some((Err(err), s));
                }
            }
        }
    });
    Response::Query(QueryResponse::new(fields, row_stream))
}
