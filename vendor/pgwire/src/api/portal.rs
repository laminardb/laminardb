use std::fmt::Debug;
use std::ops::DerefMut;
use std::sync::Arc;

use bytes::Bytes;
use futures::{FutureExt, stream::StreamExt};
use postgres_types::FromSqlOwned;
use tokio::sync::Mutex;

use crate::api::Type;
use crate::api::results::QueryResponse;
use crate::error::{PgWireError, PgWireResult};
use crate::messages::data::FORMAT_CODE_BINARY;
use crate::messages::extendedquery::Bind;
use crate::types::FromSqlText;
use crate::types::format::FormatOptions;

use super::DEFAULT_NAME;
use super::results::FieldFormat;
use super::stmt::StoredStatement;

const PORTAL_FETCH_MAX_ROWS: usize = 1024;
const PORTAL_FETCH_MAX_BYTES: usize = 8 * 1024 * 1024;
const PORTAL_FETCH_WAIT: std::time::Duration = std::time::Duration::from_secs(1);

/// Represent a prepared sql statement and its parameters bound by a `Bind`
/// request.
#[non_exhaustive]
#[derive(Debug, Default)]
pub struct Portal<S> {
    pub name: String,
    pub statement: Arc<StoredStatement<S>>,
    pub parameter_format: Format,
    pub parameters: Vec<Option<Bytes>>,
    pub result_column_format: Format,
    pub state: Arc<Mutex<PortalExecutionState>>,
}

/// Execution state of a portal during extended query processing.
#[derive(Default, Debug)]
pub enum PortalExecutionState {
    #[default]
    Initial,
    // tag, data stream, cumulative row count, and a row deferred by byte admission
    Suspended {
        response: QueryResponse,
        rows_sent: usize,
        pending_row: Option<crate::messages::data::DataRow>,
    },
    Finished,
}

/// Result of fetching rows from a portal in `Suspended` state.
#[derive(Debug)]
pub struct FetchResult {
    pub response: QueryResponse,
    pub suspended: bool,
    pub total_rows: usize,
}

/// Column format specification for parameters or result columns.
#[derive(Debug, Clone, Default)]
pub enum Format {
    #[default]
    UnifiedText,
    UnifiedBinary,
    Individual(Vec<i16>),
}

impl From<i16> for Format {
    fn from(v: i16) -> Format {
        if v == FORMAT_CODE_BINARY {
            Format::UnifiedBinary
        } else {
            Format::UnifiedText
        }
    }
}

impl Format {
    /// Get format code for given index
    pub fn format_for(&self, idx: usize) -> FieldFormat {
        match self {
            Format::UnifiedText => FieldFormat::Text,
            Format::UnifiedBinary => FieldFormat::Binary,
            Format::Individual(fv) => FieldFormat::from(fv[idx]),
        }
    }

    /// Test if `idx` field is text format
    pub fn is_text(&self, idx: usize) -> bool {
        self.format_for(idx) == FieldFormat::Text
    }

    /// Test if `idx` field is binary format
    pub fn is_binary(&self, idx: usize) -> bool {
        self.format_for(idx) == FieldFormat::Binary
    }

    fn from_codes(codes: &[i16]) -> Self {
        if codes.is_empty() {
            Format::UnifiedText
        } else if codes.len() == 1 {
            Format::from(codes[0])
        } else {
            Format::Individual(codes.to_vec())
        }
    }
}

impl<S: Clone> Portal<S> {
    /// Try to create portal from bind command and current client state
    pub fn try_new(bind: &Bind, statement: Arc<StoredStatement<S>>) -> PgWireResult<Self> {
        let portal_name = bind
            .portal_name
            .clone()
            .unwrap_or_else(|| DEFAULT_NAME.to_owned());

        // param format
        let param_format = Format::from_codes(&bind.parameter_format_codes);

        // format
        let result_format = Format::from_codes(&bind.result_column_format_codes);

        Ok(Portal {
            name: portal_name,
            statement,
            parameter_format: param_format,
            parameters: bind.parameters.clone(),
            result_column_format: result_format,
            state: Arc::new(Mutex::new(PortalExecutionState::Initial)),
        })
    }

    /// Create a cursor-oriented portal with a stored statement.
    ///
    /// The portal starts in `Initial` state. The first call to [`fetch()`](Self::fetch)
    /// after [`start()`](Self::start) will begin returning rows.
    /// Use [`start()`](Self::start) to provide a `QueryResponse` before fetching.
    pub fn new_cursor(name: String, statement: Arc<StoredStatement<S>>) -> Self {
        Portal {
            name,
            statement,
            parameter_format: Format::UnifiedText,
            parameters: vec![],
            result_column_format: Format::UnifiedText,
            state: Arc::new(Mutex::new(PortalExecutionState::Initial)),
        }
    }

    /// Get number of parameters
    pub fn parameter_len(&self) -> usize {
        self.parameters.len()
    }

    /// Attempt to get parameter at given index as type `T`.
    ///
    pub fn parameter<'a, T>(&'a self, idx: usize, pg_type: &Type) -> PgWireResult<Option<T>>
    where
        T: FromSqlOwned + FromSqlText<'a>,
    {
        if !T::accepts(pg_type) {
            return Err(PgWireError::InvalidRustTypeForParameter(
                pg_type.name().to_owned(),
            ));
        }

        let param = self
            .parameters
            .get(idx)
            .ok_or_else(|| PgWireError::ParameterIndexOutOfBound(idx))?;

        let _format = self.parameter_format.format_for(idx);

        if let Some(param) = param {
            if self.parameter_format.is_binary(idx) {
                T::from_sql(pg_type, param)
                    .map(|v| Some(v))
                    .map_err(PgWireError::FailedToParseParameter)
            } else {
                T::from_sql_text(pg_type, param, &FormatOptions::default())
                    .map(|v| Some(v))
                    .map_err(PgWireError::FailedToParseParameter)
            }
        } else {
            // Null
            Ok(None)
        }
    }

    /// Get a handle to the portal's execution state.
    pub fn state(&self) -> Arc<Mutex<PortalExecutionState>> {
        self.state.clone()
    }

    /// Transition the portal from `Initial` to `Suspended` with the given
    /// query response.
    ///
    /// This is called by the query handler after executing the portal's
    /// statement, before calling [`fetch()`](Self::fetch) to retrieve rows.
    pub async fn start(&self, response: QueryResponse) {
        let mut state = self.state.lock().await;
        *state = PortalExecutionState::Suspended {
            response,
            rows_sent: 0,
            pending_row: None,
        };
    }

    /// Fetch up to `max_rows` from a portal's suspended state.
    ///
    /// Returns a [`FetchResult`] containing the rows, the row schema, and
    /// whether the portal is still suspended (has more rows). When the
    /// underlying stream is exhausted, the portal transitions to `Finished`.
    /// When `max_rows` is 0, all remaining rows are fetched.
    ///
    /// Returns an error if the portal is in `Initial` state (call
    /// [`start()`](Self::start) first) or if the stream yields an error.
    pub async fn fetch(&self, max_rows: usize) -> PgWireResult<FetchResult> {
        let mut state = self.state.lock().await;
        let current = std::mem::replace(state.deref_mut(), PortalExecutionState::Finished);

        let PortalExecutionState::Suspended {
            mut response,
            mut rows_sent,
            mut pending_row,
        } = current
        else {
            return match current {
                PortalExecutionState::Initial => {
                    *state = PortalExecutionState::Initial;
                    Err(PgWireError::PortalNotStarted)
                }
                PortalExecutionState::Finished => Ok(FetchResult {
                    response: QueryResponse::new(Arc::new(vec![]), futures::stream::empty()),
                    suspended: false,
                    total_rows: 0,
                }),
                PortalExecutionState::Suspended { .. } => unreachable!(),
            };
        };

        let command_tag = response.command_tag().to_owned();
        let row_schema = response.row_schema();
        let row_limit = if max_rows == 0 {
            PORTAL_FETCH_MAX_ROWS
        } else {
            max_rows.min(PORTAL_FETCH_MAX_ROWS)
        };
        let mut rows = Vec::with_capacity(row_limit.min(256));
        let mut encoded_bytes = 0usize;
        let mut exhausted = false;

        while rows.len() < row_limit {
            let next = if let Some(row) = pending_row.take() {
                Some(Ok(row))
            } else if rows.is_empty() {
                match tokio::time::timeout(PORTAL_FETCH_WAIT, response.data_rows().next()).await {
                    Ok(next) => next,
                    Err(_) => break,
                }
            } else {
                match response.data_rows().next().now_or_never() {
                    Some(next) => next,
                    None => break,
                }
            };

            let Some(row) = next else {
                exhausted = true;
                break;
            };
            let row = match row {
                Ok(row) => row,
                Err(error) => {
                    *state = PortalExecutionState::Finished;
                    return Err(error);
                }
            };
            let row_bytes = row.data.len().saturating_add(7);
            if row_bytes > PORTAL_FETCH_MAX_BYTES {
                *state = PortalExecutionState::Finished;
                return Err(PgWireError::MessageTooLarge(
                    PORTAL_FETCH_MAX_BYTES,
                    row_bytes,
                ));
            }
            if encoded_bytes.saturating_add(row_bytes) > PORTAL_FETCH_MAX_BYTES {
                pending_row = Some(row);
                break;
            }

            encoded_bytes += row_bytes;
            rows.push(row);
            rows_sent = rows_sent.saturating_add(1);
        }

        let suspended = !exhausted;
        if suspended {
            *state = PortalExecutionState::Suspended {
                response,
                rows_sent,
                pending_row,
            };
        }

        let result_response = QueryResponse {
            command_tag,
            row_schema,
            data_rows: Box::pin(futures::stream::iter(rows.into_iter().map(Ok))),
        };

        Ok(FetchResult {
            response: result_response,
            suspended,
            total_rows: rows_sent,
        })
    }
}

#[cfg(test)]
mod tests {
    use futures::{StreamExt, stream};
    use postgres_types::FromSql;

    use super::*;
    use crate::messages::data::DataRow;

    #[test]
    fn test_from_sql() {
        assert_eq!(
            "helloworld",
            String::from_sql(&Type::UNKNOWN, "helloworld".as_bytes()).unwrap()
        )
    }

    fn test_portal() -> Portal<()> {
        Portal::new_cursor(
            "portal".to_owned(),
            Arc::new(StoredStatement::new("statement".to_owned(), (), vec![])),
        )
    }

    #[tokio::test]
    async fn hostile_row_count_has_bounded_fetch() {
        let portal = test_portal();
        let rows = stream::iter((0..5000).map(|_| Ok(DataRow::default())));
        portal
            .start(QueryResponse::new(Arc::new(vec![]), rows))
            .await;

        let mut result = portal.fetch(usize::MAX).await.unwrap();

        assert!(result.suspended);
        assert_eq!(result.total_rows, PORTAL_FETCH_MAX_ROWS);
        assert_eq!(
            result.response.data_rows().count().await,
            PORTAL_FETCH_MAX_ROWS
        );
    }

    #[tokio::test]
    async fn row_stream_error_makes_portal_terminal() {
        let portal = test_portal();
        let rows = stream::once(async {
            Err(PgWireError::MalformedMessage("injected portal row failure"))
        });
        portal
            .start(QueryResponse::new(Arc::new(vec![]), rows))
            .await;

        assert!(portal.fetch(1).await.is_err());
        let result = portal.fetch(1).await.unwrap();
        assert!(!result.suspended);
        assert_eq!(result.total_rows, 0);
    }
}
