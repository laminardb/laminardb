//! Session-property `SET` statements and coordinator control-mutation
//! acknowledgement resolution shared by stream/MV lifecycle DDL.

use crate::db::{parse_duration_str, LaminarDB};
use crate::error::DbError;
use crate::handle::{DdlInfo, ExecuteResult};
use crate::pipeline::{ControlMutation, ControlMutationState};

pub(crate) const CONTROL_ACK_DEADLINE: std::time::Duration = std::time::Duration::from_secs(5);

enum ControlAck {
    Response(Result<(), DbError>),
    Closed,
    TimedOut,
}

pub(super) async fn resolve_control_ack(
    operation: &str,
    acknowledgement: tokio::sync::oneshot::Receiver<Result<(), DbError>>,
    mutation: &ControlMutation,
) -> Result<(), DbError> {
    let acknowledgement = match tokio::time::timeout(CONTROL_ACK_DEADLINE, acknowledgement).await {
        Ok(Ok(response)) => ControlAck::Response(response),
        Ok(Err(_)) => ControlAck::Closed,
        Err(_) => ControlAck::TimedOut,
    };

    // The mutation CAS, not delivery of the best-effort acknowledgement, is the
    // linearization point. This also closes the timeout/receiver-drop race: either
    // the coordinator applied first or the caller atomically prevents application.
    match mutation.cancel() {
        ControlMutationState::Applied => {
            match acknowledgement {
                ControlAck::Response(Err(ref error)) => tracing::warn!(
                    operation,
                    error = %error,
                    "control mutation was applied before an inconsistent error acknowledgement"
                ),
                ControlAck::Closed => tracing::warn!(
                    operation,
                    "control mutation was applied but its acknowledgement sender closed"
                ),
                ControlAck::TimedOut => tracing::warn!(
                    operation,
                    "control mutation was applied but its acknowledgement missed the deadline"
                ),
                ControlAck::Response(Ok(())) => {}
            }
            Ok(())
        }
        ControlMutationState::Cancelled => match acknowledgement {
            ControlAck::Response(Err(error)) => Err(error),
            ControlAck::Response(Ok(())) => Err(DbError::Pipeline(format!(
                "pipeline acknowledged {operation} without committing it"
            ))),
            ControlAck::Closed => Err(DbError::Pipeline(format!(
                "pipeline stopped before acknowledging {operation}"
            ))),
            ControlAck::TimedOut => Err(DbError::Pipeline(format!(
                "pipeline did not acknowledge {operation} within {} seconds",
                CONTROL_ACK_DEADLINE.as_secs()
            ))),
        },
        ControlMutationState::Pending => {
            unreachable!("cancelling a control mutation must resolve pending state")
        }
    }
}

impl LaminarDB {
    pub(crate) fn handle_set(
        &self,
        set_stmt: &sqlparser::ast::Set,
    ) -> Result<ExecuteResult, DbError> {
        use sqlparser::ast::Set;
        match set_stmt {
            Set::SingleAssignment {
                variable, values, ..
            } => {
                let key = variable.to_string().to_lowercase();
                let value = if values.len() == 1 {
                    values[0].to_string().trim_matches('\'').to_string()
                } else {
                    values
                        .iter()
                        .map(std::string::ToString::to_string)
                        .collect::<Vec<_>>()
                        .join(", ")
                };

                if key == "checkpoint_interval" {
                    return self.handle_set_checkpoint_interval(&value);
                }

                self.session_properties.lock().insert(key.clone(), value);
                Ok(ExecuteResult::Ddl(DdlInfo {
                    statement_type: "SET".to_string(),
                    object_name: key,
                    applied: true,
                }))
            }
            _ => Err(DbError::InvalidOperation(
                "Only SET key = value syntax is supported".to_string(),
            )),
        }
    }

    pub(crate) fn handle_set_checkpoint_interval(
        &self,
        value: &str,
    ) -> Result<ExecuteResult, DbError> {
        let trimmed = value.trim().to_lowercase();
        let interval = if trimmed == "off" || trimmed == "none" || trimmed == "disabled" {
            None
        } else {
            let duration = parse_duration_str(&trimmed).ok_or_else(|| {
                DbError::InvalidOperation(format!(
                    "Invalid checkpoint_interval: '{value}'. Use a duration like '5s', '1m', '30s', or 'off'."
                ))
            })?;
            Some(duration)
        };

        self.session_properties
            .lock()
            .insert("checkpoint_interval".to_string(), value.to_string());

        tracing::info!(?interval, "Checkpoint interval updated via SET");
        Ok(ExecuteResult::Ddl(DdlInfo {
            statement_type: "SET".to_string(),
            object_name: "checkpoint_interval".to_string(),
            applied: true,
        }))
    }
}
