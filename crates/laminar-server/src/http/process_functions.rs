//! Read-only inspection of startup-bound process functions.

use std::sync::Arc;

use axum::extract::State;
use axum::Json;
use serde::Serialize;

use super::state::AppState;

#[derive(Serialize)]
pub(super) struct ProcessFunctionResponse {
    output: String,
    source: String,
    function_id: String,
    pipeline_state_id: String,
    descriptor_version: u32,
    implementation_digest: String,
    runtime: &'static str,
}

pub(super) async fn list_process_functions(
    State(state): State<Arc<AppState>>,
) -> Json<Vec<ProcessFunctionResponse>> {
    let functions = state
        .db
        .process_functions()
        .into_iter()
        .map(|binding| ProcessFunctionResponse {
            output: binding.output_name,
            source: binding.source_name,
            function_id: binding.descriptor.function_id,
            pipeline_state_id: binding.descriptor.pipeline_state_id,
            descriptor_version: binding.descriptor.version,
            implementation_digest: binding.descriptor.implementation_digest,
            runtime: match binding.descriptor.runtime {
                laminar_db::process_function::ProcessRuntime::NativeRust => "native_rust",
                laminar_db::process_function::ProcessRuntime::RemoteRust => "remote_rust",
                laminar_db::process_function::ProcessRuntime::RemotePython => "remote_python",
            },
        })
        .collect();
    Json(functions)
}
