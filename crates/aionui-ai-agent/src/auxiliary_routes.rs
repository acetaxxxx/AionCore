use std::sync::Arc;

use axum::Router;
use axum::extract::rejection::JsonRejection;
use axum::extract::{Extension, Json, Path, Query, State};
use axum::routing::{get, post};

use aionui_api_types::{AgentModeResponse, ApiResponse, SetModeRequest};
use aionui_auth::CurrentUser;
use aionui_common::{AgentType, AppError};
use serde::{Deserialize, Serialize};

use crate::acp_agent::AcpAgentManager;
use crate::agent_manager::AgentManagerHandle;
use crate::openclaw_agent::OpenClawAgentManager;
use crate::task_manager::IWorkerTaskManager;
use crate::types::SlashCommandItem;

/// Router state for auxiliary conversation routes.
#[derive(Clone)]
pub struct AuxiliaryRouterState {
    pub worker_task_manager: Arc<dyn IWorkerTaskManager>,
}

/// Build the auxiliary routes router.
pub fn auxiliary_routes(state: AuxiliaryRouterState) -> Router {
    Router::new()
        .route("/api/conversations/{id}/workspace", get(browse_workspace))
        .route("/api/conversations/{id}/side-question", post(side_question))
        .route(
            "/api/conversations/{id}/reload-context",
            post(reload_context),
        )
        .route(
            "/api/conversations/{id}/slash-commands",
            get(get_slash_commands),
        )
        .route(
            "/api/conversations/{id}/mode",
            get(get_agent_mode).put(set_agent_mode),
        )
        .route(
            "/api/conversations/{id}/openclaw/runtime",
            get(get_openclaw_runtime),
        )
        .with_state(state)
}

// ── Generic mode endpoints ────────────────────────────────────────

async fn get_agent_mode(
    State(state): State<AuxiliaryRouterState>,
    Extension(_user): Extension<CurrentUser>,
    Path(id): Path<String>,
) -> Result<Json<ApiResponse<AgentModeResponse>>, AppError> {
    let handle = state
        .worker_task_manager
        .get_task(&id)
        .ok_or_else(|| AppError::NotFound(format!("No active agent for conversation '{id}'")))?;
    let resp = handle.get_mode().await?;
    Ok(Json(ApiResponse::ok(resp)))
}

async fn set_agent_mode(
    State(state): State<AuxiliaryRouterState>,
    Extension(_user): Extension<CurrentUser>,
    Path(id): Path<String>,
    body: Result<Json<SetModeRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<()>>, AppError> {
    let Json(req) = body.map_err(|e| AppError::BadRequest(e.to_string()))?;
    if req.mode.trim().is_empty() {
        return Err(AppError::BadRequest("mode must not be empty".into()));
    }
    let handle = state
        .worker_task_manager
        .get_task(&id)
        .ok_or_else(|| AppError::NotFound(format!("No active agent for conversation '{id}'")))?;
    handle.set_mode(&req.mode).await?;
    Ok(Json(ApiResponse::success()))
}

// ── Request / Response types ───────────────────────────────────────

/// Query parameters for workspace browse.
#[derive(Debug, Deserialize)]
pub struct WorkspaceBrowseQuery {
    pub path: String,
    pub search: Option<String>,
}

/// A file or directory entry in the workspace.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirOrFile {
    pub name: String,
    #[serde(rename = "type")]
    pub entry_type: String,
}

/// Request body for side question.
#[derive(Debug, Deserialize)]
pub struct SideQuestionRequest {
    pub question: String,
}

/// Response for side question.
#[derive(Debug, Serialize)]
pub struct SideQuestionResponse {
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub answer: Option<String>,
}

// ── Max depth for workspace traversal ──────────────────────────────
const MAX_DIR_DEPTH: usize = 10;

// ── Route handlers ─────────────────────────────────────────────────

/// GET /api/conversations/:id/workspace
///
/// Browse the workspace directory associated with a conversation.
async fn browse_workspace(
    State(state): State<AuxiliaryRouterState>,
    Extension(_user): Extension<CurrentUser>,
    Path(id): Path<String>,
    Query(query): Query<WorkspaceBrowseQuery>,
) -> Result<Json<ApiResponse<Vec<DirOrFile>>>, AppError> {
    if query.path.trim().is_empty() {
        return Err(AppError::BadRequest("path must not be empty".into()));
    }

    let handle = get_task(&state, &id)?;
    let workspace = handle.workspace();

    // Resolve the browsed path relative to the workspace root
    let base = std::path::Path::new(workspace);
    let browse_path = base.join(query.path.trim_start_matches('/'));

    // Security: ensure the resolved path is within the workspace
    let canonical_base = base
        .canonicalize()
        .map_err(|e| AppError::Internal(format!("Failed to resolve workspace path: {e}")))?;
    let canonical_browse = browse_path
        .canonicalize()
        .map_err(|_| AppError::NotFound("Directory not found".into()))?;
    if !canonical_browse.starts_with(&canonical_base) {
        return Err(AppError::BadRequest(
            "Path traversal outside workspace is not allowed".into(),
        ));
    }

    // Check depth limit
    let relative = canonical_browse
        .strip_prefix(&canonical_base)
        .unwrap_or(&canonical_browse);
    let depth = relative.components().count();
    if depth > MAX_DIR_DEPTH {
        return Err(AppError::BadRequest(format!(
            "Directory depth exceeds maximum of {MAX_DIR_DEPTH}"
        )));
    }

    let mut entries = Vec::new();
    let mut dir_reader = tokio::fs::read_dir(&canonical_browse)
        .await
        .map_err(|e| AppError::Internal(format!("Failed to read directory: {e}")))?;

    while let Ok(Some(entry)) = dir_reader.next_entry().await {
        let name = entry.file_name().to_string_lossy().into_owned();

        // Apply search filter if provided
        if let Some(ref search) = query.search
            && !search.is_empty()
            && !name.to_lowercase().contains(&search.to_lowercase())
        {
            continue;
        }

        let file_type = entry
            .file_type()
            .await
            .map_err(|e| AppError::Internal(format!("Failed to read file type: {e}")))?;

        let entry_type = if file_type.is_dir() {
            "directory"
        } else {
            "file"
        };

        entries.push(DirOrFile {
            name,
            entry_type: entry_type.into(),
        });
    }

    // Sort: directories first, then alphabetically
    entries.sort_by(|a, b| {
        let type_cmp = a.entry_type.cmp(&b.entry_type);
        if type_cmp == std::cmp::Ordering::Equal {
            a.name.to_lowercase().cmp(&b.name.to_lowercase())
        } else {
            type_cmp
        }
    });

    Ok(Json(ApiResponse::ok(entries)))
}

/// POST /api/conversations/:id/side-question
///
/// Ask a side question without interrupting the main conversation.
/// Only supported for ACP Claude backend.
async fn side_question(
    State(state): State<AuxiliaryRouterState>,
    Extension(_user): Extension<CurrentUser>,
    Path(id): Path<String>,
    Json(req): Json<SideQuestionRequest>,
) -> Result<Json<ApiResponse<SideQuestionResponse>>, AppError> {
    if req.question.trim().is_empty() {
        return Err(AppError::BadRequest("question must not be empty".into()));
    }

    let handle = get_task(&state, &id)?;

    // Only ACP agents support side questions
    if handle.agent_type() != AgentType::Acp {
        return Ok(Json(ApiResponse::ok(SideQuestionResponse {
            status: "unsupported".into(),
            answer: None,
        })));
    }

    let acp = handle
        .as_any()
        .downcast_ref::<AcpAgentManager>()
        .ok_or_else(|| AppError::Internal("Failed to downcast to AcpAgentManager".into()))?;

    // Check if the backend is Claude (side question is only supported for Claude)
    let backend = acp.backend();
    if backend != aionui_common::AcpBackend::Claude {
        return Ok(Json(ApiResponse::ok(SideQuestionResponse {
            status: "unsupported".into(),
            answer: None,
        })));
    }

    // Side question is implemented by sending a special message to the ACP CLI.
    // The actual implementation requires forking the ACP session, which will
    // be fully wired in Phase 6.15 App Integration.
    // For now, return a placeholder indicating the feature exists but is pending integration.
    Ok(Json(ApiResponse::ok(SideQuestionResponse {
        status: "ok".into(),
        answer: Some("Side question support will be fully wired in app integration phase.".into()),
    })))
}

/// POST /api/conversations/:id/reload-context
///
/// Reload the session context (skills, workspace state, etc.).
async fn reload_context(
    State(state): State<AuxiliaryRouterState>,
    Extension(_user): Extension<CurrentUser>,
    Path(id): Path<String>,
) -> Result<Json<ApiResponse<()>>, AppError> {
    let _handle = get_task(&state, &id)?;

    // Context reload triggers re-discovery of skills and workspace state.
    // The specific reload behavior varies by agent type and will be
    // fully integrated in Phase 6.15. For now, acknowledge the request.
    Ok(Json(ApiResponse::success()))
}

/// GET /api/conversations/:id/slash-commands
///
/// Get the list of available slash commands for the conversation.
async fn get_slash_commands(
    State(state): State<AuxiliaryRouterState>,
    Extension(_user): Extension<CurrentUser>,
    Path(id): Path<String>,
) -> Result<Json<ApiResponse<Vec<SlashCommandItem>>>, AppError> {
    let handle = get_task(&state, &id)?;

    // Only ACP agents have slash commands
    if handle.agent_type() != AgentType::Acp {
        return Ok(Json(ApiResponse::ok(Vec::new())));
    }

    let acp = handle
        .as_any()
        .downcast_ref::<AcpAgentManager>()
        .ok_or_else(|| AppError::Internal("Failed to downcast to AcpAgentManager".into()))?;

    // Trigger the CLI to send slash commands via the event stream
    acp.load_slash_commands().await?;

    // Slash commands arrive asynchronously via the event stream.
    // The client should subscribe to WebSocket events to receive them.
    // Return empty for the synchronous HTTP response.
    Ok(Json(ApiResponse::ok(Vec::new())))
}

/// GET /api/conversations/:id/openclaw/runtime
///
/// Get OpenClaw runtime diagnostic information.
async fn get_openclaw_runtime(
    State(state): State<AuxiliaryRouterState>,
    Extension(_user): Extension<CurrentUser>,
    Path(id): Path<String>,
) -> Result<Json<ApiResponse<serde_json::Value>>, AppError> {
    let handle = get_task(&state, &id)?;

    if handle.agent_type() != AgentType::OpenclawGateway {
        return Err(AppError::BadRequest(
            "This endpoint is only available for OpenClaw agents".into(),
        ));
    }

    let openclaw = handle
        .as_any()
        .downcast_ref::<OpenClawAgentManager>()
        .ok_or_else(|| AppError::Internal("Failed to downcast to OpenClawAgentManager".into()))?;

    let diagnostics = openclaw.get_diagnostics().await;
    Ok(Json(ApiResponse::ok(diagnostics)))
}

// ── Helpers ────────────────────────────────────────────────────────

fn get_task(
    state: &AuxiliaryRouterState,
    conversation_id: &str,
) -> Result<AgentManagerHandle, AppError> {
    state
        .worker_task_manager
        .get_task(conversation_id)
        .ok_or_else(|| {
            AppError::NotFound(format!(
                "No active agent for conversation '{conversation_id}'"
            ))
        })
}
