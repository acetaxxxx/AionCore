use std::sync::Arc;

use axum::Router;
use axum::extract::rejection::JsonRejection;
use axum::extract::{Extension, Json, State};
use axum::routing::{get, post};

use aionui_api_types::{AgentInfo, ApiResponse, TestCustomAgentRequest, TestCustomAgentResponse};
use aionui_auth::CurrentUser;
use aionui_common::AppError;

use crate::acp_service;
use crate::agent_registry::AgentRegistry;

#[derive(Clone)]
pub struct AgentRouterState {
    pub agent_registry: Arc<AgentRegistry>,
}

pub fn agent_routes(state: AgentRouterState) -> Router {
    Router::new()
        .route("/api/agents", get(list_agents))
        .route("/api/agents/refresh", post(refresh_agents))
        .route("/api/agents/test", post(test_custom_agent))
        .with_state(state)
}

async fn list_agents(
    State(state): State<AgentRouterState>,
    Extension(_user): Extension<CurrentUser>,
) -> Result<Json<ApiResponse<Vec<AgentInfo>>>, AppError> {
    let agents = state.agent_registry.get_all().await;
    let infos: Vec<AgentInfo> = agents.into_iter().map(Into::into).collect();
    Ok(Json(ApiResponse::ok(infos)))
}

async fn refresh_agents(
    State(state): State<AgentRouterState>,
    Extension(_user): Extension<CurrentUser>,
) -> Result<Json<ApiResponse<Vec<AgentInfo>>>, AppError> {
    state.agent_registry.refresh_builtins().await;
    let agents = state.agent_registry.get_all().await;
    let infos: Vec<AgentInfo> = agents.into_iter().map(Into::into).collect();
    Ok(Json(ApiResponse::ok(infos)))
}

async fn test_custom_agent(
    State(_state): State<AgentRouterState>,
    Extension(_user): Extension<CurrentUser>,
    body: Result<Json<TestCustomAgentRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<TestCustomAgentResponse>>, AppError> {
    let Json(req) = body.map_err(|e| AppError::BadRequest(e.to_string()))?;
    let result = acp_service::test_custom_agent(&req.command, &req.acp_args, &req.env)?;
    Ok(Json(ApiResponse::ok(result)))
}
