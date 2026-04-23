//! HTTP route skeleton for `/api/assistants/*`.
//!
//! T1a: every handler returns `AppError::Internal("not implemented")`.
//! T1b replaces each body with the real service call.

use axum::Router;
use axum::extract::rejection::JsonRejection;
use axum::extract::{Json, Path, State};
use axum::routing::{get, patch, post};

use aionui_api_types::{
    ApiResponse, AssistantResponse, CreateAssistantRequest, ImportAssistantsRequest,
    ImportAssistantsResult, SetAssistantStateRequest, UpdateAssistantRequest,
};
use aionui_common::AppError;

pub use crate::state::AssistantRouterState;

/// Build the router for `/api/assistants/*`.
///
/// Endpoints (T1a skeleton; all return 500 "not implemented"):
/// - `GET    /api/assistants`                  — merged list
/// - `POST   /api/assistants`                  — create user assistant
/// - `PUT    /api/assistants/{id}`             — update user assistant
/// - `DELETE /api/assistants/{id}`             — delete user assistant
/// - `PATCH  /api/assistants/{id}/state`       — toggle enabled / sort order
/// - `POST   /api/assistants/import`           — bulk insert from legacy config
/// - `GET    /api/assistants/{id}/avatar`      — serve avatar bytes
/// - `POST   /api/assistants/{id}/avatar`      — upload user avatar
pub fn assistant_routes(state: AssistantRouterState) -> Router {
    Router::new()
        .route("/api/assistants", get(list).post(create))
        .route(
            "/api/assistants/{id}",
            axum::routing::put(update).delete(delete_one),
        )
        .route("/api/assistants/{id}/state", patch(set_state))
        .route(
            "/api/assistants/{id}/avatar",
            get(get_avatar).post(upload_avatar),
        )
        .route("/api/assistants/import", post(import))
        .with_state(state)
}

fn unimplemented() -> AppError {
    AppError::Internal("not implemented".into())
}

async fn list(
    State(_state): State<AssistantRouterState>,
) -> Result<Json<ApiResponse<Vec<AssistantResponse>>>, AppError> {
    Err(unimplemented())
}

async fn create(
    State(_state): State<AssistantRouterState>,
    body: Result<Json<CreateAssistantRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<AssistantResponse>>, AppError> {
    let Json(_req) = body.map_err(|e| AppError::BadRequest(e.to_string()))?;
    Err(unimplemented())
}

async fn update(
    State(_state): State<AssistantRouterState>,
    Path(_id): Path<String>,
    body: Result<Json<UpdateAssistantRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<AssistantResponse>>, AppError> {
    let Json(_req) = body.map_err(|e| AppError::BadRequest(e.to_string()))?;
    Err(unimplemented())
}

async fn delete_one(
    State(_state): State<AssistantRouterState>,
    Path(_id): Path<String>,
) -> Result<Json<ApiResponse<()>>, AppError> {
    Err(unimplemented())
}

async fn set_state(
    State(_state): State<AssistantRouterState>,
    Path(_id): Path<String>,
    body: Result<Json<SetAssistantStateRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<AssistantResponse>>, AppError> {
    let Json(_req) = body.map_err(|e| AppError::BadRequest(e.to_string()))?;
    Err(unimplemented())
}

async fn import(
    State(_state): State<AssistantRouterState>,
    body: Result<Json<ImportAssistantsRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<ImportAssistantsResult>>, AppError> {
    let Json(_req) = body.map_err(|e| AppError::BadRequest(e.to_string()))?;
    Err(unimplemented())
}

async fn get_avatar(
    State(_state): State<AssistantRouterState>,
    Path(_id): Path<String>,
) -> Result<axum::response::Response, AppError> {
    Err(unimplemented())
}

async fn upload_avatar(
    State(_state): State<AssistantRouterState>,
    Path(_id): Path<String>,
) -> Result<Json<ApiResponse<AssistantResponse>>, AppError> {
    Err(unimplemented())
}
