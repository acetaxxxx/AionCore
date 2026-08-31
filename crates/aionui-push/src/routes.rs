use axum::Router;
use axum::extract::rejection::JsonRejection;
use axum::extract::{Extension, Json, Path, State};
use axum::routing::{get, put};

use aionui_api_types::{ApiResponse, PushConfigResponse, PushSubscriptionResponse, UpsertPushSubscriptionRequest};
use aionui_auth::CurrentUser;
use aionui_common::ApiError;

use crate::error::PushError;
use crate::service::PushSubscriptionInput;
use crate::state::PushRouterState;

pub fn push_routes(state: PushRouterState) -> Router {
    Router::new()
        .route("/api/push/config", get(config))
        .route("/api/push/subscription", put(upsert_subscription))
        .route("/api/push/subscription/{id}", axum::routing::delete(delete_subscription))
        .with_state(state)
}

async fn config(State(state): State<PushRouterState>) -> Json<ApiResponse<PushConfigResponse>> {
    Json(ApiResponse::ok(PushConfigResponse {
        enabled: state.public_vapid_key.is_some(),
        public_vapid_key: state.public_vapid_key,
    }))
}

async fn upsert_subscription(
    State(state): State<PushRouterState>,
    Extension(user): Extension<CurrentUser>,
    body: Result<Json<UpsertPushSubscriptionRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<PushSubscriptionResponse>>, ApiError> {
    let Json(request) = body.map_err(ApiError::from)?;
    let subscription = state
        .service
        .upsert_subscription(
            &user.id,
            PushSubscriptionInput {
                endpoint: request.endpoint,
                p256dh: request.p256dh,
                auth: request.auth,
            },
        )
        .await
        .map_err(api_error)?;
    Ok(Json(ApiResponse::ok(PushSubscriptionResponse { id: subscription.id })))
}

async fn delete_subscription(
    State(state): State<PushRouterState>,
    Extension(user): Extension<CurrentUser>,
    Path(id): Path<String>,
) -> Result<Json<ApiResponse<()>>, ApiError> {
    state
        .service
        .delete_subscription(&user.id, &id)
        .await
        .map_err(api_error)?;
    Ok(Json(ApiResponse::success()))
}

fn api_error(error: PushError) -> ApiError {
    match error {
        PushError::InvalidSubscription | PushError::InvalidUserScope => {
            ApiError::BadRequest("Invalid push subscription".into())
        }
        PushError::NotFound => ApiError::NotFound("Push subscription not found".into()),
        PushError::Database(_) => ApiError::Internal("Push subscription storage unavailable".into()),
    }
}
