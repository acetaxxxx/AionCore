#![allow(clippy::disallowed_types)]

use std::sync::Arc;

use axum::extract::{Request, State};
use axum::http::{StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

use aionui_api_types::{EnsureExternalUserRequest, ExternalUserType};
use aionui_common::{ApiError, constants::COOKIE_NAME};
use aionui_db::{IUserRepository, UserStatus, UserType};

use crate::cloudflare::{CF_ACCESS_JWT_HEADER, CloudflareAccessAuthenticator};
use crate::extract::{extract_cookie_value, extract_token_from_headers};
use crate::{AuthProvisionService, CookieConfig, JwtService, SystemDefaultFilesystemAdopter};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthIdentityMode {
    Local,
    UserSession,
    AionPro,
}

/// Header carrying the conversation-runtime helper token minted by the backend
/// and injected into agent subprocess environments as `AIONUI_RUNTIME_TOKEN`.
pub const RUNTIME_TOKEN_HEADER: &str = "x-aionui-runtime-token";
/// Header carrying the acting user id asserted by the helper CLI.
pub const RUNTIME_USER_ID_HEADER: &str = "x-aionui-user-id";
/// Header carrying the conversation id the helper CLI runs inside.
pub const RUNTIME_CONVERSATION_ID_HEADER: &str = "x-aionui-conversation-id";

/// Port for validating conversation-runtime helper tokens.
///
/// Implemented in the composition layer over the agent runtime's token
/// service; `aionui-auth` must not depend on `aionui-ai-agent` directly.
/// A verifier must confirm the token is a live, conversation-helper-scoped
/// token bound to exactly this (user_id, conversation_id) pair.
pub trait IRuntimeTokenVerifier: Send + Sync {
    fn verify_conversation_helper(&self, token: &str, user_id: &str, conversation_id: &str) -> bool;
}

/// Authenticated user injected into request extensions by the auth middleware.
///
/// Route handlers extract this from `request.extensions()` to identify
/// the current user.
#[derive(Debug, Clone)]
pub struct CurrentUser {
    /// User ID from the database.
    pub id: String,
    /// Username.
    pub username: String,
    /// Internal identity source for the current user.
    pub user_type: UserType,
    /// Current account status. Authenticated requests only receive active users.
    pub status: UserStatus,
}

impl CurrentUser {
    pub fn local_default() -> Self {
        Self {
            id: "system_default_user".to_string(),
            username: "system_default_user".to_string(),
            user_type: UserType::Local,
            status: UserStatus::Active,
        }
    }
}

/// Shared state for the authentication middleware.
#[derive(Clone)]
pub struct AuthState {
    pub jwt_service: Arc<JwtService>,
    pub user_repo: Arc<dyn IUserRepository>,
    pub identity_mode: AuthIdentityMode,
    /// Optional second credential channel for agent-subprocess helper CLIs
    /// (`aioncore config` / `diagnose`), which cannot carry a JWT or cookies.
    /// `None` disables the channel (requests without a JWT are rejected).
    pub runtime_token_verifier: Option<Arc<dyn IRuntimeTokenVerifier>>,
    /// Optional origin-side Cloudflare Access assertion verifier. When set,
    /// JWT-less requests must present a verified assertion and are exchanged
    /// for the normal Aion session cookie before reaching handlers.
    pub cloudflare_access: Option<Arc<dyn CloudflareAccessAuthenticator>>,
    /// Optional first-account default-data filesystem adopter.
    pub fs_adopter: Option<Arc<dyn SystemDefaultFilesystemAdopter>>,
    /// Session cookie policy shared with ordinary local/WebUI login.
    pub cookie_config: Option<Arc<CookieConfig>>,
}

/// Authentication middleware that verifies JWT tokens and injects `CurrentUser`.
///
/// Flow:
/// 1. Extract bearer token from `Authorization` header or `aionui-session` cookie
/// 2. Verify JWT signature, expiration, and blacklist
/// 3. Look up user in the database to ensure they still exist
/// 4. Insert [`CurrentUser`] into request extensions
///
/// Returns HTTP 401 for authentication failures.
///
/// Use with `axum::middleware::from_fn_with_state`.
pub async fn auth_middleware(
    State(state): State<AuthState>,
    mut request: Request,
    next: Next,
) -> Result<Response, ApiError> {
    // In local mode, skip JWT verification and inject a fixed default user.
    if state.identity_mode == AuthIdentityMode::Local {
        request.extensions_mut().insert(CurrentUser::local_default());
        return Ok(next.run(request).await);
    }

    let session_cookie_present = extract_cookie_value(request.headers(), COOKIE_NAME).is_some();
    let Some(token) = extract_token_from_headers(request.headers()) else {
        if cloudflare_channel_available(&state, &request) {
            return cloudflare_session_exchange(&state, request, next).await;
        }

        // No JWT/cookie/Cloudflare assertion: fall back to the
        // conversation-helper runtime-token channel used by agent subprocess CLIs.
        return runtime_token_channel(&state, request, next).await;
    };

    let payload = match state.jwt_service.verify(&token) {
        Ok(payload) => payload,
        Err(error) => {
            tracing::debug!("Token verification failed: {error}");
            // A stale Aion session must not mask a valid Cloudflare identity.
            // Exchange the verified assertion for a fresh session before
            // returning 401, otherwise the browser is trapped in /login.
            if cloudflare_channel_available(&state, &request) {
                return match cloudflare_session_exchange(&state, request, next).await {
                    Ok(response) => Ok(response),
                    Err(_) => Ok(unauthorized_response(
                        &state,
                        session_cookie_present,
                        ApiError::Unauthorized("Invalid or expired token".into()),
                    )),
                };
            }
            return Ok(unauthorized_response(
                &state,
                session_cookie_present,
                ApiError::Unauthorized("Invalid or expired token".into()),
            ));
        }
    };

    let Some(user) = state
        .user_repo
        .find_active_by_id(&payload.user_id)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "auth middleware user lookup failed");
            ApiError::Internal("Authentication service unavailable".into())
        })?
    else {
        if cloudflare_channel_available(&state, &request) {
            return match cloudflare_session_exchange(&state, request, next).await {
                Ok(response) => Ok(response),
                Err(_) => Ok(unauthorized_response(
                    &state,
                    session_cookie_present,
                    ApiError::Unauthorized("Invalid authentication subject".into()),
                )),
            };
        }
        return Ok(unauthorized_response(
            &state,
            session_cookie_present,
            ApiError::Unauthorized("Invalid authentication subject".into()),
        ));
    };

    if state.identity_mode == AuthIdentityMode::AionPro && user.user_type != UserType::Aionpro {
        let error = ApiError::coded(StatusCode::UNAUTHORIZED, "USER_CONTEXT_REQUIRED", "User context required.", None);
        if cloudflare_channel_available(&state, &request) {
            return match cloudflare_session_exchange(&state, request, next).await {
                Ok(response) => Ok(response),
                Err(_) => Ok(unauthorized_response(&state, session_cookie_present, error)),
            };
        }
        return Ok(unauthorized_response(&state, session_cookie_present, error));
    }

    if payload.session_generation != user.session_generation {
        let error = ApiError::Unauthorized("Invalid authentication session".into());
        if cloudflare_channel_available(&state, &request) {
            return match cloudflare_session_exchange(&state, request, next).await {
                Ok(response) => Ok(response),
                Err(_) => Ok(unauthorized_response(&state, session_cookie_present, error)),
            };
        }
        return Ok(unauthorized_response(&state, session_cookie_present, error));
    }

    request.extensions_mut().insert(CurrentUser {
        id: user.id,
        username: user.username.unwrap_or_else(|| "external_user".to_string()),
        user_type: user.user_type,
        status: user.status,
    });

    Ok(next.run(request).await)
}

fn cloudflare_channel_available(state: &AuthState, request: &Request) -> bool {
    state.cloudflare_access.is_some()
        && request
            .headers()
            .get(CF_ACCESS_JWT_HEADER)
            .and_then(|value| value.to_str().ok())
            .map(str::trim)
            .is_some_and(|value| !value.is_empty())
}

/// Verify the origin assertion, provision its stable subject, and mint the
/// ordinary Aion session cookie. This is deliberately shared by the initial
/// JWT-less request and recovery from a stale/invalid session token.
async fn cloudflare_session_exchange(
    state: &AuthState,
    mut request: Request,
    next: Next,
) -> Result<Response, ApiError> {
    let cloudflare = state
        .cloudflare_access
        .as_ref()
        .expect("cloudflare_channel_available guarantees a verifier");
    let assertion = request
        .headers()
        .get(CF_ACCESS_JWT_HEADER)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .expect("cloudflare_channel_available guarantees an assertion");
    let identity = cloudflare.verify(assertion).await.map_err(|error| {
        tracing::warn!(error = %error, "Cloudflare Access assertion rejected");
        ApiError::Unauthorized("Invalid Cloudflare Access assertion".into())
    })?;

    let mut provisioner = AuthProvisionService::new(state.user_repo.clone(), state.jwt_service.clone());
    if let Some(fs_adopter) = state.fs_adopter.clone() {
        provisioner = provisioner.with_filesystem_adopter(fs_adopter);
    }
    let ensured = provisioner
        .ensure_external_user(
            &identity.subject,
            EnsureExternalUserRequest {
                user_type: ExternalUserType::Aionpro,
                username: identity.email.clone(),
                email: identity.email,
                avatar_path: None,
            },
        )
        .await
        .map_err(|error| {
            tracing::warn!(error = %error, "Cloudflare Access user provisioning rejected");
            ApiError::Unauthorized("Cloudflare Access user is not authorized".into())
        })?;
    let exchange = provisioner
        .create_external_session(aionui_api_types::EnsureExternalSessionRequest {
            user_type: ensured.user_type,
            external_user_id: ensured.external_user_id,
        })
        .await
        .map_err(|error| {
            tracing::error!(error = %error, "Cloudflare Access session exchange failed");
            ApiError::Internal("Cloudflare Access session exchange failed".into())
        })?;

    let current_user = CurrentUser {
        id: exchange.response.user.id,
        username: exchange.response.user.username,
        user_type: UserType::Aionpro,
        status: UserStatus::Active,
    };
    request.extensions_mut().insert(current_user);
    let mut response = next.run(request).await;
    if let Some(cookie_config) = state.cookie_config.as_ref()
        && let Ok(value) = header::HeaderValue::from_str(&cookie_config.build_session_cookie(&exchange.token))
    {
        response.headers_mut().append(header::SET_COOKIE, value);
    }
    Ok(response)
}

fn unauthorized_response(state: &AuthState, clear_session: bool, error: ApiError) -> Response {
    let mut response = error.into_response();
    if clear_session
        && let Some(cookie_config) = state.cookie_config.as_ref()
        && let Ok(value) = header::HeaderValue::from_str(&cookie_config.clear_session_cookie())
    {
        response.headers_mut().append(header::SET_COOKIE, value);
    }
    response
}

/// Authenticate a JWT-less request via the conversation-helper runtime token.
///
/// The helper CLI sends the token the backend minted for its conversation
/// runtime plus the (user, conversation) pair the token was bound to. The
/// verifier enforces that binding, so a forged user or conversation header
/// fails closed. On success the token's user is loaded and injected as
/// [`CurrentUser`], making ordinary user-scoped handlers work unchanged.
async fn runtime_token_channel(state: &AuthState, mut request: Request, next: Next) -> Result<Response, ApiError> {
    let headers = request.headers();
    let header = |name: &str| {
        headers
            .get(name)
            .and_then(|value| value.to_str().ok())
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_owned)
    };
    let (Some(verifier), Some(token), Some(user_id), Some(conversation_id)) = (
        state.runtime_token_verifier.as_ref(),
        header(RUNTIME_TOKEN_HEADER),
        header(RUNTIME_USER_ID_HEADER),
        header(RUNTIME_CONVERSATION_ID_HEADER),
    ) else {
        return Err(ApiError::Unauthorized("Authentication required".into()));
    };

    if !verifier.verify_conversation_helper(&token, &user_id, &conversation_id) {
        return Err(ApiError::Unauthorized("Invalid runtime token".into()));
    }

    let user = state
        .user_repo
        .find_active_by_id(&user_id)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "runtime token channel user lookup failed");
            ApiError::Internal("Authentication service unavailable".into())
        })?
        .ok_or_else(|| ApiError::Unauthorized("Invalid authentication subject".into()))?;

    if state.identity_mode == AuthIdentityMode::AionPro && user.user_type != UserType::Aionpro {
        return Err(ApiError::coded(
            StatusCode::UNAUTHORIZED,
            "USER_CONTEXT_REQUIRED",
            "User context required.",
            None,
        ));
    }

    request.extensions_mut().insert(CurrentUser {
        id: user.id,
        username: user.username.unwrap_or_else(|| "external_user".to_string()),
        user_type: user.user_type,
        status: user.status,
    });

    Ok(next.run(request).await)
}

/// Local-mode authentication middleware that skips JWT verification.
///
/// Injects a fixed `CurrentUser` with id and username `system_default_user`.
/// Used when the server runs as an embedded subprocess inside Electron.
pub async fn local_auth_middleware(mut request: Request, next: Next) -> Response {
    request.extensions_mut().insert(CurrentUser::local_default());
    next.run(request).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::Router;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use axum::routing::get;
    use tower::ServiceExt;

    async fn echo_user(request: Request<Body>) -> String {
        let user = request.extensions().get::<CurrentUser>().unwrap();
        format!("{}:{}", user.id, user.username)
    }

    #[tokio::test]
    async fn test_local_auth_middleware_injects_default_user() {
        let app = Router::new()
            .route("/test", get(echo_user))
            .route_layer(axum::middleware::from_fn(local_auth_middleware));

        let response = app
            .oneshot(Request::builder().uri("/test").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(
            std::str::from_utf8(&body).unwrap(),
            "system_default_user:system_default_user"
        );
    }
}
