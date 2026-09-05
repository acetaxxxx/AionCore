use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use axum::middleware;
use axum::routing::{get, post};
use jsonwebtoken::{DecodingKey, EncodingKey, Header, Validation, decode, encode};
use tower::ServiceExt;

use aionui_auth::{
    AuthIdentityMode, AuthState, CF_ACCESS_JWT_HEADER, CloudflareAccessAuthenticator, CloudflareAccessError,
    CloudflareIdentity, CookieConfig, CurrentUser, IRuntimeTokenVerifier, JwtService, RateLimiter, TokenPayload,
    api_rate_limit_middleware, auth_middleware, auth_rate_limit_middleware, authenticated_action_rate_limit_middleware,
    csrf_middleware, security_headers_middleware,
};
use aionui_db::{IUserRepository, SqliteUserRepository, UserStatus, UserType, init_database_memory};

async fn json_body(resp: axum::response::Response) -> serde_json::Value {
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    serde_json::from_slice(&body).unwrap()
}

// ============================================================
// T12.1 — Security response headers
// ============================================================

#[tokio::test]
async fn t12_1_security_headers_on_get() {
    let app = Router::new()
        .route("/test", get(|| async { "ok" }))
        .layer(middleware::from_fn(security_headers_middleware));

    let resp = app
        .oneshot(Request::get("/test").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(resp.headers().get("x-frame-options").unwrap(), "DENY");
    assert_eq!(resp.headers().get("x-content-type-options").unwrap(), "nosniff");
    assert_eq!(resp.headers().get("x-xss-protection").unwrap(), "1; mode=block");
    assert_eq!(
        resp.headers().get("referrer-policy").unwrap(),
        "strict-origin-when-cross-origin"
    );
}

// ============================================================
// T12.2 — CSRF protection
// ============================================================

fn csrf_app() -> Router {
    let config = Arc::new(CookieConfig {
        secure: false,
        same_site: "Lax",
    });
    Router::new()
        .route("/api/test", post(|| async { "ok" }))
        .route("/login", post(|| async { "logged in" }))
        .route("/api/auth/qr-login", post(|| async { "qr ok" }))
        .route("/get-test", get(|| async { "get ok" }))
        .route(
            "/internal/antigravity-hook/{conversation_id}",
            post(|| async { "hook ok" }),
        )
        .layer(middleware::from_fn_with_state(config, csrf_middleware))
}

#[tokio::test]
async fn t12_2_get_requests_bypass_csrf() {
    let app = csrf_app();
    let resp = app
        .oneshot(Request::get("/get-test").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

/// agy's PreToolUse callback must reach its handler without a CSRF token.
///
/// The hook is a local `aioncore antigravity-hook` process: no user session, no
/// cookie, no token to present. It authenticates with a per-conversation
/// `x-aionui-hook-token` that the handler checks. When CSRF rejected it, every
/// call came back 403, the hook read that as "no answer" and denied, and agy
/// turns produced no tool frames at all — measured 2026-08-14, 6/6 calls
/// rejected.
#[tokio::test]
async fn the_antigravity_hook_is_not_blocked_by_csrf() {
    let app = csrf_app();
    let resp = app
        .oneshot(
            Request::post("/internal/antigravity-hook/conv-1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "a hook callback with no CSRF token must still reach the handler"
    );
}

#[tokio::test]
async fn t12_2_post_without_csrf_token_rejected() {
    let app = csrf_app();
    let resp = app
        .oneshot(Request::post("/api/test").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    let json = json_body(resp).await;
    assert_eq!(json["code"], "CSRF_INVALID");
}

#[tokio::test]
async fn t12_2_post_with_matching_csrf_tokens_accepted() {
    let app = csrf_app();
    let token = "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890";
    let resp = app
        .oneshot(
            Request::post("/api/test")
                .header("cookie", format!("aionui-csrf-token={token}"))
                .header("x-csrf-token", token)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn t12_2_post_with_mismatched_csrf_tokens_rejected() {
    let app = csrf_app();
    let resp = app
        .oneshot(
            Request::post("/api/test")
                .header("cookie", "aionui-csrf-token=token_a")
                .header("x-csrf-token", "token_b")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    let json = json_body(resp).await;
    assert_eq!(json["code"], "CSRF_INVALID");
}

// ============================================================
// Auth middleware
// ============================================================

async fn auth_app(jwt_service: Arc<JwtService>) -> Router {
    let db = init_database_memory().await.unwrap();
    let user_repo = Arc::new(SqliteUserRepository::new(db.pool().clone())) as Arc<dyn IUserRepository>;
    protected_auth_app(jwt_service, user_repo)
}

fn protected_auth_app(jwt_service: Arc<JwtService>, user_repo: Arc<dyn IUserRepository>) -> Router {
    protected_auth_app_with_mode(jwt_service, user_repo, AuthIdentityMode::UserSession)
}

fn protected_auth_app_with_mode(
    jwt_service: Arc<JwtService>,
    user_repo: Arc<dyn IUserRepository>,
    identity_mode: AuthIdentityMode,
) -> Router {
    protected_auth_app_with_mode_and_verifier(jwt_service, user_repo, identity_mode, None)
}

fn protected_auth_app_with_mode_and_verifier(
    jwt_service: Arc<JwtService>,
    user_repo: Arc<dyn IUserRepository>,
    identity_mode: AuthIdentityMode,
    runtime_token_verifier: Option<Arc<dyn IRuntimeTokenVerifier>>,
) -> Router {
    let state = AuthState {
        jwt_service,
        user_repo,
        identity_mode,
        runtime_token_verifier,
        cloudflare_access: None,
        fs_adopter: None,
        cookie_config: None,
    };

    Router::new()
        .route("/protected", get(|| async { "ok" }))
        .route_layer(middleware::from_fn_with_state(state, auth_middleware))
}

#[derive(Clone)]
struct StubCloudflare {
    result: Result<CloudflareIdentity, CloudflareAccessError>,
}

#[async_trait]
impl CloudflareAccessAuthenticator for StubCloudflare {
    async fn verify(&self, _assertion: &str) -> Result<CloudflareIdentity, CloudflareAccessError> {
        self.result.clone()
    }
}

fn cloudflare_auth_app(
    jwt_service: Arc<JwtService>,
    user_repo: Arc<dyn IUserRepository>,
    verifier: Arc<dyn CloudflareAccessAuthenticator>,
) -> Router {
    let state = AuthState {
        jwt_service,
        user_repo,
        identity_mode: AuthIdentityMode::AionPro,
        runtime_token_verifier: None,
        cloudflare_access: Some(verifier),
        fs_adopter: None,
        cookie_config: Some(Arc::new(CookieConfig {
            secure: false,
            same_site: "Lax",
        })),
    };

    Router::new()
        .route("/protected", get(|| async { "ok" }))
        .route_layer(middleware::from_fn_with_state(state, auth_middleware))
}

#[tokio::test]
async fn cloudflare_first_login_provisions_external_user_and_sets_aion_cookie() {
    let db = init_database_memory().await.unwrap();
    let repo = Arc::new(SqliteUserRepository::new(db.pool().clone()));
    let repo_trait = repo.clone() as Arc<dyn IUserRepository>;
    let identity = CloudflareIdentity {
        subject: "cf-subject-1".into(),
        email: Some("alice@example.com".into()),
    };
    let verifier = Arc::new(StubCloudflare { result: Ok(identity) });
    let app = cloudflare_auth_app(
        Arc::new(JwtService::new("cloudflare_middleware_secret".into())),
        repo_trait.clone(),
        verifier,
    );

    let response = app
        .oneshot(
            Request::get("/protected")
                .header(CF_ACCESS_JWT_HEADER, "signed-assertion")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        response
            .headers()
            .get(header::SET_COOKIE)
            .unwrap()
            .to_str()
            .unwrap()
            .contains("aionui-session=")
    );
    let user = repo
        .find_by_external_user_id(aionui_db::UserType::Aionpro, "cf-subject-1")
        .await
        .unwrap()
        .expect("first Cloudflare login should create a users row");
    assert_eq!(user.email.as_deref(), Some("alice@example.com"));
}

#[tokio::test]
async fn cloudflare_repeated_login_keeps_same_internal_user_id() {
    let db = init_database_memory().await.unwrap();
    let repo = Arc::new(SqliteUserRepository::new(db.pool().clone()));
    let repo_trait = repo.clone() as Arc<dyn IUserRepository>;
    let verifier = Arc::new(StubCloudflare {
        result: Ok(CloudflareIdentity {
            subject: "cf-stable-subject".into(),
            email: Some("alice@example.com".into()),
        }),
    });
    let jwt = Arc::new(JwtService::new("cloudflare_repeat_secret".into()));
    let app = cloudflare_auth_app(jwt, repo_trait, verifier);

    let first = app
        .clone()
        .oneshot(
            Request::get("/protected")
                .header(CF_ACCESS_JWT_HEADER, "assertion-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(first.status(), StatusCode::OK);
    let first_user = repo
        .find_by_external_user_id(aionui_db::UserType::Aionpro, "cf-stable-subject")
        .await
        .unwrap()
        .unwrap();

    let second = app
        .oneshot(
            Request::get("/protected")
                .header(CF_ACCESS_JWT_HEADER, "assertion-b")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(second.status(), StatusCode::OK);
    let second_user = repo
        .find_by_external_user_id(aionui_db::UserType::Aionpro, "cf-stable-subject")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first_user.id, second_user.id);
}

#[tokio::test]
async fn expired_session_with_valid_cloudflare_assertion_recovers_session() {
    let db = init_database_memory().await.unwrap();
    let repo = Arc::new(SqliteUserRepository::new(db.pool().clone()));
    let repo_trait = repo.clone() as Arc<dyn IUserRepository>;
    let verifier = Arc::new(StubCloudflare {
        result: Ok(CloudflareIdentity {
            subject: "cf-recovery-subject".into(),
            email: Some("recovery@example.com".into()),
        }),
    });
    let jwt = Arc::new(JwtService::new("cloudflare_recovery_secret".into()));
    let app = cloudflare_auth_app(jwt.clone(), repo_trait, verifier);

    let first = app
        .clone()
        .oneshot(
            Request::get("/protected")
                .header(CF_ACCESS_JWT_HEADER, "assertion-first")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(first.status(), StatusCode::OK);
    let user = repo
        .find_by_external_user_id(UserType::Aionpro, "cf-recovery-subject")
        .await
        .unwrap()
        .unwrap();
    let stale_token = expired_token(&jwt, "cloudflare_recovery_secret", &user.id, "recovery@example.com");

    let response = app
        .oneshot(
            Request::get("/protected")
                .header(header::AUTHORIZATION, format!("Bearer {stale_token}"))
                .header(CF_ACCESS_JWT_HEADER, "assertion-recovery")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let cookie = response.headers().get(header::SET_COOKIE).unwrap().to_str().unwrap();
    assert!(cookie.contains("aionui-session="));
    assert!(!cookie.contains("Max-Age=0"));
}

#[tokio::test]
async fn expired_session_without_cloudflare_clears_session_cookie() {
    let db = init_database_memory().await.unwrap();
    let repo = Arc::new(SqliteUserRepository::new(db.pool().clone()));
    let jwt = Arc::new(JwtService::new("cloudflare_clear_secret".into()));
    let app = cloudflare_auth_app(
        jwt.clone(),
        repo as Arc<dyn IUserRepository>,
        Arc::new(StubCloudflare {
            result: Err(CloudflareAccessError::VerificationFailed),
        }),
    );
    let stale_token = expired_token(&jwt, "cloudflare_clear_secret", "stale-user", "stale@example.com");

    let response = app
        .oneshot(
            Request::get("/protected")
                .header(header::COOKIE, format!("aionui-session={stale_token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let cookie = response.headers().get(header::SET_COOKIE).unwrap().to_str().unwrap();
    assert!(cookie.contains("aionui-session="));
    assert!(cookie.contains("Max-Age=0"));
}

#[tokio::test]
async fn cloudflare_invalid_assertion_is_rejected_without_provisioning() {
    let db = init_database_memory().await.unwrap();
    let repo = Arc::new(SqliteUserRepository::new(db.pool().clone()));
    let verifier = Arc::new(StubCloudflare {
        result: Err(CloudflareAccessError::VerificationFailed),
    });
    let app = cloudflare_auth_app(
        Arc::new(JwtService::new("cloudflare_invalid_secret".into())),
        repo.clone() as Arc<dyn IUserRepository>,
        verifier,
    );

    let response = app
        .oneshot(
            Request::get("/protected")
                .header(CF_ACCESS_JWT_HEADER, "bad-assertion")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert!(
        repo.list_users()
            .await
            .unwrap()
            .iter()
            .all(|user| user.user_type != UserType::Aionpro)
    );
}

#[tokio::test]
async fn local_mode_keeps_default_user_even_when_access_header_is_present() {
    let db = init_database_memory().await.unwrap();
    let repo = Arc::new(SqliteUserRepository::new(db.pool().clone()));
    let state = AuthState {
        jwt_service: Arc::new(JwtService::new("cloudflare_local_secret".into())),
        user_repo: repo.clone() as Arc<dyn IUserRepository>,
        identity_mode: AuthIdentityMode::Local,
        runtime_token_verifier: None,
        cloudflare_access: Some(Arc::new(StubCloudflare {
            result: Err(CloudflareAccessError::VerificationFailed),
        })),
        fs_adopter: None,
        cookie_config: None,
    };
    let app = Router::new()
        .route(
            "/whoami",
            get(|user: axum::Extension<CurrentUser>| async move { user.id.clone() }),
        )
        .route_layer(middleware::from_fn_with_state(state, auth_middleware));

    let response = app
        .oneshot(
            Request::get("/whoami")
                .header(CF_ACCESS_JWT_HEADER, "ignored-in-local-mode")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert_eq!(std::str::from_utf8(&body).unwrap(), "system_default_user");
    assert!(
        repo.list_users()
            .await
            .unwrap()
            .iter()
            .all(|user| user.user_type != UserType::Aionpro)
    );
}

/// Like the protected app, but echoes the injected `CurrentUser.id` so tests
/// can assert which identity the runtime-token channel resolved.
fn identity_echo_app(
    jwt_service: Arc<JwtService>,
    user_repo: Arc<dyn IUserRepository>,
    identity_mode: AuthIdentityMode,
    runtime_token_verifier: Option<Arc<dyn IRuntimeTokenVerifier>>,
) -> Router {
    let state = AuthState {
        jwt_service,
        user_repo,
        identity_mode,
        runtime_token_verifier,
        cloudflare_access: None,
        fs_adopter: None,
        cookie_config: None,
    };

    Router::new()
        .route(
            "/whoami",
            get(|user: axum::Extension<CurrentUser>| async move { user.id.clone() }),
        )
        .route_layer(middleware::from_fn_with_state(state, auth_middleware))
}

fn expired_token(jwt_service: &JwtService, secret: &str, user_id: &str, username: &str) -> String {
    let token = jwt_service.sign(user_id, username).unwrap();
    let mut validation = Validation::default();
    validation.validate_exp = false;
    validation.validate_aud = false;

    let mut claims = decode::<TokenPayload>(&token, &DecodingKey::from_secret(secret.as_bytes()), &validation)
        .unwrap()
        .claims;

    claims.iat = 1000;
    claims.exp = 1001;

    encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(secret.as_bytes()),
    )
    .unwrap()
}

#[tokio::test]
async fn auth_middleware_missing_token_returns_unauthorized_code() {
    let app = auth_app(Arc::new(JwtService::new("middleware_test_secret".into()))).await;

    let resp = app
        .oneshot(Request::get("/protected").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    let json = json_body(resp).await;
    assert_eq!(json["code"], "UNAUTHORIZED");
}

#[tokio::test]
async fn auth_middleware_invalid_token_returns_unauthorized_code() {
    let app = auth_app(Arc::new(JwtService::new("middleware_test_secret".into()))).await;

    let resp = app
        .oneshot(
            Request::get("/protected")
                .header(header::AUTHORIZATION, "Bearer not-a-valid-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    let json = json_body(resp).await;
    assert_eq!(json["code"], "UNAUTHORIZED");
}

#[tokio::test]
async fn auth_middleware_expired_token_returns_unauthorized_code() {
    let secret = "middleware_test_secret";
    let jwt_service = Arc::new(JwtService::new(secret.into()));
    let token = expired_token(&jwt_service, secret, "system_default_user", "system_default_user");
    let app = auth_app(jwt_service).await;

    let resp = app
        .oneshot(
            Request::get("/protected")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    let json = json_body(resp).await;
    assert_eq!(json["code"], "UNAUTHORIZED");
}

#[tokio::test]
async fn auth_middleware_missing_user_returns_unauthorized_code() {
    let jwt_service = Arc::new(JwtService::new("middleware_test_secret".into()));
    let token = jwt_service.sign("missing_user", "ghost").unwrap();
    let app = auth_app(jwt_service).await;

    let resp = app
        .oneshot(
            Request::get("/protected")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    let json = json_body(resp).await;
    assert_eq!(json["code"], "UNAUTHORIZED");
}

#[tokio::test]
async fn auth_middleware_session_generation_mismatch_returns_unauthorized_code() {
    let jwt_service = Arc::new(JwtService::new("middleware_test_secret".into()));
    let token = jwt_service
        .sign_with_session_generation("system_default_user", "system_default_user", 0)
        .unwrap();
    let db = init_database_memory().await.unwrap();
    let repo = Arc::new(SqliteUserRepository::new(db.pool().clone()));
    repo.increment_session_generation("system_default_user").await.unwrap();
    let app = protected_auth_app(jwt_service, repo as Arc<dyn IUserRepository>);

    let resp = app
        .oneshot(
            Request::get("/protected")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    let json = json_body(resp).await;
    assert_eq!(json["code"], "UNAUTHORIZED");
}

#[tokio::test]
async fn auth_middleware_aionpro_rejects_local_user_token() {
    let jwt_service = Arc::new(JwtService::new("middleware_test_secret".into()));
    let token = jwt_service
        .sign_with_session_generation("system_default_user", "system_default_user", 0)
        .unwrap();
    let db = init_database_memory().await.unwrap();
    let repo = Arc::new(SqliteUserRepository::new(db.pool().clone()));
    let app = protected_auth_app_with_mode(jwt_service, repo as Arc<dyn IUserRepository>, AuthIdentityMode::AionPro);

    let resp = app
        .oneshot(
            Request::get("/protected")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    let json = json_body(resp).await;
    assert_eq!(json["code"], "USER_CONTEXT_REQUIRED");
}

#[tokio::test]
async fn auth_middleware_database_error_returns_internal_error_code() {
    let jwt_service = Arc::new(JwtService::new("middleware_test_secret".into()));
    let token = jwt_service.sign("system_default_user", "system_default_user").unwrap();
    let db = init_database_memory().await.unwrap();
    let user_repo = Arc::new(SqliteUserRepository::new(db.pool().clone())) as Arc<dyn IUserRepository>;
    db.close().await;
    let app = protected_auth_app(jwt_service, user_repo);

    let resp = app
        .oneshot(
            Request::get("/protected")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let json = json_body(resp).await;
    assert_eq!(json["code"], "INTERNAL_ERROR");
    assert_eq!(json["error"], "Internal server error.");
    let error = json["error"].as_str().unwrap();
    assert!(!error.contains("Database error"));
    assert!(!error.contains("Authentication service unavailable"));
    assert!(!error.to_ascii_lowercase().contains("closed"));
    assert!(!error.to_ascii_lowercase().contains("sqlx"));
}

#[tokio::test]
async fn t12_2_login_exempt_from_csrf() {
    let app = csrf_app();
    let resp = app
        .oneshot(Request::post("/login").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn t12_2_qr_login_exempt_from_csrf() {
    let app = csrf_app();
    let resp = app
        .oneshot(Request::post("/api/auth/qr-login").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn t12_2_csrf_cookie_set_on_first_request() {
    let app = csrf_app();
    let resp = app
        .oneshot(Request::get("/get-test").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let set_cookie = resp.headers().get(header::SET_COOKIE).unwrap().to_str().unwrap();
    assert!(set_cookie.contains("aionui-csrf-token="));
    // NOT HttpOnly (JS must read it)
    assert!(!set_cookie.contains("HttpOnly"));
}

// ============================================================
// Rate limiter middleware
// ============================================================

fn rate_limit_app(limiter: Arc<RateLimiter>) -> Router {
    Router::new()
        .route("/test", get(|| async { "ok" }))
        .layer(middleware::from_fn_with_state(limiter, api_rate_limit_middleware))
}

#[tokio::test]
async fn api_rate_limit_allows_within_quota() {
    let limiter = Arc::new(RateLimiter::new(3, Duration::from_secs(60)));
    let app = rate_limit_app(limiter);

    for _ in 0..3 {
        let resp = app
            .clone()
            .oneshot(Request::get("/test").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }
}

#[tokio::test]
async fn api_rate_limit_rejects_over_quota() {
    let limiter = Arc::new(RateLimiter::new(2, Duration::from_secs(60)));
    let app = rate_limit_app(limiter);

    // First two pass
    for _ in 0..2 {
        let resp = app
            .clone()
            .oneshot(Request::get("/test").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    // Third rejected
    let resp = app
        .oneshot(Request::get("/test").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);
}

#[tokio::test]
async fn auth_rate_limit_skips_successful_responses() {
    let limiter = Arc::new(RateLimiter::new(2, Duration::from_secs(60)));
    let app = Router::new()
        .route("/login", post(|| async { "ok" }))
        .layer(middleware::from_fn_with_state(limiter, auth_rate_limit_middleware));

    // Successful responses (200) don't count toward the limit
    for _ in 0..5 {
        let resp = app
            .clone()
            .oneshot(Request::post("/login").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }
}

#[tokio::test]
async fn auth_rate_limit_counts_failed_responses() {
    let limiter = Arc::new(RateLimiter::new(2, Duration::from_secs(60)));
    let app = Router::new()
        .route("/login", post(|| async { StatusCode::UNAUTHORIZED }))
        .layer(middleware::from_fn_with_state(limiter, auth_rate_limit_middleware));

    // First two failures pass through
    for _ in 0..2 {
        let resp = app
            .clone()
            .oneshot(Request::post("/login").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    // Third request blocked by rate limiter
    let resp = app
        .oneshot(Request::post("/login").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);
}

#[tokio::test]
async fn authenticated_action_limit_uses_user_id_key() {
    let limiter = Arc::new(RateLimiter::new(1, Duration::from_secs(60)));

    // Handler that injects a CurrentUser extension before the limiter
    let app = Router::new()
        .route("/action", post(|| async { "done" }))
        .layer(middleware::from_fn_with_state(
            limiter.clone(),
            authenticated_action_rate_limit_middleware,
        ))
        .layer(middleware::from_fn(
            |mut request: axum::extract::Request, next: axum::middleware::Next| async {
                request.extensions_mut().insert(CurrentUser {
                    id: "user_42".into(),
                    username: "admin".into(),
                    user_type: UserType::Local,
                    status: UserStatus::Active,
                });
                Ok::<_, std::convert::Infallible>(next.run(request).await)
            },
        ));

    // First request passes
    let resp = app
        .clone()
        .oneshot(Request::post("/action").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Second request for same user is rate limited
    let resp = app
        .oneshot(Request::post("/action").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);
}

// ============================================================
// T12.3 — Cookie security attributes (via CookieConfig)
// ============================================================

#[test]
fn t12_3_session_cookie_is_httponly() {
    let config = CookieConfig {
        secure: false,
        same_site: "Lax",
    };
    let cookie = config.build_session_cookie("token123");
    assert!(cookie.contains("HttpOnly"));
    assert!(cookie.contains("SameSite=Lax"));
    assert!(cookie.contains("Max-Age="));
}

#[test]
fn t12_3_session_cookie_secure_when_https() {
    let config = CookieConfig {
        secure: true,
        same_site: "Strict",
    };
    let cookie = config.build_session_cookie("token123");
    assert!(cookie.contains("; Secure"));
    assert!(cookie.contains("SameSite=Strict"));
}

// ============================================================
// T13 — Token extraction strategy
// ============================================================

#[test]
fn t13_1_authorization_header_takes_priority() {
    let mut headers = axum::http::HeaderMap::new();
    headers.insert(header::AUTHORIZATION, "Bearer header_tok".parse().unwrap());
    headers.insert(header::COOKIE, "aionui-session=cookie_tok".parse().unwrap());
    assert_eq!(
        aionui_auth::extract_token_from_headers(&headers),
        Some("header_tok".into())
    );
}

#[test]
fn t13_2_cookie_fallback() {
    let mut headers = axum::http::HeaderMap::new();
    headers.insert(header::COOKIE, "aionui-session=fallback_tok".parse().unwrap());
    assert_eq!(
        aionui_auth::extract_token_from_headers(&headers),
        Some("fallback_tok".into())
    );
}

#[test]
fn t13_3_no_token_returns_none() {
    let headers = axum::http::HeaderMap::new();
    assert_eq!(aionui_auth::extract_token_from_headers(&headers), None);
}

// ============================================================
// Runtime-token channel (conversation helper CLI)
// ============================================================

struct MatchVerifier {
    token: &'static str,
    user_id: &'static str,
    conversation_id: &'static str,
}

impl IRuntimeTokenVerifier for MatchVerifier {
    fn verify_conversation_helper(&self, token: &str, user_id: &str, conversation_id: &str) -> bool {
        token == self.token && user_id == self.user_id && conversation_id == self.conversation_id
    }
}

fn helper_request(token: Option<&str>, user_id: &str, conversation_id: &str) -> Request<Body> {
    let mut builder = Request::get("/whoami")
        .header("x-aionui-user-id", user_id)
        .header("x-aionui-conversation-id", conversation_id);
    if let Some(token) = token {
        builder = builder.header("x-aionui-runtime-token", token);
    }
    builder.body(Body::empty()).unwrap()
}

#[tokio::test]
async fn runtime_token_channel_authenticates_helper_and_injects_bound_user() {
    let jwt_service = Arc::new(JwtService::new("middleware_test_secret".into()));
    let db = init_database_memory().await.unwrap();
    let repo = Arc::new(SqliteUserRepository::new(db.pool().clone())) as Arc<dyn IUserRepository>;
    let app = identity_echo_app(
        jwt_service,
        repo,
        AuthIdentityMode::UserSession,
        Some(Arc::new(MatchVerifier {
            token: "tok-1",
            user_id: "system_default_user",
            conversation_id: "conv-1",
        })),
    );

    let resp = app
        .oneshot(helper_request(Some("tok-1"), "system_default_user", "conv-1"))
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    assert_eq!(&body[..], b"system_default_user");
}

#[tokio::test]
async fn runtime_token_channel_aionpro_authenticates_external_user() {
    let jwt_service = Arc::new(JwtService::new("middleware_test_secret".into()));
    let db = init_database_memory().await.unwrap();
    let repo = Arc::new(SqliteUserRepository::new(db.pool().clone()));
    let user = repo
        .ensure_external_user(
            UserType::Aionpro,
            "ext-user-1",
            aionui_db::ExternalUserProjection {
                username: Some("pro-user".into()),
                email: None,
                avatar_path: None,
            },
        )
        .await
        .unwrap();
    let token: &'static str = "tok-pro";
    let user_id: &'static str = Box::leak(user.id.clone().into_boxed_str());
    let app = identity_echo_app(
        jwt_service,
        repo as Arc<dyn IUserRepository>,
        AuthIdentityMode::AionPro,
        Some(Arc::new(MatchVerifier {
            token,
            user_id,
            conversation_id: "conv-pro",
        })),
    );

    let resp = app
        .oneshot(helper_request(Some(token), user_id, "conv-pro"))
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    assert_eq!(std::str::from_utf8(&body).unwrap(), user_id);
}

#[tokio::test]
async fn runtime_token_channel_rejects_forged_user_header() {
    let jwt_service = Arc::new(JwtService::new("middleware_test_secret".into()));
    let db = init_database_memory().await.unwrap();
    let repo = Arc::new(SqliteUserRepository::new(db.pool().clone())) as Arc<dyn IUserRepository>;
    let app = identity_echo_app(
        jwt_service,
        repo,
        AuthIdentityMode::UserSession,
        Some(Arc::new(MatchVerifier {
            token: "tok-1",
            user_id: "system_default_user",
            conversation_id: "conv-1",
        })),
    );

    let resp = app
        .oneshot(helper_request(Some("tok-1"), "another_user", "conv-1"))
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    let json = json_body(resp).await;
    assert_eq!(json["code"], "UNAUTHORIZED");
    assert_eq!(json["error"], "Invalid runtime token");
}

#[tokio::test]
async fn runtime_token_channel_rejects_cross_conversation_token() {
    let jwt_service = Arc::new(JwtService::new("middleware_test_secret".into()));
    let db = init_database_memory().await.unwrap();
    let repo = Arc::new(SqliteUserRepository::new(db.pool().clone())) as Arc<dyn IUserRepository>;
    let app = identity_echo_app(
        jwt_service,
        repo,
        AuthIdentityMode::UserSession,
        Some(Arc::new(MatchVerifier {
            token: "tok-1",
            user_id: "system_default_user",
            conversation_id: "conv-1",
        })),
    );

    let resp = app
        .oneshot(helper_request(Some("tok-1"), "system_default_user", "conv-other"))
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    let json = json_body(resp).await;
    assert_eq!(json["error"], "Invalid runtime token");
}

#[tokio::test]
async fn runtime_token_channel_without_token_returns_authentication_required() {
    let jwt_service = Arc::new(JwtService::new("middleware_test_secret".into()));
    let db = init_database_memory().await.unwrap();
    let repo = Arc::new(SqliteUserRepository::new(db.pool().clone())) as Arc<dyn IUserRepository>;
    let app = identity_echo_app(
        jwt_service,
        repo,
        AuthIdentityMode::AionPro,
        Some(Arc::new(MatchVerifier {
            token: "tok-1",
            user_id: "system_default_user",
            conversation_id: "conv-1",
        })),
    );

    let resp = app
        .oneshot(helper_request(None, "system_default_user", "conv-1"))
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    let json = json_body(resp).await;
    assert_eq!(json["code"], "UNAUTHORIZED");
    assert_eq!(json["error"], "Authentication required");
}

#[tokio::test]
async fn runtime_token_channel_disabled_when_verifier_absent() {
    let jwt_service = Arc::new(JwtService::new("middleware_test_secret".into()));
    let db = init_database_memory().await.unwrap();
    let repo = Arc::new(SqliteUserRepository::new(db.pool().clone())) as Arc<dyn IUserRepository>;
    let app = identity_echo_app(jwt_service, repo, AuthIdentityMode::UserSession, None);

    let resp = app
        .oneshot(helper_request(Some("tok-1"), "system_default_user", "conv-1"))
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    let json = json_body(resp).await;
    assert_eq!(json["error"], "Authentication required");
}

#[tokio::test]
async fn runtime_token_channel_aionpro_rejects_local_user_token_binding() {
    let jwt_service = Arc::new(JwtService::new("middleware_test_secret".into()));
    let db = init_database_memory().await.unwrap();
    let repo = Arc::new(SqliteUserRepository::new(db.pool().clone())) as Arc<dyn IUserRepository>;
    let app = identity_echo_app(
        jwt_service,
        repo,
        AuthIdentityMode::AionPro,
        Some(Arc::new(MatchVerifier {
            token: "tok-1",
            user_id: "system_default_user",
            conversation_id: "conv-1",
        })),
    );

    let resp = app
        .oneshot(helper_request(Some("tok-1"), "system_default_user", "conv-1"))
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    let json = json_body(resp).await;
    assert_eq!(json["code"], "USER_CONTEXT_REQUIRED");
}

#[tokio::test]
async fn csrf_exempts_requests_bearing_runtime_token_header() {
    let cookie_config = Arc::new(CookieConfig {
        secure: false,
        same_site: "Lax",
    });
    let app = Router::new()
        .route("/api/thing", post(|| async { "ok" }))
        .layer(middleware::from_fn_with_state(cookie_config, csrf_middleware));

    // Without the runtime-token header a cookieless POST is rejected.
    let resp = app
        .clone()
        .oneshot(Request::post("/api/thing").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);

    // With the header, CSRF is skipped (token validity is enforced later by
    // the auth middleware, which is not part of this app).
    let resp = app
        .oneshot(
            Request::post("/api/thing")
                .header("x-aionui-runtime-token", "tok-1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}
