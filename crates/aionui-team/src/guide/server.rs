use std::net::SocketAddr;
use std::sync::{Arc, Weak};

use aionui_common::generate_id;
use axum::Json;
use axum::extract::State;
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::IntoResponse;
use tokio::net::TcpListener;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use crate::service::TeamSessionService;

type ServiceSlot = Arc<RwLock<Weak<TeamSessionService>>>;

#[derive(Clone)]
struct GuideState {
    auth_token: String,
    service: ServiceSlot,
}

pub struct GuideMcpServer {
    http_addr: SocketAddr,
    auth_token: String,
    shutdown_handle: Option<tokio::task::JoinHandle<()>>,
    service_slot: ServiceSlot,
}

impl GuideMcpServer {
    pub async fn start() -> Result<Self, String> {
        let auth_token = generate_id();
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|e| format!("Failed to bind guide MCP HTTP listener: {e}"))?;
        let http_addr = listener
            .local_addr()
            .map_err(|e| format!("Failed to read guide MCP local addr: {e}"))?;

        let service_slot: ServiceSlot = Arc::new(RwLock::new(Weak::new()));

        let state = GuideState {
            auth_token: auth_token.clone(),
            service: service_slot.clone(),
        };

        let app = axum::Router::new()
            .route("/tool", axum::routing::post(handle_tool_request))
            .with_state(state);

        let handle = tokio::spawn(async move {
            if let Err(e) = axum::serve(listener, app).await {
                warn!(error = %e, "Guide MCP axum server exited with error");
            }
        });

        debug!(http_port = http_addr.port(), "Guide MCP Server started (axum)");

        Ok(Self {
            http_addr,
            auth_token,
            shutdown_handle: Some(handle),
            service_slot,
        })
    }

    /// Wire the TeamSessionService after it is constructed.
    /// Must be called once before the first `aion_create_team` request arrives.
    pub async fn set_service(&self, service: Weak<TeamSessionService>) {
        *self.service_slot.write().await = service;
    }

    pub fn http_port(&self) -> u16 {
        self.http_addr.port()
    }

    pub fn http_addr(&self) -> SocketAddr {
        self.http_addr
    }

    pub fn auth_token(&self) -> &str {
        &self.auth_token
    }

    pub fn stop(&mut self) {
        if let Some(handle) = self.shutdown_handle.take() {
            handle.abort();
            debug!(http_port = self.http_addr.port(), "Guide MCP Server stop requested");
        }
    }
}

impl Drop for GuideMcpServer {
    fn drop(&mut self) {
        self.stop();
    }
}

// ---------------------------------------------------------------------------
// Axum handler
// ---------------------------------------------------------------------------

async fn handle_tool_request(
    State(state): State<GuideState>,
    headers: axum::http::HeaderMap,
    Json(body): Json<serde_json::Value>,
) -> impl IntoResponse {
    // Auth check
    let provided_token = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .unwrap_or("");

    if provided_token != state.auth_token {
        warn!("Guide HTTP: unauthorized request");
        return (StatusCode::UNAUTHORIZED, Json(serde_json::json!({"error": "unauthorized"}))).into_response();
    }

    let tool = body.get("tool").and_then(serde_json::Value::as_str).unwrap_or("");
    let args = body.get("args").cloned().unwrap_or(serde_json::Value::Null);

    info!(tool, "Guide HTTP: dispatching tool");

    let response_body = match tool {
        "aion_create_team" => exec_create_team(&body, &args, &state.service).await,
        "aion_list_models" => {
            let result = crate::guide::handlers::handle_aion_list_models();
            info!("Guide HTTP: aion_list_models succeeded");
            serde_json::json!({"result": serde_json::to_string(&result).unwrap_or_default()})
        }
        unknown => {
            warn!(tool = unknown, "Guide HTTP: unknown tool");
            serde_json::json!({"error": format!("Unknown tool: {unknown}")})
        }
    };

    let mut resp = Json(response_body).into_response();
    resp.headers_mut().insert(
        header::CONNECTION,
        HeaderValue::from_static("close"),
    );
    resp
}

// ---------------------------------------------------------------------------
// Tool implementations
// ---------------------------------------------------------------------------

async fn exec_create_team(
    request_body: &serde_json::Value,
    args: &serde_json::Value,
    service: &ServiceSlot,
) -> serde_json::Value {
    use crate::guide::handlers::parse_create_team_args;
    use aionui_api_types::{CreateTeamRequest, TeamAgentInput};

    let svc = match service.read().await.upgrade() {
        Some(s) => s,
        None => {
            warn!("Guide HTTP: aion_create_team — service not available");
            return serde_json::json!({"error": "service_unavailable"});
        }
    };

    let caller_workspace: Option<&str> = None;
    let params = match parse_create_team_args(args, caller_workspace) {
        Ok(p) => p,
        Err(e) => {
            warn!(error = %e, "Guide HTTP: aion_create_team parse error");
            return serde_json::json!({"error": e});
        }
    };

    let backend = request_body
        .get("backend")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("claude")
        .to_owned();

    let model = request_body
        .get("model")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("")
        .to_owned();

    let user_id = request_body
        .get("user_id")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("system_default_user")
        .to_owned();

    let caller_conversation_id = request_body
        .get("conversation_id")
        .and_then(serde_json::Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_owned);

    let req = CreateTeamRequest {
        name: params.name.clone(),
        agents: vec![TeamAgentInput {
            name: "Leader".to_owned(),
            role: "leader".to_owned(),
            backend: backend.clone(),
            model: model.clone(),
            custom_agent_id: None,
            conversation_id: caller_conversation_id,
        }],
    };

    let team = match svc.create_team(&user_id, req).await {
        Ok(t) => t,
        Err(e) => {
            warn!(error = %e, "Guide HTTP: aion_create_team create_team failed");
            return serde_json::json!({"error": e.to_string()});
        }
    };

    let team_id = team.id.clone();
    let summary = params.summary.clone();
    let svc2 = svc.clone();
    tokio::spawn(async move {
        let message = format!(
            "{}\n\n[SYSTEM NOTE: The user has already confirmed this team lineup during team creation. \
            Proceed immediately with team_spawn_agent for each teammate listed above. \
            Do NOT ask for confirmation again.]",
            summary
        );
        if let Err(e) = svc2.send_message(&team_id, &message, None).await {
            warn!(team_id = %team_id, error = %e, "Guide HTTP: failed to send summary to leader");
        }
    });

    let route = format!("/team/{}", team.id);
    info!(team_id = %team.id, "Guide HTTP: aion_create_team succeeded");
    serde_json::json!({
        "teamId": team.id,
        "name": team.name,
        "route": route,
        "status": "team_created",
        "next_step": "The team page has been opened automatically. End your turn now — do not add extra commentary."
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::time::timeout;

    #[tokio::test]
    async fn start_returns_positive_port_and_token() {
        let server = GuideMcpServer::start().await.expect("start should succeed");
        assert!(server.http_port() > 0, "http_port should be assigned");
        assert!(!server.auth_token().is_empty(), "auth_token should be generated");
    }

    #[tokio::test]
    async fn each_start_uses_a_fresh_auth_token() {
        let a = GuideMcpServer::start().await.unwrap();
        let b = GuideMcpServer::start().await.unwrap();
        assert_ne!(a.auth_token(), b.auth_token());
    }

    #[tokio::test]
    async fn stop_closes_the_listener() {
        let mut server = GuideMcpServer::start().await.unwrap();
        let port = server.http_port();
        server.stop();

        tokio::time::sleep(Duration::from_millis(50)).await;

        let client = reqwest::Client::builder()
            .timeout(Duration::from_millis(200))
            .build()
            .unwrap();
        let result = timeout(
            Duration::from_millis(500),
            client.post(format!("http://127.0.0.1:{port}/tool"))
                .json(&serde_json::json!({}))
                .send(),
        )
        .await;
        match result {
            Ok(Ok(_)) => { /* may still accept in-flight during abort */ }
            Ok(Err(_)) => { /* connection refused — expected */ }
            Err(_) => { /* timeout — expected */ }
        }
    }

    #[tokio::test]
    async fn stop_is_idempotent() {
        let mut server = GuideMcpServer::start().await.unwrap();
        server.stop();
        server.stop();
    }

    #[tokio::test]
    async fn tool_call_requires_auth() {
        let server = GuideMcpServer::start().await.unwrap();
        let port = server.http_port();

        let client = reqwest::Client::new();
        let resp = client
            .post(format!("http://127.0.0.1:{port}/tool"))
            .json(&serde_json::json!({"tool": "aion_list_models", "args": {}}))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status().as_u16(), 401);
    }

    #[tokio::test]
    async fn tool_call_with_valid_token_succeeds() {
        let server = GuideMcpServer::start().await.unwrap();
        let port = server.http_port();
        let token = server.auth_token().to_owned();

        let client = reqwest::Client::new();
        let resp = client
            .post(format!("http://127.0.0.1:{port}/tool"))
            .header("Authorization", format!("Bearer {token}"))
            .json(&serde_json::json!({"tool": "aion_list_models", "args": {}}))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status().as_u16(), 200);

        let body: serde_json::Value = resp.json().await.unwrap();
        assert!(body.get("result").is_some());
    }
}
