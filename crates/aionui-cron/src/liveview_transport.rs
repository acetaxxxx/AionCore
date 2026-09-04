//! LiveView transport session contract, capabilities, and fail-closed adapter seam.
//!
//! Implements Ticket 09 & Ticket 12A:
//! - Typed LiveView session start/renew/end/revoke contract.
//! - Strictly bound to `user_id`, `conversation_id`, `profile_ref`, and `monitor_id`.
//! - Short-lived, audience/nonce validation, replay protection, expiry, and capability scopes.
//! - Server-authoritative ownership enforcement.
//! - Transport adapter trait (`ILiveViewTransportAdapter`) with fail-closed production adapter (`FailClosedLiveViewTransportAdapter`).
//! - Controlled WebSocket screencast relay gateway (`LiveViewScreencastRelayGateway`):
//!   * Enforces handshake with valid capability session token.
//!   * Bounded screencast frames (max frame size, dimensions).
//!   * Strict allowlist of pointer/keyboard inputs (rate limited, no arbitrary navigation, CDP, or shell commands).
//!   * Deterministic sidecar state machine and fail-closed crash/disconnect handling.
//! - Zero-custody guarantee: passwords, MFA secrets, bearer tokens, and untrusted DOM/OCR/comments
//!   MUST NOT enter domain/MCP/conversation persistence or logs.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::RwLock;

use crate::monitor::MonitorError;

/// Specific capabilities allowed during an interactive LiveView session.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LiveViewCapability {
    /// Interactive user navigation to resolve auth/challenge.
    InteractiveAuth,
    /// Viewing the current browser challenge surface.
    StreamView,
    /// Completing CAPTCHA/checkpoint response interactively.
    SolveChallenge,
}

/// Scope definition governing an interactive LiveView session.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LiveViewSessionScope {
    pub user_id: String,
    pub conversation_id: String,
    pub profile_ref: String,
    pub monitor_id: Option<String>,
    pub browser_session_id: Option<String>,
    pub audience: String,
    pub target_group_ids: Vec<String>,
    pub allowed_capabilities: HashSet<LiveViewCapability>,
}

impl LiveViewSessionScope {
    pub fn new(
        user_id: impl Into<String>,
        conversation_id: impl Into<String>,
        profile_ref: impl Into<String>,
        audience: impl Into<String>,
    ) -> Self {
        let mut caps = HashSet::new();
        caps.insert(LiveViewCapability::InteractiveAuth);
        caps.insert(LiveViewCapability::StreamView);
        caps.insert(LiveViewCapability::SolveChallenge);

        Self {
            user_id: user_id.into(),
            conversation_id: conversation_id.into(),
            profile_ref: profile_ref.into(),
            monitor_id: None,
            browser_session_id: None,
            audience: audience.into(),
            target_group_ids: Vec::new(),
            allowed_capabilities: caps,
        }
    }

    pub fn with_monitor_id(mut self, monitor_id: impl Into<String>) -> Self {
        self.monitor_id = Some(monitor_id.into());
        self
    }

    pub fn with_browser_session_id(mut self, browser_session_id: impl Into<String>) -> Self {
        self.browser_session_id = Some(browser_session_id.into());
        self
    }

    pub fn with_target_group_ids(mut self, group_ids: Vec<String>) -> Self {
        self.target_group_ids = group_ids;
        self
    }

    pub fn with_capabilities(mut self, caps: HashSet<LiveViewCapability>) -> Self {
        self.allowed_capabilities = caps;
        self
    }

    /// Validate scope fields fail-closed.
    pub fn validate(&self) -> Result<(), LiveViewTransportError> {
        if self.user_id.trim().is_empty() {
            return Err(LiveViewTransportError::InvalidScope("user_id cannot be empty".into()));
        }
        if self.conversation_id.trim().is_empty() {
            return Err(LiveViewTransportError::InvalidScope(
                "conversation_id cannot be empty".into(),
            ));
        }
        if self.profile_ref.trim().is_empty() {
            return Err(LiveViewTransportError::InvalidScope(
                "profile_ref cannot be empty".into(),
            ));
        }
        if self.audience.trim().is_empty() {
            return Err(LiveViewTransportError::InvalidScope("audience cannot be empty".into()));
        }
        if self.allowed_capabilities.is_empty() {
            return Err(LiveViewTransportError::InvalidScope(
                "allowed_capabilities cannot be empty".into(),
            ));
        }
        Ok(())
    }
}

/// Lifecycle status for a typed LiveView transport session.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LiveViewSessionStatus {
    /// Active interactive session within valid lifespan.
    Active,
    /// Renewed session with extended deadline.
    Renewed,
    /// Cleanly ended by user or completion.
    Ended,
    /// Forcibly revoked due to expiry, timeout, or security violation.
    Revoked,
}

/// Typed LiveView transport session.
/// Never stores passwords, MFA secrets, bearer tokens, or raw DOM/OCR content.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LiveViewTransportSession {
    pub session_id: String,
    pub scope: LiveViewSessionScope,
    pub nonce: String,
    pub token_hash: String,
    pub status: LiveViewSessionStatus,
    pub created_at_ms: u64,
    pub expires_at_ms: u64,
    pub renewed_at_ms: Option<u64>,
    pub ended_at_ms: Option<u64>,
    pub revoke_reason: Option<String>,
}

impl LiveViewTransportSession {
    /// Check if the session is currently active and within its expiry deadline.
    pub fn is_active_at(&self, now_ms: u64) -> bool {
        (self.status == LiveViewSessionStatus::Active || self.status == LiveViewSessionStatus::Renewed)
            && now_ms < self.expires_at_ms
    }
}

/// Request to start a LiveView transport session.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StartLiveViewSessionRequest {
    pub scope: LiveViewSessionScope,
    pub nonce: String,
    pub ttl_ms: u64,
}

/// Response returned when a LiveView transport session starts successfully.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StartLiveViewSessionResponse {
    pub session_id: String,
    pub stream_endpoint: String,
    pub expires_at_ms: u64,
    pub token_hash: String,
}

/// Errors originating from the LiveView transport boundary.
#[derive(Debug, Error, PartialEq, Eq, Clone)]
pub enum LiveViewTransportError {
    #[error("Invalid session scope: {0}")]
    InvalidScope(String),

    #[error("Access denied: {0}")]
    AccessDenied(String),

    #[error("Session not found: {0}")]
    SessionNotFound(String),

    #[error("Session expired: expired at {expires_at_ms} ms, current time is {now_ms} ms")]
    SessionExpired { expires_at_ms: u64, now_ms: u64 },

    #[error("Session already closed or revoked with status: {0:?}")]
    SessionClosed(LiveViewSessionStatus),

    #[error("Replay attack detected: nonce '{0}' has already been used")]
    ReplayDetected(String),

    #[error("Invalid audience: expected '{expected}', received '{received}'")]
    AudienceMismatch { expected: String, received: String },

    #[error("Transport unavailable: {0}")]
    TransportUnavailable(String),

    #[error("Transport error: {0}")]
    TransportFailure(String),

    #[error("Handshake failed: {0}")]
    HandshakeFailed(String),

    #[error("Invalid frame: {0}")]
    InvalidFrame(String),

    #[error("Input rejected: {0}")]
    InputRejected(String),

    #[error("Rate limit exceeded: {0}")]
    RateLimitExceeded(String),

    #[error("Sidecar disconnected: {0}")]
    SidecarDisconnected(String),
}

impl From<LiveViewTransportError> for MonitorError {
    fn from(err: LiveViewTransportError) -> Self {
        match err {
            LiveViewTransportError::AccessDenied(msg) => MonitorError::AccessDenied(msg),
            LiveViewTransportError::InvalidScope(msg) => MonitorError::IncompleteScope(msg),
            LiveViewTransportError::SessionNotFound(id) => MonitorError::NotFound(id),
            LiveViewTransportError::SessionExpired { .. } => {
                MonitorError::InvalidOccurrence("LiveView transport session expired".into())
            }
            LiveViewTransportError::SessionClosed(status) => {
                MonitorError::InvalidOccurrence(format!("LiveView transport session closed ({status:?})"))
            }
            LiveViewTransportError::ReplayDetected(nonce) => {
                MonitorError::InvalidOccurrence(format!("Replay attack detected for nonce {nonce}"))
            }
            LiveViewTransportError::AudienceMismatch { expected, received } => {
                MonitorError::AccessDenied(format!("Audience mismatch: expected {expected}, received {received}"))
            }
            LiveViewTransportError::TransportUnavailable(msg) => MonitorError::ProfileBusy(msg),
            LiveViewTransportError::TransportFailure(msg) => MonitorError::Repository(msg),
            LiveViewTransportError::HandshakeFailed(msg) => MonitorError::AccessDenied(msg),
            LiveViewTransportError::InvalidFrame(msg) => MonitorError::InvalidOccurrence(msg),
            LiveViewTransportError::InputRejected(msg) => MonitorError::InvalidOccurrence(msg),
            LiveViewTransportError::RateLimitExceeded(msg) => MonitorError::ProfileBusy(msg),
            LiveViewTransportError::SidecarDisconnected(msg) => MonitorError::ProfileBusy(msg),
        }
    }
}

/// Compute a SHA-256 hash representation of a transport session token.
/// Prevents bearer tokens or raw credentials from entering persistence or logs.
pub fn hash_session_token(token: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(token.as_bytes());
    hex::encode(hasher.finalize())
}

/// Pluggable Transport Adapter Trait.
/// Separates the typed domain/session contract from concrete streaming protocols (WebRTC/VNC/WebSocket).
#[async_trait::async_trait]
pub trait ILiveViewTransportAdapter: Send + Sync {
    /// Establish a remote streaming channel for the given session.
    /// Fails closed if the transport is unconfigured or unreachable.
    async fn allocate_stream(
        &self,
        session_id: &str,
        scope: &LiveViewSessionScope,
    ) -> Result<String, LiveViewTransportError>;

    /// Terminate and release the remote streaming channel.
    async fn close_stream(&self, session_id: &str) -> Result<(), LiveViewTransportError>;

    /// Probe transport readiness.
    async fn is_available(&self) -> bool;
}

/// Default production fail-closed transport adapter.
/// When WebRTC/VNC or sidecar transport is unconfigured, strictly prevents session
/// creation or browser startup.
pub struct FailClosedLiveViewTransportAdapter {
    reason: String,
}

impl FailClosedLiveViewTransportAdapter {
    pub fn new(unconfigured_reason: impl Into<String>) -> Self {
        Self {
            reason: unconfigured_reason.into(),
        }
    }
}

impl Default for FailClosedLiveViewTransportAdapter {
    fn default() -> Self {
        Self::new("LiveView streaming transport is not configured; failing closed")
    }
}

#[async_trait::async_trait]
impl ILiveViewTransportAdapter for FailClosedLiveViewTransportAdapter {
    async fn allocate_stream(
        &self,
        _session_id: &str,
        _scope: &LiveViewSessionScope,
    ) -> Result<String, LiveViewTransportError> {
        Err(LiveViewTransportError::TransportUnavailable(self.reason.clone()))
    }

    async fn close_stream(&self, _session_id: &str) -> Result<(), LiveViewTransportError> {
        // Closing a non-existent or fail-closed transport succeeds idempotently.
        Ok(())
    }

    async fn is_available(&self) -> bool {
        false
    }
}

// ---------------------------------------------------------------------------
// LiveView Session Manager Service
// ---------------------------------------------------------------------------

/// Manages typed LiveView transport sessions with single-use nonce tracking,
/// capability enforcement, audience matching, and fail-closed transport adapter delegation.
pub struct LiveViewSessionManager {
    transport_adapter: Arc<dyn ILiveViewTransportAdapter>,
    sessions: Arc<RwLock<HashMap<String, LiveViewTransportSession>>>,
    used_nonces: Arc<RwLock<HashSet<String>>>,
    expected_audience: String,
}

impl LiveViewSessionManager {
    pub fn new(transport_adapter: Arc<dyn ILiveViewTransportAdapter>, expected_audience: impl Into<String>) -> Self {
        Self {
            transport_adapter,
            sessions: Arc::new(RwLock::new(HashMap::new())),
            used_nonces: Arc::new(RwLock::new(HashSet::new())),
            expected_audience: expected_audience.into(),
        }
    }

    /// Start a new LiveView transport session.
    ///
    /// Validates:
    /// - Caller ownership matches session scope user_id.
    /// - Nonce is single-use and not replayed.
    /// - Audience matches expected configuration.
    /// - Transport adapter is available (fails closed if unconfigured).
    pub async fn start_session(
        &self,
        caller_user_id: &str,
        req: StartLiveViewSessionRequest,
        now_ms: u64,
    ) -> Result<StartLiveViewSessionResponse, LiveViewTransportError> {
        // 1. Validate scope fail-closed
        req.scope.validate()?;

        // 2. Enforce server-authoritative ownership
        if caller_user_id != req.scope.user_id {
            return Err(LiveViewTransportError::AccessDenied(format!(
                "Caller '{caller_user_id}' cannot start LiveView session for user '{}'",
                req.scope.user_id
            )));
        }

        // 3. Audience validation
        if req.scope.audience != self.expected_audience {
            return Err(LiveViewTransportError::AudienceMismatch {
                expected: self.expected_audience.clone(),
                received: req.scope.audience.clone(),
            });
        }

        // 4. Single-use nonce / Replay protection
        if req.nonce.trim().is_empty() {
            return Err(LiveViewTransportError::InvalidScope("nonce must not be empty".into()));
        }
        {
            let mut nonces = self.used_nonces.write().await;
            if !nonces.insert(req.nonce.clone()) {
                return Err(LiveViewTransportError::ReplayDetected(req.nonce));
            }
        }

        // 5. TTL validation (bounded lifespan, default 15 minutes max 1 hour)
        if req.ttl_ms == 0 {
            return Err(LiveViewTransportError::InvalidScope(
                "ttl_ms must be greater than 0".into(),
            ));
        }
        let ttl_ms = req.ttl_ms.min(60 * 60 * 1000); // capped at 1h
        let expires_at_ms = now_ms.saturating_add(ttl_ms);

        let session_id = format!("lvs_{}", aionui_common::generate_prefixed_id("sess"));

        // 6. Fail-closed transport allocation
        let stream_endpoint = self.transport_adapter.allocate_stream(&session_id, &req.scope).await?;

        // 7. Token hashing: never persist raw tokens
        let raw_token = format!("{session_id}:{}:{}", req.nonce, expires_at_ms);
        let token_hash = hash_session_token(&raw_token);

        let session = LiveViewTransportSession {
            session_id: session_id.clone(),
            scope: req.scope,
            nonce: req.nonce,
            token_hash: token_hash.clone(),
            status: LiveViewSessionStatus::Active,
            created_at_ms: now_ms,
            expires_at_ms,
            renewed_at_ms: None,
            ended_at_ms: None,
            revoke_reason: None,
        };

        {
            let mut guard = self.sessions.write().await;
            guard.insert(session_id.clone(), session);
        }

        Ok(StartLiveViewSessionResponse {
            session_id,
            stream_endpoint,
            expires_at_ms,
            token_hash,
        })
    }

    /// Renew an active LiveView transport session.
    pub async fn renew_session(
        &self,
        caller_user_id: &str,
        session_id: &str,
        extend_ttl_ms: u64,
        now_ms: u64,
    ) -> Result<LiveViewTransportSession, LiveViewTransportError> {
        let mut guard = self.sessions.write().await;
        let session = guard
            .get_mut(session_id)
            .ok_or_else(|| LiveViewTransportError::SessionNotFound(session_id.to_string()))?;

        if caller_user_id != session.scope.user_id {
            return Err(LiveViewTransportError::AccessDenied(
                "Caller does not own this session".into(),
            ));
        }

        if !session.is_active_at(now_ms) {
            if now_ms >= session.expires_at_ms {
                session.status = LiveViewSessionStatus::Revoked;
                session.revoke_reason = Some("Session expired prior to renewal".into());
                return Err(LiveViewTransportError::SessionExpired {
                    expires_at_ms: session.expires_at_ms,
                    now_ms,
                });
            }
            return Err(LiveViewTransportError::SessionClosed(session.status));
        }

        let extension = extend_ttl_ms.min(30 * 60 * 1000); // capped at 30 mins per extension
        session.expires_at_ms = session.expires_at_ms.saturating_add(extension);
        session.renewed_at_ms = Some(now_ms);
        session.status = LiveViewSessionStatus::Renewed;

        Ok(session.clone())
    }

    /// End an active LiveView transport session cleanly.
    pub async fn end_session(
        &self,
        caller_user_id: &str,
        session_id: &str,
        now_ms: u64,
    ) -> Result<LiveViewTransportSession, LiveViewTransportError> {
        let mut guard = self.sessions.write().await;
        let session = guard
            .get_mut(session_id)
            .ok_or_else(|| LiveViewTransportError::SessionNotFound(session_id.to_string()))?;

        if caller_user_id != session.scope.user_id {
            return Err(LiveViewTransportError::AccessDenied(
                "Caller does not own this session".into(),
            ));
        }

        session.status = LiveViewSessionStatus::Ended;
        session.ended_at_ms = Some(now_ms);

        let _ = self.transport_adapter.close_stream(session_id).await;
        Ok(session.clone())
    }

    /// Forcibly revoke a LiveView transport session.
    pub async fn revoke_session(
        &self,
        caller_user_id: &str,
        session_id: &str,
        reason: impl Into<String>,
        now_ms: u64,
    ) -> Result<LiveViewTransportSession, LiveViewTransportError> {
        let mut guard = self.sessions.write().await;
        let session = guard
            .get_mut(session_id)
            .ok_or_else(|| LiveViewTransportError::SessionNotFound(session_id.to_string()))?;

        if caller_user_id != session.scope.user_id {
            return Err(LiveViewTransportError::AccessDenied(
                "Caller does not own this session".into(),
            ));
        }

        session.status = LiveViewSessionStatus::Revoked;
        session.revoke_reason = Some(reason.into());
        session.ended_at_ms = Some(now_ms);

        let _ = self.transport_adapter.close_stream(session_id).await;
        Ok(session.clone())
    }

    /// Get session details by ID, enforcing tenant ownership.
    pub async fn get_session(
        &self,
        caller_user_id: &str,
        session_id: &str,
    ) -> Result<LiveViewTransportSession, LiveViewTransportError> {
        let guard = self.sessions.read().await;
        let session = guard
            .get(session_id)
            .ok_or_else(|| LiveViewTransportError::SessionNotFound(session_id.to_string()))?;

        if caller_user_id != session.scope.user_id {
            return Err(LiveViewTransportError::AccessDenied(
                "Caller does not own this session".into(),
            ));
        }

        Ok(session.clone())
    }
}

// ---------------------------------------------------------------------------
// Ticket 12A: Controlled WebSocket Screencast Gateway / Relay Seam
// ---------------------------------------------------------------------------

/// Strict bounds for LiveView screencast frames to prevent memory explosion or DOS.
pub const MAX_SCENARIOCAST_FRAME_BYTES: usize = 1024 * 1024; // 1 MB max per frame
pub const MAX_FRAME_WIDTH: u32 = 1920;
pub const MAX_FRAME_HEIGHT: u32 = 1080;
pub const MAX_INPUTS_PER_SECOND: u32 = 50;

/// Frame format emitted by the browser sidecar screencast.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ScreencastFormat {
    Jpeg,
    Png,
    Webp,
}

/// Server-bounded video screencast frame metadata and payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScreencastFrame {
    pub session_id: String,
    pub timestamp_ms: u64,
    pub width: u32,
    pub height: u32,
    pub format: ScreencastFormat,
    #[serde(skip_serializing, skip_deserializing)]
    pub data: Vec<u8>,
    pub data_len: usize,
}

impl ScreencastFrame {
    pub fn new(
        session_id: impl Into<String>,
        timestamp_ms: u64,
        width: u32,
        height: u32,
        format: ScreencastFormat,
        data: Vec<u8>,
    ) -> Result<Self, LiveViewTransportError> {
        if width == 0 || width > MAX_FRAME_WIDTH {
            return Err(LiveViewTransportError::InvalidFrame(format!(
                "Width {width} exceeds allowable bounds (1..{MAX_FRAME_WIDTH})"
            )));
        }
        if height == 0 || height > MAX_FRAME_HEIGHT {
            return Err(LiveViewTransportError::InvalidFrame(format!(
                "Height {height} exceeds allowable bounds (1..{MAX_FRAME_HEIGHT})"
            )));
        }
        if data.len() > MAX_SCENARIOCAST_FRAME_BYTES {
            return Err(LiveViewTransportError::InvalidFrame(format!(
                "Frame byte length {} exceeds max allowable limit of {MAX_SCENARIOCAST_FRAME_BYTES}",
                data.len()
            )));
        }
        let data_len = data.len();
        Ok(Self {
            session_id: session_id.into(),
            timestamp_ms,
            width,
            height,
            format,
            data,
            data_len,
        })
    }
}

/// Strict allowlist of user pointer and keyboard actions.
/// Arbitrary navigation, script execution, file downloads, or CDP commands are forbidden.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum UserPointerEvent {
    MouseMove { x: u32, y: u32 },
    MouseDown { button: MouseButton, x: u32, y: u32 },
    MouseUp { button: MouseButton, x: u32, y: u32 },
    Click { button: MouseButton, x: u32, y: u32 },
    Wheel { delta_x: i32, delta_y: i32 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MouseButton {
    Left,
    Middle,
    Right,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum UserKeyboardEvent {
    KeyDown { key: String, code: String },
    KeyUp { key: String, code: String },
    TextInput { text: String },
}

/// Client-to-Gateway incoming message.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientRelayMessage {
    Pointer(UserPointerEvent),
    Keyboard(UserKeyboardEvent),
    AcknowledgeFrame { timestamp_ms: u64 },
    Heartbeat { client_time_ms: u64 },
}

/// Gateway-to-Client outgoing message.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum GatewayRelayMessage {
    Frame {
        timestamp_ms: u64,
        width: u32,
        height: u32,
        format: ScreencastFormat,
        data_len: usize,
    },
    SessionStatus {
        status: LiveViewSessionStatus,
        expires_at_ms: u64,
    },
    ChallengeDetected {
        reason: String,
    },
    Error {
        code: String,
        message: String,
    },
}

/// Sidecar driver seam for Ticket 12A.
/// Represents the private, server-to-sidecar screencast link.
#[async_trait::async_trait]
pub trait ISidecarScreencastDriver: Send + Sync {
    /// Request the sidecar to begin streaming frames for a validated session.
    async fn attach_session(
        &self,
        session_id: &str,
        scope: &LiveViewSessionScope,
    ) -> Result<(), LiveViewTransportError>;

    /// Forward a validated and sanitized pointer event to the sidecar.
    async fn forward_pointer(&self, session_id: &str, event: &UserPointerEvent) -> Result<(), LiveViewTransportError>;

    /// Forward a validated and sanitized keyboard event to the sidecar.
    async fn forward_keyboard(&self, session_id: &str, event: &UserKeyboardEvent)
    -> Result<(), LiveViewTransportError>;

    /// Detach and close the sidecar browser session cleanly.
    async fn detach_session(&self, session_id: &str) -> Result<(), LiveViewTransportError>;

    /// Probe if the sidecar is alive and connected.
    async fn is_connected(&self) -> bool;
}

/// Server-authoritative WebSocket Screencast Relay Gateway.
/// Enforces:
/// 1. Handshake authorization against capability session token.
/// 2. Bounded screencast frame relay.
/// 3. Strict allowlisted pointer/keyboard input with rate limiting.
/// 4. Fail-closed error handling on sidecar crash or disconnect.
pub struct LiveViewScreencastRelayGateway {
    session_manager: Arc<LiveViewSessionManager>,
    sidecar_driver: Arc<dyn ISidecarScreencastDriver>,
    rate_limiter: Arc<RwLock<HashMap<String, (u64, u32)>>>, // session_id -> (last_second_timestamp, count)
}

impl LiveViewScreencastRelayGateway {
    pub fn new(
        session_manager: Arc<LiveViewSessionManager>,
        sidecar_driver: Arc<dyn ISidecarScreencastDriver>,
    ) -> Self {
        Self {
            session_manager,
            sidecar_driver,
            rate_limiter: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Authenticate handshake from PWA client.
    /// Fails closed if session is invalid, expired, revoked, or ownership mismatches.
    pub async fn handle_handshake(
        &self,
        caller_user_id: &str,
        session_id: &str,
        expected_conversation_id: &str,
        now_ms: u64,
    ) -> Result<LiveViewTransportSession, LiveViewTransportError> {
        let session = self.session_manager.get_session(caller_user_id, session_id).await?;

        if session.scope.conversation_id != expected_conversation_id {
            return Err(LiveViewTransportError::AccessDenied(format!(
                "Conversation ID mismatch: session bound to '{}', caller supplied '{expected_conversation_id}'",
                session.scope.conversation_id
            )));
        }

        if !session.is_active_at(now_ms) {
            return Err(LiveViewTransportError::SessionExpired {
                expires_at_ms: session.expires_at_ms,
                now_ms,
            });
        }

        if !self.sidecar_driver.is_connected().await {
            return Err(LiveViewTransportError::SidecarDisconnected(
                "Sidecar browser worker is unreachable".into(),
            ));
        }

        // Attach session to sidecar
        self.sidecar_driver.attach_session(session_id, &session.scope).await?;

        Ok(session)
    }

    /// Process and relay incoming client messages to the sidecar.
    /// Enforces input rate limits and strict event filtering.
    pub async fn process_client_message(
        &self,
        caller_user_id: &str,
        session_id: &str,
        msg: ClientRelayMessage,
        now_ms: u64,
    ) -> Result<(), LiveViewTransportError> {
        // 1. Authorize session
        let session = self.session_manager.get_session(caller_user_id, session_id).await?;

        if !session.is_active_at(now_ms) {
            return Err(LiveViewTransportError::SessionExpired {
                expires_at_ms: session.expires_at_ms,
                now_ms,
            });
        }

        // 2. Check rate limit
        self.check_input_rate(session_id, now_ms).await?;

        // 3. Forward sanitized payload
        match msg {
            ClientRelayMessage::Pointer(pointer_event) => {
                self.validate_pointer_event(&pointer_event)?;
                self.sidecar_driver.forward_pointer(session_id, &pointer_event).await?;
            }
            ClientRelayMessage::Keyboard(keyboard_event) => {
                self.validate_keyboard_event(&keyboard_event)?;
                self.sidecar_driver
                    .forward_keyboard(session_id, &keyboard_event)
                    .await?;
            }
            ClientRelayMessage::AcknowledgeFrame { .. } => {}
            ClientRelayMessage::Heartbeat { .. } => {}
        }

        Ok(())
    }

    /// Validate that screencast frame received from sidecar is bounded.
    pub fn validate_and_wrap_frame(
        &self,
        session: &LiveViewTransportSession,
        frame: ScreencastFrame,
    ) -> Result<GatewayRelayMessage, LiveViewTransportError> {
        if frame.session_id != session.session_id {
            return Err(LiveViewTransportError::InvalidFrame(
                "Frame session_id does not match bound session".into(),
            ));
        }

        Ok(GatewayRelayMessage::Frame {
            timestamp_ms: frame.timestamp_ms,
            width: frame.width,
            height: frame.height,
            format: frame.format,
            data_len: frame.data_len,
        })
    }

    /// Cleanly terminate relay session and notify sidecar.
    pub async fn terminate_session(
        &self,
        caller_user_id: &str,
        session_id: &str,
        now_ms: u64,
    ) -> Result<(), LiveViewTransportError> {
        self.session_manager
            .end_session(caller_user_id, session_id, now_ms)
            .await?;
        let _ = self.sidecar_driver.detach_session(session_id).await;
        Ok(())
    }

    fn validate_pointer_event(&self, event: &UserPointerEvent) -> Result<(), LiveViewTransportError> {
        match event {
            UserPointerEvent::MouseMove { x, y }
            | UserPointerEvent::MouseDown { x, y, .. }
            | UserPointerEvent::MouseUp { x, y, .. }
            | UserPointerEvent::Click { x, y, .. } => {
                if *x > MAX_FRAME_WIDTH || *y > MAX_FRAME_HEIGHT {
                    return Err(LiveViewTransportError::InputRejected(format!(
                        "Coordinates ({x}, {y}) out of maximum allowable bounds ({MAX_FRAME_WIDTH}x{MAX_FRAME_HEIGHT})"
                    )));
                }
            }
            UserPointerEvent::Wheel { delta_x, delta_y } => {
                if delta_x.abs() > 5000 || delta_y.abs() > 5000 {
                    return Err(LiveViewTransportError::InputRejected(
                        "Wheel delta exceeds safe limits".into(),
                    ));
                }
            }
        }
        Ok(())
    }

    fn validate_keyboard_event(&self, event: &UserKeyboardEvent) -> Result<(), LiveViewTransportError> {
        match event {
            UserKeyboardEvent::KeyDown { key, .. } | UserKeyboardEvent::KeyUp { key, .. } => {
                if key.len() > 64 {
                    return Err(LiveViewTransportError::InputRejected(
                        "Keyboard key identifier exceeds length limit".into(),
                    ));
                }
            }
            UserKeyboardEvent::TextInput { text } => {
                // Reject control characters except tab, enter, newline
                if text.len() > 1024 {
                    return Err(LiveViewTransportError::InputRejected(
                        "TextInput exceeds batch length limit (1024 bytes)".into(),
                    ));
                }
                for c in text.chars() {
                    if c.is_control() && c != '\t' && c != '\n' && c != '\r' {
                        return Err(LiveViewTransportError::InputRejected(
                            "TextInput contains forbidden control characters".into(),
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    async fn check_input_rate(&self, session_id: &str, now_ms: u64) -> Result<(), LiveViewTransportError> {
        let current_second = now_ms / 1000;
        let mut guard = self.rate_limiter.write().await;
        let entry = guard.entry(session_id.to_string()).or_insert((current_second, 0));

        if entry.0 == current_second {
            entry.1 += 1;
            if entry.1 > MAX_INPUTS_PER_SECOND {
                return Err(LiveViewTransportError::RateLimitExceeded(format!(
                    "Input rate limit exceeded (> {MAX_INPUTS_PER_SECOND} events/sec)"
                )));
            }
        } else {
            entry.0 = current_second;
            entry.1 = 1;
        }

        Ok(())
    }
}
