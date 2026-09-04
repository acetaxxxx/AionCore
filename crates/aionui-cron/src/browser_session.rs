//! Generic, fail-closed browser capability and session control plane.
//!
//! This module is deliberately transport agnostic.  Concrete WebSocket/WebRTC
//! or browser-worker implementations live behind `IBrowserSessionAdapter`; the
//! control plane owns identity, capability, lease and origin checks.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation, decode, encode};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::RwLock;
use tokio::sync::mpsc;

pub const BROWSER_IDLE_LEASE_MS: u64 = 30 * 60 * 1000;
pub const BROWSER_ABSOLUTE_LEASE_MS: u64 = 4 * 60 * 60 * 1000;
pub const BROWSER_RELAY_MAX_FRAME_BYTES: usize = 512 * 1024;
pub const BROWSER_RELAY_MAX_FRAME_WIDTH: u32 = 1920;
pub const BROWSER_RELAY_MAX_FRAME_HEIGHT: u32 = 1080;
pub const BROWSER_RELAY_MAX_INPUTS_PER_SECOND: u32 = 60;

/// Server-owned signing key set. Retired keys remain verifiable during rotation.
pub struct BrowserCapabilityKeyProvider {
    active_kid: String,
    active_secret: Vec<u8>,
    active: EncodingKey,
    retired: HashMap<String, DecodingKey>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrowserCapabilityEnvelope {
    pub iss: String,
    pub aud: String,
    pub sub: String,
    pub cid: String,
    pub tid: String,
    pub profile_id: String,
    pub allowed_origins: Vec<String>,
    pub allowed_capabilities: Vec<BrowserCapability>,
    pub lease_id: String,
    pub nonce: String,
    pub jti: String,
    pub iat: u64,
    pub exp: u64,
}

/// Client input for starting a browser lease. Server-issued claims (lease,
/// nonce, jti and timestamps) are intentionally absent from this contract.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BrowserSessionStartRequest {
    pub conversation_id: String,
    pub task_id: String,
    pub profile_id: String,
    pub allowed_origins: Vec<String>,
    pub allowed_capabilities: Vec<BrowserCapability>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrowserSessionStartOutcome {
    pub lease: BrowserLease,
    pub capability_token: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BrowserRelayFrame {
    pub content_type: String,
    pub width: u32,
    pub height: u32,
    pub payload: Vec<u8>,
}

impl BrowserRelayFrame {
    pub fn validate(&self) -> Result<(), BrowserSessionError> {
        if self.payload.len() > BROWSER_RELAY_MAX_FRAME_BYTES {
            return Err(BrowserSessionError::InputRejected("frame exceeds 512KB".into()));
        }
        if self.width == 0
            || self.height == 0
            || self.width > BROWSER_RELAY_MAX_FRAME_WIDTH
            || self.height > BROWSER_RELAY_MAX_FRAME_HEIGHT
        {
            return Err(BrowserSessionError::InputRejected(
                "frame dimensions exceed bounds".into(),
            ));
        }
        if !matches!(
            self.content_type.as_str(),
            "image/jpeg" | "image/png" | "image/webp" | "image/x-aion-diff"
        ) {
            return Err(BrowserSessionError::InputRejected("unsupported frame format".into()));
        }
        Ok(())
    }
}

#[async_trait::async_trait]
pub trait BrowserPrivateRelay: Send + Sync {
    /// Attach a validated lease and return the bounded server-to-client frame
    /// channel. The channel is intentionally transport-neutral; an Axum
    /// WebSocket adapter owns serialization and closure policy.
    async fn open_stream(
        &self,
        lease: &BrowserLease,
        capability: &BrowserCapabilityEnvelope,
    ) -> Result<mpsc::Receiver<Result<BrowserRelayFrame, BrowserSessionError>>, BrowserSessionError>;
    async fn accept(
        &self,
        lease: &BrowserLease,
        capability: &BrowserCapabilityEnvelope,
    ) -> Result<(), BrowserSessionError>;
    async fn forward_input(&self, lease: &BrowserLease, input: &BrowserInput) -> Result<(), BrowserSessionError>;
    async fn close(&self, lease_id: &str) -> Result<(), BrowserSessionError>;
    async fn is_ready(&self) -> bool;
}

/// Transport-neutral verifier for the private sidecar handshake. The bearer
/// is supplied only by the internal relay, never by the PWA query/header.
pub struct BrowserCapabilityVerifier {
    provider: Arc<BrowserCapabilityKeyProvider>,
    used_jti: Arc<RwLock<HashSet<String>>>,
}

impl BrowserCapabilityVerifier {
    pub fn new(provider: Arc<BrowserCapabilityKeyProvider>) -> Self {
        Self {
            provider,
            used_jti: Arc::new(RwLock::new(HashSet::new())),
        }
    }

    pub async fn verify_for_lease(
        &self,
        caller_user_id: &str,
        lease: &BrowserLease,
        token: &str,
        now_ms: u64,
    ) -> Result<BrowserCapabilityEnvelope, BrowserSessionError> {
        if caller_user_id.trim().is_empty() || caller_user_id != lease.scope.user_id {
            return Err(BrowserSessionError::AccessDenied(
                "caller does not own browser lease".into(),
            ));
        }
        if !lease.is_active_at(now_ms) {
            return Err(BrowserSessionError::Expired {
                expires_at_ms: lease.expires_at_ms,
                now_ms,
            });
        }
        let envelope = self.provider.verify(token)?;
        if envelope.sub != lease.scope.user_id
            || envelope.cid != lease.scope.conversation_id
            || envelope.tid != lease.scope.task_id
            || envelope.profile_id != lease.scope.profile_id
            || envelope.lease_id != lease.lease_id
            || envelope.nonce != lease.scope.nonce
        {
            return Err(BrowserSessionError::AccessDenied("capability scope mismatch".into()));
        }
        let mut used = self.used_jti.write().await;
        if !used.insert(envelope.jti.clone()) {
            return Err(BrowserSessionError::AccessDenied("capability replay detected".into()));
        }
        Ok(envelope)
    }
}

/// Application-owned boundary for validating conversation/task/profile
/// ownership. Browser routes must not infer ownership from client fields.
pub trait BrowserScopeAuthorizer: Send + Sync {
    fn authorize(&self, user_id: &str, request: &BrowserSessionStartRequest) -> Result<(), BrowserSessionError>;
}

pub struct FailClosedBrowserScopeAuthorizer;

impl BrowserScopeAuthorizer for FailClosedBrowserScopeAuthorizer {
    fn authorize(&self, _: &str, _: &BrowserSessionStartRequest) -> Result<(), BrowserSessionError> {
        Err(BrowserSessionError::AccessDenied(
            "browser scope ownership is unavailable".into(),
        ))
    }
}

impl BrowserCapabilityKeyProvider {
    pub fn new(active_kid: impl Into<String>, active_secret: impl AsRef<[u8]>) -> Result<Self, BrowserSessionError> {
        let kid = active_kid.into();
        if kid.trim().is_empty() || active_secret.as_ref().len() < 32 {
            return Err(BrowserSessionError::InvalidScope(
                "capability key must have non-empty kid and at least 32 bytes".into(),
            ));
        }
        let secret = active_secret.as_ref().to_vec();
        Ok(Self {
            active_kid: kid,
            active_secret: secret.clone(),
            active: EncodingKey::from_secret(&secret),
            retired: HashMap::new(),
        })
    }

    pub fn rotate(
        &mut self,
        new_kid: impl Into<String>,
        new_secret: impl AsRef<[u8]>,
    ) -> Result<(), BrowserSessionError> {
        let kid = new_kid.into();
        if kid.trim().is_empty() || new_secret.as_ref().len() < 32 {
            return Err(BrowserSessionError::InvalidScope("capability key is weak".into()));
        }
        let old = std::mem::replace(&mut self.active_secret, new_secret.as_ref().to_vec());
        self.active = EncodingKey::from_secret(&self.active_secret);
        self.retired
            .insert(self.active_kid.clone(), DecodingKey::from_secret(&old));
        self.active_kid = kid;
        Ok(())
    }

    pub fn sign(&self, envelope: &BrowserCapabilityEnvelope) -> Result<String, BrowserSessionError> {
        let mut header = Header::new(Algorithm::HS256);
        header.kid = Some(self.active_kid.clone());
        encode(&header, envelope, &self.active)
            .map_err(|e| BrowserSessionError::TransportFailure(format!("capability signing failed: {e}")))
    }

    pub fn verify(&self, token: &str) -> Result<BrowserCapabilityEnvelope, BrowserSessionError> {
        let header = jsonwebtoken::decode_header(token)
            .map_err(|_| BrowserSessionError::AccessDenied("invalid capability envelope".into()))?;
        let kid = header
            .kid
            .ok_or_else(|| BrowserSessionError::AccessDenied("capability kid missing".into()))?;
        let key = if kid == self.active_kid {
            DecodingKey::from_secret(&self.active_secret)
        } else {
            self.retired
                .get(&kid)
                .cloned()
                .ok_or_else(|| BrowserSessionError::AccessDenied("unknown capability kid".into()))?
        };
        let mut validation = Validation::new(Algorithm::HS256);
        validation.validate_exp = true;
        validation.set_audience(&["aion:browser-worker:sidecar"]);
        decode::<BrowserCapabilityEnvelope>(token, &key, &validation)
            .map(|data| data.claims)
            .map_err(|err| BrowserSessionError::AccessDenied(format!("capability verification failed: {err}")))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BrowserCapability {
    Navigate,
    Observe,
    Interact,
    LiveViewTakeover,
    UploadWorkspaceFile,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BrowserLeaseStatus {
    Active,
    UserTakeoverPaused,
    Ended,
    Revoked,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserCapabilityScope {
    pub user_id: String,
    pub conversation_id: String,
    pub task_id: String,
    pub profile_id: String,
    pub allowed_origins: Vec<String>,
    pub capabilities: Vec<BrowserCapability>,
    pub nonce: String,
    pub jti: String,
}

impl BrowserCapabilityScope {
    pub fn validate(&self) -> Result<(), BrowserSessionError> {
        for (name, value) in [
            ("user_id", &self.user_id),
            ("conversation_id", &self.conversation_id),
            ("task_id", &self.task_id),
            ("profile_id", &self.profile_id),
            ("nonce", &self.nonce),
            ("jti", &self.jti),
        ] {
            if value.trim().is_empty() {
                return Err(BrowserSessionError::InvalidScope(format!("{name} cannot be empty")));
            }
        }
        if self.allowed_origins.is_empty() {
            return Err(BrowserSessionError::InvalidScope(
                "allowed_origins cannot be empty".into(),
            ));
        }
        if self.capabilities.is_empty() {
            return Err(BrowserSessionError::InvalidScope("capabilities cannot be empty".into()));
        }
        for origin in &self.allowed_origins {
            StrictBrowserOriginPolicy::validate(origin)?;
        }
        Ok(())
    }

    pub fn has_capability(&self, capability: BrowserCapability) -> bool {
        self.capabilities.contains(&capability)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserLease {
    pub lease_id: String,
    pub scope: BrowserCapabilityScope,
    pub status: BrowserLeaseStatus,
    pub created_at_ms: u64,
    pub last_activity_at_ms: u64,
    pub expires_at_ms: u64,
    pub paused_at_ms: Option<u64>,
    pub closed_at_ms: Option<u64>,
    pub close_reason: Option<String>,
}

impl BrowserLease {
    pub fn is_active_at(&self, now_ms: u64) -> bool {
        matches!(
            self.status,
            BrowserLeaseStatus::Active | BrowserLeaseStatus::UserTakeoverPaused
        ) && now_ms < self.expires_at_ms
            && now_ms.saturating_sub(self.last_activity_at_ms) <= BROWSER_IDLE_LEASE_MS
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BrowserInput {
    Pointer { kind: String, x: u32, y: u32 },
    Keyboard { kind: String, key: String, code: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BrowserAction {
    Navigate,
    Click,
    Fill,
    Select,
    PressKey,
    UploadFile,
    SubmitPayment,
    DeleteResource,
    Publish,
}

impl BrowserAction {
    pub fn requires_confirmation(self) -> bool {
        matches!(self, Self::SubmitPayment | Self::DeleteResource | Self::Publish)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActionConfirmation {
    pub confirmation_id: String,
    pub lease_id: String,
    pub action: BrowserAction,
    pub details: String,
    pub approved: bool,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum BrowserSessionError {
    #[error("invalid browser scope: {0}")]
    InvalidScope(String),
    #[error("access denied: {0}")]
    AccessDenied(String),
    #[error("browser lease not found: {0}")]
    NotFound(String),
    #[error("browser lease is not active: {0:?}")]
    LeaseClosed(BrowserLeaseStatus),
    #[error("browser lease expired at {expires_at_ms}, now {now_ms}")]
    Expired { expires_at_ms: u64, now_ms: u64 },
    #[error("browser lease idle timeout")]
    IdleTimeout,
    #[error("capability denied: {0:?}")]
    CapabilityDenied(BrowserCapability),
    #[error("origin rejected: {0}")]
    OriginRejected(String),
    #[error("action requires explicit confirmation")]
    ConfirmationRequired,
    #[error("confirmation not found: {0}")]
    ConfirmationNotFound(String),
    #[error("transport unavailable: {0}")]
    TransportUnavailable(String),
    #[error("transport failure: {0}")]
    TransportFailure(String),
    #[error("input rejected: {0}")]
    InputRejected(String),
}

#[async_trait::async_trait]
pub trait IBrowserSessionAdapter: Send + Sync {
    async fn open(&self, lease_id: &str, scope: &BrowserCapabilityScope) -> Result<(), BrowserSessionError>;
    async fn close(&self, lease_id: &str) -> Result<(), BrowserSessionError>;
    async fn pause(&self, lease_id: &str) -> Result<(), BrowserSessionError>;
    async fn resume(&self, lease_id: &str) -> Result<(), BrowserSessionError>;
    async fn relay_input(&self, lease_id: &str, input: &BrowserInput) -> Result<(), BrowserSessionError>;
    async fn is_available(&self) -> bool;
}

pub trait IBrowserOriginPolicy: Send + Sync {
    fn validate(&self, origin: &str) -> Result<(), BrowserSessionError>;
    fn validate_redirect(&self, origin: &str, allowed_origins: &[String]) -> Result<(), BrowserSessionError>;
}

pub struct StrictBrowserOriginPolicy;

impl StrictBrowserOriginPolicy {
    fn validate(origin: &str) -> Result<(), BrowserSessionError> {
        if origin.trim().is_empty()
            || !origin.starts_with("https://")
            || origin.contains('@')
            || origin.contains(char::is_whitespace)
            || origin.contains("localhost")
            || origin.contains("127.")
            || origin.contains("[::1]")
            || origin.contains("169.254.")
            || origin.contains("10.")
            || origin.contains("192.168.")
            || origin.contains("172.16.")
        {
            return Err(BrowserSessionError::OriginRejected(origin.into()));
        }
        Ok(())
    }
}

impl IBrowserOriginPolicy for StrictBrowserOriginPolicy {
    fn validate(&self, origin: &str) -> Result<(), BrowserSessionError> {
        Self::validate(origin)
    }

    fn validate_redirect(&self, origin: &str, allowed_origins: &[String]) -> Result<(), BrowserSessionError> {
        Self::validate(origin)?;
        if allowed_origins.iter().any(|allowed| origin == allowed) {
            Ok(())
        } else {
            Err(BrowserSessionError::OriginRejected(format!(
                "redirect outside allowlist: {origin}"
            )))
        }
    }
}

struct PendingConfirmation {
    confirmation: ActionConfirmation,
}

/// Safe default adapter used until a configured browser worker is ready.
pub struct UnavailableBrowserSessionAdapter;

#[async_trait::async_trait]
impl IBrowserSessionAdapter for UnavailableBrowserSessionAdapter {
    async fn open(&self, _: &str, _: &BrowserCapabilityScope) -> Result<(), BrowserSessionError> {
        Err(BrowserSessionError::TransportUnavailable(
            "browser worker is not configured".into(),
        ))
    }
    async fn close(&self, _: &str) -> Result<(), BrowserSessionError> {
        Err(BrowserSessionError::TransportUnavailable(
            "browser worker is not configured".into(),
        ))
    }
    async fn pause(&self, _: &str) -> Result<(), BrowserSessionError> {
        Err(BrowserSessionError::TransportUnavailable(
            "browser worker is not configured".into(),
        ))
    }
    async fn resume(&self, _: &str) -> Result<(), BrowserSessionError> {
        Err(BrowserSessionError::TransportUnavailable(
            "browser worker is not configured".into(),
        ))
    }
    async fn relay_input(&self, _: &str, _: &BrowserInput) -> Result<(), BrowserSessionError> {
        Err(BrowserSessionError::TransportUnavailable(
            "browser worker is not configured".into(),
        ))
    }
    async fn is_available(&self) -> bool {
        false
    }
}

pub struct BrowserSessionControlPlane {
    adapter: Arc<dyn IBrowserSessionAdapter>,
    origin_policy: Arc<dyn IBrowserOriginPolicy>,
    leases: Arc<RwLock<HashMap<String, BrowserLease>>>,
    confirmations: Arc<RwLock<HashMap<String, PendingConfirmation>>>,
}

impl BrowserSessionControlPlane {
    pub fn new(adapter: Arc<dyn IBrowserSessionAdapter>) -> Self {
        Self::with_origin_policy(adapter, Arc::new(StrictBrowserOriginPolicy))
    }

    pub fn with_origin_policy(
        adapter: Arc<dyn IBrowserSessionAdapter>,
        origin_policy: Arc<dyn IBrowserOriginPolicy>,
    ) -> Self {
        Self {
            adapter,
            origin_policy,
            leases: Arc::new(RwLock::new(HashMap::new())),
            confirmations: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn start(
        &self,
        caller_user_id: &str,
        scope: BrowserCapabilityScope,
        requested_ttl_ms: u64,
        now_ms: u64,
    ) -> Result<BrowserLease, BrowserSessionError> {
        scope.validate()?;
        if caller_user_id.trim().is_empty() || caller_user_id != scope.user_id {
            return Err(BrowserSessionError::AccessDenied(
                "caller does not own browser scope".into(),
            ));
        }
        if requested_ttl_ms == 0 {
            return Err(BrowserSessionError::InvalidScope("lease ttl must be positive".into()));
        }
        if !self.adapter.is_available().await {
            return Err(BrowserSessionError::TransportUnavailable(
                "browser transport is unavailable".into(),
            ));
        }
        let lease_id = aionui_common::generate_prefixed_id("lease");
        let expires_at_ms = now_ms.saturating_add(requested_ttl_ms.min(BROWSER_ABSOLUTE_LEASE_MS));
        let lease = BrowserLease {
            lease_id: lease_id.clone(),
            scope,
            status: BrowserLeaseStatus::Active,
            created_at_ms: now_ms,
            last_activity_at_ms: now_ms,
            expires_at_ms,
            paused_at_ms: None,
            closed_at_ms: None,
            close_reason: None,
        };
        self.adapter.open(&lease_id, &lease.scope).await?;
        self.leases.write().await.insert(lease_id, lease.clone());
        Ok(lease)
    }

    /// Starts a lease from untrusted client input and issues all capability
    /// claims on the server. Callers cannot provide nonce, jti, lease or time.
    pub async fn start_server_issued(
        &self,
        caller_user_id: &str,
        request: BrowserSessionStartRequest,
        key_provider: &BrowserCapabilityKeyProvider,
        requested_ttl_ms: u64,
        now_ms: u64,
    ) -> Result<BrowserSessionStartOutcome, BrowserSessionError> {
        let scope = BrowserCapabilityScope {
            user_id: caller_user_id.to_owned(),
            conversation_id: request.conversation_id,
            task_id: request.task_id,
            profile_id: request.profile_id,
            allowed_origins: request.allowed_origins,
            capabilities: request.allowed_capabilities,
            nonce: aionui_common::generate_prefixed_id("nonce"),
            jti: aionui_common::generate_prefixed_id("jti"),
        };
        let lease = self.start(caller_user_id, scope, requested_ttl_ms, now_ms).await?;
        let envelope = BrowserCapabilityEnvelope {
            iss: "aion:core:control-plane".into(),
            aud: "aion:browser-worker:sidecar".into(),
            sub: lease.scope.user_id.clone(),
            cid: lease.scope.conversation_id.clone(),
            tid: lease.scope.task_id.clone(),
            profile_id: lease.scope.profile_id.clone(),
            allowed_origins: lease.scope.allowed_origins.clone(),
            allowed_capabilities: lease.scope.capabilities.clone(),
            lease_id: lease.lease_id.clone(),
            nonce: lease.scope.nonce.clone(),
            jti: lease.scope.jti.clone(),
            iat: now_ms / 1000,
            exp: lease.expires_at_ms / 1000,
        };
        let capability_token = key_provider.sign(&envelope)?;
        Ok(BrowserSessionStartOutcome {
            lease,
            capability_token,
        })
    }

    async fn owned_active(
        &self,
        caller_user_id: &str,
        lease_id: &str,
        now_ms: u64,
    ) -> Result<BrowserLease, BrowserSessionError> {
        let lease = self
            .leases
            .read()
            .await
            .get(lease_id)
            .cloned()
            .ok_or_else(|| BrowserSessionError::NotFound(lease_id.into()))?;
        if caller_user_id != lease.scope.user_id {
            return Err(BrowserSessionError::AccessDenied(
                "caller does not own browser lease".into(),
            ));
        }
        if now_ms >= lease.expires_at_ms {
            return Err(BrowserSessionError::Expired {
                expires_at_ms: lease.expires_at_ms,
                now_ms,
            });
        }
        if now_ms.saturating_sub(lease.last_activity_at_ms) > BROWSER_IDLE_LEASE_MS {
            return Err(BrowserSessionError::IdleTimeout);
        }
        if !matches!(
            lease.status,
            BrowserLeaseStatus::Active | BrowserLeaseStatus::UserTakeoverPaused
        ) {
            return Err(BrowserSessionError::LeaseClosed(lease.status));
        }
        Ok(lease)
    }

    async fn touch(&self, lease_id: &str, now_ms: u64) {
        if let Some(lease) = self.leases.write().await.get_mut(lease_id) {
            lease.last_activity_at_ms = lease.last_activity_at_ms.max(now_ms);
        }
    }

    pub async fn get(&self, caller_user_id: &str, lease_id: &str) -> Result<BrowserLease, BrowserSessionError> {
        let lease = self
            .leases
            .read()
            .await
            .get(lease_id)
            .cloned()
            .ok_or_else(|| BrowserSessionError::NotFound(lease_id.into()))?;
        if caller_user_id != lease.scope.user_id {
            return Err(BrowserSessionError::AccessDenied(
                "caller does not own browser lease".into(),
            ));
        }
        Ok(lease)
    }

    pub async fn renew(
        &self,
        caller_user_id: &str,
        lease_id: &str,
        requested_ttl_ms: u64,
        now_ms: u64,
    ) -> Result<BrowserLease, BrowserSessionError> {
        let lease = self.owned_active(caller_user_id, lease_id, now_ms).await?;
        if requested_ttl_ms == 0 {
            return Err(BrowserSessionError::InvalidScope("renew ttl must be positive".into()));
        }
        let mut leases = self.leases.write().await;
        let current = leases
            .get_mut(lease_id)
            .ok_or_else(|| BrowserSessionError::NotFound(lease_id.into()))?;
        current.expires_at_ms = current
            .expires_at_ms
            .max(now_ms)
            .saturating_add(requested_ttl_ms.min(BROWSER_IDLE_LEASE_MS))
            .min(current.created_at_ms.saturating_add(BROWSER_ABSOLUTE_LEASE_MS));
        current.last_activity_at_ms = current.last_activity_at_ms.max(now_ms);
        let _ = lease;
        Ok(current.clone())
    }

    pub async fn end(
        &self,
        caller_user_id: &str,
        lease_id: &str,
        now_ms: u64,
    ) -> Result<BrowserLease, BrowserSessionError> {
        let _lease = self.owned_active(caller_user_id, lease_id, now_ms).await?;
        self.adapter.close(lease_id).await?;
        let mut leases = self.leases.write().await;
        let current = leases.get_mut(lease_id).expect("lease checked above");
        current.status = BrowserLeaseStatus::Ended;
        current.closed_at_ms = Some(now_ms);
        Ok(current.clone())
    }

    pub async fn revoke(
        &self,
        caller_user_id: &str,
        lease_id: &str,
        reason: impl Into<String>,
        now_ms: u64,
    ) -> Result<BrowserLease, BrowserSessionError> {
        let lease = self.get(caller_user_id, lease_id).await?;
        self.adapter.close(lease_id).await?;
        let mut leases = self.leases.write().await;
        let current = leases.get_mut(lease_id).expect("lease checked above");
        current.status = BrowserLeaseStatus::Revoked;
        current.closed_at_ms = Some(now_ms);
        current.close_reason = Some(reason.into());
        let _ = lease;
        Ok(current.clone())
    }

    pub async fn pause_for_user_takeover(
        &self,
        caller_user_id: &str,
        lease_id: &str,
        now_ms: u64,
    ) -> Result<BrowserLease, BrowserSessionError> {
        let lease = self.owned_active(caller_user_id, lease_id, now_ms).await?;
        if !lease.scope.has_capability(BrowserCapability::LiveViewTakeover) {
            return Err(BrowserSessionError::CapabilityDenied(
                BrowserCapability::LiveViewTakeover,
            ));
        }
        self.adapter.pause(lease_id).await?;
        let mut leases = self.leases.write().await;
        let current = leases.get_mut(lease_id).expect("lease checked above");
        current.status = BrowserLeaseStatus::UserTakeoverPaused;
        current.paused_at_ms = Some(now_ms);
        Ok(current.clone())
    }

    pub async fn resume_from_user_takeover(
        &self,
        caller_user_id: &str,
        lease_id: &str,
        now_ms: u64,
    ) -> Result<BrowserLease, BrowserSessionError> {
        let lease = self.owned_active(caller_user_id, lease_id, now_ms).await?;
        if lease.status != BrowserLeaseStatus::UserTakeoverPaused {
            return Err(BrowserSessionError::LeaseClosed(lease.status));
        }
        self.adapter.resume(lease_id).await?;
        let mut leases = self.leases.write().await;
        let current = leases.get_mut(lease_id).expect("lease checked above");
        current.status = BrowserLeaseStatus::Active;
        current.last_activity_at_ms = current.last_activity_at_ms.max(now_ms);
        Ok(current.clone())
    }

    pub async fn relay_input(
        &self,
        caller_user_id: &str,
        lease_id: &str,
        input: BrowserInput,
        now_ms: u64,
    ) -> Result<(), BrowserSessionError> {
        let lease = self.owned_active(caller_user_id, lease_id, now_ms).await?;
        if !lease.scope.has_capability(BrowserCapability::LiveViewTakeover)
            || !lease.scope.has_capability(BrowserCapability::Interact)
        {
            return Err(BrowserSessionError::CapabilityDenied(BrowserCapability::Interact));
        }
        match &input {
            BrowserInput::Pointer { kind, x, y }
                if *x > 1920 || *y > 1080 || !matches!(kind.as_str(), "move" | "down" | "up" | "click" | "wheel") =>
            {
                return Err(BrowserSessionError::InputRejected(
                    "pointer outside bounded allowlist".into(),
                ));
            }
            BrowserInput::Keyboard { kind, key, code }
                if key.len() > 128 || code.len() > 128 || !matches!(kind.as_str(), "down" | "up" | "text") =>
            {
                return Err(BrowserSessionError::InputRejected(
                    "keyboard outside bounded allowlist".into(),
                ));
            }
            _ => {}
        }
        self.adapter.relay_input(lease_id, &input).await?;
        self.touch(lease_id, now_ms).await;
        Ok(())
    }

    pub fn validate_origin(&self, origin: &str) -> Result<(), BrowserSessionError> {
        self.origin_policy.validate(origin)
    }

    pub fn validate_redirect(&self, origin: &str, allowed_origins: &[String]) -> Result<(), BrowserSessionError> {
        self.origin_policy.validate_redirect(origin, allowed_origins)
    }

    pub async fn request_action_confirmation(
        &self,
        caller_user_id: &str,
        lease_id: &str,
        action: BrowserAction,
        details: impl Into<String>,
        now_ms: u64,
    ) -> Result<ActionConfirmation, BrowserSessionError> {
        let lease = self.owned_active(caller_user_id, lease_id, now_ms).await?;
        if !action.requires_confirmation() {
            return Err(BrowserSessionError::InvalidScope(
                "confirmation is only for high-impact actions".into(),
            ));
        }
        let confirmation = ActionConfirmation {
            confirmation_id: aionui_common::generate_prefixed_id("confirm"),
            lease_id: lease.lease_id,
            action,
            details: details.into(),
            approved: false,
        };
        self.confirmations.write().await.insert(
            confirmation.confirmation_id.clone(),
            PendingConfirmation {
                confirmation: confirmation.clone(),
            },
        );
        Ok(confirmation)
    }

    pub async fn approve_action(
        &self,
        caller_user_id: &str,
        confirmation_id: &str,
        now_ms: u64,
    ) -> Result<ActionConfirmation, BrowserSessionError> {
        let pending = self
            .confirmations
            .read()
            .await
            .get(confirmation_id)
            .map(|p| p.confirmation.clone())
            .ok_or_else(|| BrowserSessionError::ConfirmationNotFound(confirmation_id.into()))?;
        self.owned_active(caller_user_id, &pending.lease_id, now_ms).await?;
        let mut confirmations = self.confirmations.write().await;
        let current = confirmations
            .get_mut(confirmation_id)
            .expect("confirmation checked above");
        current.confirmation.approved = true;
        Ok(current.confirmation.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct DeterministicAdapter;

    #[async_trait::async_trait]
    impl IBrowserSessionAdapter for DeterministicAdapter {
        async fn open(&self, _: &str, _: &BrowserCapabilityScope) -> Result<(), BrowserSessionError> {
            Ok(())
        }
        async fn close(&self, _: &str) -> Result<(), BrowserSessionError> {
            Ok(())
        }
        async fn pause(&self, _: &str) -> Result<(), BrowserSessionError> {
            Ok(())
        }
        async fn resume(&self, _: &str) -> Result<(), BrowserSessionError> {
            Ok(())
        }
        async fn relay_input(&self, _: &str, _: &BrowserInput) -> Result<(), BrowserSessionError> {
            Ok(())
        }
        async fn is_available(&self) -> bool {
            true
        }
    }

    fn scope() -> BrowserCapabilityScope {
        BrowserCapabilityScope {
            user_id: "usr_1".into(),
            conversation_id: "conv_1".into(),
            task_id: "task_1".into(),
            profile_id: "profile_1".into(),
            allowed_origins: vec!["https://example.com".into()],
            capabilities: vec![BrowserCapability::Interact, BrowserCapability::LiveViewTakeover],
            nonce: "nonce_1".into(),
            jti: "jti_1".into(),
        }
    }

    #[tokio::test]
    async fn lease_is_bound_and_user_takeover_is_explicit() {
        let plane = BrowserSessionControlPlane::new(Arc::new(DeterministicAdapter));
        let lease = plane.start("usr_1", scope(), 60_000, 100).await.unwrap();
        assert_eq!(lease.scope.task_id, "task_1");
        assert_eq!(
            plane
                .pause_for_user_takeover("usr_1", &lease.lease_id, 200)
                .await
                .unwrap()
                .status,
            BrowserLeaseStatus::UserTakeoverPaused
        );
        assert_eq!(
            plane
                .resume_from_user_takeover("usr_1", &lease.lease_id, 300)
                .await
                .unwrap()
                .status,
            BrowserLeaseStatus::Active
        );
        assert!(matches!(
            plane.get("usr_2", &lease.lease_id).await,
            Err(BrowserSessionError::AccessDenied(_))
        ));
    }

    #[tokio::test]
    async fn unavailable_transport_and_untrusted_origin_fail_closed() {
        let plane = BrowserSessionControlPlane::new(Arc::new(FailClosedAdapter));
        assert!(matches!(
            plane.start("usr_1", scope(), 60_000, 0).await,
            Err(BrowserSessionError::TransportUnavailable(_))
        ));
        assert!(plane.validate_origin("http://example.com").is_err());
        assert!(
            plane
                .validate_redirect("https://evil.example", &["https://example.com".into()])
                .is_err()
        );
    }

    #[tokio::test]
    async fn server_issued_start_does_not_accept_client_claims() {
        let plane = BrowserSessionControlPlane::new(Arc::new(DeterministicAdapter));
        let keys = BrowserCapabilityKeyProvider::new("kid-a", [b'a'; 32]).unwrap();
        let outcome = plane
            .start_server_issued(
                "usr_1",
                BrowserSessionStartRequest {
                    conversation_id: "conv_1".into(),
                    task_id: "task_1".into(),
                    profile_id: "profile_1".into(),
                    allowed_origins: vec!["https://example.com".into()],
                    allowed_capabilities: vec![BrowserCapability::Observe],
                },
                &keys,
                60_000,
                1_800_000_000_000,
            )
            .await
            .unwrap();
        let claims = keys.verify(&outcome.capability_token).unwrap();
        assert_eq!(claims.lease_id, outcome.lease.lease_id);
        assert_ne!(claims.nonce, "client_nonce");
        assert_ne!(claims.jti, "client_jti");
    }

    #[tokio::test]
    async fn capability_verifier_rejects_replay_and_scope_mismatch() {
        let plane = BrowserSessionControlPlane::new(Arc::new(DeterministicAdapter));
        let keys = Arc::new(BrowserCapabilityKeyProvider::new("kid-a", [b'a'; 32]).unwrap());
        let outcome = plane
            .start_server_issued(
                "usr_1",
                BrowserSessionStartRequest {
                    conversation_id: "conv_1".into(),
                    task_id: "task_1".into(),
                    profile_id: "profile_1".into(),
                    allowed_origins: vec!["https://example.com".into()],
                    allowed_capabilities: vec![BrowserCapability::Observe],
                },
                &keys,
                60_000,
                1_800_000_000_000,
            )
            .await
            .unwrap();
        let verifier = BrowserCapabilityVerifier::new(keys);
        verifier
            .verify_for_lease("usr_1", &outcome.lease, &outcome.capability_token, 1_800_000_000_001)
            .await
            .unwrap();
        assert!(matches!(
            verifier
                .verify_for_lease("usr_1", &outcome.lease, &outcome.capability_token, 1_800_000_000_001)
                .await,
            Err(BrowserSessionError::AccessDenied(_))
        ));
        assert!(matches!(
            verifier
                .verify_for_lease("usr_2", &outcome.lease, "not-a-token", 1_800_000_000_001)
                .await,
            Err(BrowserSessionError::AccessDenied(_))
        ));
    }

    #[test]
    fn relay_frame_bounds_and_format_fail_closed() {
        let valid = BrowserRelayFrame {
            content_type: "image/jpeg".into(),
            width: 1920,
            height: 1080,
            payload: vec![0; 16],
        };
        assert!(valid.validate().is_ok());
        let mut oversized = valid.clone();
        oversized.payload = vec![0; BROWSER_RELAY_MAX_FRAME_BYTES + 1];
        assert!(oversized.validate().is_err());
        let mut bad_format = valid.clone();
        bad_format.content_type = "text/html".into();
        assert!(bad_format.validate().is_err());
        let mut bad_dimensions = valid;
        bad_dimensions.width = 1921;
        assert!(bad_dimensions.validate().is_err());
    }

    #[test]
    fn start_request_rejects_unknown_claim_fields() {
        let result = serde_json::from_str::<BrowserSessionStartRequest>(
            r#"{"conversation_id":"c","task_id":"t","profile_id":"p","allowed_origins":["https://example.com"],"allowed_capabilities":["observe"],"nonce":"client"}"#,
        );
        assert!(result.is_err());
    }

    #[test]
    fn capability_keys_are_server_owned_and_rotate_with_retired_verification() {
        let secret_a = [b'a'; 32];
        let secret_b = [b'b'; 32];
        let mut keys = BrowserCapabilityKeyProvider::new("kid-a", secret_a).unwrap();
        let envelope = BrowserCapabilityEnvelope {
            iss: "aion:core:control-plane".into(),
            aud: "aion:browser-worker:sidecar".into(),
            sub: "usr_1".into(),
            cid: "conv_1".into(),
            tid: "task_1".into(),
            profile_id: "profile_1".into(),
            allowed_origins: vec!["https://example.com".into()],
            allowed_capabilities: vec![BrowserCapability::Observe],
            lease_id: "lease_1".into(),
            nonce: "nonce_server".into(),
            jti: "jti_server".into(),
            iat: 1,
            exp: 4_102_444_800,
        };
        let token = keys.sign(&envelope).unwrap();
        assert_eq!(keys.verify(&token).unwrap().jti, "jti_server");
        keys.rotate("kid-b", secret_b).unwrap();
        assert_eq!(keys.verify(&token).unwrap().sub, "usr_1");
        let token_b = keys.sign(&envelope).unwrap();
        assert_eq!(keys.verify(&token_b).unwrap().aud, envelope.aud);
    }

    struct FailClosedAdapter;
    #[async_trait::async_trait]
    impl IBrowserSessionAdapter for FailClosedAdapter {
        async fn open(&self, _: &str, _: &BrowserCapabilityScope) -> Result<(), BrowserSessionError> {
            Err(BrowserSessionError::TransportUnavailable("off".into()))
        }
        async fn close(&self, _: &str) -> Result<(), BrowserSessionError> {
            Ok(())
        }
        async fn pause(&self, _: &str) -> Result<(), BrowserSessionError> {
            Ok(())
        }
        async fn resume(&self, _: &str) -> Result<(), BrowserSessionError> {
            Ok(())
        }
        async fn relay_input(&self, _: &str, _: &BrowserInput) -> Result<(), BrowserSessionError> {
            Ok(())
        }
        async fn is_available(&self) -> bool {
            false
        }
    }
}
