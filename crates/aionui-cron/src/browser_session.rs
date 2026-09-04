//! Generic, fail-closed browser capability and session control plane.
//!
//! This module is deliberately transport agnostic.  Concrete WebSocket/WebRTC
//! or browser-worker implementations live behind `IBrowserSessionAdapter`; the
//! control plane owns identity, capability, lease and origin checks.

use std::collections::HashMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use thiserror::Error;

pub const BROWSER_IDLE_LEASE_MS: u64 = 30 * 60 * 1000;
pub const BROWSER_ABSOLUTE_LEASE_MS: u64 = 4 * 60 * 60 * 1000;

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
            return Err(BrowserSessionError::InvalidScope("allowed_origins cannot be empty".into()));
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
        matches!(self.status, BrowserLeaseStatus::Active | BrowserLeaseStatus::UserTakeoverPaused)
            && now_ms < self.expires_at_ms
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
            Err(BrowserSessionError::OriginRejected(format!("redirect outside allowlist: {origin}")))
        }
    }
}

struct PendingConfirmation {
    confirmation: ActionConfirmation,
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
            return Err(BrowserSessionError::AccessDenied("caller does not own browser scope".into()));
        }
        if requested_ttl_ms == 0 {
            return Err(BrowserSessionError::InvalidScope("lease ttl must be positive".into()));
        }
        if !self.adapter.is_available().await {
            return Err(BrowserSessionError::TransportUnavailable("browser transport is unavailable".into()));
        }
        let lease_id = aionui_common::generate_prefixed_id("lease");
        let expires_at_ms = now_ms
            .saturating_add(requested_ttl_ms.min(BROWSER_ABSOLUTE_LEASE_MS));
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

    async fn owned_active(&self, caller_user_id: &str, lease_id: &str, now_ms: u64) -> Result<BrowserLease, BrowserSessionError> {
        let lease = self.leases.read().await.get(lease_id).cloned().ok_or_else(|| BrowserSessionError::NotFound(lease_id.into()))?;
        if caller_user_id != lease.scope.user_id {
            return Err(BrowserSessionError::AccessDenied("caller does not own browser lease".into()));
        }
        if now_ms >= lease.expires_at_ms {
            return Err(BrowserSessionError::Expired { expires_at_ms: lease.expires_at_ms, now_ms });
        }
        if now_ms.saturating_sub(lease.last_activity_at_ms) > BROWSER_IDLE_LEASE_MS {
            return Err(BrowserSessionError::IdleTimeout);
        }
        if !matches!(lease.status, BrowserLeaseStatus::Active | BrowserLeaseStatus::UserTakeoverPaused) {
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
        let lease = self.leases.read().await.get(lease_id).cloned().ok_or_else(|| BrowserSessionError::NotFound(lease_id.into()))?;
        if caller_user_id != lease.scope.user_id { return Err(BrowserSessionError::AccessDenied("caller does not own browser lease".into())); }
        Ok(lease)
    }

    pub async fn renew(&self, caller_user_id: &str, lease_id: &str, requested_ttl_ms: u64, now_ms: u64) -> Result<BrowserLease, BrowserSessionError> {
        let lease = self.owned_active(caller_user_id, lease_id, now_ms).await?;
        if requested_ttl_ms == 0 { return Err(BrowserSessionError::InvalidScope("renew ttl must be positive".into())); }
        let mut leases = self.leases.write().await;
        let current = leases.get_mut(lease_id).ok_or_else(|| BrowserSessionError::NotFound(lease_id.into()))?;
        current.expires_at_ms = current.expires_at_ms.max(now_ms).saturating_add(requested_ttl_ms.min(BROWSER_IDLE_LEASE_MS)).min(current.created_at_ms.saturating_add(BROWSER_ABSOLUTE_LEASE_MS));
        current.last_activity_at_ms = current.last_activity_at_ms.max(now_ms);
        let _ = lease;
        Ok(current.clone())
    }

    pub async fn end(&self, caller_user_id: &str, lease_id: &str, now_ms: u64) -> Result<BrowserLease, BrowserSessionError> {
        let lease = self.owned_active(caller_user_id, lease_id, now_ms).await?;
        self.adapter.close(lease_id).await?;
        let mut leases = self.leases.write().await;
        let current = leases.get_mut(lease_id).expect("lease checked above");
        current.status = BrowserLeaseStatus::Ended;
        current.closed_at_ms = Some(now_ms);
        Ok(current.clone())
    }

    pub async fn revoke(&self, caller_user_id: &str, lease_id: &str, reason: impl Into<String>, now_ms: u64) -> Result<BrowserLease, BrowserSessionError> {
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

    pub async fn pause_for_user_takeover(&self, caller_user_id: &str, lease_id: &str, now_ms: u64) -> Result<BrowserLease, BrowserSessionError> {
        let lease = self.owned_active(caller_user_id, lease_id, now_ms).await?;
        if !lease.scope.has_capability(BrowserCapability::LiveViewTakeover) { return Err(BrowserSessionError::CapabilityDenied(BrowserCapability::LiveViewTakeover)); }
        self.adapter.pause(lease_id).await?;
        let mut leases = self.leases.write().await;
        let current = leases.get_mut(lease_id).expect("lease checked above");
        current.status = BrowserLeaseStatus::UserTakeoverPaused;
        current.paused_at_ms = Some(now_ms);
        Ok(current.clone())
    }

    pub async fn resume_from_user_takeover(&self, caller_user_id: &str, lease_id: &str, now_ms: u64) -> Result<BrowserLease, BrowserSessionError> {
        let lease = self.owned_active(caller_user_id, lease_id, now_ms).await?;
        if lease.status != BrowserLeaseStatus::UserTakeoverPaused { return Err(BrowserSessionError::LeaseClosed(lease.status)); }
        self.adapter.resume(lease_id).await?;
        let mut leases = self.leases.write().await;
        let current = leases.get_mut(lease_id).expect("lease checked above");
        current.status = BrowserLeaseStatus::Active;
        current.last_activity_at_ms = current.last_activity_at_ms.max(now_ms);
        Ok(current.clone())
    }

    pub async fn relay_input(&self, caller_user_id: &str, lease_id: &str, input: BrowserInput, now_ms: u64) -> Result<(), BrowserSessionError> {
        let lease = self.owned_active(caller_user_id, lease_id, now_ms).await?;
        if !lease.scope.has_capability(BrowserCapability::LiveViewTakeover) || !lease.scope.has_capability(BrowserCapability::Interact) { return Err(BrowserSessionError::CapabilityDenied(BrowserCapability::Interact)); }
        match &input {
            BrowserInput::Pointer { kind, x, y } if *x > 1920 || *y > 1080 || !matches!(kind.as_str(), "move" | "down" | "up" | "click" | "wheel") => return Err(BrowserSessionError::InputRejected("pointer outside bounded allowlist".into())),
            BrowserInput::Keyboard { kind, key, code } if key.len() > 128 || code.len() > 128 || !matches!(kind.as_str(), "down" | "up" | "text") => return Err(BrowserSessionError::InputRejected("keyboard outside bounded allowlist".into())),
            _ => {}
        }
        self.adapter.relay_input(lease_id, &input).await?;
        self.touch(lease_id, now_ms).await;
        Ok(())
    }

    pub fn validate_origin(&self, origin: &str) -> Result<(), BrowserSessionError> { self.origin_policy.validate(origin) }

    pub fn validate_redirect(&self, origin: &str, allowed_origins: &[String]) -> Result<(), BrowserSessionError> { self.origin_policy.validate_redirect(origin, allowed_origins) }

    pub async fn request_action_confirmation(&self, caller_user_id: &str, lease_id: &str, action: BrowserAction, details: impl Into<String>, now_ms: u64) -> Result<ActionConfirmation, BrowserSessionError> {
        let lease = self.owned_active(caller_user_id, lease_id, now_ms).await?;
        if !action.requires_confirmation() { return Err(BrowserSessionError::InvalidScope("confirmation is only for high-impact actions".into())); }
        let confirmation = ActionConfirmation { confirmation_id: aionui_common::generate_prefixed_id("confirm"), lease_id: lease.lease_id, action, details: details.into(), approved: false };
        self.confirmations.write().await.insert(confirmation.confirmation_id.clone(), PendingConfirmation { confirmation: confirmation.clone() });
        Ok(confirmation)
    }

    pub async fn approve_action(&self, caller_user_id: &str, confirmation_id: &str, now_ms: u64) -> Result<ActionConfirmation, BrowserSessionError> {
        let pending = self.confirmations.read().await.get(confirmation_id).map(|p| p.confirmation.clone()).ok_or_else(|| BrowserSessionError::ConfirmationNotFound(confirmation_id.into()))?;
        self.owned_active(caller_user_id, &pending.lease_id, now_ms).await?;
        let mut confirmations = self.confirmations.write().await;
        let current = confirmations.get_mut(confirmation_id).expect("confirmation checked above");
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
        async fn open(&self, _: &str, _: &BrowserCapabilityScope) -> Result<(), BrowserSessionError> { Ok(()) }
        async fn close(&self, _: &str) -> Result<(), BrowserSessionError> { Ok(()) }
        async fn pause(&self, _: &str) -> Result<(), BrowserSessionError> { Ok(()) }
        async fn resume(&self, _: &str) -> Result<(), BrowserSessionError> { Ok(()) }
        async fn relay_input(&self, _: &str, _: &BrowserInput) -> Result<(), BrowserSessionError> { Ok(()) }
        async fn is_available(&self) -> bool { true }
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
        assert_eq!(plane.pause_for_user_takeover("usr_1", &lease.lease_id, 200).await.unwrap().status, BrowserLeaseStatus::UserTakeoverPaused);
        assert_eq!(plane.resume_from_user_takeover("usr_1", &lease.lease_id, 300).await.unwrap().status, BrowserLeaseStatus::Active);
        assert!(matches!(plane.get("usr_2", &lease.lease_id).await, Err(BrowserSessionError::AccessDenied(_))));
    }

    #[tokio::test]
    async fn unavailable_transport_and_untrusted_origin_fail_closed() {
        let plane = BrowserSessionControlPlane::new(Arc::new(FailClosedAdapter));
        assert!(matches!(plane.start("usr_1", scope(), 60_000, 0).await, Err(BrowserSessionError::TransportUnavailable(_))));
        assert!(plane.validate_origin("http://example.com").is_err());
        assert!(plane.validate_redirect("https://evil.example", &["https://example.com".into()]).is_err());
    }

    struct FailClosedAdapter;
    #[async_trait::async_trait]
    impl IBrowserSessionAdapter for FailClosedAdapter {
        async fn open(&self, _: &str, _: &BrowserCapabilityScope) -> Result<(), BrowserSessionError> { Err(BrowserSessionError::TransportUnavailable("off".into())) }
        async fn close(&self, _: &str) -> Result<(), BrowserSessionError> { Ok(()) }
        async fn pause(&self, _: &str) -> Result<(), BrowserSessionError> { Ok(()) }
        async fn resume(&self, _: &str) -> Result<(), BrowserSessionError> { Ok(()) }
        async fn relay_input(&self, _: &str, _: &BrowserInput) -> Result<(), BrowserSessionError> { Ok(()) }
        async fn is_available(&self) -> bool { false }
    }
}
