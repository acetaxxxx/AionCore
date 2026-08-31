use std::sync::Arc;

use aionui_common::{OnConversationTurnTerminal, TurnTerminalNotice};
use aionui_db::{IPushSubscriptionRepository, UpsertPushSubscriptionParams};
use base64::Engine;
use url::Url;

use crate::error::PushError;
use crate::payload::build_terminal_payload;
use crate::sender::{DisabledPushSender, PushSendError, PushSender};

const MAX_ENDPOINT_BYTES: usize = 2_048;
const MAX_USER_ID_BYTES: usize = 256;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PushSubscriptionInput {
    pub endpoint: String,
    pub p256dh: String,
    pub auth: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PushSubscriptionRef {
    pub id: String,
}

#[derive(Clone)]
pub struct PushDeliveryService {
    repository: Arc<dyn IPushSubscriptionRepository>,
    sender: Arc<dyn PushSender>,
}

#[derive(Debug, Clone, Copy)]
enum DeliveryOutcome {
    Delivered,
    Removed,
    Failed,
}

impl PushDeliveryService {
    pub fn new(repository: Arc<dyn IPushSubscriptionRepository>) -> Self {
        Self {
            repository,
            sender: Arc::new(DisabledPushSender),
        }
    }

    pub fn with_sender(mut self, sender: Arc<dyn PushSender>) -> Self {
        self.sender = sender;
        self
    }

    pub fn is_enabled(&self) -> bool {
        self.sender.is_configured()
    }

    pub async fn upsert_subscription(
        &self,
        user_id: &str,
        input: PushSubscriptionInput,
    ) -> Result<PushSubscriptionRef, PushError> {
        let user_id = validate_user_id(user_id)?;
        let input = normalize_and_validate(input)?;
        let record = self
            .repository
            .upsert(UpsertPushSubscriptionParams {
                user_id,
                endpoint: input.endpoint,
                p256dh: input.p256dh,
                auth: input.auth,
            })
            .await?;
        Ok(PushSubscriptionRef { id: record.id })
    }

    pub async fn delete_subscription(&self, user_id: &str, subscription_id: &str) -> Result<(), PushError> {
        let user_id = validate_user_id(user_id)?;
        if subscription_id.trim().is_empty() || subscription_id.len() > 128 {
            return Err(PushError::NotFound);
        }
        self.repository.delete(&user_id, subscription_id.trim()).await?;
        Ok(())
    }

    /// Dispatches one trusted terminal notice to every current subscription
    /// owned by the notice. Each provider call is isolated; a provider error
    /// never changes the already durable conversation result.
    pub async fn deliver_terminal_notice(&self, notice: TurnTerminalNotice) {
        let Ok(payload) = build_terminal_payload(&notice) else {
            tracing::warn!("terminal push notification was rejected by payload policy");
            return;
        };
        if !self.sender.is_configured() {
            return;
        }
        let subscriptions = match self.repository.list_for_user(&notice.user_id).await {
            Ok(subscriptions) => subscriptions,
            Err(_) => {
                tracing::warn!("terminal push fanout skipped because subscriptions could not be loaded");
                return;
            }
        };
        let mut deliveries = tokio::task::JoinSet::new();
        let mut attempted = 0_u32;
        for subscription in subscriptions {
            attempted = attempted.saturating_add(1);
            let sender = Arc::clone(&self.sender);
            let repository = Arc::clone(&self.repository);
            let payload = payload.clone();
            deliveries.spawn(async move {
                match sender.send(&subscription, &payload).await {
                    Ok(()) => DeliveryOutcome::Delivered,
                    Err(PushSendError::Gone) => {
                        if repository
                            .delete_by_endpoint(&subscription.user_id, &subscription.endpoint)
                            .await
                            .is_ok()
                        {
                            DeliveryOutcome::Removed
                        } else {
                            tracing::debug!("gone push subscription cleanup failed");
                            DeliveryOutcome::Failed
                        }
                    }
                    Err(PushSendError::Unavailable | PushSendError::Rejected | PushSendError::Transport) => {
                        tracing::debug!("one terminal push delivery failed; continuing fanout");
                        DeliveryOutcome::Failed
                    }
                }
            });
        }
        let mut delivered = 0_u32;
        let mut removed = 0_u32;
        let mut failed = 0_u32;
        while let Some(result) = deliveries.join_next().await {
            match result {
                Ok(DeliveryOutcome::Delivered) => delivered = delivered.saturating_add(1),
                Ok(DeliveryOutcome::Removed) => removed = removed.saturating_add(1),
                Ok(DeliveryOutcome::Failed) => failed = failed.saturating_add(1),
                Err(_) => {
                    failed = failed.saturating_add(1);
                    tracing::debug!("one terminal push delivery task failed; continuing fanout");
                }
            }
        }
        if attempted > 0 {
            tracing::info!(
                status = %payload.status,
                target_kind = %payload.target_kind,
                attempted,
                delivered,
                removed,
                failed,
                "terminal push fanout completed"
            );
        }
    }
}

#[async_trait::async_trait]
impl OnConversationTurnTerminal for PushDeliveryService {
    async fn on_turn_terminal(&self, notice: TurnTerminalNotice) {
        // The conversation terminal path is deliberately not coupled to
        // provider latency or availability. The owned service clone keeps the
        // repository/sender alive until this best-effort task finishes.
        let service = self.clone();
        tokio::spawn(async move {
            service.deliver_terminal_notice(notice).await;
        });
    }
}

fn validate_user_id(user_id: &str) -> Result<String, PushError> {
    let normalized = user_id.trim();
    if normalized.is_empty()
        || normalized.len() > MAX_USER_ID_BYTES
        || normalized.chars().any(|character| character.is_control())
    {
        return Err(PushError::InvalidUserScope);
    }
    Ok(normalized.to_owned())
}

fn normalize_and_validate(mut input: PushSubscriptionInput) -> Result<PushSubscriptionInput, PushError> {
    input.endpoint = input.endpoint.trim().to_owned();
    input.p256dh = input.p256dh.trim().to_owned();
    input.auth = input.auth.trim().to_owned();

    let endpoint = Url::parse(&input.endpoint).map_err(|_| PushError::InvalidSubscription)?;
    if endpoint.scheme() != "https"
        || endpoint.host_str().is_none()
        || endpoint.username() != ""
        || endpoint.password().is_some()
        || endpoint.fragment().is_some()
        || input.endpoint.len() > MAX_ENDPOINT_BYTES
    {
        return Err(PushError::InvalidSubscription);
    }

    validate_key(&input.p256dh, 65)?;
    validate_key(&input.auth, 16)?;
    Ok(input)
}

fn validate_key(value: &str, expected_bytes: usize) -> Result<(), PushError> {
    if value.is_empty() || value.len() > 512 || value.chars().any(|character| character.is_ascii_whitespace()) {
        return Err(PushError::InvalidSubscription);
    }
    let decoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(value)
        .or_else(|_| base64::engine::general_purpose::URL_SAFE.decode(value))
        .map_err(|_| PushError::InvalidSubscription)?;
    if decoded.len() != expected_bytes {
        return Err(PushError::InvalidSubscription);
    }
    Ok(())
}
