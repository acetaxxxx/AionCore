use std::sync::Arc;

use aionui_db::{IPushSubscriptionRepository, UpsertPushSubscriptionParams};
use base64::Engine;
use url::Url;

use crate::error::PushError;

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

pub struct PushDeliveryService {
    repository: Arc<dyn IPushSubscriptionRepository>,
}

impl PushDeliveryService {
    pub fn new(repository: Arc<dyn IPushSubscriptionRepository>) -> Self {
        Self { repository }
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
