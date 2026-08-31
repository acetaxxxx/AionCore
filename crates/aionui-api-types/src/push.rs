use serde::{Deserialize, Serialize};

/// Public Push capability state. The private VAPID key is never exposed.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PushConfigResponse {
    pub enabled: bool,
    pub public_vapid_key: Option<String>,
}

/// Browser-generated subscription material. User identity is intentionally
/// absent; the authenticated request determines ownership.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UpsertPushSubscriptionRequest {
    pub endpoint: String,
    pub p256dh: String,
    pub auth: String,
}

/// Safe subscription reference returned to the browser. Secret endpoint and
/// key material are never echoed by the API.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PushSubscriptionResponse {
    pub id: String,
}
