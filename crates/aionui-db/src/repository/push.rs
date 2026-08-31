use crate::error::DbError;

/// User-scoped browser subscription record. Endpoint and key material are
/// never suitable for logs or API responses.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PushSubscriptionRecord {
    pub id: String,
    pub user_id: String,
    pub endpoint: String,
    pub p256dh: String,
    pub auth: String,
    pub created_at: aionui_common::TimestampMs,
    pub updated_at: aionui_common::TimestampMs,
}

/// Plaintext input accepted only at the repository/service boundary. The
/// SQLite adapter encrypts the browser-controlled values before persistence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpsertPushSubscriptionParams {
    pub user_id: String,
    pub endpoint: String,
    pub p256dh: String,
    pub auth: String,
}

/// Persistence port for the browser push subscription lifecycle.
#[async_trait::async_trait]
pub trait IPushSubscriptionRepository: Send + Sync {
    async fn upsert(&self, params: UpsertPushSubscriptionParams) -> Result<PushSubscriptionRecord, DbError>;

    async fn delete(&self, user_id: &str, subscription_id: &str) -> Result<(), DbError>;

    /// Removes one provider-reported endpoint without allowing a caller to
    /// cross the authenticated owner scope.
    async fn delete_by_endpoint(&self, user_id: &str, endpoint: &str) -> Result<(), DbError>;

    async fn list_for_user(&self, user_id: &str) -> Result<Vec<PushSubscriptionRecord>, DbError>;
}
