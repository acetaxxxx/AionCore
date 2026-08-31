use aionui_common::{TimestampMs, decrypt_string, encrypt_string, generate_prefixed_id, now_ms};
use sha2::{Digest, Sha256};
use sqlx::SqlitePool;

use crate::error::DbError;
use crate::repository::push::{IPushSubscriptionRepository, PushSubscriptionRecord, UpsertPushSubscriptionParams};

/// SQLite-backed push subscription store. Secret browser material is encrypted
/// with the application storage key; the endpoint digest is only a lookup key.
#[derive(Clone)]
pub struct SqlitePushSubscriptionRepository {
    pool: SqlitePool,
    encryption_key: [u8; 32],
}

impl SqlitePushSubscriptionRepository {
    pub fn new(pool: SqlitePool, encryption_key: [u8; 32]) -> Self {
        Self { pool, encryption_key }
    }

    fn protect(value: &str, key: &[u8; 32], field: &str) -> Result<String, DbError> {
        encrypt_string(value, key).map_err(|_| DbError::Init(format!("failed to protect push {field}")))
    }

    fn reveal(value: &str, key: &[u8; 32], field: &str) -> Result<String, DbError> {
        decrypt_string(value, key).map_err(|_| DbError::Init(format!("failed to read push {field}")))
    }

    fn endpoint_digest(endpoint: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(endpoint.as_bytes());
        hex::encode(hasher.finalize())
    }

    async fn find_by_endpoint_digest(&self, digest: &str) -> Result<StoredPushSubscriptionRow, DbError> {
        sqlx::query_as::<_, StoredPushSubscriptionRow>(
            "SELECT id, user_id, endpoint_hash, endpoint_encrypted, p256dh_encrypted, auth_encrypted, created_at, updated_at \
             FROM push_subscriptions WHERE endpoint_hash = ?",
        )
        .bind(digest)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| DbError::Init("push subscription upsert did not return a row".into()))
    }

    fn to_record(&self, row: StoredPushSubscriptionRow) -> Result<PushSubscriptionRecord, DbError> {
        Ok(PushSubscriptionRecord {
            id: row.id,
            user_id: row.user_id,
            endpoint: Self::reveal(&row.endpoint_encrypted, &self.encryption_key, "endpoint")?,
            p256dh: Self::reveal(&row.p256dh_encrypted, &self.encryption_key, "p256dh")?,
            auth: Self::reveal(&row.auth_encrypted, &self.encryption_key, "auth")?,
            created_at: row.created_at,
            updated_at: row.updated_at,
        })
    }
}

#[derive(Debug, sqlx::FromRow)]
struct StoredPushSubscriptionRow {
    id: String,
    user_id: String,
    #[allow(dead_code)]
    endpoint_hash: String,
    endpoint_encrypted: String,
    p256dh_encrypted: String,
    auth_encrypted: String,
    created_at: TimestampMs,
    updated_at: TimestampMs,
}

#[async_trait::async_trait]
impl IPushSubscriptionRepository for SqlitePushSubscriptionRepository {
    async fn upsert(&self, params: UpsertPushSubscriptionParams) -> Result<PushSubscriptionRecord, DbError> {
        let now = now_ms();
        let endpoint_hash = Self::endpoint_digest(&params.endpoint);
        let endpoint_encrypted = Self::protect(&params.endpoint, &self.encryption_key, "endpoint")?;
        let p256dh_encrypted = Self::protect(&params.p256dh, &self.encryption_key, "p256dh")?;
        let auth_encrypted = Self::protect(&params.auth, &self.encryption_key, "auth")?;
        let id = generate_prefixed_id("push");

        sqlx::query(
            "INSERT INTO push_subscriptions \
                (id, user_id, endpoint_hash, endpoint_encrypted, p256dh_encrypted, auth_encrypted, created_at, updated_at) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?) \
             ON CONFLICT(endpoint_hash) DO UPDATE SET \
                user_id = excluded.user_id, \
                endpoint_encrypted = excluded.endpoint_encrypted, \
                p256dh_encrypted = excluded.p256dh_encrypted, \
                auth_encrypted = excluded.auth_encrypted, \
                updated_at = excluded.updated_at",
        )
        .bind(&id)
        .bind(&params.user_id)
        .bind(&endpoint_hash)
        .bind(&endpoint_encrypted)
        .bind(&p256dh_encrypted)
        .bind(&auth_encrypted)
        .bind(now)
        .bind(now)
        .execute(&self.pool)
        .await?;

        self.to_record(self.find_by_endpoint_digest(&endpoint_hash).await?)
    }

    async fn delete(&self, user_id: &str, subscription_id: &str) -> Result<(), DbError> {
        let result = sqlx::query("DELETE FROM push_subscriptions WHERE user_id = ? AND id = ?")
            .bind(user_id)
            .bind(subscription_id)
            .execute(&self.pool)
            .await?;

        if result.rows_affected() == 0 {
            return Err(DbError::NotFound("push subscription not found".into()));
        }
        Ok(())
    }

    async fn list_for_user(&self, user_id: &str) -> Result<Vec<PushSubscriptionRecord>, DbError> {
        let rows = sqlx::query_as::<_, StoredPushSubscriptionRow>(
            "SELECT id, user_id, endpoint_hash, endpoint_encrypted, p256dh_encrypted, auth_encrypted, created_at, updated_at \
             FROM push_subscriptions WHERE user_id = ? ORDER BY created_at ASC, id ASC",
        )
        .bind(user_id)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter().map(|row| self.to_record(row)).collect()
    }
}
