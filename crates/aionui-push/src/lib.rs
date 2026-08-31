#![warn(clippy::disallowed_types)]

//! Browser push subscription lifecycle.

mod error;
pub mod routes;
pub mod service;
pub mod state;

pub use error::PushError;
pub use routes::push_routes;
pub use service::{PushDeliveryService, PushSubscriptionInput, PushSubscriptionRef};
pub use state::PushRouterState;

#[cfg(test)]
mod subscription_lifecycle_tests {
    use base64::Engine;
    use std::sync::Arc;

    use super::{PushDeliveryService, PushError, PushSubscriptionInput};
    use crate::testing::InMemoryPushSubscriptionRepository;

    fn key_material(length: usize, value: u8) -> String {
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(vec![value; length])
    }

    #[tokio::test]
    async fn endpoint_upsert_is_idempotent_and_user_scoped() {
        let repository = Arc::new(InMemoryPushSubscriptionRepository::default());
        let service = PushDeliveryService::new(repository.clone());

        let first = service
            .upsert_subscription(
                "user-a",
                PushSubscriptionInput {
                    endpoint: "https://push.example/subscription-a".into(),
                    p256dh: key_material(65, 1),
                    auth: key_material(16, 2),
                },
            )
            .await
            .expect("first subscription should be accepted");

        let replacement = service
            .upsert_subscription(
                "user-a",
                PushSubscriptionInput {
                    endpoint: "https://push.example/subscription-a".into(),
                    p256dh: key_material(65, 3),
                    auth: key_material(16, 4),
                },
            )
            .await
            .expect("same browser should update its subscription");

        assert_eq!(first.id, replacement.id);
        assert_eq!(repository.count_for_user("user-a").await, 1);
        assert_eq!(repository.count_for_user("user-b").await, 0);
    }

    #[tokio::test]
    async fn invalid_subscription_is_rejected_before_persistence() {
        let repository = Arc::new(InMemoryPushSubscriptionRepository::default());
        let service = PushDeliveryService::new(repository.clone());

        let error = service
            .upsert_subscription(
                "user-a",
                PushSubscriptionInput {
                    endpoint: "http://push.example/insecure".into(),
                    p256dh: key_material(65, 1),
                    auth: key_material(16, 2),
                },
            )
            .await
            .expect_err("insecure endpoints must fail closed");

        assert!(matches!(error, PushError::InvalidSubscription));
        assert_eq!(repository.count_for_user("user-a").await, 0);
    }

    #[tokio::test]
    async fn delete_requires_the_authenticated_owner_scope() {
        let repository = Arc::new(InMemoryPushSubscriptionRepository::default());
        let service = PushDeliveryService::new(repository.clone());
        let subscription = service
            .upsert_subscription(
                "user-a",
                PushSubscriptionInput {
                    endpoint: "https://push.example/subscription-a".into(),
                    p256dh: key_material(65, 1),
                    auth: key_material(16, 2),
                },
            )
            .await
            .unwrap();

        let error = service
            .delete_subscription("user-b", &subscription.id)
            .await
            .expect_err("a different user cannot delete the subscription");

        assert!(matches!(error, PushError::NotFound));
        assert_eq!(repository.count_for_user("user-a").await, 1);
    }
}

#[cfg(test)]
mod testing {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use aionui_db::{DbError, IPushSubscriptionRepository, PushSubscriptionRecord, UpsertPushSubscriptionParams};

    #[derive(Clone, Default)]
    pub struct InMemoryPushSubscriptionRepository {
        records: Arc<Mutex<HashMap<String, PushSubscriptionRecord>>>,
    }

    impl InMemoryPushSubscriptionRepository {
        pub async fn count_for_user(&self, user_id: &str) -> usize {
            self.records
                .lock()
                .expect("in-memory push repository lock should not be poisoned")
                .values()
                .filter(|record| record.user_id == user_id)
                .count()
        }
    }

    #[async_trait::async_trait]
    impl IPushSubscriptionRepository for InMemoryPushSubscriptionRepository {
        async fn upsert(
            &self,
            params: UpsertPushSubscriptionParams,
        ) -> Result<PushSubscriptionRecord, DbError> {
            let mut records = self
                .records
                .lock()
                .expect("in-memory push repository lock should not be poisoned");
            let now = 1;
            if let Some(existing) = records.values_mut().find(|record| record.endpoint == params.endpoint) {
                existing.user_id = params.user_id;
                existing.p256dh = params.p256dh;
                existing.auth = params.auth;
                existing.updated_at = now;
                return Ok(existing.clone());
            }

            let record = PushSubscriptionRecord {
                id: format!("push-{}", records.len() + 1),
                user_id: params.user_id,
                endpoint: params.endpoint.clone(),
                p256dh: params.p256dh,
                auth: params.auth,
                created_at: now,
                updated_at: now,
            };
            records.insert(params.endpoint, record.clone());
            Ok(record)
        }

        async fn delete(&self, user_id: &str, subscription_id: &str) -> Result<(), DbError> {
            let mut records = self
                .records
                .lock()
                .expect("in-memory push repository lock should not be poisoned");
            let endpoint = records
                .iter()
                .find(|(_, record)| record.user_id == user_id && record.id == subscription_id)
                .map(|(endpoint, _)| endpoint.clone())
                .ok_or_else(|| DbError::NotFound("push subscription not found".into()))?;
            records.remove(&endpoint);
            Ok(())
        }

        async fn list_for_user(&self, user_id: &str) -> Result<Vec<PushSubscriptionRecord>, DbError> {
            Ok(self
                .records
                .lock()
                .expect("in-memory push repository lock should not be poisoned")
                .values()
                .filter(|record| record.user_id == user_id)
                .cloned()
                .collect())
        }
    }
}
