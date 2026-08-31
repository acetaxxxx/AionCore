#![warn(clippy::disallowed_types)]

//! Browser push subscription lifecycle.

mod error;
mod payload;
pub mod routes;
pub mod service;
mod sender;
pub mod state;

pub use error::PushError;
pub use payload::{PUSH_PAYLOAD_SCHEMA_VERSION, PushPayload, build_terminal_payload};
pub use routes::push_routes;
pub use service::{PushDeliveryService, PushSubscriptionInput, PushSubscriptionRef};
pub use sender::{DisabledPushSender, PushSendError, PushSender, WebPushSender};
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

        pub async fn endpoints_for_user(&self, user_id: &str) -> Vec<String> {
            self.records
                .lock()
                .expect("in-memory push repository lock should not be poisoned")
                .values()
                .filter(|record| record.user_id == user_id)
                .map(|record| record.endpoint.clone())
                .collect()
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

        async fn delete_by_endpoint(&self, user_id: &str, endpoint: &str) -> Result<(), DbError> {
            let mut records = self
                .records
                .lock()
                .expect("in-memory push repository lock should not be poisoned");
            if records
                .get(endpoint)
                .is_some_and(|record| record.user_id == user_id)
            {
                records.remove(endpoint);
                return Ok(());
            }
            Err(DbError::NotFound("push subscription not found".into()))
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

#[cfg(test)]
mod terminal_delivery_tests {
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use base64::Engine;
    use aionui_common::{
        OnConversationTurnTerminal, TerminalNoticeStatus, TerminalTargetKind, TurnTerminalNotice,
    };
    use aionui_db::PushSubscriptionRecord;

    use super::{
        PushDeliveryService, PushPayload, PushSendError, PushSender, build_terminal_payload,
    };
    use crate::testing::InMemoryPushSubscriptionRepository;

    fn key_material(length: usize, value: u8) -> String {
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(vec![value; length])
    }

    #[derive(Default)]
    struct RecordingSender {
        payloads: Mutex<Vec<PushPayload>>,
        fail_endpoint: Option<String>,
        gone_endpoint: Option<String>,
        notify: tokio::sync::Notify,
    }

    #[async_trait::async_trait]
    impl PushSender for RecordingSender {
        async fn send(
            &self,
            subscription: &PushSubscriptionRecord,
            payload: &PushPayload,
        ) -> Result<(), PushSendError> {
            if self.gone_endpoint.as_deref() == Some(subscription.endpoint.as_str()) {
                return Err(PushSendError::Gone);
            }
            if self.fail_endpoint.as_deref() == Some(subscription.endpoint.as_str()) {
                return Err(PushSendError::Transport);
            }
            self.payloads.lock().expect("sender lock").push(payload.clone());
            self.notify.notify_one();
            Ok(())
        }

        fn is_configured(&self) -> bool {
            true
        }
    }

    fn notice(status: TerminalNoticeStatus) -> TurnTerminalNotice {
        TurnTerminalNotice::new(
            "user-a",
            TerminalTargetKind::Conversation,
            "conversation-1",
            "turn-1",
            status,
            123,
        )
        .expect("test notice should be valid")
    }

    #[test]
    fn terminal_payload_is_bounded_and_does_not_copy_runtime_content() {
        let payload = build_terminal_payload(&notice(TerminalNoticeStatus::Success)).expect("payload");
        let encoded = serde_json::to_string(&payload).expect("payload json");

        assert_eq!(payload.schema_version, 1);
        assert_eq!(payload.target_kind, "conversation");
        assert!(!encoded.contains("assistant"));
        assert!(!encoded.contains("secret"));
        assert!(payload.title.len() <= 80);
        assert!(payload.body.len() <= 160);
    }

    #[tokio::test]
    async fn terminal_notice_fanout_attempts_each_subscription_and_is_async_hook() {
        let repository = Arc::new(InMemoryPushSubscriptionRepository::default());
        let sender = Arc::new(RecordingSender::default());
        let service = Arc::new(PushDeliveryService::new(repository.clone()).with_sender(sender.clone()));

        for (endpoint, value) in [("https://push.example/a", 1), ("https://push.example/b", 2)] {
            service
                .upsert_subscription(
                    "user-a",
                    super::PushSubscriptionInput {
                        endpoint: endpoint.into(),
                        p256dh: key_material(65, value),
                        auth: key_material(16, value + 10),
                    },
                )
                .await
                .expect("subscription");
        }

        OnConversationTurnTerminal::on_turn_terminal(service.as_ref(), notice(TerminalNoticeStatus::Success)).await;
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if sender.payloads.lock().expect("sender lock").len() == 2 {
                    break;
                }
                sender.notify.notified().await;
            }
        })
        .await
        .expect("terminal delivery should finish asynchronously");

        assert_eq!(sender.payloads.lock().expect("sender lock").len(), 2);
    }

    #[tokio::test]
    async fn sender_failure_does_not_suppress_other_subscriptions() {
        let repository = Arc::new(InMemoryPushSubscriptionRepository::default());
        let sender = Arc::new(RecordingSender {
            payloads: Mutex::new(Vec::new()),
            fail_endpoint: Some("https://push.example/a".into()),
            gone_endpoint: None,
            notify: tokio::sync::Notify::new(),
        });
        let service = PushDeliveryService::new(repository.clone()).with_sender(sender.clone());

        for (endpoint, value) in [("https://push.example/a", 1), ("https://push.example/b", 2)] {
            service
                .upsert_subscription(
                    "user-a",
                    super::PushSubscriptionInput {
                        endpoint: endpoint.into(),
                        p256dh: key_material(65, value),
                        auth: key_material(16, value + 10),
                    },
                )
                .await
                .expect("subscription");
        }

        service.deliver_terminal_notice(notice(TerminalNoticeStatus::Failed)).await;

        assert_eq!(sender.payloads.lock().expect("sender lock").len(), 1);
    }

    #[tokio::test]
    async fn provider_gone_removes_only_the_affected_subscription() {
        let repository = Arc::new(InMemoryPushSubscriptionRepository::default());
        let sender = Arc::new(RecordingSender {
            payloads: Mutex::new(Vec::new()),
            fail_endpoint: None,
            gone_endpoint: Some("https://push.example/a".into()),
            notify: tokio::sync::Notify::new(),
        });
        let service = PushDeliveryService::new(repository.clone()).with_sender(sender);

        for (endpoint, value) in [("https://push.example/a", 1), ("https://push.example/b", 2)] {
            service
                .upsert_subscription(
                    "user-a",
                    super::PushSubscriptionInput {
                        endpoint: endpoint.into(),
                        p256dh: key_material(65, value),
                        auth: key_material(16, value + 10),
                    },
                )
                .await
                .expect("subscription");
        }

        service.deliver_terminal_notice(notice(TerminalNoticeStatus::Timeout)).await;

        assert_eq!(repository.endpoints_for_user("user-a").await, vec!["https://push.example/b"]);
    }

    #[test]
    fn all_terminal_statuses_use_the_same_bounded_payload_seam() {
        let cases = [
            (TerminalNoticeStatus::Success, "success"),
            (TerminalNoticeStatus::Failed, "failed"),
            (TerminalNoticeStatus::Cancelled, "cancelled"),
            (TerminalNoticeStatus::Timeout, "timeout"),
        ];

        for (status, expected_status) in cases {
            let payload = build_terminal_payload(&notice(status)).expect("payload");
            assert_eq!(payload.status, expected_status);
            assert_eq!(payload.target_kind, "conversation");
            assert_eq!(payload.target_id, "conversation-1");
        }
    }

    #[tokio::test]
    async fn missing_vapid_configuration_disables_push_without_delivery_error() {
        let repository = Arc::new(InMemoryPushSubscriptionRepository::default());
        let service = PushDeliveryService::new(repository.clone());

        assert!(!service.is_enabled());
        service.deliver_terminal_notice(notice(TerminalNoticeStatus::Success)).await;
        assert_eq!(repository.count_for_user("user-a").await, 0);
    }
}
