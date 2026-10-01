use aionui_api_types::WebSocketMessage;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use tokio::sync::broadcast;
use tokio::sync::OwnedRwLockReadGuard;
use tracing::warn;

/// Current recipients for scoped events, shared by the event bus and the
/// final WebSocket delivery. The registry filters enqueueing, while a separate
/// process-local gate fences in-flight socket writes during revocation.
#[derive(Clone, Default)]
pub struct ScopedEventRecipients {
    recipients: Arc<RwLock<HashMap<String, HashSet<String>>>>,
    delivery_gate: Arc<tokio::sync::RwLock<()>>,
}

impl ScopedEventRecipients {
    pub fn replace(&self, scope_id: &str, user_ids: impl IntoIterator<Item = String>) {
        let recipients = user_ids.into_iter().filter(|id| !id.is_empty()).collect();
        if let Ok(mut current) = self.recipients.write() {
            current.insert(scope_id.to_owned(), recipients);
        }
    }

    pub fn revoke(&self, scope_id: &str, user_id: &str) {
        if let Ok(mut current) = self.recipients.write()
            && let Some(recipients) = current.get_mut(scope_id)
        {
            recipients.remove(user_id);
        }
    }

    pub fn snapshot(&self, scope_id: &str) -> Vec<String> {
        self.recipients
            .read()
            .ok()
            .and_then(|current| current.get(scope_id).cloned())
            .map(|current| current.into_iter().collect())
            .unwrap_or_default()
    }

    /// Runs the delivery callback while holding a read lock so a concurrent
    /// revoke cannot return until all already-authorized enqueues finish.
    /// Missing/poisoned scope state fails closed.
    pub fn with_authorized_recipients(
        &self,
        scope_id: &str,
        owner_user_id: &str,
        requested: &[String],
        mut deliver: impl FnMut(&str),
    ) -> bool {
        let Ok(current) = self.recipients.read() else {
            return false;
        };
        let authorized = current.get(scope_id);
        for user_id in requested {
            // Before a Team has loaded its membership snapshot, preserve
            // owner-only delivery without trusting collaborator IDs from the
            // event. Once registered, the current set is authoritative.
            let is_authorized = authorized
                .map(|recipients| recipients.contains(user_id))
                .unwrap_or_else(|| user_id == owner_user_id);
            if is_authorized {
                deliver(user_id);
            }
        }
        true
    }

    /// Acquires a read permit for a final socket delivery and checks current
    /// scope membership. The permit remains held through the actual socket
    /// write; revocation waits for in-flight writes before returning. This gate
    /// coordinates only this Core process, not multiple replicas.
    pub async fn authorize_delivery(
        &self,
        scope_id: &str,
        owner_user_id: &str,
        recipient_user_id: &str,
    ) -> Option<OwnedRwLockReadGuard<()>> {
        let permit = self.delivery_gate.clone().read_owned().await;
        let authorized = match self.recipients.read() {
            Ok(current) => current
                .get(scope_id)
                .map(|recipients| recipients.contains(recipient_user_id))
                .unwrap_or_else(|| recipient_user_id == owner_user_id),
            Err(_) => false,
        };
        authorized.then_some(permit)
    }

    /// Waits for socket writes that passed authorization before a synchronous
    /// recipient revocation to finish. New writes observe the revoked set.
    pub async fn wait_for_inflight_deliveries(&self) {
        let _permit = self.delivery_gate.clone().write_owned().await;
    }
}

/// Trait for broadcasting WebSocket events to all connected clients.
///
/// Business modules depend on this trait (via `Arc<dyn EventBroadcaster>`)
/// to push events without coupling to WebSocket internals.
///
/// Note: `send_to` (unicast) is intentionally NOT part of this trait.
/// Unicast is a connection-management concern handled by `WebSocketManager`.
pub trait EventBroadcaster: Send + Sync {
    /// Broadcast an event to all connected WebSocket clients.
    fn broadcast(&self, event: WebSocketMessage<serde_json::Value>);

    /// Replaces current recipients for an application-scoped event stream.
    /// Implementations that do not route scoped events may keep the default.
    fn replace_scope_recipients(&self, _scope_id: &str, _user_ids: Vec<String>) {}

    /// Synchronously removes one user from an application-scoped event stream.
    fn revoke_scope_recipient(&self, _scope_id: &str, _user_id: &str) {}

    /// Waits for already-authorized scoped socket writes to finish after
    /// recipient revocation. Implementations without scoped delivery can no-op.
    fn wait_for_scope_deliveries(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async {})
    }

    /// Returns the current recipient snapshot for event construction. Final
    /// delivery still revalidates against the shared registry in the manager.
    fn scope_recipients(&self, _scope_id: &str) -> Vec<String> {
        Vec::new()
    }
}

/// Default implementation of [`EventBroadcaster`] backed by
/// `tokio::sync::broadcast` channel.
///
/// The broadcast channel is used for module-to-WebSocket event fan-out.
/// Each `WebSocketManager` connection subscribes to this channel and
/// forwards received events to its per-connection `mpsc` sender.
pub struct BroadcastEventBus {
    tx: broadcast::Sender<WebSocketMessage<serde_json::Value>>,
    scoped_recipients: ScopedEventRecipients,
}

impl BroadcastEventBus {
    /// Create a new event bus with the given channel capacity.
    pub fn new(capacity: usize) -> Self {
        let (tx, _rx) = broadcast::channel(capacity);
        Self {
            tx,
            scoped_recipients: ScopedEventRecipients::default(),
        }
    }

    /// Subscribe to receive broadcast events.
    ///
    /// Each WebSocket connection calls this once to get its own receiver.
    pub fn subscribe(&self) -> broadcast::Receiver<WebSocketMessage<serde_json::Value>> {
        self.tx.subscribe()
    }

    /// Returns the number of active subscribers.
    pub fn receiver_count(&self) -> usize {
        self.tx.receiver_count()
    }

    pub fn scoped_recipients(&self) -> ScopedEventRecipients {
        self.scoped_recipients.clone()
    }
}

impl EventBroadcaster for BroadcastEventBus {
    fn broadcast(&self, event: WebSocketMessage<serde_json::Value>) {
        if let Err(e) = self.tx.send(event) {
            warn!(
                event_name = %e.0.name,
                "broadcast failed: no active receivers"
            );
        }
    }

    fn replace_scope_recipients(&self, scope_id: &str, user_ids: Vec<String>) {
        self.scoped_recipients.replace(scope_id, user_ids);
    }

    fn revoke_scope_recipient(&self, scope_id: &str, user_id: &str) {
        self.scoped_recipients.revoke(scope_id, user_id);
    }

    fn wait_for_scope_deliveries(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(self.scoped_recipients.wait_for_inflight_deliveries())
    }

    fn scope_recipients(&self, scope_id: &str) -> Vec<String> {
        self.scoped_recipients.snapshot(scope_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn new_bus_has_zero_receivers() {
        let bus = BroadcastEventBus::new(16);
        assert_eq!(bus.receiver_count(), 0);
    }

    #[test]
    fn subscribe_increments_receiver_count() {
        let bus = BroadcastEventBus::new(16);
        let _rx1 = bus.subscribe();
        assert_eq!(bus.receiver_count(), 1);
        let _rx2 = bus.subscribe();
        assert_eq!(bus.receiver_count(), 2);
    }

    #[test]
    fn drop_receiver_decrements_count() {
        let bus = BroadcastEventBus::new(16);
        let rx = bus.subscribe();
        assert_eq!(bus.receiver_count(), 1);
        drop(rx);
        assert_eq!(bus.receiver_count(), 0);
    }

    #[test]
    fn broadcast_without_receivers_does_not_panic() {
        let bus = BroadcastEventBus::new(16);
        let event = WebSocketMessage::new("test", json!({}));
        bus.broadcast(event);
    }

    #[tokio::test]
    async fn broadcast_delivers_to_subscriber() {
        let bus = BroadcastEventBus::new(16);
        let mut rx = bus.subscribe();

        let event = WebSocketMessage::new("chat:update", json!({"id": 1}));
        bus.broadcast(event);

        let received = rx.recv().await.unwrap();
        assert_eq!(received.name, "chat:update");
        assert_eq!(received.data["id"], 1);
    }

    #[tokio::test]
    async fn revocation_fence_waits_for_authorized_socket_write_permit() {
        let recipients = ScopedEventRecipients::default();
        recipients.replace("team-1", ["owner".into(), "collaborator".into()]);
        let delivery_permit = recipients.authorize_delivery("team-1", "owner", "collaborator").await.unwrap();
        recipients.revoke("team-1", "collaborator");

        let waiter_registry = recipients.clone();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let mut waiter = tokio::spawn(async move {
            let _ = started_tx.send(());
            waiter_registry.wait_for_inflight_deliveries().await;
        });
        started_rx.await.unwrap();
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), &mut waiter)
                .await
                .is_err(),
            "revocation must fence an in-flight socket write"
        );

        drop(delivery_permit);
        waiter.await.unwrap();
        assert!(recipients.authorize_delivery("team-1", "owner", "collaborator").await.is_none());
    }

    #[tokio::test]
    async fn broadcast_delivers_to_all_subscribers() {
        let bus = BroadcastEventBus::new(16);
        let mut rx1 = bus.subscribe();
        let mut rx2 = bus.subscribe();

        let event = WebSocketMessage::new("ping", json!({"ts": 100}));
        bus.broadcast(event);

        let msg1 = rx1.recv().await.unwrap();
        let msg2 = rx2.recv().await.unwrap();
        assert_eq!(msg1.name, "ping");
        assert_eq!(msg2.name, "ping");
        assert_eq!(msg1.data, msg2.data);
    }

    #[tokio::test]
    async fn multiple_broadcasts_in_order() {
        let bus = BroadcastEventBus::new(16);
        let mut rx = bus.subscribe();

        for i in 0..5 {
            let event = WebSocketMessage::new(format!("event-{i}"), json!({"seq": i}));
            bus.broadcast(event);
        }

        for i in 0..5 {
            let msg = rx.recv().await.unwrap();
            assert_eq!(msg.name, format!("event-{i}"));
            assert_eq!(msg.data["seq"], i);
        }
    }

    #[test]
    fn trait_object_compatible() {
        let bus = BroadcastEventBus::new(16);
        let broadcaster: &dyn EventBroadcaster = &bus;
        let event = WebSocketMessage::new("test", json!(null));
        broadcaster.broadcast(event);
    }
}
