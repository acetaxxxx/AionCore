use std::sync::atomic::{AtomicI64, Ordering};

use aionui_common::{
    AgentKillReason, AgentType, AppError, Confirmation, ConversationStatus, TimestampMs, now_ms,
};
use serde_json::Value;
use tokio::sync::broadcast;
use tracing::info;

use crate::agent_manager::IAgentManager;
use crate::stream_event::AgentStreamEvent;
use crate::types::SendMessageData;

/// Stub Agent Manager for Aionrs (self-developed Rust library).
///
/// Aionrs will be integrated as a Rust crate directly (no CLI subprocess).
/// This stub provides the `IAgentManager` interface and returns
/// "not yet implemented" errors for all operations. The actual
/// implementation will be wired when the `aionrs` crate is available.
pub struct AionrsAgentManager {
    conversation_id: String,
    workspace: String,
    event_tx: broadcast::Sender<AgentStreamEvent>,
    last_activity: AtomicI64,
}

impl AionrsAgentManager {
    /// Create a new Aionrs agent stub.
    pub fn new(conversation_id: String, workspace: String) -> Self {
        let (event_tx, _) = broadcast::channel(16);
        Self {
            conversation_id,
            workspace,
            event_tx,
            last_activity: AtomicI64::new(now_ms()),
        }
    }
}

#[async_trait::async_trait]
impl IAgentManager for AionrsAgentManager {
    fn agent_type(&self) -> AgentType {
        AgentType::Aionrs
    }

    fn status(&self) -> Option<ConversationStatus> {
        None
    }

    fn workspace(&self) -> &str {
        &self.workspace
    }

    fn conversation_id(&self) -> &str {
        &self.conversation_id
    }

    fn last_activity_at(&self) -> TimestampMs {
        self.last_activity.load(Ordering::Relaxed)
    }

    fn subscribe(&self) -> broadcast::Receiver<AgentStreamEvent> {
        self.event_tx.subscribe()
    }

    async fn send_message(&self, _data: SendMessageData) -> Result<(), AppError> {
        Err(AppError::Internal(
            "Aionrs agent is not yet implemented. Awaiting aionrs crate integration.".into(),
        ))
    }

    async fn stop(&self) -> Result<(), AppError> {
        Ok(())
    }

    fn confirm(
        &self,
        _msg_id: &str,
        _call_id: &str,
        _data: Value,
        _always_allow: bool,
    ) -> Result<(), AppError> {
        Err(AppError::Internal(
            "Aionrs agent is not yet implemented".into(),
        ))
    }

    fn get_confirmations(&self) -> Vec<Confirmation> {
        Vec::new()
    }

    fn check_approval(&self, _action: &str, _command_type: Option<&str>) -> bool {
        false
    }

    fn kill(&self, reason: Option<AgentKillReason>) -> Result<(), AppError> {
        info!(
            conversation_id = %self.conversation_id,
            ?reason,
            "Killing Aionrs agent stub (no-op)"
        );
        Ok(())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aionrs_stub_returns_correct_type() {
        let agent = AionrsAgentManager::new("conv-1".into(), "/project".into());
        assert_eq!(agent.agent_type(), AgentType::Aionrs);
        assert_eq!(agent.workspace(), "/project");
        assert_eq!(agent.conversation_id(), "conv-1");
        assert!(agent.status().is_none());
        assert!(agent.get_confirmations().is_empty());
        assert!(!agent.check_approval("any", None));
    }

    #[tokio::test]
    async fn aionrs_send_message_returns_error() {
        let agent = AionrsAgentManager::new("conv-1".into(), "/project".into());
        let data = SendMessageData {
            content: "hello".into(),
            msg_id: "m1".into(),
            files: vec![],
            inject_skills: vec![],
        };
        let result = agent.send_message(data).await;
        assert!(result.is_err());
    }

    #[test]
    fn aionrs_kill_is_noop() {
        let agent = AionrsAgentManager::new("conv-1".into(), "/project".into());
        assert!(agent.kill(None).is_ok());
        assert!(agent.kill(Some(AgentKillReason::IdleTimeout)).is_ok());
    }
}
