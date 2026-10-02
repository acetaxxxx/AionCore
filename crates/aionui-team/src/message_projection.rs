use std::sync::Arc;

use aionui_api_types::WebSocketMessage;
use aionui_db::models::MessageRow;
use aionui_realtime::EventBroadcaster;
use async_trait::async_trait;
use tracing::info;

use crate::error::TeamError;
use crate::events::TEAMMATE_MESSAGE_EVENT;
use crate::visibility::{TeamVisibilityPolicy, strip_system_notes};

const TEXT_MESSAGE_TYPE: &str = "text";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TeamProjectionSource {
    User,
    TeamSystem,
    Teammate {
        from_slot_id: String,
        from_name: String,
        sender_backend: Option<String>,
        sender_conversation_id: Option<String>,
    },
}

#[derive(Debug, Clone)]
pub struct TeamProjectionRequest {
    pub user_id: String,
    pub actor_user_id: String,
    pub authorized_user_ids: Vec<String>,
    pub team_id: String,
    pub slot_id: String,
    pub conversation_id: String,
    pub source: TeamProjectionSource,
    pub content: String,
    pub files: Vec<String>,
    pub visibility: TeamVisibilityPolicy,
    pub dedupe_key: Option<String>,
}

impl TeamProjectionRequest {
    pub fn user_visible(
        user_id: impl Into<String>,
        team_id: impl Into<String>,
        slot_id: impl Into<String>,
        conversation_id: impl Into<String>,
        content: impl Into<String>,
        files: Vec<String>,
    ) -> Self {
        Self {
            user_id: user_id.into(),
            actor_user_id: String::new(),
            authorized_user_ids: Vec::new(),
            team_id: team_id.into(),
            slot_id: slot_id.into(),
            conversation_id: conversation_id.into(),
            source: TeamProjectionSource::User,
            content: content.into(),
            files,
            visibility: TeamVisibilityPolicy::user_message(),
            dedupe_key: None,
        }
        .with_owner_actor()
    }

    #[allow(clippy::too_many_arguments)]
    pub fn teammate_visible(
        user_id: impl Into<String>,
        team_id: impl Into<String>,
        slot_id: impl Into<String>,
        conversation_id: impl Into<String>,
        from_slot_id: impl Into<String>,
        from_name: impl Into<String>,
        content: impl Into<String>,
        mailbox_message_id: impl Into<String>,
    ) -> Self {
        let team_id = team_id.into();
        let conversation_id = conversation_id.into();
        let mailbox_message_id = mailbox_message_id.into();
        Self {
            user_id: user_id.into(),
            actor_user_id: String::new(),
            authorized_user_ids: Vec::new(),
            dedupe_key: Some(teammate_dedupe_key(&team_id, &mailbox_message_id, &conversation_id)),
            team_id,
            slot_id: slot_id.into(),
            conversation_id,
            source: TeamProjectionSource::Teammate {
                from_slot_id: from_slot_id.into(),
                from_name: from_name.into(),
                sender_backend: None,
                sender_conversation_id: None,
            },
            content: content.into(),
            files: Vec::new(),
            visibility: TeamVisibilityPolicy::teammate_message(),
        }
        .with_owner_actor()
    }

    pub fn team_system_visible(
        user_id: impl Into<String>,
        team_id: impl Into<String>,
        slot_id: impl Into<String>,
        conversation_id: impl Into<String>,
        content: impl Into<String>,
        mailbox_message_id: impl Into<String>,
    ) -> Self {
        let team_id = team_id.into();
        let conversation_id = conversation_id.into();
        let mailbox_message_id = mailbox_message_id.into();
        Self {
            user_id: user_id.into(),
            actor_user_id: String::new(),
            authorized_user_ids: Vec::new(),
            dedupe_key: Some(teammate_dedupe_key(&team_id, &mailbox_message_id, &conversation_id)),
            team_id,
            slot_id: slot_id.into(),
            conversation_id,
            source: TeamProjectionSource::TeamSystem,
            content: content.into(),
            files: Vec::new(),
            visibility: TeamVisibilityPolicy::teammate_message(),
        }
        .with_owner_actor()
    }

    fn with_owner_actor(mut self) -> Self {
        self.actor_user_id = self.user_id.clone();
        self.authorized_user_ids = vec![self.user_id.clone()];
        self
    }

    pub fn with_actor_user_id(mut self, actor_user_id: impl Into<String>) -> Self {
        self.actor_user_id = actor_user_id.into();
        self
    }

    pub fn with_authorized_user_ids(mut self, user_ids: Vec<String>) -> Self {
        self.authorized_user_ids = if user_ids.is_empty() {
            vec![self.user_id.clone()]
        } else {
            user_ids
        };
        self
    }

    fn should_insert_visible_bubble(&self) -> bool {
        match self.source {
            TeamProjectionSource::User => self.visibility.insert_user_visible_bubble,
            TeamProjectionSource::TeamSystem => self.visibility.insert_teammate_visible_bubble,
            TeamProjectionSource::Teammate { .. } => self.visibility.insert_teammate_visible_bubble,
        }
    }
}

pub fn teammate_dedupe_key(team_id: &str, mailbox_message_id: &str, conversation_id: &str) -> String {
    format!("team:{team_id}:mailbox:{mailbox_message_id}:conversation:{conversation_id}")
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProjectedTeamMessage {
    Inserted { msg_id: String },
    AlreadyProjected { msg_id: String },
    Skipped,
}

#[async_trait]
pub trait TeamProjectionMessageStore: Send + Sync {
    fn mint_message_id(&self) -> String;

    async fn find_projected_message(
        &self,
        conversation_id: &str,
        msg_id: &str,
        msg_type: &str,
    ) -> Result<Option<MessageRow>, TeamError>;

    async fn insert_projected_message(&self, row: &MessageRow) -> Result<(), TeamError>;
}

pub struct TeamMessageProjection<S: ?Sized> {
    store: Arc<S>,
    broadcaster: Arc<dyn EventBroadcaster>,
}

impl<S> TeamMessageProjection<S>
where
    S: TeamProjectionMessageStore + ?Sized,
{
    pub fn new(store: Arc<S>, broadcaster: Arc<dyn EventBroadcaster>) -> Self {
        Self { store, broadcaster }
    }

    pub async fn project(&self, request: TeamProjectionRequest) -> Result<ProjectedTeamMessage, TeamError> {
        if !request.should_insert_visible_bubble() {
            info!(
                team_id = %request.team_id,
                slot_id = %request.slot_id,
                conversation_id = %request.conversation_id,
                event_name = "",
                outcome = "skipped",
                "Team message projection skipped by visibility policy"
            );
            return Ok(ProjectedTeamMessage::Skipped);
        }

        let msg_id = request
            .dedupe_key
            .clone()
            .unwrap_or_else(|| self.store.mint_message_id());

        if request.dedupe_key.is_some()
            && let Some(existing) = self
                .store
                .find_projected_message(&request.conversation_id, &msg_id, TEXT_MESSAGE_TYPE)
                .await?
        {
            let existing_msg_id = existing.msg_id.unwrap_or(existing.id);
            info!(
                team_id = %request.team_id,
                slot_id = %request.slot_id,
                conversation_id = %request.conversation_id,
                event_name = TEAMMATE_MESSAGE_EVENT,
                outcome = "already_projected",
                "Team message projection deduped"
            );
            return Ok(ProjectedTeamMessage::AlreadyProjected {
                msg_id: existing_msg_id,
            });
        }

        let row = Self::build_message_row(&request, &msg_id, aionui_common::now_ms())?;
        self.store.insert_projected_message(&row).await?;

        let teammate_event_payload = match &request.source {
            TeamProjectionSource::Teammate {
                from_slot_id,
                from_name,
                sender_backend,
                sender_conversation_id,
            } => Some(serde_json::json!({
                "user_id": request.user_id,
                "authorized_user_ids": request.authorized_user_ids.clone(),
                "team_id": request.team_id,
                "slot_id": request.slot_id,
                "conversation_id": request.conversation_id,
                "msg_id": msg_id,
                "content": request.content,
                "from_slot_id": from_slot_id,
                "from_name": from_name,
                "teammate_message": true,
                "sender_backend": sender_backend,
                "sender_conversation_id": sender_conversation_id,
            })),
            TeamProjectionSource::TeamSystem => Some(serde_json::json!({
                "user_id": request.user_id,
                "authorized_user_ids": request.authorized_user_ids.clone(),
                "team_id": request.team_id,
                "slot_id": request.slot_id,
                "conversation_id": request.conversation_id,
                "msg_id": msg_id,
                "content": request.content,
                "from_slot_id": "team_system",
                "from_name": "team_system",
                "teammate_message": true,
                "sender_backend": null,
                "sender_conversation_id": null,
            })),
            TeamProjectionSource::User => None,
        };
        if let Some(payload) = teammate_event_payload {
            self.broadcaster
                .broadcast(WebSocketMessage::new(TEAMMATE_MESSAGE_EVENT, payload));
        }

        info!(
            team_id = %request.team_id,
            slot_id = %request.slot_id,
            conversation_id = %request.conversation_id,
            event_name = match request.source {
                TeamProjectionSource::User => "message.stream",
                TeamProjectionSource::TeamSystem => TEAMMATE_MESSAGE_EVENT,
                TeamProjectionSource::Teammate { .. } => TEAMMATE_MESSAGE_EVENT,
            },
            outcome = "inserted",
            "Team message projected"
        );

        Ok(ProjectedTeamMessage::Inserted { msg_id })
    }

    pub fn build_message_row(
        request: &TeamProjectionRequest,
        msg_id: &str,
        created_at: aionui_common::TimestampMs,
    ) -> Result<MessageRow, TeamError> {
        let (position, content) = match &request.source {
            TeamProjectionSource::User => {
                let content = if request.visibility.strip_system_notes {
                    strip_system_notes(&request.content)
                } else {
                    request.content.clone()
                };
                (
                    "right",
                    serde_json::json!({
                        "content": content,
                        "actor_user_id": request.actor_user_id.clone(),
                    }),
                )
            }
            TeamProjectionSource::TeamSystem => (
                "left",
                serde_json::json!({
                    "content": request.content,
                    "teammate_message": true,
                    "sender_name": "team_system",
                    "sender_backend": null,
                    "sender_conversation_id": null,
                }),
            ),
            TeamProjectionSource::Teammate {
                from_slot_id: _,
                from_name,
                sender_backend,
                sender_conversation_id,
            } => (
                "left",
                serde_json::json!({
                    "content": request.content,
                    "teammate_message": true,
                    "sender_name": from_name,
                    "sender_backend": sender_backend,
                    "sender_conversation_id": sender_conversation_id,
                }),
            ),
        };

        Ok(MessageRow {
            id: msg_id.to_owned(),
            conversation_id: request.conversation_id.clone(),
            msg_id: Some(msg_id.to_owned()),
            r#type: TEXT_MESSAGE_TYPE.into(),
            content: serde_json::to_string(&content)?,
            position: Some(position.into()),
            status: Some("finish".into()),
            hidden: request.visibility.allow_hidden_conversation_message,
            created_at,
            backend_turn_id: None,
        })
    }
}

#[cfg(test)]
mod actor_tests {
    use super::*;
    use aionui_realtime::{BroadcastEventBus, EventBroadcaster, WebSocketManager, WsOutbound};
    use async_trait::async_trait;
    use tokio::sync::Barrier;

    struct PausedProjectionStore {
        entered_lookup: Barrier,
        resume_lookup: Barrier,
    }

    #[async_trait]
    impl TeamProjectionMessageStore for PausedProjectionStore {
        fn mint_message_id(&self) -> String {
            "message-1".to_owned()
        }

        async fn find_projected_message(
            &self,
            _conversation_id: &str,
            _msg_id: &str,
            _msg_type: &str,
        ) -> Result<Option<MessageRow>, TeamError> {
            self.entered_lookup.wait().await;
            self.resume_lookup.wait().await;
            Ok(None)
        }

        async fn insert_projected_message(&self, _row: &MessageRow) -> Result<(), TeamError> {
            Ok(())
        }
    }

    #[test]
    fn user_message_keeps_authenticated_actor_separate_from_execution_owner() {
        let request = TeamProjectionRequest::user_visible(
            "execution-owner",
            "team-1",
            "lead",
            "conversation-1",
            "hello",
            Vec::new(),
        )
        .with_actor_user_id("collaborator");

        assert_eq!(request.user_id, "execution-owner");
        assert_eq!(request.actor_user_id, "collaborator");
        let row = TeamMessageProjection::<dyn TeamProjectionMessageStore>::build_message_row(&request, "msg-1", 1)
            .expect("message row");
        let content: serde_json::Value = serde_json::from_str(&row.content).expect("JSON content");
        assert_eq!(content["actor_user_id"], "collaborator");
        assert_eq!(content["content"], "hello");
    }

    #[tokio::test]
    async fn queued_projection_snapshot_cannot_reach_member_revoked_during_lookup() {
        let bus = Arc::new(BroadcastEventBus::new(8));
        let mut event_rx = bus.subscribe();
        bus.replace_scope_recipients("team-1", vec!["owner".into(), "collaborator".into()]);

        let manager = WebSocketManager::new();
        manager.set_scoped_event_recipients(bus.scoped_recipients());
        let (owner_tx, mut owner_rx) = tokio::sync::mpsc::channel(4);
        let (collaborator_tx, mut collaborator_rx) = tokio::sync::mpsc::channel(4);
        manager.add_client_for_user("owner".into(), "owner-token".into(), owner_tx);
        manager.add_client_for_user("collaborator".into(), "collaborator-token".into(), collaborator_tx);

        let store = Arc::new(PausedProjectionStore {
            entered_lookup: Barrier::new(2),
            resume_lookup: Barrier::new(2),
        });
        let projection = TeamMessageProjection::new(store.clone(), bus.clone());
        let request = TeamProjectionRequest::team_system_visible(
            "owner",
            "team-1",
            "agent-1",
            "conversation-1",
            "terminal notice",
            "mailbox-1",
        )
        .with_authorized_user_ids(vec!["owner".into(), "collaborator".into()]);

        let projection_task = tokio::spawn(async move { projection.project(request).await });
        // Pause after the request captured its old recipients and while the
        // projection is awaiting the message-store dedupe lookup.
        store.entered_lookup.wait().await;
        bus.revoke_scope_recipient("team-1", "collaborator");
        store.resume_lookup.wait().await;
        projection_task.await.expect("projection task").expect("projection");

        let queued_event = event_rx.recv().await.expect("projected event");
        manager.broadcast_scoped(queued_event);

        assert!(matches!(owner_rx.try_recv(), Ok(WsOutbound::ScopedText { .. })));
        assert!(collaborator_rx.try_recv().is_err());
    }
}
