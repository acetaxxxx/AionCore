use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use aionui_ai_agent::{
    AgentError, AgentInstance, AgentSendError, AgentStreamEvent, IWorkerTaskManager,
    agent_task::{IAgentTask, IMockAgent},
    protocol::events::{FinishEventData, TextEventData},
    types::{BuildTaskOptions, SendMessageData},
};
use aionui_api_types::SendMessageRequest;
use aionui_common::{AgentKillReason, AgentType, Confirmation, ConversationStatus, TimestampMs, now_ms};
use aionui_conversation::{
    ConversationAgentTurnRequest, ConversationService, InMemoryMemoryCuration, InMemoryTurnJournal, MemoryCuration,
    MemoryCurationError, MemoryEvidence, MemoryEvidenceSource, RawJournalEvent,
    skill_resolver::{ResolvedAgentSkill, SkillResolver},
};
use aionui_db::{
    IConversationRepository, SqliteAcpSessionRepository, SqliteAgentMetadataRepository, SqliteConversationRepository,
    init_database_memory, models::ConversationRow,
};
use aionui_realtime::BroadcastEventBus;
use async_trait::async_trait;
use tokio::sync::broadcast;

struct EmptySkillResolver;

#[async_trait]
impl SkillResolver for EmptySkillResolver {
    async fn auto_inject_names(&self) -> Vec<String> {
        Vec::new()
    }

    async fn resolve_skills(&self, _names: &[String]) -> Vec<ResolvedAgentSkill> {
        Vec::new()
    }

    async fn link_workspace_skills(
        &self,
        _workspace: &std::path::Path,
        _rel_dirs: &[&str],
        _skills: &[ResolvedAgentSkill],
    ) -> usize {
        0
    }
}

#[derive(Clone)]
struct RecordingCuration {
    inner: InMemoryMemoryCuration,
    evidence: Arc<Mutex<Vec<MemoryEvidence>>>,
}

impl RecordingCuration {
    fn new() -> Self {
        Self {
            inner: InMemoryMemoryCuration::new(),
            evidence: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

#[async_trait]
impl MemoryCuration for RecordingCuration {
    async fn capture_candidate(&self, evidence: &MemoryEvidence) -> Result<(), MemoryCurationError> {
        self.evidence.lock().unwrap().push(evidence.clone());
        self.inner.capture_candidate(evidence).await
    }
}

struct ScriptedAgent {
    conversation_id: String,
    event_tx: broadcast::Sender<AgentStreamEvent>,
    scripts: Mutex<VecDeque<Vec<AgentStreamEvent>>>,
}

impl ScriptedAgent {
    fn new(conversation_id: &str, scripts: Vec<Vec<AgentStreamEvent>>) -> Self {
        let (event_tx, _) = broadcast::channel(64);
        Self {
            conversation_id: conversation_id.to_owned(),
            event_tx,
            scripts: Mutex::new(scripts.into()),
        }
    }
}

#[async_trait]
impl IAgentTask for ScriptedAgent {
    fn agent_type(&self) -> AgentType {
        AgentType::Acp
    }

    fn conversation_id(&self) -> &str {
        &self.conversation_id
    }

    fn workspace(&self) -> &str {
        "/tmp/test"
    }

    fn status(&self) -> Option<ConversationStatus> {
        Some(ConversationStatus::Finished)
    }

    fn last_activity_at(&self) -> TimestampMs {
        now_ms()
    }

    fn subscribe(&self) -> broadcast::Receiver<AgentStreamEvent> {
        self.event_tx.subscribe()
    }

    async fn send_message(&self, _data: SendMessageData) -> Result<(), AgentSendError> {
        let script = self.scripts.lock().unwrap().pop_front().unwrap_or_default();
        for event in script {
            let _ = self.event_tx.send(event);
        }
        Ok(())
    }

    async fn cancel(&self) -> Result<(), AgentError> {
        Ok(())
    }

    fn kill(&self, _reason: Option<AgentKillReason>) -> Result<(), AgentError> {
        Ok(())
    }
}

impl IMockAgent for ScriptedAgent {
    fn get_confirmations(&self) -> Vec<Confirmation> {
        Vec::new()
    }
}

struct ScriptedTaskManager {
    agents: Mutex<VecDeque<AgentInstance>>,
}

impl ScriptedTaskManager {
    fn new(agents: Vec<AgentInstance>) -> Self {
        Self {
            agents: Mutex::new(agents.into()),
        }
    }
}

#[async_trait]
impl IWorkerTaskManager for ScriptedTaskManager {
    fn get_task(&self, _conversation_id: &str) -> Option<AgentInstance> {
        None
    }

    async fn get_or_build_task(
        &self,
        _conversation_id: &str,
        _options: BuildTaskOptions,
    ) -> Result<AgentInstance, AgentError> {
        self.agents
            .lock()
            .unwrap()
            .pop_front()
            .ok_or_else(|| AgentError::bad_gateway("no scripted agent left"))
    }

    fn kill(&self, _conversation_id: &str, _reason: Option<AgentKillReason>) -> Result<(), AgentError> {
        Ok(())
    }

    fn kill_and_wait(
        &self,
        _conversation_id: &str,
        _reason: Option<AgentKillReason>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
        Box::pin(std::future::ready(()))
    }

    async fn clear(&self) {}

    fn active_count(&self) -> usize {
        0
    }

    fn collect_idle(&self, _idle_threshold_ms: TimestampMs) -> Vec<String> {
        Vec::new()
    }
}

async fn setup_service(
    curation: Arc<RecordingCuration>,
    agents: Vec<AgentInstance>,
) -> (
    ConversationService,
    Arc<SqliteConversationRepository>,
    Arc<dyn IWorkerTaskManager>,
    Arc<InMemoryTurnJournal>,
) {
    let db = init_database_memory().await.unwrap();
    let repo = Arc::new(SqliteConversationRepository::new(db.pool().clone()));
    let now = now_ms();
    for (id, user_id) in [
        ("owner-conv", "system_default_user"),
        ("background-conv", "system_default_user"),
    ] {
        repo.create(&ConversationRow {
            id: id.into(),
            user_id: user_id.into(),
            name: id.into(),
            r#type: "acp".into(),
            extra: r#"{"workspace":"/tmp"}"#.into(),
            model: None,
            status: Some("running".into()),
            source: Some("aionui".into()),
            channel_chat_id: None,
            pinned: false,
            pinned_at: None,
            created_at: now,
            updated_at: now,
            project_id: None,
            folder_id: None,
            name_source: None,
        })
        .await
        .unwrap();
    }
    let task_manager: Arc<dyn IWorkerTaskManager> = Arc::new(ScriptedTaskManager::new(agents));
    let journal = Arc::new(InMemoryTurnJournal::new());
    let service = ConversationService::new(
        std::env::temp_dir(),
        Arc::new(BroadcastEventBus::new(64)),
        Arc::new(EmptySkillResolver),
        task_manager.clone(),
        repo.clone(),
        Arc::new(SqliteAgentMetadataRepository::new(db.pool().clone())),
        Arc::new(SqliteAcpSessionRepository::new(db.pool().clone())),
    )
    .with_turn_journal(journal.clone())
    .with_memory_curation(curation);
    (service, repo, task_manager, journal)
}

async fn wait_for_events(
    journal: &InMemoryTurnJournal,
    user_id: &str,
    conversation_id: &str,
    turn_id: &str,
) -> Vec<RawJournalEvent> {
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let events = journal.get_turn_events(user_id, conversation_id, turn_id).await;
            if events.len() >= 2 {
                return events;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("terminal journal event should be recorded")
}

#[tokio::test]
async fn public_owner_send_records_final_assistant_text_in_exactly_one_terminal_evidence() {
    let curation = Arc::new(RecordingCuration::new());
    let agent = Arc::new(ScriptedAgent::new(
        "owner-conv",
        vec![vec![
            AgentStreamEvent::Text(TextEventData {
                content: "owner final reply".into(),
            }),
            AgentStreamEvent::Finish(FinishEventData::default()),
        ]],
    ));
    let (service, _repo, task_manager, journal) =
        setup_service(curation.clone(), vec![AgentInstance::Mock(agent)]).await;
    let response = service
        .send_message(
            "system_default_user",
            "owner-conv",
            SendMessageRequest {
                content: "remember owner preference".into(),
                files: Vec::new(),
                sessions: Vec::new(),
                inject_skills: Vec::new(),
                hidden: false,
            },
            &task_manager,
        )
        .await
        .unwrap();
    let events = wait_for_events(&journal, "system_default_user", "owner-conv", &response.turn_id).await;
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, RawJournalEvent::FinalOutcome { .. }))
            .count(),
        1
    );
    let terminal = events
        .iter()
        .find_map(|event| match event {
            RawJournalEvent::FinalOutcome { assistant_message, .. } => Some(assistant_message.as_deref()),
            _ => None,
        })
        .flatten();
    assert_eq!(terminal, Some("owner final reply"));

    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            if !curation.evidence.lock().unwrap().is_empty() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let evidence = curation.evidence.lock().unwrap();
    assert_eq!(evidence.len(), 1);
    assert_eq!(evidence[0].source, MemoryEvidenceSource::Owner);
    assert_eq!(evidence[0].assistant_message.as_deref(), Some("owner final reply"));
}

#[tokio::test]
async fn public_background_run_agent_turn_remains_raw_only_without_memory_candidate() {
    let curation = Arc::new(RecordingCuration::new());
    let agent = Arc::new(ScriptedAgent::new(
        "background-conv",
        vec![vec![
            AgentStreamEvent::Text(TextEventData {
                content: "background result".into(),
            }),
            AgentStreamEvent::Finish(FinishEventData::default()),
        ]],
    ));
    let (service, _repo, _task_manager, _journal) =
        setup_service(curation.clone(), vec![AgentInstance::Mock(agent)]).await;
    let outcome = service
        .run_agent_turn(ConversationAgentTurnRequest {
            user_id: "system_default_user".into(),
            conversation_id: "background-conv".into(),
            content: "background task".into(),
            files: Vec::new(),
            inject_skills: Vec::new(),
            required_runtime_mode: None,
            persist_user_message: false,
            user_message_hidden: true,
            memory_source: ConversationAgentTurnRequest::background_memory_source(),
            on_started: None,
        })
        .await
        .unwrap();
    assert!(matches!(
        outcome.status,
        aionui_conversation::ConversationAgentTurnStatus::Completed
    ));
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            if !curation.evidence.lock().unwrap().is_empty() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert!(
        curation.inner.candidates_for_user("system_default_user").is_empty(),
        "background turns must not create candidates"
    );
    assert_eq!(
        curation.evidence.lock().unwrap()[0].source,
        MemoryEvidenceSource::Background
    );
}
