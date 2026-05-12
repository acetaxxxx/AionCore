use std::sync::Arc;
use std::time::Duration;

use aionui_ai_agent::IWorkerTaskManager;
use aionui_ai_agent::protocol::events::AgentStreamEvent;
use aionui_ai_agent::types::SendMessageData;
use aionui_common::ConversationStatus;
use aionui_conversation::ConversationService;
use aionui_realtime::EventBroadcaster;
use dashmap::DashMap;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tracing::{info, warn};

use crate::mailbox::Mailbox;
use crate::scheduler::TeammateManager;
use crate::session::TeamSession;
use crate::types::TeammateStatus;

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);
const FINISH_TIMEOUT: Duration = Duration::from_secs(300);

/// Registry of per-agent Notify handles. Used by any trigger source to poke
/// an agent's event loop without needing to know its internals.
pub struct EventLoopRegistry {
    notifiers: DashMap<String, Arc<Notify>>,
    handles: DashMap<String, JoinHandle<()>>,
    shutdown_tx: tokio::sync::watch::Sender<bool>,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
}

impl Default for EventLoopRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl EventLoopRegistry {
    pub fn new() -> Self {
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        Self {
            notifiers: DashMap::new(),
            handles: DashMap::new(),
            shutdown_tx,
            shutdown_rx,
        }
    }

    /// Check if an event loop is registered for this slot.
    pub fn has(&self, slot_id: &str) -> bool {
        self.notifiers.contains_key(slot_id)
    }

    /// Poke the named agent's event loop so it drains its mailbox.
    pub fn notify(&self, slot_id: &str) {
        if let Some(n) = self.notifiers.get(slot_id) {
            n.notify_one();
        }
    }

    /// Register and spawn an event loop for one agent.
    pub fn spawn(&self, slot_id: &str, ctx: AgentLoopContext) {
        let notify = Arc::new(Notify::new());
        self.notifiers.insert(slot_id.to_owned(), notify.clone());
        let handle = tokio::spawn(run_event_loop(notify, self.shutdown_rx.clone(), ctx));
        self.handles.insert(slot_id.to_owned(), handle);
    }

    /// Remove an agent's event loop (agent removed from team).
    pub fn remove(&self, slot_id: &str) {
        self.notifiers.remove(slot_id);
        if let Some((_, handle)) = self.handles.remove(slot_id) {
            handle.abort();
        }
    }

    /// Shut down all event loops.
    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
        for entry in self.handles.iter() {
            entry.value().abort();
        }
        self.handles.clear();
        self.notifiers.clear();
    }
}

/// Context shared across all iterations of one agent's event loop.
pub struct AgentLoopContext {
    pub team_id: String,
    pub slot_id: String,
    pub user_id: String,
    pub session: Arc<TeamSession>,
    pub scheduler: Arc<TeammateManager>,
    pub mailbox: Arc<Mailbox>,
    pub task_manager: Arc<dyn IWorkerTaskManager>,
    pub conversation_service: ConversationService,
    pub broadcaster: Arc<dyn EventBroadcaster>,
    /// Used to notify other agents' event loops (e.g. leader after all-settled).
    pub registry: Arc<EventLoopRegistry>,
}

/// The event loop for one agent slot. Spawned as a tokio task.
///
/// Flow:
/// 1. Wait for signal (notify) or heartbeat timeout.
/// 2. Drain loop: compute_wake_input → if has messages → execute turn → await finish → repeat.
/// 3. When mailbox empty → back to step 1.
async fn run_event_loop(
    notify: Arc<Notify>,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
    ctx: AgentLoopContext,
) {
    info!(
        team_id = %ctx.team_id,
        slot_id = %ctx.slot_id,
        "agent event loop started"
    );

    loop {
        // Step 1: wait for signal, heartbeat, or shutdown
        tokio::select! {
            biased;
            _ = shutdown_rx.wait_for(|v| *v) => {
                info!(
                    team_id = %ctx.team_id,
                    slot_id = %ctx.slot_id,
                    "agent event loop shutting down"
                );
                return;
            }
            _ = notify.notified() => {}
            _ = tokio::time::sleep(HEARTBEAT_INTERVAL) => {}
        }

        // Drain loop: keep processing until mailbox is empty
        loop {
            if *shutdown_rx.borrow() {
                return;
            }

            let input = match ctx.session.compute_wake_input(&ctx.slot_id).await {
                Ok(Some(input)) => input,
                Ok(None) => break,
                Err(e) => {
                    warn!(
                        team_id = %ctx.team_id,
                        slot_id = %ctx.slot_id,
                        error = %e,
                        "event loop: compute_wake_input failed"
                    );
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    break;
                }
            };

            if !input.should_send {
                break;
            }

            let finish_ok = execute_turn(&ctx, &input).await;
            finalize_turn(&ctx, finish_ok).await;
        }
    }
}

/// Execute one agent turn: warmup → guard → set Working → StreamRelay → send → await finish.
async fn execute_turn(ctx: &AgentLoopContext, input: &crate::session::WakeInput) -> bool {
    ctx.session.mirror_unread_to_conversation(input).await;

    // Ensure agent task exists
    let handle = match ctx.task_manager.get_task(&input.conversation_id) {
        Some(h) => h,
        None => {
            if let Err(e) = ctx
                .conversation_service
                .warmup(&ctx.user_id, &input.conversation_id, &ctx.task_manager)
                .await
            {
                warn!(
                    team_id = %ctx.team_id,
                    slot_id = %ctx.slot_id,
                    conversation_id = %input.conversation_id,
                    error = %e,
                    "event loop: warmup failed"
                );
                return false;
            }
            match ctx.task_manager.get_task(&input.conversation_id) {
                Some(h) => h,
                None => {
                    warn!(
                        team_id = %ctx.team_id,
                        slot_id = %ctx.slot_id,
                        conversation_id = %input.conversation_id,
                        "event loop: no task after warmup"
                    );
                    return false;
                }
            }
        }
    };

    // Guard: skip if already running
    if handle.status() == Some(ConversationStatus::Running) {
        return false;
    }
    let repo = ctx.conversation_service.conversation_repo();
    if let Ok(Some(row)) = repo.get(&input.conversation_id).await
        && row.status.as_deref() == Some("running")
    {
        return false;
    }

    // Point-of-no-return: set Working + claim DB running
    let _ = ctx.scheduler.set_status(&ctx.slot_id, TeammateStatus::Working).await;
    let update = aionui_db::ConversationRowUpdate {
        status: Some("running".to_owned()),
        updated_at: Some(aionui_common::now_ms()),
        ..Default::default()
    };
    let _ = repo.update(&input.conversation_id, &update).await;

    // StreamRelay for response persistence + WebSocket forwarding
    let msg_id = ConversationService::mint_msg_id();
    let rx = handle.subscribe();
    let relay = aionui_conversation::stream_relay::StreamRelay::new(
        input.conversation_id.clone(),
        msg_id.clone(),
        ctx.user_id.clone(),
        Arc::clone(repo),
        ctx.broadcaster.clone(),
        None,
    );
    tokio::spawn(async move { relay.consume(rx).await });

    // Send message to agent
    let data = SendMessageData {
        content: input.first_message.clone(),
        msg_id,
        files: Vec::new(),
        inject_skills: Vec::new(),
    };

    if let Err(e) = handle.send_message(data).await {
        warn!(
            team_id = %ctx.team_id,
            slot_id = %ctx.slot_id,
            conversation_id = %input.conversation_id,
            error = %e,
            "event loop: send_message failed"
        );
        let _ = ctx.scheduler.set_status(&ctx.slot_id, TeammateStatus::Idle).await;
        let update = aionui_db::ConversationRowUpdate {
            status: Some("finished".to_owned()),
            updated_at: Some(aionui_common::now_ms()),
            ..Default::default()
        };
        let _ = repo.update(&input.conversation_id, &update).await;
        return false;
    }

    // Mark messages as read
    let msg_ids: Vec<String> = input.unread.iter().map(|m| m.id.clone()).collect();
    if !msg_ids.is_empty()
        && let Err(e) = ctx.mailbox.mark_read_batch(&msg_ids).await
    {
        warn!(
            team_id = %ctx.team_id,
            slot_id = %ctx.slot_id,
            error = %e,
            "event loop: mark_read_batch failed (non-fatal)"
        );
    }

    // Await Finish/Error from agent
    await_agent_finish(&handle.subscribe(), &ctx.team_id, &ctx.slot_id).await
}

/// Wait for the agent's turn to complete by listening for Finish/Error events.
async fn await_agent_finish(
    rx: &tokio::sync::broadcast::Receiver<AgentStreamEvent>,
    team_id: &str,
    slot_id: &str,
) -> bool {
    let mut rx = rx.resubscribe();

    tokio::select! {
        result = async {
            loop {
                match rx.recv().await {
                    Ok(AgentStreamEvent::Finish(_)) => return true,
                    Ok(AgentStreamEvent::Error(_)) => return false,
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => return false,
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        warn!(team_id, slot_id, skipped = n, "event loop: broadcast lagged");
                        continue;
                    }
                    Ok(_) => continue,
                }
            }
        } => result,
        _ = tokio::time::sleep(FINISH_TIMEOUT) => {
            warn!(team_id, slot_id, "event loop: await_finish timed out");
            false
        }
    }
}

/// Finalize a completed turn: mark idle and cascade to leader if needed.
async fn finalize_turn(ctx: &AgentLoopContext, _finish_ok: bool) {
    match ctx.scheduler.finalize_turn(&ctx.slot_id, &[]).await {
        Ok(Some(wake_target)) => {
            if wake_target != ctx.slot_id {
                ctx.registry.notify(&wake_target);
            }
        }
        Ok(None) => {}
        Err(e) => {
            warn!(
                team_id = %ctx.team_id,
                slot_id = %ctx.slot_id,
                error = %e,
                "event loop: finalize_turn failed"
            );
        }
    }
}
