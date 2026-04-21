use std::sync::Arc;

use crate::service::ConversationService;
use aionui_ai_agent::IWorkerTaskManager;

/// Shared state for conversation route handlers.
#[derive(Clone)]
pub struct ConversationRouterState {
    pub conversation_service: ConversationService,
    pub worker_task_manager: Arc<dyn IWorkerTaskManager>,
}
