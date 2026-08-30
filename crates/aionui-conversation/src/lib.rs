#![warn(clippy::disallowed_types)]

//! Conversation and message CRUD with streaming relay and event emission.
mod acp_error_recovery;
mod agent_health_policy;
mod background_stream;
mod convert;
pub mod error;
mod memory_curation;
pub(crate) mod message_cursor;
mod message_persistence;
pub mod response_middleware;
pub mod routes;
pub mod routes_aux;
mod runtime_completion;
mod runtime_persistence;
pub mod runtime_state;
pub mod service;
mod service_ops;
pub(crate) mod session_context;
pub mod session_mentions;
pub mod skill_resolver;
pub mod skill_snapshot;
mod startup_recovery;
pub mod state;
mod stream_persistence;
pub mod stream_relay;
pub mod task_options;
mod turn_continuation_policy;
pub mod turn_journal;
mod turn_orchestrator;
mod turn_recovery_policy;

pub use convert::row_to_response_with_extra;
pub use error::ConversationError;
pub use memory_curation::{
    AgentMemory, FilesystemMemoryCuration, InMemoryMemoryCuration, MemoryCandidate, MemoryCandidateLifecycleEvent,
    MemoryCandidateStatus, MemoryConsolidationReport, MemoryConsolidationScheduler, MemoryCuration,
    MemoryCurationError, MemoryEvidence, MemoryEvidenceSource, MemoryPrivacyPurgeRequest, MemoryPurgeReport,
    MemoryRecord, MemoryRetentionPolicy, MemoryRetentionReport, MemoryRetrievalItem, MemoryRetrievalRequest,
    MemoryRetrievalScope,
};
pub use response_middleware::{MessageMiddleware, MiddlewareResult, strip_think_tags};
pub use routes::conversation_routes;
pub use routes_aux::conversation_ops_routes;
pub use service::is_temp_session_workspace;
pub use service::{
    ConversationAgentTurnOutcome, ConversationAgentTurnRequest, ConversationAgentTurnStarted,
    ConversationAgentTurnStartedCallback, ConversationAgentTurnStatus, ConversationService,
};
pub use state::ConversationRouterState;
pub use turn_journal::{
    AttemptSummary, FilesystemTurnJournal, InMemoryTurnJournal, JournalError, PreTurnRecord, RawJournalEvent,
    StartupRecoveryOptions, TerminalOutcomeRecord, TokenUsageRecord, TurnJournal, TurnTerminalStatus,
    validate_identifier,
};

#[cfg(test)]
#[path = "service_test.rs"]
mod service_test;
