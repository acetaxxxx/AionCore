//! Pure Raw Event Journal implementation for Turn lifecycle orchestration.
//!
//! Provides the external [`TurnJournal`] seam for capturing pre-execution turn events
//! and reconciling final terminal outcomes, while maintaining strict user and conversation
//! isolation, complete-payload idempotency, concurrency safety, durability, and recovery invariants.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;
use tokio::sync::{Mutex, RwLock};

/// Terminal status enum restricted strictly to accepted ADR-0010 domain statuses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TurnTerminalStatus {
    Success,
    Failed,
    Cancelled,
    Timeout,
}

/// Token usage details recorded at turn completion.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct TokenUsageRecord {
    pub prompt_tokens: usize,
    pub completion_tokens: usize,
    pub total_tokens: usize,
}

/// Structured summary for individual attempt executions within a turn.
///
/// Attempt identifiers are canonically derived from logical turn and 1-based attempt ordinal:
/// `{turn_id}-att-{ordinal}` (e.g. `turn_abc-att-1`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttemptSummary {
    pub attempt_id: String,
    pub error: Option<String>,
    pub duration_ms: Option<u64>,
}

/// Input parameter for capturing pre-execution turn events.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreTurnRecord<'a> {
    pub user_id: &'a str,
    pub conversation_id: &'a str,
    pub turn_id: &'a str,
    /// For User Continuation turns, links to the immediately preceding turn's `turn_id`.
    /// Internal system continuations reuse the same `turn_id` and do not populate `parent_turn_id`.
    /// Note: HTTP API ingress for `parent_turn_id` is deferred to the subsequent routing slice.
    pub parent_turn_id: Option<&'a str>,
    pub user_message: &'a str,
    /// Opaque normalized metadata. Never used for filesystem path construction.
    pub workspace: Option<&'a str>,
    pub created_at_ms: u64,
}

/// Input parameter for recording terminal turn outcomes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TerminalOutcomeRecord<'a> {
    pub status: TurnTerminalStatus,
    pub assistant_message: Option<&'a str>,
    pub token_usage: Option<TokenUsageRecord>,
    pub attempts: u32,
    pub last_attempt_id: Option<&'a str>,
    pub retry_summaries: Option<Vec<AttemptSummary>>,
    pub error_metadata: Option<serde_json::Value>,
    pub finished_at_ms: u64,
}

/// The result of an operation that attempted to deliver a message into a
/// running turn.  This is journal metadata rather than a new lifecycle seam:
/// the public [`TurnJournal`] contract remains the two pre/terminal methods.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MidTurnEventKind {
    Accepted,
    Rejected,
    SteerRejected,
    Fallback,
    Duplicate,
}

/// Compact mid-turn event metadata.  The message body is deliberately not
/// copied into the journal; `message_id` and `content_hash` provide a stable
/// reference and conflict check without duplicating user content.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MidTurnRecord {
    pub user_id: String,
    pub conversation_id: String,
    pub turn_id: String,
    pub source_turn_id: Option<String>,
    pub attempt_id: Option<String>,
    pub message_id: Option<String>,
    pub content_hash: String,
    pub idempotency_key: String,
    pub event_kind: MidTurnEventKind,
    pub reason: Option<String>,
    pub created_at_ms: u64,
}

impl MidTurnRecord {
    /// Derive stable identity from the complete event identity and payload.
    pub fn new(
        user_id: &str,
        conversation_id: &str,
        turn_id: &str,
        source_turn_id: Option<&str>,
        attempt_id: Option<&str>,
        message_id: Option<&str>,
        content: &str,
        event_kind: MidTurnEventKind,
        reason: Option<&str>,
        created_at_ms: u64,
    ) -> Self {
        let content_hash = digest_hex(content.as_bytes());
        let event_kind_label = serde_json::to_string(&event_kind).expect("event kind is serializable");
        let mut identity = String::new();
        for part in [
            user_id,
            conversation_id,
            turn_id,
            source_turn_id.unwrap_or_default(),
            attempt_id.unwrap_or_default(),
            message_id.unwrap_or_default(),
            &content_hash,
            event_kind_label.as_str(),
        ] {
            identity.push_str(part);
            identity.push('\0');
        }
        let idempotency_key = digest_hex(identity.as_bytes());

        Self {
            user_id: user_id.to_owned(),
            conversation_id: conversation_id.to_owned(),
            turn_id: turn_id.to_owned(),
            source_turn_id: source_turn_id.map(ToOwned::to_owned),
            attempt_id: attempt_id.map(ToOwned::to_owned),
            message_id: message_id.map(ToOwned::to_owned),
            content_hash,
            idempotency_key,
            event_kind,
            reason: reason.map(ToOwned::to_owned),
            created_at_ms,
        }
    }
}

fn digest_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// Domain errors produced by TurnJournal operations.
#[derive(Debug, thiserror::Error)]
pub enum JournalError {
    #[error("Invalid identifier: {reason}")]
    InvalidIdentifier { reason: String },

    #[error("Missing pre-execution record for turn '{turn_id}': cannot reconcile terminal outcome before pre-execution is captured")]
    MissingPreExecution { turn_id: String },

    #[error("Corrupted journal log at line {line_number}: {reason}")]
    CorruptedLog {
        line_number: usize,
        reason: String,
    },

    #[error("Conflicting pre-execution record for turn '{turn_id}': {reason}")]
    ConflictingPreExecution {
        turn_id: String,
        reason: String,
    },

    #[error("Conflicting terminal outcome for turn '{turn_id}': existing {existing:?}, attempted {attempted:?}, reason: {reason}")]
    ConflictingOutcome {
        turn_id: String,
        existing: TurnTerminalStatus,
        attempted: TurnTerminalStatus,
        reason: String,
    },

    #[error("Conflicting mid-turn event for turn '{turn_id}': {reason}")]
    ConflictingMidTurn { turn_id: String, reason: String },

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("Internal journal error: {0}")]
    Internal(String),
}

/// Raw journal event stored in the append-only log.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "event_type", rename_all = "snake_case")]
pub enum RawJournalEvent {
    PreExecution {
        user_id: String,
        conversation_id: String,
        turn_id: String,
        parent_turn_id: Option<String>,
        user_message: String,
        workspace: Option<String>,
        created_at_ms: u64,
    },
    FinalOutcome {
        user_id: String,
        conversation_id: String,
        turn_id: String,
        status: TurnTerminalStatus,
        assistant_message: Option<String>,
        token_usage: Option<TokenUsageRecord>,
        attempts: u32,
        last_attempt_id: Option<String>,
        retry_summaries: Option<Vec<AttemptSummary>>,
        error_metadata: Option<serde_json::Value>,
        finished_at_ms: u64,
    },
    MidTurn { record: MidTurnRecord },
}

/// Hash the canonical serialized raw event sequence used as Memory Candidate
/// provenance. This helper does not alter the public two-method journal seam.
pub(crate) fn canonical_raw_events_hash(events: &[RawJournalEvent]) -> String {
    let payload = serde_json::to_vec(events).expect("raw journal events are serializable");
    digest_hex(&payload)
}

impl RawJournalEvent {
    pub fn user_id(&self) -> &str {
        match self {
            Self::PreExecution { user_id, .. } => user_id,
            Self::FinalOutcome { user_id, .. } => user_id,
            Self::MidTurn { record } => &record.user_id,
        }
    }

    pub fn conversation_id(&self) -> &str {
        match self {
            Self::PreExecution { conversation_id, .. } => conversation_id,
            Self::FinalOutcome { conversation_id, .. } => conversation_id,
            Self::MidTurn { record } => &record.conversation_id,
        }
    }

    pub fn turn_id(&self) -> &str {
        match self {
            Self::PreExecution { turn_id, .. } => turn_id,
            Self::FinalOutcome { turn_id, .. } => turn_id,
            Self::MidTurn { record } => &record.turn_id,
        }
    }
}

/// Normalizes a workspace identifier or path into an opaque metadata label.
///
/// Ensures raw host filesystem paths from callers or database rows NEVER leak into raw event journal files.
pub fn normalize_workspace_label(workspace: Option<&str>) -> Option<String> {
    workspace.and_then(|ws| {
        let trimmed = ws.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some("bound_workspace".to_string())
        }
    })
}

/// Validates that an identifier does not contain path traversal or invalid characters.
pub fn validate_identifier(id: &str, field_name: &str) -> Result<(), JournalError> {
    let trimmed = id.trim();
    if trimmed.is_empty() {
        return Err(JournalError::InvalidIdentifier {
            reason: format!("{field_name} must not be empty"),
        });
    }
    if trimmed.contains("..")
        || trimmed.contains('/')
        || trimmed.contains('\\')
        || trimmed.contains('\0')
        || trimmed.chars().any(|c| c.is_control())
    {
        return Err(JournalError::InvalidIdentifier {
            reason: format!("{field_name} contains forbidden path traversal or control characters: {id}"),
        });
    }
    Ok(())
}

/// The external Deep Module interface for Turn lifecycle event journaling.
///
/// Strictly exposes only two runtime lifecycle methods:
/// 1. `capture_pre_turn`: captures pre-execution turn context before worker launch.
/// 2. `reconcile_terminal`: commits terminal outcome with complete-payload idempotency.
///
/// Startup recovery scanning is segregated into the internal recovery seam [`internal_startup_recovery`].
#[async_trait]
pub trait TurnJournal: Send + Sync {
    /// Captures the pre-execution turn event and ensures user event directory exists.
    async fn capture_pre_turn(&self, record: &PreTurnRecord<'_>) -> Result<(), JournalError>;

    /// Reconciles and commits the final terminal outcome of a turn (idempotent on exact payload, conflict-checked).
    async fn reconcile_terminal(
        &self,
        user_id: &str,
        conversation_id: &str,
        turn_id: &str,
        outcome: &TerminalOutcomeRecord<'_>,
    ) -> Result<(), JournalError>;
}

// ---------------------------------------------------------------------------
// Production Filesystem Adapter
// ---------------------------------------------------------------------------

/// Type alias for testing fault injector callback at the storage/system boundary.
#[cfg(test)]
pub type FaultInjector = Arc<dyn Fn(&Path, usize, &str, &RawJournalEvent) -> Option<std::io::Error> + Send + Sync>;

/// Production filesystem-backed implementation of [`TurnJournal`].
///
/// Stores raw append-only JSONL files under `<base_dir>/users/<user_id>/events/raw/<conversation_id>/<turn_id>.jsonl`.
#[derive(Clone)]
pub struct FilesystemTurnJournal {
    base_dir: PathBuf,
    locks: Arc<RwLock<HashMap<(String, String, String), Arc<Mutex<()>>>>>,
    #[cfg(test)]
    fault_injector: Option<FaultInjector>,
}

impl FilesystemTurnJournal {
    pub fn new(base_dir: impl Into<PathBuf>) -> Self {
        Self {
            base_dir: base_dir.into(),
            locks: Arc::new(RwLock::new(HashMap::new())),
            #[cfg(test)]
            fault_injector: None,
        }
    }

    /// Testing constructor allowing system-level fault injection during durable append operations.
    #[cfg(test)]
    pub fn with_fault_injector(base_dir: impl Into<PathBuf>, injector: FaultInjector) -> Self {
        Self {
            base_dir: base_dir.into(),
            locks: Arc::new(RwLock::new(HashMap::new())),
            fault_injector: Some(injector),
        }
    }

    /// Obtains an in-memory mutex dedicated to a canonical (user_id, conversation_id, turn_id) slot.
    ///
    /// Note on Concurrency Limitation: In-process mutual exclusion across concurrent tasks and
    /// startup recovery is strictly guaranteed via this lock. Cross-process concurrency relies
    /// on append-only atomic writes and the single-host Aion daemon topology.
    pub async fn get_turn_lock(&self, user_id: &str, conversation_id: &str, turn_id: &str) -> Arc<Mutex<()>> {
        let key = (user_id.to_string(), conversation_id.to_string(), turn_id.to_string());
        {
            let read_guard = self.locks.read().await;
            if let Some(lock) = read_guard.get(&key) {
                return Arc::clone(lock);
            }
        }
        let mut write_guard = self.locks.write().await;
        Arc::clone(write_guard.entry(key).or_insert_with(|| Arc::new(Mutex::new(()))))
    }

    /// Resolves and validates the strict events directory: `<base_dir>/users/<user_id>/events/raw/<conversation_id>`.
    fn get_conversation_raw_dir(&self, user_id: &str, conversation_id: &str) -> Result<PathBuf, JournalError> {
        validate_identifier(user_id, "user_id")?;
        validate_identifier(conversation_id, "conversation_id")?;
        Ok(self
            .base_dir
            .join("users")
            .join(user_id)
            .join("events")
            .join("raw")
            .join(conversation_id))
    }

    /// Resolves the absolute path for a specific turn log file.
    fn get_turn_file_path(
        &self,
        user_id: &str,
        conversation_id: &str,
        turn_id: &str,
    ) -> Result<PathBuf, JournalError> {
        validate_identifier(turn_id, "turn_id")?;
        let raw_dir = self.get_conversation_raw_dir(user_id, conversation_id)?;
        Ok(raw_dir.join(format!("{turn_id}.jsonl")))
    }

    /// Reads turn events applying deterministic partial-tail recovery.
    ///
    /// Partial-Tail Policy:
    /// - If the file contains an uncompleted trailing line at EOF (whether without newline or with trailing newline),
    ///   the uncompleted trailing bytes are truncated back to the last valid newline `\n`.
    /// - Middle lines MUST be valid JSONL records; corrupted middle lines result in [`JournalError::CorruptedLog`]
    ///   and are NOT silently ignored or truncated.
    pub async fn read_and_sanitize_turn_events(file_path: &Path) -> Result<Vec<RawJournalEvent>, JournalError> {
        if !file_path.exists() {
            return Ok(Vec::new());
        }

        let content_bytes = tokio::fs::read(file_path).await?;
        if content_bytes.is_empty() {
            return Ok(Vec::new());
        }

        let raw_segments: Vec<&[u8]> = content_bytes.split(|&b| b == b'\n').collect();
        let total_segments = raw_segments.len();

        let mut non_empty_indices = Vec::new();
        for (i, seg) in raw_segments.iter().enumerate() {
            if i == total_segments - 1 && seg.is_empty() {
                continue;
            }
            let is_non_empty = seg.iter().any(|&b| !b.is_ascii_whitespace());
            if is_non_empty {
                non_empty_indices.push(i);
            }
        }

        let last_non_empty_idx = non_empty_indices.last().copied();
        let mut events = Vec::new();
        let mut valid_byte_offset = 0;
        let mut needs_truncation = false;

        for (i, seg) in raw_segments.iter().enumerate() {
            if i == total_segments - 1 && seg.is_empty() {
                continue;
            }
            let is_non_empty = seg.iter().any(|&b| !b.is_ascii_whitespace());
            if !is_non_empty {
                if i < total_segments - 1 {
                    valid_byte_offset += seg.len() + 1;
                } else {
                    valid_byte_offset += seg.len();
                }
                continue;
            }

            let parsed = std::str::from_utf8(seg)
                .ok()
                .and_then(|s| serde_json::from_str::<RawJournalEvent>(s.trim()).ok());

            match parsed {
                Some(event) => {
                    events.push(event);
                    if i < total_segments - 1 {
                        valid_byte_offset += seg.len() + 1;
                    } else {
                        valid_byte_offset += seg.len();
                    }
                }
                None => {
                    let is_last_non_empty = Some(i) == last_non_empty_idx;
                    if is_last_non_empty {
                        needs_truncation = true;
                        break;
                    } else {
                        return Err(JournalError::CorruptedLog {
                            line_number: i + 1,
                            reason: format!("Corrupted middle record in {}", file_path.display()),
                        });
                    }
                }
            }
        }

        if needs_truncation || valid_byte_offset < content_bytes.len() {
            let mut attempts = 0;
            loop {
                let res = async {
                    let mut file = tokio::fs::OpenOptions::new()
                        .write(true)
                        .open(file_path)
                        .await?;
                    file.set_len(valid_byte_offset as u64).await?;
                    file.flush().await?;
                    file.sync_all().await?;
                    Ok::<(), std::io::Error>(())
                }
                .await;

                match res {
                    Ok(()) => break,
                    Err(_e) if attempts < 3 => {
                        attempts += 1;
                        tokio::time::sleep(tokio::time::Duration::from_millis(25 * attempts as u64)).await;
                    }
                    Err(e) => return Err(JournalError::Io(e)),
                }
            }
        }

        Ok(events)
    }

    /// Internal helper to atomically append a raw event with durable fsync (`sync_all`) and safe retry.
    ///
    /// Unknown Commit & Durability Guarantees:
    /// - Before each append/retry attempt, checks if the target `event` is ALREADY completely committed
    ///   at the end of the file (via canonical event compare/idempotency).
    /// - Sanitizes any uncommitted partial tail before retrying write.
    /// - Bounded retry (up to 3 attempts) wraps the full IO cycle: `sanitize` -> `open` -> `write_all` -> `flush` -> `sync_all`.
    async fn append_event_durable(&self, file_path: &Path, event: &RawJournalEvent) -> Result<(), JournalError> {
        if let Some(parent) = file_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let mut json = serde_json::to_string(event)?;
        json.push('\n');

        let mut attempts = 0;
        loop {
            // Check if the event is already committed on disk (e.g. from prior attempt whose flush/sync timed out)
            let existing_events = Self::read_and_sanitize_turn_events(file_path).await?;
            if let Some(last_event) = existing_events.last() {
                if last_event == event {
                    return Ok(());
                }
            }

            let res = async {
                let mut file = tokio::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(file_path)
                    .await?;
                file.write_all(json.as_bytes()).await?;
                #[cfg(test)]
                if let Some(injector) = &self.fault_injector {
                    if let Some(err) = injector(file_path, attempts, "after_write_before_sync", event) {
                        return Err(err);
                    }
                }
                file.flush().await?;
                file.sync_all().await?;
                Ok::<(), std::io::Error>(())
            }
            .await;

            match res {
                Ok(()) => return Ok(()),
                Err(_e) if attempts < 3 => {
                    attempts += 1;
                    tokio::time::sleep(tokio::time::Duration::from_millis(25 * attempts as u64)).await;
                }
                Err(e) => return Err(JournalError::Io(e)),
            }
        }
    }

    /// Appends compact mid-turn metadata while retaining the same per-turn
    /// lock and durability policy as the public pre/terminal operations.
    ///
    /// This is intentionally an in-crate operation used by the conversation
    /// lifecycle coordinator; it is not a third [`TurnJournal`] method.
    pub(crate) async fn append_mid_turn_event(&self, record: &MidTurnRecord) -> Result<(), JournalError> {
        validate_mid_turn_record(record)?;
        let turn_lock = self.get_turn_lock(&record.user_id, &record.conversation_id, &record.turn_id).await;
        let _guard = turn_lock.lock().await;
        let file_path = self.get_turn_file_path(&record.user_id, &record.conversation_id, &record.turn_id)?;
        let existing_events = Self::read_and_sanitize_turn_events(&file_path).await?;
        if !existing_events.iter().any(|event| matches!(event, RawJournalEvent::PreExecution { .. })) {
            return Err(JournalError::MissingPreExecution {
                turn_id: record.turn_id.clone(),
            });
        }

        for existing in existing_events {
            let RawJournalEvent::MidTurn { record: existing } = existing else {
                continue;
            };
            if existing.idempotency_key == record.idempotency_key {
                if existing == *record {
                    // Exact duplicate: the original durable event is authoritative.
                    return Ok(());
                }
                return Err(JournalError::ConflictingMidTurn {
                    turn_id: record.turn_id.clone(),
                    reason: format!("idempotency key {} has a different payload", record.idempotency_key),
                });
            }
        }

        self.append_event_durable(&file_path, &RawJournalEvent::MidTurn { record: record.clone() })
            .await
    }
}

#[async_trait]
impl TurnJournal for FilesystemTurnJournal {
    async fn capture_pre_turn(&self, record: &PreTurnRecord<'_>) -> Result<(), JournalError> {
        validate_identifier(record.user_id, "user_id")?;
        validate_identifier(record.conversation_id, "conversation_id")?;
        validate_identifier(record.turn_id, "turn_id")?;
        if let Some(parent_id) = record.parent_turn_id {
            validate_identifier(parent_id, "parent_turn_id")?;
        }

        // Enforce opaque workspace normalization at adapter boundary
        let normalized_workspace = normalize_workspace_label(record.workspace);

        let turn_lock = self.get_turn_lock(record.user_id, record.conversation_id, record.turn_id).await;
        let _guard = turn_lock.lock().await;

        let file_path = self.get_turn_file_path(record.user_id, record.conversation_id, record.turn_id)?;
        let existing_events = Self::read_and_sanitize_turn_events(&file_path).await?;

        for event in &existing_events {
            if let RawJournalEvent::PreExecution {
                user_id,
                conversation_id,
                turn_id,
                parent_turn_id,
                user_message,
                workspace,
                ..
            } = event
            {
                if user_id == record.user_id
                    && conversation_id == record.conversation_id
                    && turn_id == record.turn_id
                    && parent_turn_id.as_deref() == record.parent_turn_id
                    && user_message == record.user_message
                    && workspace.as_deref() == normalized_workspace.as_deref()
                {
                    // Exact match duplicate pre-turn: Idempotent No-op
                    return Ok(());
                } else {
                    // Conflicting pre-turn execution: Reject
                    return Err(JournalError::ConflictingPreExecution {
                        turn_id: record.turn_id.to_string(),
                        reason: format!(
                            "PreExecution payload mismatch for turn {}: existing user/conv/msg/parent ({}/{}/{:?}/{:?}), attempted ({}/{}/{:?}/{:?})",
                            record.turn_id,
                            user_id, conversation_id, user_message, parent_turn_id,
                            record.user_id, record.conversation_id, record.user_message, record.parent_turn_id
                        ),
                    });
                }
            }
        }

        let event = RawJournalEvent::PreExecution {
            user_id: record.user_id.to_string(),
            conversation_id: record.conversation_id.to_string(),
            turn_id: record.turn_id.to_string(),
            parent_turn_id: record.parent_turn_id.map(ToString::to_string),
            user_message: record.user_message.to_string(),
            workspace: normalized_workspace,
            created_at_ms: record.created_at_ms,
        };

        self.append_event_durable(&file_path, &event).await
    }

    async fn reconcile_terminal(
        &self,
        user_id: &str,
        conversation_id: &str,
        turn_id: &str,
        outcome: &TerminalOutcomeRecord<'_>,
    ) -> Result<(), JournalError> {
        validate_identifier(user_id, "user_id")?;
        validate_identifier(conversation_id, "conversation_id")?;
        validate_identifier(turn_id, "turn_id")?;
        if let Some(attempt_id) = outcome.last_attempt_id {
            validate_identifier(attempt_id, "last_attempt_id")?;
        }

        let turn_lock = self.get_turn_lock(user_id, conversation_id, turn_id).await;
        let _guard = turn_lock.lock().await;

        let file_path = self.get_turn_file_path(user_id, conversation_id, turn_id)?;
        let existing_events = Self::read_and_sanitize_turn_events(&file_path).await?;

        // Invariant: Must reject turns without an existing PreExecution event
        let has_pre_execution = existing_events.iter().any(|e| matches!(e, RawJournalEvent::PreExecution { .. }));
        if !has_pre_execution {
            return Err(JournalError::MissingPreExecution {
                turn_id: turn_id.to_string(),
            });
        }

        // Check if a terminal outcome already exists (Complete-Payload Idempotency & Conflict Check)
        for event in &existing_events {
            if let RawJournalEvent::FinalOutcome {
                status,
                assistant_message,
                token_usage,
                attempts,
                last_attempt_id,
                retry_summaries,
                error_metadata,
                finished_at_ms,
                ..
            } = event
            {
                let is_exact_match = *status == outcome.status
                    && assistant_message.as_deref() == outcome.assistant_message
                    && *token_usage == outcome.token_usage
                    && *attempts == outcome.attempts
                    && last_attempt_id.as_deref() == outcome.last_attempt_id
                    && *retry_summaries == outcome.retry_summaries
                    && *error_metadata == outcome.error_metadata
                    && *finished_at_ms == outcome.finished_at_ms;

                if is_exact_match {
                    // Exact identical terminal payload: Idempotent No-op
                    return Ok(());
                } else {
                    // Conflicting outcome (different status OR different payload): Reject
                    return Err(JournalError::ConflictingOutcome {
                        turn_id: turn_id.to_string(),
                        existing: *status,
                        attempted: outcome.status,
                        reason: format!(
                            "Terminal payload mismatch for turn {}: existing status/msg/usage/att/meta ({:?}/{:?}/{:?}/{:?}/{:?}), attempted ({:?}/{:?}/{:?}/{:?}/{:?})",
                            turn_id,
                            status, assistant_message, token_usage, attempts, error_metadata,
                            outcome.status, outcome.assistant_message, outcome.token_usage, outcome.attempts, outcome.error_metadata
                        ),
                    });
                }
            }
        }

        let terminal_event = RawJournalEvent::FinalOutcome {
            user_id: user_id.to_string(),
            conversation_id: conversation_id.to_string(),
            turn_id: turn_id.to_string(),
            status: outcome.status,
            assistant_message: outcome.assistant_message.map(ToString::to_string),
            token_usage: outcome.token_usage,
            attempts: outcome.attempts,
            last_attempt_id: outcome.last_attempt_id.map(ToString::to_string),
            retry_summaries: outcome.retry_summaries.clone(),
            error_metadata: outcome.error_metadata.clone(),
            finished_at_ms: outcome.finished_at_ms,
        };

        self.append_event_durable(&file_path, &terminal_event).await
    }
}

// ---------------------------------------------------------------------------
// In-Memory Test Adapter
// ---------------------------------------------------------------------------

/// In-memory thread-safe implementation of [`TurnJournal`] for unit and integration testing.
///
/// Uses canonical key `(user_id, conversation_id, turn_id)` to ensure complete cross-conversation isolation.
#[derive(Default, Clone)]
pub struct InMemoryTurnJournal {
    events: Arc<RwLock<HashMap<(String, String, String), Vec<RawJournalEvent>>>>,
}

impl InMemoryTurnJournal {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn get_turn_events(&self, user_id: &str, conversation_id: &str, turn_id: &str) -> Vec<RawJournalEvent> {
        let guard = self.events.read().await;
        guard
            .get(&(user_id.to_string(), conversation_id.to_string(), turn_id.to_string()))
            .cloned()
            .unwrap_or_default()
    }

    #[allow(dead_code)]
    pub(crate) async fn append_mid_turn_event(&self, record: &MidTurnRecord) -> Result<(), JournalError> {
        validate_mid_turn_record(record)?;
        let canonical_key = (
            record.user_id.clone(),
            record.conversation_id.clone(),
            record.turn_id.clone(),
        );
        let mut guard = self.events.write().await;
        let entry = guard.entry(canonical_key).or_default();
        if !entry.iter().any(|event| matches!(event, RawJournalEvent::PreExecution { .. })) {
            return Err(JournalError::MissingPreExecution {
                turn_id: record.turn_id.clone(),
            });
        }
        for existing in entry.iter() {
            let RawJournalEvent::MidTurn { record: existing } = existing else {
                continue;
            };
            if existing.idempotency_key == record.idempotency_key {
                if existing == record {
                    return Ok(());
                }
                return Err(JournalError::ConflictingMidTurn {
                    turn_id: record.turn_id.clone(),
                    reason: format!("idempotency key {} has a different payload", record.idempotency_key),
                });
            }
        }
        entry.push(RawJournalEvent::MidTurn { record: record.clone() });
        Ok(())
    }
}

fn validate_mid_turn_record(record: &MidTurnRecord) -> Result<(), JournalError> {
    validate_identifier(&record.user_id, "user_id")?;
    validate_identifier(&record.conversation_id, "conversation_id")?;
    validate_identifier(&record.turn_id, "turn_id")?;
    if let Some(source_turn_id) = &record.source_turn_id {
        validate_identifier(source_turn_id, "source_turn_id")?;
    }
    if let Some(attempt_id) = &record.attempt_id {
        validate_identifier(attempt_id, "attempt_id")?;
    }
    if let Some(message_id) = &record.message_id {
        validate_identifier(message_id, "message_id")?;
    }
    if record.idempotency_key.trim().is_empty() || record.content_hash.trim().is_empty() {
        return Err(JournalError::InvalidIdentifier {
            reason: "mid-turn identity and content hash must not be empty".to_string(),
        });
    }
    Ok(())
}

/// In-crate coordinator for mid-turn events. Its target is the same
/// filesystem adapter installed as the public TurnJournal, so no second lock
/// or journal root can diverge from lifecycle pre/terminal records.
#[derive(Clone)]
pub(crate) struct MidTurnCoordinator {
    journal: Arc<FilesystemTurnJournal>,
}

impl MidTurnCoordinator {
    pub(crate) fn filesystem(journal: Arc<FilesystemTurnJournal>) -> Self {
        Self { journal }
    }

    pub(crate) async fn record(&self, record: &MidTurnRecord) -> Result<(), JournalError> {
        self.journal.append_mid_turn_event(record).await
    }
}

#[async_trait]
impl TurnJournal for InMemoryTurnJournal {
    async fn capture_pre_turn(&self, record: &PreTurnRecord<'_>) -> Result<(), JournalError> {
        validate_identifier(record.user_id, "user_id")?;
        validate_identifier(record.conversation_id, "conversation_id")?;
        validate_identifier(record.turn_id, "turn_id")?;
        if let Some(parent_id) = record.parent_turn_id {
            validate_identifier(parent_id, "parent_turn_id")?;
        }

        let normalized_workspace = normalize_workspace_label(record.workspace);

        let canonical_key = (
            record.user_id.to_string(),
            record.conversation_id.to_string(),
            record.turn_id.to_string(),
        );

        let mut guard = self.events.write().await;
        let entry = guard.entry(canonical_key).or_default();

        for event in entry.iter() {
            if let RawJournalEvent::PreExecution {
                user_id,
                conversation_id,
                turn_id,
                parent_turn_id,
                user_message,
                workspace,
                ..
            } = event
            {
                if user_id == record.user_id
                    && conversation_id == record.conversation_id
                    && turn_id == record.turn_id
                    && parent_turn_id.as_deref() == record.parent_turn_id
                    && user_message == record.user_message
                    && workspace.as_deref() == normalized_workspace.as_deref()
                {
                    return Ok(());
                } else {
                    return Err(JournalError::ConflictingPreExecution {
                        turn_id: record.turn_id.to_string(),
                        reason: format!(
                            "PreExecution payload mismatch for turn {}: existing user/conv/msg/parent ({}/{}/{:?}/{:?}), attempted ({}/{}/{:?}/{:?})",
                            record.turn_id,
                            user_id, conversation_id, user_message, parent_turn_id,
                            record.user_id, record.conversation_id, record.user_message, record.parent_turn_id
                        ),
                    });
                }
            }
        }

        let event = RawJournalEvent::PreExecution {
            user_id: record.user_id.to_string(),
            conversation_id: record.conversation_id.to_string(),
            turn_id: record.turn_id.to_string(),
            parent_turn_id: record.parent_turn_id.map(ToString::to_string),
            user_message: record.user_message.to_string(),
            workspace: normalized_workspace,
            created_at_ms: record.created_at_ms,
        };

        entry.push(event);
        Ok(())
    }

    async fn reconcile_terminal(
        &self,
        user_id: &str,
        conversation_id: &str,
        turn_id: &str,
        outcome: &TerminalOutcomeRecord<'_>,
    ) -> Result<(), JournalError> {
        validate_identifier(user_id, "user_id")?;
        validate_identifier(conversation_id, "conversation_id")?;
        validate_identifier(turn_id, "turn_id")?;
        if let Some(attempt_id) = outcome.last_attempt_id {
            validate_identifier(attempt_id, "last_attempt_id")?;
        }

        let canonical_key = (
            user_id.to_string(),
            conversation_id.to_string(),
            turn_id.to_string(),
        );

        let mut guard = self.events.write().await;
        let entry = guard.entry(canonical_key).or_default();

        // Invariant: Must reject turns without an existing PreExecution event
        let has_pre_execution = entry.iter().any(|e| matches!(e, RawJournalEvent::PreExecution { .. }));
        if !has_pre_execution {
            return Err(JournalError::MissingPreExecution {
                turn_id: turn_id.to_string(),
            });
        }

        for event in entry.iter() {
            if let RawJournalEvent::FinalOutcome {
                status,
                assistant_message,
                token_usage,
                attempts,
                last_attempt_id,
                retry_summaries,
                error_metadata,
                finished_at_ms,
                ..
            } = event
            {
                let is_exact_match = *status == outcome.status
                    && assistant_message.as_deref() == outcome.assistant_message
                    && *token_usage == outcome.token_usage
                    && *attempts == outcome.attempts
                    && last_attempt_id.as_deref() == outcome.last_attempt_id
                    && *retry_summaries == outcome.retry_summaries
                    && *error_metadata == outcome.error_metadata
                    && *finished_at_ms == outcome.finished_at_ms;

                if is_exact_match {
                    return Ok(());
                } else {
                    return Err(JournalError::ConflictingOutcome {
                        turn_id: turn_id.to_string(),
                        existing: *status,
                        attempted: outcome.status,
                        reason: format!(
                            "Terminal payload mismatch for turn {}: existing status/msg/usage/att/meta ({:?}/{:?}/{:?}/{:?}/{:?}), attempted ({:?}/{:?}/{:?}/{:?}/{:?})",
                            turn_id,
                            status, assistant_message, token_usage, attempts, error_metadata,
                            outcome.status, outcome.assistant_message, outcome.token_usage, outcome.attempts, outcome.error_metadata
                        ),
                    });
                }
            }
        }

        let terminal_event = RawJournalEvent::FinalOutcome {
            user_id: user_id.to_string(),
            conversation_id: conversation_id.to_string(),
            turn_id: turn_id.to_string(),
            status: outcome.status,
            assistant_message: outcome.assistant_message.map(ToString::to_string),
            token_usage: outcome.token_usage,
            attempts: outcome.attempts,
            last_attempt_id: outcome.last_attempt_id.map(ToString::to_string),
            retry_summaries: outcome.retry_summaries.clone(),
            error_metadata: outcome.error_metadata.clone(),
            finished_at_ms: outcome.finished_at_ms,
        };
        entry.push(terminal_event);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Internal Startup Recovery Engine (Private Bootstrap Operation)
// ---------------------------------------------------------------------------

/// Configuration options for the internal startup recovery scanner.
#[derive(Debug, Clone)]
pub struct StartupRecoveryOptions {
    pub grace_window_ms: u64,
    pub now_ms: u64,
}

/// Internal bootstrap operation to scan and reconcile unclosed in-flight turns across all users.
///
/// Uses the provided shared [`TurnJournal`] instance to guarantee per-turn locking, payload validation,
/// path jail checks, and durable `sync_all` writes without bypassing safety invariants.
pub(crate) async fn internal_startup_recovery(
    journal: &dyn TurnJournal,
    base_dir: &Path,
    options: &StartupRecoveryOptions,
) -> Result<usize, JournalError> {
    let users_dir = base_dir.join("users");
    if !users_dir.exists() {
        return Ok(0);
    }

    let mut reconciled_count = 0;
    let mut user_entries = tokio::fs::read_dir(&users_dir).await?;

    while let Some(user_entry) = user_entries.next_entry().await? {
        let user_path = user_entry.path();
        if !user_path.is_dir() {
            continue;
        }

        let user_id = match user_path.file_name().and_then(|n| n.to_str()) {
            Some(name) => name.to_string(),
            None => continue,
        };
        if let Err(e) = validate_identifier(&user_id, "scanned_user_id") {
            tracing::warn!(error = %e, user_id = %user_id, "Skipping invalid user_id in recovery scan");
            continue;
        }

        let raw_root = user_path.join("events").join("raw");
        if !raw_root.exists() {
            continue;
        }

        let mut conv_entries = tokio::fs::read_dir(&raw_root).await?;
        while let Some(conv_entry) = conv_entries.next_entry().await? {
            let conv_path = conv_entry.path();
            if !conv_path.is_dir() {
                continue;
            }

            let conv_id = match conv_path.file_name().and_then(|n| n.to_str()) {
                Some(name) => name.to_string(),
                None => continue,
            };
            if let Err(e) = validate_identifier(&conv_id, "scanned_conv_id") {
                tracing::warn!(error = %e, conv_id = %conv_id, "Skipping invalid conv_id in recovery scan");
                continue;
            }

            let mut turn_files = tokio::fs::read_dir(&conv_path).await?;
            while let Some(file_entry) = turn_files.next_entry().await? {
                let file_path = file_entry.path();
                if file_path.extension().and_then(|ext| ext.to_str()) != Some("jsonl") {
                    continue;
                }

                let turn_id = match file_path.file_stem().and_then(|s| s.to_str()) {
                    Some(stem) => stem.to_string(),
                    None => continue,
                };
                if let Err(e) = validate_identifier(&turn_id, "scanned_turn_id") {
                    tracing::warn!(error = %e, turn_id = %turn_id, "Skipping invalid turn_id in recovery scan");
                    continue;
                }

                let events = FilesystemTurnJournal::read_and_sanitize_turn_events(&file_path).await?;
                let mut pre_turn = None;
                let mut has_terminal = false;

                for event in &events {
                    // Strict identity verification between directory structure and event payload
                    if event.user_id() != user_id || event.conversation_id() != conv_id || event.turn_id() != turn_id {
                        tracing::warn!(
                            path = %file_path.display(),
                            dir_user = %user_id,
                            event_user = %event.user_id(),
                            "Mismatched identity in raw journal event payload during recovery; skipping corrupted turn file"
                        );
                        has_terminal = true;
                        break;
                    }

                    match event {
                        RawJournalEvent::PreExecution { created_at_ms, .. } => {
                            pre_turn = Some(*created_at_ms);
                        }
                        RawJournalEvent::FinalOutcome { .. } => {
                            has_terminal = true;
                            break;
                        }
                        RawJournalEvent::MidTurn { .. } => {}
                    }
                }

                if !has_terminal {
                    if let Some(created_at_ms) = pre_turn {
                        let elapsed = options.now_ms.saturating_sub(created_at_ms);
                        if elapsed >= options.grace_window_ms {
                            let timeout_outcome = TerminalOutcomeRecord {
                                status: TurnTerminalStatus::Timeout,
                                assistant_message: None,
                                token_usage: None,
                                attempts: 1,
                                last_attempt_id: None,
                                retry_summaries: None,
                                error_metadata: Some(serde_json::json!({
                                    "recovered": true,
                                    "reason": "server_restart_timeout",
                                    "grace_window_ms": options.grace_window_ms,
                                    "elapsed_ms": elapsed,
                                    "assistant_message": "unavailable",
                                    "token_usage": "unavailable"
                                })),
                                finished_at_ms: options.now_ms,
                            };

                            // Use shared reconcile_terminal with per-turn lock, PreExecution verification, and sync_all
                            journal.reconcile_terminal(&user_id, &conv_id, &turn_id, &timeout_outcome).await?;
                            reconciled_count += 1;
                        }
                    }
                }
            }
        }
    }

    Ok(reconciled_count)
}

// ---------------------------------------------------------------------------
// Interface-Level Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_mid_turn_event_is_compact_idempotent_and_conflict_checked() {
        let temp = tempdir().unwrap();
        let journal = FilesystemTurnJournal::new(temp.path());
        let pre = PreTurnRecord {
            user_id: "user_midturn",
            conversation_id: "conv_midturn",
            turn_id: "turn_midturn",
            parent_turn_id: None,
            user_message: "start",
            workspace: Some("opaque-workspace"),
            created_at_ms: 1,
        };
        journal.capture_pre_turn(&pre).await.unwrap();

        let accepted = MidTurnRecord::new(
            "user_midturn",
            "conv_midturn",
            "turn_midturn",
            None,
            None,
            Some("msg_midturn"),
            "steer body",
            MidTurnEventKind::Accepted,
            None,
            2,
        );
        journal.append_mid_turn_event(&accepted).await.unwrap();
        // Replaying the same complete payload is a no-op, not a second line.
        journal.append_mid_turn_event(&accepted).await.unwrap();

        let mut conflicting = accepted.clone();
        conflicting.reason = Some("different reason".to_owned());
        assert!(matches!(
            journal.append_mid_turn_event(&conflicting).await,
            Err(JournalError::ConflictingMidTurn { .. })
        ));

        let raw_file = temp
            .path()
            .join("users/user_midturn/events/raw/conv_midturn/turn_midturn.jsonl");
        let events = FilesystemTurnJournal::read_and_sanitize_turn_events(&raw_file)
            .await
            .unwrap();
        assert_eq!(events.len(), 2, "duplicate mid-turn append must be a no-op");
        assert!(matches!(events[1], RawJournalEvent::MidTurn { .. }));
        let serialized = serde_json::to_string(&events[1]).unwrap();
        assert!(!serialized.contains("steer body"), "mid-turn journal must not copy message text");
        assert!(serialized.contains("content_hash"));
    }

    #[tokio::test]
    async fn test_capture_pre_turn_creates_user_directory_and_appends_event() {
        let temp = tempdir().unwrap();
        let journal = FilesystemTurnJournal::new(temp.path());

        let pre_record = PreTurnRecord {
            user_id: "user_alice",
            conversation_id: "conv_123",
            turn_id: "turn_001",
            parent_turn_id: Some("turn_000"),
            user_message: "Hello assistant!",
            workspace: Some("bound_workspace"),
            created_at_ms: 1000,
        };

        journal.capture_pre_turn(&pre_record).await.unwrap();

        let raw_file = temp.path().join("users/user_alice/events/raw/conv_123/turn_001.jsonl");
        assert!(raw_file.exists());

        let events = FilesystemTurnJournal::read_and_sanitize_turn_events(&raw_file).await.unwrap();
        assert_eq!(events.len(), 1);
        match &events[0] {
            RawJournalEvent::PreExecution {
                user_id,
                conversation_id,
                turn_id,
                parent_turn_id,
                user_message,
                workspace,
                created_at_ms,
            } => {
                assert_eq!(user_id, "user_alice");
                assert_eq!(conversation_id, "conv_123");
                assert_eq!(turn_id, "turn_001");
                assert_eq!(parent_turn_id.as_deref(), Some("turn_000"));
                assert_eq!(user_message, "Hello assistant!");
                assert_eq!(workspace.as_deref(), Some("bound_workspace"));
                assert_eq!(*created_at_ms, 1000);
            }
            _ => panic!("Expected PreExecution event"),
        }
    }

    #[tokio::test]
    async fn test_capture_pre_turn_duplicate_exact_noop() {
        let temp = tempdir().unwrap();
        let journal = FilesystemTurnJournal::new(temp.path());

        let pre_record = PreTurnRecord {
            user_id: "user_alice",
            conversation_id: "conv_123",
            turn_id: "turn_001_dup",
            parent_turn_id: None,
            user_message: "Same message",
            workspace: None,
            created_at_ms: 1000,
        };

        journal.capture_pre_turn(&pre_record).await.unwrap();
        journal.capture_pre_turn(&pre_record).await.unwrap();

        let raw_file = temp.path().join("users/user_alice/events/raw/conv_123/turn_001_dup.jsonl");
        let events = FilesystemTurnJournal::read_and_sanitize_turn_events(&raw_file).await.unwrap();
        assert_eq!(events.len(), 1);
    }

    #[tokio::test]
    async fn test_capture_pre_turn_conflicting_payload_reject() {
        let temp = tempdir().unwrap();
        let journal = FilesystemTurnJournal::new(temp.path());

        let pre_record_1 = PreTurnRecord {
            user_id: "user_alice",
            conversation_id: "conv_123",
            turn_id: "turn_001_conflict",
            parent_turn_id: None,
            user_message: "Message A",
            workspace: None,
            created_at_ms: 1000,
        };
        journal.capture_pre_turn(&pre_record_1).await.unwrap();

        let pre_record_2 = PreTurnRecord {
            user_id: "user_alice",
            conversation_id: "conv_123",
            turn_id: "turn_001_conflict",
            parent_turn_id: None,
            user_message: "Message B (Different!)",
            workspace: None,
            created_at_ms: 1000,
        };

        let err = journal.capture_pre_turn(&pre_record_2).await.unwrap_err();
        assert!(matches!(err, JournalError::ConflictingPreExecution { .. }));
    }

    #[tokio::test]
    async fn test_reconcile_terminal_rejects_missing_pre_execution_in_both_adapters() {
        let temp = tempdir().unwrap();
        let fs_journal = FilesystemTurnJournal::new(temp.path());
        let mem_journal = InMemoryTurnJournal::new();

        let outcome = TerminalOutcomeRecord {
            status: TurnTerminalStatus::Success,
            assistant_message: None,
            token_usage: None,
            attempts: 1,
            last_attempt_id: Some("turn_no_pre-att-1"),
            retry_summaries: None,
            error_metadata: None,
            finished_at_ms: 2000,
        };

        // Filesystem adapter must reject without PreExecution
        let err_fs = fs_journal
            .reconcile_terminal("user_test", "conv_test", "turn_no_pre", &outcome)
            .await
            .unwrap_err();
        assert!(matches!(err_fs, JournalError::MissingPreExecution { .. }));

        // InMemory adapter must reject without PreExecution
        let err_mem = mem_journal
            .reconcile_terminal("user_test", "conv_test", "turn_no_pre", &outcome)
            .await
            .unwrap_err();
        assert!(matches!(err_mem, JournalError::MissingPreExecution { .. }));
    }

    #[tokio::test]
    async fn test_reconcile_terminal_success_with_retry_summaries() {
        let temp = tempdir().unwrap();
        let journal = FilesystemTurnJournal::new(temp.path());

        let pre_record = PreTurnRecord {
            user_id: "user_bob",
            conversation_id: "conv_456",
            turn_id: "turn_002",
            parent_turn_id: None,
            user_message: "Calculate 2+2",
            workspace: None,
            created_at_ms: 2000,
        };
        journal.capture_pre_turn(&pre_record).await.unwrap();

        let outcome = TerminalOutcomeRecord {
            status: TurnTerminalStatus::Success,
            assistant_message: None,
            token_usage: None,
            attempts: 2,
            last_attempt_id: Some("turn_002-att-2"),
            retry_summaries: Some(vec![
                AttemptSummary {
                    attempt_id: "turn_002-att-1".to_string(),
                    error: Some("transient_timeout".to_string()),
                    duration_ms: Some(1500),
                },
                AttemptSummary {
                    attempt_id: "turn_002-att-2".to_string(),
                    error: None,
                    duration_ms: Some(800),
                },
            ]),
            error_metadata: Some(serde_json::json!({
                "assistant_message": "unavailable",
                "token_usage": "unavailable"
            })),
            finished_at_ms: 2500,
        };

        journal
            .reconcile_terminal("user_bob", "conv_456", "turn_002", &outcome)
            .await
            .unwrap();

        let raw_file = temp.path().join("users/user_bob/events/raw/conv_456/turn_002.jsonl");
        let events = FilesystemTurnJournal::read_and_sanitize_turn_events(&raw_file).await.unwrap();
        assert_eq!(events.len(), 2);
        match &events[1] {
            RawJournalEvent::FinalOutcome {
                status,
                assistant_message,
                token_usage,
                attempts,
                last_attempt_id,
                retry_summaries,
                finished_at_ms,
                ..
            } => {
                assert_eq!(*status, TurnTerminalStatus::Success);
                assert_eq!(assistant_message.as_deref(), None);
                assert_eq!(*token_usage, None);
                assert_eq!(*attempts, 2);
                assert_eq!(last_attempt_id.as_deref(), Some("turn_002-att-2"));
                assert_eq!(retry_summaries.as_ref().unwrap().len(), 2);
                assert_eq!(*finished_at_ms, 2500);
            }
            _ => panic!("Expected FinalOutcome event"),
        }
    }

    #[tokio::test]
    async fn test_reconcile_terminal_duplicate_exact_payload_noop() {
        let temp = tempdir().unwrap();
        let journal = FilesystemTurnJournal::new(temp.path());

        let pre_record = PreTurnRecord {
            user_id: "user_bob",
            conversation_id: "conv_456",
            turn_id: "turn_003",
            parent_turn_id: None,
            user_message: "Calculate 2+2",
            workspace: None,
            created_at_ms: 2000,
        };
        journal.capture_pre_turn(&pre_record).await.unwrap();

        let outcome = TerminalOutcomeRecord {
            status: TurnTerminalStatus::Success,
            assistant_message: None,
            token_usage: None,
            attempts: 1,
            last_attempt_id: Some("turn_003-att-1"),
            retry_summaries: None,
            error_metadata: Some(serde_json::json!({"assistant_message": "unavailable", "token_usage": "unavailable"})),
            finished_at_ms: 3000,
        };

        journal
            .reconcile_terminal("user_bob", "conv_456", "turn_003", &outcome)
            .await
            .unwrap();

        journal
            .reconcile_terminal("user_bob", "conv_456", "turn_003", &outcome)
            .await
            .unwrap();

        let raw_file = temp.path().join("users/user_bob/events/raw/conv_456/turn_003.jsonl");
        let events = FilesystemTurnJournal::read_and_sanitize_turn_events(&raw_file).await.unwrap();
        assert_eq!(events.len(), 2);
    }

    #[tokio::test]
    async fn test_reconcile_terminal_same_status_different_payload_reject() {
        let temp = tempdir().unwrap();
        let journal = FilesystemTurnJournal::new(temp.path());

        let pre_record = PreTurnRecord {
            user_id: "user_bob",
            conversation_id: "conv_456",
            turn_id: "turn_003_diff",
            parent_turn_id: None,
            user_message: "Calculate",
            workspace: None,
            created_at_ms: 2000,
        };
        journal.capture_pre_turn(&pre_record).await.unwrap();

        let outcome_1 = TerminalOutcomeRecord {
            status: TurnTerminalStatus::Success,
            assistant_message: None,
            token_usage: None,
            attempts: 1,
            last_attempt_id: Some("turn_003_diff-att-1"),
            retry_summaries: None,
            error_metadata: Some(serde_json::json!({"ver": 1})),
            finished_at_ms: 3000,
        };

        journal
            .reconcile_terminal("user_bob", "conv_456", "turn_003_diff", &outcome_1)
            .await
            .unwrap();

        let outcome_2 = TerminalOutcomeRecord {
            status: TurnTerminalStatus::Success,
            assistant_message: None,
            token_usage: None,
            attempts: 1,
            last_attempt_id: Some("turn_003_diff-att-1"),
            retry_summaries: None,
            error_metadata: Some(serde_json::json!({"ver": 2})),
            finished_at_ms: 3000,
        };

        let err = journal
            .reconcile_terminal("user_bob", "conv_456", "turn_003_diff", &outcome_2)
            .await
            .unwrap_err();

        match err {
            JournalError::ConflictingOutcome { existing, attempted, reason, .. } => {
                assert_eq!(existing, TurnTerminalStatus::Success);
                assert_eq!(attempted, TurnTerminalStatus::Success);
                assert!(reason.contains("payload mismatch"));
            }
            _ => panic!("Expected ConflictingOutcome error on different payload"),
        }
    }

    #[tokio::test]
    async fn test_reconcile_terminal_rejects_conflicting_state() {
        let temp = tempdir().unwrap();
        let journal = FilesystemTurnJournal::new(temp.path());

        let pre_record = PreTurnRecord {
            user_id: "user_bob",
            conversation_id: "conv_456",
            turn_id: "turn_004",
            parent_turn_id: None,
            user_message: "Calculate",
            workspace: None,
            created_at_ms: 2000,
        };
        journal.capture_pre_turn(&pre_record).await.unwrap();

        let success_outcome = TerminalOutcomeRecord {
            status: TurnTerminalStatus::Success,
            assistant_message: None,
            token_usage: None,
            attempts: 1,
            last_attempt_id: Some("turn_004-att-1"),
            retry_summaries: None,
            error_metadata: None,
            finished_at_ms: 4000,
        };

        journal
            .reconcile_terminal("user_bob", "conv_456", "turn_004", &success_outcome)
            .await
            .unwrap();

        let failed_outcome = TerminalOutcomeRecord {
            status: TurnTerminalStatus::Failed,
            assistant_message: None,
            token_usage: None,
            attempts: 2,
            last_attempt_id: Some("turn_004-att-2"),
            retry_summaries: None,
            error_metadata: Some(serde_json::json!({"error": "network_err"})),
            finished_at_ms: 4100,
        };

        let err = journal
            .reconcile_terminal("user_bob", "conv_456", "turn_004", &failed_outcome)
            .await
            .unwrap_err();

        match err {
            JournalError::ConflictingOutcome { existing, attempted, .. } => {
                assert_eq!(existing, TurnTerminalStatus::Success);
                assert_eq!(attempted, TurnTerminalStatus::Failed);
            }
            _ => panic!("Expected ConflictingOutcome error"),
        }
    }

    #[tokio::test]
    async fn test_concurrent_terminal_reconciliation() {
        let temp = tempdir().unwrap();
        let journal = Arc::new(FilesystemTurnJournal::new(temp.path()));

        let pre_record = PreTurnRecord {
            user_id: "user_concur",
            conversation_id: "conv_concur",
            turn_id: "turn_concur_1",
            parent_turn_id: None,
            user_message: "Concurrent test",
            workspace: None,
            created_at_ms: 1000,
        };
        journal.capture_pre_turn(&pre_record).await.unwrap();

        let mut handles = Vec::new();
        for _ in 0..10 {
            let j = Arc::clone(&journal);
            handles.push(tokio::spawn(async move {
                let outcome = TerminalOutcomeRecord {
                    status: TurnTerminalStatus::Success,
                    assistant_message: None,
                    token_usage: None,
                    attempts: 1,
                    last_attempt_id: Some("turn_concur_1-att-1"),
                    retry_summaries: None,
                    error_metadata: None,
                    finished_at_ms: 2000,
                };
                j.reconcile_terminal("user_concur", "conv_concur", "turn_concur_1", &outcome).await
            }));
        }

        for h in handles {
            let res = h.await.unwrap();
            assert!(res.is_ok());
        }

        let raw_file = temp.path().join("users/user_concur/events/raw/conv_concur/turn_concur_1.jsonl");
        let events = FilesystemTurnJournal::read_and_sanitize_turn_events(&raw_file).await.unwrap();
        assert_eq!(events.len(), 2);
    }

    #[tokio::test]
    async fn test_recovery_vs_live_terminal_race() {
        let temp = tempdir().unwrap();
        let journal = Arc::new(FilesystemTurnJournal::new(temp.path()));

        let pre_record = PreTurnRecord {
            user_id: "user_race",
            conversation_id: "conv_race",
            turn_id: "turn_race_1",
            parent_turn_id: None,
            user_message: "Race test",
            workspace: None,
            created_at_ms: 1000,
        };
        journal.capture_pre_turn(&pre_record).await.unwrap();

        let options = StartupRecoveryOptions {
            grace_window_ms: 0,
            now_ms: 5000,
        };

        let j_live = Arc::clone(&journal);
        let live_handle = tokio::spawn(async move {
            let outcome = TerminalOutcomeRecord {
                status: TurnTerminalStatus::Success,
                assistant_message: None,
                token_usage: None,
                attempts: 1,
                last_attempt_id: Some("turn_race_1-att-1"),
                retry_summaries: None,
                error_metadata: None,
                finished_at_ms: 2000,
            };
            j_live.reconcile_terminal("user_race", "conv_race", "turn_race_1", &outcome).await
        });

        let j_rec = Arc::clone(&journal);
        let rec_handle = tokio::spawn(async move {
            internal_startup_recovery(j_rec.as_ref(), &j_rec.base_dir, &options).await
        });

        let (live_res, rec_res) = tokio::join!(live_handle, rec_handle);
        assert!(live_res.unwrap().is_ok() || rec_res.unwrap().is_ok());

        let raw_file = temp.path().join("users/user_race/events/raw/conv_race/turn_race_1.jsonl");
        let events = FilesystemTurnJournal::read_and_sanitize_turn_events(&raw_file).await.unwrap();
        assert_eq!(events.len(), 2, "Must contain exactly 1 PreExecution and 1 FinalOutcome under race");
    }

    #[tokio::test]
    async fn test_cross_conversation_isolation_with_same_turn_id() {
        let temp = tempdir().unwrap();
        let journal = FilesystemTurnJournal::new(temp.path());

        journal
            .capture_pre_turn(&PreTurnRecord {
                user_id: "user_multi",
                conversation_id: "conv_alpha",
                turn_id: "turn_shared_name",
                parent_turn_id: None,
                user_message: "Message in Alpha",
                workspace: None,
                created_at_ms: 1000,
            })
            .await
            .unwrap();

        journal
            .capture_pre_turn(&PreTurnRecord {
                user_id: "user_multi",
                conversation_id: "conv_beta",
                turn_id: "turn_shared_name",
                parent_turn_id: None,
                user_message: "Message in Beta",
                workspace: None,
                created_at_ms: 1000,
            })
            .await
            .unwrap();

        let file_alpha = temp.path().join("users/user_multi/events/raw/conv_alpha/turn_shared_name.jsonl");
        let file_beta = temp.path().join("users/user_multi/events/raw/conv_beta/turn_shared_name.jsonl");

        assert!(file_alpha.exists());
        assert!(file_beta.exists());

        let events_alpha = FilesystemTurnJournal::read_and_sanitize_turn_events(&file_alpha).await.unwrap();
        let events_beta = FilesystemTurnJournal::read_and_sanitize_turn_events(&file_beta).await.unwrap();

        assert_eq!(events_alpha.len(), 1);
        assert_eq!(events_beta.len(), 1);
        assert_ne!(events_alpha[0], events_beta[0]);
    }

    #[tokio::test]
    async fn test_build_failure_terminal_without_stream_relay() {
        let temp = tempdir().unwrap();
        let journal = FilesystemTurnJournal::new(temp.path());

        let pre_record = PreTurnRecord {
            user_id: "user_carol",
            conversation_id: "conv_789",
            turn_id: "turn_005",
            parent_turn_id: None,
            user_message: "Do something",
            workspace: None,
            created_at_ms: 5000,
        };
        journal.capture_pre_turn(&pre_record).await.unwrap();

        let build_failure = TerminalOutcomeRecord {
            status: TurnTerminalStatus::Failed,
            assistant_message: None,
            token_usage: None,
            attempts: 0,
            last_attempt_id: None,
            retry_summaries: None,
            error_metadata: Some(serde_json::json!({
                "stage": "worker_build",
                "error": "workspace_inaccessible",
                "assistant_message": "unavailable",
                "token_usage": "unavailable"
            })),
            finished_at_ms: 5050,
        };

        journal
            .reconcile_terminal("user_carol", "conv_789", "turn_005", &build_failure)
            .await
            .unwrap();

        let raw_file = temp.path().join("users/user_carol/events/raw/conv_789/turn_005.jsonl");
        let events = FilesystemTurnJournal::read_and_sanitize_turn_events(&raw_file).await.unwrap();
        assert_eq!(events.len(), 2);
        match &events[1] {
            RawJournalEvent::FinalOutcome { status, error_metadata, .. } => {
                assert_eq!(*status, TurnTerminalStatus::Failed);
                assert_eq!(
                    error_metadata.as_ref().unwrap().get("stage").unwrap(),
                    "worker_build"
                );
            }
            _ => panic!("Expected FinalOutcome event"),
        }
    }

    #[tokio::test]
    async fn test_cancel_and_timeout_status_mapping() {
        let temp = tempdir().unwrap();
        let journal = FilesystemTurnJournal::new(temp.path());

        journal
            .capture_pre_turn(&PreTurnRecord {
                user_id: "user_dave",
                conversation_id: "conv_111",
                turn_id: "turn_006",
                parent_turn_id: None,
                user_message: "Cancel me",
                workspace: None,
                created_at_ms: 1000,
            })
            .await
            .unwrap();

        let cancelled_outcome = TerminalOutcomeRecord {
            status: TurnTerminalStatus::Cancelled,
            assistant_message: None,
            token_usage: None,
            attempts: 1,
            last_attempt_id: Some("turn_006-att-1"),
            retry_summaries: None,
            error_metadata: Some(serde_json::json!({"reason": "user_cancelled"})),
            finished_at_ms: 6000,
        };
        journal
            .reconcile_terminal("user_dave", "conv_111", "turn_006", &cancelled_outcome)
            .await
            .unwrap();

        journal
            .capture_pre_turn(&PreTurnRecord {
                user_id: "user_dave",
                conversation_id: "conv_111",
                turn_id: "turn_007",
                parent_turn_id: None,
                user_message: "Timeout me",
                workspace: None,
                created_at_ms: 1000,
            })
            .await
            .unwrap();

        let timeout_outcome = TerminalOutcomeRecord {
            status: TurnTerminalStatus::Timeout,
            assistant_message: None,
            token_usage: None,
            attempts: 1,
            last_attempt_id: Some("turn_007-att-1"),
            retry_summaries: None,
            error_metadata: Some(serde_json::json!({"timeout_ms": 300000})),
            finished_at_ms: 6500,
        };
        journal
            .reconcile_terminal("user_dave", "conv_111", "turn_007", &timeout_outcome)
            .await
            .unwrap();

        let events_6 = FilesystemTurnJournal::read_and_sanitize_turn_events(
            &temp.path().join("users/user_dave/events/raw/conv_111/turn_006.jsonl"),
        )
        .await
        .unwrap();
        assert_eq!(events_6.len(), 2);
        assert!(matches!(events_6[1], RawJournalEvent::FinalOutcome { status: TurnTerminalStatus::Cancelled, .. }));

        let events_7 = FilesystemTurnJournal::read_and_sanitize_turn_events(
            &temp.path().join("users/user_dave/events/raw/conv_111/turn_007.jsonl"),
        )
        .await
        .unwrap();
        assert_eq!(events_7.len(), 2);
        assert!(matches!(events_7[1], RawJournalEvent::FinalOutcome { status: TurnTerminalStatus::Timeout, .. }));
    }

    #[tokio::test]
    async fn test_startup_recovery_reconciles_unclosed_turns_to_timeout() {
        let temp = tempdir().unwrap();
        let journal = FilesystemTurnJournal::new(temp.path());

        let pre_record = PreTurnRecord {
            user_id: "user_eve",
            conversation_id: "conv_222",
            turn_id: "turn_008",
            parent_turn_id: None,
            user_message: "Unfinished turn",
            workspace: None,
            created_at_ms: 10_000,
        };
        journal.capture_pre_turn(&pre_record).await.unwrap();

        let options = StartupRecoveryOptions {
            grace_window_ms: 5_000,
            now_ms: 25_000,
        };

        let reconciled = internal_startup_recovery(&journal, temp.path(), &options).await.unwrap();
        assert_eq!(reconciled, 1);

        let raw_file = temp.path().join("users/user_eve/events/raw/conv_222/turn_008.jsonl");
        let events = FilesystemTurnJournal::read_and_sanitize_turn_events(&raw_file).await.unwrap();
        assert_eq!(events.len(), 2);
        match &events[1] {
            RawJournalEvent::FinalOutcome { status, error_metadata, .. } => {
                assert_eq!(*status, TurnTerminalStatus::Timeout);
                let meta = error_metadata.as_ref().unwrap();
                assert_eq!(meta.get("recovered").unwrap(), true);
                assert_eq!(meta.get("reason").unwrap(), "server_restart_timeout");
            }
            _ => panic!("Expected FinalOutcome event"),
        }

        let second_run = internal_startup_recovery(&journal, temp.path(), &options).await.unwrap();
        assert_eq!(second_run, 0);
    }

    #[tokio::test]
    async fn test_startup_recovery_respects_grace_window() {
        let temp = tempdir().unwrap();
        let journal = FilesystemTurnJournal::new(temp.path());

        let pre_record = PreTurnRecord {
            user_id: "user_eve",
            conversation_id: "conv_222",
            turn_id: "turn_009",
            parent_turn_id: None,
            user_message: "Fresh turn",
            workspace: None,
            created_at_ms: 20_000,
        };
        journal.capture_pre_turn(&pre_record).await.unwrap();

        let options = StartupRecoveryOptions {
            grace_window_ms: 5_000,
            now_ms: 22_000,
        };

        let reconciled = internal_startup_recovery(&journal, temp.path(), &options).await.unwrap();
        assert_eq!(reconciled, 0);

        let raw_file = temp.path().join("users/user_eve/events/raw/conv_222/turn_009.jsonl");
        let events = FilesystemTurnJournal::read_and_sanitize_turn_events(&raw_file).await.unwrap();
        assert_eq!(events.len(), 1);
    }

    #[tokio::test]
    async fn test_partial_tail_truncation_and_recovery() {
        let temp = tempdir().unwrap();
        let raw_file = temp.path().join("users/user_frank/events/raw/conv_333/turn_010.jsonl");
        tokio::fs::create_dir_all(raw_file.parent().unwrap()).await.unwrap();

        let valid_pre = serde_json::to_string(&RawJournalEvent::PreExecution {
            user_id: "user_frank".to_string(),
            conversation_id: "conv_333".to_string(),
            turn_id: "turn_010".to_string(),
            parent_turn_id: None,
            user_message: "Hi".to_string(),
            workspace: None,
            created_at_ms: 1000,
        })
        .unwrap();

        // Write valid record with newline, followed by an incomplete corrupted tail with no trailing newline
        let corrupted_content = format!("{valid_pre}\n{{\"event_type\":\"final_outcome\",\"broken_json_half_written");
        tokio::fs::write(&raw_file, corrupted_content).await.unwrap();

        let events = FilesystemTurnJournal::read_and_sanitize_turn_events(&raw_file).await.unwrap();
        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], RawJournalEvent::PreExecution { .. }));

        // Verify the file was truncated back to valid newline, removing partial tail
        let journal = FilesystemTurnJournal::new(temp.path());
        let outcome = TerminalOutcomeRecord {
            status: TurnTerminalStatus::Success,
            assistant_message: None,
            token_usage: None,
            attempts: 1,
            last_attempt_id: Some("turn_010-att-1"),
            retry_summaries: None,
            error_metadata: None,
            finished_at_ms: 2000,
        };

        journal
            .reconcile_terminal("user_frank", "conv_333", "turn_010", &outcome)
            .await
            .unwrap();

        let updated_events = FilesystemTurnJournal::read_and_sanitize_turn_events(&raw_file).await.unwrap();
        assert_eq!(updated_events.len(), 2);
    }

    #[tokio::test]
    async fn test_partial_tail_with_trailing_newline_sanitized_and_appended() {
        let temp = tempdir().unwrap();
        let raw_file = temp.path().join("users/user_newline/events/raw/conv_nl/turn_nl_1.jsonl");
        tokio::fs::create_dir_all(raw_file.parent().unwrap()).await.unwrap();

        let valid_pre = serde_json::to_string(&RawJournalEvent::PreExecution {
            user_id: "user_newline".to_string(),
            conversation_id: "conv_nl".to_string(),
            turn_id: "turn_nl_1".to_string(),
            parent_turn_id: None,
            user_message: "Hi".to_string(),
            workspace: None,
            created_at_ms: 1000,
        })
        .unwrap();

        // Malformed tail that DOES have a trailing newline
        let corrupted_content = format!("{valid_pre}\n{{\"event_type\":\"corrupted_tail\"}}\n");
        tokio::fs::write(&raw_file, corrupted_content).await.unwrap();

        let events = FilesystemTurnJournal::read_and_sanitize_turn_events(&raw_file).await.unwrap();
        assert_eq!(events.len(), 1);

        let journal = FilesystemTurnJournal::new(temp.path());
        let outcome = TerminalOutcomeRecord {
            status: TurnTerminalStatus::Success,
            assistant_message: None,
            token_usage: None,
            attempts: 1,
            last_attempt_id: Some("turn_nl_1-att-1"),
            retry_summaries: None,
            error_metadata: None,
            finished_at_ms: 2000,
        };

        journal
            .reconcile_terminal("user_newline", "conv_nl", "turn_nl_1", &outcome)
            .await
            .unwrap();

        let updated_events = FilesystemTurnJournal::read_and_sanitize_turn_events(&raw_file).await.unwrap();
        assert_eq!(updated_events.len(), 2);
        assert!(matches!(updated_events[1], RawJournalEvent::FinalOutcome { .. }));
    }

    #[tokio::test]
    async fn test_append_durable_retry_unknown_commit_idempotency() {
        let temp = tempdir().unwrap();
        let journal = FilesystemTurnJournal::new(temp.path());

        let pre_record = PreTurnRecord {
            user_id: "user_idemp",
            conversation_id: "conv_idemp",
            turn_id: "turn_idemp_1",
            parent_turn_id: None,
            user_message: "Idempotent write",
            workspace: None,
            created_at_ms: 1000,
        };
        journal.capture_pre_turn(&pre_record).await.unwrap();

        let outcome = TerminalOutcomeRecord {
            status: TurnTerminalStatus::Success,
            assistant_message: None,
            token_usage: None,
            attempts: 1,
            last_attempt_id: Some("turn_idemp_1-att-1"),
            retry_summaries: None,
            error_metadata: None,
            finished_at_ms: 2000,
        };

        // First reconciliation commits event
        journal
            .reconcile_terminal("user_idemp", "conv_idemp", "turn_idemp_1", &outcome)
            .await
            .unwrap();

        // Second call with exact payload performs safe idempotent check without duplicate line
        journal
            .reconcile_terminal("user_idemp", "conv_idemp", "turn_idemp_1", &outcome)
            .await
            .unwrap();

        let raw_file = temp.path().join("users/user_idemp/events/raw/conv_idemp/turn_idemp_1.jsonl");
        let events = FilesystemTurnJournal::read_and_sanitize_turn_events(&raw_file).await.unwrap();
        assert_eq!(events.len(), 2, "Duplicate reconciliation must not produce duplicate lines");
    }

    #[tokio::test]
    async fn test_durable_append_retry_with_unknown_commit_fault_injection_preserves_single_terminal() {
        let temp = tempdir().unwrap();
        let terminal_fault_injected = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let terminal_fault_clone = Arc::clone(&terminal_fault_injected);
        let terminal_attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let terminal_attempts_clone = Arc::clone(&terminal_attempts);

        // System IO boundary fault injection:
        // Pre-turn append is observed without fault injection.
        // Terminal outcome append (FinalOutcome) triggers simulated unknown-commit transient error on attempt 0.
        // Terminal outcome retry (attempt 1) is allowed to succeed cleanly.
        let injector: FaultInjector = Arc::new(move |_path, _attempt, phase, event| {
            if matches!(event, RawJournalEvent::FinalOutcome { .. }) && phase == "after_write_before_sync" {
                let prev = terminal_attempts_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                if prev == 0 {
                    terminal_fault_clone.store(true, std::sync::atomic::Ordering::SeqCst);
                    return Some(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "Simulated transient sync timeout (unknown commit failure) during terminal outcome append",
                    ));
                }
            }
            None
        });

        let journal = FilesystemTurnJournal::with_fault_injector(temp.path(), injector);

        let pre_record = PreTurnRecord {
            user_id: "user_fault_inj",
            conversation_id: "conv_fault_inj",
            turn_id: "turn_fault_inj_1",
            parent_turn_id: None,
            user_message: "Fault injection test prompt",
            workspace: None,
            created_at_ms: 1000,
        };

        // 1. Capture PreExecution event (must succeed without fault injection)
        journal.capture_pre_turn(&pre_record).await.unwrap();
        assert!(
            !terminal_fault_injected.load(std::sync::atomic::Ordering::SeqCst),
            "Pre-turn capture must NOT trigger terminal fault injection"
        );

        let outcome = TerminalOutcomeRecord {
            status: TurnTerminalStatus::Success,
            assistant_message: Some("Completed response"),
            token_usage: None,
            attempts: 1,
            last_attempt_id: Some("turn_fault_inj_1-att-1"),
            retry_summaries: None,
            error_metadata: None,
            finished_at_ms: 2000,
        };

        // 2. Reconcile terminal outcome (encounters transient sync error on attempt 0, retries and resolves via canonical compare no-op)
        journal
            .reconcile_terminal("user_fault_inj", "conv_fault_inj", "turn_fault_inj_1", &outcome)
            .await
            .unwrap();

        assert!(
            terminal_fault_injected.load(std::sync::atomic::Ordering::SeqCst),
            "Fault injector MUST have triggered during terminal outcome append"
        );
        assert_eq!(
            terminal_attempts.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "Terminal outcome append must resolve unknown commit via canonical compare no-op on retry without executing a redundant second write"
        );

        // 3. Verify exactly 1 PreExecution and exactly 1 FinalOutcome in the file
        let raw_file = temp
            .path()
            .join("users/user_fault_inj/events/raw/conv_fault_inj/turn_fault_inj_1.jsonl");
        let events = FilesystemTurnJournal::read_and_sanitize_turn_events(&raw_file)
            .await
            .unwrap();

        assert_eq!(events.len(), 2, "Must contain exactly 1 PreExecution and 1 FinalOutcome");
        assert!(matches!(events[0], RawJournalEvent::PreExecution { .. }));
        match &events[1] {
            RawJournalEvent::FinalOutcome { status, assistant_message, .. } => {
                assert_eq!(*status, TurnTerminalStatus::Success);
                assert_eq!(assistant_message.as_deref(), Some("Completed response"));
            }
            _ => panic!("Expected FinalOutcome event"),
        }

        // 4. Subsequent idempotent reconciliation call must be a no-op and preserve exactly 2 events
        journal
            .reconcile_terminal("user_fault_inj", "conv_fault_inj", "turn_fault_inj_1", &outcome)
            .await
            .unwrap();

        let final_events = FilesystemTurnJournal::read_and_sanitize_turn_events(&raw_file)
            .await
            .unwrap();
        assert_eq!(
            final_events.len(),
            2,
            "Idempotent retry must strictly preserve exactly 2 events without duplicates"
        );
    }

    #[tokio::test]
    async fn test_middle_corruption_rejects_and_errors() {
        let temp = tempdir().unwrap();
        let raw_file = temp.path().join("users/user_mid/events/raw/conv_mid/turn_mid.jsonl");
        tokio::fs::create_dir_all(raw_file.parent().unwrap()).await.unwrap();

        let valid_pre = serde_json::to_string(&RawJournalEvent::PreExecution {
            user_id: "user_mid".to_string(),
            conversation_id: "conv_mid".to_string(),
            turn_id: "turn_mid".to_string(),
            parent_turn_id: None,
            user_message: "Hi".to_string(),
            workspace: None,
            created_at_ms: 1000,
        })
        .unwrap();

        let valid_term = serde_json::to_string(&RawJournalEvent::FinalOutcome {
            user_id: "user_mid".to_string(),
            conversation_id: "conv_mid".to_string(),
            turn_id: "turn_mid".to_string(),
            status: TurnTerminalStatus::Success,
            assistant_message: None,
            token_usage: None,
            attempts: 1,
            last_attempt_id: None,
            retry_summaries: None,
            error_metadata: None,
            finished_at_ms: 2000,
        })
        .unwrap();

        // Corrupted middle line between two lines
        let corrupted_content = format!("{valid_pre}\n{{broken_middle_line}}\n{valid_term}\n");
        tokio::fs::write(&raw_file, corrupted_content).await.unwrap();

        let err = FilesystemTurnJournal::read_and_sanitize_turn_events(&raw_file).await.unwrap_err();
        assert!(matches!(err, JournalError::CorruptedLog { line_number: 2, .. }));
    }

    #[tokio::test]
    async fn test_startup_recovery_id_mismatch_and_jail_validation() {
        let temp = tempdir().unwrap();
        let raw_file = temp.path().join("users/user_good/events/raw/conv_good/turn_good.jsonl");
        tokio::fs::create_dir_all(raw_file.parent().unwrap()).await.unwrap();

        // PreExecution event with mismatched user_id
        let mismatched_event = RawJournalEvent::PreExecution {
            user_id: "user_impostor".to_string(),
            conversation_id: "conv_good".to_string(),
            turn_id: "turn_good".to_string(),
            parent_turn_id: None,
            user_message: "Mismatched".to_string(),
            workspace: None,
            created_at_ms: 10_000,
        };
        tokio::fs::write(&raw_file, format!("{}\n", serde_json::to_string(&mismatched_event).unwrap())).await.unwrap();

        let options = StartupRecoveryOptions {
            grace_window_ms: 5_000,
            now_ms: 25_000,
        };

        let journal = FilesystemTurnJournal::new(temp.path());

        // Recovery scanner skips mismatched file
        let count = internal_startup_recovery(&journal, temp.path(), &options).await.unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn test_path_isolation_rejects_traversal_on_all_identifiers() {
        let temp = tempdir().unwrap();
        let journal = FilesystemTurnJournal::new(temp.path());

        // Traversal in user_id
        let err1 = journal
            .capture_pre_turn(&PreTurnRecord {
                user_id: "../../../etc",
                conversation_id: "conv_1",
                turn_id: "turn_1",
                parent_turn_id: None,
                user_message: "attack",
                workspace: None,
                created_at_ms: 100,
            })
            .await
            .unwrap_err();
        assert!(matches!(err1, JournalError::InvalidIdentifier { .. }));

        // Traversal in conversation_id
        let err2 = journal
            .capture_pre_turn(&PreTurnRecord {
                user_id: "user_valid",
                conversation_id: "../secret_conv",
                turn_id: "turn_1",
                parent_turn_id: None,
                user_message: "attack",
                workspace: None,
                created_at_ms: 100,
            })
            .await
            .unwrap_err();
        assert!(matches!(err2, JournalError::InvalidIdentifier { .. }));

        // Traversal in turn_id
        let err3 = journal
            .capture_pre_turn(&PreTurnRecord {
                user_id: "user_valid",
                conversation_id: "conv_valid",
                turn_id: "turn/../../escape",
                parent_turn_id: None,
                user_message: "attack",
                workspace: None,
                created_at_ms: 100,
            })
            .await
            .unwrap_err();
        assert!(matches!(err3, JournalError::InvalidIdentifier { .. }));

        // Traversal in parent_turn_id
        let err4 = journal
            .capture_pre_turn(&PreTurnRecord {
                user_id: "user_valid",
                conversation_id: "conv_valid",
                turn_id: "turn_valid",
                parent_turn_id: Some("../escape_parent"),
                user_message: "attack",
                workspace: None,
                created_at_ms: 100,
            })
            .await
            .unwrap_err();
        assert!(matches!(err4, JournalError::InvalidIdentifier { .. }));

        assert!(!temp.path().join("etc").exists());
        assert!(!temp.path().join("secret_conv").exists());
    }

    #[tokio::test]
    async fn test_workspace_invariant_normalizes_host_paths_at_adapter_boundary() {
        let temp = tempdir().unwrap();
        let journal = FilesystemTurnJournal::new(temp.path());

        // Caller passes un-sanitized raw host path directly to adapter
        let raw_ws = "/root/.aionui-web-dev/conversations/users/alice/workspaces/sandbox";

        let pre_record = PreTurnRecord {
            user_id: "user_normal",
            conversation_id: "conv_normal",
            turn_id: "turn_normal",
            parent_turn_id: None,
            user_message: "work",
            workspace: Some(raw_ws),
            created_at_ms: 500,
        };

        journal.capture_pre_turn(&pre_record).await.unwrap();

        let raw_file = temp.path().join("users/user_normal/events/raw/conv_normal/turn_normal.jsonl");
        assert!(raw_file.exists());

        let events = FilesystemTurnJournal::read_and_sanitize_turn_events(&raw_file).await.unwrap();
        assert_eq!(events.len(), 1);
        if let RawJournalEvent::PreExecution { workspace, .. } = &events[0] {
            assert_eq!(workspace.as_deref(), Some("bound_workspace"));
            assert!(!workspace.as_ref().unwrap().contains("/root"));
        } else {
            panic!("Expected PreExecution");
        }
    }

    #[tokio::test]
    async fn test_human_first_boundary_isolation() {
        let temp = tempdir().unwrap();
        let vault_dir = temp.path().join("users/user_human/vault");
        let human_note = vault_dir.join("knowledge/notes/my_daily_journal.md");
        tokio::fs::create_dir_all(human_note.parent().unwrap()).await.unwrap();
        tokio::fs::write(&human_note, "# My Journal\nHuman content").await.unwrap();

        let journal = FilesystemTurnJournal::new(temp.path());
        let pre_record = PreTurnRecord {
            user_id: "user_human",
            conversation_id: "conv_human",
            turn_id: "turn_human_1",
            parent_turn_id: None,
            user_message: "Chat",
            workspace: None,
            created_at_ms: 100,
        };
        journal.capture_pre_turn(&pre_record).await.unwrap();

        let outcome = TerminalOutcomeRecord {
            status: TurnTerminalStatus::Success,
            assistant_message: None,
            token_usage: None,
            attempts: 1,
            last_attempt_id: Some("turn_human_1-att-1"),
            retry_summaries: None,
            error_metadata: None,
            finished_at_ms: 200,
        };
        journal
            .reconcile_terminal("user_human", "conv_human", "turn_human_1", &outcome)
            .await
            .unwrap();

        let content = tokio::fs::read_to_string(&human_note).await.unwrap();
        assert_eq!(content, "# My Journal\nHuman content");
    }

    #[tokio::test]
    async fn test_in_memory_adapter_parity_and_canonical_isolation() {
        let journal = InMemoryTurnJournal::new();

        let pre_record_1 = PreTurnRecord {
            user_id: "user_mem",
            conversation_id: "conv_mem_1",
            turn_id: "turn_mem_1",
            parent_turn_id: Some("turn_mem_0"),
            user_message: "Mem test 1",
            workspace: Some("/root/test/path"),
            created_at_ms: 1000,
        };
        journal.capture_pre_turn(&pre_record_1).await.unwrap();

        let pre_record_2 = PreTurnRecord {
            user_id: "user_mem",
            conversation_id: "conv_mem_2",
            turn_id: "turn_mem_1",
            parent_turn_id: None,
            user_message: "Mem test 2",
            workspace: None,
            created_at_ms: 1000,
        };
        journal.capture_pre_turn(&pre_record_2).await.unwrap();

        let outcome = TerminalOutcomeRecord {
            status: TurnTerminalStatus::Success,
            assistant_message: None,
            token_usage: None,
            attempts: 1,
            last_attempt_id: Some("turn_mem_1-att-1"),
            retry_summaries: None,
            error_metadata: None,
            finished_at_ms: 1500,
        };
        journal
            .reconcile_terminal("user_mem", "conv_mem_1", "turn_mem_1", &outcome)
            .await
            .unwrap();

        let events_1 = journal.get_turn_events("user_mem", "conv_mem_1", "turn_mem_1").await;
        let events_2 = journal.get_turn_events("user_mem", "conv_mem_2", "turn_mem_1").await;

        assert_eq!(events_1.len(), 2);
        assert_eq!(events_2.len(), 1);
        if let RawJournalEvent::PreExecution { workspace, .. } = &events_1[0] {
            assert_eq!(workspace.as_deref(), Some("bound_workspace"));
        }
    }

    #[tokio::test]
    async fn test_capture_before_broadcast_and_fail_closed_compensation() {
        let temp = tempdir().unwrap();
        let journal = FilesystemTurnJournal::new(temp.path());

        // Invalid identifier must fail-closed before any side effects
        let bad_record = PreTurnRecord {
            user_id: "user/../evil",
            conversation_id: "conv_123",
            turn_id: "turn_001",
            parent_turn_id: None,
            user_message: "Failing capture",
            workspace: None,
            created_at_ms: 1000,
        };

        let err = journal.capture_pre_turn(&bad_record).await.unwrap_err();
        assert!(matches!(err, JournalError::InvalidIdentifier { .. }));

        assert!(!temp.path().join("users/user/../evil").exists());
        assert!(!temp.path().join("users/evil").exists());
    }

    #[tokio::test]
    async fn test_user_continuation_generates_new_turn_id_with_parent() {
        let temp = tempdir().unwrap();
        let journal = FilesystemTurnJournal::new(temp.path());

        // Initial Turn
        let turn1 = PreTurnRecord {
            user_id: "user_cont",
            conversation_id: "conv_cont",
            turn_id: "turn_101",
            parent_turn_id: None,
            user_message: "First question",
            workspace: None,
            created_at_ms: 1000,
        };
        journal.capture_pre_turn(&turn1).await.unwrap();

        let outcome1 = TerminalOutcomeRecord {
            status: TurnTerminalStatus::Success,
            assistant_message: None,
            token_usage: None,
            attempts: 1,
            last_attempt_id: Some("turn_101-att-1"),
            retry_summaries: None,
            error_metadata: None,
            finished_at_ms: 1500,
        };
        journal
            .reconcile_terminal("user_cont", "conv_cont", "turn_101", &outcome1)
            .await
            .unwrap();

        // Continuation Turn (New turn_id + parent_turn_id)
        let turn2 = PreTurnRecord {
            user_id: "user_cont",
            conversation_id: "conv_cont",
            turn_id: "turn_102",
            parent_turn_id: Some("turn_101"),
            user_message: "Follow-up question",
            workspace: None,
            created_at_ms: 2000,
        };
        journal.capture_pre_turn(&turn2).await.unwrap();

        let file1 = temp.path().join("users/user_cont/events/raw/conv_cont/turn_101.jsonl");
        let file2 = temp.path().join("users/user_cont/events/raw/conv_cont/turn_102.jsonl");

        assert!(file1.exists());
        assert!(file2.exists());

        let events2 = FilesystemTurnJournal::read_and_sanitize_turn_events(&file2).await.unwrap();
        assert_eq!(events2.len(), 1);
        match &events2[0] {
            RawJournalEvent::PreExecution { turn_id, parent_turn_id, .. } => {
                assert_eq!(turn_id, "turn_102");
                assert_eq!(parent_turn_id.as_deref(), Some("turn_101"));
            }
            _ => panic!("Expected PreExecution event"),
        }
    }

    #[tokio::test]
    async fn test_auto_replay_records_single_final_outcome_with_attempt_summaries() {
        let temp = tempdir().unwrap();
        let journal = FilesystemTurnJournal::new(temp.path());

        let pre = PreTurnRecord {
            user_id: "user_replay",
            conversation_id: "conv_replay",
            turn_id: "turn_replay_1",
            parent_turn_id: None,
            user_message: "Replay prompt",
            workspace: None,
            created_at_ms: 1000,
        };
        journal.capture_pre_turn(&pre).await.unwrap();

        // Replay finishes with single final outcome holding 2 attempts
        let outcome = TerminalOutcomeRecord {
            status: TurnTerminalStatus::Success,
            assistant_message: None,
            token_usage: None,
            attempts: 2,
            last_attempt_id: Some("turn_replay_1-att-2"),
            retry_summaries: Some(vec![
                AttemptSummary {
                    attempt_id: "turn_replay_1-att-1".to_string(),
                    error: Some("socket_closed".to_string()),
                    duration_ms: Some(1200),
                },
                AttemptSummary {
                    attempt_id: "turn_replay_1-att-2".to_string(),
                    error: None,
                    duration_ms: Some(850),
                },
            ]),
            error_metadata: Some(serde_json::json!({
                "assistant_message": "unavailable",
                "token_usage": "unavailable"
            })),
            finished_at_ms: 3100,
        };

        journal
            .reconcile_terminal("user_replay", "conv_replay", "turn_replay_1", &outcome)
            .await
            .unwrap();

        let file = temp.path().join("users/user_replay/events/raw/conv_replay/turn_replay_1.jsonl");
        let events = FilesystemTurnJournal::read_and_sanitize_turn_events(&file).await.unwrap();
        assert_eq!(events.len(), 2);
        match &events[1] {
            RawJournalEvent::FinalOutcome {
                status,
                attempts,
                last_attempt_id,
                retry_summaries,
                ..
            } => {
                assert_eq!(*status, TurnTerminalStatus::Success);
                assert_eq!(*attempts, 2);
                assert_eq!(last_attempt_id.as_deref(), Some("turn_replay_1-att-2"));
                let summaries = retry_summaries.as_ref().unwrap();
                assert_eq!(summaries.len(), 2);
                assert_eq!(summaries[0].attempt_id, "turn_replay_1-att-1");
                assert_eq!(summaries[0].error.as_deref(), Some("socket_closed"));
                assert_eq!(summaries[1].attempt_id, "turn_replay_1-att-2");
            }
            _ => panic!("Expected FinalOutcome event"),
        }
    }

    #[tokio::test]
    async fn test_deferred_cancel_and_build_failure_delegated_finalization() {
        let temp = tempdir().unwrap();
        let journal = FilesystemTurnJournal::new(temp.path());

        // 1. Build failure
        journal
            .capture_pre_turn(&PreTurnRecord {
                user_id: "user_fail",
                conversation_id: "conv_build",
                turn_id: "turn_build_1",
                parent_turn_id: None,
                user_message: "Build fail",
                workspace: None,
                created_at_ms: 1000,
            })
            .await
            .unwrap();

        journal
            .reconcile_terminal(
                "user_fail",
                "conv_build",
                "turn_build_1",
                &TerminalOutcomeRecord {
                    status: TurnTerminalStatus::Failed,
                    assistant_message: None,
                    token_usage: None,
                    attempts: 0,
                    last_attempt_id: None,
                    retry_summaries: None,
                    error_metadata: Some(serde_json::json!({
                        "stage": "worker_build",
                        "assistant_message": "unavailable",
                        "token_usage": "unavailable"
                    })),
                    finished_at_ms: 1050,
                },
            )
            .await
            .unwrap();

        // 2. Deferred cancel
        journal
            .capture_pre_turn(&PreTurnRecord {
                user_id: "user_fail",
                conversation_id: "conv_cancel",
                turn_id: "turn_cancel_1",
                parent_turn_id: None,
                user_message: "Cancel",
                workspace: None,
                created_at_ms: 2000,
            })
            .await
            .unwrap();

        journal
            .reconcile_terminal(
                "user_fail",
                "conv_cancel",
                "turn_cancel_1",
                &TerminalOutcomeRecord {
                    status: TurnTerminalStatus::Cancelled,
                    assistant_message: None,
                    token_usage: None,
                    attempts: 0,
                    last_attempt_id: None,
                    retry_summaries: None,
                    error_metadata: Some(serde_json::json!({
                        "stage": "pre_stream_cancelled",
                        "assistant_message": "unavailable",
                        "token_usage": "unavailable"
                    })),
                    finished_at_ms: 2050,
                },
            )
            .await
            .unwrap();

        let file_build = temp.path().join("users/user_fail/events/raw/conv_build/turn_build_1.jsonl");
        let file_cancel = temp.path().join("users/user_fail/events/raw/conv_cancel/turn_cancel_1.jsonl");

        let events_build = FilesystemTurnJournal::read_and_sanitize_turn_events(&file_build).await.unwrap();
        let events_cancel = FilesystemTurnJournal::read_and_sanitize_turn_events(&file_cancel).await.unwrap();

        assert_eq!(events_build.len(), 2);
        assert_eq!(events_cancel.len(), 2);
        assert!(matches!(events_build[1], RawJournalEvent::FinalOutcome { status: TurnTerminalStatus::Failed, .. }));
        assert!(matches!(events_cancel[1], RawJournalEvent::FinalOutcome { status: TurnTerminalStatus::Cancelled, .. }));
    }
}
