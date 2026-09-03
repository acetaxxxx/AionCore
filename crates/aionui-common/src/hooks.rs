//! Cross-crate lifecycle hook traits.
//!
//! Hooks defined here let lower-layer crates (e.g. `aionui-ai-agent`,
//! `aionui-cron`) react to events owned by higher-layer crates (e.g.
//! `aionui-conversation`) without forming a dependency cycle.

use async_trait::async_trait;

const MAX_TERMINAL_NOTICE_ID_BYTES: usize = 128;

/// The only navigation targets that may be embedded in a terminal push
/// notification.  A complete URL is deliberately not a valid target.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TerminalTargetKind {
    Team,
    Conversation,
}

/// The terminal states that are safe to expose as a user-facing reminder.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TerminalNoticeStatus {
    Success,
    Failed,
    Cancelled,
    Timeout,
}

/// Immutable, identity-bound terminal outcome metadata shared across the
/// conversation domain and optional delivery adapters.
///
/// This value intentionally contains no prompt, assistant output, provider
/// error, token, endpoint, or user-supplied URL.  The `user_id` is retained
/// only for the trusted repository lookup; delivery payloads must omit it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnTerminalNotice {
    pub user_id: String,
    pub target_kind: TerminalTargetKind,
    pub target_id: String,
    /// Best-effort display title for the notification target. This is kept on
    /// the trusted server-side notice so delivery adapters can build useful,
    /// bounded copy without reading conversation storage or sending content.
    pub target_title: Option<String>,
    pub turn_id: String,
    pub status: TerminalNoticeStatus,
    pub finished_at_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum TerminalNoticeError {
    #[error("terminal notice {0} must not be empty")]
    EmptyField(&'static str),
    #[error("terminal notice {0} contains an invalid route identifier")]
    InvalidRouteIdentifier(&'static str),
}

impl TurnTerminalNotice {
    pub fn new(
        user_id: &str,
        target_kind: TerminalTargetKind,
        target_id: &str,
        turn_id: &str,
        status: TerminalNoticeStatus,
        finished_at_ms: u64,
    ) -> Result<Self, TerminalNoticeError> {
        if user_id.trim().is_empty() {
            return Err(TerminalNoticeError::EmptyField("user_id"));
        }
        if target_id.is_empty() {
            return Err(TerminalNoticeError::EmptyField("target_id"));
        }
        if turn_id.is_empty() {
            return Err(TerminalNoticeError::EmptyField("turn_id"));
        }
        for (field, value) in [("target_id", target_id), ("turn_id", turn_id)] {
            if value.len() > MAX_TERMINAL_NOTICE_ID_BYTES
                || !value
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
            {
                return Err(TerminalNoticeError::InvalidRouteIdentifier(field));
            }
        }
        Ok(Self {
            user_id: user_id.trim().to_owned(),
            target_kind,
            target_id: target_id.to_owned(),
            target_title: None,
            turn_id: turn_id.to_owned(),
            status,
            finished_at_ms,
        })
    }

    /// Attach the trusted display title resolved by the conversation boundary.
    /// The PushDelivery adapter remains responsible for sanitizing and
    /// bounding this value before it reaches a browser provider.
    pub fn with_target_title(mut self, target_title: impl Into<String>) -> Self {
        self.target_title = Some(target_title.into());
        self
    }
}

/// Receives a newly durable conversation terminal outcome.
///
/// Implementations must be best effort: hook failures must not alter the
/// already-committed conversation result.  Delivery adapters should enqueue
/// or spawn their work so the terminal path is never network-bound.
#[async_trait]
pub trait OnConversationTurnTerminal: Send + Sync {
    async fn on_turn_terminal(&self, notice: TurnTerminalNotice);
}

/// Notified before a conversation row is deleted via
/// `ConversationService::delete`.
///
/// Implementors are responsible for cleaning up their per-conversation state
/// (kill agent processes, drop cron job state, etc.). Hooks run sequentially
/// in registration order; failures must be logged inside the hook and not
/// propagated.
#[async_trait]
pub trait OnConversationDelete: Send + Sync {
    async fn on_conversation_deleted(&self, user_id: &str, conversation_id: &str);
}

/// Why a turn was cancelled.
///
/// Passed to [`OnConversationTurnCancelled`] because "the user pressed stop" and
/// "we are recycling a wedged agent process" want opposite treatment, and a hook
/// cannot tell them apart from the conversation id alone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TurnCancelCause {
    /// A user-initiated stop — the cancel route, or a runtime-facing adapter
    /// standing in for it.
    UserRequested,
    /// `restart_runtime` cancelling the active turn as a precondition for
    /// killing and rebuilding the agent process. NOT a request to abandon work
    /// aimed at this conversation: the user wants the conversation working
    /// again, and the restart leaves it idle — precisely the state a pending
    /// delivery has been waiting for.
    RuntimeRestart,
}

/// Notified when a conversation's turn was actually cancelled via
/// `ConversationService::cancel`.
///
/// Exists so an upper-layer crate (`aionui-session-message`) can drop the
/// pending deliveries aimed at that conversation without
/// `aionui-conversation` depending upwards. Without it, "stop" is a lie: the
/// user cancels A's turn, and a second later the drainer delivers B's queued
/// message to A, which starts a new turn — whack-a-mole the user cannot win.
///
/// Only fired on the branches where a cancel really took effect. A cancel whose
/// `turn_id` did not match the active turn cancelled nothing, and must NOT
/// clear the queue — doing so would silently drop messages, which is the worst
/// failure mode this feature has. Implementors must also honour `cause`: see
/// [`TurnCancelCause::RuntimeRestart`].
///
/// Hooks run sequentially in registration order; failures must be logged
/// inside the hook and not propagated.
#[async_trait]
pub trait OnConversationTurnCancelled: Send + Sync {
    async fn on_turn_cancelled(&self, user_id: &str, conversation_id: &str, turn_id: &str, cause: TurnCancelCause);
}
