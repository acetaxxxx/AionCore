use aionui_common::{TerminalNoticeStatus, TerminalTargetKind, TurnTerminalNotice};
use serde::Serialize;

pub const PUSH_PAYLOAD_SCHEMA_VERSION: u8 = 1;
const MAX_TITLE_BYTES: usize = 80;
const MAX_BODY_BYTES: usize = 160;

/// Bounded, allowlisted push transport payload. It is intentionally separate
/// from `TurnTerminalNotice`: user identity and lifecycle internals stay on
/// the server side and are never sent to a browser push provider.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PushPayload {
    pub schema_version: u8,
    pub status: String,
    pub title: String,
    pub body: String,
    pub target_kind: String,
    pub target_id: String,
}

pub fn build_terminal_payload(notice: &TurnTerminalNotice) -> Result<PushPayload, &'static str> {
    let (title, body) = match notice.status {
        TerminalNoticeStatus::Success => ("Aion turn completed", "Your task has finished."),
        TerminalNoticeStatus::Failed => ("Aion turn needs attention", "Your task ended with an error."),
        TerminalNoticeStatus::Cancelled => ("Aion turn cancelled", "The task was cancelled before completion."),
        TerminalNoticeStatus::Timeout => ("Aion turn timed out", "The task did not finish in time."),
    };
    let (status, target_kind) = match (notice.status, notice.target_kind) {
        (TerminalNoticeStatus::Success, target) => ("success", target),
        (TerminalNoticeStatus::Failed, target) => ("failed", target),
        (TerminalNoticeStatus::Cancelled, target) => ("cancelled", target),
        (TerminalNoticeStatus::Timeout, target) => ("timeout", target),
    };
    let target_kind = match target_kind {
        TerminalTargetKind::Team => "team",
        TerminalTargetKind::Conversation => "conversation",
    };
    if notice.target_id.is_empty()
        || notice.target_id.len() > 128
        || !notice
            .target_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
        || title.len() > MAX_TITLE_BYTES
        || body.len() > MAX_BODY_BYTES
    {
        return Err("invalid terminal push payload");
    }
    Ok(PushPayload {
        schema_version: PUSH_PAYLOAD_SCHEMA_VERSION,
        status: status.to_owned(),
        title: title.to_owned(),
        body: body.to_owned(),
        target_kind: target_kind.to_owned(),
        target_id: notice.target_id.clone(),
    })
}
