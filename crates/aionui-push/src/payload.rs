use aionui_common::{TerminalNoticeStatus, TerminalTargetKind, TurnTerminalNotice};
use serde::Serialize;

pub const PUSH_PAYLOAD_SCHEMA_VERSION: u8 = 1;
const MAX_TITLE_CHARS: usize = 30;
const MAX_BODY_CHARS: usize = 50;
const MAX_TOTAL_COPY_CHARS: usize = 80;
// The longest localized title prefix plus its separator occupies 12 chars.
const MAX_TARGET_TITLE_CHARS: usize = 18;

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
    let target_title = sanitize_target_title(notice.target_title.as_deref());
    let (title_prefix, body_suffix) = match notice.status {
        TerminalNoticeStatus::Success => ("Aion 任務已完成", "已完成。"),
        TerminalNoticeStatus::Failed => ("Aion 任務需要處理", "執行失敗，請查看詳情。"),
        TerminalNoticeStatus::Cancelled => ("Aion 任務已取消", "在完成前已取消。"),
        TerminalNoticeStatus::Timeout => ("Aion 任務逾時", "未在時限內完成。"),
    };
    let (title, body) = match target_title.as_deref() {
        Some(target_title) => (
            format!("{title_prefix}：{target_title}"),
            format!("「{target_title}」{body_suffix}"),
        ),
        None => (title_prefix.to_owned(), format!("這項任務{body_suffix}")),
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
        || title.chars().count() > MAX_TITLE_CHARS
        || body.chars().count() > MAX_BODY_CHARS
        || title.chars().count() + body.chars().count() > MAX_TOTAL_COPY_CHARS
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

/// Normalize a user- or agent-authored entity title before it is included in
/// a push payload. Control characters and line breaks are removed, runs of
/// whitespace become one space, and the result is capped by Unicode scalar
/// values rather than UTF-8 bytes so Traditional Chinese remains intact.
/// URL- and secret-like input is rejected in full instead of being truncated.
fn sanitize_target_title(value: Option<&str>) -> Option<String> {
    let value = value.unwrap_or_default();
    if contains_sensitive_reference(value) {
        return None;
    }

    let mut normalized = String::new();
    let mut pending_space = false;
    let mut chars = 0;

    for character in value.chars() {
        if character.is_control() {
            continue;
        }
        if character.is_whitespace() {
            pending_space = !normalized.is_empty();
            continue;
        }
        if pending_space {
            normalized.push(' ');
            pending_space = false;
        }
        normalized.push(character);
        chars += 1;
        if chars == MAX_TARGET_TITLE_CHARS {
            break;
        }
    }

    let normalized = normalized.trim().to_owned();
    (!normalized.is_empty()).then_some(normalized)
}

fn contains_sensitive_reference(value: &str) -> bool {
    let lower = value.to_ascii_lowercase();
    let compact: String = lower
        .chars()
        .filter(|character| !character.is_whitespace() && !character.is_control())
        .collect();
    const BLOCKED_MARKERS: [&str; 11] = [
        "http://",
        "https://",
        "www.",
        "token=",
        "bearer",
        "api_key=",
        "apikey=",
        "access_token=",
        "secret=",
        "password=",
        "sk-",
    ];

    BLOCKED_MARKERS
        .iter()
        .any(|marker| compact.contains(marker))
        || value
            .split(|character: char| {
                !(character.is_ascii_alphanumeric()
                    || matches!(character, '.' | '_' | '-' | '='))
            })
            .any(looks_like_url_or_secret)
}

fn looks_like_url_or_secret(value: &str) -> bool {
    let candidate = value.trim_matches(|character: char| {
        matches!(character, '"' | '\'' | '(' | ')' | '[' | ']' | '{' | '}' | ',' | ';')
    });
    let host = candidate
        .split(['/', ':', '?', '#'])
        .next()
        .unwrap_or_default();
    let looks_like_domain = host.rsplit_once('.').is_some_and(|(prefix, suffix)| {
        !prefix.is_empty()
            && (2..=24).contains(&suffix.len())
            && suffix.bytes().all(|byte| byte.is_ascii_alphabetic())
            && prefix
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-'))
    });
    let looks_like_opaque_secret = candidate.len() >= 32
        && candidate
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
        && candidate.bytes().any(|byte| byte.is_ascii_alphabetic())
        && candidate.bytes().any(|byte| byte.is_ascii_digit());
    let mut jwt_segments = candidate.split('.');
    let looks_like_jwt = jwt_segments
        .next()
        .is_some_and(|segment| segment.len() >= 8 && is_base64url(segment))
        && jwt_segments
            .next()
            .is_some_and(|segment| segment.len() >= 8 && is_base64url(segment))
        && jwt_segments
            .next()
            .is_some_and(|segment| segment.len() >= 8 && is_base64url(segment))
        && jwt_segments.next().is_none();

    looks_like_domain || looks_like_opaque_secret || looks_like_jwt
}

fn is_base64url(value: &str) -> bool {
    value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'='))
}
