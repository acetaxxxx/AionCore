//! Safe capture of user evidence for the future Memory Curation pipeline.
//!
//! This module deliberately stops at the user-scoped Candidate Ledger. It does
//! not promote candidates into a curated vault or expose them to retrieval.

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::turn_journal::TurnTerminalStatus;

const MAX_CANDIDATE_CONTENT_CHARS: usize = 2_048;
const APPEND_RETRY_LIMIT: usize = 3;

/// The trust boundary used by candidate eligibility policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryEvidenceSource {
    Owner,
    CompliantAgent,
    Untrusted,
    System,
    Background,
}

/// Raw evidence handed to the high-level Memory Curation port.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryEvidence {
    pub user_id: String,
    pub conversation_id: String,
    pub turn_id: String,
    pub source_event_id: String,
    pub source_hash: String,
    pub user_message: String,
    pub assistant_message: Option<String>,
    pub status: TurnTerminalStatus,
    pub source: MemoryEvidenceSource,
    pub observed_at_ms: u64,
}

impl MemoryEvidence {
    /// Build provenance from the already recorded raw turn. The hash binds the
    /// source identity and content without copying raw content into the ledger.
    pub fn from_turn(
        user_id: &str,
        conversation_id: &str,
        turn_id: &str,
        user_message: &str,
        status: TurnTerminalStatus,
        source: MemoryEvidenceSource,
        observed_at_ms: u64,
    ) -> Self {
        // Raw TurnJournal events do not carry a separately allocated event
        // id, so this is the stable logical reference to the pre+terminal
        // pair. The canonical pair hash below binds the complete payload.
        let source_event_id = format!("{turn_id}:pre_execution+final_outcome");
        let mut source_material = String::new();
        for part in [
            user_id,
            conversation_id,
            turn_id,
            &source_event_id,
            user_message,
        ] {
            source_material.push_str(part);
            source_material.push('\0');
        }
        source_material.push_str(&serde_json::to_string(&status).expect("terminal status is serializable"));

        Self {
            user_id: user_id.to_owned(),
            conversation_id: conversation_id.to_owned(),
            turn_id: turn_id.to_owned(),
            source_event_id,
            source_hash: digest_hex(source_material.as_bytes()),
            user_message: user_message.to_owned(),
            assistant_message: None,
            status,
            source,
            observed_at_ms,
        }
    }

    /// Replace the fallback provenance hash with the canonical hash of the
    /// raw PreExecution + FinalOutcome events once terminal reconciliation has
    /// produced the complete payload.
    pub fn with_source_hash(mut self, source_hash: String) -> Self {
        self.source_hash = source_hash;
        self
    }
}

/// Candidate lifecycle. Ticket 01 intentionally ends at `Detected`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryCandidateStatus {
    Detected,
}

/// A redacted, user-scoped candidate and its auditable provenance.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryCandidate {
    pub candidate_id: String,
    pub status: MemoryCandidateStatus,
    pub user_id: String,
    pub conversation_id: String,
    pub turn_id: String,
    pub source_event_id: String,
    pub source_hash: String,
    pub fingerprint: String,
    pub content: String,
    pub detected_at_ms: u64,
}

/// Errors from candidate policy or Candidate Ledger persistence.
#[derive(Debug, thiserror::Error)]
pub enum MemoryCurationError {
    #[error("invalid candidate identity: {reason}")]
    InvalidIdentity { reason: String },

    #[error("candidate ledger IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("candidate ledger serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("conflicting candidate '{candidate_id}' for user '{user_id}'")]
    ConflictingCandidate { candidate_id: String, user_id: String },

    #[error("candidate ledger lock unavailable")]
    LockUnavailable,

    #[error("candidate capture task failed: {0}")]
    Internal(String),
}

/// Single high-level port used by conversation execution after terminal
/// durability. Ledger adapters remain behind this port.
#[async_trait]
pub trait MemoryCuration: Send + Sync {
    async fn capture_candidate(&self, evidence: &MemoryEvidence) -> Result<(), MemoryCurationError>;
}

/// A no-op default for isolated callers that do not install production
/// composition. Production app services install `FilesystemMemoryCuration`.
#[derive(Debug, Default)]
pub(crate) struct NoopMemoryCuration;

#[async_trait]
impl MemoryCuration for NoopMemoryCuration {
    async fn capture_candidate(&self, _evidence: &MemoryEvidence) -> Result<(), MemoryCurationError> {
        Ok(())
    }
}

/// In-memory adapter for contract tests and application-level test fixtures.
#[derive(Clone, Default)]
pub struct InMemoryMemoryCuration {
    candidates: Arc<Mutex<HashMap<String, MemoryCandidate>>>,
}

impl InMemoryMemoryCuration {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn candidates_for_user(&self, user_id: &str) -> Vec<MemoryCandidate> {
        self.candidates
            .lock()
            .map(|candidates| {
                candidates
                    .values()
                    .filter(|candidate| candidate.user_id == user_id)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn candidates(&self) -> Vec<MemoryCandidate> {
        self.candidates
            .lock()
            .map(|candidates| candidates.values().cloned().collect())
            .unwrap_or_default()
    }
}

#[async_trait]
impl MemoryCuration for InMemoryMemoryCuration {
    async fn capture_candidate(&self, evidence: &MemoryEvidence) -> Result<(), MemoryCurationError> {
        let Some(candidate) = candidate_from_evidence(evidence)? else {
            return Ok(());
        };
        let mut candidates = self.candidates.lock().map_err(|_| MemoryCurationError::LockUnavailable)?;
        if let Some(existing) = candidates.get(&candidate.candidate_id) {
            if existing == &candidate {
                return Ok(());
            }
            return Err(MemoryCurationError::ConflictingCandidate {
                candidate_id: candidate.candidate_id,
                user_id: candidate.user_id,
            });
        }
        candidates.insert(candidate.candidate_id.clone(), candidate);
        Ok(())
    }
}

/// Durable user-scoped Candidate Ledger adapter. It intentionally has no
/// public path-based read/write API beyond the supplied data root.
#[derive(Clone)]
pub struct FilesystemMemoryCuration {
    data_dir: PathBuf,
    locks: Arc<Mutex<HashMap<String, Arc<Mutex<()>>>>>,
}

impl FilesystemMemoryCuration {
    pub fn new(data_dir: PathBuf) -> Self {
        Self {
            data_dir,
            locks: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn candidates_for_user(&self, user_id: &str) -> Result<Vec<MemoryCandidate>, MemoryCurationError> {
        crate::turn_journal::validate_identifier(user_id, "user_id").map_err(|error| MemoryCurationError::InvalidIdentity {
            reason: error.to_string(),
        })?;
        read_candidates(&candidate_path(&self.data_dir, user_id))
    }

    fn user_lock(&self, user_id: &str) -> Result<Arc<Mutex<()>>, MemoryCurationError> {
        let mut locks = self.locks.lock().map_err(|_| MemoryCurationError::LockUnavailable)?;
        Ok(Arc::clone(
            locks
                .entry(user_id.to_owned())
                .or_insert_with(|| Arc::new(Mutex::new(()))),
        ))
    }
}

#[async_trait]
impl MemoryCuration for FilesystemMemoryCuration {
    async fn capture_candidate(&self, evidence: &MemoryEvidence) -> Result<(), MemoryCurationError> {
        let evidence = evidence.clone();
        let adapter = self.clone();
        tokio::task::spawn_blocking(move || adapter.capture_candidate_blocking(&evidence))
            .await
            .map_err(|error| MemoryCurationError::Internal(error.to_string()))?
    }
}

impl FilesystemMemoryCuration {
    fn capture_candidate_blocking(&self, evidence: &MemoryEvidence) -> Result<(), MemoryCurationError> {
        let Some(candidate) = candidate_from_evidence(evidence)? else {
            return Ok(());
        };
        let lock = self.user_lock(&candidate.user_id)?;
        let _guard = lock.lock().map_err(|_| MemoryCurationError::LockUnavailable)?;
        let path = candidate_path(&self.data_dir, &candidate.user_id);
        fs::create_dir_all(path.parent().expect("candidate path has a parent"))?;

        for attempt in 0..APPEND_RETRY_LIMIT {
            let existing = read_candidates(&path)?;
            if let Some(previous) = existing.iter().find(|item| item.candidate_id == candidate.candidate_id) {
                if previous == &candidate {
                    return Ok(());
                }
                return Err(MemoryCurationError::ConflictingCandidate {
                    candidate_id: candidate.candidate_id,
                    user_id: candidate.user_id,
                });
            }

            let write_result = append_candidate(&path, &candidate);
            match write_result {
                Ok(()) => return Ok(()),
                Err(_error) if attempt + 1 < APPEND_RETRY_LIMIT => {
                    // A write/flush/sync error may be an unknown commit. A
                    // complete line wins over retrying and duplicating it.
                    if let Ok(after_error) = read_candidates(&path)
                        && after_error.iter().any(|item| item == &candidate)
                    {
                        return Ok(());
                    }
                }
                Err(error) => return Err(error),
            }
        }
        unreachable!("candidate append retry loop always returns")
    }
}

fn evidence_identity<'a>(user_id: &'a str, conversation_id: &'a str, turn_id: &'a str, source_event_id: &'a str) -> [&'a str; 4] {
    [user_id, conversation_id, turn_id, source_event_id]
}

fn validate_identity(parts: [&str; 4]) -> Result<(), MemoryCurationError> {
    for (part, field) in parts.into_iter().zip(["user_id", "conversation_id", "turn_id", "source_event_id"]) {
        crate::turn_journal::validate_identifier(part, field).map_err(|error| MemoryCurationError::InvalidIdentity {
            reason: error.to_string(),
        })?;
    }
    Ok(())
}

fn candidate_from_evidence(evidence: &MemoryEvidence) -> Result<Option<MemoryCandidate>, MemoryCurationError> {
    validate_identity(evidence_identity(
        &evidence.user_id,
        &evidence.conversation_id,
        &evidence.turn_id,
        &evidence.source_event_id,
    ))?;

    if evidence.status != TurnTerminalStatus::Success
        || !matches!(evidence.source, MemoryEvidenceSource::Owner | MemoryEvidenceSource::CompliantAgent)
        || contains_secret(&evidence.user_message)
        || evidence
            .assistant_message
            .as_deref()
            .is_some_and(contains_secret)
    {
        return Ok(None);
    }

    let content = normalize_candidate_content(&evidence.user_message);
    if content.is_empty() {
        return Ok(None);
    }
    let fingerprint = digest_hex(content.as_bytes());
    let mut identity = String::new();
    for part in [&evidence.user_id, &evidence.source_event_id, &evidence.source_hash, &fingerprint] {
        identity.push_str(part);
        identity.push('\0');
    }

    Ok(Some(MemoryCandidate {
        candidate_id: digest_hex(identity.as_bytes()),
        status: MemoryCandidateStatus::Detected,
        user_id: evidence.user_id.clone(),
        conversation_id: evidence.conversation_id.clone(),
        turn_id: evidence.turn_id.clone(),
        source_event_id: evidence.source_event_id.clone(),
        source_hash: evidence.source_hash.clone(),
        fingerprint,
        content,
        detected_at_ms: evidence.observed_at_ms,
    }))
}

fn normalize_candidate_content(content: &str) -> String {
    content
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .chars()
        .take(MAX_CANDIDATE_CONTENT_CHARS)
        .collect()
}

fn contains_secret(content: &str) -> bool {
    let lower = content.to_ascii_lowercase();
    if (lower.contains("-----begin ") && lower.contains("private key-----"))
        || lower.contains("private key")
        || lower.contains("bearer ")
    {
        return true;
    }
    for marker in [
        "api_key",
        "api-key",
        "api key",
        "secret_key",
        "secret-key",
        "secret key",
        "password",
        "token",
        "cookie",
        "set-cookie",
    ] {
        let mut search_from = 0;
        while let Some(offset) = lower[search_from..].find(marker) {
            let start = search_from + offset + marker.len();
            let rest = lower[start..].trim_start();
            if rest.starts_with('=') || rest.starts_with(':') {
                let value = rest[1..].trim_start();
                if !value.is_empty() {
                    return true;
                }
            }
            search_from = start;
            if search_from >= lower.len() {
                break;
            }
        }
    }
    if content.split_whitespace().any(|word| {
        word.starts_with("sk-")
            || word.starts_with("ghp_")
            || word.starts_with("xoxb-")
            || word.starts_with("AKIA")
    }) {
        return true;
    }
    if lower.contains("mongodb://")
        || lower.contains("postgres://")
        || lower.contains("postgresql://")
        || lower.contains("mysql://")
        || lower.contains("redis://")
        || lower.contains("connection string")
    {
        return true;
    }
    // JWTs have three base64url sections and begin with the conventional
    // encoded JSON header prefix. Do not reject ordinary dotted prose.
    content.split_whitespace().any(|word| {
        let sections = word.split('.').collect::<Vec<_>>();
        word.starts_with("eyJ") && sections.len() == 3 && sections.iter().all(|section| !section.is_empty())
    })
}

fn candidate_path(data_dir: &Path, user_id: &str) -> PathBuf {
    data_dir
        .join("users")
        .join(user_id)
        .join("runtime")
        .join("memory")
        .join("candidates.jsonl")
}

fn read_candidates(path: &Path) -> Result<Vec<MemoryCandidate>, MemoryCurationError> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let file = File::open(path)?;
    BufReader::new(file)
        .lines()
        .map(|line| {
            let line = line?;
            Ok(serde_json::from_str(&line)?)
        })
        .collect()
}

fn append_candidate(path: &Path, candidate: &MemoryCandidate) -> Result<(), MemoryCurationError> {
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    serde_json::to_writer(&mut file, candidate)?;
    file.write_all(b"\n")?;
    file.flush()?;
    file.sync_all()?;
    Ok(())
}

fn digest_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn evidence(user_id: &str, source: MemoryEvidenceSource, status: TurnTerminalStatus, content: &str) -> MemoryEvidence {
        MemoryEvidence::from_turn(user_id, "conversation", "turn", content, status, source, 1)
    }

    #[tokio::test]
    async fn eligible_success_starts_detected_candidate() {
        let curation = InMemoryMemoryCuration::new();
        curation.capture_candidate(&evidence("alice", MemoryEvidenceSource::Owner, TurnTerminalStatus::Success, "I prefer concise replies")).await.unwrap();
        let candidates = curation.candidates_for_user("alice");
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].status, MemoryCandidateStatus::Detected);
        assert_eq!(candidates[0].content, "I prefer concise replies");
    }

    #[tokio::test]
    async fn ineligible_and_secret_evidence_stays_raw_only() {
        let curation = InMemoryMemoryCuration::new();
        for (source, status, content) in [
            (MemoryEvidenceSource::Owner, TurnTerminalStatus::Failed, "failure"),
            (MemoryEvidenceSource::Owner, TurnTerminalStatus::Cancelled, "cancelled"),
            (MemoryEvidenceSource::Owner, TurnTerminalStatus::Timeout, "timed out"),
            (MemoryEvidenceSource::Untrusted, TurnTerminalStatus::Success, "untrusted"),
            (MemoryEvidenceSource::Background, TurnTerminalStatus::Success, "background"),
            (MemoryEvidenceSource::Owner, TurnTerminalStatus::Success, "api_key=secret-value"),
        ] {
            curation.capture_candidate(&evidence("alice", source, status, content)).await.unwrap();
        }
        assert!(curation.candidates().is_empty());
    }

    #[test]
    fn secret_boundary_covers_common_transport_and_token_forms() {
        for secret in [
            "api key: value",
            "cookie=session-value",
            "connection string: postgres://user:pass@example.invalid/db",
            "-----BEGIN PRIVATE KEY-----\nsecret\n-----END PRIVATE KEY-----",
            "jwt eyJheader.payload.signature",
            "Authorization: Bearer opaque-token",
        ] {
            assert!(contains_secret(secret), "secret form was not rejected: {secret}");
        }
    }

    #[tokio::test]
    async fn retries_are_idempotent_and_user_scoped() {
        let curation = InMemoryMemoryCuration::new();
        let alice = evidence("alice", MemoryEvidenceSource::CompliantAgent, TurnTerminalStatus::Success, "remember this");
        curation.capture_candidate(&alice).await.unwrap();
        curation.capture_candidate(&alice).await.unwrap();
        let bob = evidence("bob", MemoryEvidenceSource::CompliantAgent, TurnTerminalStatus::Success, "remember this");
        curation.capture_candidate(&bob).await.unwrap();
        assert_eq!(curation.candidates_for_user("alice").len(), 1);
        assert_eq!(curation.candidates_for_user("bob").len(), 1);
        assert_ne!(curation.candidates_for_user("alice")[0].candidate_id, curation.candidates_for_user("bob")[0].candidate_id);
    }

    #[tokio::test]
    async fn filesystem_ledger_is_durable_and_idempotent_per_user() {
        let temp = tempfile::tempdir().unwrap();
        let curation = FilesystemMemoryCuration::new(temp.path().to_path_buf());
        let item = evidence("alice", MemoryEvidenceSource::Owner, TurnTerminalStatus::Success, "remember this");
        curation.capture_candidate(&item).await.unwrap();
        curation.capture_candidate(&item).await.unwrap();
        assert_eq!(curation.candidates_for_user("alice").unwrap().len(), 1);
        assert!(curation.candidates_for_user("bob").unwrap().is_empty());
    }
}
