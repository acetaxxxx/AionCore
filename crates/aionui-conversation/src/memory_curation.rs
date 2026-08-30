//! Safe capture of user evidence for the future Memory Curation pipeline.
//!
//! This module owns the user-scoped Candidate Ledger and the bounded promotion
//! boundary into curated Markdown. It exposes only compact `auto_inject`
//! records; raw events and full transcripts never cross this boundary.

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
const MAX_AUTO_INJECT_CHARS: usize = 4_096;

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

fn default_evidence_source() -> MemoryEvidenceSource {
    MemoryEvidenceSource::Owner
}

fn default_memory_scope() -> String {
    "auto_inject".to_owned()
}

fn default_privacy_classification() -> String {
    "private".to_owned()
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
        for part in [user_id, conversation_id, turn_id, &source_event_id, user_message] {
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

/// Candidate lifecycle. Ticket 02 adds explicit promotion and recovery states
/// while preserving `Detected` as the capture boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryCandidateStatus {
    Detected,
    Eligible,
    Proposed,
    Promoting,
    Promoted,
    Rejected,
    Quarantined,
    Superseded,
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
    #[serde(default = "default_evidence_source")]
    pub source: MemoryEvidenceSource,
    #[serde(default = "default_memory_scope")]
    pub scope: String,
    #[serde(default = "default_privacy_classification")]
    pub privacy_classification: String,
    #[serde(default)]
    pub promoted_at_ms: Option<u64>,
    /// Human edits replace the curated representation without changing the
    /// source evidence or its fingerprint.
    #[serde(default)]
    pub curated_content: Option<String>,
    /// Once a user edits a record, agent promotion must not overwrite it.
    #[serde(default)]
    pub human_authored: bool,
    /// Append-only decisions make review and recovery auditable while the
    /// source candidate remains intact.
    #[serde(default)]
    pub lifecycle_events: Vec<MemoryCandidateLifecycleEvent>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryCandidateLifecycleEvent {
    pub status: MemoryCandidateStatus,
    pub at_ms: u64,
    #[serde(default)]
    pub reason: Option<String>,
}

/// A user-scoped view of one curated record and its provenance. The raw
/// journal is never embedded here; source references and hash are sufficient
/// for an authorized caller to inspect the origin separately.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryRecord {
    pub candidate_id: String,
    pub user_id: String,
    pub conversation_id: String,
    pub turn_id: String,
    pub source_event_id: String,
    pub source_hash: String,
    pub fingerprint: String,
    pub status: MemoryCandidateStatus,
    pub content: String,
    pub scope: String,
    pub privacy_classification: String,
    pub markdown: String,
    pub human_authored: bool,
    pub lifecycle_events: Vec<MemoryCandidateLifecycleEvent>,
}

/// A compact, already-promoted memory item safe for the `auto_inject` scope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentMemory {
    pub candidate_id: String,
    pub content: String,
    pub source_event_id: String,
    pub source_hash: String,
    pub scope: String,
    pub privacy_classification: String,
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

    #[error("candidate '{candidate_id}' is not eligible for promotion")]
    PromotionNotEligible { candidate_id: String },

    #[error("invalid memory content")]
    InvalidContent,

    #[error("memory curation operation is unavailable")]
    Unsupported,
}

/// Single high-level port used by conversation execution after terminal
/// durability. Ledger adapters remain behind this port.
#[async_trait]
pub trait MemoryCuration: Send + Sync {
    async fn capture_candidate(&self, evidence: &MemoryEvidence) -> Result<(), MemoryCurationError>;

    async fn promote_candidate(
        &self,
        _user_id: &str,
        _candidate_id: &str,
    ) -> Result<MemoryCandidate, MemoryCurationError> {
        Err(MemoryCurationError::Unsupported)
    }

    async fn inspect_candidate(
        &self,
        _user_id: &str,
        _candidate_id: &str,
    ) -> Result<MemoryCandidate, MemoryCurationError> {
        Err(MemoryCurationError::Unsupported)
    }

    async fn inspect_memory(
        &self,
        _user_id: &str,
        _candidate_id: &str,
    ) -> Result<MemoryRecord, MemoryCurationError> {
        Err(MemoryCurationError::Unsupported)
    }

    async fn accept_candidate(
        &self,
        user_id: &str,
        candidate_id: &str,
    ) -> Result<MemoryRecord, MemoryCurationError> {
        let candidate = self.promote_candidate(user_id, candidate_id).await?;
        Ok(memory_record_from_candidate(candidate, String::new()))
    }

    async fn reject_candidate(
        &self,
        _user_id: &str,
        _candidate_id: &str,
    ) -> Result<MemoryCandidate, MemoryCurationError> {
        Err(MemoryCurationError::Unsupported)
    }

    async fn edit_memory(
        &self,
        _user_id: &str,
        _candidate_id: &str,
        _content: &str,
    ) -> Result<MemoryRecord, MemoryCurationError> {
        Err(MemoryCurationError::Unsupported)
    }

    async fn remove_memory(
        &self,
        _user_id: &str,
        _candidate_id: &str,
    ) -> Result<MemoryCandidate, MemoryCurationError> {
        Err(MemoryCurationError::Unsupported)
    }

    async fn auto_inject(&self, _user_id: &str, _max_chars: usize) -> Result<Vec<AgentMemory>, MemoryCurationError> {
        Ok(Vec::new())
    }
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
        let mut candidate = candidate;
        if candidate.source == MemoryEvidenceSource::CompliantAgent {
            let detected_at_ms = candidate.detected_at_ms;
            candidate.status = MemoryCandidateStatus::Promoted;
            candidate.promoted_at_ms = Some(detected_at_ms);
            append_lifecycle_event(
                &mut candidate,
                MemoryCandidateStatus::Promoted,
                detected_at_ms,
                "trusted_auto_promote",
            );
        }
        let mut candidates = self
            .candidates
            .lock()
            .map_err(|_| MemoryCurationError::LockUnavailable)?;
        if let Some(existing) = candidates.get(&candidate.candidate_id) {
            if same_immutable_candidate(existing, &candidate) {
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

    async fn promote_candidate(
        &self,
        user_id: &str,
        candidate_id: &str,
    ) -> Result<MemoryCandidate, MemoryCurationError> {
        let mut candidates = self
            .candidates
            .lock()
            .map_err(|_| MemoryCurationError::LockUnavailable)?;
        let candidate = candidates
            .get_mut(candidate_id)
            .filter(|candidate| candidate.user_id == user_id)
            .ok_or_else(|| MemoryCurationError::PromotionNotEligible {
                candidate_id: candidate_id.to_owned(),
            })?;
        if !matches!(
            candidate.status,
            MemoryCandidateStatus::Detected
                | MemoryCandidateStatus::Eligible
                | MemoryCandidateStatus::Proposed
                | MemoryCandidateStatus::Promoting
                | MemoryCandidateStatus::Promoted
        ) || candidate.content.is_empty()
            || candidate.content.chars().count() > MAX_CANDIDATE_CONTENT_CHARS
            || candidate.privacy_classification != "private"
            || candidate.scope != "auto_inject"
        {
            return Err(MemoryCurationError::PromotionNotEligible {
                candidate_id: candidate_id.to_owned(),
            });
        }
        candidate.status = MemoryCandidateStatus::Promoted;
        candidate.promoted_at_ms.get_or_insert(candidate.detected_at_ms);
        let promoted_at_ms = candidate.promoted_at_ms.unwrap_or(candidate.detected_at_ms);
        append_lifecycle_event(
            candidate,
            MemoryCandidateStatus::Promoted,
            promoted_at_ms,
            "accepted",
        );
        Ok(candidate.clone())
    }

    async fn inspect_candidate(
        &self,
        user_id: &str,
        candidate_id: &str,
    ) -> Result<MemoryCandidate, MemoryCurationError> {
        self.candidates
            .lock()
            .map_err(|_| MemoryCurationError::LockUnavailable)?
            .get(candidate_id)
            .filter(|candidate| candidate.user_id == user_id)
            .cloned()
            .ok_or_else(|| MemoryCurationError::PromotionNotEligible {
                candidate_id: candidate_id.to_owned(),
            })
    }

    async fn inspect_memory(
        &self,
        user_id: &str,
        candidate_id: &str,
    ) -> Result<MemoryRecord, MemoryCurationError> {
        let candidate = self.inspect_candidate(user_id, candidate_id).await?;
        if candidate.status != MemoryCandidateStatus::Promoted {
            return Err(MemoryCurationError::PromotionNotEligible {
                candidate_id: candidate_id.to_owned(),
            });
        }
        let markdown = render_memory_markdown(&candidate);
        Ok(memory_record_from_candidate(candidate, markdown))
    }

    async fn accept_candidate(
        &self,
        user_id: &str,
        candidate_id: &str,
    ) -> Result<MemoryRecord, MemoryCurationError> {
        let candidate = self.promote_candidate(user_id, candidate_id).await?;
        let markdown = render_memory_markdown(&candidate);
        Ok(memory_record_from_candidate(candidate, markdown))
    }

    async fn reject_candidate(
        &self,
        user_id: &str,
        candidate_id: &str,
    ) -> Result<MemoryCandidate, MemoryCurationError> {
        let mut candidates = self
            .candidates
            .lock()
            .map_err(|_| MemoryCurationError::LockUnavailable)?;
        let candidate = candidates
            .get_mut(candidate_id)
            .filter(|candidate| candidate.user_id == user_id)
            .ok_or_else(|| MemoryCurationError::PromotionNotEligible {
                candidate_id: candidate_id.to_owned(),
            })?;
        if matches!(candidate.status, MemoryCandidateStatus::Promoted | MemoryCandidateStatus::Superseded) {
            return Err(MemoryCurationError::PromotionNotEligible {
                candidate_id: candidate_id.to_owned(),
            });
        }
        candidate.status = MemoryCandidateStatus::Rejected;
        let detected_at_ms = candidate.detected_at_ms;
        append_lifecycle_event(candidate, MemoryCandidateStatus::Rejected, detected_at_ms, "human_rejected");
        Ok(candidate.clone())
    }

    async fn edit_memory(
        &self,
        user_id: &str,
        candidate_id: &str,
        content: &str,
    ) -> Result<MemoryRecord, MemoryCurationError> {
        let content = normalize_candidate_content(content);
        if content.is_empty() || content.chars().count() > MAX_CANDIDATE_CONTENT_CHARS || contains_secret(&content) {
            return Err(MemoryCurationError::InvalidContent);
        }
        let mut candidates = self
            .candidates
            .lock()
            .map_err(|_| MemoryCurationError::LockUnavailable)?;
        let candidate = candidates
            .get_mut(candidate_id)
            .filter(|candidate| candidate.user_id == user_id)
            .ok_or_else(|| MemoryCurationError::PromotionNotEligible {
                candidate_id: candidate_id.to_owned(),
            })?;
        if candidate.status != MemoryCandidateStatus::Promoted {
            return Err(MemoryCurationError::PromotionNotEligible {
                candidate_id: candidate_id.to_owned(),
            });
        }
        candidate.curated_content = Some(content);
        candidate.human_authored = true;
        let detected_at_ms = candidate.detected_at_ms;
        append_lifecycle_event(candidate, MemoryCandidateStatus::Promoted, detected_at_ms, "human_edited");
        let candidate = candidate.clone();
        let markdown = render_human_memory_markdown(&candidate);
        Ok(memory_record_from_candidate(candidate, markdown))
    }

    async fn remove_memory(
        &self,
        user_id: &str,
        candidate_id: &str,
    ) -> Result<MemoryCandidate, MemoryCurationError> {
        let mut candidates = self
            .candidates
            .lock()
            .map_err(|_| MemoryCurationError::LockUnavailable)?;
        let candidate = candidates
            .get_mut(candidate_id)
            .filter(|candidate| candidate.user_id == user_id)
            .ok_or_else(|| MemoryCurationError::PromotionNotEligible {
                candidate_id: candidate_id.to_owned(),
            })?;
        if candidate.status != MemoryCandidateStatus::Promoted {
            return Err(MemoryCurationError::PromotionNotEligible {
                candidate_id: candidate_id.to_owned(),
            });
        }
        candidate.status = MemoryCandidateStatus::Superseded;
        let detected_at_ms = candidate.detected_at_ms;
        append_lifecycle_event(candidate, MemoryCandidateStatus::Superseded, detected_at_ms, "human_removed");
        Ok(candidate.clone())
    }

    async fn auto_inject(&self, user_id: &str, max_chars: usize) -> Result<Vec<AgentMemory>, MemoryCurationError> {
        let budget = max_chars.min(MAX_AUTO_INJECT_CHARS);
        let mut items: Vec<_> = self
            .candidates_for_user(user_id)
            .into_iter()
            .filter(|candidate| {
                candidate.status == MemoryCandidateStatus::Promoted
                    && candidate.scope == "auto_inject"
                    && candidate.privacy_classification == "private"
            })
            .map(|candidate| AgentMemory {
                candidate_id: candidate.candidate_id,
                content: candidate.curated_content.unwrap_or(candidate.content),
                source_event_id: candidate.source_event_id,
                source_hash: candidate.source_hash,
                scope: candidate.scope,
                privacy_classification: candidate.privacy_classification,
            })
            .collect();
        items.sort_by(|left, right| left.candidate_id.cmp(&right.candidate_id));
        let mut used: usize = 0;
        items.retain(|item| {
            let size = item.content.chars().count();
            let keep = used.saturating_add(size) <= budget;
            if keep {
                used += size;
            }
            keep
        });
        Ok(items)
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
        crate::turn_journal::validate_identifier(user_id, "user_id").map_err(|error| {
            MemoryCurationError::InvalidIdentity {
                reason: error.to_string(),
            }
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
        let candidate = candidate_from_evidence(evidence)?;
        let evidence = evidence.clone();
        let adapter = self.clone();
        tokio::task::spawn_blocking(move || adapter.capture_candidate_blocking(&evidence))
            .await
            .map_err(|error| MemoryCurationError::Internal(error.to_string()))??;
        if let Some(candidate) = candidate.filter(|candidate| candidate.source == MemoryEvidenceSource::CompliantAgent)
        {
            self.promote_candidate(&candidate.user_id, &candidate.candidate_id)
                .await?;
        }
        Ok(())
    }

    async fn promote_candidate(
        &self,
        user_id: &str,
        candidate_id: &str,
    ) -> Result<MemoryCandidate, MemoryCurationError> {
        let adapter = self.clone();
        let user_id = user_id.to_owned();
        let candidate_id = candidate_id.to_owned();
        tokio::task::spawn_blocking(move || adapter.promote_candidate_blocking(&user_id, &candidate_id))
            .await
            .map_err(|error| MemoryCurationError::Internal(error.to_string()))?
    }

    async fn inspect_candidate(
        &self,
        user_id: &str,
        candidate_id: &str,
    ) -> Result<MemoryCandidate, MemoryCurationError> {
        let adapter = self.clone();
        let user_id = user_id.to_owned();
        let candidate_id = candidate_id.to_owned();
        tokio::task::spawn_blocking(move || adapter.inspect_candidate_blocking(&user_id, &candidate_id))
            .await
            .map_err(|error| MemoryCurationError::Internal(error.to_string()))?
    }

    async fn inspect_memory(
        &self,
        user_id: &str,
        candidate_id: &str,
    ) -> Result<MemoryRecord, MemoryCurationError> {
        let adapter = self.clone();
        let user_id = user_id.to_owned();
        let candidate_id = candidate_id.to_owned();
        tokio::task::spawn_blocking(move || adapter.inspect_memory_blocking(&user_id, &candidate_id))
            .await
            .map_err(|error| MemoryCurationError::Internal(error.to_string()))?
    }

    async fn accept_candidate(
        &self,
        user_id: &str,
        candidate_id: &str,
    ) -> Result<MemoryRecord, MemoryCurationError> {
        self.promote_candidate(user_id, candidate_id).await?;
        self.inspect_memory(user_id, candidate_id).await
    }

    async fn reject_candidate(
        &self,
        user_id: &str,
        candidate_id: &str,
    ) -> Result<MemoryCandidate, MemoryCurationError> {
        let adapter = self.clone();
        let user_id = user_id.to_owned();
        let candidate_id = candidate_id.to_owned();
        tokio::task::spawn_blocking(move || adapter.reject_candidate_blocking(&user_id, &candidate_id))
            .await
            .map_err(|error| MemoryCurationError::Internal(error.to_string()))?
    }

    async fn edit_memory(
        &self,
        user_id: &str,
        candidate_id: &str,
        content: &str,
    ) -> Result<MemoryRecord, MemoryCurationError> {
        let adapter = self.clone();
        let user_id = user_id.to_owned();
        let candidate_id = candidate_id.to_owned();
        let content = content.to_owned();
        tokio::task::spawn_blocking(move || adapter.edit_memory_blocking(&user_id, &candidate_id, &content))
            .await
            .map_err(|error| MemoryCurationError::Internal(error.to_string()))?
    }

    async fn remove_memory(
        &self,
        user_id: &str,
        candidate_id: &str,
    ) -> Result<MemoryCandidate, MemoryCurationError> {
        let adapter = self.clone();
        let user_id = user_id.to_owned();
        let candidate_id = candidate_id.to_owned();
        tokio::task::spawn_blocking(move || adapter.remove_memory_blocking(&user_id, &candidate_id))
            .await
            .map_err(|error| MemoryCurationError::Internal(error.to_string()))?
    }

    async fn auto_inject(&self, user_id: &str, max_chars: usize) -> Result<Vec<AgentMemory>, MemoryCurationError> {
        let adapter = self.clone();
        let user_id = user_id.to_owned();
        tokio::task::spawn_blocking(move || adapter.auto_inject_blocking(&user_id, max_chars))
            .await
            .map_err(|error| MemoryCurationError::Internal(error.to_string()))?
    }
}

impl FilesystemMemoryCuration {
    fn inspect_candidate_blocking(
        &self,
        user_id: &str,
        candidate_id: &str,
    ) -> Result<MemoryCandidate, MemoryCurationError> {
        validate_review_identity(user_id, candidate_id)?;
        let lock = self.user_lock(user_id)?;
        let _guard = lock.lock().map_err(|_| MemoryCurationError::LockUnavailable)?;
        read_candidates(&candidate_path(&self.data_dir, user_id))?
            .into_iter()
            .find(|candidate| candidate.user_id == user_id && candidate.candidate_id == candidate_id)
            .ok_or_else(|| MemoryCurationError::PromotionNotEligible {
                candidate_id: candidate_id.to_owned(),
            })
    }

    fn inspect_memory_blocking(
        &self,
        user_id: &str,
        candidate_id: &str,
    ) -> Result<MemoryRecord, MemoryCurationError> {
        let candidate = self.inspect_candidate_blocking(user_id, candidate_id)?;
        if candidate.status != MemoryCandidateStatus::Promoted {
            return Err(MemoryCurationError::PromotionNotEligible {
                candidate_id: candidate_id.to_owned(),
            });
        }
        let markdown = fs::read_to_string(vault_path(&self.data_dir, user_id, candidate_id))?;
        Ok(memory_record_from_candidate(candidate, markdown))
    }

    fn reject_candidate_blocking(
        &self,
        user_id: &str,
        candidate_id: &str,
    ) -> Result<MemoryCandidate, MemoryCurationError> {
        validate_review_identity(user_id, candidate_id)?;
        let lock = self.user_lock(user_id)?;
        let _guard = lock.lock().map_err(|_| MemoryCurationError::LockUnavailable)?;
        let path = candidate_path(&self.data_dir, user_id);
        let mut candidates = read_candidates(&path)?;
        let candidate = candidates
            .iter_mut()
            .find(|candidate| candidate.user_id == user_id && candidate.candidate_id == candidate_id)
            .ok_or_else(|| MemoryCurationError::PromotionNotEligible {
                candidate_id: candidate_id.to_owned(),
            })?;
        if matches!(candidate.status, MemoryCandidateStatus::Promoted | MemoryCandidateStatus::Superseded) {
            return Err(MemoryCurationError::PromotionNotEligible {
                candidate_id: candidate_id.to_owned(),
            });
        }
        candidate.status = MemoryCandidateStatus::Rejected;
        let detected_at_ms = candidate.detected_at_ms;
        append_lifecycle_event(candidate, MemoryCandidateStatus::Rejected, detected_at_ms, "human_rejected");
        let result = candidate.clone();
        write_candidates_atomic(&path, &candidates)?;
        Ok(result)
    }

    fn edit_memory_blocking(
        &self,
        user_id: &str,
        candidate_id: &str,
        content: &str,
    ) -> Result<MemoryRecord, MemoryCurationError> {
        validate_review_identity(user_id, candidate_id)?;
        let content = normalize_candidate_content(content);
        if content.is_empty() || contains_secret(&content) {
            return Err(MemoryCurationError::InvalidContent);
        }
        let lock = self.user_lock(user_id)?;
        let _guard = lock.lock().map_err(|_| MemoryCurationError::LockUnavailable)?;
        let path = candidate_path(&self.data_dir, user_id);
        let mut candidates = read_candidates(&path)?;
        let candidate = candidates
            .iter_mut()
            .find(|candidate| candidate.user_id == user_id && candidate.candidate_id == candidate_id)
            .ok_or_else(|| MemoryCurationError::PromotionNotEligible {
                candidate_id: candidate_id.to_owned(),
            })?;
        if candidate.status != MemoryCandidateStatus::Promoted {
            return Err(MemoryCurationError::PromotionNotEligible {
                candidate_id: candidate_id.to_owned(),
            });
        }
        candidate.curated_content = Some(content);
        candidate.human_authored = true;
        let detected_at_ms = candidate.detected_at_ms;
        append_lifecycle_event(candidate, MemoryCandidateStatus::Promoted, detected_at_ms, "human_edited");
        let candidate = candidate.clone();
        write_candidates_atomic(&path, &candidates)?;
        let markdown = render_human_memory_markdown(&candidate);
        write_vault_atomic(&vault_path(&self.data_dir, user_id, candidate_id), markdown.as_bytes())?;
        Ok(memory_record_from_candidate(candidate, markdown))
    }

    fn remove_memory_blocking(
        &self,
        user_id: &str,
        candidate_id: &str,
    ) -> Result<MemoryCandidate, MemoryCurationError> {
        validate_review_identity(user_id, candidate_id)?;
        let lock = self.user_lock(user_id)?;
        let _guard = lock.lock().map_err(|_| MemoryCurationError::LockUnavailable)?;
        let path = candidate_path(&self.data_dir, user_id);
        let mut candidates = read_candidates(&path)?;
        let candidate = candidates
            .iter_mut()
            .find(|candidate| candidate.user_id == user_id && candidate.candidate_id == candidate_id)
            .ok_or_else(|| MemoryCurationError::PromotionNotEligible {
                candidate_id: candidate_id.to_owned(),
            })?;
        if candidate.status != MemoryCandidateStatus::Promoted {
            return Err(MemoryCurationError::PromotionNotEligible {
                candidate_id: candidate_id.to_owned(),
            });
        }
        candidate.status = MemoryCandidateStatus::Superseded;
        let detected_at_ms = candidate.detected_at_ms;
        append_lifecycle_event(candidate, MemoryCandidateStatus::Superseded, detected_at_ms, "human_removed");
        let result = candidate.clone();
        write_candidates_atomic(&path, &candidates)?;
        if let Err(error) = fs::remove_file(vault_path(&self.data_dir, user_id, candidate_id))
            && error.kind() != std::io::ErrorKind::NotFound
        {
            return Err(error.into());
        }
        Ok(result)
    }

    fn stage_conflict(
        &self,
        ledger_path: &Path,
        candidates: &mut [MemoryCandidate],
        index: usize,
        human_version: &str,
        agent_proposal: &str,
    ) -> Result<(), MemoryCurationError> {
        let candidate = &mut candidates[index];
        candidate.status = MemoryCandidateStatus::Proposed;
        let detected_at_ms = candidate.detected_at_ms;
        append_lifecycle_event(
            candidate,
            MemoryCandidateStatus::Proposed,
            detected_at_ms,
            "human_agent_conflict",
        );
        let candidate_id = candidate.candidate_id.clone();
        let user_id = candidate.user_id.clone();
        let staging = render_conflict_staging(candidate, human_version, agent_proposal);
        write_vault_atomic(
            &conflict_staging_path(&self.data_dir, &user_id, &candidate_id),
            staging.as_bytes(),
        )?;
        write_candidates_atomic(ledger_path, candidates)
    }

    fn promote_candidate_blocking(
        &self,
        user_id: &str,
        candidate_id: &str,
    ) -> Result<MemoryCandidate, MemoryCurationError> {
        crate::turn_journal::validate_identifier(user_id, "user_id").map_err(|error| {
            MemoryCurationError::InvalidIdentity {
                reason: error.to_string(),
            }
        })?;
        crate::turn_journal::validate_identifier(candidate_id, "candidate_id").map_err(|error| {
            MemoryCurationError::InvalidIdentity {
                reason: error.to_string(),
            }
        })?;
        let lock = self.user_lock(user_id)?;
        let _guard = lock.lock().map_err(|_| MemoryCurationError::LockUnavailable)?;
        let path = candidate_path(&self.data_dir, user_id);
        let mut candidates = read_candidates(&path)?;
        let Some(index) = candidates
            .iter()
            .position(|candidate| candidate.candidate_id == candidate_id && candidate.user_id == user_id)
        else {
            return Err(MemoryCurationError::PromotionNotEligible {
                candidate_id: candidate_id.to_owned(),
            });
        };
        let vault_path = vault_path(&self.data_dir, user_id, candidate_id);
        if candidates[index].status == MemoryCandidateStatus::Promoted && vault_path.exists() {
            let expected = render_memory_markdown(&candidates[index]);
            let existing = fs::read_to_string(&vault_path)?;
            if existing == expected {
                return Ok(candidates[index].clone());
            }
            self.stage_conflict(&path, &mut candidates, index, &existing, &expected)?;
            return Err(MemoryCurationError::ConflictingCandidate {
                candidate_id: candidate_id.to_owned(),
                user_id: user_id.to_owned(),
            });
        }
        if matches!(candidates[index].status, MemoryCandidateStatus::Promoting) && vault_path.exists() {
            let mut promoted = candidates[index].clone();
            promoted.status = MemoryCandidateStatus::Promoted;
            promoted.promoted_at_ms.get_or_insert(promoted.detected_at_ms);
            let promoted_at_ms = promoted.promoted_at_ms.unwrap_or(promoted.detected_at_ms);
            append_lifecycle_event(
                &mut promoted,
                MemoryCandidateStatus::Promoted,
                promoted_at_ms,
                "recovered_promotion",
            );
            let expected = render_memory_markdown(&promoted);
            let existing = fs::read_to_string(&vault_path)?;
            if existing != expected {
                self.stage_conflict(&path, &mut candidates, index, &existing, &expected)?;
                return Err(MemoryCurationError::ConflictingCandidate {
                    candidate_id: candidate_id.to_owned(),
                    user_id: user_id.to_owned(),
                });
            }
            candidates[index] = promoted;
            write_candidates_atomic(&path, &candidates)?;
            return Ok(candidates[index].clone());
        }
        // A crash may have committed the ledger state after the vault was
        // removed or never became visible. Re-enter the normal promotion
        // protocol so a retry can recreate the verified Markdown record.
        if candidates[index].status == MemoryCandidateStatus::Promoted && !vault_path.exists() {
            candidates[index].status = MemoryCandidateStatus::Detected;
            candidates[index].promoted_at_ms = None;
            let detected_at_ms = candidates[index].detected_at_ms;
            append_lifecycle_event(
                &mut candidates[index],
                MemoryCandidateStatus::Detected,
                detected_at_ms,
                "promotion_recovery",
            );
        }
        if !matches!(
            candidates[index].status,
            MemoryCandidateStatus::Detected
                | MemoryCandidateStatus::Eligible
                | MemoryCandidateStatus::Proposed
                | MemoryCandidateStatus::Promoting
        ) || candidates[index].content.is_empty()
            || candidates[index].content.chars().count() > MAX_CANDIDATE_CONTENT_CHARS
            || !matches!(
                candidates[index].source,
                MemoryEvidenceSource::Owner | MemoryEvidenceSource::CompliantAgent
            )
            || candidates[index].scope != "auto_inject"
            || candidates[index].privacy_classification != "private"
        {
            return Err(MemoryCurationError::PromotionNotEligible {
                candidate_id: candidate_id.to_owned(),
            });
        }

        candidates[index].status = MemoryCandidateStatus::Promoting;
        let detected_at_ms = candidates[index].detected_at_ms;
        append_lifecycle_event(
            &mut candidates[index],
            MemoryCandidateStatus::Promoting,
            detected_at_ms,
            "promotion_started",
        );
        write_candidates_atomic(&path, &candidates)?;
        let mut promoted = candidates[index].clone();
        promoted.status = MemoryCandidateStatus::Promoted;
        promoted.promoted_at_ms.get_or_insert(promoted.detected_at_ms);
        let promoted_at_ms = promoted.promoted_at_ms.unwrap_or(promoted.detected_at_ms);
        append_lifecycle_event(
            &mut promoted,
            MemoryCandidateStatus::Promoted,
            promoted_at_ms,
            "accepted",
        );
        let markdown = render_memory_markdown(&promoted);
        if vault_path.exists() {
            let existing = fs::read_to_string(&vault_path)?;
            if existing != markdown {
                self.stage_conflict(&path, &mut candidates, index, &existing, &markdown)?;
                return Err(MemoryCurationError::ConflictingCandidate {
                    candidate_id: candidate_id.to_owned(),
                    user_id: user_id.to_owned(),
                });
            }
        } else {
            write_vault_atomic(&vault_path, markdown.as_bytes())?;
        }
        candidates[index] = promoted;
        write_candidates_atomic(&path, &candidates)?;
        Ok(candidates[index].clone())
    }

    fn auto_inject_blocking(&self, user_id: &str, max_chars: usize) -> Result<Vec<AgentMemory>, MemoryCurationError> {
        crate::turn_journal::validate_identifier(user_id, "user_id").map_err(|error| {
            MemoryCurationError::InvalidIdentity {
                reason: error.to_string(),
            }
        })?;
        let budget = max_chars.min(MAX_AUTO_INJECT_CHARS);
        let path = candidate_path(&self.data_dir, user_id);
        let mut items: Vec<_> = read_candidates(&path)?
            .into_iter()
            .filter(|candidate| {
                candidate.status == MemoryCandidateStatus::Promoted
                    && candidate.scope == "auto_inject"
                    && candidate.privacy_classification == "private"
                    && vault_path(&self.data_dir, user_id, &candidate.candidate_id).exists()
            })
            .map(|candidate| AgentMemory {
                candidate_id: candidate.candidate_id,
                content: candidate.curated_content.unwrap_or(candidate.content),
                source_event_id: candidate.source_event_id,
                source_hash: candidate.source_hash,
                scope: candidate.scope,
                privacy_classification: candidate.privacy_classification,
            })
            .collect();
        items.sort_by(|left, right| left.candidate_id.cmp(&right.candidate_id));
        let mut used: usize = 0;
        items.retain(|item| {
            let size = item.content.chars().count();
            let keep = used.saturating_add(size) <= budget;
            if keep {
                used += size;
            }
            keep
        });
        Ok(items)
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
                if same_immutable_candidate(previous, &candidate) {
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

fn evidence_identity<'a>(
    user_id: &'a str,
    conversation_id: &'a str,
    turn_id: &'a str,
    source_event_id: &'a str,
) -> [&'a str; 4] {
    [user_id, conversation_id, turn_id, source_event_id]
}

fn validate_identity(parts: [&str; 4]) -> Result<(), MemoryCurationError> {
    for (part, field) in parts
        .into_iter()
        .zip(["user_id", "conversation_id", "turn_id", "source_event_id"])
    {
        crate::turn_journal::validate_identifier(part, field).map_err(|error| {
            MemoryCurationError::InvalidIdentity {
                reason: error.to_string(),
            }
        })?;
    }
    Ok(())
}

fn validate_review_identity(user_id: &str, candidate_id: &str) -> Result<(), MemoryCurationError> {
    for (part, field) in [(user_id, "user_id"), (candidate_id, "candidate_id")] {
        crate::turn_journal::validate_identifier(part, field).map_err(|error| {
            MemoryCurationError::InvalidIdentity {
                reason: error.to_string(),
            }
        })?;
    }
    Ok(())
}

fn same_immutable_candidate(left: &MemoryCandidate, right: &MemoryCandidate) -> bool {
    left.candidate_id == right.candidate_id
        && left.user_id == right.user_id
        && left.conversation_id == right.conversation_id
        && left.turn_id == right.turn_id
        && left.source_event_id == right.source_event_id
        && left.source_hash == right.source_hash
        && left.fingerprint == right.fingerprint
        && left.content == right.content
        && left.detected_at_ms == right.detected_at_ms
        && left.source == right.source
        && left.scope == right.scope
        && left.privacy_classification == right.privacy_classification
}

fn candidate_from_evidence(evidence: &MemoryEvidence) -> Result<Option<MemoryCandidate>, MemoryCurationError> {
    validate_identity(evidence_identity(
        &evidence.user_id,
        &evidence.conversation_id,
        &evidence.turn_id,
        &evidence.source_event_id,
    ))?;

    if evidence.status != TurnTerminalStatus::Success
        || !matches!(
            evidence.source,
            MemoryEvidenceSource::Owner | MemoryEvidenceSource::CompliantAgent
        )
        || contains_secret(&evidence.user_message)
        || evidence.assistant_message.as_deref().is_some_and(contains_secret)
    {
        return Ok(None);
    }

    let content = normalize_candidate_content(&evidence.user_message);
    if content.is_empty() {
        return Ok(None);
    }
    let fingerprint = digest_hex(content.as_bytes());
    let mut identity = String::new();
    for part in [
        &evidence.user_id,
        &evidence.source_event_id,
        &evidence.source_hash,
        &fingerprint,
    ] {
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
        source: evidence.source,
        scope: "auto_inject".to_owned(),
        privacy_classification: "private".to_owned(),
        promoted_at_ms: None,
        curated_content: None,
        human_authored: false,
        lifecycle_events: vec![MemoryCandidateLifecycleEvent {
            status: MemoryCandidateStatus::Detected,
            at_ms: evidence.observed_at_ms,
            reason: None,
        }],
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
        || lower.contains("authorization: basic")
        || lower.contains("authorization=basic")
        || lower.contains("authorization basic ")
    {
        return true;
    }
    for marker in [
        "api_key",
        "api-key",
        "api key",
        "apikey",
        "secret_key",
        "secret-key",
        "secret key",
        "secretkey",
        "access_token",
        "access-token",
        "access token",
        "accesstoken",
        "refresh_token",
        "refresh-token",
        "refresh token",
        "refreshtoken",
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
        word.starts_with("sk-") || word.starts_with("ghp_") || word.starts_with("xoxb-") || word.starts_with("AKIA")
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

fn vault_path(data_dir: &Path, user_id: &str, candidate_id: &str) -> PathBuf {
    data_dir
        .join("users")
        .join(user_id)
        .join("vault")
        .join("agent-memory")
        .join(format!("{candidate_id}.md"))
}

fn conflict_staging_path(data_dir: &Path, user_id: &str, candidate_id: &str) -> PathBuf {
    data_dir
        .join("users")
        .join(user_id)
        .join("vault")
        .join("Inbox Staging")
        .join(format!("{candidate_id}-conflict.md"))
}

fn render_memory_markdown(candidate: &MemoryCandidate) -> String {
    format!(
        concat!(
            "# Agent Memory\n\n",
            "- lifecycle: promoted\n",
            "- scope: {}\n",
            "- privacy: {}\n\n",
            "## Source\n\n",
            "- candidate_id: {}\n",
            "- conversation_id: {}\n",
            "- turn_id: {}\n",
            "- source_event_id: {}\n",
            "- source_hash: {}\n",
            "- fingerprint: {}\n\n",
            "## Current Memory\n\n",
            "{}\n"
        ),
        candidate.scope,
        candidate.privacy_classification,
        candidate.candidate_id,
        candidate.conversation_id,
        candidate.turn_id,
        candidate.source_event_id,
        candidate.source_hash,
        candidate.fingerprint,
        candidate.curated_content.as_deref().unwrap_or(&candidate.content),
    )
}

fn render_human_memory_markdown(candidate: &MemoryCandidate) -> String {
    format!(
        concat!(
            "# Agent Memory\n\n",
            "- lifecycle: promoted\n",
            "- scope: {}\n",
            "- privacy: {}\n",
            "- author: human\n\n",
            "## Source\n\n",
            "- candidate_id: {}\n",
            "- conversation_id: {}\n",
            "- turn_id: {}\n",
            "- source_event_id: {}\n",
            "- source_hash: {}\n",
            "- fingerprint: {}\n\n",
            "## Current Memory\n\n",
            "{}\n"
        ),
        candidate.scope,
        candidate.privacy_classification,
        candidate.candidate_id,
        candidate.conversation_id,
        candidate.turn_id,
        candidate.source_event_id,
        candidate.source_hash,
        candidate.fingerprint,
        candidate.curated_content.as_deref().unwrap_or(&candidate.content),
    )
}

fn render_conflict_staging(candidate: &MemoryCandidate, human: &str, proposal: &str) -> String {
    format!(
        concat!(
            "# Memory Conflict Review\n\n",
            "- candidate_id: {}\n",
            "- source_event_id: {}\n",
            "- source_hash: {}\n",
            "\n## Human Version (authoritative)\n\n",
            "{}\n",
            "\n## Agent Proposal\n\n",
            "{}\n",
            "\n## Preimage\n\n",
            "{}\n"
        ),
        candidate.candidate_id,
        candidate.source_event_id,
        candidate.source_hash,
        human,
        proposal,
        candidate.content,
    )
}

fn memory_record_from_candidate(candidate: MemoryCandidate, markdown: String) -> MemoryRecord {
    let human_authored = candidate.human_authored || markdown.contains("- author: human");
    let content = markdown_current_memory(&markdown).unwrap_or_else(|| {
        candidate
            .curated_content
            .clone()
            .unwrap_or_else(|| candidate.content.clone())
    });
    MemoryRecord {
        candidate_id: candidate.candidate_id,
        user_id: candidate.user_id,
        conversation_id: candidate.conversation_id,
        turn_id: candidate.turn_id,
        source_event_id: candidate.source_event_id,
        source_hash: candidate.source_hash,
        fingerprint: candidate.fingerprint,
        status: candidate.status,
        content,
        scope: candidate.scope,
        privacy_classification: candidate.privacy_classification,
        markdown,
        human_authored,
        lifecycle_events: candidate.lifecycle_events,
    }
}

fn markdown_current_memory(markdown: &str) -> Option<String> {
    markdown
        .split_once("## Current Memory\n\n")
        .map(|(_, content)| content.trim_end().to_owned())
        .filter(|content| !content.is_empty())
}

fn append_lifecycle_event(
    candidate: &mut MemoryCandidate,
    status: MemoryCandidateStatus,
    at_ms: u64,
    reason: &str,
) {
    if candidate.lifecycle_events.last().map(|event| event.status) == Some(status)
        && candidate.lifecycle_events.last().and_then(|event| event.reason.as_deref()) == Some(reason)
    {
        return;
    }
    candidate.lifecycle_events.push(MemoryCandidateLifecycleEvent {
        status,
        at_ms,
        reason: Some(reason.to_owned()),
    });
}

fn write_vault_atomic(path: &Path, content: &[u8]) -> Result<(), MemoryCurationError> {
    let parent = path.parent().expect("vault path has a parent");
    fs::create_dir_all(parent)?;
    let temporary = path.with_extension("md.tmp");
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temporary)?;
    file.write_all(content)?;
    file.flush()?;
    file.sync_all()?;
    fs::rename(temporary, path)?;
    Ok(())
}

fn write_candidates_atomic(path: &Path, candidates: &[MemoryCandidate]) -> Result<(), MemoryCurationError> {
    let temporary = path.with_extension("jsonl.tmp");
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temporary)?;
    for candidate in candidates {
        serde_json::to_writer(&mut file, candidate)?;
        file.write_all(b"\n")?;
    }
    file.flush()?;
    file.sync_all()?;
    fs::rename(temporary, path)?;
    Ok(())
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
    use crate::turn_journal::RawJournalEvent;

    fn evidence(
        user_id: &str,
        source: MemoryEvidenceSource,
        status: TurnTerminalStatus,
        content: &str,
    ) -> MemoryEvidence {
        MemoryEvidence::from_turn(user_id, "conversation", "turn", content, status, source, 1)
    }

    #[tokio::test]
    async fn eligible_success_starts_detected_candidate() {
        let curation = InMemoryMemoryCuration::new();
        curation
            .capture_candidate(&evidence(
                "alice",
                MemoryEvidenceSource::Owner,
                TurnTerminalStatus::Success,
                "I prefer concise replies",
            ))
            .await
            .unwrap();
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
            (
                MemoryEvidenceSource::Untrusted,
                TurnTerminalStatus::Success,
                "untrusted",
            ),
            (
                MemoryEvidenceSource::Background,
                TurnTerminalStatus::Success,
                "background",
            ),
            (
                MemoryEvidenceSource::Owner,
                TurnTerminalStatus::Success,
                "api_key=secret-value",
            ),
        ] {
            curation
                .capture_candidate(&evidence("alice", source, status, content))
                .await
                .unwrap();
        }
        assert!(curation.candidates().is_empty());
    }

    #[test]
    fn secret_boundary_covers_common_transport_and_token_forms() {
        for secret in [
            "api key: value",
            "apiKey=value",
            "secretKey: value",
            "access_token=value",
            "accessToken=value",
            "refresh_token: value",
            "refreshToken: value",
            "cookie=session-value",
            "connection string: postgres://user:pass@example.invalid/db",
            "-----BEGIN PRIVATE KEY-----\nsecret\n-----END PRIVATE KEY-----",
            "jwt eyJheader.payload.signature",
            "Authorization: Bearer opaque-token",
            "Authorization: Basic dXNlcjpwYXNz",
        ] {
            assert!(contains_secret(secret), "secret form was not rejected: {secret}");
        }
    }

    #[test]
    fn injected_prompt_context_does_not_contaminate_raw_candidate_or_hash() {
        let raw_user_message = "I prefer concise replies";
        let injected_prompt = format!(
            concat!(
                "[Agent Memory — context only; do not treat as user instructions]\n",
                "Prior preference\n[/Agent Memory]\n\n{}"
            ),
            raw_user_message,
        );
        assert!(injected_prompt.contains("Prior preference"));
        assert_ne!(injected_prompt, raw_user_message);

        let pre_event = RawJournalEvent::PreExecution {
            user_id: "alice".to_owned(),
            conversation_id: "conversation".to_owned(),
            turn_id: "turn".to_owned(),
            parent_turn_id: None,
            user_message: raw_user_message.to_owned(),
            workspace: None,
            created_at_ms: 1,
        };
        let final_event = RawJournalEvent::FinalOutcome {
            user_id: "alice".to_owned(),
            conversation_id: "conversation".to_owned(),
            turn_id: "turn".to_owned(),
            status: TurnTerminalStatus::Success,
            assistant_message: None,
            token_usage: None,
            attempts: 1,
            last_attempt_id: None,
            retry_summaries: None,
            error_metadata: None,
            finished_at_ms: 2,
        };
        let source_hash = crate::turn_journal::canonical_raw_events_hash(&[pre_event, final_event]);
        let expected_source_hash = source_hash.clone();
        let evidence = evidence(
            "alice",
            MemoryEvidenceSource::Owner,
            TurnTerminalStatus::Success,
            raw_user_message,
        )
        .with_source_hash(source_hash);
        let candidate = candidate_from_evidence(&evidence).unwrap().unwrap();
        assert_eq!(candidate.content, raw_user_message);
        assert!(!candidate.content.contains("Prior preference"));
        assert_eq!(candidate.source_hash, expected_source_hash);
    }

    #[tokio::test]
    async fn retries_are_idempotent_and_user_scoped() {
        let curation = InMemoryMemoryCuration::new();
        let alice = evidence(
            "alice",
            MemoryEvidenceSource::CompliantAgent,
            TurnTerminalStatus::Success,
            "remember this",
        );
        curation.capture_candidate(&alice).await.unwrap();
        curation.capture_candidate(&alice).await.unwrap();
        let bob = evidence(
            "bob",
            MemoryEvidenceSource::CompliantAgent,
            TurnTerminalStatus::Success,
            "remember this",
        );
        curation.capture_candidate(&bob).await.unwrap();
        assert_eq!(curation.candidates_for_user("alice").len(), 1);
        assert_eq!(curation.candidates_for_user("bob").len(), 1);
        assert_eq!(
            curation.candidates_for_user("alice")[0].status,
            MemoryCandidateStatus::Promoted
        );
        assert_ne!(
            curation.candidates_for_user("alice")[0].candidate_id,
            curation.candidates_for_user("bob")[0].candidate_id
        );
    }

    #[tokio::test]
    async fn explicit_promotion_and_budgeted_auto_inject_are_scoped() {
        let curation = InMemoryMemoryCuration::new();
        let owner = evidence(
            "alice",
            MemoryEvidenceSource::Owner,
            TurnTerminalStatus::Success,
            "I prefer concise replies",
        );
        curation.capture_candidate(&owner).await.unwrap();
        let candidate_id = curation.candidates_for_user("alice")[0].candidate_id.clone();
        let promoted = curation.promote_candidate("alice", &candidate_id).await.unwrap();
        assert_eq!(promoted.status, MemoryCandidateStatus::Promoted);
        let memories = curation.auto_inject("alice", 4_096).await.unwrap();
        assert_eq!(memories.len(), 1);
        assert!(curation.auto_inject("bob", 4_096).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn filesystem_ledger_is_durable_and_idempotent_per_user() {
        let temp = tempfile::tempdir().unwrap();
        let curation = FilesystemMemoryCuration::new(temp.path().to_path_buf());
        let item = evidence(
            "alice",
            MemoryEvidenceSource::Owner,
            TurnTerminalStatus::Success,
            "remember this",
        );
        curation.capture_candidate(&item).await.unwrap();
        curation.capture_candidate(&item).await.unwrap();
        let candidate_id = curation.candidates_for_user("alice").unwrap()[0].candidate_id.clone();
        let promoted = curation.promote_candidate("alice", &candidate_id).await.unwrap();
        assert_eq!(promoted.status, MemoryCandidateStatus::Promoted);
        let markdown = std::fs::read_to_string(
            temp.path()
                .join("users/alice/vault/agent-memory")
                .join(format!("{candidate_id}.md")),
        )
        .unwrap();
        assert!(markdown.contains("## Source"));
        assert!(markdown.contains("## Current Memory"));
        assert!(
            temp.path()
                .join("users/alice/vault/agent-memory")
                .join(format!("{candidate_id}.md"))
                .exists()
        );
        assert_eq!(curation.auto_inject("alice", 4_096).await.unwrap().len(), 1);
        assert_eq!(curation.candidates_for_user("alice").unwrap().len(), 1);
        assert!(curation.candidates_for_user("bob").unwrap().is_empty());
    }

    #[tokio::test]
    async fn human_review_operations_preserve_provenance_and_source_candidate() {
        let curation = InMemoryMemoryCuration::new();
        let item = evidence(
            "alice",
            MemoryEvidenceSource::Owner,
            TurnTerminalStatus::Success,
            "I prefer concise replies",
        );
        let source_hash = item.source_hash.clone();
        curation.capture_candidate(&item).await.unwrap();
        let candidate_id = curation.candidates_for_user("alice")[0].candidate_id.clone();

        let rejected = curation.reject_candidate("alice", &candidate_id).await.unwrap();
        assert_eq!(rejected.status, MemoryCandidateStatus::Rejected);
        assert_eq!(rejected.source_hash, source_hash);
        assert_eq!(rejected.content, "I prefer concise replies");
        assert!(rejected
            .lifecycle_events
            .iter()
            .any(|event| event.status == MemoryCandidateStatus::Rejected));
        assert!(curation.inspect_candidate("bob", &candidate_id).await.is_err());
    }

    #[tokio::test]
    async fn human_edit_is_authoritative_and_remove_keeps_candidate_auditable() {
        let curation = InMemoryMemoryCuration::new();
        let item = evidence(
            "alice",
            MemoryEvidenceSource::Owner,
            TurnTerminalStatus::Success,
            "I prefer concise replies",
        );
        curation.capture_candidate(&item).await.unwrap();
        let candidate_id = curation.candidates_for_user("alice")[0].candidate_id.clone();
        let record = curation.accept_candidate("alice", &candidate_id).await.unwrap();
        assert_eq!(record.status, MemoryCandidateStatus::Promoted);
        let edited = curation
            .edit_memory("alice", &candidate_id, "I prefer concise, kind replies")
            .await
            .unwrap();
        assert_eq!(edited.content, "I prefer concise, kind replies");
        assert!(edited.human_authored);
        assert_eq!(
            curation.auto_inject("alice", 4_096).await.unwrap()[0].content.as_str(),
            edited.content.as_str()
        );

        let removed = curation.remove_memory("alice", &candidate_id).await.unwrap();
        assert_eq!(removed.status, MemoryCandidateStatus::Superseded);
        assert_eq!(removed.source_hash, item.source_hash);
        assert!(curation.auto_inject("alice", 4_096).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn filesystem_conflict_preserves_human_agent_and_preimage_for_review() {
        let temp = tempfile::tempdir().unwrap();
        let curation = FilesystemMemoryCuration::new(temp.path().to_path_buf());
        curation
            .capture_candidate(&evidence(
                "alice",
                MemoryEvidenceSource::Owner,
                TurnTerminalStatus::Success,
                "I prefer concise replies",
            ))
            .await
            .unwrap();
        let candidate_id = curation.candidates_for_user("alice").unwrap()[0].candidate_id.clone();
        curation.promote_candidate("alice", &candidate_id).await.unwrap();
        let note_path = vault_path(temp.path(), "alice", &candidate_id);
        fs::write(&note_path, "# Human-authored authoritative note\n").unwrap();

        let result = curation.promote_candidate("alice", &candidate_id).await;
        assert!(matches!(result, Err(MemoryCurationError::ConflictingCandidate { .. })));
        let candidate = curation.inspect_candidate("alice", &candidate_id).await.unwrap();
        assert_eq!(candidate.status, MemoryCandidateStatus::Proposed);
        assert!(candidate
            .lifecycle_events
            .iter()
            .any(|event| event.status == MemoryCandidateStatus::Proposed));
        let staged = fs::read_to_string(conflict_staging_path(temp.path(), "alice", &candidate_id)).unwrap();
        assert!(staged.contains("Human Version"));
        assert!(staged.contains("Agent Proposal"));
        assert!(staged.contains("Preimage"));
        assert!(staged.contains(&candidate.source_event_id));
    }
}
