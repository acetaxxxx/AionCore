//! Conversation-bound Facebook MonitorJob control domain and public service seam.
//!
//! This module implements Ticket 01:
//! - Conversation-owned `MonitorJob` domain aggregate and lifecycle.
//! - Complete target, query, lookback, and schedule scope enforcement.
//! - Supplied schedule acceptance vs. agent-proposed default schedule approval flow (no premature job persistence).
//! - Lifecycle control semantics: create, pause, resume, cancel, get, list, and conversation termination hook.
//! - Strict user and originating conversation scope isolation (fail-closed against cross-account/conversation access).
//! - Public `MonitorRunner` seam for future execution tickets.

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::scheduler::{validate_cron_expression, validate_timezone};
use crate::types::CronSchedule;

// ---------------------------------------------------------------------------
// Error Definitions
// ---------------------------------------------------------------------------

#[derive(Debug, Error, PartialEq, Eq, Clone)]
pub enum MonitorError {
    #[error("Incomplete monitor scope: {0}")]
    IncompleteScope(String),

    #[error("Invalid target scope: {0}")]
    InvalidTargetScope(String),

    #[error("Invalid query scope: {0}")]
    InvalidQueryScope(String),

    #[error("Invalid lookback scope: {0}")]
    InvalidLookbackScope(String),

    #[error("Invalid schedule scope: {0}")]
    InvalidScheduleScope(String),

    #[error("Monitor job not found: {0}")]
    NotFound(String),

    #[error("Access denied: {0}")]
    AccessDenied(String),

    #[error("Invalid lifecycle transition: current status is {current:?}, cannot perform {requested}")]
    InvalidLifecycleTransition {
        current: MonitorJobStatus,
        requested: String,
    },
}

// ---------------------------------------------------------------------------
// Domain Enums and Types
// ---------------------------------------------------------------------------

/// Lifecycle status for a conversation-bound `MonitorJob`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MonitorJobStatus {
    /// Actively scheduled for background monitor executions.
    Active,
    /// Paused due to explicit user pause, authentication expiry, or checkpoint/CAPTCHA.
    Paused,
    /// Permanently cancelled by explicit request or conversation closure/archival/deletion.
    Cancelled,
}

/// Bounded reason why a monitor job was stopped, paused, or cancelled.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MonitorStopReason {
    ExplicitUserCancellation,
    ExplicitUserPause,
    ConversationClosed,
    ConversationArchived,
    ConversationDeleted,
    AuthExpired,
    CheckpointDetected,
    CaptchaDetected,
    Other(String),
}

/// Last execution outcome of a monitor run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MonitorRunOutcome {
    Success,
    Partial,
    Unavailable,
    AuthExpired,
    Failed,
}

/// A validated Facebook group target.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FacebookTarget {
    pub target_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
}

impl FacebookTarget {
    pub fn new(target_id: impl Into<String>) -> Self {
        Self {
            target_id: target_id.into(),
            display_name: None,
        }
    }

    pub fn with_display_name(mut self, display_name: impl Into<String>) -> Self {
        self.display_name = Some(display_name.into());
        self
    }
}

/// Query and filter scope for Facebook post observation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MonitorQuery {
    pub query_text: String,
    #[serde(default)]
    pub filters: HashMap<String, String>,
    #[serde(default = "default_query_revision")]
    pub revision: u64,
}

fn default_query_revision() -> u64 {
    1
}

impl MonitorQuery {
    pub fn new(query_text: impl Into<String>) -> Self {
        Self {
            query_text: query_text.into(),
            filters: HashMap::new(),
            revision: 1,
        }
    }

    pub fn with_filters(mut self, filters: HashMap<String, String>) -> Self {
        self.filters = filters;
        self
    }

    pub fn with_revision(mut self, revision: u64) -> Self {
        self.revision = revision;
        self
    }
}

/// Historical lookback scope window for post scans.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct LookbackScope {
    pub duration_ms: u64,
}

impl LookbackScope {
    pub fn from_millis(duration_ms: u64) -> Self {
        Self { duration_ms }
    }

    pub fn from_days(days: u32) -> Self {
        Self {
            duration_ms: (days as u64) * 24 * 60 * 60 * 1000,
        }
    }

    pub fn from_hours(hours: u32) -> Self {
        Self {
            duration_ms: (hours as u64) * 60 * 60 * 1000,
        }
    }
}

// ---------------------------------------------------------------------------
// MonitorJob Aggregate Root
// ---------------------------------------------------------------------------

/// Durable domain record representing an explicit, conversation-bound monitoring request.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MonitorJob {
    pub id: String,
    pub user_id: String,
    pub conversation_id: String,
    pub targets: Vec<FacebookTarget>,
    pub query: MonitorQuery,
    pub lookback: LookbackScope,
    pub schedule: CronSchedule,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profile_ref: Option<String>,
    pub status: MonitorJobStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stop_reason: Option<MonitorStopReason>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_outcome: Option<MonitorRunOutcome>,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_execution_at_ms: Option<u64>,
}

// ---------------------------------------------------------------------------
// Proposal and Creation Payloads
// ---------------------------------------------------------------------------

/// Ephemeral domain proposal for an unapproved monitoring request.
/// Produced when schedule is omitted during creation; no durable job is created
/// until explicit user approval.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MonitorJobProposal {
    pub proposal_id: String,
    pub user_id: String,
    pub conversation_id: String,
    pub targets: Vec<FacebookTarget>,
    pub query: MonitorQuery,
    pub lookback: LookbackScope,
    pub proposed_schedule: CronSchedule,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profile_ref: Option<String>,
    pub created_at_ms: u64,
}

/// Request payload to create a new MonitorJob or produce an agent proposal.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreateMonitorJobRequest {
    pub targets: Vec<FacebookTarget>,
    pub query: MonitorQuery,
    pub lookback: LookbackScope,
    /// If supplied, validated and accepted without redundant confirmation.
    /// If omitted, produces an agent-proposed default requiring approval.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schedule: Option<CronSchedule>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profile_ref: Option<String>,
}

/// Outcome of attempting to create a MonitorJob.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CreateMonitorJobOutcome {
    /// A complete schedule was supplied; job was created and is immediately active.
    Active { job: MonitorJob },
    /// Schedule was omitted; agent proposed a default schedule awaiting user approval before job creation.
    RequiresApproval { proposal: MonitorJobProposal },
}

// ---------------------------------------------------------------------------
// MonitorRunner Port Seam (for Ticket 02+ execution)
// ---------------------------------------------------------------------------

/// Scan outcome produced by a bounded runner execution.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MonitorScanResult {
    pub outcome: MonitorRunOutcome,
    pub observations_count: usize,
    pub error_message: Option<String>,
}

/// High-level conversation-scoped runner port.
#[async_trait::async_trait]
pub trait MonitorRunner: Send + Sync {
    async fn run_scan(&self, job: &MonitorJob) -> Result<MonitorScanResult, String>;
}

// ---------------------------------------------------------------------------
// Repository Trait & In-Memory Store
// ---------------------------------------------------------------------------

#[async_trait::async_trait]
pub trait IMonitorJobRepository: Send + Sync {
    async fn save(&self, job: &MonitorJob) -> Result<(), MonitorError>;
    async fn get(&self, user_id: &str, conversation_id: &str, job_id: &str) -> Result<Option<MonitorJob>, MonitorError>;
    async fn list_by_conversation(&self, user_id: &str, conversation_id: &str) -> Result<Vec<MonitorJob>, MonitorError>;
    async fn delete(&self, user_id: &str, conversation_id: &str, job_id: &str) -> Result<bool, MonitorError>;
}

#[derive(Default)]
pub struct InMemoryMonitorJobRepository {
    jobs: RwLock<HashMap<String, MonitorJob>>,
}

impl InMemoryMonitorJobRepository {
    pub fn new() -> Self {
        Self {
            jobs: RwLock::new(HashMap::new()),
        }
    }
}

#[async_trait::async_trait]
impl IMonitorJobRepository for InMemoryMonitorJobRepository {
    async fn save(&self, job: &MonitorJob) -> Result<(), MonitorError> {
        let mut guard = self.jobs.write().await;
        guard.insert(job.id.clone(), job.clone());
        Ok(())
    }

    async fn get(&self, user_id: &str, conversation_id: &str, job_id: &str) -> Result<Option<MonitorJob>, MonitorError> {
        let guard = self.jobs.read().await;
        if let Some(job) = guard.get(job_id) {
            // Strict user and conversation isolation:
            if job.user_id == user_id && job.conversation_id == conversation_id {
                return Ok(Some(job.clone()));
            }
        }
        Ok(None)
    }

    async fn list_by_conversation(&self, user_id: &str, conversation_id: &str) -> Result<Vec<MonitorJob>, MonitorError> {
        let guard = self.jobs.read().await;
        let mut matching: Vec<MonitorJob> = guard
            .values()
            .filter(|job| job.user_id == user_id && job.conversation_id == conversation_id)
            .cloned()
            .collect();
        matching.sort_by_key(|j| j.created_at_ms);
        Ok(matching)
    }

    async fn delete(&self, user_id: &str, conversation_id: &str, job_id: &str) -> Result<bool, MonitorError> {
        let mut guard = self.jobs.write().await;
        if let Some(job) = guard.get(job_id) {
            if job.user_id == user_id && job.conversation_id == conversation_id {
                guard.remove(job_id);
                return Ok(true);
            }
        }
        Ok(false)
    }
}

// ---------------------------------------------------------------------------
// Default Schedule Policy & Schedule Validation
// ---------------------------------------------------------------------------

/// Propose a safe default monitoring schedule (every 6 hours) when user omits schedule.
pub fn propose_default_schedule() -> CronSchedule {
    CronSchedule::Every {
        every_ms: 6 * 60 * 60 * 1000,
        description: Some("Agent-proposed default schedule: every 6 hours".to_owned()),
    }
}

/// Fail-closed typed validation of a CronSchedule configuration.
pub fn validate_schedule(schedule: &CronSchedule) -> Result<(), MonitorError> {
    match schedule {
        CronSchedule::At { at_ms, .. } => {
            if *at_ms <= 0 {
                return Err(MonitorError::InvalidScheduleScope(
                    "at_ms timestamp must be greater than 0".into(),
                ));
            }
        }
        CronSchedule::Every { every_ms, .. } => {
            if *every_ms <= 0 {
                return Err(MonitorError::InvalidScheduleScope(
                    "every_ms interval must be greater than 0".into(),
                ));
            }
        }
        CronSchedule::Cron { expr, tz, .. } => {
            validate_cron_expression(expr)
                .map_err(|e| MonitorError::InvalidScheduleScope(format!("invalid cron expr: {e}")))?;
            if let Some(ref tz_str) = tz {
                validate_timezone(tz_str)
                    .map_err(|e| MonitorError::InvalidScheduleScope(format!("invalid timezone: {e}")))?;
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Public Domain Service: MonitorControlService
// ---------------------------------------------------------------------------

pub struct MonitorControlService {
    repo: Arc<dyn IMonitorJobRepository>,
}

impl MonitorControlService {
    pub fn new(repo: Arc<dyn IMonitorJobRepository>) -> Self {
        Self { repo }
    }

    pub fn with_in_memory_repo() -> Self {
        Self::new(Arc::new(InMemoryMonitorJobRepository::new()))
    }

    /// Validate ownership scope (user_id and conversation_id).
    fn validate_ownership_scope(user_id: &str, conversation_id: &str) -> Result<(), MonitorError> {
        if user_id.trim().is_empty() || conversation_id.trim().is_empty() {
            return Err(MonitorError::IncompleteScope(
                "user_id and conversation_id must be non-empty".into(),
            ));
        }
        Ok(())
    }

    /// Validate complete target, query, and lookback scope.
    fn validate_scope(
        targets: &[FacebookTarget],
        query: &MonitorQuery,
        lookback: &LookbackScope,
    ) -> Result<(), MonitorError> {
        if targets.is_empty() {
            return Err(MonitorError::IncompleteScope("targets must not be empty".into()));
        }
        for (i, t) in targets.iter().enumerate() {
            if t.target_id.trim().is_empty() {
                return Err(MonitorError::InvalidTargetScope(format!(
                    "target at index {i} has empty target_id"
                )));
            }
        }

        if query.query_text.trim().is_empty() {
            return Err(MonitorError::InvalidQueryScope("query_text must not be empty".into()));
        }

        if lookback.duration_ms == 0 {
            return Err(MonitorError::InvalidLookbackScope(
                "lookback duration_ms must be greater than 0".into(),
            ));
        }

        Ok(())
    }

    /// Create a new MonitorJob explicitly bound to the originating conversation.
    /// Rejects incomplete scope. If schedule is omitted, produces an agent-proposed
    /// default proposal without persisting a job until explicit approval.
    pub async fn create_job(
        &self,
        user_id: &str,
        conversation_id: &str,
        req: CreateMonitorJobRequest,
        now_ms: u64,
    ) -> Result<CreateMonitorJobOutcome, MonitorError> {
        Self::validate_ownership_scope(user_id, conversation_id)?;
        Self::validate_scope(&req.targets, &req.query, &req.lookback)?;

        match req.schedule {
            Some(supplied_schedule) => {
                validate_schedule(&supplied_schedule)?;

                let job_id = format!("mon_{}", uuid::Uuid::now_v7());
                let job = MonitorJob {
                    id: job_id,
                    user_id: user_id.to_owned(),
                    conversation_id: conversation_id.to_owned(),
                    targets: req.targets,
                    query: req.query,
                    lookback: req.lookback,
                    schedule: supplied_schedule,
                    profile_ref: req.profile_ref,
                    status: MonitorJobStatus::Active,
                    stop_reason: None,
                    last_outcome: None,
                    created_at_ms: now_ms,
                    updated_at_ms: now_ms,
                    next_execution_at_ms: Some(now_ms),
                };

                self.repo.save(&job).await?;
                Ok(CreateMonitorJobOutcome::Active { job })
            }
            None => {
                let proposed = propose_default_schedule();
                let proposal_id = format!("prop_{}", uuid::Uuid::now_v7());
                let proposal = MonitorJobProposal {
                    proposal_id,
                    user_id: user_id.to_owned(),
                    conversation_id: conversation_id.to_owned(),
                    targets: req.targets,
                    query: req.query,
                    lookback: req.lookback,
                    proposed_schedule: proposed,
                    profile_ref: req.profile_ref,
                    created_at_ms: now_ms,
                };

                // Do not persist unapproved job to repo
                Ok(CreateMonitorJobOutcome::RequiresApproval { proposal })
            }
        }
    }

    /// Approve an agent proposal to create the durable MonitorJob.
    /// User may accept the proposed schedule or supply an approved override schedule.
    pub async fn approve_proposal(
        &self,
        user_id: &str,
        conversation_id: &str,
        proposal: MonitorJobProposal,
        approved_schedule: Option<CronSchedule>,
        now_ms: u64,
    ) -> Result<MonitorJob, MonitorError> {
        Self::validate_ownership_scope(user_id, conversation_id)?;

        // Proposal ownership must match caller context
        if proposal.user_id != user_id || proposal.conversation_id != conversation_id {
            return Err(MonitorError::AccessDenied(
                "Proposal ownership does not match caller context".into(),
            ));
        }

        Self::validate_scope(&proposal.targets, &proposal.query, &proposal.lookback)?;

        let final_schedule = match approved_schedule {
            Some(sched) => sched,
            None => proposal.proposed_schedule,
        };

        validate_schedule(&final_schedule)?;

        let job_id = format!("mon_{}", uuid::Uuid::now_v7());
        let job = MonitorJob {
            id: job_id,
            user_id: user_id.to_owned(),
            conversation_id: conversation_id.to_owned(),
            targets: proposal.targets,
            query: proposal.query,
            lookback: proposal.lookback,
            schedule: final_schedule,
            profile_ref: proposal.profile_ref,
            status: MonitorJobStatus::Active,
            stop_reason: None,
            last_outcome: None,
            created_at_ms: now_ms,
            updated_at_ms: now_ms,
            next_execution_at_ms: Some(now_ms),
        };

        self.repo.save(&job).await?;
        Ok(job)
    }

    /// Pause an active monitor job with an explicit reason.
    pub async fn pause_job(
        &self,
        user_id: &str,
        conversation_id: &str,
        job_id: &str,
        reason: MonitorStopReason,
        now_ms: u64,
    ) -> Result<MonitorJob, MonitorError> {
        Self::validate_ownership_scope(user_id, conversation_id)?;

        let mut job = self
            .repo
            .get(user_id, conversation_id, job_id)
            .await?
            .ok_or_else(|| MonitorError::NotFound(job_id.to_owned()))?;

        if job.status != MonitorJobStatus::Active {
            return Err(MonitorError::InvalidLifecycleTransition {
                current: job.status,
                requested: "pause".into(),
            });
        }

        job.status = MonitorJobStatus::Paused;
        job.stop_reason = Some(reason);
        job.updated_at_ms = now_ms;
        job.next_execution_at_ms = None;

        self.repo.save(&job).await?;
        Ok(job)
    }

    /// Resume a paused monitor job.
    pub async fn resume_job(
        &self,
        user_id: &str,
        conversation_id: &str,
        job_id: &str,
        now_ms: u64,
    ) -> Result<MonitorJob, MonitorError> {
        Self::validate_ownership_scope(user_id, conversation_id)?;

        let mut job = self
            .repo
            .get(user_id, conversation_id, job_id)
            .await?
            .ok_or_else(|| MonitorError::NotFound(job_id.to_owned()))?;

        if job.status != MonitorJobStatus::Paused {
            return Err(MonitorError::InvalidLifecycleTransition {
                current: job.status,
                requested: "resume".into(),
            });
        }

        job.status = MonitorJobStatus::Active;
        job.stop_reason = None;
        job.updated_at_ms = now_ms;
        job.next_execution_at_ms = Some(now_ms);

        self.repo.save(&job).await?;
        Ok(job)
    }

    /// Cancel a monitor job permanently.
    pub async fn cancel_job(
        &self,
        user_id: &str,
        conversation_id: &str,
        job_id: &str,
        reason: MonitorStopReason,
        now_ms: u64,
    ) -> Result<MonitorJob, MonitorError> {
        Self::validate_ownership_scope(user_id, conversation_id)?;

        let mut job = self
            .repo
            .get(user_id, conversation_id, job_id)
            .await?
            .ok_or_else(|| MonitorError::NotFound(job_id.to_owned()))?;

        if job.status == MonitorJobStatus::Cancelled {
            return Ok(job);
        }

        job.status = MonitorJobStatus::Cancelled;
        job.stop_reason = Some(reason);
        job.updated_at_ms = now_ms;
        job.next_execution_at_ms = None;

        self.repo.save(&job).await?;
        Ok(job)
    }

    /// Inspect a monitor job from the originating conversation.
    pub async fn get_job(
        &self,
        user_id: &str,
        conversation_id: &str,
        job_id: &str,
    ) -> Result<MonitorJob, MonitorError> {
        Self::validate_ownership_scope(user_id, conversation_id)?;

        self.repo
            .get(user_id, conversation_id, job_id)
            .await?
            .ok_or_else(|| MonitorError::NotFound(job_id.to_owned()))
    }

    /// List all monitor jobs bound to the originating conversation.
    pub async fn list_jobs(
        &self,
        user_id: &str,
        conversation_id: &str,
    ) -> Result<Vec<MonitorJob>, MonitorError> {
        Self::validate_ownership_scope(user_id, conversation_id)?;
        self.repo.list_by_conversation(user_id, conversation_id).await
    }

    /// Hook invoked when originating conversation is closed, archived, or deleted.
    /// Automatically cancels all active or paused monitor jobs bound to that conversation.
    pub async fn on_conversation_ended(
        &self,
        user_id: &str,
        conversation_id: &str,
        reason: MonitorStopReason,
        now_ms: u64,
    ) -> Result<usize, MonitorError> {
        Self::validate_ownership_scope(user_id, conversation_id)?;

        let jobs = self.repo.list_by_conversation(user_id, conversation_id).await?;
        let mut cancelled_count = 0;

        for mut job in jobs {
            if job.status == MonitorJobStatus::Active || job.status == MonitorJobStatus::Paused {
                job.status = MonitorJobStatus::Cancelled;
                job.stop_reason = Some(reason.clone());
                job.updated_at_ms = now_ms;
                job.next_execution_at_ms = None;
                self.repo.save(&job).await?;
                cancelled_count += 1;
            }
        }

        Ok(cancelled_count)
    }
}
