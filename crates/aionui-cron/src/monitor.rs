//! Conversation-bound Facebook MonitorJob control domain and public service seam.
//!
//! This module implements Ticket 01:
//! - Conversation-owned `MonitorJob` domain aggregate and lifecycle.
//! - Complete target, query, and lookback scope enforcement.
//! - Supplied schedule acceptance vs. agent-proposed default schedule approval flow.
//! - Lifecycle control semantics: create, pause, resume, cancel, get, list, and conversation termination hook.
//! - Strict user and originating conversation scope isolation (fail-closed against cross-account/conversation access).
//! - Public `MonitorRunner` seam for future execution tickets.

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use serde::{Deserialize, Serialize};
use thiserror::Error;

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

    #[error("Monitor job not found: {0}")]
    NotFound(String),

    #[error("Access denied: {0}")]
    AccessDenied(String),

    #[error("Invalid lifecycle transition: current status is {current:?}, cannot perform {requested}")]
    InvalidLifecycleTransition {
        current: MonitorJobStatus,
        requested: String,
    },

    #[error("Proposal already resolved for job: {0}")]
    ProposalAlreadyResolved(String),
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
    /// Created with an agent-proposed default schedule; awaiting user approval before activation.
    PendingApproval,
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
// Request and Outcome Payloads
// ---------------------------------------------------------------------------

/// Request payload to create a new MonitorJob.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreateMonitorJobRequest {
    pub targets: Vec<FacebookTarget>,
    pub query: MonitorQuery,
    pub lookback: LookbackScope,
    /// If supplied, accepted without redundant confirmation. If omitted,
    /// an agent-proposed default is generated requiring user approval.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schedule: Option<CronSchedule>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profile_ref: Option<String>,
}

/// Outcome of attempting to create a MonitorJob.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CreateMonitorJobOutcome {
    /// A complete schedule was supplied; job is immediately active.
    Active { job: MonitorJob },
    /// Schedule was omitted; agent proposed a default schedule and awaits user approval.
    RequiresApproval {
        job: MonitorJob,
        proposed_schedule: CronSchedule,
    },
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
            // If user_id or conversation_id does not match, return None (fail closed).
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
// Default Schedule Policy
// ---------------------------------------------------------------------------

/// Propose a safe default monitoring schedule (every 6 hours) when user omits schedule.
pub fn propose_default_schedule() -> CronSchedule {
    CronSchedule::Every {
        every_ms: 6 * 60 * 60 * 1000,
        description: Some("Agent-proposed default schedule: every 6 hours".to_owned()),
    }
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

    /// Validate complete target, query, and lookback scope.
    fn validate_scope(req: &CreateMonitorJobRequest) -> Result<(), MonitorError> {
        if req.targets.is_empty() {
            return Err(MonitorError::IncompleteScope("targets must not be empty".into()));
        }
        for (i, t) in req.targets.iter().enumerate() {
            if t.target_id.trim().is_empty() {
                return Err(MonitorError::InvalidTargetScope(format!(
                    "target at index {i} has empty target_id"
                )));
            }
        }

        if req.query.query_text.trim().is_empty() {
            return Err(MonitorError::InvalidQueryScope("query_text must not be empty".into()));
        }

        if req.lookback.duration_ms == 0 {
            return Err(MonitorError::InvalidLookbackScope(
                "lookback duration_ms must be greater than 0".into(),
            ));
        }

        Ok(())
    }

    /// Create a new MonitorJob explicitly bound to the originating conversation.
    /// Rejects incomplete scope. If schedule is omitted, creates with `PendingApproval`
    /// status and returns `RequiresApproval`.
    pub async fn create_job(
        &self,
        user_id: &str,
        conversation_id: &str,
        req: CreateMonitorJobRequest,
        now_ms: u64,
    ) -> Result<CreateMonitorJobOutcome, MonitorError> {
        if user_id.trim().is_empty() || conversation_id.trim().is_empty() {
            return Err(MonitorError::IncompleteScope(
                "user_id and conversation_id must be non-empty".into(),
            ));
        }

        Self::validate_scope(&req)?;

        let job_id = format!("mon_{}", uuid::Uuid::now_v7());

        match req.schedule {
            Some(supplied_schedule) => {
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
                let job = MonitorJob {
                    id: job_id,
                    user_id: user_id.to_owned(),
                    conversation_id: conversation_id.to_owned(),
                    targets: req.targets,
                    query: req.query,
                    lookback: req.lookback,
                    schedule: proposed.clone(),
                    profile_ref: req.profile_ref,
                    status: MonitorJobStatus::PendingApproval,
                    stop_reason: None,
                    last_outcome: None,
                    created_at_ms: now_ms,
                    updated_at_ms: now_ms,
                    next_execution_at_ms: None,
                };

                self.repo.save(&job).await?;
                Ok(CreateMonitorJobOutcome::RequiresApproval {
                    job,
                    proposed_schedule: proposed,
                })
            }
        }
    }

    /// Approve an agent-proposed schedule for a job in `PendingApproval` state.
    pub async fn approve_proposal(
        &self,
        user_id: &str,
        conversation_id: &str,
        job_id: &str,
        approved_schedule: Option<CronSchedule>,
        now_ms: u64,
    ) -> Result<MonitorJob, MonitorError> {
        let mut job = self
            .repo
            .get(user_id, conversation_id, job_id)
            .await?
            .ok_or_else(|| MonitorError::NotFound(job_id.to_owned()))?;

        if job.status != MonitorJobStatus::PendingApproval {
            return Err(MonitorError::ProposalAlreadyResolved(format!(
                "Job {job_id} is in status {:?}, not PendingApproval",
                job.status
            )));
        }

        if let Some(schedule) = approved_schedule {
            job.schedule = schedule;
        }
        job.status = MonitorJobStatus::Active;
        job.updated_at_ms = now_ms;
        job.next_execution_at_ms = Some(now_ms);

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
        let jobs = self.repo.list_by_conversation(user_id, conversation_id).await?;
        let mut cancelled_count = 0;

        for mut job in jobs {
            if job.status == MonitorJobStatus::Active || job.status == MonitorJobStatus::Paused || job.status == MonitorJobStatus::PendingApproval {
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

// ---------------------------------------------------------------------------
// Unit Tests for MonitorJob Control
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_target() -> FacebookTarget {
        FacebookTarget::new("fb_group_123").with_display_name("VIP Deals")
    }

    fn sample_query() -> MonitorQuery {
        MonitorQuery::new("discount coupon")
    }

    fn sample_lookback() -> LookbackScope {
        LookbackScope::from_days(7)
    }

    fn sample_schedule() -> CronSchedule {
        CronSchedule::Every {
            every_ms: 3600000,
            description: Some("hourly".into()),
        }
    }

    // 1. Explicit complete creation with supplied schedule
    #[tokio::test]
    async fn test_explicit_create_with_supplied_schedule_becomes_active() {
        let svc = MonitorControlService::with_in_memory_repo();
        let req = CreateMonitorJobRequest {
            targets: vec![sample_target()],
            query: sample_query(),
            lookback: sample_lookback(),
            schedule: Some(sample_schedule()),
            profile_ref: Some("profile_1".into()),
        };

        let outcome = svc.create_job("user_1", "conv_1", req, 1000).await.unwrap();

        match outcome {
            CreateMonitorJobOutcome::Active { job } => {
                assert_eq!(job.user_id, "user_1");
                assert_eq!(job.conversation_id, "conv_1");
                assert_eq!(job.status, MonitorJobStatus::Active);
                assert_eq!(job.targets.len(), 1);
                assert_eq!(job.targets[0].target_id, "fb_group_123");
                assert_eq!(job.query.query_text, "discount coupon");
                assert_eq!(job.lookback.duration_ms, 7 * 24 * 3600 * 1000);
                assert_eq!(job.schedule, sample_schedule());
                assert_eq!(job.profile_ref.as_deref(), Some("profile_1"));
                assert_eq!(job.created_at_ms, 1000);
                assert_eq!(job.updated_at_ms, 1000);
                assert_eq!(job.next_execution_at_ms, Some(1000));
                assert!(job.stop_reason.is_none());
            }
            CreateMonitorJobOutcome::RequiresApproval { .. } => {
                panic!("Expected immediate active job when schedule was provided");
            }
        }
    }

    // 2. Omitted schedule produces agent-proposed default requiring approval
    #[tokio::test]
    async fn test_omitted_schedule_proposes_default_and_requires_approval() {
        let svc = MonitorControlService::with_in_memory_repo();
        let req = CreateMonitorJobRequest {
            targets: vec![sample_target()],
            query: sample_query(),
            lookback: sample_lookback(),
            schedule: None,
            profile_ref: None,
        };

        let outcome = svc.create_job("user_1", "conv_1", req, 1000).await.unwrap();

        let job_id = match outcome {
            CreateMonitorJobOutcome::RequiresApproval { job, proposed_schedule } => {
                assert_eq!(job.status, MonitorJobStatus::PendingApproval);
                assert!(job.next_execution_at_ms.is_none());
                assert_eq!(proposed_schedule, propose_default_schedule());
                job.id
            }
            CreateMonitorJobOutcome::Active { .. } => panic!("Expected approval required"),
        };

        // Approving the proposed schedule transitions to Active
        let approved_job = svc
            .approve_proposal("user_1", "conv_1", &job_id, None, 2000)
            .await
            .unwrap();
        assert_eq!(approved_job.status, MonitorJobStatus::Active);
        assert_eq!(approved_job.updated_at_ms, 2000);
        assert_eq!(approved_job.next_execution_at_ms, Some(2000));

        // Approving already approved job fails
        let err = svc
            .approve_proposal("user_1", "conv_1", &job_id, None, 3000)
            .await
            .unwrap_err();
        assert!(matches!(err, MonitorError::ProposalAlreadyResolved(_)));
    }

    // 3. Incomplete scope validation
    #[tokio::test]
    async fn test_empty_targets_rejected() {
        let svc = MonitorControlService::with_in_memory_repo();
        let req = CreateMonitorJobRequest {
            targets: vec![],
            query: sample_query(),
            lookback: sample_lookback(),
            schedule: Some(sample_schedule()),
            profile_ref: None,
        };

        let err = svc.create_job("user_1", "conv_1", req, 1000).await.unwrap_err();
        assert!(matches!(err, MonitorError::IncompleteScope(_)));
    }

    #[tokio::test]
    async fn test_blank_target_id_rejected() {
        let svc = MonitorControlService::with_in_memory_repo();
        let req = CreateMonitorJobRequest {
            targets: vec![FacebookTarget::new("   ")],
            query: sample_query(),
            lookback: sample_lookback(),
            schedule: Some(sample_schedule()),
            profile_ref: None,
        };

        let err = svc.create_job("user_1", "conv_1", req, 1000).await.unwrap_err();
        assert!(matches!(err, MonitorError::InvalidTargetScope(_)));
    }

    #[tokio::test]
    async fn test_blank_query_rejected() {
        let svc = MonitorControlService::with_in_memory_repo();
        let req = CreateMonitorJobRequest {
            targets: vec![sample_target()],
            query: MonitorQuery::new("   "),
            lookback: sample_lookback(),
            schedule: Some(sample_schedule()),
            profile_ref: None,
        };

        let err = svc.create_job("user_1", "conv_1", req, 1000).await.unwrap_err();
        assert!(matches!(err, MonitorError::InvalidQueryScope(_)));
    }

    #[tokio::test]
    async fn test_zero_lookback_rejected() {
        let svc = MonitorControlService::with_in_memory_repo();
        let req = CreateMonitorJobRequest {
            targets: vec![sample_target()],
            query: sample_query(),
            lookback: LookbackScope::from_millis(0),
            schedule: Some(sample_schedule()),
            profile_ref: None,
        };

        let err = svc.create_job("user_1", "conv_1", req, 1000).await.unwrap_err();
        assert!(matches!(err, MonitorError::InvalidLookbackScope(_)));
    }

    // 4. Pause, Resume, Cancel lifecycle transitions
    #[tokio::test]
    async fn test_pause_and_resume_lifecycle() {
        let svc = MonitorControlService::with_in_memory_repo();
        let req = CreateMonitorJobRequest {
            targets: vec![sample_target()],
            query: sample_query(),
            lookback: sample_lookback(),
            schedule: Some(sample_schedule()),
            profile_ref: None,
        };

        let outcome = svc.create_job("user_1", "conv_1", req, 1000).await.unwrap();
        let job_id = match outcome {
            CreateMonitorJobOutcome::Active { job } => job.id,
            _ => panic!("Expected active"),
        };

        // Pause job on auth expiry
        let paused = svc
            .pause_job("user_1", "conv_1", &job_id, MonitorStopReason::AuthExpired, 2000)
            .await
            .unwrap();
        assert_eq!(paused.status, MonitorJobStatus::Paused);
        assert_eq!(paused.stop_reason, Some(MonitorStopReason::AuthExpired));
        assert!(paused.next_execution_at_ms.is_none());

        // Pausing already paused job fails
        let err = svc
            .pause_job("user_1", "conv_1", &job_id, MonitorStopReason::ExplicitUserPause, 2500)
            .await
            .unwrap_err();
        assert!(matches!(err, MonitorError::InvalidLifecycleTransition { .. }));

        // Resume job
        let resumed = svc.resume_job("user_1", "conv_1", &job_id, 3000).await.unwrap();
        assert_eq!(resumed.status, MonitorJobStatus::Active);
        assert!(resumed.stop_reason.is_none());
        assert_eq!(resumed.next_execution_at_ms, Some(3000));

        // Resuming active job fails
        let err = svc.resume_job("user_1", "conv_1", &job_id, 3500).await.unwrap_err();
        assert!(matches!(err, MonitorError::InvalidLifecycleTransition { .. }));

        // Cancel job
        let cancelled = svc
            .cancel_job(
                "user_1",
                "conv_1",
                &job_id,
                MonitorStopReason::ExplicitUserCancellation,
                4000,
            )
            .await
            .unwrap();
        assert_eq!(cancelled.status, MonitorJobStatus::Cancelled);
        assert_eq!(
            cancelled.stop_reason,
            Some(MonitorStopReason::ExplicitUserCancellation)
        );

        // Resuming cancelled job fails
        let err = svc.resume_job("user_1", "conv_1", &job_id, 5000).await.unwrap_err();
        assert!(matches!(err, MonitorError::InvalidLifecycleTransition { .. }));
    }

    // 5. User and Conversation Scope Isolation
    #[tokio::test]
    async fn test_user_and_conversation_isolation() {
        let svc = MonitorControlService::with_in_memory_repo();
        let req = CreateMonitorJobRequest {
            targets: vec![sample_target()],
            query: sample_query(),
            lookback: sample_lookback(),
            schedule: Some(sample_schedule()),
            profile_ref: None,
        };

        let outcome = svc.create_job("user_1", "conv_1", req, 1000).await.unwrap();
        let job_id = match outcome {
            CreateMonitorJobOutcome::Active { job } => job.id,
            _ => panic!("Expected active"),
        };

        // User 2 cannot access user 1's job
        let err = svc.get_job("user_2", "conv_1", &job_id).await.unwrap_err();
        assert!(matches!(err, MonitorError::NotFound(_)));

        let err = svc
            .pause_job("user_2", "conv_1", &job_id, MonitorStopReason::ExplicitUserPause, 2000)
            .await
            .unwrap_err();
        assert!(matches!(err, MonitorError::NotFound(_)));

        let err = svc
            .cancel_job(
                "user_2",
                "conv_1",
                &job_id,
                MonitorStopReason::ExplicitUserCancellation,
                2000,
            )
            .await
            .unwrap_err();
        assert!(matches!(err, MonitorError::NotFound(_)));

        // Conversation 2 cannot access conversation 1's job (even for same user)
        let err = svc.get_job("user_1", "conv_2", &job_id).await.unwrap_err();
        assert!(matches!(err, MonitorError::NotFound(_)));

        let list_conv2 = svc.list_jobs("user_1", "conv_2").await.unwrap();
        assert!(list_conv2.is_empty());

        let list_conv1 = svc.list_jobs("user_1", "conv_1").await.unwrap();
        assert_eq!(list_conv1.len(), 1);
        assert_eq!(list_conv1[0].id, job_id);
    }

    // 6. Conversation termination hook
    #[tokio::test]
    async fn test_conversation_ended_cancels_all_jobs_for_that_conversation() {
        let svc = MonitorControlService::with_in_memory_repo();

        // Create job 1 in conv_1
        let req1 = CreateMonitorJobRequest {
            targets: vec![sample_target()],
            query: sample_query(),
            lookback: sample_lookback(),
            schedule: Some(sample_schedule()),
            profile_ref: None,
        };
        let out1 = svc.create_job("user_1", "conv_1", req1, 1000).await.unwrap();
        let job1_id = match out1 {
            CreateMonitorJobOutcome::Active { job } => job.id,
            _ => panic!("Expected active"),
        };

        // Create job 2 in conv_2 (should NOT be affected)
        let req2 = CreateMonitorJobRequest {
            targets: vec![sample_target()],
            query: sample_query(),
            lookback: sample_lookback(),
            schedule: Some(sample_schedule()),
            profile_ref: None,
        };
        let out2 = svc.create_job("user_1", "conv_2", req2, 1000).await.unwrap();
        let job2_id = match out2 {
            CreateMonitorJobOutcome::Active { job } => job.id,
            _ => panic!("Expected active"),
        };

        // Conversation 1 is closed
        let count = svc
            .on_conversation_ended("user_1", "conv_1", MonitorStopReason::ConversationClosed, 2000)
            .await
            .unwrap();
        assert_eq!(count, 1);

        let job1 = svc.get_job("user_1", "conv_1", &job1_id).await.unwrap();
        assert_eq!(job1.status, MonitorJobStatus::Cancelled);
        assert_eq!(job1.stop_reason, Some(MonitorStopReason::ConversationClosed));

        let job2 = svc.get_job("user_1", "conv_2", &job2_id).await.unwrap();
        assert_eq!(job2.status, MonitorJobStatus::Active);
    }
}
