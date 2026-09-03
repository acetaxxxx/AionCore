//! Conversation-bound Facebook MonitorJob control domain and public service seam.
//!
//! Implements Tickets 01, 02, 03, 04, 05, and 06:
//! - Conversation-owned `MonitorJob` domain aggregate and lifecycle.
//! - Complete target, query, lookback, and schedule scope enforcement.
//! - Supplied schedule acceptance vs. agent-proposed default schedule approval flow.
//! - Lifecycle control semantics: create, pause, resume, cancel, get, list, and conversation termination hook.
//! - Strict user and originating conversation scope isolation (fail-closed against cross-account/conversation access).
//! - Bounded occurrence execution with durable report persistence and idempotency.
//! - `MonitorCursor` scoped to job, target, and query revision.
//! - Single-target delta reporting: `New`, `Changed`, `Unchanged`, and `Backfill` observations.
//! - Distinction between monitor-seen, conversation-reported, and user-acknowledged states (`unread`/`needs_attention`).
//! - Atomic cursor advancement upon scan & durable report success; cursor retention on failure.
//! - Query revision, lookback rescan, and separate `Backfill` findings grouping.
//! - `FacebookProfile` server-authorized ownership and credentials privacy.
//! - Authentication failure (AuthExpired, Checkpoint, CAPTCHA) auto-pause without automatic retries.
//! - Interactive LiveView re-authentication resuming eligible jobs only for their next scheduled run.
//! - LiveView session precedence over background MonitorRun.
//! - Multi-target isolated evaluation: partial results, healthy group reporting on partial target failure.
//! - Post availability status: `Removed` (confirmed deletion) vs `Unavailable` (temporary access issue preserving last known content/time).
//! - Untrusted DOM / validation failure fails closed without advancing cursor.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::scheduler::{compute_cron_next_run, normalize_cron_expr, validate_cron_expression, validate_timezone};
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

    #[error("Invalid occurrence payload: {0}")]
    InvalidOccurrence(String),

    #[error("Database error: {0}")]
    Database(String),

    #[error("Repository error: {0}")]
    Repository(String),

    #[error("Profile busy: {0}")]
    ProfileBusy(String),

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

/// Classification of an observation compared against cursor state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObservationDeltaKind {
    New,
    Changed,
    Unchanged,
    Backfill,
    Removed,
    Unavailable,
}

/// Authentication state for a user-owned Facebook profile.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProfileAuthState {
    Authenticated,
    Expired,
    CheckpointRequired,
    CaptchaRequired,
}

/// User-owned browser storage profile reference.
/// Never stores passwords, MFA secrets, or unrestricted tokens.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FacebookProfile {
    pub id: String,
    pub user_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    pub auth_state: ProfileAuthState,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

impl FacebookProfile {
    pub fn new(id: impl Into<String>, user_id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            user_id: user_id.into(),
            display_name: None,
            auth_state: ProfileAuthState::Authenticated,
            created_at_ms: 0,
            updated_at_ms: 0,
        }
    }

    pub fn with_display_name(mut self, name: impl Into<String>) -> Self {
        self.display_name = Some(name.into());
        self
    }

    pub fn with_auth_state(mut self, auth_state: ProfileAuthState) -> Self {
        self.auth_state = auth_state;
        self
    }
}

/// Raw observation item discovered during a target scan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FacebookObservation {
    pub id: String,
    pub target_id: String,
    pub content_hash: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub author: Option<String>,
    #[serde(default)]
    pub published_at_ms: u64,
    #[serde(default)]
    pub is_confirmed_deleted: bool,
    #[serde(default)]
    pub is_temporarily_unavailable: bool,
}

impl FacebookObservation {
    pub fn new(id: impl Into<String>, target_id: impl Into<String>, content_hash: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            target_id: target_id.into(),
            content_hash: content_hash.into(),
            title: None,
            body: None,
            author: None,
            published_at_ms: 0,
            is_confirmed_deleted: false,
            is_temporarily_unavailable: false,
        }
    }

    pub fn confirmed_deleted(id: impl Into<String>, target_id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            target_id: target_id.into(),
            content_hash: String::new(),
            title: None,
            body: None,
            author: None,
            published_at_ms: 0,
            is_confirmed_deleted: true,
            is_temporarily_unavailable: false,
        }
    }

    pub fn temporarily_unavailable(id: impl Into<String>, target_id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            target_id: target_id.into(),
            content_hash: String::new(),
            title: None,
            body: None,
            author: None,
            published_at_ms: 0,
            is_confirmed_deleted: false,
            is_temporarily_unavailable: true,
        }
    }

    pub fn with_content(mut self, title: Option<String>, body: Option<String>, author: Option<String>) -> Self {
        self.title = title;
        self.body = body;
        self.author = author;
        self
    }

    pub fn with_published_at(mut self, published_at_ms: u64) -> Self {
        self.published_at_ms = published_at_ms;
        self
    }
}

/// State of an observation preserved in the MonitorCursor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CursorItemState {
    pub observation_id: String,
    pub target_id: String,
    pub content_hash: String,
    pub first_seen_at_ms: u64,
    pub last_seen_at_ms: u64,
    pub reported_at_ms: Option<u64>,
    pub acknowledged_at_ms: Option<u64>,
}

impl CursorItemState {
    pub fn is_acknowledged(&self) -> bool {
        self.acknowledged_at_ms.is_some()
    }

    pub fn is_unread_needs_attention(&self) -> bool {
        self.reported_at_ms.is_some() && self.acknowledged_at_ms.is_none()
    }
}

/// Durable observation cursor scoped to MonitorJob, Target, and Query Revision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MonitorCursor {
    pub job_id: String,
    pub target_id: String,
    pub query_revision: u64,
    pub last_successful_observed_at_ms: Option<u64>,
    pub items: HashMap<String, CursorItemState>,
    pub updated_at_ms: u64,
}

impl MonitorCursor {
    pub fn new(job_id: impl Into<String>, target_id: impl Into<String>, query_revision: u64) -> Self {
        Self {
            job_id: job_id.into(),
            target_id: target_id.into(),
            query_revision,
            last_successful_observed_at_ms: None,
            items: HashMap::new(),
            updated_at_ms: 0,
        }
    }
}

/// An observation reported in a run delta report.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReportedObservation {
    pub delta_kind: ObservationDeltaKind,
    pub observation: FacebookObservation,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query_revised_at_ms: Option<u64>,
}

// ---------------------------------------------------------------------------
// Proposal and Creation Payloads
// ---------------------------------------------------------------------------

/// Ephemeral domain proposal for an unapproved monitoring request.
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
// Scan Outcome and Multi-Target Types
// ---------------------------------------------------------------------------

/// Per-target scan outcome produced by a bounded runner execution.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TargetScanResult {
    pub target_id: String,
    pub outcome: MonitorRunOutcome,
    #[serde(default)]
    pub observations: Vec<FacebookObservation>,
    pub error_message: Option<String>,
    #[serde(default)]
    pub is_untrusted_structure: bool,
}

impl TargetScanResult {
    pub fn success(target_id: impl Into<String>, observations: Vec<FacebookObservation>) -> Self {
        Self {
            target_id: target_id.into(),
            outcome: MonitorRunOutcome::Success,
            observations,
            error_message: None,
            is_untrusted_structure: false,
        }
    }

    pub fn failed(target_id: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            target_id: target_id.into(),
            outcome: MonitorRunOutcome::Failed,
            observations: Vec::new(),
            error_message: Some(message.into()),
            is_untrusted_structure: false,
        }
    }

    pub fn unavailable(target_id: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            target_id: target_id.into(),
            outcome: MonitorRunOutcome::Unavailable,
            observations: Vec::new(),
            error_message: Some(message.into()),
            is_untrusted_structure: false,
        }
    }

    pub fn untrusted_dom(target_id: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            target_id: target_id.into(),
            outcome: MonitorRunOutcome::Failed,
            observations: Vec::new(),
            error_message: Some(message.into()),
            is_untrusted_structure: true,
        }
    }
}

/// Target failure record indicating target ID and failure reason.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TargetFailure {
    pub target_id: String,
    pub reason: String,
}

/// Overall scan outcome produced by a bounded runner execution.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MonitorScanResult {
    pub outcome: MonitorRunOutcome,
    pub observations_count: usize,
    pub error_message: Option<String>,
    #[serde(default)]
    pub observations: Vec<FacebookObservation>,
    #[serde(default)]
    pub target_results: Vec<TargetScanResult>,
}

impl MonitorScanResult {
    pub fn success(observations: Vec<FacebookObservation>) -> Self {
        let count = observations.len();
        Self {
            outcome: MonitorRunOutcome::Success,
            observations_count: count,
            error_message: None,
            observations,
            target_results: Vec::new(),
        }
    }

    pub fn multi_target(target_results: Vec<TargetScanResult>) -> Self {
        let any_failed = target_results
            .iter()
            .any(|r| r.outcome != MonitorRunOutcome::Success || r.is_untrusted_structure);
        let all_failed = target_results
            .iter()
            .all(|r| r.outcome != MonitorRunOutcome::Success || r.is_untrusted_structure);
        let outcome = if all_failed {
            MonitorRunOutcome::Failed
        } else if any_failed {
            MonitorRunOutcome::Partial
        } else {
            MonitorRunOutcome::Success
        };
        let observations_count = target_results.iter().map(|r| r.observations.len()).sum();
        Self {
            outcome,
            observations_count,
            error_message: None,
            observations: Vec::new(),
            target_results,
        }
    }

    pub fn failed(message: impl Into<String>) -> Self {
        Self {
            outcome: MonitorRunOutcome::Failed,
            observations_count: 0,
            error_message: Some(message.into()),
            observations: Vec::new(),
            target_results: Vec::new(),
        }
    }

    pub fn auth_expired(message: impl Into<String>) -> Self {
        Self {
            outcome: MonitorRunOutcome::AuthExpired,
            observations_count: 0,
            error_message: Some(message.into()),
            observations: Vec::new(),
            target_results: Vec::new(),
        }
    }
}

/// Durable, bounded result for one scheduled occurrence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MonitorRunReport {
    pub job_id: String,
    pub user_id: String,
    pub conversation_id: String,
    pub scheduled_at_ms: u64,
    pub outcome: MonitorRunOutcome,
    pub observations_count: usize,
    pub error_message: Option<String>,
    #[serde(default)]
    pub reported_observations: Vec<ReportedObservation>,
    #[serde(default)]
    pub target_id: Option<String>,
    #[serde(default)]
    pub query_revision: Option<u64>,
    #[serde(default)]
    pub lookback_window_ms: Option<u64>,
    #[serde(default)]
    pub successful_targets: Vec<String>,
    #[serde(default)]
    pub failed_targets: Vec<TargetFailure>,
}

impl MonitorRunReport {
    pub fn new_findings(&self) -> Vec<&FacebookObservation> {
        self.reported_observations
            .iter()
            .filter(|r| r.delta_kind == ObservationDeltaKind::New)
            .map(|r| &r.observation)
            .collect()
    }

    pub fn changed_findings(&self) -> Vec<&FacebookObservation> {
        self.reported_observations
            .iter()
            .filter(|r| r.delta_kind == ObservationDeltaKind::Changed)
            .map(|r| &r.observation)
            .collect()
    }

    pub fn backfill_findings(&self) -> Vec<&FacebookObservation> {
        self.reported_observations
            .iter()
            .filter(|r| r.delta_kind == ObservationDeltaKind::Backfill)
            .map(|r| &r.observation)
            .collect()
    }

    pub fn removed_findings(&self) -> Vec<&FacebookObservation> {
        self.reported_observations
            .iter()
            .filter(|r| r.delta_kind == ObservationDeltaKind::Removed)
            .map(|r| &r.observation)
            .collect()
    }

    pub fn unavailable_findings(&self) -> Vec<&FacebookObservation> {
        self.reported_observations
            .iter()
            .filter(|r| r.delta_kind == ObservationDeltaKind::Unavailable)
            .map(|r| &r.observation)
            .collect()
    }

    /// Formatted presentation grouping findings separately with
    /// query revision and lookback window shown.
    pub fn format_conversation_report(&self) -> String {
        let mut out = String::new();
        let rev = self.query_revision.unwrap_or(1);
        let lookback_desc = self
            .lookback_window_ms
            .map(|ms| format!("{}ms", ms))
            .unwrap_or_else(|| "default".into());

        let backfills = self.backfill_findings();
        let news = self.new_findings();
        let changed = self.changed_findings();
        let removed = self.removed_findings();
        let unavailable = self.unavailable_findings();

        if !backfills.is_empty() {
            out.push_str(&format!(
                "### Backfill Findings (Query Revision {}, Lookback {})\n",
                rev, lookback_desc
            ));
            for item in backfills {
                out.push_str(&format!("- [Backfill] {} (hash: {})\n", item.id, item.content_hash));
            }
        }

        if !news.is_empty() {
            out.push_str("### New Findings\n");
            for item in news {
                out.push_str(&format!("- [New] {} (hash: {})\n", item.id, item.content_hash));
            }
        }

        if !changed.is_empty() {
            out.push_str("### Changed Findings\n");
            for item in changed {
                out.push_str(&format!("- [Changed] {} (hash: {})\n", item.id, item.content_hash));
            }
        }

        if !removed.is_empty() {
            out.push_str("### Removed Findings\n");
            for item in removed {
                out.push_str(&format!("- [Removed] {}\n", item.id));
            }
        }

        if !unavailable.is_empty() {
            out.push_str("### Unavailable Findings\n");
            for item in unavailable {
                out.push_str(&format!("- [Unavailable] {}\n", item.id));
            }
        }

        if !self.failed_targets.is_empty() {
            out.push_str("### Failed Targets\n");
            for f in &self.failed_targets {
                out.push_str(&format!("- Target {}: {}\n", f.target_id, f.reason));
            }
        }

        out
    }
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
    async fn get(&self, user_id: &str, conversation_id: &str, job_id: &str)
    -> Result<Option<MonitorJob>, MonitorError>;
    async fn list_by_conversation(&self, user_id: &str, conversation_id: &str)
    -> Result<Vec<MonitorJob>, MonitorError>;
    async fn delete(&self, user_id: &str, conversation_id: &str, job_id: &str) -> Result<bool, MonitorError>;
    async fn get_run_report(
        &self,
        user_id: &str,
        conversation_id: &str,
        job_id: &str,
        scheduled_at_ms: u64,
    ) -> Result<Option<MonitorRunReport>, MonitorError>;
    async fn save_run_report(&self, report: &MonitorRunReport) -> Result<(), MonitorError>;
    /// Atomically persist the canonical report, the job's post-run state, and updated cursors.
    async fn save_run_completion(
        &self,
        report: &MonitorRunReport,
        job: &MonitorJob,
        cursors: &[MonitorCursor],
    ) -> Result<MonitorRunReport, MonitorError>;
    async fn list_run_reports(
        &self,
        user_id: &str,
        conversation_id: &str,
        job_id: &str,
    ) -> Result<Vec<MonitorRunReport>, MonitorError>;
    async fn get_cursor(
        &self,
        user_id: &str,
        conversation_id: &str,
        job_id: &str,
        target_id: &str,
        query_revision: u64,
    ) -> Result<Option<MonitorCursor>, MonitorError>;
    async fn save_cursor(
        &self,
        user_id: &str,
        conversation_id: &str,
        cursor: &MonitorCursor,
    ) -> Result<(), MonitorError>;
    async fn save_profile(&self, profile: &FacebookProfile) -> Result<(), MonitorError>;
    async fn get_profile(&self, user_id: &str, profile_id: &str) -> Result<Option<FacebookProfile>, MonitorError>;
    async fn find_profile_by_id(&self, profile_id: &str) -> Result<Option<FacebookProfile>, MonitorError>;
}

#[derive(Default)]
pub struct InMemoryMonitorJobRepository {
    jobs: RwLock<HashMap<String, MonitorJob>>,
    run_reports: RwLock<HashMap<String, MonitorRunReport>>,
    cursors: RwLock<HashMap<String, MonitorCursor>>,
    profiles: RwLock<HashMap<String, FacebookProfile>>,
}

impl InMemoryMonitorJobRepository {
    pub fn new() -> Self {
        Self {
            jobs: RwLock::new(HashMap::new()),
            run_reports: RwLock::new(HashMap::new()),
            cursors: RwLock::new(HashMap::new()),
            profiles: RwLock::new(HashMap::new()),
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

    async fn get(
        &self,
        user_id: &str,
        conversation_id: &str,
        job_id: &str,
    ) -> Result<Option<MonitorJob>, MonitorError> {
        let guard = self.jobs.read().await;
        if let Some(job) = guard.get(job_id) {
            if job.user_id == user_id && job.conversation_id == conversation_id {
                return Ok(Some(job.clone()));
            }
        }
        Ok(None)
    }

    async fn list_by_conversation(
        &self,
        user_id: &str,
        conversation_id: &str,
    ) -> Result<Vec<MonitorJob>, MonitorError> {
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

    async fn get_run_report(
        &self,
        user_id: &str,
        conversation_id: &str,
        job_id: &str,
        scheduled_at_ms: u64,
    ) -> Result<Option<MonitorRunReport>, MonitorError> {
        let key = run_report_key(user_id, conversation_id, job_id, scheduled_at_ms);
        let guard = self.run_reports.read().await;
        Ok(guard.get(&key).cloned())
    }

    async fn save_run_report(&self, report: &MonitorRunReport) -> Result<(), MonitorError> {
        let key = run_report_key(
            &report.user_id,
            &report.conversation_id,
            &report.job_id,
            report.scheduled_at_ms,
        );
        let mut guard = self.run_reports.write().await;
        guard.insert(key, report.clone());
        Ok(())
    }

    async fn save_run_completion(
        &self,
        report: &MonitorRunReport,
        job: &MonitorJob,
        cursors: &[MonitorCursor],
    ) -> Result<MonitorRunReport, MonitorError> {
        let report_key = run_report_key(
            &report.user_id,
            &report.conversation_id,
            &report.job_id,
            report.scheduled_at_ms,
        );
        let mut reports = self.run_reports.write().await;
        let mut jobs = self.jobs.write().await;
        let canonical = reports.entry(report_key).or_insert_with(|| report.clone()).clone();
        jobs.insert(job.id.clone(), job.clone());

        let mut cursors_guard = self.cursors.write().await;
        for c in cursors {
            let ckey = cursor_key(
                &job.user_id,
                &job.conversation_id,
                &c.job_id,
                &c.target_id,
                c.query_revision,
            );
            cursors_guard.insert(ckey, c.clone());
        }

        Ok(canonical)
    }

    async fn list_run_reports(
        &self,
        user_id: &str,
        conversation_id: &str,
        job_id: &str,
    ) -> Result<Vec<MonitorRunReport>, MonitorError> {
        let prefix = format!("{user_id}\0{conversation_id}\0{job_id}\0");
        let mut reports: Vec<_> = self
            .run_reports
            .read()
            .await
            .iter()
            .filter(|(key, _)| key.starts_with(&prefix))
            .map(|(_, report)| report.clone())
            .collect();
        reports.sort_by_key(|report| report.scheduled_at_ms);
        Ok(reports)
    }

    async fn get_cursor(
        &self,
        user_id: &str,
        conversation_id: &str,
        job_id: &str,
        target_id: &str,
        query_revision: u64,
    ) -> Result<Option<MonitorCursor>, MonitorError> {
        let key = cursor_key(user_id, conversation_id, job_id, target_id, query_revision);
        let guard = self.cursors.read().await;
        Ok(guard.get(&key).cloned())
    }

    async fn save_cursor(
        &self,
        user_id: &str,
        conversation_id: &str,
        cursor: &MonitorCursor,
    ) -> Result<(), MonitorError> {
        let key = cursor_key(
            user_id,
            conversation_id,
            &cursor.job_id,
            &cursor.target_id,
            cursor.query_revision,
        );
        let mut guard = self.cursors.write().await;
        guard.insert(key, cursor.clone());
        Ok(())
    }

    async fn save_profile(&self, profile: &FacebookProfile) -> Result<(), MonitorError> {
        let mut guard = self.profiles.write().await;
        guard.insert(profile.id.clone(), profile.clone());
        Ok(())
    }

    async fn get_profile(&self, user_id: &str, profile_id: &str) -> Result<Option<FacebookProfile>, MonitorError> {
        let guard = self.profiles.read().await;
        if let Some(prof) = guard.get(profile_id) {
            if prof.user_id == user_id {
                return Ok(Some(prof.clone()));
            }
        }
        Ok(None)
    }

    async fn find_profile_by_id(&self, profile_id: &str) -> Result<Option<FacebookProfile>, MonitorError> {
        let guard = self.profiles.read().await;
        Ok(guard.get(profile_id).cloned())
    }
}

fn run_report_key(user_id: &str, conversation_id: &str, job_id: &str, scheduled_at_ms: u64) -> String {
    format!("{user_id}\0{conversation_id}\0{job_id}\0{scheduled_at_ms}")
}

fn cursor_key(user_id: &str, conversation_id: &str, job_id: &str, target_id: &str, query_revision: u64) -> String {
    format!("{user_id}\0{conversation_id}\0{job_id}\0{target_id}\0{query_revision}")
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
    occurrence_locks: Arc<Mutex<HashMap<String, Arc<Mutex<()>>>>>,
    liveview_sessions: Arc<Mutex<HashSet<String>>>,
}

impl MonitorControlService {
    pub fn new(repo: Arc<dyn IMonitorJobRepository>) -> Self {
        Self {
            repo,
            occurrence_locks: Arc::new(Mutex::new(HashMap::new())),
            liveview_sessions: Arc::new(Mutex::new(HashSet::new())),
        }
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

    /// Register a server-authorized user-owned FacebookProfile.
    pub async fn register_profile(&self, profile: FacebookProfile) -> Result<(), MonitorError> {
        self.repo.save_profile(&profile).await
    }

    /// Retrieve a user's FacebookProfile by id.
    pub async fn get_profile(&self, user_id: &str, profile_id: &str) -> Result<Option<FacebookProfile>, MonitorError> {
        self.repo.get_profile(user_id, profile_id).await
    }

    /// Begin an interactive LiveView session for a profile, taking precedence over background runs.
    pub async fn start_liveview_session(&self, user_id: &str, profile_id: &str) -> Result<(), MonitorError> {
        if let Some(prof) = self.repo.find_profile_by_id(profile_id).await? {
            if prof.user_id != user_id {
                return Err(MonitorError::AccessDenied("Profile belongs to another user".into()));
            }
        }
        let mut guard = self.liveview_sessions.lock().await;
        guard.insert(profile_id.to_string());
        Ok(())
    }

    /// Terminate an interactive LiveView session.
    pub async fn end_liveview_session(&self, profile_id: &str) {
        let mut guard = self.liveview_sessions.lock().await;
        guard.remove(profile_id);
    }

    /// Check if LiveView is currently active for a profile.
    pub async fn is_liveview_active(&self, profile_id: &str) -> bool {
        let guard = self.liveview_sessions.lock().await;
        guard.contains(profile_id)
    }

    /// Create a new MonitorJob explicitly bound to the originating conversation.
    pub async fn create_job(
        &self,
        user_id: &str,
        conversation_id: &str,
        req: CreateMonitorJobRequest,
        now_ms: u64,
    ) -> Result<CreateMonitorJobOutcome, MonitorError> {
        Self::validate_ownership_scope(user_id, conversation_id)?;
        Self::validate_scope(&req.targets, &req.query, &req.lookback)?;

        if let Some(ref pid) = req.profile_ref {
            if let Some(existing) = self.repo.find_profile_by_id(pid).await? {
                if existing.user_id != user_id {
                    return Err(MonitorError::AccessDenied(format!(
                        "Profile '{pid}' does not belong to caller '{user_id}'"
                    )));
                }
            }
        }

        match req.schedule {
            Some(supplied_schedule) => {
                validate_schedule(&supplied_schedule)?;

                let job_id = aionui_common::generate_prefixed_id("mon");
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
                    query_revised_at_ms: None,
                };

                self.repo.save(&job).await?;
                Ok(CreateMonitorJobOutcome::Active { job })
            }
            None => {
                let proposed = propose_default_schedule();
                let proposal_id = aionui_common::generate_prefixed_id("prop");
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

                Ok(CreateMonitorJobOutcome::RequiresApproval { proposal })
            }
        }
    }

    /// Approve an agent proposal to create the durable MonitorJob.
    pub async fn approve_proposal(
        &self,
        user_id: &str,
        conversation_id: &str,
        proposal: MonitorJobProposal,
        approved_schedule: Option<CronSchedule>,
        now_ms: u64,
    ) -> Result<MonitorJob, MonitorError> {
        Self::validate_ownership_scope(user_id, conversation_id)?;

        if proposal.user_id != user_id || proposal.conversation_id != conversation_id {
            return Err(MonitorError::AccessDenied(
                "Proposal ownership does not match caller context".into(),
            ));
        }

        Self::validate_scope(&proposal.targets, &proposal.query, &proposal.lookback)?;

        if let Some(ref pid) = proposal.profile_ref {
            if let Some(existing) = self.repo.find_profile_by_id(pid).await? {
                if existing.user_id != user_id {
                    return Err(MonitorError::AccessDenied(format!(
                        "Profile '{pid}' does not belong to caller '{user_id}'"
                    )));
                }
            }
        }

        let final_schedule = match approved_schedule {
            Some(sched) => sched,
            None => proposal.proposed_schedule,
        };

        validate_schedule(&final_schedule)?;

        let job_id = aionui_common::generate_prefixed_id("mon");
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
            query_revised_at_ms: None,
        };

        self.repo.save(&job).await?;
        Ok(job)
    }

    /// Update the query text, optional filters, and optional lookback for an active or paused job.
    pub async fn update_query(
        &self,
        user_id: &str,
        conversation_id: &str,
        job_id: &str,
        query_text: impl Into<String>,
        filters: Option<HashMap<String, String>>,
        lookback: Option<LookbackScope>,
        now_ms: u64,
    ) -> Result<MonitorJob, MonitorError> {
        Self::validate_ownership_scope(user_id, conversation_id)?;

        let text = query_text.into();
        if text.trim().is_empty() {
            return Err(MonitorError::InvalidQueryScope("query_text must not be empty".into()));
        }

        if let Some(ref lb) = lookback {
            if lb.duration_ms == 0 {
                return Err(MonitorError::InvalidLookbackScope(
                    "lookback duration_ms must be greater than 0".into(),
                ));
            }
        }

        let mut job = self
            .repo
            .get(user_id, conversation_id, job_id)
            .await?
            .ok_or_else(|| MonitorError::NotFound(job_id.to_owned()))?;

        if job.status == MonitorJobStatus::Cancelled {
            return Err(MonitorError::InvalidLifecycleTransition {
                current: job.status,
                requested: "update_query".into(),
            });
        }

        let new_revision = job.query.revision + 1;
        job.query = MonitorQuery::new(text)
            .with_filters(filters.unwrap_or_default())
            .with_revision(new_revision);

        if let Some(lb) = lookback {
            job.lookback = lb;
        }

        job.query_revised_at_ms = Some(now_ms);
        job.updated_at_ms = now_ms;

        self.repo.save(&job).await?;
        Ok(job)
    }

    /// Complete interactive LiveView re-authentication for a profile.
    /// Resumes eligible paused jobs in the same conversation using this profile
    /// for their NEXT scheduled occurrence only.
    pub async fn complete_liveview_reauth(
        &self,
        user_id: &str,
        conversation_id: &str,
        profile_id: &str,
        now_ms: u64,
    ) -> Result<Vec<MonitorJob>, MonitorError> {
        Self::validate_ownership_scope(user_id, conversation_id)?;

        if let Some(mut prof) = self.repo.find_profile_by_id(profile_id).await? {
            if prof.user_id != user_id {
                return Err(MonitorError::AccessDenied("Profile belongs to another user".into()));
            }
            prof.auth_state = ProfileAuthState::Authenticated;
            prof.updated_at_ms = now_ms;
            self.repo.save_profile(&prof).await?;
        }

        self.end_liveview_session(profile_id).await;

        let jobs = self.repo.list_by_conversation(user_id, conversation_id).await?;
        let mut resumed_jobs = Vec::new();

        for mut job in jobs {
            if job.profile_ref.as_deref() == Some(profile_id) && job.status == MonitorJobStatus::Paused {
                let is_auth_paused = matches!(
                    job.stop_reason,
                    Some(MonitorStopReason::AuthExpired)
                        | Some(MonitorStopReason::CheckpointDetected)
                        | Some(MonitorStopReason::CaptchaDetected)
                );

                if is_auth_paused {
                    job.status = MonitorJobStatus::Active;
                    job.stop_reason = None;
                    job.updated_at_ms = now_ms;
                    job.next_execution_at_ms = compute_next_run_after_occurrence(&job.schedule, now_ms, now_ms);
                    self.repo.save(&job).await?;
                    resumed_jobs.push(job);
                }
            }
        }

        Ok(resumed_jobs)
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
    pub async fn list_jobs(&self, user_id: &str, conversation_id: &str) -> Result<Vec<MonitorJob>, MonitorError> {
        Self::validate_ownership_scope(user_id, conversation_id)?;
        self.repo.list_by_conversation(user_id, conversation_id).await
    }

    /// Get cursor state for a specific job, target, and query revision.
    pub async fn get_cursor(
        &self,
        user_id: &str,
        conversation_id: &str,
        job_id: &str,
        target_id: &str,
        query_revision: u64,
    ) -> Result<Option<MonitorCursor>, MonitorError> {
        Self::validate_ownership_scope(user_id, conversation_id)?;
        self.repo
            .get_cursor(user_id, conversation_id, job_id, target_id, query_revision)
            .await
    }

    /// Mark an observation as acknowledged by the user.
    pub async fn acknowledge_observation(
        &self,
        user_id: &str,
        conversation_id: &str,
        job_id: &str,
        target_id: &str,
        query_revision: u64,
        observation_id: &str,
        ack_time_ms: u64,
    ) -> Result<CursorItemState, MonitorError> {
        Self::validate_ownership_scope(user_id, conversation_id)?;

        let mut cursor = self
            .repo
            .get_cursor(user_id, conversation_id, job_id, target_id, query_revision)
            .await?
            .ok_or_else(|| {
                MonitorError::NotFound(format!(
                    "Cursor not found for job {job_id} target {target_id} rev {query_revision}"
                ))
            })?;

        let item = cursor
            .items
            .get_mut(observation_id)
            .ok_or_else(|| MonitorError::NotFound(format!("Observation {observation_id} not found in cursor")))?;

        item.acknowledged_at_ms = Some(ack_time_ms);
        let updated = item.clone();

        cursor.updated_at_ms = ack_time_ms;
        self.repo.save_cursor(user_id, conversation_id, &cursor).await?;

        Ok(updated)
    }

    /// Run one bounded occurrence for an existing active job and persist its
    /// report in the originating conversation scope. Supports multi-target independent
    /// evaluation, partial results, removed vs unavailable posts, and untrusted DOM fail-closed.
    pub async fn run_occurrence(
        &self,
        user_id: &str,
        conversation_id: &str,
        job_id: &str,
        scheduled_at_ms: u64,
        runner: &dyn MonitorRunner,
    ) -> Result<MonitorRunReport, MonitorError> {
        Self::validate_ownership_scope(user_id, conversation_id)?;
        if scheduled_at_ms == 0 {
            return Err(MonitorError::InvalidOccurrence(
                "scheduled_at_ms must be greater than 0".into(),
            ));
        }

        let occurrence_key = run_report_key(user_id, conversation_id, job_id, scheduled_at_ms);
        let occurrence_lock = {
            let mut locks = self.occurrence_locks.lock().await;
            locks
                .entry(occurrence_key)
                .or_insert_with(|| Arc::new(Mutex::new(())))
                .clone()
        };
        let _occurrence_guard = occurrence_lock.lock().await;

        let mut job = self
            .repo
            .get(user_id, conversation_id, job_id)
            .await?
            .ok_or_else(|| MonitorError::NotFound(job_id.to_owned()))?;
        if job.status != MonitorJobStatus::Active {
            return Err(MonitorError::InvalidLifecycleTransition {
                current: job.status,
                requested: "run_occurrence".into(),
            });
        }

        // LiveView precedence: background monitor yields if LiveView is active
        if let Some(ref pid) = job.profile_ref {
            if self.is_liveview_active(pid).await {
                return Err(MonitorError::ProfileBusy(format!(
                    "LiveView session actively in progress for profile '{pid}'"
                )));
            }
        }

        // Repeated delivery is idempotent for the same occurrence
        if let Some(existing) = self
            .repo
            .get_run_report(user_id, conversation_id, job_id, scheduled_at_ms)
            .await?
        {
            return Ok(existing);
        }

        let scan = match runner.run_scan(&job).await {
            Ok(scan) => scan,
            Err(error_message) => {
                let lower = error_message.to_lowercase();
                let outcome = if lower.contains("checkpoint") || lower.contains("captcha") {
                    MonitorRunOutcome::AuthExpired
                } else {
                    MonitorRunOutcome::Failed
                };
                MonitorScanResult {
                    outcome,
                    observations_count: 0,
                    error_message: Some(error_message),
                    observations: Vec::new(),
                    target_results: Vec::new(),
                }
            }
        };

        let query_revision = job.query.revision;
        let mut delta_observations = Vec::new();
        let mut updated_cursors = Vec::new();
        let mut successful_targets = Vec::new();
        let mut failed_targets = Vec::new();

        // Evaluate targets independently
        for target in &job.targets {
            let tid = &target.target_id;

            // Find target scan result if present, or construct from top-level scan
            let target_result = if !scan.target_results.is_empty() {
                scan.target_results.iter().find(|tr| &tr.target_id == tid).cloned()
            } else {
                // Backwards compatibility for single-target runners
                let target_obs: Vec<FacebookObservation> = scan
                    .observations
                    .iter()
                    .filter(|o| o.target_id.is_empty() || &o.target_id == tid)
                    .cloned()
                    .collect();
                Some(TargetScanResult {
                    target_id: tid.clone(),
                    outcome: scan.outcome.clone(),
                    observations: target_obs,
                    error_message: scan.error_message.clone(),
                    is_untrusted_structure: false,
                })
            };

            match target_result {
                Some(tr) if tr.is_untrusted_structure => {
                    // Untrusted DOM structure or unknown layout fails closed: never advances cursor
                    failed_targets.push(TargetFailure {
                        target_id: tid.clone(),
                        reason: tr
                            .error_message
                            .unwrap_or("Untrusted DOM structure / changed layout".into()),
                    });
                }
                Some(tr) if tr.outcome == MonitorRunOutcome::Success => {
                    successful_targets.push(tid.clone());

                    let mut cursor = self
                        .repo
                        .get_cursor(user_id, conversation_id, job_id, tid, query_revision)
                        .await?
                        .unwrap_or_else(|| MonitorCursor::new(job_id, tid, query_revision));

                    for obs in &tr.observations {
                        if obs.is_confirmed_deleted {
                            if cursor.items.contains_key(&obs.id) {
                                delta_observations.push(ReportedObservation {
                                    delta_kind: ObservationDeltaKind::Removed,
                                    observation: obs.clone(),
                                });
                                cursor.items.remove(&obs.id);
                            }
                        } else if obs.is_temporarily_unavailable {
                            if cursor.items.contains_key(&obs.id) {
                                delta_observations.push(ReportedObservation {
                                    delta_kind: ObservationDeltaKind::Unavailable,
                                    observation: obs.clone(),
                                });
                                // Preserves last known content and observation time; never deletes!
                            }
                        } else {
                            let delta_kind = match cursor.items.get(&obs.id) {
                                None => {
                                    if job.query.revision > 1 {
                                        let revised_at = job.query_revised_at_ms.unwrap_or(scheduled_at_ms);
                                        if obs.published_at_ms > 0 && obs.published_at_ms < revised_at {
                                            ObservationDeltaKind::Backfill
                                        } else if obs.published_at_ms >= revised_at {
                                            ObservationDeltaKind::New
                                        } else {
                                            ObservationDeltaKind::Backfill
                                        }
                                    } else {
                                        ObservationDeltaKind::New
                                    }
                                }
                                Some(existing) => {
                                    if existing.content_hash != obs.content_hash {
                                        ObservationDeltaKind::Changed
                                    } else {
                                        ObservationDeltaKind::Unchanged
                                    }
                                }
                            };

                            match delta_kind {
                                ObservationDeltaKind::New | ObservationDeltaKind::Backfill => {
                                    cursor.items.insert(
                                        obs.id.clone(),
                                        CursorItemState {
                                            observation_id: obs.id.clone(),
                                            target_id: tid.clone(),
                                            content_hash: obs.content_hash.clone(),
                                            first_seen_at_ms: scheduled_at_ms,
                                            last_seen_at_ms: scheduled_at_ms,
                                            reported_at_ms: Some(scheduled_at_ms),
                                            acknowledged_at_ms: None,
                                        },
                                    );
                                    delta_observations.push(ReportedObservation {
                                        delta_kind,
                                        observation: obs.clone(),
                                    });
                                }
                                ObservationDeltaKind::Changed => {
                                    let first_seen = cursor
                                        .items
                                        .get(&obs.id)
                                        .map(|i| i.first_seen_at_ms)
                                        .unwrap_or(scheduled_at_ms);
                                    cursor.items.insert(
                                        obs.id.clone(),
                                        CursorItemState {
                                            observation_id: obs.id.clone(),
                                            target_id: tid.clone(),
                                            content_hash: obs.content_hash.clone(),
                                            first_seen_at_ms: first_seen,
                                            last_seen_at_ms: scheduled_at_ms,
                                            reported_at_ms: Some(scheduled_at_ms),
                                            acknowledged_at_ms: None,
                                        },
                                    );
                                    delta_observations.push(ReportedObservation {
                                        delta_kind,
                                        observation: obs.clone(),
                                    });
                                }
                                ObservationDeltaKind::Unchanged => {
                                    if let Some(existing) = cursor.items.get_mut(&obs.id) {
                                        existing.last_seen_at_ms = scheduled_at_ms;
                                    }
                                }
                                _ => {}
                            }
                        }
                    }

                    cursor.last_successful_observed_at_ms = Some(scheduled_at_ms);
                    cursor.updated_at_ms = scheduled_at_ms;
                    updated_cursors.push(cursor);
                }
                Some(tr) => {
                    // Target failed or unavailable: preserves existing cursor, does not advance
                    let reason = tr
                        .error_message
                        .unwrap_or_else(|| format!("Target scan returned {:?}", tr.outcome));
                    failed_targets.push(TargetFailure {
                        target_id: tid.clone(),
                        reason,
                    });
                }
                None => {
                    failed_targets.push(TargetFailure {
                        target_id: tid.clone(),
                        reason: "Target was not scanned by runner".into(),
                    });
                }
            }
        }

        // Determine aggregate outcome
        let aggregate_outcome = if scan.outcome == MonitorRunOutcome::AuthExpired {
            MonitorRunOutcome::AuthExpired
        } else if successful_targets.is_empty() {
            if failed_targets
                .iter()
                .all(|f| f.reason.to_lowercase().contains("unavailable"))
            {
                MonitorRunOutcome::Unavailable
            } else {
                MonitorRunOutcome::Failed
            }
        } else if !failed_targets.is_empty() {
            MonitorRunOutcome::Partial
        } else {
            MonitorRunOutcome::Success
        };

        let observations_count = if !delta_observations.is_empty() {
            delta_observations.len()
        } else {
            scan.observations_count
        };

        let primary_target_id = job.targets.first().map(|t| t.target_id.clone());

        let report = MonitorRunReport {
            job_id: job.id.clone(),
            user_id: job.user_id.clone(),
            conversation_id: job.conversation_id.clone(),
            scheduled_at_ms,
            outcome: aggregate_outcome.clone(),
            observations_count,
            error_message: scan.error_message.clone(),
            reported_observations: delta_observations,
            target_id: primary_target_id,
            query_revision: Some(query_revision),
            lookback_window_ms: Some(job.lookback.duration_ms),
            successful_targets,
            failed_targets,
        };

        job.last_outcome = Some(aggregate_outcome.clone());
        job.updated_at_ms = job.updated_at_ms.max(scheduled_at_ms);

        // Check authentication expiry, checkpoint, or CAPTCHA: auto-pause without automatic retries
        let auth_pause_reason = match aggregate_outcome {
            MonitorRunOutcome::AuthExpired => Some(MonitorStopReason::AuthExpired),
            _ => {
                if let Some(ref msg) = scan.error_message {
                    let lower = msg.to_lowercase();
                    if lower.contains("checkpoint") {
                        Some(MonitorStopReason::CheckpointDetected)
                    } else if lower.contains("captcha") {
                        Some(MonitorStopReason::CaptchaDetected)
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
        };

        if let Some(reason) = auth_pause_reason {
            job.status = MonitorJobStatus::Paused;
            job.stop_reason = Some(reason.clone());
            job.next_execution_at_ms = None;

            if let Some(ref pid) = job.profile_ref {
                if let Some(mut prof) = self.repo.get_profile(user_id, pid).await? {
                    prof.auth_state = match reason {
                        MonitorStopReason::AuthExpired => ProfileAuthState::Expired,
                        MonitorStopReason::CheckpointDetected => ProfileAuthState::CheckpointRequired,
                        MonitorStopReason::CaptchaDetected => ProfileAuthState::CaptchaRequired,
                        _ => ProfileAuthState::Expired,
                    };
                    prof.updated_at_ms = scheduled_at_ms;
                    let _ = self.repo.save_profile(&prof).await;
                }
            }
        } else {
            job.next_execution_at_ms =
                compute_next_run_after_occurrence(&job.schedule, scheduled_at_ms, scheduled_at_ms);
        }

        // Durable atomic save
        self.repo.save_run_completion(&report, &job, &updated_cursors).await
    }

    /// List reports for a job in its originating conversation scope.
    pub async fn list_run_reports(
        &self,
        user_id: &str,
        conversation_id: &str,
        job_id: &str,
    ) -> Result<Vec<MonitorRunReport>, MonitorError> {
        Self::validate_ownership_scope(user_id, conversation_id)?;
        self.repo.list_run_reports(user_id, conversation_id, job_id).await
    }

    /// Hook invoked when originating conversation is closed, archived, or deleted.
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

// ---------------------------------------------------------------------------
// Schedule helper
// ---------------------------------------------------------------------------

fn compute_next_run_after_occurrence(schedule: &CronSchedule, scheduled_at_ms: u64, now_ms: u64) -> Option<u64> {
    match schedule {
        CronSchedule::At { at_ms, .. } => {
            let at_u64 = *at_ms as u64;
            if at_u64 > scheduled_at_ms && at_u64 > now_ms {
                Some(at_u64)
            } else {
                None
            }
        }
        CronSchedule::Every { every_ms, .. } => {
            if *every_ms <= 0 {
                return None;
            }
            let step = *every_ms as u64;
            let mut next = scheduled_at_ms.saturating_add(step);
            while next <= now_ms {
                next = next.saturating_add(step);
            }
            Some(next)
        }
        CronSchedule::Cron { expr, tz, .. } => {
            let base_ms = (scheduled_at_ms.max(now_ms)) as i64;
            compute_cron_next_run(expr, tz.as_deref(), base_ms).map(|ts| ts as u64)
        }
    }
}
