mod describe_support;
mod response_builder;
pub(crate) mod spawn_support;

use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock, Weak};
use std::time::{Duration, Instant};

use aionui_ai_agent::{ActiveLeaseRegistry, AgentError, AgentInstance, IWorkerTaskManager, IdleCleanupCoordinator};
use aionui_api_types::ChatFileRef;
use aionui_api_types::{
    AddAgentRequest, AssistantMcpBindingChanged, ConfirmationListResponse, ConversationArtifactListResponse,
    ConversationResponse, CreateTeamRequest, EligibleTeamCollaboratorResponse, GetConfigOptionsResponse,
    InterruptTeamAgentRequest, ListMessagesQuery, MessageListResponse, MessageResponse, SetConfigOptionRequest,
    SetConfigOptionResponse, SlashCommandItem, TeamActivityCursor, TeamActivityPageResponse, TeamAgentResponse,
    TeamAgentRuntimeStatus, TeamContextResetAvailability, TeamContextResetResponse, TeamContextResetRuntimeStatus,
    TeamContextResetStatus, TeamInterruptAgentResponse, TeamMailboxMessageResponse, TeamMemberResponse, TeamResponse,
    TeamRunAckResponse, TeamRunStateResponse, TeamSessionBinding, TeamSessionPhase, TeamSessionStatus,
    TeamSessionStatusPayload, TeamTaskResponse, TeamToolCall, TeamToolContextResponse, TeamToolErrorCode,
    TeamToolErrorPayload, TeamToolTransport, TeamUploadResponse, WebSocketMessage,
};
use aionui_common::{AgentKillReason, ConversationStatus, TimestampMs, generate_id, now_ms};
use aionui_db::models::{MAX_ELIGIBLE_TEAM_USERS, TeamAccessRole, TeamRow, TeamSharingMode};
use aionui_db::{
    ActivityCursor, IAgentMetadataRepository, IAssistantDefinitionRepository, IAssistantOverlayRepository,
    IProviderRepository, ITeamRepository, IUserOrderStore, OrderItemRef, OrderItemType, PageDirection,
    UpdateTeamParams,
};
use aionui_project::{ProjectService, canonical};
use aionui_realtime::EventBroadcaster;
use dashmap::DashMap;
use tracing::{debug, info, warn};

use crate::activity_mapping::{
    mailbox_row_to_response, message_row_to_activity_item, sort_activity_items, task_row_to_activity_item,
    task_to_response,
};
use crate::error::TeamError;
use crate::event_loop::{AgentLoopContext, EventLoopRegistrationError};
use crate::events::{
    TEAM_CREATED_EVENT, TEAM_REMOVED_EVENT, TEAM_RENAMED_EVENT, TEAM_SESSION_STATUS_CHANGED_EVENT, TeamEventEmitter,
};
use crate::member_runtime::{
    AttachLease, AttachOutcome, AttachWaiter, BeginRemove, MemberRuntimeFailure, MemberRuntimeSnapshot, ReserveAttach,
};
use crate::message_projection::TeamProjectionMessageStore;
use crate::ports::{
    AgentTurnCancellationPort, AgentTurnExecutionPort, NativeSlashCommandPort, NoopNativeSlashCommandPort,
    TeamAssistantCatalogPort, TeamToolCapabilityPort, UnknownTeamToolCapabilityPort,
};
use crate::prompt_dump::TeamPromptDumpConfig;
use crate::provisioning::{TeamAgentProvisioner, TeamConversationProvisioningPort};
use crate::runtime_tools::{
    ResolvedTeamToolContext, agent_for_conversation, error_payload, execute_with_scheduler, role_to_tool_role,
};
use crate::session::{
    AgentMessageQueueResult, TeamSession, attach_member_runtime, attach_member_runtime_after_kill,
    spawn_attach_agent_process_bg,
};
use crate::team_run::TeamRunManager;
use crate::types::{Team, TeamAgent, TeamTask, TeammateRole};
use crate::work_coordinator::{
    McpRefreshDisposition, ObserveMessagesResult, RuntimeConstraint, RuntimeRestartRejection,
};
use crate::work_source::WorkSource;
use crate::workspace::validate_create_workspace_path;

pub(crate) const TEAM_UPLOAD_MAX_FILE_BYTES: usize = aionui_common::constants::UPLOAD_MAX_SIZE;
const TEAM_UPLOAD_MAX_STORAGE_BYTES: u64 = 100 * 1024 * 1024;
const TEAM_UPLOAD_MAX_FILE_COUNT: usize = 100;
const TEAM_UPLOAD_MAX_IN_FLIGHT: usize = 4;
const TEAM_UPLOAD_RATE_BURST: f64 = 20.0;
const TEAM_UPLOAD_RATE_REFILL_PER_SECOND: f64 = 1.0;
const TEAM_UPLOAD_RATE_STATE_TTL: Duration = Duration::from_secs(10 * 60);
const TEAM_UPLOAD_RATE_MAX_TEAMS: usize = 4096;

/// Default number of activity items returned when the client omits `limit`.
pub const DEFAULT_ACTIVITY_LIMIT: i64 = 500;
/// Hard upper bound for the activity `limit` query parameter.
pub const MAX_ACTIVITY_LIMIT: i64 = 1000;
/// Upper bound on how many task ids one dependency-resolution request may
/// look up, to bound query size regardless of client input.
pub const MAX_TASK_ID_LOOKUP: usize = 200;
/// Account references can be used once within five minutes of being listed.
const ELIGIBLE_ACCOUNT_REF_TTL_MS: TimestampMs = 5 * 60 * 1000;
const ELIGIBLE_LIST_RATE_LIMIT: Duration = Duration::from_secs(1);
const ELIGIBLE_OWNER_RECORD_IDLE_TTL: Duration = Duration::from_secs(10 * 60);
const ELIGIBLE_OWNER_RECORD_SWEEP_INTERVAL: Duration = Duration::from_secs(60);
const MAX_OUTSTANDING_ELIGIBLE_ACCOUNT_REFS_PER_OWNER: usize = MAX_ELIGIBLE_TEAM_USERS;

#[derive(Default)]
struct TeamUploadRateLimit {
    buckets: Mutex<HashMap<String, TeamUploadRateBucket>>,
}

struct TeamUploadRateBucket {
    tokens: f64,
    updated_at: Instant,
    last_seen_at: Instant,
}

impl TeamUploadRateLimit {
    fn check(&self, team_id: &str) -> Result<(), TeamError> {
        let now = Instant::now();
        let mut buckets = self.buckets.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        buckets.retain(|_, bucket| now.saturating_duration_since(bucket.last_seen_at) < TEAM_UPLOAD_RATE_STATE_TTL);

        if !buckets.contains_key(team_id) && buckets.len() >= TEAM_UPLOAD_RATE_MAX_TEAMS {
            return Err(TeamError::TeamUploadRateLimited);
        }

        let bucket = buckets.entry(team_id.to_owned()).or_insert(TeamUploadRateBucket {
            tokens: TEAM_UPLOAD_RATE_BURST,
            updated_at: now,
            last_seen_at: now,
        });
        let elapsed = now.saturating_duration_since(bucket.updated_at).as_secs_f64();
        bucket.tokens = (bucket.tokens + elapsed * TEAM_UPLOAD_RATE_REFILL_PER_SECOND).min(TEAM_UPLOAD_RATE_BURST);
        bucket.updated_at = now;
        bucket.last_seen_at = now;

        if bucket.tokens < 1.0 {
            return Err(TeamError::TeamUploadRateLimited);
        }

        bucket.tokens -= 1.0;
        Ok(())
    }
}
/// Which item kinds the unified activity feed returns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActivityKind {
    /// Merged messages and tasks.
    All,
    /// Messages only.
    Message,
    /// Tasks only.
    Task,
}

/// Authenticated actor and persisted Team execution owner after authorization.
/// The two identities intentionally remain separate for collaborator actions.
#[derive(Debug, Clone)]
pub struct TeamAuthorizationContext {
    pub actor_user_id: String,
    pub execution_owner_id: String,
    pub role: TeamAccessRole,
    pub sharing_mode: TeamSharingMode,
    pub team: TeamRow,
}

fn can_send_direct_team_message(role: TeamAccessRole, lead_slot_id: Option<&str>, target_slot_id: &str) -> bool {
    match role {
        TeamAccessRole::Owner => true,
        TeamAccessRole::Collaborator => lead_slot_id == Some(target_slot_id),
    }
}

/// Usernames for federated identities may be the email address itself. Keep
/// those values out of the owner-visible picker and use a per-list ordinal;
/// ordinary usernames remain the useful display label.
fn collaborator_display_label(username: &str, ordinal: usize) -> String {
    let username = username.trim();
    if username.contains('@') {
        format!("Account {ordinal}")
    } else {
        username.to_owned()
    }
}

pub(crate) fn inherit_team_workspace(extra: &mut serde_json::Value, workspace: &str) {
    if !workspace.trim().is_empty() {
        extra["workspace"] = serde_json::Value::String(workspace.to_owned());
    }
}

/// Why a member's model selection is being persisted. Decides whether a runtime
/// that is mid-start may block the write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ModelPersistTrigger {
    /// A direct preference update from the model endpoint. Nothing has been
    /// applied yet, so a runtime that is mid-start is a legitimate reason to
    /// refuse — the caller can retry once it settles.
    ExplicitRequest,
    /// The member's runtime has already accepted the switch through the generic
    /// config-option path. Persistence must go through regardless of runtime
    /// state: refusing would leave the roster disagreeing with a live runtime.
    RuntimeConfirmed,
}

impl ModelPersistTrigger {
    fn as_str(self) -> &'static str {
        match self {
            Self::ExplicitRequest => "explicit_request",
            Self::RuntimeConfirmed => "runtime_confirmed",
        }
    }
}

struct SessionEntry {
    session: Arc<TeamSession>,
    slow_monitor_handle: tokio::task::JoinHandle<()>,
}

#[derive(Clone)]
struct EligibleAccountRef {
    user_id: String,
    display_name: String,
    expires_at: TimestampMs,
}

#[derive(Default)]
struct EligibleAccountRefStore {
    /// Each authenticated owner has one aggregate ref budget across all Teams.
    by_owner: DashMap<String, OwnerEligibleAccountRefs>,
    /// Throttles the idle-owner sweep; ordinary requests never scan all owners.
    last_idle_sweep_at: Mutex<Option<Instant>>,
}

#[derive(Default)]
struct OwnerEligibleAccountRefs {
    last_listing_at: Option<Instant>,
    last_activity_at: Option<Instant>,
    /// The latest started list request per Team prevents a slow old response
    /// from replacing the references issued by a newer request.
    latest_listing_by_team: HashMap<String, (String, Instant)>,
    by_team: HashMap<String, HashMap<String, EligibleAccountRef>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EligibleAccountRefStoreError {
    OwnerQuotaReached,
    ListingSuperseded,
}

fn ensure_candidate_count_supported(count: usize) -> Result<(), TeamError> {
    if count > MAX_ELIGIBLE_TEAM_USERS {
        return Err(TeamError::EligibleCollaboratorCandidateLimitExceeded);
    }
    Ok(())
}

impl OwnerEligibleAccountRefs {
    fn prune_expired(&mut self, now: TimestampMs, instant: Instant) {
        self.by_team.retain(|_, refs| {
            refs.retain(|_, reference| reference.expires_at > now);
            !refs.is_empty()
        });
        self.latest_listing_by_team.retain(|_, (_, started_at)| {
            instant.saturating_duration_since(*started_at) < ELIGIBLE_OWNER_RECORD_IDLE_TTL
        });
    }

    fn can_evict(&self, now: Instant) -> bool {
        self.by_team.is_empty()
            && self.latest_listing_by_team.is_empty()
            && self
                .last_activity_at
                .is_none_or(|last| now.saturating_duration_since(last) >= ELIGIBLE_OWNER_RECORD_IDLE_TTL)
    }
}

impl EligibleAccountRefStore {
    fn sweep_idle_owners_if_due(&self, now: Instant, now_ms: TimestampMs) {
        let mut last_sweep = self
            .last_idle_sweep_at
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if last_sweep.is_some_and(|last| now.saturating_duration_since(last) < ELIGIBLE_OWNER_RECORD_SWEEP_INTERVAL) {
            return;
        }
        *last_sweep = Some(now);
        drop(last_sweep);

        self.by_owner.retain(|_, owner| {
            owner.prune_expired(now_ms, now);
            !owner.can_evict(now)
        });
    }

    fn begin_listing(&self, owner_user_id: &str, team_id: &str, now: Instant, now_ms: TimestampMs) -> Option<String> {
        self.sweep_idle_owners_if_due(now, now_ms);
        let mut owner = self.by_owner.entry(owner_user_id.to_owned()).or_default();
        owner.prune_expired(now_ms, now);
        owner.last_activity_at = Some(now);
        if owner
            .last_listing_at
            .is_some_and(|last| now.saturating_duration_since(last) < ELIGIBLE_LIST_RATE_LIMIT)
        {
            return None;
        }
        owner.last_listing_at = Some(now);
        let generation = generate_id();
        owner
            .latest_listing_by_team
            .insert(team_id.to_owned(), (generation.clone(), now));
        Some(generation)
    }

    fn ensure_capacity(
        &self,
        owner_user_id: &str,
        team_id: &str,
        replacement_count: usize,
        now: Instant,
        now_ms: TimestampMs,
    ) -> Result<(), EligibleAccountRefStoreError> {
        self.sweep_idle_owners_if_due(now, now_ms);
        let mut owner = self.by_owner.entry(owner_user_id.to_owned()).or_default();
        owner.prune_expired(now_ms, now);
        owner.last_activity_at = Some(now);
        Self::check_capacity(&owner, team_id, replacement_count)
    }

    fn replace(
        &self,
        owner_user_id: &str,
        team_id: &str,
        generation: &str,
        refs: HashMap<String, EligibleAccountRef>,
        now: Instant,
        now_ms: TimestampMs,
    ) -> Result<(), EligibleAccountRefStoreError> {
        self.sweep_idle_owners_if_due(now, now_ms);
        let mut owner = self.by_owner.entry(owner_user_id.to_owned()).or_default();
        owner.prune_expired(now_ms, now);
        owner.last_activity_at = Some(now);
        if owner
            .latest_listing_by_team
            .get(team_id)
            .is_none_or(|(latest, _)| latest.as_str() != generation)
        {
            return Err(EligibleAccountRefStoreError::ListingSuperseded);
        }
        Self::check_capacity(&owner, team_id, refs.len())?;
        if refs.is_empty() {
            owner.by_team.remove(team_id);
        } else {
            owner.by_team.insert(team_id.to_owned(), refs);
        }
        owner.latest_listing_by_team.remove(team_id);
        Ok(())
    }

    fn check_capacity(
        owner: &OwnerEligibleAccountRefs,
        team_id: &str,
        replacement_count: usize,
    ) -> Result<(), EligibleAccountRefStoreError> {
        let other_team_refs = owner
            .by_team
            .iter()
            .filter(|(existing_team_id, _)| existing_team_id.as_str() != team_id)
            .map(|(_, refs)| refs.len())
            .sum::<usize>();
        if other_team_refs.saturating_add(replacement_count) > MAX_OUTSTANDING_ELIGIBLE_ACCOUNT_REFS_PER_OWNER {
            return Err(EligibleAccountRefStoreError::OwnerQuotaReached);
        }
        Ok(())
    }

    fn take(
        &self,
        owner_user_id: &str,
        team_id: &str,
        account_ref: &str,
        now: Instant,
        now_ms: TimestampMs,
    ) -> Option<EligibleAccountRef> {
        self.sweep_idle_owners_if_due(now, now_ms);
        let mut owner = self.by_owner.get_mut(owner_user_id)?;
        owner.prune_expired(now_ms, now);
        owner.last_activity_at = Some(now);
        let (reference, team_is_empty) = {
            let team_refs = owner.by_team.get_mut(team_id)?;
            let reference = team_refs.remove(account_ref);
            (reference, team_refs.is_empty())
        };
        if team_is_empty {
            owner.by_team.remove(team_id);
        }
        reference
    }

    #[cfg(test)]
    fn len_for_owner(&self, owner_user_id: &str) -> usize {
        self.by_owner
            .get(owner_user_id)
            .map_or(0, |owner| owner.by_team.values().map(HashMap::len).sum())
    }

    #[cfg(test)]
    fn owner_count(&self) -> usize {
        self.by_owner.len()
    }
}

pub struct TeamIdleCleanupCoordinator {
    service: Arc<TeamSessionService>,
    active_leases: Arc<ActiveLeaseRegistry>,
}

impl TeamIdleCleanupCoordinator {
    pub fn new(service: Arc<TeamSessionService>, active_leases: Arc<ActiveLeaseRegistry>) -> Self {
        Self { service, active_leases }
    }
}

#[async_trait::async_trait]
impl IdleCleanupCoordinator for TeamIdleCleanupCoordinator {
    async fn cleanup_idle_conversations(
        &self,
        idle_conversation_ids: Vec<String>,
        idle_threshold_ms: TimestampMs,
    ) -> Vec<String> {
        self.service
            .cleanup_idle_team_runtime_tasks(idle_conversation_ids, &self.active_leases, idle_threshold_ms)
            .await
    }
}

struct MemberRuntimeReconcileWork {
    agent: TeamAgent,
    waiter: AttachWaiter,
    owner: Option<AttachLease>,
}

pub struct TeamSessionService {
    repo: Arc<dyn ITeamRepository>,
    agent_metadata_repo: Arc<dyn IAgentMetadataRepository>,
    assistant_catalog: Arc<dyn TeamAssistantCatalogPort>,
    assistant_definition_repo: Arc<dyn IAssistantDefinitionRepository>,
    assistant_overlay_repo: Arc<dyn IAssistantOverlayRepository>,
    provider_repo: Arc<dyn IProviderRepository>,
    conversation_port: Arc<dyn TeamConversationProvisioningPort>,
    projection_store: Arc<dyn TeamProjectionMessageStore>,
    broadcaster: Arc<dyn EventBroadcaster>,
    task_manager: Arc<dyn IWorkerTaskManager>,
    turn_port: Arc<dyn AgentTurnExecutionPort>,
    cancellation_port: Arc<dyn AgentTurnCancellationPort>,
    capability_port: Arc<dyn TeamToolCapabilityPort>,
    /// Native slash-command recognizer injected into each `TeamSession`
    /// (ELECTRON-3RN). No-op by default (see `NoopNativeSlashCommandPort`).
    slash_command_port: Arc<dyn NativeSlashCommandPort>,
    backend_binary_path: Arc<PathBuf>,
    prompt_dump: TeamPromptDumpConfig,
    sessions: Arc<DashMap<String, SessionEntry>>,
    /// Per-team mutex serializing membership mutations with session startup so
    /// callers cannot read-modify-write the `agents` JSON or rebuild a runtime
    /// session from a stale roster snapshot.
    add_agent_locks: Arc<DashMap<String, Weak<tokio::sync::Mutex<()>>>>,
    /// Per-team mutex serializing `ensure_session` so concurrent callers cannot
    /// race and start two sessions for the same team.
    ensure_session_locks: Arc<DashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    /// Short-lived, single-use account references scoped to the listing owner
    /// and Team. Raw Core user IDs never cross the API boundary.
    eligible_account_refs: EligibleAccountRefStore,
    /// Bounded, expiring Team-scoped upload buckets. Checked before multipart
    /// parsing so authenticated bursts cannot force unbounded body buffering.
    team_upload_rate_limit: TeamUploadRateLimit,
    /// One TeamSessionService is constructed for the Core app process; router
    /// clones share this process-wide bound on buffered upload bodies.
    team_upload_in_flight: Arc<tokio::sync::Semaphore>,
    /// Upload bytes live outside the mutable Team workspace so workspace
    /// writers cannot rename or replace the directory during staging.
    team_upload_storage_root: Arc<RwLock<Option<PathBuf>>>,
    /// Project-bind side branch (optional). `None` → team binding is a no-op,
    /// so team create/read behaves exactly as before.
    project_service: Arc<RwLock<Option<Arc<ProjectService>>>>,
    /// Sidebar ordering store (optional). Set → `remove_team` cascade-deletes the
    /// team's `user_order` rows (design §4.3, path 2). `None` → no-op, so team
    /// deletion behaves exactly as before.
    user_order: Arc<RwLock<Option<Arc<dyn IUserOrderStore>>>>,
    /// Back-pointer used by [`TeamSession::spawn_agent`] to reach DB-facing
    /// orchestration without threading the service through every session method.
    /// Stored as `Weak` so the session map does not create a strong cycle with
    /// the service that owns it. Set once during [`TeamSessionService::new`]
    /// via [`Arc::new_cyclic`].
    self_ref: Weak<TeamSessionService>,
}

impl TeamSessionService {
    fn team_membership_lock(&self, team_id: &str) -> Arc<tokio::sync::Mutex<()>> {
        match self.add_agent_locks.entry(team_id.to_owned()) {
            dashmap::mapref::entry::Entry::Occupied(mut entry) => {
                if let Some(lock) = entry.get().upgrade() {
                    lock
                } else {
                    let lock = Arc::new(tokio::sync::Mutex::new(()));
                    entry.insert(Arc::downgrade(&lock));
                    lock
                }
            }
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                let lock = Arc::new(tokio::sync::Mutex::new(()));
                entry.insert(Arc::downgrade(&lock));
                lock
            }
        }
    }

    fn prune_team_membership_lock(&self, team_id: &str, lock: &Arc<tokio::sync::Mutex<()>>) {
        {
            let entry = self.add_agent_locks.entry(team_id.to_owned());
            if let dashmap::mapref::entry::Entry::Occupied(entry) = entry
                && entry.get().ptr_eq(&Arc::downgrade(lock))
                && Arc::strong_count(lock) == 1
            {
                entry.remove();
            }
        }

        // Weak entries do not retain mutexes, but pruning expired keys here
        // keeps the registry from growing with deleted Team IDs.
        self.add_agent_locks.retain(|_, lock| lock.strong_count() > 0);
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        repo: Arc<dyn ITeamRepository>,
        agent_metadata_repo: Arc<dyn IAgentMetadataRepository>,
        assistant_catalog: Arc<dyn TeamAssistantCatalogPort>,
        assistant_definition_repo: Arc<dyn IAssistantDefinitionRepository>,
        assistant_overlay_repo: Arc<dyn IAssistantOverlayRepository>,
        provider_repo: Arc<dyn IProviderRepository>,
        conversation_port: Arc<dyn TeamConversationProvisioningPort>,
        projection_store: Arc<dyn TeamProjectionMessageStore>,
        broadcaster: Arc<dyn EventBroadcaster>,
        task_manager: Arc<dyn IWorkerTaskManager>,
        turn_port: Arc<dyn AgentTurnExecutionPort>,
        cancellation_port: Arc<dyn AgentTurnCancellationPort>,
        backend_binary_path: Arc<PathBuf>,
    ) -> Arc<Self> {
        Self::new_with_prompt_dump(
            repo,
            agent_metadata_repo,
            assistant_catalog,
            assistant_definition_repo,
            assistant_overlay_repo,
            provider_repo,
            conversation_port,
            projection_store,
            broadcaster,
            task_manager,
            turn_port,
            cancellation_port,
            Arc::new(NoopNativeSlashCommandPort),
            Arc::new(UnknownTeamToolCapabilityPort),
            backend_binary_path,
            TeamPromptDumpConfig::disabled(),
        )
    }

    /// Construct with an explicit backend-capability resolver while keeping
    /// the default no-op slash catalog and disabled prompt dump.
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_capability_port(
        repo: Arc<dyn ITeamRepository>,
        agent_metadata_repo: Arc<dyn IAgentMetadataRepository>,
        assistant_catalog: Arc<dyn TeamAssistantCatalogPort>,
        assistant_definition_repo: Arc<dyn IAssistantDefinitionRepository>,
        assistant_overlay_repo: Arc<dyn IAssistantOverlayRepository>,
        provider_repo: Arc<dyn IProviderRepository>,
        conversation_port: Arc<dyn TeamConversationProvisioningPort>,
        projection_store: Arc<dyn TeamProjectionMessageStore>,
        broadcaster: Arc<dyn EventBroadcaster>,
        task_manager: Arc<dyn IWorkerTaskManager>,
        turn_port: Arc<dyn AgentTurnExecutionPort>,
        cancellation_port: Arc<dyn AgentTurnCancellationPort>,
        capability_port: Arc<dyn TeamToolCapabilityPort>,
        backend_binary_path: Arc<PathBuf>,
    ) -> Arc<Self> {
        Self::new_with_prompt_dump(
            repo,
            agent_metadata_repo,
            assistant_catalog,
            assistant_definition_repo,
            assistant_overlay_repo,
            provider_repo,
            conversation_port,
            projection_store,
            broadcaster,
            task_manager,
            turn_port,
            cancellation_port,
            Arc::new(NoopNativeSlashCommandPort),
            capability_port,
            backend_binary_path,
            TeamPromptDumpConfig::disabled(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_with_prompt_dump(
        repo: Arc<dyn ITeamRepository>,
        agent_metadata_repo: Arc<dyn IAgentMetadataRepository>,
        assistant_catalog: Arc<dyn TeamAssistantCatalogPort>,
        assistant_definition_repo: Arc<dyn IAssistantDefinitionRepository>,
        assistant_overlay_repo: Arc<dyn IAssistantOverlayRepository>,
        provider_repo: Arc<dyn IProviderRepository>,
        conversation_port: Arc<dyn TeamConversationProvisioningPort>,
        projection_store: Arc<dyn TeamProjectionMessageStore>,
        broadcaster: Arc<dyn EventBroadcaster>,
        task_manager: Arc<dyn IWorkerTaskManager>,
        turn_port: Arc<dyn AgentTurnExecutionPort>,
        cancellation_port: Arc<dyn AgentTurnCancellationPort>,
        slash_command_port: Arc<dyn NativeSlashCommandPort>,
        capability_port: Arc<dyn TeamToolCapabilityPort>,
        backend_binary_path: Arc<PathBuf>,
        prompt_dump: TeamPromptDumpConfig,
    ) -> Arc<Self> {
        Arc::new_cyclic(|weak| Self {
            repo,
            agent_metadata_repo,
            assistant_catalog,
            assistant_definition_repo,
            assistant_overlay_repo,
            provider_repo,
            conversation_port,
            projection_store,
            broadcaster,
            task_manager,
            turn_port,
            cancellation_port,
            slash_command_port,
            capability_port,
            backend_binary_path,
            prompt_dump,
            sessions: Arc::new(DashMap::new()),
            add_agent_locks: Arc::new(DashMap::new()),
            ensure_session_locks: Arc::new(DashMap::new()),
            eligible_account_refs: EligibleAccountRefStore::default(),
            team_upload_rate_limit: TeamUploadRateLimit::default(),
            team_upload_in_flight: Arc::new(tokio::sync::Semaphore::new(TEAM_UPLOAD_MAX_IN_FLIGHT)),
            team_upload_storage_root: Arc::new(RwLock::new(None)),
            project_service: Arc::new(RwLock::new(None)),
            user_order: Arc::new(RwLock::new(None)),
            self_ref: weak.clone(),
        })
    }

    pub(crate) fn provisioner(&self) -> TeamAgentProvisioner {
        TeamAgentProvisioner::new(
            self.repo.clone(),
            self.agent_metadata_repo.clone(),
            self.assistant_catalog.clone(),
            self.provider_repo.clone(),
            self.conversation_port.clone(),
            self.capability_port.clone(),
        )
    }

    /// Apply an assistant MCP binding event to matching members in active team
    /// sessions. Persisted snapshots update immediately; ready idle runtimes are
    /// rebuilt now, while active work records a deferred refresh.
    pub async fn handle_assistant_mcp_binding_changed(&self, event: AssistantMcpBindingChanged) {
        let sessions = self
            .sessions
            .iter()
            .filter(|entry| entry.session.user_id() == event.user_id)
            .map(|entry| Arc::clone(&entry.session))
            .collect::<Vec<_>>();
        for session in sessions {
            let agents = session.scheduler().list_agents().await;
            for agent in agents
                .into_iter()
                .filter(|agent| agent.assistant_id.as_deref() == Some(event.assistant_id.as_str()))
            {
                self.refresh_member_mcp_binding(&session, &event.user_id, &agent).await;
            }
        }
    }

    /// Re-resolve the MCP binding of EVERY member in EVERY active session.
    ///
    /// Recovery path for when binding-change events were missed rather than
    /// observed — the shared event bus can drop events under load, and a dropped
    /// event would otherwise leave a member running a stale MCP set until its
    /// next attach. Idempotent: members whose fingerprint already matches take
    /// the `Unchanged` branch and are left alone.
    pub async fn reconcile_all_assistant_mcp_bindings(&self) {
        let sessions = self
            .sessions
            .iter()
            .map(|entry| Arc::clone(&entry.session))
            .collect::<Vec<_>>();
        let session_count = sessions.len();
        let mut member_count = 0usize;
        for session in sessions {
            let user_id = session.user_id().to_owned();
            for agent in session.scheduler().list_agents().await {
                member_count += 1;
                self.refresh_member_mcp_binding(&session, &user_id, &agent).await;
            }
        }
        info!(
            session_count,
            member_count, "reconciled assistant MCP bindings across active team sessions"
        );
    }

    /// Refresh one member's persisted MCP snapshot and decide what to do with its
    /// runtime: leave dormant/failed slots alone, defer while attaching or
    /// removing, and restart a ready idle runtime so it picks the new set up.
    async fn refresh_member_mcp_binding(&self, session: &Arc<TeamSession>, user_id: &str, agent: &TeamAgent) {
        let fingerprint = match self
            .provisioner()
            .refresh_agent_mcp_snapshot(user_id, session.team_id(), agent)
            .await
        {
            Ok(Some(fingerprint)) => fingerprint,
            Ok(None) => return,
            Err(error) => {
                warn!(
                    team_id = session.team_id(),
                    slot_id = agent.slot_id,
                    assistant_id = agent.assistant_id.as_deref().unwrap_or_default(),
                    error = %error,
                    "assistant MCP snapshot refresh failed"
                );
                return;
            }
        };
        match session.member_runtimes().snapshot(&agent.slot_id) {
            MemberRuntimeSnapshot::Absent
            | MemberRuntimeSnapshot::Failed { .. }
            | MemberRuntimeSnapshot::SessionStopped => {}
            MemberRuntimeSnapshot::Attaching { .. } | MemberRuntimeSnapshot::Removing { .. } => {
                session
                    .work_coordinator()
                    .defer_mcp_refresh(&agent.slot_id, &fingerprint);
            }
            MemberRuntimeSnapshot::Ready => {
                match session
                    .work_coordinator()
                    .request_mcp_refresh(&agent.slot_id, &fingerprint)
                {
                    McpRefreshDisposition::Unchanged | McpRefreshDisposition::Deferred => {}
                    McpRefreshDisposition::RestartNow => {
                        if let Err(error) = self
                            .restart_agent_runtime_for_mcp_refresh(user_id, session.team_id(), &agent.slot_id)
                            .await
                        {
                            session
                                .work_coordinator()
                                .defer_mcp_refresh(&agent.slot_id, &fingerprint);
                            warn!(
                                team_id = session.team_id(),
                                slot_id = agent.slot_id,
                                error = %error,
                                "assistant MCP runtime refresh deferred after restart race"
                            );
                        }
                    }
                }
            }
        }
    }

    /// Inject the project-bind service (project-bind side branch). When unset,
    /// binding/backfill are no-ops.
    pub fn with_project_service(&self, project_service: Arc<ProjectService>) {
        if let Ok(mut guard) = self.project_service.write() {
            *guard = Some(project_service);
        }
    }

    /// Inject the sidebar ordering store so `remove_team` cascade-deletes the
    /// team's `user_order` rows (design §4.3, path 2). When unset, the cascade is
    /// a no-op. Member conversations are handled separately by the conversation
    /// delete hook (they route through `ConversationService::delete`).
    pub fn with_user_order_store(&self, user_order: Arc<dyn IUserOrderStore>) {
        if let Ok(mut guard) = self.user_order.write() {
            *guard = Some(user_order);
        }
    }

    /// Configure Core-owned durable storage for opaque Team uploads.
    pub fn with_team_upload_storage_root(&self, root: PathBuf) {
        if let Ok(mut guard) = self.team_upload_storage_root.write() {
            *guard = Some(root);
        }
    }

    /// Best-effort cascade of a removed team's `user_order` row (design §4.3,
    /// path 2). Store unset → no-op. An error is logged, not propagated: an
    /// orphan `team` row self-heals on read (the pinned group only emits teams
    /// present in the live aggregate), so it must never block team deletion.
    async fn remove_team_order_row(&self, user_id: &str, team_id: &str) {
        let store = self.user_order.read().ok().and_then(|guard| guard.clone());
        let Some(store) = store else { return };
        let item = OrderItemRef::new(OrderItemType::Team, team_id);
        if let Err(err) = store.remove_item(user_id, &item).await {
            warn!(
                user_id = %user_id,
                team_id = %team_id,
                error = %err,
                "sidebar: failed to cascade-delete user_order row for removed team"
            );
        }
    }

    /// Resolve a team workspace into `(project_id, folder_id)`. Best-effort:
    /// missing service / empty workspace / bad URI / resolve error → `(None, None)`,
    /// logged at `warn`. Never affects team create/read.
    async fn resolve_binding_best_effort(&self, user_id: &str, workspace: &str) -> (Option<String>, Option<String>) {
        let project_service = self.project_service.read().ok().and_then(|guard| guard.clone());
        let Some(project_service) = project_service else {
            return (None, None);
        };
        if workspace.trim().is_empty() {
            return (None, None);
        }
        let uri = match canonical::to_file_uri(Path::new(workspace)) {
            Ok(uri) => uri,
            Err(err) => {
                warn!(error = err.code(), "team project bind skipped: bad workspace uri");
                return (None, None);
            }
        };
        match project_service.resolve_existing(user_id, uri).await {
            Ok(out) => (Some(out.project.project_id), Some(out.folder.folder_id)),
            Err(err) => {
                warn!(error = err.code(), "team project bind skipped");
                (None, None)
            }
        }
    }

    /// Lazily backfill `teams.project_id`/`folder_id` on read. Best-effort;
    /// no-op when already bound, workspace empty, or service unset.
    async fn backfill_team_binding_best_effort(&self, row: &TeamRow) {
        if row.project_id.is_some() || row.workspace.trim().is_empty() {
            return;
        }
        let (Some(project_id), Some(folder_id)) = self.resolve_binding_best_effort(&row.user_id, &row.workspace).await
        else {
            return;
        };
        let params = UpdateTeamParams {
            project_id: Some(project_id),
            folder_id: Some(folder_id),
            ..Default::default()
        };
        if let Err(err) = self.repo.update_team(&row.user_id, &row.id, &params).await {
            warn!(team_id = %row.id, error = %err, "team project bind: backfill update failed");
        }
    }

    async fn load_owned_team(&self, user_id: &str, team_id: &str) -> Result<Team, TeamError> {
        let row = self.load_owned_team_row(user_id, team_id).await?;
        Ok(Team::from_row(&row)?)
    }

    async fn load_owned_team_row(&self, user_id: &str, team_id: &str) -> Result<TeamRow, TeamError> {
        let access = self.authorize_team(user_id, team_id).await?;
        if access.role != TeamAccessRole::Owner {
            return Err(TeamError::TeamNotFound(team_id.into()));
        }
        Ok(access.team)
    }

    /// Central Team authorization seam. Current handlers remain owner-only
    /// unless they explicitly consume this context as a collaborator operation.
    pub async fn authorize_team(
        &self,
        actor_user_id: &str,
        team_id: &str,
    ) -> Result<TeamAuthorizationContext, TeamError> {
        let role = self
            .repo
            .team_access_role(team_id, actor_user_id)
            .await?
            .ok_or_else(|| TeamError::TeamNotFound(team_id.into()))?;
        let team = match role {
            TeamAccessRole::Owner => self.repo.get_team(actor_user_id, team_id).await?,
            TeamAccessRole::Collaborator => self.repo.get_team_for_restore(team_id).await?,
        }
        .ok_or_else(|| TeamError::TeamNotFound(team_id.into()))?;
        let sharing_mode = self.repo.get_team_sharing_mode(team_id).await?;
        if role == TeamAccessRole::Collaborator && sharing_mode != TeamSharingMode::Shared {
            return Err(TeamError::TeamNotFound(team_id.into()));
        }
        Ok(TeamAuthorizationContext {
            actor_user_id: actor_user_id.to_owned(),
            execution_owner_id: team.user_id.clone(),
            role,
            sharing_mode,
            team,
        })
    }

    async fn authorize_rostered_conversation(
        &self,
        actor_user_id: &str,
        team_id: &str,
        conversation_id: &str,
    ) -> Result<TeamAuthorizationContext, TeamError> {
        let access = self.authorize_team(actor_user_id, team_id).await?;
        let team = Team::from_row(&access.team)?;
        let Some(agent) = team
            .agents
            .iter()
            .find(|agent| agent.conversation_id == conversation_id)
        else {
            return Err(TeamError::TeamNotFound(team_id.to_owned()));
        };
        if access.role == TeamAccessRole::Collaborator && agent.role != TeammateRole::Lead {
            return Err(TeamError::TeamNotFound(team_id.to_owned()));
        }
        let Some(binding) = self
            .conversation_port
            .lookup_team_binding_by_conversation(conversation_id)
            .await?
        else {
            return Err(TeamError::TeamNotFound(team_id.to_owned()));
        };
        let expected_role = agent.role.to_string();
        if binding.user_id != access.execution_owner_id
            || binding.team_id.as_deref() != Some(team_id)
            || binding.slot_id.as_deref() != Some(agent.slot_id.as_str())
            || binding.role.as_deref() != Some(expected_role.as_str())
        {
            return Err(TeamError::TeamNotFound(team_id.to_owned()));
        }
        if access.role == TeamAccessRole::Collaborator {
            let Some(conversation_workspace) = self.conversation_port.conversation_workspace(conversation_id).await?
            else {
                return Err(TeamError::TeamNotFound(team_id.to_owned()));
            };
            // Creation may preserve an equivalent lexical alias in the
            // conversation extra while the Team row stores the canonical
            // path returned by shared-workspace provisioning. Authorize only
            // when both values resolve to this Team's exact dedicated path.
            // This keeps path identity strict without rejecting harmless
            // aliases such as a trailing `/.`.
            if !self
                .conversation_port
                .is_shared_team_workspace(team_id, &conversation_workspace)
                .await?
                || !self
                    .conversation_port
                    .is_shared_team_workspace(team_id, &access.team.workspace)
                    .await?
            {
                return Err(TeamError::TeamNotFound(team_id.to_owned()));
            }
        }
        Ok(access)
    }

    async fn lock_authorized_rostered_conversation(
        &self,
        actor_user_id: &str,
        team_id: &str,
        conversation_id: &str,
    ) -> Result<(tokio::sync::OwnedMutexGuard<()>, TeamAuthorizationContext), TeamError> {
        let guard = self.team_membership_lock(team_id).lock_owned().await;
        let access = self
            .authorize_rostered_conversation(actor_user_id, team_id, conversation_id)
            .await?;
        Ok((guard, access))
    }

    pub(crate) async fn team_owner_user_id(&self, team_id: &str) -> Result<String, TeamError> {
        let row = self
            .repo
            .get_team_for_restore(team_id)
            .await?
            .ok_or_else(|| TeamError::TeamNotFound(team_id.into()))?;
        Ok(row.user_id)
    }

    /// Returns the most recent team-wide mailbox messages (all recipients),
    /// newest first, for the read-only activity view. `limit` is clamped to
    /// `[1, MAX_ACTIVITY_LIMIT]`. Current membership is revalidated first.
    pub async fn list_team_mailbox(
        &self,
        user_id: &str,
        team_id: &str,
        limit: i64,
    ) -> Result<Vec<TeamMailboxMessageResponse>, TeamError> {
        self.authorize_team(user_id, team_id).await?;
        let clamped = limit.clamp(1, MAX_ACTIVITY_LIMIT);
        let rows = self.repo.list_messages_by_team(team_id, clamped).await?;
        let responses: Vec<TeamMailboxMessageResponse> = rows.iter().map(mailbox_row_to_response).collect();
        info!(kind = "team", team_id, count = responses.len(), "team mailbox listed");
        Ok(responses)
    }

    /// Returns the team's tasks, newest first (`created_at` DESC, `id` as a
    /// stable secondary key), truncated to a clamped `limit`, for the
    /// read-only activity view. Reuses the existing ASC `list_tasks` and sorts
    /// in the service. Repository scoping uses the authorized execution owner.
    pub async fn list_team_tasks(
        &self,
        user_id: &str,
        team_id: &str,
        limit: i64,
    ) -> Result<Vec<TeamTaskResponse>, TeamError> {
        let access = self.authorize_team(user_id, team_id).await?;
        let clamped = limit.clamp(1, MAX_ACTIVITY_LIMIT);
        let rows = self.repo.list_tasks(&access.execution_owner_id, team_id).await?;
        let mut tasks: Vec<TeamTask> = rows.iter().filter_map(|r| TeamTask::from_row(r).ok()).collect();
        tasks.sort_by(|a, b| b.created_at.cmp(&a.created_at).then_with(|| b.id.cmp(&a.id)));
        tasks.truncate(clamped as usize);
        let responses: Vec<TeamTaskResponse> = tasks.iter().map(task_to_response).collect();
        info!(kind = "team", team_id, count = responses.len(), "team tasks listed");
        Ok(responses)
    }

    /// Returns the team's tasks matching `ids` (newest first), for resolving
    /// dependency (`blocked_by`) subjects that may lie outside the loaded
    /// activity page. Current membership is revalidated first; `ids` is clamped to
    /// `MAX_TASK_ID_LOOKUP`. An empty `ids` yields an empty result.
    pub async fn list_team_tasks_by_ids(
        &self,
        user_id: &str,
        team_id: &str,
        ids: &[String],
    ) -> Result<Vec<TeamTaskResponse>, TeamError> {
        let access = self.authorize_team(user_id, team_id).await?;
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let capped = &ids[..ids.len().min(MAX_TASK_ID_LOOKUP)];
        let rows = self
            .repo
            .list_tasks_by_ids(&access.execution_owner_id, team_id, capped)
            .await?;
        let tasks: Vec<TeamTask> = rows.iter().filter_map(|r| TeamTask::from_row(r).ok()).collect();
        let responses: Vec<TeamTaskResponse> = tasks.iter().map(task_to_response).collect();
        info!(
            kind = "team",
            team_id,
            count = responses.len(),
            "team tasks resolved by ids"
        );
        Ok(responses)
    }

    /// Returns one keyset-paginated page of the unified activity feed (messages
    /// and/or tasks per `kind`), ordered by `(created_at, id)` in `direction`.
    /// Current membership is revalidated first, so unauthorized callers see
    /// from a missing one (`TeamNotFound`). For `kind = All`, each stream is
    /// fetched up to `limit` rows and merged; the global top-`limit` is
    /// mathematically complete (any item newer/older than the cursor is within
    /// its own stream's top-`limit`). `has_more` is conservative: a full sub-
    /// query or a post-merge truncation both flag "possibly more".
    pub async fn list_team_activity(
        &self,
        user_id: &str,
        team_id: &str,
        cursor: Option<ActivityCursor>,
        direction: PageDirection,
        kind: ActivityKind,
        limit: i64,
    ) -> Result<TeamActivityPageResponse, TeamError> {
        let access = self.authorize_team(user_id, team_id).await?;
        let limit = limit.clamp(1, MAX_ACTIVITY_LIMIT);

        let (mut items, mailbox_full, tasks_full) = match kind {
            ActivityKind::Message => {
                let rows = self
                    .repo
                    .list_messages_by_team_paged(team_id, cursor.clone(), direction, limit)
                    .await?;
                let full = rows.len() as i64 == limit;
                (
                    rows.iter().map(message_row_to_activity_item).collect::<Vec<_>>(),
                    full,
                    false,
                )
            }
            ActivityKind::Task => {
                let rows = self
                    .repo
                    .list_tasks_paged(&access.execution_owner_id, team_id, cursor.clone(), direction, limit)
                    .await?;
                let full = rows.len() as i64 == limit;
                (
                    rows.iter().filter_map(task_row_to_activity_item).collect::<Vec<_>>(),
                    false,
                    full,
                )
            }
            ActivityKind::All => {
                let msgs = self
                    .repo
                    .list_messages_by_team_paged(team_id, cursor.clone(), direction, limit)
                    .await?;
                let tasks = self
                    .repo
                    .list_tasks_paged(&access.execution_owner_id, team_id, cursor.clone(), direction, limit)
                    .await?;
                let mailbox_full = msgs.len() as i64 == limit;
                let tasks_full = tasks.len() as i64 == limit;
                let mut merged: Vec<_> = msgs
                    .iter()
                    .map(message_row_to_activity_item)
                    .chain(tasks.iter().filter_map(task_row_to_activity_item))
                    .collect();
                sort_activity_items(&mut merged, direction);
                (merged, mailbox_full, tasks_full)
            }
        };

        // Truncate to the top `limit`; whether we cut anything feeds `has_more`.
        let truncated = items.len() as i64 > limit;
        items.truncate(limit as usize);

        let has_more = mailbox_full || tasks_full || truncated;
        let next_cursor = if has_more {
            items.last().map(|i| TeamActivityCursor {
                ts: i.created_at,
                id: i.id.clone(),
            })
        } else {
            None
        };

        info!(
            kind = "team",
            team_id,
            count = items.len(),
            first_page = cursor.is_none(),
            "team activity listed"
        );

        Ok(TeamActivityPageResponse {
            items,
            next_cursor,
            has_more,
        })
    }

    pub async fn renew_active_lease(
        &self,
        user_id: &str,
        team_id: &str,
        active_leases: &ActiveLeaseRegistry,
    ) -> Result<(), TeamError> {
        let _membership_guard = self.team_membership_lock(team_id).lock_owned().await;
        let access = self.authorize_team(user_id, team_id).await?;
        let team = Team::from_row(&access.team)?;

        let conversation_ids = team
            .agents
            .iter()
            .map(|agent| agent.conversation_id.as_str())
            .filter(|conversation_id| !conversation_id.trim().is_empty());
        let (covered_count, expires_at) = active_leases.renew_many(conversation_ids);

        debug!(
            kind = "team",
            team_id, covered_count, expires_at, "Team active lease renewed"
        );
        Ok(())
    }

    /// Restore sessions for all existing teams. Called once at app startup
    /// so that MCP servers are available before any user sends a message.
    pub async fn restore_all_sessions(&self) {
        let teams = match self.repo.list_teams_for_restore().await {
            Ok(t) => t,
            Err(e) => {
                tracing::warn!(error = %e, "failed to list teams for session restore");
                return;
            }
        };
        for team in &teams {
            if let Err(e) = self.ensure_session_inner(&team.id, None).await {
                tracing::warn!(team_id = %team.id, error = %e, "failed to restore session on startup");
                continue;
            }
        }
        if !teams.is_empty() {
            tracing::info!(count = teams.len(), "team sessions restored on startup");
        }
    }

    pub async fn create_team(&self, user_id: &str, req: CreateTeamRequest) -> Result<TeamResponse, TeamError> {
        if req.agents.is_empty() {
            return Err(TeamError::InvalidRequest("at least one agent is required".into()));
        }
        if req
            .agents
            .iter()
            .any(|agent| agent.conversation_id.as_deref().is_some_and(|id| !id.trim().is_empty()))
        {
            return Err(TeamError::InvalidRequest(
                "creating Team agents from existing conversations are no longer supported; omit agents[].conversation_id"
                    .into(),
            ));
        }

        let team_id = generate_id();
        let now = now_ms();
        let shared_workspace = match req.sharing_mode {
            aionui_api_types::TeamSharingMode::Private => match req.workspace.as_deref() {
                Some(workspace) if !workspace.is_empty() => Some(validate_create_workspace_path(workspace)?),
                _ => None,
            },
            aionui_api_types::TeamSharingMode::Shared => {
                if req
                    .workspace
                    .as_deref()
                    .is_some_and(|workspace| !workspace.trim().is_empty())
                {
                    return Err(TeamError::InvalidRequest(
                        "Shared Team workspace is provisioned by the server".into(),
                    ));
                }
                Some(self.conversation_port.create_shared_team_workspace(&team_id).await?)
            }
        };

        let provisioned = self
            .provisioner()
            .provision_initial_agents(
                user_id,
                &team_id,
                &req.name,
                &req.agents,
                shared_workspace.as_deref(),
                req.sharing_mode == aionui_api_types::TeamSharingMode::Shared,
            )
            .await?;
        let agents = provisioned.agents;
        let lead_agent_id = provisioned.lead_agent_id;
        let team_workspace = provisioned.team_workspace;
        let agents_json = serde_json::to_string(&agents)?;

        // Project-bind side branch (best-effort; never affects team creation).
        let (project_id, folder_id) = self.resolve_binding_best_effort(user_id, &team_workspace).await;

        let row = TeamRow {
            id: team_id.clone(),
            user_id: user_id.to_owned(),
            name: req.name.clone(),
            workspace: team_workspace.clone(),
            workspace_mode: "shared".into(),
            agents: agents_json,
            lead_agent_id: lead_agent_id.clone(),
            session_mode: None,
            agents_version: "1.0.1".into(),
            created_at: now,
            updated_at: now,
            project_id,
            folder_id,
        };
        let sharing_mode = match req.sharing_mode {
            aionui_api_types::TeamSharingMode::Private => TeamSharingMode::Private,
            aionui_api_types::TeamSharingMode::Shared => TeamSharingMode::Shared,
        };
        self.repo.create_team_with_sharing_mode(&row, sharing_mode).await?;

        let team = Team {
            id: team_id,
            name: req.name,
            workspace: team_workspace,
            agents,
            lead_agent_id,
            created_at: now,
            updated_at: now,
        };

        info!(
            team_id = %team.id,
            workspace_source = if shared_workspace.is_some() {
                "explicit_team_workspace"
            } else {
                "auto_from_leader"
            },
            agent_count = team.agents.len(),
            "Team created"
        );

        self.broadcast_team_created(user_id, &team.id, &team.name);

        self.build_team_response_for_access(
            user_id,
            &team,
            req.sharing_mode,
            aionui_api_types::TeamAccessRole::Owner,
        )
        .await
    }

    pub async fn list_teams(&self, user_id: &str) -> Result<Vec<TeamResponse>, TeamError> {
        let mut rows = self.repo.list_teams_by_user(user_id).await?;
        let mut team_ids: HashSet<String> = rows.iter().map(|row| row.id.clone()).collect();
        for row in self.repo.list_teams_by_member(user_id).await? {
            if team_ids.insert(row.id.clone()) {
                rows.push(row);
            }
        }
        let mut teams = Vec::with_capacity(rows.len());
        for row in &rows {
            let Some(role) = self.repo.team_access_role(&row.id, user_id).await? else {
                continue;
            };
            let sharing_mode = self.repo.get_team_sharing_mode(&row.id).await?;
            match Team::from_row(row) {
                Ok(team) => match self
                    .build_team_response_for_access(
                        &row.user_id,
                        &team,
                        match sharing_mode {
                            TeamSharingMode::Private => aionui_api_types::TeamSharingMode::Private,
                            TeamSharingMode::Shared => aionui_api_types::TeamSharingMode::Shared,
                        },
                        match role {
                            TeamAccessRole::Owner => aionui_api_types::TeamAccessRole::Owner,
                            TeamAccessRole::Collaborator => aionui_api_types::TeamAccessRole::Collaborator,
                        },
                    )
                    .await
                {
                    Ok(resp) => teams.push(resp),
                    Err(e) => {
                        tracing::warn!(team_id = %row.id, error = %e, "skipping team with build error");
                    }
                },
                Err(e) => {
                    tracing::warn!(team_id = %row.id, error = %e, "skipping team with invalid agents JSON");
                }
            }
        }
        Ok(teams)
    }

    pub async fn list_eligible_collaborators(
        &self,
        owner_user_id: &str,
        team_id: &str,
    ) -> Result<Vec<EligibleTeamCollaboratorResponse>, TeamError> {
        self.load_owned_team_row(owner_user_id, team_id).await?;
        if self.repo.get_team_sharing_mode(team_id).await? != TeamSharingMode::Shared {
            return Err(TeamError::TeamNotFound(team_id.to_owned()));
        }
        let started_at = Instant::now();
        let Some(listing_generation) =
            self.eligible_account_refs
                .begin_listing(owner_user_id, team_id, started_at, now_ms())
        else {
            return Err(TeamError::RateLimited);
        };
        let issued_at = now_ms();
        let candidates = self.repo.list_eligible_team_users(owner_user_id, team_id).await?;
        ensure_candidate_count_supported(candidates.len())?;
        self.eligible_account_refs
            .ensure_capacity(owner_user_id, team_id, candidates.len(), Instant::now(), now_ms())
            .map_err(|error| match error {
                EligibleAccountRefStoreError::OwnerQuotaReached => TeamError::EligibleCollaboratorQuotaReached,
                EligibleAccountRefStoreError::ListingSuperseded => TeamError::EligibleCollaboratorListingSuperseded,
            })?;
        let mut labels = HashSet::new();
        let mut refs = HashMap::new();
        let mut response = Vec::with_capacity(candidates.len());
        for (index, candidate) in candidates.into_iter().enumerate() {
            let base_label = collaborator_display_label(&candidate.display_name, index + 1);
            let mut display_name = base_label.clone();
            let mut suffix = 1;
            while !labels.insert(display_name.clone()) {
                suffix += 1;
                display_name = format!("{base_label} ({suffix})");
            }
            let account_ref = generate_id();
            refs.insert(
                account_ref.clone(),
                EligibleAccountRef {
                    user_id: candidate.user_id,
                    display_name: display_name.clone(),
                    expires_at: issued_at.saturating_add(ELIGIBLE_ACCOUNT_REF_TTL_MS),
                },
            );
            response.push(EligibleTeamCollaboratorResponse {
                account_ref,
                display_name,
            });
        }
        self.eligible_account_refs
            .replace(
                owner_user_id,
                team_id,
                &listing_generation,
                refs,
                Instant::now(),
                now_ms(),
            )
            .map_err(|error| match error {
                EligibleAccountRefStoreError::OwnerQuotaReached => TeamError::EligibleCollaboratorQuotaReached,
                EligibleAccountRefStoreError::ListingSuperseded => TeamError::EligibleCollaboratorListingSuperseded,
            })?;
        Ok(response)
    }

    /// Lists active collaborators without exposing internal user identifiers.
    pub async fn list_team_members(
        &self,
        owner_user_id: &str,
        team_id: &str,
    ) -> Result<Vec<TeamMemberResponse>, TeamError> {
        self.load_owned_team_row(owner_user_id, team_id).await?;
        if self.repo.get_team_sharing_mode(team_id).await? != TeamSharingMode::Shared {
            return Err(TeamError::TeamNotFound(team_id.to_owned()));
        }
        let members = self.repo.list_team_members(team_id).await?;
        Ok(members
            .into_iter()
            .map(|member| TeamMemberResponse {
                membership_ref: member.membership_ref,
                display_name: member.display_name,
                created_at: member.created_at,
            })
            .collect())
    }

    pub async fn add_team_member(
        &self,
        owner_user_id: &str,
        team_id: &str,
        account_ref: &str,
    ) -> Result<(), TeamError> {
        self.load_owned_team_row(owner_user_id, team_id).await?;
        if self.repo.get_team_sharing_mode(team_id).await? != TeamSharingMode::Shared {
            return Err(TeamError::TeamNotFound(team_id.to_owned()));
        }
        let now = now_ms();
        let invalid_ref = || TeamError::InvalidRequest("Invalid or expired account reference".into());
        let Some(reference) = self
            .eligible_account_refs
            .take(owner_user_id, team_id, account_ref, Instant::now(), now)
        else {
            return Err(invalid_ref());
        };
        if reference.expires_at <= now {
            return Err(invalid_ref());
        }

        // Re-read eligibility immediately before the owner-scoped insert. This
        // rejects disabled, removed, or otherwise stale users after listing.
        if self
            .repo
            .find_eligible_team_user(owner_user_id, team_id, &reference.user_id)
            .await?
            .is_none()
        {
            return Err(invalid_ref());
        }
        let membership = aionui_db::models::TeamMembershipRow {
            membership_ref: generate_id(),
            team_id: team_id.to_owned(),
            user_id: reference.user_id,
            display_name: Some(reference.display_name),
            created_at: now_ms(),
        };
        self.repo.add_team_member_for_owner(owner_user_id, &membership).await?;
        Ok(())
    }

    /// Revokes by server-issued membership reference after an owner-scoped check.
    pub async fn remove_team_member(
        &self,
        owner_user_id: &str,
        team_id: &str,
        membership_ref: &str,
    ) -> Result<(), TeamError> {
        self.load_owned_team_row(owner_user_id, team_id).await?;
        let _membership_guard = self.team_membership_lock(team_id).lock_owned().await;
        self.load_owned_team_row(owner_user_id, team_id).await?;
        let member_user_id = self
            .repo
            .list_team_members(team_id)
            .await?
            .into_iter()
            .find(|member| member.membership_ref == membership_ref)
            .map(|member| member.user_id);
        let revocation_started = if let Some(user_id) = member_user_id.as_deref() {
            // Remove authorization before persistence, then wait for socket
            // writes that passed their final check before revocation. Queued
            // frames not yet drained will fail their send-loop check.
            self.broadcaster.revoke_scope_recipient(team_id, user_id);
            if let Some(session) = self.sessions.get(team_id) {
                session.session.revoke_event_user(user_id);
            }
            true
        } else {
            false
        };
        if revocation_started {
            self.broadcaster.wait_for_scope_deliveries().await;
        }
        if let Err(error) = self
            .repo
            .remove_team_member(owner_user_id, team_id, membership_ref)
            .await
        {
            self.refresh_session_event_users(team_id, owner_user_id).await;
            return Err(error.into());
        }
        self.refresh_session_event_users(team_id, owner_user_id).await;
        Ok(())
    }

    pub async fn list_team_mcp_allowlist(&self, owner_user_id: &str, team_id: &str) -> Result<Vec<String>, TeamError> {
        let access = self.authorize_team(owner_user_id, team_id).await?;
        if access.role != TeamAccessRole::Owner || access.sharing_mode != TeamSharingMode::Shared {
            return Err(TeamError::TeamNotFound(team_id.to_owned()));
        }
        self.repo
            .list_team_mcp_allowlist(owner_user_id, team_id)
            .await
            .map_err(Into::into)
    }

    pub async fn replace_team_mcp_allowlist(
        &self,
        owner_user_id: &str,
        team_id: &str,
        mcp_server_ids: Vec<String>,
    ) -> Result<(), TeamError> {
        let access = self.authorize_team(owner_user_id, team_id).await?;
        if access.role != TeamAccessRole::Owner || access.sharing_mode != TeamSharingMode::Shared {
            return Err(TeamError::TeamNotFound(team_id.to_owned()));
        }
        let mut normalized = Vec::with_capacity(mcp_server_ids.len());
        let mut unique = HashSet::with_capacity(mcp_server_ids.len());
        for id in mcp_server_ids {
            let id = id.trim();
            if id.is_empty() || !unique.insert(id.to_owned()) {
                return Err(TeamError::InvalidRequest(
                    "MCP allowlist IDs must be non-empty and unique".into(),
                ));
            }
            normalized.push(id.to_owned());
        }

        let lock = self.team_membership_lock(team_id);
        let guard = lock.lock().await;
        self.load_owned_team_row(owner_user_id, team_id).await?;
        if self.repo.get_team_sharing_mode(team_id).await? != TeamSharingMode::Shared {
            return Err(TeamError::TeamNotFound(team_id.to_owned()));
        }
        self.repo
            .replace_team_mcp_allowlist(owner_user_id, team_id, &normalized)
            .await?;
        drop(guard);

        if let Some(session) = self.sessions.get(team_id).map(|entry| Arc::clone(&entry.session)) {
            for agent in session.scheduler().list_agents().await {
                self.refresh_member_mcp_binding(&session, owner_user_id, &agent).await;
            }
        }
        Ok(())
    }

    async fn refresh_session_event_users(&self, team_id: &str, owner_user_id: &str) {
        match self.repo.list_team_members(team_id).await {
            Ok(members) => {
                let user_ids = std::iter::once(owner_user_id.to_owned())
                    .chain(members.into_iter().map(|member| member.user_id))
                    .collect::<Vec<_>>();
                self.broadcaster.replace_scope_recipients(team_id, user_ids.clone());
                if let Some(session) = self.sessions.get(team_id).map(|entry| Arc::clone(&entry.session)) {
                    session.set_authorized_event_users(user_ids);
                }
            }
            Err(error) => {
                warn!(team_id, error = %error, "team event recipient refresh failed; revoked recipients remain removed")
            }
        }
    }

    pub async fn get_team(&self, user_id: &str, team_id: &str) -> Result<TeamResponse, TeamError> {
        let lock = self.team_membership_lock(team_id);
        let _guard = lock.lock().await;
        let access = self.authorize_team(user_id, team_id).await?;
        // Project-bind side branch: lazily backfill binding only when a single
        // team is opened (never during list_teams / lease renew).
        if access.role == TeamAccessRole::Owner {
            self.backfill_team_binding_best_effort(&access.team).await;
        }
        let sharing_mode = self.repo.get_team_sharing_mode(team_id).await?;
        let team = Team::from_row(&access.team)?;
        // Deliberately does NOT reconcile legacy model facts. That repair reads
        // three extra tables PER MEMBER, and this is a plain read endpoint the
        // frontend hits whenever a team is opened. Session start owns the repair
        // (`ensure_session`), which is the point where a stale roster would
        // actually feed a rebuilt runtime.
        self.build_team_response_for_access(
            &access.execution_owner_id,
            &team,
            match sharing_mode {
                TeamSharingMode::Private => aionui_api_types::TeamSharingMode::Private,
                TeamSharingMode::Shared => aionui_api_types::TeamSharingMode::Shared,
            },
            match access.role {
                TeamAccessRole::Owner => aionui_api_types::TeamAccessRole::Owner,
                TeamAccessRole::Collaborator => aionui_api_types::TeamAccessRole::Collaborator,
            },
        )
        .await
    }

    pub async fn remove_team(&self, user_id: &str, team_id: &str) -> Result<(), TeamError> {
        // Upload requests reauthorize and persist under this same lock. Holding
        // it across cleanup prevents a buffered request from recreating the
        // Team's upload directory after deletion.
        let membership_lock = self.team_membership_lock(team_id);
        let membership_guard = Arc::clone(&membership_lock).lock_owned().await;
        let team = self.load_owned_team(user_id, team_id).await?;

        self.stop_team_runtime_and_agents(team_id, &team, AgentKillReason::TeamDeleted)
            .await;

        // Remove staged bytes before deleting the Team row. If filesystem
        // cleanup fails, leave the Team available and report the failure rather
        // than orphaning storage outside the active-Team quota.
        self.remove_team_upload_storage(team_id)?;

        for agent in &team.agents {
            let _ = self
                .conversation_port
                .delete_team_conversation(user_id, &agent.conversation_id)
                .await;
        }

        self.repo.delete_mailbox_by_team(user_id, team_id).await?;
        self.repo.delete_tasks_by_team(user_id, team_id).await?;
        self.repo.delete_team(user_id, team_id).await?;

        // Cascade the team's sidebar ordering row (design §4.3, path 2). Members'
        // conversation rows are dropped by the conversation delete hook via the
        // `delete_team_conversation` calls above. Best-effort: an orphan `team`
        // row self-heals on read (the pinned group only emits teams present in
        // the live aggregate), so it never blocks deletion.
        self.remove_team_order_row(user_id, team_id).await;

        drop(membership_guard);
        self.prune_team_membership_lock(team_id, &membership_lock);

        info!(team_id = %team_id, "Team removed");
        self.broadcast_team_removed(user_id, team_id);
        Ok(())
    }

    /// Tear down a team's live runtime and every member agent process WITHOUT
    /// deleting any data. Shared by `remove_team` (which then drops the rows)
    /// and `stop_team_processes` (archive, which keeps them). Best-effort: a
    /// stuck kill is bounded by a 3s timeout, mirroring the delete path.
    async fn stop_team_runtime_and_agents(&self, team_id: &str, team: &Team, reason: AgentKillReason) {
        self.stop_session_unchecked(team_id);

        let kill_futures: Vec<_> = team
            .agents
            .iter()
            .map(|agent| self.task_manager.kill_and_wait(&agent.conversation_id, Some(reason)))
            .collect();

        let _ = tokio::time::timeout(
            std::time::Duration::from_secs(3),
            futures_util::future::join_all(kill_futures),
        )
        .await;
    }

    /// Archive-time teardown: stop the team runtime and kill every member agent
    /// process, but keep all rows intact (the archive flip lives in the sidebar
    /// service). Mirrors the process-stopping half of `remove_team` so an
    /// archived team stops streaming just like a deleted one; unarchiving
    /// cold-starts a fresh runtime.
    pub async fn stop_team_processes(&self, user_id: &str, team_id: &str) -> Result<(), TeamError> {
        let team = self.load_owned_team(user_id, team_id).await?;
        self.stop_team_runtime_and_agents(team_id, &team, AgentKillReason::Archived)
            .await;
        Ok(())
    }

    pub async fn rename_team(&self, user_id: &str, team_id: &str, name: &str) -> Result<(), TeamError> {
        self.load_owned_team(user_id, team_id).await?;

        self.repo
            .update_team(
                user_id,
                team_id,
                &UpdateTeamParams {
                    name: Some(name.to_owned()),
                    ..Default::default()
                },
            )
            .await?;
        self.broadcast_team_renamed(user_id, team_id, name);
        Ok(())
    }

    pub async fn add_agent(
        &self,
        user_id: &str,
        team_id: &str,
        req: AddAgentRequest,
    ) -> Result<TeamAgentResponse, TeamError> {
        let lock = self.team_membership_lock(team_id);
        let _guard = lock.lock().await;

        let row = self.load_owned_team_row(user_id, team_id).await?;
        let mut team = Team::from_row(&row)?;
        let agent = self.provisioner().add_agent(user_id, &row, &mut team, req).await?;

        if let Some(session) = self.sessions.get(team_id).map(|e| Arc::clone(&e.session)) {
            let reservation = session.reserve_dynamic_member_attach(&agent);
            session.add_manual_agent(&agent).await?;
            let service = self
                .self_ref
                .upgrade()
                .ok_or_else(|| TeamError::InvalidRequest("add_agent requires a live TeamSessionService".into()))?;
            self.broadcast_agent_runtime_status(user_id, team_id, &agent, TeamAgentRuntimeStatus::Pending, None);
            spawn_attach_agent_process_bg(
                service,
                session,
                user_id.to_owned(),
                agent.clone(),
                self.task_manager.clone(),
                reservation,
                // User-initiated add: failures surface inline, do not wake leader.
                false,
            );
            info!(
                team_id = %team_id,
                slot_id = %agent.slot_id,
                assistant_id = %agent.assistant_id.as_deref().unwrap_or(""),
                role = %agent.role,
                notification_written = true,
                wake_requested = true,
                "manual teammate added"
            );
        } else {
            TeamEventEmitter::new(team_id.to_owned(), user_id.to_owned(), self.broadcaster.clone())
                .broadcast_agent_spawned(&agent);
            info!(
                team_id = %team_id,
                slot_id = %agent.slot_id,
                assistant_id = %agent.assistant_id.as_deref().unwrap_or(""),
                role = %agent.role,
                notification_written = false,
                wake_requested = false,
                "manual teammate added"
            );
        }

        self.build_agent_response(user_id, team_id, &agent).await
    }

    pub async fn remove_agent(&self, user_id: &str, team_id: &str, slot_id: &str) -> Result<(), TeamError> {
        let lock = self.team_membership_lock(team_id);
        let (removed, session, removal_lease) = {
            let _guard = lock.lock().await;
            let team = self.load_owned_team(user_id, team_id).await?;
            let removed = team
                .agents
                .iter()
                .find(|agent| agent.slot_id == slot_id)
                .cloned()
                .ok_or_else(|| TeamError::AgentNotFound(slot_id.into()))?;
            if removed.role == crate::types::TeammateRole::Lead {
                return Err(TeamError::InvalidRequest("cannot remove the team lead".into()));
            }
            let session = self.sessions.get(team_id).map(|entry| Arc::clone(&entry.session));
            let removal = session
                .as_ref()
                .map(|session| session.member_runtimes().begin_remove(slot_id));
            let removal_lease = match removal {
                Some(BeginRemove::Start(lease)) => Some(lease),
                Some(BeginRemove::Join(waiter)) => {
                    drop(_guard);
                    return match waiter.wait().await {
                        AttachOutcome::Removed => Ok(()),
                        AttachOutcome::Failed(failure) => Err(TeamError::MemberRuntimeFailed {
                            team_id: team_id.to_owned(),
                            slot_id: removed.slot_id,
                            conversation_id: removed.conversation_id,
                            public_reason: failure.public_reason,
                        }),
                        AttachOutcome::Ready | AttachOutcome::SessionStopped => {
                            Err(TeamError::SessionNotFound(team_id.to_owned()))
                        }
                    };
                }
                Some(BeginRemove::Absent | BeginRemove::SessionStopped) | None => None,
            };
            (removed, session, removal_lease)
        };

        // Cancellation and process cleanup intentionally happen without the
        // membership lock. Concurrent ensure calls observe Removing and join
        // the same registry operation instead of starting a replacement.
        if let Some(session) = &session {
            session.event_loops().remove(slot_id);
        }
        self.task_manager
            .kill_and_wait(&removed.conversation_id, Some(AgentKillReason::TeamDeleted))
            .await;

        let persist_result = {
            let _guard = lock.lock().await;
            let mut current = self.load_owned_team(user_id, team_id).await?;
            current.agents.retain(|agent| agent.slot_id != slot_id);
            let agents_json = serde_json::to_string(&current.agents)?;
            self.repo
                .update_team(
                    user_id,
                    team_id,
                    &UpdateTeamParams {
                        agents: Some(agents_json),
                        ..Default::default()
                    },
                )
                .await
        };

        if let Err(error) = persist_result {
            if let (Some(session), Some(lease)) = (&session, removal_lease.as_ref()) {
                session
                    .member_runtimes()
                    .restore_attach_required_after_remove_persist_error(
                        lease,
                        MemberRuntimeFailure {
                            classification: "membership_persist_failed",
                            public_reason: "Agent runtime needs to restart after membership update failed".to_owned(),
                        },
                    );
                self.refresh_member_runtime_status(session).await;
            }
            return Err(error.into());
        }

        let published_session = self.sessions.get(team_id).map(|entry| Arc::clone(&entry.session));
        let active_session = if let Some(current) = published_session {
            let current_removal_lease = if session.as_ref().is_some_and(|captured| Arc::ptr_eq(captured, &current)) {
                removal_lease
            } else {
                match current.member_runtimes().begin_remove(slot_id) {
                    BeginRemove::Start(lease) => Some(lease),
                    BeginRemove::Join(waiter) => {
                        let _ = waiter.wait().await;
                        None
                    }
                    BeginRemove::Absent | BeginRemove::SessionStopped => None,
                }
            };
            current.event_loops().remove(slot_id);
            self.task_manager
                .kill_and_wait(&removed.conversation_id, Some(AgentKillReason::TeamDeleted))
                .await;
            match current.scheduler().remove_agent(slot_id).await {
                Ok(_) | Err(TeamError::AgentNotFound(_)) => {}
                Err(error) => return Err(error),
            }
            if let Some(lease) = current_removal_lease.as_ref() {
                current.member_runtimes().finish_remove(lease);
            }
            Some(current)
        } else {
            None
        };

        if let Err(error) = self
            .conversation_port
            .delete_team_conversation(user_id, &removed.conversation_id)
            .await
        {
            warn!(
                team_id,
                slot_id,
                conversation_id = %removed.conversation_id,
                error = %error,
                "removed team member conversation cleanup failed"
            );
        }

        if let Some(session) = active_session.filter(|session| self.capture_published_session(session).is_some()) {
            session.notify_leader_membership_removed(&removed).await?;
            self.refresh_member_runtime_status(&session).await;
            info!(
                team_id = %team_id,
                slot_id = %removed.slot_id,
                assistant_id = %removed.assistant_id.as_deref().unwrap_or(""),
                role = %removed.role,
                notification_written = true,
                wake_requested = true,
                "manual teammate removed"
            );
        } else {
            TeamEventEmitter::new(team_id.to_owned(), user_id.to_owned(), self.broadcaster.clone())
                .broadcast_agent_removed(slot_id);
            info!(
                team_id = %team_id,
                slot_id = %removed.slot_id,
                assistant_id = %removed.assistant_id.as_deref().unwrap_or(""),
                role = %removed.role,
                notification_written = false,
                wake_requested = false,
                "manual teammate removed"
            );
        }

        Ok(())
    }

    pub async fn rename_agent(&self, user_id: &str, team_id: &str, slot_id: &str, name: &str) -> Result<(), TeamError> {
        let lock = self.team_membership_lock(team_id);
        let _guard = lock.lock().await;

        let mut team = self.load_owned_team(user_id, team_id).await?;

        let normalized = crate::scheduler::normalize_name(name);
        if normalized.is_empty() {
            return Err(TeamError::InvalidRequest(
                "rename_agent.name is empty after normalization".into(),
            ));
        }

        // Uniqueness check against all other agents in the team.
        let has_conflict = team
            .agents
            .iter()
            .any(|a| a.slot_id != slot_id && crate::scheduler::normalize_name(&a.name) == normalized);
        if has_conflict {
            return Err(TeamError::DuplicateAgentName(name.to_owned()));
        }

        let agent = team
            .agents
            .iter_mut()
            .find(|a| a.slot_id == slot_id)
            .ok_or_else(|| TeamError::AgentNotFound(slot_id.into()))?;
        agent.name = name.to_owned();

        let agents_json = serde_json::to_string(&team.agents)?;
        self.repo
            .update_team(
                user_id,
                team_id,
                &UpdateTeamParams {
                    agents: Some(agents_json),
                    ..Default::default()
                },
            )
            .await?;

        if let Some(session) = self.sessions.get(team_id).map(|e| Arc::clone(&e.session)) {
            let _ = session.rename_agent(slot_id, name).await;
        }

        Ok(())
    }

    pub async fn update_agent_model(
        &self,
        user_id: &str,
        team_id: &str,
        slot_id: &str,
        model: &str,
    ) -> Result<(), TeamError> {
        let model = model.trim();
        if model.is_empty() {
            return Err(TeamError::InvalidRequest("model must not be empty".into()));
        }
        self.persist_member_model_selection(user_id, team_id, slot_id, model, ModelPersistTrigger::ExplicitRequest)
            .await
    }

    /// Record that a team member's model is now `model`, in every place a rebuilt
    /// member runtime reads it from.
    ///
    /// Sole implementation on purpose. A model switch has to land in three places
    /// — the conversation's persisted runtime state, the team roster, and the live
    /// session's in-memory agent — and both entry points (the explicit model
    /// endpoint and the generic config-option path) must update all three.
    /// Previously each did half and the frontend chained them, so a failure
    /// between the two calls left the runtime switched and the roster stale.
    async fn persist_member_model_selection(
        &self,
        user_id: &str,
        team_id: &str,
        slot_id: &str,
        model: &str,
        trigger: ModelPersistTrigger,
    ) -> Result<(), TeamError> {
        let lock = self.team_membership_lock(team_id);
        let _guard = lock.lock().await;
        let mut team = self.load_owned_team(user_id, team_id).await?;
        let target = team
            .agents
            .iter()
            .find(|agent| agent.slot_id == slot_id)
            .cloned()
            .ok_or_else(|| TeamError::AgentNotFound(slot_id.to_owned()))?;
        // Only an explicit preference update is refused mid-start. When the
        // runtime has ALREADY accepted the switch, refusing here would drop the
        // persistence and silently revert the member on its next rebuild.
        if trigger == ModelPersistTrigger::ExplicitRequest && self.member_runtime_is_starting(team_id, &target.slot_id)
        {
            return Err(Self::member_runtime_starting_error(team_id, &target));
        }
        let agent = team
            .agents
            .iter_mut()
            .find(|agent| agent.slot_id == slot_id)
            .ok_or_else(|| TeamError::AgentNotFound(slot_id.to_owned()))?;
        let conversation_id = agent.conversation_id.clone();

        self.conversation_port
            .persist_confirmed_model(&conversation_id, model)
            .await?;
        agent.model = model.to_owned();
        self.repo
            .update_team(
                user_id,
                team_id,
                &UpdateTeamParams {
                    agents: Some(serde_json::to_string(&team.agents)?),
                    ..Default::default()
                },
            )
            .await?;

        if let Some(session) = self.sessions.get(team_id).map(|entry| Arc::clone(&entry.session)) {
            session.update_agent_model(slot_id, model).await?;
        }
        info!(
            team_id,
            slot_id,
            conversation_id,
            model,
            trigger = trigger.as_str(),
            "team agent model preference persisted"
        );
        Ok(())
    }

    async fn reconcile_legacy_team_models(
        &self,
        user_id: &str,
        team_id: &str,
        team: &mut Team,
    ) -> Result<(), TeamError> {
        let mut roster_changed = false;
        let mut repaired = Vec::new();

        for agent in &mut team.agents {
            let facts = self
                .conversation_port
                .conversation_model_facts(&agent.conversation_id)
                .await?;
            let Some(model) = facts
                .confirmed_model_id
                .map(|value| value.trim().to_owned())
                .filter(|value| !value.is_empty())
            else {
                continue;
            };
            let seed_changed = facts.runtime_seed_model_id.as_deref() != Some(model.as_str());
            if seed_changed {
                self.conversation_port
                    .patch_runtime_config(&agent.conversation_id, serde_json::json!({ "current_model_id": model }))
                    .await?;
            }
            let agent_changed = agent.model != model;
            if agent_changed {
                agent.model.clone_from(&model);
                roster_changed = true;
            }
            if seed_changed || agent_changed {
                repaired.push((agent.slot_id.clone(), model));
            }
        }

        if roster_changed {
            self.repo
                .update_team(
                    user_id,
                    team_id,
                    &UpdateTeamParams {
                        agents: Some(serde_json::to_string(&team.agents)?),
                        ..Default::default()
                    },
                )
                .await?;
        }
        if let Some(session) = self.sessions.get(team_id).map(|entry| Arc::clone(&entry.session)) {
            for (slot_id, model) in &repaired {
                session.update_agent_model(slot_id, model).await?;
            }
        }
        if !repaired.is_empty() {
            info!(
                team_id,
                repaired_agent_count = repaired.len(),
                "reconciled legacy team model facts"
            );
        }
        Ok(())
    }

    /// Start the team's MCP server and rebuild every agent process so it
    /// carries a fresh `team_mcp_stdio_config` pointing at the new server.
    ///
    /// Flow (mcp.md §4.3):
    /// 1. Start `TeamSession` (opens the MCP TCP server).
    /// 2. For each agent: persist `team_mcp_stdio_config` into
    ///    `conversation.extra` → `task_manager.kill_and_wait(conv_id, TeamMcpRebuild)`
    ///    → `TeamConversationProvisioningPort::warmup_agent_process(...)`
    ///    rebuilds the ACP process with
    ///    the new extra.
    /// 3. Spawn per-agent event loops that drain the mailbox whenever notified.
    /// 4. Only insert into `sessions` after every step above succeeds — on
    ///    any failure, stop the session and leave the map untouched so a
    ///    retry can start cleanly.
    pub async fn ensure_session(&self, user_id: &str, team_id: &str) -> Result<(), TeamError> {
        self.ensure_session_inner(team_id, Some(user_id)).await
    }

    async fn ensure_session_inner(&self, team_id: &str, requested_user_id: Option<&str>) -> Result<(), TeamError> {
        let membership_guard = self.team_membership_lock(team_id).lock_owned().await;

        // When a request supplies its authenticated actor, recheck membership
        // under the same lock used by member removal. Startup restoration has
        // no request actor and intentionally skips this check.
        if let Some(actor_user_id) = requested_user_id {
            self.authorize_team(actor_user_id, team_id).await?;
        }

        let row = match self.repo.get_team_for_restore(team_id).await {
            Ok(Some(row)) => row,
            Ok(None) => {
                if let Some(user_id) = requested_user_id {
                    self.broadcast_session_status(
                        user_id,
                        team_id,
                        TeamSessionStatus::Failed,
                        Some(TeamSessionPhase::LoadingTeam),
                        |p| {
                            p.error = Some(format!("team not found: {team_id}"));
                        },
                    );
                }
                return Err(TeamError::TeamNotFound(team_id.into()));
            }
            Err(e) => {
                if let Some(user_id) = requested_user_id {
                    self.broadcast_session_status(
                        user_id,
                        team_id,
                        TeamSessionStatus::Failed,
                        Some(TeamSessionPhase::LoadingTeam),
                        |p| {
                            p.error = Some(e.to_string());
                        },
                    );
                }
                return Err(e.into());
            }
        };
        let user_id = row.user_id.clone();
        let mut team = Team::from_row(&row)?;
        self.reconcile_legacy_team_models(&user_id, team_id, &mut team).await?;
        let agents_snapshot: Vec<TeamAgent> = team.agents.clone();

        if let Some(session) = self.sessions.get(team_id).map(|entry| Arc::clone(&entry.session)) {
            let work = self
                .reserve_member_runtime_reconciliation(&session, &agents_snapshot)
                .await?;
            drop(membership_guard);
            return self
                .complete_member_runtime_reconciliation(team_id, &user_id, session, work)
                .await;
        }

        let lock = self
            .ensure_session_locks
            .entry(team_id.to_owned())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone();
        let ensure_guard = lock.lock().await;

        if let Some(session) = self.sessions.get(team_id).map(|entry| Arc::clone(&entry.session)) {
            let work = self
                .reserve_member_runtime_reconciliation(&session, &agents_snapshot)
                .await?;
            drop(membership_guard);
            drop(ensure_guard);
            return self
                .complete_member_runtime_reconciliation(team_id, &user_id, session, work)
                .await;
        }

        self.broadcast_session_status(
            &user_id,
            team_id,
            TeamSessionStatus::Starting,
            Some(TeamSessionPhase::LoadingTeam),
            |_| {},
        );

        self.broadcast_session_status(
            &user_id,
            team_id,
            TeamSessionStatus::Starting,
            Some(TeamSessionPhase::StartingBridge),
            |_| {},
        );

        let session = match TeamSession::start_with_prompt_dump(
            team,
            self.repo.clone(),
            self.broadcaster.clone(),
            self.backend_binary_path.clone(),
            self.task_manager.clone(),
            self.turn_port.clone(),
            self.cancellation_port.clone(),
            self.projection_store.clone(),
            user_id.clone(),
            self.self_ref.clone(),
            self.prompt_dump.clone(),
        )
        .await
        {
            Ok(session) => Arc::new(session.with_slash_command_port(self.slash_command_port.clone())),
            Err(e) => {
                self.broadcast_session_status(
                    &user_id,
                    team_id,
                    TeamSessionStatus::Failed,
                    Some(TeamSessionPhase::StartingBridge),
                    |p| {
                        p.error = Some(e.to_string());
                    },
                );
                return Err(e);
            }
        };

        match self.repo.list_team_members(team_id).await {
            Ok(members) => session.set_authorized_event_users(
                std::iter::once(user_id.clone()).chain(members.into_iter().map(|member| member.user_id)),
            ),
            Err(error) => {
                warn!(team_id, error = %error, "team event recipients unavailable; keeping owner-only fanout")
            }
        }

        self.broadcast_session_status(
            &user_id,
            team_id,
            TeamSessionStatus::Starting,
            Some(TeamSessionPhase::AttachingAgents),
            |_| {},
        );

        let service = self
            .self_ref
            .upgrade()
            .ok_or_else(|| TeamError::InvalidRequest("team service is shutting down".to_owned()))?;

        // Leader-only warmup: only the lead slot is attached at first start.
        // Teammates stay dormant (Absent in the registry) until a delivery
        // lazily wakes them (spec 5.1).
        let Some(leader) = agents_snapshot
            .iter()
            .find(|agent| agent.role == TeammateRole::Lead)
            .cloned()
        else {
            let error = TeamError::InvalidRequest("team has no lead agent".to_owned());
            self.broadcast_session_status(
                &user_id,
                team_id,
                TeamSessionStatus::Failed,
                Some(TeamSessionPhase::AttachingAgents),
                |p| p.error = Some(error.to_string()),
            );
            session.stop();
            return Err(error);
        };

        // Publish the session BEFORE attaching so the single attach path
        // (`attach_member_runtime`) observes it as the current published
        // session. Drop the startup guards before awaiting the attach: that
        // path re-acquires the membership lock (via `refresh_member_runtime_status`)
        // and the ensure lock (via `cleanup_stale_member_runtime_task` on
        // failure), so holding them here would deadlock. Concurrent ensures
        // that were blocked on membership_guard now observe the published
        // session and take the reconciliation path instead of cold-starting.
        let slow_monitor_handle = Self::spawn_slow_monitor(session.clone());
        let entry = SessionEntry {
            session: session.clone(),
            slow_monitor_handle,
        };
        self.sessions.insert(team_id.to_owned(), entry);
        drop(membership_guard);
        drop(ensure_guard);

        self.broadcast_agent_runtime_status(&user_id, team_id, &leader, TeamAgentRuntimeStatus::Pending, None);
        let leader_outcome = match session.member_runtimes().reserve_attach(&leader.slot_id, false) {
            ReserveAttach::Start(lease) => {
                attach_member_runtime(
                    Arc::clone(&service),
                    session.clone(),
                    user_id.clone(),
                    leader.clone(),
                    self.task_manager.clone(),
                    lease,
                    // Leader cold-start failure bubbles to a session-level Failed
                    // (full-screen card), not an inline per-member notice.
                    false,
                )
                .await
            }
            ReserveAttach::Join(waiter) | ReserveAttach::Removing(waiter) => waiter.wait().await,
            ReserveAttach::AlreadyReady => AttachOutcome::Ready,
            ReserveAttach::SessionStopped => AttachOutcome::SessionStopped,
        };

        match leader_outcome {
            AttachOutcome::Ready | AttachOutcome::Removed => {}
            AttachOutcome::Failed(failure) => {
                self.broadcast_session_status(
                    &user_id,
                    team_id,
                    TeamSessionStatus::Failed,
                    Some(TeamSessionPhase::AttachingAgents),
                    |p| p.error = Some(failure.public_reason.clone()),
                );
                session.stop();
                self.sessions.remove(team_id);
                return Err(TeamError::MemberRuntimeFailed {
                    team_id: team_id.to_owned(),
                    slot_id: leader.slot_id.clone(),
                    conversation_id: leader.conversation_id.clone(),
                    public_reason: failure.public_reason,
                });
            }
            AttachOutcome::SessionStopped => {
                session.stop();
                self.sessions.remove(team_id);
                return Err(TeamError::InvalidRequest(
                    "team session stopped during leader warmup".to_owned(),
                ));
            }
        }

        // Teammates start dormant; the leader's Ready was already broadcast by
        // its successful attach.
        for agent in agents_snapshot.iter().filter(|a| a.role != TeammateRole::Lead) {
            self.broadcast_agent_runtime_status(&user_id, team_id, agent, TeamAgentRuntimeStatus::Dormant, None);
        }

        self.broadcast_session_status(
            &user_id,
            team_id,
            TeamSessionStatus::Starting,
            Some(TeamSessionPhase::Recovering),
            |_| {},
        );

        if let Err(err) = session.try_start_recovery_drain("ensure_session_ready").await {
            warn!(
                team_id,
                error = %err,
                "team recovery scan failed after session ensure"
            );
        }

        self.broadcast_session_status(&user_id, team_id, TeamSessionStatus::Ready, None, |p| {
            p.server_count = Some(agents_snapshot.len());
        });

        Ok(())
    }

    async fn reserve_member_runtime_reconciliation(
        &self,
        session: &Arc<TeamSession>,
        agents: &[TeamAgent],
    ) -> Result<Vec<MemberRuntimeReconcileWork>, TeamError> {
        let scheduler_slots = session
            .scheduler()
            .list_agents()
            .await
            .into_iter()
            .map(|agent| agent.slot_id)
            .collect::<HashSet<_>>();
        let mut work = Vec::new();

        for agent in agents {
            if !scheduler_slots.contains(&agent.slot_id) {
                session.scheduler().add_agent(agent).await;
            }
            let snapshot = session.member_runtimes().snapshot(&agent.slot_id);
            // Skip dormant teammates: an Absent non-lead member was never
            // triggered, so a re-ensure (second warmupSession, model switch,
            // retry) must NOT wake it, or it would punch through lazy warmup
            // (spec 5.1). The leader, Ready-repair, Failed-retry, and in-flight
            // members still reconcile.
            if matches!(snapshot, MemberRuntimeSnapshot::Absent) && agent.role != TeammateRole::Lead {
                continue;
            }
            let reservation = match snapshot {
                MemberRuntimeSnapshot::Ready if self.task_manager.get_task(&agent.conversation_id).is_none() => {
                    session.member_runtimes().reserve_repair(&agent.slot_id)
                }
                MemberRuntimeSnapshot::Ready => ReserveAttach::AlreadyReady,
                _ => session.member_runtimes().reserve_attach(&agent.slot_id, true),
            };

            match reservation {
                ReserveAttach::Start(owner) => {
                    self.broadcast_agent_runtime_status(
                        session.user_id(),
                        session.team_id(),
                        agent,
                        TeamAgentRuntimeStatus::Pending,
                        None,
                    );
                    work.push(MemberRuntimeReconcileWork {
                        agent: agent.clone(),
                        waiter: owner.waiter(),
                        owner: Some(owner),
                    });
                }
                ReserveAttach::Join(waiter) | ReserveAttach::Removing(waiter) => {
                    info!(
                        team_id = session.team_id(),
                        slot_id = agent.slot_id,
                        conversation_id = agent.conversation_id,
                        operation_id = waiter.operation_id(),
                        generation = session.generation(),
                        duration_ms = 0,
                        error_classification = "none",
                        "team member runtime reconciliation waiting"
                    );
                    work.push(MemberRuntimeReconcileWork {
                        agent: agent.clone(),
                        waiter,
                        owner: None,
                    });
                }
                ReserveAttach::AlreadyReady => {}
                ReserveAttach::SessionStopped => {
                    return Err(TeamError::InvalidRequest(
                        "team session stopped during reconciliation reservation".to_owned(),
                    ));
                }
            }
        }
        Ok(work)
    }

    async fn complete_member_runtime_reconciliation(
        &self,
        team_id: &str,
        user_id: &str,
        session: Arc<TeamSession>,
        work: Vec<MemberRuntimeReconcileWork>,
    ) -> Result<(), TeamError> {
        // Session-level `Starting` is leader-scoped (spec 5.4/5.5): only a
        // leader (re)attach may raise the overlay. Reconciliation that touches
        // only teammates (Ready-repair, Failed-retry of a non-lead member) keeps
        // its progress inline via `agentRuntimeStatusChanged`.
        if work.iter().any(|item| item.agent.role == TeammateRole::Lead) {
            self.publish_member_runtime_starting_if_current(&session);
        }
        let mut waiters = Vec::with_capacity(work.len());
        for item in work {
            let waiter = item.waiter;
            if let Some(owner) = item.owner {
                tokio::spawn(attach_member_runtime(
                    self.self_ref
                        .upgrade()
                        .ok_or_else(|| TeamError::InvalidRequest("team service is shutting down".to_owned()))?,
                    Arc::clone(&session),
                    user_id.to_owned(),
                    item.agent.clone(),
                    self.task_manager.clone(),
                    owner,
                    // Reconciliation repairs runtimes without a fresh delivery to
                    // re-delegate; failures surface via runtime status only.
                    false,
                ));
            }
            waiters.push((item.agent, waiter));
        }

        let outcomes = futures_util::future::join_all(
            waiters
                .into_iter()
                .map(|(agent, waiter)| async move { (agent, waiter.wait().await) }),
        )
        .await;

        let _membership_guard = self.team_membership_lock(team_id).lock_owned().await;
        let current_agents = match self.repo.get_team(user_id, team_id).await? {
            Some(row) => Team::from_row(&row)?.agents,
            None => return Err(TeamError::TeamNotFound(team_id.to_owned())),
        };
        let current_slots = current_agents
            .iter()
            .map(|agent| agent.slot_id.as_str())
            .collect::<HashSet<_>>();
        let current_session = self
            .sessions
            .get(team_id)
            .ok_or_else(|| TeamError::SessionNotFound(team_id.to_owned()))?;
        if !Arc::ptr_eq(&current_session.session, &session) {
            return Err(TeamError::SessionNotFound(team_id.to_owned()));
        }

        for (agent, outcome) in outcomes {
            if !current_slots.contains(agent.slot_id.as_str()) {
                continue;
            }
            match outcome {
                AttachOutcome::Ready | AttachOutcome::Removed => {}
                AttachOutcome::Failed(failure) => {
                    // Session-level Failed / the whole-team error are leader-scoped
                    // (spec 5.4/5.5): only a leader failure raises the full-screen
                    // failure card and blocks the team. A teammate reconciliation
                    // failure stays inline — its `agentRuntimeStatusChanged=failed`
                    // already fired in `attach_member_runtime` — and the team stays
                    // usable because the leader is ready. `ensure_session` (invoked
                    // on mount, model switches, and before sends via warmupSession)
                    // must not fail just because an unrelated teammate is broken.
                    if agent.role == TeammateRole::Lead {
                        self.broadcast_session_status(
                            user_id,
                            team_id,
                            TeamSessionStatus::Failed,
                            Some(TeamSessionPhase::AttachingAgents),
                            |payload| payload.error = Some(failure.public_reason.clone()),
                        );
                        return Err(TeamError::MemberRuntimeFailed {
                            team_id: team_id.to_owned(),
                            slot_id: agent.slot_id,
                            conversation_id: agent.conversation_id,
                            public_reason: failure.public_reason,
                        });
                    }
                }
                AttachOutcome::SessionStopped => {
                    return Err(TeamError::InvalidRequest(
                        "team session stopped during reconciliation".to_owned(),
                    ));
                }
            }
        }

        self.broadcast_session_status(user_id, team_id, TeamSessionStatus::Ready, None, |payload| {
            payload.server_count = Some(current_agents.len());
        });
        Ok(())
    }

    pub(crate) async fn cleanup_stale_member_runtime_task(
        &self,
        captured_session: &TeamSession,
        conversation_id: &str,
    ) {
        let lock = self
            .ensure_session_locks
            .entry(captured_session.team_id().to_owned())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone();
        let _guard = lock.lock().await;
        if self
            .sessions
            .get(captured_session.team_id())
            .is_some_and(|entry| !std::ptr::eq(entry.session.as_ref(), captured_session))
        {
            return;
        }
        self.task_manager
            .kill_and_wait(conversation_id, Some(AgentKillReason::TeamMcpRebuild))
            .await;
    }

    pub async fn get_conversation_config_options(
        &self,
        user_id: &str,
        team_id: &str,
        conversation_id: &str,
    ) -> Result<GetConfigOptionsResponse, TeamError> {
        let (_membership_guard, access) = self
            .lock_authorized_rostered_conversation(user_id, team_id, conversation_id)
            .await?;
        if access.role != TeamAccessRole::Owner {
            return Err(TeamError::TeamNotFound(team_id.to_owned()));
        }
        let team = Team::from_row(&access.team)?;
        let member = team
            .agents
            .iter()
            .find(|agent| agent.conversation_id == conversation_id)
            .ok_or_else(|| TeamError::TeamNotFound(team_id.to_owned()))?;
        if self.member_runtime_is_starting(team_id, &member.slot_id) {
            return Err(Self::member_runtime_starting_error(team_id, member));
        }

        self.conversation_port.get_config_options(conversation_id).await
    }

    pub async fn answer_team_conversation_ask(
        &self,
        user_id: &str,
        team_id: &str,
        conversation_id: &str,
        request_id: &str,
        answers: Option<Vec<aionui_api_types::AskQuestionAnswer>>,
    ) -> Result<(), TeamError> {
        let (_membership_guard, access) = self
            .lock_authorized_rostered_conversation(user_id, team_id, conversation_id)
            .await?;
        if access.sharing_mode != TeamSharingMode::Shared {
            return Err(TeamError::TeamNotFound(team_id.to_owned()));
        }
        let team = Team::from_row(&access.team)?;
        let member = team
            .agents
            .iter()
            .find(|agent| agent.conversation_id == conversation_id)
            .ok_or_else(|| TeamError::TeamNotFound(team_id.to_owned()))?;
        if member.role != TeammateRole::Lead {
            return Err(TeamError::TeamNotFound(team_id.to_owned()));
        }
        self.conversation_port
            .answer_team_conversation_ask(
                &access.execution_owner_id,
                conversation_id,
                request_id,
                answers,
                &self.task_manager,
            )
            .await
    }

    pub async fn get_team_conversation(
        &self,
        user_id: &str,
        team_id: &str,
        conversation_id: &str,
    ) -> Result<ConversationResponse, TeamError> {
        let (_membership_guard, access) = self
            .lock_authorized_rostered_conversation(user_id, team_id, conversation_id)
            .await?;
        let conversation = self
            .conversation_port
            .get_team_conversation(&access.execution_owner_id, conversation_id)
            .await?;
        Ok(safe_team_conversation_projection(conversation, team_id))
    }

    pub async fn list_team_conversation_messages(
        &self,
        user_id: &str,
        team_id: &str,
        conversation_id: &str,
        query: ListMessagesQuery,
    ) -> Result<MessageListResponse, TeamError> {
        let (_membership_guard, access) = self
            .lock_authorized_rostered_conversation(user_id, team_id, conversation_id)
            .await?;
        self.conversation_port
            .list_team_conversation_messages(&access.execution_owner_id, conversation_id, query)
            .await
    }

    pub async fn latest_team_conversation_message(
        &self,
        user_id: &str,
        team_id: &str,
        conversation_id: &str,
        message_type: &str,
    ) -> Result<Option<MessageResponse>, TeamError> {
        let (_membership_guard, access) = self
            .lock_authorized_rostered_conversation(user_id, team_id, conversation_id)
            .await?;
        self.conversation_port
            .latest_team_conversation_message(&access.execution_owner_id, conversation_id, message_type)
            .await
    }

    pub async fn list_team_conversation_artifacts(
        &self,
        user_id: &str,
        team_id: &str,
        conversation_id: &str,
    ) -> Result<ConversationArtifactListResponse, TeamError> {
        let (_membership_guard, access) = self
            .lock_authorized_rostered_conversation(user_id, team_id, conversation_id)
            .await?;
        self.conversation_port
            .list_team_conversation_artifacts(&access.execution_owner_id, conversation_id)
            .await
    }

    pub async fn team_conversation_slash_commands(
        &self,
        user_id: &str,
        team_id: &str,
        conversation_id: &str,
    ) -> Result<Vec<SlashCommandItem>, TeamError> {
        let (_membership_guard, access) = self
            .lock_authorized_rostered_conversation(user_id, team_id, conversation_id)
            .await?;
        self.conversation_port
            .team_conversation_slash_commands(&access.execution_owner_id, conversation_id)
            .await
    }

    pub async fn team_conversation_usage(
        &self,
        user_id: &str,
        team_id: &str,
        conversation_id: &str,
    ) -> Result<Option<serde_json::Value>, TeamError> {
        let (_membership_guard, access) = self
            .lock_authorized_rostered_conversation(user_id, team_id, conversation_id)
            .await?;
        self.conversation_port
            .team_conversation_usage(&access.execution_owner_id, conversation_id)
            .await
    }

    pub async fn list_team_conversation_confirmations(
        &self,
        user_id: &str,
        team_id: &str,
        conversation_id: &str,
    ) -> Result<ConfirmationListResponse, TeamError> {
        let (_membership_guard, access) = self
            .lock_authorized_rostered_conversation(user_id, team_id, conversation_id)
            .await?;
        self.conversation_port
            .list_team_conversation_confirmations(&access.execution_owner_id, conversation_id, &self.task_manager)
            .await
    }

    pub async fn set_conversation_config_option(
        &self,
        user_id: &str,
        team_id: &str,
        conversation_id: &str,
        option_id: &str,
        request: SetConfigOptionRequest,
    ) -> Result<SetConfigOptionResponse, TeamError> {
        let row = self.load_owned_team_row(user_id, team_id).await?;
        let team = Team::from_row(&row)?;
        let member = team
            .agents
            .iter()
            .find(|agent| agent.conversation_id == conversation_id)
            .ok_or_else(|| TeamError::AgentNotFound(conversation_id.to_owned()))?;
        if self.member_runtime_is_starting(team_id, &member.slot_id) {
            return Err(Self::member_runtime_starting_error(team_id, member));
        }

        let options = self.conversation_port.get_config_options(conversation_id).await?;
        let is_global_mode = member.role == TeammateRole::Lead
            && options.config_options.iter().any(|option| {
                option.id == option_id && (option.category.as_deref() == Some("mode") || option.id == "mode")
            });
        if is_global_mode
            && let Some(starting_member) = team
                .agents
                .iter()
                .find(|agent| self.member_runtime_is_starting(team_id, &agent.slot_id))
        {
            return Err(Self::member_runtime_starting_error(team_id, starting_member));
        }
        // Matches the frontend's own model-option lookup (category first, then a
        // literal `model` id), so both sides agree on which option is the model.
        let is_model_option = options.config_options.iter().any(|option| {
            option.id == option_id && (option.category.as_deref() == Some("model") || option.id == "model")
        });
        let slot_id = member.slot_id.clone();
        // Captured before the call moves `request`. This is the value to persist,
        // NOT the option's `current_value` in the response: a `PendingNextTurn`
        // confirmation deliberately still reads back the OLD value, so echoing the
        // readback would persist the model the user just switched away from.
        let requested_model = is_model_option.then(|| request.value.trim().to_owned());

        let response = self
            .conversation_port
            .set_config_option(conversation_id, option_id, request)
            .await?;

        // The runtime accepted the switch, so the roster and the persisted
        // conversation state must follow — including for `PendingNextTurn`, where
        // the value governs from the next turn and would otherwise be lost on the
        // next rebuild. A persistence failure must NOT be reported as a failed
        // switch, because the switch already happened; log it and let the
        // session-start reconcile repair the roster.
        if let Some(model) = requested_model.filter(|value| !value.is_empty())
            && let Err(error) = self
                .persist_member_model_selection(
                    user_id,
                    team_id,
                    &slot_id,
                    &model,
                    ModelPersistTrigger::RuntimeConfirmed,
                )
                .await
        {
            warn!(
                team_id,
                slot_id,
                conversation_id,
                model,
                error = %error,
                "team member model switch applied but could not be persisted"
            );
        }

        Ok(response)
    }

    fn member_runtime_is_starting(&self, team_id: &str, slot_id: &str) -> bool {
        self.sessions
            .get(team_id)
            .and_then(|entry| entry.session.work_coordinator().slot_snapshot(slot_id))
            .is_some_and(|snapshot| matches!(snapshot.runtime_constraint, RuntimeConstraint::Starting { .. }))
    }

    fn member_runtime_starting_error(team_id: &str, member: &TeamAgent) -> TeamError {
        TeamError::MemberRuntimeStarting {
            team_id: team_id.to_owned(),
            slot_id: member.slot_id.clone(),
            conversation_id: member.conversation_id.clone(),
        }
    }

    fn broadcast_session_status<F>(
        &self,
        user_id: &str,
        team_id: &str,
        status: TeamSessionStatus,
        phase: Option<TeamSessionPhase>,
        customize: F,
    ) where
        F: FnOnce(&mut TeamSessionStatusPayload),
    {
        let mut payload = TeamSessionStatusPayload {
            team_id: team_id.to_owned(),
            status,
            phase,
            server_count: None,
            error: None,
        };
        customize(&mut payload);
        // Session-level status drives the full-screen warmup overlay (leader-only,
        // spec 5.4/5.5). This is a low-volume lifecycle boundary per team, so log
        // at info for production diagnosability — the reason is already the
        // sanitized public failure text, never a raw payload.
        info!(
            team_id = %payload.team_id,
            status = ?payload.status,
            phase = ?payload.phase,
            server_count = ?payload.server_count,
            error = payload.error.as_deref().unwrap_or(""),
            "team session status broadcast"
        );
        // Keep per-user scoping so the overlay event reaches only the owning
        // user's WebSocket subscribers.
        let mut value = serde_json::to_value(payload).expect("serialize team session status payload");
        value["user_id"] = serde_json::Value::String(user_id.to_owned());
        let event = WebSocketMessage::new(TEAM_SESSION_STATUS_CHANGED_EVENT, value);
        self.broadcaster.broadcast(event);
    }

    fn spawn_slow_monitor(session: Arc<TeamSession>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                let snapshot = session.work_coordinator().snapshot();
                session.team_run_manager().publish_snapshot_update(&snapshot);
            }
        })
    }

    fn broadcast_team_created(&self, user_id: &str, team_id: &str, team_name: &str) {
        info!(team_id = %team_id, event_name = TEAM_CREATED_EVENT, "team event broadcast");
        TeamEventEmitter::new(team_id.to_owned(), user_id.to_owned(), self.broadcaster.clone()).broadcast_event(
            TEAM_CREATED_EVENT,
            serde_json::json!({ "user_id": user_id, "team_id": team_id, "team_name": team_name }),
        );
        self.broadcast_team_list_changed(user_id, team_id, "created");
    }

    fn broadcast_team_removed(&self, user_id: &str, team_id: &str) {
        info!(team_id = %team_id, event_name = TEAM_REMOVED_EVENT, "team event broadcast");
        TeamEventEmitter::new(team_id.to_owned(), user_id.to_owned(), self.broadcaster.clone()).broadcast_event(
            TEAM_REMOVED_EVENT,
            serde_json::json!({ "user_id": user_id, "team_id": team_id }),
        );
        self.broadcast_team_list_changed(user_id, team_id, "removed");
    }

    fn broadcast_team_renamed(&self, user_id: &str, team_id: &str, team_name: &str) {
        info!(team_id = %team_id, event_name = TEAM_RENAMED_EVENT, "team event broadcast");
        TeamEventEmitter::new(team_id.to_owned(), user_id.to_owned(), self.broadcaster.clone()).broadcast_event(
            TEAM_RENAMED_EVENT,
            serde_json::json!({ "user_id": user_id, "team_id": team_id, "team_name": team_name }),
        );
        self.broadcast_team_list_changed(user_id, team_id, "renamed");
    }

    fn broadcast_team_list_changed(&self, user_id: &str, team_id: &str, action: &str) {
        info!(team_id = %team_id, event_name = crate::events::TEAM_LIST_CHANGED_EVENT, action, "team event broadcast");
        TeamEventEmitter::new(team_id.to_owned(), user_id.to_owned(), self.broadcaster.clone()).broadcast_event(
            crate::events::TEAM_LIST_CHANGED_EVENT,
            serde_json::json!({ "user_id": user_id, "team_id": team_id, "action": action }),
        );
    }

    pub(crate) fn broadcast_agent_runtime_status(
        &self,
        user_id: &str,
        team_id: &str,
        agent: &TeamAgent,
        status: TeamAgentRuntimeStatus,
        error: Option<String>,
    ) {
        TeamEventEmitter::new(team_id.to_owned(), user_id.to_owned(), self.broadcaster.clone())
            .broadcast_agent_runtime_status(agent, status, error);
    }

    /// Register an event loop for an attaching agent.
    ///
    /// Called from `attach_member_runtime` (the single attach path used by
    /// leader cold-start, reconciliation, `add_agent`, `spawn_agent`, and lazy
    /// wakeup) after the agent process warms up, so it gets its own drain loop.
    pub(crate) fn register_event_loop(
        &self,
        session: &Arc<TeamSession>,
        slot_id: &str,
    ) -> Result<bool, EventLoopRegistrationError> {
        let registry = session.event_loops();

        let ctx = AgentLoopContext {
            team_id: session.team_id().to_owned(),
            slot_id: slot_id.to_owned(),
            user_id: session.user_id().to_owned(),
            session: session.clone(),
            scheduler: session.scheduler().clone(),
            mailbox: session.mailbox().clone(),
            turn_port: self.turn_port.clone(),
            registry: registry.clone(),
        };
        match registry.spawn(slot_id, ctx) {
            Ok(()) => {
                info!(
                    team_id = session.team_id(),
                    slot_id,
                    generation = session.generation(),
                    "agent event loop registered"
                );
                Ok(true)
            }
            Err(EventLoopRegistrationError::Duplicate) => Ok(false),
            Err(error) => Err(error),
        }
    }

    pub async fn get_session_user_id(&self, team_id: &str) -> Option<String> {
        self.sessions.get(team_id).map(|e| e.session.user_id().to_owned())
    }

    pub(crate) fn capture_published_session(&self, expected: &TeamSession) -> Option<Arc<TeamSession>> {
        self.sessions
            .get(expected.team_id())
            .and_then(|entry| std::ptr::eq(entry.session.as_ref(), expected).then(|| Arc::clone(&entry.session)))
    }

    /// Run a synchronous side effect only while `expected` is still the
    /// published session. Keeping the map guard alive through `action`
    /// serializes the effect with session removal/replacement.
    pub(crate) fn with_published_session<R>(
        &self,
        expected: &TeamSession,
        action: impl FnOnce(&TeamSession) -> R,
    ) -> Option<R> {
        let entry = self.sessions.get(expected.team_id())?;
        std::ptr::eq(entry.session.as_ref(), expected).then(|| action(&entry.session))
    }

    pub(crate) fn publish_member_runtime_ready_if_current(&self, expected: &TeamSession, agent: &TeamAgent) -> bool {
        self.with_published_session(expected, |_| {
            self.broadcast_agent_runtime_status(
                expected.user_id(),
                expected.team_id(),
                agent,
                TeamAgentRuntimeStatus::Ready,
                None,
            );
        })
        .is_some()
    }

    pub(crate) fn publish_member_runtime_starting_if_current(&self, expected: &TeamSession) -> bool {
        self.with_published_session(expected, |_| {
            self.broadcast_session_status(
                expected.user_id(),
                expected.team_id(),
                TeamSessionStatus::Starting,
                Some(TeamSessionPhase::AttachingAgents),
                |_| {},
            );
        })
        .is_some()
    }

    pub(crate) fn publish_member_runtime_failed_if_current(&self, expected: &TeamSession, reason: &str) -> bool {
        self.with_published_session(expected, |_| {
            self.broadcast_session_status(
                expected.user_id(),
                expected.team_id(),
                TeamSessionStatus::Failed,
                Some(TeamSessionPhase::AttachingAgents),
                |payload| payload.error = Some(reason.to_owned()),
            );
        })
        .is_some()
    }

    pub(crate) async fn refresh_member_runtime_status(&self, expected: &TeamSession) {
        let _membership_guard = self.team_membership_lock(expected.team_id()).lock_owned().await;
        let Ok(Some(row)) = self.repo.get_team(expected.user_id(), expected.team_id()).await else {
            return;
        };
        let Ok(team) = Team::from_row(&row) else {
            return;
        };

        // Session-level status is leader-scoped: "Ready = leader ready = team
        // usable" (spec 5.2). It drives the full-screen warmup overlay, which
        // must reflect the leader only (spec 5.4/5.5). Teammate runtimes
        // (dormant/pending/ready/failed) are surfaced per-member via
        // `agentRuntimeStatusChanged` and must NOT flip the session status, or
        // the overlay would resurface on lazy wakeup / add-member and a teammate
        // failure would raise the full-screen failure card.
        let Some(leader) = team.agents.iter().find(|agent| agent.role == TeammateRole::Lead) else {
            // No lead in the roster is a malformed team; bootstrap already
            // reports it. Nothing to publish here.
            return;
        };

        match expected.member_runtimes().snapshot(&leader.slot_id) {
            MemberRuntimeSnapshot::Ready => {
                let _ = self.with_published_session(expected, |_| {
                    self.broadcast_session_status(
                        expected.user_id(),
                        expected.team_id(),
                        TeamSessionStatus::Ready,
                        None,
                        |payload| {
                            payload.server_count = Some(team.agents.len());
                        },
                    );
                });
            }
            MemberRuntimeSnapshot::Failed { failure, .. } => {
                self.publish_member_runtime_failed_if_current(expected, &failure.public_reason);
            }
            // An in-flight leader attach/remove (cold start, repair, retry) is
            // the only case that legitimately raises the overlay again. The lead
            // cannot be removed, so `Removing` is defensive.
            MemberRuntimeSnapshot::Attaching { .. } | MemberRuntimeSnapshot::Removing { .. } => {
                self.publish_member_runtime_starting_if_current(expected);
            }
            // The leader is attached at bootstrap and never dormant in steady
            // state; treat a stray Absent defensively as in-flight rather than
            // prematurely declaring Ready.
            MemberRuntimeSnapshot::Absent => {
                self.publish_member_runtime_starting_if_current(expected);
            }
            MemberRuntimeSnapshot::SessionStopped => {}
        }
    }

    pub async fn get_run_state(&self, user_id: &str, team_id: &str) -> Result<TeamRunStateResponse, TeamError> {
        self.authorize_team(user_id, team_id).await?;
        let session = self.sessions.get(team_id).map(|entry| Arc::clone(&entry.session));
        let Some(session) = session else {
            return Ok(TeamRunStateResponse {
                session_generation: None,
                active_run: None,
                slot_work: Vec::new(),
            });
        };
        let snapshot = session.work_coordinator().snapshot();
        let active_run = session.team_run_manager().current_payload(&snapshot).filter(|run| {
            matches!(
                run.status,
                aionui_api_types::TeamRunStatus::Accepted
                    | aionui_api_types::TeamRunStatus::Running
                    | aionui_api_types::TeamRunStatus::Cancelling
            )
        });
        let slot_work = snapshot.slots.iter().map(TeamRunManager::slot_payload).collect();
        Ok(TeamRunStateResponse {
            session_generation: Some(snapshot.session_generation),
            active_run,
            slot_work,
        })
    }

    pub fn get_session_scheduler(&self, team_id: &str) -> Option<Arc<crate::scheduler::TeammateManager>> {
        self.sessions.get(team_id).map(|e| e.session.scheduler().clone())
    }

    pub async fn resolve_team_tool_context(
        &self,
        user_id: &str,
        conversation_id: &str,
    ) -> Result<ResolvedTeamToolContext, TeamToolErrorPayload> {
        let Some(binding_lookup) = self
            .conversation_port
            .lookup_team_binding_by_conversation(conversation_id)
            .await
            .map_err(|error| error_payload(TeamToolErrorCode::RuntimeContextMissing, error.to_string()))?
        else {
            return Err(error_payload(
                TeamToolErrorCode::ConversationNotFound,
                "conversation not found",
            ));
        };

        if binding_lookup.user_id != user_id {
            return Err(error_payload(
                TeamToolErrorCode::PermissionDenied,
                "conversation does not belong to user",
            ));
        }

        let Some(team_id) = binding_lookup.team_id.clone() else {
            return Ok(ResolvedTeamToolContext {
                response: TeamToolContextResponse {
                    in_team: false,
                    conversation_id: conversation_id.to_owned(),
                    team_id: None,
                    team_name: None,
                    slot_id: None,
                    role: None,
                    agent_name: None,
                    transport: None,
                    allowed_tools: Vec::new(),
                },
                context: None,
            });
        };

        let team_row = self
            .repo
            .get_team(user_id, &team_id)
            .await
            .map_err(|error| error_payload(TeamToolErrorCode::RuntimeContextMissing, error.to_string()))?
            .ok_or_else(|| error_payload(TeamToolErrorCode::TeamNotFound, "team not found"))?;

        let binding = TeamSessionBinding {
            team_id: team_id.clone(),
            team_name: Some(team_row.name.clone()),
            slot_id: binding_lookup.slot_id,
            role: binding_lookup.role,
            runtime_seed: Default::default(),
            mcp: None,
        };
        let agents: Vec<crate::types::TeamAgent> = serde_json::from_str(&team_row.agents)
            .map_err(|error| error_payload(TeamToolErrorCode::RuntimeContextMissing, error.to_string()))?;
        let agent = agent_for_conversation(&agents, conversation_id, &binding)?;
        let context = crate::tool_executor::TeamToolContext {
            team_id: team_id.clone(),
            caller_slot_id: agent.slot_id.clone(),
            caller_role: agent.role,
            user_id: Some(user_id.to_owned()),
            conversation_id: Some(conversation_id.to_owned()),
            transport: TeamToolTransport::CliAssumed,
        };
        let allowed_tools = aionui_api_types::team_tool_descriptors_for_role(role_to_tool_role(agent.role))
            .into_iter()
            .map(|descriptor| descriptor.name)
            .collect::<Vec<_>>();
        Ok(ResolvedTeamToolContext {
            response: TeamToolContextResponse {
                in_team: true,
                conversation_id: conversation_id.to_owned(),
                team_id: Some(team_id),
                team_name: Some(team_row.name),
                slot_id: Some(agent.slot_id.clone()),
                role: Some(role_to_tool_role(agent.role)),
                agent_name: Some(agent.name.clone()),
                transport: Some(TeamToolTransport::CliAssumed),
                allowed_tools,
            },
            context: Some(context),
        })
    }

    pub async fn execute_team_tool(
        &self,
        context: &crate::tool_executor::TeamToolContext,
        call: TeamToolCall,
    ) -> Result<serde_json::Value, TeamToolErrorPayload> {
        let scheduler = self
            .get_session_scheduler(&context.team_id)
            .ok_or_else(|| error_payload(TeamToolErrorCode::TeamNotFound, "active team session not found"))?;
        execute_with_scheduler(&scheduler, &self.self_ref, context, call).await
    }

    #[cfg(test)]
    fn session_has_slow_monitor(&self, team_id: &str) -> bool {
        self.sessions
            .get(team_id)
            .map(|entry| !entry.slow_monitor_handle.is_finished())
            .unwrap_or(false)
    }

    #[cfg(test)]
    fn session_count_for_test(&self) -> usize {
        self.sessions.len()
    }

    pub async fn stop_session(&self, user_id: &str, team_id: &str) -> Result<(), TeamError> {
        self.load_owned_team(user_id, team_id).await?;
        self.stop_session_unchecked(team_id);
        Ok(())
    }

    pub fn stop_sessions_for_user(&self, user_id: &str) -> usize {
        let team_ids: Vec<String> = self
            .sessions
            .iter()
            .filter(|entry| entry.session.user_id() == user_id)
            .map(|entry| entry.key().clone())
            .collect();
        let stopped = team_ids.len();
        for team_id in team_ids {
            self.stop_session_unchecked(&team_id);
        }
        stopped
    }

    fn stop_session_unchecked(&self, team_id: &str) {
        if let Some((_, entry)) = self.sessions.remove(team_id) {
            entry.slow_monitor_handle.abort();
            entry.session.stop();
        }
    }

    pub async fn cleanup_idle_team_runtime_tasks(
        &self,
        idle_conversation_ids: Vec<String>,
        active_leases: &ActiveLeaseRegistry,
        idle_threshold_ms: TimestampMs,
    ) -> Vec<String> {
        if idle_conversation_ids.is_empty() {
            return Vec::new();
        }

        let idle_conversation_set: HashSet<String> = idle_conversation_ids.iter().cloned().collect();
        let now = now_ms();
        let mut handled_conversations = HashSet::new();
        let mut cleanup_teams = Vec::new();

        for entry in self.sessions.iter() {
            let team_id = entry.key().clone();
            let session = Arc::clone(&entry.session);
            let agents = session.scheduler().list_agents().await;
            let matched_idle_count = agents
                .iter()
                .filter(|agent| idle_conversation_set.contains(&agent.conversation_id))
                .count();
            if matched_idle_count == 0 {
                continue;
            }

            for agent in &agents {
                handled_conversations.insert(agent.conversation_id.clone());
            }

            if session.team_run_manager().current_active_run_id().is_some() {
                debug!(
                    team_id,
                    matched_idle_count, "team idle cleanup skipped because team run is active"
                );
                continue;
            }

            if agents
                .iter()
                .any(|agent| active_leases.active_until(&agent.conversation_id).is_some())
            {
                debug!(
                    team_id,
                    matched_idle_count, "team idle cleanup skipped because at least one member has an active lease"
                );
                continue;
            }

            if !agents.iter().all(|agent| {
                self.task_manager
                    .get_task(&agent.conversation_id)
                    .map(|task| is_idle_collectable_team_member(&task, now, idle_threshold_ms))
                    .unwrap_or(true)
            }) {
                debug!(
                    team_id,
                    matched_idle_count, "team idle cleanup skipped because at least one member runtime task is active"
                );
                continue;
            }

            cleanup_teams.push((team_id, agents, matched_idle_count));
        }

        for (team_id, agents, matched_idle_count) in cleanup_teams {
            info!(
                team_id,
                matched_idle_count,
                member_count = agents.len(),
                "team idle cleanup stopping idle team session"
            );
            info!(team_id, reason = "idle_cleanup", "broadcasting team session stopped");
            if let Some(entry) = self.sessions.get(&team_id) {
                self.broadcast_session_status(
                    entry.session.user_id(),
                    &team_id,
                    TeamSessionStatus::Stopped,
                    None,
                    |_| {},
                );
            }
            self.stop_session_unchecked(&team_id);
            for agent in agents {
                self.task_manager
                    .kill_and_wait(&agent.conversation_id, Some(AgentKillReason::IdleTimeout))
                    .await;
            }
        }

        idle_conversation_ids
            .into_iter()
            .filter(|conversation_id| !handled_conversations.contains(conversation_id))
            .collect()
    }

    pub async fn send_message(
        &self,
        user_id: &str,
        team_id: &str,
        content: &str,
        files: Option<Vec<ChatFileRef>>,
    ) -> Result<TeamRunAckResponse, TeamError> {
        self.send_message_with_local_admin(user_id, user_id == "system_default_user", team_id, content, files)
            .await
    }

    pub async fn send_message_with_local_admin(
        &self,
        user_id: &str,
        is_local_admin: bool,
        team_id: &str,
        content: &str,
        files: Option<Vec<ChatFileRef>>,
    ) -> Result<TeamRunAckResponse, TeamError> {
        let access = self.authorize_team(user_id, team_id).await?;
        self.ensure_session_inner(team_id, Some(&access.execution_owner_id))
            .await?;
        let (content, files) = self
            .resolve_team_message_attachments(&access, is_local_admin, content, files)
            .await?;
        // Serialize final membership validation and enqueue with member removal.
        let _membership_guard = self.team_membership_lock(team_id).lock_owned().await;
        let current_access = self.authorize_team(user_id, team_id).await?;
        if current_access.execution_owner_id != access.execution_owner_id {
            return Err(TeamError::TeamNotFound(team_id.to_owned()));
        }
        let session = self.published_session(team_id)?;
        session.send_message_as_actor(user_id, &content, files).await
    }

    pub async fn send_message_to_agent(
        &self,
        user_id: &str,
        team_id: &str,
        slot_id: &str,
        content: &str,
        files: Option<Vec<ChatFileRef>>,
    ) -> Result<TeamRunAckResponse, TeamError> {
        self.send_message_to_agent_with_local_admin(
            user_id,
            user_id == "system_default_user",
            team_id,
            slot_id,
            content,
            files,
        )
        .await
    }

    pub async fn send_message_to_agent_with_local_admin(
        &self,
        user_id: &str,
        is_local_admin: bool,
        team_id: &str,
        slot_id: &str,
        content: &str,
        files: Option<Vec<ChatFileRef>>,
    ) -> Result<TeamRunAckResponse, TeamError> {
        let access = self.authorize_team(user_id, team_id).await?;
        if !can_send_direct_team_message(access.role, access.team.lead_agent_id.as_deref(), slot_id) {
            return Err(TeamError::Forbidden(
                "collaborators may only send directly to the shared Team Lead".into(),
            ));
        }
        self.ensure_session_inner(team_id, Some(&access.execution_owner_id))
            .await?;
        let (content, files) = self
            .resolve_team_message_attachments(&access, is_local_admin, content, files)
            .await?;
        let _membership_guard = self.team_membership_lock(team_id).lock_owned().await;
        let current_access = self.authorize_team(user_id, team_id).await?;
        if current_access.execution_owner_id != access.execution_owner_id {
            return Err(TeamError::TeamNotFound(team_id.to_owned()));
        }
        if !can_send_direct_team_message(
            current_access.role,
            current_access.team.lead_agent_id.as_deref(),
            slot_id,
        ) {
            return Err(TeamError::Forbidden(
                "collaborators may only send directly to the shared Team Lead".into(),
            ));
        }
        let session = self.published_session(team_id)?;
        session
            .send_message_to_agent_as_actor(user_id, slot_id, &content, files)
            .await
    }

    pub async fn interrupt_agent(
        &self,
        user_id: &str,
        team_id: &str,
        slot_id: &str,
        request: InterruptTeamAgentRequest,
    ) -> Result<TeamInterruptAgentResponse, TeamError> {
        self.interrupt_agent_with_local_admin(user_id, user_id == "system_default_user", team_id, slot_id, request)
            .await
    }

    pub async fn interrupt_agent_with_local_admin(
        &self,
        user_id: &str,
        is_local_admin: bool,
        team_id: &str,
        slot_id: &str,
        request: InterruptTeamAgentRequest,
    ) -> Result<TeamInterruptAgentResponse, TeamError> {
        let access = self.authorize_team(user_id, team_id).await?;
        if access.role != TeamAccessRole::Owner {
            return Err(TeamError::TeamNotFound(team_id.to_owned()));
        }
        self.ensure_session_inner(team_id, Some(&access.execution_owner_id))
            .await?;
        let (message, files) = self
            .resolve_team_message_attachments(&access, is_local_admin, &request.message, request.files)
            .await?;
        let _membership_guard = self.team_membership_lock(team_id).lock_owned().await;
        let current_access = self.authorize_team(user_id, team_id).await?;
        if current_access.role != TeamAccessRole::Owner
            || current_access.execution_owner_id != access.execution_owner_id
        {
            return Err(TeamError::TeamNotFound(team_id.to_owned()));
        }
        self.published_session(team_id)?
            .interrupt_agent_from_user(slot_id, &message, files, request.reason, request.queued_policy)
            .await
    }

    fn published_session(&self, team_id: &str) -> Result<Arc<TeamSession>, TeamError> {
        self.sessions
            .get(team_id)
            .map(|entry| Arc::clone(&entry.session))
            .ok_or_else(|| TeamError::SessionNotFound(team_id.to_owned()))
    }

    pub(crate) async fn peek_agent_messages(
        &self,
        team_id: &str,
        slot_id: &str,
    ) -> Result<crate::session::AgentInboxPeek, TeamError> {
        self.published_session(team_id)?.peek_agent_messages(slot_id).await
    }

    pub(crate) async fn observe_agent_messages(
        &self,
        team_id: &str,
        slot_id: &str,
        expected_batch_id: &str,
        message_ids: &[String],
    ) -> Result<ObserveMessagesResult, TeamError> {
        self.published_session(team_id)?
            .observe_agent_messages(slot_id, expected_batch_id, message_ids)
            .await
    }
}

#[derive(Debug, Clone)]
pub struct TeamUploadStreamData {
    pub file_bytes: Vec<u8>,
    pub file_name: Option<String>,
    pub content_type: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct TeamUploadMetadata {
    pub upload_id: String,
    pub extension: Option<String>,
    pub content_type: String,
    pub size_bytes: usize,
    pub created_at: TimestampMs,
}

struct TeamUploadDirectory {
    path: PathBuf,
    #[cfg(unix)]
    handle: std::fs::File,
}

#[cfg(unix)]
fn open_upload_child_directory(parent: &std::fs::File, name: &str, team_id: &str) -> Result<std::fs::File, TeamError> {
    use rustix::fs::{Mode, OFlags, openat};

    let flags = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC;
    match openat(parent, name, flags, Mode::empty()) {
        Ok(directory) => Ok(std::fs::File::from(directory)),
        Err(error) if error == rustix::io::Errno::NOENT => {
            match rustix::fs::mkdirat(parent, name, Mode::from_raw_mode(0o700)) {
                Ok(()) => {}
                Err(error) if error == rustix::io::Errno::EXIST => {}
                Err(_) => return Err(TeamError::TeamNotFound(team_id.to_owned())),
            }
            openat(parent, name, flags, Mode::empty())
                .map(std::fs::File::from)
                .map_err(|_| TeamError::TeamNotFound(team_id.to_owned()))
        }
        Err(_) => Err(TeamError::TeamNotFound(team_id.to_owned())),
    }
}

impl TeamUploadDirectory {
    fn remove_team(storage_root: &Path, team_id: &str) -> Result<(), TeamError> {
        if !team_id
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || character == '-' || character == '_')
        {
            return Err(TeamError::TeamUploadStorageCleanupFailed);
        }

        let root_metadata = match std::fs::symlink_metadata(storage_root) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(_) => return Err(TeamError::TeamUploadStorageCleanupFailed),
        };
        if root_metadata.file_type().is_symlink() || !root_metadata.is_dir() {
            return Err(TeamError::TeamUploadStorageCleanupFailed);
        }

        let team_path = storage_root.join(team_id);
        let team_metadata = match std::fs::symlink_metadata(&team_path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(_) => return Err(TeamError::TeamUploadStorageCleanupFailed),
        };
        if team_metadata.file_type().is_symlink() || !team_metadata.is_dir() {
            return Err(TeamError::TeamUploadStorageCleanupFailed);
        }

        std::fs::remove_dir_all(team_path).map_err(|_| TeamError::TeamUploadStorageCleanupFailed)
    }

    #[cfg(unix)]
    fn open(storage_root: &Path, team_id: &str) -> Result<Self, TeamError> {
        if !team_id
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || character == '-' || character == '_')
        {
            return Err(TeamError::TeamNotFound(team_id.to_owned()));
        }
        std::fs::create_dir_all(storage_root).map_err(|_| TeamError::TeamNotFound(team_id.to_owned()))?;
        let root_metadata =
            std::fs::symlink_metadata(storage_root).map_err(|_| TeamError::TeamNotFound(team_id.to_owned()))?;
        if root_metadata.file_type().is_symlink() || !root_metadata.is_dir() {
            return Err(TeamError::TeamNotFound(team_id.to_owned()));
        }
        let root_handle = std::fs::File::open(storage_root).map_err(|_| TeamError::TeamNotFound(team_id.to_owned()))?;
        let handle = open_upload_child_directory(&root_handle, team_id, team_id)?;
        let canonical_root = storage_root
            .canonicalize()
            .map_err(|_| TeamError::TeamNotFound(team_id.to_owned()))?;
        let path = canonical_root.join(team_id);
        let canonical_path = path
            .canonicalize()
            .map_err(|_| TeamError::TeamNotFound(team_id.to_owned()))?;
        if !canonical_path.starts_with(&canonical_root) {
            return Err(TeamError::TeamNotFound(team_id.to_owned()));
        }
        Ok(Self {
            path: canonical_path,
            handle,
        })
    }

    #[cfg(target_os = "linux")]
    fn quota_path(&self) -> PathBuf {
        use std::os::fd::AsRawFd;

        PathBuf::from(format!("/proc/self/fd/{}", self.handle.as_raw_fd()))
    }

    #[cfg(all(unix, not(target_os = "linux")))]
    fn quota_path(&self) -> PathBuf {
        self.path.clone()
    }

    #[cfg(not(unix))]
    fn open(storage_root: &Path, team_id: &str) -> Result<Self, TeamError> {
        if !team_id
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || character == '-' || character == '_')
        {
            return Err(TeamError::TeamNotFound(team_id.to_owned()));
        }
        std::fs::create_dir_all(storage_root).map_err(|_| TeamError::TeamNotFound(team_id.to_owned()))?;
        let root_metadata =
            std::fs::symlink_metadata(storage_root).map_err(|_| TeamError::TeamNotFound(team_id.to_owned()))?;
        if root_metadata.file_type().is_symlink() || !root_metadata.is_dir() {
            return Err(TeamError::TeamNotFound(team_id.to_owned()));
        }
        let team_dir = storage_root.join(team_id);
        if !team_dir.exists() {
            std::fs::create_dir(&team_dir).map_err(|_| TeamError::TeamNotFound(team_id.to_owned()))?;
        }
        let metadata = std::fs::symlink_metadata(&team_dir).map_err(|_| TeamError::TeamNotFound(team_id.to_owned()))?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(TeamError::TeamNotFound(team_id.to_owned()));
        }
        let canonical_root = storage_root
            .canonicalize()
            .map_err(|_| TeamError::TeamNotFound(team_id.to_owned()))?;
        let path = team_dir
            .canonicalize()
            .map_err(|_| TeamError::TeamNotFound(team_id.to_owned()))?;
        if !path.starts_with(&canonical_root) {
            return Err(TeamError::TeamNotFound(team_id.to_owned()));
        }
        Ok(Self { path })
    }

    #[cfg(not(unix))]
    fn quota_path(&self) -> PathBuf {
        self.path.clone()
    }

    fn create_new(&self, name: &str, bytes: &[u8]) -> Result<(), TeamError> {
        #[cfg(unix)]
        let mut file = {
            use rustix::fs::{Mode, OFlags, openat};

            let flags = OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC;
            let file = openat(&self.handle, name, flags, Mode::from_raw_mode(0o600))
                .map_err(|_| TeamError::InvalidRequest("failed to create uploaded file".into()))?;
            std::fs::File::from(file)
        };
        #[cfg(not(unix))]
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(self.path.join(name))
            .map_err(|_| TeamError::InvalidRequest("failed to create uploaded file".into()))?;

        if file.write_all(bytes).is_err() {
            drop(file);
            self.remove_file(name);
            return Err(TeamError::InvalidRequest("failed to write uploaded file".into()));
        }
        Ok(())
    }

    fn remove_file(&self, name: &str) {
        #[cfg(unix)]
        {
            let _ = rustix::fs::unlinkat(&self.handle, name, rustix::fs::AtFlags::empty());
        }
        #[cfg(not(unix))]
        {
            let _ = std::fs::remove_file(self.path.join(name));
        }
    }
}

fn team_upload_usage(uploads_dir: &Path, team_id: &str) -> Result<(u64, usize), TeamError> {
    let entries = std::fs::read_dir(uploads_dir).map_err(|_| TeamError::TeamNotFound(team_id.to_owned()))?;
    let mut total_bytes = 0u64;
    let mut file_count = 0usize;
    let mut entry_count = 0usize;

    for entry in entries {
        let entry = entry.map_err(|_| TeamError::TeamNotFound(team_id.to_owned()))?;
        let metadata =
            std::fs::symlink_metadata(entry.path()).map_err(|_| TeamError::TeamNotFound(team_id.to_owned()))?;
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err(TeamError::TeamNotFound(team_id.to_owned()));
        }

        entry_count = entry_count.saturating_add(1);
        total_bytes = total_bytes.saturating_add(metadata.len());
        if !entry.file_name().to_string_lossy().ends_with(".meta.json") {
            file_count = file_count.saturating_add(1);
        }
        if total_bytes > TEAM_UPLOAD_MAX_STORAGE_BYTES
            || file_count > TEAM_UPLOAD_MAX_FILE_COUNT
            || entry_count > TEAM_UPLOAD_MAX_FILE_COUNT * 2
        {
            return Err(TeamError::TeamUploadQuotaExceeded);
        }
    }

    Ok((total_bytes, file_count))
}

fn extract_sanitized_extension(file_name: Option<&str>, content_type: Option<&str>) -> Option<String> {
    if let Some(name) = file_name
        && let Some(ext) = Path::new(name).extension().and_then(|e| e.to_str())
    {
        let ext = ext.trim().to_ascii_lowercase();
        if !ext.is_empty() && ext.len() <= 10 && ext.chars().all(|c| c.is_ascii_alphanumeric()) {
            return Some(ext);
        }
    }
    match content_type.map(|c| c.trim().to_ascii_lowercase()).as_deref() {
        Some("image/png") => Some("png".into()),
        Some("image/jpeg") | Some("image/jpg") => Some("jpg".into()),
        Some("image/gif") => Some("gif".into()),
        Some("image/webp") => Some("webp".into()),
        Some("image/svg+xml") => Some("svg".into()),
        Some("image/bmp") => Some("bmp".into()),
        Some("application/pdf") => Some("pdf".into()),
        Some("text/plain") => Some("txt".into()),
        _ => None,
    }
}

fn sanitize_content_type(raw_ct: Option<&str>) -> String {
    raw_ct
        .map(|ct| ct.trim().to_ascii_lowercase())
        .filter(|ct| {
            !ct.is_empty()
                && ct.len() <= 128
                && ct
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '/' || c == '-' || c == '+' || c == '.')
        })
        .unwrap_or_else(|| "application/octet-stream".to_string())
}

impl TeamSessionService {
    /// Consume one request from this Team's upload burst budget. Call only
    /// after Team/workspace authorization and before reading multipart bytes.
    pub fn check_team_upload_rate_limit(&self, team_id: &str) -> Result<(), TeamError> {
        self.team_upload_rate_limit.check(team_id)
    }

    /// Reserve one bounded upload buffer slot. The owned permit is held by the
    /// route handler through multipart parsing and durable storage, and releases
    /// automatically on success, error, or cancellation.
    pub fn try_acquire_team_upload_slot(&self) -> Result<tokio::sync::OwnedSemaphorePermit, TeamError> {
        Arc::clone(&self.team_upload_in_flight)
            .try_acquire_owned()
            .map_err(|_| TeamError::TeamUploadConcurrencyLimited)
    }

    fn team_upload_storage_root(&self, team_id: &str) -> Result<PathBuf, TeamError> {
        self.team_upload_storage_root
            .read()
            .ok()
            .and_then(|root| root.clone())
            .ok_or_else(|| TeamError::TeamNotFound(team_id.to_owned()))
    }

    fn remove_team_upload_storage(&self, team_id: &str) -> Result<(), TeamError> {
        let storage_root = self
            .team_upload_storage_root
            .read()
            .map_err(|_| TeamError::TeamUploadStorageCleanupFailed)?
            .clone();
        if let Some(storage_root) = storage_root {
            TeamUploadDirectory::remove_team(&storage_root, team_id)?;
        }
        Ok(())
    }

    async fn store_team_upload(
        &self,
        access: &TeamAuthorizationContext,
        storage_root: &Path,
        upload_data: TeamUploadStreamData,
    ) -> Result<TeamUploadResponse, TeamError> {
        if upload_data.file_bytes.len() > TEAM_UPLOAD_MAX_FILE_BYTES {
            return Err(TeamError::TeamUploadFileTooLarge);
        }

        // Upload bytes live under Core-owned data storage, away from the
        // mutable Team workspace. On Unix the descriptor also prevents
        // symlink replacement from redirecting file creation.
        let upload_directory = TeamUploadDirectory::open(storage_root, &access.team.id)?;

        let upload_id = generate_id();
        let extension =
            extract_sanitized_extension(upload_data.file_name.as_deref(), upload_data.content_type.as_deref());
        let content_type = sanitize_content_type(upload_data.content_type.as_deref());

        let file_name_on_disk = match &extension {
            Some(ext) => format!("{upload_id}.{ext}"),
            None => upload_id.clone(),
        };
        let meta_name_on_disk = format!("{upload_id}.meta.json");

        let metadata = TeamUploadMetadata {
            upload_id: upload_id.clone(),
            extension,
            content_type,
            size_bytes: upload_data.file_bytes.len(),
            created_at: now_ms(),
        };
        let meta_bytes = serde_json::to_vec_pretty(&metadata)
            .map_err(|e| TeamError::InvalidRequest(format!("failed to serialize upload metadata: {e}")))?;

        let (used_bytes, used_files) = team_upload_usage(&upload_directory.quota_path(), &access.team.id)?;
        let incoming_bytes = (upload_data.file_bytes.len() as u64).saturating_add(meta_bytes.len() as u64);
        if used_files >= TEAM_UPLOAD_MAX_FILE_COUNT
            || used_bytes.saturating_add(incoming_bytes) > TEAM_UPLOAD_MAX_STORAGE_BYTES
        {
            return Err(TeamError::TeamUploadQuotaExceeded);
        }

        upload_directory.create_new(&file_name_on_disk, &upload_data.file_bytes)?;
        if let Err(error) = upload_directory.create_new(&meta_name_on_disk, &meta_bytes) {
            upload_directory.remove_file(&file_name_on_disk);
            return Err(error);
        }

        Ok(TeamUploadResponse { upload_id })
    }

    pub async fn upload_team_file(
        &self,
        user_id: &str,
        team_id: &str,
        upload_data: TeamUploadStreamData,
    ) -> Result<TeamUploadResponse, TeamError> {
        // Serialize storage with membership revocation, then re-read authorization
        // and the persisted workspace after the request body has been received.
        let _membership_guard = self.team_membership_lock(team_id).lock_owned().await;
        let access = self.authorize_team(user_id, team_id).await?;
        self.verify_and_resolve_team_workspace(&access).await?;
        let storage_root = self.team_upload_storage_root(team_id)?;
        self.store_team_upload(&access, &storage_root, upload_data).await
    }

    pub(crate) async fn verify_and_resolve_team_workspace(
        &self,
        access: &TeamAuthorizationContext,
    ) -> Result<PathBuf, TeamError> {
        let raw_workspace = access.team.workspace.trim();
        if raw_workspace.is_empty() {
            return Err(TeamError::TeamNotFound(access.team.id.clone()));
        }

        if access.sharing_mode == TeamSharingMode::Shared
            && !self
                .conversation_port
                .is_shared_team_workspace(&access.team.id, raw_workspace)
                .await?
        {
            return Err(TeamError::TeamNotFound(access.team.id.clone()));
        }

        let canonical_workspace = Path::new(raw_workspace)
            .canonicalize()
            .map_err(|_| TeamError::TeamNotFound(access.team.id.clone()))?;
        let ws_meta = std::fs::symlink_metadata(&canonical_workspace)
            .map_err(|_| TeamError::TeamNotFound(access.team.id.clone()))?;
        if ws_meta.file_type().is_symlink() || !ws_meta.is_dir() {
            return Err(TeamError::TeamNotFound(access.team.id.clone()));
        }

        if access.sharing_mode == TeamSharingMode::Shared
            && !self
                .conversation_port
                .is_shared_team_workspace(&access.team.id, &canonical_workspace.to_string_lossy())
                .await?
        {
            return Err(TeamError::TeamNotFound(access.team.id.clone()));
        }

        Ok(canonical_workspace)
    }

    async fn resolve_team_upload_file(
        &self,
        access: &TeamAuthorizationContext,
        upload_id: &str,
    ) -> Result<String, TeamError> {
        if upload_id.is_empty()
            || !upload_id
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
        {
            return Err(TeamError::TeamNotFound(access.team.id.clone()));
        }

        self.verify_and_resolve_team_workspace(access).await?;
        let storage_root = self.team_upload_storage_root(&access.team.id)?;
        let upload_directory = TeamUploadDirectory::open(&storage_root, &access.team.id)?;
        let canonical_uploads_dir = &upload_directory.path;

        let meta_path = canonical_uploads_dir.join(format!("{upload_id}.meta.json"));
        let meta_metadata =
            std::fs::symlink_metadata(&meta_path).map_err(|_| TeamError::TeamNotFound(access.team.id.clone()))?;
        if meta_metadata.file_type().is_symlink() || !meta_metadata.is_file() {
            return Err(TeamError::TeamNotFound(access.team.id.clone()));
        }
        let canonical_meta = meta_path
            .canonicalize()
            .map_err(|_| TeamError::TeamNotFound(access.team.id.clone()))?;
        if !canonical_meta.starts_with(canonical_uploads_dir) {
            return Err(TeamError::TeamNotFound(access.team.id.clone()));
        }

        let meta_content =
            std::fs::read_to_string(&canonical_meta).map_err(|_| TeamError::TeamNotFound(access.team.id.clone()))?;
        let upload_meta: TeamUploadMetadata =
            serde_json::from_str(&meta_content).map_err(|_| TeamError::TeamNotFound(access.team.id.clone()))?;

        if upload_meta.upload_id != upload_id {
            return Err(TeamError::TeamNotFound(access.team.id.clone()));
        }

        let file_name_on_disk = match &upload_meta.extension {
            Some(ext) if !ext.is_empty() && ext.len() <= 10 && ext.chars().all(|c| c.is_ascii_alphanumeric()) => {
                format!("{upload_id}.{ext}")
            }
            _ => upload_id.to_string(),
        };

        let file_path = canonical_uploads_dir.join(&file_name_on_disk);
        let file_meta =
            std::fs::symlink_metadata(&file_path).map_err(|_| TeamError::TeamNotFound(access.team.id.clone()))?;
        if file_meta.file_type().is_symlink() || !file_meta.is_file() {
            return Err(TeamError::TeamNotFound(access.team.id.clone()));
        }
        let canonical_file = file_path
            .canonicalize()
            .map_err(|_| TeamError::TeamNotFound(access.team.id.clone()))?;
        if !canonical_file.starts_with(canonical_uploads_dir) {
            return Err(TeamError::TeamNotFound(access.team.id.clone()));
        }

        Ok(canonical_file.to_string_lossy().into_owned())
    }

    async fn resolve_team_message_attachments(
        &self,
        access: &TeamAuthorizationContext,
        is_local_admin: bool,
        content: &str,
        files: Option<Vec<ChatFileRef>>,
    ) -> Result<(String, Option<Vec<String>>), TeamError> {
        let files = match files {
            Some(files) if !files.is_empty() => files,
            _ => return Ok((content.to_owned(), None)),
        };

        if access.role == TeamAccessRole::Collaborator {
            for file in &files {
                match file {
                    ChatFileRef::TeamUpload { .. } => {}
                    _ => {
                        return Err(TeamError::InvalidRequest(
                            "arbitrary file paths are not allowed for team collaborators".into(),
                        ));
                    }
                }
            }
        }

        let mut resolved_paths = Vec::with_capacity(files.len());
        let mut generic_refs = Vec::new();
        let mut generic_indices = Vec::new();

        for (idx, file) in files.into_iter().enumerate() {
            match file {
                ChatFileRef::TeamUpload { upload_id } => {
                    let path = self.resolve_team_upload_file(access, &upload_id).await?;
                    resolved_paths.push((idx, path));
                }
                other => {
                    generic_refs.push(other);
                    generic_indices.push(idx);
                }
            }
        }

        if !generic_refs.is_empty() {
            let project = self
                .project_service
                .read()
                .ok()
                .and_then(|guard| guard.clone())
                .ok_or_else(|| {
                    TeamError::InvalidRequest("project service unavailable; cannot resolve file attachments".into())
                })?;
            let upload_root = std::env::temp_dir().join("aionui");
            for (file, idx) in generic_refs.into_iter().zip(generic_indices) {
                let resolved = project
                    .resolve_chat_file_ref_with_local_admin(
                        &access.execution_owner_id,
                        is_local_admin,
                        &file,
                        &upload_root,
                        aionui_project::FileOp::Read,
                    )
                    .await
                    .map_err(|err| match err {
                        aionui_project::ProjectError::LocalPathForbidden => {
                            TeamError::Forbidden("local file access is not authorized".to_owned())
                        }
                        err => TeamError::InvalidRequest(err.to_string()),
                    })?;
                resolved_paths.push((idx, resolved));
            }
        }

        resolved_paths.sort_by_key(|(idx, _)| *idx);
        let paths: Vec<String> = resolved_paths.into_iter().map(|(_, p)| p).collect();

        let formatted_content = if paths.is_empty() {
            content.to_owned()
        } else {
            format!(
                "{content}\n\n{}\n{}",
                aionui_common::constants::AIONUI_FILES_MARKER,
                paths.join("\n")
            )
        };

        Ok((formatted_content, Some(paths)))
    }

    /// Directed retry/wakeup for a single member runtime (dormant or failed),
    /// reusing the one attach path. Backs the send-box "retry start" entry.
    /// `reserve_attach(slot, true)` retries a `Failed` member; a dormant
    /// (`Absent`) member is attached fresh. Non-blocking: the attach runs in
    /// the background and any preserved unread mailbox rows are re-drained by
    /// the member's event loop via `reconcile_mailbox`.
    pub async fn attach_agent_runtime(&self, user_id: &str, team_id: &str, slot_id: &str) -> Result<(), TeamError> {
        self.load_owned_team(user_id, team_id).await?;
        self.ensure_session_inner(team_id, Some(user_id)).await?;
        let session = {
            let entry = self
                .sessions
                .get(team_id)
                .ok_or_else(|| TeamError::SessionNotFound(team_id.into()))?;
            Arc::clone(&entry.session)
        };
        let agent = session.scheduler().get_agent(slot_id).await?;
        let service = self
            .self_ref
            .upgrade()
            .ok_or_else(|| TeamError::InvalidRequest("team service is shutting down".to_owned()))?;
        let reservation = session.member_runtimes().reserve_attach(slot_id, true);
        self.broadcast_agent_runtime_status(user_id, team_id, &agent, TeamAgentRuntimeStatus::Pending, None);
        spawn_attach_agent_process_bg(
            service,
            Arc::clone(&session),
            user_id.to_owned(),
            agent,
            self.task_manager.clone(),
            reservation,
            // Directed user retry: failures surface inline, do not wake the leader.
            false,
        );
        Ok(())
    }

    /// Force-rebuild one team member runtime while preserving its conversation
    /// and resume anchor. This waits for the shared attach path to reach a
    /// terminal outcome so callers only receive success once the runtime is
    /// ready.
    pub async fn restart_agent_runtime(&self, user_id: &str, team_id: &str, slot_id: &str) -> Result<(), TeamError> {
        self.restart_agent_runtime_inner(user_id, team_id, slot_id, false).await
    }

    pub(crate) async fn restart_agent_runtime_for_mcp_refresh(
        &self,
        user_id: &str,
        team_id: &str,
        slot_id: &str,
    ) -> Result<(), TeamError> {
        self.restart_agent_runtime_inner(user_id, team_id, slot_id, true).await
    }

    async fn restart_agent_runtime_inner(
        &self,
        user_id: &str,
        team_id: &str,
        slot_id: &str,
        allow_queued: bool,
    ) -> Result<(), TeamError> {
        let team = self.load_owned_team(user_id, team_id).await?;
        let requested_agent = team
            .agents
            .iter()
            .find(|agent| agent.slot_id == slot_id)
            .ok_or_else(|| TeamError::AgentNotFound(slot_id.to_owned()))?;
        if allow_queued {
            self.ensure_session_inner(team_id, Some(user_id)).await?;
        }
        let session = {
            let entry = self.sessions.get(team_id).ok_or_else(|| TeamError::RuntimeNotReady {
                conversation_id: requested_agent.conversation_id.clone(),
            })?;
            Arc::clone(&entry.session)
        };
        let agent = session.scheduler().get_agent(slot_id).await?;
        let service = self
            .self_ref
            .upgrade()
            .ok_or_else(|| TeamError::InvalidRequest("team service is shutting down".to_owned()))?;
        let busy_error = || TeamError::MemberBusy {
            team_id: team_id.to_owned(),
            slot_id: slot_id.to_owned(),
            conversation_id: agent.conversation_id.clone(),
        };
        if !allow_queued {
            match session.member_runtimes().snapshot(slot_id) {
                MemberRuntimeSnapshot::Ready => {}
                MemberRuntimeSnapshot::Attaching { .. } => {
                    return Err(TeamError::MemberRuntimeStarting {
                        team_id: team_id.to_owned(),
                        slot_id: slot_id.to_owned(),
                        conversation_id: agent.conversation_id.clone(),
                    });
                }
                MemberRuntimeSnapshot::Removing { .. } => {
                    return Err(TeamError::InvalidRequest(format!(
                        "team member runtime is being removed: {slot_id}"
                    )));
                }
                MemberRuntimeSnapshot::Absent
                | MemberRuntimeSnapshot::Failed { .. }
                | MemberRuntimeSnapshot::SessionStopped => {
                    return Err(TeamError::RuntimeNotReady {
                        conversation_id: agent.conversation_id.clone(),
                    });
                }
            }
        }
        let restart_gate = if allow_queued {
            session.work_coordinator().begin_mcp_runtime_restart(slot_id)
        } else {
            session.work_coordinator().begin_runtime_restart(slot_id)
        }
        .map_err(|rejection| match rejection {
            RuntimeRestartRejection::Busy => busy_error(),
            RuntimeRestartRejection::Removing => {
                TeamError::InvalidRequest(format!("team member runtime is being removed: {slot_id}"))
            }
            RuntimeRestartRejection::SessionStopped => TeamError::SessionNotFound(team_id.to_owned()),
        })?;

        let lease = match session.member_runtimes().reserve_restart(slot_id) {
            ReserveAttach::Start(lease) => lease,
            ReserveAttach::Join(_) | ReserveAttach::AlreadyReady => {
                session.work_coordinator().abort_runtime_restart(slot_id, &restart_gate);
                return Err(busy_error());
            }
            ReserveAttach::Removing(_) => {
                session.work_coordinator().abort_runtime_restart(slot_id, &restart_gate);
                return Err(TeamError::InvalidRequest(format!(
                    "team member runtime is being removed: {slot_id}"
                )));
            }
            ReserveAttach::SessionStopped => {
                session.work_coordinator().abort_runtime_restart(slot_id, &restart_gate);
                return Err(TeamError::SessionNotFound(team_id.to_owned()));
            }
        };

        self.broadcast_agent_runtime_status(user_id, team_id, &agent, TeamAgentRuntimeStatus::Pending, None);
        info!(
            team_id,
            slot_id,
            conversation_id = agent.conversation_id,
            "team member runtime restart requested"
        );
        match attach_member_runtime(
            service,
            Arc::clone(&session),
            user_id.to_owned(),
            agent.clone(),
            self.task_manager.clone(),
            lease,
            false,
        )
        .await
        {
            AttachOutcome::Ready => Ok(()),
            AttachOutcome::Failed(failure) => Err(TeamError::MemberRuntimeFailed {
                team_id: team_id.to_owned(),
                slot_id: slot_id.to_owned(),
                conversation_id: agent.conversation_id,
                public_reason: failure.public_reason,
            }),
            AttachOutcome::Removed => Err(TeamError::AgentNotFound(slot_id.to_owned())),
            AttachOutcome::SessionStopped => Err(TeamError::SessionNotFound(team_id.to_owned())),
        }
    }

    /// Reset one team member's ACP resume anchor and synchronously rebuild its
    /// runtime. Conversation metadata and visible history are deliberately
    /// retained; only the backend thread identity is cleared.
    pub async fn clear_agent_context(
        &self,
        user_id: &str,
        team_id: &str,
        slot_id: &str,
    ) -> Result<TeamContextResetResponse, TeamError> {
        self.clear_agent_context_inner(user_id, team_id, slot_id).await
    }

    /// MCP calls originate from an already-running team session. Keeping this
    /// variant free of `ensure_session` avoids a recursive async type through
    /// `TeamMcpServer::start` while retaining the same reset orchestration.
    pub(crate) async fn clear_agent_context_in_session(
        &self,
        user_id: &str,
        team_id: &str,
        slot_id: &str,
    ) -> Result<TeamContextResetResponse, TeamError> {
        self.clear_agent_context_inner(user_id, team_id, slot_id).await
    }

    async fn clear_agent_context_inner(
        &self,
        user_id: &str,
        team_id: &str,
        slot_id: &str,
    ) -> Result<TeamContextResetResponse, TeamError> {
        let team = self.load_owned_team(user_id, team_id).await?;
        let agent = team
            .agents
            .iter()
            .find(|agent| agent.slot_id == slot_id)
            .cloned()
            .ok_or_else(|| TeamError::AgentNotFound(slot_id.to_owned()))?;
        let session = self.sessions.get(team_id).map(|entry| Arc::clone(&entry.session));
        let capability = self
            .context_reset_capability_for_session(user_id, &agent, session.as_deref())
            .await?;
        match capability.availability {
            TeamContextResetAvailability::Ready => {}
            TeamContextResetAvailability::LeaderNotTargetable => {
                return Err(TeamError::ContextResetLeaderNotTargetable {
                    team_id: team_id.to_owned(),
                    slot_id: slot_id.to_owned(),
                    conversation_id: agent.conversation_id,
                });
            }
            TeamContextResetAvailability::Unsupported => {
                return Err(TeamError::MemberUnsupported {
                    team_id: team_id.to_owned(),
                    slot_id: slot_id.to_owned(),
                    conversation_id: agent.conversation_id,
                    backend: agent.backend,
                });
            }
            availability => {
                return Err(TeamError::ContextResetUnavailable {
                    team_id: team_id.to_owned(),
                    slot_id: slot_id.to_owned(),
                    conversation_id: agent.conversation_id,
                    availability,
                });
            }
        }
        let session = session.ok_or_else(|| TeamError::ContextResetUnavailable {
            team_id: team_id.to_owned(),
            slot_id: slot_id.to_owned(),
            conversation_id: agent.conversation_id.clone(),
            availability: TeamContextResetAvailability::SessionStopped,
        })?;
        let service = self
            .self_ref
            .upgrade()
            .ok_or_else(|| TeamError::InvalidRequest("team service is shutting down".to_owned()))?;
        let busy_error = || TeamError::MemberBusy {
            team_id: team_id.to_owned(),
            slot_id: slot_id.to_owned(),
            conversation_id: agent.conversation_id.clone(),
        };
        let restart_gate = session
            .work_coordinator()
            .begin_runtime_restart(slot_id)
            .map_err(|rejection| match rejection {
                RuntimeRestartRejection::Busy => busy_error(),
                RuntimeRestartRejection::Removing => TeamError::ContextResetUnavailable {
                    team_id: team_id.to_owned(),
                    slot_id: slot_id.to_owned(),
                    conversation_id: agent.conversation_id.clone(),
                    availability: TeamContextResetAvailability::Removing,
                },
                RuntimeRestartRejection::SessionStopped => TeamError::ContextResetUnavailable {
                    team_id: team_id.to_owned(),
                    slot_id: slot_id.to_owned(),
                    conversation_id: agent.conversation_id.clone(),
                    availability: TeamContextResetAvailability::SessionStopped,
                },
            })?;

        let preserved_unread_count = match session.mailbox().peek_unread(team_id, slot_id).await {
            Ok(messages) => messages.len(),
            Err(error) => {
                session.work_coordinator().abort_runtime_restart(slot_id, &restart_gate);
                return Err(error);
            }
        };

        let lease = match session.member_runtimes().reserve_restart(slot_id) {
            ReserveAttach::Start(lease) => lease,
            ReserveAttach::Join(_) | ReserveAttach::AlreadyReady => {
                session.work_coordinator().abort_runtime_restart(slot_id, &restart_gate);
                return Err(busy_error());
            }
            ReserveAttach::Removing(_) => {
                session.work_coordinator().abort_runtime_restart(slot_id, &restart_gate);
                return Err(TeamError::ContextResetUnavailable {
                    team_id: team_id.to_owned(),
                    slot_id: slot_id.to_owned(),
                    conversation_id: agent.conversation_id.clone(),
                    availability: TeamContextResetAvailability::Removing,
                });
            }
            ReserveAttach::SessionStopped => {
                session.work_coordinator().abort_runtime_restart(slot_id, &restart_gate);
                return Err(TeamError::ContextResetUnavailable {
                    team_id: team_id.to_owned(),
                    slot_id: slot_id.to_owned(),
                    conversation_id: agent.conversation_id.clone(),
                    availability: TeamContextResetAvailability::SessionStopped,
                });
            }
        };
        let operation_id = lease.operation_id();

        self.broadcast_agent_runtime_status(user_id, team_id, &agent, TeamAgentRuntimeStatus::Pending, None);
        info!(
            team_id,
            slot_id,
            conversation_id = agent.conversation_id,
            operation_id,
            preserved_unread_count,
            "team member context reset requested"
        );
        self.task_manager
            .kill_and_wait(&agent.conversation_id, Some(AgentKillReason::TeamContextReset))
            .await;
        let cleared = match self
            .conversation_port
            .clear_context_anchor(user_id, &agent.conversation_id)
            .await
        {
            Ok(cleared) => cleared,
            Err(error) => {
                let recovery = attach_member_runtime_after_kill(
                    Arc::clone(&service),
                    Arc::clone(&session),
                    user_id.to_owned(),
                    agent.clone(),
                    self.task_manager.clone(),
                    lease,
                    false,
                )
                .await;
                warn!(
                    team_id,
                    slot_id,
                    conversation_id = agent.conversation_id,
                    operation_id,
                    recovery_ready = matches!(recovery, AttachOutcome::Ready),
                    error = %error,
                    "team member context reset failed before anchor clear"
                );
                return Err(error);
            }
        };
        if !cleared {
            let recovery = attach_member_runtime_after_kill(
                Arc::clone(&service),
                Arc::clone(&session),
                user_id.to_owned(),
                agent.clone(),
                self.task_manager.clone(),
                lease,
                false,
            )
            .await;
            let runtime_status = if matches!(recovery, AttachOutcome::Ready) {
                TeamContextResetRuntimeStatus::Ready
            } else {
                TeamContextResetRuntimeStatus::Failed
            };
            warn!(
                team_id,
                slot_id,
                conversation_id = agent.conversation_id,
                operation_id,
                preserved_unread_count,
                runtime_ready = runtime_status == TeamContextResetRuntimeStatus::Ready,
                "team member context reset was not applied"
            );
            return Ok(TeamContextResetResponse {
                reset_status: TeamContextResetStatus::NotApplied,
                runtime_status,
                preserved_unread_count,
            });
        }

        info!(
            team_id,
            slot_id,
            conversation_id = agent.conversation_id,
            operation_id,
            preserved_unread_count,
            "team member context reset anchor cleared"
        );
        let role_prompt_error = session.scheduler().require_role_prompt(slot_id).await.err();
        if let Some(error) = &role_prompt_error {
            warn!(
                team_id,
                slot_id,
                conversation_id = agent.conversation_id,
                operation_id,
                error = %error,
                "team member context reset could not schedule role prompt reinjection"
            );
        }

        let attach_outcome = attach_member_runtime_after_kill(
            service,
            Arc::clone(&session),
            user_id.to_owned(),
            agent.clone(),
            self.task_manager.clone(),
            lease,
            false,
        )
        .await;
        let runtime_status = if role_prompt_error.is_none() && matches!(attach_outcome, AttachOutcome::Ready) {
            TeamContextResetRuntimeStatus::Ready
        } else {
            TeamContextResetRuntimeStatus::Failed
        };
        if let Err(error) = session.project_context_reset_notice(slot_id, runtime_status).await {
            warn!(
                team_id,
                slot_id,
                conversation_id = agent.conversation_id,
                operation_id,
                error = %error,
                "team member context reset notice projection failed"
            );
        }
        if runtime_status == TeamContextResetRuntimeStatus::Ready {
            info!(
                team_id,
                slot_id,
                conversation_id = agent.conversation_id,
                operation_id,
                preserved_unread_count,
                "team member context reset completed"
            );
        } else {
            warn!(
                team_id,
                slot_id,
                conversation_id = agent.conversation_id,
                operation_id,
                preserved_unread_count,
                "team member context reset completed but runtime attach failed"
            );
        }
        Ok(TeamContextResetResponse {
            reset_status: TeamContextResetStatus::Completed,
            runtime_status,
            preserved_unread_count,
        })
    }

    pub async fn cancel_run(
        &self,
        user_id: &str,
        team_id: &str,
        team_run_id: &str,
        target_slot_id: Option<String>,
        reason: Option<String>,
    ) -> Result<(), TeamError> {
        self.load_owned_team(user_id, team_id).await?;
        self.ensure_session_inner(team_id, Some(user_id)).await?;
        let session = {
            let entry = self
                .sessions
                .get(team_id)
                .ok_or_else(|| TeamError::SessionNotFound(team_id.into()))?;
            Arc::clone(&entry.session)
        };
        session.cancel_run(team_run_id, target_slot_id, reason).await
    }

    pub async fn cancel_child_turn(
        &self,
        user_id: &str,
        team_id: &str,
        team_run_id: &str,
        slot_id: &str,
        reason: Option<String>,
    ) -> Result<(), TeamError> {
        self.load_owned_team(user_id, team_id).await?;
        self.ensure_session_inner(team_id, Some(user_id)).await?;
        let session = {
            let entry = self
                .sessions
                .get(team_id)
                .ok_or_else(|| TeamError::SessionNotFound(team_id.into()))?;
            Arc::clone(&entry.session)
        };
        session.cancel_child_turn(team_run_id, slot_id, reason).await
    }

    pub async fn pause_slot_work(
        &self,
        user_id: &str,
        team_id: &str,
        team_run_id: &str,
        slot_id: &str,
        reason: Option<String>,
    ) -> Result<(), TeamError> {
        self.load_owned_team(user_id, team_id).await?;
        self.ensure_session_inner(team_id, Some(user_id)).await?;
        let session = {
            let entry = self
                .sessions
                .get(team_id)
                .ok_or_else(|| TeamError::SessionNotFound(team_id.into()))?;
            Arc::clone(&entry.session)
        };
        session.pause_slot_work(team_run_id, slot_id, reason).await
    }

    pub async fn set_session_mode(&self, user_id: &str, team_id: &str, mode: &str) -> Result<(), TeamError> {
        let team = self.load_owned_team(user_id, team_id).await?;
        if let Some(starting_member) = team
            .agents
            .iter()
            .find(|agent| self.member_runtime_is_starting(team_id, &agent.slot_id))
        {
            return Err(Self::member_runtime_starting_error(team_id, starting_member));
        }
        let provisioner = self.provisioner();
        self.repo
            .update_team(
                user_id,
                team_id,
                &UpdateTeamParams {
                    session_mode: Some(mode.to_owned()),
                    ..Default::default()
                },
            )
            .await?;

        for agent in &team.agents {
            let mode_applied = match self.task_manager.get_task(&agent.conversation_id) {
                Some(instance) => match set_active_agent_session_mode(&instance, mode).await {
                    Ok(()) => true,
                    Err(e) => {
                        warn!(
                            team_id,
                            slot_id = %agent.slot_id,
                            conversation_id = %agent.conversation_id,
                            error = %e,
                            "failed to set session mode on agent"
                        );
                        false
                    }
                },
                None => true,
            };
            if mode_applied && let Err(e) = provisioner.update_session_mode_seed(agent, mode).await {
                warn!(
                    team_id,
                    slot_id = %agent.slot_id,
                    conversation_id = %agent.conversation_id,
                    error = %e,
                    "failed to persist team session mode seed"
                );
            }
        }

        Ok(())
    }

    pub async fn send_agent_message_from_agent(
        &self,
        team_id: &str,
        from_slot_id: &str,
        to_slot_id: &str,
        content: &str,
        files: Option<Vec<String>>,
    ) -> Result<AgentMessageQueueResult, TeamError> {
        let session = {
            let entry = self
                .sessions
                .get(team_id)
                .ok_or_else(|| TeamError::SessionNotFound(team_id.into()))?;
            Arc::clone(&entry.session)
        };
        session
            .send_agent_message_from_agent(from_slot_id, to_slot_id, content, files)
            .await
    }

    pub async fn interrupt_agent_from_agent(
        &self,
        team_id: &str,
        from_slot_id: &str,
        to_slot_id: &str,
        message: &str,
        files: Option<Vec<String>>,
        reason: Option<String>,
    ) -> Result<TeamInterruptAgentResponse, TeamError> {
        self.published_session(team_id)?
            .interrupt_agent_from_agent(from_slot_id, to_slot_id, message, files, reason)
            .await
    }

    pub async fn shutdown_agent_in_session(
        &self,
        team_id: &str,
        caller_slot_id: &str,
        target_slot_id: &str,
        reason: Option<String>,
    ) -> Result<(), TeamError> {
        let session = {
            let entry = self
                .sessions
                .get(team_id)
                .ok_or_else(|| TeamError::SessionNotFound(team_id.into()))?;
            Arc::clone(&entry.session)
        };
        session.shutdown_agent(caller_slot_id, target_slot_id, reason).await
    }

    pub(crate) async fn wake_leader_after_recovery_message(
        &self,
        team_id: &str,
        source_slot_id: &str,
        source: WorkSource,
    ) -> Result<(), TeamError> {
        let entry = self
            .sessions
            .get(team_id)
            .ok_or_else(|| TeamError::SessionNotFound(team_id.into()))?;
        entry
            .session
            .wake_leader_after_recovery_message(source_slot_id, source)
            .await
    }
}

fn safe_team_conversation_projection(mut conversation: ConversationResponse, team_id: &str) -> ConversationResponse {
    let mut safe_extra = serde_json::Map::new();

    // Keep only the scalar display/runtime metadata consumed by Team UI. Never
    // copy arbitrary nested values out of the persisted `extra` object: it also
    // stores MCP credentials and per-session environment/header configuration.
    for key in [
        "workspace",
        "session_mode",
        "backend",
        "agent_name",
        "current_model_id",
        "current_model_label",
    ] {
        if let Some(value) = conversation.extra.get(key).and_then(serde_json::Value::as_str) {
            safe_extra.insert(key.to_owned(), serde_json::Value::String(value.to_owned()));
        }
    }
    for key in ["skills", "mcp_servers"] {
        if let Some(values) = conversation.extra.get(key).and_then(serde_json::Value::as_array) {
            let safe_values = values
                .iter()
                .filter_map(serde_json::Value::as_str)
                .map(|value| serde_json::Value::String(value.to_owned()))
                .collect();
            safe_extra.insert(key.to_owned(), serde_json::Value::Array(safe_values));
        }
    }
    if let Some(value) = conversation
        .extra
        .get("is_temporary_workspace")
        .and_then(serde_json::Value::as_bool)
    {
        safe_extra.insert("is_temporary_workspace".to_owned(), serde_json::Value::Bool(value));
    }
    if let Some(statuses) = conversation
        .extra
        .get("mcp_statuses")
        .and_then(serde_json::Value::as_array)
    {
        let safe_statuses = statuses
            .iter()
            .filter_map(serde_json::Value::as_object)
            .map(|status| {
                let mut safe_status = serde_json::Map::new();
                for key in ["id", "name", "status"] {
                    if let Some(value) = status.get(key).and_then(serde_json::Value::as_str) {
                        safe_status.insert(key.to_owned(), serde_json::Value::String(value.to_owned()));
                    }
                }
                serde_json::Value::Object(safe_status)
            })
            .collect();
        safe_extra.insert("mcp_statuses".to_owned(), serde_json::Value::Array(safe_statuses));
    }
    safe_extra.insert("team_id".to_owned(), serde_json::Value::String(team_id.to_owned()));
    safe_extra.insert("teamId".to_owned(), serde_json::Value::String(team_id.to_owned()));
    conversation.extra = serde_json::Value::Object(safe_extra);
    conversation.project_id = None;
    conversation.fork_capability = None;
    conversation.prompt_capability = None;
    conversation
}

async fn set_active_agent_session_mode(instance: &AgentInstance, mode: &str) -> Result<(), AgentError> {
    #[allow(unreachable_patterns)]
    match instance {
        AgentInstance::Acp(_) => instance.set_config_option("mode", mode).await.map(|_| ()),
        AgentInstance::Aionrs(manager) => manager.set_mode(mode).await,
        _ => instance.set_config_option("mode", mode).await.map(|_| ()),
    }
}

fn is_idle_collectable_team_member(task: &AgentInstance, now: TimestampMs, idle_threshold_ms: TimestampMs) -> bool {
    if !matches!(
        task.status(),
        None | Some(ConversationStatus::Pending | ConversationStatus::Finished)
    ) {
        return false;
    }
    now.saturating_sub(task.last_activity_at()) > idle_threshold_ms
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use aionui_ai_agent::types::{BuildTaskOptions, SendMessageData};
    use aionui_ai_agent::{
        ActiveLeaseRegistry, AgentError, AgentInstance, AgentSendError, AgentStreamEvent, IAgentTask, IMockAgent,
        IWorkerTaskManager, IdleCleanupCoordinator,
    };
    use aionui_api_types::{
        AddAgentRequest, ConfigOptionConfirmation, SetConfigOptionRequest, SetConfigOptionResponse,
        TeamContextResetAvailability, TeamContextResetRuntimeStatus, TeamContextResetStatus, TeamRunTargetRole,
    };
    use aionui_common::{AgentKillReason, AgentType, ConversationStatus, TimestampMs, now_ms};
    use aionui_db::{IConversationRepository, ITeamRepository};
    use tokio::sync::broadcast;

    use super::TeamIdleCleanupCoordinator;
    use crate::member_runtime::{MemberRuntimeFailure, ReserveAttach};
    use crate::test_utils::workspace_harness::{
        setup_with_factory_metadata_team_repo_and_conversation_repo,
        setup_with_factory_metadata_team_repo_conversation_repo_and_broadcaster,
        setup_with_factory_metadata_team_repo_conversation_repo_broadcaster_and_task_manager,
        single_agent_team_request,
    };
    use crate::types::MailboxMessageType;
    use crate::work_coordinator::{CausalBinding, EnqueueRequest, ReconcileDecision, RuntimeConstraint};
    use crate::work_source::WorkSource;
    use crate::{TeamError, TeamSession};

    struct ModeSettingAgent {
        conversation_id: String,
        agent_type: AgentType,
        mode_result: Mutex<Result<(), String>>,
        event_tx: broadcast::Sender<AgentStreamEvent>,
        status: Option<ConversationStatus>,
        last_activity_at: TimestampMs,
    }

    impl ModeSettingAgent {
        fn accepts_mode(conversation_id: &str) -> Self {
            Self::new(conversation_id, Ok(()))
        }

        fn rejects_mode(conversation_id: &str, message: &str) -> Self {
            Self::new(conversation_id, Err(message.to_owned()))
        }

        fn new(conversation_id: &str, mode_result: Result<(), String>) -> Self {
            let (event_tx, _) = broadcast::channel(1);
            Self {
                conversation_id: conversation_id.to_owned(),
                agent_type: AgentType::Acp,
                mode_result: Mutex::new(mode_result),
                event_tx,
                status: None,
                last_activity_at: now_ms(),
            }
        }

        fn idle_finished(conversation_id: &str) -> Self {
            Self::accepts_mode(conversation_id)
                .with_status(Some(ConversationStatus::Finished))
                .with_last_activity(now_ms() - 600_000)
        }

        fn idle_pending_aionrs(conversation_id: &str) -> Self {
            Self::accepts_mode(conversation_id)
                .with_agent_type(AgentType::Aionrs)
                .with_status(Some(ConversationStatus::Pending))
                .with_last_activity(now_ms() - 600_000)
        }

        fn with_agent_type(mut self, agent_type: AgentType) -> Self {
            self.agent_type = agent_type;
            self
        }

        fn with_status(mut self, status: Option<ConversationStatus>) -> Self {
            self.status = status;
            self
        }

        fn with_last_activity(mut self, last_activity_at: TimestampMs) -> Self {
            self.last_activity_at = last_activity_at;
            self
        }
    }

    #[async_trait::async_trait]
    impl IAgentTask for ModeSettingAgent {
        fn agent_type(&self) -> AgentType {
            self.agent_type
        }

        fn conversation_id(&self) -> &str {
            &self.conversation_id
        }

        fn workspace(&self) -> &str {
            "/tmp/aioncore-team-mode-test"
        }

        fn status(&self) -> Option<ConversationStatus> {
            self.status
        }

        fn last_activity_at(&self) -> TimestampMs {
            self.last_activity_at
        }

        fn subscribe(&self) -> broadcast::Receiver<AgentStreamEvent> {
            self.event_tx.subscribe()
        }

        async fn send_message(&self, _data: SendMessageData) -> Result<(), AgentSendError> {
            Ok(())
        }

        async fn cancel(&self) -> Result<(), AgentError> {
            Ok(())
        }

        fn kill(&self, _reason: Option<AgentKillReason>) -> Result<(), AgentError> {
            Ok(())
        }
    }

    #[async_trait::async_trait]
    impl IMockAgent for ModeSettingAgent {
        async fn set_config_option(&self, option_id: &str, value: &str) -> Result<SetConfigOptionResponse, AgentError> {
            assert_eq!(option_id, "mode");
            assert_eq!(value, "read-only");
            match self.mode_result.lock().unwrap().clone() {
                Ok(()) => Ok(SetConfigOptionResponse {
                    confirmation: ConfigOptionConfirmation::Observed,
                    config_options: None,
                }),
                Err(message) => Err(AgentError::bad_request(message)),
            }
        }
    }

    struct StaticTaskManager {
        tasks: HashMap<String, AgentInstance>,
    }

    impl StaticTaskManager {
        fn new(tasks: HashMap<String, AgentInstance>) -> Self {
            Self { tasks }
        }
    }

    #[async_trait::async_trait]
    impl IWorkerTaskManager for StaticTaskManager {
        fn get_task(&self, conversation_id: &str) -> Option<AgentInstance> {
            self.tasks.get(conversation_id).cloned()
        }

        async fn get_or_build_task(
            &self,
            _conversation_id: &str,
            _options: BuildTaskOptions,
        ) -> Result<AgentInstance, AgentError> {
            Err(AgentError::internal("static task manager does not build tasks"))
        }

        fn kill(&self, _conversation_id: &str, _reason: Option<AgentKillReason>) -> Result<(), AgentError> {
            Ok(())
        }

        fn kill_and_wait(
            &self,
            _conversation_id: &str,
            _reason: Option<AgentKillReason>,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
            Box::pin(std::future::ready(()))
        }

        async fn clear(&self) {}

        fn active_count(&self) -> usize {
            self.tasks.len()
        }

        fn collect_idle(&self, _idle_threshold_ms: TimestampMs) -> Vec<String> {
            Vec::new()
        }
    }

    struct MutableTaskManager {
        tasks: Mutex<HashMap<String, AgentInstance>>,
        kills: Mutex<Vec<String>>,
    }

    impl MutableTaskManager {
        fn new() -> Self {
            Self {
                tasks: Mutex::new(HashMap::new()),
                kills: Mutex::new(Vec::new()),
            }
        }

        fn insert_mode_agent(&self, conversation_id: &str) {
            self.tasks.lock().unwrap().insert(
                conversation_id.to_owned(),
                AgentInstance::Mock(Arc::new(ModeSettingAgent::accepts_mode(conversation_id))),
            );
        }

        fn insert_idle_finished_agent(&self, conversation_id: &str) {
            self.tasks.lock().unwrap().insert(
                conversation_id.to_owned(),
                AgentInstance::Mock(Arc::new(ModeSettingAgent::idle_finished(conversation_id))),
            );
        }

        fn insert_idle_pending_aionrs_agent(&self, conversation_id: &str) {
            self.tasks.lock().unwrap().insert(
                conversation_id.to_owned(),
                AgentInstance::Mock(Arc::new(ModeSettingAgent::idle_pending_aionrs(conversation_id))),
            );
        }

        fn remove(&self, conversation_id: &str) {
            self.tasks.lock().unwrap().remove(conversation_id);
        }

        fn reset_kills(&self) {
            self.kills.lock().unwrap().clear();
        }

        fn kills(&self) -> Vec<String> {
            self.kills.lock().unwrap().clone()
        }
    }

    fn two_agent_team_request(name: &str) -> aionui_api_types::CreateTeamRequest {
        aionui_api_types::CreateTeamRequest {
            sharing_mode: Default::default(),
            name: name.into(),
            agents: vec![
                aionui_api_types::TeamAgentInput {
                    name: "Lead".into(),
                    role: "lead".into(),
                    backend: Some("acp".into()),
                    model: "claude".into(),
                    assistant_id: None,
                    conversation_id: None,
                },
                aionui_api_types::TeamAgentInput {
                    name: "Worker".into(),
                    role: "teammate".into(),
                    backend: Some("acp".into()),
                    model: "claude".into(),
                    assistant_id: None,
                    conversation_id: None,
                },
            ],
            workspace: None,
        }
    }

    fn team_with_aionrs_worker_request(name: &str) -> aionui_api_types::CreateTeamRequest {
        let mut request = two_agent_team_request(name);
        request.agents.push(aionui_api_types::TeamAgentInput {
            name: "Butler".into(),
            role: "teammate".into(),
            backend: Some("aionrs".into()),
            model: "claude-sonnet".into(),
            assistant_id: None,
            conversation_id: None,
        });
        request
    }

    fn mark_member_runtime_ready(session: &TeamSession, slot_id: &str) {
        let lease = match session.member_runtimes().reserve_attach(slot_id, false) {
            ReserveAttach::Start(lease) => lease,
            other => panic!("ready-state seed must start, got {other:?}"),
        };
        assert!(session.member_runtimes().commit_ready(&lease));
        session
            .work_coordinator()
            .set_runtime_constraint(slot_id, RuntimeConstraint::Ready);
    }

    #[async_trait::async_trait]
    impl IWorkerTaskManager for MutableTaskManager {
        fn get_task(&self, conversation_id: &str) -> Option<AgentInstance> {
            self.tasks.lock().unwrap().get(conversation_id).cloned()
        }

        async fn get_or_build_task(
            &self,
            _conversation_id: &str,
            _options: BuildTaskOptions,
        ) -> Result<AgentInstance, AgentError> {
            Err(AgentError::internal("mutable task manager does not build tasks"))
        }

        fn kill(&self, conversation_id: &str, _reason: Option<AgentKillReason>) -> Result<(), AgentError> {
            self.kills.lock().unwrap().push(conversation_id.to_owned());
            self.remove(conversation_id);
            Ok(())
        }

        fn kill_and_wait(
            &self,
            conversation_id: &str,
            reason: Option<AgentKillReason>,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
            let _ = self.kill(conversation_id, reason);
            Box::pin(std::future::ready(()))
        }

        async fn clear(&self) {
            self.tasks.lock().unwrap().clear();
        }

        fn active_count(&self) -> usize {
            self.tasks.lock().unwrap().len()
        }

        fn collect_idle(&self, _idle_threshold_ms: TimestampMs) -> Vec<String> {
            Vec::new()
        }
    }

    #[tokio::test]
    async fn session_has_slow_monitor() {
        let (svc, _repo, _task_manager, _conv_repo) = setup_with_factory_metadata_team_repo_and_conversation_repo();
        let created = svc
            .create_team("user-test", single_agent_team_request("Slow Monitor"))
            .await
            .unwrap();

        svc.ensure_session("user-test", &created.id).await.unwrap();

        assert!(svc.session_has_slow_monitor(&created.id));
        svc.stop_session("user-test", &created.id).await.unwrap();
    }

    #[tokio::test]
    async fn stop_sessions_for_user_keeps_other_user_sessions() {
        let (svc, _repo, _task_manager, _conv_repo) = setup_with_factory_metadata_team_repo_and_conversation_repo();
        let owned = svc
            .create_team("user-test", single_agent_team_request("Owned Session"))
            .await
            .unwrap();
        let other = svc
            .create_team("user-other", single_agent_team_request("Other Session"))
            .await
            .unwrap();

        svc.ensure_session("user-test", &owned.id).await.unwrap();
        svc.ensure_session("user-other", &other.id).await.unwrap();

        assert_eq!(svc.stop_sessions_for_user("user-test"), 1);
        assert_eq!(svc.session_count_for_test(), 1);
        assert!(!svc.session_has_slow_monitor(&owned.id));
        assert!(svc.session_has_slow_monitor(&other.id));

        svc.stop_session("user-other", &other.id).await.unwrap();
    }

    #[tokio::test]
    async fn ensure_session_emits_agent_runtime_ready_after_member_warmup() {
        let (svc, _repo, _task_manager, _conv_repo, broadcaster) =
            setup_with_factory_metadata_team_repo_conversation_repo_and_broadcaster();
        let created = svc
            .create_team("user-test", single_agent_team_request("Runtime Events"))
            .await
            .unwrap();
        let assistant = created.assistants.first().expect("team assistant");

        svc.ensure_session("user-test", &created.id).await.unwrap();

        let events = broadcaster.events_by_name("team.agentRuntimeStatusChanged");
        let statuses: Vec<&str> = events
            .iter()
            .map(|event| event.data.get("status").and_then(serde_json::Value::as_str).unwrap())
            .collect();

        assert_eq!(statuses, vec!["pending", "ready"]);
        assert_eq!(
            events[0].data.get("team_id").and_then(serde_json::Value::as_str),
            Some(created.id.as_str())
        );
        assert_eq!(
            events[0].data.get("slot_id").and_then(serde_json::Value::as_str),
            Some(assistant.slot_id.as_str())
        );
        assert_eq!(
            events[0]
                .data
                .get("conversation_id")
                .and_then(serde_json::Value::as_str),
            Some(assistant.conversation_id.as_str())
        );
    }

    #[tokio::test]
    async fn ensure_session_repairs_only_missing_member_runtime_in_place() {
        let task_manager = Arc::new(MutableTaskManager::new());
        let (svc, _repo, _task_manager, _conv_repo, broadcaster) =
            setup_with_factory_metadata_team_repo_conversation_repo_broadcaster_and_task_manager(task_manager.clone());
        let created = svc
            .create_team("user-test", two_agent_team_request("Runtime Repair"))
            .await
            .unwrap();
        let lead = created.assistants.iter().find(|agent| agent.role == "lead").unwrap();
        let worker = created
            .assistants
            .iter()
            .find(|agent| agent.role == "teammate")
            .unwrap();

        svc.ensure_session("user-test", &created.id).await.unwrap();
        // Leader-only warmup: only the lead runtime exists after first start;
        // the worker stays dormant (spec 5.1), so repair now targets the lead.
        task_manager.insert_mode_agent(&lead.conversation_id);
        task_manager.reset_kills();
        let original_session = Arc::clone(&svc.sessions.get(&created.id).expect("session").session);
        let original_generation = original_session.generation();
        // Simulate the lead runtime disappearing so reconciliation repairs it in place.
        task_manager.remove(&lead.conversation_id);

        svc.ensure_session("user-test", &created.id).await.unwrap();

        let current_session = Arc::clone(&svc.sessions.get(&created.id).expect("session").session);
        assert!(Arc::ptr_eq(&original_session, &current_session));
        assert_eq!(current_session.generation(), original_generation);
        assert_eq!(task_manager.kills(), vec![lead.conversation_id.clone()]);
        assert!(current_session.event_loops().has(&lead.slot_id));
        // The dormant worker is never woken by reconciliation (spec 5.1).
        assert!(!current_session.event_loops().has(&worker.slot_id));

        let events = broadcaster.events_by_name("team.agentRuntimeStatusChanged");
        let lead_statuses: Vec<&str> = events
            .iter()
            .filter(|event| {
                event.data.get("slot_id").and_then(serde_json::Value::as_str) == Some(lead.slot_id.as_str())
            })
            .map(|event| event.data.get("status").and_then(serde_json::Value::as_str).unwrap())
            .collect();
        assert_eq!(lead_statuses, vec!["pending", "ready", "pending", "ready"]);

        let worker_statuses: Vec<&str> = events
            .iter()
            .filter(|event| {
                event.data.get("slot_id").and_then(serde_json::Value::as_str) == Some(worker.slot_id.as_str())
            })
            .map(|event| event.data.get("status").and_then(serde_json::Value::as_str).unwrap())
            .collect();
        assert_eq!(worker_statuses, vec!["dormant"]);
    }

    #[tokio::test]
    async fn restart_agent_runtime_forces_a_ready_member_through_the_attach_chain() {
        let task_manager = Arc::new(MutableTaskManager::new());
        let (svc, _repo, _task_manager, _conv_repo, broadcaster) =
            setup_with_factory_metadata_team_repo_conversation_repo_broadcaster_and_task_manager(task_manager.clone());
        let created = svc
            .create_team("user-test", single_agent_team_request("Runtime Restart Ready"))
            .await
            .unwrap();
        let lead = created.assistants.first().unwrap();
        svc.ensure_session("user-test", &created.id).await.unwrap();
        task_manager.insert_mode_agent(&lead.conversation_id);
        task_manager.reset_kills();

        svc.restart_agent_runtime("user-test", &created.id, &lead.slot_id)
            .await
            .unwrap();

        assert_eq!(task_manager.kills(), vec![lead.conversation_id.clone()]);
        let statuses = broadcaster
            .events_by_name("team.agentRuntimeStatusChanged")
            .into_iter()
            .filter(|event| {
                event.data.get("slot_id").and_then(serde_json::Value::as_str) == Some(lead.slot_id.as_str())
            })
            .filter_map(|event| {
                event
                    .data
                    .get("status")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned)
            })
            .collect::<Vec<_>>();
        assert_eq!(statuses, vec!["pending", "ready", "pending", "ready"]);
    }

    #[tokio::test]
    async fn restart_agent_runtime_rejects_absent_and_failed_members() {
        let task_manager = Arc::new(MutableTaskManager::new());
        let (svc, _repo, _task_manager, _conv_repo, _broadcaster) =
            setup_with_factory_metadata_team_repo_conversation_repo_broadcaster_and_task_manager(task_manager.clone());
        let created = svc
            .create_team("user-test", two_agent_team_request("Runtime Restart Dormant"))
            .await
            .unwrap();
        let worker = created
            .assistants
            .iter()
            .find(|agent| agent.role == "teammate")
            .unwrap();
        svc.ensure_session("user-test", &created.id).await.unwrap();
        task_manager.reset_kills();

        let absent_error = svc
            .restart_agent_runtime("user-test", &created.id, &worker.slot_id)
            .await
            .unwrap_err();
        assert!(matches!(
            absent_error,
            TeamError::RuntimeNotReady { conversation_id }
                if conversation_id == worker.conversation_id
        ));
        assert!(task_manager.kills().is_empty());

        let session = Arc::clone(&svc.sessions.get(&created.id).unwrap().session);
        let failed_lease = match session.member_runtimes().reserve_restart(&worker.slot_id) {
            ReserveAttach::Start(lease) => lease,
            other => panic!("failed state seed must start, got {other:?}"),
        };
        let attaching_error = svc
            .restart_agent_runtime("user-test", &created.id, &worker.slot_id)
            .await
            .unwrap_err();
        assert!(matches!(
            attaching_error,
            TeamError::MemberRuntimeStarting {
                team_id,
                slot_id,
                conversation_id,
            } if team_id == created.id
                && slot_id == worker.slot_id
                && conversation_id == worker.conversation_id
        ));
        assert!(task_manager.kills().is_empty());

        assert!(session.member_runtimes().commit_failed(
            &failed_lease,
            MemberRuntimeFailure {
                classification: "transport",
                public_reason: "Agent runtime failed to start".to_owned(),
            },
        ));
        session.work_coordinator().set_runtime_constraint(
            &worker.slot_id,
            RuntimeConstraint::Failed {
                operation_id: failed_lease.operation_id(),
                classification: "transport",
            },
        );
        task_manager.reset_kills();

        let failed_error = svc
            .restart_agent_runtime("user-test", &created.id, &worker.slot_id)
            .await
            .unwrap_err();
        assert!(matches!(
            failed_error,
            TeamError::RuntimeNotReady { conversation_id }
                if conversation_id == worker.conversation_id
        ));
        assert!(task_manager.kills().is_empty());
    }

    #[tokio::test]
    async fn restart_agent_runtime_does_not_start_an_unpublished_team_session() {
        let task_manager = Arc::new(MutableTaskManager::new());
        let (svc, _repo, _task_manager, _conv_repo, _broadcaster) =
            setup_with_factory_metadata_team_repo_conversation_repo_broadcaster_and_task_manager(task_manager.clone());
        let created = svc
            .create_team("user-test", single_agent_team_request("Runtime Restart Starting"))
            .await
            .unwrap();
        let lead = created.assistants.first().unwrap();

        let error = svc
            .restart_agent_runtime("user-test", &created.id, &lead.slot_id)
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            TeamError::RuntimeNotReady { conversation_id }
                if conversation_id == lead.conversation_id
        ));
        assert!(!svc.sessions.contains_key(&created.id));
        assert!(task_manager.kills().is_empty());
    }

    #[tokio::test]
    async fn restart_agent_runtime_rejects_active_work_without_killing_the_runtime() {
        let task_manager = Arc::new(MutableTaskManager::new());
        let (svc, _repo, _task_manager, _conv_repo, _broadcaster) =
            setup_with_factory_metadata_team_repo_conversation_repo_broadcaster_and_task_manager(task_manager.clone());
        let created = svc
            .create_team("user-test", single_agent_team_request("Runtime Restart Busy"))
            .await
            .unwrap();
        let lead = created.assistants.first().unwrap();
        svc.ensure_session("user-test", &created.id).await.unwrap();
        task_manager.insert_mode_agent(&lead.conversation_id);
        let session = Arc::clone(&svc.sessions.get(&created.id).unwrap().session);
        let lease = session
            .work_coordinator()
            .acquire_enqueue(EnqueueRequest {
                slot_id: lead.slot_id.clone(),
                role: TeamRunTargetRole::Lead,
                source: WorkSource::UserMessage,
                binding: CausalBinding::UserVisible,
            })
            .unwrap();
        session
            .work_coordinator()
            .commit_enqueue(&lease, Some("message-1".to_owned()))
            .unwrap();
        assert!(matches!(
            session.work_coordinator().next(&lead.slot_id),
            ReconcileDecision::Claim(_)
        ));
        task_manager.reset_kills();

        let error = svc
            .restart_agent_runtime("user-test", &created.id, &lead.slot_id)
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            TeamError::MemberBusy {
                team_id,
                slot_id,
                conversation_id,
            } if team_id == created.id
                && slot_id == lead.slot_id
                && conversation_id == lead.conversation_id
        ));
        assert!(task_manager.kills().is_empty());
    }

    #[tokio::test]
    async fn restart_agent_runtime_rejects_a_stopped_member_registry() {
        let task_manager = Arc::new(MutableTaskManager::new());
        let (svc, _repo, _task_manager, _conv_repo, _broadcaster) =
            setup_with_factory_metadata_team_repo_conversation_repo_broadcaster_and_task_manager(task_manager.clone());
        let created = svc
            .create_team("user-test", single_agent_team_request("Runtime Restart Stopped"))
            .await
            .unwrap();
        let lead = created.assistants.first().unwrap();
        svc.ensure_session("user-test", &created.id).await.unwrap();
        task_manager.insert_mode_agent(&lead.conversation_id);
        let session = Arc::clone(&svc.sessions.get(&created.id).unwrap().session);
        assert!(session.member_runtimes().stop());
        task_manager.reset_kills();

        let error = svc
            .restart_agent_runtime("user-test", &created.id, &lead.slot_id)
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            TeamError::RuntimeNotReady { conversation_id }
                if conversation_id == lead.conversation_id
        ));
        assert!(task_manager.kills().is_empty());
    }

    #[tokio::test]
    async fn starting_member_blocks_only_its_config_and_model_updates() {
        let (svc, _repo, _task_manager, _conv_repo) = setup_with_factory_metadata_team_repo_and_conversation_repo();
        let created = svc
            .create_team("user-test", two_agent_team_request("Starting Config Gate"))
            .await
            .unwrap();
        svc.ensure_session("user-test", &created.id).await.unwrap();
        let lead = created.assistants.iter().find(|agent| agent.role == "lead").unwrap();
        let worker = created
            .assistants
            .iter()
            .find(|agent| agent.role == "teammate")
            .unwrap();
        let session = Arc::clone(&svc.sessions.get(&created.id).unwrap().session);
        session
            .work_coordinator()
            .set_runtime_constraint(&lead.slot_id, RuntimeConstraint::Ready);
        session
            .work_coordinator()
            .set_runtime_constraint(&worker.slot_id, RuntimeConstraint::Starting { operation_id: 42 });

        let lead_result = svc
            .set_conversation_config_option(
                "user-test",
                &created.id,
                &lead.conversation_id,
                "model",
                SetConfigOptionRequest {
                    value: "ready-slot-model".to_owned(),
                },
            )
            .await
            .expect("a Ready slot remains configurable");
        assert_eq!(lead_result.confirmation, ConfigOptionConfirmation::Observed);

        let global_mode_error = svc
            .set_conversation_config_option(
                "user-test",
                &created.id,
                &lead.conversation_id,
                "mode",
                SetConfigOptionRequest {
                    value: "full_auto".to_owned(),
                },
            )
            .await
            .expect_err("leader mode must not partially update while another member is Starting");
        assert!(matches!(
            global_mode_error,
            TeamError::MemberRuntimeStarting { ref slot_id, .. } if slot_id == &worker.slot_id
        ));

        let config_error = svc
            .set_conversation_config_option(
                "user-test",
                &created.id,
                &worker.conversation_id,
                "model",
                SetConfigOptionRequest {
                    value: "blocked-model".to_owned(),
                },
            )
            .await
            .expect_err("a Starting slot must reject config changes");
        assert!(matches!(
            config_error,
            TeamError::MemberRuntimeStarting { ref slot_id, .. } if slot_id == &worker.slot_id
        ));

        let model_error = svc
            .update_agent_model("user-test", &created.id, &worker.slot_id, "blocked-model")
            .await
            .expect_err("the model endpoint must share the Starting gate");
        assert!(matches!(
            model_error,
            TeamError::MemberRuntimeStarting { ref slot_id, .. } if slot_id == &worker.slot_id
        ));
    }

    #[tokio::test]
    async fn session_mode_update_is_rejected_before_persistence_when_any_member_is_starting() {
        let (svc, repo, _task_manager, _conv_repo) = setup_with_factory_metadata_team_repo_and_conversation_repo();
        let created = svc
            .create_team("user-test", two_agent_team_request("Starting Mode Gate"))
            .await
            .unwrap();
        svc.ensure_session("user-test", &created.id).await.unwrap();
        let worker = created
            .assistants
            .iter()
            .find(|agent| agent.role == "teammate")
            .unwrap();
        let session = Arc::clone(&svc.sessions.get(&created.id).unwrap().session);
        session
            .work_coordinator()
            .set_runtime_constraint(&worker.slot_id, RuntimeConstraint::Starting { operation_id: 42 });
        let before = repo
            .get_team("user-test", &created.id)
            .await
            .unwrap()
            .expect("team row")
            .session_mode;

        let error = svc
            .set_session_mode("user-test", &created.id, "full_auto")
            .await
            .expect_err("global mode must wait for every member runtime");

        assert!(matches!(
            error,
            TeamError::MemberRuntimeStarting { ref slot_id, .. } if slot_id == &worker.slot_id
        ));
        let after = repo
            .get_team("user-test", &created.id)
            .await
            .unwrap()
            .expect("team row")
            .session_mode;
        assert_eq!(after, before);
    }

    #[tokio::test]
    async fn clear_agent_context_resets_anchor_and_runtime_state_without_membership_noise() {
        let task_manager = Arc::new(MutableTaskManager::new());
        let (svc, _repo, _task_manager, conv_repo, broadcaster) =
            setup_with_factory_metadata_team_repo_conversation_repo_broadcaster_and_task_manager(task_manager.clone());
        let created = svc
            .create_team("user-test", two_agent_team_request("Clear Context"))
            .await
            .unwrap();
        let worker = created
            .assistants
            .iter()
            .find(|agent| agent.role == "teammate")
            .unwrap();
        svc.ensure_session("user-test", &created.id).await.unwrap();
        let session = Arc::clone(&svc.sessions.get(&created.id).unwrap().session);
        mark_member_runtime_ready(&session, &worker.slot_id);
        assert!(session.scheduler().take_needs_role_prompt(&worker.slot_id).await);
        session
            .mailbox()
            .write(
                &created.id,
                &worker.slot_id,
                "lead",
                MailboxMessageType::Message,
                "stale unread",
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            session
                .mailbox()
                .peek_unread(&created.id, &worker.slot_id)
                .await
                .unwrap()
                .len(),
            1
        );
        svc.set_session_mode("user-test", &created.id, "full_auto")
            .await
            .unwrap();
        let original_mode = conv_repo.get_extra(&worker.conversation_id).unwrap()["session_mode"].clone();
        let original_model = worker.model.clone();
        task_manager.insert_mode_agent(&worker.conversation_id);
        task_manager.reset_kills();
        let spawned_before = broadcaster.events_by_name("team.agentSpawned").len();
        let removed_before = broadcaster.events_by_name("team.agentRemoved").len();
        let notices_before = broadcaster.events_by_name("team.teammateMessage").len();

        let outcome = svc
            .clear_agent_context_in_session("user-test", &created.id, &worker.slot_id)
            .await
            .unwrap();

        assert_eq!(outcome.reset_status, TeamContextResetStatus::Completed);
        assert_eq!(outcome.runtime_status, TeamContextResetRuntimeStatus::Ready);
        assert_eq!(outcome.preserved_unread_count, 1);
        assert_eq!(task_manager.kills(), vec![worker.conversation_id.clone()]);
        let extra = conv_repo.get_extra(&worker.conversation_id).unwrap();
        assert_eq!(extra["mock_acp_session_id"], serde_json::Value::Null);
        assert_eq!(extra["session_mode"], original_mode);
        assert_eq!(
            svc.get_team("user-test", &created.id)
                .await
                .unwrap()
                .assistants
                .into_iter()
                .find(|agent| agent.slot_id == worker.slot_id)
                .unwrap()
                .model,
            original_model
        );
        let unread = session
            .mailbox()
            .peek_unread(&created.id, &worker.slot_id)
            .await
            .unwrap();
        assert_eq!(unread.len(), 1);
        assert_eq!(unread[0].content, "stale unread");
        assert!(session.scheduler().take_needs_role_prompt(&worker.slot_id).await);
        assert_eq!(broadcaster.events_by_name("team.agentSpawned").len(), spawned_before);
        assert_eq!(broadcaster.events_by_name("team.agentRemoved").len(), removed_before);
        let notices = broadcaster.events_by_name("team.teammateMessage");
        assert_eq!(notices.len(), notices_before + 1);
        let notice: aionui_api_types::TeamContextResetNotice = serde_json::from_str(
            notices
                .last()
                .and_then(|event| event.data.get("content"))
                .and_then(serde_json::Value::as_str)
                .expect("semantic reset notice"),
        )
        .unwrap();
        assert_eq!(notice.kind, "context_reset");
        assert_eq!(notice.member_name, worker.name);
        assert_eq!(notice.runtime_status, TeamContextResetRuntimeStatus::Ready);
    }

    #[tokio::test]
    async fn clear_agent_context_rejects_queued_work_without_kill_or_anchor_change() {
        let task_manager = Arc::new(MutableTaskManager::new());
        let (svc, _repo, _task_manager, conv_repo, _broadcaster) =
            setup_with_factory_metadata_team_repo_conversation_repo_broadcaster_and_task_manager(task_manager.clone());
        let created = svc
            .create_team("user-test", two_agent_team_request("Clear Busy"))
            .await
            .unwrap();
        let worker = created
            .assistants
            .iter()
            .find(|agent| agent.role == "teammate")
            .unwrap();
        svc.ensure_session("user-test", &created.id).await.unwrap();
        let session = Arc::clone(&svc.sessions.get(&created.id).unwrap().session);
        mark_member_runtime_ready(&session, &worker.slot_id);
        let lease = session
            .work_coordinator()
            .acquire_enqueue(EnqueueRequest {
                slot_id: worker.slot_id.clone(),
                role: TeamRunTargetRole::Teammate,
                source: WorkSource::UserMessage,
                binding: CausalBinding::UserVisible,
            })
            .unwrap();
        session
            .work_coordinator()
            .commit_enqueue(&lease, Some("queued-message".to_owned()))
            .unwrap();
        task_manager.reset_kills();

        let error = svc
            .clear_agent_context_in_session("user-test", &created.id, &worker.slot_id)
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            TeamError::ContextResetUnavailable {
                availability: TeamContextResetAvailability::Busy,
                ..
            }
        ));
        assert!(task_manager.kills().is_empty());
        assert_eq!(
            conv_repo.get_extra(&worker.conversation_id).unwrap()["mock_acp_session_id"],
            "anchor"
        );
    }

    #[tokio::test]
    async fn clear_agent_context_rejects_stopped_session_without_starting_or_mutating_it() {
        let task_manager = Arc::new(MutableTaskManager::new());
        let (svc, _repo, _task_manager, conv_repo, _broadcaster) =
            setup_with_factory_metadata_team_repo_conversation_repo_broadcaster_and_task_manager(task_manager.clone());
        let created = svc
            .create_team("user-test", two_agent_team_request("Clear Stopped"))
            .await
            .unwrap();
        let worker = created
            .assistants
            .iter()
            .find(|agent| agent.role == "teammate")
            .unwrap();

        let error = svc
            .clear_agent_context("user-test", &created.id, &worker.slot_id)
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            TeamError::ContextResetUnavailable {
                availability: TeamContextResetAvailability::SessionStopped,
                ..
            }
        ));
        assert!(!svc.sessions.contains_key(&created.id));
        assert!(task_manager.kills().is_empty());
        assert_eq!(
            conv_repo.get_extra(&worker.conversation_id).unwrap()["mock_acp_session_id"],
            "anchor"
        );
    }

    #[tokio::test]
    async fn clear_agent_context_reports_completed_when_fresh_runtime_attach_fails() {
        let task_manager = Arc::new(MutableTaskManager::new());
        let (svc, _repo, _task_manager, conv_repo, broadcaster) =
            setup_with_factory_metadata_team_repo_conversation_repo_broadcaster_and_task_manager(task_manager.clone());
        let created = svc
            .create_team("user-test", two_agent_team_request("Clear Partial"))
            .await
            .unwrap();
        let worker = created
            .assistants
            .iter()
            .find(|agent| agent.role == "teammate")
            .unwrap();
        svc.ensure_session("user-test", &created.id).await.unwrap();
        let session = Arc::clone(&svc.sessions.get(&created.id).unwrap().session);
        mark_member_runtime_ready(&session, &worker.slot_id);
        task_manager.insert_mode_agent(&worker.conversation_id);
        conv_repo.mark_runtime_attach_failed(&worker.conversation_id);

        let outcome = svc
            .clear_agent_context("user-test", &created.id, &worker.slot_id)
            .await
            .unwrap();

        assert_eq!(outcome.reset_status, TeamContextResetStatus::Completed);
        assert_eq!(outcome.runtime_status, TeamContextResetRuntimeStatus::Failed);
        assert_eq!(
            conv_repo.get_extra(&worker.conversation_id).unwrap()["mock_acp_session_id"],
            serde_json::Value::Null
        );
        let notices = broadcaster.events_by_name("team.teammateMessage");
        let notice: aionui_api_types::TeamContextResetNotice = serde_json::from_str(
            notices
                .last()
                .and_then(|event| event.data.get("content"))
                .and_then(serde_json::Value::as_str)
                .expect("partial-success semantic reset notice"),
        )
        .unwrap();
        assert_eq!(notice.runtime_status, TeamContextResetRuntimeStatus::Failed);
    }

    #[tokio::test]
    async fn clear_agent_context_rejects_wrong_owner_and_aionrs_before_kill() {
        let task_manager = Arc::new(MutableTaskManager::new());
        let (svc, _repo, _task_manager, _conv_repo, _broadcaster) =
            setup_with_factory_metadata_team_repo_conversation_repo_broadcaster_and_task_manager(task_manager.clone());
        let created = svc
            .create_team("user-test", team_with_aionrs_worker_request("Clear Unsupported"))
            .await
            .unwrap();
        let butler = created.assistants.iter().find(|agent| agent.name == "Butler").unwrap();
        let lead = created.assistants.iter().find(|agent| agent.role == "lead").unwrap();
        svc.ensure_session("user-test", &created.id).await.unwrap();
        task_manager.insert_mode_agent(&lead.conversation_id);
        task_manager.reset_kills();

        let ownership_error = svc
            .clear_agent_context("other-user", &created.id, &butler.slot_id)
            .await
            .unwrap_err();
        assert!(matches!(ownership_error, TeamError::TeamNotFound(_)));
        assert!(task_manager.kills().is_empty());

        let refreshed = svc.get_team("user-test", &created.id).await.unwrap();
        let leader_capability = &refreshed
            .assistants
            .iter()
            .find(|agent| agent.slot_id == lead.slot_id)
            .unwrap()
            .context_reset;
        assert!(!leader_capability.supported);
        assert_eq!(
            leader_capability.availability,
            TeamContextResetAvailability::LeaderNotTargetable
        );
        let unsupported_capability = &refreshed
            .assistants
            .iter()
            .find(|agent| agent.slot_id == butler.slot_id)
            .unwrap()
            .context_reset;
        assert!(!unsupported_capability.supported);
        assert_eq!(
            unsupported_capability.availability,
            TeamContextResetAvailability::Unsupported
        );

        let leader_error = svc
            .clear_agent_context("user-test", &created.id, &lead.slot_id)
            .await
            .unwrap_err();
        assert!(matches!(
            leader_error,
            TeamError::ContextResetLeaderNotTargetable { .. }
        ));
        assert!(task_manager.kills().is_empty());

        let unsupported = svc
            .clear_agent_context("user-test", &created.id, &butler.slot_id)
            .await
            .unwrap_err();
        assert!(matches!(unsupported, TeamError::MemberUnsupported { backend, .. } if backend == "aionrs"));
        assert!(task_manager.kills().is_empty());
    }

    #[tokio::test]
    async fn clear_messages_follow_normal_team_send_paths_without_resetting_context() {
        let task_manager = Arc::new(MutableTaskManager::new());
        let (svc, repo, _task_manager, conv_repo, broadcaster) =
            setup_with_factory_metadata_team_repo_conversation_repo_broadcaster_and_task_manager(task_manager.clone());
        let mut request = two_agent_team_request("Clear Slash");
        request.agents[1].name = "My Worker".into();
        let created = svc.create_team("user-test", request).await.unwrap();
        let worker = created
            .assistants
            .iter()
            .find(|agent| agent.role == "teammate")
            .unwrap();
        let lead = created.assistants.iter().find(|agent| agent.role == "lead").unwrap();
        svc.ensure_session("user-test", &created.id).await.unwrap();
        task_manager.insert_mode_agent(&lead.conversation_id);
        task_manager.insert_mode_agent(&worker.conversation_id);
        task_manager.reset_kills();

        let leader_ack = svc
            .send_message("user-test", &created.id, "/clear", None)
            .await
            .unwrap();
        let member_ack = svc
            .send_message_to_agent("user-test", &created.id, &worker.slot_id, "/clear", None)
            .await
            .unwrap();
        let named_ack = svc
            .send_message("user-test", &created.id, " \t/clear\t  My Worker \t", None)
            .await
            .unwrap();

        assert_eq!(leader_ack.run.target_slot_id, lead.slot_id);
        assert_eq!(named_ack.run.target_slot_id, lead.slot_id);
        let worker_messages = repo
            .list_messages_by_ids(std::slice::from_ref(&member_ack.message_id))
            .await
            .unwrap();
        assert!(
            worker_messages
                .iter()
                .any(|message| { message.id == member_ack.message_id && message.content == "/clear" })
        );
        assert_eq!(
            conv_repo.get_extra(&lead.conversation_id).unwrap()["mock_acp_session_id"],
            "anchor"
        );
        assert_eq!(
            conv_repo.get_extra(&worker.conversation_id).unwrap()["mock_acp_session_id"],
            "anchor"
        );
        assert!(broadcaster.events_by_name("team.teammateMessage").is_empty());
    }

    #[tokio::test]
    async fn no_work_reconciliation_rejects_replaced_session() {
        let (svc, _repo, _task_manager, _conv_repo) = setup_with_factory_metadata_team_repo_and_conversation_repo();
        let created = svc
            .create_team("user-test", single_agent_team_request("No work replacement"))
            .await
            .unwrap();
        svc.ensure_session("user-test", &created.id).await.unwrap();
        let old = Arc::clone(&svc.sessions.get(&created.id).unwrap().session);
        svc.stop_session("user-test", &created.id).await.unwrap();
        svc.ensure_session("user-test", &created.id).await.unwrap();

        let result = svc
            .complete_member_runtime_reconciliation(&created.id, "user-test", old, Vec::new())
            .await;
        assert!(matches!(result, Err(crate::TeamError::SessionNotFound(_))));
    }

    #[tokio::test]
    async fn replaced_session_cannot_publish_dynamic_runtime_ready() {
        let (svc, _repo, _task_manager, _conv_repo, broadcaster) =
            setup_with_factory_metadata_team_repo_conversation_repo_and_broadcaster();
        let created = svc
            .create_team("user-test", single_agent_team_request("Runtime ready generation fence"))
            .await
            .unwrap();
        svc.ensure_session("user-test", &created.id).await.unwrap();
        let old = Arc::clone(&svc.sessions.get(&created.id).unwrap().session);
        let agent = old.scheduler().list_agents().await.remove(0);

        svc.stop_session("user-test", &created.id).await.unwrap();
        svc.ensure_session("user-test", &created.id).await.unwrap();
        let ready_before = broadcaster
            .events_by_name("team.agentRuntimeStatusChanged")
            .into_iter()
            .filter(|event| event.data.get("status").and_then(serde_json::Value::as_str) == Some("ready"))
            .count();

        assert!(!svc.publish_member_runtime_ready_if_current(&old, &agent));
        let ready_after = broadcaster
            .events_by_name("team.agentRuntimeStatusChanged")
            .into_iter()
            .filter(|event| event.data.get("status").and_then(serde_json::Value::as_str) == Some("ready"))
            .count();
        assert_eq!(ready_after, ready_before, "old generations must not publish Ready");
    }

    #[tokio::test]
    async fn stale_cleanup_does_not_kill_replacement_runtime() {
        let task_manager = Arc::new(MutableTaskManager::new());
        let (svc, _repo, _task_manager, _conv_repo, _broadcaster) =
            setup_with_factory_metadata_team_repo_conversation_repo_broadcaster_and_task_manager(task_manager.clone());
        let created = svc
            .create_team("user-test", single_agent_team_request("Cleanup fence"))
            .await
            .unwrap();
        svc.ensure_session("user-test", &created.id).await.unwrap();
        let old = Arc::clone(&svc.sessions.get(&created.id).unwrap().session);
        svc.stop_session("user-test", &created.id).await.unwrap();
        svc.ensure_session("user-test", &created.id).await.unwrap();
        let conversation_id = created.assistants[0].conversation_id.clone();
        task_manager.insert_mode_agent(&conversation_id);

        svc.cleanup_stale_member_runtime_task(&old, &conversation_id).await;

        assert!(task_manager.get_task(&conversation_id).is_some());
    }

    #[tokio::test]
    async fn idle_cleanup_stops_team_session_and_kills_all_members_when_team_is_collectable() {
        let task_manager = Arc::new(MutableTaskManager::new());
        let (svc, _repo, _task_manager, _conv_repo, _broadcaster) =
            setup_with_factory_metadata_team_repo_conversation_repo_broadcaster_and_task_manager(task_manager.clone());
        let created = svc
            .create_team("user-test", two_agent_team_request("Idle Cleanup"))
            .await
            .unwrap();
        let lead = created.assistants.iter().find(|agent| agent.role == "lead").unwrap();
        let worker = created
            .assistants
            .iter()
            .find(|agent| agent.role == "teammate")
            .unwrap();
        task_manager.insert_idle_finished_agent(&lead.conversation_id);
        task_manager.insert_idle_finished_agent(&worker.conversation_id);

        svc.ensure_session("user-test", &created.id).await.unwrap();

        let unhandled = svc
            .cleanup_idle_team_runtime_tasks(vec![lead.conversation_id.clone()], &ActiveLeaseRegistry::new(), 300_000)
            .await;

        assert!(unhandled.is_empty());
        assert_eq!(svc.session_count_for_test(), 0);
        assert_eq!(task_manager.active_count(), 0);
    }

    #[tokio::test]
    async fn idle_cleanup_broadcasts_team_session_stopped() {
        let task_manager = Arc::new(MutableTaskManager::new());
        let (svc, _repo, _task_manager, _conv_repo, broadcaster) =
            setup_with_factory_metadata_team_repo_conversation_repo_broadcaster_and_task_manager(task_manager.clone());
        let created = svc
            .create_team("user-test", two_agent_team_request("Idle Cleanup Stopped Broadcast"))
            .await
            .unwrap();
        let lead = created.assistants.iter().find(|agent| agent.role == "lead").unwrap();
        let worker = created
            .assistants
            .iter()
            .find(|agent| agent.role == "teammate")
            .unwrap();
        task_manager.insert_idle_finished_agent(&lead.conversation_id);
        task_manager.insert_idle_finished_agent(&worker.conversation_id);

        svc.ensure_session("user-test", &created.id).await.unwrap();

        let unhandled = svc
            .cleanup_idle_team_runtime_tasks(vec![lead.conversation_id.clone()], &ActiveLeaseRegistry::new(), 300_000)
            .await;

        assert!(unhandled.is_empty());
        assert_eq!(svc.session_count_for_test(), 0);
        assert_eq!(task_manager.active_count(), 0);

        let stopped_events: Vec<_> = broadcaster
            .events_by_name("team.sessionStatusChanged")
            .into_iter()
            .filter(|event| event.data.get("status").and_then(serde_json::Value::as_str) == Some("stopped"))
            .collect();
        assert_eq!(
            stopped_events.len(),
            1,
            "idle cleanup must broadcast exactly one stopped status"
        );
        assert_eq!(
            stopped_events[0]
                .data
                .get("team_id")
                .and_then(serde_json::Value::as_str),
            Some(created.id.as_str())
        );
    }

    #[tokio::test]
    async fn explicit_stop_session_does_not_broadcast_team_session_stopped() {
        let (svc, _repo, _task_manager, _conv_repo, broadcaster) =
            setup_with_factory_metadata_team_repo_conversation_repo_and_broadcaster();
        let created = svc
            .create_team(
                "user-test",
                single_agent_team_request("Explicit Stop No Stopped Broadcast"),
            )
            .await
            .unwrap();
        svc.ensure_session("user-test", &created.id).await.unwrap();

        svc.stop_session("user-test", &created.id).await.unwrap();

        let stopped_count = broadcaster
            .events_by_name("team.sessionStatusChanged")
            .into_iter()
            .filter(|event| event.data.get("status").and_then(serde_json::Value::as_str) == Some("stopped"))
            .count();
        assert_eq!(stopped_count, 0, "explicit stop must not broadcast a stopped status");
    }

    #[test]
    fn idle_collectable_team_member_accepts_idle_pending_aionrs_runtime() {
        let task = AgentInstance::Mock(Arc::new(ModeSettingAgent::idle_pending_aionrs("aionrs-idle")));

        assert!(super::is_idle_collectable_team_member(&task, now_ms(), 300_000));
    }

    #[test]
    fn idle_collectable_team_member_rejects_running_aionrs_runtime() {
        let task = AgentInstance::Mock(Arc::new(
            ModeSettingAgent::accepts_mode("aionrs-running")
                .with_agent_type(AgentType::Aionrs)
                .with_status(Some(ConversationStatus::Running))
                .with_last_activity(now_ms() - 600_000),
        ));

        assert!(!super::is_idle_collectable_team_member(&task, now_ms(), 300_000));
    }

    #[tokio::test]
    async fn idle_cleanup_stops_team_session_when_aionrs_member_is_idle_pending() {
        let task_manager = Arc::new(MutableTaskManager::new());
        let (svc, _repo, _task_manager, _conv_repo, _broadcaster) =
            setup_with_factory_metadata_team_repo_conversation_repo_broadcaster_and_task_manager(task_manager.clone());
        let created = svc
            .create_team("user-test", team_with_aionrs_worker_request("Idle Aionrs Cleanup"))
            .await
            .unwrap();
        let lead = created.assistants.iter().find(|agent| agent.role == "lead").unwrap();
        let acp_worker = created.assistants.iter().find(|agent| agent.name == "Worker").unwrap();
        let aionrs_worker = created.assistants.iter().find(|agent| agent.name == "Butler").unwrap();
        task_manager.insert_idle_finished_agent(&lead.conversation_id);
        task_manager.insert_idle_finished_agent(&acp_worker.conversation_id);
        task_manager.insert_idle_pending_aionrs_agent(&aionrs_worker.conversation_id);

        svc.ensure_session("user-test", &created.id).await.unwrap();

        let unhandled = svc
            .cleanup_idle_team_runtime_tasks(
                vec![lead.conversation_id.clone(), acp_worker.conversation_id.clone()],
                &ActiveLeaseRegistry::new(),
                300_000,
            )
            .await;

        assert!(unhandled.is_empty());
        assert_eq!(svc.session_count_for_test(), 0);
        assert_eq!(task_manager.active_count(), 0);
    }

    #[tokio::test]
    async fn team_idle_cleanup_coordinator_delegates_to_team_service() {
        let task_manager = Arc::new(MutableTaskManager::new());
        let (svc, _repo, _task_manager, _conv_repo, _broadcaster) =
            setup_with_factory_metadata_team_repo_conversation_repo_broadcaster_and_task_manager(task_manager.clone());
        let created = svc
            .create_team("user-test", two_agent_team_request("Idle Coordinator"))
            .await
            .unwrap();
        for agent in &created.assistants {
            task_manager.insert_idle_finished_agent(&agent.conversation_id);
        }

        svc.ensure_session("user-test", &created.id).await.unwrap();
        let coordinator = TeamIdleCleanupCoordinator::new(svc.clone(), Arc::new(ActiveLeaseRegistry::new()));

        let unhandled = coordinator
            .cleanup_idle_conversations(vec![created.assistants[0].conversation_id.clone()], 300_000)
            .await;

        assert!(unhandled.is_empty());
        assert_eq!(svc.session_count_for_test(), 0);
        assert_eq!(task_manager.active_count(), 0);
    }

    #[tokio::test]
    async fn manual_add_agent_in_active_session_emits_runtime_ready_after_background_attach() {
        let (svc, _repo, _task_manager, _conv_repo, broadcaster) =
            setup_with_factory_metadata_team_repo_conversation_repo_and_broadcaster();
        let created = svc
            .create_team("user-test", single_agent_team_request("Manual Runtime Events"))
            .await
            .unwrap();
        svc.ensure_session("user-test", &created.id).await.unwrap();

        let added = svc
            .add_agent(
                "user-test",
                &created.id,
                AddAgentRequest {
                    name: "Worker".to_owned(),
                    role: "teammate".to_owned(),
                    backend: Some("acp".to_owned()),
                    model: "claude".to_owned(),
                    assistant_id: None,
                },
            )
            .await
            .unwrap();

        let events = tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                let events = broadcaster.events_by_name("team.agentRuntimeStatusChanged");
                let added_events: Vec<_> = events
                    .into_iter()
                    .filter(|event| {
                        event.data.get("slot_id").and_then(serde_json::Value::as_str) == Some(added.slot_id.as_str())
                    })
                    .collect();
                if added_events
                    .iter()
                    .any(|event| event.data.get("status").and_then(serde_json::Value::as_str) == Some("ready"))
                {
                    break added_events;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("runtime ready event should be emitted");
        let statuses: Vec<&str> = events
            .iter()
            .map(|event| event.data.get("status").and_then(serde_json::Value::as_str).unwrap())
            .collect();

        assert_eq!(statuses, vec!["pending", "ready"]);
        let session = Arc::clone(&svc.sessions.get(&created.id).expect("session").session);
        assert_eq!(session.event_loops().len(), 2, "new slot must own one event loop");
        assert!(session.event_loops().has(&added.slot_id));
        assert_eq!(
            session.member_runtimes().snapshot(&added.slot_id),
            crate::member_runtime::MemberRuntimeSnapshot::Ready
        );
    }

    #[tokio::test]
    async fn dynamic_member_is_reserved_before_membership_event() {
        let (svc, _repo, _task_manager, _conv_repo, broadcaster) =
            setup_with_factory_metadata_team_repo_conversation_repo_and_broadcaster();
        let created = svc
            .create_team("user-test", single_agent_team_request("Reservation ordering"))
            .await
            .unwrap();
        svc.ensure_session("user-test", &created.id).await.unwrap();
        let session = Arc::clone(&svc.sessions.get(&created.id).unwrap().session);
        let observed = Arc::new(std::sync::Mutex::new(None));
        let observed_for_event = Arc::clone(&observed);
        broadcaster.set_observer(Arc::new(move |event| {
            if event.name != crate::events::TEAM_AGENT_SPAWNED_EVENT {
                return;
            }
            let slot_id = event
                .data
                .get("assistant")
                .and_then(|assistant| assistant.get("slot_id"))
                .and_then(serde_json::Value::as_str);
            if let Some(slot_id) = slot_id {
                *observed_for_event.lock().unwrap() = Some(session.member_runtimes().snapshot(slot_id));
            }
        }));

        svc.add_agent(
            "user-test",
            &created.id,
            AddAgentRequest {
                name: "Worker".to_owned(),
                role: "teammate".to_owned(),
                backend: Some("acp".to_owned()),
                model: "claude".to_owned(),
                assistant_id: None,
            },
        )
        .await
        .unwrap();

        assert!(matches!(
            observed.lock().unwrap().as_ref(),
            Some(
                crate::member_runtime::MemberRuntimeSnapshot::Attaching { .. }
                    | crate::member_runtime::MemberRuntimeSnapshot::Ready
            )
        ));
    }

    #[tokio::test]
    async fn set_session_mode_persists_team_mode_and_new_agents_inherit_it() {
        let (svc, repo, _task_manager, conv_repo) = setup_with_factory_metadata_team_repo_and_conversation_repo();
        let created = svc
            .create_team("user-test", single_agent_team_request("Team Mode Seed"))
            .await
            .unwrap();

        svc.set_session_mode("user-test", &created.id, "full_auto")
            .await
            .unwrap();

        let row = repo
            .get_team("user-test", &created.id)
            .await
            .unwrap()
            .expect("team row");
        assert_eq!(row.session_mode.as_deref(), Some("full_auto"));

        let added = svc
            .add_agent(
                "user-test",
                &created.id,
                AddAgentRequest {
                    name: "Worker".to_owned(),
                    role: "teammate".to_owned(),
                    backend: Some("acp".to_owned()),
                    model: "claude".to_owned(),
                    assistant_id: None,
                },
            )
            .await
            .unwrap();
        let extra = conv_repo
            .get_extra(&added.conversation_id)
            .expect("added conversation extra");

        assert_eq!(
            extra.get("session_mode").and_then(serde_json::Value::as_str),
            Some("full_auto")
        );
    }

    #[tokio::test]
    async fn set_session_mode_does_not_persist_agent_seed_when_active_runtime_rejects_mode() {
        let accepting_conversation_id = "conv-accepts";
        let rejecting_conversation_id = "conv-rejects";
        let task_manager = Arc::new(StaticTaskManager::new(HashMap::from([
            (
                accepting_conversation_id.to_owned(),
                AgentInstance::Mock(Arc::new(ModeSettingAgent::accepts_mode(accepting_conversation_id))),
            ),
            (
                rejecting_conversation_id.to_owned(),
                AgentInstance::Mock(Arc::new(ModeSettingAgent::rejects_mode(
                    rejecting_conversation_id,
                    "Value 'read-only' is not selectable for config option 'mode'",
                ))),
            ),
        ])));
        let (svc, repo, _task_manager, conv_repo, _broadcaster) =
            setup_with_factory_metadata_team_repo_conversation_repo_broadcaster_and_task_manager(task_manager);
        let created = svc
            .create_team("user-test", single_agent_team_request("Partial Mode Seed"))
            .await
            .unwrap();
        let mut row = repo
            .get_team("user-test", &created.id)
            .await
            .unwrap()
            .expect("team row");
        row.agents = serde_json::json!([
            {
                "slot_id": "slot-accepts",
                "name": "Codex CLI",
                "role": "lead",
                "conversation_id": accepting_conversation_id,
                "backend": "codex",
                "model": "openai.gpt-5.5",
                "assistant_id": "bare:codex"
            },
            {
                "slot_id": "slot-rejects",
                "name": "Claude Code",
                "role": "teammate",
                "conversation_id": rejecting_conversation_id,
                "backend": "claude",
                "model": "global.anthropic.claude-opus-4-8",
                "assistant_id": "bare:claude"
            }
        ])
        .to_string();
        repo.update_team(
            "user-test",
            &created.id,
            &aionui_db::UpdateTeamParams {
                agents: Some(row.agents),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        conv_repo
            .create(&aionui_db::models::ConversationRow {
                id: accepting_conversation_id.to_owned(),
                user_id: "user-test".to_owned(),
                name: "Codex CLI".to_owned(),
                r#type: AgentType::Acp.serde_name().to_owned(),
                extra: serde_json::json!({
                    "current_mode_id": "default",
                    "session_mode": "default"
                })
                .to_string(),
                model: None,
                status: Some("pending".to_owned()),
                source: None,
                channel_chat_id: None,
                pinned: false,
                pinned_at: None,
                created_at: now_ms(),
                updated_at: now_ms(),
                project_id: None,
                folder_id: None,
                name_source: None,
            })
            .await
            .unwrap();
        conv_repo
            .create(&aionui_db::models::ConversationRow {
                id: rejecting_conversation_id.to_owned(),
                user_id: "user-test".to_owned(),
                name: "Claude Code".to_owned(),
                r#type: AgentType::Acp.serde_name().to_owned(),
                extra: serde_json::json!({
                    "current_mode_id": "default",
                    "session_mode": "default"
                })
                .to_string(),
                model: None,
                status: Some("pending".to_owned()),
                source: None,
                channel_chat_id: None,
                pinned: false,
                pinned_at: None,
                created_at: now_ms(),
                updated_at: now_ms(),
                project_id: None,
                folder_id: None,
                name_source: None,
            })
            .await
            .unwrap();

        svc.set_session_mode("user-test", &created.id, "read-only")
            .await
            .unwrap();

        let team = repo
            .get_team("user-test", &created.id)
            .await
            .unwrap()
            .expect("team row");
        assert_eq!(team.session_mode.as_deref(), Some("read-only"));

        let accepting_extra = conv_repo.get_extra(accepting_conversation_id).unwrap();
        assert_eq!(
            accepting_extra.get("session_mode").and_then(serde_json::Value::as_str),
            Some("read-only")
        );

        let rejecting_extra = conv_repo.get_extra(rejecting_conversation_id).unwrap();
        assert_eq!(
            rejecting_extra.get("session_mode").and_then(serde_json::Value::as_str),
            Some("default")
        );
    }

    #[tokio::test]
    async fn run_state_returns_none_without_session_and_does_not_create_session() {
        let (svc, _repo, _task_manager, _conv_repo) = setup_with_factory_metadata_team_repo_and_conversation_repo();
        let created = svc
            .create_team("user-test", single_agent_team_request("Run State"))
            .await
            .unwrap();
        svc.stop_session("user-test", &created.id).await.unwrap();

        assert_eq!(svc.session_count_for_test(), 0);

        let state = svc.get_run_state("user-test", &created.id).await.unwrap();

        assert!(state.active_run.is_none());
        assert_eq!(svc.session_count_for_test(), 0);
    }

    #[tokio::test]
    async fn config_options_returns_snapshot_without_creating_team_session() {
        let (svc, _repo, _task_manager, _conv_repo) = setup_with_factory_metadata_team_repo_and_conversation_repo();
        let created = svc
            .create_team("user-test", single_agent_team_request("Config Options"))
            .await
            .unwrap();
        let conversation_id = &created.assistants[0].conversation_id;

        assert_eq!(svc.session_count_for_test(), 0);

        let options = svc
            .get_conversation_config_options("user-test", &created.id, conversation_id)
            .await
            .unwrap();

        assert_eq!(options.config_options[0].id, "model");
        assert_eq!(svc.session_count_for_test(), 0);
    }

    #[tokio::test]
    async fn run_state_returns_current_active_payload() {
        let (svc, _repo, _task_manager, _conv_repo) = setup_with_factory_metadata_team_repo_and_conversation_repo();
        let created = svc
            .create_team("user-test", single_agent_team_request("Active Run State"))
            .await
            .unwrap();

        let ack = svc.send_message("user-test", &created.id, "hello", None).await.unwrap();
        let state = svc.get_run_state("user-test", &created.id).await.unwrap();
        let active_run = state.active_run.expect("active run state");

        assert_eq!(active_run.team_id, created.id);
        assert_eq!(active_run.team_run_id, ack.run.team_run_id);
        assert_eq!(active_run.status, ack.run.status);
        assert_eq!(active_run.target_slot_id, ack.run.target_slot_id);
        assert_eq!(active_run.target_role, ack.run.target_role);
        assert_eq!(active_run.queued_intent_count, 1);
        assert_eq!(active_run.slot_work.len(), 1);
        assert_eq!(active_run.slot_work[0].slot_id, ack.run.slot_work[0].slot_id);
    }

    #[tokio::test]
    async fn config_options_return_member_runtime_snapshot() {
        let (svc, _repo, _task_manager, _conv_repo) = setup_with_factory_metadata_team_repo_and_conversation_repo();
        let created = svc
            .create_team("user-test", single_agent_team_request("Team Config"))
            .await
            .unwrap();
        let conversation_id = created.assistants[0].conversation_id.clone();

        let response = svc
            .get_conversation_config_options("user-test", &created.id, &conversation_id)
            .await
            .unwrap();

        let model = response
            .config_options
            .iter()
            .find(|option| option.id == "model")
            .expect("model config option");
        assert_eq!(model.current_value.as_deref(), Some("claude"));
    }

    #[tokio::test]
    async fn config_options_reports_runtime_not_ready_for_member_conversation() {
        let (svc, _repo, _task_manager, conv_repo) = setup_with_factory_metadata_team_repo_and_conversation_repo();
        let created = svc
            .create_team("user-test", single_agent_team_request("Team Config Pending"))
            .await
            .unwrap();
        let conversation_id = created.assistants[0].conversation_id.clone();
        conv_repo.mark_runtime_not_ready(&conversation_id);

        let err = svc
            .get_conversation_config_options("user-test", &created.id, &conversation_id)
            .await
            .expect_err("member runtime readiness should be reported distinctly");

        assert!(matches!(
            err,
            crate::error::TeamError::RuntimeNotReady {
                conversation_id: ref id
            } if id == &conversation_id
        ));
    }

    #[tokio::test]
    async fn config_options_reject_non_member_conversation() {
        let (svc, _repo, _task_manager, _conv_repo) = setup_with_factory_metadata_team_repo_and_conversation_repo();
        let created = svc
            .create_team("user-test", single_agent_team_request("Team Config Reject"))
            .await
            .unwrap();

        let err = svc
            .get_conversation_config_options("user-test", &created.id, "other-conversation")
            .await
            .expect_err("non-member conversation must be rejected");

        assert!(matches!(err, crate::error::TeamError::TeamNotFound(_)));
    }

    #[tokio::test]
    async fn config_options_reject_cross_user_access() {
        let (svc, _repo, _task_manager, _conv_repo) = setup_with_factory_metadata_team_repo_and_conversation_repo();
        let created = svc
            .create_team("user-test", single_agent_team_request("Team Config Owner"))
            .await
            .unwrap();
        let conversation_id = created.assistants[0].conversation_id.clone();

        let err = svc
            .get_conversation_config_options("other-user", &created.id, &conversation_id)
            .await
            .expect_err("team config options must reject cross-user access");

        assert!(matches!(err, crate::error::TeamError::TeamNotFound(_)));
    }

    #[tokio::test]
    async fn team_membership_lock_serializes_rostered_reads_with_revoke() {
        let (service, _repo, _task_manager, _conversation_repo) =
            setup_with_factory_metadata_team_repo_and_conversation_repo();
        let revoke_lock = service.team_membership_lock("team-serial");
        let read_lock = service.team_membership_lock("team-serial");
        assert!(Arc::ptr_eq(&revoke_lock, &read_lock));

        let revoke_guard = revoke_lock.lock_owned().await;
        let (read_acquired_tx, mut read_acquired_rx) = tokio::sync::oneshot::channel();
        let reader = tokio::spawn(async move {
            let _read_guard = read_lock.lock_owned().await;
            read_acquired_tx.send(()).unwrap();
        });

        tokio::task::yield_now().await;
        assert!(
            read_acquired_rx.try_recv().is_err(),
            "read must wait while revoke owns the Team lock"
        );
        drop(revoke_guard);
        read_acquired_rx.await.unwrap();
        reader.await.unwrap();
    }

    #[tokio::test]
    async fn team_membership_lock_pruning_preserves_live_waiters_and_drops_idle_entries() {
        let (service, _repo, _task_manager, _conversation_repo) =
            setup_with_factory_metadata_team_repo_and_conversation_repo();
        let lock = service.team_membership_lock("team-prune");
        let guard = Arc::clone(&lock).lock_owned().await;
        let waiter_lock = service.team_membership_lock("team-prune");
        let (waiter_started_tx, waiter_started_rx) = tokio::sync::oneshot::channel();
        let waiter = tokio::spawn(async move {
            waiter_started_tx.send(()).unwrap();
            let _guard = waiter_lock.lock_owned().await;
        });
        waiter_started_rx.await.unwrap();
        tokio::task::yield_now().await;

        // A queued caller still owns an Arc, so pruning must leave the shared
        // mutex discoverable until every holder has finished.
        service.prune_team_membership_lock("team-prune", &lock);
        let current_lock = service.team_membership_lock("team-prune");
        assert!(Arc::ptr_eq(&lock, &current_lock));
        assert!(service.add_agent_locks.contains_key("team-prune"));

        drop(guard);
        waiter.await.unwrap();
        drop(current_lock);
        service.prune_team_membership_lock("team-prune", &lock);
        assert!(!service.add_agent_locks.contains_key("team-prune"));

        // A later request may create a fresh mutex only after the old mutex
        // has no guard or queued caller.
        let replacement = service.team_membership_lock("team-prune");
        assert!(!Arc::ptr_eq(&lock, &replacement));
    }

    #[test]
    fn collaborator_direct_message_target_is_limited_to_shared_lead() {
        use aionui_db::models::TeamAccessRole;

        assert!(super::can_send_direct_team_message(
            TeamAccessRole::Collaborator,
            Some("lead-slot"),
            "lead-slot"
        ));
        assert!(!super::can_send_direct_team_message(
            TeamAccessRole::Collaborator,
            Some("lead-slot"),
            "worker-slot"
        ));
        assert!(!super::can_send_direct_team_message(
            TeamAccessRole::Collaborator,
            None,
            "worker-slot"
        ));
        assert!(super::can_send_direct_team_message(
            TeamAccessRole::Owner,
            Some("lead-slot"),
            "worker-slot"
        ));
    }
}

#[cfg(test)]
mod eligible_collaborator_reference_tests {
    use super::*;
    use std::sync::Barrier;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn candidate_limit_accepts_two_thousand_and_rejects_overflow_sentinel() {
        assert!(ensure_candidate_count_supported(MAX_ELIGIBLE_TEAM_USERS).is_ok());
        assert!(matches!(
            ensure_candidate_count_supported(MAX_ELIGIBLE_TEAM_USERS + 1),
            Err(TeamError::EligibleCollaboratorCandidateLimitExceeded)
        ));
    }

    fn reference(user: &str, expires_at: TimestampMs) -> EligibleAccountRef {
        EligibleAccountRef {
            user_id: user.into(),
            display_name: user.into(),
            expires_at,
        }
    }

    fn begin_listing(store: &EligibleAccountRefStore, owner: &str, team: &str, now: Instant) -> String {
        store
            .begin_listing(owner, team, now, 100)
            .expect("listing should pass the owner-wide rate gate")
    }

    fn replace_refs(
        store: &EligibleAccountRefStore,
        owner: &str,
        team: &str,
        generation: &str,
        refs: HashMap<String, EligibleAccountRef>,
        now: Instant,
    ) -> Result<(), EligibleAccountRefStoreError> {
        store.replace(owner, team, generation, refs, now, 100)
    }

    #[test]
    fn repeated_refresh_replaces_grants_without_growing_scope_storage() {
        let store = EligibleAccountRefStore::default();
        let start = Instant::now();
        let other_generation = begin_listing(&store, "owner", "other-team", start);
        replace_refs(
            &store,
            "owner",
            "other-team",
            &other_generation,
            HashMap::from([("other-ref".into(), reference("other-user", 10_000))]),
            start,
        )
        .unwrap();

        for refresh in 0..1_000 {
            let now = start + Duration::from_secs(refresh + 1);
            let generation = begin_listing(&store, "owner", "team", now);
            let refs = (0..3)
                .map(|candidate| {
                    let account_ref = format!("refresh-{refresh}-candidate-{candidate}");
                    (account_ref.clone(), reference(&account_ref, 10_000))
                })
                .collect();
            replace_refs(&store, "owner", "team", &generation, refs, now).unwrap();
            assert_eq!(store.len_for_owner("owner"), 4);
        }

        let now = start + Duration::from_secs(1_001);
        assert!(store.take("owner", "team", "refresh-0-candidate-0", now, 100).is_none());
        assert!(store.take("owner", "other-team", "other-ref", now, 100).is_some());
        assert_eq!(store.len_for_owner("owner"), 3);
        assert_eq!(store.len_for_owner("another-owner"), 0);
    }

    #[test]
    fn concurrent_consumers_can_take_a_scoped_reference_only_once() {
        let store = Arc::new(EligibleAccountRefStore::default());
        let account_ref = "single-use-ref".to_owned();
        let start = Instant::now();
        let generation = begin_listing(&store, "owner", "team", start);
        replace_refs(
            &store,
            "owner",
            "team",
            &generation,
            HashMap::from([(account_ref.clone(), reference("user", 10_000))]),
            start,
        )
        .unwrap();

        let workers = 8;
        let barrier = Arc::new(Barrier::new(workers));
        let successes = Arc::new(AtomicUsize::new(0));
        std::thread::scope(|threads| {
            for _ in 0..workers {
                let store = Arc::clone(&store);
                let account_ref = account_ref.clone();
                let barrier = Arc::clone(&barrier);
                let successes = Arc::clone(&successes);
                threads.spawn(move || {
                    barrier.wait();
                    if store
                        .take("owner", "team", &account_ref, start + Duration::from_secs(1), 100)
                        .is_some()
                    {
                        successes.fetch_add(1, Ordering::SeqCst);
                    }
                });
            }
        });
        assert_eq!(successes.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn concurrent_team_refreshes_share_the_owner_reference_budget() {
        let store = Arc::new(EligibleAccountRefStore::default());
        let workers = 16;
        let barrier = Arc::new(Barrier::new(workers));
        let start = Instant::now();
        let generations = (0..workers)
            .map(|worker| {
                begin_listing(
                    &store,
                    "owner",
                    &format!("team-{worker}"),
                    start + Duration::from_secs(worker as u64),
                )
            })
            .collect::<Vec<_>>();
        std::thread::scope(|threads| {
            for worker in 0..workers {
                let store = Arc::clone(&store);
                let barrier = Arc::clone(&barrier);
                let generation = generations[worker].clone();
                threads.spawn(move || {
                    let refs = (0..4)
                        .map(|candidate| {
                            let account_ref = format!("{worker}-{candidate}");
                            (account_ref.clone(), reference(&account_ref, 10_000))
                        })
                        .collect();
                    barrier.wait();
                    replace_refs(
                        &store,
                        "owner",
                        &format!("team-{worker}"),
                        &generation,
                        refs,
                        start + Duration::from_secs(workers as u64),
                    )
                    .unwrap();
                });
            }
        });
        assert_eq!(store.len_for_owner("owner"), workers * 4);
    }

    #[test]
    fn concurrent_cross_team_listings_share_one_owner_rate_gate() {
        let store = Arc::new(EligibleAccountRefStore::default());
        let workers = 16;
        let barrier = Arc::new(Barrier::new(workers));
        let admitted = Arc::new(AtomicUsize::new(0));
        let request_time = Instant::now();
        std::thread::scope(|threads| {
            for _ in 0..workers {
                let store = Arc::clone(&store);
                let barrier = Arc::clone(&barrier);
                let admitted = Arc::clone(&admitted);
                threads.spawn(move || {
                    barrier.wait();
                    if store.begin_listing("owner", "team", request_time, 100).is_some() {
                        admitted.fetch_add(1, Ordering::SeqCst);
                    }
                });
            }
        });
        assert_eq!(admitted.load(Ordering::SeqCst), 1);
        assert!(
            store
                .begin_listing("owner", "another-team", request_time + ELIGIBLE_LIST_RATE_LIMIT, 100)
                .is_some()
        );
        assert!(
            store
                .begin_listing("another-owner", "team", request_time, 100)
                .is_some()
        );
    }

    #[test]
    fn owner_quota_is_aggregate_across_teams_and_does_not_evict_existing_refs() {
        let store = EligibleAccountRefStore::default();
        let initial_refs = (0..MAX_OUTSTANDING_ELIGIBLE_ACCOUNT_REFS_PER_OWNER)
            .map(|index| {
                let account_ref = format!("initial-{index}");
                (account_ref.clone(), reference(&account_ref, 200))
            })
            .collect();
        let start = Instant::now();
        let first_generation = begin_listing(&store, "owner", "team-a", start);
        replace_refs(&store, "owner", "team-a", &first_generation, initial_refs, start).unwrap();
        let second_start = start + ELIGIBLE_LIST_RATE_LIMIT;
        let second_generation = begin_listing(&store, "owner", "team-b", second_start);
        let overflow = HashMap::from([("overflow-ref".into(), reference("overflow-user", 200))]);
        assert!(replace_refs(&store, "owner", "team-b", &second_generation, overflow, second_start,).is_err());
        assert_eq!(
            store.len_for_owner("owner"),
            MAX_OUTSTANDING_ELIGIBLE_ACCOUNT_REFS_PER_OWNER
        );
        let retry_at = second_start + ELIGIBLE_LIST_RATE_LIMIT;
        assert!(store.take("owner", "team-a", "initial-0", retry_at, 100).is_some());
        assert_eq!(
            store.len_for_owner("owner"),
            MAX_OUTSTANDING_ELIGIBLE_ACCOUNT_REFS_PER_OWNER - 1
        );
        let retry_generation = begin_listing(&store, "owner", "team-b", retry_at);
        assert!(
            replace_refs(
                &store,
                "owner",
                "team-b",
                &retry_generation,
                HashMap::from([("overflow-ref".into(), reference("overflow-user", 200))]),
                retry_at,
            )
            .is_ok()
        );
        assert!(store.take("owner", "team-b", "overflow-ref", retry_at, 100).is_some());
    }

    #[test]
    fn candidate_cap_rejects_lists_above_limit_without_truncating() {
        assert!(ensure_candidate_count_supported(MAX_OUTSTANDING_ELIGIBLE_ACCOUNT_REFS_PER_OWNER).is_ok());
        assert!(matches!(
            ensure_candidate_count_supported(MAX_OUTSTANDING_ELIGIBLE_ACCOUNT_REFS_PER_OWNER + 1),
            Err(TeamError::EligibleCollaboratorCandidateLimitExceeded)
        ));
    }

    #[test]
    fn expired_refs_release_quota_without_affecting_other_owners_or_teams() {
        let store = EligibleAccountRefStore::default();
        let start = Instant::now();
        let generation_a = begin_listing(&store, "owner", "team-a", start);
        replace_refs(
            &store,
            "owner",
            "team-a",
            &generation_a,
            HashMap::from([("expires-at-boundary".into(), reference("user-a", 200))]),
            start,
        )
        .unwrap();
        let start_b = start + ELIGIBLE_LIST_RATE_LIMIT;
        let generation_b = begin_listing(&store, "owner", "team-b", start_b);
        replace_refs(
            &store,
            "owner",
            "team-b",
            &generation_b,
            HashMap::from([("other-team-ref".into(), reference("user-b", 300))]),
            start_b,
        )
        .unwrap();
        let other_generation = begin_listing(&store, "other-owner", "team-a", start);
        replace_refs(
            &store,
            "other-owner",
            "team-a",
            &other_generation,
            HashMap::from([("other-owner-ref".into(), reference("user-c", 300))]),
            start,
        )
        .unwrap();

        let at_boundary = start_b + Duration::from_secs(1);
        assert!(
            store
                .take("owner", "team-a", "expires-at-boundary", at_boundary, 200)
                .is_none()
        );
        assert_eq!(store.len_for_owner("owner"), 1);
        assert!(
            store
                .take("owner", "team-b", "other-team-ref", at_boundary, 200)
                .is_some()
        );
        assert!(
            store
                .take("other-owner", "team-a", "other-owner-ref", at_boundary, 200)
                .is_some()
        );
        assert_eq!(store.len_for_owner("other-owner"), 0);
    }

    #[test]
    fn idle_owner_records_are_swept_but_live_refs_and_rate_gates_survive() {
        let store = EligibleAccountRefStore::default();
        let start = Instant::now();
        for index in 0..128 {
            let owner = format!("idle-owner-{index}");
            assert!(store.begin_listing(&owner, "team", start, 100).is_some());
        }
        let live_generation = begin_listing(&store, "live-owner", "team", start);
        replace_refs(
            &store,
            "live-owner",
            "team",
            &live_generation,
            HashMap::from([("live-ref".into(), reference("live-user", 10_000))]),
            start,
        )
        .unwrap();
        assert_eq!(store.owner_count(), 129);

        let sweep_at = start + ELIGIBLE_OWNER_RECORD_IDLE_TTL + Duration::from_secs(1);
        store.sweep_idle_owners_if_due(sweep_at, 100);
        assert_eq!(store.owner_count(), 1);
        assert_eq!(store.len_for_owner("live-owner"), 1);

        assert!(
            store
                .begin_listing("idle-owner-0", "team", sweep_at + Duration::from_secs(1), 100)
                .is_some()
        );
        assert_eq!(store.owner_count(), 2);
    }

    #[test]
    fn slow_older_listing_cannot_replace_a_newer_teams_references() {
        let store = EligibleAccountRefStore::default();
        let start = Instant::now();
        let older = begin_listing(&store, "owner", "team", start);
        let newer_started_at = start + ELIGIBLE_LIST_RATE_LIMIT;
        let newer = begin_listing(&store, "owner", "team", newer_started_at);
        let new_refs = HashMap::from([("new-ref".into(), reference("new-user", 10_000))]);
        replace_refs(
            &store,
            "owner",
            "team",
            &newer,
            new_refs,
            newer_started_at + Duration::from_millis(1),
        )
        .unwrap();

        let old_refs = HashMap::from([("old-ref".into(), reference("old-user", 10_000))]);
        assert!(matches!(
            replace_refs(
                &store,
                "owner",
                "team",
                &older,
                old_refs,
                newer_started_at + Duration::from_millis(2),
            ),
            Err(EligibleAccountRefStoreError::ListingSuperseded)
        ));
        assert_eq!(store.len_for_owner("owner"), 1);
        assert!(
            store
                .take(
                    "owner",
                    "team",
                    "new-ref",
                    newer_started_at + Duration::from_secs(1),
                    100
                )
                .is_some()
        );
    }
}
