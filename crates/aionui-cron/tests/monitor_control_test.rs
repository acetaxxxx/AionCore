//! Public service-boundary integration tests for Ticket 01: Conversation-bound MonitorJob control.
//!
//! Verifies:
//! - Explicit creation binds to originating conversation and user.
//! - Complete target, query, lookback, and schedule scope requirement (incomplete/invalid rejected).
//! - Typed fail-closed schedule validation (Every <=0, invalid Cron expr, invalid TZ).
//! - Schedule supply vs. agent proposal and approval (no premature persistence).
//! - Blank user_id/conversation_id fail-closed scope rejection across all operations.
//! - Lifecycle control semantics: inspect, pause, resume, cancel, list.
//! - Strict user and conversation isolation (no cross-conversation or cross-user exposure).
//! - Conversation termination lifecycle hook.
//! - MonitorRunner port seam contract.

use std::sync::Arc;

use aionui_cron::monitor::{
    CreateMonitorJobOutcome, CreateMonitorJobRequest, CursorItemState, FacebookObservation,
    FacebookProfile, FacebookTarget, IMonitorJobRepository, InMemoryMonitorJobRepository,
    LookbackScope, MonitorControlService, MonitorCursor, MonitorError, MonitorJob,
    MonitorJobStatus, MonitorQuery, MonitorRunOutcome, MonitorRunReport, MonitorRunner,
    MonitorScanResult, MonitorStopReason, ObservationDeltaKind, ProfileAuthState,
    ReportedObservation, TargetFailure, TargetScanResult, propose_default_schedule,
    validate_schedule,
};
use aionui_cron::types::CronSchedule;

struct FakeMonitorRunner;

#[async_trait::async_trait]
impl MonitorRunner for FakeMonitorRunner {
    async fn run_scan(&self, job: &MonitorJob) -> Result<MonitorScanResult, String> {
        if job.status != MonitorJobStatus::Active {
            return Err("Cannot run inactive monitor".into());
        }
        Ok(MonitorScanResult {
            outcome: MonitorRunOutcome::Success,
            observations_count: 5,
            error_message: None,
            observations: Vec::new(),
            target_results: Vec::new(),
        })
    }
}

struct FailingMonitorRunner;

#[async_trait::async_trait]
impl MonitorRunner for FailingMonitorRunner {
    async fn run_scan(&self, _job: &MonitorJob) -> Result<MonitorScanResult, String> {
        Err("sidecar unavailable".into())
    }
}

struct FailingCompletionRepository {
    inner: InMemoryMonitorJobRepository,
}

#[async_trait::async_trait]
impl IMonitorJobRepository for FailingCompletionRepository {
    async fn save(&self, job: &MonitorJob) -> Result<(), MonitorError> {
        self.inner.save(job).await
    }

    async fn get(&self, user_id: &str, conversation_id: &str, job_id: &str) -> Result<Option<MonitorJob>, MonitorError> {
        self.inner.get(user_id, conversation_id, job_id).await
    }

    async fn list_by_conversation(&self, user_id: &str, conversation_id: &str) -> Result<Vec<MonitorJob>, MonitorError> {
        self.inner.list_by_conversation(user_id, conversation_id).await
    }

    async fn delete(&self, user_id: &str, conversation_id: &str, job_id: &str) -> Result<bool, MonitorError> {
        self.inner.delete(user_id, conversation_id, job_id).await
    }

    async fn get_run_report(&self, user_id: &str, conversation_id: &str, job_id: &str, scheduled_at_ms: u64) -> Result<Option<MonitorRunReport>, MonitorError> {
        self.inner.get_run_report(user_id, conversation_id, job_id, scheduled_at_ms).await
    }

    async fn save_run_report(&self, report: &MonitorRunReport) -> Result<(), MonitorError> {
        self.inner.save_run_report(report).await
    }

    async fn save_run_completion(
        &self,
        _report: &MonitorRunReport,
        _job: &MonitorJob,
        _cursors: &[MonitorCursor],
    ) -> Result<MonitorRunReport, MonitorError> {
        Err(MonitorError::Repository("transaction unavailable".into()))
    }


    async fn list_run_reports(&self, user_id: &str, conversation_id: &str, job_id: &str) -> Result<Vec<MonitorRunReport>, MonitorError> {
        self.inner.list_run_reports(user_id, conversation_id, job_id).await
    }

    async fn get_cursor(
        &self,
        user_id: &str,
        conversation_id: &str,
        job_id: &str,
        target_id: &str,
        query_revision: u64,
    ) -> Result<Option<MonitorCursor>, MonitorError> {
        self.inner.get_cursor(user_id, conversation_id, job_id, target_id, query_revision).await
    }

    async fn save_cursor(
        &self,
        user_id: &str,
        conversation_id: &str,
        cursor: &MonitorCursor,
    ) -> Result<(), MonitorError> {
        self.inner.save_cursor(user_id, conversation_id, cursor).await
    }

    async fn save_profile(&self, profile: &FacebookProfile) -> Result<(), MonitorError> {
        self.inner.save_profile(profile).await
    }

    async fn get_profile(&self, user_id: &str, profile_id: &str) -> Result<Option<FacebookProfile>, MonitorError> {
        self.inner.get_profile(user_id, profile_id).await
    }

    async fn find_profile_by_id(&self, profile_id: &str) -> Result<Option<FacebookProfile>, MonitorError> {
        self.inner.find_profile_by_id(profile_id).await
    }
}



fn valid_target(id: &str, name: &str) -> FacebookTarget {
    FacebookTarget::new(id).with_display_name(name)
}

fn valid_request(schedule: Option<CronSchedule>) -> CreateMonitorJobRequest {
    CreateMonitorJobRequest {
        targets: vec![
            valid_target("fb_group_react_tw", "React Taiwan"),
            valid_target("fb_group_rust_tw", "Rust Taiwan"),
        ],
        query: MonitorQuery::new("remote hiring").with_revision(1),
        lookback: LookbackScope::from_days(14),
        schedule,
        profile_ref: Some("profile_hank".into()),
    }
}

#[tokio::test]
async fn test_explicit_create_binds_to_originating_conversation_and_user() {
    let svc = MonitorControlService::with_in_memory_repo();
    let supplied_schedule = CronSchedule::Every {
        every_ms: 1800000,
        description: Some("every 30m".into()),
    };

    let req = valid_request(Some(supplied_schedule.clone()));
    let outcome = svc
        .create_job("user_hank", "conv_main_123", req, 1788400000000)
        .await
        .expect("creation should succeed");

    let job = match outcome {
        CreateMonitorJobOutcome::Active { job } => job,
        CreateMonitorJobOutcome::RequiresApproval { .. } => panic!("Expected active job"),
    };

    assert_eq!(job.user_id, "user_hank");
    assert_eq!(job.conversation_id, "conv_main_123");
    assert_eq!(job.status, MonitorJobStatus::Active);
    assert_eq!(job.targets.len(), 2);
    assert_eq!(job.query.query_text, "remote hiring");
    assert_eq!(job.query.revision, 1);
    assert_eq!(job.lookback.duration_ms, 14 * 24 * 3600 * 1000);
    assert_eq!(job.schedule, supplied_schedule);
    assert_eq!(job.profile_ref.as_deref(), Some("profile_hank"));
    assert_eq!(job.created_at_ms, 1788400000000);
    assert_eq!(job.updated_at_ms, 1788400000000);
    assert_eq!(job.next_execution_at_ms, Some(1788400000000));
    assert!(job.stop_reason.is_none());
    assert!(job.last_outcome.is_none());

    // Inspect job from originating conversation
    let inspected = svc
        .get_job("user_hank", "conv_main_123", &job.id)
        .await
        .expect("job should be inspectable");
    assert_eq!(inspected, job);
}

#[tokio::test]
async fn test_incomplete_scope_is_rejected_without_creating_job() {
    let svc = MonitorControlService::with_in_memory_repo();

    // 1. Missing targets
    let mut req = valid_request(None);
    req.targets = vec![];
    let err = svc
        .create_job("user_hank", "conv_1", req, 1000)
        .await
        .unwrap_err();
    assert!(matches!(err, MonitorError::IncompleteScope(_)));

    // 2. Target with whitespace-only id
    let mut req = valid_request(None);
    req.targets = vec![FacebookTarget::new("  \t ")];
    let err = svc
        .create_job("user_hank", "conv_1", req, 1000)
        .await
        .unwrap_err();
    assert!(matches!(err, MonitorError::InvalidTargetScope(_)));

    // 3. Blank query
    let mut req = valid_request(None);
    req.query = MonitorQuery::new("   \n ");
    let err = svc
        .create_job("user_hank", "conv_1", req, 1000)
        .await
        .unwrap_err();
    assert!(matches!(err, MonitorError::InvalidQueryScope(_)));

    // 4. Zero lookback duration
    let mut req = valid_request(None);
    req.lookback = LookbackScope::from_millis(0);
    let err = svc
        .create_job("user_hank", "conv_1", req, 1000)
        .await
        .unwrap_err();
    assert!(matches!(err, MonitorError::InvalidLookbackScope(_)));

    // Verify no jobs exist
    let jobs = svc.list_jobs("user_hank", "conv_1").await.unwrap();
    assert!(jobs.is_empty());
}

#[tokio::test]
async fn test_invalid_schedule_scope_rejected() {
    let svc = MonitorControlService::with_in_memory_repo();

    // 1. Every schedule with 0 or negative interval
    let req = valid_request(Some(CronSchedule::Every {
        every_ms: 0,
        description: None,
    }));
    let err = svc.create_job("user_hank", "conv_1", req, 1000).await.unwrap_err();
    assert!(matches!(err, MonitorError::InvalidScheduleScope(_)));

    let req = valid_request(Some(CronSchedule::Every {
        every_ms: -1000,
        description: None,
    }));
    let err = svc.create_job("user_hank", "conv_1", req, 1000).await.unwrap_err();
    assert!(matches!(err, MonitorError::InvalidScheduleScope(_)));

    // 2. At schedule with invalid timestamp
    let req = valid_request(Some(CronSchedule::At {
        at_ms: -5,
        description: None,
    }));
    let err = svc.create_job("user_hank", "conv_1", req, 1000).await.unwrap_err();
    assert!(matches!(err, MonitorError::InvalidScheduleScope(_)));

    // 3. Invalid cron expression
    let req = valid_request(Some(CronSchedule::Cron {
        expr: "not a valid cron expression".into(),
        tz: None,
        description: None,
    }));
    let err = svc.create_job("user_hank", "conv_1", req, 1000).await.unwrap_err();
    assert!(matches!(err, MonitorError::InvalidScheduleScope(_)));

    // 4. Invalid timezone
    let req = valid_request(Some(CronSchedule::Cron {
        expr: "0 0 9 * * *".into(),
        tz: Some("Invalid/Timezone_Name".into()),
        description: None,
    }));
    let err = svc.create_job("user_hank", "conv_1", req, 1000).await.unwrap_err();
    assert!(matches!(err, MonitorError::InvalidScheduleScope(_)));

    // Direct validate_schedule helper tests
    assert!(validate_schedule(&CronSchedule::Every { every_ms: 60000, description: None }).is_ok());
    assert!(validate_schedule(&CronSchedule::Cron { expr: "0 */5 * * * *".into(), tz: Some("Asia/Taipei".into()), description: None }).is_ok());
    assert!(validate_schedule(&CronSchedule::Every { every_ms: -1, description: None }).is_err());
}

#[tokio::test]
async fn test_blank_user_or_conversation_rejected_fail_closed() {
    let svc = MonitorControlService::with_in_memory_repo();
    let req = valid_request(Some(CronSchedule::Every {
        every_ms: 3600000,
        description: None,
    }));

    // Blank user_id on create
    let err = svc.create_job("   ", "conv_1", req.clone(), 1000).await.unwrap_err();
    assert!(matches!(err, MonitorError::IncompleteScope(_)));

    // Blank conversation_id on create
    let err = svc.create_job("user_1", "  \t ", req, 1000).await.unwrap_err();
    assert!(matches!(err, MonitorError::IncompleteScope(_)));

    // Blank identifiers on get
    let err = svc.get_job("", "conv_1", "job_1").await.unwrap_err();
    assert!(matches!(err, MonitorError::IncompleteScope(_)));

    // Blank identifiers on list
    let err = svc.list_jobs("user_1", "   ").await.unwrap_err();
    assert!(matches!(err, MonitorError::IncompleteScope(_)));

    // Blank identifiers on pause/resume/cancel
    let err = svc.pause_job("", "conv_1", "job_1", MonitorStopReason::ExplicitUserPause, 1000).await.unwrap_err();
    assert!(matches!(err, MonitorError::IncompleteScope(_)));

    let err = svc.resume_job("user_1", "", "job_1", 1000).await.unwrap_err();
    assert!(matches!(err, MonitorError::IncompleteScope(_)));

    let err = svc.cancel_job("   ", "   ", "job_1", MonitorStopReason::ExplicitUserCancellation, 1000).await.unwrap_err();
    assert!(matches!(err, MonitorError::IncompleteScope(_)));

    let err = svc.on_conversation_ended("", "conv_1", MonitorStopReason::ConversationClosed, 1000).await.unwrap_err();
    assert!(matches!(err, MonitorError::IncompleteScope(_)));
}

#[tokio::test]
async fn test_omitted_schedule_proposal_and_approval_flow() {
    let svc = MonitorControlService::with_in_memory_repo();
    let req = valid_request(None); // Omitted schedule

    let outcome = svc
        .create_job("user_hank", "conv_1", req, 1000)
        .await
        .expect("creation should yield proposal");

    let proposal = match outcome {
        CreateMonitorJobOutcome::RequiresApproval { proposal } => {
            assert_eq!(proposal.user_id, "user_hank");
            assert_eq!(proposal.conversation_id, "conv_1");
            assert_eq!(proposal.proposed_schedule, propose_default_schedule());
            proposal
        }
        CreateMonitorJobOutcome::Active { .. } => panic!("Expected proposal flow"),
    };

    // CRITICAL: verify that no MonitorJob was persisted prior to approval
    let existing_jobs = svc.list_jobs("user_hank", "conv_1").await.unwrap();
    assert!(existing_jobs.is_empty(), "No job should be persisted prior to approval");

    // Cross-user approval attempt rejected
    let err = svc
        .approve_proposal("user_other", "conv_1", proposal.clone(), None, 2000)
        .await
        .unwrap_err();
    assert!(matches!(err, MonitorError::AccessDenied(_)));

    // Approve the proposed schedule
    let approved_job = svc
        .approve_proposal("user_hank", "conv_1", proposal, None, 2000)
        .await
        .expect("approval should succeed");

    assert_eq!(approved_job.user_id, "user_hank");
    assert_eq!(approved_job.conversation_id, "conv_1");
    assert_eq!(approved_job.status, MonitorJobStatus::Active);
    assert_eq!(approved_job.schedule, propose_default_schedule());
    assert_eq!(approved_job.created_at_ms, 2000);
    assert_eq!(approved_job.updated_at_ms, 2000);
    assert_eq!(approved_job.next_execution_at_ms, Some(2000));

    // Now exactly 1 job is persisted in the repository
    let existing_jobs = svc.list_jobs("user_hank", "conv_1").await.unwrap();
    assert_eq!(existing_jobs.len(), 1);
    assert_eq!(existing_jobs[0].id, approved_job.id);

    // Runner can now execute the approved job
    let runner = FakeMonitorRunner;
    let run_res = runner.run_scan(&approved_job).await.unwrap();
    assert_eq!(run_res.outcome, MonitorRunOutcome::Success);
}

#[test]
fn test_monitor_payload_serde_uses_existing_cron_schedule_contract() {
    let schedule = CronSchedule::Cron {
        expr: "0 30 9 * * MON-FRI".into(),
        tz: Some("Asia/Taipei".into()),
        description: Some("weekday morning".into()),
    };
    let request = CreateMonitorJobRequest {
        targets: vec![valid_target("group-1", "Group 1")],
        query: MonitorQuery::new("班表"),
        lookback: LookbackScope::from_days(7),
        schedule: Some(schedule.clone()),
        profile_ref: None,
    };

    let encoded = serde_json::to_value(&request).expect("request should serialize");
    assert_eq!(encoded["schedule"]["kind"], "cron");
    assert_eq!(encoded["schedule"]["expr"], "0 30 9 * * MON-FRI");
    let decoded: CreateMonitorJobRequest =
        serde_json::from_value(encoded).expect("request should deserialize");
    assert_eq!(decoded.schedule, Some(schedule));

    let outcome = CreateMonitorJobOutcome::Active {
        job: MonitorJob {
            id: "mon_test".into(),
            user_id: "user_1".into(),
            conversation_id: "conversation_1".into(),
            targets: vec![valid_target("group-1", "Group 1")],
            query: MonitorQuery::new("班表"),
            lookback: LookbackScope::from_days(7),
            schedule: decoded.schedule.expect("schedule should be present"),
            profile_ref: None,
            status: MonitorJobStatus::Active,
            stop_reason: None,
            last_outcome: None,
            created_at_ms: 1,
            updated_at_ms: 1,
            next_execution_at_ms: Some(1),
        },
    };
    let encoded = serde_json::to_value(&outcome).expect("outcome should serialize");
    assert_eq!(encoded["type"], "active");
    assert_eq!(encoded["job"]["schedule"]["kind"], "cron");
    let decoded: CreateMonitorJobOutcome =
        serde_json::from_value(encoded).expect("outcome should deserialize");
    assert_eq!(decoded, outcome);
}

#[tokio::test]
async fn test_lifecycle_pause_resume_cancel_controls() {
    let svc = MonitorControlService::with_in_memory_repo();
    let req = valid_request(Some(CronSchedule::Every {
        every_ms: 3600000,
        description: Some("hourly".into()),
    }));

    let outcome = svc.create_job("user_hank", "conv_1", req, 1000).await.unwrap();
    let job_id = match outcome {
        CreateMonitorJobOutcome::Active { job } => job.id,
        _ => panic!("Expected active job"),
    };

    // 1. Pause on Checkpoint detection
    let paused = svc
        .pause_job(
            "user_hank",
            "conv_1",
            &job_id,
            MonitorStopReason::CheckpointDetected,
            2000,
        )
        .await
        .expect("pause should succeed");
    assert_eq!(paused.status, MonitorJobStatus::Paused);
    assert_eq!(
        paused.stop_reason,
        Some(MonitorStopReason::CheckpointDetected)
    );
    assert!(paused.next_execution_at_ms.is_none());

    // 2. Resume after resolution
    let resumed = svc
        .resume_job("user_hank", "conv_1", &job_id, 3000)
        .await
        .expect("resume should succeed");
    assert_eq!(resumed.status, MonitorJobStatus::Active);
    assert!(resumed.stop_reason.is_none());
    assert_eq!(resumed.next_execution_at_ms, Some(3000));

    // 3. Cancel explicitly
    let cancelled = svc
        .cancel_job(
            "user_hank",
            "conv_1",
            &job_id,
            MonitorStopReason::ExplicitUserCancellation,
            4000,
        )
        .await
        .expect("cancel should succeed");
    assert_eq!(cancelled.status, MonitorJobStatus::Cancelled);
    assert_eq!(
        cancelled.stop_reason,
        Some(MonitorStopReason::ExplicitUserCancellation)
    );
    assert!(cancelled.next_execution_at_ms.is_none());

    // 4. Cannot resume cancelled job
    let err = svc
        .resume_job("user_hank", "conv_1", &job_id, 5000)
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        MonitorError::InvalidLifecycleTransition {
            current: MonitorJobStatus::Cancelled,
            ..
        }
    ));
}

#[tokio::test]
async fn test_scheduled_occurrence_runs_existing_monitor_job_through_runner_seam() {
    let svc = MonitorControlService::with_in_memory_repo();
    let req = valid_request(Some(CronSchedule::Every {
        every_ms: 3600000,
        description: Some("hourly".into()),
    }));
    let outcome = svc.create_job("user_hank", "conv_1", req, 1000).await.unwrap();
    let job_id = match outcome {
        CreateMonitorJobOutcome::Active { job } => job.id,
        _ => panic!("Expected active job"),
    };

    let runner = FakeMonitorRunner;
    let result = svc
        .run_occurrence("user_hank", "conv_1", &job_id, 2000, &runner)
        .await
        .expect("scheduled occurrence should run");

    assert_eq!(result.outcome, MonitorRunOutcome::Success);
    assert_eq!(result.observations_count, 5);

    let repeated = svc
        .run_occurrence("user_hank", "conv_1", &job_id, 2000, &runner)
        .await
        .expect("repeated delivery should be idempotent");
    assert_eq!(repeated, result);
    let reports = svc
        .list_run_reports("user_hank", "conv_1", &job_id)
        .await
        .expect("report should be visible in originating conversation scope");
    assert_eq!(reports.len(), 1);

    let (first, second) = tokio::join!(
        svc.run_occurrence("user_hank", "conv_1", &job_id, 3000, &runner),
        svc.run_occurrence("user_hank", "conv_1", &job_id, 3000, &runner),
    );
    assert_eq!(first.expect("first concurrent delivery should succeed"), second.expect("second concurrent delivery should succeed"));
    assert_eq!(
        svc.list_run_reports("user_hank", "conv_1", &job_id)
            .await
            .unwrap()
            .len(),
        2
    );

    let job = svc.get_job("user_hank", "conv_1", &job_id).await.unwrap();
    assert_eq!(job.updated_at_ms, 3000);
}

#[tokio::test]
async fn test_late_occurrence_does_not_move_job_updated_timestamp_backwards() {
    let svc = MonitorControlService::with_in_memory_repo();
    let outcome = svc
        .create_job(
            "user_hank",
            "conv_1",
            valid_request(Some(CronSchedule::Every {
                every_ms: 3600000,
                description: None,
            })),
            1000,
        )
        .await
        .unwrap();
    let job_id = match outcome {
        CreateMonitorJobOutcome::Active { job } => job.id,
        _ => panic!("Expected active job"),
    };
    let runner = FakeMonitorRunner;
    svc.run_occurrence("user_hank", "conv_1", &job_id, 5000, &runner)
        .await
        .unwrap();
    svc.run_occurrence("user_hank", "conv_1", &job_id, 3000, &runner)
        .await
        .unwrap();

    let job = svc.get_job("user_hank", "conv_1", &job_id).await.unwrap();
    assert_eq!(job.updated_at_ms, 5000);
}

#[tokio::test]
async fn test_occurrence_fail_closed_for_non_active_or_foreign_scope_and_reports_runner_error() {
    let svc = MonitorControlService::with_in_memory_repo();
    let req = valid_request(Some(CronSchedule::Every {
        every_ms: 3600000,
        description: None,
    }));
    let outcome = svc.create_job("user_hank", "conv_1", req, 1000).await.unwrap();
    let job_id = match outcome {
        CreateMonitorJobOutcome::Active { job } => job.id,
        _ => panic!("Expected active job"),
    };

    let error_report = svc
        .run_occurrence("user_hank", "conv_1", &job_id, 3000, &FailingMonitorRunner)
        .await
        .expect("runner failure should be recorded as a bounded failed outcome");
    assert_eq!(error_report.outcome, MonitorRunOutcome::Failed);
    assert_eq!(error_report.observations_count, 0);
    assert_eq!(error_report.error_message.as_deref(), Some("sidecar unavailable"));

    let foreign = svc
        .run_occurrence("other_user", "conv_1", &job_id, 4000, &FakeMonitorRunner)
        .await
        .unwrap_err();
    assert!(matches!(foreign, MonitorError::NotFound(_)));

    let paused = svc
        .pause_job("user_hank", "conv_1", &job_id, MonitorStopReason::AuthExpired, 5000)
        .await
        .unwrap();
    assert_eq!(paused.status, MonitorJobStatus::Paused);
    let paused_err = svc
        .run_occurrence("user_hank", "conv_1", &job_id, 6000, &FakeMonitorRunner)
        .await
        .unwrap_err();
    assert!(matches!(paused_err, MonitorError::InvalidLifecycleTransition { .. }));
}

#[tokio::test]
async fn test_completion_failure_does_not_expose_partial_report_or_job_state() {
    let repo = Arc::new(FailingCompletionRepository {
        inner: InMemoryMonitorJobRepository::new(),
    });
    let svc = MonitorControlService::new(repo);
    let outcome = svc
        .create_job(
            "user_hank",
            "conv_1",
            valid_request(Some(CronSchedule::Every {
                every_ms: 3600000,
                description: None,
            })),
            1000,
        )
        .await
        .unwrap();
    let job_id = match outcome {
        CreateMonitorJobOutcome::Active { job } => job.id,
        _ => panic!("Expected active job"),
    };

    let err = svc
        .run_occurrence("user_hank", "conv_1", &job_id, 2000, &FakeMonitorRunner)
        .await
        .unwrap_err();
    assert!(matches!(err, MonitorError::Repository(_)));
    assert!(svc
        .list_run_reports("user_hank", "conv_1", &job_id)
        .await
        .unwrap()
        .is_empty());
    assert_eq!(svc.get_job("user_hank", "conv_1", &job_id).await.unwrap().last_outcome, None);
}

#[tokio::test]
async fn test_strict_isolation_across_users_and_conversations() {
    let svc = MonitorControlService::with_in_memory_repo();

    let req = valid_request(Some(CronSchedule::Every {
        every_ms: 3600000,
        description: Some("hourly".into()),
    }));
    let outcome = svc
        .create_job("user_alice", "conv_alice_1", req, 1000)
        .await
        .unwrap();
    let job_id = match outcome {
        CreateMonitorJobOutcome::Active { job } => job.id,
        _ => panic!("Expected active job"),
    };

    // User Bob attempts to inspect Alice's job -> NotFound (fail closed)
    let err = svc.get_job("user_bob", "conv_alice_1", &job_id).await.unwrap_err();
    assert!(matches!(err, MonitorError::NotFound(_)));

    // User Bob attempts to pause Alice's job -> NotFound
    let err = svc
        .pause_job("user_bob", "conv_alice_1", &job_id, MonitorStopReason::ExplicitUserPause, 2000)
        .await
        .unwrap_err();
    assert!(matches!(err, MonitorError::NotFound(_)));

    // User Bob lists jobs in Alice's conversation -> empty list
    let bob_list = svc.list_jobs("user_bob", "conv_alice_1").await.unwrap();
    assert!(bob_list.is_empty());

    // Alice in another conversation conv_alice_2 cannot access conv_alice_1's job
    let err = svc.get_job("user_alice", "conv_alice_2", &job_id).await.unwrap_err();
    assert!(matches!(err, MonitorError::NotFound(_)));

    // Alice in conv_alice_1 can access
    let alice_job = svc.get_job("user_alice", "conv_alice_1", &job_id).await.unwrap();
    assert_eq!(alice_job.id, job_id);
}

#[tokio::test]
async fn test_conversation_lifecycle_termination_ends_monitors() {
    let svc = MonitorControlService::with_in_memory_repo();

    // Create 2 jobs in conv_target
    let out1 = svc
        .create_job(
            "user_hank",
            "conv_target",
            valid_request(Some(CronSchedule::Every {
                every_ms: 3600000,
                description: None,
            })),
            1000,
        )
        .await
        .unwrap();
    let job1_id = match out1 {
        CreateMonitorJobOutcome::Active { job } => job.id,
        _ => panic!(),
    };

    let out2 = svc
        .create_job(
            "user_hank",
            "conv_target",
            valid_request(Some(CronSchedule::Every {
                every_ms: 7200000,
                description: None,
            })),
            1000,
        )
        .await
        .unwrap();
    let job2_id = match out2 {
        CreateMonitorJobOutcome::Active { job } => job.id,
        _ => panic!(),
    };

    // Pause job2 on auth expired
    svc.pause_job("user_hank", "conv_target", &job2_id, MonitorStopReason::AuthExpired, 1500).await.unwrap();

    // Create 1 job in conv_other
    let out3 = svc
        .create_job(
            "user_hank",
            "conv_other",
            valid_request(Some(CronSchedule::Every {
                every_ms: 3600000,
                description: None,
            })),
            1000,
        )
        .await
        .unwrap();
    let job3_id = match out3 {
        CreateMonitorJobOutcome::Active { job } => job.id,
        _ => panic!(),
    };

    // End conv_target (e.g. ConversationDeleted)
    let cancelled = svc
        .on_conversation_ended(
            "user_hank",
            "conv_target",
            MonitorStopReason::ConversationDeleted,
            2000,
        )
        .await
        .unwrap();
    assert_eq!(cancelled, 2);

    let j1 = svc.get_job("user_hank", "conv_target", &job1_id).await.unwrap();
    assert_eq!(j1.status, MonitorJobStatus::Cancelled);
    assert_eq!(j1.stop_reason, Some(MonitorStopReason::ConversationDeleted));

    let j2 = svc.get_job("user_hank", "conv_target", &job2_id).await.unwrap();
    assert_eq!(j2.status, MonitorJobStatus::Cancelled);
    assert_eq!(j2.stop_reason, Some(MonitorStopReason::ConversationDeleted));

    // conv_other job is untouched
    let j3 = svc.get_job("user_hank", "conv_other", &job3_id).await.unwrap();
    assert_eq!(j3.status, MonitorJobStatus::Active);
}

// ---------------------------------------------------------------------------
// Ticket 03 Tests: MonitorCursor and single-target delta reporting
// ---------------------------------------------------------------------------

struct ObservationMonitorRunner {
    observations: Vec<FacebookObservation>,
    outcome: MonitorRunOutcome,
}

impl ObservationMonitorRunner {
    fn with_observations(observations: Vec<FacebookObservation>) -> Self {
        Self {
            observations,
            outcome: MonitorRunOutcome::Success,
        }
    }

    fn failing() -> Self {
        Self {
            observations: Vec::new(),
            outcome: MonitorRunOutcome::Failed,
        }
    }
}

#[async_trait::async_trait]
impl MonitorRunner for ObservationMonitorRunner {
    async fn run_scan(&self, job: &MonitorJob) -> Result<MonitorScanResult, String> {
        if job.status != MonitorJobStatus::Active {
            return Err("Cannot run inactive monitor".into());
        }
        if self.outcome != MonitorRunOutcome::Success {
            return Ok(MonitorScanResult::failed("scan failed on platform check"));
        }
        Ok(MonitorScanResult::success(self.observations.clone()))
    }
}

#[tokio::test]
async fn test_first_successful_observation_reported_as_new_and_recorded_in_cursor() {
    let svc = MonitorControlService::with_in_memory_repo();
    let req = valid_request(Some(CronSchedule::Every {
        every_ms: 3600000,
        description: None,
    }));
    let outcome = svc.create_job("user_hank", "conv_1", req, 1000).await.unwrap();
    let job_id = match outcome {
        CreateMonitorJobOutcome::Active { job } => job.id,
        _ => panic!(),
    };

    let obs1 = FacebookObservation::new("post_101", "fb_group_react_tw", "hash_alpha");
    let runner = ObservationMonitorRunner::with_observations(vec![obs1.clone()]);

    let report = svc
        .run_occurrence("user_hank", "conv_1", &job_id, 2000, &runner)
        .await
        .unwrap();

    assert_eq!(report.outcome, MonitorRunOutcome::Success);
    assert_eq!(report.observations_count, 1);
    assert_eq!(report.reported_observations.len(), 1);
    assert_eq!(
        report.reported_observations[0].delta_kind,
        ObservationDeltaKind::New
    );
    assert_eq!(report.reported_observations[0].observation.id, "post_101");

    // Check cursor state
    let cursor = svc
        .get_cursor("user_hank", "conv_1", &job_id, "fb_group_react_tw", 1)
        .await
        .unwrap()
        .expect("cursor should exist after successful run");

    assert_eq!(cursor.last_successful_observed_at_ms, Some(2000));
    assert_eq!(cursor.updated_at_ms, 2000);

    let item = cursor.items.get("post_101").expect("item in cursor");
    assert_eq!(item.observation_id, "post_101");
    assert_eq!(item.target_id, "fb_group_react_tw");
    assert_eq!(item.content_hash, "hash_alpha");
    assert_eq!(item.first_seen_at_ms, 2000);
    assert_eq!(item.last_seen_at_ms, 2000);
    assert_eq!(item.reported_at_ms, Some(2000));
    assert_eq!(item.acknowledged_at_ms, None);
    assert!(item.is_unread_needs_attention());
    assert!(!item.is_acknowledged());
}

#[tokio::test]
async fn test_normalized_content_change_reported_as_changed_and_unchanged_omitted() {
    let svc = MonitorControlService::with_in_memory_repo();
    let req = valid_request(Some(CronSchedule::Every {
        every_ms: 3600000,
        description: None,
    }));
    let outcome = svc.create_job("user_hank", "conv_1", req, 1000).await.unwrap();
    let job_id = match outcome {
        CreateMonitorJobOutcome::Active { job } => job.id,
        _ => panic!(),
    };

    // Run 1: sees post_101 (hash_alpha) and post_102 (hash_beta)
    let obs1 = FacebookObservation::new("post_101", "fb_group_react_tw", "hash_alpha");
    let obs2 = FacebookObservation::new("post_102", "fb_group_react_tw", "hash_beta");
    let runner1 = ObservationMonitorRunner::with_observations(vec![obs1.clone(), obs2.clone()]);
    let report1 = svc.run_occurrence("user_hank", "conv_1", &job_id, 2000, &runner1).await.unwrap();
    assert_eq!(report1.reported_observations.len(), 2);

    // Run 2: post_101 changed to hash_alpha_v2; post_102 unchanged (hash_beta)
    let obs1_changed = FacebookObservation::new("post_101", "fb_group_react_tw", "hash_alpha_v2");
    let runner2 = ObservationMonitorRunner::with_observations(vec![obs1_changed.clone(), obs2.clone()]);
    let report2 = svc.run_occurrence("user_hank", "conv_1", &job_id, 3000, &runner2).await.unwrap();

    // Only post_101 is reported with Changed; post_102 is unchanged and omitted from report
    assert_eq!(report2.reported_observations.len(), 1);
    assert_eq!(report2.reported_observations[0].delta_kind, ObservationDeltaKind::Changed);
    assert_eq!(report2.reported_observations[0].observation.id, "post_101");
    assert_eq!(report2.reported_observations[0].observation.content_hash, "hash_alpha_v2");

    // Run 3: both post_101 (hash_alpha_v2) and post_102 (hash_beta) unchanged
    let runner3 = ObservationMonitorRunner::with_observations(vec![obs1_changed, obs2]);
    let report3 = svc.run_occurrence("user_hank", "conv_1", &job_id, 4000, &runner3).await.unwrap();

    // Both are unchanged; report has 0 reported_observations (not emitted again)
    assert!(report3.reported_observations.is_empty());
}

#[tokio::test]
async fn test_seen_reported_acknowledged_states_and_unacknowledged_needs_attention() {
    let svc = MonitorControlService::with_in_memory_repo();
    let req = valid_request(Some(CronSchedule::Every {
        every_ms: 3600000,
        description: None,
    }));
    let outcome = svc.create_job("user_hank", "conv_1", req, 1000).await.unwrap();
    let job_id = match outcome {
        CreateMonitorJobOutcome::Active { job } => job.id,
        _ => panic!(),
    };

    let obs = FacebookObservation::new("post_201", "fb_group_react_tw", "hash_initial");
    let runner = ObservationMonitorRunner::with_observations(vec![obs.clone()]);
    svc.run_occurrence("user_hank", "conv_1", &job_id, 2000, &runner).await.unwrap();

    // Initial state: seen, reported, but NOT acknowledged -> is_unread_needs_attention = true
    let cursor = svc.get_cursor("user_hank", "conv_1", &job_id, "fb_group_react_tw", 1).await.unwrap().unwrap();
    let item = cursor.items.get("post_201").unwrap();
    assert_eq!(item.reported_at_ms, Some(2000));
    assert_eq!(item.acknowledged_at_ms, None);
    assert!(item.is_unread_needs_attention());
    assert!(!item.is_acknowledged());

    // Subsequent unchanged run: item is still unread/needs_attention without repeated alerts
    let runner2 = ObservationMonitorRunner::with_observations(vec![obs.clone()]);
    let report2 = svc.run_occurrence("user_hank", "conv_1", &job_id, 3000, &runner2).await.unwrap();
    assert!(report2.reported_observations.is_empty(), "Unchanged item is not repeatedly alerted");

    let cursor2 = svc.get_cursor("user_hank", "conv_1", &job_id, "fb_group_react_tw", 1).await.unwrap().unwrap();
    let item2 = cursor2.items.get("post_201").unwrap();
    assert!(item2.is_unread_needs_attention(), "Unacknowledged item retains unread/needs_attention");

    // User explicitly acknowledges the item
    let acked_item = svc
        .acknowledge_observation("user_hank", "conv_1", &job_id, "fb_group_react_tw", 1, "post_201", 3500)
        .await
        .expect("acknowledgement should succeed");

    assert!(acked_item.is_acknowledged());
    assert!(!acked_item.is_unread_needs_attention());
    assert_eq!(acked_item.acknowledged_at_ms, Some(3500));

    // After acknowledgement, run 3 with unchanged item maintains acknowledged state
    let runner3 = ObservationMonitorRunner::with_observations(vec![obs]);
    let report3 = svc.run_occurrence("user_hank", "conv_1", &job_id, 4000, &runner3).await.unwrap();
    assert!(report3.reported_observations.is_empty());

    let cursor3 = svc.get_cursor("user_hank", "conv_1", &job_id, "fb_group_react_tw", 1).await.unwrap().unwrap();
    let item3 = cursor3.items.get("post_201").unwrap();
    assert!(item3.is_acknowledged());
    assert!(!item3.is_unread_needs_attention());

    // Content change resets acknowledgement: needs attention again
    let obs_edited = FacebookObservation::new("post_201", "fb_group_react_tw", "hash_edited");
    let runner4 = ObservationMonitorRunner::with_observations(vec![obs_edited]);
    let report4 = svc.run_occurrence("user_hank", "conv_1", &job_id, 5000, &runner4).await.unwrap();
    assert_eq!(report4.reported_observations.len(), 1);
    assert_eq!(report4.reported_observations[0].delta_kind, ObservationDeltaKind::Changed);

    let cursor4 = svc.get_cursor("user_hank", "conv_1", &job_id, "fb_group_react_tw", 1).await.unwrap().unwrap();
    let item4 = cursor4.items.get("post_201").unwrap();
    assert!(item4.is_unread_needs_attention(), "Edited content requires attention again");
    assert!(!item4.is_acknowledged());
}

#[tokio::test]
async fn test_cursor_advances_only_on_success_and_preserves_on_failure() {
    let svc = MonitorControlService::with_in_memory_repo();
    let req = valid_request(Some(CronSchedule::Every {
        every_ms: 3600000,
        description: None,
    }));
    let outcome = svc.create_job("user_hank", "conv_1", req, 1000).await.unwrap();
    let job_id = match outcome {
        CreateMonitorJobOutcome::Active { job } => job.id,
        _ => panic!(),
    };

    // Step 1: Successful initial run with post_1
    let obs1 = FacebookObservation::new("post_1", "fb_group_react_tw", "hash_v1");
    let runner1 = ObservationMonitorRunner::with_observations(vec![obs1]);
    svc.run_occurrence("user_hank", "conv_1", &job_id, 2000, &runner1).await.unwrap();

    let cursor_before = svc
        .get_cursor("user_hank", "conv_1", &job_id, "fb_group_react_tw", 1)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(cursor_before.last_successful_observed_at_ms, Some(2000));
    assert_eq!(cursor_before.items.len(), 1);

    // Step 2: Failed run (e.g. platform error / checkpoint)
    let failing_runner = ObservationMonitorRunner::failing();
    let report_failed = svc
        .run_occurrence("user_hank", "conv_1", &job_id, 3000, &failing_runner)
        .await
        .unwrap();
    assert_eq!(report_failed.outcome, MonitorRunOutcome::Failed);

    // Cursor must NOT have advanced
    let cursor_after = svc
        .get_cursor("user_hank", "conv_1", &job_id, "fb_group_react_tw", 1)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(cursor_after.last_successful_observed_at_ms, Some(2000), "Cursor should not advance on failed run");
    assert_eq!(cursor_after.items.len(), 1);
    assert_eq!(cursor_after.items.get("post_1").unwrap().last_seen_at_ms, 2000);

    // Step 3: Failure at repository write boundary also preserves previous cursor
    let failing_repo = Arc::new(FailingCompletionRepository {
        inner: InMemoryMonitorJobRepository::new(),
    });
    let svc_fail_repo = MonitorControlService::new(failing_repo.clone());
    let req2 = valid_request(Some(CronSchedule::Every {
        every_ms: 3600000,
        description: None,
    }));
    let out2 = svc_fail_repo.create_job("user_hank", "conv_1", req2, 1000).await.unwrap();
    let job2_id = match out2 {
        CreateMonitorJobOutcome::Active { job } => job.id,
        _ => panic!(),
    };

    let obs_new = FacebookObservation::new("post_fail", "fb_group_react_tw", "hash_new");
    let runner_ok = ObservationMonitorRunner::with_observations(vec![obs_new]);
    let err = svc_fail_repo.run_occurrence("user_hank", "conv_1", &job2_id, 4000, &runner_ok).await.unwrap_err();
    assert!(matches!(err, MonitorError::Repository(_)));

    let cursor_fail = svc_fail_repo
        .get_cursor("user_hank", "conv_1", &job2_id, "fb_group_react_tw", 1)
        .await
        .unwrap();
    assert!(cursor_fail.is_none(), "Cursor must not be persisted when durable report write fails");
}

#[tokio::test]
async fn test_repeated_scheduled_run_idempotency_with_delta_reporting() {
    let svc = MonitorControlService::with_in_memory_repo();
    let req = valid_request(Some(CronSchedule::Every {
        every_ms: 3600000,
        description: None,
    }));
    let outcome = svc.create_job("user_hank", "conv_1", req, 1000).await.unwrap();
    let job_id = match outcome {
        CreateMonitorJobOutcome::Active { job } => job.id,
        _ => panic!(),
    };

    let obs = FacebookObservation::new("post_repeat", "fb_group_react_tw", "hash_repeat");
    let runner = ObservationMonitorRunner::with_observations(vec![obs]);

    // First call at 2000 ms
    let report1 = svc.run_occurrence("user_hank", "conv_1", &job_id, 2000, &runner).await.unwrap();
    assert_eq!(report1.reported_observations.len(), 1);

    // Repeated call with SAME scheduled_at_ms (2000 ms)
    let report2 = svc.run_occurrence("user_hank", "conv_1", &job_id, 2000, &runner).await.unwrap();
    assert_eq!(report1, report2, "Repeated delivery must return identical canonical report");

    // All reports for this job list exactly 1 entry
    let all_reports = svc.list_run_reports("user_hank", "conv_1", &job_id).await.unwrap();
    assert_eq!(all_reports.len(), 1);
}

#[tokio::test]
async fn test_cursor_isolation_across_users_and_conversations() {
    let svc = MonitorControlService::with_in_memory_repo();
    let req = valid_request(Some(CronSchedule::Every {
        every_ms: 3600000,
        description: None,
    }));
    let outcome = svc.create_job("user_alice", "conv_alice", req, 1000).await.unwrap();
    let job_id = match outcome {
        CreateMonitorJobOutcome::Active { job } => job.id,
        _ => panic!(),
    };

    let obs = FacebookObservation::new("post_secret", "fb_group_react_tw", "hash_secret");
    let runner = ObservationMonitorRunner::with_observations(vec![obs]);
    svc.run_occurrence("user_alice", "conv_alice", &job_id, 2000, &runner).await.unwrap();

    // User Bob cannot read Alice's cursor
    let bob_cursor = svc
        .get_cursor("user_bob", "conv_alice", &job_id, "fb_group_react_tw", 1)
        .await
        .unwrap();
    assert!(bob_cursor.is_none(), "Cross-user cursor read must return None");

    // User Bob cannot acknowledge Alice's cursor observation
    let err = svc
        .acknowledge_observation("user_bob", "conv_alice", &job_id, "fb_group_react_tw", 1, "post_secret", 2500)
        .await
        .unwrap_err();
    assert!(matches!(err, MonitorError::NotFound(_)));

    // Conversation other cannot read or acknowledge
    let err = svc
        .acknowledge_observation("user_alice", "conv_other", &job_id, "fb_group_react_tw", 1, "post_secret", 2500)
        .await
        .unwrap_err();
    assert!(matches!(err, MonitorError::NotFound(_)));
}

// ---------------------------------------------------------------------------
// Ticket 04 Tests: Query revision, lookback, and backfill
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_editing_query_bumps_revision_and_creates_isolated_cursor() {
    let svc = MonitorControlService::with_in_memory_repo();
    let req = valid_request(Some(CronSchedule::Every {
        every_ms: 3600000,
        description: None,
    }));
    let outcome = svc.create_job("user_hank", "conv_1", req, 1000).await.unwrap();
    let job_id = match outcome {
        CreateMonitorJobOutcome::Active { job } => job.id,
        _ => panic!(),
    };

    // Run occurrence 1 under revision 1
    let obs1 = FacebookObservation::new("post_1", "fb_group_react_tw", "hash_1")
        .with_published_at(1500);
    let runner1 = ObservationMonitorRunner::with_observations(vec![obs1]);
    let report1 = svc.run_occurrence("user_hank", "conv_1", &job_id, 2000, &runner1).await.unwrap();
    assert_eq!(report1.reported_observations.len(), 1);
    assert_eq!(report1.reported_observations[0].delta_kind, ObservationDeltaKind::New);

    // Cursor for revision 1 exists
    let cursor_rev1 = svc
        .get_cursor("user_hank", "conv_1", &job_id, "fb_group_react_tw", 1)
        .await
        .unwrap()
        .expect("revision 1 cursor exists");
    assert_eq!(cursor_rev1.query_revision, 1);
    assert_eq!(cursor_rev1.items.len(), 1);

    // Edit query to bump to revision 2
    let updated_job = svc
        .update_query(
            "user_hank",
            "conv_1",
            &job_id,
            "rust distributed systems hiring",
            None,
            None,
            2500,
        )
        .await
        .expect("update_query should succeed");

    assert_eq!(updated_job.query.revision, 2);
    assert_eq!(updated_job.query.query_text, "rust distributed systems hiring");
    assert_eq!(updated_job.query_revised_at_ms, Some(2500));
    assert_eq!(updated_job.updated_at_ms, 2500);

    // CRITICAL: revision 2 has NOT run yet; its cursor is empty/None and does NOT reuse rev 1 cursor
    let cursor_rev2 = svc
        .get_cursor("user_hank", "conv_1", &job_id, "fb_group_react_tw", 2)
        .await
        .unwrap();
    assert!(cursor_rev2.is_none(), "Revision 2 must not reuse revision 1's cursor");

    // Revision 1 cursor remains intact and isolated
    let cursor_rev1_intact = svc
        .get_cursor("user_hank", "conv_1", &job_id, "fb_group_react_tw", 1)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(cursor_rev1_intact.items.len(), 1);
}

#[tokio::test]
async fn test_revised_query_lookback_labels_existing_as_backfill_and_recent_as_new() {
    let svc = MonitorControlService::with_in_memory_repo();
    let req = valid_request(Some(CronSchedule::Every {
        every_ms: 3600000,
        description: None,
    }));
    let outcome = svc.create_job("user_hank", "conv_1", req, 1000).await.unwrap();
    let job_id = match outcome {
        CreateMonitorJobOutcome::Active { job } => job.id,
        _ => panic!(),
    };

    // Update query at 2500 ms -> revision 2
    svc.update_query(
        "user_hank",
        "conv_1",
        &job_id,
        "remote eline rust engineer",
        None,
        Some(LookbackScope::from_days(14)),
        2500,
    )
    .await
    .unwrap();

    // Run occurrence under revision 2 at 3000 ms:
    // - post_old was published at 1500 ms (< 2500 ms revision boundary): existing in lookback window
    // - post_fresh was published at 2800 ms (>= 2500 ms revision boundary): newly published post
    let post_old = FacebookObservation::new("post_old", "fb_group_react_tw", "hash_old")
        .with_published_at(1500);
    let post_fresh = FacebookObservation::new("post_fresh", "fb_group_react_tw", "hash_fresh")
        .with_published_at(2800);

    let runner = ObservationMonitorRunner::with_observations(vec![post_old.clone(), post_fresh.clone()]);
    let report = svc.run_occurrence("user_hank", "conv_1", &job_id, 3000, &runner).await.unwrap();

    assert_eq!(report.outcome, MonitorRunOutcome::Success);
    assert_eq!(report.query_revision, Some(2));
    assert_eq!(report.lookback_window_ms, Some(14 * 24 * 3600 * 1000));

    // Check classification
    let backfills = report.backfill_findings();
    let news = report.new_findings();

    assert_eq!(backfills.len(), 1);
    assert_eq!(backfills[0].id, "post_old");

    assert_eq!(news.len(), 1);
    assert_eq!(news[0].id, "post_fresh");

    // Conversation report formatting groups backfill separately with revision and lookback
    let formatted = report.format_conversation_report();
    assert!(formatted.contains("### Backfill Findings (Query Revision 2, Lookback 1209600000ms)"));
    assert!(formatted.contains("post_old"));
    assert!(formatted.contains("### New Findings"));
    assert!(formatted.contains("post_fresh"));
}

#[tokio::test]
async fn test_replaying_revised_query_produces_no_duplicate_backfill() {
    let svc = MonitorControlService::with_in_memory_repo();
    let req = valid_request(Some(CronSchedule::Every {
        every_ms: 3600000,
        description: None,
    }));
    let outcome = svc.create_job("user_hank", "conv_1", req, 1000).await.unwrap();
    let job_id = match outcome {
        CreateMonitorJobOutcome::Active { job } => job.id,
        _ => panic!(),
    };

    svc.update_query(
        "user_hank",
        "conv_1",
        &job_id,
        "lead rust engineer",
        None,
        None,
        2500,
    )
    .await
    .unwrap();

    let post_backfill = FacebookObservation::new("post_bf", "fb_group_react_tw", "hash_bf")
        .with_published_at(1800);
    let runner = ObservationMonitorRunner::with_observations(vec![post_backfill.clone()]);

    // Initial run of revision 2: reports 1 backfill observation
    let report1 = svc.run_occurrence("user_hank", "conv_1", &job_id, 3000, &runner).await.unwrap();
    assert_eq!(report1.backfill_findings().len(), 1);

    // Replay on subsequent run at 4000 ms with the same observation unchanged:
    let runner2 = ObservationMonitorRunner::with_observations(vec![post_backfill]);
    let report2 = svc.run_occurrence("user_hank", "conv_1", &job_id, 4000, &runner2).await.unwrap();

    // Idempotent delta reporting: unchanged backfill item is NOT emitted again
    assert!(report2.reported_observations.is_empty(), "Replaying same revision produces no duplicate backfill");
    assert!(report2.backfill_findings().is_empty());
}

#[tokio::test]
async fn test_updating_query_validation_and_lifecycle_guards() {
    let svc = MonitorControlService::with_in_memory_repo();
    let req = valid_request(Some(CronSchedule::Every {
        every_ms: 3600000,
        description: None,
    }));
    let outcome = svc.create_job("user_hank", "conv_1", req, 1000).await.unwrap();
    let job_id = match outcome {
        CreateMonitorJobOutcome::Active { job } => job.id,
        _ => panic!(),
    };

    // 1. Blank query text fails closed
    let err = svc
        .update_query("user_hank", "conv_1", &job_id, "   ", None, None, 2000)
        .await
        .unwrap_err();
    assert!(matches!(err, MonitorError::InvalidQueryScope(_)));

    // 2. Zero lookback duration fails closed
    let err = svc
        .update_query(
            "user_hank",
            "conv_1",
            &job_id,
            "valid query",
            None,
            Some(LookbackScope::from_millis(0)),
            2000,
        )
        .await
        .unwrap_err();
    assert!(matches!(err, MonitorError::InvalidLookbackScope(_)));

    // 3. Cross-user update rejected with NotFound
    let err = svc
        .update_query("user_other", "conv_1", &job_id, "valid query", None, None, 2000)
        .await
        .unwrap_err();
    assert!(matches!(err, MonitorError::NotFound(_)));

    // 4. Cancelled job cannot be updated
    svc.cancel_job("user_hank", "conv_1", &job_id, MonitorStopReason::ExplicitUserCancellation, 2500)
        .await
        .unwrap();

    let err = svc
        .update_query("user_hank", "conv_1", &job_id, "valid query", None, None, 3000)
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        MonitorError::InvalidLifecycleTransition {
            current: MonitorJobStatus::Cancelled,
            ..
        }
    ));
}

// ---------------------------------------------------------------------------
// Ticket 05 Tests: FacebookProfile authentication pause and resume
// ---------------------------------------------------------------------------

struct AuthFailingRunner {
    kind: &'static str,
}

#[async_trait::async_trait]
impl MonitorRunner for AuthFailingRunner {
    async fn run_scan(&self, _job: &MonitorJob) -> Result<MonitorScanResult, String> {
        match self.kind {
            "expired" => Ok(MonitorScanResult::auth_expired("Facebook session cookie expired")),
            "checkpoint" => Err("Facebook security Checkpoint detected on page load".into()),
            "captcha" => Err("Facebook anti-bot CAPTCHA challenge presented".into()),
            _ => Err("Unknown error".into()),
        }
    }
}

#[tokio::test]
async fn test_client_cannot_bind_another_users_facebook_profile_fail_closed() {
    let svc = MonitorControlService::with_in_memory_repo();

    // Alice registers her profile
    let alice_prof = FacebookProfile::new("prof_alice_primary", "user_alice")
        .with_display_name("Alice Facebook");
    svc.register_profile(alice_prof).await.unwrap();

    // Bob tries to create a job referencing Alice's profile
    let mut req_bob = valid_request(Some(CronSchedule::Every {
        every_ms: 3600000,
        description: None,
    }));
    req_bob.profile_ref = Some("prof_alice_primary".into());

    let err = svc
        .create_job("user_bob", "conv_bob_1", req_bob, 1000)
        .await
        .unwrap_err();
    assert!(
        matches!(err, MonitorError::AccessDenied(_)),
        "Client input choosing another user's profile must fail closed"
    );

    // Bob tries to start LiveView on Alice's profile
    let err = svc
        .start_liveview_session("user_bob", "prof_alice_primary")
        .await
        .unwrap_err();
    assert!(matches!(err, MonitorError::AccessDenied(_)));
}

#[tokio::test]
async fn test_auth_expiry_checkpoint_captcha_auto_pauses_without_auto_retry() {
    let svc = MonitorControlService::with_in_memory_repo();
    let prof = FacebookProfile::new("prof_hank", "user_hank");
    svc.register_profile(prof).await.unwrap();

    // 1. Auth Expired
    let mut req = valid_request(Some(CronSchedule::Every {
        every_ms: 3600000,
        description: None,
    }));
    req.profile_ref = Some("prof_hank".into());
    let out = svc.create_job("user_hank", "conv_1", req.clone(), 1000).await.unwrap();
    let job1_id = match out {
        CreateMonitorJobOutcome::Active { job } => job.id,
        _ => panic!(),
    };

    let runner_expired = AuthFailingRunner { kind: "expired" };
    svc.run_occurrence("user_hank", "conv_1", &job1_id, 2000, &runner_expired).await.unwrap();

    let job1 = svc.get_job("user_hank", "conv_1", &job1_id).await.unwrap();
    assert_eq!(job1.status, MonitorJobStatus::Paused);
    assert_eq!(job1.stop_reason, Some(MonitorStopReason::AuthExpired));
    assert_eq!(job1.next_execution_at_ms, None, "Must perform no automatic authentication retry");

    let p1 = svc.get_profile("user_hank", "prof_hank").await.unwrap().unwrap();
    assert_eq!(p1.auth_state, ProfileAuthState::Expired);

    // 2. Checkpoint Detected
    let out2 = svc.create_job("user_hank", "conv_1", req.clone(), 1000).await.unwrap();
    let job2_id = match out2 {
        CreateMonitorJobOutcome::Active { job } => job.id,
        _ => panic!(),
    };

    let runner_cp = AuthFailingRunner { kind: "checkpoint" };
    svc.run_occurrence("user_hank", "conv_1", &job2_id, 3000, &runner_cp).await.unwrap();

    let job2 = svc.get_job("user_hank", "conv_1", &job2_id).await.unwrap();
    assert_eq!(job2.status, MonitorJobStatus::Paused);
    assert_eq!(job2.stop_reason, Some(MonitorStopReason::CheckpointDetected));
    assert_eq!(job2.next_execution_at_ms, None);

    // 3. CAPTCHA Detected
    let out3 = svc.create_job("user_hank", "conv_1", req, 1000).await.unwrap();
    let job3_id = match out3 {
        CreateMonitorJobOutcome::Active { job } => job.id,
        _ => panic!(),
    };

    let runner_captcha = AuthFailingRunner { kind: "captcha" };
    svc.run_occurrence("user_hank", "conv_1", &job3_id, 4000, &runner_captcha).await.unwrap();

    let job3 = svc.get_job("user_hank", "conv_1", &job3_id).await.unwrap();
    assert_eq!(job3.status, MonitorJobStatus::Paused);
    assert_eq!(job3.stop_reason, Some(MonitorStopReason::CaptchaDetected));
    assert_eq!(job3.next_execution_at_ms, None);
}

#[tokio::test]
async fn test_liveview_reauth_resumes_paused_jobs_only_for_next_scheduled_run() {
    let svc = MonitorControlService::with_in_memory_repo();
    let prof = FacebookProfile::new("prof_shared", "user_hank");
    svc.register_profile(prof).await.unwrap();

    let mut req = valid_request(Some(CronSchedule::Every {
        every_ms: 3600000, // 1 hour
        description: None,
    }));
    req.profile_ref = Some("prof_shared".into());

    // Job A in conv_1 (AuthExpired)
    let out_a = svc.create_job("user_hank", "conv_1", req.clone(), 1000).await.unwrap();
    let job_a_id = match out_a {
        CreateMonitorJobOutcome::Active { job } => job.id,
        _ => panic!(),
    };
    let runner_expired = AuthFailingRunner { kind: "expired" };
    svc.run_occurrence("user_hank", "conv_1", &job_a_id, 2000, &runner_expired).await.unwrap();

    // Job B in conv_1 (Explicit user pause, NOT auth failure)
    let out_b = svc.create_job("user_hank", "conv_1", req.clone(), 1000).await.unwrap();
    let job_b_id = match out_b {
        CreateMonitorJobOutcome::Active { job } => job.id,
        _ => panic!(),
    };
    svc.pause_job("user_hank", "conv_1", &job_b_id, MonitorStopReason::ExplicitUserPause, 2100).await.unwrap();

    // Job C in conv_other (AuthExpired, but in different conversation)
    let out_c = svc.create_job("user_hank", "conv_other", req.clone(), 1000).await.unwrap();
    let job_c_id = match out_c {
        CreateMonitorJobOutcome::Active { job } => job.id,
        _ => panic!(),
    };
    svc.run_occurrence("user_hank", "conv_other", &job_c_id, 2200, &runner_expired).await.unwrap();

    // User completes interactive LiveView re-auth at 5000 ms in conv_1
    let resumed = svc
        .complete_liveview_reauth("user_hank", "conv_1", "prof_shared", 5000)
        .await
        .expect("re-auth should succeed");

    // Only Job A in conv_1 should be resumed!
    assert_eq!(resumed.len(), 1);
    assert_eq!(resumed[0].id, job_a_id);
    assert_eq!(resumed[0].status, MonitorJobStatus::Active);
    assert_eq!(resumed[0].stop_reason, None);

    // CRITICAL: Next run is scheduled for the NEXT occurrence (5000 + 3600000 = 8600000), NOT immediate!
    assert_eq!(
        resumed[0].next_execution_at_ms,
        Some(8600000),
        "Resumed job must be eligible ONLY for its next scheduled run"
    );

    // Job B (explicit pause) remains Paused
    let job_b = svc.get_job("user_hank", "conv_1", &job_b_id).await.unwrap();
    assert_eq!(job_b.status, MonitorJobStatus::Paused);
    assert_eq!(job_b.stop_reason, Some(MonitorStopReason::ExplicitUserPause));

    // Job C in conv_other remains Paused (conversation isolation)
    let job_c = svc.get_job("user_hank", "conv_other", &job_c_id).await.unwrap();
    assert_eq!(job_c.status, MonitorJobStatus::Paused);

    // Profile auth state is now Authenticated
    let prof_updated = svc.get_profile("user_hank", "prof_shared").await.unwrap().unwrap();
    assert_eq!(prof_updated.auth_state, ProfileAuthState::Authenticated);
}

#[tokio::test]
async fn test_liveview_interactive_precedence_over_background_monitor_run() {
    let svc = MonitorControlService::with_in_memory_repo();
    let prof = FacebookProfile::new("prof_interactive", "user_hank");
    svc.register_profile(prof).await.unwrap();

    let mut req = valid_request(Some(CronSchedule::Every {
        every_ms: 3600000,
        description: None,
    }));
    req.profile_ref = Some("prof_interactive".into());

    let out = svc.create_job("user_hank", "conv_1", req, 1000).await.unwrap();
    let job_id = match out {
        CreateMonitorJobOutcome::Active { job } => job.id,
        _ => panic!(),
    };

    // User starts LiveView session
    svc.start_liveview_session("user_hank", "prof_interactive").await.unwrap();
    assert!(svc.is_liveview_active("prof_interactive").await);

    // Conflicting background MonitorRun attempts to execute
    let runner = FakeMonitorRunner;
    let err = svc
        .run_occurrence("user_hank", "conv_1", &job_id, 2000, &runner)
        .await
        .unwrap_err();

    assert!(
        matches!(err, MonitorError::ProfileBusy(_)),
        "LiveView session must take precedence and cause background run to yield"
    );

    // User finishes LiveView session
    svc.end_liveview_session("prof_interactive").await;
    assert!(!svc.is_liveview_active("prof_interactive").await);

    // Background run now succeeds without conflict
    let report = svc
        .run_occurrence("user_hank", "conv_1", &job_id, 2000, &runner)
        .await
        .expect("Monitor run succeeds once LiveView ends");
    assert_eq!(report.outcome, MonitorRunOutcome::Success);
}

#[tokio::test]
async fn test_passwords_and_mfa_secrets_not_stored_in_domain_or_reports() {
    let prof = FacebookProfile::new("prof_secure", "user_hank");
    let serialized_prof = serde_json::to_string(&prof).unwrap();
    assert!(!serialized_prof.contains("password"));
    assert!(!serialized_prof.contains("mfa"));
    assert!(!serialized_prof.contains("secret"));
    assert!(!serialized_prof.contains("token"));

    let req = valid_request(Some(CronSchedule::Every { every_ms: 60000, description: None }));
    let serialized_req = serde_json::to_string(&req).unwrap();
    assert!(!serialized_req.contains("password"));
    assert!(!serialized_req.contains("mfa"));
}

// ---------------------------------------------------------------------------
// Ticket 06 Tests: Multi-target partial results and availability status
// ---------------------------------------------------------------------------

struct MultiTargetRunner {
    results: Vec<TargetScanResult>,
}

#[async_trait::async_trait]
impl MonitorRunner for MultiTargetRunner {
    async fn run_scan(&self, _job: &MonitorJob) -> Result<MonitorScanResult, String> {
        Ok(MonitorScanResult::multi_target(self.results.clone()))
    }
}

#[tokio::test]
async fn test_multi_target_partial_results_isolated_failure() {
    let svc = MonitorControlService::with_in_memory_repo();
    let targets = vec![
        valid_target("group_a", "Group A"),
        valid_target("group_b", "Group B"),
        valid_target("group_c", "Group C"),
    ];
    let req = CreateMonitorJobRequest {
        targets,
        query: MonitorQuery::new("rust engineer"),
        lookback: LookbackScope::from_days(7),
        schedule: Some(CronSchedule::Every {
            every_ms: 3600000,
            description: None,
        }),
        profile_ref: None,
    };

    let outcome = svc.create_job("user_hank", "conv_1", req, 1000).await.unwrap();
    let job_id = match outcome {
        CreateMonitorJobOutcome::Active { job } => job.id,
        _ => panic!(),
    };

    // Runner where group_a and group_c succeed, but group_b fails with rate limiting
    let obs_a = FacebookObservation::new("post_a1", "group_a", "hash_a1");
    let obs_c = FacebookObservation::new("post_c1", "group_c", "hash_c1");

    let runner = MultiTargetRunner {
        results: vec![
            TargetScanResult::success("group_a", vec![obs_a]),
            TargetScanResult::failed("group_b", "Rate limited by Facebook API (HTTP 429)"),
            TargetScanResult::success("group_c", vec![obs_c]),
        ],
    };

    let report = svc.run_occurrence("user_hank", "conv_1", &job_id, 2000, &runner).await.unwrap();

    // 1. Overall outcome is explicitly Partial
    assert_eq!(report.outcome, MonitorRunOutcome::Partial);

    // 2. Identifies successful and failed targets
    assert_eq!(report.successful_targets, vec!["group_a".to_string(), "group_c".to_string()]);
    assert_eq!(report.failed_targets.len(), 1);
    assert_eq!(report.failed_targets[0].target_id, "group_b");
    assert!(report.failed_targets[0].reason.contains("Rate limited"));

    // 3. Healthy targets produced reports without being hidden or blocked
    let news = report.new_findings();
    assert_eq!(news.len(), 2);
    let ids: Vec<_> = news.iter().map(|o| o.id.as_str()).collect();
    assert!(ids.contains(&"post_a1"));
    assert!(ids.contains(&"post_c1"));

    // 4. Healthy target cursors advanced
    let cursor_a = svc.get_cursor("user_hank", "conv_1", &job_id, "group_a", 1).await.unwrap().unwrap();
    assert_eq!(cursor_a.last_successful_observed_at_ms, Some(2000));
    assert!(cursor_a.items.contains_key("post_a1"));

    let cursor_c = svc.get_cursor("user_hank", "conv_1", &job_id, "group_c", 1).await.unwrap().unwrap();
    assert_eq!(cursor_c.last_successful_observed_at_ms, Some(2000));
    assert!(cursor_c.items.contains_key("post_c1"));

    // 5. Failed target cursor did NOT advance
    let cursor_b = svc.get_cursor("user_hank", "conv_1", &job_id, "group_b", 1).await.unwrap();
    assert!(cursor_b.is_none(), "Failed target cursor must not be advanced");
}

#[tokio::test]
async fn test_confirmed_deletion_reports_removed_and_cleans_cursor() {
    let svc = MonitorControlService::with_in_memory_repo();
    let req = valid_request(Some(CronSchedule::Every {
        every_ms: 3600000,
        description: None,
    }));
    let outcome = svc.create_job("user_hank", "conv_1", req, 1000).await.unwrap();
    let job_id = match outcome {
        CreateMonitorJobOutcome::Active { job } => job.id,
        _ => panic!(),
    };

    // Run 1 discovers post_1
    let obs1 = FacebookObservation::new("post_1", "fb_group_react_tw", "hash_1");
    let runner1 = ObservationMonitorRunner::with_observations(vec![obs1]);
    svc.run_occurrence("user_hank", "conv_1", &job_id, 2000, &runner1).await.unwrap();

    let cursor = svc.get_cursor("user_hank", "conv_1", &job_id, "fb_group_react_tw", 1).await.unwrap().unwrap();
    assert!(cursor.items.contains_key("post_1"));

    // Run 2: post_1 is confirmed deleted (platform returned 404/post removed)
    let del_obs = FacebookObservation::confirmed_deleted("post_1", "fb_group_react_tw");
    let runner2 = ObservationMonitorRunner::with_observations(vec![del_obs]);
    let report2 = svc.run_occurrence("user_hank", "conv_1", &job_id, 3000, &runner2).await.unwrap();

    // Reported as Removed
    assert_eq!(report2.outcome, MonitorRunOutcome::Success);
    let removed = report2.removed_findings();
    assert_eq!(removed.len(), 1);
    assert_eq!(removed[0].id, "post_1");

    // Cursor removes the item but advances its timestamp
    let cursor2 = svc.get_cursor("user_hank", "conv_1", &job_id, "fb_group_react_tw", 1).await.unwrap().unwrap();
    assert!(!cursor2.items.contains_key("post_1"), "Confirmed deleted item must be removed from cursor");
    assert_eq!(cursor2.last_successful_observed_at_ms, Some(3000));
}

#[tokio::test]
async fn test_temporary_unavailability_preserves_content_and_last_seen_time() {
    let svc = MonitorControlService::with_in_memory_repo();
    let req = valid_request(Some(CronSchedule::Every {
        every_ms: 3600000,
        description: None,
    }));
    let outcome = svc.create_job("user_hank", "conv_1", req, 1000).await.unwrap();
    let job_id = match outcome {
        CreateMonitorJobOutcome::Active { job } => job.id,
        _ => panic!(),
    };

    // Run 1 discovers post_transient
    let obs1 = FacebookObservation::new("post_transient", "fb_group_react_tw", "hash_original");
    let runner1 = ObservationMonitorRunner::with_observations(vec![obs1]);
    svc.run_occurrence("user_hank", "conv_1", &job_id, 2000, &runner1).await.unwrap();

    // Run 2: post is temporarily unavailable (unconfirmed deletion / access timeout)
    let unavail_obs = FacebookObservation::temporarily_unavailable("post_transient", "fb_group_react_tw");
    let runner2 = ObservationMonitorRunner::with_observations(vec![unavail_obs]);
    let report2 = svc.run_occurrence("user_hank", "conv_1", &job_id, 3000, &runner2).await.unwrap();

    // Reported as Unavailable, NEVER Removed
    assert_eq!(report2.unavailable_findings().len(), 1);
    assert_eq!(report2.unavailable_findings()[0].id, "post_transient");
    assert!(report2.removed_findings().is_empty(), "Transient unavailability must never be reported as removed");

    // Cursor PRESERVES item content hash and first seen time
    let cursor = svc.get_cursor("user_hank", "conv_1", &job_id, "fb_group_react_tw", 1).await.unwrap().unwrap();
    let item = cursor.items.get("post_transient").expect("Item must be preserved in cursor");
    assert_eq!(item.content_hash, "hash_original");
    assert_eq!(item.first_seen_at_ms, 2000);
}

#[tokio::test]
async fn test_untrusted_dom_structure_fails_closed_and_does_not_advance_cursor() {
    let svc = MonitorControlService::with_in_memory_repo();
    let req = valid_request(Some(CronSchedule::Every {
        every_ms: 3600000,
        description: None,
    }));
    let outcome = svc.create_job("user_hank", "conv_1", req, 1000).await.unwrap();
    let job_id = match outcome {
        CreateMonitorJobOutcome::Active { job } => job.id,
        _ => panic!(),
    };

    // Runner encounters changed or untrusted DOM structure
    let runner = MultiTargetRunner {
        results: vec![
            TargetScanResult::untrusted_dom(
                "fb_group_react_tw",
                "Facebook post container layout changed or unrecognizable DOM tree",
            ),
        ],
    };

    let report = svc.run_occurrence("user_hank", "conv_1", &job_id, 2000, &runner).await.unwrap();

    // Fails closed
    assert_eq!(report.outcome, MonitorRunOutcome::Failed);
    assert_eq!(report.failed_targets.len(), 1);
    assert!(report.failed_targets[0].reason.contains("layout changed"));

    // Cursor is NOT advanced
    let cursor = svc.get_cursor("user_hank", "conv_1", &job_id, "fb_group_react_tw", 1).await.unwrap();
    assert!(cursor.is_none(), "Untrusted DOM scan must not advance or save cursor");
}

#[tokio::test]
async fn test_format_conversation_report_presentation() {
    let report = MonitorRunReport {
        job_id: "mon_test".into(),
        user_id: "user_hank".into(),
        conversation_id: "conv_1".into(),
        scheduled_at_ms: 2000,
        outcome: MonitorRunOutcome::Partial,
        observations_count: 3,
        error_message: None,
        reported_observations: vec![
            ReportedObservation {
                delta_kind: ObservationDeltaKind::Removed,
                observation: FacebookObservation::new("p_del", "target_1", ""),
            },
            ReportedObservation {
                delta_kind: ObservationDeltaKind::Unavailable,
                observation: FacebookObservation::new("p_unavail", "target_1", ""),
            },
        ],
        target_id: Some("target_1".into()),
        query_revision: Some(1),
        lookback_window_ms: Some(86400000),
        successful_targets: vec!["target_1".into()],
        failed_targets: vec![TargetFailure {
            target_id: "target_2".into(),
            reason: "HTTP 503 Service Unavailable".into(),
        }],
    };

    let formatted = report.format_conversation_report();
    assert!(formatted.contains("### Removed Findings"));
    assert!(formatted.contains("p_del"));
    assert!(formatted.contains("### Unavailable Findings"));
    assert!(formatted.contains("p_unavail"));
    assert!(formatted.contains("### Failed Targets"));
    assert!(formatted.contains("target_2: HTTP 503"));
}




