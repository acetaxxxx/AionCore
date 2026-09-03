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
    FacebookTarget, IMonitorJobRepository, InMemoryMonitorJobRepository, LookbackScope,
    MonitorControlService, MonitorCursor, MonitorError, MonitorJob, MonitorJobStatus,
    MonitorQuery, MonitorRunOutcome, MonitorRunReport, MonitorRunner, MonitorScanResult,
    MonitorStopReason, ObservationDeltaKind, ReportedObservation, propose_default_schedule,
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
        _cursor: Option<&MonitorCursor>,
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

