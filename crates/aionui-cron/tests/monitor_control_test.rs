//! Public service-boundary integration tests for Ticket 01: Conversation-bound MonitorJob control.
//!
//! Verifies:
//! - Explicit creation binds to originating conversation and user.
//! - Complete target, query, and lookback scope requirement (incomplete rejected).
//! - Schedule supply vs. agent proposal and approval.
//! - Lifecycle control semantics: inspect, pause, resume, cancel, list.
//! - Strict user and conversation isolation (no cross-conversation or cross-user exposure).
//! - Conversation termination lifecycle hook.
//! - Tests verify only the public service seam and domain behavior.

use aionui_cron::monitor::{
    CreateMonitorJobOutcome, CreateMonitorJobRequest, FacebookTarget, LookbackScope,
    MonitorControlService, MonitorError, MonitorJob, MonitorJobStatus, MonitorQuery,
    MonitorRunOutcome, MonitorRunner, MonitorScanResult, MonitorStopReason,
    propose_default_schedule,
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
        })
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
async fn test_omitted_schedule_flow_requires_user_approval() {
    let svc = MonitorControlService::with_in_memory_repo();
    let req = valid_request(None); // Omitted schedule

    let outcome = svc
        .create_job("user_hank", "conv_1", req, 1000)
        .await
        .expect("creation should yield proposal");

    let (job_id, proposed) = match outcome {
        CreateMonitorJobOutcome::RequiresApproval {
            job,
            proposed_schedule,
        } => {
            assert_eq!(job.status, MonitorJobStatus::PendingApproval);
            assert!(job.next_execution_at_ms.is_none());
            (job.id, proposed_schedule)
        }
        CreateMonitorJobOutcome::Active { .. } => panic!("Expected approval flow"),
    };

    assert_eq!(proposed, propose_default_schedule());

    // Runner cannot execute pending approval job
    let pending_job = svc.get_job("user_hank", "conv_1", &job_id).await.unwrap();
    let runner = FakeMonitorRunner;
    let run_err = runner.run_scan(&pending_job).await.unwrap_err();
    assert!(run_err.contains("Cannot run inactive monitor"));

    // Approve the proposed schedule
    let approved_job = svc
        .approve_proposal("user_hank", "conv_1", &job_id, None, 2000)
        .await
        .expect("approval should succeed");

    assert_eq!(approved_job.status, MonitorJobStatus::Active);
    assert_eq!(approved_job.schedule, proposed);
    assert_eq!(approved_job.updated_at_ms, 2000);
    assert_eq!(approved_job.next_execution_at_ms, Some(2000));

    // Runner can now execute
    let run_res = runner.run_scan(&approved_job).await.unwrap();
    assert_eq!(run_res.outcome, MonitorRunOutcome::Success);
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
            valid_request(None), // PendingApproval
            1000,
        )
        .await
        .unwrap();
    let job2_id = match out2 {
        CreateMonitorJobOutcome::RequiresApproval { job, .. } => job.id,
        _ => panic!(),
    };

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
