//! Narrow Facebook browser capability adapter implementing the `MonitorRunner` port seam.
//!
//! Implements Ticket 07:
//! - Bounded browser capability connecting authenticated browser sessions to `MonitorRunner`.
//! - Uses isolated `FacebookProfile` and `BrowserContext` per user/conversation.
//! - Enforces that page text, DOM, OCR, and comments remain untrusted observation data.
//! - Fails closed on authentication expiry, checkpoint, CAPTCHA, and DOM drift without automatic retries.
//! - Deterministically closes browser context after each run; no permanent resident browser.
//! - Operates at the public `MonitorRunner` seam without exposing generic browser automation.

use sha2::{Digest, Sha256};
use std::sync::Arc;

use crate::monitor::{
    FacebookObservation, LookbackScope, MonitorJob, MonitorQuery, MonitorRunOutcome, MonitorRunner, MonitorScanResult,
    TargetScanResult,
};

/// Raw post item extracted from a target group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawPostData {
    pub raw_id: String,
    pub raw_title: Option<String>,
    pub raw_body: Option<String>,
    pub raw_author: Option<String>,
    pub published_at_ms: u64,
    pub is_confirmed_deleted: bool,
    pub is_temporarily_unavailable: bool,
}

impl RawPostData {
    pub fn available(
        id: impl Into<String>,
        title: Option<String>,
        body: Option<String>,
        author: Option<String>,
        published_at_ms: u64,
    ) -> Self {
        Self {
            raw_id: id.into(),
            raw_title: title,
            raw_body: body,
            raw_author: author,
            published_at_ms,
            is_confirmed_deleted: false,
            is_temporarily_unavailable: false,
        }
    }

    pub fn confirmed_deleted(id: impl Into<String>) -> Self {
        Self {
            raw_id: id.into(),
            raw_title: None,
            raw_body: None,
            raw_author: None,
            published_at_ms: 0,
            is_confirmed_deleted: true,
            is_temporarily_unavailable: false,
        }
    }

    pub fn temporarily_unavailable(id: impl Into<String>) -> Self {
        Self {
            raw_id: id.into(),
            raw_title: None,
            raw_body: None,
            raw_author: None,
            published_at_ms: 0,
            is_confirmed_deleted: false,
            is_temporarily_unavailable: true,
        }
    }
}

/// Raw outcome of scanning a single target page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RawTargetScanOutcome {
    Success(Vec<RawPostData>),
    AuthExpired(String),
    CheckpointDetected(String),
    CaptchaDetected(String),
    UntrustedDomStructure(String),
    Unavailable(String),
    Failed(String),
}

/// Bounded browser session scoped to a user and conversation context.
#[async_trait::async_trait]
pub trait IFacebookBrowserSession: Send + Sync {
    async fn scan_target(
        &mut self,
        target_id: &str,
        query: &MonitorQuery,
        lookback: LookbackScope,
    ) -> Result<RawTargetScanOutcome, String>;

    /// Cleanly close and dispose the browser context.
    async fn close(self: Box<Self>) -> Result<(), String>;
}

/// Factory driver for allocating bounded browser sessions.
#[async_trait::async_trait]
pub trait IFacebookBrowserDriver: Send + Sync {
    async fn create_session(
        &self,
        user_id: &str,
        conversation_id: &str,
        profile_ref: Option<&str>,
    ) -> Result<Box<dyn IFacebookBrowserSession>, String>;
}

/// Compute a deterministic SHA-256 content hash of normalized post text.
pub fn compute_normalized_content_hash(title: Option<&str>, body: Option<&str>, author: Option<&str>) -> String {
    let mut hasher = Sha256::new();
    let t = title.unwrap_or("").trim();
    let b = body.unwrap_or("").trim();
    let a = author.unwrap_or("").trim();
    hasher.update(t.as_bytes());
    hasher.update(b"\0");
    hasher.update(b.as_bytes());
    hasher.update(b"\0");
    hasher.update(a.as_bytes());
    hex::encode(hasher.finalize())
}

/// Sanitize untrusted post text by removing control characters while preserving whitespace and unicode.
pub fn sanitize_observation_text(input: &str) -> String {
    input
        .chars()
        .filter(|c| *c == '\n' || *c == '\t' || !c.is_control())
        .collect()
}

/// Bounded Facebook browser capability adapter implementing `MonitorRunner`.
pub struct FacebookBrowserCapabilityAdapter {
    driver: Arc<dyn IFacebookBrowserDriver>,
}

impl FacebookBrowserCapabilityAdapter {
    pub fn new(driver: Arc<dyn IFacebookBrowserDriver>) -> Self {
        Self { driver }
    }
}

#[async_trait::async_trait]
impl MonitorRunner for FacebookBrowserCapabilityAdapter {
    async fn run_scan(&self, job: &MonitorJob) -> Result<MonitorScanResult, String> {
        let mut session = self
            .driver
            .create_session(&job.user_id, &job.conversation_id, job.profile_ref.as_deref())
            .await?;

        let mut target_results = Vec::new();
        let mut top_auth_expired = None;

        for target in &job.targets {
            let scan_outcome = session.scan_target(&target.target_id, &job.query, job.lookback).await;

            match scan_outcome {
                Ok(RawTargetScanOutcome::Success(raw_posts)) => {
                    let mut observations = Vec::new();
                    for raw in raw_posts {
                        if raw.is_confirmed_deleted {
                            observations.push(FacebookObservation::confirmed_deleted(raw.raw_id, &target.target_id));
                        } else if raw.is_temporarily_unavailable {
                            observations.push(FacebookObservation::temporarily_unavailable(
                                raw.raw_id,
                                &target.target_id,
                            ));
                        } else {
                            let title = raw.raw_title.as_deref().map(sanitize_observation_text);
                            let body = raw.raw_body.as_deref().map(sanitize_observation_text);
                            let author = raw.raw_author.as_deref().map(sanitize_observation_text);
                            let hash =
                                compute_normalized_content_hash(title.as_deref(), body.as_deref(), author.as_deref());
                            let obs = FacebookObservation::new(raw.raw_id, &target.target_id, hash)
                                .with_content(title, body, author)
                                .with_published_at(raw.published_at_ms);
                            observations.push(obs);
                        }
                    }
                    target_results.push(TargetScanResult::success(&target.target_id, observations));
                }
                Ok(RawTargetScanOutcome::AuthExpired(reason)) => {
                    top_auth_expired = Some(reason.clone());
                    target_results.push(TargetScanResult {
                        target_id: target.target_id.clone(),
                        outcome: MonitorRunOutcome::AuthExpired,
                        observations: Vec::new(),
                        error_message: Some(reason),
                        is_untrusted_structure: false,
                    });
                }
                Ok(RawTargetScanOutcome::CheckpointDetected(reason)) => {
                    top_auth_expired = Some(format!("checkpoint detected: {reason}"));
                    target_results.push(TargetScanResult {
                        target_id: target.target_id.clone(),
                        outcome: MonitorRunOutcome::AuthExpired,
                        observations: Vec::new(),
                        error_message: Some(format!("checkpoint detected: {reason}")),
                        is_untrusted_structure: false,
                    });
                }
                Ok(RawTargetScanOutcome::CaptchaDetected(reason)) => {
                    top_auth_expired = Some(format!("captcha challenge: {reason}"));
                    target_results.push(TargetScanResult {
                        target_id: target.target_id.clone(),
                        outcome: MonitorRunOutcome::AuthExpired,
                        observations: Vec::new(),
                        error_message: Some(format!("captcha challenge: {reason}")),
                        is_untrusted_structure: false,
                    });
                }
                Ok(RawTargetScanOutcome::UntrustedDomStructure(reason)) => {
                    target_results.push(TargetScanResult::untrusted_dom(&target.target_id, reason));
                }
                Ok(RawTargetScanOutcome::Unavailable(reason)) => {
                    target_results.push(TargetScanResult::unavailable(&target.target_id, reason));
                }
                Ok(RawTargetScanOutcome::Failed(reason)) => {
                    target_results.push(TargetScanResult::failed(&target.target_id, reason));
                }
                Err(err_msg) => {
                    target_results.push(TargetScanResult::failed(&target.target_id, err_msg));
                }
            }
        }

        // Bounded lifecycle: always close the browser session upon run completion
        if let Err(close_err) = session.close().await {
            tracing::warn!("Failed to cleanly close browser session: {close_err}");
        }

        if let Some(auth_err) = top_auth_expired {
            return Ok(MonitorScanResult {
                outcome: MonitorRunOutcome::AuthExpired,
                observations_count: 0,
                error_message: Some(auth_err),
                observations: Vec::new(),
                target_results,
            });
        }

        Ok(MonitorScanResult::multi_target(target_results))
    }
}
