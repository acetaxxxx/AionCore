#![warn(clippy::disallowed_types)]

//! Scheduled job engine: cron scheduler, executor, and lifecycle event emitter.
mod artifacts;
pub mod browser_session;
pub mod error;
pub mod events;
pub mod executor;
pub mod facebook_adapter;
pub mod liveview_transport;
pub mod monitor;
pub mod prompt;
pub mod routes;
pub mod scheduler;
pub mod service;
pub mod skill_file;
pub mod skill_suggest;
pub mod state;
pub mod types;

pub use browser_session::{
    ActionConfirmation, BROWSER_ABSOLUTE_LEASE_MS, BROWSER_IDLE_LEASE_MS, BrowserAction, BrowserCapability,
    BrowserCapabilityEnvelope, BrowserCapabilityKeyProvider, BrowserCapabilityScope, BrowserCapabilityVerifier,
    BrowserInput, BrowserLease, BrowserLeaseStatus, BrowserPrivateRelay, BrowserRelayFrame, BrowserScopeAuthorizer,
    BrowserSessionControlPlane, BrowserSessionError, BrowserSessionStartOutcome, BrowserSessionStartRequest,
    FailClosedBrowserScopeAuthorizer, IBrowserOriginPolicy, IBrowserSessionAdapter, StrictBrowserOriginPolicy,
    UnavailableBrowserSessionAdapter,
};
pub use events::CronEventEmitter;
pub use facebook_adapter::{
    FacebookBrowserCapabilityAdapter, IFacebookBrowserDriver, IFacebookBrowserSession, RawPostData,
    RawTargetScanOutcome, compute_normalized_content_hash, sanitize_observation_text,
};
pub use liveview_transport::{
    ClientRelayMessage, FailClosedLiveViewTransportAdapter, GatewayRelayMessage, ILiveViewTransportAdapter,
    ISidecarScreencastDriver, LiveViewCapability, LiveViewScreencastRelayGateway, LiveViewSessionManager,
    LiveViewSessionScope, LiveViewSessionStatus, LiveViewTransportError, LiveViewTransportSession, MAX_FRAME_HEIGHT,
    MAX_FRAME_WIDTH, MAX_INPUTS_PER_SECOND, MAX_SCENARIOCAST_FRAME_BYTES, MouseButton, ScreencastFormat,
    ScreencastFrame, StartLiveViewSessionRequest, StartLiveViewSessionResponse, UserKeyboardEvent, UserPointerEvent,
    hash_session_token,
};
pub use monitor::{
    CreateMonitorJobOutcome, CreateMonitorJobRequest, CursorItemState, FacebookObservation, FacebookProfile,
    FacebookTarget, IMonitorJobRepository, InMemoryMonitorJobRepository, LookbackScope, MonitorControlService,
    MonitorCursor, MonitorError, MonitorJob, MonitorJobProposal, MonitorJobStatus, MonitorQuery, MonitorRunOutcome,
    MonitorRunReport, MonitorRunner, MonitorScanResult, MonitorStopReason, ObservationDeltaKind, ProfileAuthState,
    ReportedObservation, TargetFailure, TargetScanResult, propose_default_schedule, validate_schedule,
};

pub use routes::cron_routes;
pub use state::{BrowserRouterState, CronRouterState};
