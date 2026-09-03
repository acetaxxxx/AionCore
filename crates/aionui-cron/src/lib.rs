#![warn(clippy::disallowed_types)]

//! Scheduled job engine: cron scheduler, executor, and lifecycle event emitter.
mod artifacts;
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

pub use events::CronEventEmitter;
pub use facebook_adapter::{
    compute_normalized_content_hash, sanitize_observation_text, FacebookBrowserCapabilityAdapter,
    IFacebookBrowserDriver, IFacebookBrowserSession, RawPostData, RawTargetScanOutcome,
};
pub use liveview_transport::{
    hash_session_token, ClientRelayMessage, FailClosedLiveViewTransportAdapter, GatewayRelayMessage,
    ILiveViewTransportAdapter, ISidecarScreencastDriver, LiveViewCapability, LiveViewScreencastRelayGateway,
    LiveViewSessionManager, LiveViewSessionScope, LiveViewSessionStatus, LiveViewTransportError,
    LiveViewTransportSession, MouseButton, ScreencastFormat, ScreencastFrame, StartLiveViewSessionRequest,
    StartLiveViewSessionResponse, UserKeyboardEvent, UserPointerEvent, MAX_FRAME_HEIGHT, MAX_FRAME_WIDTH,
    MAX_INPUTS_PER_SECOND, MAX_SCENARIOCAST_FRAME_BYTES,
};
pub use monitor::{
    propose_default_schedule, validate_schedule, CreateMonitorJobOutcome, CreateMonitorJobRequest, CursorItemState,
    FacebookObservation, FacebookProfile, FacebookTarget, IMonitorJobRepository, InMemoryMonitorJobRepository,
    LookbackScope, MonitorControlService, MonitorCursor, MonitorError, MonitorJob, MonitorJobProposal, MonitorJobStatus,
    MonitorQuery, MonitorRunOutcome, MonitorRunReport, MonitorRunner, MonitorScanResult, MonitorStopReason,
    ObservationDeltaKind, ProfileAuthState, ReportedObservation, TargetFailure, TargetScanResult,
};

pub use routes::cron_routes;
pub use state::CronRouterState;
