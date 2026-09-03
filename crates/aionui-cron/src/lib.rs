#![warn(clippy::disallowed_types)]

//! Scheduled job engine: cron scheduler, executor, and lifecycle event emitter.
mod artifacts;
pub mod error;
pub mod events;
pub mod executor;
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
pub use monitor::{
    CreateMonitorJobOutcome, CreateMonitorJobRequest, FacebookTarget, IMonitorJobRepository,
    InMemoryMonitorJobRepository, LookbackScope, MonitorControlService, MonitorError, MonitorJob,
    MonitorJobProposal, MonitorJobStatus, MonitorQuery, MonitorRunOutcome, MonitorRunner,
    MonitorScanResult, MonitorStopReason, propose_default_schedule, validate_schedule,
};

pub use routes::cron_routes;
pub use state::CronRouterState;

