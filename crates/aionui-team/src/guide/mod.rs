//! Team Guide module — capability descriptor, lead-facing tool arg parsing,
//! and Guide MCP server.
//!
//! The Guide MCP server is injected into single-chat agents to expose
//! `aion_create_team` / `aion_list_models` tools. Independent from the
//! per-team `TeamMcpServer`.

pub mod capability;
pub mod handlers;
pub mod server;

pub use handlers::{CreateTeamParams, parse_create_team_args};
pub use server::GuideMcpServer;
