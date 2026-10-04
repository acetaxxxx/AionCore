//! Self-host preview MCP delivery before dispatch to any concrete backend.

use std::collections::HashMap;
use std::path::Path;

use aionui_api_types::{SessionMcpServer, SessionMcpTransport};
use aionui_runtime::ensure_runtime_command;

use crate::error::AgentError;
use crate::session_context::{AgentSessionContext, AgentSessionKind};

const NAME: &str = "workspace-preview";
const INSTRUCTIONS: &str = "[Workspace Preview]\nWhen creating a browser-facing HTML artifact, keep index.html and relative assets in a project directory inside the current workspace. Use workspace-preview preview_create to start a preview, pass the project path, and return its URL as a clickable link to the user. You may register an existing directory before index.html is ready; the page waits and updates automatically when the files appear. Continue editing that same directory; no upload, deploy, or per-edit registration is needed. Use preview_get/list to reuse previews. Never invent a preview URL or claim success after a tool error. Do not publish secrets. Preview visibility is shared with the configured Access audience. Conversation binding is provided by the runtime.";

pub(super) async fn configure_from_env(context: &mut AgentSessionContext) -> Result<(), AgentError> {
    let Ok(bridge) = std::env::var("AIONUI_WORKSPACE_PREVIEW_BRIDGE") else {
        return Ok(());
    };
    if bridge.trim().is_empty() {
        return Ok(());
    }
    let Some(server_id) = authorized_preview_id(context).map(str::to_owned) else {
        tracing::info!(
            conversation_id = %context.conversation.conversation_id,
            "workspace preview MCP skipped: no authorized Team preview selection"
        );
        return Ok(());
    };
    let token = std::env::var("GATEWAY_MCP_TOKEN").unwrap_or_default();
    if !Path::new(&bridge).is_absolute() || token.trim().len() < 32 {
        return Err(AgentError::bad_gateway(
            "workspace preview runtime is not configured correctly",
        ));
    }
    let launch = ensure_runtime_command("node")
        .await
        .map_err(|_| AgentError::bad_gateway("workspace preview Node runtime is unavailable"))?;
    let mut args: Vec<String> = launch
        .args_prefix
        .iter()
        .map(|arg| arg.to_string_lossy().into_owned())
        .collect();
    args.push(bridge);
    let mut env: HashMap<String, String> = launch
        .env
        .into_iter()
        .map(|(k, v)| (k.to_string_lossy().into_owned(), v.to_string_lossy().into_owned()))
        .collect();
    env.insert("GATEWAY_MCP_TOKEN".into(), token);
    env.insert(
        "GATEWAY_MCP_URL".into(),
        std::env::var("GATEWAY_MCP_URL").unwrap_or_else(|_| "http://workspace-gateway:3000/mcp".into()),
    );
    env.insert(
        "AIONUI_CONVERSATION_ID".into(),
        context.conversation.conversation_id.clone(),
    );
    env.insert("AIONUI_USER_ID".into(), context.conversation.user_id.clone());
    if let Some(team) = context.team.as_ref() {
        env.insert("AIONUI_PREVIEW_TEAM_ID".into(), team.team_id.clone());
    }
    let server = SessionMcpServer {
        id: server_id,
        name: NAME.into(),
        transport: SessionMcpTransport::Stdio {
            command: launch.program.to_string_lossy().into_owned(),
            args,
            env,
        },
    };
    install(context, server);
    tracing::info!(conversation_id = %context.conversation.conversation_id, "workspace preview MCP configured for Agent session");
    Ok(())
}

fn append_instructions(prompt: &mut Option<String>) {
    if prompt.as_ref().is_some_and(|text| text.contains("[Workspace Preview]")) {
        return;
    }
    *prompt = Some(match prompt.take() {
        Some(existing) => format!("{existing}\n\n{INSTRUCTIONS}"),
        None => INSTRUCTIONS.to_owned(),
    });
}

fn install(context: &mut AgentSessionContext, mut server: SessionMcpServer) {
    let Some(server_id) = authorized_preview_id(context).map(str::to_owned) else {
        return;
    };
    server.id = server_id;
    match &mut context.kind {
        AgentSessionKind::Acp(build) => {
            build.config.session_mcp_servers.retain(|item| item.name != NAME);
            build.config.session_mcp_servers.push(server);
            append_instructions(&mut build.config.preset_context);
        }
        AgentSessionKind::Antigravity(build) => {
            build.config.session_mcp_servers.retain(|item| item.name != NAME);
            build.config.session_mcp_servers.push(server);
            append_instructions(&mut build.config.preset_context);
        }
        AgentSessionKind::Aionrs(build) => {
            build.config.session_mcp_servers.retain(|item| item.name != NAME);
            build.config.session_mcp_servers.push(server);
            append_instructions(&mut build.config.preset_rules);
        }
    }
}

// Team provisioning already intersects persisted Owner selections with the
// Team allowlist. Never add a new capability after that policy boundary.
fn authorized_preview_id(context: &AgentSessionContext) -> Option<&str> {
    let (belongs_to_team, selected, servers) = match &context.kind {
        AgentSessionKind::Acp(build) => (
            build.belongs_to_team || build.team.is_some(),
            &build.config.mcp_server_ids,
            &build.config.session_mcp_servers,
        ),
        AgentSessionKind::Antigravity(build) => (
            build.belongs_to_team || build.team.is_some(),
            &build.config.mcp_server_ids,
            &build.config.session_mcp_servers,
        ),
        AgentSessionKind::Aionrs(build) => (
            build.belongs_to_team || build.team.is_some(),
            &build.config.mcp_server_ids,
            &build.config.session_mcp_servers,
        ),
    };
    if context.team.is_none() && !belongs_to_team {
        return Some("self-host-workspace-preview");
    }
    servers
        .iter()
        .find(|server| server.name == NAME && selected.as_ref().is_some_and(|ids| ids.contains(&server.id)))
        .map(|server| server.id.as_str())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session_context::{
        AcpSessionBuildContext, AionrsSessionBuildContext, AntigravitySessionBuildContext, ConversationContext,
        WorkspaceContext,
    };
    use aionui_common::{AgentType, ProviderWithModel};

    fn context(kind: AgentSessionKind) -> AgentSessionContext {
        AgentSessionContext {
            conversation: ConversationContext {
                conversation_id: "conv".into(),
                user_id: "user".into(),
                agent_type: AgentType::Acp,
                source: None,
            },
            workspace: WorkspaceContext {
                path: "/data/project".into(),
                stored_path: "/data/project".into(),
                is_custom: false,
            },
            model: ProviderWithModel {
                provider_id: "provider".into(),
                model: "model".into(),
                use_model: None,
            },
            skills: vec![],
            runtime_env: vec![],
            team: None,
            kind,
        }
    }

    fn kinds() -> Vec<AgentSessionKind> {
        vec![
            AgentSessionKind::Acp(Box::new(AcpSessionBuildContext {
                config: Default::default(),
                team: None,
                belongs_to_team: false,
                session_id: None,
                session_snapshot: None,
            })),
            AgentSessionKind::Antigravity(Box::new(AntigravitySessionBuildContext {
                config: Default::default(),
                team: None,
                belongs_to_team: false,
                session_id: None,
                session_snapshot: None,
            })),
            AgentSessionKind::Aionrs(Box::new(AionrsSessionBuildContext {
                config: Default::default(),
                team: None,
                belongs_to_team: false,
            })),
        ]
    }

    fn server(id: &str) -> SessionMcpServer {
        SessionMcpServer {
            id: id.into(),
            name: NAME.into(),
            transport: SessionMcpTransport::Stdio {
                command: "/node".into(),
                args: vec!["/bridge.mjs".into()],
                env: HashMap::new(),
            },
        }
    }

    fn config_mut(
        context: &mut AgentSessionContext,
    ) -> (&mut Option<Vec<String>>, &mut Vec<SessionMcpServer>, &mut Option<String>) {
        match &mut context.kind {
            AgentSessionKind::Acp(build) => (
                &mut build.config.mcp_server_ids,
                &mut build.config.session_mcp_servers,
                &mut build.config.preset_context,
            ),
            AgentSessionKind::Antigravity(build) => (
                &mut build.config.mcp_server_ids,
                &mut build.config.session_mcp_servers,
                &mut build.config.preset_context,
            ),
            AgentSessionKind::Aionrs(build) => (
                &mut build.config.mcp_server_ids,
                &mut build.config.session_mcp_servers,
                &mut build.config.preset_rules,
            ),
        }
    }

    fn mark_team(context: &mut AgentSessionContext, marker: usize) {
        let binding = aionui_api_types::TeamSessionBinding::from_extra_value(&serde_json::json!({
            "teamId": "shared-team",
        }))
        .unwrap()
        .unwrap();
        match marker {
            0 => match &mut context.kind {
                AgentSessionKind::Acp(build) => build.belongs_to_team = true,
                AgentSessionKind::Antigravity(build) => build.belongs_to_team = true,
                AgentSessionKind::Aionrs(build) => build.belongs_to_team = true,
            },
            1 => context.team = Some(binding),
            2 => match &mut context.kind {
                AgentSessionKind::Acp(build) => build.team = Some(binding),
                AgentSessionKind::Antigravity(build) => build.team = Some(binding),
                AgentSessionKind::Aionrs(build) => build.team = Some(binding),
            },
            _ => unreachable!(),
        }
    }

    #[test]
    fn personal_preview_is_automatic_for_all_backends_and_selection_states() {
        for kind in kinds() {
            for selection in [None, Some(vec![]), Some(vec!["other".into()])] {
                let mut context = context(kind.clone());
                let (ids, servers, prompt) = config_mut(&mut context);
                *ids = selection.clone();
                let mut unrelated = server("other");
                unrelated.name = "docs".into();
                servers.push(unrelated.clone());
                *prompt = Some("Existing rules".into());

                install(&mut context, server("self-host-workspace-preview"));
                install(&mut context, server("self-host-workspace-preview"));

                let (ids, servers, prompt) = config_mut(&mut context);
                assert_eq!(*ids, selection);
                assert_eq!(*servers, vec![unrelated, server("self-host-workspace-preview")]);
                let prompt = prompt.as_ref().unwrap();
                assert!(prompt.starts_with("Existing rules\n\n"));
                assert!(prompt.contains("preview_create"));
                assert!(prompt.contains("clickable link"));
                assert_eq!(prompt.matches("[Workspace Preview]").count(), 1);
            }
        }
    }

    #[test]
    fn team_preview_requires_selected_resolved_row_for_every_backend_and_team_marker() {
        let registered = server("owner-imported-row-42");
        let mut other = server("other");
        other.name = "docs".into();
        let denied = [
            (None, vec![]),
            (Some(vec![]), vec![]),
            (Some(vec![registered.id.clone()]), vec![]),
            (Some(vec!["self-host-workspace-preview".into()]), vec![]),
            (None, vec![registered.clone()]),
            (Some(vec![]), vec![registered.clone()]),
            (Some(vec![other.id.clone()]), vec![registered.clone()]),
            (Some(vec![other.id.clone()]), vec![other]),
        ];
        for kind in kinds() {
            for marker in 0..3 {
                for (selection, resolved) in &denied {
                    let mut context = context(kind.clone());
                    mark_team(&mut context, marker);
                    let (ids, servers, prompt) = config_mut(&mut context);
                    *ids = selection.clone();
                    *servers = resolved.clone();
                    *prompt = Some("Existing rules".into());

                    install(&mut context, server("self-host-workspace-preview"));

                    let (ids, servers, prompt) = config_mut(&mut context);
                    assert_eq!(ids, selection);
                    assert_eq!(servers, resolved, "denied Team must not receive the runtime bridge");
                    assert_eq!(prompt.as_deref(), Some("Existing rules"));
                }
            }
        }
    }

    #[test]
    fn authorized_team_preview_replaces_registered_transport_and_preserves_persisted_id() {
        for kind in kinds() {
            for marker in 0..3 {
                let mut context = context(kind.clone());
                mark_team(&mut context, marker);
                let mut registered = server("owner-imported-row-42");
                registered.transport = SessionMcpTransport::Stdio {
                    command: "/registered-placeholder".into(),
                    args: vec![],
                    env: HashMap::new(),
                };
                let mut unrelated = server("other");
                unrelated.name = "docs".into();
                let (ids, servers, _) = config_mut(&mut context);
                *ids = Some(vec![registered.id.clone()]);
                *servers = vec![server("stale-preview-row"), registered, unrelated.clone()];

                install(&mut context, server("self-host-workspace-preview"));
                install(&mut context, server("self-host-workspace-preview"));

                let (ids, servers, prompt) = config_mut(&mut context);
                assert_eq!(*ids, Some(vec!["owner-imported-row-42".into()]));
                assert_eq!(*servers, vec![unrelated, server("owner-imported-row-42")]);
                let prompt = prompt.as_ref().unwrap();
                assert!(prompt.contains("preview_create"));
                assert_eq!(prompt.matches("[Workspace Preview]").count(), 1);
            }
        }
    }
}
