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
        id: "self-host-workspace-preview".into(),
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

fn install(context: &mut AgentSessionContext, server: SessionMcpServer) {
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

    #[test]
    fn installs_preview_in_all_factory_contexts_even_with_empty_user_selection() {
        let kinds = vec![
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
        ];
        let server = SessionMcpServer {
            id: "preview".into(),
            name: NAME.into(),
            transport: SessionMcpTransport::Stdio {
                command: "/node".into(),
                args: vec!["/bridge.mjs".into()],
                env: HashMap::new(),
            },
        };
        for kind in kinds {
            let mut context = context(kind);
            install(&mut context, server.clone());
            install(&mut context, server.clone());
            let (servers, prompt) = match &context.kind {
                AgentSessionKind::Acp(build) => (&build.config.session_mcp_servers, &build.config.preset_context),
                AgentSessionKind::Antigravity(build) => {
                    (&build.config.session_mcp_servers, &build.config.preset_context)
                }
                AgentSessionKind::Aionrs(build) => (&build.config.session_mcp_servers, &build.config.preset_rules),
            };
            assert_eq!(servers, &vec![server.clone()]);
            assert!(prompt.as_ref().unwrap().contains("preview_create"));
            assert!(prompt.as_ref().unwrap().contains("clickable link"));
        }
    }
}
