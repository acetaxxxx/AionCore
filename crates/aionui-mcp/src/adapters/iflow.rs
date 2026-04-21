use std::collections::HashMap;

use aionui_common::McpSource;

use crate::adapter::{DetectedServer, McpAgentAdapter};
use crate::error::McpError;
use crate::types::McpServerTransport;

use super::cli_helpers::{
    DETECT_TIMEOUT, MUTATE_TIMEOUT, build_env_args, build_header_args, is_cli_installed,
    parse_standard_list_output, run_cli,
};

const CLI_NAME: &str = "iflow";

/// Scopes tried when removing (user first, then project).
const REMOVE_SCOPES: &[&str] = &["user", "project"];

/// MCP Agent adapter for iFlow CLI.
///
/// # CLI Commands
///
/// - **detect**: `iflow mcp list`
/// - **install (stdio)**: `iflow mcp add <name> <cmd> [args...] --transport stdio [--env K=V]... [-H K:V]... -s user`
/// - **install (http/sse)**: `iflow mcp add <name> <url> --transport <type> [-H K:V]... -s user`
/// - **remove**: `iflow mcp remove <name> -s user` → `-s project`
///
/// iFlow's list output may contain Chinese status text (已连接/已断开).
pub struct IFlowAdapter;

#[async_trait::async_trait]
impl McpAgentAdapter for IFlowAdapter {
    fn source(&self) -> McpSource {
        McpSource::IFlow
    }

    async fn is_installed(&self) -> Result<bool, McpError> {
        is_cli_installed(CLI_NAME).await
    }

    async fn detect_existing(&self) -> Result<Vec<DetectedServer>, McpError> {
        if !self.is_installed().await? {
            return Err(McpError::AgentNotInstalled(CLI_NAME.into()));
        }

        let (stdout, _stderr) = run_cli(CLI_NAME, &["mcp", "list"], DETECT_TIMEOUT).await?;
        Ok(parse_standard_list_output(&stdout))
    }

    async fn install_server(
        &self,
        name: &str,
        transport: &McpServerTransport,
    ) -> Result<(), McpError> {
        if !self.is_installed().await? {
            return Err(McpError::AgentNotInstalled(CLI_NAME.into()));
        }

        match transport {
            McpServerTransport::Stdio { command, args, env } => {
                let mut cli_args = vec![
                    "mcp".to_owned(),
                    "add".to_owned(),
                    name.to_owned(),
                    command.clone(),
                ];
                cli_args.extend(args.iter().cloned());
                cli_args.push("--transport".to_owned());
                cli_args.push("stdio".to_owned());
                cli_args.extend(build_env_args(env, "--env"));
                cli_args.push("-s".to_owned());
                cli_args.push("user".to_owned());

                let arg_refs: Vec<&str> = cli_args.iter().map(|s| s.as_str()).collect();
                run_cli(CLI_NAME, &arg_refs, MUTATE_TIMEOUT).await?;
            }
            McpServerTransport::Sse { url, headers } => {
                install_http_like(name, "sse", url, headers).await?;
            }
            McpServerTransport::Http { url, headers } => {
                install_http_like(name, "http", url, headers).await?;
            }
        }

        Ok(())
    }

    async fn remove_server(&self, name: &str) -> Result<(), McpError> {
        if !self.is_installed().await? {
            return Err(McpError::AgentNotInstalled(CLI_NAME.into()));
        }

        for scope in REMOVE_SCOPES {
            let (stdout, _stderr) = run_cli(
                CLI_NAME,
                &["mcp", "remove", name, "-s", scope],
                MUTATE_TIMEOUT,
            )
            .await?;
            let lower = stdout.to_lowercase();
            if lower.contains("removed")
                || lower.contains("not found")
                || lower.contains("does not exist")
            {
                return Ok(());
            }
        }

        Ok(())
    }
}

/// Install an HTTP-like (sse/http) server via `iflow mcp add`.
async fn install_http_like(
    name: &str,
    transport_type: &str,
    url: &str,
    headers: &HashMap<String, String>,
) -> Result<(), McpError> {
    let mut cli_args = vec![
        "mcp".to_owned(),
        "add".to_owned(),
        name.to_owned(),
        url.to_owned(),
        "--transport".to_owned(),
        transport_type.to_owned(),
    ];
    // iFlow uses -H flag for headers
    cli_args.extend(build_header_args(headers, "-H"));
    cli_args.push("-s".to_owned());
    cli_args.push("user".to_owned());

    let arg_refs: Vec<&str> = cli_args.iter().map(|s| s.as_str()).collect();
    run_cli(CLI_NAME, &arg_refs, MUTATE_TIMEOUT).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_is_iflow() {
        assert_eq!(IFlowAdapter.source(), McpSource::IFlow);
    }

    #[test]
    fn parse_iflow_chinese_status() {
        let output = "\
✓ my-server: npx -y @test/server (stdio) - 已连接
✗ broken: node bad.js (stdio) - 已断开";

        let servers = parse_standard_list_output(output);
        assert_eq!(servers.len(), 2);
        assert_eq!(servers[0].name, "my-server");
        assert_eq!(servers[1].name, "broken");
    }

    #[test]
    fn parse_iflow_mixed_status() {
        let output = "\
✓ srv-a: npx test (stdio) - Connected
✗ srv-b: node bad.js (stdio) - 已断开
✓ srv-c: https://example.com/mcp (http) - 已连接";

        let servers = parse_standard_list_output(output);
        assert_eq!(servers.len(), 3);
    }

    #[test]
    fn parse_iflow_sse_server() {
        let output = "✓ sse-srv: https://example.com/sse (sse) - Connected";
        let servers = parse_standard_list_output(output);
        assert_eq!(servers.len(), 1);
        match &servers[0].transport {
            McpServerTransport::Sse { url, .. } => {
                assert_eq!(url, "https://example.com/sse");
            }
            _ => panic!("expected Sse"),
        }
    }

    #[test]
    fn trait_is_object_safe() {
        let adapter: Box<dyn McpAgentAdapter> = Box::new(IFlowAdapter);
        assert_eq!(adapter.source(), McpSource::IFlow);
    }
}
