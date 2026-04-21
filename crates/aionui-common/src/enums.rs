use serde::{Deserialize, Serialize};

/// Type of AI agent backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum AgentType {
    Gemini,
    Acp,
    OpenclawGateway,
    Nanobot,
    Remote,
    Aionrs,
}

/// ACP sub-backend identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum AcpBackend {
    Claude,
    Gemini,
    Qwen,
    #[serde(rename = "iFlow")]
    IFlow,
    Codex,
    CodeBuddy,
    Droid,
    Goose,
    Auggie,
    Kimi,
    OpenCode,
    Copilot,
    Qoder,
    OpenclawGateway,
    Vibe,
    Nanobot,
    Cursor,
    Kiro,
    Remote,
    Aionrs,
    Custom,
}

/// Runtime status of a conversation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ConversationStatus {
    Pending,
    Running,
    Finished,
}

/// Origin of a conversation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ConversationSource {
    Aionui,
    Telegram,
    Lark,
    Dingtalk,
    Weixin,
}

/// Type discriminant for messages in a conversation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum MessageType {
    Text,
    Tips,
    ToolCall,
    ToolGroup,
    AgentStatus,
    AcpPermission,
    AcpToolCall,
    CodexPermission,
    CodexToolCall,
    Plan,
    Thinking,
    AvailableCommands,
    SkillSuggest,
    CronTrigger,
}

/// Display position of a message in the chat UI.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum MessagePosition {
    Right,
    Left,
    Center,
    Pop,
}

/// Processing status of a message.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum MessageStatus {
    Finish,
    Pending,
    Error,
    Work,
}

/// LLM API protocol type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ProtocolType {
    #[serde(rename = "openai")]
    OpenAI,
    Anthropic,
    Gemini,
    Unknown,
}

/// Remote Agent protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum RemoteAgentProtocol {
    OpenClaw,
    ZeroClaw,
    Acp,
}

/// Remote Agent authentication method.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum RemoteAgentAuthType {
    Bearer,
    Password,
    None,
}

/// Remote Agent connection status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum RemoteAgentStatus {
    Unknown,
    Connected,
    Pending,
    Error,
}

/// Reason for terminating an Agent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum AgentKillReason {
    IdleTimeout,
}

/// Preview content type for document preview history.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum PreviewContentType {
    Markdown,
    Diff,
    Code,
    Html,
    Pdf,
    Ppt,
    Word,
    Excel,
    Image,
    Url,
}

/// File change operation type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum FileChangeOperation {
    Create,
    Modify,
    Delete,
}

/// AI Agent CLI source identifier for MCP configuration sync.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum McpSource {
    Claude,
    Gemini,
    Qwen,
    #[serde(rename = "iflow")]
    IFlow,
    Codex,
    #[serde(rename = "codebuddy")]
    CodeBuddy,
    #[serde(rename = "opencode")]
    OpenCode,
    Aionrs,
    Nanobot,
    Aionui,
}

/// MCP server connection status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum McpServerStatus {
    Connected,
    Disconnected,
    Error,
    Testing,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_agent_type_serde_roundtrip() {
        let val = AgentType::OpenclawGateway;
        let json = serde_json::to_string(&val).unwrap();
        assert_eq!(json, r#""openclawGateway""#);
        let parsed: AgentType = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, val);
    }

    #[test]
    fn test_acp_backend_iflow() {
        let val = AcpBackend::IFlow;
        let json = serde_json::to_string(&val).unwrap();
        assert_eq!(json, r#""iFlow""#);
    }

    #[test]
    fn test_protocol_type_openai() {
        let val = ProtocolType::OpenAI;
        let json = serde_json::to_string(&val).unwrap();
        assert_eq!(json, r#""openai""#);
        let parsed: ProtocolType = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, ProtocolType::OpenAI);
    }

    #[test]
    fn test_conversation_status_lowercase() {
        let val = ConversationStatus::Pending;
        let json = serde_json::to_string(&val).unwrap();
        assert_eq!(json, r#""pending""#);
    }

    #[test]
    fn test_message_type_camel_case() {
        let val = MessageType::ToolCall;
        let json = serde_json::to_string(&val).unwrap();
        assert_eq!(json, r#""toolCall""#);
    }

    #[test]
    fn test_file_change_operation_roundtrip() {
        for op in [
            FileChangeOperation::Create,
            FileChangeOperation::Modify,
            FileChangeOperation::Delete,
        ] {
            let json = serde_json::to_string(&op).unwrap();
            let parsed: FileChangeOperation = serde_json::from_str(&json).unwrap();
            assert_eq!(parsed, op);
        }
    }

    #[test]
    fn test_mcp_source_serde_roundtrip() {
        let cases = [
            (McpSource::Claude, r#""claude""#),
            (McpSource::Gemini, r#""gemini""#),
            (McpSource::Qwen, r#""qwen""#),
            (McpSource::IFlow, r#""iflow""#),
            (McpSource::Codex, r#""codex""#),
            (McpSource::CodeBuddy, r#""codebuddy""#),
            (McpSource::OpenCode, r#""opencode""#),
            (McpSource::Aionrs, r#""aionrs""#),
            (McpSource::Nanobot, r#""nanobot""#),
            (McpSource::Aionui, r#""aionui""#),
        ];
        for (variant, expected_json) in cases {
            let json = serde_json::to_string(&variant).unwrap();
            assert_eq!(json, expected_json, "serialize {variant:?}");
            let parsed: McpSource = serde_json::from_str(&json).unwrap();
            assert_eq!(parsed, variant, "deserialize {expected_json}");
        }
    }

    #[test]
    fn test_mcp_server_status_serde_roundtrip() {
        let cases = [
            (McpServerStatus::Connected, r#""connected""#),
            (McpServerStatus::Disconnected, r#""disconnected""#),
            (McpServerStatus::Error, r#""error""#),
            (McpServerStatus::Testing, r#""testing""#),
        ];
        for (variant, expected_json) in cases {
            let json = serde_json::to_string(&variant).unwrap();
            assert_eq!(json, expected_json, "serialize {variant:?}");
            let parsed: McpServerStatus = serde_json::from_str(&json).unwrap();
            assert_eq!(parsed, variant, "deserialize {expected_json}");
        }
    }
}
