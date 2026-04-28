use aionui_common::{AcpBackend, AgentType};
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct AgentInfo {
    pub id: String,
    pub name: String,
    pub agent_type: AgentType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub backend: Option<AcpBackend>,
    pub available: bool,
    pub source: crate::AgentSource,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cli_path: Option<String>,
}

impl From<crate::DetectedAgent> for AgentInfo {
    fn from(a: crate::DetectedAgent) -> Self {
        Self {
            id: a.id,
            name: a.name,
            agent_type: a.agent_type,
            backend: a.backend,
            available: a.available,
            source: a.source,
            cli_path: a.command,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn agent_info_from_detected() {
        let detected = crate::DetectedAgent {
            id: "abc123".into(),
            name: "Claude".into(),
            agent_type: AgentType::Acp,
            backend: Some(AcpBackend::Claude),
            available: true,
            source: crate::AgentSource::Builtin,
            command: Some("/usr/bin/claude".into()),
            args: vec!["--experimental-acp".into()],
            env: vec![crate::EnvVar {
                name: "FOO".into(),
                value: "bar".into(),
            }],
        };
        let info = AgentInfo::from(detected);
        assert_eq!(info.id, "abc123");
        assert_eq!(info.name, "Claude");
        assert_eq!(info.agent_type, AgentType::Acp);
        assert_eq!(info.backend, Some(AcpBackend::Claude));
        assert!(info.available);
        assert_eq!(info.source, crate::AgentSource::Builtin);
        assert_eq!(info.cli_path.as_deref(), Some("/usr/bin/claude"));
    }

    #[test]
    fn agent_info_serde_acp() {
        let info = AgentInfo {
            id: "abc123".into(),
            name: "Claude".into(),
            agent_type: AgentType::Acp,
            backend: Some(AcpBackend::Claude),
            available: true,
            source: crate::AgentSource::Builtin,
            cli_path: Some("/usr/bin/claude".into()),
        };
        let json = serde_json::to_value(&info).unwrap();
        assert_eq!(json["agent_type"], "acp");
        assert_eq!(json["backend"], "claude");
    }

    #[test]
    fn agent_info_serde_non_acp() {
        let info = AgentInfo {
            id: "aionrs".into(),
            name: "Aion CLI".into(),
            agent_type: AgentType::Aionrs,
            backend: None,
            available: true,
            source: crate::AgentSource::Internal,
            cli_path: None,
        };
        let json = serde_json::to_value(&info).unwrap();
        assert_eq!(json["agent_type"], "aionrs");
        assert!(json.get("backend").is_none());
        assert_eq!(json["source"], "internal");
    }
}
