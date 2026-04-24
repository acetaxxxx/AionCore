use aionui_common::AcpBackend;
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct AgentInfo {
    pub id: String,
    pub name: String,
    pub backend: AcpBackend,
    pub available: bool,
    pub source: crate::AgentSource,
}

impl From<crate::DetectedAgent> for AgentInfo {
    fn from(a: crate::DetectedAgent) -> Self {
        Self {
            id: a.id,
            name: a.name,
            backend: a.backend,
            available: a.available,
            source: a.source,
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
            backend: AcpBackend::Claude,
            available: true,
            source: crate::AgentSource::Builtin,
            command: Some("/usr/bin/claude".into()),
            args: vec!["--experimental-acp".into()],
            env: vec![],
        };
        let info = AgentInfo::from(detected);
        assert_eq!(info.id, "abc123");
        assert_eq!(info.name, "Claude");
        assert_eq!(info.backend, AcpBackend::Claude);
        assert!(info.available);
        assert_eq!(info.source, crate::AgentSource::Builtin);
    }

    #[test]
    fn agent_info_serde() {
        let info = AgentInfo {
            id: "aionrs".into(),
            name: "Aion CLI".into(),
            backend: AcpBackend::Aionrs,
            available: true,
            source: crate::AgentSource::Internal,
        };
        let json = serde_json::to_value(&info).unwrap();
        assert_eq!(json["id"], "aionrs");
        assert_eq!(json["backend"], "aionrs");
        assert_eq!(json["source"], "internal");
    }
}
