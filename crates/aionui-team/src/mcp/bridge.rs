use std::collections::HashMap;

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// TeamMcpStdioConfig
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TeamMcpStdioConfig {
    pub port: u16,
    pub token: String,
    pub slot_id: String,
}

impl TeamMcpStdioConfig {
    pub fn new(port: u16, token: String, slot_id: String) -> Self {
        Self {
            port,
            token,
            slot_id,
        }
    }

    pub fn to_env_map(&self) -> HashMap<String, String> {
        HashMap::from([
            ("TEAM_MCP_PORT".into(), self.port.to_string()),
            ("TEAM_MCP_TOKEN".into(), self.token.clone()),
            ("TEAM_AGENT_SLOT_ID".into(), self.slot_id.clone()),
        ])
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_creation() {
        let config = TeamMcpStdioConfig::new(12345, "tok-abc".into(), "slot-1".into());
        assert_eq!(config.port, 12345);
        assert_eq!(config.token, "tok-abc");
        assert_eq!(config.slot_id, "slot-1");
    }

    #[test]
    fn env_map_contains_all_keys() {
        let config = TeamMcpStdioConfig::new(8080, "secret-token".into(), "agent-slot".into());
        let env = config.to_env_map();
        assert_eq!(env.len(), 3);
        assert_eq!(env["TEAM_MCP_PORT"], "8080");
        assert_eq!(env["TEAM_MCP_TOKEN"], "secret-token");
        assert_eq!(env["TEAM_AGENT_SLOT_ID"], "agent-slot");
    }

    #[test]
    fn serialization_roundtrip() {
        let config = TeamMcpStdioConfig::new(9999, "tok".into(), "s1".into());
        let json = serde_json::to_string(&config).unwrap();
        let parsed: TeamMcpStdioConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(config, parsed);
    }

    #[test]
    fn different_agents_get_different_configs() {
        let cfg1 = TeamMcpStdioConfig::new(5000, "token".into(), "slot-a".into());
        let cfg2 = TeamMcpStdioConfig::new(5000, "token".into(), "slot-b".into());
        assert_ne!(cfg1, cfg2);

        let env1 = cfg1.to_env_map();
        let env2 = cfg2.to_env_map();
        assert_eq!(env1["TEAM_MCP_PORT"], env2["TEAM_MCP_PORT"]);
        assert_eq!(env1["TEAM_MCP_TOKEN"], env2["TEAM_MCP_TOKEN"]);
        assert_ne!(env1["TEAM_AGENT_SLOT_ID"], env2["TEAM_AGENT_SLOT_ID"]);
    }
}
