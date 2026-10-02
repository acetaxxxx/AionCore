-- A Team may explicitly allow selected owner-configured MCPs. No rows means
-- no MCP sharing; personal assistant selection is never implicitly inherited.
CREATE TABLE team_mcp_allowlist (
    team_id TEXT NOT NULL REFERENCES teams(id) ON DELETE CASCADE,
    mcp_server_id TEXT NOT NULL REFERENCES mcp_servers(id) ON DELETE CASCADE,
    PRIMARY KEY (team_id, mcp_server_id)
);
