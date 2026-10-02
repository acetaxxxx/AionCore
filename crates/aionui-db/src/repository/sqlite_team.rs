use aionui_common::now_ms;
use sqlx::SqlitePool;

use crate::error::DbError;
use crate::models::{
    EligibleTeamUserRow, MailboxMessageRow, TeamAccessRole, TeamMembershipRow, TeamRow, TeamSharingMode, TeamTaskRow,
};
use crate::repository::team::{ActivityCursor, ITeamRepository, PageDirection, UpdateTaskParams, UpdateTeamParams};

/// SQLite-backed implementation of [`ITeamRepository`].
#[derive(Clone, Debug)]
pub struct SqliteTeamRepository {
    pool: SqlitePool,
}

impl SqliteTeamRepository {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    async fn insert_team(&self, row: &TeamRow, mode: TeamSharingMode) -> Result<(), DbError> {
        sqlx::query(
            "INSERT INTO teams (id, user_id, name, workspace, workspace_mode, agents, lead_agent_id, session_mode, agents_version, created_at, updated_at, project_id, folder_id, sharing_mode) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&row.id)
        .bind(&row.user_id)
        .bind(&row.name)
        .bind(&row.workspace)
        .bind(&row.workspace_mode)
        .bind(&row.agents)
        .bind(&row.lead_agent_id)
        .bind(&row.session_mode)
        .bind(&row.agents_version)
        .bind(row.created_at)
        .bind(row.updated_at)
        .bind(&row.project_id)
        .bind(&row.folder_id)
        .bind(mode.as_str())
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl ITeamRepository for SqliteTeamRepository {
    // ── Team CRUD ────────────────────────────────────────────────────

    async fn create_team(&self, row: &TeamRow) -> Result<(), DbError> {
        self.insert_team(row, TeamSharingMode::Private).await
    }

    async fn create_team_with_sharing_mode(&self, row: &TeamRow, mode: TeamSharingMode) -> Result<(), DbError> {
        self.insert_team(row, mode).await
    }

    async fn get_team_sharing_mode(&self, team_id: &str) -> Result<TeamSharingMode, DbError> {
        let mode: Option<String> = sqlx::query_scalar("SELECT sharing_mode FROM teams WHERE id = ?")
            .bind(team_id)
            .fetch_optional(&self.pool)
            .await?;
        match mode.as_deref() {
            Some("private") => Ok(TeamSharingMode::Private),
            Some("shared") => Ok(TeamSharingMode::Shared),
            Some(other) => Err(DbError::Init(format!("unknown Team sharing mode: {other}"))),
            None => Err(DbError::NotFound(format!("team {team_id}"))),
        }
    }

    async fn list_team_members(&self, team_id: &str) -> Result<Vec<TeamMembershipRow>, DbError> {
        sqlx::query_as::<_, TeamMembershipRow>(
            "SELECT m.membership_ref, m.team_id, m.user_id, \
             COALESCE(m.display_name, 'Collaborator ' || substr(m.membership_ref, 1, 8)) AS display_name, \
             m.created_at \
             FROM team_memberships m \
             JOIN users u ON u.id = m.user_id AND u.status = 'active' \
             WHERE m.team_id = ? ORDER BY m.created_at, m.membership_ref",
        )
        .bind(team_id)
        .fetch_all(&self.pool)
        .await
        .map_err(Into::into)
    }

    async fn list_eligible_team_users(
        &self,
        owner_user_id: &str,
        team_id: &str,
    ) -> Result<Vec<EligibleTeamUserRow>, DbError> {
        sqlx::query_as::<_, EligibleTeamUserRow>(
            "SELECT u.id AS user_id, u.username AS display_name FROM users u \
             WHERE u.status = 'active' AND u.username IS NOT NULL AND trim(u.username) <> '' \
             AND u.id <> ? AND u.id <> 'system_default_user' \
             AND EXISTS (SELECT 1 FROM teams t WHERE t.id = ? AND t.user_id = ? AND t.sharing_mode = 'shared') \
             AND NOT EXISTS (SELECT 1 FROM team_memberships m WHERE m.team_id = ? AND m.user_id = u.id) \
             ORDER BY u.username COLLATE NOCASE, u.id",
        )
        .bind(owner_user_id)
        .bind(team_id)
        .bind(owner_user_id)
        .bind(team_id)
        .fetch_all(&self.pool)
        .await
        .map_err(Into::into)
    }

    async fn add_team_member(&self, row: &TeamMembershipRow) -> Result<(), DbError> {
        let result = sqlx::query(
            "INSERT INTO team_memberships (membership_ref, team_id, user_id, display_name, created_at) \
             SELECT ?, t.id, u.id, ?, ? FROM teams t JOIN users u ON u.id = ? \
             WHERE t.id = ? AND t.sharing_mode = 'shared' AND u.status = 'active' \
             AND u.id <> t.user_id AND u.id <> 'system_default_user' \
             AND NOT EXISTS (SELECT 1 FROM team_memberships m WHERE m.team_id = t.id AND m.user_id = u.id)",
        )
        .bind(&row.membership_ref)
        .bind(&row.display_name)
        .bind(row.created_at)
        .bind(&row.user_id)
        .bind(&row.team_id)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 0 {
            return Err(DbError::NotFound(format!(
                "active shared Team or account for {}",
                row.team_id
            )));
        }
        Ok(())
    }

    async fn remove_team_member(
        &self,
        owner_user_id: &str,
        team_id: &str,
        membership_ref: &str,
    ) -> Result<(), DbError> {
        let result = sqlx::query(
            "DELETE FROM team_memberships WHERE team_id = ? AND membership_ref = ? \
             AND EXISTS (SELECT 1 FROM teams t WHERE t.id = team_memberships.team_id AND t.user_id = ?)",
        )
        .bind(team_id)
        .bind(membership_ref)
        .bind(owner_user_id)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 0 {
            return Err(DbError::NotFound(format!("Team membership {membership_ref}")));
        }
        Ok(())
    }

    async fn list_team_mcp_allowlist(&self, owner_user_id: &str, team_id: &str) -> Result<Vec<String>, DbError> {
        let team_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM teams WHERE id = ? AND user_id = ? AND sharing_mode = 'shared'")
                .bind(team_id)
                .bind(owner_user_id)
                .fetch_one(&self.pool)
                .await?;
        if team_count == 0 {
            return Err(DbError::NotFound(format!("shared Team {team_id}")));
        }
        sqlx::query_scalar(
            "SELECT allowlist.mcp_server_id FROM team_mcp_allowlist allowlist \
             JOIN mcp_servers mcp ON mcp.id = allowlist.mcp_server_id AND mcp.user_id = ? AND mcp.deleted_at IS NULL \
             WHERE allowlist.team_id = ? ORDER BY allowlist.mcp_server_id",
        )
        .bind(owner_user_id)
        .bind(team_id)
        .fetch_all(&self.pool)
        .await
        .map_err(Into::into)
    }

    async fn replace_team_mcp_allowlist(
        &self,
        owner_user_id: &str,
        team_id: &str,
        mcp_server_ids: &[String],
    ) -> Result<(), DbError> {
        let mut tx = self.pool.begin().await?;
        let team_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM teams WHERE id = ? AND user_id = ? AND sharing_mode = 'shared'")
                .bind(team_id)
                .bind(owner_user_id)
                .fetch_one(&mut *tx)
                .await?;
        if team_count == 0 {
            return Err(DbError::NotFound(format!("shared Team {team_id}")));
        }
        for mcp_server_id in mcp_server_ids {
            let server_count: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM mcp_servers WHERE id = ? AND user_id = ? AND deleted_at IS NULL",
            )
            .bind(mcp_server_id)
            .bind(owner_user_id)
            .fetch_one(&mut *tx)
            .await?;
            if server_count == 0 {
                return Err(DbError::NotFound(format!("owner MCP server {mcp_server_id}")));
            }
        }
        sqlx::query("DELETE FROM team_mcp_allowlist WHERE team_id = ?")
            .bind(team_id)
            .execute(&mut *tx)
            .await?;
        for mcp_server_id in mcp_server_ids {
            sqlx::query("INSERT INTO team_mcp_allowlist (team_id, mcp_server_id) VALUES (?, ?)")
                .bind(team_id)
                .bind(mcp_server_id)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    async fn list_teams_by_member(&self, user_id: &str) -> Result<Vec<TeamRow>, DbError> {
        sqlx::query_as::<_, TeamRow>(
            "SELECT t.* FROM teams t JOIN team_memberships m ON m.team_id = t.id \
             JOIN users u ON u.id = m.user_id AND u.status = 'active' \
             WHERE m.user_id = ? AND t.sharing_mode = 'shared' AND t.archived_at IS NULL \
             ORDER BY t.created_at ASC",
        )
        .bind(user_id)
        .fetch_all(&self.pool)
        .await
        .map_err(Into::into)
    }

    async fn team_access_role(&self, team_id: &str, user_id: &str) -> Result<Option<TeamAccessRole>, DbError> {
        let role: Option<String> = sqlx::query_scalar(
            "SELECT CASE WHEN t.user_id = ? THEN 'owner' ELSE 'collaborator' END \
             FROM teams t JOIN users u ON u.id = ? AND u.status = 'active' \
             WHERE t.id = ? AND (t.user_id = u.id OR (t.sharing_mode = 'shared' AND EXISTS (\
                 SELECT 1 FROM team_memberships m WHERE m.team_id = t.id AND m.user_id = u.id)))",
        )
        .bind(user_id)
        .bind(user_id)
        .bind(team_id)
        .fetch_optional(&self.pool)
        .await?;
        match role.as_deref() {
            Some("owner") => Ok(Some(TeamAccessRole::Owner)),
            Some("collaborator") => Ok(Some(TeamAccessRole::Collaborator)),
            Some(other) => Err(DbError::Init(format!("unknown Team access role: {other}"))),
            None => Ok(None),
        }
    }

    async fn list_teams_for_restore(&self) -> Result<Vec<TeamRow>, DbError> {
        let rows = sqlx::query_as::<_, TeamRow>("SELECT * FROM teams ORDER BY created_at ASC")
            .fetch_all(&self.pool)
            .await?;
        Ok(rows)
    }

    async fn list_teams_by_user(&self, user_id: &str) -> Result<Vec<TeamRow>, DbError> {
        // Archived teams are hidden from the active list; they surface only in the
        // sidebar archive read model. Restore paths use the `_for_restore` variants.
        let rows = sqlx::query_as::<_, TeamRow>(
            "SELECT * FROM teams WHERE user_id = ? AND archived_at IS NULL ORDER BY created_at ASC",
        )
        .bind(user_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    async fn get_team(&self, user_id: &str, team_id: &str) -> Result<Option<TeamRow>, DbError> {
        let row = sqlx::query_as::<_, TeamRow>("SELECT * FROM teams WHERE user_id = ? AND id = ?")
            .bind(user_id)
            .bind(team_id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row)
    }

    async fn get_team_for_restore(&self, team_id: &str) -> Result<Option<TeamRow>, DbError> {
        let row = sqlx::query_as::<_, TeamRow>("SELECT * FROM teams WHERE id = ?")
            .bind(team_id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row)
    }

    async fn update_team(&self, user_id: &str, team_id: &str, params: &UpdateTeamParams) -> Result<(), DbError> {
        let mut set_clauses = Vec::new();
        if params.name.is_some() {
            set_clauses.push("name = ?");
        }
        if params.workspace.is_some() {
            set_clauses.push("workspace = ?");
        }
        if params.agents.is_some() {
            set_clauses.push("agents = ?");
        }
        if params.lead_agent_id.is_some() {
            set_clauses.push("lead_agent_id = ?");
        }
        if params.session_mode.is_some() {
            set_clauses.push("session_mode = ?");
        }
        if params.project_id.is_some() {
            set_clauses.push("project_id = ?");
        }
        if params.folder_id.is_some() {
            set_clauses.push("folder_id = ?");
        }

        if set_clauses.is_empty() {
            return Ok(());
        }

        set_clauses.push("updated_at = ?");
        let sql = format!(
            "UPDATE teams SET {} WHERE user_id = ? AND id = ?",
            set_clauses.join(", ")
        );

        let mut query = sqlx::query(&sql);
        if let Some(ref name) = params.name {
            query = query.bind(name);
        }
        if let Some(ref workspace) = params.workspace {
            query = query.bind(workspace);
        }
        if let Some(ref agents) = params.agents {
            query = query.bind(agents);
        }
        if let Some(ref lead_agent_id) = params.lead_agent_id {
            query = query.bind(lead_agent_id);
        }
        if let Some(ref session_mode) = params.session_mode {
            query = query.bind(session_mode);
        }
        if let Some(ref project_id) = params.project_id {
            query = query.bind(project_id);
        }
        if let Some(ref folder_id) = params.folder_id {
            query = query.bind(folder_id);
        }
        query = query.bind(now_ms());
        query = query.bind(user_id);
        query = query.bind(team_id);

        let result = query.execute(&self.pool).await?;
        if result.rows_affected() == 0 {
            return Err(DbError::NotFound(format!("team {team_id}")));
        }
        Ok(())
    }

    async fn delete_team(&self, user_id: &str, team_id: &str) -> Result<(), DbError> {
        let mut tx = self.pool.begin().await?;
        sqlx::query(
            "DELETE FROM team_memberships WHERE team_id = ? \
             AND EXISTS (SELECT 1 FROM teams t WHERE t.id = team_memberships.team_id AND t.user_id = ?)",
        )
        .bind(team_id)
        .bind(user_id)
        .execute(&mut *tx)
        .await?;
        let result = sqlx::query("DELETE FROM teams WHERE user_id = ? AND id = ?")
            .bind(user_id)
            .bind(team_id)
            .execute(&mut *tx)
            .await?;
        if result.rows_affected() == 0 {
            return Err(DbError::NotFound(format!("team {team_id}")));
        }
        tx.commit().await?;
        Ok(())
    }

    // ── Mailbox ──────────────────────────────────────────────────────

    async fn write_message(&self, user_id: &str, row: &MailboxMessageRow) -> Result<(), DbError> {
        let result = sqlx::query(
            "INSERT INTO mailbox \
                (id, team_id, to_agent_id, from_agent_id, actor_user_id, type, content, summary, files, read, created_at) \
             SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? \
             WHERE EXISTS (SELECT 1 FROM teams t WHERE t.id = ? AND t.user_id = ?)",
        )
        .bind(&row.id)
        .bind(&row.team_id)
        .bind(&row.to_agent_id)
        .bind(&row.from_agent_id)
        .bind(&row.actor_user_id)
        .bind(&row.msg_type)
        .bind(&row.content)
        .bind(&row.summary)
        .bind(&row.files)
        .bind(row.read)
        .bind(row.created_at)
        .bind(&row.team_id)
        .bind(user_id)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 0 {
            return Err(DbError::NotFound(format!("team {}", row.team_id)));
        }
        Ok(())
    }

    async fn read_unread_and_mark(
        &self,
        user_id: &str,
        team_id: &str,
        to_agent_id: &str,
    ) -> Result<Vec<MailboxMessageRow>, DbError> {
        // Use BEGIN IMMEDIATE for atomicity: prevents concurrent readers
        // from seeing the same unread messages.
        let mut tx = self.pool.begin().await?;

        // SQLite does not support RETURNING on UPDATE, so we use a
        // two-step approach within the same IMMEDIATE transaction.
        sqlx::query("PRAGMA read_uncommitted = false").execute(&mut *tx).await?;

        let rows = sqlx::query_as::<_, MailboxMessageRow>(
            "SELECT id, team_id, to_agent_id, from_agent_id, actor_user_id, \
                    type, content, summary, files, read, created_at \
             FROM mailbox \
             WHERE team_id = ? AND to_agent_id = ? AND read = 0 \
               AND EXISTS (SELECT 1 FROM teams t WHERE t.id = mailbox.team_id AND t.user_id = ?) \
             ORDER BY created_at ASC",
        )
        .bind(team_id)
        .bind(to_agent_id)
        .bind(user_id)
        .fetch_all(&mut *tx)
        .await?;

        if !rows.is_empty() {
            sqlx::query(
                "UPDATE mailbox SET read = 1 \
                 WHERE team_id = ? AND to_agent_id = ? AND read = 0 \
                   AND EXISTS (SELECT 1 FROM teams t WHERE t.id = mailbox.team_id AND t.user_id = ?)",
            )
            .bind(team_id)
            .bind(to_agent_id)
            .bind(user_id)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        Ok(rows)
    }

    async fn peek_unread(
        &self,
        user_id: &str,
        team_id: &str,
        to_agent_id: &str,
    ) -> Result<Vec<MailboxMessageRow>, DbError> {
        let rows = sqlx::query_as::<_, MailboxMessageRow>(
            "SELECT id, team_id, to_agent_id, from_agent_id, actor_user_id, \
                    type, content, summary, files, read, created_at \
             FROM mailbox \
             WHERE team_id = ? AND to_agent_id = ? AND read = 0 \
               AND EXISTS (SELECT 1 FROM teams t WHERE t.id = mailbox.team_id AND t.user_id = ?) \
             ORDER BY created_at ASC, id ASC",
        )
        .bind(team_id)
        .bind(to_agent_id)
        .bind(user_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    async fn peek_unread_by_ids(
        &self,
        user_id: &str,
        team_id: &str,
        to_agent_id: &str,
        ids: &[String],
    ) -> Result<Vec<MailboxMessageRow>, DbError> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut rows = Vec::new();
        for chunk in ids.chunks(500) {
            let placeholders = chunk.iter().map(|_| "?").collect::<Vec<_>>().join(",");
            let sql = format!(
                "SELECT id, team_id, to_agent_id, from_agent_id, actor_user_id, \
                        type, content, summary, files, read, created_at \
                 FROM mailbox \
                 WHERE team_id = ? AND to_agent_id = ? AND read = 0 \
                   AND id IN ({placeholders}) \
                   AND EXISTS (SELECT 1 FROM teams t WHERE t.id = mailbox.team_id AND t.user_id = ?) \
                 ORDER BY created_at ASC, id ASC"
            );
            let mut query = sqlx::query_as::<_, MailboxMessageRow>(&sql)
                .bind(team_id)
                .bind(to_agent_id);
            for id in chunk {
                query = query.bind(id);
            }
            query = query.bind(user_id);
            rows.extend(query.fetch_all(&self.pool).await?);
        }
        // Each chunk is ordered by SQL, but chunk boundaries would still interleave
        // out of order, so restore the global FIFO order the trait promises.
        rows.sort_by(|left, right| {
            left.created_at
                .cmp(&right.created_at)
                .then_with(|| left.id.cmp(&right.id))
        });
        Ok(rows)
    }

    async fn mark_read_batch(&self, user_id: &str, team_id: &str, ids: &[String]) -> Result<(), DbError> {
        if ids.is_empty() {
            return Ok(());
        }
        // SQLite placeholder limit is 999; batch if needed.
        for chunk in ids.chunks(500) {
            let placeholders: String = chunk.iter().map(|_| "?").collect::<Vec<_>>().join(",");
            let sql = format!(
                "UPDATE mailbox SET read = 1 \
                 WHERE team_id = ? AND id IN ({placeholders}) \
                   AND EXISTS (SELECT 1 FROM teams t WHERE t.id = mailbox.team_id AND t.user_id = ?)"
            );
            let mut query = sqlx::query(&sql);
            query = query.bind(team_id);
            for id in chunk {
                query = query.bind(id);
            }
            query = query.bind(user_id);
            query.execute(&self.pool).await?;
        }
        Ok(())
    }

    async fn get_history(
        &self,
        user_id: &str,
        team_id: &str,
        to_agent_id: &str,
        limit: Option<i64>,
    ) -> Result<Vec<MailboxMessageRow>, DbError> {
        let rows = if let Some(limit) = limit {
            sqlx::query_as::<_, MailboxMessageRow>(
                "SELECT id, team_id, to_agent_id, from_agent_id, actor_user_id, \
                        type, content, summary, files, read, created_at \
                 FROM mailbox \
                 WHERE team_id = ? AND to_agent_id = ? \
                   AND EXISTS (SELECT 1 FROM teams t WHERE t.id = mailbox.team_id AND t.user_id = ?) \
                 ORDER BY created_at ASC \
                 LIMIT ?",
            )
            .bind(team_id)
            .bind(to_agent_id)
            .bind(user_id)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?
        } else {
            sqlx::query_as::<_, MailboxMessageRow>(
                "SELECT id, team_id, to_agent_id, from_agent_id, actor_user_id, \
                        type, content, summary, files, read, created_at \
                 FROM mailbox \
                 WHERE team_id = ? AND to_agent_id = ? \
                   AND EXISTS (SELECT 1 FROM teams t WHERE t.id = mailbox.team_id AND t.user_id = ?) \
                 ORDER BY created_at ASC",
            )
            .bind(team_id)
            .bind(to_agent_id)
            .bind(user_id)
            .fetch_all(&self.pool)
            .await?
        };
        Ok(rows)
    }

    async fn list_messages_by_team(&self, team_id: &str, limit: i64) -> Result<Vec<MailboxMessageRow>, DbError> {
        let rows = sqlx::query_as::<_, MailboxMessageRow>(
            "SELECT id, team_id, to_agent_id, from_agent_id, actor_user_id, \
                    type, content, summary, files, read, created_at \
             FROM mailbox \
             WHERE team_id = ? \
             ORDER BY created_at DESC \
             LIMIT ?",
        )
        .bind(team_id)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    async fn list_messages_by_team_paged(
        &self,
        team_id: &str,
        cursor: Option<ActivityCursor>,
        direction: PageDirection,
        limit: i64,
    ) -> Result<Vec<MailboxMessageRow>, DbError> {
        let (cmp, order) = match direction {
            PageDirection::Desc => ("<", "DESC"),
            PageDirection::Asc => (">", "ASC"),
        };
        let cursor_clause = if cursor.is_some() {
            format!("AND (created_at {cmp} ? OR (created_at = ? AND id {cmp} ?)) ")
        } else {
            String::new()
        };
        let sql = format!(
            "SELECT id, team_id, to_agent_id, from_agent_id, actor_user_id, \
                    type, content, summary, files, read, created_at \
             FROM mailbox \
             WHERE team_id = ? {cursor_clause}\
             ORDER BY created_at {order}, id {order} \
             LIMIT ?"
        );
        let mut q = sqlx::query_as::<_, MailboxMessageRow>(&sql).bind(team_id);
        if let Some(c) = &cursor {
            q = q.bind(c.created_at).bind(c.created_at).bind(&c.id);
        }
        let rows = q.bind(limit).fetch_all(&self.pool).await?;
        Ok(rows)
    }

    async fn list_messages_by_ids(&self, ids: &[String]) -> Result<Vec<MailboxMessageRow>, DbError> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        // SQLite placeholder limit is 999; batch in the same way as
        // `mark_read_batch` and merge the chunks.
        let mut out = Vec::new();
        for chunk in ids.chunks(500) {
            let placeholders: String = chunk.iter().map(|_| "?").collect::<Vec<_>>().join(",");
            let sql = format!(
                "SELECT id, team_id, to_agent_id, from_agent_id, actor_user_id, \
                        type, content, summary, files, read, created_at \
                 FROM mailbox \
                 WHERE id IN ({placeholders}) \
                 ORDER BY created_at DESC"
            );
            let mut query = sqlx::query_as::<_, MailboxMessageRow>(&sql);
            for id in chunk {
                query = query.bind(id);
            }
            let rows = query.fetch_all(&self.pool).await?;
            out.extend(rows);
        }
        Ok(out)
    }

    async fn delete_mailbox_by_team(&self, user_id: &str, team_id: &str) -> Result<(), DbError> {
        sqlx::query(
            "DELETE FROM mailbox \
             WHERE team_id = ? \
               AND EXISTS (SELECT 1 FROM teams t WHERE t.id = mailbox.team_id AND t.user_id = ?)",
        )
        .bind(team_id)
        .bind(user_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    // ── Tasks ────────────────────────────────────────────────────────

    async fn create_task(&self, user_id: &str, row: &TeamTaskRow) -> Result<(), DbError> {
        let result = sqlx::query(
            "INSERT INTO team_tasks \
                (id, team_id, subject, description, status, owner, \
                 blocked_by, blocks, metadata, created_at, updated_at) \
             SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? \
             WHERE EXISTS (SELECT 1 FROM teams t WHERE t.id = ? AND t.user_id = ?)",
        )
        .bind(&row.id)
        .bind(&row.team_id)
        .bind(&row.subject)
        .bind(&row.description)
        .bind(&row.status)
        .bind(&row.owner)
        .bind(&row.blocked_by)
        .bind(&row.blocks)
        .bind(&row.metadata)
        .bind(row.created_at)
        .bind(row.updated_at)
        .bind(&row.team_id)
        .bind(user_id)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 0 {
            return Err(DbError::NotFound(format!("team {}", row.team_id)));
        }
        Ok(())
    }

    async fn find_task_by_id(
        &self,
        user_id: &str,
        team_id: &str,
        task_id: &str,
    ) -> Result<Option<TeamTaskRow>, DbError> {
        let row = sqlx::query_as::<_, TeamTaskRow>(
            "SELECT * FROM team_tasks \
             WHERE team_id = ? AND id = ? \
               AND EXISTS (SELECT 1 FROM teams t WHERE t.id = team_tasks.team_id AND t.user_id = ?)",
        )
        .bind(team_id)
        .bind(task_id)
        .bind(user_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row)
    }

    async fn update_task(
        &self,
        user_id: &str,
        team_id: &str,
        task_id: &str,
        params: &UpdateTaskParams,
    ) -> Result<(), DbError> {
        let mut set_clauses = Vec::new();
        if params.status.is_some() {
            set_clauses.push("status = ?");
        }
        if params.description.is_some() {
            set_clauses.push("description = ?");
        }
        if params.owner.is_some() {
            set_clauses.push("owner = ?");
        }
        if params.blocked_by.is_some() {
            set_clauses.push("blocked_by = ?");
        }
        if params.metadata.is_some() {
            set_clauses.push("metadata = ?");
        }

        if set_clauses.is_empty() {
            return Ok(());
        }

        set_clauses.push("updated_at = ?");
        let sql = format!(
            "UPDATE team_tasks SET {} \
             WHERE team_id = ? AND id = ? \
               AND EXISTS (SELECT 1 FROM teams t WHERE t.id = team_tasks.team_id AND t.user_id = ?)",
            set_clauses.join(", ")
        );

        let mut query = sqlx::query(&sql);
        if let Some(ref status) = params.status {
            query = query.bind(status);
        }
        if let Some(ref description) = params.description {
            query = query.bind(description);
        }
        if let Some(ref owner) = params.owner {
            query = query.bind(owner);
        }
        if let Some(ref blocked_by) = params.blocked_by {
            query = query.bind(blocked_by);
        }
        if let Some(ref metadata) = params.metadata {
            query = query.bind(metadata);
        }
        query = query.bind(now_ms());
        query = query.bind(team_id);
        query = query.bind(task_id);
        query = query.bind(user_id);

        let result = query.execute(&self.pool).await?;
        if result.rows_affected() == 0 {
            return Err(DbError::NotFound(format!("task {task_id}")));
        }
        Ok(())
    }

    async fn list_tasks(&self, user_id: &str, team_id: &str) -> Result<Vec<TeamTaskRow>, DbError> {
        let rows = sqlx::query_as::<_, TeamTaskRow>(
            "SELECT * FROM team_tasks \
             WHERE team_id = ? \
               AND EXISTS (SELECT 1 FROM teams t WHERE t.id = team_tasks.team_id AND t.user_id = ?) \
             ORDER BY created_at ASC",
        )
        .bind(team_id)
        .bind(user_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    async fn list_tasks_by_ids(
        &self,
        user_id: &str,
        team_id: &str,
        ids: &[String],
    ) -> Result<Vec<TeamTaskRow>, DbError> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        // SQLite placeholder limit is 999; chunk defensively like
        // `list_messages_by_ids` and merge the results.
        let mut out = Vec::new();
        for chunk in ids.chunks(500) {
            let placeholders: String = chunk.iter().map(|_| "?").collect::<Vec<_>>().join(",");
            let sql = format!(
                "SELECT * FROM team_tasks \
                 WHERE team_id = ? \
                   AND EXISTS (SELECT 1 FROM teams t WHERE t.id = team_tasks.team_id AND t.user_id = ?) \
                   AND id IN ({placeholders}) \
                 ORDER BY created_at DESC"
            );
            let mut q = sqlx::query_as::<_, TeamTaskRow>(&sql).bind(team_id).bind(user_id);
            for id in chunk {
                q = q.bind(id);
            }
            out.extend(q.fetch_all(&self.pool).await?);
        }
        Ok(out)
    }

    async fn list_tasks_paged(
        &self,
        user_id: &str,
        team_id: &str,
        cursor: Option<ActivityCursor>,
        direction: PageDirection,
        limit: i64,
    ) -> Result<Vec<TeamTaskRow>, DbError> {
        let (cmp, order) = match direction {
            PageDirection::Desc => ("<", "DESC"),
            PageDirection::Asc => (">", "ASC"),
        };
        let cursor_clause = if cursor.is_some() {
            format!("AND (created_at {cmp} ? OR (created_at = ? AND id {cmp} ?)) ")
        } else {
            String::new()
        };
        let sql = format!(
            "SELECT * FROM team_tasks \
             WHERE team_id = ? \
               AND EXISTS (SELECT 1 FROM teams t WHERE t.id = team_tasks.team_id AND t.user_id = ?) \
               {cursor_clause}\
             ORDER BY created_at {order}, id {order} \
             LIMIT ?"
        );
        let mut q = sqlx::query_as::<_, TeamTaskRow>(&sql).bind(team_id).bind(user_id);
        if let Some(c) = &cursor {
            q = q.bind(c.created_at).bind(c.created_at).bind(&c.id);
        }
        let rows = q.bind(limit).fetch_all(&self.pool).await?;
        Ok(rows)
    }

    async fn append_to_blocks(
        &self,
        user_id: &str,
        team_id: &str,
        task_id: &str,
        blocked_task_id: &str,
    ) -> Result<(), DbError> {
        // Read current blocks, append, and write back within a transaction.
        let mut tx = self.pool.begin().await?;

        let row = sqlx::query_as::<_, TeamTaskRow>(
            "SELECT * FROM team_tasks \
             WHERE team_id = ? AND id = ? \
               AND EXISTS (SELECT 1 FROM teams t WHERE t.id = team_tasks.team_id AND t.user_id = ?)",
        )
        .bind(team_id)
        .bind(task_id)
        .bind(user_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| DbError::NotFound(format!("task {task_id}")))?;

        let mut blocks: Vec<String> = serde_json::from_str(&row.blocks).unwrap_or_default();
        if !blocks.contains(&blocked_task_id.to_string()) {
            blocks.push(blocked_task_id.to_string());
        }
        let new_blocks = serde_json::to_string(&blocks).unwrap_or_else(|_| "[]".to_string());

        sqlx::query(
            "UPDATE team_tasks SET blocks = ?, updated_at = ? \
             WHERE team_id = ? AND id = ? \
               AND EXISTS (SELECT 1 FROM teams t WHERE t.id = team_tasks.team_id AND t.user_id = ?)",
        )
        .bind(&new_blocks)
        .bind(now_ms())
        .bind(team_id)
        .bind(task_id)
        .bind(user_id)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(())
    }

    async fn remove_from_blocked_by(
        &self,
        user_id: &str,
        team_id: &str,
        task_id: &str,
        unblocked_task_id: &str,
    ) -> Result<(), DbError> {
        // Read current blocked_by, remove, and write back within a transaction.
        let mut tx = self.pool.begin().await?;

        let row = sqlx::query_as::<_, TeamTaskRow>(
            "SELECT * FROM team_tasks \
             WHERE team_id = ? AND id = ? \
               AND EXISTS (SELECT 1 FROM teams t WHERE t.id = team_tasks.team_id AND t.user_id = ?)",
        )
        .bind(team_id)
        .bind(task_id)
        .bind(user_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| DbError::NotFound(format!("task {task_id}")))?;

        let mut blocked_by: Vec<String> = serde_json::from_str(&row.blocked_by).unwrap_or_default();
        blocked_by.retain(|id| id != unblocked_task_id);
        let new_blocked_by = serde_json::to_string(&blocked_by).unwrap_or_else(|_| "[]".to_string());

        sqlx::query(
            "UPDATE team_tasks SET blocked_by = ?, updated_at = ? \
             WHERE team_id = ? AND id = ? \
               AND EXISTS (SELECT 1 FROM teams t WHERE t.id = team_tasks.team_id AND t.user_id = ?)",
        )
        .bind(&new_blocked_by)
        .bind(now_ms())
        .bind(team_id)
        .bind(task_id)
        .bind(user_id)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(())
    }

    async fn delete_tasks_by_team(&self, user_id: &str, team_id: &str) -> Result<(), DbError> {
        sqlx::query(
            "DELETE FROM team_tasks \
             WHERE team_id = ? \
               AND EXISTS (SELECT 1 FROM teams t WHERE t.id = team_tasks.team_id AND t.user_id = ?)",
        )
        .bind(team_id)
        .bind(user_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}
