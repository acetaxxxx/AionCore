-- Store a non-identifying Team-local label instead of exposing account usernames,
-- which may contain the user's email address in Cloudflare-backed deployments.
ALTER TABLE team_memberships ADD COLUMN display_name TEXT;
