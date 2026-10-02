# Shared Team collaborator selection

`GET /api/teams/eligible-collaborators` is available only to the owner of a
persisted Shared Team. Eligibility is based on the Core `users` table: the
account must have `status = active`, must not be the Team owner or
`system_default_user`, and must not already have a membership for that Team.
The SQLite query performs these checks together with the Team owner/sharing
check. `POST /api/teams/{id}/members` accepts only a server-issued
`account_ref`; it never accepts a database user ID.

Each listing issues an opaque, owner- and Team-scoped reference that expires
after five minutes and can be used once. A successful new listing replaces
outstanding references for that owner+Team atomically; other owners and Teams keep their
own reference sets. Candidate listing is limited in each Core process to one
request per second per owner across all Teams. Each owner may hold at most
2,000 outstanding collaborator references across all Teams. If a complete
candidate list would exceed this budget, the API returns an actionable HTTP
429 and does not truncate or replace the existing choices; wait up to five
minutes for old references to expire and retry. The owner-level state is
accessed directly, without a per-request scan of all owners or Teams.
If the host's active account directory itself contains more than 2,000 eligible
candidates, listing returns a distinct actionable HTTP 429 asking the host
administrator to reduce that directory; it never silently omits candidates.
SQLite fetches no more than 2,001 eligible rows, using the last row only as an
overflow sentinel. Add-member revalidates its single referenced user with an
ID-filtered eligibility query before the existing atomic owner/shared/status
insert check. Owner limiter records become evictable after ten minutes idle;
a throttled sweep removes them during collaborator-ref operations, at most
once per minute, and preserves owner state while references are live. The
owner map is not scanned on every request. If overlapping requests complete
out of order, an older response fails with
`TEAM_COLLABORATOR_LISTING_SUPERSEDED` instead of replacing newer refs.
Add-member resolves that reference in process memory, reloads the eligible DB
user, then inserts through a single-statement owner-scoped query that
rechecks Team ownership, Shared mode, active user status, and existing
membership. A restart invalidates outstanding references. References are not
durable credentials or proof of host login.

Only a username is available as the Core account label; the User schema has no
separate display-name field. Since AionPro provisioning can store the email
address in `username`, email-shaped usernames are returned as `Account N`
ordinal labels rather than exposing the address. These labels distinguish
options within one response but do not identify the person behind an
AionPro account. Hosts that require human-identifiable selection need a
separate privacy-reviewed display-name source; the picker must not fall back to
email, external identity, or database IDs.
