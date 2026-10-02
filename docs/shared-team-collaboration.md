# Shared Team collaborator selection

`GET /api/teams/eligible-collaborators` is available only to the owner of a
persisted Shared Team. Eligibility is based on the Core `users` table: the
account must have `status = active`, must not be the Team owner or
`system_default_user`, and must not already have a membership for that Team.
The SQLite query performs these checks together with the Team owner/sharing
check. `POST /api/teams/{id}/members` accepts only a server-issued
`account_ref`; it never accepts a database user ID.

Each listing issues an opaque, owner- and Team-scoped reference that expires
after five minutes and can be used once. Add-member resolves that reference in
process memory, reloads the eligible DB users, then inserts through a
single-statement owner-scoped query that rechecks Team ownership, Shared mode,
active user status, and existing membership. A restart invalidates outstanding
references. References are not durable credentials or proof of host login.

Only a username is available as the Core account label; the User schema has no
separate display-name field. Since AionPro provisioning can store the email
address in `username`, email-shaped usernames are returned as `Account N`
ordinal labels rather than exposing the address. These labels distinguish
options within one response but do not identify the person behind an
AionPro account. Hosts that require human-identifiable selection need a
separate privacy-reviewed display-name source; the picker must not fall back to
email, external identity, or database IDs.
