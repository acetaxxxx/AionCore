use std::path::{Component, Path};

use aionui_auth::CurrentUser;

use crate::error::FileError;

/// Validates that `path` is accessible by `user`.
///
/// Rules:
/// 1. Local admin (`user.is_local_admin()`) retains full access to all allowed roots.
/// 2. General/tenant users (`!user.is_local_admin()`) are forbidden from:
///    - Accessing system databases: `aionui-backend.db*`, `aionui-memory.db*`, or system logs.
///    - Browsing the system data root `/data` or shared user root directories (`/data/users`, `conversations/users`).
///    - Accessing any other tenant's directory under `users/<other_user_id>` or `conversations/users/<other_user_id>`.
pub fn validate_tenant_path(user: &CurrentUser, path: &str) -> Result<(), FileError> {
    if user.is_local_admin() {
        return Ok(());
    }

    let trimmed = path.trim();
    if trimmed.is_empty() {
        return Err(FileError::Forbidden("path is required".to_owned()));
    }

    // Normalize separators to forward slash and lowercase for pattern checks
    let normalized = trimmed.replace('\\', "/");
    let lower = normalized.to_ascii_lowercase();

    // 1. Prohibit access to system databases and logs
    if lower.contains("aionui-backend.db") || lower.contains("aionui-memory.db") {
        return Err(FileError::Forbidden(
            "access to system database is forbidden".to_owned(),
        ));
    }
    if lower == "/data/logs"
        || lower.starts_with("/data/logs/")
        || lower == "logs"
        || lower.starts_with("logs/")
        || lower.contains("/logs/")
        || lower.ends_with("/logs")
    {
        return Err(FileError::Forbidden("access to system logs is forbidden".to_owned()));
    }

    // 2. Prohibit browsing root directories directly
    let clean_lower = lower.trim_matches('/');
    if clean_lower == "data"
        || clean_lower == "data/users"
        || clean_lower == "data/conversations"
        || clean_lower == "data/conversations/users"
        || clean_lower == "users"
        || clean_lower == "conversations"
        || clean_lower == "conversations/users"
    {
        return Err(FileError::Forbidden(format!(
            "access to system root directory '{}' is forbidden",
            trimmed
        )));
    }

    // 3. Prohibit cross-tenant access to `conversations/users/{other_user_id}` or `users/{other_user_id}`
    let p = Path::new(&normalized);
    let components: Vec<&str> = p
        .components()
        .filter_map(|c| match c {
            Component::Normal(s) => s.to_str(),
            _ => None,
        })
        .collect();

    for i in 0..components.len() {
        if components[i].eq_ignore_ascii_case("conversations")
            && i + 2 < components.len()
            && components[i + 1].eq_ignore_ascii_case("users")
        {
            let target_user = components[i + 2];
            if target_user != user.id {
                return Err(FileError::Forbidden(
                    "cross-tenant access to conversation path is forbidden".to_owned(),
                ));
            }
        }

        if components[i].eq_ignore_ascii_case("users") && i + 1 < components.len() {
            // Check if this is "conversations/users" (handled above) or standalone "users/{id}"
            if i == 0 || !components[i - 1].eq_ignore_ascii_case("conversations") {
                let target_user = components[i + 1];
                if target_user != user.id {
                    return Err(FileError::Forbidden(
                        "cross-tenant access to user path is forbidden".to_owned(),
                    ));
                }
            }
        }
    }

    // Also check canonical path if the file exists on disk
    if let Ok(canonical) = std::fs::canonicalize(p) {
        let canonical_str = canonical.to_string_lossy().replace('\\', "/");
        let canonical_lower = canonical_str.to_ascii_lowercase();

        if canonical_lower.contains("aionui-backend.db") || canonical_lower.contains("aionui-memory.db") {
            return Err(FileError::Forbidden(
                "access to system database is forbidden".to_owned(),
            ));
        }

        let clean_canonical = canonical_lower.trim_matches('/');
        if clean_canonical == "data"
            || clean_canonical == "data/users"
            || clean_canonical == "data/conversations"
            || clean_canonical == "data/conversations/users"
        {
            return Err(FileError::Forbidden(
                "access to system root directory is forbidden".to_owned(),
            ));
        }

        let can_p = Path::new(&canonical_str);
        let can_components: Vec<&str> = can_p
            .components()
            .filter_map(|c| match c {
                Component::Normal(s) => s.to_str(),
                _ => None,
            })
            .collect();

        for i in 0..can_components.len() {
            if can_components[i].eq_ignore_ascii_case("conversations")
                && i + 2 < can_components.len()
                && can_components[i + 1].eq_ignore_ascii_case("users")
            {
                let target_user = can_components[i + 2];
                if target_user != user.id {
                    return Err(FileError::Forbidden(
                        "cross-tenant access to conversation path is forbidden".to_owned(),
                    ));
                }
            }

            if can_components[i].eq_ignore_ascii_case("users") && i + 1 < can_components.len() {
                if i == 0 || !can_components[i - 1].eq_ignore_ascii_case("conversations") {
                    let target_user = can_components[i + 1];
                    if target_user != user.id {
                        return Err(FileError::Forbidden(
                            "cross-tenant access to user path is forbidden".to_owned(),
                        ));
                    }
                }
            }
        }
    }

    Ok(())
}
