//! Unit tests for filesystem tenant guard.
//!
//! Validates:
//! 1. Local admin preserves full access (e.g. /data, /data/aionui-backend.db, any user path).
//! 2. Regular tenant is forbidden from accessing /data root, system DBs, and other tenants' directories.
//! 3. Regular tenant is allowed to access their own conversation and user paths.

use aionui_auth::CurrentUser;
use aionui_db::{UserStatus, UserType};
use aionui_file::validate_tenant_path;

fn make_admin() -> CurrentUser {
    CurrentUser::local_default()
}

fn make_tenant(id: &str) -> CurrentUser {
    CurrentUser {
        id: id.to_string(),
        username: format!("{id}@example.com"),
        user_type: UserType::Aionpro,
        status: UserStatus::Active,
    }
}

#[test]
fn local_admin_allows_system_files_and_roots() {
    let admin = make_admin();
    assert!(admin.is_local_admin());

    assert!(validate_tenant_path(&admin, "/data").is_ok());
    assert!(validate_tenant_path(&admin, "/data/aionui-backend.db").is_ok());
    assert!(validate_tenant_path(&admin, "/data/aionui-memory.db").is_ok());
    assert!(validate_tenant_path(&admin, "/data/logs").is_ok());
    assert!(validate_tenant_path(&admin, "/data/users/any_user/vault").is_ok());
    assert!(validate_tenant_path(&admin, "/data/conversations/users/any_user/workspace").is_ok());
}

#[test]
fn tenant_prohibits_system_databases() {
    let tenant = make_tenant("user_tenant_1");
    assert!(!tenant.is_local_admin());

    assert!(validate_tenant_path(&tenant, "/data/aionui-backend.db").is_err());
    assert!(validate_tenant_path(&tenant, "/data/aionui-backend.db-wal").is_err());
    assert!(validate_tenant_path(&tenant, "/data/aionui-memory.db").is_err());
    assert!(validate_tenant_path(&tenant, "aionui-backend.db").is_err());
}

#[test]
fn tenant_prohibits_system_root_browsing() {
    let tenant = make_tenant("user_tenant_1");

    assert!(validate_tenant_path(&tenant, "/data").is_err());
    assert!(validate_tenant_path(&tenant, "/data/").is_err());
    assert!(validate_tenant_path(&tenant, "/data/users").is_err());
    assert!(validate_tenant_path(&tenant, "/data/conversations/users").is_err());
    assert!(validate_tenant_path(&tenant, "/data/logs").is_err());
}

#[test]
fn tenant_prohibits_cross_tenant_access() {
    let tenant_alice = make_tenant("user_alice");

    // Attempting to access Bob's user vault by either raw ID or disk directory name.
    assert!(validate_tenant_path(&tenant_alice, "/data/users/user_bob/vault/key.txt").is_err());
    assert!(validate_tenant_path(&tenant_alice, "users/user_bob/notes.md").is_err());
    assert!(validate_tenant_path(&tenant_alice, "/data/users/bob/vault/key.txt").is_err());
    assert!(validate_tenant_path(&tenant_alice, "users/bob/notes.md").is_err());

    // Attempting to access Bob's conversation workspaces.
    assert!(validate_tenant_path(&tenant_alice, "/data/conversations/users/user_bob/2026/09/05/conv-1").is_err());
    assert!(validate_tenant_path(&tenant_alice, "conversations/users/user_bob/workspace/main.rs").is_err());
    assert!(validate_tenant_path(&tenant_alice, "/data/conversations/users/bob/2026/09/05/conv-1").is_err());
    assert!(validate_tenant_path(&tenant_alice, "conversations/users/bob/workspace/main.rs").is_err());
}

#[test]
fn tenant_allows_own_user_and_conversation_paths() {
    let tenant_alice = make_tenant("user_alice");

    // Own user paths
    assert!(validate_tenant_path(&tenant_alice, "/data/users/alice/vault/key.txt").is_ok());
    assert!(validate_tenant_path(&tenant_alice, "users/alice/notes.md").is_ok());

    // Own conversation paths
    assert!(validate_tenant_path(&tenant_alice, "/data/conversations/users/alice/2026/09/05/conv-1").is_ok());
    assert!(validate_tenant_path(&tenant_alice, "conversations/users/alice/workspace/main.rs").is_ok());

    // The raw ID spelling is not a persisted directory and must not be accepted as own data.
    assert!(validate_tenant_path(&tenant_alice, "/data/users/user_alice/vault/key.txt").is_err());

    // Temporary upload paths
    assert!(validate_tenant_path(&tenant_alice, "/tmp/aionui/uploaded_image.png").is_ok());
}

#[test]
fn tenant_allows_existing_paths_under_its_normalized_user_directory() {
    let tenant_alice = make_tenant("user_alice");
    let data_root = tempfile::tempdir().unwrap();
    let own_user_file = data_root.path().join("users/alice/vault/key.txt");
    let own_conversation_file = data_root.path().join("conversations/users/alice/2026/10/02/note.txt");
    let foreign_file = data_root.path().join("users/bob/vault/key.txt");
    std::fs::create_dir_all(own_user_file.parent().unwrap()).unwrap();
    std::fs::create_dir_all(own_conversation_file.parent().unwrap()).unwrap();
    std::fs::create_dir_all(foreign_file.parent().unwrap()).unwrap();
    std::fs::write(&own_user_file, b"own").unwrap();
    std::fs::write(&own_conversation_file, b"own").unwrap();
    std::fs::write(&foreign_file, b"foreign").unwrap();

    assert!(validate_tenant_path(&tenant_alice, own_user_file.to_string_lossy().as_ref()).is_ok());
    assert!(validate_tenant_path(&tenant_alice, own_conversation_file.to_string_lossy().as_ref()).is_ok());
    assert!(validate_tenant_path(&tenant_alice, foreign_file.to_string_lossy().as_ref()).is_err());
}

#[test]
fn tenant_rejects_invalid_user_directory_names() {
    let tenant = make_tenant("user_../bob");

    assert!(validate_tenant_path(&tenant, "/data/users/bob/private.txt").is_err());
}
