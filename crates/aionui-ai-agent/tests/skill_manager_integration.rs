//! Integration tests for the skill system.
//!
//! These tests verify the full skill lifecycle:
//! - Skill discovery across multiple directories
//! - Skill index generation
//! - Lazy loading of skill bodies
//! - LOAD_SKILL detection in agent output
//! - System instruction building
//! - First message preparation

use std::fs;
use std::path::Path;
use std::sync::Arc;

use aionui_ai_agent::skill_manager::{
    AcpSkillManager, build_skills_index_text, build_system_instructions, detect_skill_load_request,
    prepare_first_message, prepare_first_message_with_skills_index,
};
use aionui_extension::{SkillPaths, resolve_skill_paths};
use tempfile::TempDir;

/// Build SkillPaths rooted at `base` for test use.
fn test_paths(base: &Path) -> Arc<SkillPaths> {
    Arc::new(resolve_skill_paths(base, base))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Create a skill directory with a SKILL.md file.
fn create_skill(base: &Path, category: &str, dir_name: &str, name: &str, desc: &str, body: &str) {
    let dir = base.join(category).join(dir_name);
    fs::create_dir_all(&dir).unwrap();
    let content = format!("---\nname: {name}\ndescription: {desc}\n---\n{body}");
    fs::write(dir.join("SKILL.md"), content).unwrap();
}

/// Create a directory that looks like a skill but has no SKILL.md.
fn create_non_skill_dir(base: &Path, category: &str, dir_name: &str) {
    let dir = base.join(category).join(dir_name);
    fs::create_dir_all(&dir).unwrap();
    fs::write(dir.join("README.md"), "not a skill").unwrap();
}

// ---------------------------------------------------------------------------
// 5.1 Skill Discovery
// ---------------------------------------------------------------------------

#[tokio::test]
async fn discover_builtin_skills() {
    let tmp = TempDir::new().unwrap();
    create_skill(
        tmp.path(),
        "_builtin",
        "code-review",
        "code-review",
        "Review code for quality issues",
        "Full review instructions here.",
    );

    let mgr = AcpSkillManager::new(test_paths(tmp.path()));
    let index = mgr.discover_skills(tmp.path(), None).await;

    assert_eq!(index.len(), 1);
    assert_eq!(index[0].name, "code-review");
    assert_eq!(index[0].description, "Review code for quality issues");
}

#[tokio::test]
async fn discover_user_custom_skills() {
    let tmp = TempDir::new().unwrap();
    create_skill(
        tmp.path(),
        "skills",
        "my-debugger",
        "my-debugger",
        "Custom debugging tool",
        "Step 1: reproduce the bug...",
    );

    let mgr = AcpSkillManager::new(test_paths(tmp.path()));
    let index = mgr.discover_skills(tmp.path(), None).await;

    assert_eq!(index.len(), 1);
    assert_eq!(index[0].name, "my-debugger");
}

#[tokio::test]
async fn discover_returns_empty_for_empty_directories() {
    let tmp = TempDir::new().unwrap();
    // Create the scan directories but with no skills
    fs::create_dir_all(tmp.path().join("_builtin")).unwrap();
    fs::create_dir_all(tmp.path().join("skills")).unwrap();

    let mgr = AcpSkillManager::new(test_paths(tmp.path()));
    let index = mgr.discover_skills(tmp.path(), None).await;

    assert!(index.is_empty());
}

#[tokio::test]
async fn discover_returns_empty_when_no_directories_exist() {
    let tmp = TempDir::new().unwrap();
    let mgr = AcpSkillManager::new(test_paths(tmp.path()));
    let index = mgr.discover_skills(tmp.path(), None).await;
    assert!(index.is_empty());
}

#[tokio::test]
async fn discover_ignores_directories_without_skill_md() {
    let tmp = TempDir::new().unwrap();
    create_non_skill_dir(tmp.path(), "skills", "not-a-skill");
    create_skill(
        tmp.path(),
        "skills",
        "real-skill",
        "real-skill",
        "A real skill",
        "body",
    );

    let mgr = AcpSkillManager::new(test_paths(tmp.path()));
    let index = mgr.discover_skills(tmp.path(), None).await;

    assert_eq!(index.len(), 1);
    assert_eq!(index[0].name, "real-skill");
}

#[tokio::test]
async fn discover_skills_across_all_three_directories() {
    let tmp = TempDir::new().unwrap();
    create_skill(tmp.path(), "_builtin", "a", "a", "Builtin A", "body");
    create_skill(tmp.path(), "builtin-skills", "b", "b", "Packaged B", "body");
    create_skill(tmp.path(), "skills", "c", "c", "Custom C", "body");

    let mgr = AcpSkillManager::new(test_paths(tmp.path()));
    let index = mgr.discover_skills(tmp.path(), None).await;

    assert_eq!(index.len(), 3);
    let names: Vec<&str> = index.iter().map(|s| s.name.as_str()).collect();
    assert!(names.contains(&"a"));
    assert!(names.contains(&"b"));
    assert!(names.contains(&"c"));
}

#[tokio::test]
async fn discover_with_enabled_filter() {
    let tmp = TempDir::new().unwrap();
    create_skill(tmp.path(), "skills", "alpha", "alpha", "Alpha", "body");
    create_skill(tmp.path(), "skills", "beta", "beta", "Beta", "body");
    create_skill(tmp.path(), "skills", "gamma", "gamma", "Gamma", "body");

    let mgr = AcpSkillManager::new(test_paths(tmp.path()));
    let enabled = vec!["alpha".to_string(), "gamma".to_string()];
    let index = mgr.discover_skills(tmp.path(), Some(&enabled)).await;

    assert_eq!(index.len(), 2);
    let names: Vec<&str> = index.iter().map(|s| s.name.as_str()).collect();
    assert!(names.contains(&"alpha"));
    assert!(names.contains(&"gamma"));
    assert!(!names.contains(&"beta"));
}

// ---------------------------------------------------------------------------
// 5.2 Skill Index
// ---------------------------------------------------------------------------

#[tokio::test]
async fn get_skills_index_returns_name_and_description_only() {
    let tmp = TempDir::new().unwrap();
    create_skill(
        tmp.path(),
        "skills",
        "test-skill",
        "test-skill",
        "A test skill",
        "Detailed body content that should NOT appear in the index.",
    );

    let mgr = AcpSkillManager::new(test_paths(tmp.path()));
    mgr.discover_skills(tmp.path(), None).await;

    let index = mgr.get_skills_index().await;
    assert_eq!(index.len(), 1);
    assert_eq!(index[0].name, "test-skill");
    assert_eq!(index[0].description, "A test skill");
}

#[test]
fn build_index_text_contains_load_protocol() {
    let skills = vec![
        aionui_ai_agent::SkillIndex {
            name: "security".into(),
            description: "Security review".into(),
        },
        aionui_ai_agent::SkillIndex {
            name: "tdd".into(),
            description: "Test-driven development".into(),
        },
    ];
    let text = build_skills_index_text(&skills);

    assert!(text.contains("[LOAD_SKILL: skill-name]"));
    assert!(text.contains("- **security**: Security review"));
    assert!(text.contains("- **tdd**: Test-driven development"));
}

// ---------------------------------------------------------------------------
// 5.3 Lazy Loading
// ---------------------------------------------------------------------------

#[tokio::test]
async fn lazy_load_skill_body_from_file() {
    let tmp = TempDir::new().unwrap();
    create_skill(
        tmp.path(),
        "skills",
        "lazy-skill",
        "lazy-skill",
        "Lazy loaded",
        "This is the complete skill body.\nWith multiple lines.",
    );

    let mgr = AcpSkillManager::new(test_paths(tmp.path()));
    mgr.discover_skills(tmp.path(), None).await;

    let skill = mgr.get_skill("lazy-skill").await.unwrap();
    assert!(skill.body.is_some());
    assert!(
        skill
            .body
            .as_deref()
            .unwrap()
            .contains("complete skill body")
    );
    assert!(skill.body.as_deref().unwrap().contains("multiple lines"));
}

#[tokio::test]
async fn cached_load_does_not_reread_file() {
    let tmp = TempDir::new().unwrap();
    create_skill(
        tmp.path(),
        "skills",
        "cached",
        "cached",
        "Cached skill",
        "Original body",
    );

    let mgr = AcpSkillManager::new(test_paths(tmp.path()));
    mgr.discover_skills(tmp.path(), None).await;

    // First load
    let first = mgr.get_skill("cached").await.unwrap();
    assert_eq!(first.body.as_deref(), Some("Original body"));

    // Modify the file after first load
    let skill_path = tmp.path().join("skills/cached/SKILL.md");
    fs::write(
        &skill_path,
        "---\nname: cached\ndescription: Cached skill\n---\nModified body",
    )
    .unwrap();

    // Second load should return cached version
    let second = mgr.get_skill("cached").await.unwrap();
    assert_eq!(second.body.as_deref(), Some("Original body"));
}

#[tokio::test]
async fn get_skill_returns_none_for_unknown() {
    let tmp = TempDir::new().unwrap();
    let mgr = AcpSkillManager::new(test_paths(tmp.path()));
    mgr.discover_skills(tmp.path(), None).await;

    assert!(mgr.get_skill("nonexistent").await.is_none());
}

// ---------------------------------------------------------------------------
// 5.4 LOAD_SKILL Detection
// ---------------------------------------------------------------------------

#[test]
fn detect_single_load_skill_request() {
    let content = "I need to use [LOAD_SKILL: security-review] to check this code.";
    let skills = detect_skill_load_request(content);
    assert_eq!(skills, vec!["security-review"]);
}

#[test]
fn detect_multiple_load_skill_requests() {
    let content = "[LOAD_SKILL: a] then [LOAD_SKILL: b] and [LOAD_SKILL: c]";
    let skills = detect_skill_load_request(content);
    assert_eq!(skills, vec!["a", "b", "c"]);
}

#[test]
fn detect_no_load_skill_in_normal_text() {
    let content = "This is just normal text without any skill requests.";
    let skills = detect_skill_load_request(content);
    assert!(skills.is_empty());
}

#[test]
fn detect_load_skill_handles_whitespace() {
    let content = "[LOAD_SKILL:   spaced-name   ]";
    let skills = detect_skill_load_request(content);
    assert_eq!(skills, vec!["spaced-name"]);
}

// ---------------------------------------------------------------------------
// System instruction and first message builders
// ---------------------------------------------------------------------------

#[test]
fn system_instructions_with_loaded_skills() {
    let skills = vec![aionui_ai_agent::SkillDefinition {
        name: "helper".into(),
        description: "A helper".into(),
        location: std::path::PathBuf::new(),
        source: aionui_extension::SkillSource::Custom,
        relative_location: None,
        body: Some("Complete helper instructions.".into()),
    }];
    let result = build_system_instructions("Base system prompt", &skills);

    assert!(result.starts_with("Base system prompt"));
    assert!(result.contains("## Skill: helper"));
    assert!(result.contains("Complete helper instructions."));
}

#[test]
fn first_message_with_skills_index_for_acp() {
    let skills = vec![aionui_ai_agent::SkillIndex {
        name: "review".into(),
        description: "Code review".into(),
    }];
    let result = prepare_first_message_with_skills_index("Please review my code.", &skills, None);

    assert!(result.contains("[Assistant Rules]"));
    assert!(result.contains("- **review**: Code review"));
    assert!(result.contains("[/Assistant Rules]"));
    assert!(result.ends_with("Please review my code."));
}

#[test]
fn first_message_with_full_skills_for_gemini() {
    let skills = vec![aionui_ai_agent::SkillDefinition {
        name: "debug".into(),
        description: "Debug".into(),
        location: std::path::PathBuf::new(),
        source: aionui_extension::SkillSource::Custom,
        relative_location: None,
        body: Some("Full debug skill content.".into()),
    }];
    let result = prepare_first_message("Hello", &skills, Some("Be helpful."));

    assert!(result.contains("[Assistant Rules]"));
    assert!(result.contains("Be helpful."));
    assert!(result.contains("Full debug skill content."));
    assert!(result.contains("[/Assistant Rules]"));
    assert!(result.ends_with("Hello"));
}

// ---------------------------------------------------------------------------
// User override: skills/ takes precedence over _builtin/
// ---------------------------------------------------------------------------

#[tokio::test]
async fn user_skills_override_builtin() {
    let tmp = TempDir::new().unwrap();
    create_skill(
        tmp.path(),
        "_builtin",
        "review",
        "review",
        "Built-in review (should be overridden)",
        "builtin body",
    );
    create_skill(
        tmp.path(),
        "skills",
        "review",
        "review",
        "Custom review (override)",
        "custom body",
    );

    let mgr = AcpSkillManager::new(test_paths(tmp.path()));
    let index = mgr.discover_skills(tmp.path(), None).await;

    // Should only have one entry for "review"
    assert_eq!(index.len(), 1);
    assert_eq!(index[0].description, "Custom review (override)");

    // Load the body to verify override
    let skill = mgr.get_skill("review").await.unwrap();
    assert_eq!(skill.body.as_deref(), Some("custom body"));
}
