use std::path::{Path, PathBuf};

use include_dir::{Dir, include_dir};
use tracing::{debug, warn};

use crate::constants::{
    AGENT_SKILLS_SUBDIR, ASSISTANT_RULES_DIR_NAME, ASSISTANT_SKILLS_DIR_NAME,
    BUILTIN_AUTO_SKILLS_SUBDIR, BUILTIN_RULES_DIR_NAME, BUILTIN_SKILLS_VIEW_SUBDIR,
    COMMON_SKILL_DIRS, SKILL_MANIFEST_FILE, SKILLS_DIR_NAME,
};
use crate::error::ExtensionError;

/// Built-in skill corpus embedded into the binary at compile time.
///
/// Mirrors the strategy used by `aionui-assistant::builtin`: the corpus is
/// authoritative at build time; an optional on-disk override
/// (`AIONUI_BUILTIN_SKILLS_PATH`) is consulted at runtime for rapid
/// iteration and E2E fixtures.
static BUILTIN_SKILLS: Dir<'static> =
    include_dir!("$CARGO_MANIFEST_DIR/../aionui-app/assets/builtin-skills");

/// Name of the environment variable that, when set, overrides the embedded
/// corpus with an on-disk directory. Consumed by
/// [`resolve_skill_paths`] when building [`SkillPaths`].
pub const BUILTIN_SKILLS_ENV_VAR: &str = "AIONUI_BUILTIN_SKILLS_PATH";

// ---------------------------------------------------------------------------
// Skill paths resolution
// ---------------------------------------------------------------------------

/// Resolved base directories for skill and rule management.
///
/// `builtin_skills_dir` is `Some(path)` only when the
/// `AIONUI_BUILTIN_SKILLS_PATH` env var points at an on-disk corpus, or
/// when tests construct the struct directly. In normal production use it
/// is `None` and the embedded [`BUILTIN_SKILLS`] corpus is consulted
/// instead.
#[derive(Debug, Clone)]
pub struct SkillPaths {
    /// Root data directory (~/.aionui/).
    pub data_dir: PathBuf,
    /// User-created skills directory (~/.aionui/skills/).
    pub user_skills_dir: PathBuf,
    /// Built-in skills directory on disk. `None` means "use the embedded
    /// corpus"; `Some(path)` means "read from disk" (env override or
    /// test fixture).
    pub builtin_skills_dir: Option<PathBuf>,
    /// Built-in rules directory (app bundle resource).
    pub builtin_rules_dir: PathBuf,
    /// Assistant-level rules directory (~/.aionui/assistant-rules/).
    pub assistant_rules_dir: PathBuf,
    /// Assistant-level skills directory (~/.aionui/assistant-skills/).
    pub assistant_skills_dir: PathBuf,
}

/// Resolve standard skill paths.
///
/// `app_resource_dir` is the application's bundled resource directory
/// (e.g. the binary's parent or a configured resource path); only
/// `builtin_rules_dir` is still derived from it — built-in skills live
/// embedded in the binary unless overridden via
/// [`BUILTIN_SKILLS_ENV_VAR`].
///
/// `data_dir` is the user-level data root (e.g. `~/.aionui/`) and
/// determines where user skills, assistant resources, the built-in
/// skills "view" (`{data_dir}/builtin-skills-view/`), and per-agent
/// materialized skill dirs (`{data_dir}/agent-skills/`) live.
pub fn resolve_skill_paths(app_resource_dir: &Path, data_dir: &Path) -> SkillPaths {
    let builtin_skills_dir = std::env::var(BUILTIN_SKILLS_ENV_VAR)
        .ok()
        .filter(|s| !s.is_empty())
        .map(PathBuf::from);

    SkillPaths {
        data_dir: data_dir.to_path_buf(),
        user_skills_dir: data_dir.join(SKILLS_DIR_NAME),
        builtin_skills_dir,
        builtin_rules_dir: app_resource_dir.join(BUILTIN_RULES_DIR_NAME),
        assistant_rules_dir: data_dir.join(ASSISTANT_RULES_DIR_NAME),
        assistant_skills_dir: data_dir.join(ASSISTANT_SKILLS_DIR_NAME),
    }
}

// ---------------------------------------------------------------------------
// A. Built-in resource reading
// ---------------------------------------------------------------------------

/// Read a built-in rule file by name.
///
/// Returns the file content as a string. Returns an empty string if the
/// file does not exist (graceful degradation per API spec).
pub async fn read_builtin_rule(
    paths: &SkillPaths,
    file_name: &str,
) -> Result<String, ExtensionError> {
    validate_filename(file_name)?;
    let file_path = paths.builtin_rules_dir.join(file_name);
    read_file_or_empty(&file_path).await
}

/// Read a built-in skill file by name.
///
/// `file_name` is a relative path inside the built-in skills corpus
/// (e.g. `"auto-inject/cron/SKILL.md"` or `"mermaid/SKILL.md"`). Returns
/// the file content as a string, or an empty string if the file does not
/// exist (preserves the legacy graceful-degradation contract consumed by
/// the renderer).
///
/// When `paths.builtin_skills_dir` is `Some`, reads from that on-disk
/// directory; otherwise consults the embedded corpus. Rejects
/// `..`-style traversal regardless of source.
pub async fn read_builtin_skill(
    paths: &SkillPaths,
    file_name: &str,
) -> Result<String, ExtensionError> {
    validate_builtin_skill_path(file_name)?;

    if let Some(dir) = &paths.builtin_skills_dir {
        let file_path = dir.join(file_name);
        return read_file_or_empty(&file_path).await;
    }

    Ok(BUILTIN_SKILLS
        .get_file(file_name)
        .and_then(|f| f.contents_utf8())
        .map(|s| s.to_string())
        .unwrap_or_default())
}

// ---------------------------------------------------------------------------
// B. Assistant-level CRUD
// ---------------------------------------------------------------------------

/// Read an assistant rule with locale fallback.
///
/// Fallback order:
/// 1. `{assistantId}.{locale}.md` (if locale provided)
/// 2. `{assistantId}.md`
/// 3. Empty string
pub async fn read_assistant_rule(
    paths: &SkillPaths,
    assistant_id: &str,
    locale: Option<&str>,
) -> Result<String, ExtensionError> {
    read_assistant_resource(&paths.assistant_rules_dir, assistant_id, locale).await
}

/// Write an assistant rule.
///
/// Creates `{assistantId}.{locale}.md` or `{assistantId}.md` in the
/// assistant rules directory.
pub async fn write_assistant_rule(
    paths: &SkillPaths,
    assistant_id: &str,
    content: &str,
    locale: Option<&str>,
) -> Result<bool, ExtensionError> {
    write_assistant_resource(&paths.assistant_rules_dir, assistant_id, content, locale).await
}

/// Delete all locale versions of an assistant rule.
pub async fn delete_assistant_rule(
    paths: &SkillPaths,
    assistant_id: &str,
) -> Result<bool, ExtensionError> {
    delete_assistant_resource(&paths.assistant_rules_dir, assistant_id).await
}

/// Read an assistant skill with locale fallback.
pub async fn read_assistant_skill(
    paths: &SkillPaths,
    assistant_id: &str,
    locale: Option<&str>,
) -> Result<String, ExtensionError> {
    read_assistant_resource(&paths.assistant_skills_dir, assistant_id, locale).await
}

/// Write an assistant skill.
pub async fn write_assistant_skill(
    paths: &SkillPaths,
    assistant_id: &str,
    content: &str,
    locale: Option<&str>,
) -> Result<bool, ExtensionError> {
    write_assistant_resource(&paths.assistant_skills_dir, assistant_id, content, locale).await
}

/// Delete all locale versions of an assistant skill.
pub async fn delete_assistant_skill(
    paths: &SkillPaths,
    assistant_id: &str,
) -> Result<bool, ExtensionError> {
    delete_assistant_resource(&paths.assistant_skills_dir, assistant_id).await
}

// ---------------------------------------------------------------------------
// C. Skill listing & info
// ---------------------------------------------------------------------------

/// Origin of a listed skill.
///
/// Matches the renderer contract in
/// `src/common/adapter/ipcBridge.ts::listAvailableSkills`, which filters the
/// Skills Hub UI by this value. `Extension` is reserved for
/// extension-contributed skills once `ExtensionRegistry` is wired into the
/// Rust backend; the pilot only emits `Builtin` / `Custom`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SkillSource {
    Builtin,
    Custom,
    Extension,
}

/// A discovered skill item for listing.
///
/// For `source=Builtin`, `location` is the absolute path of the lazily
/// materialized on-disk "view" (under `{data_dir}/builtin-skills-view/`),
/// and `relative_location` carries the relative path suitable for
/// `POST /api/skills/builtin-skill` (e.g. `"auto-inject/cron/SKILL.md"`
/// or `"mermaid/SKILL.md"`). Other sources leave `relative_location`
/// `None`.
#[derive(Debug, Clone, PartialEq)]
pub struct SkillListItem {
    pub name: String,
    pub description: String,
    pub location: String,
    pub relative_location: Option<String>,
    pub is_custom: bool,
    pub source: SkillSource,
}

/// List all available skills (built-in + user custom), deduplicated.
///
/// User custom skills override built-in skills with the same name.
///
/// For built-in entries, the caller sees an absolute `location` pointing
/// into `{data_dir}/builtin-skills-view/{name}/SKILL.md` — that view is
/// lazily materialized from the embedded corpus so downstream consumers
/// (e.g. the SkillsHubSettings export-symlink flow) can resolve the
/// path on disk. `relative_location` is populated for built-ins only.
pub async fn list_available_skills(
    paths: &SkillPaths,
) -> Result<Vec<SkillListItem>, ExtensionError> {
    let mut skills = std::collections::HashMap::new();

    // 1. Built-in skills (lower priority)
    for item in list_builtin_skills(paths).await {
        skills.insert(item.name.clone(), item);
    }

    // 2. User custom skills (higher priority, overrides builtin)
    if let Ok(entries) = scan_skill_dirs(&paths.user_skills_dir).await {
        for item in entries {
            skills.insert(
                item.name.clone(),
                SkillListItem {
                    name: item.name,
                    description: item.description,
                    location: item.path,
                    relative_location: None,
                    is_custom: true,
                    source: SkillSource::Custom,
                },
            );
        }
    }

    let mut result: Vec<SkillListItem> = skills.into_values().collect();
    result.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(result)
}

/// Emit a [`SkillListItem`] for every built-in skill (both auto-inject
/// and opt-in), materializing the on-disk view lazily.
async fn list_builtin_skills(paths: &SkillPaths) -> Vec<SkillListItem> {
    if let Some(dir) = &paths.builtin_skills_dir {
        return list_builtin_skills_from_disk(paths, dir).await;
    }
    list_builtin_skills_from_embedded(paths).await
}

async fn list_builtin_skills_from_disk(paths: &SkillPaths, dir: &Path) -> Vec<SkillListItem> {
    let mut items = Vec::new();

    // Top-level opt-in skills (siblings of auto-inject/).
    if let Ok(top) = scan_skill_dirs(dir).await {
        for s in top {
            if s.name == BUILTIN_AUTO_SKILLS_SUBDIR {
                continue;
            }
            let rel = format!("{}/{SKILL_MANIFEST_FILE}", s.name);
            let location = materialize_builtin_view_from_disk(paths, dir, &s.name).await;
            items.push(SkillListItem {
                name: s.name,
                description: s.description,
                location,
                relative_location: Some(rel),
                is_custom: false,
                source: SkillSource::Builtin,
            });
        }
    }

    // auto-inject children.
    let auto_dir = dir.join(BUILTIN_AUTO_SKILLS_SUBDIR);
    if let Ok(auto) = scan_skill_dirs(&auto_dir).await {
        for s in auto {
            let rel = format!(
                "{BUILTIN_AUTO_SKILLS_SUBDIR}/{}/{SKILL_MANIFEST_FILE}",
                s.name
            );
            let location = materialize_builtin_view_from_disk(paths, dir, &s.name).await;
            items.push(SkillListItem {
                name: s.name,
                description: s.description,
                location,
                relative_location: Some(rel),
                is_custom: false,
                source: SkillSource::Builtin,
            });
        }
    }

    items
}

async fn list_builtin_skills_from_embedded(paths: &SkillPaths) -> Vec<SkillListItem> {
    let mut items = Vec::new();

    // Top-level opt-in skills.
    for subdir in BUILTIN_SKILLS.dirs() {
        let Some(sub_name) = subdir
            .path()
            .file_name()
            .and_then(|n| n.to_str())
            .map(|s| s.to_string())
        else {
            continue;
        };
        if sub_name == BUILTIN_AUTO_SKILLS_SUBDIR {
            continue;
        }
        let skill_rel = format!("{sub_name}/{SKILL_MANIFEST_FILE}");
        let Some((name, description)) = read_embedded_skill_meta(&skill_rel) else {
            continue;
        };
        let final_name = if name.is_empty() {
            sub_name.clone()
        } else {
            name
        };
        let location = materialize_builtin_view_from_embedded(paths, subdir).await;
        items.push(SkillListItem {
            name: final_name,
            description,
            location,
            relative_location: Some(skill_rel),
            is_custom: false,
            source: SkillSource::Builtin,
        });
    }

    // auto-inject children.
    if let Some(auto_dir) = BUILTIN_SKILLS.get_dir(BUILTIN_AUTO_SKILLS_SUBDIR) {
        for subdir in auto_dir.dirs() {
            let Some(sub_name) = subdir
                .path()
                .file_name()
                .and_then(|n| n.to_str())
                .map(|s| s.to_string())
            else {
                continue;
            };
            let skill_rel =
                format!("{BUILTIN_AUTO_SKILLS_SUBDIR}/{sub_name}/{SKILL_MANIFEST_FILE}");
            let Some((name, description)) = read_embedded_skill_meta(&skill_rel) else {
                continue;
            };
            let final_name = if name.is_empty() {
                sub_name.clone()
            } else {
                name
            };
            let location = materialize_builtin_view_from_embedded(paths, subdir).await;
            items.push(SkillListItem {
                name: final_name,
                description,
                location,
                relative_location: Some(skill_rel),
                is_custom: false,
                source: SkillSource::Builtin,
            });
        }
    }

    items
}

fn read_embedded_skill_meta(skill_rel: &str) -> Option<(String, String)> {
    let file = BUILTIN_SKILLS.get_file(skill_rel)?;
    let content = file.contents_utf8()?;
    parse_frontmatter_fields(content)
}

/// Materialize a single built-in skill (keyed by `skill_name`) into
/// `{data_dir}/builtin-skills-view/{skill_name}/` from the on-disk
/// source corpus. Returns the absolute path to the SKILL.md file; the
/// caller treats any materialization failure as a recoverable warning
/// and surfaces the would-be absolute path anyway so the UI can at
/// least display something consistent.
async fn materialize_builtin_view_from_disk(
    paths: &SkillPaths,
    source_root: &Path,
    skill_name: &str,
) -> String {
    let target_dir = paths
        .data_dir
        .join(BUILTIN_SKILLS_VIEW_SUBDIR)
        .join(skill_name);
    let target_file = target_dir.join(SKILL_MANIFEST_FILE);
    // Source can live either at {root}/{name}/ or {root}/auto-inject/{name}/.
    let candidates = [
        source_root.join(skill_name),
        source_root
            .join(BUILTIN_AUTO_SKILLS_SUBDIR)
            .join(skill_name),
    ];
    let source_dir = candidates.into_iter().find(|p| p.is_dir());
    if let Some(src) = source_dir
        && let Err(e) = copy_dir_recursive(&src, &target_dir).await
    {
        warn!(
            skill = %skill_name,
            error = %e,
            "failed to materialize builtin skill view (disk source)"
        );
    }
    target_file.to_string_lossy().into_owned()
}

/// Materialize a single built-in skill into `{data_dir}/builtin-skills-view/`
/// from the embedded corpus. Returns the absolute SKILL.md path.
async fn materialize_builtin_view_from_embedded(
    paths: &SkillPaths,
    source_dir: &Dir<'static>,
) -> String {
    let skill_name = source_dir
        .path()
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or_default()
        .to_string();
    let target_dir = paths
        .data_dir
        .join(BUILTIN_SKILLS_VIEW_SUBDIR)
        .join(&skill_name);
    if let Err(e) = extract_embedded_dir(source_dir, &target_dir).await {
        warn!(
            skill = %skill_name,
            error = %e,
            "failed to materialize builtin skill view (embedded source)"
        );
    }
    target_dir
        .join(SKILL_MANIFEST_FILE)
        .to_string_lossy()
        .into_owned()
}

/// Recursively extract an embedded [`Dir`] into a filesystem target,
/// overwriting existing files. Parent directories are created as needed.
async fn extract_embedded_dir(source: &Dir<'static>, target: &Path) -> Result<(), ExtensionError> {
    tokio::fs::create_dir_all(target).await?;
    for file in source.files() {
        let rel = file
            .path()
            .strip_prefix(source.path())
            .unwrap_or(file.path());
        let dest = target.join(rel);
        if let Some(parent) = dest.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::write(&dest, file.contents()).await?;
    }
    for subdir in source.dirs() {
        let rel = subdir
            .path()
            .strip_prefix(source.path())
            .unwrap_or(subdir.path());
        let sub_target = target.join(rel);
        Box::pin(extract_embedded_dir(subdir, &sub_target)).await?;
    }
    Ok(())
}

/// A skill discovered during directory scanning.
#[derive(Debug, Clone, PartialEq)]
pub struct ScannedSkill {
    pub name: String,
    pub description: String,
    pub path: String,
}

/// An auto-injected built-in skill.
///
/// Returned by `GET /api/skills/builtin-auto`. `location` is the
/// relative path the frontend passes back into
/// `POST /api/skills/builtin-skill`, e.g. `"auto-inject/cron/SKILL.md"`.
#[derive(Debug, Clone, PartialEq)]
pub struct BuiltinAutoSkillItem {
    pub name: String,
    pub description: String,
    pub location: String,
}

/// List built-in skills that are auto-injected into every assistant.
///
/// Reads from the embedded corpus under `auto-inject/`, or from
/// `{builtin_skills_dir}/auto-inject/` when a disk override is in
/// effect. A missing `auto-inject/` directory yields an empty list,
/// matching the graceful-degradation semantics used elsewhere in this
/// module.
pub async fn list_builtin_auto_skills(
    paths: &SkillPaths,
) -> Result<Vec<BuiltinAutoSkillItem>, ExtensionError> {
    let items = if let Some(dir) = &paths.builtin_skills_dir {
        list_auto_skills_from_disk(&dir.join(BUILTIN_AUTO_SKILLS_SUBDIR)).await
    } else {
        list_auto_skills_from_embedded()
    };
    let mut items = items;
    items.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(items)
}

async fn list_auto_skills_from_disk(auto_dir: &Path) -> Vec<BuiltinAutoSkillItem> {
    let entries = match scan_skill_dirs(auto_dir).await {
        Ok(entries) => entries,
        Err(_) => return Vec::new(),
    };
    entries
        .into_iter()
        .map(|s| {
            let name = s.name.clone();
            BuiltinAutoSkillItem {
                name,
                description: s.description,
                location: format!(
                    "{BUILTIN_AUTO_SKILLS_SUBDIR}/{}/{SKILL_MANIFEST_FILE}",
                    s.name
                ),
            }
        })
        .collect()
}

fn list_auto_skills_from_embedded() -> Vec<BuiltinAutoSkillItem> {
    let Some(auto_dir) = BUILTIN_SKILLS.get_dir(BUILTIN_AUTO_SKILLS_SUBDIR) else {
        return Vec::new();
    };
    let mut items = Vec::new();
    for subdir in auto_dir.dirs() {
        let Some(skill_name) = subdir
            .path()
            .file_name()
            .and_then(|n| n.to_str())
            .map(|s| s.to_string())
        else {
            continue;
        };
        let skill_rel = format!("{BUILTIN_AUTO_SKILLS_SUBDIR}/{skill_name}/{SKILL_MANIFEST_FILE}");
        let Some(file) = BUILTIN_SKILLS.get_file(&skill_rel) else {
            continue;
        };
        let Some(content) = file.contents_utf8() else {
            warn!(path = %skill_rel, "embedded SKILL.md is not valid UTF-8");
            continue;
        };
        let Some((name, description)) = parse_frontmatter_fields(content) else {
            continue;
        };
        let final_name = if name.is_empty() { skill_name } else { name };
        items.push(BuiltinAutoSkillItem {
            name: final_name,
            description,
            location: skill_rel,
        });
    }
    items
}

/// Read skill info from a SKILL.md file without importing.
///
/// Returns `(name, description)` extracted from frontmatter.
pub async fn read_skill_info(skill_path: &Path) -> Result<(String, String), ExtensionError> {
    let skill_file = if skill_path.is_dir() {
        skill_path.join(SKILL_MANIFEST_FILE)
    } else {
        skill_path.to_path_buf()
    };

    let content = tokio::fs::read_to_string(&skill_file)
        .await
        .map_err(|_| ExtensionError::SkillNotFound(skill_path.display().to_string()))?;

    let (name, description) = parse_frontmatter_fields(&content).ok_or_else(|| {
        ExtensionError::InvalidSkillPath(format!(
            "No valid frontmatter in {}",
            skill_file.display()
        ))
    })?;

    // Fallback: use directory name if name is empty
    let final_name = if name.is_empty() {
        skill_path
            .file_name()
            .map(|f| f.to_string_lossy().into_owned())
            .unwrap_or_default()
    } else {
        name
    };

    Ok((final_name, description))
}

// ---------------------------------------------------------------------------
// D. Skill import / export / delete
// ---------------------------------------------------------------------------

/// Import a skill by copying its directory to the user skills directory.
///
/// Returns the skill name.
pub async fn import_skill(paths: &SkillPaths, skill_path: &Path) -> Result<String, ExtensionError> {
    let (name, _) = read_skill_info(skill_path).await?;
    validate_filename(&name)?;

    let target_dir = paths.user_skills_dir.join(&name);
    tokio::fs::create_dir_all(&paths.user_skills_dir).await?;

    copy_dir_recursive(skill_path, &target_dir).await?;

    debug!(skill = %name, target = %target_dir.display(), "skill imported (copy)");
    Ok(name)
}

/// Import a skill by creating a symlink in the user skills directory.
///
/// Returns the skill name.
pub async fn import_skill_with_symlink(
    paths: &SkillPaths,
    skill_path: &Path,
) -> Result<String, ExtensionError> {
    let (name, _) = read_skill_info(skill_path).await?;
    validate_filename(&name)?;

    let target_link = paths.user_skills_dir.join(&name);
    tokio::fs::create_dir_all(&paths.user_skills_dir).await?;

    // Remove existing link/dir if present
    if target_link.exists() {
        if target_link.is_symlink() || target_link.is_file() {
            tokio::fs::remove_file(&target_link).await?;
        } else {
            tokio::fs::remove_dir_all(&target_link).await?;
        }
    }

    create_symlink(skill_path, &target_link).await?;

    debug!(skill = %name, link = %target_link.display(), "skill imported (symlink)");
    Ok(name)
}

/// Export a skill by creating a symlink in the target directory.
pub async fn export_skill_with_symlink(
    skill_path: &Path,
    target_dir: &Path,
) -> Result<(), ExtensionError> {
    let skill_name = skill_path
        .file_name()
        .map(|f| f.to_string_lossy().into_owned())
        .ok_or_else(|| ExtensionError::InvalidSkillPath(skill_path.display().to_string()))?;

    let target_link = target_dir.join(&skill_name);
    tokio::fs::create_dir_all(target_dir).await?;

    // Remove existing link if present
    if target_link.exists() {
        if target_link.is_symlink() || target_link.is_file() {
            tokio::fs::remove_file(&target_link).await?;
        } else {
            tokio::fs::remove_dir_all(&target_link).await?;
        }
    }

    create_symlink(skill_path, &target_link).await?;

    debug!(
        skill = %skill_name,
        link = %target_link.display(),
        "skill exported (symlink)"
    );
    Ok(())
}

/// Delete a user-custom skill by name.
///
/// Returns an error if the skill is built-in or does not exist.
pub async fn delete_skill(paths: &SkillPaths, skill_name: &str) -> Result<(), ExtensionError> {
    // Safety: reject path traversal
    if skill_name.contains('/') || skill_name.contains('\\') || skill_name.contains("..") {
        return Err(ExtensionError::PathTraversal(skill_name.to_string()));
    }

    let user_path = paths.user_skills_dir.join(skill_name);

    if !user_path.exists() {
        // Check if it exists as a built-in (disk override → filesystem,
        // otherwise embedded corpus).
        if builtin_skill_exists(paths, skill_name) {
            return Err(ExtensionError::BuiltinSkillDeletion(skill_name.to_string()));
        }
        return Err(ExtensionError::SkillNotFound(skill_name.to_string()));
    }

    if user_path.is_symlink() || user_path.is_file() {
        tokio::fs::remove_file(&user_path).await?;
    } else {
        tokio::fs::remove_dir_all(&user_path).await?;
    }

    debug!(skill = %skill_name, "skill deleted");
    Ok(())
}

/// Check whether a skill name exists in the built-in corpus — either as
/// a top-level opt-in skill or under `auto-inject/`. Consults the disk
/// override when present; otherwise the embedded corpus.
fn builtin_skill_exists(paths: &SkillPaths, skill_name: &str) -> bool {
    if let Some(dir) = &paths.builtin_skills_dir {
        return dir.join(skill_name).is_dir()
            || dir
                .join(BUILTIN_AUTO_SKILLS_SUBDIR)
                .join(skill_name)
                .is_dir();
    }
    BUILTIN_SKILLS.get_dir(skill_name).is_some()
        || BUILTIN_SKILLS
            .get_dir(format!("{BUILTIN_AUTO_SKILLS_SUBDIR}/{skill_name}"))
            .is_some()
}

// ---------------------------------------------------------------------------
// D2. Per-agent skill materialization
// ---------------------------------------------------------------------------

/// Materialize the listed skills into a per-conversation directory
/// under `{data_dir}/agent-skills/{conversation_id}/`.
///
/// Layout is flat: every skill lands at `{target}/{name}/SKILL.md`,
/// regardless of whether it originated from the `auto-inject/` subtree
/// or a top-level opt-in folder. The `auto-inject/` intermediate
/// directory is flattened away because gemini CLI's `--extensions`
/// loader expects one skill per subdir.
///
/// Callers pass the fully resolved snapshot — this function does NOT
/// implicitly include auto-inject; the caller (e.g. the
/// `ConversationService::create` snapshot) is the source of truth.
/// Unknown names are silently skipped — a warning is emitted but the
/// operation still returns success. Returns the absolute path of the
/// target directory.
pub async fn materialize_skills_for_agent(
    paths: &SkillPaths,
    conversation_id: &str,
    skills: &[String],
) -> Result<PathBuf, ExtensionError> {
    validate_filename(conversation_id)?;

    let target = paths
        .data_dir
        .join(AGENT_SKILLS_SUBDIR)
        .join(conversation_id);

    // Fresh directory on every call — ensures re-runs don't carry stale
    // files between retries.
    if target.exists() {
        tokio::fs::remove_dir_all(&target).await?;
    }
    tokio::fs::create_dir_all(&target).await?;

    for name in skills {
        if name.is_empty() {
            continue;
        }
        if name.contains('/') || name.contains('\\') || name.contains("..") {
            warn!(skill = %name, "skipping skill with invalid name");
            continue;
        }
        let wrote = write_skill_by_name(paths, name, &target).await?;
        if !wrote {
            warn!(skill = %name, "skill not found in any source");
        }
    }

    Ok(target)
}

/// Write a single named skill into `{target}/{name}/`. Resolves in
/// order: embedded/disk builtin (top-level + auto-inject) → user
/// skills dir. Returns `true` if the skill was found and written.
async fn write_skill_by_name(
    paths: &SkillPaths,
    name: &str,
    target: &Path,
) -> Result<bool, ExtensionError> {
    let dest = target.join(name);

    // Disk corpus override.
    if let Some(dir) = &paths.builtin_skills_dir {
        let top = dir.join(name);
        if top.is_dir() {
            if dest.exists() {
                tokio::fs::remove_dir_all(&dest).await?;
            }
            copy_dir_recursive(&top, &dest).await?;
            return Ok(true);
        }
        let auto = dir.join(BUILTIN_AUTO_SKILLS_SUBDIR).join(name);
        if auto.is_dir() {
            if dest.exists() {
                tokio::fs::remove_dir_all(&dest).await?;
            }
            copy_dir_recursive(&auto, &dest).await?;
            return Ok(true);
        }
    } else {
        // Embedded corpus.
        if let Some(top) = BUILTIN_SKILLS.get_dir(name) {
            if dest.exists() {
                tokio::fs::remove_dir_all(&dest).await?;
            }
            extract_embedded_dir(top, &dest).await?;
            return Ok(true);
        }
        if let Some(auto) = BUILTIN_SKILLS.get_dir(format!("{BUILTIN_AUTO_SKILLS_SUBDIR}/{name}")) {
            if dest.exists() {
                tokio::fs::remove_dir_all(&dest).await?;
            }
            extract_embedded_dir(auto, &dest).await?;
            return Ok(true);
        }
    }

    // User skill.
    let user = paths.user_skills_dir.join(name);
    if user.is_dir() {
        if dest.exists() {
            tokio::fs::remove_dir_all(&dest).await?;
        }
        copy_dir_recursive(&user, &dest).await?;
        return Ok(true);
    }

    Ok(false)
}

/// Remove the per-conversation agent-skills directory.
/// Idempotent: missing directory is not an error.
pub async fn cleanup_agent_skills(
    paths: &SkillPaths,
    conversation_id: &str,
) -> Result<(), ExtensionError> {
    validate_filename(conversation_id)?;
    let target = paths
        .data_dir
        .join(AGENT_SKILLS_SUBDIR)
        .join(conversation_id);
    match tokio::fs::remove_dir_all(&target).await {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(ExtensionError::Io(e)),
    }
}

/// Remove orphan per-conversation agent-skills subdirectories.
///
/// Scans `{data_dir}/agent-skills/*` and deletes any entry whose name
/// is not a currently-live conversation, as reported by the
/// `is_live_conversation` predicate. The predicate is injected so this
/// crate does not need to depend on `aionui-conversation` — the
/// composition layer (`aionui-app`) wires in the real repository
/// check.
///
/// Intended to be called once on startup; logs each removal at debug
/// level. Non-fatal errors are swallowed (best-effort cleanup).
pub async fn cleanup_orphan_agent_skills<F>(
    paths: &SkillPaths,
    is_live_conversation: F,
) -> Result<usize, ExtensionError>
where
    F: Fn(&str) -> bool,
{
    let root = paths.data_dir.join(AGENT_SKILLS_SUBDIR);
    let mut entries = match tokio::fs::read_dir(&root).await {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(ExtensionError::Io(e)),
    };

    let mut removed = 0usize;
    while let Ok(Some(entry)) = entries.next_entry().await {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if is_live_conversation(name) {
            continue;
        }
        match tokio::fs::remove_dir_all(&path).await {
            Ok(()) => {
                debug!(conversation_id = %name, "removed orphan agent-skills dir");
                removed += 1;
            }
            Err(e) => warn!(
                conversation_id = %name,
                error = %e,
                "failed to remove orphan agent-skills dir"
            ),
        }
    }
    Ok(removed)
}

// ---------------------------------------------------------------------------
// E. Scanning & discovery
// ---------------------------------------------------------------------------

/// Scan a directory for subdirectories containing SKILL.md.
pub async fn scan_for_skills(folder_path: &Path) -> Result<Vec<ScannedSkill>, ExtensionError> {
    scan_skill_dirs(folder_path).await
}

/// Named filesystem path.
#[derive(Debug, Clone, PartialEq)]
pub struct NamedPath {
    pub name: String,
    pub path: String,
}

/// Detect common skill paths relative to the user's home directory.
///
/// Returns paths that actually exist on the filesystem.
pub async fn detect_common_skill_paths() -> Vec<NamedPath> {
    let Some(home) = dirs::home_dir() else {
        return Vec::new();
    };

    let mut result = Vec::new();
    for (name, rel_path, _slug) in COMMON_SKILL_DIRS {
        let full_path = home.join(rel_path);
        if full_path.exists() {
            result.push(NamedPath {
                name: (*name).to_string(),
                path: full_path.to_string_lossy().into_owned(),
            });
        }
    }

    result
}

/// An external skill source with discovered skills.
///
/// `source` is a stable slug identifying the origin — matches the
/// `ExternalSkillSourceResponse.source` contract consumed by the renderer.
/// Values are drawn from [`COMMON_SKILL_DIRS`] for built-in entries or
/// `format!("custom-{path}")` for user-added paths, so they stay unique
/// across the returned list.
#[derive(Debug, Clone, PartialEq)]
pub struct ExternalSkillSource {
    pub name: String,
    pub path: String,
    pub source: String,
    pub skill_count: usize,
    pub skills: Vec<ScannedSkill>,
}

/// Compute the stable `source` slug for a custom external path.
fn custom_source_slug(path: &str) -> String {
    format!("custom-{path}")
}

/// Discover external skills from common paths and custom external paths.
///
/// The returned list preserves deterministic `source` slugs — see
/// [`ExternalSkillSource::source`] for the contract.
pub async fn detect_and_count_external_skills(
    custom_paths: &[NamedPath],
) -> Vec<ExternalSkillSource> {
    let Some(home) = dirs::home_dir() else {
        return Vec::new();
    };

    let mut sources = Vec::new();

    // 1. Common paths (iterate the constant table so we keep the per-entry slug).
    for (name, rel_path, slug) in COMMON_SKILL_DIRS {
        let full_path = home.join(rel_path);
        if !full_path.exists() {
            continue;
        }
        if let Ok(skills) = scan_skill_dirs(&full_path).await {
            sources.push(ExternalSkillSource {
                name: (*name).to_string(),
                path: full_path.to_string_lossy().into_owned(),
                source: (*slug).to_string(),
                skill_count: skills.len(),
                skills,
            });
        }
    }

    // 2. Custom external paths
    for np in custom_paths {
        let path = Path::new(&np.path);
        if let Ok(skills) = scan_skill_dirs(path).await {
            sources.push(ExternalSkillSource {
                name: np.name.clone(),
                path: np.path.clone(),
                source: custom_source_slug(&np.path),
                skill_count: skills.len(),
                skills,
            });
        }
    }

    sources
}

/// Get the user and built-in skill directory paths.
///
/// When the built-in corpus is embedded (the production case), the
/// returned built-in path is a placeholder URL (`embedded://builtin-skills`)
/// — consumers (`SkillsHubSettings`) only use it for display, never to
/// resolve on-disk files. When a disk override is active, the override
/// path is returned verbatim.
pub fn get_skill_paths(paths: &SkillPaths) -> (String, String) {
    let builtin = paths
        .builtin_skills_dir
        .as_ref()
        .map(|p| p.to_string_lossy().into_owned())
        .unwrap_or_else(|| "embedded://builtin-skills".to_string());
    (
        paths.user_skills_dir.to_string_lossy().into_owned(),
        builtin,
    )
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Read a file and return its content, or an empty string if it does not exist.
async fn read_file_or_empty(path: &Path) -> Result<String, ExtensionError> {
    match tokio::fs::read_to_string(path).await {
        Ok(content) => Ok(content),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(String::new()),
        Err(e) => Err(ExtensionError::Io(e)),
    }
}

/// Validate a filename to prevent path traversal.
fn validate_filename(name: &str) -> Result<(), ExtensionError> {
    if name.contains('/') || name.contains('\\') || name.contains("..") || name.is_empty() {
        return Err(ExtensionError::PathTraversal(name.to_string()));
    }
    Ok(())
}

/// Validate a relative path inside the built-in skill corpus. Allows
/// forward slashes (paths like `"auto-inject/cron/SKILL.md"` are
/// normal) but forbids empty segments, backslashes, leading slash,
/// absolute paths, and any `..` component.
fn validate_builtin_skill_path(rel: &str) -> Result<(), ExtensionError> {
    if rel.is_empty() || rel.contains('\\') || rel.contains("..") || rel.starts_with('/') {
        return Err(ExtensionError::PathTraversal(rel.to_string()));
    }
    if rel.split('/').any(|seg| seg.is_empty()) {
        return Err(ExtensionError::PathTraversal(rel.to_string()));
    }
    if Path::new(rel).is_absolute() {
        return Err(ExtensionError::PathTraversal(rel.to_string()));
    }
    Ok(())
}

/// Read an assistant resource (rule or skill) with locale fallback.
async fn read_assistant_resource(
    dir: &Path,
    assistant_id: &str,
    locale: Option<&str>,
) -> Result<String, ExtensionError> {
    validate_filename(assistant_id)?;
    if let Some(loc) = locale {
        validate_filename(loc)?;
    }

    // 1. Try locale-specific file
    if let Some(loc) = locale
        && !loc.is_empty()
    {
        let locale_file = dir.join(format!("{assistant_id}.{loc}.md"));
        if let Ok(content) = tokio::fs::read_to_string(&locale_file).await {
            return Ok(content);
        }
    }

    // 2. Try default file (no locale suffix)
    let default_file = dir.join(format!("{assistant_id}.md"));
    match tokio::fs::read_to_string(&default_file).await {
        Ok(content) => Ok(content),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(String::new()),
        Err(e) => Err(ExtensionError::Io(e)),
    }
}

/// Write an assistant resource file.
async fn write_assistant_resource(
    dir: &Path,
    assistant_id: &str,
    content: &str,
    locale: Option<&str>,
) -> Result<bool, ExtensionError> {
    validate_filename(assistant_id)?;
    if let Some(loc) = locale {
        validate_filename(loc)?;
    }

    tokio::fs::create_dir_all(dir).await?;

    let filename = match locale {
        Some(loc) if !loc.is_empty() => format!("{assistant_id}.{loc}.md"),
        _ => format!("{assistant_id}.md"),
    };

    let file_path = dir.join(filename);
    tokio::fs::write(&file_path, content).await?;
    debug!(path = %file_path.display(), "assistant resource written");
    Ok(true)
}

/// Delete all files matching `{assistant_id}*.md` in a directory.
async fn delete_assistant_resource(dir: &Path, assistant_id: &str) -> Result<bool, ExtensionError> {
    validate_filename(assistant_id)?;

    let mut deleted_any = false;

    let mut entries = match tokio::fs::read_dir(dir).await {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(e) => return Err(ExtensionError::Io(e)),
    };

    let prefix = format!("{assistant_id}.");
    let exact = format!("{assistant_id}.md");

    while let Ok(Some(entry)) = entries.next_entry().await {
        let name = entry.file_name().to_string_lossy().into_owned();
        if name == exact || (name.starts_with(&prefix) && name.ends_with(".md")) {
            tokio::fs::remove_file(entry.path()).await?;
            deleted_any = true;
            debug!(file = %name, "deleted assistant resource");
        }
    }

    Ok(deleted_any)
}

/// Scan a directory for subdirectories containing a SKILL.md file.
async fn scan_skill_dirs(dir: &Path) -> Result<Vec<ScannedSkill>, ExtensionError> {
    let mut result = Vec::new();

    let mut entries = match tokio::fs::read_dir(dir).await {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(result),
        Err(e) => return Err(ExtensionError::Io(e)),
    };

    while let Ok(Some(entry)) = entries.next_entry().await {
        let entry_path = entry.path();
        if !entry_path.is_dir() {
            continue;
        }

        let skill_file = entry_path.join(SKILL_MANIFEST_FILE);
        if !skill_file.exists() {
            continue;
        }

        match tokio::fs::read_to_string(&skill_file).await {
            Ok(content) => {
                if let Some((name, description)) = parse_frontmatter_fields(&content) {
                    let final_name = if name.is_empty() {
                        entry_path
                            .file_name()
                            .map(|f| f.to_string_lossy().into_owned())
                            .unwrap_or_default()
                    } else {
                        name
                    };
                    result.push(ScannedSkill {
                        name: final_name,
                        description,
                        path: entry_path.to_string_lossy().into_owned(),
                    });
                }
            }
            Err(e) => {
                warn!(
                    path = %skill_file.display(),
                    error = %e,
                    "failed to read SKILL.md"
                );
            }
        }
    }

    result.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(result)
}

/// Parse SKILL.md frontmatter to extract name and description.
///
/// Expected format:
/// ```text
/// ---
/// name: skill-name
/// description: One line description
/// ---
/// Body content here...
/// ```
fn parse_frontmatter_fields(content: &str) -> Option<(String, String)> {
    let trimmed = content.trim_start();
    if !trimmed.starts_with("---") {
        return None;
    }

    let after_open = &trimmed[3..];
    let close_idx = after_open.find("---")?;
    let frontmatter = &after_open[..close_idx];

    let mut name = String::new();
    let mut description = String::new();

    for line in frontmatter.lines() {
        let line = line.trim();
        if let Some(val) = line.strip_prefix("name:") {
            name = val.trim().to_string();
        } else if let Some(val) = line.strip_prefix("description:") {
            description = val.trim().to_string();
        }
    }

    if description.is_empty() {
        return None;
    }

    Some((name, description))
}

/// Recursively copy a directory.
async fn copy_dir_recursive(src: &Path, dst: &Path) -> Result<(), ExtensionError> {
    tokio::fs::create_dir_all(dst).await?;

    let mut entries = tokio::fs::read_dir(src).await?;
    while let Ok(Some(entry)) = entries.next_entry().await {
        let entry_path = entry.path();
        let dest_path = dst.join(entry.file_name());

        if entry_path.is_dir() {
            Box::pin(copy_dir_recursive(&entry_path, &dest_path)).await?;
        } else {
            tokio::fs::copy(&entry_path, &dest_path).await?;
        }
    }

    Ok(())
}

/// Create a symlink (platform-aware).
#[cfg(unix)]
async fn create_symlink(src: &Path, dst: &Path) -> Result<(), ExtensionError> {
    tokio::fs::symlink(src, dst)
        .await
        .map_err(ExtensionError::Io)
}

#[cfg(windows)]
async fn create_symlink(src: &Path, dst: &Path) -> Result<(), ExtensionError> {
    // Use junction on Windows for directory symlinks
    if src.is_dir() {
        tokio::fs::symlink_dir(src, dst)
            .await
            .map_err(ExtensionError::Io)
    } else {
        tokio::fs::symlink_file(src, dst)
            .await
            .map_err(ExtensionError::Io)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    // -----------------------------------------------------------------------
    // Frontmatter parsing
    // -----------------------------------------------------------------------

    #[test]
    fn parse_frontmatter_valid() {
        let content = "---\nname: my-skill\ndescription: A useful skill\n---\nBody content here.";
        let (name, desc) = parse_frontmatter_fields(content).unwrap();
        assert_eq!(name, "my-skill");
        assert_eq!(desc, "A useful skill");
    }

    #[test]
    fn parse_frontmatter_empty_name() {
        let content = "---\nname: \ndescription: Has description\n---\nBody";
        let (name, desc) = parse_frontmatter_fields(content).unwrap();
        assert!(name.is_empty());
        assert_eq!(desc, "Has description");
    }

    #[test]
    fn parse_frontmatter_no_opening() {
        let content = "name: test\ndescription: desc\n---\nbody";
        assert!(parse_frontmatter_fields(content).is_none());
    }

    #[test]
    fn parse_frontmatter_no_closing() {
        let content = "---\nname: test\ndescription: desc";
        assert!(parse_frontmatter_fields(content).is_none());
    }

    #[test]
    fn parse_frontmatter_missing_description() {
        let content = "---\nname: test\n---\nbody";
        assert!(parse_frontmatter_fields(content).is_none());
    }

    // -----------------------------------------------------------------------
    // Filename validation
    // -----------------------------------------------------------------------

    #[test]
    fn validate_filename_normal() {
        assert!(validate_filename("code-review.md").is_ok());
    }

    #[test]
    fn validate_filename_path_traversal() {
        assert!(validate_filename("../etc/passwd").is_err());
        assert!(validate_filename("foo/bar.md").is_err());
        assert!(validate_filename("foo\\bar.md").is_err());
    }

    #[test]
    fn validate_filename_empty() {
        assert!(validate_filename("").is_err());
    }

    // -----------------------------------------------------------------------
    // Built-in resource reading
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn read_builtin_rule_existing_file() {
        let tmp = TempDir::new().unwrap();
        let rules_dir = tmp.path().join(BUILTIN_RULES_DIR_NAME);
        std::fs::create_dir_all(&rules_dir).unwrap();
        std::fs::write(rules_dir.join("code-review.md"), "# Review rules").unwrap();

        let paths = SkillPaths {
            data_dir: tmp.path().to_path_buf(),
            user_skills_dir: tmp.path().join(SKILLS_DIR_NAME),
            builtin_skills_dir: Some(tmp.path().join(crate::constants::BUILTIN_SKILLS_DIR_NAME)),
            builtin_rules_dir: rules_dir,
            assistant_rules_dir: tmp.path().join(ASSISTANT_RULES_DIR_NAME),
            assistant_skills_dir: tmp.path().join(ASSISTANT_SKILLS_DIR_NAME),
        };

        let content = read_builtin_rule(&paths, "code-review.md").await.unwrap();
        assert_eq!(content, "# Review rules");
    }

    #[tokio::test]
    async fn read_builtin_rule_missing_file() {
        let tmp = TempDir::new().unwrap();
        let paths = make_test_paths(tmp.path());

        let content = read_builtin_rule(&paths, "nonexistent.md").await.unwrap();
        assert!(content.is_empty());
    }

    #[tokio::test]
    async fn read_builtin_rule_path_traversal() {
        let tmp = TempDir::new().unwrap();
        let paths = make_test_paths(tmp.path());

        let result = read_builtin_rule(&paths, "../secret.md").await;
        assert!(result.is_err());
    }

    // -----------------------------------------------------------------------
    // Assistant CRUD
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn assistant_rule_write_and_read() {
        let tmp = TempDir::new().unwrap();
        let paths = make_test_paths(tmp.path());

        write_assistant_rule(&paths, "abc123", "Be helpful.", None)
            .await
            .unwrap();

        let content = read_assistant_rule(&paths, "abc123", None).await.unwrap();
        assert_eq!(content, "Be helpful.");
    }

    #[tokio::test]
    async fn assistant_rule_locale_fallback() {
        let tmp = TempDir::new().unwrap();
        let paths = make_test_paths(tmp.path());

        // Write default (no locale)
        write_assistant_rule(&paths, "abc123", "Default content", None)
            .await
            .unwrap();

        // Write zh-CN locale
        write_assistant_rule(&paths, "abc123", "中文内容", Some("zh-CN"))
            .await
            .unwrap();

        // Read with matching locale
        let content = read_assistant_rule(&paths, "abc123", Some("zh-CN"))
            .await
            .unwrap();
        assert_eq!(content, "中文内容");

        // Read with non-matching locale → falls back to default
        let content = read_assistant_rule(&paths, "abc123", Some("ja-JP"))
            .await
            .unwrap();
        assert_eq!(content, "Default content");

        // Read without locale → default
        let content = read_assistant_rule(&paths, "abc123", None).await.unwrap();
        assert_eq!(content, "Default content");
    }

    #[tokio::test]
    async fn assistant_rule_read_nonexistent() {
        let tmp = TempDir::new().unwrap();
        let paths = make_test_paths(tmp.path());

        let content = read_assistant_rule(&paths, "missing", None).await.unwrap();
        assert!(content.is_empty());
    }

    #[tokio::test]
    async fn assistant_rule_delete_all_locales() {
        let tmp = TempDir::new().unwrap();
        let paths = make_test_paths(tmp.path());

        write_assistant_rule(&paths, "abc123", "Default", None)
            .await
            .unwrap();
        write_assistant_rule(&paths, "abc123", "Chinese", Some("zh-CN"))
            .await
            .unwrap();
        write_assistant_rule(&paths, "abc123", "English", Some("en-US"))
            .await
            .unwrap();

        let deleted = delete_assistant_rule(&paths, "abc123").await.unwrap();
        assert!(deleted);

        // Verify all files are gone
        let content = read_assistant_rule(&paths, "abc123", None).await.unwrap();
        assert!(content.is_empty());
        let content = read_assistant_rule(&paths, "abc123", Some("zh-CN"))
            .await
            .unwrap();
        assert!(content.is_empty());
    }

    #[tokio::test]
    async fn assistant_skill_write_and_read() {
        let tmp = TempDir::new().unwrap();
        let paths = make_test_paths(tmp.path());

        write_assistant_skill(&paths, "abc123", "Skill content", None)
            .await
            .unwrap();

        let content = read_assistant_skill(&paths, "abc123", None).await.unwrap();
        assert_eq!(content, "Skill content");
    }

    // -----------------------------------------------------------------------
    // Assistant CRUD — path traversal prevention
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn read_assistant_rule_rejects_traversal_id() {
        let tmp = TempDir::new().unwrap();
        let paths = make_test_paths(tmp.path());
        let result = read_assistant_rule(&paths, "../etc/passwd", None).await;
        assert!(matches!(result, Err(ExtensionError::PathTraversal(_))));
    }

    #[tokio::test]
    async fn read_assistant_rule_rejects_traversal_locale() {
        let tmp = TempDir::new().unwrap();
        let paths = make_test_paths(tmp.path());
        let result = read_assistant_rule(&paths, "valid-id", Some("../evil")).await;
        assert!(matches!(result, Err(ExtensionError::PathTraversal(_))));
    }

    #[tokio::test]
    async fn write_assistant_rule_rejects_traversal_id() {
        let tmp = TempDir::new().unwrap();
        let paths = make_test_paths(tmp.path());
        let result = write_assistant_rule(&paths, "../../escape", "content", None).await;
        assert!(matches!(result, Err(ExtensionError::PathTraversal(_))));
    }

    #[tokio::test]
    async fn write_assistant_rule_rejects_traversal_locale() {
        let tmp = TempDir::new().unwrap();
        let paths = make_test_paths(tmp.path());
        let result = write_assistant_rule(&paths, "valid-id", "content", Some("../bad")).await;
        assert!(matches!(result, Err(ExtensionError::PathTraversal(_))));
    }

    #[tokio::test]
    async fn delete_assistant_rule_rejects_traversal_id() {
        let tmp = TempDir::new().unwrap();
        let paths = make_test_paths(tmp.path());
        let result = delete_assistant_rule(&paths, "foo/bar").await;
        assert!(matches!(result, Err(ExtensionError::PathTraversal(_))));
    }

    #[tokio::test]
    async fn read_assistant_skill_rejects_traversal_id() {
        let tmp = TempDir::new().unwrap();
        let paths = make_test_paths(tmp.path());
        let result = read_assistant_skill(&paths, "..\\windows", None).await;
        assert!(matches!(result, Err(ExtensionError::PathTraversal(_))));
    }

    #[tokio::test]
    async fn write_assistant_skill_rejects_traversal_id() {
        let tmp = TempDir::new().unwrap();
        let paths = make_test_paths(tmp.path());
        let result = write_assistant_skill(&paths, "../escape", "content", None).await;
        assert!(matches!(result, Err(ExtensionError::PathTraversal(_))));
    }

    #[tokio::test]
    async fn delete_assistant_skill_rejects_traversal_id() {
        let tmp = TempDir::new().unwrap();
        let paths = make_test_paths(tmp.path());
        let result = delete_assistant_skill(&paths, "a/b").await;
        assert!(matches!(result, Err(ExtensionError::PathTraversal(_))));
    }

    // -----------------------------------------------------------------------
    // Skill listing
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn list_skills_builtin_and_custom() {
        let tmp = TempDir::new().unwrap();
        let paths = make_disk_builtin_paths(tmp.path());
        let builtin_dir = disk_builtin_dir(&paths).to_path_buf();

        // Create builtin skills
        create_skill_in_dir(&builtin_dir, "review", "Code review skill");
        create_skill_in_dir(&builtin_dir, "debug", "Debugging skill");

        // Create custom skill (overrides review)
        create_skill_in_dir(&paths.user_skills_dir, "review", "Custom review skill");
        create_skill_in_dir(&paths.user_skills_dir, "my-skill", "My custom skill");

        let skills = list_available_skills(&paths).await.unwrap();

        assert_eq!(skills.len(), 3); // debug + review (custom) + my-skill

        let review = skills.iter().find(|s| s.name == "review").unwrap();
        assert!(review.is_custom);
        assert_eq!(review.description, "Custom review skill");
        assert_eq!(review.source, SkillSource::Custom);

        let debug_skill = skills.iter().find(|s| s.name == "debug").unwrap();
        assert!(!debug_skill.is_custom);
        assert_eq!(debug_skill.source, SkillSource::Builtin);
        assert_eq!(
            debug_skill.relative_location.as_deref(),
            Some("debug/SKILL.md")
        );

        let my_skill = skills.iter().find(|s| s.name == "my-skill").unwrap();
        assert_eq!(my_skill.source, SkillSource::Custom);
        assert!(my_skill.relative_location.is_none());
    }

    #[tokio::test]
    async fn list_skills_empty_dirs() {
        let tmp = TempDir::new().unwrap();
        let paths = make_test_paths(tmp.path());

        let skills = list_available_skills(&paths).await.unwrap();
        assert!(skills.is_empty());
    }

    // -----------------------------------------------------------------------
    // Built-in auto skills
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn list_builtin_auto_skills_from_disk_override() {
        let tmp = TempDir::new().unwrap();
        let paths = make_disk_builtin_paths(tmp.path());
        let builtin_dir = disk_builtin_dir(&paths).to_path_buf();
        let auto_dir = builtin_dir.join(BUILTIN_AUTO_SKILLS_SUBDIR);

        create_skill_in_dir(&auto_dir, "cron", "Schedule recurring tasks");
        create_skill_in_dir(&auto_dir, "skill-creator", "Scaffold a new skill");

        // A top-level built-in skill (NOT under auto-inject/) must be excluded.
        create_skill_in_dir(&builtin_dir, "review", "Top-level builtin");

        let autos = list_builtin_auto_skills(&paths).await.unwrap();

        assert_eq!(autos.len(), 2);
        let names: std::collections::HashSet<_> = autos.iter().map(|s| s.name.as_str()).collect();
        assert!(names.contains("cron"));
        assert!(names.contains("skill-creator"));
        assert!(!names.contains("review"));

        let cron = autos.iter().find(|s| s.name == "cron").unwrap();
        assert_eq!(cron.description, "Schedule recurring tasks");
        assert_eq!(cron.location, "auto-inject/cron/SKILL.md");
    }

    #[tokio::test]
    async fn list_builtin_auto_skills_missing_dir_returns_empty() {
        let tmp = TempDir::new().unwrap();
        let paths = make_disk_builtin_paths(tmp.path());
        // No auto-inject/ directory created under the disk override.

        let autos = list_builtin_auto_skills(&paths).await.unwrap();
        assert!(autos.is_empty());
    }

    // -----------------------------------------------------------------------
    // Skill info
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn read_skill_info_valid() {
        let tmp = TempDir::new().unwrap();
        let skill_dir = tmp.path().join("my-skill");
        std::fs::create_dir_all(&skill_dir).unwrap();
        std::fs::write(
            skill_dir.join(SKILL_MANIFEST_FILE),
            "---\nname: my-skill\ndescription: A test skill\n---\nBody",
        )
        .unwrap();

        let (name, desc) = read_skill_info(&skill_dir).await.unwrap();
        assert_eq!(name, "my-skill");
        assert_eq!(desc, "A test skill");
    }

    #[tokio::test]
    async fn read_skill_info_missing() {
        let tmp = TempDir::new().unwrap();
        let result = read_skill_info(&tmp.path().join("nonexistent")).await;
        assert!(result.is_err());
    }

    // -----------------------------------------------------------------------
    // Skill import / delete
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn import_skill_copies_directory() {
        let tmp = TempDir::new().unwrap();
        let paths = make_test_paths(tmp.path());

        // Create source skill
        let source_dir = tmp.path().join("source-skill");
        std::fs::create_dir_all(&source_dir).unwrap();
        std::fs::write(
            source_dir.join(SKILL_MANIFEST_FILE),
            "---\nname: imported\ndescription: Imported skill\n---\nBody",
        )
        .unwrap();
        std::fs::write(source_dir.join("extra.txt"), "extra data").unwrap();

        let name = import_skill(&paths, &source_dir).await.unwrap();
        assert_eq!(name, "imported");

        // Verify the skill was copied
        let imported_dir = paths.user_skills_dir.join("imported");
        assert!(imported_dir.join(SKILL_MANIFEST_FILE).exists());
        assert!(imported_dir.join("extra.txt").exists());
    }

    #[tokio::test]
    async fn import_skill_with_symlink_creates_link() {
        let tmp = TempDir::new().unwrap();
        let paths = make_test_paths(tmp.path());

        let source_dir = tmp.path().join("link-skill");
        std::fs::create_dir_all(&source_dir).unwrap();
        std::fs::write(
            source_dir.join(SKILL_MANIFEST_FILE),
            "---\nname: linked\ndescription: Linked skill\n---\nBody",
        )
        .unwrap();

        let name = import_skill_with_symlink(&paths, &source_dir)
            .await
            .unwrap();
        assert_eq!(name, "linked");

        let link_path = paths.user_skills_dir.join("linked");
        assert!(link_path.is_symlink());
    }

    #[tokio::test]
    async fn import_skill_rejects_traversal_name() {
        let tmp = TempDir::new().unwrap();
        let paths = make_test_paths(tmp.path());

        // Create a skill whose frontmatter name contains path traversal
        let source_dir = tmp.path().join("evil-skill");
        std::fs::create_dir_all(&source_dir).unwrap();
        std::fs::write(
            source_dir.join(SKILL_MANIFEST_FILE),
            "---\nname: ../../../etc/evil\ndescription: Malicious skill\n---\nBody",
        )
        .unwrap();

        let result = import_skill(&paths, &source_dir).await;
        assert!(matches!(result, Err(ExtensionError::PathTraversal(_))));
    }

    #[tokio::test]
    async fn import_skill_with_symlink_rejects_traversal_name() {
        let tmp = TempDir::new().unwrap();
        let paths = make_test_paths(tmp.path());

        let source_dir = tmp.path().join("evil-skill");
        std::fs::create_dir_all(&source_dir).unwrap();
        std::fs::write(
            source_dir.join(SKILL_MANIFEST_FILE),
            "---\nname: ../../escape\ndescription: Malicious skill\n---\nBody",
        )
        .unwrap();

        let result = import_skill_with_symlink(&paths, &source_dir).await;
        assert!(matches!(result, Err(ExtensionError::PathTraversal(_))));
    }

    #[tokio::test]
    async fn delete_custom_skill() {
        let tmp = TempDir::new().unwrap();
        let paths = make_test_paths(tmp.path());

        create_skill_in_dir(&paths.user_skills_dir, "to-delete", "Will be deleted");

        delete_skill(&paths, "to-delete").await.unwrap();
        assert!(!paths.user_skills_dir.join("to-delete").exists());
    }

    #[tokio::test]
    async fn delete_builtin_skill_rejected() {
        let tmp = TempDir::new().unwrap();
        let paths = make_disk_builtin_paths(tmp.path());
        let builtin_dir = disk_builtin_dir(&paths).to_path_buf();

        create_skill_in_dir(&builtin_dir, "protected", "Built-in skill");

        let result = delete_skill(&paths, "protected").await;
        assert!(matches!(
            result,
            Err(ExtensionError::BuiltinSkillDeletion(_))
        ));
    }

    #[tokio::test]
    async fn delete_nonexistent_skill() {
        let tmp = TempDir::new().unwrap();
        let paths = make_test_paths(tmp.path());

        let result = delete_skill(&paths, "ghost").await;
        assert!(matches!(result, Err(ExtensionError::SkillNotFound(_))));
    }

    #[tokio::test]
    async fn delete_skill_path_traversal() {
        let tmp = TempDir::new().unwrap();
        let paths = make_test_paths(tmp.path());

        let result = delete_skill(&paths, "../etc").await;
        assert!(matches!(result, Err(ExtensionError::PathTraversal(_))));
    }

    // -----------------------------------------------------------------------
    // Scanning
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn scan_for_skills_finds_valid() {
        let tmp = TempDir::new().unwrap();
        create_skill_in_dir(tmp.path(), "skill-a", "First skill");
        create_skill_in_dir(tmp.path(), "skill-b", "Second skill");

        // Create a dir without SKILL.md (should be ignored)
        std::fs::create_dir_all(tmp.path().join("not-a-skill")).unwrap();

        let skills = scan_for_skills(tmp.path()).await.unwrap();
        assert_eq!(skills.len(), 2);
        assert_eq!(skills[0].name, "skill-a");
        assert_eq!(skills[1].name, "skill-b");
    }

    #[tokio::test]
    async fn scan_for_skills_empty_dir() {
        let tmp = TempDir::new().unwrap();
        let skills = scan_for_skills(tmp.path()).await.unwrap();
        assert!(skills.is_empty());
    }

    #[tokio::test]
    async fn scan_for_skills_nonexistent_dir() {
        let skills = scan_for_skills(Path::new("/nonexistent/path"))
            .await
            .unwrap();
        assert!(skills.is_empty());
    }

    // -----------------------------------------------------------------------
    // Export
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn export_skill_creates_symlink() {
        let tmp = TempDir::new().unwrap();
        let source_dir = tmp.path().join("my-skill");
        std::fs::create_dir_all(&source_dir).unwrap();
        std::fs::write(
            source_dir.join(SKILL_MANIFEST_FILE),
            "---\nname: my-skill\ndescription: Test\n---\nBody",
        )
        .unwrap();

        let target_dir = tmp.path().join("exports");
        export_skill_with_symlink(&source_dir, &target_dir)
            .await
            .unwrap();

        let link = target_dir.join("my-skill");
        assert!(link.is_symlink());
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    fn make_test_paths(base: &Path) -> SkillPaths {
        // Tests historically seed `builtin_skills_dir` with on-disk
        // content, so we always hand out a disk override here. Tests
        // exercising the embedded corpus use `make_embedded_paths`.
        SkillPaths {
            data_dir: base.to_path_buf(),
            user_skills_dir: base.join(SKILLS_DIR_NAME),
            builtin_skills_dir: Some(base.join(crate::constants::BUILTIN_SKILLS_DIR_NAME)),
            builtin_rules_dir: base.join(BUILTIN_RULES_DIR_NAME),
            assistant_rules_dir: base.join(ASSISTANT_RULES_DIR_NAME),
            assistant_skills_dir: base.join(ASSISTANT_SKILLS_DIR_NAME),
        }
    }

    fn make_embedded_paths(base: &Path) -> SkillPaths {
        // For tests that want to exercise the embedded-corpus code
        // paths without a disk override.
        SkillPaths {
            data_dir: base.to_path_buf(),
            user_skills_dir: base.join(SKILLS_DIR_NAME),
            builtin_skills_dir: None,
            builtin_rules_dir: base.join(BUILTIN_RULES_DIR_NAME),
            assistant_rules_dir: base.join(ASSISTANT_RULES_DIR_NAME),
            assistant_skills_dir: base.join(ASSISTANT_SKILLS_DIR_NAME),
        }
    }

    /// Return a `SkillPaths` rooted at `base` but whose
    /// `builtin_skills_dir` is `Some(path)`, so tests can seed
    /// on-disk fixtures.
    fn make_disk_builtin_paths(base: &Path) -> SkillPaths {
        make_test_paths(base)
    }

    fn disk_builtin_dir(paths: &SkillPaths) -> &Path {
        paths
            .builtin_skills_dir
            .as_deref()
            .expect("disk override must be set for test")
    }

    fn create_skill_in_dir(base: &Path, name: &str, description: &str) {
        let dir = base.join(name);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join(SKILL_MANIFEST_FILE),
            format!("---\nname: {name}\ndescription: {description}\n---\nBody content for {name}."),
        )
        .unwrap();
    }

    // -----------------------------------------------------------------------
    // Embedded corpus
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn embedded_lists_auto_inject_from_corpus() {
        let tmp = TempDir::new().unwrap();
        let paths = make_embedded_paths(tmp.path());

        let autos = list_builtin_auto_skills(&paths).await.unwrap();
        assert!(
            autos.len() >= 4,
            "expected ≥4 auto-inject entries, got {}",
            autos.len()
        );
        for item in &autos {
            assert!(
                item.location.starts_with("auto-inject/"),
                "location must start with auto-inject/, got {}",
                item.location
            );
            assert!(item.location.ends_with("/SKILL.md"));
            assert!(!item.description.is_empty());
        }
    }

    #[tokio::test]
    async fn embedded_reads_builtin_skill_content() {
        let tmp = TempDir::new().unwrap();
        let paths = make_embedded_paths(tmp.path());

        let content = read_builtin_skill(&paths, "auto-inject/cron/SKILL.md")
            .await
            .unwrap();
        assert!(!content.is_empty(), "embedded cron SKILL.md is empty");
        assert!(
            content.trim_start().starts_with("---"),
            "expected frontmatter, got: {}",
            content.chars().take(80).collect::<String>()
        );
    }

    #[tokio::test]
    async fn embedded_rejects_path_traversal() {
        let tmp = TempDir::new().unwrap();
        let paths = make_embedded_paths(tmp.path());

        let result = read_builtin_skill(&paths, "../etc/passwd").await;
        assert!(matches!(result, Err(ExtensionError::PathTraversal(_))));

        let result = read_builtin_skill(&paths, "auto-inject/../../secret").await;
        assert!(matches!(result, Err(ExtensionError::PathTraversal(_))));
    }

    #[tokio::test]
    async fn embedded_handles_missing_file() {
        let tmp = TempDir::new().unwrap();
        let paths = make_embedded_paths(tmp.path());

        let content = read_builtin_skill(&paths, "nonexistent/SKILL.md")
            .await
            .unwrap();
        assert!(content.is_empty());
    }

    #[tokio::test]
    async fn disk_override_reads_from_disk_not_embedded() {
        let tmp = TempDir::new().unwrap();
        let paths = make_disk_builtin_paths(tmp.path());
        let builtin_dir = disk_builtin_dir(&paths).to_path_buf();
        let auto_dir = builtin_dir.join(BUILTIN_AUTO_SKILLS_SUBDIR);
        create_skill_in_dir(&auto_dir, "fixture-only", "Fixture-only skill");

        let autos = list_builtin_auto_skills(&paths).await.unwrap();
        let names: Vec<&str> = autos.iter().map(|s| s.name.as_str()).collect();
        assert!(
            names.contains(&"fixture-only"),
            "disk override should reflect seeded skill; got {names:?}"
        );
        // Embedded skills (e.g. `cron`) must NOT leak into the disk view.
        assert!(
            !names.contains(&"cron"),
            "disk override must not include embedded skills"
        );
    }

    #[tokio::test]
    async fn list_skills_builtin_has_relative_location_from_embedded() {
        let tmp = TempDir::new().unwrap();
        let paths = make_embedded_paths(tmp.path());

        let skills = list_available_skills(&paths).await.unwrap();
        let builtins: Vec<_> = skills
            .iter()
            .filter(|s| s.source == SkillSource::Builtin)
            .collect();
        assert!(!builtins.is_empty(), "no builtin skills listed");
        for s in &builtins {
            let rel = s
                .relative_location
                .as_deref()
                .expect("builtin must have relative_location");
            assert!(
                rel.ends_with("/SKILL.md"),
                "relative_location must end in /SKILL.md, got {rel}"
            );
            assert!(
                s.location.contains(BUILTIN_SKILLS_VIEW_SUBDIR),
                "builtin location must live under the view dir, got {}",
                s.location
            );
            // Lazy materialization wrote SKILL.md to disk.
            assert!(
                std::path::Path::new(&s.location).exists(),
                "materialized view missing: {}",
                s.location
            );
        }
    }

    // -----------------------------------------------------------------------
    // Materialize / cleanup
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn materialize_creates_fresh_dir_each_call() {
        let tmp = TempDir::new().unwrap();
        let paths = make_embedded_paths(tmp.path());

        let dir = materialize_skills_for_agent(&paths, "conv-1", &[])
            .await
            .unwrap();
        assert!(dir.is_dir());
        // Drop a sentinel file; second call should wipe it.
        std::fs::write(dir.join("sentinel.txt"), b"stale").unwrap();
        let dir2 = materialize_skills_for_agent(&paths, "conv-1", &[])
            .await
            .unwrap();
        assert_eq!(dir, dir2);
        assert!(
            !dir2.join("sentinel.txt").exists(),
            "materialize must start fresh"
        );
    }

    #[tokio::test]
    async fn materialize_resolves_auto_inject_skill_by_name() {
        // Auto-inject skills are now only materialized when the caller
        // names them explicitly (see `ConversationService::create`
        // snapshot). Passing `&[]` yields an empty dir; passing a known
        // auto-inject name (e.g. "cron") writes it at the flat layout.
        let tmp = TempDir::new().unwrap();
        let paths = make_embedded_paths(tmp.path());

        let empty = materialize_skills_for_agent(&paths, "conv-empty", &[])
            .await
            .unwrap();
        assert!(empty.is_dir());
        assert!(
            !empty.join("cron").exists(),
            "auto-inject must NOT be implicit anymore"
        );

        let named = materialize_skills_for_agent(&paths, "conv-named", &["cron".to_owned()])
            .await
            .unwrap();
        assert!(
            named.join("cron").join(SKILL_MANIFEST_FILE).exists(),
            "named auto-inject skill not materialized"
        );
        assert!(
            !named.join(BUILTIN_AUTO_SKILLS_SUBDIR).exists(),
            "auto-inject/ wrapper should be flattened away; layout is flat"
        );
    }

    #[tokio::test]
    async fn materialize_includes_opt_in() {
        let tmp = TempDir::new().unwrap();
        let paths = make_embedded_paths(tmp.path());

        let dir = materialize_skills_for_agent(&paths, "conv-opt", &["mermaid".to_string()])
            .await
            .unwrap();
        assert!(
            dir.join("mermaid").join(SKILL_MANIFEST_FILE).exists(),
            "mermaid opt-in skill not materialized"
        );
        assert!(
            !dir.join("pdf").exists(),
            "non-requested opt-in skill must not be materialized"
        );
    }

    #[tokio::test]
    async fn materialize_handles_nonexistent_skill_name() {
        let tmp = TempDir::new().unwrap();
        let paths = make_embedded_paths(tmp.path());

        let dir =
            materialize_skills_for_agent(&paths, "conv-missing", &["no-such-skill".to_string()])
                .await
                .unwrap();
        // Unknown name is silently skipped; the dir exists but stays empty.
        assert!(dir.is_dir());
        assert!(!dir.join("no-such-skill").exists());
    }

    #[tokio::test]
    async fn materialize_rejects_bad_conversation_id() {
        let tmp = TempDir::new().unwrap();
        let paths = make_embedded_paths(tmp.path());

        let err = materialize_skills_for_agent(&paths, "../evil", &[])
            .await
            .unwrap_err();
        assert!(matches!(err, ExtensionError::PathTraversal(_)));
    }

    #[tokio::test]
    async fn cleanup_is_idempotent() {
        let tmp = TempDir::new().unwrap();
        let paths = make_embedded_paths(tmp.path());

        materialize_skills_for_agent(&paths, "conv-del", &[])
            .await
            .unwrap();
        cleanup_agent_skills(&paths, "conv-del").await.unwrap();
        // Second call should not error.
        cleanup_agent_skills(&paths, "conv-del").await.unwrap();
        assert!(
            !paths
                .data_dir
                .join(AGENT_SKILLS_SUBDIR)
                .join("conv-del")
                .exists()
        );
    }

    #[tokio::test]
    async fn orphan_cleanup_removes_stale_but_preserves_live() {
        let tmp = TempDir::new().unwrap();
        let paths = make_embedded_paths(tmp.path());

        // Seed: one live + one orphan conversation dir.
        let root = paths.data_dir.join(AGENT_SKILLS_SUBDIR);
        std::fs::create_dir_all(root.join("live-conv")).unwrap();
        std::fs::create_dir_all(root.join("orphan-conv")).unwrap();
        std::fs::write(root.join("live-conv/marker"), b"keep").unwrap();
        std::fs::write(root.join("orphan-conv/marker"), b"drop").unwrap();

        let removed = cleanup_orphan_agent_skills(&paths, |id| id == "live-conv")
            .await
            .unwrap();
        assert_eq!(removed, 1);
        assert!(root.join("live-conv").exists());
        assert!(!root.join("orphan-conv").exists());
    }

    #[tokio::test]
    async fn orphan_cleanup_missing_root_is_noop() {
        let tmp = TempDir::new().unwrap();
        let paths = make_embedded_paths(tmp.path());

        let removed = cleanup_orphan_agent_skills(&paths, |_| true).await.unwrap();
        assert_eq!(removed, 0);
    }
}
