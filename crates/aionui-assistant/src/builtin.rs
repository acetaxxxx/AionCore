//! Built-in assistant registry — stub for T1a, implementation in T1b.

use std::collections::HashMap;
use std::path::PathBuf;

use serde::Deserialize;

/// Single built-in assistant entry, loaded from `assistants.json`.
///
/// Full implementation (loader, locale resolution, asset dispatch) lands
/// in T1b.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BuiltinAssistant {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub name_i18n: HashMap<String, String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub description_i18n: HashMap<String, String>,
    #[serde(default)]
    pub avatar: Option<String>,
    pub preset_agent_type: String,
    #[serde(default)]
    pub enabled_skills: Vec<String>,
    #[serde(default)]
    pub custom_skill_names: Vec<String>,
    #[serde(default)]
    pub disabled_builtin_skills: Vec<String>,
    #[serde(default)]
    pub rule_file: Option<String>,
    #[serde(default)]
    pub skill_file: Option<String>,
    #[serde(default)]
    pub prompts: Vec<String>,
    #[serde(default)]
    pub prompts_i18n: HashMap<String, Vec<String>>,
    #[serde(default)]
    pub models: Vec<String>,
}

/// In-memory registry of built-in assistants. T1a scaffolds the type; T1b
/// adds the on-disk loader and dispatch helpers.
pub struct BuiltinAssistantRegistry {
    assistants: HashMap<String, BuiltinAssistant>,
    assets_dir: PathBuf,
}

impl BuiltinAssistantRegistry {
    /// Construct an empty registry. Used as a safe fallback and for tests.
    pub fn empty() -> Self {
        Self {
            assistants: HashMap::new(),
            assets_dir: PathBuf::new(),
        }
    }

    /// Return `true` if the registry contains an assistant with the given id.
    pub fn has(&self, id: &str) -> bool {
        self.assistants.contains_key(id)
    }

    /// Lookup by id.
    pub fn get(&self, id: &str) -> Option<&BuiltinAssistant> {
        self.assistants.get(id)
    }

    /// Iterator over all built-in entries.
    pub fn all(&self) -> impl Iterator<Item = &BuiltinAssistant> {
        self.assistants.values()
    }

    /// Assets directory root (resolved by the loader in T1b).
    pub fn assets_dir(&self) -> &std::path::Path {
        &self.assets_dir
    }
}

impl Default for BuiltinAssistantRegistry {
    fn default() -> Self {
        Self::empty()
    }
}
