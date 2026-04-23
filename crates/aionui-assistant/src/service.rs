//! Assistant service — stub for T1a, full implementation in T1b.
//!
//! Owns the three-source merge (built-in + user + extension), CRUD, state
//! overrides, and import semantics. For T1a this file only declares the
//! type shell so routes compile; every method returns `unimplemented!()`.

use std::sync::Arc;

use aionui_api_types::{
    AssistantResponse, CreateAssistantRequest, ImportAssistantsRequest, ImportAssistantsResult,
    SetAssistantStateRequest, UpdateAssistantRequest,
};
use aionui_common::AppError;
use aionui_db::{IAssistantOverrideRepository, IAssistantRepository};
use aionui_extension::ExtensionRegistry;

use crate::builtin::BuiltinAssistantRegistry;

/// Aggregated business logic for `/api/assistants/*` and rule/skill dispatch.
///
/// Full method bodies land in T1b; T1a provides only the type shell so the
/// router skeleton compiles.
pub struct AssistantService {
    #[allow(dead_code)]
    repo: Arc<dyn IAssistantRepository>,
    #[allow(dead_code)]
    override_repo: Arc<dyn IAssistantOverrideRepository>,
    #[allow(dead_code)]
    builtin: Arc<BuiltinAssistantRegistry>,
    #[allow(dead_code)]
    extension_registry: ExtensionRegistry,
}

impl AssistantService {
    pub fn new(
        repo: Arc<dyn IAssistantRepository>,
        override_repo: Arc<dyn IAssistantOverrideRepository>,
        builtin: Arc<BuiltinAssistantRegistry>,
        extension_registry: ExtensionRegistry,
    ) -> Self {
        Self {
            repo,
            override_repo,
            builtin,
            extension_registry,
        }
    }

    pub async fn list(&self) -> Result<Vec<AssistantResponse>, AppError> {
        unimplemented!("AssistantService::list lands in T1b")
    }

    pub async fn get(&self, _id: &str) -> Result<AssistantResponse, AppError> {
        unimplemented!("AssistantService::get lands in T1b")
    }

    pub async fn create(
        &self,
        _req: CreateAssistantRequest,
    ) -> Result<AssistantResponse, AppError> {
        unimplemented!("AssistantService::create lands in T1b")
    }

    pub async fn update(
        &self,
        _id: &str,
        _req: UpdateAssistantRequest,
    ) -> Result<AssistantResponse, AppError> {
        unimplemented!("AssistantService::update lands in T1b")
    }

    pub async fn delete(&self, _id: &str) -> Result<(), AppError> {
        unimplemented!("AssistantService::delete lands in T1b")
    }

    pub async fn set_state(
        &self,
        _id: &str,
        _req: SetAssistantStateRequest,
    ) -> Result<AssistantResponse, AppError> {
        unimplemented!("AssistantService::set_state lands in T1b")
    }

    pub async fn import(
        &self,
        _req: ImportAssistantsRequest,
    ) -> Result<ImportAssistantsResult, AppError> {
        unimplemented!("AssistantService::import lands in T1b")
    }
}
