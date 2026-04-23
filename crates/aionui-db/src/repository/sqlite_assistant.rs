//! SQLite-backed assistant repositories.
//!
//! T1a scaffolds the types so the dependency graph compiles; every method
//! body is `unimplemented!()`. Real SQL lands in T1b.

use sqlx::SqlitePool;

use crate::error::DbError;
use crate::models::{
    AssistantOverrideRow, AssistantRow, CreateAssistantParams, UpdateAssistantParams,
    UpsertOverrideParams,
};
use crate::repository::assistant::{IAssistantOverrideRepository, IAssistantRepository};

/// SQLite-backed implementation of [`IAssistantRepository`].
#[derive(Clone, Debug)]
pub struct SqliteAssistantRepository {
    #[allow(dead_code)]
    pool: SqlitePool,
}

impl SqliteAssistantRepository {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

#[async_trait::async_trait]
impl IAssistantRepository for SqliteAssistantRepository {
    async fn list(&self) -> Result<Vec<AssistantRow>, DbError> {
        unimplemented!("SqliteAssistantRepository::list lands in T1b")
    }

    async fn get(&self, _id: &str) -> Result<Option<AssistantRow>, DbError> {
        unimplemented!("SqliteAssistantRepository::get lands in T1b")
    }

    async fn create(&self, _params: &CreateAssistantParams<'_>) -> Result<AssistantRow, DbError> {
        unimplemented!("SqliteAssistantRepository::create lands in T1b")
    }

    async fn update(
        &self,
        _id: &str,
        _params: &UpdateAssistantParams<'_>,
    ) -> Result<Option<AssistantRow>, DbError> {
        unimplemented!("SqliteAssistantRepository::update lands in T1b")
    }

    async fn delete(&self, _id: &str) -> Result<bool, DbError> {
        unimplemented!("SqliteAssistantRepository::delete lands in T1b")
    }

    async fn upsert(&self, _params: &CreateAssistantParams<'_>) -> Result<AssistantRow, DbError> {
        unimplemented!("SqliteAssistantRepository::upsert lands in T1b")
    }
}

/// SQLite-backed implementation of [`IAssistantOverrideRepository`].
#[derive(Clone, Debug)]
pub struct SqliteAssistantOverrideRepository {
    #[allow(dead_code)]
    pool: SqlitePool,
}

impl SqliteAssistantOverrideRepository {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

#[async_trait::async_trait]
impl IAssistantOverrideRepository for SqliteAssistantOverrideRepository {
    async fn get(&self, _assistant_id: &str) -> Result<Option<AssistantOverrideRow>, DbError> {
        unimplemented!("SqliteAssistantOverrideRepository::get lands in T1b")
    }

    async fn get_all(&self) -> Result<Vec<AssistantOverrideRow>, DbError> {
        unimplemented!("SqliteAssistantOverrideRepository::get_all lands in T1b")
    }

    async fn upsert(
        &self,
        _params: &UpsertOverrideParams<'_>,
    ) -> Result<AssistantOverrideRow, DbError> {
        unimplemented!("SqliteAssistantOverrideRepository::upsert lands in T1b")
    }

    async fn delete(&self, _assistant_id: &str) -> Result<bool, DbError> {
        unimplemented!("SqliteAssistantOverrideRepository::delete lands in T1b")
    }

    async fn delete_orphans(&self, _valid_ids: &[&str]) -> Result<u64, DbError> {
        unimplemented!("SqliteAssistantOverrideRepository::delete_orphans lands in T1b")
    }
}
