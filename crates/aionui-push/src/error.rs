use aionui_db::DbError;

#[derive(Debug, thiserror::Error)]
pub enum PushError {
    #[error("invalid push subscription")]
    InvalidSubscription,
    #[error("invalid user scope")]
    InvalidUserScope,
    #[error("push subscription not found")]
    NotFound,
    #[error("push subscription storage failed")]
    Database(#[source] DbError),
}

impl From<DbError> for PushError {
    fn from(error: DbError) -> Self {
        if matches!(&error, DbError::NotFound(_)) {
            Self::NotFound
        } else {
            Self::Database(error)
        }
    }
}
