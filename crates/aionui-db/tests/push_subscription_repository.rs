use aionui_db::{
    DbError, IPushSubscriptionRepository, IUserRepository, SqlitePushSubscriptionRepository, SqliteUserRepository,
    UpsertPushSubscriptionParams, init_database_memory,
};

const ENCRYPTION_KEY: [u8; 32] = [0x42; 32];

fn params(user_id: impl Into<String>, endpoint: &str, p256dh: &str, auth: &str) -> UpsertPushSubscriptionParams {
    UpsertPushSubscriptionParams {
        user_id: user_id.into(),
        endpoint: endpoint.to_owned(),
        p256dh: p256dh.to_owned(),
        auth: auth.to_owned(),
    }
}

#[tokio::test]
async fn endpoint_upsert_is_idempotent_and_encrypts_browser_material() {
    let db = init_database_memory().await.unwrap();
    let repository = SqlitePushSubscriptionRepository::new(db.pool().clone(), ENCRYPTION_KEY);

    let first = repository
        .upsert(params(
            "system_default_user",
            "https://push.example/subscription-a",
            "p256dh-secret-a",
            "auth-secret-a",
        ))
        .await
        .unwrap();

    let stored: (String, String, String) = sqlx::query_as(
        "SELECT endpoint_encrypted, p256dh_encrypted, auth_encrypted FROM push_subscriptions WHERE id = ?",
    )
    .bind(&first.id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert!(!stored.0.contains("push.example"));
    assert!(!stored.1.contains("p256dh-secret-a"));
    assert!(!stored.2.contains("auth-secret-a"));

    let replacement = repository
        .upsert(params(
            "system_default_user",
            "https://push.example/subscription-a",
            "p256dh-secret-b",
            "auth-secret-b",
        ))
        .await
        .unwrap();
    assert_eq!(first.id, replacement.id);

    let visible = repository.list_for_user("system_default_user").await.unwrap();
    assert_eq!(visible.len(), 1);
    assert_eq!(visible[0].p256dh, "p256dh-secret-b");
    assert_eq!(visible[0].auth, "auth-secret-b");
}

#[tokio::test]
async fn delete_is_owner_scoped_and_user_delete_cascades() {
    let db = init_database_memory().await.unwrap();
    let user_repository = SqliteUserRepository::new(db.pool().clone());
    let owner = user_repository.create_user("push-owner", "hash").await.unwrap();
    let other = user_repository.create_user("push-other", "hash").await.unwrap();
    let repository = SqlitePushSubscriptionRepository::new(db.pool().clone(), ENCRYPTION_KEY);

    let record = repository
        .upsert(params(
            &owner.id,
            "https://push.example/subscription-owner",
            "p256dh-secret",
            "auth-secret",
        ))
        .await
        .unwrap();

    assert!(matches!(
        repository.delete(&other.id, &record.id).await,
        Err(DbError::NotFound(_))
    ));
    assert_eq!(repository.list_for_user(&owner.id).await.unwrap().len(), 1);

    sqlx::query("DELETE FROM users WHERE id = ?")
        .bind(&owner.id)
        .execute(db.pool())
        .await
        .unwrap();
    assert!(repository.list_for_user(&owner.id).await.unwrap().is_empty());
}
