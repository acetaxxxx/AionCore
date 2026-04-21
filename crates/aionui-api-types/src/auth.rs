use serde::{Deserialize, Serialize};

/// Public user info returned in API responses.
///
/// Contains only the fields safe to expose to clients.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PublicUser {
    pub id: String,
    pub username: String,
}

/// Login request body for `POST /login`.
#[derive(Debug, Deserialize)]
pub struct LoginRequest {
    pub username: String,
    pub password: String,
}

/// Login success response for `POST /login` and `POST /api/auth/qr-login`.
#[derive(Debug, Serialize, Deserialize)]
pub struct LoginResponse {
    pub success: bool,
    pub message: String,
    pub user: PublicUser,
    pub token: String,
}

impl LoginResponse {
    pub fn new(user: PublicUser, token: String) -> Self {
        Self {
            success: true,
            message: "Login successful".to_owned(),
            user,
            token,
        }
    }
}

/// Change password request body for `POST /api/auth/change-password`.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChangePasswordRequest {
    pub current_password: String,
    pub new_password: String,
}

/// QR code login request body for `POST /api/auth/qr-login`.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QrLoginRequest {
    pub qr_token: String,
}

/// Auth status response for `GET /api/auth/status`.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AuthStatusResponse {
    pub success: bool,
    pub needs_setup: bool,
    pub user_count: u64,
    pub is_authenticated: bool,
}

/// Refresh token request body for `POST /api/auth/refresh`.
#[derive(Debug, Deserialize)]
pub struct RefreshTokenRequest {
    pub token: String,
}

/// User info response for `GET /api/auth/user`.
#[derive(Debug, Serialize)]
pub struct UserInfoResponse {
    pub success: bool,
    pub user: PublicUser,
}

/// Refresh token response for `POST /api/auth/refresh`.
#[derive(Debug, Serialize)]
pub struct RefreshResponse {
    pub success: bool,
    pub token: String,
}

/// WebSocket token response for `GET /api/ws-token`.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WsTokenResponse {
    pub success: bool,
    pub ws_token: String,
    pub expires_in: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_public_user_serialization() {
        let user = PublicUser {
            id: "auth_1712345678_abc".into(),
            username: "admin".into(),
        };
        let json = serde_json::to_value(&user).unwrap();
        assert_eq!(json["id"], "auth_1712345678_abc");
        assert_eq!(json["username"], "admin");
    }

    #[test]
    fn test_login_request_deserialization() {
        let raw = r#"{"username":"admin","password":"secret123"}"#;
        let req: LoginRequest = serde_json::from_str(raw).unwrap();
        assert_eq!(req.username, "admin");
        assert_eq!(req.password, "secret123");
    }

    #[test]
    fn test_login_request_missing_field() {
        let raw = r#"{"username":"admin"}"#;
        let result = serde_json::from_str::<LoginRequest>(raw);
        assert!(result.is_err());
    }

    #[test]
    fn test_login_response_new() {
        let user = PublicUser {
            id: "user_1".into(),
            username: "admin".into(),
        };
        let resp = LoginResponse::new(user.clone(), "jwt_token".into());
        assert!(resp.success);
        assert_eq!(resp.message, "Login successful");
        assert_eq!(resp.user, user);
        assert_eq!(resp.token, "jwt_token");
    }

    #[test]
    fn test_login_response_serialization() {
        let resp = LoginResponse::new(
            PublicUser {
                id: "auth_123".into(),
                username: "admin".into(),
            },
            "eyJhbGciOi".into(),
        );
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["success"], true);
        assert_eq!(json["message"], "Login successful");
        assert_eq!(json["user"]["id"], "auth_123");
        assert_eq!(json["user"]["username"], "admin");
        assert_eq!(json["token"], "eyJhbGciOi");
    }

    #[test]
    fn test_change_password_request_camel_case() {
        let raw = r#"{"currentPassword":"old123","newPassword":"new456"}"#;
        let req: ChangePasswordRequest = serde_json::from_str(raw).unwrap();
        assert_eq!(req.current_password, "old123");
        assert_eq!(req.new_password, "new456");
    }

    #[test]
    fn test_change_password_request_snake_case_rejected() {
        let raw = r#"{"current_password":"old","new_password":"new"}"#;
        let result = serde_json::from_str::<ChangePasswordRequest>(raw);
        assert!(result.is_err());
    }

    #[test]
    fn test_qr_login_request_camel_case() {
        let raw = r#"{"qrToken":"abc123"}"#;
        let req: QrLoginRequest = serde_json::from_str(raw).unwrap();
        assert_eq!(req.qr_token, "abc123");
    }

    #[test]
    fn test_qr_login_request_snake_case_rejected() {
        let raw = r#"{"qr_token":"abc"}"#;
        let result = serde_json::from_str::<QrLoginRequest>(raw);
        assert!(result.is_err());
    }

    #[test]
    fn test_auth_status_response_camel_case() {
        let resp = AuthStatusResponse {
            success: true,
            needs_setup: true,
            user_count: 0,
            is_authenticated: false,
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["success"], true);
        assert_eq!(json["needsSetup"], true);
        assert_eq!(json["userCount"], 0);
        assert_eq!(json["isAuthenticated"], false);
        // Verify camelCase keys exist, not snake_case
        assert!(json.get("needs_setup").is_none());
        assert!(json.get("user_count").is_none());
        assert!(json.get("is_authenticated").is_none());
    }

    #[test]
    fn test_auth_status_response_deserialization() {
        let raw = json!({
            "success": true,
            "needsSetup": false,
            "userCount": 3,
            "isAuthenticated": true
        });
        let resp: AuthStatusResponse = serde_json::from_value(raw).unwrap();
        assert!(resp.success);
        assert!(!resp.needs_setup);
        assert_eq!(resp.user_count, 3);
        assert!(resp.is_authenticated);
    }

    #[test]
    fn test_refresh_token_request_deserialization() {
        let raw = r#"{"token":"eyJhbGciOiJIUzI1NiJ9"}"#;
        let req: RefreshTokenRequest = serde_json::from_str(raw).unwrap();
        assert_eq!(req.token, "eyJhbGciOiJIUzI1NiJ9");
    }

    #[test]
    fn test_refresh_token_request_missing_token() {
        let raw = r#"{}"#;
        let result = serde_json::from_str::<RefreshTokenRequest>(raw);
        assert!(result.is_err());
    }
}
