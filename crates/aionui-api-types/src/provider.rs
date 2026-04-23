use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use aionui_common::ProtocolType;

/// Model capability type discriminant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelType {
    Text,
    Vision,
    FunctionCalling,
    ImageGeneration,
    WebSearch,
    Reasoning,
    Embedding,
    Rerank,
    #[serde(rename = "excludeFromPrimary")]
    ExcludeFromPrimary,
}

/// A single model capability entry.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ModelCapability {
    #[serde(rename = "type")]
    pub capability_type: ModelType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_user_selected: Option<bool>,
}

/// Health status values for a model.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum HealthStatus {
    Unknown,
    Healthy,
    Unhealthy,
}

/// Per-model health check information.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ModelHealthStatus {
    pub status: HealthStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_check: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latency: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// AWS Bedrock authentication method.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BedrockAuthMethod {
    #[serde(rename = "accessKey")]
    AccessKey,
    Profile,
}

/// AWS Bedrock-specific configuration.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct BedrockConfig {
    pub auth_method: BedrockAuthMethod,
    pub region: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub access_key_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub secret_access_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub profile: Option<String>,
}

/// Provider response for `GET /api/providers` and single-provider endpoints.
///
/// The `api_key` field is always masked (e.g. `sk-ant-***abcd`).
/// Full API keys are never included in responses.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ProviderResponse {
    pub id: String,
    pub platform: String,
    pub name: String,
    pub base_url: String,
    /// Masked API key (e.g. `sk-ant-***abcd`).
    pub api_key: String,
    pub models: Vec<String>,
    pub enabled: bool,
    pub capabilities: Vec<ModelCapability>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context_limit: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model_protocols: Option<HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model_enabled: Option<HashMap<String, bool>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model_health: Option<HashMap<String, ModelHealthStatus>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bedrock_config: Option<BedrockConfig>,
    pub created_at: i64,
    pub updated_at: i64,
}

/// Request body for `POST /api/providers`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateProviderRequest {
    pub platform: String,
    pub name: String,
    pub base_url: String,
    /// Plain-text API key (supports comma/newline-separated multi-keys).
    pub api_key: String,
    #[serde(default)]
    pub models: Vec<String>,
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default)]
    pub capabilities: Vec<ModelCapability>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context_limit: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bedrock_config: Option<BedrockConfig>,
}

fn default_true() -> bool {
    true
}

/// Request body for `PUT /api/providers/:id`.
///
/// All fields are optional — partial update semantics.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct UpdateProviderRequest {
    pub platform: Option<String>,
    pub name: Option<String>,
    pub base_url: Option<String>,
    pub api_key: Option<String>,
    pub models: Option<Vec<String>>,
    pub enabled: Option<bool>,
    pub capabilities: Option<Vec<ModelCapability>>,
    pub context_limit: Option<i64>,
    pub model_protocols: Option<HashMap<String, String>>,
    pub model_enabled: Option<HashMap<String, bool>>,
    pub model_health: Option<HashMap<String, ModelHealthStatus>>,
    pub bedrock_config: Option<BedrockConfig>,
}

/// Request body for `POST /api/providers/:id/models`.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct FetchModelsRequest {
    #[serde(default)]
    pub try_fix: bool,
}

/// A model entry that can be either a bare ID string or an object with
/// id and name.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum ModelInfo {
    Id(String),
    Named { id: String, name: String },
}

/// Response for `POST /api/providers/:id/models`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct FetchModelsResponse {
    pub models: Vec<ModelInfo>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fixed_base_url: Option<String>,
}

/// Request body for `POST /api/providers/detect-protocol`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DetectProtocolRequest {
    pub base_url: String,
    /// Plain-text API key (supports multi-key).
    pub api_key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout: Option<u64>,
    #[serde(default)]
    pub test_all_keys: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub preferred_protocol: Option<ProtocolType>,
}

/// Suggestion type for protocol detection results.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SuggestionType {
    None,
    CheckKey,
    SwitchPlatform,
}

/// Actionable suggestion from protocol detection.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DetectionSuggestion {
    #[serde(rename = "type")]
    pub suggestion_type: SuggestionType,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub i18n_key: Option<String>,
}

/// Per-key test result in multi-key protocol detection.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct KeyTestResult {
    pub index: usize,
    pub masked_key: String,
    pub valid: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latency: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Aggregated result of testing multiple API keys.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct MultiKeyResult {
    pub total: usize,
    pub valid: usize,
    pub invalid: usize,
    pub details: Vec<KeyTestResult>,
}

/// Response for `POST /api/providers/detect-protocol`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ProtocolDetectionResponse {
    pub protocol: ProtocolType,
    pub confidence: u8,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fixed_base_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub models: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub suggestion: Option<DetectionSuggestion>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub multi_key_result: Option<MultiKeyResult>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // -- ModelType --

    #[test]
    fn test_model_type_serialization() {
        assert_eq!(
            serde_json::to_string(&ModelType::Text).unwrap(),
            r#""text""#
        );
        assert_eq!(
            serde_json::to_string(&ModelType::FunctionCalling).unwrap(),
            r#""function_calling""#
        );
        assert_eq!(
            serde_json::to_string(&ModelType::ExcludeFromPrimary).unwrap(),
            r#""excludeFromPrimary""#
        );
    }

    #[test]
    fn test_model_type_roundtrip() {
        for mt in [
            ModelType::Text,
            ModelType::Vision,
            ModelType::FunctionCalling,
            ModelType::ImageGeneration,
            ModelType::WebSearch,
            ModelType::Reasoning,
            ModelType::Embedding,
            ModelType::Rerank,
            ModelType::ExcludeFromPrimary,
        ] {
            let json = serde_json::to_string(&mt).unwrap();
            let parsed: ModelType = serde_json::from_str(&json).unwrap();
            assert_eq!(parsed, mt);
        }
    }

    // -- ModelCapability --

    #[test]
    fn test_model_capability_serialization() {
        let cap = ModelCapability {
            capability_type: ModelType::Vision,
            is_user_selected: Some(true),
        };
        let json = serde_json::to_value(&cap).unwrap();
        assert_eq!(json["type"], "vision");
        assert_eq!(json["isUserSelected"], true);
    }

    #[test]
    fn test_model_capability_optional_field_skipped() {
        let cap = ModelCapability {
            capability_type: ModelType::Text,
            is_user_selected: None,
        };
        let json = serde_json::to_value(&cap).unwrap();
        assert_eq!(json["type"], "text");
        assert!(json.get("isUserSelected").is_none());
    }

    // -- HealthStatus / ModelHealthStatus --

    #[test]
    fn test_health_status_serialization() {
        assert_eq!(
            serde_json::to_string(&HealthStatus::Healthy).unwrap(),
            r#""healthy""#
        );
        assert_eq!(
            serde_json::to_string(&HealthStatus::Unhealthy).unwrap(),
            r#""unhealthy""#
        );
        assert_eq!(
            serde_json::to_string(&HealthStatus::Unknown).unwrap(),
            r#""unknown""#
        );
    }

    #[test]
    fn test_model_health_status_full() {
        let status = ModelHealthStatus {
            status: HealthStatus::Healthy,
            last_check: Some(1712345678000),
            latency: Some(320),
            error: None,
        };
        let json = serde_json::to_value(&status).unwrap();
        assert_eq!(json["status"], "healthy");
        assert_eq!(json["lastCheck"], 1712345678000_i64);
        assert_eq!(json["latency"], 320);
        assert!(json.get("error").is_none());
    }

    #[test]
    fn test_model_health_status_minimal() {
        let status = ModelHealthStatus {
            status: HealthStatus::Unknown,
            last_check: None,
            latency: None,
            error: None,
        };
        let json = serde_json::to_value(&status).unwrap();
        assert_eq!(json["status"], "unknown");
        assert!(json.get("lastCheck").is_none());
    }

    // -- BedrockConfig --

    #[test]
    fn test_bedrock_config_access_key() {
        let cfg = BedrockConfig {
            auth_method: BedrockAuthMethod::AccessKey,
            region: "us-east-1".into(),
            access_key_id: Some("AKIAIOSFODNN7".into()),
            secret_access_key: Some("wJalrXUtnFEMI/K7MDENG".into()),
            profile: None,
        };
        let json = serde_json::to_value(&cfg).unwrap();
        assert_eq!(json["authMethod"], "accessKey");
        assert_eq!(json["region"], "us-east-1");
        assert_eq!(json["accessKeyId"], "AKIAIOSFODNN7");
        assert!(json.get("profile").is_none());
    }

    #[test]
    fn test_bedrock_config_profile() {
        let cfg = BedrockConfig {
            auth_method: BedrockAuthMethod::Profile,
            region: "eu-west-1".into(),
            access_key_id: None,
            secret_access_key: None,
            profile: Some("my-profile".into()),
        };
        let json = serde_json::to_value(&cfg).unwrap();
        assert_eq!(json["authMethod"], "profile");
        assert_eq!(json["profile"], "my-profile");
        assert!(json.get("accessKeyId").is_none());
    }

    // -- ProviderResponse --

    #[test]
    fn test_provider_response_serialization() {
        let resp = ProviderResponse {
            id: "uuid-xxx".into(),
            platform: "anthropic".into(),
            name: "Anthropic".into(),
            base_url: "https://api.anthropic.com".into(),
            api_key: "sk-ant-***abcd".into(),
            models: vec!["claude-sonnet-4-20250514".into()],
            enabled: true,
            capabilities: vec![ModelCapability {
                capability_type: ModelType::Text,
                is_user_selected: None,
            }],
            context_limit: None,
            model_protocols: None,
            model_enabled: Some(HashMap::from([("claude-sonnet-4-20250514".into(), true)])),
            model_health: None,
            bedrock_config: None,
            created_at: 1712345678000,
            updated_at: 1712345678000,
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["id"], "uuid-xxx");
        assert_eq!(json["platform"], "anthropic");
        assert_eq!(json["apiKey"], "sk-ant-***abcd");
        assert_eq!(json["baseUrl"], "https://api.anthropic.com");
        assert_eq!(json["models"][0], "claude-sonnet-4-20250514");
        assert_eq!(json["modelEnabled"]["claude-sonnet-4-20250514"], true);
        assert!(json.get("contextLimit").is_none());
        assert!(json.get("modelProtocols").is_none());
        assert!(json.get("bedrockConfig").is_none());
    }

    // -- CreateProviderRequest --

    #[test]
    fn test_create_provider_request_required_fields() {
        let raw = json!({
            "platform": "anthropic",
            "name": "Anthropic",
            "baseUrl": "https://api.anthropic.com",
            "apiKey": "sk-ant-api03-test"
        });
        let req: CreateProviderRequest = serde_json::from_value(raw).unwrap();
        assert_eq!(req.platform, "anthropic");
        assert_eq!(req.name, "Anthropic");
        assert_eq!(req.base_url, "https://api.anthropic.com");
        assert_eq!(req.api_key, "sk-ant-api03-test");
        assert!(req.models.is_empty());
        assert!(req.enabled);
        assert!(req.capabilities.is_empty());
        assert!(req.context_limit.is_none());
        assert!(req.bedrock_config.is_none());
    }

    #[test]
    fn test_create_provider_request_missing_required_field() {
        let raw = json!({"platform": "anthropic", "name": "Anthropic"});
        let result = serde_json::from_value::<CreateProviderRequest>(raw);
        assert!(result.is_err());
    }

    #[test]
    fn test_create_provider_request_with_optional_fields() {
        let raw = json!({
            "platform": "bedrock",
            "name": "AWS Bedrock",
            "baseUrl": "https://bedrock.us-east-1.amazonaws.com",
            "apiKey": "",
            "models": ["anthropic.claude-3-sonnet"],
            "enabled": false,
            "capabilities": [{"type": "text"}, {"type": "vision", "isUserSelected": true}],
            "contextLimit": 200000,
            "bedrockConfig": {
                "authMethod": "accessKey",
                "region": "us-east-1",
                "accessKeyId": "AKIA...",
                "secretAccessKey": "secret"
            }
        });
        let req: CreateProviderRequest = serde_json::from_value(raw).unwrap();
        assert!(!req.enabled);
        assert_eq!(req.models.len(), 1);
        assert_eq!(req.capabilities.len(), 2);
        assert_eq!(req.context_limit, Some(200000));
        assert!(req.bedrock_config.is_some());
    }

    // -- UpdateProviderRequest --

    #[test]
    fn test_update_provider_request_partial() {
        let raw = json!({"name": "New Name"});
        let req: UpdateProviderRequest = serde_json::from_value(raw).unwrap();
        assert_eq!(req.name.as_deref(), Some("New Name"));
        assert!(req.platform.is_none());
        assert!(req.api_key.is_none());
    }

    #[test]
    fn test_update_provider_request_empty() {
        let raw = json!({});
        let req: UpdateProviderRequest = serde_json::from_value(raw).unwrap();
        assert!(req.platform.is_none());
        assert!(req.name.is_none());
    }

    // -- FetchModelsRequest --

    #[test]
    fn test_fetch_models_request_default() {
        let raw = json!({});
        let req: FetchModelsRequest = serde_json::from_value(raw).unwrap();
        assert!(!req.try_fix);
    }

    #[test]
    fn test_fetch_models_request_with_try_fix() {
        let raw = json!({"tryFix": true});
        let req: FetchModelsRequest = serde_json::from_value(raw).unwrap();
        assert!(req.try_fix);
    }

    // -- ModelInfo --

    #[test]
    fn test_model_info_string() {
        let info: ModelInfo = serde_json::from_value(json!("gpt-4")).unwrap();
        assert_eq!(info, ModelInfo::Id("gpt-4".into()));
    }

    #[test]
    fn test_model_info_named() {
        let info: ModelInfo =
            serde_json::from_value(json!({"id": "gpt-4", "name": "GPT-4"})).unwrap();
        assert_eq!(
            info,
            ModelInfo::Named {
                id: "gpt-4".into(),
                name: "GPT-4".into()
            }
        );
    }

    #[test]
    fn test_model_info_mixed_array() {
        let raw = json!(["gpt-4", {"id": "claude-3", "name": "Claude 3"}]);
        let models: Vec<ModelInfo> = serde_json::from_value(raw).unwrap();
        assert_eq!(models.len(), 2);
        assert_eq!(models[0], ModelInfo::Id("gpt-4".into()));
    }

    // -- FetchModelsResponse --

    #[test]
    fn test_fetch_models_response_without_fixed_url() {
        let resp = FetchModelsResponse {
            models: vec![
                ModelInfo::Id("claude-sonnet-4-20250514".into()),
                ModelInfo::Id("claude-opus-4-20250514".into()),
            ],
            fixed_base_url: None,
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["models"].as_array().unwrap().len(), 2);
        assert!(json.get("fixedBaseUrl").is_none());
    }

    #[test]
    fn test_fetch_models_response_with_fixed_url() {
        let resp = FetchModelsResponse {
            models: vec![ModelInfo::Id("gpt-4".into())],
            fixed_base_url: Some("https://api.openai.com/v1".into()),
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["fixedBaseUrl"], "https://api.openai.com/v1");
    }

    // -- DetectProtocolRequest --

    #[test]
    fn test_detect_protocol_request_required_only() {
        let raw = json!({
            "baseUrl": "https://api.example.com",
            "apiKey": "sk-xxx"
        });
        let req: DetectProtocolRequest = serde_json::from_value(raw).unwrap();
        assert_eq!(req.base_url, "https://api.example.com");
        assert_eq!(req.api_key, "sk-xxx");
        assert!(req.timeout.is_none());
        assert!(!req.test_all_keys);
        assert!(req.preferred_protocol.is_none());
    }

    #[test]
    fn test_detect_protocol_request_full() {
        let raw = json!({
            "baseUrl": "https://api.anthropic.com",
            "apiKey": "sk-ant-xxx",
            "timeout": 10000,
            "testAllKeys": true,
            "preferredProtocol": "anthropic"
        });
        let req: DetectProtocolRequest = serde_json::from_value(raw).unwrap();
        assert_eq!(req.timeout, Some(10000));
        assert!(req.test_all_keys);
        assert_eq!(req.preferred_protocol, Some(ProtocolType::Anthropic));
    }

    // -- SuggestionType --

    #[test]
    fn test_suggestion_type_serialization() {
        assert_eq!(
            serde_json::to_string(&SuggestionType::None).unwrap(),
            r#""none""#
        );
        assert_eq!(
            serde_json::to_string(&SuggestionType::CheckKey).unwrap(),
            r#""check_key""#
        );
        assert_eq!(
            serde_json::to_string(&SuggestionType::SwitchPlatform).unwrap(),
            r#""switch_platform""#
        );
    }

    // -- ProtocolDetectionResponse --

    #[test]
    fn test_protocol_detection_response_minimal() {
        let resp = ProtocolDetectionResponse {
            protocol: ProtocolType::Unknown,
            confidence: 0,
            fixed_base_url: None,
            models: None,
            suggestion: None,
            multi_key_result: None,
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["protocol"], "unknown");
        assert_eq!(json["confidence"], 0);
        assert!(json.get("fixedBaseUrl").is_none());
        assert!(json.get("models").is_none());
        assert!(json.get("suggestion").is_none());
        assert!(json.get("multiKeyResult").is_none());
    }

    #[test]
    fn test_protocol_detection_response_full() {
        let resp = ProtocolDetectionResponse {
            protocol: ProtocolType::Anthropic,
            confidence: 95,
            fixed_base_url: None,
            models: Some(vec!["claude-sonnet-4-20250514".into()]),
            suggestion: Some(DetectionSuggestion {
                suggestion_type: SuggestionType::None,
                message: "Detected Anthropic protocol".into(),
                i18n_key: Some("settings.protocolDetected".into()),
            }),
            multi_key_result: None,
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["protocol"], "anthropic");
        assert_eq!(json["confidence"], 95);
        assert_eq!(json["suggestion"]["type"], "none");
        assert_eq!(json["suggestion"]["message"], "Detected Anthropic protocol");
        assert_eq!(json["suggestion"]["i18nKey"], "settings.protocolDetected");
    }

    #[test]
    fn test_protocol_detection_response_multi_key() {
        let resp = ProtocolDetectionResponse {
            protocol: ProtocolType::OpenAI,
            confidence: 90,
            fixed_base_url: None,
            models: None,
            suggestion: None,
            multi_key_result: Some(MultiKeyResult {
                total: 3,
                valid: 2,
                invalid: 1,
                details: vec![
                    KeyTestResult {
                        index: 0,
                        masked_key: "sk-***abcd".into(),
                        valid: true,
                        latency: Some(320),
                        error: None,
                    },
                    KeyTestResult {
                        index: 1,
                        masked_key: "sk-***efgh".into(),
                        valid: true,
                        latency: Some(280),
                        error: None,
                    },
                    KeyTestResult {
                        index: 2,
                        masked_key: "sk-***ijkl".into(),
                        valid: false,
                        latency: Some(150),
                        error: Some("Invalid API key".into()),
                    },
                ],
            }),
        };
        let json = serde_json::to_value(&resp).unwrap();
        let mkr = &json["multiKeyResult"];
        assert_eq!(mkr["total"], 3);
        assert_eq!(mkr["valid"], 2);
        assert_eq!(mkr["invalid"], 1);
        assert_eq!(mkr["details"].as_array().unwrap().len(), 3);
        assert_eq!(mkr["details"][0]["maskedKey"], "sk-***abcd");
        assert_eq!(mkr["details"][2]["error"], "Invalid API key");
    }
}
