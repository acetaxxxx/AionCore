use serde::{Deserialize, Serialize};

/// Universal paginated result for list APIs.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PaginatedResult<T> {
    pub items: Vec<T>,
    pub total: u64,
    pub has_more: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialize_camel_case() {
        let result = PaginatedResult {
            items: vec![1, 2, 3],
            total: 10,
            has_more: true,
        };
        let json = serde_json::to_value(&result).unwrap();
        assert_eq!(json["items"], serde_json::json!([1, 2, 3]));
        assert_eq!(json["total"], 10);
        assert_eq!(json["hasMore"], true);
    }

    #[test]
    fn test_empty_result() {
        let result: PaginatedResult<String> = PaginatedResult {
            items: vec![],
            total: 0,
            has_more: false,
        };
        let json = serde_json::to_value(&result).unwrap();
        assert_eq!(json["items"], serde_json::json!([]));
        assert_eq!(json["total"], 0);
        assert_eq!(json["hasMore"], false);
    }

    #[test]
    fn test_deserialize() {
        let json = r#"{"items":[1,2],"total":5,"hasMore":true}"#;
        let result: PaginatedResult<i32> = serde_json::from_str(json).unwrap();
        assert_eq!(result.items, vec![1, 2]);
        assert_eq!(result.total, 5);
        assert!(result.has_more);
    }
}
