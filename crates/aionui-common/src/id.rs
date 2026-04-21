use uuid::Uuid;

/// Generate a time-ordered globally unique ID (UUID v7).
pub fn generate_id() -> String {
    Uuid::now_v7().to_string()
}

/// Generate a prefixed ID (e.g., "cron_01234...", "mcp_01234...").
pub fn generate_prefixed_id(prefix: &str) -> String {
    format!("{prefix}_{}", Uuid::now_v7())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn test_generate_id_is_valid_uuid() {
        let id = generate_id();
        assert!(Uuid::parse_str(&id).is_ok());
    }

    #[test]
    fn test_generate_id_is_v7() {
        let id = generate_id();
        let uuid = Uuid::parse_str(&id).unwrap();
        assert_eq!(uuid.get_version_num(), 7);
    }

    #[test]
    fn test_generate_prefixed_id_format() {
        let id = generate_prefixed_id("msg");
        assert!(id.starts_with("msg_"));
        let uuid_part = &id[4..];
        assert!(Uuid::parse_str(uuid_part).is_ok());
    }

    #[test]
    fn test_id_uniqueness() {
        let ids: HashSet<String> = (0..1000).map(|_| generate_id()).collect();
        assert_eq!(ids.len(), 1000);
    }

    #[test]
    fn test_id_time_ordering() {
        let id1 = generate_id();
        let id2 = generate_id();
        assert!(id2 >= id1);
    }

    #[test]
    fn test_long_prefix() {
        let prefix = "a".repeat(1000);
        let id = generate_prefixed_id(&prefix);
        assert!(id.starts_with(&prefix));
    }
}
