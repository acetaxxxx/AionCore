//! Runtime gates for additive conversation diagnostics.

/// Enables per-turn provider token snapshots written to the diagnostic sidecar.
///
/// The feature is deliberately opt-in because these records are additive
/// persistent telemetry and are not required by the conversation UI. Accepts
/// the common explicit true values; every other value, including an unset
/// variable, keeps the feature disabled.
pub(crate) const PROVIDER_USAGE_DIAGNOSTICS_ENV: &str = "AIONUI_ENABLE_PROVIDER_USAGE_DIAGNOSTICS";
/// Enables persisted context attribution records emitted by conversation turns.
pub const CONTEXT_ATTRIBUTION_ENV: &str = "AIONUI_ENABLE_CONTEXT_ATTRIBUTION";

pub(crate) fn provider_usage_diagnostics_enabled() -> bool {
    provider_usage_diagnostics_enabled_value(std::env::var(PROVIDER_USAGE_DIAGNOSTICS_ENV).ok().as_deref())
}

/// Returns whether persisted context attribution is explicitly enabled.
pub fn context_attribution_enabled() -> bool {
    context_attribution_enabled_value(std::env::var(CONTEXT_ATTRIBUTION_ENV).ok().as_deref())
}

fn context_attribution_enabled_value(value: Option<&str>) -> bool {
    value.is_some_and(|value| matches!(value.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
}

fn provider_usage_diagnostics_enabled_value(value: Option<&str>) -> bool {
    value.is_some_and(|value| matches!(value.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
}

#[cfg(test)]
mod tests {
    use super::{context_attribution_enabled_value, provider_usage_diagnostics_enabled_value};

    #[test]
    fn provider_usage_diagnostics_is_opt_in() {
        assert!(!provider_usage_diagnostics_enabled_value(None));
        assert!(!provider_usage_diagnostics_enabled_value(Some("0")));
        assert!(!provider_usage_diagnostics_enabled_value(Some("false")));
        assert!(provider_usage_diagnostics_enabled_value(Some("1")));
        assert!(provider_usage_diagnostics_enabled_value(Some(" TRUE ")));
    }

    #[test]
    fn context_attribution_is_opt_in_separately() {
        assert!(!context_attribution_enabled_value(None));
        assert!(!context_attribution_enabled_value(Some("0")));
        assert!(context_attribution_enabled_value(Some("on")));
    }
}
