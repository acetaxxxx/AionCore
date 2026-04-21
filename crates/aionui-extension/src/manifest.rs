use crate::constants::RESERVED_NAME_PREFIXES;
use crate::error::ExtensionError;
use crate::types::ExtensionManifest;

/// Validate an extension manifest for required fields, name format, and version format.
pub fn validate_manifest(manifest: &ExtensionManifest) -> Result<(), ExtensionError> {
    validate_name(&manifest.name)?;
    validate_version(&manifest.version)?;
    Ok(())
}

/// Reject extension names that use reserved prefixes.
fn validate_name(name: &str) -> Result<(), ExtensionError> {
    if name.is_empty() {
        return Err(ExtensionError::ManifestValidation(
            "extension name must not be empty".into(),
        ));
    }

    let lower = name.to_lowercase();
    for prefix in RESERVED_NAME_PREFIXES {
        if lower.starts_with(prefix) {
            return Err(ExtensionError::ReservedNamePrefix {
                name: name.to_owned(),
                prefix: (*prefix).to_owned(),
            });
        }
    }
    Ok(())
}

/// Validate that the version string is valid semver.
fn validate_version(version: &str) -> Result<(), ExtensionError> {
    if version.is_empty() {
        return Err(ExtensionError::ManifestValidation(
            "extension version must not be empty".into(),
        ));
    }

    semver::Version::parse(version).map_err(|e| ExtensionError::InvalidVersion {
        version: version.to_owned(),
        reason: e.to_string(),
    })?;
    Ok(())
}

/// Parse and validate a manifest from JSON bytes.
pub fn parse_manifest(json_bytes: &[u8]) -> Result<ExtensionManifest, ExtensionError> {
    let manifest: ExtensionManifest = serde_json::from_slice(json_bytes)?;
    validate_manifest(&manifest)?;
    Ok(manifest)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // -- validate_manifest --

    #[test]
    fn test_valid_manifest() {
        let manifest = ExtensionManifest {
            name: "my-cool-ext".into(),
            version: "1.0.0".into(),
            display_name: None,
            description: None,
            author: None,
            license: None,
            homepage: None,
            icon: None,
            engine: None,
            api_version: None,
            dependencies: Default::default(),
            entry_point: None,
            permissions: None,
            contributes: None,
            lifecycle: None,
            i18n: None,
        };
        assert!(validate_manifest(&manifest).is_ok());
    }

    #[test]
    fn test_empty_name_rejected() {
        let manifest = ExtensionManifest {
            name: "".into(),
            version: "1.0.0".into(),
            display_name: None,
            description: None,
            author: None,
            license: None,
            homepage: None,
            icon: None,
            engine: None,
            api_version: None,
            dependencies: Default::default(),
            entry_point: None,
            permissions: None,
            contributes: None,
            lifecycle: None,
            i18n: None,
        };
        let err = validate_manifest(&manifest).unwrap_err();
        assert!(matches!(err, ExtensionError::ManifestValidation(_)));
    }

    #[test]
    fn test_reserved_prefix_aion() {
        let manifest = ExtensionManifest {
            name: "aion-my-ext".into(),
            version: "1.0.0".into(),
            display_name: None,
            description: None,
            author: None,
            license: None,
            homepage: None,
            icon: None,
            engine: None,
            api_version: None,
            dependencies: Default::default(),
            entry_point: None,
            permissions: None,
            contributes: None,
            lifecycle: None,
            i18n: None,
        };
        let err = validate_manifest(&manifest).unwrap_err();
        assert!(matches!(
            err,
            ExtensionError::ReservedNamePrefix { ref prefix, .. } if prefix == "aion-"
        ));
    }

    #[test]
    fn test_all_reserved_prefixes_rejected() {
        for prefix in RESERVED_NAME_PREFIXES {
            let name = format!("{prefix}test");
            let manifest = ExtensionManifest {
                name,
                version: "1.0.0".into(),
                display_name: None,
                description: None,
                author: None,
                license: None,
                homepage: None,
                icon: None,
                engine: None,
                api_version: None,
                dependencies: Default::default(),
                entry_point: None,
                permissions: None,
                contributes: None,
                lifecycle: None,
                i18n: None,
            };
            assert!(
                validate_manifest(&manifest).is_err(),
                "prefix '{prefix}' should be rejected"
            );
        }
    }

    #[test]
    fn test_reserved_prefix_case_insensitive() {
        let manifest = ExtensionManifest {
            name: "AION-upper".into(),
            version: "1.0.0".into(),
            display_name: None,
            description: None,
            author: None,
            license: None,
            homepage: None,
            icon: None,
            engine: None,
            api_version: None,
            dependencies: Default::default(),
            entry_point: None,
            permissions: None,
            contributes: None,
            lifecycle: None,
            i18n: None,
        };
        assert!(validate_manifest(&manifest).is_err());
    }

    #[test]
    fn test_empty_version_rejected() {
        let manifest = ExtensionManifest {
            name: "my-ext".into(),
            version: "".into(),
            display_name: None,
            description: None,
            author: None,
            license: None,
            homepage: None,
            icon: None,
            engine: None,
            api_version: None,
            dependencies: Default::default(),
            entry_point: None,
            permissions: None,
            contributes: None,
            lifecycle: None,
            i18n: None,
        };
        let err = validate_manifest(&manifest).unwrap_err();
        assert!(matches!(err, ExtensionError::ManifestValidation(_)));
    }

    #[test]
    fn test_invalid_semver_rejected() {
        let manifest = ExtensionManifest {
            name: "my-ext".into(),
            version: "not-semver".into(),
            display_name: None,
            description: None,
            author: None,
            license: None,
            homepage: None,
            icon: None,
            engine: None,
            api_version: None,
            dependencies: Default::default(),
            entry_point: None,
            permissions: None,
            contributes: None,
            lifecycle: None,
            i18n: None,
        };
        let err = validate_manifest(&manifest).unwrap_err();
        assert!(matches!(err, ExtensionError::InvalidVersion { .. }));
    }

    #[test]
    fn test_valid_semver_versions() {
        for version in &["0.0.1", "1.0.0", "1.2.3", "10.20.30", "1.0.0-alpha.1"] {
            let manifest = ExtensionManifest {
                name: "ext".into(),
                version: (*version).into(),
                display_name: None,
                description: None,
                author: None,
                license: None,
                homepage: None,
                icon: None,
                engine: None,
                api_version: None,
                dependencies: Default::default(),
                entry_point: None,
                permissions: None,
                contributes: None,
                lifecycle: None,
                i18n: None,
            };
            assert!(
                validate_manifest(&manifest).is_ok(),
                "version '{version}' should be accepted"
            );
        }
    }

    // -- parse_manifest --

    #[test]
    fn test_parse_manifest_valid() {
        let raw = json!({"name": "my-ext", "version": "1.0.0"});
        let bytes = serde_json::to_vec(&raw).unwrap();
        let manifest = parse_manifest(&bytes).unwrap();
        assert_eq!(manifest.name, "my-ext");
        assert_eq!(manifest.version, "1.0.0");
    }

    #[test]
    fn test_parse_manifest_invalid_json() {
        let err = parse_manifest(b"not json").unwrap_err();
        assert!(matches!(err, ExtensionError::JsonParse(_)));
    }

    #[test]
    fn test_parse_manifest_missing_name() {
        let raw = json!({"version": "1.0.0"});
        let bytes = serde_json::to_vec(&raw).unwrap();
        let err = parse_manifest(&bytes).unwrap_err();
        assert!(matches!(err, ExtensionError::JsonParse(_)));
    }

    #[test]
    fn test_parse_manifest_reserved_name() {
        let raw = json!({"name": "internal-test", "version": "1.0.0"});
        let bytes = serde_json::to_vec(&raw).unwrap();
        let err = parse_manifest(&bytes).unwrap_err();
        assert!(matches!(err, ExtensionError::ReservedNamePrefix { .. }));
    }
}
