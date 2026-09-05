//! Cloudflare Access assertion verification.
//!
//! Access signs the assertion with an RSA key published by the configured team
//! domain.  This module keeps the verifier behind a small async port so auth
//! middleware tests can exercise provisioning without trusting an unverified
//! header or making network calls.

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header};
use reqwest::Client;
use serde::Deserialize;
use tokio::sync::RwLock;

const JWKS_TTL: Duration = Duration::from_secs(10 * 60);

/// Header emitted by Cloudflare Access for a request that passed Access.
pub const CF_ACCESS_JWT_HEADER: &str = "cf-access-jwt-assertion";

/// Identity claims consumed by AionCore.  `sub` is the only stable identity
/// key; email is metadata and must never become a database primary key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloudflareIdentity {
    pub subject: String,
    pub email: Option<String>,
}

/// Configuration for one Cloudflare Access team/application.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloudflareAccessConfig {
    pub team_domain: String,
    pub audience: String,
    pub issuer: String,
    pub jwks_url: String,
}

impl CloudflareAccessConfig {
    pub fn new(team_domain: impl Into<String>, audience: impl Into<String>) -> Result<Self, CloudflareAccessError> {
        let raw_domain = team_domain.into().trim().trim_end_matches('/').to_owned();
        let domain = raw_domain
            .strip_prefix("https://")
            .or_else(|| raw_domain.strip_prefix("http://"))
            .unwrap_or(&raw_domain)
            .trim_end_matches('/');
        let audience = audience.into().trim().to_owned();
        if domain.is_empty() || audience.is_empty() || domain.contains('/') {
            return Err(CloudflareAccessError::Configuration(
                "Cloudflare Access team domain and audience must be non-empty host/value strings".to_owned(),
            ));
        }

        Ok(Self {
            team_domain: domain.to_owned(),
            issuer: format!("https://{domain}"),
            jwks_url: format!("https://{domain}/cdn-cgi/access/certs"),
            audience,
        })
    }

    /// Read the pair of deployment variables. A partially configured pair is
    /// rejected at startup rather than silently disabling origin verification.
    pub fn from_env() -> Result<Option<Self>, CloudflareAccessError> {
        let domain = std::env::var("AIONUI_CF_ACCESS_TEAM_DOMAIN").ok();
        let audience = std::env::var("AIONUI_CF_ACCESS_AUDIENCE").ok();
        match (domain, audience) {
            (None, None) => Ok(None),
            (Some(domain), Some(audience)) => Self::new(domain, audience).map(Some),
            _ => Err(CloudflareAccessError::Configuration(
                "AIONUI_CF_ACCESS_TEAM_DOMAIN and AIONUI_CF_ACCESS_AUDIENCE must be configured together".to_owned(),
            )),
        }
    }
}

/// Safe errors for the caller.  Raw assertion/JWKS content is intentionally
/// excluded so malformed external input cannot leak into production logs.
#[derive(Debug, Clone, thiserror::Error)]
pub enum CloudflareAccessError {
    #[error("invalid Cloudflare Access configuration: {0}")]
    Configuration(String),
    #[error("invalid Cloudflare Access assertion")]
    InvalidAssertion,
    #[error("Cloudflare Access key set unavailable")]
    JwksUnavailable,
    #[error("Cloudflare Access assertion verification failed")]
    VerificationFailed,
}

/// Port used by authentication middleware.  Keeping the network verifier
/// behind this trait makes bad-path and isolation tests deterministic.
#[async_trait]
pub trait CloudflareAccessAuthenticator: Send + Sync {
    async fn verify(&self, assertion: &str) -> Result<CloudflareIdentity, CloudflareAccessError>;
}

#[derive(Debug, Clone, Deserialize)]
struct CloudflareClaims {
    sub: String,
    #[serde(default)]
    email: Option<String>,
    exp: u64,
    iss: String,
    #[serde(rename = "aud")]
    _aud: serde_json::Value,
}

#[derive(Debug, Clone, Deserialize)]
struct JwkSet {
    keys: Vec<Jwk>,
}

#[derive(Debug, Clone, Deserialize)]
struct Jwk {
    kty: String,
    kid: String,
    #[serde(default)]
    alg: Option<String>,
    n: String,
    e: String,
}

struct CachedJwks {
    loaded_at: Instant,
    keys: Arc<Vec<Jwk>>,
}

/// Production Cloudflare Access verifier. Keys are cached briefly and
/// reloaded on a key-id miss to tolerate normal Access key rotation.
pub struct CloudflareAccessVerifier {
    config: CloudflareAccessConfig,
    client: Client,
    jwks: RwLock<Option<CachedJwks>>,
}

impl CloudflareAccessVerifier {
    pub fn new(config: CloudflareAccessConfig) -> Result<Self, CloudflareAccessError> {
        let client = Client::builder()
            .https_only(true)
            .timeout(Duration::from_secs(5))
            .build()
            .map_err(|_| CloudflareAccessError::Configuration("unable to create HTTPS client".to_owned()))?;
        Ok(Self {
            config,
            client,
            jwks: RwLock::new(None),
        })
    }

    pub fn config(&self) -> &CloudflareAccessConfig {
        &self.config
    }

    async fn load_keys(&self, force_refresh: bool) -> Result<Arc<Vec<Jwk>>, CloudflareAccessError> {
        if !force_refresh
            && let Some(cached) = self.jwks.read().await.as_ref()
            && cached.loaded_at.elapsed() < JWKS_TTL
        {
            return Ok(cached.keys.clone());
        }

        let response = self
            .client
            .get(&self.config.jwks_url)
            .send()
            .await
            .map_err(|_| CloudflareAccessError::JwksUnavailable)?
            .error_for_status()
            .map_err(|_| CloudflareAccessError::JwksUnavailable)?;
        let set = response
            .json::<JwkSet>()
            .await
            .map_err(|_| CloudflareAccessError::JwksUnavailable)?;
        let keys = Arc::new(set.keys);
        *self.jwks.write().await = Some(CachedJwks {
            loaded_at: Instant::now(),
            keys: keys.clone(),
        });
        Ok(keys)
    }
}

#[async_trait]
impl CloudflareAccessAuthenticator for CloudflareAccessVerifier {
    async fn verify(&self, assertion: &str) -> Result<CloudflareIdentity, CloudflareAccessError> {
        let header = decode_header(assertion).map_err(|_| CloudflareAccessError::InvalidAssertion)?;
        if header.alg != Algorithm::RS256 {
            return Err(CloudflareAccessError::InvalidAssertion);
        }
        let kid = header.kid.ok_or(CloudflareAccessError::InvalidAssertion)?;

        let mut keys = self.load_keys(false).await?;
        let mut key = keys.iter().find(|candidate| candidate.kid == kid).cloned();
        if key.is_none() {
            keys = self.load_keys(true).await?;
            key = keys.iter().find(|candidate| candidate.kid == kid).cloned();
        }
        let key = key.ok_or(CloudflareAccessError::InvalidAssertion)?;
        if key.kty != "RSA" || key.alg.as_deref().is_some_and(|alg| alg != "RS256") {
            return Err(CloudflareAccessError::InvalidAssertion);
        }

        let decoding_key = DecodingKey::from_rsa_components(&key.n, &key.e)
            .map_err(|_| CloudflareAccessError::InvalidAssertion)?;
        let mut validation = Validation::new(Algorithm::RS256);
        validation.set_issuer(&[self.config.issuer.as_str()]);
        validation.set_audience(&[self.config.audience.as_str()]);
        let claims = decode::<CloudflareClaims>(assertion, &decoding_key, &validation)
            .map_err(|_| CloudflareAccessError::VerificationFailed)?
            .claims;
        if claims.sub.trim().is_empty() || claims.exp == 0 || claims.iss != self.config.issuer {
            return Err(CloudflareAccessError::VerificationFailed);
        }

        Ok(CloudflareIdentity {
            subject: claims.sub,
            email: claims.email.filter(|email| !email.trim().is_empty()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_normalizes_https_team_domain() {
        let config = CloudflareAccessConfig::new("https://team.cloudflareaccess.com/", "aud").unwrap();
        assert_eq!(config.team_domain, "team.cloudflareaccess.com");
        assert_eq!(config.issuer, "https://team.cloudflareaccess.com");
        assert_eq!(config.jwks_url, "https://team.cloudflareaccess.com/cdn-cgi/access/certs");
    }

    #[test]
    fn config_rejects_partial_or_path_domain() {
        assert!(CloudflareAccessConfig::new("", "aud").is_err());
        assert!(CloudflareAccessConfig::new("team.example/path", "aud").is_err());
        assert!(CloudflareAccessConfig::new("team.example", "").is_err());
    }

    #[tokio::test]
    async fn malformed_assertion_is_rejected_before_jwks_fetch() {
        let config = CloudflareAccessConfig::new("team.example", "aud").unwrap();
        let verifier = CloudflareAccessVerifier::new(config).unwrap();
        assert!(matches!(
            verifier.verify("not-a-jwt").await,
            Err(CloudflareAccessError::InvalidAssertion)
        ));
    }
}
