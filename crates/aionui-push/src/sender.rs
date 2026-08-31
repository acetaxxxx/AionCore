use std::time::{SystemTime, UNIX_EPOCH};

use aionui_db::PushSubscriptionRecord;
use async_trait::async_trait;
use aes_gcm::aead::{Aead, KeyInit};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use p256::ecdsa::signature::Signer;
use p256::ecdsa::{Signature, SigningKey};
use p256::ecdh::diffie_hellman;
use p256::elliptic_curve::rand_core::OsRng;
use p256::{PublicKey, SecretKey};
use reqwest::header::{AUTHORIZATION, CONTENT_ENCODING, CONTENT_TYPE, HeaderValue};
use serde_json::json;
use sha2::Sha256;

use crate::payload::PushPayload;

const PUSH_TTL_SECONDS: &str = "86400";
const RECORD_SIZE: u32 = 4096;
const VAPID_LIFETIME_SECONDS: u64 = 12 * 60 * 60;

#[derive(Debug, thiserror::Error)]
pub enum PushSendError {
    #[error("push sender is not configured")]
    Unavailable,
    #[error("push endpoint is gone")]
    Gone,
    #[error("push provider rejected delivery")]
    Rejected,
    #[error("push transport failed")]
    Transport,
}

/// External provider boundary. Implementations receive only an already
/// bounded payload and a single subscription; they must never log either
/// endpoint or browser key material.
#[async_trait]
pub trait PushSender: Send + Sync {
    async fn send(&self, subscription: &PushSubscriptionRecord, payload: &PushPayload)
        -> Result<(), PushSendError>;

    fn is_configured(&self) -> bool;
}

#[derive(Default)]
pub struct DisabledPushSender;

#[async_trait]
impl PushSender for DisabledPushSender {
    async fn send(
        &self,
        _subscription: &PushSubscriptionRecord,
        _payload: &PushPayload,
    ) -> Result<(), PushSendError> {
        Err(PushSendError::Unavailable)
    }

    fn is_configured(&self) -> bool {
        false
    }
}

/// Standards-based Web Push sender using deployment-provided VAPID material.
/// The private key is read once at construction and is never stored in the DB
/// or included in application state exposed to routes.
pub struct WebPushSender {
    client: reqwest::Client,
    signing_key: SigningKey,
    public_key: [u8; 65],
    subject: String,
}

impl WebPushSender {
    pub fn from_env(client: reqwest::Client) -> Option<Self> {
        let private_key = std::env::var("AIONUI_VAPID_PRIVATE_KEY").ok()?;
        let subject = std::env::var("AIONUI_VAPID_SUBJECT").ok()?;
        Self::new(client, &private_key, &subject).ok()
    }

    pub fn new(client: reqwest::Client, private_key: &str, subject: &str) -> Result<Self, PushSendError> {
        let bytes = URL_SAFE_NO_PAD
            .decode(private_key.trim())
            .map_err(|_| PushSendError::Unavailable)?;
        let private_bytes: [u8; 32] = bytes.try_into().map_err(|_| PushSendError::Unavailable)?;
        let signing_key = SigningKey::from_bytes((&private_bytes).into()).map_err(|_| PushSendError::Unavailable)?;
        let encoded = signing_key.verifying_key().to_encoded_point(false);
        let public_key: [u8; 65] = encoded
            .as_bytes()
            .try_into()
            .map_err(|_| PushSendError::Unavailable)?;
        let subject = subject.trim();
        if !(subject.starts_with("mailto:") || subject.starts_with("https://") || subject.starts_with("http://")) {
            return Err(PushSendError::Unavailable);
        }
        Ok(Self {
            client,
            signing_key,
            public_key,
            subject: subject.to_owned(),
        })
    }

    fn vapid_authorization(&self, endpoint: &str) -> Result<HeaderValue, PushSendError> {
        let endpoint_url = url::Url::parse(endpoint).map_err(|_| PushSendError::Rejected)?;
        let host = endpoint_url.host_str().ok_or(PushSendError::Rejected)?;
        if endpoint_url.scheme() != "https"
            || endpoint_url.username() != ""
            || endpoint_url.password().is_some()
            || endpoint_url.fragment().is_some()
        {
            return Err(PushSendError::Rejected);
        }
        let audience = match endpoint_url.port() {
            Some(port) => format!("{}://{}:{port}", endpoint_url.scheme(), host),
            None => format!("{}://{host}", endpoint_url.scheme()),
        };
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| PushSendError::Unavailable)?
            .as_secs();
        let header = URL_SAFE_NO_PAD.encode(json!({ "typ": "JWT", "alg": "ES256" }).to_string());
        let claims = URL_SAFE_NO_PAD.encode(
            json!({
                "aud": audience,
                "exp": now.saturating_add(VAPID_LIFETIME_SECONDS),
                "sub": self.subject,
            })
            .to_string(),
        );
        let signing_input = format!("{header}.{claims}");
        let signature: Signature = self.signing_key.sign(signing_input.as_bytes());
        let jwt = format!("{signing_input}.{}", URL_SAFE_NO_PAD.encode(signature.to_bytes()));
        HeaderValue::from_str(&format!("vapid t={jwt}, k={}", URL_SAFE_NO_PAD.encode(self.public_key)))
            .map_err(|_| PushSendError::Unavailable)
    }

    fn encrypt_payload(
        &self,
        subscription: &PushSubscriptionRecord,
        payload: &PushPayload,
    ) -> Result<Vec<u8>, PushSendError> {
        let ua_public = decode_exact(&subscription.p256dh, 65)?;
        let auth_secret = decode_exact(&subscription.auth, 16)?;
        let ua_key = PublicKey::from_sec1_bytes(&ua_public).map_err(|_| PushSendError::Rejected)?;
        let ephemeral_secret = SecretKey::random(&mut OsRng);
        let ephemeral_public = PublicKey::from(&ephemeral_secret);
        let ephemeral_public_bytes = ephemeral_public.to_encoded_point(false);
        let shared = diffie_hellman(ephemeral_secret.to_nonzero_scalar(), ua_key.as_affine());

        let mut key_info = b"WebPush: info\0".to_vec();
        key_info.extend_from_slice(&ua_public);
        key_info.extend_from_slice(ephemeral_public_bytes.as_bytes());
        let auth_prk = hkdf::Hkdf::<Sha256>::new(Some(&auth_secret), shared.raw_secret_bytes());
        let mut ikm = [0u8; 32];
        auth_prk
            .expand(&[&key_info], &mut ikm)
            .map_err(|_| PushSendError::Rejected)?;

        let mut salt = [0u8; 16];
        getrandom::getrandom(&mut salt).map_err(|_| PushSendError::Transport)?;
        let content_prk = hkdf::Hkdf::<Sha256>::new(Some(&salt), &ikm);
        let mut cek = [0u8; 16];
        let mut nonce = [0u8; 12];
        content_prk
            .expand(&[b"Content-Encoding: aes128gcm\0"], &mut cek)
            .map_err(|_| PushSendError::Rejected)?;
        content_prk
            .expand(&[b"Content-Encoding: nonce\0"], &mut nonce)
            .map_err(|_| PushSendError::Rejected)?;

        let mut plaintext = serde_json::to_vec(payload).map_err(|_| PushSendError::Rejected)?;
        plaintext.push(2);
        let cipher = aes_gcm::Aes128Gcm::new_from_slice(&cek).map_err(|_| PushSendError::Rejected)?;
        let ciphertext = cipher
            .encrypt(aes_gcm::Nonce::from_slice(&nonce), plaintext.as_ref())
        .map_err(|_| PushSendError::Rejected)?;

        let mut body = Vec::with_capacity(16 + 4 + 1 + 65 + ciphertext.len());
        body.extend_from_slice(&salt);
        body.extend_from_slice(&RECORD_SIZE.to_be_bytes());
        body.push(ephemeral_public_bytes.as_bytes().len() as u8);
        body.extend_from_slice(ephemeral_public_bytes.as_bytes());
        body.extend_from_slice(&ciphertext);
        Ok(body)
    }
}

#[async_trait]
impl PushSender for WebPushSender {
    async fn send(
        &self,
        subscription: &PushSubscriptionRecord,
        payload: &PushPayload,
    ) -> Result<(), PushSendError> {
        let body = self.encrypt_payload(subscription, payload)?;
        let authorization = self.vapid_authorization(&subscription.endpoint)?;
        let response = self
            .client
            .post(&subscription.endpoint)
            .header(AUTHORIZATION, authorization)
            .header(CONTENT_ENCODING, "aes128gcm")
            .header(CONTENT_TYPE, "application/octet-stream")
            .header("TTL", PUSH_TTL_SECONDS)
            .body(body)
            .send()
            .await
            .map_err(|_| PushSendError::Transport)?;
        if response.status() == reqwest::StatusCode::GONE || response.status() == reqwest::StatusCode::NOT_FOUND {
            return Err(PushSendError::Gone);
        }
        if !response.status().is_success() {
            return Err(PushSendError::Rejected);
        }
        Ok(())
    }

    fn is_configured(&self) -> bool {
        true
    }
}

fn decode_exact(value: &str, expected: usize) -> Result<Vec<u8>, PushSendError> {
    let decoded = URL_SAFE_NO_PAD
        .decode(value)
        .or_else(|_| base64::engine::general_purpose::URL_SAFE.decode(value))
        .map_err(|_| PushSendError::Rejected)?;
    if decoded.len() != expected {
        return Err(PushSendError::Rejected);
    }
    Ok(decoded)
}
