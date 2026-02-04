//! DPoP (RFC 9449): signed JWT proof for token and API requests.

use anyhow::Context;
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64;
use rsa::RsaPrivateKey;
use rsa::pkcs1v15::SigningKey;
use rsa::signature::{SignatureEncoding, Signer};
use rsa::traits::PublicKeyParts;
use serde_json::json;
use sha2::Sha256;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Cache of DPoP key per source (same key used for token and API requests when token is DPoP-bound).
pub type DPoPKeyCache = Arc<RwLock<HashMap<String, RsaPrivateKey>>>;

pub fn new_dpop_key_cache() -> DPoPKeyCache {
    Arc::new(RwLock::new(HashMap::new()))
}

/// Generate a new RSA 2048-bit key for DPoP.
fn generate_rsa_key() -> anyhow::Result<RsaPrivateKey> {
    use rsa::rand_core::OsRng;
    RsaPrivateKey::new(&mut OsRng, 2048).context("generate DPoP RSA key")
}

/// Get or create the DPoP key for a source.
pub async fn get_or_create_dpop_key(
    cache: &DPoPKeyCache,
    source_id: &str,
) -> anyhow::Result<RsaPrivateKey> {
    {
        let g = cache.read().await;
        if let Some(k) = g.get(source_id) {
            return Ok(k.clone());
        }
    }
    let key = generate_rsa_key()?;
    {
        let mut g = cache.write().await;
        g.insert(source_id.to_string(), key.clone());
    }
    Ok(key)
}

/// htu is the HTTP target URI without query and fragment (RFC 9449 §4.2).
fn uri_without_query_and_fragment(url: &str) -> anyhow::Result<String> {
    let u = reqwest::Url::parse(url).context("parse URL for htu")?;
    let mut u = u.clone();
    u.set_query(None);
    u.set_fragment(None);
    Ok(u.to_string())
}

/// Build DPoP proof JWT (RFC 9449): htm, htu (no query/fragment), iat, jti; optional nonce and ath when provided.
pub fn build_dpop_proof(
    method: &str,
    url: &str,
    private_key: &RsaPrivateKey,
    jti: &str,
    iat_secs: u64,
    nonce: Option<&str>,
    access_token: Option<&str>,
) -> anyhow::Result<String> {
    use sha2::Digest;
    let htu = uri_without_query_and_fragment(url)?;
    let public_key = private_key.to_public_key();
    let n = public_key.n();
    let e = public_key.e();
    let n_b64 = B64.encode(n.to_bytes_be());
    let e_b64 = B64.encode(e.to_bytes_be());
    let jwk = json!({
        "kty": "RSA",
        "n": n_b64,
        "e": e_b64,
        "alg": "RS256",
        "use": "sig"
    });
    let header = json!({
        "typ": "dpop+jwt",
        "alg": "RS256",
        "jwk": jwk
    });
    let mut payload = json!({
        "htm": method,
        "htu": htu,
        "iat": iat_secs,
        "jti": jti
    });
    if let Some(n) = nonce {
        payload["nonce"] = serde_json::Value::String(n.to_string());
    }
    if let Some(tok) = access_token {
        let hash = sha2::Sha256::digest(tok.as_bytes());
        payload["ath"] = serde_json::Value::String(B64.encode(hash));
    }
    let header_b64 = B64.encode(header.to_string());
    let payload_b64 = B64.encode(payload.to_string());
    let message = format!("{}.{}", header_b64, payload_b64);
    let message_bytes = message.as_bytes();

    let signing_key = SigningKey::<Sha256>::new(private_key.clone());
    let signature = signing_key.sign(message_bytes);
    let sig_b64 = B64.encode(signature.to_bytes());

    Ok(format!("{}.{}", message, sig_b64))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dpop_proof_shape() {
        let key = RsaPrivateKey::new(&mut rsa::rand_core::OsRng, 2048).unwrap();
        let proof = build_dpop_proof(
            "POST",
            "https://example.com/oauth2/v1/token",
            &key,
            "test-jti",
            12345,
            None,
            None,
        )
        .unwrap();
        let parts: Vec<&str> = proof.split('.').collect();
        assert_eq!(parts.len(), 3);
        let header_json = String::from_utf8(B64.decode(parts[0]).unwrap()).unwrap();
        assert!(header_json.contains("dpop+jwt"));
        assert!(header_json.contains("RS256"));
        assert!(header_json.contains("jwk"));
    }
}
