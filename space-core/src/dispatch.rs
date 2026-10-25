//! Operation-bound Core-to-Provider authorization. The generic Core transport
//! token is deliberately not a dispatch credential.
use crate::action::DispatchRequest;
#[cfg(test)]
use crate::ActionRequest;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use ring::hmac;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    sync::Mutex,
    time::{SystemTime, UNIX_EPOCH},
};

const MAX_AGE_SECONDS: u64 = 15;
const MAX_REPLAY_ENTRIES: usize = 10_000;

#[derive(Clone, Serialize, Deserialize)]
pub struct DispatchPayload {
    pub version: u8,
    pub provider_instance: String,
    pub request: DispatchRequest,
    pub issued_at: u64,
    pub expires_at: u64,
    pub nonce: String,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct DispatchProof {
    pub payload: DispatchPayload,
    pub mac: String,
}

fn now() -> Result<u64, String> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|t| t.as_secs())
        .map_err(|_| "clock unavailable".into())
}

fn key_from_base64(encoded: &str) -> Result<hmac::Key, String> {
    let bytes = URL_SAFE_NO_PAD
        .decode(encoded)
        .map_err(|_| "invalid dispatch key encoding")?;
    if bytes.len() != 32 {
        return Err("dispatch key must be 32 bytes".into());
    }
    Ok(hmac::Key::new(hmac::HMAC_SHA256, &bytes))
}

pub fn key_from_env(provider_id: &str) -> Result<String, String> {
    let name = match provider_id {
        "filesystem" => "SPACEOS_DISPATCH_KEY_FILESYSTEM",
        "browser" => "SPACEOS_DISPATCH_KEY_BROWSER",
        _ => return Err("unknown dispatch provider".into()),
    };
    let encoded = std::env::var(name).map_err(|_| format!("{name} is not configured"))?;
    key_from_base64(&encoded)?;
    Ok(encoded)
}

fn signed_bytes(payload: &DispatchPayload) -> Result<Vec<u8>, String> {
    // Struct field order is fixed; serde_json::Value maps use deterministic
    // key ordering in this workspace (preserve_order is not enabled).
    serde_json::to_vec(payload).map_err(|_| "cannot serialize dispatch".into())
}

pub fn issue(
    key: &str,
    provider_instance: &str,
    request: impl Into<DispatchRequest>,
) -> Result<DispatchProof, String> {
    if provider_instance.is_empty() {
        return Err("missing provider instance".into());
    }
    issue_at(key, provider_instance, &request.into(), now()?)
}

fn issue_at(
    key: &str,
    provider_instance: &str,
    request: impl Into<DispatchRequest>,
    issued_at: u64,
) -> Result<DispatchProof, String> {
    let payload = DispatchPayload {
        version: 2,
        provider_instance: provider_instance.to_owned(),
        request: request.into(),
        issued_at,
        expires_at: issued_at + MAX_AGE_SECONDS,
        nonce: uuid::Uuid::new_v4().to_string(),
    };
    let mac = hmac::sign(&key_from_base64(key)?, &signed_bytes(&payload)?);
    Ok(DispatchProof {
        payload,
        mac: URL_SAFE_NO_PAD.encode(mac.as_ref()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn key() -> String {
        URL_SAFE_NO_PAD.encode([3u8; 32])
    }
    fn request() -> ActionRequest {
        ActionRequest::new(
            "read",
            json!({"path":"C:\\fixture\\item"}),
            "filesystem",
            "agent",
        )
    }

    #[test]
    fn credential_free_wire_and_proof_are_not_actor_authentication() {
        let policy=crate::policy::Policy::new();
        let token=policy.register_scoped_service("space-world-browser",vec!["semantic.read".into()],vec!["A".into()]).unwrap();
        let mut public=request();public.actor="space-world-browser".into();public.actor_token=token.clone();
        assert!(serde_json::to_string(&public).unwrap().contains(&token));
        let wire=DispatchRequest::from(&public);
        let verifier=DispatchVerifier::new("filesystem",Some(&key()));
        let proof=issue(&key(),verifier.instance_id(),&wire).unwrap();
        for json in [serde_json::to_value(&wire).unwrap(),serde_json::to_value(&proof).unwrap()] {
            let data=json.to_string();assert!(!data.contains("actor_token") && !data.contains(&token));
            assert!(!policy.authenticate("space-world-browser",Some(&data)));
        }
        assert!(!policy.authenticate("space-world-browser",Some(&proof.mac)));
        let mut params=serde_json::to_value(&wire).unwrap();params["actor_token"]=json!(token);
        assert!(DispatchRequest::from_provider_params(&params).is_err());
        assert!(verifier.verify(Some(&proof),&wire).is_ok());
    }
    #[test]
    fn exact_operation_replay_tamper_expiry_and_restart() {
        let verifier = DispatchVerifier::new("filesystem", Some(&key()));
        let original = request();
        let proof = issue(&key(), verifier.instance_id(), &original).unwrap();
        assert!(verifier.verify(None, &original).is_err());
        let mut tampered = original.clone();
        tampered.action = "delete".into();
        assert!(verifier.verify(Some(&proof), &tampered).is_err());
        tampered = original.clone();
        tampered.target = json!({"path":"C:\\fixture\\other"});
        assert!(verifier.verify(Some(&proof), &tampered).is_err());
        tampered = original.clone();
        tampered.actor = "system".into();
        assert!(verifier.verify(Some(&proof), &tampered).is_err());
        let browser = DispatchVerifier::new("browser", Some(&key()));
        assert!(browser.verify(Some(&proof), &original).is_err());
        let mut forged = proof.clone();
        forged.payload.request.action = "delete".into();
        assert!(verifier
            .verify(Some(&forged), &forged.payload.request)
            .is_err());
        let expired = issue_at(
            &key(),
            verifier.instance_id(),
            &original,
            now().unwrap() - 60,
        )
        .unwrap();
        assert!(verifier.verify(Some(&expired), &original).is_err());
        assert!(verifier.verify(Some(&proof), &original).is_ok());
        assert!(verifier.verify(Some(&proof), &original).is_err());
        let restarted = DispatchVerifier::new("filesystem", Some(&key()));
        assert!(restarted.verify(Some(&proof), &original).is_err());
    }

    #[test]
    fn absent_invalid_key_fails_closed() {
        let req = request();
        let verifier = DispatchVerifier::new("filesystem", None);
        assert!(issue("short", verifier.instance_id(), &req).is_err());
        assert!(verifier.verify(None, &req).is_err());
    }
}

/// One verifier per Provider process. Provider restart changes instance_id;
/// Core restart does not clear this replay cache in a still-running Provider.
pub struct DispatchVerifier {
    provider_id: String,
    instance_id: String,
    key: Option<hmac::Key>,
    seen: Mutex<HashMap<String, u64>>,
}

impl DispatchVerifier {
    pub fn new(provider_id: &str, key: Option<&str>) -> Self {
        let key = key.and_then(|s| key_from_base64(s).ok());
        Self {
            provider_id: provider_id.to_owned(),
            instance_id: uuid::Uuid::new_v4().to_string(),
            key,
            seen: Mutex::new(HashMap::new()),
        }
    }

    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }

    pub fn verify(
        &self,
        proof: Option<&DispatchProof>,
        request: impl Into<DispatchRequest>,
    ) -> Result<(), String> {
        let request = request.into();
        let proof = proof.ok_or("dispatch proof required")?;
        let key = self
            .key
            .as_ref()
            .ok_or("provider dispatch key not configured")?;
        let p = &proof.payload;
        if p.version != 2
            || p.provider_instance != self.instance_id
            || p.request.provider_id != self.provider_id
            || request.provider_id != self.provider_id
            || p.nonce.is_empty()
        {
            return Err("dispatch binding mismatch".into());
        }
        let requested = serde_json::to_vec(&request).map_err(|_| "invalid request")?;
        let authorized = serde_json::to_vec(&p.request).map_err(|_| "invalid authorization")?;
        if requested != authorized {
            return Err("dispatch request mismatch".into());
        }
        let current = now()?;
        if p.issued_at > current + 2
            || p.expires_at < current
            || p.expires_at < p.issued_at
            || p.expires_at.saturating_sub(p.issued_at) > MAX_AGE_SECONDS
        {
            return Err("dispatch expired".into());
        }
        let mac = URL_SAFE_NO_PAD
            .decode(&proof.mac)
            .map_err(|_| "invalid dispatch mac")?;
        hmac::verify(key, &signed_bytes(p)?, &mac).map_err(|_| "invalid dispatch mac")?;
        let mut seen = self.seen.lock().map_err(|_| "replay state unavailable")?;
        seen.retain(|_, expiry| *expiry >= current);
        if seen.contains_key(&p.nonce) {
            return Err("dispatch replay".into());
        }
        if seen.len() >= MAX_REPLAY_ENTRIES {
            return Err("replay cache full".into());
        }
        seen.insert(p.nonce.clone(), p.expires_at);
        Ok(())
    }
}
