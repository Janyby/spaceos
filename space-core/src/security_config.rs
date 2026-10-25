//! Startup validation of configured credential domains. This does not choose
//! production secret storage or grant missing capabilities.
use anyhow::{bail, Result};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use std::collections::HashSet;

const ROLE_KEYS: &[&str] = &[
    "SPACEOS_PRIVILEGED_TOKEN_SYSTEM",
    "SPACEOS_PRIVILEGED_TOKEN_FILESYSTEM",
    "SPACEOS_PRIVILEGED_TOKEN_BROWSER",
    "SPACEOS_PRIVILEGED_TOKEN_CLI",
    "SPACEOS_SERVICE_REGISTRAR_TOKEN_SEMANTIC",
    "SPACEOS_SERVICE_REGISTRAR_TOKEN_DASHBOARD",
    "SPACEOS_SERVICE_REGISTRAR_TOKEN_BROWSER_WORLD",
    "SPACEOS_SERVICE_REGISTRAR_TOKEN_MCP",
    "SPACEOS_SERVICE_REGISTRAR_TOKEN_WORLD_READ",
    "SPACEOS_SERVICE_REGISTRAR_TOKEN_MCP_READ",
    "SPACEOS_DISPATCH_KEY_FILESYSTEM",
    "SPACEOS_DISPATCH_KEY_BROWSER",
];

fn validate_tokens<'a>(tokens: impl IntoIterator<Item = &'a str>, transport: &str) -> Result<()> {
    let mut seen = HashSet::new();
    for token in tokens {
        let decoded = URL_SAFE_NO_PAD
            .decode(token)
            .map_err(|_| anyhow::anyhow!("invalid security credential configuration"))?;
        let secret: [u8; 32] = decoded
            .try_into()
            .map_err(|_| anyhow::anyhow!("invalid security credential configuration"))?;
        if token == transport || !seen.insert(secret) {
            bail!("security credential domains must be distinct");
        }
    }
    Ok(())
}

/// Verify every *present* Core-side role credential before opening SQLite.
/// Missing credentials still deny their respective operations; the production
/// launcher must separately decide which roles are required for its topology.
pub fn validate_present_credential_domains(transport: &str) -> Result<()> {
    // Isolated fixtures inject synthetic registries; never inspect inherited
    // production credential variables in an isolated process.
    if std::env::var_os("SPACEOS_TEST_ISOLATION_ROOT").is_some() {
        return Ok(());
    }
    validate_configuration(transport, |key| match std::env::var(key) {
        Ok(value)=>Ok(Some(value)),
        Err(std::env::VarError::NotPresent)=>Ok(None),
        Err(_)=>bail!("invalid security credential configuration"),
    })
}

fn validate_configuration(transport:&str, lookup:impl Fn(&str)->Result<Option<String>>) -> Result<()> {
    let mut tokens = Vec::new();
    for key in ROLE_KEYS {
        if let Some(value) = lookup(key)? {
            tokens.push(value);
        }
    }
    if let Some(raw) = lookup("SPACEOS_PROVIDER_INSTANCE_BINDINGS")? {
        let bindings: serde_json::Value = serde_json::from_str(&raw)
            .map_err(|_| anyhow::anyhow!("invalid Provider binding configuration"))?;
        let entries = bindings.as_array()
            .ok_or_else(|| anyhow::anyhow!("invalid Provider binding configuration"))?;
        for entry in entries {
            let token = entry.get("instance_token").and_then(|v| v.as_str())
                .ok_or_else(|| anyhow::anyhow!("invalid Provider binding configuration"))?;
            tokens.push(token.to_owned());
        }
    }
    if let Some(raw)=lookup("SPACEOS_BROWSER_OBSERVATION_BINDINGS")? {
        crate::provider_observation::ObservationBindings::default().configure_browser(&raw)?;
        let entries:serde_json::Value=serde_json::from_str(&raw).map_err(|_|anyhow::anyhow!("invalid browser binding configuration"))?;
        for entry in entries.as_array().ok_or_else(||anyhow::anyhow!("invalid browser binding configuration"))? {
            tokens.push(entry["instance_token"].as_str().ok_or_else(||anyhow::anyhow!("invalid browser binding credential"))?.to_owned());
        }
    }
    validate_tokens(tokens.iter().map(String::as_str), transport)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn browser_world_is_validated_before_storage_without_reading_environment() {
        let role="SPACEOS_SERVICE_REGISTRAR_TOKEN_BROWSER_WORLD";
        let key=URL_SAFE_NO_PAD.encode([77u8;32]);
        for value in ["malformed".to_owned(),URL_SAFE_NO_PAD.encode([1u8;31]),String::new()] {
            assert!(validate_configuration("transport",|name|Ok((name==role).then(||value.clone()))).is_err());
        }
        assert!(validate_configuration(&key,|name|Ok((name==role).then(||key.clone()))).is_err());
        for other in ROLE_KEYS.iter().filter(|n|**n!=role) {
            assert!(validate_configuration("transport",|name|Ok((name==role || name==*other).then(||key.clone()))).is_err(),"{other}");
        }
        let instance=serde_json::json!([{"instance_token":key}]).to_string();
        assert!(validate_configuration("transport",|name|Ok(if name==role {Some(key.clone())} else if name=="SPACEOS_PROVIDER_INSTANCE_BINDINGS" {Some(instance.clone())} else {None})).is_err());
        let browser=serde_json::json!([{"provider":"browser","instance_id":"startup-instance","instance_token":key,
            "source_id":"11c3b28d-9262-4729-b324-11af48111001","runtime_epoch":"11c3b28d-9262-4729-b324-11af48111002",
            "world_id":"A","cdp_origin":"http://127.0.0.1:21989","expected_browser_debugger_id":"synthetic-startup"}]).to_string();
        assert!(validate_configuration("transport",|name|Ok(if name==role {Some(key.clone())} else if name=="SPACEOS_BROWSER_OBSERVATION_BINDINGS" {Some(browser.clone())} else {None})).is_err());
        assert!(validate_configuration("transport",|name|Ok((name==role).then(||key.clone()))).is_ok());
        assert!(validate_configuration("transport",|_|Ok(None)).is_ok());
    }
    #[test]
    fn rejects_cross_domain_substitution_without_echoing_secret() {
        let token = URL_SAFE_NO_PAD.encode([91u8; 32]);
        let error = validate_tokens([token.as_str(), token.as_str()], "transport")
            .unwrap_err().to_string();
        assert!(!error.contains(&token));
        assert!(error.contains("distinct"));
        assert!(validate_tokens([token.as_str()], &token).is_err());
        assert!(validate_tokens(["malformed"], "transport").is_err());
        let other = URL_SAFE_NO_PAD.encode([92u8; 32]);
        assert!(validate_tokens([token.as_str(), other.as_str()], "transport").is_ok());
    }
}
