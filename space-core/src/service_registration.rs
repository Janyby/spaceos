//! Narrow, server-controlled registration profiles for Bridge service actors.
//! A registrar credential is not a Core principal or an agent credential.
use crate::principal::PrincipalRegistry;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use std::collections::HashMap;
use subtle::ConstantTimeEq;

pub const SEMANTIC_PROFILE: &str = "semantic_world";
pub const DASHBOARD_PROFILE: &str = "dashboard";
pub const BROWSER_WORLD_PROFILE: &str = "browser_world";
pub const BROWSER_WORLD_ACTOR: &str = "space-world-browser";
pub const BROWSER_WORLD_CAPS: &[&str] = &["semantic.read", "action.get_tabs", "action.screenshot"];
pub const MCP_PROFILE: &str = "mcp";
pub const WORLD_READ_PROFILE: &str = "world_read";
pub const MCP_READ_PROFILE: &str = "mcp_read";
pub const MCP_READ_ACTOR: &str = "space-mcp-reader";
pub const WORLD_READ_ACTOR: &str = "space-world-reader";
pub const WORLD_READ_CAPS: &[&str] = &["semantic.read"];

pub const SEMANTIC_CAPS: &[&str] = &[
    "mission.command",
    "semantic.object.create",
    "semantic.object.patch",
    "semantic.relation.create",
];
pub const DASHBOARD_CAPS: &[&str] = &[
    "action.navigate",
    "action.reload",
    "action.get_tabs",
    "action.get_page_content",
    "action.screenshot",
    "action.new_tab",
    "action.activate_tab",
    "action.close_tab",
    "action.scan",
    "action.read",
    "action.create",
    "action.write",
    "event.record",
    "observation.report",
];
pub const MCP_CAPS: &[&str] = &[
    "action.navigate",
    "action.reload",
    "action.get_tabs",
    "action.get_page_content",
    "action.new_tab",
    "action.activate_tab",
    "action.close_tab",
    "action.scan",
    "action.read",
    "action.create",
    "action.write",
    "event.record",
    "observation.report",
];

pub fn actor_for(profile: &str) -> Option<&'static str> {
    match profile {
        SEMANTIC_PROFILE => Some(crate::semantic::SEMANTIC_ACTOR),
        DASHBOARD_PROFILE => Some("space-bridge"),
        BROWSER_WORLD_PROFILE => Some(BROWSER_WORLD_ACTOR),
        MCP_PROFILE => Some("space-bridge-mcp"),
        WORLD_READ_PROFILE => Some(WORLD_READ_ACTOR),
        MCP_READ_PROFILE => Some(MCP_READ_ACTOR),
        _ => None,
    }
}

pub fn capabilities_for(profile: &str) -> Option<&'static [&'static str]> {
    match profile {
        SEMANTIC_PROFILE => Some(SEMANTIC_CAPS),
        DASHBOARD_PROFILE => Some(DASHBOARD_CAPS),
        BROWSER_WORLD_PROFILE => Some(BROWSER_WORLD_CAPS),
        MCP_PROFILE => Some(MCP_CAPS),
        WORLD_READ_PROFILE => Some(WORLD_READ_CAPS),
        MCP_READ_PROFILE => Some(WORLD_READ_CAPS),
        _ => None,
    }
}

#[derive(Default)]
pub struct ServiceRegistrarRegistry {
    secrets: HashMap<&'static str, [u8; 32]>,
}

impl ServiceRegistrarRegistry {
    /// Production configuration is read only by Core. Tests inject synthetic
    /// records and cannot inherit production credentials.
    pub fn from_env(principals: &PrincipalRegistry, transport_token: &str) -> Self {
        let mut result = Self::default();
        if std::env::var_os("SPACEOS_TEST_ISOLATION_ROOT").is_some() {
            return result;
        }
        for (profile, suffix) in [
            (SEMANTIC_PROFILE, "SEMANTIC"),
            (DASHBOARD_PROFILE, "DASHBOARD"),
            (BROWSER_WORLD_PROFILE, "BROWSER_WORLD"),
            (MCP_PROFILE, "MCP"),
            (WORLD_READ_PROFILE, "WORLD_READ"),
            (MCP_READ_PROFILE, "MCP_READ"),
        ] {
            let Ok(encoded) = std::env::var(format!("SPACEOS_SERVICE_REGISTRAR_TOKEN_{suffix}"))
            else {
                continue;
            };
            let Ok(decoded) = URL_SAFE_NO_PAD.decode(&encoded) else {
                continue;
            };
            let Ok(secret) = <[u8; 32]>::try_from(decoded) else {
                continue;
            };
            let _ = result.insert(profile, secret, principals, transport_token);
        }
        result
    }

    /// Reject cross-role credential reuse before a record becomes active.
    pub fn insert(
        &mut self,
        profile: &'static str,
        secret: [u8; 32],
        principals: &PrincipalRegistry,
        transport_token: &str,
    ) -> Result<(), &'static str> {
        if actor_for(profile).is_none() {
            return Err("unsupported service profile");
        }
        if self.secrets.contains_key(profile)
            || self
                .secrets
                .values()
                .any(|existing| bool::from(existing.ct_eq(&secret)))
            || principals.contains_secret(&secret)
            || URL_SAFE_NO_PAD.encode(secret) == transport_token
        {
            return Err("service credential conflict");
        }
        self.secrets.insert(profile, secret);
        Ok(())
    }

    pub fn authenticate(&self, profile: &str, credential: Option<&str>) -> bool {
        let (Some(secret), Some(encoded)) = (self.secrets.get(profile), credential) else {
            return false;
        };
        let Ok(decoded) = URL_SAFE_NO_PAD.decode(encoded) else {
            return false;
        };
        bool::from(secret.as_slice().ct_eq(&decoded))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn profile_credentials_are_independent() {
        let mut principals = PrincipalRegistry::default();
        principals
            .insert("system", [1; 32], ["agent.register"], ["world-a"])
            .unwrap();
        let mut registrar = ServiceRegistrarRegistry::default();
        assert!(registrar
            .insert(SEMANTIC_PROFILE, [1; 32], &principals, "transport")
            .is_err());
        assert!(registrar
            .insert(
                SEMANTIC_PROFILE,
                [3; 32],
                &principals,
                &URL_SAFE_NO_PAD.encode([3; 32])
            )
            .is_err());
        registrar
            .insert(SEMANTIC_PROFILE, [2; 32], &principals, "transport")
            .unwrap();
        assert!(registrar
            .insert(MCP_PROFILE, [2; 32], &principals, "transport")
            .is_err());
        assert!(registrar.authenticate(SEMANTIC_PROFILE, Some(&URL_SAFE_NO_PAD.encode([2; 32]))));
        assert!(!registrar.authenticate(MCP_PROFILE, Some(&URL_SAFE_NO_PAD.encode([2; 32]))));
        assert!(!registrar.authenticate(SEMANTIC_PROFILE, Some("invalid")));
        assert!(!registrar.authenticate(SEMANTIC_PROFILE, None));
    }
}
