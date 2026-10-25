//! Explicit, fail-closed Core-principal identity. A transport token is not a
//! system/provider/CLI identity credential.
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use std::collections::{HashMap, HashSet};
use subtle::ConstantTimeEq;

struct PrincipalRecord {
    secret: [u8; 32],
    capabilities: HashSet<String>,
    worlds: HashSet<String>,
}

#[derive(Default)]
pub struct PrincipalRegistry {
    records: HashMap<String, PrincipalRecord>,
}

impl PrincipalRegistry {
    pub(crate) fn contains_secret(&self, secret: &[u8; 32]) -> bool {
        self.records
            .values()
            .any(|record| bool::from(record.secret.ct_eq(secret)))
    }
    /// Server-side configuration only. Isolated tests never read inherited
    /// production principal credentials; fixtures inject synthetic records.
    pub fn from_env() -> Self {
        let mut registry = Self::default();
        if std::env::var_os("SPACEOS_TEST_ISOLATION_ROOT").is_some() {
            return registry;
        }
        for (actor, suffix) in [
            ("system", "SYSTEM"),
            ("filesystem", "FILESYSTEM"),
            ("browser", "BROWSER"),
            ("cli", "CLI"),
        ] {
            let Ok(encoded) = std::env::var(format!("SPACEOS_PRIVILEGED_TOKEN_{suffix}")) else {
                continue;
            };
            let Ok(bytes) = URL_SAFE_NO_PAD.decode(encoded) else {
                continue;
            };
            let Ok(secret) = <[u8; 32]>::try_from(bytes) else {
                continue;
            };
            let capabilities =
                std::env::var(format!("SPACEOS_PRIVILEGED_CAPS_{suffix}")).unwrap_or_default();
            let worlds =
                std::env::var(format!("SPACEOS_PRIVILEGED_WORLDS_{suffix}")).unwrap_or_default();
            let _ = registry.insert(
                actor,
                secret,
                capabilities.split(',').map(str::trim),
                worlds.split(',').map(str::trim),
            );
        }
        registry
    }

    /// Explicit fixture/bootstrap injection. Duplicated credentials across
    /// principals are refused so one token cannot impersonate two identities.
    pub fn insert<'a>(
        &mut self,
        actor: &str,
        secret: [u8; 32],
        capabilities: impl IntoIterator<Item = &'a str>,
        worlds: impl IntoIterator<Item = &'a str>,
    ) -> Result<(), &'static str> {
        if !matches!(actor, "system" | "filesystem" | "browser" | "cli") {
            return Err("unknown privileged principal");
        }
        if self.records.contains_key(actor)
            || self
                .records
                .values()
                .any(|record| bool::from(record.secret.ct_eq(&secret)))
        {
            return Err("duplicate principal credential");
        }
        let capabilities = capabilities
            .into_iter()
            .filter(|s| !s.is_empty())
            .map(str::to_owned)
            .collect();
        let worlds = worlds
            .into_iter()
            .filter(|s| !s.is_empty())
            .map(str::to_owned)
            .collect();
        self.records.insert(
            actor.to_owned(),
            PrincipalRecord {
                secret,
                capabilities,
                worlds,
            },
        );
        Ok(())
    }

    pub fn authenticate(&self, actor: &str, credential: Option<&str>) -> bool {
        let Some(record) = self.records.get(actor) else {
            return false;
        };
        let Some(encoded) = credential else {
            return false;
        };
        let Ok(bytes) = URL_SAFE_NO_PAD.decode(encoded) else {
            return false;
        };
        bool::from(record.secret.as_slice().ct_eq(&bytes))
    }

    pub fn can(&self, actor: &str, capability: &str, world_id: Option<&str>) -> bool {
        let Some(record) = self.records.get(actor) else {
            return false;
        };
        record.capabilities.contains(capability)
            && world_id.map_or(true, |world| record.worlds.contains(world))
    }

    pub fn world_allowed(&self, actor: &str, world_id: &str) -> bool {
        self.records
            .get(actor)
            .is_some_and(|record| record.worlds.contains(world_id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn distinct_principal_credentials_and_caps() {
        let mut registry = PrincipalRegistry::default();
        registry
            .insert("system", [1; 32], ["agent.register"], ["fixture-world"])
            .unwrap();
        assert!(registry
            .insert("filesystem", [1; 32], ["event.record"], [])
            .is_err());
        let credential = URL_SAFE_NO_PAD.encode([1; 32]);
        assert!(registry.authenticate("system", Some(&credential)));
        assert!(!registry.authenticate("filesystem", Some(&credential)));
        assert!(!registry.authenticate("system", None));
        assert!(!registry.authenticate("system", Some("invalid")));
        assert!(registry.can("system", "agent.register", Some("fixture-world")));
        assert!(!registry.can("system", "action.create", None));
        assert!(!registry.can("system", "agent.register", Some("other")));
    }
}
