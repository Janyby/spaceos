use std::collections::HashMap;
use std::sync::Mutex;

/// Minimal, explicit capability registry driven through Policy.
///
/// Trust model (local install):
/// - The core token is only a transport/bootstrap credential; it does not
///   grant Core-principal authority. IPC verifies privileged credentials.
/// - Per-agent binding comes from an agent token issued at `register_agent`.
/// - The IPC layer additionally restricts each privileged principal to its
///   configured credential and explicit capability set.
#[derive(Debug, Clone)]
pub struct AgentRecord {
    /// Only authenticated register_agent grants qualify for delegated writes.
    pub delegated_agent: bool,
    pub capabilities: Vec<String>,
    /// Worlds this agent may touch. Empty = no world access.
    pub worlds: Vec<String>,
    pub token: String,
}

pub struct Policy {
    agents: Mutex<HashMap<String, AgentRecord>>,
}

impl Default for Policy {
    fn default() -> Self {
        Self::new()
    }
}

impl Policy {
    pub fn new() -> Self {
        Self {
            agents: Mutex::new(HashMap::new()),
        }
    }

    pub fn is_core_principal(actor: &str) -> bool {
        matches!(actor, "system" | "filesystem" | "browser" | "cli")
    }

    /// Only trusted principals may register agents. Returns the agent
    /// token that binds this agent identity. Re-registering rotates it.
    pub fn register(
        &self,
        actor: &str,
        principal: &str,
        capabilities: Vec<String>,
        worlds: Vec<String>,
    ) -> Option<String> {
        if principal != "system" || actor.trim().is_empty() {
            return None;
        }
        if Self::is_core_principal(actor) || actor == crate::service_registration::BROWSER_WORLD_ACTOR {
            return None;
        }
        let mut agents = self.agents.lock().unwrap();
        Some(Self::replace_record(
            &mut agents,
            actor,
            capabilities,
            worlds,
            true,
        ))
    }

    /// The IPC service registrar calls this only after authenticating a
    /// fixed Core profile. Existing authority must match exactly: a service
    /// credential cannot silently replace a different grant set.
    pub fn register_scoped_service(
        &self,
        actor: &str,
        capabilities: Vec<String>,
        worlds: Vec<String>,
    ) -> Result<String, &'static str> {
        if actor.trim().is_empty() || Self::is_core_principal(actor) {
            return Err("invalid service actor");
        }
        let mut agents = self.agents.lock().unwrap();
        if let Some(existing) = agents.get(actor) {
            if !same_authority(&existing.capabilities, &capabilities)
                || !same_authority(&existing.worlds, &worlds)
            {
                return Err("registration conflict");
            }
        }
        Ok(Self::replace_record(
            &mut agents,
            actor,
            capabilities,
            worlds,
            false,
        ))
    }

    fn replace_record(
        agents: &mut HashMap<String, AgentRecord>,
        actor: &str,
        capabilities: Vec<String>,
        worlds: Vec<String>,
        delegated_agent: bool,
    ) -> String {
        let token = uuid::Uuid::new_v4().to_string();
        agents.insert(
            actor.to_string(),
            AgentRecord {
                delegated_agent,
                capabilities,
                worlds,
                token: token.clone(),
            },
        );
        token
    }

    /// Legacy shape kept for unit tests: no worlds, returns bool.
    #[allow(dead_code)]
    fn register_simple(&self, actor: &str, principal: &str, capabilities: Vec<String>) -> bool {
        self.register(actor, principal, capabilities, vec![])
            .is_some()
    }

    /// Authenticate a caller: core principals pass with the transport
    /// token already verified by the caller; agents must present their
    /// own agent token.
    pub fn authenticate(&self, actor: &str, actor_token: Option<&str>) -> bool {
        if Self::is_core_principal(actor) {
            return true;
        }
        match (self.agents.lock().unwrap().get(actor), actor_token) {
            (Some(rec), Some(t)) => rec.token == t,
            _ => false,
        }
    }

    pub fn can(&self, actor: &str, capability: &str) -> bool {
        if Self::is_core_principal(actor) {
            return true;
        }
        self.agents
            .lock()
            .unwrap()
            .get(actor)
            .map(|v| v.capabilities.iter().any(|c| c == capability))
            .unwrap_or(false)
    }

    pub fn may_access_world(&self, actor: &str, world_id: &str) -> bool {
        if Self::is_core_principal(actor) {
            return true;
        }
        self.agents
            .lock()
            .unwrap()
            .get(actor)
            .map(|v| v.worlds.iter().any(|w| w == world_id))
            .unwrap_or(false)
    }

    pub fn capabilities_of(&self, actor: &str) -> Vec<String> {
        self.agents
            .lock()
            .unwrap()
            .get(actor)
            .map(|r| r.capabilities.clone())
            .unwrap_or_default()
    }

    pub fn worlds_of(&self, actor: &str) -> Vec<String> {
        self.agents
            .lock()
            .unwrap()
            .get(actor)
            .map(|r| r.worlds.clone())
            .unwrap_or_default()
    }

    pub fn is_registered(&self, actor: &str) -> bool {
        self.agents.lock().unwrap().contains_key(actor)
    }

    pub fn is_delegated_agent(&self, actor: &str) -> bool {
        if matches!(actor,"space-world-user"|"space-world-reader"|"space-mcp-reader"|"space-bridge"|"space-bridge-mcp"|"space-world-browser") {
            return false;
        }
        self.agents.lock().unwrap().get(actor)
            .map(|record| record.delegated_agent).unwrap_or(false)
    }

    pub fn set_allow_self_grant(&self, _allow: bool) {
        // Disabled by design: self-grant is never allowed.
    }
}

fn same_authority(left: &[String], right: &[String]) -> bool {
    let mut left = left.to_vec();
    let mut right = right.to_vec();
    left.sort();
    right.sort();
    left == right
}

#[cfg(test)]
mod scoped_service_tests {
    use super::Policy;

    #[test]
    fn exact_reregistration_rotates_token_but_conflict_is_atomic() {
        let policy = Policy::new();
        let caps = vec!["semantic.object.create".to_string()];
        let worlds = vec!["world-a".to_string()];
        let first = policy
            .register_scoped_service("space-world-user", caps.clone(), worlds.clone())
            .unwrap();
        let rotated = policy
            .register_scoped_service("space-world-user", caps.clone(), worlds.clone())
            .unwrap();
        assert_ne!(first, rotated);
        assert!(!policy.authenticate("space-world-user", Some(&first)));
        assert!(policy
            .register_scoped_service(
                "space-world-user",
                vec!["action.create".into()],
                worlds.clone()
            )
            .is_err());
        assert!(policy
            .register_scoped_service("space-world-user", caps.clone(), vec!["world-b".into()])
            .is_err());
        assert!(policy.authenticate("space-world-user", Some(&rotated)));
        assert_eq!(policy.capabilities_of("space-world-user"), caps);
        assert_eq!(policy.worlds_of("space-world-user"), worlds);
    }
}
