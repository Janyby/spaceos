use std::collections::HashMap;
use std::sync::Mutex;

/// Minimal, explicit capability registry driven through Policy.
///
/// Trust model (local install):
/// - The core token (transport secret) proves "same SpaceOS install".
/// - It does NOT distinguish agents from each other.
/// - Per-agent binding comes from an agent token issued at
///   `register_agent` time. Only the holder of that token may act as
///   that agent. Core principals ("system"/"filesystem"/"cli") use the
///   core token directly.
#[derive(Debug, Clone)]
pub struct AgentRecord {
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
        matches!(actor, "system" | "filesystem" | "cli")
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
        if Self::is_core_principal(actor) {
            return None;
        }
        let token = uuid::Uuid::new_v4().to_string();
        self.agents.lock().unwrap().insert(
            actor.to_string(),
            AgentRecord {
                capabilities,
                worlds,
                token: token.clone(),
            },
        );
        Some(token)
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

    pub fn set_allow_self_grant(&self, _allow: bool) {
        // Disabled by design: self-grant is never allowed.
    }
}
