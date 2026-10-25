//! Server-side Bridge registrar credentials. Never expose these through HTTP,
//! WebSocket, agent prompts, logging, or a frontend bundle.

pub(crate) fn from_env(profile: &str) -> Option<String> {
    if std::env::var_os("SPACEOS_TEST_ISOLATION_ROOT").is_some() {
        return None;
    }
    let key = match profile {
        space_core::service_registration::SEMANTIC_PROFILE => {
            "SPACEOS_SERVICE_REGISTRAR_TOKEN_SEMANTIC"
        }
        space_core::service_registration::DASHBOARD_PROFILE => {
            "SPACEOS_SERVICE_REGISTRAR_TOKEN_DASHBOARD"
        }
        space_core::service_registration::BROWSER_WORLD_PROFILE => "SPACEOS_SERVICE_REGISTRAR_TOKEN_BROWSER_WORLD",
        space_core::service_registration::MCP_PROFILE => "SPACEOS_SERVICE_REGISTRAR_TOKEN_MCP",
        space_core::service_registration::WORLD_READ_PROFILE => {
            "SPACEOS_SERVICE_REGISTRAR_TOKEN_WORLD_READ"
        }
        space_core::service_registration::MCP_READ_PROFILE => "SPACEOS_SERVICE_REGISTRAR_TOKEN_MCP_READ",
        _ => return None,
    };
    std::env::var(key).ok().filter(|secret| !secret.is_empty())
}

#[cfg(test)]
mod tests {
    #[test]
    fn isolated_tests_cannot_load_inherited_registrar_credentials() {
        assert!(std::env::var_os("SPACEOS_TEST_ISOLATION_ROOT").is_some());
        assert!(super::from_env(space_core::service_registration::SEMANTIC_PROFILE).is_none());
        assert!(super::from_env(space_core::service_registration::MCP_PROFILE).is_none());
    }
}
