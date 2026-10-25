use crate::principal::PrincipalRegistry;
use crate::provider_observation::ObservationBindings;
use crate::query::{EventQuery, ObjectQuery, RelationQuery};
use crate::semantic::{
    semantic_world_id_from_env, validate_semantic_world_id, CreateSemanticObject,
    CreateSemanticRelation, PatchSemanticObject, SemanticCommandEnvelope, SEMANTIC_ACTOR,
};
use crate::semantic_read::{ObjectContextQuery, SemanticReadEnvelope};
use crate::service_registration::{self, ServiceRegistrarRegistry};
use crate::{
    ActionRequest, ActionResult, ActionStatus, Event, Object, Observation, ProviderInfo, Relation,
    Space, Storage,
};
use anyhow::{Context, Result};
use serde_json::{json, Value};
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

const TOKEN_FILE: &str = r"C:\spaceos\runtime\space-core.token";

fn configured_legacy_read_ids() -> HashSet<String> {
    // Laboratory fixtures inject their own allowlist and must never inherit
    // a production compatibility grant from the parent environment.
    if std::env::var_os("SPACEOS_TEST_ISOLATION_ROOT").is_some() {
        return HashSet::new();
    }
    let Ok(raw) = std::env::var("SPACEOS_LEGACY_READ_OBJECT_IDS") else {
        return HashSet::new();
    };
    // Invalid configuration grants nothing. Never interpret missing or bad
    // configuration as permission to enumerate all historical objects.
    serde_json::from_str::<Vec<String>>(&raw)
        .ok()
        .unwrap_or_default()
        .into_iter()
        .filter(|id| !id.trim().is_empty() && id.len() <= 512)
        .collect()
}

pub fn core_token() -> String {
    try_core_token().expect("Core transport credential is not configured")
}

/// Resolve an existing transport credential without ever creating a new one.
/// Production launchers must provision it before Core opens SQLite.
pub fn try_core_token() -> Result<String> {
    let explicit = std::env::var("SPACEOS_CORE_TOKEN")
        .ok()
        .filter(|t| !t.is_empty());
    resolve_core_token(
        explicit,
        std::env::var_os("SPACEOS_TEST_ISOLATION_ROOT").is_some(),
        || read_existing_core_token(std::path::Path::new(TOKEN_FILE)),
    )
}

fn read_existing_core_token(path: &std::path::Path) -> Result<String> {
    let token = std::fs::read_to_string(path)
        .context("Core transport credential file is missing or unreadable")?;
    let token = token.trim();
    anyhow::ensure!(!token.is_empty(), "Core transport credential file is empty");
    Ok(token.to_owned())
}

fn resolve_core_token(
    explicit: Option<String>,
    isolated: bool,
    fallback: impl FnOnce() -> Result<String>,
) -> Result<String> {
    // The isolation guard runs before the fallback closure, so a test cannot
    // silently read or create the live user's token file.
    if isolated {
        return explicit.context("isolated tests require SPACEOS_CORE_TOKEN");
    }
    explicit.map_or_else(fallback, Ok)
}

#[cfg(test)]
mod credential_isolation_tests {
    #[test]
    fn transport_credential_collision_cannot_authenticate_system() {
        use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
        let credential = URL_SAFE_NO_PAD.encode([33u8; 32]);
        let mut principals = crate::principal::PrincipalRegistry::default();
        principals
            .insert("system", [33u8; 32], ["agent.register"], [])
            .unwrap();
        let state = super::CoreState::new_with_semantic_world_and_identities(
            crate::Storage::open(":memory:").unwrap(),
            None,
            r"\\.\pipe\spaceos-4-3a4-unused-fs",
            r"\\.\pipe\spaceos-4-3a4-unused-browser",
            principals,
            crate::service_registration::ServiceRegistrarRegistry::default(),
            credential.clone(),
        );
        assert!(!super::principal_authenticated(
            &state,
            &serde_json::json!({"principal_token":credential}),
            "system"
        ));
    }

    #[test]
    fn missing_explicit_token_cannot_reach_fallback_in_isolation() {
        assert!(super::resolve_core_token(None, true, || panic!("production credential accessed"))
            .is_err());
        assert_eq!(
            super::resolve_core_token(Some("fixture".into()), true, || panic!("fallback")).unwrap(),
            "fixture"
        );
    }

    #[test]
    fn missing_transport_file_is_not_auto_created() {
        let path = std::env::temp_dir().join(format!(
            "spaceos-i2-absent-token-{}", uuid::Uuid::new_v4()
        ));
        assert!(!path.exists());
        assert!(super::read_existing_core_token(&path).is_err());
        assert!(!path.exists());
    }

    #[test]
    fn trace_payload_never_contains_principal_credential() {
        let mut payload = serde_json::json!({
            "principal_token": "top-secret",
            "instance_token": "instance-secret",
            "nested": [{"principal_token": "nested-secret", "registrar_token": "registrar-secret", "instance_token": "nested-instance-secret"}],
            "safe": "kept"
        });
        super::sanitize_trace_value(&mut payload);
        assert_eq!(payload, serde_json::json!({"nested": [{}], "safe": "kept"}));
    }
}

/// Principal identity is independent of the transport credential. Every
/// privileged actor has its own configured 32-byte credential and explicit
/// capability set. Agent requests remain Model A: actor_token alone suffices.
fn principal_authenticated(state: &CoreState, params: &Value, actor: &str) -> bool {
    let named = [
        params.get("actor").and_then(Value::as_str),
        params.get("actor_id").and_then(Value::as_str),
        params
            .get("auth")
            .and_then(|v| v.get("actor"))
            .and_then(Value::as_str),
    ];
    if named.into_iter().flatten().any(|name| name != actor) {
        return false;
    }
    let top_agent_token = params.get("actor_token").and_then(Value::as_str);
    let nested_agent_token = params
        .get("auth")
        .and_then(|v| v.get("actor_token"))
        .and_then(Value::as_str);
    if top_agent_token.is_some()
        && nested_agent_token.is_some()
        && top_agent_token != nested_agent_token
    {
        return false;
    }
    if Policy::is_core_principal(actor) {
        if top_agent_token.is_some_and(|s| !s.is_empty())
            || nested_agent_token.is_some_and(|s| !s.is_empty())
        {
            return false;
        }
        let principal_token = params.get("principal_token").and_then(Value::as_str);
        if principal_token == Some(state.core_token.as_str()) {
            return false;
        }
        state.privileged.authenticate(actor, principal_token)
    } else {
        if params.get("principal_token").is_some() {
            return false;
        }
        state
            .policy
            .authenticate(actor, top_agent_token.or(nested_agent_token))
    }
}

fn principal_authorized(
    state: &CoreState,
    params: &Value,
    actor: &str,
    capability: &str,
    world_id: Option<&str>,
) -> bool {
    principal_authenticated(state, params, actor)
        && state.policy.can(actor, capability)
        && (!Policy::is_core_principal(actor) || state.privileged.can(actor, capability, world_id))
}

/// Scope for the legacy read names. The separate read service may see only
/// the configured semantic World plus explicitly named historical objects.
/// Other Model A agents must request one World they actually belong to.
struct LegacyReadScope {
    world_id: Option<String>,
    legacy_ids: HashSet<String>,
}

impl LegacyReadScope {
    fn allows_object(&self, object: &Object) -> bool {
        let world = object.properties.get("world_id").and_then(Value::as_str).unwrap_or("");
        if world.is_empty() {
            self.legacy_ids.contains(&object.id)
        } else {
            self.world_id.as_deref() == Some(world)
        }
    }

    fn allows_relation(&self, relation: &Relation, storage: &Storage) -> bool {
        let in_scope = if relation.world_id.is_empty() {
            !self.legacy_ids.is_empty()
        } else {
            self.world_id.as_deref() == Some(relation.world_id.as_str())
        };
        if !in_scope {
            return false;
        }
        let Ok(Some(source)) = storage.get_object(&relation.source_object_id) else {
            return false;
        };
        let Ok(Some(target)) = storage.get_object(&relation.target_object_id) else {
            return false;
        };
        let source_world = source.properties.get("world_id").and_then(Value::as_str).unwrap_or("");
        let target_world = target.properties.get("world_id").and_then(Value::as_str).unwrap_or("");
        source_world == relation.world_id
            && target_world == relation.world_id
            && self.allows_object(&source)
            && self.allows_object(&target)
    }

    fn allows_event(&self, event: &Event) -> bool {
        // Legacy events have no trustworthy object-level ownership link.
        // Do not infer their visibility from an allowlisted object ID.
        self.world_id.as_deref() == Some(event.world_id.as_str())
            && !event.world_id.is_empty()
    }
}

fn legacy_read_scope(
    state: &CoreState,
    params: &Value,
) -> std::result::Result<LegacyReadScope, &'static str> {
    let actor = params.get("actor").and_then(Value::as_str).unwrap_or("");
    if actor.is_empty() {
        return Err("denied: authenticated read actor is required");
    }
    let requested = params.get("world_id").and_then(Value::as_str);
    let is_reader = actor == service_registration::WORLD_READ_ACTOR;
    let configured = state.semantic_world_id.as_ref().ok();
    let world_id = if is_reader {
        let Some(configured) = configured else {
            return Err("denied: semantic read World is not configured");
        };
        match requested {
            None => Some(configured.clone()),
            Some("") => None,
            Some(world) if world == configured => Some(configured.clone()),
            Some(_) => return Err("denied: read World is outside the service scope"),
        }
    } else {
        let Some(world) = requested.filter(|world| !world.is_empty()) else {
            return Err("denied: explicit read World is required");
        };
        Some(world.to_string())
    };
    let auth_world = if is_reader {
        configured.map(String::as_str)
    } else {
        world_id.as_deref()
    };
    if !principal_authorized(state, params, actor, "semantic.read", auth_world)
        || !auth_world.is_some_and(|world| state.policy.may_access_world(actor, world))
    {
        return Err("denied: read identity, capability or World membership is invalid");
    }
    let legacy_ids = if is_reader && requested.is_none_or(|world| world.is_empty()) {
        state.legacy_read_ids.lock().unwrap().clone()
    } else {
        HashSet::new()
    };
    Ok(LegacyReadScope { world_id, legacy_ids })
}

// SpaceTrace mutation wrapper (Slice 1). Emits start/commit/rollback
// around a Storage mutation. No semantic change: returns inner result.
// Hooks are no-ops when SpaceTrace is not initialized (I1, I17).
// Security (§24, deep): auth secrets are stripped from trace payloads at
// ANY nesting depth — metadata only, never tokens/credentials.
fn sanitize_trace_value(v: &mut Value) {
    const SECRETS: [&str; 11] = [
        "auth",
        "actor_token",
        "principal_token",
        "instance_token",
        "registrar_token",
        "token",
        "api_key",
        "password",
        "secret",
        "private_key",
        "session_secret",
    ];
    match v {
        Value::Object(map) => {
            for key in SECRETS {
                map.remove(key);
            }
            for child in map.values_mut() {
                sanitize_trace_value(child);
            }
        }
        Value::Array(items) => {
            for child in items.iter_mut() {
                sanitize_trace_value(child);
            }
        }
        _ => {}
    }
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ServiceRegistrationRequest {
    profile: String,
    name: String,
    worlds: Vec<String>,
    capabilities: Vec<String>,
    registrar_token: String,
    // The legacy IPC helper injects transport auth; it is not registration
    // authority and is deliberately ignored for this method.
    #[serde(default, rename = "auth")]
    _transport_auth: Option<String>,
}

fn trace_mutation<T>(
    method: &str,
    actor: &str,
    world_id: Option<&str>,
    correlation_id: &str,
    params: &Value,
    f: impl FnOnce() -> anyhow::Result<T>,
) -> anyhow::Result<T> {
    use spacetrace::hooks as st_hooks;
    let cid = correlation_id.to_string();
    let start = std::time::Instant::now();
    let mut safe_params = params.clone();
    sanitize_trace_value(&mut safe_params);
    st_hooks::global_hooks().core_mutation_start(
        &cid,
        method,
        actor,
        world_id,
        serde_json::to_value(&safe_params).unwrap_or_default(),
    );
    let result = f();
    let duration_ms = start.elapsed().as_millis() as u64;
    match &result {
        Ok(_) => {
            st_hooks::global_hooks().core_mutation_commit(
                &cid,
                method,
                actor,
                world_id,
                duration_ms,
            );
        }
        Err(e) => {
            st_hooks::global_hooks().core_mutation_rollback(
                &cid,
                method,
                actor,
                world_id,
                &e.to_string(),
            );
        }
    }
    result
}

pub const CORE_PIPE: &str = r"\\.\pipe\spaceos-core";
pub const PROVIDER_PIPE: &str = r"\\.\pipe\spaceos-fs-provider";
pub const BROWSER_PROVIDER_PIPE: &str = r"\\.\pipe\spaceos-browser-provider";

use crate::policy::Policy;
use crate::subscription::Subscription;
use spacetrace::SpaceTrace;
use std::collections::HashMap;

pub struct CoreState {
    pub storage: Arc<Mutex<Storage>>,
    pub events_tx: tokio::sync::broadcast::Sender<Event>,
    pub policy: Policy,
    pub privileged: PrincipalRegistry,
    /// Core-controlled Provider source to World map. Missing config denies observations.
    pub observation_bindings: ObservationBindings,
    pub service_registrars: ServiceRegistrarRegistry,
    pub core_token: String,
    /// Dispatch destinations. Defaults preserve the production pipe names;
    /// isolated tests can route to unique pipe fixtures instead.
    pub filesystem_provider_pipe: String,
    pub browser_provider_pipe: String,
    /// Fail-closed canonical scope for user semantic commands. A bad or
    /// missing environment setting disables only those commands.
    pub semantic_world_id: Result<String, String>,
    /// Explicit Core-controlled IDs of historical unscoped objects visible
    /// to the separate World read profile. No implicit legacy wildcard.
    pub legacy_read_ids: Mutex<HashSet<String>>,
    /// Ephemeral routing state. Subscriptions are NOT World state:
    /// they are never stored in SQLite and never reconstructed.
    pub subs: Mutex<HashMap<String, Subscription>>,
    /// Optional SpaceTrace observability subsystem.
    /// None = tracing disabled (no overhead). Some = active tracing.
    pub spacetrace: Option<SpaceTrace>,
    /// CoreSender for CoreHandle to communicate with Core.
    pub sender: CoreSender,
}

impl CoreState {
    /// Synthetic-only fixture configuration. Production obtains the same
    /// allowlist from Core's server-side environment at startup.
    pub fn set_isolated_legacy_read_ids(&self, ids: &[&str]) -> Result<()> {
        anyhow::ensure!(
            std::env::var_os("SPACEOS_TEST_ISOLATION_ROOT").is_some(),
            "legacy fixture configuration requires isolation"
        );
        anyhow::ensure!(
            ids.iter().all(|id| !id.trim().is_empty() && id.len() <= 512),
            "invalid legacy object ID"
        );
        *self.legacy_read_ids.lock().unwrap() = ids.iter().map(|id| (*id).to_string()).collect();
        Ok(())
    }

    pub fn new(storage: Storage) -> Arc<Self> {
        let (tx, _) = tokio::sync::broadcast::channel(512);
        let spacetrace = SpaceTrace::new().ok();
        if let Some(ref st) = spacetrace {
            spacetrace::init_global_hooks(st.buffer(), st.correlation());
        }
        let storage = Arc::new(Mutex::new(storage));
        let sender = CoreSender::new(tx.clone(), storage.clone());
        let privileged = PrincipalRegistry::from_env();
        let transport_token = core_token();
        let service_registrars = ServiceRegistrarRegistry::from_env(&privileged, &transport_token);
        let observation_bindings = ObservationBindings::from_env(&privileged, &transport_token);
        Arc::new(Self {
            storage,
            events_tx: tx,
            policy: Policy::new(),
            privileged,
            observation_bindings,
            service_registrars,
            core_token: transport_token,
            filesystem_provider_pipe: PROVIDER_PIPE.to_string(),
            browser_provider_pipe: BROWSER_PROVIDER_PIPE.to_string(),
            semantic_world_id: semantic_world_id_from_env(),
            legacy_read_ids: Mutex::new(configured_legacy_read_ids()),
            subs: Mutex::new(HashMap::new()),
            spacetrace,
            sender,
        })
    }

    /// Test/bootstrap constructor that does not mutate process environment.
    pub fn new_with_semantic_world(storage: Storage, world_id: Option<&str>) -> Arc<Self> {
        Self::new_with_semantic_world_and_provider_pipes(
            storage,
            world_id,
            PROVIDER_PIPE,
            BROWSER_PROVIDER_PIPE,
        )
    }

    /// Explicit dispatch destinations for isolated Core/Provider tests.
    /// Normal startup and existing callers retain the production defaults.
    pub fn new_with_semantic_world_and_provider_pipes(
        storage: Storage,
        world_id: Option<&str>,
        filesystem_provider_pipe: &str,
        browser_provider_pipe: &str,
    ) -> Arc<Self> {
        Self::new_with_semantic_world_and_provider_pipes_and_principals(
            storage,
            world_id,
            filesystem_provider_pipe,
            browser_provider_pipe,
            PrincipalRegistry::from_env(),
        )
    }

    /// Explicit principal fixture for isolated IPC tests. No environment
    /// secrets are read when the isolation marker is present.
    pub fn new_with_semantic_world_and_provider_pipes_and_principals(
        storage: Storage,
        world_id: Option<&str>,
        filesystem_provider_pipe: &str,
        browser_provider_pipe: &str,
        privileged: PrincipalRegistry,
    ) -> Arc<Self> {
        let transport_token = core_token();
        let service_registrars = ServiceRegistrarRegistry::from_env(&privileged, &transport_token);
        Self::new_with_semantic_world_and_identities(
            storage,
            world_id,
            filesystem_provider_pipe,
            browser_provider_pipe,
            privileged,
            service_registrars,
            transport_token,
        )
    }

    /// Synthetic identities for isolated IPC/HTTP fixtures.
    pub fn new_with_semantic_world_and_identities(
        storage: Storage,
        world_id: Option<&str>,
        filesystem_provider_pipe: &str,
        browser_provider_pipe: &str,
        privileged: PrincipalRegistry,
        service_registrars: ServiceRegistrarRegistry,
        transport_token: String,
    ) -> Arc<Self> {
        let (tx, _) = tokio::sync::broadcast::channel(512);
        let spacetrace = SpaceTrace::new().ok();
        if let Some(ref st) = spacetrace {
            spacetrace::init_global_hooks(st.buffer(), st.correlation());
        }
        let storage = Arc::new(Mutex::new(storage));
        let sender = CoreSender::new(tx.clone(), storage.clone());
        let observation_bindings = ObservationBindings::from_env(&privileged, &transport_token);
        Arc::new(Self {
            storage,
            events_tx: tx,
            policy: Policy::new(),
            privileged,
            observation_bindings,
            service_registrars,
            core_token: transport_token,
            filesystem_provider_pipe: filesystem_provider_pipe.to_string(),
            browser_provider_pipe: browser_provider_pipe.to_string(),
            semantic_world_id: world_id.map_or_else(
                || {
                    Err("semantic mutation API is unavailable: SPACEOS_SEMANTIC_WORLD_ID is not set"
                        .to_string())
                },
                validate_semantic_world_id,
            ),
            legacy_read_ids: Mutex::new(HashSet::new()),
            subs: Mutex::new(HashMap::new()),
            spacetrace,
            sender,
        })
    }

    pub fn new_with_storage(storage: Storage) -> Self {
        let (tx, _) = tokio::sync::broadcast::channel(512);
        let spacetrace = SpaceTrace::new().ok();
        if let Some(ref st) = spacetrace {
            spacetrace::init_global_hooks(st.buffer(), st.correlation());
        }
        let storage = Arc::new(Mutex::new(storage));
        let sender = CoreSender::new(tx.clone(), storage.clone());
        let privileged = PrincipalRegistry::from_env();
        let transport_token = core_token();
        let service_registrars = ServiceRegistrarRegistry::from_env(&privileged, &transport_token);
        let observation_bindings = ObservationBindings::from_env(&privileged, &transport_token);
        Self {
            storage,
            events_tx: tx,
            policy: Policy::new(),
            privileged,
            observation_bindings,
            service_registrars,
            core_token: transport_token,
            filesystem_provider_pipe: PROVIDER_PIPE.to_string(),
            browser_provider_pipe: BROWSER_PROVIDER_PIPE.to_string(),
            semantic_world_id: semantic_world_id_from_env(),
            legacy_read_ids: Mutex::new(configured_legacy_read_ids()),
            subs: Mutex::new(HashMap::new()),
            spacetrace,
            sender,
        }
    }
}

/// Sender for CoreHandle to communicate with Core.
#[derive(Clone)]
pub struct CoreSender {
    events_tx: tokio::sync::broadcast::Sender<crate::Event>,
    storage: Arc<Mutex<Storage>>,
}

impl CoreSender {
    pub fn new(
        events_tx: tokio::sync::broadcast::Sender<crate::Event>,
        storage: Arc<Mutex<Storage>>,
    ) -> Self {
        Self { events_tx, storage }
    }

    /// Record an event from a provider.
    pub async fn record_event(&self, event: crate::Event) -> Result<(), anyhow::Error> {
        // Fire and forget - if no subscribers, event is dropped
        let _ = self.events_tx.send(event);
        Ok(())
    }

    /// Query objects (read-only).
    pub async fn query_objects(
        &self,
        query: crate::query::ObjectQuery,
    ) -> Result<Vec<crate::Object>, anyhow::Error> {
        let storage = self.storage.lock().unwrap();
        storage.query_objects(&query).map_err(Into::into)
    }

    /// Get provider info by id.
    pub async fn provider_info(
        &self,
        id: &str,
    ) -> Result<Option<crate::ProviderInfo>, anyhow::Error> {
        let storage = self.storage.lock().unwrap();
        storage.get_provider(id).map_err(Into::into)
    }
}
/// (boot: provider + agents + views + tests) collides with 231 while
/// the loop recreates its instance. N concurrent loops keep N pending
/// instances; connection handlers are unchanged. Bounded, no protocol
/// or semantic change.
pub const ACCEPT_LOOPS: usize = 32;

pub async fn run_server(state: Arc<CoreState>, pipe_path: &str) -> Result<()> {
    // Shutdown guard (Breakage B4 follow-up): aborting `run_server` must
    // stop the detached accept loops too, otherwise a "restarted" server
    // races stale loops still serving the OLD state on the same pipe.
    // Dropping run_server (return or abort) fires the Notify; every loop
    // selects on it. No test or caller changes needed.
    struct KillOnDrop {
        n: std::sync::Arc<tokio::sync::Notify>,
    }
    impl Drop for KillOnDrop {
        fn drop(&mut self) {
            self.n.notify_waiters();
        }
    }
    #[cfg(windows)]
    {
        use tokio::net::windows::named_pipe::ServerOptions;
        let notify = std::sync::Arc::new(tokio::sync::Notify::new());
        let _guard = KillOnDrop { n: notify.clone() };
        for _ in 0..ACCEPT_LOOPS {
            let state = state.clone();
            let path = pipe_path.to_string();
            let n = notify.clone();
            tokio::spawn(async move {
                loop {
                    let server = match ServerOptions::new()
                        .reject_remote_clients(true)
                        .create(&path)
                    {
                        Ok(s) => s,
                        Err(e) => {
                            tracing::warn!("core accept create error: {e}");
                            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                            continue;
                        }
                    };
                    tokio::select! {
                        _ = n.notified() => break,
                        r = server.connect() => {
                            if r.is_err() {
                                continue;
                            }
                        }
                    }
                    let state = state.clone();
                    tokio::spawn(async move {
                        if let Err(e) = handle_connection(server, state).await {
                            tracing::warn!("core connection error: {e}");
                        }
                    });
                }
            });
        }
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
        }
    }
    #[cfg(not(windows))]
    {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:7777").await?;
        loop {
            let (stream, _) = listener.accept().await?;
            let state = state.clone();
            tokio::spawn(async move {
                if let Err(e) = handle_connection_tcp(stream, state).await {
                    tracing::warn!("core connection error: {e}");
                }
            });
        }
    }
}

async fn write_line<W: AsyncWriteExt + Unpin>(w: &mut W, v: &Value) -> Result<()> {
    let mut s = serde_json::to_string(v)?;
    s.push('\n');
    w.write_all(s.as_bytes()).await?;
    w.flush().await?;
    Ok(())
}

#[cfg(windows)]
async fn handle_connection(
    server: tokio::net::windows::named_pipe::NamedPipeServer,
    state: Arc<CoreState>,
) -> Result<()> {
    let (reader, mut writer) = tokio::io::split(server);
    run_protocol(reader, &mut writer, state).await
}

#[cfg(not(windows))]
async fn handle_connection_tcp(stream: tokio::net::TcpStream, state: Arc<CoreState>) -> Result<()> {
    let (reader, mut writer) = stream.into_split();
    run_protocol(reader, &mut writer, state).await
}

async fn run_protocol<R, W>(reader: R, writer: &mut W, state: Arc<CoreState>) -> Result<()>
where
    R: tokio::io::AsyncRead + Unpin,
    W: AsyncWriteExt + Unpin,
{
    let mut lines = BufReader::new(reader).lines();
    // For subscribe we need to keep writing notifications; use a writer channel.
    let (notify_tx, mut notify_rx) = tokio::sync::mpsc::channel::<(String, Arc<Value>, Event)>(256);
    let mut owned = ConnectionSubscriptions { state: state.clone(), tasks: Vec::new() };

    loop {
        tokio::select! {
            line = lines.next_line() => {
                match line? {
                    Some(l) if l.trim().is_empty() => {}
                    Some(l) => {
                        let req: Value = match serde_json::from_str(&l) {
                            Ok(v) => v,
                            Err(e) => {
                                write_line(writer, &json!({"ok": false, "error": format!("bad json: {e}")})).await?;
                                continue;
                            }
                        };
                        let method = req["method"].as_str().unwrap_or("").to_string();
                        let params = req.get("params").cloned().unwrap_or(json!({}));
                        if method == "subscribe" {
                            let proof = Arc::new(params.clone());
                            let resp = handle_subscribe(&state, params);
                            if let Some((sub_id, filter)) = resp.1 {
                                let mut rx = state.events_tx.subscribe();
                                let notify_tx = notify_tx.clone();
                                let state2 = state.clone();
                                let task_id = sub_id.clone();
                                let task = tokio::spawn(async move {
                                    while let Ok(ev) = rx.recv().await {
                                        // Subscription may have been cancelled.
                                        let subscriber = state2
                                            .subs
                                            .lock()
                                            .unwrap()
                                            .get(&sub_id)
                                            .map(|s| s.subscriber.clone());
                                        let Some(subscriber) = subscriber else {
                                            break;
                                        };
                                        if !subscription_authorized(&state2, &proof) {
                                            state2.subs.lock().unwrap().remove(&sub_id);
                                            break;
                                        }
                                        // Server-side filter BEFORE delivery:
                                        // non-matching events never reach this pipe.
                                        if !filter.matches(
                                            &ev.event_type,
                                            &ev.world_id,
                                            &ev.object_ids,
                                        ) {
                                            continue;
                                        }
                                        // T3->T4 trace: latency since Core commit (ev.timestamp).
                                        let latency_ms = chrono::Utc::now()
                                            .signed_duration_since(ev.timestamp)
                                            .num_milliseconds()
                                            .max(0) as u64;
                                        spacetrace::hooks::global_hooks().core_subscription_delivery(
                                            &sub_id,
                                            &subscriber,
                                            &ev.id,
                                            ev.correlation_id.clone(),
                                            &ev.actor,
                                            &ev.world_id,
                                            latency_ms,
                                        );
                                        if notify_tx.send((sub_id.clone(), proof.clone(), ev)).await.is_err() {
                                            break;
                                        }
                                    }
                                });
                                owned.tasks.push((task_id, task.abort_handle()));
                            }
                            write_line(writer, &resp.0).await?;
                            continue;
                        }
                        if method == "unsubscribe" {
                            let resp = handle_unsubscribe(&state, params);
                            write_line(writer, &resp).await?;
                            continue;
                        }
                        // IPC timing: measure full request→response time on Core side.
                        // Correlation id from params, fallback to generated uuid.
                        let correlation_id = params
                            .get("correlation_id")
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string())
                            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
                        // Extract actor from a clone to avoid borrow conflict with moving params
                        let actor = params
                            .get("actor")
                            .and_then(|v| v.as_str())
                            .or_else(|| params.get("actor_id").and_then(|v| v.as_str()))
                            .or_else(|| params.get("provider_id").and_then(|v| v.as_str()))
                            .or_else(|| {
                                params
                                    .get("auth")
                                    .and_then(|v| v.get("actor"))
                                    .and_then(|v| v.as_str())
                            })
                            .map(|s| s.to_string())
                            .unwrap_or_else(|| "unknown".to_string());
                        let start = std::time::Instant::now();
                        spacetrace::hooks::global_hooks().ipc_request_received(
                            &correlation_id,
                            &method,
                            &actor,
                            params.get("world_id").and_then(|v| v.as_str()),
                        );
                        spacetrace::hooks::global_hooks().ipc_request_dispatched(
                            &correlation_id,
                            &method,
                            &actor,
                        );
                        let resp = handle_method(&state, &method, params, &correlation_id).await;
                        let elapsed = start.elapsed().as_millis() as u64;
                        let ok = resp.get("ok").and_then(|v| v.as_bool()).unwrap_or(false);
                        // Lock wait will be reported by handle_method via thread-local; default 0.
                        // Note: we don't have lock_wait_ms here; handle_method will emit its own
                        // response hook with the lock wait measurement.
                        // We emit a second response hook here for the full IPC round-trip.
                        // To avoid double-reporting, handle_method will NOT emit ipc_response_sent.
                        // Instead, we emit the full IPC round-trip timing here.
                        spacetrace::hooks::global_hooks().ipc_response_sent(
                            &correlation_id,
                            &method,
                            &actor,
                            ok,
                            elapsed,
                            0, // lock_wait_ms would need coordination; 0 for now
                        );
                        write_line(writer, &resp).await?;
                    }
                    None => return Ok(()),
                }
            }
            Some((id, proof, event)) = notify_rx.recv() => {
                // Recheck at the final write boundary, including queued notifications.
                if state.subs.lock().unwrap().get(&id).is_some()
                    && subscription_authorized(&state, &proof) {
                    write_line(writer, &json!({"event": event})).await?;
                } else {
                    state.subs.lock().unwrap().remove(&id);
                }
            }
        }
    }
}

struct ConnectionSubscriptions {
    state: Arc<CoreState>,
    tasks: Vec<(String, tokio::task::AbortHandle)>,
}
impl Drop for ConnectionSubscriptions {
    fn drop(&mut self) {
        let mut subs = self.state.subs.lock().unwrap();
        for (id, task) in &self.tasks { subs.remove(id); task.abort(); }
    }
}

fn subscription_authorized(state: &CoreState, params: &Value) -> bool {
    let Some(actor) = params.get("subscriber").and_then(Value::as_str) else { return false; };
    let Some(world) = params.get("world_id").and_then(Value::as_str)
        .filter(|w| !w.trim().is_empty() && *w == w.trim()) else { return false; };
    principal_authorized(state, params, actor, "semantic.read", Some(world))
        && state.policy.may_access_world(actor, world)
        && (!Policy::is_core_principal(actor)
            || principal_authorized(state, params, actor, "subscription.subscribe", Some(world)))
}

#[cfg(test)]
mod i5_subscription_tests {
    use super::*;
    #[test]
    fn semantic_read_world_scope_and_rotation_are_rechecked() {
        let mut principals = PrincipalRegistry::default();
        use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
        principals.insert("cli", [13;32], ["semantic.read","subscription.subscribe"], ["A"]).unwrap();
        let state=CoreState::new_with_semantic_world_and_identities(Storage::open(":memory:").unwrap(),None,
            r"\\.\pipe\spaceos-4-3a4-unused-fs",r"\\.\pipe\spaceos-4-3a4-unused-browser",principals,
            service_registration::ServiceRegistrarRegistry::default(),"synthetic-i5".into());
        let first=state.policy.register("reader","system",vec!["semantic.read".into()],vec!["A".into()]).unwrap();
        let mut p=json!({"subscriber":"reader","actor_token":first,"world_id":"A"});
        assert!(subscription_authorized(&state,&p));
        p["world_id"]=json!("B"); assert!(!subscription_authorized(&state,&p));
        p["world_id"]=json!(""); assert!(!subscription_authorized(&state,&p));
        p.as_object_mut().unwrap().remove("world_id"); assert!(!subscription_authorized(&state,&p));
        p["world_id"]=json!("A");
        let next=state.policy.register("reader","system",vec![],vec!["A".into()]).unwrap();
        assert!(!subscription_authorized(&state,&p));
        p["actor_token"]=json!(next); assert!(!subscription_authorized(&state,&p));
        let mut principal=json!({"subscriber":"cli","principal_token":URL_SAFE_NO_PAD.encode([13;32])});
        assert!(!subscription_authorized(&state,&principal));
        principal["world_id"]=json!("A"); assert!(subscription_authorized(&state,&principal));
        principal["world_id"]=json!("B"); assert!(!subscription_authorized(&state,&principal));
        principal["world_id"]=json!("A"); principal["principal_token"]=json!("synthetic-i5");
        assert!(!subscription_authorized(&state,&principal));
    }
}

/// Authenticated subscription creation.
/// Returns (response, Option<(sub_id, filter)>). The filter is enforced
/// server-side before anything is written to the subscriber's pipe.
fn handle_subscribe(
    state: &Arc<CoreState>,
    params: Value,
) -> (Value, Option<(String, Subscription)>) {
    let deny = |msg: &str| (json!({"ok": false, "error": msg}), None);
    let subscriber = params
        .get("subscriber")
        .and_then(|v| v.as_str())
        .unwrap_or("cli")
        .to_string();
    // Legacy single-type form still accepted.
    let mut event_types: Vec<String> = params
        .get("event_types")
        .and_then(|v| v.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|x| x.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default();
    if let Some(t) = params.get("event_type").and_then(|v| v.as_str()) {
        if !event_types.contains(&t.to_string()) {
            event_types.push(t.to_string());
        }
    }
    let object_ids: Vec<String> = params
        .get("object_ids")
        .and_then(|v| v.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|x| x.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default();
    let world_id = params
        .get("world_id")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    if !subscription_authorized(state, &params) {
        audit_sub_event(state, &subscriber, "subscribe-read-authority-denied");
        return deny("denied: explicit World and authenticated semantic.read authority required");
    }
    // 1. Authenticate: subscriber must prove who it is.
    let authed = if Policy::is_core_principal(&subscriber) {
        principal_authorized(
            state,
            &params,
            &subscriber,
            "subscription.subscribe",
            world_id.as_deref(),
        )
    } else {
        match params.get("actor_token").and_then(|v| v.as_str()) {
            Some(t) => state.policy.authenticate(&subscriber, Some(t)),
            None => false,
        }
    };
    if !authed {
        audit_sub_event(state, &subscriber, "subscribe-denied");
        return deny("denied: subscribe requires authentication as subscriber");
    }
    // 2. Binding: subscriber must be a known principal (registered agent
    //    or core principal). Arbitrary names are rejected.
    let known = Policy::is_core_principal(&subscriber) || state.policy.is_registered(&subscriber);
    if !known {
        audit_sub_event(state, &subscriber, "subscribe-unknown-subscriber");
        return deny("denied: unknown subscriber");
    }
    // 3. World scope: agents may only subscribe inside their worlds.
    if let Some(w) = &world_id {
        if !state.policy.may_access_world(&subscriber, w) {
            audit_sub_event(state, &subscriber, "subscribe-world-denied");
            return deny("denied: subscriber has no access to this world");
        }
    } else if !Policy::is_core_principal(&subscriber) {
        audit_sub_event(state, &subscriber, "subscribe-unscoped-denied");
        return deny("denied: agents must subscribe with an explicit world_id");
    }

    let sub = Subscription::new(subscriber.clone(), world_id, event_types, object_ids);
    let id = sub.id.clone();
    state.subs.lock().unwrap().insert(id.clone(), sub.clone());
    (
        json!({"ok": true, "result": {"subscribed": true, "subscription_id": id, "subscriber": subscriber}}),
        Some((id, sub)),
    )
}

fn audit_sub_event(state: &Arc<CoreState>, subscriber: &str, what: &str) {
    let storage = state.storage.lock().unwrap();
    let mut e = Event::new(
        "SubscriptionDenied",
        subscriber.to_string(),
        json!({"what": what}),
    );
    e.provider_id = Some(subscriber.to_string());
    let _ = storage.record_event(&e);
    spacetrace::hooks::global_hooks().core_event_published(
        &e.id,
        e.correlation_id.clone(),
        &e.event_type,
        &e.actor,
        &e.world_id,
        e.provider_id.clone(),
    );
    let _ = state.events_tx.send(e);
}

fn handle_unsubscribe(state: &Arc<CoreState>, params: Value) -> Value {
    let id = params
        .get("subscription_id")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let mut subs = state.subs.lock().unwrap();
    let Some(sub) = subs.get(id).cloned() else {
        return json!({"ok": false, "error": "unknown subscription_id"});
    };
    let caller = params
        .get("subscriber")
        .and_then(|v| v.as_str())
        .unwrap_or("cli")
        .to_string();
    let authed = if Policy::is_core_principal(&caller) {
        principal_authorized(state, &params, &caller, "subscription.unsubscribe", sub.world_id.as_deref())
    } else {
        match params.get("actor_token").and_then(|v| v.as_str()) {
            Some(t) => state.policy.authenticate(&caller, Some(t)),
            None => false,
        }
    };
    // Only the owner (or a core principal) may cancel.
    if !authed || (caller != sub.subscriber && !Policy::is_core_principal(&caller)) {
        return json!({"ok": false, "error": "denied: only the owner may cancel"});
    }
    subs.remove(id);
    json!({"ok": true, "result": {"unsubscribed": id}})
}

async fn handle_method(
    state: &Arc<CoreState>,
    method: &str,
    params: Value,
    correlation_id: &str,
) -> Value {
    macro_rules! ok {
        ($v:expr) => {
            json!({"ok": true, "result": $v})
        };
    }
    macro_rules! err {
        ($e:expr) => {
            json!({"ok": false, "error": format!("{}", $e)})
        };
    }
    if method == "request_action" {
        return handle_action(state, params).await;
    }
    if method == "complete_provider_action" { return complete_provider_action(state, params).await; }
    if method == "get_object_context" {
        let envelope = match serde_json::from_value::<SemanticReadEnvelope<ObjectContextQuery>>(
            params.clone(),
        ) {
            Ok(envelope) => envelope,
            Err(error) => return json!({"ok": false, "error": format!("{error}")}),
        };
        if let Err(error) = envelope.query.validate() {
            return json!({"ok": false, "error": format!("{error}")});
        }
        let actor = envelope.actor.trim();
        if actor.is_empty() {
            return json!({"ok": false, "error": "denied: read actor is required"});
        }
        // Model A still accepts an actor_token without the general transport
        // token, but membership alone must not grant this read capability.
        let authed = principal_authorized(
            state,
            &params,
            actor,
            "semantic.read",
            Some(&envelope.query.world_id),
        );
        if !authed
            || !state
                .policy
                .may_access_world(actor, &envelope.query.world_id)
        {
            return json!({
                "ok": false,
                "error": "denied: authenticated actor has no access to the requested world"
            });
        }
        let mut storage = state.storage.lock().unwrap();
        return match storage.get_object_context(&envelope.query) {
            Ok(value) => ok!(json!(value)),
            Err(error) => err!(error),
        };
    }
    if method == "query_provider_objects" {
        let deny = |code: &str| json!({"ok":false,"error":code,"error_code":code});
        let instance = params["instance_id"].as_str().unwrap_or("");
        let token = params["instance_token"].as_str().unwrap_or("");
        let root = std::path::Path::new(params["source_root"].as_str().unwrap_or(""));
        let (root, world) = match state.observation_bindings.read_source(instance, token, root) {
            Ok(scope) => scope,
            Err(e) => return deny(if e.to_string() == "invalid_binding" {"invalid_binding"} else {"authorization_denied"}),
        };
        if !principal_authorized(state, &params, "filesystem", "filesystem.reconcile.read", Some(&world)) {
            return deny("authorization_denied");
        }
        if params.get("world_id").is_some_and(|v| v.as_str() != Some(&world)) { return deny("invalid_binding"); }
        let storage = state.storage.lock().unwrap();
        let query = crate::query::ObjectQuery { provider_id:Some("filesystem".into()), world_id:Some(world.clone()), ..Default::default() };
        return match storage.query_objects(&query) {
            Ok(objects) => {
                let objects: Vec<_> = objects.into_iter().filter(|o|
                    crate::provider_observation::ObservationBindings::source_owns_object(&root, &world, o)
                    && params.get("external_id").is_none_or(|v| o.external_id.as_deref() == v.as_str())).collect();
                ok!(json!({"objects":objects,"world_id":world}))
            },
            Err(_) => deny("database_error"),
        };
    }
    if method == "mission_execution" {
        #[derive(serde::Deserialize)] #[serde(deny_unknown_fields)]
        struct Envelope {actor:String,actor_token:String,command:crate::mission::MissionExecution,
            #[serde(default,rename="auth")] _auth:Option<Value>}
        let envelope:Envelope=match serde_json::from_value(params.clone()) {
            Ok(v)=>v,Err(_)=>return json!({"ok":false,"error":"invalid execution envelope"})};
        if envelope.actor!=SEMANTIC_ACTOR || envelope.actor_token.is_empty()
            || state.semantic_world_id.as_ref().ok()!=Some(&envelope.command.world_id)
            || !principal_authorized(state,&params,SEMANTIC_ACTOR,"mission.command",Some(&envelope.command.world_id))
            || !state.policy.may_access_world(SEMANTIC_ACTOR,&envelope.command.world_id) {
            return json!({"ok":false,"error":"denied: Mission execution authority or World"});
        }
        let result=state.storage.lock().unwrap().mission_execution(&envelope.command);
        return match result {Ok(result)=>{publish_committed_event(state,&result.event);ok!(json!(result))},Err(error)=>err!(error)};
    }
    if method == "mission_command" {
        use crate::mission::{MissionCommand, MissionError};
        #[derive(serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Envelope { actor: String, actor_token: String, command: MissionCommand,
            #[serde(default, rename="auth")] _auth: Option<Value> }
        let deny = |code: &str, message: &str| json!({"ok":false,"error_code":code,"error":message});
        let world = match &state.semantic_world_id {
            Ok(world) => world,
            Err(_) => return deny("configuration_error", "Mission commands require configured semantic World"),
        };
        let envelope: Envelope = match serde_json::from_value(params.clone()) {
            Ok(v) => v,
            Err(_) => return deny("validation_error", "invalid Mission command envelope"),
        };
        if envelope.actor != SEMANTIC_ACTOR || envelope.command.world_id != *world
            || envelope.actor_token.is_empty()
            || !principal_authorized(state,&params,SEMANTIC_ACTOR,"mission.command",Some(world))
            || !state.policy.may_access_world(SEMANTIC_ACTOR,world) {
            return deny("authorization_denied", "denied: Mission actor/World/capability or missing auth");
        }
        let result = state.storage.lock().unwrap().mission_command(&envelope.command);
        return match result {
            Ok(result) => {
                // The storage method has returned only after COMMIT.
                if result.applied { publish_committed_event(state,&result.event); }
                ok!(json!(result))
            },
            Err(error) => match error.downcast_ref::<MissionError>() {
                Some(error) => deny(error.code,&error.message),
                None => deny("runtime_error","Mission transaction failed"),
            },
        };
    }
    let read_scope = if matches!(
        method,
        "get_object"
            | "get_object_by_external"
            | "query_objects"
            | "get_space"
            | "list_spaces"
            | "get_world"
            | "get_relation"
            | "list_relations"
            | "query_relations"
            | "query_events"
    ) {
        match legacy_read_scope(state, &params) {
            Ok(scope) => Some(scope),
            Err(error) => return json!({"ok": false, "error": error}),
        }
    } else {
        None
    };
    if is_semantic_method(method) {
        if let Err(error) = &state.semantic_world_id {
            return json!({"ok": false, "error": error});
        }
    }
    // Policy gate: all World writes go through a single check.
    // Caller identity is bound independently of the transport token:
    // core principals use principal_token; agents use actor_token.
    if let Some((actor, cap, actor_token)) = mutation_gate(method, &params) {
        let _ = actor_token;
        let requested_world = params
            .get("command")
            .and_then(|v| v.get("world_id"))
            .or_else(|| params.get("world_id"))
            .and_then(Value::as_str);
        let authed = principal_authenticated(state, &params, &actor);
        // Delegation is intentionally limited to PATCH and related_to. Service
        // registrations and privileged principals do not become agent grants.
        let semantic_actor_allowed = actor == SEMANTIC_ACTOR
            || (matches!(method, "patch_semantic_object" | "create_semantic_relation")
                && state.policy.is_delegated_agent(&actor));
        let semantic_actor_mismatch = is_semantic_method(method) && !semantic_actor_allowed;
        // Observation scope is resolved from a trusted source below. Never
        // authorize it using an untrusted world_id in the request envelope.
        let gate_world = if method == "report_observation" { None } else { requested_world };
        let allowed = !semantic_actor_mismatch
            && principal_authorized(state, &params, &actor, &cap, gate_world);
        if !allowed {
            if method != "report_observation" {
                let lock_start = std::time::Instant::now();
                let storage = state.storage.lock().unwrap();
                let lock_wait_ms = lock_start.elapsed().as_millis() as u64;
                if lock_wait_ms > 0 {
                    spacetrace::hooks::global_hooks().ipc_response_sent(
                        correlation_id,
                        method,
                        &actor,
                        false,
                        0,
                        lock_wait_ms,
                    );
                }
                let mut e = Event::new(
                    "MutationDenied",
                    actor.clone(),
                    serde_json::json!({"method": method, "capability": cap, "auth": authed}),
                );
                e.provider_id = Some(actor.clone());
                let _ = storage.record_event(&e);
            }
            return json!({"ok": false, "error": if semantic_actor_mismatch {
                "denied: semantic actor or world scope is not allowed".to_string()
            } else {
                format!("denied: {actor} lacks {cap} or missing auth")
            }});
        }
        if is_semantic_method(method) {
            let configured_world = state
                .semantic_world_id
                .as_ref()
                .expect("semantic configuration checked above");
            let requested_world = params
                .get("command")
                .and_then(|v| v.get("world_id"))
                .and_then(|v| v.as_str());
            let scoped = semantic_actor_allowed
                && requested_world == Some(configured_world.as_str())
                && state.policy.may_access_world(&actor, configured_world);
            if !scoped {
                let storage = state.storage.lock().unwrap();
                let mut event = Event::new(
                    "MutationDenied",
                    actor.clone(),
                    serde_json::json!({
                        "method": method,
                        "capability": cap,
                        "reason": "semantic actor or world scope denied",
                    }),
                );
                event.provider_id = Some(actor.clone());
                event.world_id = requested_world.unwrap_or_default().to_string();
                let _ = storage.record_event(&event);
                return json!({"ok": false, "error": "denied: semantic actor or world scope is not allowed"});
            }
        }
    }
    let lock_start = std::time::Instant::now();
    let mut storage = state.storage.lock().unwrap();
    let lock_wait_ms = lock_start.elapsed().as_millis() as u64;
    if lock_wait_ms > 0 {
        let lock_actor = params
            .get("actor")
            .and_then(|v| v.as_str())
            .or_else(|| params.get("actor_id").and_then(|v| v.as_str()))
            .or_else(|| params.get("provider_id").and_then(|v| v.as_str()))
            .unwrap_or("core");
        spacetrace::hooks::global_hooks().ipc_response_sent(
            correlation_id,
            method,
            lock_actor,
            true,
            0,
            lock_wait_ms,
        );
    }
    match method {
        "ping" => ok!(json!({"pong": true})),
        "register_provider" => {
            let principal = params
                .get("principal")
                .and_then(Value::as_str)
                .unwrap_or("");
            let provider_id = params.get("id").and_then(Value::as_str).unwrap_or("");
            let scoped = match principal {
                "filesystem" => matches!(provider_id, "filesystem" | "process"),
                "browser" => provider_id == "browser",
                "system" => state.privileged.can(
                    "system",
                    &format!("provider.register.{provider_id}"),
                    None,
                ),
                _ => false,
            };
            if crate::provider_observation::reserved_browser_pin(provider_id) || !scoped
                || !principal_authorized(state, &params, principal, "provider.register", None)
            {
                json!({"ok": false, "error": "denied: provider registration requires authorized principal"})
            } else {
                match serde_json::from_value::<ProviderInfo>(params) {
                    Ok(p) => match storage.upsert_provider(&p) {
                        Ok(()) => ok!(json!({"registered": p.id})),
                        Err(e) => err!(e),
                    },
                    Err(e) => err!(e),
                }
            }
        }
        "register_provider_instance" => {
            let principal = params.get("actor").and_then(Value::as_str).unwrap_or("");
            let instance_id = params.get("instance_id").and_then(Value::as_str).unwrap_or("");
            let instance_token = params.get("instance_token").and_then(Value::as_str).unwrap_or("");
            if principal=="browser" {
                let a=match state.observation_bindings.browser_authority(instance_id,instance_token){Ok(a)=>a,Err(_)=>return json!({"ok":false,"error":"denied: unknown browser instance"})};
                if !principal_authorized(state,&params,"browser","provider.register",Some(&a.runtime.world_id))
                    || storage.get_provider("browser").ok().flatten().is_none()
                    || params["source_id"]!=a.runtime.source_id||params["runtime_epoch"]!=a.runtime.runtime_epoch||params["browser_debugger_id"]!=a.runtime.expected_browser_debugger_id {
                    return json!({"ok":false,"error":"denied: browser registration authority mismatch"});
                }
                return match storage.register_browser_authority(&a){Ok(())=>{state.observation_bindings.register_browser(instance_id);ok!(json!({"registered_instance":instance_id}))},Err(_)=>json!({"ok":false,"error":"denied: browser registration pin conflict"})};
            }
            let authorized = principal == "filesystem"
                && principal_authorized(state, &params, principal, "provider.register", None)
                && storage.get_provider("filesystem").ok().flatten().is_some();
            let record = if authorized {
                state.observation_bindings.registration_record(instance_id, instance_token)
            } else { None };
            match record {
                Some(record) => match storage.get_provider(&record.id) {
                    Ok(Some(previous)) if previous.name == record.name
                        && previous.capabilities == record.capabilities => {
                            state.observation_bindings.register_instance(instance_id, instance_token);
                            ok!(json!({"registered_instance": instance_id}))
                        }
                    Ok(None) => match storage.upsert_provider(&record) {
                        Ok(()) => {
                            state.observation_bindings.register_instance(instance_id, instance_token);
                            ok!(json!({"registered_instance": instance_id}))
                        }
                        Err(_) => json!({"ok": false, "error": "denied: Provider instance registration failed"}),
                    },
                    _ => json!({"ok": false, "error": "denied: Provider instance binding changed or invalid"}),
                },
                None => json!({"ok": false, "error": "denied: unknown or unauthenticated Provider instance"}),
            }
        }
        "list_providers" => match storage.list_providers() {
            Ok(v) => ok!(json!(v)),
            Err(e) => err!(e),
        },
        "create_object" => match serde_json::from_value::<Object>(params.clone()) {
            Ok(o) => {
                let actor = o.provider_id.clone();
                let world_id = o
                    .properties
                    .get("world_id")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                let cid = params
                    .get("correlation_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or(o.id.as_str())
                    .to_string();
                match trace_mutation(
                    "create_object",
                    &actor,
                    world_id.as_deref(),
                    &cid,
                    &params,
                    || storage.create_object(&o),
                ) {
                    Ok(()) => ok!(json!(o)),
                    Err(e) => err!(e),
                }
            }
            Err(e) => err!(e),
        },
        "create_semantic_object" => {
            match serde_json::from_value::<SemanticCommandEnvelope<CreateSemanticObject>>(
                params.clone(),
            ) {
                Ok(envelope) => match trace_mutation(
                    "create_semantic_object",
                    &envelope.actor,
                    Some(&envelope.command.world_id),
                    &envelope.command.correlation_id,
                    &params,
                    || storage.create_semantic_object(&envelope.command),
                ) {
                    Ok(result) => {
                        publish_committed_event(state, &result.event);
                        ok!(json!(result))
                    }
                    Err(e) => err!(e),
                },
                Err(e) => err!(e),
            }
        }
        "patch_semantic_object" => {
            match serde_json::from_value::<SemanticCommandEnvelope<PatchSemanticObject>>(
                params.clone(),
            ) {
                Ok(envelope) => match trace_mutation(
                    "patch_semantic_object",
                    &envelope.actor,
                    Some(&envelope.command.world_id),
                    &envelope.command.correlation_id,
                    &params,
                    || storage.patch_semantic_object_as(&envelope.command, &envelope.actor),
                ) {
                    Ok(result) => {
                        publish_committed_event(state, &result.event);
                        ok!(json!(result))
                    }
                    Err(e) => err!(e),
                },
                Err(e) => err!(e),
            }
        }
        "get_object" => {
            let id = params["id"].as_str().unwrap_or("");
            match storage.get_object(id) {
                Ok(Some(v)) if read_scope.as_ref().unwrap().allows_object(&v) => ok!(json!(v)),
                Ok(_) => json!({"ok": false, "error": "denied: object is outside read scope"}),
                Err(e) => err!(e),
            }
        }
        "get_object_by_external" => {
            let p = params["provider_id"].as_str().unwrap_or("");
            let e = params["external_id"].as_str().unwrap_or("");
            match storage.get_object_by_external(p, e) {
                Ok(Some(v)) if read_scope.as_ref().unwrap().allows_object(&v) => ok!(json!(v)),
                Ok(_) => json!({"ok": false, "error": "denied: object is outside read scope"}),
                Err(e) => err!(e),
            }
        }
        "update_object" => match serde_json::from_value::<Object>(params.clone()) {
            Ok(o) => {
                let internal = o.provider_id != "system";
                if o.r#type == "Agent" && internal && o.properties.get("capabilities").is_some() {
                    // capability changes on Agent objects are denied for non-system principals
                    if let Ok(Some(existing)) = storage.get_object(&o.id) {
                        if existing.properties.get("capabilities")
                            != o.properties.get("capabilities")
                        {
                            return json!({"ok": false, "error": "denied: capability self-grant is not allowed"});
                        }
                    }
                }
                let actor = o.provider_id.clone();
                let world_id = o
                    .properties
                    .get("world_id")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                let cid = params
                    .get("correlation_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or(o.id.as_str())
                    .to_string();
                match trace_mutation(
                    "update_object",
                    &actor,
                    world_id.as_deref(),
                    &cid,
                    &params,
                    || storage.update_object(&o),
                ) {
                    Ok(()) => ok!(json!(o)),
                    Err(e) => err!(e),
                }
            }
            Err(e) => err!(e),
        },
        "delete_object" => {
            let id = params["id"].as_str().unwrap_or("");
            let cid = params
                .get("correlation_id")
                .and_then(|v| v.as_str())
                .unwrap_or(id)
                .to_string();
            let world_id = params
                .get("world_id")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            match trace_mutation(
                "delete_object",
                "system",
                world_id.as_deref(),
                &cid,
                &params,
                || storage.delete_object(id),
            ) {
                Ok(()) => ok!(json!({"deleted": id})),
                Err(e) => err!(e),
            }
        }
        "query_objects" => match serde_json::from_value::<ObjectQuery>(params) {
            Ok(q) => match storage.query_objects(&q) {
                Ok(v) => ok!(json!(v.into_iter().filter(|object| read_scope.as_ref().unwrap().allows_object(object)).collect::<Vec<_>>())),
                Err(e) => err!(e),
            },
            Err(e) => err!(e),
        },
        "create_space" => match serde_json::from_value::<Space>(params.clone()) {
            Ok(s) => {
                let cid = params
                    .get("correlation_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or(s.id.as_str())
                    .to_string();
                match trace_mutation("create_space", "system", None, &cid, &params, || {
                    storage.create_space(&s)
                }) {
                    Ok(()) => ok!(json!(s)),
                    Err(e) => err!(e),
                }
            }
            Err(e) => err!(e),
        },
        "get_space" => {
            let id = params["id"].as_str().unwrap_or("");
            if read_scope.as_ref().unwrap().world_id.as_deref() != Some(id) {
                return json!({"ok": false, "error": "denied: Space is outside read scope"});
            }
            match storage.get_space(id) {
                Ok(v) => ok!(json!(v)),
                Err(e) => err!(e),
            }
        }
        "list_spaces" => match storage.list_spaces() {
            Ok(v) => ok!(json!(v.into_iter().filter(|space| read_scope.as_ref().unwrap().world_id.as_deref() == Some(space.id.as_str())).collect::<Vec<_>>())),
            Err(e) => err!(e),
        },
        "register_service_actor" => {
            let request: ServiceRegistrationRequest = match serde_json::from_value(params) {
                Ok(request) => request,
                Err(_) => {
                    return json!({"ok": false, "error": "invalid service registration request"})
                }
            };
            let Some(expected_actor) = service_registration::actor_for(&request.profile) else {
                return json!({"ok": false, "error": "unsupported service registration profile"});
            };
            if !state
                .service_registrars
                .authenticate(&request.profile, Some(&request.registrar_token))
            {
                return json!({"ok": false, "error": "service registration credential missing or invalid"});
            }
            let Some(expected_caps) = service_registration::capabilities_for(&request.profile)
            else {
                return json!({"ok": false, "error": "unsupported service registration profile"});
            };
            let expected_worlds = if matches!(
                request.profile.as_str(),
                service_registration::SEMANTIC_PROFILE | service_registration::WORLD_READ_PROFILE | service_registration::MCP_READ_PROFILE | service_registration::BROWSER_WORLD_PROFILE
            ) {
                match &state.semantic_world_id {
                    Ok(world) => vec![world.clone()],
                    Err(_) => {
                        return json!({"ok": false, "error": "semantic world configuration unavailable"})
                    }
                }
            } else {
                vec![]
            };
            if request.name != expected_actor {
                return json!({"ok": false, "error": "service actor does not match profile"});
            }
            if request.worlds != expected_worlds {
                return json!({"ok": false, "error": "service world does not match Core configuration"});
            }
            let expected_caps: Vec<String> =
                expected_caps.iter().map(|cap| (*cap).to_string()).collect();
            if request.capabilities != expected_caps {
                return json!({"ok": false, "error": "service capabilities do not match profile"});
            }
            if request.profile == service_registration::BROWSER_WORLD_PROFILE {
                if state.observation_bindings.credential_matches_instance(&request.registrar_token)
                    || ["SPACEOS_DISPATCH_KEY_BROWSER", "SPACEOS_DISPATCH_KEY_FILESYSTEM"].iter()
                        .any(|name|std::env::var(name).is_ok_and(|key|key == request.registrar_token)) {
                    return json!({"ok":false,"error":"scoped Browser registrar credential domain conflict"});
                }
                let authority = match state.observation_bindings.browser_action_authority(&expected_worlds[0]) {
                    Ok(a) => a,
                    Err(_) => return json!({"ok":false,"error":"scoped Browser source unavailable or ambiguous"}),
                };
                if storage.validate_browser_authority(&authority).is_err() {
                    return json!({"ok":false,"error":"scoped Browser authority pins unavailable"});
                }
            }
            match state.policy.register_scoped_service(
                expected_actor,
                expected_caps,
                expected_worlds,
            ) {
                Ok(token) => {
                    spacetrace::hooks::global_hooks().agent_register(
                        expected_actor,
                        &format!("agent:{expected_actor}"),
                        None,
                        None,
                    );
                    ok!(json!({"registered": expected_actor, "token": token}))
                }
                Err(error) => json!({"ok": false, "error": error}),
            }
        }
        "register_agent" => {
            let principal = params["principal"].as_str().unwrap_or("");
            let name = params["name"].as_str().unwrap_or("");
            let auth = principal_authorized(state, &params, principal, "agent.register", None);
            let caps: Vec<String> = params["capabilities"]
                .as_array()
                .map(|v| {
                    v.iter()
                        .filter_map(|x| x.as_str().map(|s| s.to_string()))
                        .collect()
                })
                .unwrap_or_default();
            let worlds: Vec<String> = params["worlds"]
                .as_array()
                .map(|v| {
                    v.iter()
                        .filter_map(|x| x.as_str().map(|s| s.to_string()))
                        .collect()
                })
                .unwrap_or_default();
            let delegated = caps.iter().all(|cap| {
                state
                    .privileged
                    .can(principal, &format!("agent.grant.{cap}"), None)
            }) && worlds
                .iter()
                .all(|world| state.privileged.world_allowed(principal, world));
            if principal == "system" && auth && delegated {
                match state.policy.register(name, principal, caps, worlds) {
                    Some(token) => {
                        spacetrace::hooks::global_hooks().agent_register(
                            name,
                            &format!("agent:{name}"),
                            None,
                            None,
                        );
                        ok!(json!({"registered": name, "token": token}))
                    }
                    None => json!({"ok": false, "error": "denied: invalid agent name"}),
                }
            } else {
                json!({"ok": false, "error": "denied: register_agent requires authorized system principal"})
            }
        }
        "policy_caps" => {
            let actor = params["actor"].as_str().unwrap_or("");
            if actor.is_empty() || !principal_authenticated(state, &params, actor) {
                json!({"ok": false, "error": "denied: policy capabilities require verified self identity"})
            } else {
                ok!(json!({"capabilities": state.policy.capabilities_of(actor)}))
            }
        }
        "get_world" => {
            let sid = params["world_id"].as_str().map(|s| s.to_string());
            let objects = storage.query_objects(&crate::query::ObjectQuery {
                world_id: sid.clone(),
                ..Default::default()
            });
            let relations = storage.query_relations(&crate::query::RelationQuery {
                world_id: sid.clone(),
                ..Default::default()
            });
            let events = storage.query_events(&crate::query::EventQuery {
                world_id: sid.clone(),
                ..Default::default()
            });
            let spaces = storage.list_spaces();
            match (spaces, objects, relations, events) {
                (Ok(spaces), Ok(objects), Ok(relations), Ok(events)) => {
                    let scope = read_scope.as_ref().unwrap();
                    ok!(json!({
                        "spaces": spaces.into_iter().filter(|space| scope.world_id.as_deref() == Some(space.id.as_str())).collect::<Vec<_>>(),
                        "objects": objects.into_iter().filter(|object| scope.allows_object(object)).collect::<Vec<_>>(),
                        "relations": relations.into_iter().filter(|relation| scope.allows_relation(relation, &storage)).collect::<Vec<_>>(),
                        "events": events.into_iter().filter(|event| scope.allows_event(event)).collect::<Vec<_>>(),
                    }))
                },
                (s, o, r, e) => {
                    let msg = match (s, o, r, e) {
                        (Err(e), _, _, _)
                        | (_, Err(e), _, _)
                        | (_, _, Err(e), _)
                        | (_, _, _, Err(e)) => e.to_string(),
                        _ => unreachable!(),
                    };
                    json!({"ok": false, "error": msg})
                }
            }
        }
        "get_relation" => {
            let id = params["id"].as_str().unwrap_or("");
            match storage.get_relation(id) {
                Ok(Some(v)) if read_scope.as_ref().unwrap().allows_relation(&v, &storage) => ok!(json!(v)),
                Ok(_) => json!({"ok": false, "error": "denied: relation is outside read scope"}),
                Err(e) => err!(e),
            }
        }
        "list_relations" => {
            match storage.query_relations(&crate::query::RelationQuery::default()) {
                Ok(v) => ok!(json!(v.into_iter().filter(|relation| read_scope.as_ref().unwrap().allows_relation(relation, &storage)).collect::<Vec<_>>())),
                Err(e) => err!(e),
            }
        }
        "create_relation" => match serde_json::from_value::<Relation>(params.clone()) {
            Ok(r) => {
                let world_id = Some(r.world_id.clone());
                let cid = params
                    .get("correlation_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or(r.id.as_str())
                    .to_string();
                // Actor: explicit param wins (mutation_gate resolution); the
                // source object id is only a fallback (Level D finding).
                let actor = params
                    .get("actor")
                    .and_then(|v| v.as_str())
                    .or_else(|| params.get("actor_id").and_then(|v| v.as_str()))
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| r.source_object_id.clone());
                match trace_mutation(
                    "create_relation",
                    &actor,
                    world_id.as_deref(),
                    &cid,
                    &params,
                    || storage.create_relation(&r),
                ) {
                    Ok(()) => ok!(json!(r)),
                    Err(e) => err!(e),
                }
            }
            Err(e) => err!(e),
        },
        "create_semantic_relation" => {
            // Additional read authority is required only for provider references;
            // preserve the established semantic-to-semantic capability contract.
            // The Core storage lock is held; storage revalidates both endpoints
            // and the durable receipt inside the relation's write transaction.
            for key in ["source_id", "target_id"] {
                if let Some(id) = params["command"][key].as_str() {
                    match storage.get_object(id) {
                        Ok(Some(object)) if object.provider_id != SEMANTIC_ACTOR => {
                            let actor = params["actor"].as_str().unwrap_or("");
                            let world = params["command"]["world_id"].as_str().unwrap_or("");
                            if !principal_authorized(state, &params, actor, "semantic.read", Some(world)) {
                                return json!({"ok":false,"error":"denied: provider reference requires semantic.read in the requested World"});
                            }
                        }
                        Err(error) => return err!(error),
                        _ => {}
                    }
                }
            }
            match serde_json::from_value::<SemanticCommandEnvelope<CreateSemanticRelation>>(
                params.clone(),
            ) {
                Ok(envelope) => match trace_mutation(
                    "create_semantic_relation",
                    &envelope.actor,
                    Some(&envelope.command.world_id),
                    &envelope.command.correlation_id,
                    &params,
                    || storage.create_semantic_relation_as(&envelope.command, &envelope.actor),
                ) {
                    Ok(result) => {
                        publish_committed_event(state, &result.event);
                        ok!(json!(result))
                    }
                    Err(e) => err!(e),
                },
                Err(e) => err!(e),
            }
        }
        "delete_relation" => {
            let id = params["id"].as_str().unwrap_or("");
            let cid = params
                .get("correlation_id")
                .and_then(|v| v.as_str())
                .unwrap_or(id)
                .to_string();
            let world_id = params
                .get("world_id")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            match trace_mutation(
                "delete_relation",
                "system",
                world_id.as_deref(),
                &cid,
                &params,
                || storage.delete_relation(id),
            ) {
                Ok(()) => ok!(json!({"deleted": id})),
                Err(e) => err!(e),
            }
        }
        "query_relations" => match serde_json::from_value::<RelationQuery>(params) {
            Ok(q) => match storage.query_relations(&q) {
                Ok(v) => ok!(json!(v.into_iter().filter(|relation| read_scope.as_ref().unwrap().allows_relation(relation, &storage)).collect::<Vec<_>>())),
                Err(e) => err!(e),
            },
            Err(e) => err!(e),
        },
        "record_event" => match serde_json::from_value::<Event>(params.clone()) {
            Ok(mut e) => {
                let cid = e.correlation_id.clone().unwrap_or_else(|| e.id.clone());
                let world_id = e.world_id.clone();
                let actor = e.actor.clone();
                let r = trace_mutation(
                    "record_event",
                    &actor,
                    Some(world_id.as_str()),
                    &cid,
                    &params,
                    || storage.record_event_outcome(&e),
                );
                if let Ok((inserted, seq)) = r.as_ref() {
                    e.seq = *seq;
                    if *inserted {
                    let _ = state.events_tx.send(e.clone());
                    // T3 trace: published only after durable commit (ST-B13).
                    spacetrace::hooks::global_hooks().core_event_published(
                        &e.id,
                        e.correlation_id.clone(),
                        &e.event_type,
                        &e.actor,
                        &e.world_id,
                        e.provider_id.clone(),
                    );
                    }
                }
                match r {
                    Ok(_) => ok!(json!(e)),
                    Err(e) => err!(e),
                }
            }
            Err(e) => err!(e),
        },
        // Phase B: providers report facts; ONLY Core applies domain
        // changes, atomically (objects → relations → event → log).
        // Legacy per-row methods above keep working unchanged.
        "report_observation" => match serde_json::from_value::<Observation>(params.clone()) {
            Ok(obs) => {
                // The credential proves the fixed filesystem principal; JSON
                // fields are only consistency checks, never identity grants.
                let actor = params
                    .get("actor")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                if actor=="browser" {
                    let top=["observation_id","world_id","facts","objects","relations","delete_ids","event","actor","principal_token","instance_id","instance_token","auth"];
                    let object_fields=["id","type","properties","provider_id","external_id","created_at","updated_at"];
                    let event_fields=["id","event_type","actor","provider_id","object_ids","payload","timestamp","correlation_id","world_id","seq"];
                    if params.as_object().is_none_or(|o|o.keys().any(|k|!top.contains(&k.as_str())))
                        ||params["objects"].as_array().is_none_or(|objects|objects.iter().any(|o|o.as_object().is_none_or(|m|m.keys().any(|k|!object_fields.contains(&k.as_str())))))
                        ||params["event"].as_object().is_none_or(|o|o.keys().any(|k|!event_fields.contains(&k.as_str())))
                        ||serde_json::to_vec(&params).map_or(true,|b|b.len()>24576){return json!({"ok":false,"error":"denied: invalid browser envelope"});}
                    let v=match state.observation_bindings.verify_browser_tab_created(params["instance_id"].as_str().unwrap_or(""),params["instance_token"].as_str().unwrap_or(""),obs){Ok(v)=>v,Err(_)=>return json!({"ok":false,"error":"denied: browser observation validation failed"})};
                    if !principal_authorized(state,&params,"browser","observation.report",Some(&v.authority.runtime.world_id)){return json!({"ok":false,"error":"denied: browser World grant missing"});}
                    return match storage.apply_verified_browser_tab_created(&v){Ok((outcome,object,event))=>{if outcome.applied{publish_committed_event(state,&event);}ok!(json!({"applied":outcome.applied,"event_id":outcome.event_id,"seq":outcome.seq,"object":object}))},Err(_)=>json!({"ok":false,"error":"denied: browser persistence identity or transaction conflict"})};
                }
                if actor != "filesystem"
                    || params.get("actor_id").is_some()
                    || params.get("provider_id").is_some()
                {
                    return json!({"ok": false, "error": "denied: unsupported observation principal claim"});
                }
                let instance_id = params.get("instance_id").and_then(Value::as_str).unwrap_or("");
                let instance_token = params.get("instance_token").and_then(Value::as_str).unwrap_or("");
                let admission = if obs.event.event_type == "FileChanged" {
                    state.observation_bindings.verify_file_changed(instance_id, instance_token, obs, &storage)
                } else {
                    state.observation_bindings.verify_file_created(instance_id, instance_token, obs, &storage)
                };
                let verified = match admission {
                    Ok(verified) if state.privileged.can("filesystem", "observation.report", Some(&verified.observation().world_id)) => verified,
                    Ok(_) => return json!({"ok": false, "error": "denied: Provider lacks observation World grant"}),
                    Err(error) => return json!({"ok": false, "error": format!("denied: {error}")}),
                };
                let obs = verified.observation();
                let world_id = &obs.world_id;
                let cid = obs
                    .event
                    .correlation_id
                    .clone()
                    .unwrap_or_else(|| obs.observation_id.clone());
                // Reuses the outer storage lock (same thread: no re-lock).
                let r = trace_mutation(
                    "report_observation",
                    actor,
                    Some(world_id.as_str()),
                    &cid,
                    &params,
                    || if obs.event.event_type == "FileChanged" {
                        storage.apply_verified_file_changed(&verified)
                    } else {
                        storage.apply_admitted_file_created(&verified)
                    },
                );
                match r {
                    Ok(outcome) => {
                        // Broadcast only what was durably saved: redelivery
                        // ({applied:false}) and failures announce nothing.
                        if outcome.applied {
                            let mut e = obs.event.clone();
                            e.seq = outcome.seq;
                            let _ = state.events_tx.send(e.clone());
                            spacetrace::hooks::global_hooks().core_event_published(
                                &e.id,
                                e.correlation_id.clone(),
                                &e.event_type,
                                &e.actor,
                                &e.world_id,
                                e.provider_id.clone(),
                            );
                        }
                        ok!(json!(outcome))
                    }
                    Err(e) => err!(e),
                }
            }
            Err(e) => err!(e),
        },
        "query_events" => match serde_json::from_value::<EventQuery>(params) {
            Ok(q) => {
                let limit = q.limit.map(|value| value.min(crate::storage::MAX_EVENT_LIMIT));
                let mut unbounded = q;
                unbounded.limit = None;
                match storage.query_events(&unbounded) {
                Ok(v) => {
                    let mut visible: Vec<_> = v.into_iter().filter(|event| read_scope.as_ref().unwrap().allows_event(event)).collect();
                    if let Some(limit) = limit {
                        if visible.len() > limit {
                            visible = visible.split_off(visible.len() - limit);
                        }
                    }
                    ok!(json!(visible))
                },
                Err(e) => err!(e),
            }
            },
            Err(e) => err!(e),
        },
        "trace_stats" => {
            // I22: trace access is policy controlled. Authenticated callers
            // only; counts without identifiers (no payload leak).
            let caller = params
                .get("subscriber")
                .and_then(|v| v.as_str())
                .or_else(|| params.get("actor").and_then(|v| v.as_str()))
                .unwrap_or("cli")
                .to_string();
            let is_core = Policy::is_core_principal(&caller);
            let authed = if is_core {
                principal_authorized(state, &params, &caller, "trace.read", None)
            } else {
                match params.get("actor_token").and_then(|v| v.as_str()) {
                    Some(t) => state.policy.authenticate(&caller, Some(t)),
                    None => false,
                }
            };
            let known = is_core || state.policy.is_registered(&caller);
            if !authed || !known {
                json!({"ok": false, "error": "denied: trace_stats requires authentication"})
            } else if let Some(st) = state.spacetrace.as_ref() {
                ok!(json!(st.provider().all_stats()))
            } else {
                json!({"ok": false, "error": "trace disabled"})
            }
        }
        "trace_query" => {
            // I22/I23: authenticated + world-scoped for agents; core principals unscoped.
            let caller = params
                .get("subscriber")
                .and_then(|v| v.as_str())
                .or_else(|| params.get("actor").and_then(|v| v.as_str()))
                .unwrap_or("cli")
                .to_string();
            let is_core = Policy::is_core_principal(&caller);
            let authed = if is_core {
                principal_authorized(
                    state,
                    &params,
                    &caller,
                    "trace.read",
                    params.get("world_id").and_then(Value::as_str),
                )
            } else {
                match params.get("actor_token").and_then(|v| v.as_str()) {
                    Some(t) => state.policy.authenticate(&caller, Some(t)),
                    None => false,
                }
            };
            let known = is_core || state.policy.is_registered(&caller);
            if !authed || !known {
                json!({"ok": false, "error": "denied: trace_query requires authentication"})
            } else {
                let world_id = params
                    .get("world_id")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                if let Some(w) = &world_id {
                    if !is_core && !state.policy.may_access_world(&caller, w) {
                        return json!({"ok": false, "error": "denied: no access to this world"});
                    }
                } else if !is_core {
                    return json!({"ok": false, "error": "denied: agents must query with an explicit world_id"});
                }
                match state.spacetrace.as_ref() {
                    Some(st) => {
                        let mut events =
                            spacetrace::provider_ipc::handle(st.provider(), "query", params);
                        // I23 enforcement: non-core callers only ever see their world.
                        if !is_core {
                            if let Some(w) = &world_id {
                                if let Some(arr) =
                                    events.get_mut("result").and_then(|v| v.as_array_mut())
                                {
                                    arr.retain(|e| {
                                        e.get("world_id").and_then(|v| v.as_str())
                                            == Some(w.as_str())
                                    });
                                }
                            }
                        }
                        events
                    }
                    None => json!({"ok": false, "error": "trace disabled"}),
                }
            }
        }
        other => json!({"ok": false, "error": format!("unknown method {other}")}),
    }
}

fn mutation_gate(method: &str, params: &Value) -> Option<(String, String, Option<String>)> {
    let actor = params
        .get("actor")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .or_else(|| {
            params
                .get("actor_id")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
        })
        .or_else(|| {
            params
                .get("provider_id")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
        })
        .or_else(|| {
            params
                .get("auth")
                .and_then(|v| v.get("actor"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
        });
    let actor_token = params
        .get("actor_token")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .or_else(|| {
            params
                .get("auth")
                .and_then(|v| v.get("actor_token"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
        });
    let cap = match method {
        "create_object" => Some("object.create"),
        "update_object" => Some("object.update"),
        "delete_object" => Some("object.delete"),
        "create_relation" => Some("relation.create"),
        "delete_relation" => Some("relation.delete"),
        "record_event" => Some("event.record"),
        "report_observation" => Some("observation.report"),
        "create_space" => Some("space.create"),
        "create_semantic_object" => Some("semantic.object.create"),
        "patch_semantic_object" => Some("semantic.object.patch"),
        "create_semantic_relation" => Some("semantic.relation.create"),
        _ => None,
    };
    match (actor, cap) {
        (Some(a), Some(c)) => Some((a, c.to_string(), actor_token)),
        (None, Some(c)) => Some(("anonymous".to_string(), c.to_string(), actor_token)),
        _ => None,
    }
}

fn is_semantic_method(method: &str) -> bool {
    matches!(
        method,
        "create_semantic_object" | "patch_semantic_object" | "create_semantic_relation"
    )
}

fn publish_committed_event(state: &CoreState, event: &Event) {
    let _ = state.events_tx.send(event.clone());
    spacetrace::hooks::global_hooks().core_event_published(
        &event.id,
        event.correlation_id.clone(),
        &event.event_type,
        &event.actor,
        &event.world_id,
        event.provider_id.clone(),
    );
}

#[path = "browser_actions.rs"]
mod browser_actions;

async fn handle_action(state: &Arc<CoreState>, params: Value) -> Value {
    if params["actor"] == service_registration::BROWSER_WORLD_ACTOR {
        return browser_actions::handle(state, params).await;
    }
    if params["target"].get("scope_version").is_some() || params["target"].get("runtime").is_some() {
        return json!({"ok":false,"error":"denied: scoped Browser actor required"});
    }
    let req: ActionRequest = match serde_json::from_value(params.clone()) {
        Ok(r) => r,
        Err(e) => return json!({"ok": false, "error": format!("{e}")}),
    };
    let ok = |v: Value| json!({"ok": true, "result": v});
    let authed = principal_authenticated(state, &params, &req.actor);
    let need_cap = format!("action.{}", req.action);
    let provider_scoped = !Policy::is_core_principal(&req.actor)
        || state.privileged.can(
            &req.actor,
            &format!("dispatch.{}.{}", req.provider_id, req.action),
            None,
        );
    if !principal_authorized(state, &params, &req.actor, &need_cap, None) || !provider_scoped {
        let mut denied = Event::new(
            "ActionDenied",
            req.actor.clone(),
            json!({"action": req.action, "auth": authed}),
        );
        denied.provider_id = Some(req.provider_id.clone());
        denied.correlation_id = Some(req.id.clone());
        {
            let storage = state.storage.lock().unwrap();
            let _ = storage.record_event(&denied);
        }
        spacetrace::hooks::global_hooks().core_event_published(
            &denied.id,
            denied.correlation_id.clone(),
            &denied.event_type,
            &denied.actor,
            &denied.world_id,
            denied.provider_id.clone(),
        );
        let _ = state.events_tx.send(denied);
        return ok(json!(ActionResult {
            action_id: req.id,
            status: ActionStatus::Denied,
            detail: json!({"error": "denied: missing auth or capability"}),
        }));
    }
    if req.action.trim().is_empty() {
        let mut denied = Event::new(
            "ActionDenied",
            req.actor.clone(),
            json!({"action": req.action}),
        );
        denied.provider_id = Some(req.provider_id.clone());
        {
            let storage = state.storage.lock().unwrap();
            let _ = storage.record_event(&denied);
        }
        spacetrace::hooks::global_hooks().core_event_published(
            &denied.id,
            denied.correlation_id.clone(),
            &denied.event_type,
            &denied.actor,
            &denied.world_id,
            denied.provider_id.clone(),
        );
        let _ = state.events_tx.send(denied);
        return ok(json!({"status": "Denied"}));
    }
    let mut requested = Event::new(
        "ActionRequested",
        req.actor.clone(),
        json!({"action": req.action, "target": req.target}),
    );
    requested.provider_id = Some(req.provider_id.clone());
    requested.correlation_id = Some(req.id.clone());
    {
        let storage = state.storage.lock().unwrap();
        let _ = storage.record_event(&requested);
    }
    spacetrace::hooks::global_hooks().core_event_published(
        &requested.id,
        requested.correlation_id.clone(),
        &requested.event_type,
        &requested.actor,
        &requested.world_id,
        requested.provider_id.clone(),
    );
    let _ = state.events_tx.send(requested);
    let outcome = forward_action_to_provider(
        state,
        &req,
        &req.id,
        &state.filesystem_provider_pipe,
        &state.browser_provider_pipe,
    )
    .await;
    match outcome {
        Ok(result) => {
            // Started is already durably confirmed by the dispatch path.
            if result.status == ActionStatus::Started { return ok(json!(result)); }
            let mut done = Event::new(
                match result.status {
                    ActionStatus::Completed => "ActionCompleted",
                    ActionStatus::Failed => "ActionFailed",
                    ActionStatus::Denied => "ActionDenied",
                    // Async jobs answer Started immediately; the terminal
                    // ActionCompleted/ActionFailed event arrives later under
                    // the same action id as correlation_id.
                    ActionStatus::Started | ActionStatus::Requested => "ActionStarted",
                },
                req.actor.clone(),
                result.detail.clone(),
            );
            done.provider_id = Some(req.provider_id.clone());
            done.correlation_id = Some(req.id.clone());
            {
                let storage = state.storage.lock().unwrap();
                let _ = storage.record_event(&done);
            }
            spacetrace::hooks::global_hooks().core_event_published(
                &done.id,
                done.correlation_id.clone(),
                &done.event_type,
                &done.actor,
                &done.world_id,
                done.provider_id.clone(),
            );
            let _ = state.events_tx.send(done);
            ok(json!(result))
        }
        Err(e) => {
            // A lost dispatch/Started response is not proof external execution failed.
            json!({"ok":false,"error":e.to_string(),"execution_confirmed":false})
        }
    }
}

async fn forward_action_to_provider(
    state: &Arc<CoreState>,
    req: &ActionRequest,
    correlation_id: &str,
    filesystem_provider_pipe: &str,
    browser_provider_pipe: &str,
) -> Result<ActionResult> {
    // Route to the appropriate provider pipe based on provider_id
    let pipe = match req.provider_id.as_str() {
        "browser" => browser_provider_pipe,
        "filesystem" => filesystem_provider_pipe,
        _ => anyhow::bail!("unsupported dispatch provider"),
    };
    let key = crate::dispatch::key_from_env(&req.provider_id).map_err(anyhow::Error::msg)?;
    anyhow::ensure!(
        key != core_token(),
        "provider dispatch credential conflicts with Core transport credential"
    );
    // Burst rule (measured: 24 concurrent clients overflow the single
    // provider accept queue): retry ONLY connection-establishment
    // failures ("open pipe" = the provider never saw the request, so the
    // same action id is safe to re-send). Any error past a live
    // connection fails immediately — re-sending those could
    // double-execute a non-idempotent action.
    let mut backoff_ms: u64 = 50;
    let mut reserved: Option<crate::storage::ActionAuthority> = None;
    loop {
        // Bind each proof to the currently running Provider instance. If it
        // restarts between ping and dispatch, verification fails closed.
        let hello = send_request(pipe, "ping", json!({})).await?;
        anyhow::ensure!(hello["result"]["dispatch_protocol"] == 2, "credential-free dispatch protocol unavailable");
        if req.actor == service_registration::BROWSER_WORLD_ACTOR {
            anyhow::ensure!(hello["result"]["browser_world_protocol"] == 1, "Browser scoped protocol unavailable");
        }
        let instance = hello["result"]["instance_id"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("provider does not support secure dispatch"))?;
        if reserved.is_none() {
            let mut operation=crate::action::DispatchRequest::from(req);
            operation.correlation_id=correlation_id.into();
            let mut binding_id=uuid::Uuid::new_v4().to_string();
            if req.provider_id=="filesystem" && req.action=="scan" {
                let observation_instance=hello["result"]["observation_instance"].as_str().ok_or_else(||anyhow::anyhow!("scan instance unavailable"))?;
                let path=std::path::Path::new(req.target["path"].as_str().ok_or_else(||anyhow::anyhow!("scan path required"))?);
                let binding=state.observation_bindings.dispatch_source(observation_instance,path)?;
                anyhow::ensure!(if Policy::is_core_principal(&req.actor) {state.privileged.can(&req.actor,"action.scan",Some(&binding.world_id))}
                    else {state.policy.may_access_world(&req.actor,&binding.world_id)},"scan World denied");
                binding_id=binding.binding_id.clone(); operation.completion=Some(binding);
            }
            let authority=crate::storage::ActionAuthority {operation,performer:instance.into(),binding_id,state:"reserved".into()};
            state.storage.lock().unwrap().reserve_action(&authority)?;
            reserved=Some(authority);
        }
        let authority=reserved.as_ref().unwrap();
        anyhow::ensure!(authority.performer==instance,"dispatch performer changed; action unresolved");
        let proof = crate::dispatch::issue(&key, instance, &authority.operation).map_err(anyhow::Error::msg)?;
        let mut params = json!(authority.operation);
        params["dispatch_proof"] = json!(proof);
        match send_request(pipe, "action", params).await {
            Ok(resp) => {
                if resp["ok"].as_bool() == Some(true) {
                    let mut result: ActionResult = serde_json::from_value(resp["result"].clone())?;
                    anyhow::ensure!(result.action_id == req.id, "provider action response identity mismatch");
                    if result.status==ActionStatus::Started {
                        anyhow::ensure!(authority.operation.completion.is_some(),"unexpected async action");
                        anyhow::ensure!(result.detail["job"]==format!("scan-{}",authority.binding_id),"Started job binding mismatch");
                        let event=state.storage.lock().unwrap().start_action(&req.id,&result.detail)?;
                        publish_committed_event(state,&event);
                    } else {
                        let completion=crate::action::ActionCompletion {action_id:req.id.clone(),binding_id:authority.binding_id.clone(),performer:instance.into(),
                            outcome_id:uuid::Uuid::new_v4().to_string(),status:if result.status==ActionStatus::Completed {ActionStatus::Completed} else {ActionStatus::Failed},
                            // Do not turn authority bookkeeping into screenshot/file-content storage.
                            // The immediate result still reaches the original caller unchanged.
                            detail:json!({"action":req.action})};
                        if let Some(event)=state.storage.lock().unwrap().finish_action(&completion,false)? { publish_committed_event(state,&event); }
                    }
                    if req.actor == service_registration::BROWSER_WORLD_ACTOR {
                        result.detail["performer_instance"] = json!(instance);
                    }
                    return Ok(result);
                }
                anyhow::bail!(resp["error"]
                    .as_str()
                    .unwrap_or("provider error")
                    .to_string())
            }
            Err(e) => {
                let msg = format!("{e:#}");
                let connect_only = msg.contains("open pipe");
                if !connect_only || backoff_ms > 800 {
                    return Err(e);
                }
                tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                backoff_ms *= 2;
            }
        }
    }
}

async fn complete_provider_action(state:&Arc<CoreState>, params:Value) -> Value {
    let run:Result<Option<Event>> = async {
        anyhow::ensure!(params.as_object().is_some_and(|p|p.keys().all(|k|matches!(k.as_str(),"auth"|"actor"|"principal_token"|"instance_id"|"instance_token"|"completion"))),"unsupported completion envelope");
        anyhow::ensure!(params.get("actor_token").is_none() && principal_authenticated(state,&params,"filesystem"),"completion Provider authentication denied");
        let completion:crate::action::ActionCompletion=serde_json::from_value(params["completion"].clone())?;
        let a=state.storage.lock().unwrap().action_authority(&completion.action_id)?.ok_or_else(||anyhow::anyhow!("unknown action"))?;
        let b=a.operation.completion.as_ref().ok_or_else(||anyhow::anyhow!("not an async action"))?;
        anyhow::ensure!(a.operation.provider_id=="filesystem" && a.operation.action=="scan" && params["instance_id"]==b.instance_id
            && completion.performer==a.performer && completion.binding_id==a.binding_id,"completion authority mismatch");
        let detail=completion.detail.as_object().ok_or_else(||anyhow::anyhow!("invalid scan completion detail"))?;
        anyhow::ensure!(detail.len()==2 && completion.detail["job"]==format!("scan-{}",a.binding_id),"invalid scan completion job");
        if completion.status==ActionStatus::Completed {
            let stats=completion.detail["stats"].as_object().ok_or_else(||anyhow::anyhow!("scan stats required"))?;
            anyhow::ensure!(stats.len()==6 && ["created","updated","renamed","removed","uncertain"].iter().all(|k|stats.get(*k).is_some_and(|v|v.as_u64().is_some()))
                && stats.get("root")==a.operation.target.get("path"),"invalid scan completion stats");
        } else {
            anyhow::ensure!(completion.status==ActionStatus::Failed && completion.detail["error"].as_str().is_some_and(|s|s.len()<=8192),"invalid scan failure detail");
        }
        let token=params["instance_token"].as_str().unwrap_or("");
        state.observation_bindings.read_source(&b.instance_id,token,std::path::Path::new(&b.source_root))?;
        let current=state.observation_bindings.dispatch_source(&b.instance_id,std::path::Path::new(&b.source_root))?;
        anyhow::ensure!(current.source_root==b.source_root && current.world_id==b.world_id && current.source_digest==b.source_digest
            && state.privileged.can("filesystem","provider.register",Some(&b.world_id)),"completion source changed or World denied");
        let hello=tokio::time::timeout(std::time::Duration::from_secs(5),send_request(&state.filesystem_provider_pipe,"ping",json!({}))).await??;
        anyhow::ensure!(hello["result"]["dispatch_protocol"]==2 && hello["result"]["instance_id"]==a.performer
            && hello["result"]["observation_instance"]==b.instance_id,"completion performer unavailable or changed");
        state.storage.lock().unwrap().finish_action(&completion,true)
    }.await;
    match run {
        Ok(event)=>{let replay=event.is_none();if let Some(e)=event {publish_committed_event(state,&e);}
            json!({"ok":true,"result":{"durable":true,"replay":replay}})},
        Err(e)=>json!({"ok":false,"error":e.to_string(),"retryable":e.to_string().contains("Started not confirmed")})
    }
}

/// Send one JSON-lines request to a pipe/TCP endpoint and read one response line.
///
/// Transport rule (Breakage B1): only the connection-establishment step
/// is retried (bounded, with backoff). Under burst load the server may
/// not have recreated its pipe instance yet (Windows ERROR 231), which
/// is transient. The request itself is NEVER retried here: re-sending
/// could double-execute non-idempotent actions (at-least-once Core
/// semantics). Retry policy is therefore safe for every caller.
pub async fn send_request(pipe_path: &str, method: &str, params: Value) -> Result<Value> {
    #[cfg(windows)]
    {
        use tokio::net::windows::named_pipe::ClientOptions;
        let mut backoff_ms: u64 = 5;
        let stream = loop {
            match ClientOptions::new().open(pipe_path) {
                Ok(s) => break s,
                Err(_) if backoff_ms <= 640 => {
                    // Transient under burst (e.g. Windows ERROR 231: all
                    // pipe instances busy). Back off and try again.
                    // Total budget ≈1.2s; genuine outages still error out.
                    tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                    backoff_ms *= 2;
                }
                Err(e) => return Err(e).context("open pipe"),
            }
        };
        let (reader, mut writer) = tokio::io::split(stream);
        let mut params = params;
        params["auth"] = json!(core_token());
        let req = json!({"method": method, "params": params});
        let mut s = serde_json::to_string(&req)?;
        s.push('\n');
        writer.write_all(s.as_bytes()).await?;
        writer.flush().await?;
        let mut lines = BufReader::new(reader).lines();
        if let Some(line) = lines.next_line().await? {
            return Ok(serde_json::from_str(&line)?);
        }
        anyhow::bail!("no response")
    }
    #[cfg(not(windows))]
    {
        let stream = tokio::net::TcpStream::connect("127.0.0.1:7777").await?;
        let (reader, mut writer) = stream.into_split();
        let req = json!({"method": method, "params": params});
        let mut s = serde_json::to_string(&req)?;
        s.push('\n');
        writer.write_all(s.as_bytes()).await?;
        writer.flush().await?;
        let mut lines = BufReader::new(reader).lines();
        if let Some(line) = lines.next_line().await? {
            return Ok(serde_json::from_str(&line)?);
        }
        anyhow::bail!("no response")
    }
}

/// Client-side envelope for the two standalone Provider principals. This
/// does not grant authority: Core authenticates the credential and checks the
/// method capability. The general transport token remains insufficient.
/// Launchers must inject a distinct value into each Provider process.
pub fn provider_principal_credential() -> Result<String> {
    use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
    let credential = std::env::var("SPACEOS_PROVIDER_PRINCIPAL_TOKEN")
        .map_err(|_| anyhow::anyhow!("provider principal credential not configured"))?;
    let decoded = URL_SAFE_NO_PAD
        .decode(&credential)
        .map_err(|_| anyhow::anyhow!("invalid provider principal credential"))?;
    anyhow::ensure!(decoded.len() == 32, "invalid provider principal credential");
    anyhow::ensure!(
        credential != core_token(),
        "provider principal credential conflicts with Core transport credential"
    );
    Ok(credential)
}

/// Separate per-instance observation credential; never use a generic Core
/// transport or Provider-type principal token as an instance proof.
pub fn provider_instance_credential() -> Result<(String, String)> {
    use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
    let instance_id = std::env::var("SPACEOS_PROVIDER_INSTANCE_ID")
        .map_err(|_| anyhow::anyhow!("Provider instance ID not configured"))?;
    anyhow::ensure!(!instance_id.trim().is_empty() && instance_id == instance_id.trim(), "invalid Provider instance ID");
    let token = std::env::var("SPACEOS_PROVIDER_INSTANCE_TOKEN")
        .map_err(|_| anyhow::anyhow!("Provider instance credential not configured"))?;
    let decoded = URL_SAFE_NO_PAD.decode(&token)
        .map_err(|_| anyhow::anyhow!("invalid Provider instance credential"))?;
    anyhow::ensure!(decoded.len() == 32, "invalid Provider instance credential");
    anyhow::ensure!(token != core_token() && token != provider_principal_credential()?,
        "Provider instance credential conflicts with another credential");
    Ok((instance_id, token))
}

pub fn configured_provider_pipe(env_name: &str, fallback: &str) -> Result<String> {
    let explicit = std::env::var(env_name).ok().filter(|s| !s.is_empty());
    if std::env::var_os("SPACEOS_TEST_ISOLATION_ROOT").is_some() {
        let pipe =
            explicit.ok_or_else(|| anyhow::anyhow!("isolated provider endpoint not configured"))?;
        anyhow::ensure!(
            pipe.starts_with(r"\\.\pipe\spaceos-4-3a4-"),
            "unsafe isolated provider endpoint"
        );
        Ok(pipe)
    } else {
        Ok(explicit.unwrap_or_else(|| fallback.to_string()))
    }
}

pub async fn send_provider_principal_request(
    pipe_path: &str,
    principal: &str,
    method: &str,
    mut params: Value,
) -> Result<Value> {
    anyhow::ensure!(
        matches!(principal, "filesystem" | "browser"),
        "unsupported provider principal"
    );
    let credential = provider_principal_credential()?;
    anyhow::ensure!(params.is_object(), "provider request must be an object");
    anyhow::ensure!(
        params.get("principal_token").is_none(),
        "provider request supplies principal credential"
    );
    if method == "register_provider" {
        anyhow::ensure!(
            params.get("principal").is_none(),
            "provider request supplies principal"
        );
        params["principal"] = json!(principal);
    } else {
        anyhow::ensure!(
            params.get("actor").and_then(Value::as_str) == Some(principal)
                || params.get("provider_id").and_then(Value::as_str) == Some(principal),
            "provider request principal mismatch"
        );
    }
    params["principal_token"] = json!(credential);
    let response = send_request(pipe_path, method, params).await?;
    anyhow::ensure!(
        response.get("ok").and_then(Value::as_bool) == Some(true),
        "provider Core request denied: {}",
        response
            .get("error")
            .and_then(Value::as_str)
            .unwrap_or("unknown error")
    );
    Ok(response)
}

#[cfg(test)]
mod provider_observation_security_tests {
    use super::*;
    use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};

    fn fixture(registered: bool) -> (Arc<CoreState>, std::path::PathBuf, std::path::PathBuf, String) {
        let lab = std::env::var("SPACEOS_TEST_ISOLATION_ROOT")
            .expect("observation tests require the isolated laboratory marker");
        assert_eq!(lab, r"C:\spaceos");
        let base = std::path::Path::new(&lab).join("test-tmp").join(format!("observation-{}", uuid::Uuid::new_v4()));
        let a = base.join("root-a");
        let b = base.join("root-b");
        std::fs::create_dir_all(&a).unwrap();
        std::fs::create_dir_all(&b).unwrap();
        let mut principals = PrincipalRegistry::default();
        principals.insert("filesystem", [71; 32], ["observation.report", "provider.register"], ["world-a", "world-b"]).unwrap();
        principals.insert("system", [72; 32], ["observation.report"], ["world-a"]).unwrap();
        let storage = Storage::open(":memory:").unwrap();
        if registered {
            storage.upsert_provider(&ProviderInfo {
                id: "filesystem".into(), name: "fixture filesystem".into(),
                capabilities: vec!["observe".into()], version: None,
            }).unwrap();
        }
        let mut state = CoreState::new_with_semantic_world_and_identities(
            storage, None,
            r"\\.\pipe\spaceos-4-3a4-unused-fs", r"\\.\pipe\spaceos-4-3a4-unused-browser",
            principals, ServiceRegistrarRegistry::default(), "synthetic-transport".into(),
        );
        Arc::get_mut(&mut state).unwrap().observation_bindings = ObservationBindings::from_json(
            &json!([
                {"provider":"filesystem", "instance_id":"instance-a", "instance_token":URL_SAFE_NO_PAD.encode([81;32]), "root":a, "world_id":"world-a"},
                {"provider":"filesystem", "instance_id":"instance-b", "instance_token":URL_SAFE_NO_PAD.encode([82;32]), "root":b, "world_id":"world-b"}
            ]).to_string(),
        ).unwrap();
        state.observation_bindings.register_instance("instance-a", &URL_SAFE_NO_PAD.encode([81;32]));
        state.observation_bindings.register_instance("instance-b", &URL_SAFE_NO_PAD.encode([82;32]));
        (state, a, b, URL_SAFE_NO_PAD.encode([71; 32]))
    }

    fn request(path: &std::path::Path, credential: &str) -> Value {
        let mut object = Object::new("File", "filesystem", json!({"path":path.to_string_lossy()}));
        object.external_id = Some(path.to_string_lossy().into_owned());
        let mut event = Event::new("FileCreated", "filesystem", json!({
            "path":path.to_string_lossy(), "object_id":object.id
        }));
        event.provider_id = Some("filesystem".into());
        event.object_ids = vec![object.id.clone()];
        let obs = Observation {
            observation_id: uuid::Uuid::new_v4().to_string(), world_id: String::new(),
            facts: vec![], objects: vec![object], relations: vec![], delete_ids: vec![], event,
        };
        let mut value = serde_json::to_value(obs).unwrap();
        value["actor"] = json!("filesystem");
        value["principal_token"] = json!(credential);
        value["instance_id"] = json!("instance-a");
        value["instance_token"] = json!(URL_SAFE_NO_PAD.encode([81;32]));
        value
    }

    fn replace_path(value: &mut Value, path: &std::path::Path) {
        let path = path.to_string_lossy().into_owned();
        value["objects"][0]["external_id"] = json!(path);
        value["objects"][0]["properties"]["path"] = json!(path);
        value["event"]["payload"]["path"] = json!(path);
    }

    async fn report(state: &Arc<CoreState>, value: Value) -> Value {
        handle_method(state, "report_observation", value, "fixture-correlation").await
    }

    #[tokio::test]
    async fn trusted_filecreated_multi_world_and_adversarial_admission() {
        let (mut state, a, b, credential) = fixture(true);
        assert!(state.observation_bindings.validate_distinct_principals(
            &state.privileged, &URL_SAFE_NO_PAD.encode([81; 32])
        ).is_err(), "Core transport token must not be an instance credential");
        let file_a = a.join("one.txt");
        let file_b = b.join("two.txt");
        let collision = a.parent().unwrap().join("root-a-collision");
        std::fs::create_dir_all(&collision).unwrap();
        let collision_file = collision.join("outside.txt");
        std::fs::write(&file_a, "a").unwrap();
        std::fs::write(&file_b, "b").unwrap();
        std::fs::write(&collision_file, "outside").unwrap();
        let valid_a = request(&file_a, &credential);
        let mut denied = vec![];
        let mut wrong_world = valid_a.clone(); wrong_world["world_id"] = json!("world-b"); denied.push(wrong_world);
        let mut wrong_event_world = valid_a.clone(); wrong_event_world["event"]["world_id"] = json!("world-b"); denied.push(wrong_event_world);
        let mut wrong_credential = valid_a.clone(); wrong_credential["principal_token"] = json!(URL_SAFE_NO_PAD.encode([99; 32])); denied.push(wrong_credential);
        let mut forged_provider = valid_a.clone(); forged_provider["objects"][0]["provider_id"] = json!("browser"); denied.push(forged_provider);
        let mut forged_actor = valid_a.clone(); forged_actor["actor"] = json!("system"); forged_actor["principal_token"] = json!(URL_SAFE_NO_PAD.encode([72; 32])); denied.push(forged_actor);
        let mut unsupported = valid_a.clone(); unsupported["event"]["event_type"] = json!("FileDeleted"); denied.push(unsupported);
        let mut malformed = valid_a.clone(); malformed["objects"] = json!("invalid"); denied.push(malformed);
        let mut outside = valid_a.clone(); replace_path(&mut outside, &collision_file); denied.push(outside);
        let mut traversal = valid_a.clone(); replace_path(&mut traversal, &a.join("..\\root-a\\one.txt")); denied.push(traversal);
        let mut forged_world_property = valid_a.clone(); forged_world_property["objects"][0]["properties"]["world_id"] = json!("world-b"); denied.push(forged_world_property);
        let mut invalid_transport = valid_a.clone(); invalid_transport["principal_token"] = json!("synthetic-transport"); denied.push(invalid_transport);
        let mut impersonate_b = valid_a.clone(); impersonate_b["instance_id"] = json!("instance-b"); denied.push(impersonate_b);
        let mut unknown_instance = valid_a.clone(); unknown_instance["instance_id"] = json!("unknown"); denied.push(unknown_instance);
        let mut invalid_instance_token = valid_a.clone(); invalid_instance_token["instance_token"] = json!(URL_SAFE_NO_PAD.encode([99;32])); denied.push(invalid_instance_token);
        for request in denied { assert_eq!(report(&state, request).await["ok"], false); }
        assert!(state.storage.lock().unwrap().get_object(valid_a["objects"][0]["id"].as_str().unwrap()).unwrap().is_none());
        assert!(state.storage.lock().unwrap().query_events(&crate::query::EventQuery::default()).unwrap().is_empty());

        let first = report(&state, valid_a.clone()).await;
        assert_eq!(first["ok"], true, "{first}");
        assert_eq!(first["result"]["applied"], true);
        let repeated = report(&state, valid_a.clone()).await;
        assert_eq!(repeated["result"]["applied"], false);
        let mut new_delivery = valid_a.clone(); new_delivery["observation_id"] = json!(uuid::Uuid::new_v4().to_string());
        assert_eq!(report(&state, new_delivery).await["ok"], false);
        let mut valid_b = request(&file_b, &credential);
        valid_b["instance_id"] = json!("instance-b");
        valid_b["instance_token"] = json!(URL_SAFE_NO_PAD.encode([82;32]));
        let result_b = report(&state, valid_b).await;
        assert_eq!(result_b["ok"], true, "{result_b}");
        let third_path = a.join("third.txt");
        std::fs::write(&third_path, "third").unwrap();
        let mut collision_event = request(&third_path, &credential);
        collision_event["event"]["id"] = valid_a["event"]["id"].clone();
        let third_id = collision_event["objects"][0]["id"].as_str().unwrap().to_owned();
        assert_eq!(report(&state, collision_event).await["ok"], false);
        assert!(state.storage.lock().unwrap().get_object(&third_id).unwrap().is_none());
        let stored = state.storage.lock().unwrap();
        let object = stored.get_object(valid_a["objects"][0]["id"].as_str().unwrap()).unwrap().unwrap();
        assert_eq!(object.properties["world_id"], "world-a");
        assert_eq!(object.provider_id, "filesystem");
        let events = stored.query_events(&crate::query::EventQuery::default()).unwrap();
        assert_eq!(events.len(), 2);
        assert!(events.iter().any(|e| e.world_id == "world-a"));
        assert!(events.iter().any(|e| e.world_id == "world-b"));
        drop(stored);

        Arc::get_mut(&mut state).unwrap().observation_bindings = ObservationBindings::default();
        assert_eq!(report(&state, request(&file_a, &credential)).await["ok"], false);
        assert!(ObservationBindings::from_json(
            &json!([
                {"provider":"filesystem", "instance_id":"instance-a", "instance_token":URL_SAFE_NO_PAD.encode([81;32]), "root":a, "world_id":"world-a"},
                {"provider":"filesystem", "instance_id":"instance-b", "instance_token":URL_SAFE_NO_PAD.encode([82;32]), "root":a, "world_id":"world-b"}
            ]).to_string(),
        ).is_err());
        assert_eq!(report(&state, request(&file_a, &credential)).await["ok"], false);
        let base = a.parent().unwrap();
        std::fs::remove_dir_all(base).unwrap();
    }

    #[tokio::test]
    async fn unregistered_provider_cannot_project_file() {
        let (state, a, _, credential) = fixture(false);
        let path = a.join("unregistered.txt");
        std::fs::write(&path, "fixture").unwrap();
        let request = request(&path, &credential);
        assert_eq!(report(&state, request.clone()).await["ok"], false);
        assert!(state.storage.lock().unwrap().get_object(request["objects"][0]["id"].as_str().unwrap()).unwrap().is_none());
        assert!(state.storage.lock().unwrap().query_events(&crate::query::EventQuery::default()).unwrap().is_empty());
        std::fs::remove_dir_all(a.parent().unwrap()).unwrap();
    }
}
