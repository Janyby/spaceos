use crate::query::{EventQuery, ObjectQuery, RelationQuery};
use crate::{
    ActionRequest, ActionResult, ActionStatus, Event, Object, Observation, ProviderInfo, Relation,
    Space, Storage,
};
use anyhow::{Context, Result};
use serde_json::{json, Value};
use std::sync::Arc;
use std::sync::Mutex;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

const TOKEN_FILE: &str = r"C:\spaceos\runtime\space-core.token";

pub fn core_token() -> String {
    std::env::var("SPACEOS_CORE_TOKEN")
        .ok()
        .filter(|t| !t.is_empty())
        .unwrap_or_else(|| match std::fs::read_to_string(TOKEN_FILE) {
            Ok(t) if !t.trim().is_empty() => t.trim().to_string(),
            _ => {
                let t = uuid::Uuid::new_v4().to_string();
                let _ = std::fs::write(TOKEN_FILE, &t);
                t
            }
        })
}

fn auth_ok(params: &Value, token: &str) -> bool {
    params.get("auth").and_then(|v| v.as_str()) == Some(token)
}

// SpaceTrace mutation wrapper (Slice 1). Emits start/commit/rollback
// around a Storage mutation. No semantic change: returns inner result.
// Hooks are no-ops when SpaceTrace is not initialized (I1, I17).
// Security (§24, deep): auth secrets are stripped from trace payloads at
// ANY nesting depth — metadata only, never tokens/credentials.
fn sanitize_trace_value(v: &mut Value) {
    const SECRETS: [&str; 8] = [
        "auth",
        "actor_token",
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
    pub core_token: String,
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
    pub fn new(storage: Storage) -> Arc<Self> {
        let (tx, _) = tokio::sync::broadcast::channel(512);
        let spacetrace = SpaceTrace::new().ok();
        if let Some(ref st) = spacetrace {
            spacetrace::init_global_hooks(st.buffer(), st.correlation());
        }
        let storage = Arc::new(Mutex::new(storage));
        let sender = CoreSender::new(tx.clone(), storage.clone());
        Arc::new(Self {
            storage,
            events_tx: tx,
            policy: Policy::new(),
            core_token: core_token(),
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
        Self {
            storage,
            events_tx: tx,
            policy: Policy::new(),
            core_token: core_token(),
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
                    let server = match ServerOptions::new().create(&path) {
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
    let (notify_tx, mut notify_rx) = tokio::sync::mpsc::channel::<Value>(256);

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
                            let resp = handle_subscribe(&state, params);
                            if let Some((sub_id, filter)) = resp.1 {
                                let mut rx = state.events_tx.subscribe();
                                let notify_tx = notify_tx.clone();
                                let state2 = state.clone();
                                tokio::spawn(async move {
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
                                        if notify_tx.send(json!({"event": ev})).await.is_err() {
                                            break;
                                        }
                                    }
                                });
                            }
                            write_line(writer, &resp.0).await?;
                            continue;
                        }
                        if method == "unsubscribe" {
                            let resp = handle_unsubscribe(&state, params);
                            write_line(writer, &resp).await?;
                            continue;
                        }
                        let resp = handle_method(&state, &method, params).await;
                        write_line(writer, &resp).await?;
                    }
                    None => return Ok(()),
                }
            }
            Some(note) = notify_rx.recv() => {
                write_line(writer, &note).await?;
            }
        }
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

    // 1. Authenticate: subscriber must prove who it is.
    let authed = if Policy::is_core_principal(&subscriber) {
        auth_ok(&params, &state.core_token)
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
        auth_ok(&params, &state.core_token)
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

async fn handle_method(state: &Arc<CoreState>, method: &str, params: Value) -> Value {
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
    // Policy gate: all World writes go through a single check.
    // Caller identity is bound: core principals use the core token,
    // agents must present their own agent token.
    if let Some((actor, cap, actor_token)) = mutation_gate(method, &params) {
        let authed = if Policy::is_core_principal(&actor) {
            auth_ok(&params, &state.core_token)
        } else {
            state.policy.authenticate(&actor, actor_token.as_deref())
        };
        let allowed = authed && state.policy.can(&actor, &cap);
        if !allowed {
            {
                let storage = state.storage.lock().unwrap();
                let mut e = Event::new(
                    "MutationDenied",
                    actor.clone(),
                    serde_json::json!({"method": method, "capability": cap, "auth": authed}),
                );
                e.provider_id = Some(actor.clone());
                let _ = storage.record_event(&e);
            }
            return json!({"ok": false, "error": format!("denied: {actor} lacks {cap} or missing auth")});
        }
    }
    let mut storage = state.storage.lock().unwrap();
    match method {
        "ping" => ok!(json!({"pong": true})),
        "register_provider" => {
            if !auth_ok(&params, &state.core_token) {
                json!({"ok": false, "error": "denied: register_provider requires valid auth"})
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
        "get_object" => {
            let id = params["id"].as_str().unwrap_or("");
            match storage.get_object(id) {
                Ok(v) => ok!(json!(v)),
                Err(e) => err!(e),
            }
        }
        "get_object_by_external" => {
            let p = params["provider_id"].as_str().unwrap_or("");
            let e = params["external_id"].as_str().unwrap_or("");
            match storage.get_object_by_external(p, e) {
                Ok(v) => ok!(json!(v)),
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
                Ok(v) => ok!(json!(v)),
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
            match storage.get_space(id) {
                Ok(v) => ok!(json!(v)),
                Err(e) => err!(e),
            }
        }
        "list_spaces" => match storage.list_spaces() {
            Ok(v) => ok!(json!(v)),
            Err(e) => err!(e),
        },
        "register_agent" => {
            let principal = params["principal"].as_str().unwrap_or("");
            let name = params["name"].as_str().unwrap_or("");
            let auth = auth_ok(&params, &state.core_token);
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
            if principal == "system" && auth {
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
                json!({"ok": false, "error": "denied: register_agent requires principal=system and valid auth"})
            }
        }
        "policy_caps" => {
            let actor = params["actor"].as_str().unwrap_or("");
            ok!(json!({"capabilities": state.policy.capabilities_of(actor)}))
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
                (Ok(spaces), Ok(objects), Ok(relations), Ok(events)) => ok!(json!({
                    "spaces": spaces,
                    "objects": objects,
                    "relations": relations,
                    "events": events,
                })),
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
                Ok(v) => ok!(json!(v)),
                Err(e) => err!(e),
            }
        }
        "list_relations" => {
            match storage.query_relations(&crate::query::RelationQuery::default()) {
                Ok(v) => ok!(json!(v)),
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
                Ok(v) => ok!(json!(v)),
                Err(e) => err!(e),
            },
            Err(e) => err!(e),
        },
        "record_event" => match serde_json::from_value::<Event>(params.clone()) {
            Ok(e) => {
                let cid = e.correlation_id.clone().unwrap_or_else(|| e.id.clone());
                let world_id = e.world_id.clone();
                let actor = e.actor.clone();
                let r = trace_mutation(
                    "record_event",
                    &actor,
                    Some(world_id.as_str()),
                    &cid,
                    &params,
                    || storage.record_event(&e),
                );
                if r.is_ok() {
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
                match r {
                    Ok(()) => ok!(json!(e)),
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
                // Actor resolution mirrors create_relation: explicit param
                // wins, provider/event fall back (Level D finding).
                let actor = params
                    .get("actor")
                    .and_then(|v| v.as_str())
                    .or_else(|| params.get("actor_id").and_then(|v| v.as_str()))
                    .or_else(|| params.get("provider_id").and_then(|v| v.as_str()))
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| obs.event.actor.clone());
                let cid = obs
                    .event
                    .correlation_id
                    .clone()
                    .unwrap_or_else(|| obs.observation_id.clone());
                let world_id = if obs.world_id.is_empty() {
                    obs.event.world_id.clone()
                } else {
                    obs.world_id.clone()
                };
                // Reuses the outer storage lock (same thread: no re-lock).
                let r = trace_mutation(
                    "report_observation",
                    &actor,
                    Some(world_id.as_str()),
                    &cid,
                    &params,
                    || storage.apply_observation(&obs),
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
            Ok(q) => match storage.query_events(&q) {
                Ok(v) => ok!(json!(v)),
                Err(e) => err!(e),
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
                auth_ok(&params, &state.core_token)
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
                auth_ok(&params, &state.core_token)
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
        });
    let actor_token = params
        .get("actor_token")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let cap = match method {
        "create_object" => Some("object.create"),
        "update_object" => Some("object.update"),
        "delete_object" => Some("object.delete"),
        "create_relation" => Some("relation.create"),
        "delete_relation" => Some("relation.delete"),
        "record_event" => Some("event.record"),
        "report_observation" => Some("observation.report"),
        "create_space" => Some("space.create"),
        _ => None,
    };
    match (actor, cap) {
        (Some(a), Some(c)) => Some((a, c.to_string(), actor_token)),
        (None, Some(c)) => Some(("anonymous".to_string(), c.to_string(), actor_token)),
        _ => None,
    }
}

async fn handle_action(state: &Arc<CoreState>, params: Value) -> Value {
    let envelope_authed = auth_ok(&params, &state.core_token);
    let req: ActionRequest = match serde_json::from_value(params) {
        Ok(r) => r,
        Err(e) => return json!({"ok": false, "error": format!("{e}")}),
    };
    let ok = |v: Value| json!({"ok": true, "result": v});
    // Bind the caller: core principals use the envelope core token,
    // agents must present their own agent token plus action capability.
    let authed = if Policy::is_core_principal(&req.actor) {
        envelope_authed
    } else {
        state.policy.authenticate(
            &req.actor,
            if req.actor_token.is_empty() {
                None
            } else {
                Some(req.actor_token.as_str())
            },
        )
    };
    let need_cap = format!("action.{}", req.action);
    if !authed || !state.policy.can(&req.actor, &need_cap) {
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
    let outcome = forward_action_to_provider(&req).await;
    match outcome {
        Ok(result) => {
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
            let mut failed = Event::new(
                "ActionFailed",
                req.actor.clone(),
                json!({"error": e.to_string()}),
            );
            failed.provider_id = Some(req.provider_id.clone());
            failed.correlation_id = Some(req.id.clone());
            {
                let storage = state.storage.lock().unwrap();
                let _ = storage.record_event(&failed);
            }
            spacetrace::hooks::global_hooks().core_event_published(
                &failed.id,
                failed.correlation_id.clone(),
                &failed.event_type,
                &failed.actor,
                &failed.world_id,
                failed.provider_id.clone(),
            );
            let _ = state.events_tx.send(failed);
            ok(json!(ActionResult {
                action_id: req.id,
                status: ActionStatus::Failed,
                detail: json!({"error": e.to_string()}),
            }))
        }
    }
}

async fn forward_action_to_provider(req: &ActionRequest) -> Result<ActionResult> {
    // Route to the appropriate provider pipe based on provider_id
    let pipe = match req.provider_id.as_str() {
        "browser" => BROWSER_PROVIDER_PIPE,
        _ => PROVIDER_PIPE, // default to filesystem provider
    };
    // Burst rule (measured: 24 concurrent clients overflow the single
    // provider accept queue): retry ONLY connection-establishment
    // failures ("open pipe" = the provider never saw the request, so the
    // same action id is safe to re-send). Any error past a live
    // connection fails immediately — re-sending those could
    // double-execute a non-idempotent action.
    let mut backoff_ms: u64 = 50;
    loop {
        match send_request(pipe, "action", json!(req)).await {
            Ok(resp) => {
                if resp["ok"].as_bool() == Some(true) {
                    let result: ActionResult = serde_json::from_value(resp["result"].clone())?;
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
