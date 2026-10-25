//! Browser + filesystem vertical slice through Core.
//!
//! Every user action travels: test -> Core `request_action` -> policy gate
//! -> provider pipe -> terminal Action* event (correlation_id = action id).
//! Stub providers stand in for the real daemons on the machine-global pipe
//! names; the global IPC lock serializes against every other suite that
//! touches those pipes (see support/global_lock.rs).
#![cfg(windows)]

use serde_json::{json, Value};
use space_core::{ipc, ActionRequest, Storage};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[path = "support/global_lock.rs"]
mod global_lock;

use global_lock::{acquire_global_ipc, ServerGuard, TaskRegistry};

type Calls = Arc<Mutex<Vec<Value>>>;

fn tmp(name: &str) -> PathBuf {
    let d = std::env::temp_dir().join("spaceos-bridge-slice").join(name);
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

async fn start_core(db: &PathBuf) -> Arc<ipc::CoreState> {
    let storage = Storage::open(db).unwrap();
    let state = ipc::CoreState::new(storage);
    let out = state.clone();
    tokio::spawn(async move {
        let _ = ipc::run_server(state, ipc::CORE_PIPE).await;
    });
    tokio::time::sleep(Duration::from_millis(400)).await;
    out
}

/// Serve one provider pipe with canned behavior.
/// Browser stub: "boom" fails, everything else completes. Records calls.
/// Filesystem stub: "scan" answers Started and completes async via Core.
/// `agent_token` is filled by the test after registration; the async
/// completion threads it back so the Core gate accepts the event, exactly
/// like production (`report_event` in filesystem-provider).
async fn run_stub(
    pipe: &'static str,
    calls: Calls,
    fs_mode: bool,
    agent_token: Arc<Mutex<Option<String>>>,
) {
    use tokio::io::{AsyncBufReadExt, BufReader};
    use tokio::net::windows::named_pipe::ServerOptions;
    loop {
        let server = match ServerOptions::new().create(pipe) {
            Ok(s) => s,
            Err(_) => {
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
        };
        if server.connect().await.is_err() {
            continue;
        }
        let (reader, mut writer) = tokio::io::split(server);
        let mut lines = BufReader::new(reader).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            let req: Value = match serde_json::from_str(&line) {
                Ok(v) => v,
                Err(_) => break,
            };
            let method = req["method"].as_str().unwrap_or("").to_string();
            let params = req.get("params").cloned().unwrap_or(json!({}));
            let resp = match method.as_str() {
                "ping" => json!({"ok": true, "result": {"pong": true}}),
                "action" => {
                    let areq: ActionRequest = match serde_json::from_value(params) {
                        Ok(r) => r,
                        Err(e) => {
                            let mut s = serde_json::to_string(
                                &json!({"ok": false, "error": e.to_string()}),
                            )
                            .unwrap();
                            s.push('\n');
                            use tokio::io::AsyncWriteExt as _;
                            let _ = writer.write_all(s.as_bytes()).await;
                            let _ = writer.flush().await;
                            break;
                        }
                    };
                    calls.lock().unwrap().push(json!({
                        "action": areq.action,
                        "actor": areq.actor,
                        "provider_id": areq.provider_id,
                        "id": areq.id,
                    }));
                    if fs_mode && areq.action == "scan" {
                        // Async job: Started now, terminal event shortly after.
                        let job = format!("job-{}", uuid::Uuid::new_v4());
                        let action_id = areq.id.clone();
                        let job2 = job.clone();
                        let tok_src = agent_token.clone();
                        tokio::spawn(async move {
                            tokio::time::sleep(Duration::from_millis(300)).await;
                            // Mirror production: the terminal event is recorded
                            // under the requesting actor, so it must carry a
                            // token the Core gate accepts (see report_event).
                            let tok = tok_src.lock().unwrap().clone().unwrap_or_default();
                            let ev = json!({
                                "id": uuid::Uuid::new_v4().to_string(),
                                "event_type": "ActionCompleted",
                                "actor": "slice-agent",
                                "actor_token": tok,
                                "provider_id": "filesystem",
                                "object_ids": [],
                                "payload": {"job": job2, "stats": {"created": 1}},
                                "timestamp": chrono::Utc::now().to_rfc3339(),
                                "correlation_id": action_id,
                                "world_id": "",
                            });
                            let _ = ipc::send_request(ipc::CORE_PIPE, "record_event", ev).await;
                        });
                        json!({"ok": true, "result": {
                            "action_id": areq.id,
                            "status": "Started",
                            "detail": {"job": job},
                        }})
                    } else if areq.action == "boom" {
                        json!({"ok": true, "result": {
                            "action_id": areq.id,
                            "status": "Failed",
                            "detail": {"error": "stub boom"},
                        }})
                    } else {
                        json!({"ok": true, "result": {
                            "action_id": areq.id,
                            "status": "Completed",
                            "detail": {"echo": areq.action},
                        }})
                    }
                }
                other => json!({"ok": false, "error": format!("unknown method {other}")}),
            };
            let mut s = serde_json::to_string(&resp).unwrap();
            s.push('\n');
            use tokio::io::AsyncWriteExt as _;
            if writer.write_all(s.as_bytes()).await.is_err() {
                break;
            }
            if writer.flush().await.is_err() {
                break;
            }
        }
    }
}

async fn register_provider(id: &str, caps: &[&str]) {
    let r = ipc::send_request(
        ipc::CORE_PIPE,
        "register_provider",
        json!({"id": id, "name": id, "capabilities": caps}),
    )
    .await
    .unwrap();
    assert_eq!(r["ok"], true, "register {id}: {r}");
}

async fn register_agent(name: &str, caps: &[&str]) -> String {
    let r = ipc::send_request(
        ipc::CORE_PIPE,
        "register_agent",
        json!({"principal": "system", "name": name, "capabilities": caps, "worlds": []}),
    )
    .await
    .unwrap();
    assert_eq!(r["ok"], true, "register_agent {name}: {r}");
    r["result"]["token"].as_str().unwrap().to_string()
}

async fn do_action(action: &str, target: Value, provider: &str, actor: &str, token: &str) -> Value {
    let mut req = ActionRequest::new(action, target, provider, actor);
    req.actor_token = token.to_string();
    ipc::send_request(ipc::CORE_PIPE, "request_action", json!(req))
        .await
        .unwrap()
}

async fn events_for(action_id: &str) -> Vec<Value> {
    let r = ipc::send_request(ipc::CORE_PIPE, "query_events", json!({"limit": 500}))
        .await
        .unwrap();
    r["result"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter(|e| e.get("correlation_id").and_then(|v| v.as_str()) == Some(action_id))
        .collect()
}

fn event_types(evs: &[Value]) -> Vec<String> {
    evs.iter()
        .filter_map(|e| {
            e.get("event_type")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
        })
        .collect()
}

struct Stack {
    _lock: global_lock::GlobalIpcLock,
    _guard: ServerGuard,
    tokens: Arc<Mutex<Option<String>>>,
    core: Arc<ipc::CoreState>,
}

async fn stack(tag: &str) -> Stack {
    let _lock = acquire_global_ipc();
    let dir = tmp(tag);
    let core = start_core(&dir.join("core.db")).await;
    let reg: Arc<TaskRegistry> = Arc::new(TaskRegistry::default());
    let _guard = ServerGuard::new(reg.clone());
    let calls_b: Calls = Arc::new(Mutex::new(vec![]));
    let calls_f: Calls = Arc::new(Mutex::new(vec![]));
    let tokens: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let cb = calls_b.clone();
    let tb = tokens.clone();
    reg.clone().spawn(async move {
        run_stub(ipc::BROWSER_PROVIDER_PIPE, cb, false, tb).await;
    });
    let cf = calls_f.clone();
    let tf = tokens.clone();
    reg.clone().spawn(async move {
        run_stub(ipc::PROVIDER_PIPE, cf, true, tf).await;
    });
    // Give stub accept loops a moment to bind.
    tokio::time::sleep(Duration::from_millis(300)).await;
    Stack {
        _lock,
        _guard,
        tokens,
        core,
    }
}

/// SpaceTrace linkage: Core hooks index mutation + publication spans under
/// the action id, so the request can be traced end to end.
fn assert_trace_linked(core: &Arc<ipc::CoreState>, action_id: &str) {
    let st = core
        .spacetrace
        .as_ref()
        .expect("SpaceTrace must be initialized");
    let linked = st.provider().query_by_correlation(action_id);
    assert!(
        linked.len() >= 2,
        "expected mutation+publication trace spans for {action_id}, got {}",
        linked.len()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn browser_action_success_end_to_end() {
    let st = stack("browser-ok").await;
    register_provider("browser", &["observe", "navigate"]).await;
    let token = register_agent("slice-agent", &["action.navigate"]).await;

    let resp = do_action(
        "navigate",
        json!({"url": "https://example.com"}),
        "browser",
        "slice-agent",
        &token,
    )
    .await;
    assert_eq!(resp["ok"], true, "envelope: {resp}");
    assert_eq!(resp["result"]["status"], "Completed", "result: {resp}");
    let action_id = resp["result"]["action_id"].as_str().unwrap().to_string();

    // Requested -> Completed, same correlation id, SpaceTrace-linked.
    assert_trace_linked(&st.core, &action_id);
    let evs = events_for(&action_id).await;
    let types = event_types(&evs);
    assert!(
        types.contains(&"ActionRequested".to_string()),
        "missing ActionRequested: {types:?}"
    );
    assert!(
        types.contains(&"ActionCompleted".to_string()),
        "missing ActionCompleted: {types:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn browser_action_denied_never_executes() {
    let _st = stack("browser-deny").await;
    register_provider("browser", &["observe", "navigate"]).await;
    let before = {
        // call count via a second navigate as agent would pollute; instead
        // count ActionRequested events for this actor (must be zero).
        let r = ipc::send_request(ipc::CORE_PIPE, "query_events", json!({"limit": 500}))
            .await
            .unwrap();
        r["result"].as_array().cloned().unwrap_or_default().len()
    };

    let resp = do_action(
        "navigate",
        json!({"url": "https://example.com"}),
        "browser",
        "nobody",
        "",
    )
    .await;
    assert_eq!(resp["ok"], true, "envelope: {resp}");
    assert_eq!(resp["result"]["status"], "Denied", "result: {resp}");
    let action_id = resp["result"]["action_id"].as_str().unwrap().to_string();

    // Denial is recorded, and no provider-side execution happened: the only
    // new events for this correlation are the denial itself.
    let evs = events_for(&action_id).await;
    let types = event_types(&evs);
    assert_eq!(types, vec!["ActionDenied".to_string()], "types: {types:?}");
    let after = {
        let r = ipc::send_request(ipc::CORE_PIPE, "query_events", json!({"limit": 500}))
            .await
            .unwrap();
        r["result"].as_array().cloned().unwrap_or_default().len()
    };
    assert!(
        after - before <= 2,
        "denied action must not fan out provider events"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn browser_action_provider_failure_is_recorded() {
    let _st = stack("browser-fail").await;
    register_provider("browser", &["observe", "navigate"]).await;
    let token = register_agent("slice-agent", &["action.boom"]).await;

    let resp = do_action("boom", json!({}), "browser", "slice-agent", &token).await;
    assert_eq!(resp["result"]["status"], "Failed", "result: {resp}");
    let action_id = resp["result"]["action_id"].as_str().unwrap().to_string();

    let evs = events_for(&action_id).await;
    let types = event_types(&evs);
    assert!(types.contains(&"ActionRequested".to_string()), "{types:?}");
    assert!(types.contains(&"ActionFailed".to_string()), "{types:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn filesystem_scan_async_lifecycle() {
    let st = stack("fs-scan").await;
    register_provider("filesystem", &["scan", "actions"]).await;
    let token = register_agent("slice-agent", &["action.scan", "event.record"]).await;
    *st.tokens.lock().unwrap() = Some(token.clone());

    let resp = do_action(
        "scan",
        json!({"path": "C:\\tmp"}),
        "filesystem",
        "slice-agent",
        &token,
    )
    .await;
    assert_eq!(resp["result"]["status"], "Started", "result: {resp}");
    let action_id = resp["result"]["action_id"].as_str().unwrap().to_string();
    assert!(
        resp["result"]["detail"]["job"].is_string(),
        "job id present: {resp}"
    );

    // Terminal completion arrives async under the same correlation id.
    let mut done = false;
    for _ in 0..40 {
        let evs = events_for(&action_id).await;
        if event_types(&evs).contains(&"ActionCompleted".to_string()) {
            done = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert!(done, "async ActionCompleted never arrived for {action_id}");
    assert_trace_linked(&st.core, &action_id);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn browser_registration_has_single_source_of_truth() {
    let _st = stack("single-reg").await;
    // Legacy daemon-style registration, then dedicated-provider style.
    register_provider("browser", &["observe"]).await;
    register_provider("browser", &["observe", "navigate"]).await;

    let r = ipc::send_request(ipc::CORE_PIPE, "list_providers", json!({}))
        .await
        .unwrap();
    let rows: Vec<Value> = serde_json::from_value(r["result"].clone()).unwrap();
    let browsers: Vec<&Value> = rows.iter().filter(|p| p["id"] == "browser").collect();
    assert_eq!(browsers.len(), 1, "exactly one browser row: {rows:?}");
    let caps = browsers[0]["capabilities"].clone();
    assert!(caps.as_array().unwrap().iter().any(|c| c == "navigate"));
}

/// Core restart rotates agent tokens (Policy is memory-only): an old
/// token is Denied, re-registration restores access. This is the exact
/// mechanism the bridge Denied-retry relies on.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn agent_token_refresh_after_core_restart() {
    let _lock = acquire_global_ipc();
    let dir = tmp("restart");
    let db = dir.join("core.db");
    let reg: Arc<TaskRegistry> = Arc::new(TaskRegistry::default());
    let _guard = ServerGuard::new(reg.clone());

    // Core instance 1.
    let core1 = {
        let storage = Storage::open(&db).unwrap();
        ipc::CoreState::new(storage)
    };
    let c1 = core1.clone();
    reg.clone().spawn(async move {
        let _ = ipc::run_server(c1, ipc::CORE_PIPE).await;
    });
    tokio::time::sleep(Duration::from_millis(400)).await;

    let token1 = register_agent("restart-agent", &["action.navigate"]).await;

    // Restart: drop instance 1's server, boot instance 2 on the same DB.
    // (The registry abort kills every tracked server task at once.)
    reg.abort_all();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let core2 = {
        let storage = Storage::open(&db).unwrap();
        ipc::CoreState::new(storage)
    };
    reg.clone().spawn(async move {
        let _ = ipc::run_server(core2, ipc::CORE_PIPE).await;
    });
    tokio::time::sleep(Duration::from_millis(400)).await;

    // Old token is dead (agent table is memory-only).
    let stale = do_action("navigate", json!({}), "browser", "restart-agent", &token1).await;
    assert_eq!(stale["result"]["status"], "Denied", "stale token: {stale}");

    // Re-register (what the bridge retry does) and the same call is gated
    // on policy only. No provider is stubbed here, so Core reports the
    // provider as unreachable — but the denial reason must be gone, i.e.
    // policy passed and Core attempted execution (Failed, not Denied).
    let token2 = register_agent("restart-agent", &["action.navigate"]).await;
    assert_ne!(token1, token2, "rotation issues a fresh token");
    let retry = do_action("navigate", json!({}), "browser", "restart-agent", &token2).await;
    assert_ne!(
        retry["result"]["status"], "Denied",
        "fresh token passes policy: {retry}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bridge_websocket_init_message() {
    use futures_util::StreamExt;
    // Own port, no pipes involved: no global lock needed.
    let state = space_bridge::BridgeState::new();
    let router = space_bridge::handlers::create_router(std::sync::Arc::new(state));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:18766")
        .await
        .unwrap();
    let srv = tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });
    tokio::time::sleep(Duration::from_millis(300)).await;

    let (mut ws, _) = tokio_tungstenite::connect_async("ws://127.0.0.1:18766/ws")
        .await
        .expect("ws connect");
    let msg = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .expect("ws init timeout")
        .expect("ws closed")
        .expect("ws error");
    srv.abort();
    let text = match msg {
        tokio_tungstenite::tungstenite::Message::Text(t) => t.to_string(),
        other => panic!("expected text, got {other:?}"),
    };
    let v: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(v["type"], "init");
    assert!(
        v.get("components").is_some(),
        "init carries components: {v}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bridge_ipc_timeout_is_surfaced() {
    // Black-hole pipe on a unique name: accepts and never replies.
    // Proves the bridge 10s IPC timeout surfaces instead of hanging.
    let pipe = format!(r"\\.\pipe\spaceos-test-blackhole-{}", std::process::id());
    let pipe_static: &'static str = Box::leak(pipe.into_boxed_str());
    tokio::spawn(async move {
        use tokio::io::{AsyncBufReadExt, BufReader};
        use tokio::net::windows::named_pipe::ServerOptions;
        let server = ServerOptions::new().create(pipe_static).unwrap();
        let _ = server.connect().await;
        let (reader, _writer) = tokio::io::split(server);
        let mut lines = BufReader::new(reader).lines();
        while lines.next_line().await.is_ok() {
            tokio::time::sleep(Duration::from_secs(60)).await;
        }
    });
    tokio::time::sleep(Duration::from_millis(300)).await;

    let start = std::time::Instant::now();
    let err = space_bridge::ipc_client::provider_request(pipe_static, "action", json!({}))
        .await
        .expect_err("black hole must time out");
    let elapsed = start.elapsed();
    assert!(
        err.to_string().contains("timed out"),
        "timeout diagnosis: {err}"
    );
    assert!(
        elapsed.as_secs() >= 9 && elapsed.as_secs() < 30,
        "bounded wait, took {elapsed:?}"
    );
}
