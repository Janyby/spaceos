//! B7 — At-least-once Action duplication. Fixed conditions, see
//! docs/BREAKAGE_CAMPAIGN.md. Same id retried: deliveries counted at a
//! stub, history grouped by correlation, recovery without resend.
#![cfg(windows)]
use serde_json::{json, Value};
use space_core::ipc;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

/// Cross-process exclusion for the GLOBAL provider pipe (see
/// support/global_lock.rs): parallel binaries/threads running their own
/// stubs on the same pipe name steal each other's deliveries.
#[path = "support/global_lock.rs"]
mod global_lock;

fn tmp_db(tag: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join("spaceos-bb7");
    std::fs::create_dir_all(&d).unwrap();
    d.join(format!("{tag}.db"))
}

async fn call(pipe: &str, method: &str, params: Value) -> Value {
    ipc::send_request(pipe, method, params).await.unwrap()
}

async fn start_core(pipe: &str, db: &std::path::Path) {
    let storage = space_core::Storage::open(db).unwrap();
    let state = ipc::CoreState::new(storage);
    let s2 = state.clone();
    let p = pipe.to_string();
    tokio::spawn(async move {
        let _ = ipc::run_server(s2, &p).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
}

async fn register(pipe: &str, name: &str, caps: &[&str], worlds: &[&str]) -> String {
    let r = call(
        pipe,
        "register_agent",
        json!({"principal": "system", "name": name, "capabilities": caps, "worlds": worlds}),
    )
    .await;
    assert_eq!(r["ok"], true);
    r["result"]["token"].as_str().unwrap().to_string()
}

fn with_token(mut p: Value, tok: &str) -> Value {
    p["actor_token"] = json!(tok);
    p
}

fn pipe_for(tag: &str) -> String {
    Box::leak(format!(r"\\.\pipe\spaceos-bb7-{tag}-{}", std::process::id()).into_boxed_str())
        .to_string()
}

/// Counting stub with its own accept loop (multiple instances, like Core).
/// PROVIDER_PIPE is a global name: the file lock (held by the caller)
/// excludes other suites, and the returned guard aborts every spawned
/// server task at test end so no zombie steals later connections.
/// Returns None when the pipe is busy (documented skip, not a pass).
async fn run_stub(calls: Arc<Mutex<Vec<String>>>) -> Option<global_lock::ServerGuard> {
    use tokio::net::windows::named_pipe::ServerOptions;
    if ServerOptions::new().create(ipc::PROVIDER_PIPE).is_err() {
        eprintln!("SKIP-STUB: provider pipe busy (parallel binary); retry separately");
        return None;
    }
    let reg = Arc::new(global_lock::TaskRegistry::default());
    // Probe succeeded; the real loop below re-creates per iteration.
    {
        let outer = (calls.clone(), reg.clone());
        outer.1.clone().spawn(async move {
            for _ in 0..8 {
                let (calls, reg) = (outer.0.clone(), outer.1.clone());
                let spawner = reg.clone();
                spawner.spawn(async move {
                    loop {
                        let server = match ServerOptions::new().create(ipc::PROVIDER_PIPE) {
                            Ok(s) => s,
                            Err(_) => {
                                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                                continue;
                            }
                        };
                        if server.connect().await.is_err() {
                            continue;
                        }
                        let calls2 = calls.clone();
                        let spawner2 = reg.clone();
                        spawner2.spawn(async move {
                            let (r, mut w) = tokio::io::split(server);
                            let mut lines = BufReader::new(r).lines();
                            while let Ok(Some(l)) = lines.next_line().await {
                                let req: Value = match serde_json::from_str(&l) {
                                    Ok(v) => v,
                                    Err(_) => break,
                                };
                                let aid = req["params"]["id"].as_str().unwrap_or("").to_string();
                                let action = req["params"]["action"].as_str().unwrap_or("").to_string();
                                calls2.lock().unwrap().push(format!("{action}:{aid}"));
                                let resp = json!({"ok": true, "result": {"action_id": aid, "status": "Completed", "detail": {"ok": true}}});
                                let mut s = serde_json::to_string(&resp).unwrap();
                                s.push('\n');
                                if w.write_all(s.as_bytes()).await.is_err() {
                                    break;
                                }
                                let _ = w.flush().await;
                            }
                        });
                    }
                });
            }
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
            }
        });
    }
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    Some(global_lock::ServerGuard::new(reg))
}

// ---------- Same id twice: exactly 2 deliveries, 1 correlation group ----------
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retry_same_id_counted_twice() {
    let _ipc_lock = global_lock::acquire_global_ipc();
    let pipe = pipe_for("dup");
    let db = tmp_db("dup.db");
    let _ = std::fs::remove_file(&db);
    start_core(&pipe, &db).await;
    let calls = Arc::new(Mutex::new(Vec::<String>::new()));
    let Some(_stub) = run_stub(calls.clone()).await else {
        return; // documented skip, not a pass
    };
    let tok = register(&pipe, "dup-a", &["action.create"], &[]).await;
    let aid = uuid::Uuid::new_v4().to_string();
    for _ in 0..2 {
        let req = json!({"id": aid, "action": "create", "target": {"path": "x"},
            "provider_id": "filesystem", "actor": "dup-a", "actor_token": tok,
            "requested_at": chrono::Utc::now().to_rfc3339()});
        let r = ipc::send_request(&pipe, "request_action", req)
            .await
            .unwrap();
        assert_eq!(r["ok"], true);
    }
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    // Poll (not fixed sleep): the stub records synchronously before
    // responding, but poll-with-deadline removes sleep marginality under
    // load without weakening the exact-count threshold.
    for _ in 0..100 {
        if calls.lock().unwrap().len() >= 2 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    let n_calls = calls.lock().unwrap().len();
    let reqs = call(
        &pipe,
        "query_events",
        json!({"event_type": "ActionRequested"}),
    )
    .await;
    let group: Vec<_> = reqs["result"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|e| e["correlation_id"] == json!(aid))
        .collect();
    eprintln!(
        "retry: stub deliveries={n_calls}, ActionRequested under correlation={}",
        group.len()
    );
    // At-least-once, exactly accounted: 2 deliveries, 2 history rows, 1 id.
    assert_eq!(n_calls, 2, "provider saw both deliveries");
    assert_eq!(
        group.len(),
        2,
        "both attempts recorded under one correlation"
    );
}

// ---------- Lost response: recover by correlation query, no resend ----------
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lost_response_recovered_by_correlation() {
    let _ipc_lock = global_lock::acquire_global_ipc();
    let pipe = pipe_for("lost");
    let db = tmp_db("lost.db");
    let _ = std::fs::remove_file(&db);
    start_core(&pipe, &db).await;
    let tok = register(&pipe, "lost-a", &["action.create"], &[]).await;
    let aid = uuid::Uuid::new_v4().to_string();
    // Raw send, close without reading (response lost on the wire).
    {
        let stream = tokio::net::windows::named_pipe::ClientOptions::new()
            .open(&pipe)
            .unwrap();
        let (r, mut w) = tokio::io::split(stream);
        let mut params = json!({"id": aid, "action": "create", "target": {"path": "x"},
            "provider_id": "filesystem", "actor": "lost-a", "actor_token": tok,
            "requested_at": chrono::Utc::now().to_rfc3339()});
        params["auth"] = json!(ipc::core_token());
        let req = json!({"method": "request_action", "params": params});
        let mut s = serde_json::to_string(&req).unwrap();
        s.push('\n');
        w.write_all(s.as_bytes()).await.unwrap();
        w.flush().await.unwrap();
        drop(r);
        drop(w);
    }
    // Recovery WITHOUT resend: the attempt is found by correlation id.
    // Poll-with-deadline (same rationale as above): the server processes
    // the dropped-connection request asynchronously.
    let mut found_n = 0usize;
    for _ in 0..100 {
        let reqs = call(
            &pipe,
            "query_events",
            json!({"event_type": "ActionRequested"}),
        )
        .await;
        found_n = reqs["result"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|e| e["correlation_id"] == json!(aid))
            .count();
        if found_n == 1 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert_eq!(
        found_n, 1,
        "lost attempt recoverable by correlation, no resend needed to learn the state"
    );
}
