//! V1.7 Phase 3 — action consistency at the Core boundary.
//! Finding (documented, not fixed): Core performs NO dedup. The same
//! action id sent twice reaches the provider twice (at-least-once).
//! Per-type end-effect idempotency is a PROVIDER property, verified
//! separately against the real `actions::perform`.
#![cfg(windows)]
use serde_json::{json, Value};
use space_core::ipc;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

/// Cross-process exclusion for the GLOBAL provider pipe (see
/// support/global_lock.rs): parallel binaries running their own stubs
/// on the same pipe name steal each other's deliveries.
#[path = "support/global_lock.rs"]
mod global_lock;

fn tmp_db(name: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join("spaceos-v17ac");
    std::fs::create_dir_all(&d).unwrap();
    d.join(format!("{name}.db"))
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

/// Counting provider stub on PROVIDER_PIPE: records every action delivery.
/// Guard-aborted at test end (see support/global_lock.rs): no zombie
/// steals later connections.
async fn start_stub(calls: Arc<Mutex<Vec<String>>>) -> global_lock::ServerGuard {
    let reg = Arc::new(global_lock::TaskRegistry::default());
    let outer = (calls.clone(), reg.clone());
    outer.1.clone().spawn(async move {
        use tokio::net::windows::named_pipe::ServerOptions;
        loop {
            let Ok(server) = ServerOptions::new().create(ipc::PROVIDER_PIPE) else { break };
            if server.connect().await.is_err() { break; }
            let (calls, reg) = (outer.0.clone(), outer.1.clone());
            reg.clone().spawn(async move {
                let (r, mut w) = tokio::io::split(server);
                let mut lines = BufReader::new(r).lines();
                while let Ok(Some(l)) = lines.next_line().await {
                    let req: Value = serde_json::from_str(&l).unwrap();
                    let aid = req["params"]["id"].as_str().unwrap_or("").to_string();
                    let action = req["params"]["action"].as_str().unwrap_or("").to_string();
                    calls.lock().unwrap().push(format!("{action}:{aid}"));
                    let resp = json!({"ok": true, "result": {"action_id": aid, "status": "Completed", "detail": {"ok": true}}});
                    let mut s = serde_json::to_string(&resp).unwrap();
                    s.push('\n');
                    if w.write_all(s.as_bytes()).await.is_err() { break; }
                    let _ = w.flush().await;
                }
            });
        }
    });
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    global_lock::ServerGuard::new(reg)
}

// ---------- Same action id twice => delivered twice (at-least-once) ----------
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn same_action_redelivered_no_dedup() {
    let _ipc_lock = global_lock::acquire_global_ipc();
    let pipe = r"\\.\pipe\spaceos-v17a-act";
    let db = tmp_db("act.db");
    let _ = std::fs::remove_file(&db);
    start_core(pipe, &db).await;
    let calls = Arc::new(Mutex::new(vec![]));
    let _stub = start_stub(calls.clone()).await;
    let tok = ipc::send_request(
        pipe,
        "register_agent",
        json!({"principal": "system", "name": "a1",
               "capabilities": ["action.create"], "worlds": []}),
    )
    .await
    .unwrap();
    let tok = tok["result"]["token"].as_str().unwrap().to_string();
    let aid = uuid::Uuid::new_v4().to_string();
    for _ in 0..2 {
        let req = json!({"id": aid, "action": "create",
            "target": {"path": "x"}, "provider_id": "filesystem",
            "actor": "a1", "actor_token": tok,
            "requested_at": chrono::Utc::now().to_rfc3339()});
        let r = ipc::send_request(pipe, "request_action", req)
            .await
            .unwrap();
        assert_eq!(r["ok"], true);
    }
    // Poll-with-deadline (not fixed sleep): stub records synchronously
    // before responding, but polling removes sleep marginality under load
    // without weakening the exact-count threshold.
    for _ in 0..100 {
        if calls.lock().unwrap().len() >= 2 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    // Core performed NO dedup: provider saw the same action twice.
    assert_eq!(
        calls.lock().unwrap().len(),
        2,
        "at-least-once delivery documented"
    );
    // ...and history holds two ActionRequested under one correlation.
    let evs = ipc::send_request(
        pipe,
        "query_events",
        json!({"event_type": "ActionRequested"}),
    )
    .await
    .unwrap();
    let n = evs["result"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|e| e["correlation_id"] == json!(aid))
        .count();
    assert_eq!(n, 2);
}

// ---------- Result tied to correlation ----------
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn result_tied_to_correlation() {
    let _ipc_lock = global_lock::acquire_global_ipc();
    let pipe = r"\\.\pipe\spaceos-v17a-res";
    let db = tmp_db("res.db");
    let _ = std::fs::remove_file(&db);
    start_core(pipe, &db).await;
    let calls = Arc::new(Mutex::new(vec![]));
    let _stub = start_stub(calls.clone()).await;
    let tok = ipc::send_request(
        pipe,
        "register_agent",
        json!({"principal": "system", "name": "a2",
               "capabilities": ["action.create"], "worlds": []}),
    )
    .await
    .unwrap();
    let tok = tok["result"]["token"].as_str().unwrap().to_string();
    let aid = uuid::Uuid::new_v4().to_string();
    let req = json!({"id": aid, "action": "create",
        "target": {"path": "x"}, "provider_id": "filesystem",
        "actor": "a2", "actor_token": tok,
        "requested_at": chrono::Utc::now().to_rfc3339()});
    let r = ipc::send_request(pipe, "request_action", req)
        .await
        .unwrap();
    assert_eq!(r["result"]["action_id"], json!(aid));
    assert_eq!(r["result"]["status"], json!("Completed"));
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let evs = ipc::send_request(
        pipe,
        "query_events",
        json!({"event_type": "ActionCompleted"}),
    )
    .await
    .unwrap();
    let done = evs["result"]
        .as_array()
        .unwrap()
        .iter()
        .find(|e| e["correlation_id"] == json!(aid))
        .expect("completed event");
    assert_eq!(
        done["correlation_id"],
        json!(aid),
        "result tied to request correlation"
    );
}
