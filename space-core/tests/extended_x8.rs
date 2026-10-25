//! X8 — Durability windows around provider acceptance and result commit.
//!
//! Hypothesis: crash/stall between external Reality change, semantic
//! commit, and Event publication may leave unrecoverable ambiguity.
//! What can break: unknown outcome with no safe retry; fake result;
//! unrecorded Reality change.
//! Failure boundary: Core action pipeline (record-requested BEFORE
//! forward; record-result AFTER response) + provider contract.
//! Why it can break: network/IPC stall between forward and response.
//! Exact workload (fault-injecting stub, real PROVIDER_PIPE + real fs):
//! provider executes the write then NEVER responds (hang mode); caller
//! times out with unknown outcome; stub flips to normal; caller retries
//! SAME action id explicitly.
//! Dataset: temp DB + temp pipe + temp reality dir.
//! Fixed threshold: hung attempt => Timeout (no fake result); Reality
//! changed; requested recorded, NO result recorded yet; explicit retry
//! completes; final history = 2 requested + 1 completed + 0 failed;
//! 2 real executions; latest content wins.
//! Expected behavior: HELD.
//! Measurement: timeout occurrence, file bytes, history counts, effects.
//! Pass criterion: all exact. Failure criterion: fake Completed on the
//! hung attempt OR missing history OR unrecorded effect.
//! Reproduction protocol: 3 identical runs.
//! (Windows A/unreached and B/dropped-ack cited from X2; publication is
//! a synchronous record+broadcast sequence with no await between —
//! hence no injectable window there, stated not tested-by-omission;
//! delivery windows cited from M6.)

#![cfg(windows)]
use serde_json::{json, Value};
use space_core::ipc;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

/// Cross-process exclusion for the GLOBAL provider pipe (see
/// support/global_lock.rs).
#[path = "support/global_lock.rs"]
mod global_lock;

fn tmp_base(tag: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join("spaceos-x8").join(tag);
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
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

async fn register(pipe: &str, name: &str) -> String {
    let r = ipc::send_request(
        pipe,
        "register_agent",
        json!({"principal":"system","name":name,
            "capabilities":["action.create"],"worlds":[]}),
    )
    .await
    .unwrap();
    assert_eq!(r["ok"], true);
    r["result"]["token"].as_str().unwrap().to_string()
}

#[derive(Clone, Copy, PartialEq)]
enum StubMode {
    Normal,
    /// Execute + hold the connection open forever (caller-side timeout).
    Hang,
}

struct Stub {
    calls: Arc<Mutex<Vec<String>>>,
    mode: Arc<Mutex<StubMode>>,
    reality: std::path::PathBuf,
}

async fn run_stub(stub: Stub) -> Option<global_lock::ServerGuard> {
    use tokio::net::windows::named_pipe::ServerOptions;
    if ServerOptions::new().create(ipc::PROVIDER_PIPE).is_err() {
        eprintln!("SKIP-STUB: provider pipe busy (parallel binary); retry separately");
        return None;
    }
    let reg = Arc::new(global_lock::TaskRegistry::default());
    let (calls, mode, reality) = (stub.calls.clone(), stub.mode.clone(), stub.reality.clone());
    let outer = (calls, mode, reality, reg.clone());
    outer.3.clone().spawn(async move {
        for _ in 0..8 {
            let (calls, mode, reality, reg) =
                (outer.0.clone(), outer.1.clone(), outer.2.clone(), outer.3.clone());
            reg.clone().spawn(async move {
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
                    let (calls, mode, reality, reg) =
                        (calls.clone(), mode.clone(), reality.clone(), reg.clone());
                    // Every spawned task is registry-tracked so the guard
                    // aborts the whole tree (including parked hang handlers).
                    reg.clone().spawn(async move {
                        let (r, mut w) = tokio::io::split(server);
                        let mut lines = BufReader::new(r).lines();
                        while let Ok(Some(l)) = lines.next_line().await {
                            let req: Value = match serde_json::from_str(&l) {
                                Ok(v) => v,
                                Err(_) => break,
                            };
                            let aid = req["params"]["id"].as_str().unwrap_or("").to_string();
                            let action = req["params"]["action"].as_str().unwrap_or("").to_string();
                            let target = req["params"]["target"].clone();
                            // REAL effect first, always.
                            let path = reality.join(format!("{aid}.txt"));
                            let content = target["content"].as_str().unwrap_or("x");
                            std::fs::write(&path, content).ok();
                            calls.lock().unwrap().push(format!("{action}:{aid}"));
                            let hang = *mode.lock().unwrap() == StubMode::Hang;
                            if hang {
                                // Hold open forever: caller must time out.
                                tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
                                break;
                            }
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
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    Some(global_lock::ServerGuard::new(reg))
}

async fn history_count(pipe: &str, event_type: &str, aid: &str) -> usize {
    let r = ipc::send_request(pipe, "query_events", json!({"event_type": event_type}))
        .await
        .unwrap();
    r["result"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|e| e["correlation_id"] == json!(aid))
        .count()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn x8_hang_then_explicit_retry() {
    let _ipc_lock = global_lock::acquire_global_ipc();
    let pipe_owned = format!(r"\\.\pipe\spaceos-x8-{}", std::process::id());
    let pipe: &'static str = Box::leak(pipe_owned.into_boxed_str());
    let base = tmp_base("x8");
    let db = base.join("core.db");
    let _ = std::fs::remove_file(&db);
    start_core(pipe, &db).await;
    let reality = base.join("reality");
    std::fs::create_dir_all(&reality).unwrap();
    let mode = Arc::new(Mutex::new(StubMode::Hang));
    let stub = Stub {
        calls: Arc::new(Mutex::new(vec![])),
        mode: mode.clone(),
        reality: reality.clone(),
    };
    let calls = stub.calls.clone();
    let Some(_stub) = run_stub(stub).await else {
        return;
    };
    let tok = register(pipe, "x8-a").await;
    let aid = "x8-act-001";
    let req = json!({"id": aid, "action": "create",
        "target": {"path": "x", "content": "v1"},
        "provider_id": "filesystem", "actor": "x8-a", "actor_token": tok,
        "requested_at": chrono::Utc::now().to_rfc3339()});

    // Attempt 1 hangs: caller-side timeout, outcome UNKNOWN (not success).
    let r = tokio::time::timeout(
        std::time::Duration::from_secs(8),
        ipc::send_request(pipe, "request_action", req.clone()),
    )
    .await;
    assert!(
        r.is_err(),
        "X8: hung provider => caller timeout, never success"
    );
    assert_eq!(
        calls.lock().unwrap().len(),
        1,
        "X8: provider DID execute once"
    );
    assert!(
        reality.join(format!("{aid}.txt")).exists(),
        "X8: Reality changed while caller knows nothing"
    );
    assert_eq!(history_count(pipe, "ActionRequested", aid).await, 1);
    assert_eq!(history_count(pipe, "ActionCompleted", aid).await, 0);
    assert_eq!(history_count(pipe, "ActionFailed", aid).await, 0);
    eprintln!("X8: unknown outcome recorded honestly (requested=1, results=0)");

    // Explicit retry, same action id, stub now normal: completes.
    *mode.lock().unwrap() = StubMode::Normal;
    let mut req2 = req.clone();
    req2["target"] = json!({"path": "x", "content": "v2"});
    let r = ipc::send_request(pipe, "request_action", req2)
        .await
        .unwrap();
    assert_eq!(r["result"]["status"], "Completed");
    assert_eq!(
        calls.lock().unwrap().len(),
        2,
        "X8: retry is a second real execution"
    );
    assert_eq!(history_count(pipe, "ActionRequested", aid).await, 2);
    assert_eq!(history_count(pipe, "ActionCompleted", aid).await, 1);
    assert_eq!(
        std::fs::read_to_string(reality.join(format!("{aid}.txt"))).unwrap(),
        "v2",
        "X8: latest retry wins in Reality"
    );
    eprintln!("X8: explicit retry converges; history 2+1+0 exact; no fake result anywhere");
    // NOTE: the hung server-side handler task for attempt 1 is still parked
    // on its stub connection (leaks until test runtime shutdown). It holds
    // no Storage lock (locks are per-statement scoped) and affects nothing;
    // documented, not hidden.
}
