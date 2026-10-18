//! X2 — Action retry / ambiguous outcome windows.
//!
//! Hypothesis: redelivery of one Action after an ambiguous outcome may
//! cause a repeated external effect or a Reality/World contradiction.
//! What can break: fake success on ambiguity; hidden dedup; lost effect
//! accounting; correlation confusion (same vs new correlation_id).
//! Failure boundary: Core action pipeline + provider contract (NOT
//! Core retry logic — Core never re-sends; all retries are caller-driven).
//! Why it can break: at-least-once delivery means every retry executes;
//! nothing in Core dedups by action id or correlation.
//! Exact workload (fault-injecting stub over real PROVIDER_PIPE +
//! real fs writes in temp dir, counted):
//!  A. provider never reached (stub down): 0 effects, Failed result.
//!  B+F. provider executes, response dropped: caller sees failure while
//!     Reality changed; same-action-id retry executes again (2 effects,
//!     2 history rows, 1 correlation).
//!  G. same intent, NEW action id + correlation: independent group,
//!     effects counted separately; convergent op converges, Core still
//!     delivers twice (no dedup anywhere).
//! Dataset: temp DB + temp pipe + temp reality dir per test.
//! Fixed threshold: no fake success (ambiguous => Failed, never
//! Completed); effect counts exact; history rows exact per correlation;
//! convergent content converges.
//! Expected behavior: HELD — ambiguity stays visible, everything counted.
//! Measurement: stub invocation counts, file bytes, history row counts.
//! Pass criterion: all exact. Failure criterion: fake success OR
//! miscounted effects OR wrong history grouping.
//! Reproduction protocol: 3 identical runs (whole file x3).
//! (B7 cited for same-id counting + lost-response recovery; NOT rerun:
//! this file attacks the uncovered windows with real effects.)
//!
//! Implemented-semantics note: Core groups action history by ACTION ID
//! (requested.correlation_id = req.id); a custom "correlation_id" field
//! on ActionRequest is ignored. So F == same action id, G == new ids.

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
    let d = std::env::temp_dir().join("spaceos-x2").join(tag);
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
    /// Execute the real fs write, then close without responding, N times.
    DropResponse {
        remaining: usize,
    },
}

struct Stub {
    calls: Arc<Mutex<Vec<String>>>,
    mode: Arc<Mutex<StubMode>>,
    reality: std::path::PathBuf,
}

/// Fault-injecting stub speaking the real provider protocol with REAL
/// fs effects. Guard-aborted at test end (see support/global_lock.rs):
/// no zombie steals later connections. Returns None when the global
/// provider pipe is busy (documented skip, not a pass).
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
                            // REAL external effect, counted.
                            let path = reality.join(format!("{aid}.txt"));
                            let content = target["content"].as_str().unwrap_or("x");
                            std::fs::write(&path, content).ok();
                            calls.lock().unwrap().push(format!("{action}:{aid}"));
                            let drop = {
                                let mut m = mode.lock().unwrap();
                                match *m {
                                    StubMode::DropResponse { remaining } if remaining > 0 => {
                                        *m = StubMode::DropResponse { remaining: remaining - 1 };
                                        true
                                    }
                                    _ => false,
                                }
                            };
                            if drop {
                                // Window B: effect done, ack never sent.
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

fn action_req(aid: &str, corr: &str, actor: &str, tok: &str, content: &str) -> Value {
    json!({"id": aid, "action": "create",
        "target": {"path": "x", "content": content},
        "provider_id": "filesystem", "actor": actor, "actor_token": tok,
        "correlation_id": corr,
        "requested_at": chrono::Utc::now().to_rfc3339()})
}

async fn history_count(pipe: &str, event_type: &str, corr: &str) -> usize {
    let r = ipc::send_request(pipe, "query_events", json!({"event_type": event_type}))
        .await
        .unwrap();
    r["result"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|e| e["correlation_id"] == json!(corr))
        .count()
}

// Window A: provider unreachable => 0 effects, Failed (never Completed).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn x2_window_a_provider_unreached() {
    let _ipc_lock = global_lock::acquire_global_ipc();
    let pipe_owned = format!(r"\\.\pipe\spaceos-x2a-{}", std::process::id());
    let pipe: &'static str = Box::leak(pipe_owned.into_boxed_str());
    let base = tmp_base("a");
    let db = base.join("core.db");
    let _ = std::fs::remove_file(&db);
    start_core(pipe, &db).await;
    // No stub: provider pipe absent.
    let tok = register(pipe, "xa-a").await;
    let aid = "x2a-act-001";
    let r = ipc::send_request(
        pipe,
        "request_action",
        action_req(aid, "x2a-corr-a", "xa-a", &tok, "v1"),
    )
    .await
    .unwrap();
    assert_eq!(r["ok"], true, "envelope ok");
    assert_eq!(
        r["result"]["status"], "Failed",
        "X2-A: unreachable provider => Failed, never Completed"
    );
    assert_eq!(
        history_count(pipe, "ActionRequested", "x2a-act-001").await,
        1
    );
    assert_eq!(history_count(pipe, "ActionFailed", "x2a-act-001").await, 1);
    assert_eq!(
        history_count(pipe, "ActionCompleted", "x2a-act-001").await,
        0
    );
    eprintln!("X2-A: 0 effects possible, Failed recorded, no fake success");
}

// Windows B+F: effect without ack, then same-action retry executes again.
// (Core groups action history by ACTION ID: requested.correlation_id =
// req.id. "Same correlation" below means same action id; "new
// correlation" means new action id.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn x2_window_bf_ambiguous_then_retry() {
    let _ipc_lock = global_lock::acquire_global_ipc();
    let pipe_owned = format!(r"\\.\pipe\spaceos-x2b-{}", std::process::id());
    let pipe: &'static str = Box::leak(pipe_owned.into_boxed_str());
    let base = tmp_base("b");
    let db = base.join("core.db");
    let _ = std::fs::remove_file(&db);
    start_core(pipe, &db).await;
    let reality = base.join("reality");
    std::fs::create_dir_all(&reality).unwrap();
    let stub = Stub {
        calls: Arc::new(Mutex::new(vec![])),
        mode: Arc::new(Mutex::new(StubMode::DropResponse { remaining: 1 })),
        reality: reality.clone(),
    };
    let calls = stub.calls.clone();
    let Some(_stub) = run_stub(stub).await else {
        return;
    };
    let tok = register(pipe, "xb-a").await;
    let aid = "x2b-act-001";

    // Attempt 1: executed in Reality, ack dropped => caller sees Failed.
    let r = ipc::send_request(
        pipe,
        "request_action",
        action_req(aid, "x2b-corr", "xb-a", &tok, "v1"),
    )
    .await
    .unwrap();
    assert_eq!(
        r["result"]["status"], "Failed",
        "X2-B: dropped ack => Failed, not Completed"
    );
    assert_eq!(calls.lock().unwrap().len(), 1, "one real execution");
    assert!(
        reality.join(format!("{aid}.txt")).exists(),
        "X2-B: Reality DID change despite caller-side failure"
    );
    assert_eq!(
        history_count(pipe, "ActionRequested", "x2b-act-001").await,
        1
    );
    assert_eq!(history_count(pipe, "ActionFailed", "x2b-act-001").await, 1);

    // Retry, same action id + same correlation: executes AGAIN (no dedup).
    let r = ipc::send_request(
        pipe,
        "request_action",
        action_req(aid, "x2b-corr", "xb-a", &tok, "v2"),
    )
    .await
    .unwrap();
    assert_eq!(r["result"]["status"], "Completed", "X2-F: retry completes");
    assert_eq!(
        calls.lock().unwrap().len(),
        2,
        "X2-F: no dedup — second real execution"
    );
    assert_eq!(
        history_count(pipe, "ActionRequested", "x2b-act-001").await,
        2
    );
    assert_eq!(
        history_count(pipe, "ActionCompleted", "x2b-act-001").await,
        1
    );
    let content = std::fs::read_to_string(reality.join(format!("{aid}.txt"))).unwrap();
    assert_eq!(content, "v2", "latest retry wins in Reality");
    eprintln!(
        "X2-BF: ambiguity visible, retry explicit, 2 effects / 1 correlation, no fake success"
    );
}

// Window G: same intent, new ids => independent groups; convergent op converges.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn x2_window_g_new_ids_independent() {
    let _ipc_lock = global_lock::acquire_global_ipc();
    let pipe_owned = format!(r"\\.\pipe\spaceos-x2g-{}", std::process::id());
    let pipe: &'static str = Box::leak(pipe_owned.into_boxed_str());
    let base = tmp_base("g");
    let db = base.join("core.db");
    let _ = std::fs::remove_file(&db);
    start_core(pipe, &db).await;
    let reality = base.join("reality");
    std::fs::create_dir_all(&reality).unwrap();
    let stub = Stub {
        calls: Arc::new(Mutex::new(vec![])),
        mode: Arc::new(Mutex::new(StubMode::Normal)),
        reality: reality.clone(),
    };
    let calls = stub.calls.clone();
    let Some(_stub) = run_stub(stub).await else {
        return;
    };
    let tok = register(pipe, "xg-a").await;

    // Same intent twice, fully new identities, same convergent content.
    for (aid, corr) in [("x2g-act-001", "x2g-corr-1"), ("x2g-act-002", "x2g-corr-2")] {
        let r = ipc::send_request(
            pipe,
            "request_action",
            action_req(aid, corr, "xg-a", &tok, "same"),
        )
        .await
        .unwrap();
        assert_eq!(r["result"]["status"], "Completed");
    }
    assert_eq!(
        calls.lock().unwrap().len(),
        2,
        "X2-G: Core never dedups — 2 executions"
    );
    assert_eq!(
        history_count(pipe, "ActionRequested", "x2g-act-001").await,
        1
    );
    assert_eq!(
        history_count(pipe, "ActionRequested", "x2g-act-002").await,
        1
    );
    assert_eq!(
        history_count(pipe, "ActionCompleted", "x2g-act-001").await,
        1
    );
    assert_eq!(
        history_count(pipe, "ActionCompleted", "x2g-act-002").await,
        1
    );
    for aid in ["x2g-act-001", "x2g-act-002"] {
        assert_eq!(
            std::fs::read_to_string(reality.join(format!("{aid}.txt"))).unwrap(),
            "same",
            "X2-G: convergent content converges"
        );
    }
    eprintln!("X2-G: independent groups, 2 effects, convergent bytes, idempotence lives in the op, not Core");
}
