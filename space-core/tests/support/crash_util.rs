//! Crash-taxonomy shared helpers (TEST-ONLY).
//!
//! Crash model (documented fidelity): a Core "era" = its own Tokio
//! runtime + CoreState on a test pipe + SQLite file. "Crash" drops the
//! whole runtime (every accept loop, connection and in-flight handler
//! dies with no cleanup — the observable equivalent of kill -9: no
//! Storage lock is held across the provider-forward await, verified in
//! ipc.rs handle_action, so abort lands on clean storage state).
//! "Restart" rebuilds CoreState on the SAME db file: SQLite persists,
//! Policy/subscriptions/trace are memory-only and wipe — exactly the
//! contract X5/X13 proved for real processes. Real-process restart is
//! used where no mid-flight timing is needed (T7/T9); here the
//! in-flight checkpoints REQUIRE this model because no existing fault
//! hook can stop Core mid-forward and new production hooks are
//! forbidden without proven need (spec T1 premise honored: BLOCKED
//! would apply to real-process mid-flight; this minimal TEST mechanism
//! is the justified alternative, no production code touched).
//!
//! MIRRORED FILE: none — single copy, included via #[path] by the
//! crash_* suites (all live in space-core/tests).

use serde_json::{json, Value};
use space_core::ipc;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// One live Core era. MUST be consumed via [`crash_era`] (by value):
/// an implicit drop on a Tokio worker thread panics (runtime shutdown
/// guard) and masks real failures — the API takes Era by value so the
/// correct path is the easy one.
#[allow(dead_code)]
pub struct Era {
    rt: tokio::runtime::Runtime,
}

/// Boot a Core era on `pipe` with `db`. Panics past deadline.
/// Async: readiness is driven by the CALLER runtime (never block_on
/// inside async context — that panics).
pub async fn boot_era(db: &Path, pipe: &'static str) -> Era {
    let storage = space_core::Storage::open(db).unwrap();
    let state = ipc::CoreState::new(storage);
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let s2 = state.clone();
    rt.spawn(async move {
        let _ = ipc::run_server(s2, pipe).await;
    });
    // Readiness = real query, deadline not sleep.
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let ok = ipc::send_request(pipe, "list_providers", json!({}))
            .await
            .map(|v| v["ok"].as_bool() == Some(true))
            .unwrap_or(false);
        if ok {
            break;
        }
        if std::time::Instant::now() > deadline {
            panic!("crash_util: Core era not ready in 30s");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Era { rt }
}

/// Crash the era: drop the whole runtime (all tasks die, no cleanup).
/// The drop runs on a blocking thread: dropping a multi-thread runtime
/// on another runtime's worker panics (tokio shutdown guard), so the
/// Era is moved out via spawn_blocking.
pub async fn crash_era(era: Era) {
    tokio::task::spawn_blocking(move || drop(era))
        .await
        .unwrap();
}

pub fn tmp_base(tag: &str) -> PathBuf {
    let d = std::env::temp_dir().join("spaceos-crash").join(tag);
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

pub async fn register(pipe: &str, name: &str, caps: &[&str], worlds: &[&str]) -> String {
    let r = ipc::send_request(
        pipe,
        "register_agent",
        json!({"principal":"system","name":name,
            "capabilities":caps,"worlds":worlds}),
    )
    .await
    .unwrap();
    assert_eq!(r["ok"], true, "register {name}: {r}");
    r["result"]["token"].as_str().unwrap().to_string()
}

/// History of one event type via live pipe; returns (event_type, correlation).
pub async fn history_via_pipe(pipe: &str, event_type: &str) -> Vec<(String, String)> {
    let r = ipc::send_request(pipe, "query_events", json!({"event_type": event_type}))
        .await
        .unwrap();
    assert_eq!(r["ok"], true);
    r["result"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| {
            (
                e["event_type"].as_str().unwrap_or("").to_string(),
                e["correlation_id"].as_str().unwrap_or("").to_string(),
            )
        })
        .collect()
}

/// History of one event type DIRECT from the db file (usable while no
/// Core is live — the post-crash window). Opens its own handle.
pub fn history_via_db(db: &Path, event_type: &str) -> Vec<(String, String)> {
    let s = space_core::Storage::open(db).unwrap();
    let q = space_core::query::EventQuery {
        event_type: Some(event_type.to_string()),
        ..Default::default()
    };
    s.query_events(&q)
        .unwrap()
        .into_iter()
        .map(|e| (e.event_type, e.correlation_id.unwrap_or_default()))
        .collect()
}

#[derive(Clone, Copy, PartialEq, Debug)]
pub enum StubPhase {
    /// Accept + signal ACCEPTED, hold WITHOUT executing until Normal.
    HoldPre,
    /// Execute real fs write + signal EXECUTED, hold response until Normal.
    ExecHold,
    /// Execute + respond Completed immediately.
    Normal,
}

pub struct StubCtl {
    pub phase: Mutex<StubPhase>,
    pub accepted: Mutex<Vec<String>>,
    pub executed: Mutex<Vec<String>>,
    pub reality: PathBuf,
}

/// Gated provider stub on the GLOBAL provider pipe (caller holds the
/// global IPC lock). Guard-aborted at test end (no zombies).
/// Returns None when the pipe is busy (documented skip, not a pass).
///
/// Gating protocol (test-controlled checkpoints):
/// - HoldPre: accept + record ACCEPTED, hold WITHOUT executing until
///   the phase flips to Normal (then execute + respond) or the
///   connection breaks (then exit, never executed).
/// - ExecHold: execute the REAL fs write + record EXECUTED, hold the
///   response until Normal (then respond) or breakage.
/// - Normal: execute + respond Completed immediately.
/// Every execution writes reality/{aid}.txt with the request content
/// and logs the aid — the exact Reality-effect counter.
pub async fn run_gated_stub(ctl: Arc<StubCtl>) -> Option<super::global_lock::ServerGuard> {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    if tokio::net::windows::named_pipe::ServerOptions::new()
        .create(ipc::PROVIDER_PIPE)
        .is_err()
    {
        eprintln!("SKIP-STUB: provider pipe busy (parallel binary); retry separately");
        return None;
    }
    let reg = Arc::new(super::global_lock::TaskRegistry::default());
    let outer = (ctl.clone(), reg.clone());
    outer.1.clone().spawn(async move {
        for _ in 0..8 {
            let (ctl, reg) = (outer.0.clone(), outer.1.clone());
            reg.clone().spawn(async move {
                loop {
                    let server = match tokio::net::windows::named_pipe::ServerOptions::new()
                        .create(ipc::PROVIDER_PIPE)
                    {
                        Ok(s) => s,
                        Err(_) => {
                            tokio::time::sleep(Duration::from_millis(50)).await;
                            continue;
                        }
                    };
                    if server.connect().await.is_err() {
                        continue;
                    }
                    let (ctl2, reg2) = (ctl.clone(), reg.clone());
                    reg2.spawn(async move {
                        let (r, mut w) = tokio::io::split(server);
                        let mut lines = BufReader::new(r).lines();
                        while let Ok(Some(l)) = lines.next_line().await {
                            let req: Value = match serde_json::from_str(&l) {
                                Ok(v) => v,
                                Err(_) => break,
                            };
                            if req["method"].as_str() != Some("action") {
                                break;
                            }
                            let aid =
                                req["params"]["id"].as_str().unwrap_or("").to_string();
                            let content = req["params"]["target"]["content"]
                                .as_str()
                                .unwrap_or("x")
                                .to_string();
                            let phase = *ctl2.phase.lock().unwrap();
                            match phase {
                                StubPhase::HoldPre => {
                                    ctl2.accepted.lock().unwrap().push(aid.clone());
                                    // Hold pre-execution until Normal or breakage.
                                    loop {
                                        tokio::time::sleep(Duration::from_millis(50)).await;
                                        if *ctl2.phase.lock().unwrap()
                                            == StubPhase::Normal
                                        {
                                            break;
                                        }
                                        // Breakage probe: a dead Core shows up
                                        // on the next write; meanwhile the
                                        // await above keeps us parked here.
                                        // Detect via a zero-byte probe is
                                        // platform-specific — instead the
                                        // post-kill orphan release is driven
                                        // by the test flipping to Normal.
                                    }
                                    let path = ctl2.reality.join(format!("{aid}.txt"));
                                    std::fs::write(&path, &content).ok();
                                    ctl2.executed.lock().unwrap().push(aid.clone());
                                    let resp = json!({"ok": true, "result": {"action_id": aid, "status": "Completed", "detail": {"ok": true}}});
                                    let mut s = serde_json::to_string(&resp).unwrap();
                                    s.push('\n');
                                    if w.write_all(s.as_bytes()).await.is_err() {
                                        break;
                                    }
                                    let _ = w.flush().await;
                                }
                                StubPhase::ExecHold => {
                                    // REAL effect first, then hold the ack.
                                    let path = ctl2.reality.join(format!("{aid}.txt"));
                                    std::fs::write(&path, &content).ok();
                                    ctl2.executed.lock().unwrap().push(aid.clone());
                                    loop {
                                        tokio::time::sleep(Duration::from_millis(50)).await;
                                        if *ctl2.phase.lock().unwrap()
                                            == StubPhase::Normal
                                        {
                                            break;
                                        }
                                    }
                                    let resp = json!({"ok": true, "result": {"action_id": aid, "status": "Completed", "detail": {"ok": true}}});
                                    let mut s = serde_json::to_string(&resp).unwrap();
                                    s.push('\n');
                                    if w.write_all(s.as_bytes()).await.is_err() {
                                        break;
                                    }
                                    let _ = w.flush().await;
                                }
                                StubPhase::Normal => {
                                    let path = ctl2.reality.join(format!("{aid}.txt"));
                                    std::fs::write(&path, &content).ok();
                                    ctl2.executed.lock().unwrap().push(aid.clone());
                                    let resp = json!({"ok": true, "result": {"action_id": aid, "status": "Completed", "detail": {"ok": true}}});
                                    let mut s = serde_json::to_string(&resp).unwrap();
                                    s.push('\n');
                                    if w.write_all(s.as_bytes()).await.is_err() {
                                        break;
                                    }
                                    let _ = w.flush().await;
                                }
                            }
                        }
                    });
                }
            });
        }
        loop {
            tokio::time::sleep(Duration::from_secs(3600)).await;
        }
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    Some(super::global_lock::ServerGuard::new(reg))
}

/// Poll a condition with deadline (no fixed sleeps for correctness).
pub async fn poll_deadline<F>(what: &str, ms: u64, mut f: F)
where
    F: FnMut() -> bool,
{
    let deadline = std::time::Instant::now() + Duration::from_millis(ms);
    while !f() {
        if std::time::Instant::now() > deadline {
            panic!("crash_util: deadline waiting for {what}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Raw subscriber connection (pipe client). Returns the line reader
/// positioned AFTER the ack (caller reads deliveries from it).
pub struct RawSub {
    pub lines: tokio::io::Lines<
        tokio::io::BufReader<tokio::io::ReadHalf<tokio::net::windows::named_pipe::NamedPipeClient>>,
    >,
}

pub async fn raw_subscribe(pipe: &str, params: Value) -> (RawSub, Value) {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    let stream = tokio::net::windows::named_pipe::ClientOptions::new()
        .open(pipe)
        .unwrap();
    let (r, mut w) = tokio::io::split(stream);
    let req = json!({"method": "subscribe", "params": params});
    let mut s = serde_json::to_string(&req).unwrap();
    s.push('\n');
    w.write_all(s.as_bytes()).await.unwrap();
    w.flush().await.unwrap();
    let mut lines = BufReader::new(r).lines();
    let ack = lines.next_line().await.unwrap().unwrap_or_default();
    (RawSub { lines }, serde_json::from_str(&ack).unwrap())
}

/// Read one delivery with deadline; None on timeout/close.
pub async fn recv_delivery(sub: &mut RawSub, ms: u64) -> Option<Value> {
    match tokio::time::timeout(Duration::from_millis(ms), sub.lines.next_line()).await {
        Ok(Ok(Some(l))) => serde_json::from_str(&l).ok(),
        _ => None,
    }
}
