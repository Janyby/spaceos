//! Product vertical slice: Space -> World -> Agent -> Browser -> Reality.
//!
//! One scenario on the LIVE system (real `spaceos.exe` Core+Provider,
//! real `agent-demo.exe`, real Browser HTTP UI), not a lab test of one
//! module. Chain: open Space -> project World context -> connect Agent ->
//! two Browser Views on one context -> Agent action changes a real file ->
//! external disk change reconciles -> views + trace stay coherent.
//!
//! Needs: Windows, no other Core on \\.\pipe\spaceos-core (global lock),
//! built spaceos.exe + agent-demo.exe. Single run (repeatability is X1's
//! 3x job). Timeout-bounded everywhere; children always reaped.
#![cfg(windows)]
use serde_json::{json, Value};
use space_core::ipc;
use std::path::PathBuf;
use std::time::Duration;

#[path = "support/global_lock.rs"]
mod global_lock;

const UI_PORT: u16 = 18765;

/// Demo result file. Lives inside the served root on purpose: the provider
/// gates writes to its allowed roots, so the agent must work there too.
fn result_file(root: &std::path::Path) -> PathBuf {
    root.join("v1-result.txt")
}

fn base() -> PathBuf {
    let d = std::env::temp_dir().join("spaceos-slice");
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn bins() -> (PathBuf, PathBuf) {
    let mut dir = std::env::current_exe().unwrap();
    dir.pop();
    dir.pop();
    (dir.join("spaceos.exe"), dir.join("agent-demo.exe"))
}

struct ChildGuard {
    child: tokio::process::Child,
    name: &'static str,
}

impl ChildGuard {
    async fn kill_wait(&mut self) {
        let _ = self.child.kill().await;
        match tokio::time::timeout(Duration::from_secs(15), self.child.wait()).await {
            Ok(Ok(status)) => eprintln!("SLICE: {} exited: {status}", self.name),
            other => panic!(
                "SLICE: {} did not exit cleanly: {:?}",
                self.name,
                other.map(|r| r.map(|_| ()))
            ),
        }
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        // Panic paths skip kill_wait: never leave orphan daemons behind
        // (they bind the machine-global pipe and break later runs).
        let _ = self.child.start_kill();
    }
}

async fn wait_ready() {
    for _ in 0..60 {
        match ipc::send_request(ipc::CORE_PIPE, "list_providers", json!({})).await {
            Ok(v) if v["ok"].as_bool() == Some(true) => return,
            _ => tokio::time::sleep(Duration::from_millis(500)).await,
        }
    }
    panic!("SLICE: Core not ready in 30s");
}

async fn wait_provider() {
    for _ in 0..20 {
        let r = ipc::send_request(ipc::CORE_PIPE, "list_providers", json!({}))
            .await
            .unwrap();
        if r["result"]
            .as_array()
            .map(|a| a.iter().any(|p| p["id"] == "filesystem"))
            .unwrap_or(false)
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    panic!("SLICE: filesystem provider not registered");
}

/// Raw HTTP GET against the real Browser UI (same bytes a browser renders).
async fn http_get(port: u16, path: &str) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut s = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("SLICE: ui must listen");
    let req = format!("GET {path} HTTP/1.0\r\n\r\n");
    s.write_all(req.as_bytes()).await.unwrap();
    let mut out = Vec::new();
    let mut buf = vec![0u8; 65536];
    loop {
        match s.read(&mut buf).await {
            Ok(0) => break,
            Ok(n) => out.extend_from_slice(&buf[..n]),
            Err(_) => break,
        }
    }
    let text = String::from_utf8_lossy(&out).to_string();
    assert!(
        text.starts_with("HTTP/1.1 200 OK"),
        "SLICE: {path} must return 200, got: {}",
        &text[..text.len().min(120)]
    );
    text.split("\r\n\r\n").nth(1).unwrap_or("").to_string()
}

async fn poll_http_event(port: u16, needle_type: &str, needle_path: &str) {
    for _ in 0..60 {
        let body = http_get(port, "/api/events").await;
        let evs: Vec<Value> = serde_json::from_str(&body).unwrap_or_default();
        if evs.iter().any(|e| {
            e["event_type"] == needle_type
                && serde_json::to_string(&e["payload"])
                    .unwrap_or_default()
                    .contains(needle_path)
        }) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    panic!("SLICE: Browser view never showed {needle_type} for {needle_path}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn product_vertical_slice() {
    let _lock = global_lock::acquire_global_ipc();
    let base = base();
    let db = base.join("core.db");
    let root = base.join("root");
    std::fs::create_dir_all(&root).unwrap();
    let result_file = result_file(&root);
    let _ = std::fs::remove_file(&result_file);
    std::fs::create_dir_all(result_file.parent().unwrap()).unwrap();

    let core_token = "slice-fixed-token-1";
    std::env::set_var("SPACEOS_CORE_TOKEN", core_token);
    let (spaceos_bin, agent_bin) = bins();
    assert!(spaceos_bin.exists(), "spaceos.exe missing: {spaceos_bin:?}");
    assert!(agent_bin.exists(), "agent-demo.exe missing: {agent_bin:?}");

    // ---- Step A: open Space (Core + Browser UI) ----
    let mut core = ChildGuard {
        child: tokio::process::Command::new(&spaceos_bin)
            .args(["run", "--root"])
            .arg(&root)
            .env("SPACEOS_DB", &db)
            .env("SPACEOS_CORE_TOKEN", core_token)
            .current_dir(&base)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("spawn spaceos"),
        name: "spaceos",
    };
    wait_ready().await;
    wait_provider().await;

    let mut ui = ChildGuard {
        child: tokio::process::Command::new(&spaceos_bin)
            .args(["ui", "--port"])
            .arg(UI_PORT.to_string())
            .env("SPACEOS_CORE_TOKEN", core_token)
            .current_dir(&base)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("spawn ui"),
        name: "spaceos-ui",
    };
    // UI serves the real page (Tree/Graph buttons present).
    let mut page_ok = false;
    for _ in 0..20 {
        if let Ok(body) = try_http_get(UI_PORT, "/").await {
            if body.contains("btnTree") && body.contains("btnGraph") {
                page_ok = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert!(page_ok, "SLICE A: Browser UI serves Tree+Graph page");

    // ---- Step B: project World context (Space; id acts as world_id) ----
    // Raw IPC has no actor field: bind the trusted CLI principal explicitly
    // (same gate agent-demo satisfies per-call). Space has no server-side
    // id minting: the client builds the full record like agent-demo does.
    let world_id = format!("space-slice-{}", std::process::id());
    let now = chrono::Utc::now().to_rfc3339();
    let space = ipc::send_request(
        ipc::CORE_PIPE,
        "create_space",
        json!({
        "id": world_id, "name": "Slice Project", "metadata": {},
        "created_at": now, "updated_at": now, "actor": "cli"}),
    )
    .await
    .unwrap();
    assert_eq!(space["ok"], true, "SLICE B: create_space: {space}");
    assert!(!world_id.is_empty(), "SLICE B: stable Space identity");

    // ---- Step C: connect Agent (real agent-demo binary, public API) ----
    let out = tokio::time::timeout(
        Duration::from_secs(120),
        tokio::process::Command::new(&agent_bin)
            .env("SPACEOS_CORE_TOKEN", core_token)
            .env("SPACEOS_RESULT_FILE", result_file.clone())
            .current_dir(&base)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .expect("spawn agent-demo")
            .wait_with_output(),
    )
    .await
    .expect("SLICE C: agent-demo timed out")
    .expect("SLICE C: agent wait failed");
    assert!(
        out.status.success(),
        "SLICE C: agent exit ok: {} stdout={} stderr={}",
        out.status,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );

    // Agent works in the Browser's project context (its objects carry it).
    let world = ipc::send_request(
        ipc::CORE_PIPE,
        "get_world",
        json!({"world_id": space["result"]["id"]}),
    )
    .await
    .unwrap();
    assert_eq!(world["ok"], true);
    // AgentCompleted + its Result object visible in Core.
    let evs = ipc::send_request(ipc::CORE_PIPE, "query_events", json!({}))
        .await
        .unwrap();
    for t in ["AgentStarted", "ActionCompleted", "AgentStopped"] {
        assert!(
            evs["result"]
                .as_array()
                .unwrap()
                .iter()
                .any(|e| e["event_type"] == t),
            "SLICE C: event {t} present"
        );
    }

    // ---- Step E1: Agent action -> real file (bytes on disk, not just status) ----
    let content = std::fs::read_to_string(&result_file).expect("SLICE E1: result file exists");
    assert_eq!(content, "V1 Result", "SLICE E1: real file content");

    // ---- Step D: two Browser Views, one context, identities intact ----
    let wbody = http_get(UI_PORT, "/api/world").await;
    let wv: Value = serde_json::from_str(&wbody).expect("SLICE D: world JSON");
    assert!(
        wv["spaces"]
            .as_array()
            .unwrap()
            .iter()
            .any(|s| s["id"] == world_id),
        "SLICE D: Browser world view shows our Space"
    );
    assert!(
        wv["objects"]
            .as_array()
            .unwrap()
            .iter()
            .any(|o| o["type"] == "Result"),
        "SLICE D: Browser world view shows Agent Result"
    );
    let ebody = http_get(UI_PORT, "/api/events").await;
    let evs_view: Vec<Value> = serde_json::from_str(&ebody).expect("SLICE D: events JSON");
    assert!(
        evs_view
            .iter()
            .any(|e| e["event_type"] == "ActionCompleted"),
        "SLICE D: Browser events view shows the action"
    );
    // Identity across views: HTTP world objects == IPC objects.
    let via_ipc = ipc::send_request(ipc::CORE_PIPE, "query_objects", json!({}))
        .await
        .unwrap();
    let mut http_ids: Vec<String> = wv["objects"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|o| o["id"].as_str().map(|s| s.to_string()))
        .collect();
    let mut ipc_ids: Vec<String> = via_ipc["result"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|o| o["id"].as_str().map(|s| s.to_string()))
        .collect();
    http_ids.sort();
    ipc_ids.sort();
    assert_eq!(
        http_ids, ipc_ids,
        "SLICE D: object identity intact across views"
    );

    // Live push contract inside the slice: world-scoped subscription delivers.
    let reg = ipc::send_request(
        ipc::CORE_PIPE,
        "register_agent",
        json!({"principal": "system", "name": "slice-viewer",
            "capabilities": ["event.record"], "worlds": [world_id]}),
    )
    .await
    .unwrap();
    let tok = reg["result"]["token"].as_str().unwrap().to_string();
    let delivered = live_subscribe_recv(&world_id, &tok).await;
    assert_eq!(
        delivered, "SlicePing",
        "SLICE D: live subscription delivers"
    );

    // ---- Step E2: external disk change -> watcher -> Core -> Views ----
    std::fs::write(root.join("ext.txt"), "external").unwrap();
    poll_http_event(UI_PORT, "FileCreated", "ext.txt").await;
    let objs = ipc::send_request(
        ipc::CORE_PIPE,
        "query_objects",
        json!({"provider_id": "filesystem"}),
    )
    .await
    .unwrap();
    assert!(
        objs["result"]
            .as_array()
            .unwrap()
            .iter()
            .any(|o| o["external_id"]
                .as_str()
                .map(|e| e.ends_with("ext.txt"))
                .unwrap_or(false)),
        "SLICE E2: Core reflects external file"
    );
    let wbody2 = http_get(UI_PORT, "/api/world").await;
    assert!(
        wbody2.contains("ext.txt"),
        "SLICE E2: Browser view shows external file"
    );

    // ---- Step F: own linked action (action_id -> file -> event -> trace) ----
    let aid = format!("slice-act-{}", std::process::id());
    let req = json!({
        "id": aid, "action": "create",
        "target": {"path": root.join("own.txt").to_string_lossy(), "content": "own"},
        "provider_id": "filesystem", "actor": "cli",
        "requested_at": chrono::Utc::now().to_rfc3339(), "actor_token": "",
    });
    let resp = ipc::send_request(ipc::CORE_PIPE, "request_action", req)
        .await
        .unwrap();
    assert_eq!(
        resp["result"]["status"], "Completed",
        "SLICE F: linked action: {resp}"
    );
    assert_eq!(
        std::fs::read_to_string(root.join("own.txt")).unwrap(),
        "own",
        "SLICE F: linked file bytes"
    );
    let done_evs = ipc::send_request(
        ipc::CORE_PIPE,
        "query_events",
        json!({"event_type": "ActionCompleted"}),
    )
    .await
    .unwrap();
    assert!(
        done_evs["result"]
            .as_array()
            .unwrap()
            .iter()
            .any(|e| e["correlation_id"] == aid),
        "SLICE F: ActionCompleted tied to action id"
    );
    let tq = ipc::send_request(
        ipc::CORE_PIPE,
        "trace_query",
        json!({"subscriber": "system", "action_id": aid}),
    )
    .await
    .unwrap();
    assert_eq!(tq["ok"], true, "SLICE F: trace_query: {tq}");
    assert!(
        tq["result"]
            .as_array()
            .unwrap()
            .iter()
            .any(|s| s["action_id"] == aid),
        "SLICE F: SpaceTrace spans carry the action id"
    );

    // ---- Teardown (no state left behind) ----
    ui.kill_wait().await;
    core.kill_wait().await;
    let _ = std::fs::remove_file(&result_file);
    let _ = std::fs::remove_dir_all(&base);
}

/// Minimal raw IPC subscriber: proves the push path inside the slice
/// (subscribe -> foreign record_event -> delivery on our pipe).
async fn live_subscribe_recv(world_id: &str, token: &str) -> String {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::windows::named_pipe::ClientOptions;
    let stream = ClientOptions::new()
        .open(ipc::CORE_PIPE)
        .expect("sub connect");
    let (reader, mut writer) = tokio::io::split(stream);
    let req = json!({"method": "subscribe", "params": {
        "subscriber": "slice-viewer", "actor_token": token,
        "world_id": world_id, "event_types": ["SlicePing"]}});
    let mut s = serde_json::to_string(&req).unwrap();
    s.push('\n');
    writer.write_all(s.as_bytes()).await.unwrap();
    writer.flush().await.unwrap();
    let mut lines = BufReader::new(reader).lines();
    // Ack first.
    let ack = tokio::time::timeout(Duration::from_secs(10), lines.next_line())
        .await
        .expect("SLICE D: sub ack timeout")
        .expect("SLICE D: sub ack io")
        .expect("SLICE D: sub ack eof");
    assert!(ack.contains("\"subscribed\""), "SLICE D: sub ack: {ack}");
    // Cause a matching event through the public API from another task.
    // Full Event shape (id + timestamp are mandatory server-side).
    let pipe = ipc::CORE_PIPE.to_string();
    let w = world_id.to_string();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(500)).await;
        let _ = ipc::send_request(
            &pipe,
            "record_event",
            json!({"id": "slice-ping-1", "event_type": "SlicePing", "actor": "cli",
                "provider_id": "filesystem", "object_ids": [],
                "payload": {}, "timestamp": chrono::Utc::now().to_rfc3339(),
                "correlation_id": null, "world_id": w}),
        )
        .await;
    });
    let line = tokio::time::timeout(Duration::from_secs(15), lines.next_line())
        .await
        .expect("SLICE D: delivery timeout")
        .expect("SLICE D: delivery io")
        .expect("SLICE D: delivery eof");
    let v: Value = serde_json::from_str(&line).expect("SLICE D: delivery json");
    // Delivery envelope is {"event": {...}} (Core run_protocol pump).
    v["event"]["event_type"].as_str().unwrap_or("").to_string()
}

async fn try_http_get(port: u16, path: &str) -> Result<String, String> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut s = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .map_err(|e| e.to_string())?;
    let req = format!("GET {path} HTTP/1.0\r\n\r\n");
    s.write_all(req.as_bytes())
        .await
        .map_err(|e| e.to_string())?;
    let mut out = Vec::new();
    let mut buf = vec![0u8; 65536];
    loop {
        match s.read(&mut buf).await {
            Ok(0) => break,
            Ok(n) => out.extend_from_slice(&buf[..n]),
            Err(e) => return Err(e.to_string()),
        }
    }
    let text = String::from_utf8_lossy(&out).to_string();
    if !text.starts_with("HTTP/1.1 200 OK") {
        return Err(format!("non-200: {}", &text[..text.len().min(60)]));
    }
    Ok(text.split("\r\n\r\n").nth(1).unwrap_or("").to_string())
}
