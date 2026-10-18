//! Roots gate: the real provider daemon refuses paths outside the
//! configured allowed roots, through the real IPC pipe.
//!
//! Uses the machine-global provider pipe: serialized by the global lock,
//! torn down (abort) at test end like every other pipe suite here.
#![cfg(windows)]

use serde_json::{json, Value};
use space_core::{ipc, Storage};
use std::path::PathBuf;
use std::time::Duration;

#[path = "support/global_lock.rs"]
mod global_lock;

fn tmp(name: &str) -> PathBuf {
    let d = std::env::temp_dir().join("spaceos-roots").join(name);
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

async fn wait_provider(timeout: Duration) {
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        if let Ok(r) = ipc::send_request(ipc::CORE_PIPE, "list_providers", json!({})).await {
            if r["result"]
                .as_array()
                .map(|a| a.iter().any(|p| p["id"] == "filesystem"))
                .unwrap_or(false)
            {
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    panic!("filesystem provider never registered");
}

async fn action_on_pipe(action: &str, target: Value) -> Value {
    let req = json!({
        "id": uuid::Uuid::new_v4().to_string(),
        "action": action,
        "target": target,
        "provider_id": "filesystem",
        "actor": "tester",
        "requested_at": chrono::Utc::now().to_rfc3339(),
    });
    let resp = ipc::send_request(ipc::PROVIDER_PIPE, "action", req)
        .await
        .unwrap();
    resp["result"].clone()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn roots_gate_end_to_end() {
    let _ipc_lock = global_lock::acquire_global_ipc();
    let base = tmp("gate");
    let allowed = base.join("allowed");
    let outside = base.join("outside");
    std::fs::create_dir_all(&allowed).unwrap();
    std::fs::create_dir_all(&outside).unwrap();
    std::fs::write(outside.join("secret.txt"), "nope").unwrap();

    // Restrict this process's daemon before it computes roots at startup.
    std::env::set_var(
        "SPACEOS_ALLOWED_ROOTS",
        allowed.to_string_lossy().to_string(),
    );
    // Serve root inside the allowed tree so the initial scan is compliant.
    let serve_root = allowed.join("srv");
    std::fs::create_dir_all(&serve_root).unwrap();

    let db = base.join("core.db");
    let storage = Storage::open(&db).unwrap();
    let state = ipc::CoreState::new(storage);
    let _core_srv = tokio::spawn(async move {
        let _ = ipc::run_server(state, ipc::CORE_PIPE).await;
    });
    tokio::time::sleep(Duration::from_millis(400)).await;

    let prov = tokio::spawn(filesystem_provider::ipc::run_server(
        "filesystem".into(),
        Some(serve_root),
    ));
    wait_provider(Duration::from_secs(30)).await;

    // 1. Roots method reports the restriction honestly.
    let roots = ipc::send_request(ipc::PROVIDER_PIPE, "roots", json!({}))
        .await
        .unwrap();
    assert_eq!(roots["ok"], true, "roots: {roots}");
    assert_eq!(roots["result"]["mode"], "restricted", "roots: {roots}");

    // 2. Create inside roots works (direct pipe: isolates the gate).
    let inside = allowed.join("a.txt");
    let r = action_on_pipe(
        "create",
        json!({"path": inside.to_string_lossy(), "content": "hi"}),
    )
    .await;
    assert_eq!(r["status"], "Completed", "create inside: {r}");
    assert!(inside.exists());

    // 3. Create outside roots is refused loudly, nothing written.
    let evil = outside.join("evil.txt");
    let r = action_on_pipe(
        "create",
        json!({"path": evil.to_string_lossy(), "content": "x"}),
    )
    .await;
    assert_eq!(r["status"], "Failed", "create outside: {r}");
    assert!(
        r["detail"]["error"]
            .as_str()
            .unwrap_or("")
            .contains("outside allowed roots"),
        "diagnosis names the rule: {r}"
    );
    assert!(!evil.exists(), "nothing written outside roots");

    // 4. `..` escape is refused even when the lexical path starts inside.
    let esc = format!(r"{}\..\evil2.txt", allowed.to_string_lossy());
    let r = action_on_pipe("create", json!({"path": esc, "content": "x"})).await;
    assert_eq!(r["status"], "Failed", "escape: {r}");

    // 5. Read outside roots is refused; read inside works.
    let r = action_on_pipe(
        "read",
        json!({"path": outside.join("secret.txt").to_string_lossy()}),
    )
    .await;
    assert_eq!(r["status"], "Failed", "read outside: {r}");
    let r = action_on_pipe("read", json!({"path": inside.to_string_lossy()})).await;
    assert_eq!(r["status"], "Completed", "read inside: {r}");
    assert_eq!(r["detail"]["content"], "hi");

    // 6. Scan outside roots fails fast (no Started job for denied work).
    let r = action_on_pipe("scan", json!({"path": outside.to_string_lossy()})).await;
    assert_eq!(r["status"], "Failed", "scan outside: {r}");

    // 7. Optimistic concurrency: stale stamp refused, fresh stamp wins.
    let f = allowed.join("race.txt");
    let r = action_on_pipe(
        "create",
        json!({"path": f.to_string_lossy(), "content": "v1"}),
    )
    .await;
    assert_eq!(r["status"], "Completed", "create race.txt: {r}");
    let r = action_on_pipe("read", json!({"path": f.to_string_lossy()})).await;
    assert_eq!(r["status"], "Completed", "read race.txt: {r}");
    let stamp = r["detail"]["modified"].as_str().unwrap_or("").to_string();
    assert!(!stamp.is_empty(), "read returns a modified stamp");
    let size = r["detail"]["size"].as_u64().unwrap();
    // Simulate a concurrent change behind our back.
    std::fs::write(&f, "v2-sneaky").unwrap();
    let r = action_on_pipe(
        "write",
        json!({"path": f.to_string_lossy(), "content": "v3",
               "expected_modified": stamp, "expected_size": size}),
    )
    .await;
    assert_eq!(r["status"], "Failed", "stale write: {r}");
    assert!(
        r["detail"]["error"]
            .as_str()
            .unwrap_or("")
            .contains("conflict:"),
        "conflict is machine-readable: {r}"
    );
    assert_eq!(std::fs::read_to_string(&f).unwrap(), "v2-sneaky");
    // Re-read, then the write lands.
    let r = action_on_pipe("read", json!({"path": f.to_string_lossy()})).await;
    let stamp2 = r["detail"]["modified"].as_str().unwrap().to_string();
    let size2 = r["detail"]["size"].as_u64().unwrap();
    let r = action_on_pipe(
        "write",
        json!({"path": f.to_string_lossy(), "content": "v3",
               "expected_modified": stamp2, "expected_size": size2}),
    )
    .await;
    assert_eq!(r["status"], "Completed", "fresh write: {r}");
    assert_eq!(std::fs::read_to_string(&f).unwrap(), "v3");

    // 8. Same denial through the full Core flow (policy + gate composed).
    let agent = ipc::send_request(
        ipc::CORE_PIPE,
        "register_agent",
        json!({"principal": "system", "name": "roots-tester",
               "capabilities": ["action.create"], "worlds": []}),
    )
    .await
    .unwrap();
    let token = agent["result"]["token"].as_str().unwrap().to_string();
    let mut req = space_core::ActionRequest::new(
        "create",
        json!({"path": evil.to_string_lossy(), "content": "x"}),
        "filesystem",
        "roots-tester",
    );
    req.actor_token = token;
    let r = ipc::send_request(ipc::CORE_PIPE, "request_action", json!(req))
        .await
        .unwrap();
    // Policy passed (cap present) so Core executed; the provider refused.
    assert_eq!(r["result"]["status"], "Failed", "core-routed denial: {r}");
    assert!(!evil.exists());

    prov.abort();
    _core_srv.abort();
    std::env::remove_var("SPACEOS_ALLOWED_ROOTS");
}
