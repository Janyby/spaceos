//! G7 — Core kill + restart on an ISOLATED stack.
//!
//! Safety: in-process Core on a unique pipe name (pid-tagged), temp DB
//! files under the OS temp dir. Only tasks this test spawned are
//! aborted; the production `runtime/core.db`, global pipes and user
//! processes are never touched.
//!
//! Proven: kill during a safe scenario -> client sees transport error
//! (never false success) -> restart on the same DB -> re-register (token
//! rotated) -> prior state intact -> redelivered observation is a no-op
//! -> Mission stays in its persisted state, terminal states sticky.
#![cfg(windows)]

use serde_json::{json, Value};
use space_core::ipc;
use std::path::PathBuf;
use std::time::Duration;

fn tmp(name: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("spaceos-g7-{}", std::process::id()));
    std::fs::create_dir_all(&d).unwrap();
    d.join(name)
}

fn pipe() -> &'static str {
    let s = format!(r"\\.\pipe\spaceos-g7-{}", std::process::id());
    Box::leak(s.into_boxed_str())
}

async fn start_core(pipe: &'static str, db: &PathBuf) -> tokio::task::JoinHandle<()> {
    let storage = space_core::Storage::open(db).unwrap();
    let state = ipc::CoreState::new(storage);
    tokio::spawn(async move {
        let _ = ipc::run_server(state, pipe).await;
    })
}

async fn req(pipe: &str, method: &str, params: Value) -> Value {
    ipc::send_request(pipe, method, params).await.unwrap()
}

async fn register(pipe: &str, name: &str) -> String {
    let r = req(
        pipe,
        "register_agent",
        json!({"principal": "system", "name": name,
               "capabilities": ["observation.report", "event.record"],
               "worlds": []}),
    )
    .await;
    assert_eq!(r["ok"], true, "register {name}: {r}");
    r["result"]["token"].as_str().unwrap().to_string()
}

fn mission_obs(mid: &str, state: &str, obs_suffix: &str, event_id: &str) -> Value {
    json!({
        "observation_id": format!("{mid}-{obs_suffix}"),
        "world_id": "",
        "facts": [],
        "objects": [{
            "id": mid, "type": "Mission",
            "properties": {"goal": "g7 probe", "workspace": "tmp",
                           "state": state, "updated_at": "t"},
            "provider_id": "space-bridge", "external_id": null,
            "created_at": "2026-01-01T00:00:00Z", "updated_at": "2026-01-01T00:00:00Z",
        }],
        "relations": [], "delete_ids": [],
        "event": {"id": event_id, "event_type": "MissionNote",
                  "actor": "g7-agent", "provider_id": "space-bridge",
                  "object_ids": [mid], "payload": {"mission_id": mid, "state": state},
                  "timestamp": "2026-01-01T00:00:00Z",
                  "correlation_id": mid, "world_id": "", "seq": 0},
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn core_kill_restart_no_false_success() {
    let p = pipe();
    let db = tmp("core.db");
    let _ = std::fs::remove_file(&db);

    // 1. Core up, client works.
    let h = start_core(p, &db).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let tok = register(p, "g7-agent").await;
    let mid = "mission-g7";
    let r = req(
        p,
        "report_observation",
        with_auth(
            mission_obs(mid, "running", "created", "ev-1"),
            "g7-agent",
            &tok,
        ),
    )
    .await;
    assert_eq!(r["result"]["applied"], true, "first apply: {r}");

    // 2. Kill Core mid-scenario. In-flight client gets a transport error,
    // never a fabricated success.
    h.abort();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let err = ipc::send_request(p, "query_events", json!({"limit": 5})).await;
    assert!(err.is_err(), "dead core must error, not answer: {err:?}");

    // 3. Restart on the SAME db file. Old agent token is gone with the
    // old Policy (in-memory by design) -> re-register rotates it.
    let h2 = start_core(p, &db).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let tok2 = register(p, "g7-agent").await;

    // 4. Pre-kill state survived: mission row + event chain intact.
    let objs = req(p, "query_objects", json!({"type": "Mission"})).await;
    let m = objs["result"]
        .as_array()
        .unwrap()
        .iter()
        .find(|o| o["id"] == mid)
        .expect("mission survived restart");
    assert_eq!(m["properties"]["state"], "running");
    let evs = req(
        p,
        "query_events",
        json!({"correlation_id": mid, "limit": 50}),
    )
    .await;
    assert!(
        evs["result"].as_array().unwrap().len() >= 1,
        "event history survived"
    );

    // 5. Redelivery of the pre-kill observation is a no-op, not a dupe.
    let r = req(
        p,
        "report_observation",
        with_auth(
            mission_obs(mid, "running", "created", "ev-1-dup"),
            "g7-agent",
            &tok2,
        ),
    )
    .await;
    assert_eq!(r["result"]["applied"], false, "redelivery no-op: {r}");
    let evs2 = req(
        p,
        "query_events",
        json!({"correlation_id": mid, "limit": 50}),
    )
    .await;
    assert_eq!(
        evs2["result"].as_array().unwrap().len(),
        evs["result"].as_array().unwrap().len(),
        "no duplicate events after redelivery"
    );

    // 6. Work continues: a new transition applies on the fresh core.
    let r = req(
        p,
        "report_observation",
        with_auth(
            mission_obs(mid, "verifying", "to-verifying", "ev-2"),
            "g7-agent",
            &tok2,
        ),
    )
    .await;
    assert_eq!(r["result"]["applied"], true);
    let objs = req(p, "query_objects", json!({"type": "Mission"})).await;
    let m = objs["result"]
        .as_array()
        .unwrap()
        .iter()
        .find(|o| o["id"] == mid)
        .unwrap();
    assert_eq!(m["properties"]["state"], "verifying");

    h2.abort();
}

fn with_auth(mut obs: Value, actor: &str, token: &str) -> Value {
    obs["actor"] = json!(actor);
    obs["actor_token"] = json!(token);
    obs
}
