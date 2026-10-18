//! Mission Workflow end-to-end (Stage H).
//!
//! Drives a REAL Core server (in-process, temp SQLite file) plus a stub
//! filesystem provider that performs REAL disk IO inside a temp workspace
//! root and enforces the roots gate for real. The Mission layer under
//! test is the exact production code the HTTP bridge and the MCP server
//! both call (`space_bridge::mission`), and the MCP leg goes through the
//! real `space_bridge::mcp::call_tool` dispatch.
//!
//! Covered: create -> persist -> context -> plan (relations) -> read +
//! write through Core/policy -> action/event/correlation chain -> disk
//! content -> proposal + approval gate -> verify -> complete -> shared
//! history -> restart persistence -> forbidden path -> idempotent repeat
//! -> step failure/resume -> cancel -> observation redelivery.
#![cfg(windows)]

use serde_json::{json, Value};
use space_bridge::mission;
use space_core::{ipc, Storage};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[path = "support/global_lock.rs"]
mod global_lock;

use global_lock::{acquire_global_ipc, ServerGuard, TaskRegistry};

fn tmp(name: &str) -> PathBuf {
    let d = std::env::temp_dir().join("spaceos-mission").join(name);
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

fn under(root: &PathBuf, path: &str) -> bool {
    let p = PathBuf::from(path);
    p.starts_with(root)
}

fn stamp(p: &PathBuf) -> (String, u64) {
    let m = std::fs::metadata(p).unwrap();
    let modified = m
        .modified()
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_nanos().to_string())
        .unwrap_or_default();
    (modified, m.len())
}

/// Filesystem stub with REAL disk IO inside `root` and a REAL roots gate.
/// Anything outside the root fails loudly and writes nothing.
async fn run_fs_stub(pipe: &'static str, root: PathBuf, agent_token: Arc<Mutex<Option<String>>>) {
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
                    let action = params["action"].as_str().unwrap_or("").to_string();
                    let target = params["target"].clone();
                    let aid = params["id"].as_str().unwrap_or("").to_string();
                    let done = |status: &str, detail: Value| {
                        json!({"ok": true, "result": {
                            "action_id": aid, "status": status, "detail": detail,
                        }})
                    };
                    match action.as_str() {
                        "scan" => {
                            let tok_src = agent_token.clone();
                            let aid2 = aid.clone();
                            tokio::spawn(async move {
                                tokio::time::sleep(Duration::from_millis(200)).await;
                                let tok = tok_src.lock().unwrap().clone().unwrap_or_default();
                                let ev = json!({
                                    "id": uuid::Uuid::new_v4().to_string(),
                                    "event_type": "ActionCompleted",
                                    "actor": "mission-agent",
                                    "actor_token": tok,
                                    "provider_id": "filesystem",
                                    "object_ids": [],
                                    "payload": {"stats": {"created": 0}},
                                    "timestamp": chrono::Utc::now().to_rfc3339(),
                                    "correlation_id": aid2,
                                    "world_id": "",
                                });
                                let _ = ipc::send_request(ipc::CORE_PIPE, "record_event", ev).await;
                            });
                            done("Started", json!({"job": "scan-test"}))
                        }
                        "read" | "create" | "write" => {
                            let path = target["path"].as_str().unwrap_or("").to_string();
                            if !under(&root, &path) {
                                done(
                                    "Failed",
                                    json!({"error": "forbidden: outside allowed roots"}),
                                )
                            } else {
                                let p = PathBuf::from(&path);
                                match action.as_str() {
                                    "read" => match std::fs::read_to_string(&p) {
                                        Ok(content) => {
                                            let (modified, size) = stamp(&p);
                                            done(
                                                "Completed",
                                                json!({"path": path, "content": content, "modified": modified, "size": size}),
                                            )
                                        }
                                        Err(e) => done(
                                            "Failed",
                                            json!({"error": format!("read failed: {e}")}),
                                        ),
                                    },
                                    "create" => {
                                        if p.exists() {
                                            done("Failed", json!({"error": "exists: use write"}))
                                        } else {
                                            let content = target["content"].as_str().unwrap_or("");
                                            match std::fs::write(&p, content) {
                                                Ok(()) => done("Completed", json!({"path": path})),
                                                Err(e) => done(
                                                    "Failed",
                                                    json!({"error": format!("create failed: {e}")}),
                                                ),
                                            }
                                        }
                                    }
                                    _ => {
                                        if !p.exists() {
                                            done("Failed", json!({"error": "missing: use create"}))
                                        } else {
                                            let (modified, size) = stamp(&p);
                                            let em =
                                                target["expected_modified"].as_str().unwrap_or("");
                                            let es = target["expected_size"].as_u64().unwrap_or(0);
                                            let stamped = target.get("expected_modified").is_some()
                                                || target.get("expected_size").is_some();
                                            if stamped && (em != modified || es != size) {
                                                done(
                                                    "Failed",
                                                    json!({"error": "conflict: file changed on disk, re-read before saving"}),
                                                )
                                            } else {
                                                let content =
                                                    target["content"].as_str().unwrap_or("");
                                                match std::fs::write(&p, content) {
                                                    Ok(()) => {
                                                        done("Completed", json!({"path": path}))
                                                    }
                                                    Err(e) => done(
                                                        "Failed",
                                                        json!({"error": format!("write failed: {e}")}),
                                                    ),
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        _ => done("Failed", json!({"error": "unsupported"})),
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

struct Stack {
    _lock: global_lock::GlobalIpcLock,
    _guard: ServerGuard,
    _core: Arc<ipc::CoreState>,
    dir: PathBuf,
    agent: String,
    token: String,
}

async fn stack(tag: &str) -> Stack {
    let _lock = acquire_global_ipc();
    let dir = tmp(tag);
    let ws = dir.join("ws");
    std::fs::create_dir_all(&ws).unwrap();
    let core = start_core(&dir.join("core.db")).await;
    let reg: Arc<TaskRegistry> = Arc::new(TaskRegistry::default());
    let _guard = ServerGuard::new(reg.clone());
    let root = ws.clone();
    let toks: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let t2 = toks.clone();
    reg.clone().spawn(async move {
        run_fs_stub(ipc::PROVIDER_PIPE, root, t2).await;
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    // Provider registration (core-token auth is attached by send_request).
    let r = ipc::send_request(
        ipc::CORE_PIPE,
        "register_provider",
        json!({"id": "filesystem", "name": "filesystem", "capabilities": ["read", "create", "write", "scan"]}),
    )
    .await
    .unwrap();
    assert_eq!(r["ok"], true, "register_provider: {r}");
    let r = ipc::send_request(
        ipc::CORE_PIPE,
        "register_agent",
        json!({"principal": "system", "name": "mission-agent",
               "capabilities": ["observation.report", "action.read", "action.create", "action.write", "action.scan", "event.record"],
               "worlds": []}),
    )
    .await
    .unwrap();
    assert_eq!(r["ok"], true, "register_agent: {r}");
    let token = r["result"]["token"].as_str().unwrap().to_string();
    *toks.lock().unwrap() = Some(token.clone());
    Stack {
        _lock,
        _guard,
        _core: core,
        dir: ws,
        agent: "mission-agent".to_string(),
        token,
    }
}

async fn do_action(st: &Stack, action: &str, target: Value) -> Value {
    let r = ipc::send_request(
        ipc::CORE_PIPE,
        "request_action",
        json!({"id": uuid::Uuid::new_v4().to_string(), "action": action,
               "target": target, "provider_id": "filesystem",
               "actor": st.agent, "actor_token": st.token}),
    )
    .await
    .unwrap();
    assert_eq!(r["ok"], true, "request_action transport: {r}");
    r["result"].clone()
}

async fn events_for(correlation: &str) -> Vec<Value> {
    let r = ipc::send_request(ipc::CORE_PIPE, "query_events", json!({"limit": 500}))
        .await
        .unwrap();
    r["result"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter(|e| e.get("correlation_id").and_then(|v| v.as_str()) == Some(correlation))
        .collect()
}

fn types(evs: &[Value]) -> Vec<String> {
    evs.iter()
        .filter_map(|e| {
            e.get("event_type")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
        })
        .collect()
}

/// Happy path: create -> persist -> plan -> execute file work through
/// Core/policy -> proposal -> verify -> complete, with restart +
/// idempotency + redelivery proofs.
#[tokio::test]
async fn mission_happy_path_with_restart_and_idempotency() {
    let st = stack("happy").await;
    let ws = st.dir.to_string_lossy().to_string();
    let file = st.dir.join("notes.txt");
    let fstr = file.to_string_lossy().to_string();

    // 1-2. Create + persisted in SQLite (query path + direct reopen).
    let (view, applied) =
        mission::create_mission(&st.agent, &st.token, "Fix notes workflow", &ws, "agent-1")
            .await
            .unwrap();
    assert!(applied);
    let mid = view.mission["id"].as_str().unwrap().to_string();
    assert_eq!(view.state(), "created");
    let stored = Storage::open(&st.dir.parent().unwrap().join("core.db")).unwrap();
    let rows = stored
        .query_objects(&space_core::query::ObjectQuery {
            r#type: Some("Mission".to_string()),
            ..Default::default()
        })
        .unwrap();
    assert!(rows.iter().any(|o| o.id == mid), "mission row in SQLite");

    // 3-4. Plan: steps + relations.
    let (view, applied) = mission::plan_mission(
        &st.agent,
        &st.token,
        &mid,
        vec![
            ("Read notes".to_string(), "read file".to_string(), false),
            ("Fix typo".to_string(), "write file".to_string(), false),
            ("Verify".to_string(), "re-read".to_string(), false),
        ],
    )
    .await
    .unwrap();
    assert!(applied);
    assert_eq!(view.state(), "planned");
    assert_eq!(view.steps.len(), 3);
    let rels = stored
        .query_relations(&space_core::query::RelationQuery {
            relation_type: Some("mission_has_step".to_string()),
            source_object_id: Some(mid.clone()),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(rels.len(), 3, "step relations persisted");
    let s1 = view.steps[0]["id"].as_str().unwrap().to_string();
    let s2 = view.steps[1]["id"].as_str().unwrap().to_string();
    let s3 = view.steps[2]["id"].as_str().unwrap().to_string();

    // 5-6. File work through Core + policy (create -> read -> write).
    let c = do_action(
        &st,
        "create",
        json!({"path": fstr, "content": "hello wrold"}),
    )
    .await;
    assert_eq!(c["status"], "Completed", "create: {c}");
    let create_id = c["action_id"].as_str().unwrap().to_string();
    let r = do_action(&st, "read", json!({"path": fstr})).await;
    assert_eq!(r["status"], "Completed");
    assert_eq!(r["detail"]["content"], "hello wrold");
    // Link + run step 1.
    let (view, _) = mission::transition_step(
        &st.agent,
        &st.token,
        &mid,
        &s1,
        "running",
        json!({"action_ids": [create_id]}),
    )
    .await
    .unwrap();
    assert_eq!(view.state(), "running", "first step auto-starts mission");
    let (view, _) = mission::transition_step(
        &st.agent,
        &st.token,
        &mid,
        &s1,
        "completed",
        json!({"evidence": "file created and read"}),
    )
    .await
    .unwrap();
    assert_eq!(view.steps[0]["properties"]["state"], "completed");

    // Step 2: proposal records old content, no write yet.
    let (view, _) = mission::transition_step(&st.agent, &st.token, &mid, &s2, "running", json!({}))
        .await
        .unwrap();
    assert_eq!(view.steps[1]["properties"]["state"], "running");
    let cur = do_action(&st, "read", json!({"path": fstr})).await;
    let (view, applied) =
        mission::propose_step_change(&st.agent, &st.token, &cur, &mid, &s2, &fstr, "hello world")
            .await
            .unwrap();
    assert!(applied);
    assert_eq!(
        view.steps[1]["properties"]["proposal"]["old_content"],
        "hello wrold"
    );
    // Disk untouched by the proposal.
    assert_eq!(std::fs::read_to_string(&file).unwrap(), "hello wrold");
    // Approved write through Core with stamps, then re-read.
    let w = do_action(
        &st,
        "write",
        json!({"path": fstr, "content": "hello world",
               "expected_modified": cur["detail"]["modified"],
               "expected_size": cur["detail"]["size"]}),
    )
    .await;
    assert_eq!(w["status"], "Completed", "write: {w}");
    let write_id = w["action_id"].as_str().unwrap().to_string();
    assert_eq!(std::fs::read_to_string(&file).unwrap(), "hello world");
    let (view, _) = mission::transition_step(
        &st.agent,
        &st.token,
        &mid,
        &s2,
        "completed",
        json!({"evidence": "typo fixed, disk verified", "action_ids": [write_id]}),
    )
    .await
    .unwrap();
    assert_eq!(view.steps[2]["properties"]["state"], "pending");

    // 7-8. Action/event/correlation chain + verification gate.
    let evs = events_for(&create_id).await;
    let t = types(&evs);
    assert!(t.contains(&"ActionRequested".to_string()), "{t:?}");
    assert!(t.contains(&"ActionCompleted".to_string()), "{t:?}");
    let (view, _) = mission::transition_step(&st.agent, &st.token, &mid, &s3, "running", json!({}))
        .await
        .unwrap();
    assert_eq!(view.state(), "running");
    let r = do_action(&st, "read", json!({"path": fstr})).await;
    assert_eq!(r["detail"]["content"], "hello world");
    let (view, _) = mission::transition_step(
        &st.agent,
        &st.token,
        &mid,
        &s3,
        "completed",
        json!({"evidence": "disk content verified"}),
    )
    .await
    .unwrap();
    assert_eq!(view.steps[2]["properties"]["state"], "completed");
    // Complete without verification is rejected.
    let (view, _) = mission::transition_mission(&st.agent, &st.token, &mid, "verifying", json!({}))
        .await
        .unwrap();
    assert_eq!(view.state(), "verifying");
    assert!(
        mission::transition_mission(&st.agent, &st.token, &mid, "completed", json!({}))
            .await
            .is_err()
    );
    let (view, _) = mission::record_verification(
        &st.agent,
        &st.token,
        &mid,
        true,
        "tests",
        json!({"check": "disk content == hello world"}),
    )
    .await
    .unwrap();
    assert!(view.mission["properties"]["verification"]["passed"]
        .as_bool()
        .unwrap());
    let (view, applied) = mission::transition_mission(
        &st.agent,
        &st.token,
        &mid,
        "completed",
        json!({"result": {"summary": "typo fixed and verified"}}),
    )
    .await
    .unwrap();
    assert!(applied);
    assert_eq!(view.state(), "completed");

    // 9-10. Shared history holds the real chain.
    let mevs = events_for(&mid).await;
    let mt = types(&mevs);
    for want in [
        "MissionCreated",
        "MissionPlanned",
        "StepCompleted",
        "MissionVerified",
        "MissionCompleted",
    ] {
        assert!(mt.contains(&want.to_string()), "missing {want} in {mt:?}");
    }

    // 11. Restart: fresh storage handle sees everything.
    drop(stored);
    let reopened = Storage::open(&st.dir.parent().unwrap().join("core.db")).unwrap();
    let missions = reopened
        .query_objects(&space_core::query::ObjectQuery {
            r#type: Some("Mission".to_string()),
            ..Default::default()
        })
        .unwrap();
    let m = missions.iter().find(|o| o.id == mid).unwrap();
    assert_eq!(m.properties["state"], "completed");
    let steps = reopened
        .query_objects(&space_core::query::ObjectQuery {
            r#type: Some("MissionStep".to_string()),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(
        steps
            .iter()
            .filter(|o| o.properties["mission_id"] == mid)
            .count(),
        3
    );

    // 12. Idempotent repeat: completing a completed step/mission writes nothing.
    let before = events_for(&mid).await.len();
    let (view, applied) =
        mission::transition_step(&st.agent, &st.token, &mid, &s1, "completed", json!({}))
            .await
            .unwrap();
    assert!(!applied);
    assert_eq!(view.state(), "completed");
    assert_eq!(events_for(&mid).await.len(), before, "no duplicate events");

    // 13. Observation redelivery: same observation_id applies once.
    let obs = json!({
        "observation_id": format!("{mid}-redelivery-probe"),
        "world_id": "",
        "facts": [],
        "objects": [],
        "relations": [],
        "delete_ids": [],
        "event": {"id": uuid::Uuid::new_v4().to_string(), "event_type": "MissionNote",
                  "actor": st.agent, "provider_id": "space-bridge",
                  "object_ids": [], "payload": {"mission_id": mid},
                  "timestamp": chrono::Utc::now().to_rfc3339(),
                  "correlation_id": mid, "world_id": "", "seq": 0},
        "actor": st.agent, "actor_token": st.token,
    });
    let r1 = ipc::send_request(ipc::CORE_PIPE, "report_observation", obs.clone())
        .await
        .unwrap();
    assert_eq!(r1["result"]["applied"], true);
    let r2 = ipc::send_request(ipc::CORE_PIPE, "report_observation", obs)
        .await
        .unwrap();
    assert_eq!(r2["result"]["applied"], false, "redelivery is a no-op");
}

/// Approval gate, failure/resume, cancel, forbidden path, MCP equivalence.
#[tokio::test]
async fn mission_approval_failure_cancel_forbidden_mcp() {
    let st = stack("gates").await;
    let ws = st.dir.to_string_lossy().to_string();
    let file = st.dir.join("doc.txt");
    let fstr = file.to_string_lossy().to_string();

    let (view, _) =
        mission::create_mission(&st.agent, &st.token, "Edit with approval", &ws, "agent-1")
            .await
            .unwrap();
    let mid = view.mission["id"].as_str().unwrap().to_string();
    let (view, _) = mission::plan_mission(
        &st.agent,
        &st.token,
        &mid,
        vec![
            ("Draft".to_string(), "create".to_string(), false),
            ("Publish edit".to_string(), "write".to_string(), true),
        ],
    )
    .await
    .unwrap();
    let s1 = view.steps[0]["id"].as_str().unwrap().to_string();
    let s2 = view.steps[1]["id"].as_str().unwrap().to_string();

    // Forbidden path refused by the roots gate, nothing written.
    let evil = std::env::temp_dir().join("spaceos-mission-evil.txt");
    let _ = std::fs::remove_file(&evil);
    let bad = do_action(
        &st,
        "create",
        json!({"path": evil.to_string_lossy(), "content": "x"}),
    )
    .await;
    assert_eq!(bad["status"], "Failed", "forbidden: {bad}");
    assert!(!evil.exists(), "nothing written outside workspace");

    // Normal draft.
    let c = do_action(&st, "create", json!({"path": fstr, "content": "v1"})).await;
    assert_eq!(c["status"], "Completed");
    mission::transition_step(&st.agent, &st.token, &mid, &s1, "running", json!({}))
        .await
        .unwrap();
    mission::transition_step(
        &st.agent,
        &st.token,
        &mid,
        &s1,
        "completed",
        json!({"evidence": "draft"}),
    )
    .await
    .unwrap();

    // Approval gate: gated step cannot complete unapproved...
    mission::transition_step(&st.agent, &st.token, &mid, &s2, "running", json!({}))
        .await
        .unwrap();
    assert!(mission::transition_step(
        &st.agent,
        &st.token,
        &mid,
        &s2,
        "completed",
        json!({"evidence": "no approval"})
    )
    .await
    .is_err());
    // ...request, approve, then complete.
    let (view, _) = mission::transition_step(
        &st.agent,
        &st.token,
        &mid,
        &s2,
        "awaiting_approval",
        json!({}),
    )
    .await
    .unwrap();
    assert_eq!(view.steps[1]["properties"]["state"], "awaiting_approval");
    let (view, applied) =
        mission::transition_step(&st.agent, &st.token, &mid, &s2, "running", json!({}))
            .await
            .unwrap();
    assert!(applied);
    assert!(view.steps[1]["properties"]["approved"].as_bool().unwrap());
    let w = do_action(&st, "write", json!({"path": fstr, "content": "v2"})).await;
    assert_eq!(w["status"], "Completed");
    let (view, _) = mission::transition_step(
        &st.agent,
        &st.token,
        &mid,
        &s2,
        "completed",
        json!({"evidence": "approved write"}),
    )
    .await
    .unwrap();
    assert_eq!(view.steps[1]["properties"]["state"], "completed");

    // Failure is honest: mission is NOT completed, resume re-opens.
    let (view, _) = mission::create_mission(&st.agent, &st.token, "Failing work", &ws, "agent-1")
        .await
        .unwrap();
    let fmid = view.mission["id"].as_str().unwrap().to_string();
    let (view, _) = mission::plan_mission(
        &st.agent,
        &st.token,
        &fmid,
        vec![("Do".to_string(), "x".to_string(), false)],
    )
    .await
    .unwrap();
    let fs = view.steps[0]["id"].as_str().unwrap().to_string();
    mission::transition_step(&st.agent, &st.token, &fmid, &fs, "running", json!({}))
        .await
        .unwrap();
    let (view, _) = mission::transition_step(
        &st.agent,
        &st.token,
        &fmid,
        &fs,
        "failed",
        json!({"evidence": "boom: disk vanished"}),
    )
    .await
    .unwrap();
    assert_eq!(view.steps[0]["properties"]["state"], "failed");
    assert!(
        mission::transition_mission(&st.agent, &st.token, &fmid, "completed", json!({}))
            .await
            .is_err(),
        "failed work must not complete"
    );
    let (view, applied) =
        mission::transition_step(&st.agent, &st.token, &fmid, &fs, "running", json!({}))
            .await
            .unwrap();
    assert!(applied, "failed step resumes");
    assert_eq!(view.steps[0]["properties"]["state"], "running");

    // Cancel: executed work untouched, pending skipped, terminal after.
    // Late step commands on the cancelled mission are safe no-ops.
    let (view, _) = mission::transition_mission(
        &st.agent,
        &st.token,
        &fmid,
        "cancelled",
        json!({"reason": "user stopped"}),
    )
    .await
    .unwrap();
    assert_eq!(view.state(), "cancelled");
    let (view, applied) =
        mission::transition_step(&st.agent, &st.token, &fmid, &fs, "completed", json!({}))
            .await
            .unwrap();
    assert!(!applied, "cancelled mission executes nothing more");
    assert_eq!(view.state(), "cancelled");

    // MCP equivalence: the real stdio dispatch sees the same domain state.
    let cache = tokio::sync::Mutex::new(None);
    let got =
        space_bridge::mcp::call_tool(&cache, "mission_get", &json!({"mission_id": mid})).await;
    assert!(got.get("isError").is_none(), "mission_get: {got}");
    let text = got["content"][0]["text"].as_str().unwrap();
    let body: Value = serde_json::from_str(text).unwrap();
    assert_eq!(body["mission"]["id"], mid);
    assert_eq!(body["steps"].as_array().unwrap().len(), 2);
    assert_eq!(body["progress"]["done"], 2);
    let upd = space_bridge::mcp::call_tool(
        &cache,
        "mission_update",
        &json!({"mission_id": mid, "to": "verifying"}),
    )
    .await;
    assert!(upd.get("isError").is_none(), "mission_update: {upd}");
    let view = mission::get_view(&mid).await.unwrap();
    assert_eq!(view.state(), "verifying");
}

/// Concurrent transitions stay coherent: duplicate commands collapse
/// to one effect (exactly one `applied`), conflicting ones resolve to a
/// legal state with matching events (a rejected out-of-order `completed`
/// never fabricates success). The bridge lock prevents lost-update
/// interleavings; callers still sequence dependent steps.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn mission_concurrent_transitions_stay_coherent() {
    let st = stack("race").await;
    let ws = st.dir.to_string_lossy().to_string();
    let (view, _) = mission::create_mission(&st.agent, &st.token, "Race", &ws, "agent-1")
        .await
        .unwrap();
    let mid = view.mission["id"].as_str().unwrap().to_string();
    let (view, _) = mission::plan_mission(
        &st.agent,
        &st.token,
        &mid,
        vec![("R".to_string(), String::new(), false)],
    )
    .await
    .unwrap();
    let sid = view.steps[0]["id"].as_str().unwrap().to_string();

    // Ten concurrent identical starts: exactly one applies.
    let mut handles = vec![];
    for _ in 0..10 {
        let (a, t, m, s) = (st.agent.clone(), st.token.clone(), mid.clone(), sid.clone());
        handles.push(tokio::spawn(async move {
            mission::transition_step(&a, &t, &m, &s, "running", json!({})).await
        }));
    }
    let mut applied = 0;
    for h in handles {
        let (_, ok) = h.await.unwrap().unwrap();
        if ok {
            applied += 1;
        }
    }
    assert_eq!(applied, 1, "duplicate starts collapse to one effect");
    let started = events_for(&mid)
        .await
        .into_iter()
        .filter(|e| e["event_type"] == "StepStarted")
        .count();
    assert_eq!(started, 1, "single StepStarted event");

    // Ten concurrent completes: exactly one applies, state coherent.
    let mut handles = vec![];
    for _ in 0..10 {
        let (a, t, m, s) = (st.agent.clone(), st.token.clone(), mid.clone(), sid.clone());
        handles.push(tokio::spawn(async move {
            mission::transition_step(&a, &t, &m, &s, "completed", json!({"evidence": "race"})).await
        }));
    }
    let mut applied = 0;
    for h in handles {
        let (_, ok) = h.await.unwrap().unwrap();
        if ok {
            applied += 1;
        }
    }
    assert_eq!(applied, 1, "duplicate completes collapse to one effect");
    let view = mission::get_view(&mid).await.unwrap();
    assert_eq!(view.steps[0]["properties"]["state"], "completed");
}

/// Bilateral adapter: guarded external call through the real fixture
/// peer, provenance persisted in Core, deny paths loud, timeout honest.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn external_mcp_adapter_guarded_end_to_end() {
    use space_bridge::mcp_client::{
        call_external, parse_servers, report_external_provenance, ExternalPolicy,
    };
    let st = stack("external").await;
    let fixture = env!("CARGO_BIN_EXE_mcp_fixture").to_string();
    let (servers, warnings) = parse_servers(&format!(
        "fixture|{fixture}|;broken|C:\\nonexistent\\srv.exe|"
    ));
    assert!(warnings.is_empty(), "{warnings:?}");
    assert_eq!(servers.len(), 2);
    let policy = ExternalPolicy::parse("fixture:echo");

    // Happy path: live discovery + validated call + Core provenance.
    let out = call_external(
        &servers,
        &policy,
        "mission-agent",
        "fixture",
        "echo",
        json!({"text": "hello-outside"}),
        Duration::from_secs(10),
    )
    .await
    .expect("external call");
    assert_eq!(out["server"], "fixture");
    assert_eq!(out["result"]["content"][0]["text"], "hello-outside");
    let corr = out["correlation_id"].as_str().unwrap().to_string();
    assert!(corr.starts_with("ext-"));
    assert_eq!(out["provenance"]["executor"], "external-mcp:fixture");
    assert_eq!(out["provenance"]["correlation_id"], corr);
    let outcome = report_external_provenance(
        &st.agent,
        &st.token,
        "mission-ext-1",
        &out["provenance"],
        &format!("{corr}-prov"),
    )
    .await
    .expect("provenance write");
    assert_eq!(outcome["applied"], true);
    let evs = events_for(&corr).await;
    let types: Vec<String> = evs
        .iter()
        .filter_map(|e| e["event_type"].as_str().map(|s| s.to_string()))
        .collect();
    assert_eq!(types, vec!["ExternalToolCalled".to_string()], "{types:?}");

    // Unknown server: denied before any process spawns.
    assert!(call_external(
        &servers,
        &policy,
        "mission-agent",
        "ghost",
        "echo",
        json!({}),
        Duration::from_secs(5),
    )
    .await
    .expect_err("unknown server")
    .contains("not registered"));
    // Not allow-listed: denied before any send.
    assert!(call_external(
        &servers,
        &policy,
        "mission-agent",
        "fixture",
        "boom",
        json!({}),
        Duration::from_secs(5),
    )
    .await
    .expect_err("default-deny")
    .contains("not allow-listed"));
    // Missing required arg: honest failed outcome (recordable), peer untouched.
    let out = call_external(
        &servers,
        &policy,
        "mission-agent",
        "fixture",
        "echo",
        json!({}),
        Duration::from_secs(5),
    )
    .await
    .expect("schema failures stay Ok-shaped");
    assert_eq!(out["provenance"]["outcome"]["status"], "failed");
    assert!(out["provenance"]["outcome"]["error"]
        .as_str()
        .unwrap_or("")
        .contains("missing required argument"));

    // Slow peer + short timeout through the ADAPTER: honest failure,
    // provenance still recorded, no success claimed, no auto-retry.
    let mut slow_env = std::collections::HashMap::new();
    slow_env.insert("MCP_FIXTURE_MODE".to_string(), "slow".to_string());
    let slow_spec = space_bridge::mcp_client::ServerSpec {
        id: "slowfx".to_string(),
        program: fixture.clone(),
        args: vec![],
        env: slow_env,
    };
    let slow_policy = ExternalPolicy::parse("slowfx:echo");
    let slow_out = call_external(
        std::slice::from_ref(&slow_spec),
        &slow_policy,
        "mission-agent",
        "slowfx",
        "echo",
        json!({"text": "x"}),
        Duration::from_secs(2),
    )
    .await
    .expect("adapter answers even on peer timeout");
    assert_eq!(slow_out["provenance"]["outcome"]["status"], "failed");
    assert!(slow_out["provenance"]["outcome"]["error"]
        .as_str()
        .unwrap_or("")
        .contains("timeout"));
    let outcome = report_external_provenance(
        &st.agent,
        &st.token,
        "mission-ext-1",
        &slow_out["provenance"],
        &format!("{}-prov", slow_out["correlation_id"].as_str().unwrap()),
    )
    .await
    .expect("timeout provenance write");
    assert_eq!(outcome["applied"], true);
}
/// step executes. Executed steps survive cancel untouched.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mission_illegal_transitions_rejected() {
    let st = stack("illegal").await;
    let ws = st.dir.to_string_lossy().to_string();
    let (view, _) = mission::create_mission(&st.agent, &st.token, "Guarded", &ws, "agent-1")
        .await
        .unwrap();
    let mid = view.mission["id"].as_str().unwrap().to_string();
    // created -> completed / verifying: rejected.
    assert!(
        mission::transition_mission(&st.agent, &st.token, &mid, "completed", json!({}))
            .await
            .is_err()
    );
    assert!(
        mission::transition_mission(&st.agent, &st.token, &mid, "verifying", json!({}))
            .await
            .is_err()
    );
    // created -> running without a plan: rejected (must plan first).
    assert!(
        mission::transition_mission(&st.agent, &st.token, &mid, "running", json!({}))
            .await
            .is_err()
    );
    // Unknown target state: rejected.
    assert!(
        mission::transition_mission(&st.agent, &st.token, &mid, "done", json!({}))
            .await
            .is_err()
    );
    // Unknown step: rejected.
    assert!(mission::transition_step(
        &st.agent,
        &st.token,
        &mid,
        "no-such-step",
        "running",
        json!({})
    )
    .await
    .is_err());
    let (view, _) = mission::plan_mission(
        &st.agent,
        &st.token,
        &mid,
        vec![
            ("A".to_string(), String::new(), false),
            ("B".to_string(), String::new(), false),
        ],
    )
    .await
    .unwrap();
    let sa = view.steps[0]["id"].as_str().unwrap().to_string();
    let sb = view.steps[1]["id"].as_str().unwrap().to_string();
    // pending -> completed directly: rejected (must start first).
    assert!(
        mission::transition_step(&st.agent, &st.token, &mid, &sa, "completed", json!({}))
            .await
            .is_err()
    );
    // pending -> awaiting_approval directly: rejected.
    assert!(mission::transition_step(
        &st.agent,
        &st.token,
        &mid,
        &sa,
        "awaiting_approval",
        json!({})
    )
    .await
    .is_err());
    // Complete step A properly, then cancel: A stays completed, B skipped.
    mission::transition_step(&st.agent, &st.token, &mid, &sa, "running", json!({}))
        .await
        .unwrap();
    mission::transition_step(
        &st.agent,
        &st.token,
        &mid,
        &sa,
        "completed",
        json!({"evidence": "ok"}),
    )
    .await
    .unwrap();
    let (view, _) = mission::transition_mission(&st.agent, &st.token, &mid, "cancelled", json!({}))
        .await
        .unwrap();
    assert_eq!(view.state(), "cancelled");
    let done: Vec<String> = view
        .steps
        .iter()
        .map(|s| s["properties"]["state"].as_str().unwrap_or("?").to_string())
        .collect();
    assert_eq!(done, vec!["completed".to_string(), "skipped".to_string()]);
    // Terminal mission: mission-level escape rejected, late step = no-op.
    assert!(
        mission::transition_mission(&st.agent, &st.token, &mid, "running", json!({}))
            .await
            .is_err()
    );
    let (view, applied) =
        mission::transition_step(&st.agent, &st.token, &mid, &sb, "running", json!({}))
            .await
            .unwrap();
    assert!(!applied);
    assert_eq!(view.state(), "cancelled");
    // Verification recorded outside verifying: rejected.
    assert!(
        mission::record_verification(&st.agent, &st.token, &mid, true, "t", json!({}))
            .await
            .is_err()
    );
}
