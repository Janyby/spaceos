//! V1.2 вЂ” capability/policy enforcement tests via real IPC.
use serde_json::json;
use space_core::{ipc, Event, Object, Storage};
use std::path::PathBuf;

fn tmp(name: &str) -> PathBuf {
    let d = std::env::temp_dir().join("spaceos-v12");
    std::fs::create_dir_all(&d).unwrap();
    d.join(format!("{name}.db"))
}

async fn start_core(pipe: &str) -> String {
    let db = tmp(&pipe.replace('\\', "_").replace(':', "_"));
    let _ = std::fs::remove_file(&db);
    let storage = Storage::open(&db).unwrap();
    let state = ipc::CoreState::new(storage);
    let s2 = state.clone();
    let p = pipe.to_string();
    tokio::spawn(async move {
        let _ = ipc::run_server(s2, &p).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    pipe.to_string()
}

async fn call(pipe: &str, method: &str, params: serde_json::Value) -> serde_json::Value {
    ipc::send_request(pipe, method, params).await.unwrap()
}

async fn call_retry(pipe: &str, method: &str, params: serde_json::Value) -> serde_json::Value {
    for _ in 0..50 {
        match ipc::send_request(pipe, method, params.clone()).await {
            Ok(v) => return v,
            Err(_) => tokio::time::sleep(std::time::Duration::from_millis(50)).await,
        }
    }
    call(pipe, method, params).await
}

async fn register(pipe: &str, name: &str, caps: &[&str], worlds: &[&str]) -> String {
    let r = call(
        pipe,
        "register_agent",
        json!({"principal":"system","name":name,
            "capabilities": caps, "worlds": worlds}),
    )
    .await;
    assert_eq!(r["ok"], true);
    r["result"]["token"].as_str().unwrap().to_string()
}

fn with_token(mut params: serde_json::Value, token: &str) -> serde_json::Value {
    params["actor_token"] = json!(token);
    params
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn capability_allow_creates_object() {
    let pipe = start_core(r"\\.\pipe\spaceos-v12-allow").await;
    let tok = register(&pipe, "agent-x", &["object.create", "event.record"], &[]).await;
    let o = Object::new("Project", "agent-x", json!({"name":"P"}));
    let r = call(&pipe, "create_object", with_token(json!(o), &tok)).await;
    assert_eq!(r["ok"], true);
    // agent records its own lifecycle events; core does not auto-create from create_object
    let ev = Event::new("ObjectCreated", "agent-x", json!({}));
    call(&pipe, "record_event", with_token(json!(ev), &tok)).await;
    let ev = call(&pipe, "query_events", json!({})).await;
    assert!(ev["result"].as_array().unwrap().len() >= 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn capability_deny_blocks_create_object() {
    let pipe = start_core(r"\\.\pipe\spaceos-v12-deny").await;
    // agent-y has no capabilities registered
    let o = Object::new("Project", "agent-y", json!({"name":"P"}));
    let r = call(&pipe, "create_object", json!(o)).await;
    assert_eq!(r["ok"], false);
    assert!(r["error"].as_str().unwrap().contains("denied"));
    let evs = call(
        &pipe,
        "query_events",
        json!({"event_type":"MutationDenied"}),
    )
    .await;
    assert!(evs["result"].as_array().unwrap().len() >= 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn self_grant_denied() {
    let pipe = start_core(r"\\.\pipe\spaceos-v12-selfgrant").await;
    // agent-z must be able to create its Agent object
    let tok = register(&pipe, "agent-z", &["object.update", "object.create"], &[]).await;
    let agent = Object::new(
        "Agent",
        "agent-z",
        json!({"name":"A","capabilities":["object.create"]}),
    );
    let aid = agent.id.clone();
    let c = call(&pipe, "create_object", with_token(json!(agent), &tok)).await;
    assert_eq!(c["ok"], true);
    let victim = Object {
        id: aid.clone(),
        r#type: "Agent".into(),
        properties: json!({"name":"A","capabilities":["object.create","system.admin"]}),
        provider_id: "agent-z".into(),
        external_id: None,
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    };
    let r = call(&pipe, "update_object", with_token(json!(victim), &tok)).await;
    assert_eq!(r["ok"], false);
    assert!(r["error"]
        .as_str()
        .unwrap()
        .contains("capability self-grant"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn policy_self_modification_denied() {
    let pipe = start_core(r"\\.\pipe\spaceos-v12-policymod").await;
    let r = call(&pipe, "set_policy", json!({})).await;
    assert!(r["error"].as_str().unwrap().contains("unknown method") || r["ok"] == false);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn relation_capability_gate() {
    let pipe = start_core(r"\\.\pipe\spaceos-v12-relcap").await;
    // no caps -> deny
    let rel = json!({"id": uuid::Uuid::new_v4().to_string(), "source_object_id":"a","relation_type":"x","target_object_id":"b","properties":{},"created_at": chrono::Utc::now().to_rfc3339(), "world_id":""});
    let r = call(&pipe, "create_relation", rel.clone()).await;
    assert_eq!(r["ok"], false);
    let mut rel2 = rel.clone();
    rel2["id"] = json!(uuid::Uuid::new_v4().to_string());
    rel2["actor"] = json!("ag");
    let tok = register(&pipe, "ag", &["object.create", "relation.create"], &[]).await;
    // Phase B: FK — both relation endpoints must exist first.
    for id in ["a", "b"] {
        let mut o = Object::new("Project", "ag", json!({"name": id}));
        o.id = id.into();
        let ra = call(&pipe, "create_object", with_token(json!(o), &tok)).await;
        assert_eq!(ra["ok"], true, "seed endpoint {id}: {ra}");
    }
    let r2 = call(&pipe, "create_relation", with_token(rel2, &tok)).await;
    assert_eq!(r2["ok"], true);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn event_delete_and_missing_path_denied() {
    let pipe = start_core(r"\\.\pipe\spaceos-v12-event").await;
    let r = call(&pipe, "delete_event", json!({})).await;
    assert!(r["error"].as_str().unwrap().contains("unknown method") || r["ok"] == false);
    let r2 = call(&pipe, "update_event", json!({})).await;
    assert!(r2["error"].as_str().unwrap().contains("unknown method") || r2["ok"] == false);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cross_world_event_isolation() {
    let pipe = start_core(r"\\.\pipe\spaceos-v12-cross").await;
    let tok = register(&pipe, "ag", &["event.record"], &[]).await;
    let e1 = Event::new("ObjectCreated", "ag", json!({"world_id":"A"}));
    let e2 = Event::new("ObjectCreated", "ag", json!({"world_id":"B"}));
    call(&pipe, "record_event", with_token(json!(e1), &tok)).await;
    call(&pipe, "record_event", with_token(json!(e2), &tok)).await;
    let in_a = call(&pipe, "query_events", json!({"world_id":"A"})).await;
    let in_b = call(&pipe, "query_events", json!({"world_id":"B"})).await;
    assert_eq!(in_a["result"].as_array().unwrap().len(), 1);
    assert_eq!(in_b["result"].as_array().unwrap().len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_world_writes_no_cross_corruption() {
    let pipe = start_core(r"\\.\pipe\spaceos-v12-concurrent").await;
    let tok = register(
        &pipe,
        "ag",
        &["object.create", "relation.create", "event.record"],
        &[],
    )
    .await;
    // Phase B: FK — one shared hub object supplies the fixed target
    // endpoint (keeps 1 object + 1 relation per world).
    let hub = Object::new("Hub", "ag", json!({"name": "hub"}));
    let hub_id = hub.id.clone();
    let hb = call_retry(&pipe, "create_object", with_token(json!(hub), &tok)).await;
    assert_eq!(hb["ok"], true, "seed hub: {hb}");
    let mut handles = vec![];
    for w in 0..5 {
        let p = pipe.clone();
        let tok = tok.clone();
        let hub_id = hub_id.clone();
        handles.push(tokio::spawn(async move {
            let sid = format!("W{w}");
            let o = Object::new("Project", "ag", json!({"world_id": sid, "name": format!("WF{w}")}));
            let oid = o.id.clone();
            let _ = call_retry(&p, "create_object", with_token(json!(o), &tok)).await;
            let rel = json!({"id": uuid::Uuid::new_v4().to_string(), "source_object_id": oid, "relation_type": "r", "target_object_id": hub_id, "properties":{},"created_at": chrono::Utc::now().to_rfc3339(), "world_id": format!("W{w}"), "actor": "ag", "actor_token": tok});
            let _ = call_retry(&p, "create_relation", rel).await;
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
    for w in 0..5 {
        let got = call(&pipe, "query_objects", json!({"world_id": format!("W{w}")})).await;
        assert_eq!(got["result"].as_array().unwrap().len(), 1);
        let rels = call(
            &pipe,
            "query_relations",
            json!({"world_id": format!("W{w}")}),
        )
        .await;
        assert_eq!(rels["result"].as_array().unwrap().len(), 1);
    }
}
