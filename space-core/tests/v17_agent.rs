//! V1.7 Phase 2 — Agent identity lifecycle.
//! Definition (name + caps + worlds, stable) vs Instance (token, rotated).
//! Object identity (stable id, upsert) vs runtime state (token, never stored).
#![cfg(windows)]
use serde_json::{json, Value};
use space_core::{ipc, Object, Storage};

fn tmp_db(name: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join("spaceos-v17a");
    std::fs::create_dir_all(&d).unwrap();
    d.join(format!("{name}.db"))
}

async fn call(pipe: &str, method: &str, params: Value) -> Value {
    ipc::send_request(pipe, method, params).await.unwrap()
}

async fn start_core(pipe: &str, db: &std::path::Path) -> tokio::task::JoinHandle<()> {
    let storage = Storage::open(db).unwrap();
    let state = ipc::CoreState::new(storage);
    let s2 = state.clone();
    let p = pipe.to_string();
    let h = tokio::spawn(async move {
        let _ = ipc::run_server(s2, &p).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    h
}

async fn register(pipe: &str, name: &str, caps: &[&str], worlds: &[&str]) -> Value {
    call(
        pipe,
        "register_agent",
        json!({"principal": "system", "name": name, "capabilities": caps, "worlds": worlds}),
    )
    .await
}

fn with_token(mut p: Value, tok: &str) -> Value {
    p["actor_token"] = json!(tok);
    p
}

fn tok_of(r: &Value) -> String {
    r["result"]["token"].as_str().unwrap().to_string()
}

// ---------- 2A/2B: definition stable, instance rotates ----------
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn definition_stable_instance_rotates() {
    let pipe = r"\\.\pipe\spaceos-v17a-def";
    let db = tmp_db("def.db");
    let _ = std::fs::remove_file(&db);
    let h = start_core(pipe, &db).await;
    let r1 = register(pipe, "worker", &["object.create"], &["WA"]).await;
    assert_eq!(r1["ok"], true);
    let t1 = tok_of(&r1);
    let o = Object::new("Project", "worker", json!({"name": "P"}));
    assert_eq!(
        call(pipe, "create_object", with_token(json!(o), &t1)).await["ok"],
        true
    );
    // Re-register (restart): same definition, NEW instance token.
    let r2 = register(pipe, "worker", &["object.create"], &["WA"]).await;
    let t2 = tok_of(&r2);
    assert_ne!(t1, t2, "rotation on re-register");
    // Old instance is dead: no split-brain.
    let o2 = Object::new("Project", "worker", json!({"name": "P2"}));
    assert_eq!(
        call(pipe, "create_object", with_token(json!(o2), &t1)).await["ok"],
        false
    );
    // New instance works with the same definition.
    let caps = call(pipe, "policy_caps", json!({"actor": "worker"})).await;
    assert!(caps["result"]["capabilities"]
        .as_array()
        .unwrap()
        .iter()
        .any(|c| c == "object.create"));
    let o3 = Object::new("Project", "worker", json!({"name": "P3"}));
    assert_eq!(
        call(pipe, "create_object", with_token(json!(o3), &t2)).await["ok"],
        true
    );
    h.abort();
}

// ---------- 2A: duplicate start = last writer wins, loser locked out ----------
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duplicate_start_no_split_brain() {
    let pipe = r"\\.\pipe\spaceos-v17a-dup";
    let db = tmp_db("dup.db");
    let _ = std::fs::remove_file(&db);
    let h = start_core(pipe, &db).await;
    let t_first = tok_of(&register(pipe, "solo", &["object.create"], &[]).await);
    let t_second = tok_of(&register(pipe, "solo", &["object.create"], &[]).await);
    assert_ne!(t_first, t_second);
    let o = Object::new("Project", "solo", json!({}));
    // First runtime's token is dead the moment the second registered.
    assert_eq!(
        call(pipe, "create_object", with_token(json!(o), &t_first)).await["ok"],
        false
    );
    h.abort();
}

// ---------- 2B/2D: core restart wipes runtime state, definition re-applied ----------
// Policy is memory-only by design: restart loses tokens AND grants.
// Recovery = trusted bootstrap re-registers (same definition, new token).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn core_restart_requires_rebootstrap() {
    let pipe = r"\\.\pipe\spaceos-v17a-rs";
    let db = tmp_db("rs.db");
    let _ = std::fs::remove_file(&db);
    let h = start_core(pipe, &db).await;
    let t1 = tok_of(&register(pipe, "w", &["object.create"], &["WA"]).await);
    h.abort();
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    let h2 = start_core(pipe, &db).await;
    // Old token is dead (nothing persisted).
    let o = Object::new("Project", "w", json!({}));
    assert_eq!(
        call(pipe, "create_object", with_token(json!(o), &t1)).await["ok"],
        false
    );
    // Re-bootstrap restores the same definition with a fresh instance.
    let t2 = tok_of(&register(pipe, "w", &["object.create"], &["WA"]).await);
    assert_ne!(t1, t2);
    let o2 = Object::new("Project", "w", json!({}));
    assert_eq!(
        call(pipe, "create_object", with_token(json!(o2), &t2)).await["ok"],
        true
    );
    h2.abort();
}

// ---------- 2C: scope change is not identity change ----------
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scope_change_keeps_identity() {
    let pipe = r"\\.\pipe\spaceos-v17a-scope";
    let db = tmp_db("scope.db");
    let _ = std::fs::remove_file(&db);
    let h = start_core(pipe, &db).await;
    let _t1 = tok_of(&register(pipe, "ctx", &["object.create"], &["WA"]).await);
    // Same name, wider scope: identity (name) unchanged, instance rotated.
    let t2 = tok_of(&register(pipe, "ctx", &["object.create"], &["WA", "WB"]).await);
    let o = Object::new("Project", "ctx", json!({"world_id": "WB"}));
    assert_eq!(
        call(pipe, "create_object", with_token(json!(o), &t2)).await["ok"],
        true
    );
    h.abort();
}

// ---------- create_space is gated (Phase 0 audit finding, fixed) ----------
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_space_requires_capability() {
    let pipe = r"\\.\pipe\spaceos-v17a-space";
    let db = tmp_db("space.db");
    let _ = std::fs::remove_file(&db);
    let h = start_core(pipe, &db).await;
    // Anonymous: denied.
    let s = space_core::Space::new("Evil", json!({}));
    assert_eq!(call(pipe, "create_space", json!(s)).await["ok"], false);
    // Registered without the capability: denied.
    let t = tok_of(&register(pipe, "plain", &["object.create"], &[]).await);
    let s2 = space_core::Space::new("Plain", json!({}));
    let mut p2 = json!(s2);
    p2["actor"] = json!("plain");
    assert_eq!(
        call(pipe, "create_space", with_token(p2, &t)).await["ok"],
        false
    );
    // With space.create: allowed.
    let t3 = tok_of(&register(pipe, "admin", &["space.create"], &[]).await);
    let s3 = space_core::Space::new("Good", json!({}));
    let mut p3 = json!(s3);
    p3["actor"] = json!("admin");
    assert_eq!(
        call(pipe, "create_space", with_token(p3, &t3)).await["ok"],
        true
    );
    h.abort();
}
#[test]
fn agent_object_stable_id_no_duplicate() {
    let p = tmp_db("aobj.db");
    let _ = std::fs::remove_file(&p);
    let s = Storage::open(&p).unwrap();
    for status in ["running", "completed"] {
        let mut a = Object::new("Agent", "agent-demo", json!({"name": "Demo"}));
        a.id = "agent:Demo".to_string();
        a.properties["status"] = json!(status);
        s.create_object(&a).unwrap();
    }
    let all = s
        .query_objects(&space_core::query::ObjectQuery::default())
        .unwrap();
    assert_eq!(
        all.len(),
        1,
        "stable definition id upserts, never duplicates"
    );
    assert_eq!(
        all[0].properties["status"],
        json!("completed"),
        "latest state wins"
    );
}
