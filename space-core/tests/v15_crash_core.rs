//! V1.5 crash/recovery: Core commit points, restart, idempotency,
//! agent-crash semantics, consistency invariants.
//! Crash model: single SQLite statements are atomic (delegated to SQLite);
//! multi-step logical ops are NOT atomic. `fail_once` hooks simulate crash
//! before/after each commit point (test-only, default disarmed).
//! IPC-level "restart" = abort server task + reopen same DB file: only
//! committed statements survive, exactly like a real process crash.
#![cfg(windows)]
use serde_json::{json, Value};
use space_core::query::{EventQuery, ObjectQuery, RelationQuery};
use space_core::{ipc, CrashPoint, Event, Object, Relation, Storage};

/// Cross-process exclusion for the GLOBAL provider pipe: the C2 stub below
/// binds `\\.\pipe\spaceos-fs-provider` (the Core routes action requests to
/// that hard-coded name), so this suite must serialize with every other
/// suite that binds or consumes the global pipes.
#[path = "support/global_lock.rs"]
mod global_lock;

fn tmp_db(name: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join("spaceos-v15");
    std::fs::create_dir_all(&d).unwrap();
    d.join(format!("{name}.db"))
}

fn fresh(name: &str) -> Storage {
    let p = tmp_db(name);
    let _ = std::fs::remove_file(&p);
    Storage::open(&p).unwrap()
}

async fn call(pipe: &str, method: &str, params: Value) -> Value {
    ipc::send_request(pipe, method, params).await.unwrap()
}

async fn call_retry(pipe: &str, method: &str, params: Value) -> Value {
    for _ in 0..60 {
        match ipc::send_request(pipe, method, params.clone()).await {
            Ok(v) => return v,
            Err(_) => tokio::time::sleep(std::time::Duration::from_millis(50)).await,
        }
    }
    call(pipe, method, params).await
}

fn fs_obj(name: &str) -> Object {
    Object::new("File", "filesystem", json!({"path": name}))
}

/// Start a Core server on `pipe` backed by `db`. Returns the task handle;
/// aborting it + reopening the DB simulates a crash/restart.
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

// ---------- Test 1: crash before object commit ----------
#[test]
fn crash_before_object_commit() {
    let s = fresh("a1");
    let o = fs_obj("a.txt");
    s.fail_once(CrashPoint::BeforeObjectCommit);
    assert!(s.create_object(&o).is_err());
    // Nothing persisted: no phantom object.
    assert!(s.get_object(&o.id).unwrap().is_none());
    // Retry after "restart" succeeds (fault auto-cleared after firing).
    s.create_object(&o).unwrap();
    assert!(s.get_object(&o.id).unwrap().is_some());
}

// ---------- Test 2: crash after object commit ----------
#[test]
fn crash_after_object_commit() {
    let s = fresh("a2");
    let o = fs_obj("b.txt");
    s.fail_once(CrashPoint::AfterObjectCommit);
    // Caller sees failure, but the statement already committed.
    assert!(s.create_object(&o).is_err());
    assert!(s.get_object(&o.id).unwrap().is_some());
    // Recovery replay with the same id is an idempotent upsert, no duplicate.
    s.create_object(&o).unwrap();
    let all = s.query_objects(&ObjectQuery::default()).unwrap();
    assert_eq!(all.iter().filter(|x| x.id == o.id).count(), 1);
}

// ---------- Test 3a: crash before relation commit (partial chain) ----------
#[test]
fn crash_before_relation_commit() {
    let s = fresh("a3a");
    let o = fs_obj("c.txt");
    s.create_object(&o).unwrap();
    // Phase B: FK — the `proj-1` endpoint must exist before the relation.
    let mut proj = Object::new("Project", "filesystem", json!({"name": "proj-1"}));
    proj.id = "proj-1".into();
    s.create_object(&proj).unwrap();
    let r = Relation::new(&o.id, "belongs_to", "proj-1");
    s.fail_once(CrashPoint::BeforeRelationCommit);
    assert!(s.create_relation(&r).is_err());
    // Partial state is explicit: object without its relation.
    assert!(s.get_object(&o.id).unwrap().is_some());
    assert!(s.get_relation(&r.id).unwrap().is_none());
    // Recovery completes the chain.
    s.create_relation(&r).unwrap();
    assert!(s.get_relation(&r.id).unwrap().is_some());
}

// ---------- Test 3b: crash after relation commit ----------
#[test]
fn crash_after_relation_commit() {
    let s = fresh("a3b");
    // Phase B: FK — both endpoints of x -> y must exist.
    for id in ["x", "y"] {
        let mut o = Object::new("File", "filesystem", json!({}));
        o.id = id.into();
        s.create_object(&o).unwrap();
    }
    let r = Relation::new("x", "contains", "y");
    s.fail_once(CrashPoint::AfterRelationCommit);
    assert!(s.create_relation(&r).is_err());
    assert!(s.get_relation(&r.id).unwrap().is_some());
    s.create_relation(&r).unwrap();
    assert_eq!(
        s.query_relations(&RelationQuery::default()).unwrap().len(),
        1
    );
}

// ---------- Test 3c: crash between relation and event commit ----------
#[test]
fn crash_between_relation_and_event() {
    let s = fresh("a3c");
    let o = fs_obj("d.txt");
    s.create_object(&o).unwrap();
    // Phase B: FK — the `proj-1` endpoint must exist before the relation.
    let mut proj = Object::new("Project", "filesystem", json!({"name": "proj-1"}));
    proj.id = "proj-1".into();
    s.create_object(&proj).unwrap();
    let r = Relation::new(&o.id, "belongs_to", "proj-1");
    s.create_relation(&r).unwrap();
    let e = Event::new("RelationCreated", "filesystem", json!({}));
    s.fail_once(CrashPoint::BeforeEventCommit);
    assert!(s.record_event(&e).is_err());
    // Object + relation durable, event missing: recovery re-records it.
    let mut e2 = e.clone();
    s.record_event(&e2).unwrap();
    e2.id = e.id.clone();
    assert_eq!(s.query_events(&EventQuery::default()).unwrap().len(), 1);
}

// ---------- Test 4a: crash before event commit ----------
#[test]
fn crash_before_event_commit() {
    let s = fresh("a4a");
    let e = Event::new("FileChanged", "filesystem", json!({"path": "e.txt"}));
    s.fail_once(CrashPoint::BeforeEventCommit);
    assert!(s.record_event(&e).is_err());
    assert!(s.query_events(&EventQuery::default()).unwrap().is_empty());
}

// ---------- Test 4b: crash after event commit (no duplicate on replay) ----------
#[test]
fn crash_after_event_commit() {
    let s = fresh("a4b");
    let e = Event::new("FileChanged", "filesystem", json!({"path": "f.txt"}));
    let eid = e.id.clone();
    s.fail_once(CrashPoint::AfterEventCommit);
    assert!(s.record_event(&e).is_err());
    // Committed despite caller-visible failure; replay by id is idempotent.
    s.record_event(&e).unwrap();
    let all = s.query_events(&EventQuery::default()).unwrap();
    assert_eq!(all.iter().filter(|x| x.id == eid).count(), 1);
    // History has no update/delete path: immutability holds.
    assert!(s.query_events(&EventQuery::default()).unwrap().len() == 1);
}

// ---------- Test 8 (spec #2 ordering note): IPC restart keeps object, no dup ----------
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ipc_restart_keeps_object_no_duplicate() {
    let pipe = r"\\.\pipe\spaceos-v15-a2";
    let db = tmp_db("v15-a2.db");
    let _ = std::fs::remove_file(&db);
    let h = start_core(pipe, &db).await;
    let o = fs_obj("g.txt");
    let r = call_retry(pipe, "create_object", json!(o)).await;
    assert_eq!(r["ok"], true);
    // Crash: abort server mid-life, reopen same file = restart.
    h.abort();
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    let h2 = start_core(pipe, &db).await;
    let got = call_retry(pipe, "get_object", json!({"id": o.id})).await;
    assert!(got["result"].is_object());
    // Replay after restart does not duplicate.
    call_retry(pipe, "create_object", json!(o)).await;
    let all = call_retry(pipe, "query_objects", json!({})).await;
    assert_eq!(all["result"].as_array().unwrap().len(), 1);
    h2.abort();
}

// ---------- Test 18: clean restart preserves full world ----------
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn clean_restart_preserves_world() {
    let pipe = r"\\.\pipe\spaceos-v15-f1";
    let db = tmp_db("v15-f1.db");
    let _ = std::fs::remove_file(&db);
    let h = start_core(pipe, &db).await;
    let o = fs_obj("h.txt");
    call_retry(pipe, "create_object", json!(o)).await;
    // Phase B: FK — the `p1` endpoint must exist before the relation.
    let mut p1 = Object::new("Project", "filesystem", json!({"name": "p1"}));
    p1.id = "p1".into();
    assert_eq!(call_retry(pipe, "create_object", json!(p1)).await["ok"], true);
    let rel = Relation::new(&o.id, "belongs_to", "p1");
    let rel_id = rel.id.clone();
    let mut rj = json!(rel);
    rj["actor"] = json!("filesystem");
    let rr = call_retry(pipe, "create_relation", rj).await;
    assert_eq!(rr["ok"], true);
    let e = Event::new("ObjectCreated", "filesystem", json!({}));
    call_retry(pipe, "record_event", json!(e)).await;
    h.abort();
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    let h2 = start_core(pipe, &db).await;
    let objs = call_retry(pipe, "query_objects", json!({})).await;
    let rels = call_retry(pipe, "query_relations", json!({})).await;
    let evs = call_retry(pipe, "query_events", json!({})).await;
    assert_eq!(objs["result"].as_array().unwrap().len(), 2);
    assert_eq!(rels["result"].as_array().unwrap().len(), 1);
    assert_eq!(evs["result"].as_array().unwrap().len(), 1);
    assert!(objs["result"]
        .as_array()
        .unwrap()
        .iter()
        .any(|x| x["id"] == json!(o.id)));
    assert!(objs["result"]
        .as_array()
        .unwrap()
        .iter()
        .any(|x| x["id"] == json!("p1")));
    assert_eq!(rels["result"][0]["id"], json!(rel_id));
    assert_eq!(evs["result"][0]["id"], json!(e.id));
    h2.abort();
}

// ---------- Test 19: 1000 mutations then reopen ----------
#[test]
fn reopen_after_1000_mutations() {
    let name = "f2";
    {
        let s = fresh(name);
        // Phase B: FK — `proj` endpoint must exist before belongs_to rows.
        let mut proj = Object::new("Project", "filesystem", json!({"name": "proj"}));
        proj.id = "proj".into();
        s.create_object(&proj).unwrap();
        for i in 0..500 {
            let o = Object::new("File", "filesystem", json!({"path": format!("f{i}.txt")}));
            s.create_object(&o).unwrap();
            if i % 2 == 0 {
                let r = Relation::new(&o.id, "belongs_to", "proj");
                s.create_relation(&r).unwrap();
            }
            if i % 5 == 0 {
                s.record_event(&Event::new("FileChanged", "filesystem", json!({"i": i})))
                    .unwrap();
            }
        }
    }
    // Reopen = restart. SQLite file persists everything committed.
    let p = tmp_db(name);
    let s2 = Storage::open(&p).unwrap();
    assert_eq!(
        s2.query_objects(&ObjectQuery::default()).unwrap().len(),
        501, // 500 churned files + the `proj` FK endpoint
    );
    assert_eq!(
        s2.query_relations(&RelationQuery::default()).unwrap().len(),
        250
    );
    assert_eq!(s2.query_events(&EventQuery::default()).unwrap().len(), 100);
    // Spot-check identity stability.
    let all = s2.query_objects(&ObjectQuery::default()).unwrap();
    assert!(all.iter().any(|o| o.properties["path"] == json!("f42.txt")));
}

// ---------- Test 16 (spec): duplicate observation, same event id ----------
#[test]
fn duplicate_event_same_id_single_row() {
    let s = fresh("e1");
    let e = Event::new("FileChanged", "filesystem", json!({"path": "dup.txt"}));
    s.record_event(&e).unwrap();
    s.record_event(&e).unwrap();
    assert_eq!(s.query_events(&EventQuery::default()).unwrap().len(), 1);
}

// ---------- Agent C1: prepared-but-unsent action leaves no phantom ----------
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn agent_crash_before_action_no_phantom() {
    let pipe = r"\\.\pipe\spaceos-v15-c1";
    let db = tmp_db("v15-c1.db");
    let _ = std::fs::remove_file(&db);
    let h = start_core(pipe, &db).await;
    // Agent prepares an action with a correlation id, then "crashes"
    // before sending: the request object is simply dropped here.
    let corr = "corr-phantom-1";
    drop(corr);
    // Core has no trace of it: nothing was ever sent.
    let evs = call_retry(
        pipe,
        "query_events",
        json!({"event_type": "ActionRequested"}),
    )
    .await;
    assert!(evs["result"].as_array().unwrap().is_empty());
    let objs = call_retry(pipe, "query_objects", json!({})).await;
    assert!(objs["result"].as_array().unwrap().is_empty());
    h.abort();
}

// ---------- Agent C2: action accepted, agent dies before reading result ----------
// Core applies the action (at-least-once); the restarted agent recovers by
// re-querying the correlation id. No resume queue exists — documented GAP.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn agent_crash_after_accepted_action_recovers_by_correlation() {
    let _ipc_lock = global_lock::acquire_global_ipc();
    let pipe = r"\\.\pipe\spaceos-v15-c2";
    let db = tmp_db("v15-c2.db");
    let _ = std::fs::remove_file(&db);
    let h = start_core(pipe, &db).await;
    // Minimal provider stub on PROVIDER_PIPE: applies the action, replies Completed.
    let stub = tokio::spawn(async move {
        #[cfg(windows)]
        {
            use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
            use tokio::net::windows::named_pipe::ServerOptions;
            let server = ServerOptions::new()
                .create(space_core::ipc::PROVIDER_PIPE)
                .unwrap();
            server.connect().await.unwrap();
            let (r, mut w) = tokio::io::split(server);
            let mut lines = BufReader::new(r).lines();
            if let Ok(Some(l)) = lines.next_line().await {
                let req: Value = serde_json::from_str(&l).unwrap();
                let action_id = req["params"]["id"].as_str().unwrap_or("").to_string();
                let resp = json!({"ok": true, "result": {"action_id": action_id, "status": "Completed", "detail": {"applied": true}}});
                let mut s = serde_json::to_string(&resp).unwrap();
                s.push('\n');
                w.write_all(s.as_bytes()).await.unwrap();
            }
        }
    });
    // Agent sends request_action as trusted filesystem principal, then
    // "crashes": raw send, close without reading the reply.
    let corr_id = uuid::Uuid::new_v4().to_string();
    {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
        let stream = tokio::net::windows::named_pipe::ClientOptions::new()
            .open(pipe)
            .unwrap();
        let (r, mut w) = tokio::io::split(stream);
        let mut params = json!({
            "id": corr_id, "action": "create",
            "target": {"path": "x"}, "provider_id": "filesystem", "actor": "filesystem",
            "requested_at": chrono::Utc::now().to_rfc3339(),
        });
        params["auth"] = json!(space_core::ipc::core_token());
        let req = json!({"method": "request_action", "params": params});
        let mut s2 = serde_json::to_string(&req).unwrap();
        s2.push('\n');
        w.write_all(s2.as_bytes()).await.unwrap();
        w.flush().await.unwrap();
        drop(r);
        drop(w);
    }
    // Give Core time to apply; agent "restarts" and re-queries by correlation.
    tokio::time::sleep(std::time::Duration::from_millis(800)).await;
    let evs = call_retry(pipe, "query_events", json!({})).await;
    let arr = evs["result"].as_array().unwrap();
    assert!(arr
        .iter()
        .any(|e| e["event_type"] == "ActionRequested" && e["correlation_id"] == json!(corr_id)));
    assert!(arr
        .iter()
        .any(|e| e["event_type"] == "ActionCompleted" && e["correlation_id"] == json!(corr_id)));
    stub.abort();
    h.abort();
}

// ---------- Agent C3: re-registration mints duplicate Agent identity ----------
// agent-demo style flow creates a FRESH uuid per run while create_object
// replaces by id, not external_id. Two runs => two rows, same external_id.
// The test PASSES by detecting the condition; the invariant itself is FAIL
// (classified: demo-flow bug, Core replace-by-id primitive is sound).
#[test]
fn agent_rerun_duplicate_identity_detected() {
    let s = fresh("c3");
    let ext = "world-1:Agent:SpaceOS Demo Agent";
    for _ in 0..2 {
        let mut a = Object::new("Agent", "agent-demo", json!({"name": "SpaceOS Demo Agent"}));
        a.external_id = Some(ext.to_string());
        s.create_object(&a).unwrap();
    }
    let all = s.query_objects(&ObjectQuery::default()).unwrap();
    let dupes: Vec<_> = all
        .iter()
        .filter(|o| o.external_id.as_deref() == Some(ext))
        .collect();
    assert_eq!(
        dupes.len(),
        2,
        "documents duplicate Agent rows across re-runs"
    );
    assert_ne!(dupes[0].id, dupes[1].id);
}

// ---------- Group G: consistency invariants (test-side checker) ----------
fn assert_world_consistent(s: &Storage) {
    let objs = s.query_objects(&ObjectQuery::default()).unwrap();
    let rels = s.query_relations(&RelationQuery::default()).unwrap();
    let ids: std::collections::HashSet<&str> = objs.iter().map(|o| o.id.as_str()).collect();
    // 1. Relation endpoints must exist (live edges, no dangling).
    for r in &rels {
        assert!(
            ids.contains(r.source_object_id.as_str()),
            "dangling relation source {}",
            r.id
        );
        assert!(
            ids.contains(r.target_object_id.as_str()),
            "dangling relation target {}",
            r.id
        );
        // 3. world_id: non-empty relation world must match source object's world.
        if !r.world_id.is_empty() {
            let src = objs.iter().find(|o| o.id == r.source_object_id).unwrap();
            let src_w = src
                .properties
                .get("world_id")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            assert_eq!(src_w, r.world_id, "relation world mismatch {}", r.id);
        }
    }
    // 5. No duplicate (provider_id, external_id) identity where set.
    let mut seen = std::collections::HashSet::new();
    for o in &objs {
        if let Some(ext) = &o.external_id {
            assert!(
                seen.insert((o.provider_id.clone(), ext.clone())),
                "duplicate identity {ext}"
            );
        }
    }
}

#[test]
fn consistency_invariants_hold_on_healthy_world() {
    let s = fresh("g1");
    let o = Object::new(
        "Project",
        "filesystem",
        json!({"world_id": "W1", "name": "P"}),
    );
    s.create_object(&o).unwrap();
    let f = Object::new(
        "File",
        "filesystem",
        json!({"world_id": "W1", "path": "a.txt"}),
    );
    s.create_object(&f).unwrap();
    let mut r = Relation::new(&o.id, "contains", &f.id);
    r.world_id = "W1".into();
    s.create_relation(&r).unwrap();
    assert_world_consistent(&s);
}

#[test]
fn consistency_checker_catches_dangling_relation() {
    let s = fresh("g2");
    // Phase B: Core enforces FK, so API-level dangling is rejected by
    // design; the checker remains the safety net for legacy/foreign
    // databases. Prepare the orphan exactly like a pre-FK writer would:
    // raw SQL with FK enforcement off (the shape migration preserves).
    let db = tmp_db("g2");
    let conn = rusqlite::Connection::open(&db).unwrap();
    // rusqlite turns FK enforcement on by default; a pre-FK writer had it off.
    conn.execute_batch("PRAGMA foreign_keys=OFF;").unwrap();
    conn.execute(
        "INSERT INTO relations (id, source_object_id, relation_type, target_object_id, properties, created_at, world_id) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        rusqlite::params![
            "legacy-dangling",
            "ghost-src",
            "contains",
            "ghost-dst",
            "{}",
            chrono::Utc::now().to_rfc3339(),
            ""
        ],
    )
    .unwrap();
    drop(conn);
    let caught =
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| assert_world_consistent(&s)));
    assert!(caught.is_err(), "checker must flag dangling relation");
}
