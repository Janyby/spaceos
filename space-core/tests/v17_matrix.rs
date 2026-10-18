//! V1.7 Phase 8 — failure-injection matrix: crash points x operations.
//! Uses the V1.5 fail_once hooks (storage level, precise) plus IPC
//! abort/reopen (coarse). Each cell asserts the exact resulting shape:
//! committed-or-absent, never silent partial contradiction.
#![cfg(windows)]
use serde_json::{json, Value};
use space_core::{ipc, CrashPoint, Event, Object, Relation, Storage};

fn tmp_db(name: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join("spaceos-v17m");
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

// ---------- Matrix: every hook point x its operation ----------
#[test]
fn hook_matrix_all_points() {
    // (point, op closure returns true if committed, then cleanup check)
    let s = fresh("matrix");
    // BeforeObjectCommit
    let o = Object::new("File", "filesystem", json!({}));
    s.fail_once(CrashPoint::BeforeObjectCommit);
    assert!(s.create_object(&o).is_err());
    assert!(s.get_object(&o.id).unwrap().is_none());
    // AfterObjectCommit
    s.fail_once(CrashPoint::AfterObjectCommit);
    assert!(s.create_object(&o).is_err());
    assert!(s.get_object(&o.id).unwrap().is_some());
    s.clear_faults();
    s.create_object(&o).unwrap();
    // Phase B: FK — the `x` endpoint must exist before the relation.
    let mut x = Object::new("File", "filesystem", json!({}));
    x.id = "x".into();
    s.create_object(&x).unwrap();
    // BeforeRelationCommit
    let r = Relation::new(&o.id, "contains", "x");
    s.fail_once(CrashPoint::BeforeRelationCommit);
    assert!(s.create_relation(&r).is_err());
    assert!(s.get_relation(&r.id).unwrap().is_none());
    // AfterRelationCommit
    s.fail_once(CrashPoint::AfterRelationCommit);
    assert!(s.create_relation(&r).is_err());
    assert!(s.get_relation(&r.id).unwrap().is_some());
    s.clear_faults();
    // BeforeEventCommit
    let e = Event::new("T", "filesystem", json!({}));
    s.fail_once(CrashPoint::BeforeEventCommit);
    assert!(s.record_event(&e).is_err());
    // AfterEventCommit
    s.fail_once(CrashPoint::AfterEventCommit);
    assert!(s.record_event(&e).is_err());
    s.clear_faults();
    s.record_event(&e).unwrap();
    // BeforeObjectDelete
    s.fail_once(CrashPoint::BeforeObjectDelete);
    assert!(s.delete_object(&o.id).is_err());
    assert!(s.get_object(&o.id).unwrap().is_some());
    assert!(s.get_relation(&r.id).unwrap().is_some());
    // AfterObjectDelete
    s.fail_once(CrashPoint::AfterObjectDelete);
    assert!(s.delete_object(&o.id).is_err());
    assert!(s.get_object(&o.id).unwrap().is_none());
    assert!(
        s.get_relation(&r.id).unwrap().is_none(),
        "cascade committed before the fault"
    );
    s.clear_faults();
    s.delete_object(&o.id).unwrap(); // idempotent retry converges
}

// ---------- Combined chain with faults at two points ----------
#[test]
fn combined_chain_partial_shape() {
    let s = fresh("combo");
    let o = Object::new("Project", "cli", json!({}));
    s.create_object(&o).unwrap();
    // Phase B: FK — the `f` endpoint must exist before the relation.
    let mut f = Object::new("File", "cli", json!({}));
    f.id = "f".into();
    s.create_object(&f).unwrap();
    // Relation fails, event would have followed: exact partial shape.
    let r = Relation::new(&o.id, "contains", "f");
    s.fail_once(CrashPoint::BeforeRelationCommit);
    assert!(s.create_relation(&r).is_err());
    assert!(s.get_object(&o.id).unwrap().is_some());
    assert!(s.get_relation(&r.id).unwrap().is_none());
    // Recovery completes; chain fully consistent afterwards.
    s.create_relation(&r).unwrap();
    s.record_event(&Event::new("RelationCreated", "cli", json!({})))
        .unwrap();
    assert!(s.get_relation(&r.id).unwrap().is_some());
}

// ---------- Core abort between IPC steps of one logical chain ----------
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn core_abort_between_chain_steps() {
    let pipe = r"\\.\pipe\spaceos-v17m-chain";
    let db = tmp_db("chain.db");
    let _ = std::fs::remove_file(&db);
    let h = start_core(pipe, &db).await;
    let o = Object::new("Project", "cli", json!({}));
    assert_eq!(call(pipe, "create_object", json!(o)).await["ok"], true);
    // Phase B: FK — the `f` endpoint must exist before the relation.
    let mut f = Object::new("File", "cli", json!({}));
    f.id = "f".into();
    assert_eq!(call(pipe, "create_object", json!(f)).await["ok"], true);
    h.abort(); // crash between object and relation steps
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    let h2 = start_core(pipe, &db).await;
    // Chain resumes after restart; final state is the full triple.
    let mut r = json!(Relation::new(&o.id, "contains", "f"));
    r["actor"] = json!("cli");
    assert_eq!(call(pipe, "create_relation", r).await["ok"], true);
    let e = Event::new("RelationCreated", "cli", json!({}));
    assert_eq!(call(pipe, "record_event", json!(e)).await["ok"], true);
    assert!(call(pipe, "get_object", json!({"id": o.id})).await["result"].is_object());
    h2.abort();
}

// ---------- Phase 14: recovery/reconciliation paths cannot bypass policy ----------
// Scanner, watcher and retry paths all bind actor; an actorless delete
// (the pre-V1.6 shape) is denied and audited, never silently applied.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn anonymous_delete_denied_and_audited() {
    let pipe = r"\\.\pipe\spaceos-v17m-anon";
    let db = tmp_db("anon.db");
    let _ = std::fs::remove_file(&db);
    let h = start_core(pipe, &db).await;
    let o = Object::new("File", "filesystem", json!({"path": "a.txt"}));
    assert_eq!(call(pipe, "create_object", json!(o)).await["ok"], true);
    // No actor field at all (legacy shape): denied.
    let r = call(pipe, "delete_object", json!({"id": o.id})).await;
    assert_eq!(r["ok"], false);
    assert!(r["error"].as_str().unwrap().contains("denied"));
    // Row untouched by the denied attempt.
    assert!(call(pipe, "get_object", json!({"id": o.id})).await["result"].is_object());
    // Audit trail exists.
    let denied = call(
        pipe,
        "query_events",
        json!({"event_type": "MutationDenied"}),
    )
    .await;
    assert!(!denied["result"].as_array().unwrap().is_empty());
    h.abort();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscriber_drop_resync_via_query() {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    let pipe = r"\\.\pipe\spaceos-v17m-sub";
    let db = tmp_db("sub.db");
    let _ = std::fs::remove_file(&db);
    let h = start_core(pipe, &db).await;
    // Subscribe, receive one, then "crash" (drop the connection).
    let stream = tokio::net::windows::named_pipe::ClientOptions::new()
        .open(pipe)
        .unwrap();
    let (r, mut w) = tokio::io::split(stream);
    let mut params = json!({});
    params["auth"] = json!(ipc::core_token());
    let req = json!({"method": "subscribe", "params": params});
    let mut s = serde_json::to_string(&req).unwrap();
    s.push('\n');
    w.write_all(s.as_bytes()).await.unwrap();
    w.flush().await.unwrap();
    let mut lines = BufReader::new(r).lines();
    let ack = lines.next_line().await.unwrap().unwrap();
    assert!(serde_json::from_str::<Value>(&ack).unwrap()["ok"]
        .as_bool()
        .unwrap());
    let e1 = Event::new("Tick", "filesystem", json!({"i": 1}));
    call(pipe, "record_event", json!(e1)).await;
    let got = tokio::time::timeout(std::time::Duration::from_millis(2000), lines.next_line())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(got.contains("\"Tick\""));
    drop(lines);
    drop(w); // subscriber crash: server task ends, subscription stays registered (no leak assertion possible)
             // Two more events while "down", then resync purely via query (no replay).
    for i in 2..=3 {
        call(
            pipe,
            "record_event",
            json!(Event::new("Tick", "filesystem", json!({"i": i}))),
        )
        .await;
    }
    let all = call(pipe, "query_events", json!({"event_type": "Tick"})).await;
    assert_eq!(
        all["result"].as_array().unwrap().len(),
        3,
        "missed events recoverable via query"
    );
    h.abort();
}
