//! V1.6 deletion lifecycle, Core side: hard-delete + cascade semantics,
//! idempotency, relation/graph/tree contracts, crash around delete,
//! event-type discipline, world isolation, scale, final checker.
//! Rule R1: delete_object removes the row AND attached relations
//! (relations-first). Rule R3: deletion terminates identity.
//! Rule R4: repeated delete is idempotent and quiet.
#![cfg(windows)]
use serde_json::{json, Value};
use space_core::query::{EventQuery, ObjectQuery, RelationQuery};
use space_core::{ipc, CrashPoint, Event, Object, Relation, Storage};

fn tmp_db(name: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join("spaceos-v16");
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

fn rel_json(source: &str, rtype: &str, target: &str, world: &str) -> Value {
    let mut r = json!(Relation::new(source, rtype, target));
    r["world_id"] = json!(world);
    r["actor"] = json!("filesystem");
    r
}

/// IPC delete with mandatory actor binding (anonymous deletes are denied).
async fn del(pipe: &str, id: &str, actor: &str) -> Value {
    call_retry(pipe, "delete_object", json!({"id": id, "actor": actor})).await
}

// ---------- Test 1: Core delete object ----------
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn core_delete_object() {
    let pipe = r"\\.\pipe\spaceos-v16-c1";
    let db = tmp_db("c1.db");
    let _ = std::fs::remove_file(&db);
    let h = start_core(pipe, &db).await;
    let o = Object::new("File", "filesystem", json!({"path": "a.txt"}));
    assert_eq!(
        call_retry(pipe, "create_object", json!(o)).await["ok"],
        true
    );
    // Identity resolvable before delete...
    assert!(call_retry(pipe, "get_object", json!({"id": o.id})).await["result"].is_object());
    // ...gone after. World query, identity: all agree.
    assert_eq!(del(pipe, &o.id, "filesystem").await["ok"], true);
    assert!(call_retry(pipe, "get_object", json!({"id": o.id})).await["result"].is_null());
    let w = call_retry(pipe, "get_world", json!({})).await;
    assert!(w["result"]["objects"].as_array().unwrap().is_empty());
    h.abort();
}

// ---------- Tests 6+7: relation source/target deletion cascades ----------
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn relation_endpoints_cascade_on_delete() {
    let pipe = r"\\.\pipe\spaceos-v16-c67";
    let db = tmp_db("c67.db");
    let _ = std::fs::remove_file(&db);
    let h = start_core(pipe, &db).await;
    let a = Object::new("Project", "cli", json!({"name": "A"}));
    let b = Object::new("File", "cli", json!({"name": "B"}));
    let c = Object::new("File", "cli", json!({"name": "C"}));
    let d = Object::new("File", "cli", json!({"name": "D"}));
    for o in [&a, &b, &c, &d] {
        assert_eq!(
            call_retry(pipe, "create_object", json!(o)).await["ok"],
            true
        );
    }
    // A->B, B->A (both directions), C->D (unrelated control).
    for (s, t) in [(&a, &b), (&b, &a), (&c, &d)] {
        let r = rel_json(&s.id, "contains", &t.id, "");
        assert_eq!(call_retry(pipe, "create_relation", r).await["ok"], true);
    }
    // Delete target B: both A->B and B->A vanish; C->D survives.
    assert_eq!(del(pipe, &b.id, "cli").await["ok"], true);
    let rels = call_retry(pipe, "query_relations", json!({})).await;
    let arr = rels["result"].as_array().unwrap();
    assert_eq!(arr.len(), 1, "only the unrelated relation survives");
    assert_eq!(arr[0]["source_object_id"], json!(c.id));
    h.abort();
}

// ---------- Test 5: repeated delete is idempotent and quiet ----------
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repeated_delete_idempotent() {
    let pipe = r"\\.\pipe\spaceos-v16-c5";
    let db = tmp_db("c5.db");
    let _ = std::fs::remove_file(&db);
    let h = start_core(pipe, &db).await;
    let o = Object::new("File", "filesystem", json!({"path": "a.txt"}));
    call_retry(pipe, "create_object", json!(o)).await;
    let e1 = call_retry(pipe, "query_events", json!({})).await;
    let n_before = e1["result"].as_array().unwrap().len();
    for _ in 0..3 {
        let r = del(pipe, &o.id, "filesystem").await;
        assert_eq!(r["ok"], true, "repeat delete stays ok, no panic");
    }
    // Core delete emits no events itself: no storm, no resurrection.
    let e2 = call_retry(pipe, "query_events", json!({})).await;
    assert_eq!(e2["result"].as_array().unwrap().len(), n_before);
    assert!(call_retry(pipe, "get_object", json!({"id": o.id})).await["result"].is_null());
    h.abort();
}

// ---------- Tests 18+19: graph/tree contract over get_world ----------
async fn seed_project(pipe: &str) -> (String, String, String) {
    let p = Object::new("Project", "cli", json!({"name": "P"}));
    let f = Object::new("File", "cli", json!({"name": "main.rs"}));
    let d = Object::new("Document", "cli", json!({"name": "Arch"}));
    for o in [&p, &f, &d] {
        call_retry(pipe, "create_object", json!(o)).await;
    }
    for (s, t) in [(&p, &f), (&p, &d)] {
        call_retry(
            pipe,
            "create_relation",
            rel_json(&s.id, "contains", &t.id, ""),
        )
        .await;
    }
    (p.id.clone(), f.id.clone(), d.id.clone())
}

/// Pure projection mirroring runtime/static/app.js buildGraph: an edge is
/// drawable iff both endpoints exist in the world.
fn graph_edges(objects: &Value, relations: &Value) -> Vec<(String, String, String)> {
    let objs = objects.as_array().unwrap();
    let mut out = vec![];
    for r in relations.as_array().unwrap() {
        let s = objs.iter().find(|o| o["id"] == r["source_object_id"]);
        let t = objs.iter().find(|o| o["id"] == r["target_object_id"]);
        if let (Some(s), Some(t)) = (s, t) {
            out.push((
                s["properties"]["name"].as_str().unwrap_or("?").to_string(),
                r["relation_type"].as_str().unwrap().to_string(),
                t["properties"]["name"].as_str().unwrap_or("?").to_string(),
            ));
        }
    }
    out
}

/// Pure projection mirroring app.js buildTree: children via contains edges.
fn tree_children(objects: &Value, relations: &Value, parent: &str) -> Vec<String> {
    relations
        .as_array()
        .unwrap()
        .iter()
        .filter(|r| r["source_object_id"] == json!(parent) && r["relation_type"] == "contains")
        .filter_map(|r| {
            objects
                .as_array()
                .unwrap()
                .iter()
                .find(|o| o["id"] == r["target_object_id"])
        })
        .filter_map(|o| o["properties"]["name"].as_str().map(|s| s.to_string()))
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn graph_after_deletion_shows_no_impossible_state() {
    let pipe = r"\\.\pipe\spaceos-v16-c18";
    let db = tmp_db("c18.db");
    let _ = std::fs::remove_file(&db);
    let h = start_core(pipe, &db).await;
    let (p, f, _d) = seed_project(pipe).await;
    let _ = p;
    del(pipe, &f, "cli").await;
    let w = call_retry(pipe, "get_world", json!({})).await;
    let edges = graph_edges(&w["result"]["objects"], &w["result"]["relations"]);
    assert!(
        !edges.iter().any(|(_, _, t)| t == "main.rs"),
        "no edge to a deleted node"
    );
    assert!(
        edges.iter().any(|(_, _, t)| t == "Arch"),
        "surviving edges intact"
    );
    h.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tree_after_deletion_hides_file() {
    let pipe = r"\\.\pipe\spaceos-v16-c19";
    let db = tmp_db("c19.db");
    let _ = std::fs::remove_file(&db);
    let h = start_core(pipe, &db).await;
    let (p, f, _d) = seed_project(pipe).await;
    del(pipe, &f, "cli").await;
    let w = call_retry(pipe, "get_world", json!({})).await;
    let kids = tree_children(&w["result"]["objects"], &w["result"]["relations"], &p);
    assert!(
        !kids.contains(&"main.rs".to_string()),
        "tree hides deleted file (World rule, not UI choice)"
    );
    assert!(kids.contains(&"Arch".to_string()));
    h.abort();
}

// ---------- Test 14: crash during/after Core deletion ----------
#[test]
fn crash_before_object_delete_changes_nothing() {
    let s = fresh("c14a");
    let o = Object::new("File", "filesystem", json!({"path": "a.txt"}));
    s.create_object(&o).unwrap();
    // Phase B: FK — endpoint for the belongs_to relation.
    let mut p = Object::new("Project", "filesystem", json!({"name": "p"}));
    p.id = "p".into();
    s.create_object(&p).unwrap();
    let r = Relation::new(&o.id, "belongs_to", "p");
    s.create_relation(&r).unwrap();
    s.fail_once(CrashPoint::BeforeObjectDelete);
    assert!(s.delete_object(&o.id).is_err());
    // Nothing happened: object AND relations intact.
    assert!(s.get_object(&o.id).unwrap().is_some());
    assert!(s.get_relation(&r.id).unwrap().is_some());
}

#[test]
fn crash_after_object_delete_retry_converges() {
    let s = fresh("c14b");
    let o = Object::new("File", "filesystem", json!({"path": "a.txt"}));
    s.create_object(&o).unwrap();
    // Phase B: FK — endpoint for the belongs_to relation.
    let mut p = Object::new("Project", "filesystem", json!({"name": "p"}));
    p.id = "p".into();
    s.create_object(&p).unwrap();
    let r = Relation::new(&o.id, "belongs_to", "p");
    s.create_relation(&r).unwrap();
    s.fail_once(CrashPoint::AfterObjectDelete);
    // Caller sees failure, but cascade committed.
    assert!(s.delete_object(&o.id).is_err());
    assert!(s.get_object(&o.id).unwrap().is_none());
    assert!(s.get_relation(&r.id).unwrap().is_none());
    // Retry converges (idempotent): still gone, still no relations.
    s.delete_object(&o.id).unwrap();
    assert!(s
        .query_relations(&RelationQuery::default())
        .unwrap()
        .is_empty());
}

// ---------- Test 20: delete event semantics ----------
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_event_semantics() {
    let pipe = r"\\.\pipe\spaceos-v16-c20";
    let db = tmp_db("c20.db");
    let _ = std::fs::remove_file(&db);
    let h = start_core(pipe, &db).await;
    // No ObjectDeleted/ObjectTombstoned/ObjectRestored/RelationDeleted
    // mutation endpoints exist: deletion is observed via FileDeleted.
    for m in [
        "delete_event",
        "update_event",
        "tombstone_object",
        "restore_object",
    ] {
        let r = call_retry(pipe, m, json!({})).await;
        assert_eq!(r["ok"], false, "{m} must not exist");
    }
    // FileDeleted is recordable and immutable afterwards.
    let e = Event::new("FileDeleted", "filesystem", json!({"path": "a.txt"}));
    assert_eq!(call_retry(pipe, "record_event", json!(e)).await["ok"], true);
    let evs = call_retry(pipe, "query_events", json!({"event_type": "FileDeleted"})).await;
    assert_eq!(evs["result"].as_array().unwrap().len(), 1);
    h.abort();
}

// ---------- Test 17: world isolation after deletion ----------
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn world_isolation_after_deletion() {
    let pipe = r"\\.\pipe\spaceos-v16-c17";
    let db = tmp_db("c17.db");
    let _ = std::fs::remove_file(&db);
    let h = start_core(pipe, &db).await;
    let mk = |t: &str, w: &str| Object::new(t, "cli", json!({"world_id": w, "name": t}));
    let xa = mk("File", "WA");
    let ya = mk("Doc", "WA");
    let xb = mk("File", "WB");
    for o in [&xa, &ya, &xb] {
        call_retry(pipe, "create_object", json!(o)).await;
    }
    let mut ra = json!(Relation::new(&xa.id, "contains", &ya.id));
    ra["world_id"] = json!("WA");
    ra["actor"] = json!("cli");
    call_retry(pipe, "create_relation", ra).await;
    let mut ea = Event::new("ObjectCreated", "cli", json!({}));
    ea.world_id = "WA".into();
    call_retry(pipe, "record_event", json!(ea)).await;
    let mut eb = Event::new("ObjectCreated", "cli", json!({}));
    eb.world_id = "WB".into();
    call_retry(pipe, "record_event", json!(eb)).await;
    // Delete X in WA (cascades its relation).
    assert_eq!(del(pipe, &xa.id, "cli").await["ok"], true);
    // World B untouched across objects, relations (none there), events.
    let ob = call_retry(pipe, "query_objects", json!({"world_id": "WB"})).await;
    assert_eq!(ob["result"].as_array().unwrap().len(), 1);
    let oa = call_retry(pipe, "query_objects", json!({"world_id": "WA"})).await;
    assert_eq!(oa["result"].as_array().unwrap().len(), 1);
    assert_eq!(oa["result"][0]["id"], json!(ya.id));
    let eb2 = call_retry(pipe, "query_events", json!({"world_id": "WB"})).await;
    assert_eq!(eb2["result"].as_array().unwrap().len(), 1);
    h.abort();
}

// ---------- Test 23: 1000-object deletion scale ----------
#[test]
fn delete_1000_objects_scale() {
    let s = fresh("c23");
    for i in 0..1000 {
        s.create_object(
            &Object::new("File", "filesystem", json!({"path": format!("f{i}.txt")})).tap_id(i),
        )
        .unwrap();
    }
    assert_eq!(
        s.query_objects(&ObjectQuery::default()).unwrap().len(),
        1000
    );
    let t0 = std::time::Instant::now();
    for o in s.query_objects(&ObjectQuery::default()).unwrap() {
        s.delete_object(&o.id).unwrap();
    }
    let dt = t0.elapsed();
    eprintln!("1000 deletes: {}ms", dt.as_millis());
    assert!(s.query_objects(&ObjectQuery::default()).unwrap().is_empty());
    let t1 = std::time::Instant::now();
    let _ = s.query_objects(&ObjectQuery::default()).unwrap();
    eprintln!("query-after-delete: {}us", t1.elapsed().as_micros());
    let size = std::fs::metadata(tmp_db("c23"))
        .map(|m| m.len())
        .unwrap_or(0);
    eprintln!("db size after deletes: {size}B");
}

// ---------- Test 25: final lifecycle consistency checker ----------
fn assert_lifecycle_consistent(s: &Storage) {
    let objs = s.query_objects(&ObjectQuery::default()).unwrap();
    let rels = s.query_relations(&RelationQuery::default()).unwrap();
    let evs = s.query_events(&EventQuery::default()).unwrap();
    let ids: std::collections::HashSet<&str> = objs.iter().map(|o| o.id.as_str()).collect();
    // Every live relation endpoint resolves (cascade guarantees this).
    for r in &rels {
        assert!(
            ids.contains(r.source_object_id.as_str()),
            "dangling {}",
            r.id
        );
        assert!(
            ids.contains(r.target_object_id.as_str()),
            "dangling {}",
            r.id
        );
    }
    // No duplicate (provider, external_id) identities.
    let mut seen = std::collections::HashSet::new();
    for o in &objs {
        if let Some(ext) = &o.external_id {
            assert!(
                seen.insert((o.provider_id.clone(), ext.clone())),
                "dup identity {ext}"
            );
        }
    }
    // Events reference live objects or recorded deletions (FileDeleted for
    // missing ids is consistent; anything else dangling is flagged).
    let deleted: std::collections::HashSet<String> = evs
        .iter()
        .filter(|e| e.event_type == "FileDeleted")
        .flat_map(|e| e.object_ids.clone())
        .collect();
    for e in &evs {
        for oid in &e.object_ids {
            assert!(
                ids.contains(oid.as_str()) || deleted.contains(oid),
                "event {} references unknown live object {oid}",
                e.id
            );
        }
    }
}

#[test]
fn lifecycle_checker_healthy_and_after_deletion() {
    let s = fresh("c25");
    let p = Object::new("Project", "cli", json!({"name": "P"}));
    let f = Object::new("File", "filesystem", json!({"path": "a.txt"}));
    s.create_object(&p).unwrap();
    s.create_object(&f).unwrap();
    let r = Relation::new(&p.id, "contains", &f.id);
    s.create_relation(&r).unwrap();
    assert_lifecycle_consistent(&s);
    // Delete the file: cascade removes the relation; checker still holds.
    s.delete_object(&f.id).unwrap();
    s.record_event(&{
        let mut e = Event::new("FileDeleted", "filesystem", json!({"path": "a.txt"}));
        e.object_ids = vec![f.id.clone()];
        e
    })
    .unwrap();
    assert_lifecycle_consistent(&s);
}

// helper: deterministic ids for scale test readability
trait TapId {
    fn tap_id(self, i: usize) -> Self;
}
impl TapId for Object {
    fn tap_id(mut self, i: usize) -> Self {
        self.id = format!("scale-{i:04}");
        self
    }
}
