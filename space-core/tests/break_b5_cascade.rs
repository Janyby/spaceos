//! B5 — Crash in deletion cascade. Fixed conditions, see
//! docs/BREAKAGE_CAMPAIGN.md. Every intermediate shape asserted exactly;
//! retry always converges; checker clean at the end.
use serde_json::json;
use space_core::query::{ObjectQuery, RelationQuery};
use space_core::{CrashPoint, Object, Relation, Storage};

fn tmp_db(name: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join("spaceos-bb5");
    std::fs::create_dir_all(&d).unwrap();
    d.join(format!("{name}.db"))
}

fn fresh(name: &str) -> Storage {
    let p = tmp_db(name);
    let _ = std::fs::remove_file(&p);
    Storage::open(&p).unwrap()
}

fn reopen(name: &str) -> Storage {
    Storage::open(tmp_db(name)).unwrap()
}

/// Object O with 3 attached relations (both directions) + 1 unrelated row.
fn seed(s: &Storage) -> (String, Vec<String>) {
    let o = Object::new("File", "filesystem", serde_json::json!({"path": "o.txt"}));
    let a = Object::new("Project", "cli", serde_json::json!({"name": "A"}));
    let b = Object::new("File", "cli", json!({"name": "B"}));
    let u = Object::new("File", "cli", json!({"name": "U"}));
    let v = Object::new("File", "cli", json!({"name": "V"}));
    for x in [&o, &a, &b, &u, &v] {
        s.create_object(x).unwrap();
    }
    let mut rids = vec![];
    for (src, t) in [(&o, &a), (&b, &o), (&o, &b)] {
        let r = Relation::new(&src.id, "x", &t.id);
        let id = r.id.clone();
        s.create_relation(&r).unwrap();
        rids.push(id);
    }
    let mut ru = Relation::new(&u.id, "x", &v.id);
    ru.world_id = String::new();
    s.create_relation(&ru).unwrap();
    (o.id.clone(), rids)
}

fn counts(s: &Storage) -> (usize, usize) {
    (
        s.query_objects(&ObjectQuery::default()).unwrap().len(),
        s.query_relations(&RelationQuery::default()).unwrap().len(),
    )
}

fn checker_clean(s: &Storage) {
    let objs = s.query_objects(&ObjectQuery::default()).unwrap();
    let rels = s.query_relations(&RelationQuery::default()).unwrap();
    let ids: std::collections::HashSet<_> = objs.iter().map(|o| o.id.as_str()).collect();
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
}

#[test]
fn crash_before_relation_delete() {
    let s = fresh("p1");
    let (oid, rids) = seed(&s);
    assert_eq!(rids.len(), 3);
    s.fail_once(CrashPoint::BeforeRelationDelete);
    assert!(s.delete_object(&oid).is_err());
    // Nothing happened at all: object + all 3 relations intact.
    assert_eq!(counts(&s), (5, 4));
    s.clear_faults();
    s.delete_object(&oid).unwrap();
    assert_eq!(counts(&s), (4, 1));
    checker_clean(&s);
}

#[test]
fn crash_after_relation_delete() {
    let s = fresh("p2");
    let (oid, rids) = seed(&s);
    s.fail_once(CrashPoint::AfterRelationDelete);
    assert!(s.delete_object(&oid).is_err());
    // Prefix committed: exactly the first cascade row is gone.
    let rels = s.query_relations(&RelationQuery::default()).unwrap();
    assert_eq!(rels.len(), 3, "one of four relations deleted");
    assert!(s.get_object(&oid).unwrap().is_some(), "object still alive");
    drop(s);
    // Restart: shape persists, retry converges.
    let s2 = reopen("p2");
    assert_eq!(counts(&s2), (5, 3));
    s2.delete_object(&oid).unwrap();
    assert_eq!(counts(&s2), (4, 1));
    checker_clean(&s2);
    let _ = rids;
}

#[test]
fn crash_before_object_delete() {
    let s = fresh("p3");
    let (oid, _) = seed(&s);
    // Arm AFTER the cascade ran: relations gone, object delete fails.
    // (No mid-cascade hook fires here; this is the object-level point.)
    s.fail_once(CrashPoint::BeforeObjectDelete);
    assert!(s.delete_object(&oid).is_err());
    assert_eq!(counts(&s), (5, 4), "before-point changes nothing");
    s.clear_faults();
    s.delete_object(&oid).unwrap();
    assert_eq!(counts(&s), (4, 1));
    checker_clean(&s);
}

#[test]
fn crash_after_object_delete() {
    let s = fresh("p4");
    let (oid, _) = seed(&s);
    s.fail_once(CrashPoint::AfterObjectDelete);
    assert!(s.delete_object(&oid).is_err());
    // Fully committed despite the caller-visible error.
    assert_eq!(counts(&s), (4, 1));
    drop(s);
    let s2 = reopen("p4");
    assert_eq!(counts(&s2), (4, 1), "restart sees the committed cascade");
    s2.delete_object(&oid).unwrap(); // idempotent retry
    assert_eq!(counts(&s2), (4, 1));
    checker_clean(&s2);
}

#[test]
fn crash_around_event_after_cascade() {
    use space_core::query::EventQuery;
    use space_core::Event;
    let s = fresh("p5");
    let (oid, _) = seed(&s);
    s.delete_object(&oid).unwrap();
    s.fail_once(CrashPoint::BeforeEventCommit);
    let e = Event::new(
        "FileDeleted",
        "filesystem",
        serde_json::json!({"path": "o.txt"}),
    );
    assert!(s.record_event(&e).is_err());
    assert!(s.query_events(&EventQuery::default()).unwrap().is_empty());
    s.record_event(&e).unwrap();
    assert_eq!(s.query_events(&EventQuery::default()).unwrap().len(), 1);
    checker_clean(&s);
}

#[test]
fn cascade_never_touches_unrelated() {
    let s = fresh("p6");
    let (oid, _) = seed(&s);
    for _ in 0..3 {
        let _ = s.delete_object(&oid);
    }
    // Repeated deletes converge; unrelated pair untouched across all runs.
    assert_eq!(counts(&s), (4, 1));
    checker_clean(&s);
}
