//! V1.7 Phase 9 — strengthened consistency checker.
//! The checker FINDS violations; it never fixes or hides them.
//! Each rule below maps to one numbered check; fixtures prove both
//! the clean case (no findings) and each broken case (finding present).
use serde_json::json;
use space_core::query::{EventQuery, ObjectQuery, RelationQuery};
use space_core::{Event, Object, Relation, Storage};
use std::collections::{HashMap, HashSet};

fn tmp_db(name: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join("spaceos-v17k");
    std::fs::create_dir_all(&d).unwrap();
    d.join(format!("{name}.db"))
}

fn fresh(name: &str) -> Storage {
    let p = tmp_db(name);
    let _ = std::fs::remove_file(&p);
    Storage::open(&p).unwrap()
}

/// Returns human-readable findings; empty = healthy.
/// 1 object ids unique · 2 agent external identity unique ·
/// 3 relation endpoints valid · 4 relation world matches source ·
/// 5 event world column/payload agree · 6 actor attribution present ·
/// 7 correlation chains resolve · 8 no duplicate (provider, external_id) ·
/// 9 no live object already recorded deleted · 10 event objects share
/// event world (when both known) · 11 denied mutations carry audit detail.
pub fn check(s: &Storage) -> Vec<String> {
    let mut out = vec![];
    let objs = s.query_objects(&ObjectQuery::default()).unwrap();
    let rels = s.query_relations(&RelationQuery::default()).unwrap();
    let evs = s.query_events(&EventQuery::default()).unwrap();

    let mut ids = HashSet::new();
    for o in &objs {
        if !ids.insert(o.id.clone()) {
            out.push(format!("1: duplicate object id {}", o.id));
        }
    }
    let mut agent_ext = HashSet::new();
    for o in objs.iter().filter(|o| o.r#type == "Agent") {
        if let Some(e) = &o.external_id {
            if !agent_ext.insert(e.clone()) {
                out.push(format!("2: duplicate agent identity {e}"));
            }
        }
    }
    let by_id: HashMap<&str, &Object> = objs.iter().map(|o| (o.id.as_str(), o)).collect();
    for r in &rels {
        if !by_id.contains_key(r.source_object_id.as_str()) {
            out.push(format!("3: relation {} dangling source", r.id));
        }
        if !by_id.contains_key(r.target_object_id.as_str()) {
            out.push(format!("3: relation {} dangling target", r.id));
        }
        if !r.world_id.is_empty() {
            if let Some(src) = by_id.get(r.source_object_id.as_str()) {
                let w = src
                    .properties
                    .get("world_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                if w != r.world_id {
                    out.push(format!("4: relation {} world mismatch", r.id));
                }
            }
        }
    }
    for e in &evs {
        let pw = e
            .payload
            .get("world_id")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if !e.world_id.is_empty() && !pw.is_empty() && e.world_id != pw {
            out.push(format!("5: event {} world column/payload disagree", e.id));
        }
        if e.actor.trim().is_empty() || e.actor == "anonymous" {
            out.push(format!("6: event {} unauthenticated actor", e.id));
        }
    }
    // 7: every ActionCompleted correlation resolves to an ActionRequested.
    let requested: HashSet<&str> = evs
        .iter()
        .filter(|e| e.event_type == "ActionRequested")
        .filter_map(|e| e.correlation_id.as_deref())
        .collect();
    for e in evs.iter().filter(|e| e.event_type == "ActionCompleted") {
        match &e.correlation_id {
            Some(c) if requested.contains(c.as_str()) => {}
            _ => out.push(format!("7: event {} correlation unresolvable", e.id)),
        }
    }
    let mut exts = HashSet::new();
    for o in &objs {
        if let Some(e) = &o.external_id {
            if !exts.insert((o.provider_id.clone(), e.clone())) {
                out.push(format!("8: duplicate identity {e}"));
            }
        }
    }
    // 9: live object already recorded deleted (missed delete / resurrection).
    let deleted: HashSet<&str> = evs
        .iter()
        .filter(|e| e.event_type == "FileDeleted")
        .flat_map(|e| e.object_ids.iter().map(|s| s.as_str()))
        .collect();
    for o in &objs {
        if deleted.contains(o.id.as_str()) {
            out.push(format!("9: object {} live but recorded deleted", o.id));
        }
    }
    // 10: event objects share the event world when both are known.
    for e in &evs {
        if e.world_id.is_empty() {
            continue;
        }
        for oid in &e.object_ids {
            if let Some(o) = by_id.get(oid.as_str()) {
                let w = o
                    .properties
                    .get("world_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                if !w.is_empty() && w != e.world_id {
                    out.push(format!("10: event {} crosses into world {w}", e.id));
                }
            }
        }
    }
    // 11: denied mutations carry audit detail.
    for e in evs.iter().filter(|e| e.event_type == "MutationDenied") {
        if e.payload.get("method").is_none() || e.payload.get("capability").is_none() {
            out.push(format!("11: denial {} lacks audit detail", e.id));
        }
    }
    out
}

fn seed_healthy(s: &Storage) -> (Object, Object) {
    let p = Object::new("Project", "cli", json!({"world_id": "W", "name": "P"}));
    let f = Object::new("File", "cli", json!({"world_id": "W", "path": "a"}));
    s.create_object(&p).unwrap();
    s.create_object(&f).unwrap();
    let mut r = Relation::new(&p.id, "contains", &f.id);
    r.world_id = "W".into();
    s.create_relation(&r).unwrap();
    let mut e = Event::new("ObjectCreated", "cli", json!({"world_id": "W"}));
    e.world_id = "W".into();
    e.object_ids = vec![f.id.clone()];
    s.record_event(&e).unwrap();
    (p, f)
}

#[test]
fn checker_clean_on_healthy_world() {
    let s = fresh("k-ok");
    seed_healthy(&s);
    assert!(check(&s).is_empty(), "healthy world must have no findings");
}

#[test]
fn checker_finds_dangling_relation() {
    let s = fresh("k-dangle");
    seed_healthy(&s);
    // Phase B: FK rejects API-level dangling; the checker stays the
    // safety net for legacy/foreign databases. Prepare the orphan like a
    // pre-FK writer would: raw SQL with FK enforcement off.
    let db = tmp_db("k-dangle");
    let conn = rusqlite::Connection::open(&db).unwrap();
    // rusqlite turns FK enforcement on by default; a pre-FK writer had it off.
    conn.execute_batch("PRAGMA foreign_keys=OFF;").unwrap();
    conn.execute(
        "INSERT INTO relations (id, source_object_id, relation_type, target_object_id, properties, created_at, world_id) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        rusqlite::params![
            "legacy-dangling",
            "ghost",
            "contains",
            "ghost2",
            "{}",
            chrono::Utc::now().to_rfc3339(),
            ""
        ],
    )
    .unwrap();
    drop(conn);
    let f = check(&s);
    assert!(f.iter().any(|x| x.starts_with("3:")), "rule 3 fires: {f:?}");
}

#[test]
fn checker_finds_world_mismatch() {
    let s = fresh("k-world");
    let (_p, f) = seed_healthy(&s);
    // Source lives in OTHER but the relation claims W.
    let g = Object::new("File", "cli", json!({"world_id": "OTHER"}));
    s.create_object(&g).unwrap();
    let mut r = Relation::new(&g.id, "contains", &f.id);
    r.world_id = "W".into();
    s.create_relation(&r).unwrap();
    let found = check(&s);
    assert!(
        found.iter().any(|x| x.starts_with("4:")),
        "rule 4 fires: {found:?}"
    );
}

#[test]
fn checker_finds_duplicate_agent() {
    let s = fresh("k-agent");
    for _ in 0..2 {
        let mut a = Object::new("Agent", "demo", json!({}));
        a.external_id = Some("def".into());
        s.create_object(&a).unwrap();
    }
    let f = check(&s);
    assert!(f.iter().any(|x| x.starts_with("2:")), "rule 2 fires: {f:?}");
}

#[test]
fn checker_finds_stale_live_object() {
    let s = fresh("k-stale");
    let (_p, f) = seed_healthy(&s);
    let mut e = Event::new("FileDeleted", "filesystem", json!({}));
    e.object_ids = vec![f.id.clone()];
    s.record_event(&e).unwrap();
    let found = check(&s);
    assert!(
        found.iter().any(|x| x.starts_with("9:")),
        "rule 9 fires: {found:?}"
    );
}

#[test]
fn checker_finds_broken_correlation() {
    let s = fresh("k-corr");
    let e = Event::new("ActionCompleted", "cli", json!({}));
    // No ActionRequested with a correlation; leave correlation None.
    s.record_event(&e).unwrap();
    let found = check(&s);
    assert!(
        found.iter().any(|x| x.starts_with("7:")),
        "rule 7 fires: {found:?}"
    );
}

#[test]
fn checker_finds_cross_world_event() {
    let s = fresh("k-xw");
    let (_p, f) = seed_healthy(&s);
    let mut e = Event::new("ObjectCreated", "cli", json!({}));
    e.world_id = "FOREIGN".into();
    e.object_ids = vec![f.id.clone()];
    s.record_event(&e).unwrap();
    let found = check(&s);
    assert!(
        found.iter().any(|x| x.starts_with("10:")),
        "rule 10 fires: {found:?}"
    );
}
