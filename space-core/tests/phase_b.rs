//! Phase B: unified event model + SQLite integration.
//!
//! Covers exactly what the phase requires: migration of a pre-existing
//! DB (rows + text ids preserved, repeatable), transaction rollback,
//! observation redelivery, reopen persistence, cursor semantics,
//! event-id collision, orphan preservation, relation-graph safety on
//! object update, and the legacy `record_event` shape.
#![cfg(windows)]

use rusqlite::Connection;
use space_core::query::EventQuery;
use space_core::{Event, Object, Observation, Relation, Storage};

fn tmp(name: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join("spaceos-phase-b").join(name);
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn obj(id: &str, props: serde_json::Value) -> Object {
    let mut o = Object::new("File", "filesystem", props);
    o.id = id.to_string();
    o.external_id = Some(format!("ext:{id}"));
    o
}

fn rel(id: &str, src: &str, dst: &str) -> Relation {
    let mut r = Relation::new(src, "contains", dst);
    r.id = id.to_string();
    r
}

fn ev(id: &str, corr: &str) -> Event {
    let mut e = Event::new("FileCreated", "filesystem", serde_json::json!({"f": id}));
    e.id = id.to_string();
    e.correlation_id = Some(corr.to_string());
    e
}

fn obs(id: &str, event_id: &str) -> Observation {
    Observation {
        observation_id: id.to_string(),
        world_id: "w1".to_string(),
        facts: vec![serde_json::json!({"seen": event_id})],
        objects: vec![obj("o1", serde_json::json!({"p": 1}))],
        relations: vec![],
        delete_ids: vec![],
        event: ev(event_id, id),
    }
}

/// A genuinely legacy database: the v1 shape (TEXT PK events, no seq,
/// no observation_log, no indexes), written with raw SQL exactly as the
/// old `init()` + old writers left it.
fn write_legacy_db(path: &std::path::Path) {
    let conn = Connection::open(path).unwrap();
    conn.execute_batch(
        "CREATE TABLE objects (id TEXT PRIMARY KEY, type TEXT NOT NULL, properties TEXT NOT NULL, provider_id TEXT NOT NULL, external_id TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
         CREATE TABLE relations (id TEXT PRIMARY KEY, source_object_id TEXT NOT NULL, relation_type TEXT NOT NULL, target_object_id TEXT NOT NULL, properties TEXT NOT NULL, created_at TEXT NOT NULL, world_id TEXT DEFAULT '');
         CREATE TABLE events (id TEXT PRIMARY KEY, event_type TEXT NOT NULL, actor TEXT NOT NULL, provider_id TEXT, object_ids TEXT NOT NULL, payload TEXT NOT NULL, timestamp TEXT NOT NULL, correlation_id TEXT, world_id TEXT DEFAULT '');
         CREATE TABLE providers (id TEXT PRIMARY KEY, name TEXT NOT NULL, capabilities TEXT NOT NULL);
         CREATE TABLE spaces (id TEXT PRIMARY KEY, name TEXT NOT NULL, metadata TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL);",
    )
    .unwrap();
    conn.execute(
        "INSERT INTO objects VALUES ('lo1','File','{\"p\":1}','filesystem','ext:lo1','2026-01-01T00:00:00+00:00','2026-01-01T00:00:00+00:00')",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO relations VALUES ('lr1','lo1','contains','ghost','{}','2026-01-01T00:00:00+00:00','w1')",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO events VALUES ('le1','FileCreated','filesystem','filesystem','[]','{\"f\":1}','2026-01-01T00:00:00+00:00','c1','w1')",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO events VALUES ('le2','FileCreated','filesystem','filesystem','[]','{\"f\":2}','2026-01-02T00:00:00+00:00','c2','w1')",
        [],
    )
    .unwrap();
}

#[test]
fn migration_preserves_legacy_rows_and_ids() {
    let dir = tmp("migrate");
    let db = dir.join("core.db");
    write_legacy_db(&db);
    let s = Storage::open(&db).unwrap();
    // Rows intact with identical text ids.
    assert!(s.get_object("lo1").unwrap().is_some());
    let evs = s
        .query_events(&EventQuery {
            limit: None,
            ..Default::default()
        })
        .unwrap();
    assert_eq!(evs.len(), 2);
    assert!(evs.iter().any(|e| e.id == "le1"));
    assert!(evs.iter().any(|e| e.id == "le2"));
    // Every row (old included) carries a durable cursor, ordered.
    assert!(evs.iter().all(|e| e.seq >= 1));
    let mut seqs: Vec<u64> = evs.iter().map(|e| e.seq).collect();
    seqs.sort_unstable();
    assert_eq!(seqs, vec![1, 2]);
    // Grandfathered orphan survives migration (reported, not deleted).
    let rels = s
        .query_relations(&space_core::query::RelationQuery::default())
        .unwrap();
    assert_eq!(rels.len(), 1);
    assert_eq!(rels[0].id, "lr1");
    assert!(!s.fk_violations().unwrap().is_empty());
}

#[test]
fn migration_repeatable_and_versioned() {
    let dir = tmp("migrate-twice");
    let db = dir.join("core.db");
    write_legacy_db(&db);
    {
        let _ = Storage::open(&db).unwrap();
    }
    {
        let s = Storage::open(&db).unwrap();
        let evs = s
            .query_events(&EventQuery {
                limit: None,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(evs.len(), 2, "re-migration must not duplicate rows");
        let conn = Connection::open(&db).unwrap();
        let v: i64 = conn
            .query_row("PRAGMA user_version;", [], |r| r.get(0))
            .unwrap();
        assert_eq!(v, space_core::storage::SCHEMA_VERSION);
    }
}

#[test]
fn tx_rollback_on_bad_relation() {
    let dir = tmp("rollback");
    let mut s = Storage::open(&dir.join("core.db")).unwrap();
    let before_objs = s
        .query_objects(&space_core::query::ObjectQuery::default())
        .unwrap()
        .len();
    let r = s.transaction(|tx| {
        tx.create_object(&obj("ro1", serde_json::json!({})))?;
        // Dangling endpoint: FK violation must abort the whole batch.
        tx.create_relation(&rel("rr1", "ro1", "missing"))?;
        tx.record_event(&ev("re1", "c"))?;
        Ok(())
    });
    assert!(r.is_err(), "dangling relation must fail the transaction");
    // Nothing partially applied: no object, no event.
    assert!(s.get_object("ro1").unwrap().is_none());
    assert_eq!(
        s.query_objects(&space_core::query::ObjectQuery::default())
            .unwrap()
            .len(),
        before_objs
    );
    assert!(s
        .query_events(&EventQuery {
            limit: None,
            ..Default::default()
        })
        .unwrap()
        .iter()
        .all(|e| e.id != "re1"));
}

#[test]
fn observation_repeat_no_dupes() {
    let dir = tmp("redelivery");
    let mut s = Storage::open(&dir.join("core.db")).unwrap();
    let o1 = s.apply_observation(&obs("obs-1", "ev-1")).unwrap();
    assert!(o1.applied);
    assert_eq!(o1.event_id, "ev-1");
    assert!(o1.seq >= 1);
    let o2 = s.apply_observation(&obs("obs-1", "ev-1")).unwrap();
    assert!(!o2.applied, "redelivery must be recognized");
    let evs = s
        .query_events(&EventQuery {
            limit: None,
            ..Default::default()
        })
        .unwrap();
    assert_eq!(evs.len(), 1, "exactly one event row");
    assert_eq!(evs[0].id, "ev-1");
    // Facts landed in the event payload for audit.
    assert_eq!(evs[0].payload["facts"][0]["seen"], "ev-1");
}

#[test]
fn reopen_persists_everything() {
    let dir = tmp("reopen");
    let db = dir.join("core.db");
    let seq = {
        let mut s = Storage::open(&db).unwrap();
        s.apply_observation(&obs("obs-r", "ev-r")).unwrap().seq
    };
    let _ = seq;
    let s2 = Storage::open(&db).unwrap();
    assert!(s2.get_object("o1").unwrap().is_some());
    let evs = s2
        .query_events(&EventQuery {
            limit: None,
            ..Default::default()
        })
        .unwrap();
    assert_eq!(evs.len(), 1);
    assert_eq!(evs[0].seq, 1);
    assert_eq!(s2.max_seq().unwrap(), 1);
}

#[test]
fn cursor_order_after_seq_next_seq_limits() {
    let dir = tmp("cursor");
    let mut s = Storage::open(&dir.join("core.db")).unwrap();
    for i in 1..=5u32 {
        let mut o = obs(&format!("obs-c{i}"), &format!("ev-c{i}"));
        o.objects = vec![];
        s.apply_observation(&o).unwrap();
    }
    assert_eq!(s.max_seq().unwrap(), 5);
    let page = s
        .query_events(&EventQuery {
            after_seq: Some(2),
            limit: None,
            ..Default::default()
        })
        .unwrap();
    assert_eq!(page.len(), 3);
    assert_eq!(page[0].seq, 3);
    assert_eq!(page[2].seq, 5);
    // next_seq convention: last row's seq; empty page reuses after_seq.
    let next_seq = page.last().map(|e| e.seq).unwrap();
    assert_eq!(next_seq, 5);
    let empty = s
        .query_events(&EventQuery {
            after_seq: Some(5),
            limit: None,
            ..Default::default()
        })
        .unwrap();
    assert!(empty.is_empty());
    // Correlation filter composes with the cursor.
    let one = s
        .query_events(&EventQuery {
            correlation_id: Some("obs-c4".to_string()),
            limit: None,
            ..Default::default()
        })
        .unwrap();
    assert_eq!(one.len(), 1);
    assert_eq!(one[0].id, "ev-c4");
    // Limit clamp: ask absurd, get at most MAX_EVENT_LIMIT.
    for i in 6..=515u32 {
        let mut o = obs(&format!("obs-c{i}"), &format!("ev-c{i}"));
        o.objects = vec![];
        s.apply_observation(&o).unwrap();
    }
    let big = s
        .query_events(&EventQuery {
            limit: Some(1_000_000),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(big.len(), space_core::storage::MAX_EVENT_LIMIT);
}

#[test]
fn event_id_collision_on_new_observation_fails() {
    let dir = tmp("collision");
    let mut s = Storage::open(&dir.join("core.db")).unwrap();
    s.apply_observation(&obs("obs-a", "ev-dup")).unwrap();
    // New observation, same event id: caller bug → loud error, and the
    // new observation's objects must NOT be applied either.
    let mut bad = obs("obs-b", "ev-dup");
    bad.objects = vec![obj("o-sneaky", serde_json::json!({}))];
    assert!(s.apply_observation(&bad).is_err());
    assert!(s.get_object("o-sneaky").unwrap().is_none());
    let evs = s
        .query_events(&EventQuery {
            limit: None,
            ..Default::default()
        })
        .unwrap();
    assert_eq!(evs.len(), 1);
}

#[test]
fn upsert_object_preserves_relations() {
    let dir = tmp("graph-safe");
    let mut s = Storage::open(&dir.join("core.db")).unwrap();
    let mut o = obs("obs-g", "ev-g");
    o.objects = vec![
        obj("parent", serde_json::json!({"v": 1})),
        obj("child", serde_json::json!({})),
    ];
    o.relations = vec![rel("rel-g", "parent", "child")];
    s.apply_observation(&o).unwrap();
    // Update the object (same id, new props): relations must survive.
    let mut o2 = obs("obs-g2", "ev-g2");
    o2.objects = vec![obj("parent", serde_json::json!({"v": 2}))];
    s.apply_observation(&o2).unwrap();
    let rels = s
        .query_relations(&space_core::query::RelationQuery::default())
        .unwrap();
    assert_eq!(rels.len(), 1);
    assert_eq!(rels[0].id, "rel-g");
    let objs = s
        .query_objects(&space_core::query::ObjectQuery::default())
        .unwrap();
    assert_eq!(objs.len(), 2, "no duplicate object rows");
    assert_eq!(
        objs.iter().find(|x| x.id == "parent").unwrap().properties["v"],
        2
    );
    assert!(s.fk_violations().unwrap().is_empty());
}

#[test]
fn record_event_legacy_shape_single_row() {
    let dir = tmp("legacy-event");
    let s = Storage::open(&dir.join("core.db")).unwrap();
    s.record_event(&ev("ev-leg", "c")).unwrap();
    s.record_event(&ev("ev-leg", "c")).unwrap();
    let evs = s
        .query_events(&EventQuery {
            limit: None,
            ..Default::default()
        })
        .unwrap();
    assert_eq!(evs.len(), 1, "redelivery keeps one row, no overwrite dupes");
    // Legacy callers only need ok/err; cursor is additive.
    assert!(evs[0].seq >= 1);
}

#[test]
fn observation_requires_id() {
    let dir = tmp("obs-id");
    let mut s = Storage::open(&dir.join("core.db")).unwrap();
    let mut o = obs("", "ev-x");
    o.observation_id = "   ".to_string();
    assert!(s.apply_observation(&o).is_err());
}

#[test]
fn observation_delete_ids_atomic_with_event() {
    let dir = tmp("obs-delete");
    let mut s = Storage::open(&dir.join("core.db")).unwrap();
    // Seed: one object to delete later.
    s.apply_observation(&obs("obs-seed", "ev-seed")).unwrap();
    assert!(s.get_object("o1").unwrap().is_some());
    // Atomic delete + event in one observation.
    let mut del = obs("obs-del", "ev-del");
    del.objects = vec![];
    del.delete_ids = vec!["o1".to_string()];
    let out = s.apply_observation(&del).unwrap();
    assert!(out.applied);
    assert!(s.get_object("o1").unwrap().is_none(), "object deleted");
    let evs = s
        .query_events(&EventQuery {
            limit: None,
            ..Default::default()
        })
        .unwrap();
    assert_eq!(evs.len(), 2, "seed event + delete event, nothing else");
    // Failed observation (event collision) must NOT delete either.
    let mut reseed = obs("obs-reseed", "ev-reseed");
    reseed.objects = vec![obj("keeper", serde_json::json!({}))];
    s.apply_observation(&reseed).unwrap();
    let mut bad2 = obs("obs-bad2", "ev-reseed");
    bad2.objects = vec![];
    bad2.delete_ids = vec!["keeper".to_string()];
    assert!(
        s.apply_observation(&bad2).is_err(),
        "colliding event id fails"
    );
    assert!(
        s.get_object("keeper").unwrap().is_some(),
        "failed tx deletes nothing"
    );
}

#[test]
fn migration_handles_pre_world_id_relations() {
    // The live runtime DB predates world_id on relations (old init only
    // backfilled events). The rebuild must not fail on such databases.
    let dir = tmp("migrate-old-rel");
    let db = dir.join("core.db");
    {
        let conn = Connection::open(&db).unwrap();
        conn.execute_batch(
            "CREATE TABLE objects (id TEXT PRIMARY KEY, type TEXT NOT NULL, properties TEXT NOT NULL, provider_id TEXT NOT NULL, external_id TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
             CREATE TABLE relations (id TEXT PRIMARY KEY, source_object_id TEXT NOT NULL, relation_type TEXT NOT NULL, target_object_id TEXT NOT NULL, properties TEXT NOT NULL, created_at TEXT NOT NULL);
             CREATE TABLE events (id TEXT PRIMARY KEY, event_type TEXT NOT NULL, actor TEXT NOT NULL, provider_id TEXT, object_ids TEXT NOT NULL, payload TEXT NOT NULL, timestamp TEXT NOT NULL, correlation_id TEXT, world_id TEXT DEFAULT '');
             CREATE TABLE providers (id TEXT PRIMARY KEY, name TEXT NOT NULL, capabilities TEXT NOT NULL);
             CREATE TABLE spaces (id TEXT PRIMARY KEY, name TEXT NOT NULL, metadata TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL);",
        )
        .unwrap();
        conn.execute(
            "INSERT INTO objects VALUES ('o1','File','{}','filesystem','ext:o1','2026-01-01T00:00:00+00:00','2026-01-01T00:00:00+00:00')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO relations VALUES ('r1','o1','contains','o1','{}','2026-01-01T00:00:00+00:00')",
            [],
        )
        .unwrap();
    }
    let mut s = Storage::open(&db).unwrap();
    let rels = s
        .query_relations(&space_core::query::RelationQuery::default())
        .unwrap();
    assert_eq!(rels.len(), 1);
    assert_eq!(rels[0].id, "r1");
    // New writes work (FK targets exist).
    let mut o = obs("obs-wid", "ev-wid");
    o.objects = vec![obj("n1", serde_json::json!({}))];
    o.relations = vec![{
        let mut r = rel("nr1", "o1", "n1");
        r.world_id = "w1".to_string();
        r
    }];
    assert!(s.apply_observation(&o).is_ok());
}
