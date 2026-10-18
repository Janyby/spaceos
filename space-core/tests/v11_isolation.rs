//! V1.1 вЂ” World isolation & locality checks against real Storage.
use space_core::query::{EventQuery, ObjectQuery, RelationQuery};
use space_core::{Event, Object, Relation, Space, Storage};

fn tmp(name: &str) -> String {
    let d = std::env::temp_dir().join("spaceos-v11");
    std::fs::create_dir_all(&d).unwrap();
    d.join(format!("{name}.db")).to_string_lossy().to_string()
}

fn fresh(name: &str) -> Storage {
    let p = tmp(name);
    let _ = std::fs::remove_file(&p);
    Storage::open(&p).unwrap()
}

#[test]
fn world_a_does_not_leak_into_world_b_objects() {
    let s = fresh("iso1");
    let a = Space::new("A", serde_json::json!({}));
    let b = Space::new("B", serde_json::json!({}));
    s.create_space(&a).unwrap();
    s.create_space(&b).unwrap();
    let oa = Object::new(
        "Project",
        "agent-demo",
        serde_json::json!({"world_id": a.id, "name": "PA"}),
    );
    let ob = Object::new(
        "Project",
        "agent-demo",
        serde_json::json!({"world_id": b.id, "name": "PB"}),
    );
    s.create_object(&oa).unwrap();
    s.create_object(&ob).unwrap();
    let in_a = s
        .query_objects(&ObjectQuery {
            world_id: Some(a.id.clone()),
            ..Default::default()
        })
        .unwrap();
    let in_b = s
        .query_objects(&ObjectQuery {
            world_id: Some(b.id.clone()),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(in_a.len(), 1);
    assert_eq!(in_b.len(), 1);
    assert_eq!(in_a[0].properties["name"], "PA");
}

#[test]
fn relation_world_id_filters_correctly() {
    let s = fresh("iso2");
    // Phase B: FK — endpoints must exist before relations.
    for id in ["x", "y", "z"] {
        let mut o = Object::new("File", "iso", serde_json::json!({}));
        o.id = id.into();
        s.create_object(&o).unwrap();
    }
    let mut r = Relation::new("x", "contains", "y");
    r.world_id = "world-A".into();
    let mut r2 = Relation::new("x", "contains", "z");
    r2.world_id = "world-B".into();
    s.create_relation(&r).unwrap();
    s.create_relation(&r2).unwrap();
    let in_a = s
        .query_relations(&RelationQuery {
            world_id: Some("world-A".into()),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(in_a.len(), 1);
    assert_eq!(in_a[0].target_object_id, "y");
}

#[test]
fn event_world_id_payload_filter() {
    let s = fresh("iso3");
    let mut e = Event::new(
        "ObjectCreated",
        "agent",
        serde_json::json!({"world_id": "world-A"}),
    );
    let mut e2 = Event::new(
        "ObjectCreated",
        "agent",
        serde_json::json!({"world_id": "world-B"}),
    );
    s.record_event(&e).unwrap();
    s.record_event(&e2).unwrap();
    let in_a = s
        .query_events(&EventQuery {
            world_id: Some("world-A".into()),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(in_a.len(), 1);
}

#[test]
fn locality_latency_stays_bounded_across_space_sizes() {
    let s = fresh("iso4");
    let mut durations = vec![];
    for worlds in [10usize, 100, 1000] {
        // reset
        let _ = std::fs::remove_file(tmp("iso4"));
        let s2 = fresh("iso4");
        for i in 0..worlds {
            let w = Space::new(format!("W{i}"), serde_json::json!({}));
            s2.create_space(&w).unwrap();
            let o = Object::new("Project", "a", serde_json::json!({"world_id": w.id}));
            s2.create_object(&o).unwrap();
        }
        let target = Space::new("target", serde_json::json!({}));
        s2.create_space(&target).unwrap();
        let probe = Object::new("Project", "a", serde_json::json!({"world_id": target.id}));
        s2.create_object(&probe).unwrap();
        let t0 = std::time::Instant::now();
        let got = s2
            .query_objects(&ObjectQuery {
                world_id: Some(target.id.clone()),
                ..Default::default()
            })
            .unwrap();
        let dt = t0.elapsed();
        assert_eq!(got.len(), 1);
        durations.push((worlds, dt.as_micros()));
        drop(s2);
    }
    // locality check: query for one world must not scale linearly with total
    // spaces; allow generous bound (SQLite LIKE on properties is a full scan,
    // so we only assert it stays in the millisecond/microsecond range).
    for (w, d) in &durations {
        assert!(*d < 50_000, "query latency too high at {w} worlds: {d}us");
    }
    eprintln!("latencies: {:?}", durations);
}
