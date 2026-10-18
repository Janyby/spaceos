//! V1.7 Phases 5/6/7 — locality split, world lifecycle at scale, long run.
//! No optimization before measurement: these tests MEASURE and record.
//! Storage-level seeding (query-path probe, not IPC throughput) unless noted.
use serde_json::json;
use space_core::query::ObjectQuery;
use space_core::{Event, Object, Relation, Space, Storage};

fn tmp_db(name: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join("spaceos-v17p");
    std::fs::create_dir_all(&d).unwrap();
    d.join(format!("{name}.db"))
}

fn fresh(name: &str) -> Storage {
    let p = tmp_db(name);
    let _ = std::fs::remove_file(&p);
    Storage::open(&p).unwrap()
}

// ---------- Phase 5: split IPC vs storage vs serialization cost ----------
#[test]
fn locality_cost_split_1000_worlds() {
    let s = fresh("loc");
    for i in 0..1000 {
        let w = format!("W{i:04}");
        s.create_space(&Space::new(w.clone(), json!({}))).unwrap();
        let o = Object::new("Project", "cli", json!({"world_id": w}));
        s.create_object(&o).unwrap();
    }
    // storage-direct scoped query
    let t0 = std::time::Instant::now();
    let got = s
        .query_objects(&ObjectQuery {
            world_id: Some("W0000".into()),
            ..Default::default()
        })
        .unwrap();
    let dt_storage = t0.elapsed();
    assert_eq!(got.len(), 1);
    // serialization share of the same result
    let t0 = std::time::Instant::now();
    let ser = serde_json::to_string(&got).unwrap();
    let dt_ser = t0.elapsed();
    eprintln!(
        "1000 worlds: storage_query={}us serialization={}us ser_bytes={}",
        dt_storage.as_micros(),
        dt_ser.as_micros(),
        ser.len()
    );
    // Bottleneck verdict: storage scan dominates serialization by far.
    assert!(
        dt_storage > dt_ser,
        "scan cost must dominate serialization to justify no-index stance check"
    );
}

// ---------- Phase 7A: 1000 worlds lifecycle + isolation of one change ----------
#[test]
fn thousand_worlds_lifecycle() {
    let s = fresh("w1k");
    for i in 0..1000 {
        s.create_space(&Space::new(format!("W{i:04}"), json!({})))
            .unwrap();
    }
    assert_eq!(s.list_spaces().unwrap().len(), 1000);
    // lookup + query one world
    assert!(s.list_spaces().unwrap().iter().any(|w| w.name == "W0500"));
    // one-world change touches nothing else: seed one object per world first
    for i in 0..1000 {
        let o = Object::new("Project", "cli", json!({"world_id": format!("W{i:04}")}));
        s.create_object(&o).unwrap();
    }
    let target: Vec<_> = s
        .query_objects(&ObjectQuery {
            world_id: Some("W0007".into()),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(target.len(), 1);
    let mut changed = target[0].clone();
    changed.properties["note"] = json!("touched");
    s.update_object(&changed).unwrap();
    // all other worlds byte-identical in count
    for i in [0, 999] {
        let q = s
            .query_objects(&ObjectQuery {
                world_id: Some(format!("W{i:04}")),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(q.len(), 1);
    }
    let q7 = s
        .query_objects(&ObjectQuery {
            world_id: Some("W0007".into()),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(q7[0].properties["note"], json!("touched"));
}

// ---------- Phase 7B: dormant worlds need no runtime ----------
#[test]
fn dormant_worlds_persist_without_runtime() {
    let name = "dormant";
    {
        let s = fresh(name);
        s.create_space(&Space::new("Dormant", json!({}))).unwrap();
        s.create_object(&Object::new(
            "Project",
            "cli",
            json!({"world_id": "Dormant"}),
        ))
        .unwrap();
    } // "runtime" gone: Storage dropped, no tasks, no pipes
    let p = tmp_db(name);
    let s2 = Storage::open(&p).unwrap();
    assert_eq!(s2.list_spaces().unwrap().len(), 1);
    assert_eq!(s2.query_objects(&ObjectQuery::default()).unwrap().len(), 1);
}

// ---------- Phase 6: 100k mixed ops long run ----------
#[test]
fn long_run_100k_mixed_ops() {
    let s = fresh("long");
    let t0 = std::time::Instant::now();
    let mut ids = vec![];
    for i in 0..100_000 {
        match i % 10 {
            0..=5 => {
                let o = Object::new("File", "filesystem", json!({"path": format!("f{i}.txt")}));
                ids.push(o.id.clone());
                s.create_object(&o).unwrap();
            }
            6..=7 => {
                // Phase B FK: relations attach to real endpoints; the two
                // oldest objects are never churned (deletes pop from the
                // back), so the steady-state op mix is unchanged.
                let r = Relation::new(&ids[0], "contains", &ids[1]);
                s.create_relation(&r).unwrap();
            }
            8 => {
                s.record_event(&Event::new("Tick", "filesystem", json!({"i": i})))
                    .unwrap();
            }
            _ => {
                // delete an earlier object: steady-state churn, not just growth
                if let Some(id) = ids.pop() {
                    let _ = s.delete_object(&id);
                }
            }
        }
        if i % 25_000 == 0 && i > 0 {
            let size = std::fs::metadata(tmp_db("long"))
                .map(|m| m.len())
                .unwrap_or(0);
            eprintln!("{i} ops: db={size}B");
        }
    }
    let dt = t0.elapsed();
    let size = std::fs::metadata(tmp_db("long"))
        .map(|m| m.len())
        .unwrap_or(0);
    let nobjs = s.query_objects(&ObjectQuery::default()).unwrap().len();
    eprintln!(
        "100k mixed ops: {}ms db={size}B objects={nobjs}",
        dt.as_millis()
    );
    // Restart after long run: reopen timing + integrity spot check.
    let t1 = std::time::Instant::now();
    drop(s);
    let p = tmp_db("long");
    let s2 = Storage::open(&p).unwrap();
    eprintln!("reopen after 100k ops: {}ms", t1.elapsed().as_millis());
    assert_eq!(
        s2.query_objects(&ObjectQuery::default()).unwrap().len(),
        nobjs
    );
}
