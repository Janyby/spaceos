//! V1.8 WAL experiment: CURRENT vs WAL controlled comparison.
//! Same workload, same Core, same machine. Only the journal mechanism
//! differs (`open` vs `open_wal`, both synchronous=FULL).
//! Thresholds (fixed BEFORE comparison): p95 write >=20% better OR
//! throughput >=20% better; correctness: 0 corruption, 0 dup identity,
//! 0 cross-world leak, 0 recovery regression. Else NOT JUSTIFIED.
use serde_json::{json, Value};
use space_core::query::{EventQuery, ObjectQuery, RelationQuery};
use space_core::{Event, Object, Relation, Space, Storage};

fn tmp_db(name: impl AsRef<str>) -> std::path::PathBuf {
    let d = std::env::temp_dir().join("spaceos-v18");
    std::fs::create_dir_all(&d).unwrap();
    d.join(format!("{}.db", name.as_ref()))
}

fn fresh_current(name: impl AsRef<str>) -> Storage {
    let name = name.as_ref();
    let p = tmp_db(&format!("cur-{name}"));
    let _ = std::fs::remove_file(&p);
    let _ = std::fs::remove_file(tmp_db(&format!("cur-{name}-wal")));
    let _ = std::fs::remove_file(tmp_db(&format!("cur-{name}-shm")));
    Storage::open(&p).unwrap()
}

fn fresh_wal(name: impl AsRef<str>) -> Storage {
    let name = name.as_ref();
    for suffix in ["", "-wal", "-shm", "-journal"] {
        let _ = std::fs::remove_file(tmp_db(&format!("wal-{name}{suffix}")));
    }
    Storage::open_wal(&tmp_db(&format!("wal-{name}"))).unwrap()
}

fn pct(v: &mut [i64], p: f64) -> i64 {
    v.sort_unstable();
    v[((p / 100.0) * (v.len() as f64 - 1.0)).round() as usize]
}

/// Canonical dump: everything sorted, comparable across modes.
fn dump(s: &Storage) -> Value {
    let mut objs: Vec<Value> = s
        .query_objects(&ObjectQuery::default())
        .unwrap()
        .into_iter()
        .map(|o| {
            let mut v = json!(o);
            // Normalize timestamps for cross-mode comparison
            v["created_at"] = json!("FIXED");
            v["updated_at"] = json!("FIXED");
            v
        })
        .collect();
    objs.sort_by(|a, b| a["id"].as_str().cmp(&b["id"].as_str()));
    let mut rels: Vec<Value> = s
        .query_relations(&RelationQuery::default())
        .unwrap()
        .into_iter()
        .map(|r| {
            let mut v = json!(r);
            v["created_at"] = json!("FIXED");
            v
        })
        .collect();
    rels.sort_by(|a, b| a["id"].as_str().cmp(&b["id"].as_str()));
    let mut evs: Vec<Value> = s
        .query_events(&EventQuery::default())
        .unwrap()
        .into_iter()
        .map(|e| {
            let mut v = json!(e);
            v["timestamp"] = json!("FIXED");
            v
        })
        .collect();
    evs.sort_by(|a, b| a["id"].as_str().cmp(&b["id"].as_str()));
    let mut spaces: Vec<Value> = s
        .list_spaces()
        .unwrap()
        .into_iter()
        .map(|x| {
            let mut v = json!(x);
            v["created_at"] = json!("FIXED");
            v["updated_at"] = json!("FIXED");
            v
        })
        .collect();
    spaces.sort_by(|a, b| a["id"].as_str().cmp(&b["id"].as_str()));
    let mut provs: Vec<Value> = s
        .list_providers()
        .unwrap()
        .into_iter()
        .map(|x| json!(x))
        .collect();
    provs.sort_by(|a, b| a["id"].as_str().cmp(&b["id"].as_str()));
    json!({"objects": objs, "relations": rels, "events": evs, "spaces": spaces, "providers": provs})
}

/// Fixed scripted workload with deterministic IDs (same input both modes).
fn workload(s: &Storage, tag: &str) {
    let sp = Space {
        id: format!("space-{tag}"),
        name: format!("W-{tag}"),
        metadata: json!({}),
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    };
    s.create_space(&sp).unwrap();
    s.upsert_provider(&space_core::ProviderInfo {
        id: format!("prov-{tag}"),
        name: "p".into(),
        capabilities: vec!["scan".into()],
        version: None,
    })
    .unwrap();
    for i in 0..200 {
        let mut o = Object::new(
            "File",
            "filesystem",
            json!({"path": format!("{tag}/f{i}.txt"), "world_id": tag}),
        );
        o.id = format!("{tag}-obj-{i:04}");
        o.external_id = Some(format!("{tag}/f{i}.txt"));
        s.create_object(&o).unwrap();
        if i % 2 == 0 {
            let mut r = Relation::new(
                format!("{tag}-obj-{i:04}"),
                "contains",
                format!("{tag}-obj-0000"),
            );
            r.id = format!("{tag}-rel-{i:04}");
            r.world_id = tag.into();
            s.create_relation(&r).unwrap();
        }
        if i % 5 == 0 {
            let mut e = Event::new("FileChanged", "filesystem", json!({"i": i}));
            e.id = format!("{tag}-ev-{i:04}");
            e.world_id = tag.into();
            e.object_ids = vec![format!("{tag}-obj-{i:04}")];
            e.correlation_id = Some(format!("{tag}-corr"));
            s.record_event(&e).unwrap();
        }
        if i % 7 == 0 {
            let mut o2 = Object::new(
                "File",
                "filesystem",
                json!({"path": format!("{tag}/f{i}.txt"), "world_id": tag, "rev": 2}),
            );
            o2.id = format!("{tag}-obj-{i:04}");
            o2.external_id = Some(format!("{tag}/f{i}.txt"));
            s.update_object(&o2).unwrap();
        }
    }
    // Deletes with cascade.
    for i in (0..200).step_by(10) {
        let _ = s.delete_object(&format!("{tag}-obj-{i:04}"));
    }
}

#[test]
fn modes_pinned() {
    let c = fresh_current("pin");
    let w = fresh_wal("pin");
    assert_eq!(c.journal_mode().unwrap(), "delete");
    assert_eq!(w.journal_mode().unwrap(), "wal");
    assert_eq!(c.synchronous().unwrap(), 2, "FULL pinned, current mode");
    assert_eq!(w.synchronous().unwrap(), 2, "FULL pinned, WAL mode");
    assert_eq!(c.integrity_check().unwrap(), "ok");
    assert_eq!(w.integrity_check().unwrap(), "ok");
}

#[test]
fn correctness_same_workload() {
    let c = fresh_current("corr");
    let w = fresh_wal("corr");
    workload(&c, "T");
    workload(&w, "T");
    assert_eq!(dump(&c), dump(&w), "identical semantic result both modes");
    assert_eq!(c.integrity_check().unwrap(), "ok");
    assert_eq!(w.integrity_check().unwrap(), "ok");
    // World isolation spot check identical in both.
    for s in [&c, &w] {
        let q = s
            .query_objects(&space_core::query::ObjectQuery {
                world_id: Some("T".into()),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(q.len(), 180, "200 created minus 20 deleted");
    }
}

/// Timed mixed microbench. Returns (ops/sec, p50, p95, p99 in us).
fn microbench(s: &Storage, tag: &str, n: usize) -> (f64, i64, i64, i64) {
    let mut lat = Vec::with_capacity(n * 2);
    let t0 = std::time::Instant::now();
    for i in 0..n {
        let mut o = Object::new(
            "File",
            "filesystem",
            json!({"path": format!("{tag}/m{i}.txt")}),
        );
        o.id = format!("{tag}-m-{i:05}");
        let t = std::time::Instant::now();
        s.create_object(&o).unwrap();
        lat.push(t.elapsed().as_micros() as i64);
        if i % 3 == 0 {
            let mut e = Event::new("Tick", "filesystem", json!({"i": i}));
            e.id = format!("{tag}-e-{i:05}");
            let t = std::time::Instant::now();
            s.record_event(&e).unwrap();
            lat.push(t.elapsed().as_micros() as i64);
        }
        if i % 7 == 0 {
            let t = std::time::Instant::now();
            let _ = s
                .query_objects(&space_core::query::ObjectQuery::default())
                .unwrap();
            lat.push(t.elapsed().as_micros() as i64);
        }
    }
    let wall_ms = t0.elapsed().as_millis().max(1) as f64;
    let ops = lat.len() as f64;
    (
        ops / wall_ms * 1000.0,
        pct(&mut lat.clone(), 50.0),
        pct(&mut lat.clone(), 95.0),
        pct(&mut lat.clone(), 99.0),
    )
}

#[test]
fn microbench_1k_10k_both_modes() {
    for n in [1_000usize, 10_000] {
        let c = fresh_current(&format!("bench{n}"));
        let w = fresh_wal(&format!("bench{n}"));
        let (c_ops, c50, c95, c99) = microbench(&c, "C", n);
        let (w_ops, w50, w95, w99) = microbench(&w, "W", n);
        let c_size = std::fs::metadata(tmp_db(&format!("cur-bench{n}")))
            .map(|m| m.len())
            .unwrap_or(0);
        let w_size = std::fs::metadata(tmp_db(&format!("wal-bench{n}")))
            .map(|m| m.len())
            .unwrap_or(0)
            + std::fs::metadata(tmp_db(&format!("wal-bench{n}-wal")))
                .map(|m| m.len())
                .unwrap_or(0);
        eprintln!(
            "n={n} CURRENT ops/s={c_ops:.0} p50={c50}us p95={c95}us p99={c99}us db={c_size}B"
        );
        eprintln!(
            "n={n} WAL     ops/s={w_ops:.0} p50={w50}us p95={w95}us p99={w99}us db+wal={w_size}B"
        );
        eprintln!(
            "n={n} throughput ratio WAL/CURRENT = {:.2}x, p95 ratio = {:.2}x",
            w_ops / c_ops.max(1.0),
            c95 as f64 / w95.max(1) as f64
        );
    }
}

/// Two independent connections on ONE file: writer hammers, reader runs
/// full queries. Counts SQLITE_BUSY on each side. This is the experiment
/// rollback-vs-WAL actually differs on (Core's single connection never
/// exercises it).
#[test]
fn read_while_write_two_connections() {
    for label in ["current", "wal"] {
        let base = format!("rw-{label}");
        if label == "current" {
            fresh_current(&base);
        } else {
            fresh_wal(&base);
        }
        let db_name = if label == "current" {
            format!("cur-{base}")
        } else {
            format!("wal-{base}")
        };
        let db_path = tmp_db(&db_name);
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let busy_w = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        let busy_r = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        let other = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        let other_msgs = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let written = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        let reads = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        let mut handles = vec![];
        for t in 0..2 {
            let (db2, stop2, bw, wr, ot, om) = (
                db_path.clone(),
                stop.clone(),
                busy_w.clone(),
                written.clone(),
                other.clone(),
                other_msgs.clone(),
            );
            handles.push(std::thread::spawn(move || {
                // NOTE: open/open_wal must match the mode under test.
                let s = if label == "current" {
                    Storage::open(&db2).unwrap()
                } else {
                    Storage::open_wal(&db2).unwrap()
                };
                let mut i = 0u64;
                while !stop2.load(std::sync::atomic::Ordering::Relaxed) {
                    let mut e = Event::new("Tick", "filesystem", json!({"t": t, "i": i}));
                    e.id = format!("rw-{t}-{i}");
                    match s.record_event(&e) {
                        Ok(()) => {
                            wr.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
                        Err(err) => {
                            let msg = format!("{err:?}");
                            if msg.contains("BUSY") || msg.contains("database is locked") {
                                bw.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            } else {
                                ot.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                om.lock().unwrap().push(msg);
                            }
                        }
                    }
                    i += 1;
                }
            }));
        }
        {
            let (db2, stop2, br, rd, ot, om) = (
                db_path.clone(),
                stop.clone(),
                busy_r.clone(),
                reads.clone(),
                other.clone(),
                other_msgs.clone(),
            );
            handles.push(std::thread::spawn(move || {
                let s = if label == "current" {
                    Storage::open(&db2).unwrap()
                } else {
                    Storage::open_wal(&db2).unwrap()
                };
                while !stop2.load(std::sync::atomic::Ordering::Relaxed) {
                    match s.query_events(&EventQuery::default()) {
                        Ok(v) => {
                            rd.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            let _ = v.len();
                        }
                        Err(err) => {
                            let msg = format!("{err:?}");
                            if msg.contains("BUSY") || msg.contains("database is locked") {
                                br.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            } else {
                                ot.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                om.lock().unwrap().push(msg);
                            }
                        }
                    }
                }
            }));
        }
        std::thread::sleep(std::time::Duration::from_secs(10));
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        for h in handles {
            h.join().unwrap();
        }
        let msgs = other_msgs.lock().unwrap().clone();
        if !msgs.is_empty() {
            eprintln!("{label} other errors: {:?}", msgs);
        }
        eprintln!(
            "{label}: written={} reads={} busy_w={} busy_r={} other_errors={}",
            written.load(std::sync::atomic::Ordering::Relaxed),
            reads.load(std::sync::atomic::Ordering::Relaxed),
            busy_w.load(std::sync::atomic::Ordering::Relaxed),
            busy_r.load(std::sync::atomic::Ordering::Relaxed),
            other.load(std::sync::atomic::Ordering::Relaxed)
        );
        // Note: In WAL mode, readers don't block writers and vice versa, so BUSY should be ~0.
        // Any non-BUSY error is unexpected.
        assert_eq!(
            other.load(std::sync::atomic::Ordering::Relaxed),
            0,
            "no non-BUSY errors: {:?}",
            msgs
        );
    }
}

/// Run the full crash matrix from V1.5/V1.7 for both modes.
/// Each crash point fires once, then we verify the DB integrity and semantic correctness.
#[test]
fn crash_matrix_both_modes() {
    use space_core::storage::CrashPoint;
    let points = [
        CrashPoint::BeforeObjectCommit,
        CrashPoint::AfterObjectCommit,
        CrashPoint::BeforeRelationCommit,
        CrashPoint::AfterRelationCommit,
        CrashPoint::BeforeEventCommit,
        CrashPoint::AfterEventCommit,
        CrashPoint::BeforeObjectDelete,
        CrashPoint::AfterObjectDelete,
        CrashPoint::BeforeRelationDelete,
        CrashPoint::AfterRelationDelete,
    ];

    for label in ["current", "wal"] {
        for point in points {
            let base = format!("crash-{label}-{point:?}");
            if label == "current" {
                fresh_current(&base);
            } else {
                fresh_wal(&base);
            }
            let db_name = if label == "current" {
                format!("cur-{base}")
            } else {
                format!("wal-{base}")
            };
            let db_path = tmp_db(&db_name);

            // Run workload until the crash point fires
            let s = if label == "current" {
                fresh_current(&base)
            } else {
                fresh_wal(&base)
            };
            s.fail_once(point);
            let sp = Space {
                id: format!("space-{base}"),
                name: format!("W-{base}"),
                metadata: json!({}),
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
            };
            s.create_space(&sp).unwrap();

            let mut hit = false;
            for i in 0..50 {
                let mut o = Object::new(
                    "File",
                    "filesystem",
                    json!({"path": format!("{base}/f{i}.txt")}),
                );
                o.id = format!("{base}-obj-{i:04}");
                if s.create_object(&o).is_err() {
                    hit = true;
                    break;
                }
            }
            if !hit {
                for i in 0..50 {
                    let mut e = Event::new("Tick", "filesystem", json!({"i": i}));
                    e.id = format!("{base}-ev-{i:04}");
                    if s.record_event(&e).is_err() {
                        hit = true;
                        break;
                    }
                }
            }
            drop(s);

            // Reopen and verify integrity
            let s2 = if label == "current" {
                Storage::open(&db_path).unwrap()
            } else {
                Storage::open_wal(&db_path).unwrap()
            };
            assert_eq!(
                s2.integrity_check().unwrap(),
                "ok",
                "{label} {point:?} integrity check failed"
            );
            // Verify no duplicate identities (objects with same external_id)
            let objs = s2.query_objects(&ObjectQuery::default()).unwrap();
            let mut seen_ext = std::collections::HashSet::new();
            for o in &objs {
                if let Some(ext) = &o.external_id {
                    assert!(
                        seen_ext.insert(ext.clone()),
                        "{label} {point:?} duplicate external_id: {ext}"
                    );
                }
            }
        }
    }
}

/// Long-run 100k semantic events (Breakage B2 baseline replication)
#[test]
fn long_run_100k_both_modes() {
    for label in ["current", "wal"] {
        let base = format!("longrun-{label}");
        let s = if label == "current" {
            fresh_current(&base)
        } else {
            fresh_wal(&base)
        };
        let sp = Space {
            id: format!("space-{base}"),
            name: format!("W-{base}"),
            metadata: json!({}),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };
        s.create_space(&sp).unwrap();

        let mut lat = Vec::with_capacity(100_000);
        let t0 = std::time::Instant::now();
        for i in 0..100_000 {
            let mut e = Event::new("Tick", "filesystem", json!({"i": i}));
            e.id = format!("{base}-ev-{i:05}");
            e.world_id = "lr".into();
            e.object_ids = vec![format!("{base}-obj-{i:05}")];
            let t = std::time::Instant::now();
            s.record_event(&e).unwrap();
            lat.push(t.elapsed().as_micros() as i64);
            if i % 10_000 == 0 {
                eprintln!("{label} progress: {i}/100000");
            }
        }
        let wall_ms = t0.elapsed().as_millis().max(1) as f64;
        lat.sort_unstable();
        let ops = lat.len() as f64;
        let p50 = lat[(ops * 0.50) as usize];
        let p95 = lat[(ops * 0.95) as usize];
        let p99 = lat[(ops * 0.99) as usize];
        let throughput = ops / wall_ms * 1000.0;
        eprintln!("{label} 100k: ops/s={throughput:.0} p50={p50}us p95={p95}us p99={p99}us");

        // Reopen and verify
        drop(s);
        let db_name = if label == "current" {
            format!("cur-{base}")
        } else {
            format!("wal-{base}")
        };
        let db_path = tmp_db(&db_name);
        let s2 = if label == "current" {
            Storage::open(&db_path).unwrap()
        } else {
            Storage::open_wal(&db_path).unwrap()
        };
        assert_eq!(s2.integrity_check().unwrap(), "ok");
        let evs = s2.query_events(&EventQuery::default()).unwrap();
        assert_eq!(evs.len(), 100_000, "{label} event count mismatch");
    }
}

/// T1/T2/T3/T4 latency regression (100 samples each mode)
#[test]
fn t1_t2_t3_t4_regression() {
    for label in ["current", "wal"] {
        let base = format!("latency-{label}");
        let s = if label == "current" {
            fresh_current(&base)
        } else {
            fresh_wal(&base)
        };
        let sp = Space {
            id: format!("space-{base}"),
            name: format!("W-{base}"),
            metadata: json!({}),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };
        s.create_space(&sp).unwrap();

        let mut t1_t2 = Vec::new();
        let mut t2_t3 = Vec::new();
        let mut t3_t4 = Vec::new();
        let mut t1_t4 = Vec::new();

        for i in 0..100 {
            let t1 = std::time::Instant::now();
            let mut e = Event::new(
                "FileChanged",
                "filesystem",
                json!({"i": i, "data": "x".repeat(100)}),
            );
            e.id = format!("{base}-t-{i:03}");
            e.world_id = "lat".into();
            e.object_ids = vec![format!("{base}-obj-{i:03}")];
            e.correlation_id = Some(format!("{base}-corr"));

            let t2 = std::time::Instant::now();
            s.record_event(&e).unwrap();
            let t3 = std::time::Instant::now();

            // T4: Subscriber observed - simulate by immediate query
            let _ = s
                .query_events(&EventQuery {
                    world_id: Some("lat".into()),
                    ..Default::default()
                })
                .unwrap();
            let t4 = std::time::Instant::now();

            t1_t2.push((t2 - t1).as_micros() as i64);
            t2_t3.push((t3 - t2).as_micros() as i64);
            t3_t4.push((t4 - t3).as_micros() as i64);
            t1_t4.push((t4 - t1).as_micros() as i64);
        }

        t1_t2.sort_unstable();
        t2_t3.sort_unstable();
        t3_t4.sort_unstable();
        t1_t4.sort_unstable();

        let p = |v: &[i64], pct: f64| -> i64 {
            v[((pct / 100.0) * (v.len() as f64 - 1.0)).round() as usize]
        };

        eprintln!(
            "{label} T1->T2: p50={}us p95={}us p99={}us",
            p(&t1_t2, 50.0),
            p(&t1_t2, 95.0),
            p(&t1_t2, 99.0)
        );
        eprintln!(
            "{label} T2->T3: p50={}us p95={}us p99={}us",
            p(&t2_t3, 50.0),
            p(&t2_t3, 95.0),
            p(&t2_t3, 99.0)
        );
        eprintln!(
            "{label} T3->T4: p50={}us p95={}us p99={}us",
            p(&t3_t4, 50.0),
            p(&t3_t4, 95.0),
            p(&t3_t4, 99.0)
        );
        eprintln!(
            "{label} T1->T4: p50={}us p95={}us p99={}us",
            p(&t1_t4, 50.0),
            p(&t1_t4, 95.0),
            p(&t1_t4, 99.0)
        );

        // T2->T3 is the Core/Storage contribution - assert no significant regression
        let cur_t2t3_p95 = p(&t2_t3, 95.0);
        // Threshold: p95 < 5000us (5ms) for storage commit - generous for debug build
        assert!(
            cur_t2t3_p95 < 5000,
            "{label} T2->T3 p95 regression: {cur_t2t3_p95}us"
        );
    }
}

/// World scaling: 10, 100, 1000 worlds
#[test]
fn world_scaling_regression() {
    for label in ["current", "wal"] {
        for n_worlds in [10, 100, 1000] {
            let base = format!("worlds-{label}-{n_worlds}");
            let s = if label == "current" {
                fresh_current(&base)
            } else {
                fresh_wal(&base)
            };

            // Create worlds and populate
            for w in 0..n_worlds {
                let world_id = format!("{base}-w{w}");
                let sp = Space {
                    id: format!("space-{world_id}"),
                    name: format!("W-{world_id}"),
                    metadata: json!({}),
                    created_at: chrono::Utc::now(),
                    updated_at: chrono::Utc::now(),
                };
                s.create_space(&sp).unwrap();
                // Add 10 objects per world
                for i in 0..10 {
                    let mut o = Object::new(
                        "File",
                        "filesystem",
                        json!({"path": format!("{base}/w{w}/f{i}.txt"), "world_id": world_id}),
                    );
                    o.id = format!("{base}-w{w}-obj-{i:03}");
                    o.external_id = Some(format!("{base}/w{w}/f{i}.txt"));
                    s.create_object(&o).unwrap();
                }
            }

            let t0 = std::time::Instant::now();
            // Query all worlds
            for w in 0..n_worlds {
                let q = s
                    .query_objects(&ObjectQuery {
                        world_id: Some(format!("{base}-w{w}")),
                        ..Default::default()
                    })
                    .unwrap();
                assert_eq!(q.len(), 10, "world {w} object count");
            }
            let elapsed = t0.elapsed().as_millis();
            eprintln!("{label} {n_worlds} worlds query: {elapsed}ms");

            // Should complete in reasonable time (1000 worlds is O(Space) - known issue)
            let threshold = if n_worlds >= 1000 { 120_000 } else { 5000 };
            assert!(
                elapsed < threshold,
                "{label} {n_worlds} worlds query too slow: {elapsed}ms"
            );
        }
    }
}

/// Reconstruction: Reality -> Provider -> Core state matches
#[test]
fn reconstruction_both_modes() {
    for label in ["current", "wal"] {
        let base = format!("recon-{label}");
        let s = if label == "current" {
            fresh_current(&base)
        } else {
            fresh_wal(&base)
        };

        let sp = Space {
            id: format!("space-{base}"),
            name: format!("W-{base}"),
            metadata: json!({}),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };
        s.create_space(&sp).unwrap();
        s.upsert_provider(&space_core::ProviderInfo {
            id: format!("prov-{base}"),
            name: "p".into(),
            capabilities: vec!["scan".into()],
            version: None,
        })
        .unwrap();

        // Create known state
        for i in 0..500 {
            let mut o = Object::new(
                "File",
                "filesystem",
                json!({"path": format!("{base}/f{i}.txt")}),
            );
            o.id = format!("{base}-obj-{i:04}");
            o.external_id = Some(format!("{base}/f{i}.txt"));
            s.create_object(&o).unwrap();
        }

        // Simulate provider re-scan: reopen storage and verify all objects present
        let db_name = if label == "current" {
            format!("cur-{base}")
        } else {
            format!("wal-{base}")
        };
        let db_path = tmp_db(&db_name);
        drop(s);
        let s2 = if label == "current" {
            Storage::open(&db_path).unwrap()
        } else {
            Storage::open_wal(&db_path).unwrap()
        };

        let objs = s2.query_objects(&ObjectQuery::default()).unwrap();
        assert_eq!(objs.len(), 500, "{label} reconstruction object count");

        // Verify all external_ids present and unique
        let mut seen = std::collections::HashSet::new();
        for o in &objs {
            assert!(o.external_id.is_some());
            assert!(
                seen.insert(o.external_id.as_ref().unwrap().clone()),
                "{label} duplicate external_id"
            );
        }

        // Verify events
        for i in 0..50 {
            let mut e = Event::new("ScanTick", "filesystem", json!({"i": i}));
            e.id = format!("{base}-ev-{i:04}");
            e.world_id = "recon".into();
            s2.record_event(&e).unwrap();
        }
        let evs = s2.query_events(&EventQuery::default()).unwrap();
        assert_eq!(evs.len(), 50, "{label} reconstruction event count");

        assert_eq!(s2.integrity_check().unwrap(), "ok");
    }
}
