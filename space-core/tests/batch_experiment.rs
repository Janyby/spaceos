//! V1.9 Transaction Batching Experiment
//! Controlled comparison: WAL (single-stmt) vs WAL + batching.
//! Thresholds fixed BEFORE: throughput >=20% better OR T2→T3 p95 >=20% better;
//! correctness: 0 corruption, 0 dup identity, 0 cross-world leak, 0 recovery regression.

use serde_json::{json, Value};
use space_core::query::{EventQuery, ObjectQuery, RelationQuery};
use space_core::{Event, Object, ProviderInfo, Relation, Space, Storage};

fn tmp_db(name: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join("spaceos-v19");
    std::fs::create_dir_all(&d).unwrap();
    d.join(format!("{name}.db"))
}

fn fresh_wal(name: &str) -> Storage {
    for suffix in ["", "-wal", "-shm", "-journal"] {
        let _ = std::fs::remove_file(tmp_db(&format!("{name}{suffix}")));
    }
    Storage::open_wal(&tmp_db(name)).unwrap()
}

fn pct(v: &mut [i64], p: f64) -> i64 {
    v.sort_unstable();
    v[((p / 100.0) * (v.len() as f64 - 1.0)).round() as usize]
}

#[test]
fn modes_pinned() {
    let mut w = fresh_wal("pin");
    assert_eq!(w.journal_mode().unwrap(), "wal");
    assert_eq!(w.synchronous().unwrap(), 2, "FULL pinned");
    assert_eq!(w.integrity_check().unwrap(), "ok");
}

#[test]
fn batch_basic_semantic_equivalence() {
    // Same workload: individual commits vs single batch
    let mut w1 = fresh_wal("indiv");
    let mut w2 = fresh_wal("batch");

    let sp = Space {
        id: "space-T".into(),
        name: "W-T".into(),
        metadata: json!({}),
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    };
    w1.create_space(&sp).unwrap();
    w2.create_space(&sp).unwrap();

    // Individual commits
    for i in 0..100 {
        let mut o = Object::new("File", "filesystem", json!({"path": format!("T/f{i}.txt")}));
        o.id = format!("T-obj-{i:03}");
        o.external_id = Some(format!("T/f{i}.txt"));
        w1.create_object(&o).unwrap();
    }

    // Batched
    w2.transaction(|tx| {
        for i in 0..100 {
            let mut o = Object::new("File", "filesystem", json!({"path": format!("T/f{i}.txt")}));
            o.id = format!("T-obj-{i:03}");
            o.external_id = Some(format!("T/f{i}.txt"));
            tx.create_object(&o).unwrap();
        }
        Ok(())
    })
    .unwrap();

    // Compare results (normalize timestamps)
    fn dump(s: &Storage) -> Value {
        let mut objs: Vec<Value> = s
            .query_objects(&ObjectQuery::default())
            .unwrap()
            .into_iter()
            .map(|o| {
                let mut v = json!(o);
                v["created_at"] = json!("FIXED");
                v["updated_at"] = json!("FIXED");
                v
            })
            .collect();
        objs.sort_by(|a, b| a["id"].as_str().cmp(&b["id"].as_str()));
        json!({"objects": objs})
    }
    assert_eq!(dump(&w1), dump(&w2), "semantic result identical");
    assert_eq!(w1.integrity_check().unwrap(), "ok");
    assert_eq!(w2.integrity_check().unwrap(), "ok");
}

#[test]
fn batch_size_curve() {
    for batch_size in [1, 2, 5, 10, 25, 50, 100, 250, 500, 1000] {
        let mut w = fresh_wal(&format!("curve-{batch_size}"));
        let sp = Space {
            id: format!("space-c-{batch_size}"),
            name: format!("W-c-{batch_size}"),
            metadata: json!({}),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };
        w.create_space(&sp).unwrap();

        let total_ops = 5000;
        let t0 = std::time::Instant::now();

        for chunk in (0..total_ops).step_by(batch_size) {
            let end = (chunk + batch_size).min(total_ops);
            w.transaction(|tx| {
                for i in chunk..end {
                    let mut o = Object::new(
                        "File",
                        "filesystem",
                        json!({"path": format!("c-{batch_size}/f{i}.txt")}),
                    );
                    o.id = format!("c-{batch_size}-obj-{i:05}");
                    o.external_id = Some(format!("c-{batch_size}/f{i}.txt"));
                    tx.create_object(&o).unwrap();
                }
                Ok(())
            })
            .unwrap();
        }

        let wall_ms = t0.elapsed().as_millis().max(1) as f64;
        let throughput = total_ops as f64 / wall_ms * 1000.0;
        eprintln!("batch={batch_size:4} ops/s={throughput:.0}");
    }
}

#[test]
fn microbench_1k_10k_wal_vs_batch() {
    for n in [1_000usize, 10_000] {
        // WAL single-stmt
        let mut w1 = fresh_wal(&format!("wal-{n}"));
        let sp = Space {
            id: format!("space-wal-{n}"),
            name: format!("W-wal-{n}"),
            metadata: json!({}),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };
        w1.create_space(&sp).unwrap();

        let mut lat1: Vec<i64> = Vec::with_capacity(n);
        let t0 = std::time::Instant::now();
        for i in 0..n {
            let mut o = Object::new(
                "File",
                "filesystem",
                json!({"path": format!("wal-{n}/f{i}.txt")}),
            );
            o.id = format!("wal-{n}-obj-{i:05}");
            let t = std::time::Instant::now();
            w1.create_object(&o).unwrap();
            lat1.push(t.elapsed().as_micros() as i64);
        }
        let wall1 = t0.elapsed().as_millis().max(1) as f64;

        // WAL + batch (batch=100)
        let mut w2 = fresh_wal(&format!("batch-{n}"));
        let sp = Space {
            id: format!("space-batch-{n}"),
            name: format!("W-batch-{n}"),
            metadata: json!({}),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };
        w2.create_space(&sp).unwrap();

        let mut lat2: Vec<i64> = Vec::with_capacity(n);
        let t0 = std::time::Instant::now();
        for chunk in (0..n).step_by(100) {
            let end = (chunk + 100).min(n);
            let batch_lat_start = std::time::Instant::now();
            w2.transaction(|tx| {
                for i in chunk..end {
                    let mut o = Object::new(
                        "File",
                        "filesystem",
                        json!({"path": format!("batch-{n}/f{i}.txt")}),
                    );
                    o.id = format!("batch-{n}-obj-{i:05}");
                    tx.create_object(&o).unwrap();
                }
                Ok(())
            })
            .unwrap();
            let batch_elapsed = batch_lat_start.elapsed().as_micros() as i64;
            // Distribute batch time across operations for comparison
            let ops_in_batch = end - chunk;
            for _ in 0..ops_in_batch {
                lat2.push(batch_elapsed / ops_in_batch as i64);
            }
        }
        let wall2 = t0.elapsed().as_millis().max(1) as f64;

        lat1.sort_unstable();
        lat2.sort_unstable();

        let ops1 = lat1.len() as f64 / wall1 * 1000.0;
        let ops2 = lat2.len() as f64 / wall2 * 1000.0;
        let p50_1 = pct(&mut lat1.clone(), 50.0);
        let p95_1 = pct(&mut lat1.clone(), 95.0);
        let p50_2 = pct(&mut lat2.clone(), 50.0);
        let p95_2 = pct(&mut lat2.clone(), 95.0);

        eprintln!("n={n} WAL:     ops/s={ops1:.0} p50={p50_1}us p95={p95_1}us");
        eprintln!("n={n} BATCH:   ops/s={ops2:.0} p50={p50_2}us p95={p95_2}us");
        eprintln!(
            "n={n} ratio: throughput={:.2}x p95={:.2}x",
            ops2 / ops1.max(1.0),
            p95_1 as f64 / p95_2.max(1) as f64
        );
    }
}

#[test]
fn long_run_100k_wal_vs_batch() {
    for (label, batch_size) in [("wal", 1), ("batch-100", 100)] {
        let mut w = fresh_wal(&format!("lr-{label}"));
        let sp = Space {
            id: format!("space-lr-{label}"),
            name: format!("W-lr-{label}"),
            metadata: json!({}),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };
        w.create_space(&sp).unwrap();

        let mut lat: Vec<i64> = Vec::with_capacity(100_000);
        let t0 = std::time::Instant::now();

        for chunk in (0..100_000).step_by(batch_size) {
            let end = (chunk + batch_size).min(100_000);
            let batch_lat_start = std::time::Instant::now();
            w.transaction(|tx| {
                for i in chunk..end {
                    let mut e = Event::new("Tick", "filesystem", json!({"i": i}));
                    e.id = format!("lr-{label}-ev-{i:05}");
                    e.world_id = "lr".into();
                    e.object_ids = vec![format!("lr-{label}-obj-{i:05}")];
                    tx.record_event(&e).unwrap();
                }
                Ok(())
            })
            .unwrap();
            let batch_elapsed = batch_lat_start.elapsed().as_micros() as i64;
            let ops_in_batch = end - chunk;
            for _ in 0..ops_in_batch {
                lat.push(batch_elapsed / ops_in_batch as i64);
            }
        }

        let wall_ms = t0.elapsed().as_millis().max(1) as f64;
        lat.sort_unstable();
        let ops = lat.len() as f64 / wall_ms * 1000.0;
        let p50 = lat[(ops * 0.50) as usize];
        let p95 = lat[(ops * 0.95) as usize];
        let p99 = lat[(ops * 0.99) as usize];
        eprintln!("{label} 100k: ops/s={ops:.0} p50={p50}us p95={p95}us p99={p99}us");

        // Reopen verify
        let db_path = tmp_db(&format!("lr-{label}"));
        let w2 = Storage::open_wal(&db_path).unwrap();
        assert_eq!(w2.integrity_check().unwrap(), "ok");
        let evs = w2.query_events(&EventQuery::default()).unwrap();
        assert_eq!(evs.len(), 100_000);
    }
}

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

    for label in ["wal", "batch"] {
        let batch_size = if label == "wal" { 1 } else { 50 };
        for point in points {
            let base = format!("crash-{label}-{point:?}");
            let mut w = fresh_wal(&base);
            w.fail_once(point);
            let sp = Space {
                id: format!("space-{base}"),
                name: format!("W-{base}"),
                metadata: json!({}),
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
            };
            w.create_space(&sp).unwrap();

            let mut hit = false;
            for i in 0..50 {
                let mut o = Object::new(
                    "File",
                    "filesystem",
                    json!({"path": format!("{base}/f{i}.txt")}),
                );
                o.id = format!("{base}-obj-{i:04}");
                if label == "wal" {
                    if w.create_object(&o).is_err() {
                        hit = true;
                        break;
                    }
                } else {
                    w.transaction(|tx| {
                        tx.create_object(&o).unwrap();
                        Ok(())
                    })
                    .unwrap();
                }
            }
            if !hit {
                for i in 0..50 {
                    let mut e = Event::new("Tick", "filesystem", json!({"i": i}));
                    e.id = format!("{base}-ev-{i:04}");
                    if label == "wal" {
                        if w.record_event(&e).is_err() {
                            hit = true;
                            break;
                        }
                    } else {
                        w.transaction(|tx| {
                            tx.record_event(&e).unwrap();
                            Ok(())
                        })
                        .unwrap();
                    }
                }
            }
            drop(w);

            let db_path = tmp_db(&base);
            let w2 = Storage::open_wal(&db_path).unwrap();
            assert_eq!(
                w2.integrity_check().unwrap(),
                "ok",
                "{label} {point:?} integrity"
            );

            let mut seen = std::collections::HashSet::new();
            for o in w2.query_objects(&ObjectQuery::default()).unwrap() {
                if let Some(ext) = &o.external_id {
                    assert!(
                        seen.insert(ext.clone()),
                        "{label} {point:?} dup external_id: {ext}"
                    );
                }
            }
        }
    }
}

#[test]
fn read_while_batch_write() {
    for (label, batch_size) in [("wal", 1), ("batch-50", 50)] {
        let base = format!("rw-{label}");
        let mut w = fresh_wal(&base);
        let db_path = tmp_db(&base);

        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let written = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        let reads = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        let mut handles = vec![];

        // Writer thread
        for t in 0..2 {
            let (db2, stop2, wr) = (db_path.clone(), stop.clone(), written.clone());
            handles.push(std::thread::spawn(move || {
                let mut s = Storage::open_wal(&db2).unwrap();
                let mut i = 0u64;
                while !stop2.load(std::sync::atomic::Ordering::Relaxed) {
                    s.transaction(|tx| {
                        for _ in 0..batch_size {
                            let mut e = Event::new("Tick", "filesystem", json!({"t": t, "i": i}));
                            e.id = format!("rw-{t}-{i}");
                            tx.record_event(&e).unwrap();
                            i += 1;
                        }
                        Ok(())
                    })
                    .unwrap();
                    wr.fetch_add(batch_size as u64, std::sync::atomic::Ordering::Relaxed);
                }
            }));
        }

        // Reader thread
        {
            let (db2, stop2, rd) = (db_path.clone(), stop.clone(), reads.clone());
            handles.push(std::thread::spawn(move || {
                let mut s = Storage::open_wal(&db2).unwrap();
                while !stop2.load(std::sync::atomic::Ordering::Relaxed) {
                    let _ = s.query_events(&EventQuery::default());
                    rd.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            }));
        }

        std::thread::sleep(std::time::Duration::from_secs(5));
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        for h in handles {
            h.join().unwrap();
        }

        eprintln!(
            "{label}: written={} reads={}",
            written.load(std::sync::atomic::Ordering::Relaxed),
            reads.load(std::sync::atomic::Ordering::Relaxed)
        );
    }
}

#[test]
fn t1_t2_t3_t4_regression() {
    for (label, batch_size) in [("wal", 1), ("batch-10", 10)] {
        let base = format!("lat-{label}");
        let mut w = fresh_wal(&base);
        let sp = Space {
            id: format!("space-{base}"),
            name: format!("W-{base}"),
            metadata: json!({}),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };
        w.create_space(&sp).unwrap();

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

            let t2 = std::time::Instant::now();
            w.transaction(|tx| {
                tx.record_event(&e).unwrap();
                Ok(())
            })
            .unwrap();
            let t3 = std::time::Instant::now();

            let _ = w
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
            "{label} T1->T2: p50={} p95={} p99={}",
            p(&t1_t2, 50.0),
            p(&t1_t2, 95.0),
            p(&t1_t2, 99.0)
        );
        eprintln!(
            "{label} T2->T3: p50={} p95={} p99={}",
            p(&t2_t3, 50.0),
            p(&t2_t3, 95.0),
            p(&t2_t3, 99.0)
        );
        eprintln!(
            "{label} T3->T4: p50={} p95={} p99={}",
            p(&t3_t4, 50.0),
            p(&t3_t4, 95.0),
            p(&t3_t4, 99.0)
        );
        eprintln!(
            "{label} T1->T4: p50={} p95={} p99={}",
            p(&t1_t4, 50.0),
            p(&t1_t4, 95.0),
            p(&t1_t4, 99.0)
        );

        assert!(p(&t2_t3, 95.0) < 5000, "{label} T2->T3 p95 regression");
    }
}

#[test]
fn deletion_cascade_batch() {
    for label in ["wal", "batch"] {
        let base = format!("del-{label}");
        let mut w = fresh_wal(&base);
        let sp = Space {
            id: format!("space-{base}"),
            name: format!("W-{base}"),
            metadata: json!({}),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };
        w.create_space(&sp).unwrap();

        // Create parent + children relations
        let parent = Object::new(
            "Dir",
            "filesystem",
            json!({"path": format!("{base}/parent")}),
        );
        let mut p = parent.clone();
        p.id = format!("{base}-parent");
        p.external_id = Some(format!("{base}/parent"));
        w.transaction(|tx| {
            tx.create_object(&p).unwrap();
            Ok(())
        })
        .unwrap();

        for i in 0..20 {
            let mut o = Object::new(
                "File",
                "filesystem",
                json!({"path": format!("{base}/parent/f{i}")}),
            );
            o.id = format!("{base}-child-{i}");
            o.external_id = Some(format!("{base}/parent/f{i}"));
            w.transaction(|tx| {
                tx.create_object(&o).unwrap();
                Ok(())
            })
            .unwrap();

            let mut r = Relation::new(p.id.clone(), "contains", o.id.clone());
            r.id = format!("{base}-rel-{i}");
            w.transaction(|tx| {
                tx.create_relation(&r).unwrap();
                Ok(())
            })
            .unwrap();
        }

        // Delete parent (cascades)
        if label == "wal" {
            let before = w.query_objects(&ObjectQuery::default()).unwrap().len();
            let before_rels = w.query_relations(&RelationQuery::default()).unwrap().len();
            eprintln!(
                "DEBUG WAL: before delete - objects={}, relations={}",
                before, before_rels
            );
            w.delete_object(&p.id).unwrap();
            let after = w.query_objects(&ObjectQuery::default()).unwrap().len();
            let after_rels = w.query_relations(&RelationQuery::default()).unwrap().len();
            eprintln!(
                "DEBUG WAL: after delete - objects={}, relations={}",
                after, after_rels
            );
        } else {
            let before = w.query_objects(&ObjectQuery::default()).unwrap().len();
            let before_rels = w.query_relations(&RelationQuery::default()).unwrap().len();
            eprintln!(
                "DEBUG BATCH: before delete - objects={}, relations={}",
                before, before_rels
            );
            w.transaction(|tx| {
                tx.delete_object_cascade(&p.id).unwrap();
                Ok(())
            })
            .unwrap();
            let after = w.query_objects(&ObjectQuery::default()).unwrap().len();
            let after_rels = w.query_relations(&RelationQuery::default()).unwrap().len();
            eprintln!(
                "DEBUG BATCH: after delete - objects={}, relations={}",
                after, after_rels
            );
        }

        // Verify cascade: parent deleted, relations gone, children remain (V1.6 cascade = relations only)
        let objs = w.query_objects(&ObjectQuery::default()).unwrap();
        assert_eq!(
            objs.len(),
            20,
            "{label} children should remain after parent delete"
        );
        let rels = w.query_relations(&RelationQuery::default()).unwrap();
        assert_eq!(rels.len(), 0, "{label} relations should be gone");

        // Reopen verify
        let db_path = tmp_db(&base);
        let w2 = Storage::open_wal(&db_path).unwrap();
        assert_eq!(w2.integrity_check().unwrap(), "ok");
    }
}
