//! X9 — Long-run mixed load with delivery + trace growth (EXTEND, new conditions).
//!
//! Existing baselines (cited, NOT duplicated): B2 100k events (~380s),
//! v17_perf 100k ops, wal/batch 100k event runs. None combined mixed
//! mutations + live delivery + resync + trace growth.
//! Hypothesis: sustained mixed load degrades delivery, counts, trace
//! accounting, or resource footprint beyond the known linear shape.
//! What can break: subscriber loss over time; count drift;
//! trace unbounded growth handling; DB bloat; slowdown.
//! Failure boundary: full stack under duration (single Core, WAL).
//! Exact workload: 100k mixed IPC ops (40 create / 10 update / 10
//! relation / 30 event / 10 delete-temp) with one concurrently draining
//! subscriber; checkpoint resync + trace stats every 10k.
//! Dataset: temp DB (WAL default? Storage::open = rollback; Core tests
//! use rollback — note as condition, not a flaw) + temp pipe.
//! Fixed threshold: 0 errors; final counts exact (objects 40k-10k=30k,
//! relations 10k, events 30k); subscriber receives 30k/30k; every
//! checkpoint resync exact; trace drops stay 0 under this owned rate
//! (informational if nonzero but accounted).
//! Expected behavior: HELD with linear growth.
//! Measurement: counts, delivery, p50/p95 producer latency, DB bytes,
//! trace kept/dropped per checkpoint.
//! Pass criterion: all exact. Failure criterion: any drift/loss.
//! Reproduction protocol: 3 identical runs (long; background).
//! Machine budget: ~10 min/run single-threaded IPC; 250k+ NOT attempted
//! (no target load justifies it; stop rule stands).

use serde_json::{json, Value};
use space_core::{ipc, Object, Storage};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

fn tmp_db(tag: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join("spaceos-x9");
    std::fs::create_dir_all(&d).unwrap();
    d.join(format!("{tag}.db"))
}

async fn register(pipe: &str, name: &str) -> String {
    let r = ipc::send_request(
        pipe,
        "register_agent",
        json!({"principal":"system","name":name,
            "capabilities":["object.create","object.update","object.delete",
                "relation.create","relation.delete","event.record"],
            "worlds":["WA"]}),
    )
    .await
    .unwrap();
    assert_eq!(r["ok"], true);
    r["result"]["token"].as_str().unwrap().to_string()
}

struct SubConn {
    lines: tokio::io::Lines<
        BufReader<tokio::io::ReadHalf<tokio::net::windows::named_pipe::NamedPipeClient>>,
    >,
}

async fn subscribe(pipe: &str, params: Value) -> (SubConn, Value) {
    let stream = tokio::net::windows::named_pipe::ClientOptions::new()
        .open(pipe)
        .unwrap();
    let (r, mut w) = tokio::io::split(stream);
    let req = json!({"method": "subscribe", "params": params});
    let mut s = serde_json::to_string(&req).unwrap();
    s.push('\n');
    w.write_all(s.as_bytes()).await.unwrap();
    w.flush().await.unwrap();
    let mut lines = BufReader::new(r).lines();
    let ack = lines.next_line().await.unwrap().unwrap_or_default();
    (SubConn { lines }, serde_json::from_str(&ack).unwrap())
}

fn pct(v: &mut [i64], p: f64) -> i64 {
    v.sort_unstable();
    v[((p / 100.0) * (v.len() as f64 - 1.0)).round() as usize]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn x9_mixed_100k_with_delivery() {
    let pipe_owned = format!(r"\\.\pipe\spaceos-x9-{}", std::process::id());
    let pipe: &'static str = Box::leak(pipe_owned.into_boxed_str());
    let db = tmp_db("x9");
    let _ = std::fs::remove_file(&db);
    let storage = Storage::open(&db).unwrap();
    let state = ipc::CoreState::new(storage);
    let s2 = state.clone();
    let p = pipe.to_string();
    tokio::spawn(async move {
        let _ = ipc::run_server(s2, &p).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    let tok = register(pipe, "long-x9").await;
    let (mut sub, ack) = subscribe(
        pipe,
        json!({"subscriber": "long-x9", "actor_token": tok, "world_id": "WA"}),
    )
    .await;
    assert_eq!(ack["ok"], true);

    // Live drain task for all 30k events.
    let mut drain = tokio::spawn(async move {
        let mut n = 0u32;
        for _ in 0..30_000 {
            match tokio::time::timeout(
                std::time::Duration::from_millis(20000),
                sub.lines.next_line(),
            )
            .await
            {
                Ok(Ok(Some(_))) => n += 1,
                _ => break,
            }
        }
        (n, sub)
    });

    let mut lat = Vec::with_capacity(100_000);
    let mut temps: Vec<String> = vec![];
    let t_all = std::time::Instant::now();
    for i in 0..100_000 {
        let t0 = std::time::Instant::now();
        match i % 10 {
            0..=3 => {
                let mut o = Object::new("File", "long-x9", json!({"i": i, "world_id": "WA"}));
                o.id = format!("x9-o-{i:06}");
                let mut p = json!(o);
                p["actor_token"] = json!(tok);
                let r = ipc::send_request(pipe, "create_object", p).await.unwrap();
                assert_eq!(r["ok"], true, "create {i}");
                if i % 10 == 0 {
                    temps.push(format!("x9-o-{i:06}"));
                }
            }
            4 => {
                let id = format!("x9-o-{:06}", (i / 10) * 10);
                let mut o = Object::new(
                    "File",
                    "long-x9",
                    json!({"i": i, "world_id": "WA", "rev": 2}),
                );
                o.id = id;
                let mut p = json!(o);
                p["actor_token"] = json!(tok);
                let r = ipc::send_request(pipe, "update_object", p).await.unwrap();
                assert_eq!(r["ok"], true, "update {i}");
            }
            5 => {
                // Anchored on NON-temp objects (temps are deleted at 9 and
                // would cascade these relations away).
                let base = (i / 10) * 10;
                let mut r = space_core::Relation::new(
                    format!("x9-o-{:06}", base + 1),
                    "link",
                    format!("x9-o-{:06}", base + 2),
                );
                r.id = format!("x9-r-{i:06}");
                r.world_id = "WA".to_string();
                let mut p = json!(r);
                p["actor"] = json!("long-x9");
                p["actor_token"] = json!(tok);
                let r = ipc::send_request(pipe, "create_relation", p).await.unwrap();
                assert_eq!(r["ok"], true, "relation {i}: {r}");
            }
            6..=8 => {
                let mut e = space_core::Event::new("Flow", "long-x9", json!({"i": i}));
                e.id = format!("x9-e-{i:06}");
                e.world_id = "WA".to_string();
                let mut p = json!(e);
                p["actor_token"] = json!(tok);
                let r = ipc::send_request(pipe, "record_event", p).await.unwrap();
                assert_eq!(r["ok"], true, "event {i}");
            }
            _ => {
                if let Some(id) = temps.pop() {
                    let r = ipc::send_request(
                        pipe,
                        "delete_object",
                        json!({"id": id, "actor": "long-x9", "actor_token": tok}),
                    )
                    .await
                    .unwrap();
                    assert_eq!(r["ok"], true, "delete {id}: {r}");
                }
            }
        }
        lat.push(t0.elapsed().as_micros() as i64);
        if i % 10_000 == 9_999 {
            // Checkpoint: resync exact + trace growth snapshot.
            let q = ipc::send_request(pipe, "query_events", json!({"world_id": "WA"}))
                .await
                .unwrap();
            let evs: Vec<space_core::Event> = serde_json::from_value(q["result"].clone()).unwrap();
            let expect_ev = ((i + 1) / 10) * 3;
            assert_eq!(evs.len(), expect_ev, "X9 checkpoint resync {i}");
            if let Some(st) = state.spacetrace.as_ref() {
                let bs = st.provider().buffer_stats();
                eprintln!(
                    "X9 checkpoint {i}: events={expect_ev} trace_kept={} dropped={}",
                    bs.current_len, bs.dropped_count
                );
            }
        }
    }
    let wall_s = t_all.elapsed().as_secs_f64();
    let mut lat2 = lat.clone();
    eprintln!(
        "X9: 100k mixed in {wall_s:.0}s ({:.0}/s) p50={}us p95={}us",
        100_000.0 / wall_s,
        pct(&mut lat2.clone(), 50.0),
        pct(&mut lat2.clone(), 95.0)
    );

    let (n_got, _sub) = drain.await.unwrap();
    assert_eq!(n_got, 30_000, "X9: live delivery 30k/30k");

    // Final counts: objects 40k created - 10k deleted = 30k; relations 10k;
    // events 30k. (Deletes target temp objects with no relations.)
    let objs = ipc::send_request(pipe, "query_objects", json!({}))
        .await
        .unwrap();
    let objs: Vec<Object> = serde_json::from_value(objs["result"].clone()).unwrap();
    assert_eq!(
        objs.len(),
        30_000,
        "X9: objects net 30k, got {}",
        objs.len()
    );
    let rels = ipc::send_request(pipe, "query_relations", json!({}))
        .await
        .unwrap();
    let rels: Vec<space_core::Relation> = serde_json::from_value(rels["result"].clone()).unwrap();
    assert_eq!(rels.len(), 10_000, "X9: relations 10k, got {}", rels.len());
    let evs = ipc::send_request(pipe, "query_events", json!({}))
        .await
        .unwrap();
    let evs: Vec<space_core::Event> = serde_json::from_value(evs["result"].clone()).unwrap();
    assert_eq!(evs.len(), 30_000, "X9: events 30k");
    // No dangling relations (deleted temps had none attached by construction,
    // but verify the invariant explicitly).
    let ids: std::collections::HashSet<&str> = objs.iter().map(|o| o.id.as_str()).collect();
    for r in &rels {
        assert!(
            ids.contains(r.source_object_id.as_str()) && ids.contains(r.target_object_id.as_str()),
            "X9: 0 dangling"
        );
    }
    let db_bytes = std::fs::metadata(&db).map(|m| m.len()).unwrap_or(0);
    eprintln!("X9: db={db_bytes}B objects=30k relations=10k events=30k delivered=30k/30k");
}
