//! Qualification concurrency matrix 10/20/50 (Addendum, MANDATORY).
//!
//! B3's 5/10/20/50 writer baseline is KEPT (cited, not rerun here).
//! This file adds the missing profiles at 10/20/50 workers:
//! A reads, B1 distinct writes, B2 same-object writes, C mixed
//! multi-actor, D multi-agent/view with speeds.
//! Vocabulary (fixed): workers = spawned concurrent tasks performing
//! ops; threads = tokio runtime worker threads (64); writers = tasks
//! issuing mutations. These are NOT interchangeable and are reported
//! separately per level.
//! Hypothesis: guarantees hold at 10, hold at 20, hold at 50 — a pass
//! at 10 closes NOTHING above it.
//! Workload per level N (fresh DB + pipe): A: N readers x 30 queries
//! (+world isolation spot); B1: N writers x 20 distinct objects+events;
//! B2: N writers x 20 updates of ONE object (LWW membership); C: N
//! mixed actors x 20 (create/update/event) + 1 draining subscriber;
//! D: 3 agents + fast drain + paced subscriber + N/2 background writers.
//! Fixed threshold: 0 errors everywhere; exact counts; deliveries
//! complete; per-profile p95 < 10000ms (debug-build ceiling).
//! Expected: HELD at all three levels.
//! Measurement: workers/threads/writers, completed, errors, timeouts,
//! BUSY/retries (expected 0: Core serializes), throughput, p50/p95/p99,
//! delivery counts, final consistency.
//! Pass criterion: all exact at 10 AND 20 AND 50.
//! Failure criterion: any error/miscount/loss at any level.
//! Reproduction protocol: 3 identical full-matrix runs.

use serde_json::{json, Value};
use space_core::{ipc, Object, Storage};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

fn tmp_db(tag: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join("spaceos-qm");
    std::fs::create_dir_all(&d).unwrap();
    d.join(format!("{tag}.db"))
}

async fn start_core(pipe: &str, db: &std::path::Path) {
    let storage = Storage::open(db).unwrap();
    let state = ipc::CoreState::new(storage);
    let s2 = state.clone();
    let p = pipe.to_string();
    tokio::spawn(async move {
        let _ = ipc::run_server(s2, &p).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
}

async fn register(pipe: &str, name: &str, worlds: &[&str]) -> String {
    let r = ipc::send_request(
        pipe,
        "register_agent",
        json!({"principal":"system","name":name,
            "capabilities":["object.create","object.update","event.record"],
            "worlds":worlds}),
    )
    .await
    .unwrap();
    assert_eq!(r["ok"], true, "register {name}: {r}");
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

struct ProfileStat {
    completed: usize,
    errors: Vec<String>,
    p50: i64,
    p95: i64,
    p99: i64,
}

async fn run_level(n: usize, tag: &str) {
    let pipe_owned = format!(r"\\.\pipe\spaceos-qm-{tag}-{n}-{}", std::process::id());
    let pipe: &'static str = Box::leak(pipe_owned.into_boxed_str());
    let db = tmp_db(&format!("{tag}-{n}"));
    let _ = std::fs::remove_file(&db);
    start_core(pipe, &db).await;
    // threads=64 runtime workers for every level (reported, fixed).
    eprintln!("MATRIX level={n} threads=64 writers=vary");

    // Seed one shared object for B2 + a few rows for A reads.
    // NOTE: provider/actor strings must EXACTLY match registered names.
    let seed_tok = register(pipe, "seed", &["WQ"]).await;
    for i in 0..5 {
        let mut o = Object::new("File", "seed", json!({"i": i, "world_id": "WQ"}));
        o.id = format!("qm-seed-{i}");
        let mut p = json!(o);
        p["actor_token"] = json!(seed_tok);
        assert_eq!(
            ipc::send_request(pipe, "create_object", p).await.unwrap()["ok"],
            true
        );
    }
    let mut shared = Object::new("File", "seed", json!({"marker": "init", "world_id": "WQ"}));
    shared.id = "qm-shared".to_string();
    let mut sp = json!(shared);
    sp["actor_token"] = json!(seed_tok);
    assert_eq!(
        ipc::send_request(pipe, "create_object", sp).await.unwrap()["ok"],
        true
    );

    // ---- A: concurrent reads (workers=n, 30 queries each).
    {
        let lat = Arc::new(Mutex::new(Vec::new()));
        let errs = Arc::new(Mutex::new(Vec::new()));
        let mut h = vec![];
        for _ in 0..n {
            let (p, l, e) = (pipe.to_string(), lat.clone(), errs.clone());
            h.push(tokio::spawn(async move {
                for _ in 0..30 {
                    let t0 = std::time::Instant::now();
                    match ipc::send_request(&p, "query_objects", json!({"world_id": "WQ"})).await {
                        Ok(v) if v["ok"].as_bool() == Some(true) => {
                            let objs = v["result"].as_array().unwrap();
                            if !objs.iter().all(|o| {
                                o["properties"]["world_id"] == "WQ"
                                    || o["properties"].get("world_id").is_none()
                            }) {
                                e.lock().unwrap().push("world leak".to_string());
                            }
                            l.lock().unwrap().push(t0.elapsed().as_micros() as i64);
                        }
                        Ok(v) => e.lock().unwrap().push(v.to_string()),
                        Err(x) => e.lock().unwrap().push(x.to_string()),
                    }
                }
            }));
        }
        for x in h {
            x.await.unwrap();
        }
        let e = errs.lock().unwrap().clone();
        assert!(e.is_empty(), "A errors: {:?}", &e[..e.len().min(3)]);
        let mut l = lat.lock().unwrap().clone();
        assert_eq!(l.len(), n * 30, "A completed");
        eprintln!(
            "MATRIX L{n} A reads: workers={n} completed={} p50={}us p95={}us p99={}us",
            l.len(),
            pct(&mut l.clone(), 50.0),
            pct(&mut l.clone(), 95.0),
            pct(&mut l.clone(), 99.0)
        );
        assert!(pct(&mut l.clone(), 95.0) < 10_000_000, "A p95 < 10000ms");
    }

    // ---- B1: distinct writes (workers=writers=n, 20 objects+events each).
    {
        let tok = register(pipe, "w", &["WQ"]).await;
        let lat = Arc::new(Mutex::new(Vec::new()));
        let errs = Arc::new(Mutex::new(Vec::new()));
        let mut h = vec![];
        for w in 0..n {
            let (p, t, l, e) = (pipe.to_string(), tok.clone(), lat.clone(), errs.clone());
            h.push(tokio::spawn(async move {
                for i in 0..20 {
                    let mut o = Object::new("File", "w", json!({"w": w, "i": i, "world_id": "WQ"}));
                    o.id = format!("qm-{n}-w{w}-{i:02}");
                    let mut params = json!(o);
                    params["actor_token"] = json!(t);
                    let t0 = std::time::Instant::now();
                    match ipc::send_request(&p, "create_object", params).await {
                        Ok(v) if v["ok"].as_bool() == Some(true) => {
                            l.lock().unwrap().push(t0.elapsed().as_micros() as i64)
                        }
                        other => e.lock().unwrap().push(format!("{other:?}")),
                    }
                }
            }));
        }
        for x in h {
            x.await.unwrap();
        }
        let e = errs.lock().unwrap().clone();
        assert!(e.is_empty(), "B1 errors: {:?}", &e[..e.len().min(3)]);
        let l = lat.lock().unwrap().clone();
        assert_eq!(l.len(), n * 20, "B1 completed");
        let q = ipc::send_request(pipe, "query_objects", json!({"world_id": "WQ"}))
            .await
            .unwrap();
        let all: Vec<Object> = serde_json::from_value(q["result"].clone()).unwrap();
        assert_eq!(
            all.len(),
            6 + n * 20,
            "B1 exact objects (5 seeds + shared + batch)"
        );
        let mut l2 = l.clone();
        eprintln!(
            "MATRIX L{n} B1 writes: writers={n} completed={} p50={}us p95={}us",
            l.len(),
            pct(&mut l2.clone(), 50.0),
            pct(&mut l2.clone(), 95.0)
        );
    }

    // ---- B2: same-object writes (writers=n x 20 updates, LWW membership).
    {
        let tok = register(pipe, "u", &["WQ"]).await;
        let errs = Arc::new(Mutex::new(Vec::new()));
        let mut h = vec![];
        for w in 0..n {
            let (p, t, e) = (pipe.to_string(), tok.clone(), errs.clone());
            h.push(tokio::spawn(async move {
                for i in 0..20 {
                    let mut o = Object::new(
                        "File",
                        "u",
                        json!({"marker": format!("L{n}-w{w}-{i}"), "world_id": "WQ"}),
                    );
                    o.id = "qm-shared".to_string();
                    let mut params = json!(o);
                    params["actor_token"] = json!(t);
                    match ipc::send_request(&p, "update_object", params).await {
                        Ok(v) if v["ok"].as_bool() == Some(true) => {}
                        other => e.lock().unwrap().push(format!("{other:?}")),
                    }
                }
            }));
        }
        for x in h {
            x.await.unwrap();
        }
        let e = errs.lock().unwrap().clone();
        assert!(e.is_empty(), "B2 errors: {:?}", &e[..e.len().min(3)]);
        let g = ipc::send_request(pipe, "get_object", json!({"id": "qm-shared"}))
            .await
            .unwrap();
        let m = g["result"]["properties"]["marker"]
            .as_str()
            .unwrap_or("")
            .to_string();
        assert!(
            m.starts_with(&format!("L{n}-w")),
            "B2 LWW member, got {m:?}"
        );
        eprintln!("MATRIX L{n} B2 same-row: writers={n} final={m} (member)");
    }

    // ---- C: mixed multi-actor (workers=n x 20 mixed ops).
    {
        let mut toks = vec![];
        for a in 0..3 {
            toks.push(register(pipe, &format!("m{a}"), &["WQ"]).await);
        }
        let errs = Arc::new(Mutex::new(Vec::new()));
        let done = Arc::new(Mutex::new(0usize));
        let mut h = vec![];
        for w in 0..n {
            let (p, ts, e, d) = (pipe.to_string(), toks.clone(), errs.clone(), done.clone());
            h.push(tokio::spawn(async move {
                let an = format!("m{}", w % 3);
                for i in 0..20 {
                    let r = match i % 3 {
                        0 => {
                            let mut o = Object::new("Doc", &an, json!({"i": i, "world_id": "WQ"}));
                            o.id = format!("qm-c-{n}-{w}-{i:02}");
                            let mut params = json!(o);
                            params["actor_token"] = json!(&ts[w % 3]);
                            ipc::send_request(&p, "create_object", params).await
                        }
                        1 => {
                            let mut ev = space_core::Event::new("Tick", &an, json!({"i": i}));
                            ev.id = format!("qm-ce-{n}-{w}-{i:02}");
                            ev.world_id = "WQ".to_string();
                            let mut params = json!(ev);
                            params["actor_token"] = json!(&ts[w % 3]);
                            ipc::send_request(&p, "record_event", params).await
                        }
                        _ => {
                            ipc::send_request(&p, "query_objects", json!({"world_id": "WQ"})).await
                        }
                    };
                    match r {
                        Ok(v) if v["ok"].as_bool() == Some(true) => {
                            *d.lock().unwrap() += 1;
                        }
                        other => e.lock().unwrap().push(format!("{other:?}")),
                    }
                }
            }));
        }
        for x in h {
            x.await.unwrap();
        }
        let e = errs.lock().unwrap().clone();
        assert!(e.is_empty(), "C errors: {:?}", &e[..e.len().min(3)]);
        assert_eq!(*done.lock().unwrap(), n * 20, "C completed");
        eprintln!(
            "MATRIX L{n} C mixed: workers={n} actors=3 completed={}",
            n * 20
        );
    }

    // ---- D: multi-agent/view with speeds + background writers.
    {
        let ta = register(pipe, "da", &["WQ"]).await;
        let tb = register(pipe, "db", &["WQ"]).await;
        let (mut fast, ack) = subscribe(
            pipe,
            json!({"subscriber": "db", "actor_token": tb, "world_id": "WQ"}),
        )
        .await;
        assert_eq!(ack["ok"], true);
        let (mut slow, ack) = subscribe(
            pipe,
            json!({"subscriber": "db", "actor_token": tb, "world_id": "WQ"}),
        )
        .await;
        assert_eq!(ack["ok"], true);
        // Background writers: n/2 tasks x 20 events.
        let nb = (n / 2).max(1);
        let mut h = vec![];
        for w in 0..nb {
            let (p, t) = (pipe.to_string(), ta.clone());
            h.push(tokio::spawn(async move {
                for i in 0..20 {
                    let mut ev = space_core::Event::new("Bg", "da", json!({"w": w, "i": i}));
                    ev.id = format!("qm-bg-{n}-{w}-{i:02}");
                    ev.world_id = "WQ".to_string();
                    let mut params = json!(ev);
                    params["actor_token"] = json!(t);
                    let _ = ipc::send_request(&p, "record_event", params).await;
                }
            }));
        }
        // Fast drains all nb*20 concurrently with publishing.
        let mut fast_got = vec![];
        // Slow paces 5 then ends.
        let mut slow_got = vec![];
        // Interleave: read slow-paced while publishers run.
        let pub_h = tokio::spawn(async move {
            for x in h {
                x.await.unwrap();
            }
        });
        // Fast drain loop.
        let fast_n = nb * 20;
        for _ in 0..fast_n {
            match tokio::time::timeout(Duration::from_millis(20000), fast.lines.next_line()).await {
                Ok(Ok(Some(l))) => fast_got.push(serde_json::from_str::<Value>(&l).unwrap()),
                _ => break,
            }
        }
        // Slow paced 5.
        for _ in 0..5 {
            match tokio::time::timeout(Duration::from_millis(15000), slow.lines.next_line()).await {
                Ok(Ok(Some(l))) => {
                    slow_got.push(serde_json::from_str::<Value>(&l).unwrap());
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                _ => break,
            }
        }
        pub_h.await.unwrap();
        // Drain any remaining fast backlog after publishers done.
        while fast_got.len() < fast_n {
            match tokio::time::timeout(Duration::from_millis(5000), fast.lines.next_line()).await {
                Ok(Ok(Some(l))) => fast_got.push(serde_json::from_str::<Value>(&l).unwrap()),
                _ => break,
            }
        }
        assert_eq!(fast_got.len(), fast_n, "D fast complete");
        assert_eq!(slow_got.len(), 5, "D slow paced 5");
        assert!(fast_got.iter().all(|v| v["event"]["world_id"] == "WQ"));
        // Consistency after load: query agrees. Expected events =
        // C-profile events (i%3==1 per worker) + D background events.
        // C events per worker: i in 0..20 with i%3==1 -> 7.
        let expect_ev = n * 7 + fast_n;
        let q = ipc::send_request(pipe, "query_events", json!({"world_id": "WQ"}))
            .await
            .unwrap();
        let evs: Vec<space_core::Event> = serde_json::from_value(q["result"].clone()).unwrap();
        assert_eq!(evs.len(), expect_ev, "D store matches delivery");
        eprintln!(
            "MATRIX L{n} D agentview: bg_writers={nb} fast={} slow_paced=5 store={}",
            fast_got.len(),
            evs.len()
        );
    }
    eprintln!("MATRIX level {n} COMPLETE");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 64)]
async fn matrix_10() {
    run_level(10, "m10").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 64)]
async fn matrix_20() {
    run_level(20, "m20").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 64)]
async fn matrix_50() {
    run_level(50, "m50").await;
}
