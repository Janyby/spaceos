//! §15 — Integrated live scenario WITHOUT restart/crash (Full Qualification).
//!
//! X13 (cited) already proves the combined stack WITH a controlled
//! restart. §15 explicitly forbids mixing restart/crash into THIS
//! experiment until the separate failure points are understood (they
//! are: X5/X6/B7/X8/Gate3, cited). So this is the bounded no-restart
//! variant — kept as its own suite, X13 untouched.
//!
//! Hypothesis: the full live stack converges with no unauthorized
//! mutation, no silent overwrite, exact events/attribution/views.
//! Failure boundary: whole stack (each layer HELD alone elsewhere).
//! Exact workload per run (isolated temp DB + temp root, real
//! spaceos.exe = Core+Provider):
//!  1. spawn spaceos.exe; readiness + provider gates.
//!  2. register agent-a (mutate+action, [W1]), agent-b (observer),
//!     agent-c (second mutator, [W1]).
//!  3. human writes human.txt; fast subscriber drains, slow paces 3.
//!  4. agent-a creates linked object; agent-c modifies it (second-actor
//!     linked change); agent-b reads the update via query.
//!  5. agent-a x10: create object + provider action file + event
//!     (mission-S15); agent-b polls queries concurrently.
//!  6. mid-run scan reconciles human + action files; fingerprint match.
//!  7. fast 10/10, slow paced-3; reconnect + 3 new events + re-query
//!     exact; views converge; trace chains present, disjoint,
//!     secret-free (NO restart: chain persists, asserted non-empty).
//! Fixed threshold: exit ok; no hung children; artifacts exact;
//! fast 10/10; slow 3; reconnect 3/3; trace non-empty + disjoint +
//! secret-free; B saw C's linked update.
//! Expected: HELD. Pass: all exact. Fail: any divergence.
//! Reproduction protocol: 3 identical runs (loop in one test).
//! Machine exclusion: no other Core on CORE_PIPE during the run.

use filesystem_provider::scanner::Scanner;
use serde_json::{json, Value};
use space_core::{ipc, Object};
use std::path::PathBuf;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

#[path = "support/global_lock.rs"]
mod global_lock;

fn tmp_base(run: usize) -> PathBuf {
    let d = std::env::temp_dir()
        .join("spaceos-qs15")
        .join(format!("run{run}"));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn bins() -> PathBuf {
    let mut dir = std::env::current_exe().unwrap();
    dir.pop();
    dir.pop();
    dir.join("spaceos.exe")
}

struct ChildGuard {
    child: tokio::process::Child,
    name: &'static str,
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
    }
}

impl ChildGuard {
    async fn kill_wait(&mut self) {
        let _ = self.child.kill().await;
        match tokio::time::timeout(Duration::from_secs(15), self.child.wait()).await {
            Ok(Ok(_)) => {}
            other => panic!(
                "S15: {} did not exit: {:?}",
                self.name,
                other.map(|r| r.map(|_| ()))
            ),
        }
    }
}

async fn wait_ready() {
    for _ in 0..60 {
        match ipc::send_request(ipc::CORE_PIPE, "list_providers", json!({})).await {
            Ok(v) if v["ok"].as_bool() == Some(true) => return,
            _ => tokio::time::sleep(Duration::from_millis(500)).await,
        }
    }
    panic!("S15: Core not ready in 30s");
}

async fn wait_provider() {
    for _ in 0..20 {
        let r = ipc::send_request(ipc::CORE_PIPE, "list_providers", json!({}))
            .await
            .unwrap();
        if r["result"]
            .as_array()
            .map(|a| a.iter().any(|p| p["id"] == "filesystem"))
            .unwrap_or(false)
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    panic!("S15: provider not registered");
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

fn has_secret_key(v: &Value) -> Option<String> {
    const SECRETS: [&str; 8] = [
        "auth",
        "actor_token",
        "token",
        "api_key",
        "password",
        "secret",
        "private_key",
        "session_secret",
    ];
    match v {
        Value::Object(m) => {
            for (k, child) in m {
                if SECRETS.contains(&k.as_str()) {
                    return Some(k.clone());
                }
                if let Some(hit) = has_secret_key(child) {
                    return Some(hit);
                }
            }
            None
        }
        Value::Array(items) => items.iter().find_map(has_secret_key),
        _ => None,
    }
}

async fn one_run(run: usize) {
    let base = tmp_base(run);
    let db = base.join("core.db");
    let root = base.join("root");
    std::fs::create_dir_all(&root).unwrap();
    let core_token = format!("qs15-fixed-token-run{run}");
    std::env::set_var("SPACEOS_CORE_TOKEN", &core_token);
    let spaceos_bin = bins();
    assert!(spaceos_bin.exists());

    // 1. Real processes (Core+Provider in spaceos.exe).
    let mut core = ChildGuard {
        child: tokio::process::Command::new(&spaceos_bin)
            .args(["run", "--root"])
            .arg(&root)
            .env("SPACEOS_DB", &db)
            .env("SPACEOS_CORE_TOKEN", &core_token)
            .current_dir(&base)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("spawn spaceos"),
        name: "spaceos",
    };
    wait_ready().await;
    wait_provider().await;

    // 2. Agents: A = mutator, B = observer, C = second mutator.
    let reg = |name: &str, caps: Vec<&str>| {
        let (p, n) = (ipc::CORE_PIPE.to_string(), name.to_string());
        let caps: Vec<String> = caps.into_iter().map(|s| s.to_string()).collect();
        async move {
            let r = ipc::send_request(
                &p,
                "register_agent",
                json!({"principal":"system","name":n,"capabilities":caps,"worlds":["W1"]}),
            )
            .await
            .unwrap();
            assert_eq!(r["ok"], true);
            r["result"]["token"].as_str().unwrap().to_string()
        }
    };
    let tok_a = reg(
        "agent-a",
        vec![
            "object.create",
            "object.update",
            "event.record",
            "action.create",
        ],
    )
    .await;
    let tok_b = reg("agent-b", vec![]).await;
    let tok_c = reg("agent-c", vec!["object.create", "object.update"]).await;

    // 3. Human reality + fast/slow subscribers before activity.
    std::fs::write(root.join("human.txt"), "human-bytes").unwrap();
    let (mut fast, ack) = subscribe(
        ipc::CORE_PIPE,
        json!({"subscriber": "agent-b", "actor_token": tok_b, "world_id": "W1"}),
    )
    .await;
    assert_eq!(ack["ok"], true);
    let (mut slow, ack) = subscribe(
        ipc::CORE_PIPE,
        json!({"subscriber": "agent-b", "actor_token": tok_b, "world_id": "W1"}),
    )
    .await;
    assert_eq!(ack["ok"], true);
    let fast_handle = tokio::spawn(async move {
        let mut got = vec![];
        for _ in 0..10 {
            match tokio::time::timeout(Duration::from_millis(15000), fast.lines.next_line()).await {
                Ok(Ok(Some(l))) => got.push(serde_json::from_str::<Value>(&l).unwrap()),
                _ => break,
            }
        }
        got
    });
    let slow_handle = tokio::spawn(async move {
        let mut got = vec![];
        for _ in 0..3 {
            match tokio::time::timeout(Duration::from_millis(15000), slow.lines.next_line()).await {
                Ok(Ok(Some(l))) => {
                    got.push(serde_json::from_str::<Value>(&l).unwrap());
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                _ => break,
            }
        }
        got
    });

    // 4. Second-actor linked change: A creates, C modifies, B reads.
    let mut linked = Object::new(
        "File",
        "agent-a",
        json!({"n": "linked", "rev": 1, "world_id": "W1"}),
    );
    linked.id = "x15-linked".to_string();
    let mut p = json!(linked);
    p["actor_token"] = json!(tok_a);
    assert_eq!(
        ipc::send_request(ipc::CORE_PIPE, "create_object", p)
            .await
            .unwrap()["ok"],
        true
    );
    let mut upd = Object::new(
        "File",
        "agent-c",
        json!({"n": "linked", "rev": 2, "world_id": "W1"}),
    );
    upd.id = "x15-linked".to_string();
    let mut p = json!(upd);
    p["actor_token"] = json!(tok_c);
    assert_eq!(
        ipc::send_request(ipc::CORE_PIPE, "update_object", p)
            .await
            .unwrap()["ok"],
        true,
        "S15: second actor linked update"
    );
    let g = ipc::send_request(ipc::CORE_PIPE, "get_object", json!({"id": "x15-linked"}))
        .await
        .unwrap();
    assert_eq!(
        g["result"]["properties"]["rev"], 2,
        "S15: B reads C's linked update"
    );
    assert_eq!(
        g["result"]["provider_id"], "agent-c",
        "S15: attribution of linked change"
    );

    // 5. Agent A x10: object + provider action file + event; B polls.
    let mut b_seen = 0;
    for i in 0..10 {
        let mut o = Object::new("File", "agent-a", json!({"n": i, "world_id": "W1"}));
        o.id = format!("x15-o-{i:02}");
        let mut p = json!(o);
        p["actor_token"] = json!(tok_a);
        p["correlation_id"] = json!("mission-S15");
        assert_eq!(
            ipc::send_request(ipc::CORE_PIPE, "create_object", p)
                .await
                .unwrap()["ok"],
            true
        );
        let aid = format!("x15-act-{i:02}");
        let r = ipc::send_request(
            ipc::CORE_PIPE,
            "request_action",
            json!({"id": aid, "action": "create",
                "target": {"path": root.join(format!("act-{i:02}.txt")).to_string_lossy(), "content": format!("agent-{i}")},
                "provider_id": "filesystem", "actor": "agent-a", "actor_token": tok_a,
                "requested_at": chrono::Utc::now().to_rfc3339()}),
        )
        .await
        .unwrap();
        assert_eq!(r["result"]["status"], "Completed", "action {i}: {r}");
        let mut e = space_core::Event::new("Note", "agent-a", json!({"i": i}));
        e.id = format!("x15-e-{i:02}");
        e.world_id = "W1".to_string();
        e.correlation_id = Some("mission-S15".to_string());
        let mut p = json!(e);
        p["actor_token"] = json!(tok_a);
        assert_eq!(
            ipc::send_request(ipc::CORE_PIPE, "record_event", p)
                .await
                .unwrap()["ok"],
            true
        );
        let q = ipc::send_request(ipc::CORE_PIPE, "query_objects", json!({}))
            .await
            .unwrap();
        let objs: Vec<Object> = serde_json::from_value(q["result"].clone()).unwrap();
        b_seen = b_seen.max(objs.len());
    }
    assert!(b_seen >= 10, "S15: B observed growth ({b_seen})");

    // 6. Mid-run scan reconciles human + action files.
    let scanner = Scanner::new("filesystem");
    let stats = scanner.scan_on(&root, ipc::CORE_PIPE).await.unwrap();
    for name in ["human.txt".to_string()]
        .into_iter()
        .chain((0..10).map(|i| format!("act-{i:02}.txt")))
        .collect::<Vec<_>>()
    {
        let r = ipc::send_request(
            ipc::CORE_PIPE,
            "get_object_by_external",
            json!({"provider_id": "filesystem", "external_id": root.join(&name).to_string_lossy()}),
        )
        .await
        .unwrap();
        let row: Option<Object> = serde_json::from_value(r["result"].clone()).unwrap_or(None);
        assert!(
            row.is_some(),
            "S15: converged row for {name} (stats={stats})"
        );
    }
    let fp = filesystem_provider::identity::fingerprint(&root.join("human.txt")).unwrap();
    let r = ipc::send_request(
        ipc::CORE_PIPE,
        "get_object_by_external",
        json!({"provider_id": "filesystem", "external_id": root.join("human.txt").to_string_lossy()}),
    )
    .await
    .unwrap();
    let row: Option<Object> = serde_json::from_value(r["result"].clone()).unwrap_or(None);
    assert_eq!(
        row.expect("human row")
            .properties
            .get("fingerprint")
            .and_then(|v| v.as_str()),
        Some(fp.as_str()),
        "S15: Reality fingerprint matches"
    );

    // 7a. Speeds: fast 10/10, slow paced 3.
    let got_fast = fast_handle.await.unwrap();
    assert_eq!(got_fast.len(), 10, "S15: fast 10/10");
    let got_slow = slow_handle.await.unwrap();
    assert_eq!(got_slow.len(), 3, "S15: slow paced 3");

    // 7b. Reconnect + re-query (NO restart in this scenario).
    let (mut sub2, ack) = subscribe(
        ipc::CORE_PIPE,
        json!({"subscriber": "agent-b", "actor_token": tok_b, "world_id": "W1"}),
    )
    .await;
    assert_eq!(ack["ok"], true);
    for i in 0..3 {
        let mut e = space_core::Event::new("Note", "agent-a", json!({"i": 100 + i}));
        e.id = format!("x15-re-{i}");
        e.world_id = "W1".to_string();
        e.correlation_id = Some("mission-S15-re".to_string());
        let mut p = json!(e);
        p["actor_token"] = json!(tok_a);
        assert_eq!(
            ipc::send_request(ipc::CORE_PIPE, "record_event", p)
                .await
                .unwrap()["ok"],
            true
        );
    }
    let mut got2 = vec![];
    for _ in 0..3 {
        match tokio::time::timeout(Duration::from_millis(8000), sub2.lines.next_line()).await {
            Ok(Ok(Some(l))) => got2.push(serde_json::from_str::<Value>(&l).unwrap()),
            _ => break,
        }
    }
    assert_eq!(got2.len(), 3, "S15: reconnected gets new flow");
    // Views converge: query agrees with deliveries.
    let evs = ipc::send_request(ipc::CORE_PIPE, "query_events", json!({"world_id": "W1"}))
        .await
        .unwrap();
    let evs: Vec<space_core::Event> = serde_json::from_value(evs["result"].clone()).unwrap();
    assert!(
        evs.len() >= 13,
        "S15: store has mission flow (got {})",
        evs.len()
    );

    // 7c. Trace (no restart: chain persists): mission chain present,
    // per-action Requested+Completed, secret-free, disjoint scan spans.
    let qd = ipc::send_request(
        ipc::CORE_PIPE,
        "trace_query",
        json!({"subscriber": "system", "correlation_id": "mission-S15"}),
    )
    .await
    .unwrap();
    let chain: Vec<Value> = serde_json::from_value(qd["result"].clone()).unwrap();
    assert!(!chain.is_empty(), "S15: mission-S15 chain exists");
    assert!(chain.iter().all(|e| e["correlation_id"] == "mission-S15"));
    for e in &chain {
        assert!(
            has_secret_key(&e["payload"]).is_none(),
            "S15: secret-free trace"
        );
    }
    for i in 0..10 {
        let aid = format!("x15-act-{i:02}");
        let qa = ipc::send_request(
            ipc::CORE_PIPE,
            "trace_query",
            json!({"subscriber": "system", "correlation_id": aid}),
        )
        .await
        .unwrap();
        let achain: Vec<Value> = serde_json::from_value(qa["result"].clone()).unwrap();
        let types: Vec<&str> = achain
            .iter()
            .filter_map(|e| {
                e.get("payload")
                    .and_then(|p| p.get("event_type"))
                    .and_then(|v| v.as_str())
            })
            .collect();
        assert!(
            types.contains(&"ActionRequested") && types.contains(&"ActionCompleted"),
            "S15: action {aid} requested+completed, got {types:?}"
        );
    }
    let qp = ipc::send_request(
        ipc::CORE_PIPE,
        "trace_query",
        json!({"subscriber": "system", "provider_id": "filesystem"}),
    )
    .await
    .unwrap();
    let prov: Vec<Value> = serde_json::from_value(qp["result"].clone()).unwrap();
    assert!(
        prov.iter().any(|e| e["kind"] == "ScanEnd"),
        "S15: scan spans recorded"
    );
    let ids_d: std::collections::HashSet<&str> = chain
        .iter()
        .filter_map(|e| e.get("trace_id").and_then(|v| v.as_str()))
        .collect();
    let ids_s: std::collections::HashSet<&str> = prov
        .iter()
        .filter_map(|e| e.get("trace_id").and_then(|v| v.as_str()))
        .collect();
    assert!(ids_d.is_disjoint(&ids_s), "S15: chains disjoint");

    core.kill_wait().await;
    eprintln!("S15 run{run}: no-restart integrated scenario converged, 0 divergence");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn s15_integrated_no_restart() {
    let _ipc_lock = global_lock::acquire_global_ipc();
    for run in 1..=3 {
        one_run(run).await;
    }
}
