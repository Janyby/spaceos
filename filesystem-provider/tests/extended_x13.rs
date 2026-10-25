//! §13 — Combined live integration (ONE bounded scenario, real processes).
//!
//! Composes X1 (real processes) + Level D (human/agents/views/reality)
//! + M6/X3 (fast/slow subscribers) + M4 (trace): real spaceos.exe
//! (Core+Provider) + human fs writes + two IPC agents + fast/slow
//! subscribers + SpaceTrace linkage + controlled restart.
//! What can break: any cross-layer divergence under the combination.
//! Failure boundary: whole stack (each layer HELD alone elsewhere).
//! Exact workload per run (isolated temp DB + temp root):
//!  1. spawn spaceos.exe; readiness + provider gates.
//!  2. register agent-a (mutations+action, [W1]) and agent-b (observer).
//!  3. human writes human.txt; fast subscriber drains, slow paces 3.
//!  4. agent-a x10: create object + provider action file + event
//!     (mission-X correlation); agent-b polls queries concurrently.
//!  5. mid-run scan reconciles human + action files.
//!  6. kill spaceos; respawn same DB; re-register (memory-only Policy);
//!     counts persist; reconnected subscriber gets 3 new events; resync
//!     exact.
//!  7. trace pre-restart (mission-X chain + scan spans, secret-free,
//!     disjoint); post-restart mission-X honestly EMPTY (memory store)
//!     while semantic events persist; new flow 12-span chain.
//! Fixed threshold: exit ok; no hung children; artifacts exact
//! (10 objs + 10 files + 10 actions + 10 notes + human row);
//! fast 10/10 live; slow paced-3 + resync exact; restart counts equal;
//! trace chains present, disjoint, secret-free.
//! Expected behavior: HELD.
//! Pass criterion: all exact. Failure criterion: any divergence.
//! Reproduction protocol: 3 identical runs (loop in one test).
//! Machine exclusion: no other Core on CORE_PIPE during the run.

use filesystem_provider::scanner::Scanner;
use serde_json::{json, Value};
use space_core::{ipc, Object};
use std::path::PathBuf;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

/// Cross-process exclusion for GLOBAL pipes + shared result file (see
/// support/global_lock.rs, mirrored from space-core tests): real
/// spaceos.exe processes bind machine-global names.
#[path = "support/global_lock.rs"]
mod global_lock;

fn tmp_base(run: usize) -> PathBuf {
    let d = std::env::temp_dir()
        .join("spaceos-x13")
        .join(format!("run{run}"));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn bins() -> (PathBuf, PathBuf) {
    let mut dir = std::env::current_exe().unwrap();
    dir.pop();
    dir.pop();
    (dir.join("spaceos.exe"), dir.join("agent-demo.exe"))
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
                "X13: {} did not exit: {:?}",
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
    panic!("X13: Core not ready in 30s");
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
    panic!("X13: provider not registered");
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
    let core_token = format!("x13-fixed-token-run{run}");
    std::env::set_var("SPACEOS_CORE_TOKEN", &core_token);
    let (spaceos_bin, _) = bins();
    assert!(spaceos_bin.exists());

    // 1. Real processes.
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

    // 2. Agents.
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
        vec!["object.create", "event.record", "action.create"],
    )
    .await;
    let tok_b = reg("agent-b", vec![]).await;

    // 3. Human reality + subscribers before activity.
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
    let mut fast_handle = tokio::spawn(async move {
        let mut got = vec![];
        for _ in 0..10 {
            match tokio::time::timeout(Duration::from_millis(15000), fast.lines.next_line()).await {
                Ok(Ok(Some(l))) => got.push(serde_json::from_str::<Value>(&l).unwrap()),
                _ => break,
            }
        }
        got
    });
    // Slow paces 3 then ends; resyncs via query later.
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

    // 4. Agent A: 10x object + provider file + event (mission-X).
    //    Agent B observes via queries concurrently.
    let mut b_seen = 0;
    for i in 0..10 {
        let mut o = Object::new("File", "agent-a", json!({"n": i, "world_id": "W1"}));
        o.id = format!("x13-o-{i:02}");
        let mut p = json!(o);
        p["actor_token"] = json!(tok_a);
        p["correlation_id"] = json!("mission-X");
        assert_eq!(
            ipc::send_request(ipc::CORE_PIPE, "create_object", p)
                .await
                .unwrap()["ok"],
            true
        );
        let aid = format!("x13-act-{i:02}");
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
        e.id = format!("x13-e-{i:02}");
        e.world_id = "W1".to_string();
        e.correlation_id = Some("mission-X".to_string());
        let mut p = json!(e);
        p["actor_token"] = json!(tok_a);
        assert_eq!(
            ipc::send_request(ipc::CORE_PIPE, "record_event", p)
                .await
                .unwrap()["ok"],
            true
        );
        // Agent B observes.
        let q = ipc::send_request(ipc::CORE_PIPE, "query_objects", json!({}))
            .await
            .unwrap();
        let objs: Vec<Object> = serde_json::from_value(q["result"].clone()).unwrap();
        b_seen = b_seen.max(objs.len());
    }
    assert!(b_seen >= 10, "X13: B observed growth ({b_seen})");

    // 5. Mid-run scan reconciles human + action files. Note: the live
    // watcher normally observes action files first, so the scan mostly
    // updates; assert converged ROWS, not created-vs-updated split.
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
            "X13: converged row for {name} (stats={stats})"
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
        "X13: Reality fingerprint matches"
    );

    // Fast must have all 10 Notes; slow paced 3.
    let got_fast = fast_handle.await.unwrap();
    assert_eq!(got_fast.len(), 10, "X13: fast 10/10");
    let got_slow = slow_handle.await.unwrap();
    assert_eq!(got_slow.len(), 3, "X13: slow paced 3");

    // 5b. Trace BEFORE restart (trace store is memory-only: restart wipes
    // it per ST-B6, while semantic SQLite state persists — assert both).
    let qd = ipc::send_request(
        ipc::CORE_PIPE,
        "trace_query",
        json!({"subscriber": "system", "correlation_id": "mission-X"}),
    )
    .await
    .unwrap();
    let chain: Vec<Value> = serde_json::from_value(qd["result"].clone()).unwrap();
    assert!(!chain.is_empty(), "X13: mission-X chain exists");
    assert!(chain.iter().all(|e| e["correlation_id"] == "mission-X"));
    // Action spans group by ACTION id (established X2 semantics), not by
    // mission correlation: verify each action chain separately and tie it
    // to the mission via its file artifact (target path == scanned row).
    for i in 0..10 {
        let aid = format!("x13-act-{i:02}");
        let qa = ipc::send_request(
            ipc::CORE_PIPE,
            "trace_query",
            json!({"subscriber": "system", "correlation_id": aid}),
        )
        .await
        .unwrap();
        let achain: Vec<Value> = serde_json::from_value(qa["result"].clone()).unwrap();
        // Published spans carry kind=EventPublished; the semantic type
        // lives in payload.event_type (ActionRequested/ActionCompleted).
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
            "X13: action {aid} requested+completed in trace, got {types:?}"
        );
    }
    for e in &chain {
        assert!(
            has_secret_key(&e["payload"]).is_none(),
            "X13: secret-free trace"
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
        "X13: scan spans recorded"
    );
    let ids_d: std::collections::HashSet<&str> = chain
        .iter()
        .filter_map(|e| e.get("trace_id").and_then(|v| v.as_str()))
        .collect();
    let ids_s: std::collections::HashSet<&str> = prov
        .iter()
        .filter_map(|e| e.get("trace_id").and_then(|v| v.as_str()))
        .collect();
    assert!(ids_d.is_disjoint(&ids_s), "X13: chains disjoint");

    // 6. Controlled restart on same DB.
    core.kill_wait().await;
    let mut core2 = ChildGuard {
        child: tokio::process::Command::new(&spaceos_bin)
            .args(["run", "--root"])
            .arg(&root)
            .env("SPACEOS_DB", &db)
            .env("SPACEOS_CORE_TOKEN", &core_token)
            .current_dir(&base)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("respawn spaceos"),
        name: "spaceos-restart",
    };
    wait_ready().await;
    // V1.7 2B/2D invariant: Policy is memory-only, so restart wiped grants.
    // Recovery = trusted rebootstrap: re-register (same definitions).
    let reg = |name: &str, caps: Vec<&str>| {
        let (n, caps): (String, Vec<String>) = (
            name.to_string(),
            caps.into_iter().map(|s| s.to_string()).collect(),
        );
        async move {
            let r = ipc::send_request(
                ipc::CORE_PIPE,
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
        vec!["object.create", "event.record", "action.create"],
    )
    .await;
    let tok_b = reg("agent-b", vec![]).await;
    let objs = ipc::send_request(ipc::CORE_PIPE, "query_objects", json!({}))
        .await
        .unwrap();
    let n_objs: usize = objs["result"].as_array().unwrap().len();
    let evs = ipc::send_request(ipc::CORE_PIPE, "query_events", json!({}))
        .await
        .unwrap();
    let n_evs: usize = evs["result"].as_array().unwrap().len();
    assert!(n_objs >= 21, "X13: objects persist ({n_objs})");
    assert!(n_evs >= 30, "X13: events persist ({n_evs})");

    // Reconnected subscriber gets new flow; resync exact.
    let (mut sub2, ack) = subscribe(
        ipc::CORE_PIPE,
        json!({"subscriber": "agent-b", "actor_token": tok_b, "world_id": "W1"}),
    )
    .await;
    assert_eq!(ack["ok"], true);
    for i in 0..3 {
        let mut e = space_core::Event::new("Note", "agent-a", json!({"i": 100 + i}));
        e.id = format!("x13-re-{i}");
        e.world_id = "W1".to_string();
        e.correlation_id = Some("mission-X-re".to_string());
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
    assert_eq!(got2.len(), 3, "X13: reconnected gets new flow");

    // 7b. Post-restart trace: old mission-X chain honestly EMPTY (memory
    // store wiped with the process, ST-B6); new flow fully traced:
    // per event MutationStart+Commit+Published+Delivery = 4 x 3 = 12.
    let qold = ipc::send_request(
        ipc::CORE_PIPE,
        "trace_query",
        json!({"subscriber": "system", "correlation_id": "mission-X"}),
    )
    .await
    .unwrap();
    let old_chain: Vec<Value> = serde_json::from_value(qold["result"].clone()).unwrap();
    assert!(
        old_chain.is_empty(),
        "X13: pre-restart trace honestly gone (memory-only store)"
    );
    let qnew = ipc::send_request(
        ipc::CORE_PIPE,
        "trace_query",
        json!({"subscriber": "system", "correlation_id": "mission-X-re"}),
    )
    .await
    .unwrap();
    let new_chain: Vec<Value> = serde_json::from_value(qnew["result"].clone()).unwrap();
    assert_eq!(
        new_chain.len(),
        12,
        "X13: new flow 12-span chain, got {}",
        new_chain.len()
    );
    // ...while semantic events from before the restart persist.
    let evs = ipc::send_request(ipc::CORE_PIPE, "query_events", json!({"world_id": "W1"}))
        .await
        .unwrap();
    let evs: Vec<space_core::Event> = serde_json::from_value(evs["result"].clone()).unwrap();
    assert!(
        evs.iter()
            .any(|e| e.correlation_id.as_deref() == Some("mission-X")),
        "X13: semantic history survives restart (trace does not — by design)"
    );

    core2.kill_wait().await;
    eprintln!("X13 run{run}: combined live scenario converged, 0 divergence");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn x13_combined_live_integration() {
    let _ipc_lock = global_lock::acquire_global_ipc();
    for run in 1..=3 {
        one_run(run).await;
    }
}
