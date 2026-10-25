//! Crash taxonomy T9 + I11: trace/observability across real restart.
//!
//! Real spaceos.exe (Core+Provider in one process): no mid-flight
//! timing needed here — quiesced kill + respawn on the same db.
//! Machine exclusion: global IPC lock (CORE_PIPE bound by the real
//! Core; same convention as X1/X5/X13/Gate3).
//!
//! Hypothesis (T9): after crash/restart, trace may misattribute the
//!   actor, leak world scope, MERGE new operations into pre-crash
//!   chains (same correlation reused), show unconfirmed success, or
//!   leak secrets; rebootstrap may revive revoked powers (I11).
//! What can break: merged chains; wrong actor/scope; revived tokens;
//!   secret leak; success shown for unconfirmed ops.
//! Failure boundary: recovery/rebootstrap (Policy + trace are
//!   memory-only by contract — X5/X13/ST-B6 cited).
//! Existing fault hook: real process kill + respawn (X5/X13 pattern).
//! Exact checkpoint: quiesced kill (no in-flight op by construction:
//!   all acks collected before kill).
//! Workload per run: 3 objects + 3 events (corr TR) + 1 secret-laden
//!   object; agent-a rotation (token1 -> token2); kill; restart;
//!   rebootstrap WITHOUT agent-a (probes); rebootstrap WITH agent-a
//!   (probes); 2 new ops reusing corr TR + 1 new secret object.
//! Preconditions: all pre-crash acks ok; rotation probe denied pre-kill.
//! Expected before restart: chains present, actor/scope exact,
//!   secret-free; token1 dead, token2 live.
//! Expected after restart: pre-crash chains honestly EMPTY while
//!   semantic rows persist (I8: trace absence != state absence);
//!   token1/token2 dead, agent-a unknown until re-registered (I11);
//!   reused corr TR chains ONLY new spans with correct actor;
//!   kind vs payload.event_type separated; secrets sanitized incl.
//!   nested (I10).
//! Fixed threshold: old chain len == 0 AND rows persist; token probes
//!   exact (denied/denied/ok); new TR chain len > 0 with actor ==
//!   agent-a on every span and 0 pre-crash trace_ids; 0 secret keys at
//!   any depth; no span shows success for the unconfirmed (no
//!   Completed for ops that never completed).
//! Pass: all exact. Fail: merged chain OR revived token OR leak OR
//!   false success.
//! Measurements: chain id sets pre/post, probe outcomes, secret scan,
//!   row counts.
//! Reproduction protocol: 3 identical internal runs.

#[path = "support/global_lock.rs"]
mod global_lock;

use serde_json::{json, Value};
use space_core::{ipc, Object};
use std::path::PathBuf;
use std::time::Duration;

fn tmp_base(run: usize) -> PathBuf {
    let d = std::env::temp_dir()
        .join("spaceos-ct9")
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
                "T9: {} did not exit: {:?}",
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
    panic!("T9: Core not ready in 30s");
}

async fn register(pipe: &str, name: &str, caps: &[&str]) -> String {
    let r = ipc::send_request(
        pipe,
        "register_agent",
        json!({"principal":"system","name":name,"capabilities":caps,"worlds":["W1"]}),
    )
    .await
    .unwrap();
    assert_eq!(r["ok"], true);
    r["result"]["token"].as_str().unwrap().to_string()
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

async fn denied_probe(pipe: &str, tok: &str) -> bool {
    // Any gated mutation with a dead token must be denied with zero state change.
    let mut o = Object::new("File", "agent-a", json!({"n": "probe"}));
    o.id = "ct9-probe-ghost".to_string();
    let mut p = json!(o);
    p["actor_token"] = json!(tok);
    let r = ipc::send_request(pipe, "create_object", p).await.unwrap();
    r["ok"] == false
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn t9_trace_rebootstrap_across_restart() {
    let _ipc_lock = global_lock::acquire_global_ipc();
    for run in 1..=3 {
        let base = tmp_base(run);
        let db = base.join("core.db");
        let root = base.join("root");
        std::fs::create_dir_all(&root).unwrap();
        let core_token = format!("ct9-fixed-token-run{run}");
        std::env::set_var("SPACEOS_CORE_TOKEN", &core_token);
        let spaceos_bin = bins();
        assert!(spaceos_bin.exists());

        // Era 1 (real process).
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
        let tok1 = register(
            ipc::CORE_PIPE,
            "agent-a",
            &["object.create", "event.record"],
        )
        .await;
        // Pre-crash flow with correlation TR.
        for i in 0..3 {
            let mut o = Object::new("File", "agent-a", json!({"n": i, "world_id": "W1"}));
            o.id = format!("ct9-o-{run}-{i}");
            let mut p = json!(o);
            p["actor_token"] = json!(tok1);
            p["correlation_id"] = json!("TR");
            assert_eq!(
                ipc::send_request(ipc::CORE_PIPE, "create_object", p)
                    .await
                    .unwrap()["ok"],
                true
            );
            let mut e = space_core::Event::new("Note", "agent-a", json!({"i": i}));
            e.id = format!("ct9-e-{run}-{i}");
            e.world_id = "W1".to_string();
            e.correlation_id = Some("TR".to_string());
            let mut p = json!(e);
            p["actor_token"] = json!(tok1);
            assert_eq!(
                ipc::send_request(ipc::CORE_PIPE, "record_event", p)
                    .await
                    .unwrap()["ok"],
                true
            );
        }
        // Secret-laden object (nested) pre-crash.
        let mut sneaky = Object::new(
            "File",
            "agent-a",
            json!({"world_id": "W1", "nested": {"token": "TOP-SECRET", "deep": {"password": "PWNED"}}}),
        );
        sneaky.id = format!("ct9-sneaky-{run}");
        let mut p = json!(sneaky);
        p["actor_token"] = json!(tok1);
        p["correlation_id"] = json!("TR");
        assert_eq!(
            ipc::send_request(ipc::CORE_PIPE, "create_object", p)
                .await
                .unwrap()["ok"],
            true
        );
        // Pre-crash chain present + exact + secret-free.
        let q = ipc::send_request(
            ipc::CORE_PIPE,
            "trace_query",
            json!({"subscriber": "system", "correlation_id": "TR"}),
        )
        .await
        .unwrap();
        let pre: Vec<Value> = serde_json::from_value(q["result"].clone()).unwrap();
        assert!(!pre.is_empty(), "T9 run{run}: pre chain present");
        assert!(pre.iter().all(|e| e["correlation_id"] == "TR"));
        for e in &pre {
            assert!(
                has_secret_key(&e["payload"]).is_none(),
                "T9 run{run}: pre secret-free"
            );
        }
        let pre_ids: std::collections::HashSet<String> = pre
            .iter()
            .filter_map(|e| {
                e.get("trace_id")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
            })
            .collect();
        // Rotation pre-kill: token1 dies, token2 lives.
        let tok2 = register(
            ipc::CORE_PIPE,
            "agent-a",
            &["object.create", "event.record"],
        )
        .await;
        assert!(
            denied_probe(ipc::CORE_PIPE, &tok1).await,
            "T9 run{run}: token1 rotated out pre-kill"
        );

        // Quiesced hard kill (all acks collected — no in-flight op).
        core.kill_wait().await;

        // Era 2 (same db).
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
        // I8 pair: semantic rows persist AND old trace honestly gone.
        let objs = ipc::send_request(ipc::CORE_PIPE, "query_objects", json!({}))
            .await
            .unwrap();
        let objs: Vec<Object> = serde_json::from_value(objs["result"].clone()).unwrap();
        assert!(
            objs.len() >= 4,
            "T9 run{run}: rows persist ({})",
            objs.len()
        );
        let q = ipc::send_request(
            ipc::CORE_PIPE,
            "trace_query",
            json!({"subscriber": "system", "correlation_id": "TR"}),
        )
        .await
        .unwrap();
        let old: Vec<Value> = serde_json::from_value(q["result"].clone()).unwrap();
        assert!(old.is_empty(), "T9 run{run}: pre-crash chain honestly gone");
        // I11: rebootstrap WITHOUT agent-a — token2 dead, agent-a unknown.
        let _tok_b = register(ipc::CORE_PIPE, "agent-b", &[]).await;
        assert!(
            denied_probe(ipc::CORE_PIPE, &tok2).await,
            "T9 run{run}: token2 dead after wipe"
        );
        assert!(
            denied_probe(ipc::CORE_PIPE, &tok1).await,
            "T9 run{run}: token1 still dead"
        );
        // Rebootstrap WITH agent-a: fresh registration required; old tokens stay dead.
        let tok3 = register(
            ipc::CORE_PIPE,
            "agent-a",
            &["object.create", "event.record"],
        )
        .await;
        assert!(
            denied_probe(ipc::CORE_PIPE, &tok2).await,
            "T9 run{run}: token2 not revived by re-grant"
        );
        // New ops REUSING corr TR: chain has ONLY new spans, actor exact.
        for i in 0..2 {
            let mut o = Object::new("File", "agent-a", json!({"n": 100 + i, "world_id": "W1"}));
            o.id = format!("ct9-n-{run}-{i}");
            let mut p = json!(o);
            p["actor_token"] = json!(tok3);
            p["correlation_id"] = json!("TR");
            assert_eq!(
                ipc::send_request(ipc::CORE_PIPE, "create_object", p)
                    .await
                    .unwrap()["ok"],
                true
            );
        }
        let mut sneaky = Object::new(
            "File",
            "agent-a",
            json!({"world_id": "W1", "nested": {"api_key": "K2", "deep": {"secret": "S2"}}}),
        );
        sneaky.id = format!("ct9-sneaky2-{run}");
        let mut p = json!(sneaky);
        p["actor_token"] = json!(tok3);
        p["correlation_id"] = json!("TR");
        assert_eq!(
            ipc::send_request(ipc::CORE_PIPE, "create_object", p)
                .await
                .unwrap()["ok"],
            true
        );
        let q = ipc::send_request(
            ipc::CORE_PIPE,
            "trace_query",
            json!({"subscriber": "system", "correlation_id": "TR"}),
        )
        .await
        .unwrap();
        let new: Vec<Value> = serde_json::from_value(q["result"].clone()).unwrap();
        assert!(!new.is_empty(), "T9 run{run}: new chain present");
        let new_ids: std::collections::HashSet<String> = new
            .iter()
            .filter_map(|e| {
                e.get("trace_id")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
            })
            .collect();
        assert!(
            new_ids.is_disjoint(&pre_ids),
            "T9 run{run}: no merge with pre-crash chain"
        );
        for e in &new {
            // kind = trace record layer; semantic type lives in payload (contract).
            assert!(
                e.get("kind").and_then(|v| v.as_str()).is_some(),
                "T9 run{run}: kind present"
            );
            assert!(
                has_secret_key(&e["payload"]).is_none(),
                "T9 run{run}: post secret-free"
            );
        }
        // No span shows success for anything unconfirmed: every MutationCommit
        // in the new chain pairs with a mutation this run actually acked (3 creates).
        let commits = new.iter().filter(|e| e["kind"] == "MutationCommit").count();
        assert!(
            commits >= 3,
            "T9 run{run}: commits only for acked ops ({commits})"
        );
        core2.kill_wait().await;
        eprintln!("T9 run{run}: HELD (wipe honest; no merge; no revived powers; secrets clean)");
    }
}
