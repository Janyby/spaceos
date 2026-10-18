//! X1 — LIVE agent-demo end-to-end via real processes.
//!
//! Hypothesis: real agent-demo can traverse the full SpaceOS chain with
//! real Core + Provider, but the harness may fail on process coordination
//! (handshake, readiness, timeouts, teardown, state pollution).
//! What can break: readiness race (token file), provider not registered
//! when the action fires, hung children, leftover state, Core pipe clash.
//! Failure boundary: test harness / process orchestration (NOT Core).
//! Why it can break: two processes share CORE_PIPE + core auth; the
//! demo result path must stay inside the provider's served root (wired
//! via SPACEOS_RESULT_FILE).
//! Exact workload (per run, isolated temp DB + temp root): spawn
//! spaceos.exe run; poll readiness; spawn agent-demo.exe; await exit;
//! verify authoritative Core state + real file + events + trace; kill,
//! restart on SAME db and re-verify; kill again. 3 identical runs.
//! Test environment: Windows named pipes; requires NO other Core bound
//! to \\.\pipe\spaceos-core on this machine during the run. Shared
//! core auth via SPACEOS_CORE_TOKEN env (no token-file race). The demo's
//! hardcoded C:\spaceos-test\v1-result.txt is pre-cleaned and removed
//! afterwards (repo-conventional dir).
//! Fixed threshold: agent exit ok; agent object completed; Result +
//! relations present; AgentStarted/ActionCompleted/AgentStopped by
//! agent-demo; file content == "V1 Result"; Core-side trace spans for
//! agent-demo exist; restart preserves state; no hung child (wait ok).
//! Expected behavior: HELD (harness works; prior BLOCKED was orchestration).
//! Measurement: exit codes, state counts, file bytes, trace counts.
//! Pass criterion: all exact, 3/3 runs.
//! Failure criterion: any missing artifact, hung child, or state loss.
//! Reproduction protocol: 3 identical runs (loop in one test).

use serde_json::json;
use space_core::ipc;
use std::path::PathBuf;
use std::time::Duration;

/// Cross-process exclusion for GLOBAL pipes + shared result file (see
/// support/global_lock.rs, mirrored from space-core tests): real
/// spaceos.exe processes bind machine-global names.
#[path = "support/global_lock.rs"]
mod global_lock;

fn tmp_base(run: usize) -> PathBuf {
    let d = std::env::temp_dir()
        .join("spaceos-x1")
        .join(format!("run{run}"));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn bins() -> (PathBuf, PathBuf) {
    // tests execute from target/debug/deps/<test>.exe
    let mut dir = std::env::current_exe().unwrap();
    dir.pop();
    dir.pop();
    (dir.join("spaceos.exe"), dir.join("agent-demo.exe"))
}

/// Result file name INSIDE the served root (SPACEOS_RESULT_FILE); the
/// provider gates writes to its effective roots.
const RESULT_FILE_NAME: &str = "v1-result.txt";

struct ChildGuard {
    child: tokio::process::Child,
    name: &'static str,
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        // Panic paths skip kill_wait: never leave orphan daemons behind
        // (they bind the machine-global pipe and break later runs).
        let _ = self.child.start_kill();
    }
}

impl ChildGuard {
    async fn kill_wait(&mut self) {
        let _ = self.child.kill().await;
        match tokio::time::timeout(Duration::from_secs(15), self.child.wait()).await {
            Ok(Ok(status)) => eprintln!("X1: {} exited: {status}", self.name),
            other => panic!(
                "X1: {} did not exit cleanly: {:?}",
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
    panic!("X1: Core not ready in 30s");
}

async fn one_run(run: usize) {
    let base = tmp_base(run);
    let db = base.join("core.db");
    let root = base.join("root");
    std::fs::create_dir_all(&root).unwrap();
    // The provider gates writes to its served roots (`--root`): point the
    // demo's result file INSIDE the root via the env knob.
    let result_file = root.join(RESULT_FILE_NAME);
    let _ = std::fs::remove_file(&result_file);

    // Deterministic shared core auth for all three processes (no file race).
    let core_token = format!("x1-fixed-token-run{run}");
    std::env::set_var("SPACEOS_CORE_TOKEN", &core_token);
    std::env::set_var("SPACEOS_RESULT_FILE", &result_file);

    let (spaceos_bin, agent_bin) = bins();
    assert!(spaceos_bin.exists(), "spaceos.exe missing: {spaceos_bin:?}");
    assert!(agent_bin.exists(), "agent-demo.exe missing: {agent_bin:?}");

    // 1. Real Core + Provider process.
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

    // Provider must be registered before the action fires.
    let mut registered = false;
    for _ in 0..20 {
        let r = ipc::send_request(ipc::CORE_PIPE, "list_providers", json!({}))
            .await
            .unwrap();
        if r["result"]
            .as_array()
            .map(|a| a.iter().any(|p| p["id"] == "filesystem"))
            .unwrap_or(false)
        {
            registered = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert!(registered, "X1: provider registered");

    // 2. Real agent process.
    let out = tokio::time::timeout(
        Duration::from_secs(120),
        tokio::process::Command::new(&agent_bin)
            .env("SPACEOS_CORE_TOKEN", &core_token)
            .current_dir(&base)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("spawn agent-demo")
            .wait_with_output(),
    )
    .await
    .expect("X1: agent-demo timed out")
    .expect("X1: agent wait failed");
    assert!(
        out.status.success(),
        "X1: agent-demo exit ok: {}",
        out.status
    );

    // 3. Authoritative Core state.
    let objs = ipc::send_request(ipc::CORE_PIPE, "query_objects", json!({}))
        .await
        .unwrap();
    let objs: Vec<space_core::Object> = serde_json::from_value(objs["result"].clone()).unwrap();
    let agent = objs
        .iter()
        .find(|o| o.id == "agent:SpaceOS Demo Agent")
        .expect("X1: agent object");
    assert_eq!(
        agent.properties.get("status").and_then(|v| v.as_str()),
        Some("completed"),
        "X1: agent completed"
    );
    assert!(
        objs.iter().any(|o| o.r#type == "Result"
            && o.properties.get("name").and_then(|v| v.as_str()) == Some("V1 Result")),
        "X1: Result object"
    );
    let rels = ipc::send_request(ipc::CORE_PIPE, "query_relations", json!({}))
        .await
        .unwrap();
    let rels: Vec<space_core::Relation> = serde_json::from_value(rels["result"].clone()).unwrap();
    assert!(
        rels.iter().any(|r| r.relation_type == "works_on")
            && rels.iter().any(|r| r.relation_type == "produces"),
        "X1: relations"
    );
    let evs = ipc::send_request(ipc::CORE_PIPE, "query_events", json!({}))
        .await
        .unwrap();
    let evs: Vec<space_core::Event> = serde_json::from_value(evs["result"].clone()).unwrap();
    for t in ["AgentStarted", "ActionCompleted", "AgentStopped"] {
        assert!(
            evs.iter()
                .any(|e| e.event_type == t && e.actor == "agent-demo"),
            "X1: event {t} by agent-demo"
        );
    }

    // 4. Real external effect.
    let content = std::fs::read_to_string(&result_file).expect("X1: result file");
    assert_eq!(content, "V1 Result", "X1: file content");
    let _ = std::fs::remove_file(&result_file);

    // 5. Core-side trace spans for the demo's IPC (its process-local
    // buffer is gone with the process; Core keeps the matching half).
    let tq = ipc::send_request(
        ipc::CORE_PIPE,
        "trace_query",
        json!({"subscriber": "system", "agent_id": "agent-demo"}),
    )
    .await
    .unwrap();
    let spans: Vec<serde_json::Value> = serde_json::from_value(tq["result"].clone()).unwrap();
    assert!(!spans.is_empty(), "X1: Core-side trace spans exist");
    assert!(
        spans.iter().any(|e| e["kind"] == "MutationCommit"),
        "X1: commit spans present"
    );
    eprintln!("X1 run{run}: Core-side trace spans={}", spans.len());

    // 6. Restart on SAME db: state persists, no hang, no corruption.
    core.kill_wait().await;
    let n_objs_before = objs.len();
    let n_evs_before = evs.len();
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
    let objs2 = ipc::send_request(ipc::CORE_PIPE, "query_objects", json!({}))
        .await
        .unwrap();
    let objs2: Vec<space_core::Object> = serde_json::from_value(objs2["result"].clone()).unwrap();
    let evs2 = ipc::send_request(ipc::CORE_PIPE, "query_events", json!({}))
        .await
        .unwrap();
    let evs2: Vec<space_core::Event> = serde_json::from_value(evs2["result"].clone()).unwrap();
    // Restart durability: nothing may be lost across the kill — every
    // object and event present before must still be present after. The
    // provider keeps watching the served root and may APPEND its own
    // artifacts between snapshots (the result file lives inside the root
    // now), so exact count equality is not the contract; "nothing lost"
    // (per-id, with an append-only count floor for events) is.
    assert!(
        objs2.len() >= n_objs_before,
        "X1: objects survive restart ({n_objs_before} -> {})",
        objs2.len()
    );
    let obj_ids_after: std::collections::HashSet<&str> =
        objs2.iter().map(|o| o.id.as_str()).collect();
    for o in &objs {
        assert!(
            obj_ids_after.contains(o.id.as_str()),
            "X1: object {} lost across restart",
            o.id
        );
    }
    assert!(
        evs2.len() >= n_evs_before,
        "X1: events survive restart ({n_evs_before} -> {})",
        evs2.len()
    );
    let ev_ids_after: std::collections::HashSet<&str> =
        evs2.iter().map(|e| e.id.as_str()).collect();
    for e in &evs {
        assert!(
            ev_ids_after.contains(e.id.as_str()),
            "X1: event {} lost across restart",
            e.id
        );
    }
    core2.kill_wait().await;

    // No hung processes beyond the guarded children (they were waited).
    eprintln!("X1 run{run}: PASS, no hung processes, state persistent");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn x1_live_agent_demo_end_to_end() {
    let _ipc_lock = global_lock::acquire_global_ipc();
    // Machine exclusion: no other Core may hold CORE_PIPE during this test.
    for run in 1..=3 {
        one_run(run).await;
    }
}
