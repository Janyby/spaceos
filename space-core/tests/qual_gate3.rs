//! Gate 3 EXTEND — peer process death during another agent's action.
//!
//! M1/M2/X5/X6/matrix cover in-process concurrency and full lifecycle
//! (cited). New boundary: a REAL agent process dying mid-burst while a
//! peer (test-driven IPC client, documented split) hammers a shared
//! object — the survivor's work must complete intact and the victim
//! must leave no corrupt/duplicate state.
//! Hypothesis: peer death may corrupt shared rows or fail the survivor.
//! Failure boundary: Core storage (per-statement atomicity) + agent
//! registry (victim's token dies with... actually tokens persist in the
//! live Core; victim simply stops calling).
//! Exact workload: real Core+Provider processes (temp DB+root); victim
//! = real agent-demo process; peer = 200 sequential updates on one
//! shared object via IPC; kill victim mid-burst; peer must finish 0
//! errors; shared row intact with member marker; agent rows <= 1.
//! Fixed threshold: peer 200/200 acked; final marker in written set;
//! row identity intact; agent rows <= 1; no hung children.
//! Expected: HELD (per-statement atomicity; dead peer writes nothing).
//! Pass criterion: all exact. Failure criterion: peer error OR corrupt
//! row OR duplicate agent rows OR hang.
//! Reproduction protocol: 3 identical runs.
//! Machine exclusion: no other Core on CORE_PIPE during the run.

use serde_json::{json, Value};
use space_core::{ipc, Object};
use std::path::PathBuf;
use std::time::Duration;

/// Cross-process exclusion for the GLOBAL core pipe (see
/// support/global_lock.rs): a real spaceos.exe process binds it.
#[path = "support/global_lock.rs"]
mod global_lock;

fn tmp_base(tag: &str) -> PathBuf {
    let d = std::env::temp_dir().join("spaceos-qg3").join(tag);
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
                "GATE3: {} did not exit: {:?}",
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
    panic!("GATE3: Core not ready in 30s");
}

async fn wait_agent_row() {
    for _ in 0..60 {
        match ipc::send_request(ipc::CORE_PIPE, "query_objects", json!({})).await {
            Ok(v) => {
                let objs: Vec<Object> =
                    serde_json::from_value(v["result"].clone()).unwrap_or_default();
                if objs.iter().any(|o| o.id == "agent:SpaceOS Demo Agent") {
                    return;
                }
            }
            _ => {}
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    panic!("GATE3: agent row never appeared");
}

async fn register(pipe: &str, name: &str) -> String {
    let r = ipc::send_request(
        pipe,
        "register_agent",
        json!({"principal":"system","name":name,
            "capabilities":["object.create","object.update"],"worlds":[]}),
    )
    .await
    .unwrap();
    assert_eq!(r["ok"], true);
    r["result"]["token"].as_str().unwrap().to_string()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn gate3_peer_death_during_burst() {
    let _ipc_lock = global_lock::acquire_global_ipc();
    let base = tmp_base("qg3");
    let db = base.join("core.db");
    let root = base.join("root");
    std::fs::create_dir_all(&root).unwrap();
    let core_token = "qg3-fixed-token";
    std::env::set_var("SPACEOS_CORE_TOKEN", core_token);
    let (spaceos_bin, agent_bin) = bins();
    assert!(spaceos_bin.exists() && agent_bin.exists());

    let mut core = ChildGuard {
        child: tokio::process::Command::new(&spaceos_bin)
            .args(["run", "--root"])
            .arg(&root)
            .env("SPACEOS_DB", &db)
            .env("SPACEOS_CORE_TOKEN", core_token)
            .current_dir(&base)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("spawn spaceos"),
        name: "spaceos",
    };
    wait_ready().await;

    // Shared object for the peer burst.
    let tok = register(ipc::CORE_PIPE, "peer-g3").await;
    let mut base_o = Object::new("File", "peer-g3", json!({"marker": "init"}));
    base_o.id = "qg3-shared".to_string();
    let mut p = json!(base_o);
    p["actor_token"] = json!(tok);
    assert_eq!(
        ipc::send_request(ipc::CORE_PIPE, "create_object", p)
            .await
            .unwrap()["ok"],
        true
    );

    // Victim: real agent-demo; kill once its mission is underway.
    let mut victim = tokio::process::Command::new(&agent_bin)
        .env("SPACEOS_CORE_TOKEN", core_token)
        .current_dir(&base)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn victim");
    wait_agent_row().await;

    // Peer burst (200 updates) WHILE victim is alive, then kill mid-burst.
    let peer = tokio::spawn(async move {
        let mut errs = vec![];
        for i in 0..200 {
            let mut o = Object::new("File", "peer-g3", json!({"marker": format!("p-{i}")}));
            o.id = "qg3-shared".to_string();
            let mut params = json!(o);
            // NOTE: token moved into async block would complicate kill
            // timing; re-read pattern used: token string cloned per iter
            // is unnecessary — token lives in this async block's scope.
            params["actor_token"] = json!(tok);
            match ipc::send_request(ipc::CORE_PIPE, "update_object", params).await {
                Ok(v) if v["ok"].as_bool() == Some(true) => {}
                other => errs.push(format!("{other:?}")),
            }
            if i == 100 {
                tokio::task::yield_now().await;
            }
        }
        errs
    });
    // Let the burst get underway, then kill the victim mid-burst.
    tokio::time::sleep(Duration::from_millis(300)).await;
    victim.kill().await.expect("kill victim");
    let _ = victim.wait().await;
    let errs: Vec<String> = peer.await.unwrap();
    assert!(
        errs.is_empty(),
        "GATE3 peer errors: {:?}",
        &errs[..errs.len().min(3)]
    );

    // Survivor state intact: member marker, identity intact, <=1 agent row.
    let g = ipc::send_request(ipc::CORE_PIPE, "get_object", json!({"id": "qg3-shared"}))
        .await
        .unwrap();
    let obj: Object = serde_json::from_value(g["result"].clone()).unwrap();
    assert_eq!(obj.id, "qg3-shared");
    assert_eq!(obj.r#type, "File");
    let marker = obj
        .properties
        .get("marker")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    assert!(
        marker == "init" || marker.starts_with("p-"),
        "GATE3 marker member, got {marker:?}"
    );
    let q = ipc::send_request(ipc::CORE_PIPE, "query_objects", json!({}))
        .await
        .unwrap();
    let objs: Vec<Object> = serde_json::from_value(q["result"].clone()).unwrap();
    assert!(
        objs.iter()
            .filter(|o| o.id == "agent:SpaceOS Demo Agent")
            .count()
            <= 1,
        "GATE3: at most one agent row"
    );
    core.kill_wait().await;
    eprintln!("GATE3: peer 200/200 through victim death, shared row intact, no dupes");
}
