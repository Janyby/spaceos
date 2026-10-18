//! X5 — Agent runtime lifecycle with REAL processes.
//!
//! Hypothesis: process crash, restart, duplicate start, or IPC loss may
//! confuse semantic Agent identity with process identity (duplicate rows,
//! duplicated side effects, stale capabilities).
//! What can break: duplicate agent rows;半截 mission state blocking
//! restart; old token accepted after re-registration.
//! Failure boundary: agent process lifecycle vs Core identity registry
//! (v17_agent covers registry logic via test clients, cited; THIS file
//! covers real OS processes).
//! Why it can break: crash mid-mission leaves partial state; duplicate
//! processes race mutable shared rows.
//! Exact workload: real Core+Provider process + real agent-demo
//! processes: (a) kill agent mid-mission (after its row appears), respawn,
//! await completion; (b) STAGGERED duplicate start: dup1 runs until its
//! row exists, then dup2 starts — dup2's registration rotates the token
//! mid-mission, so dup1 must fail (lockout) and dup2 completes.
//! (Revision note: v1 of this test required BOTH duplicates to complete;
//! run 1 proved that contradicts the confirmed lockout invariant
//! (v17_agent: last writer wins, loser locked out). Original result kept
//! as evidence; this revision asserts the lockout contract instead.
//! Thresholds were fixed in this revision BEFORE its first run.)
//! Dataset: temp DB + temp root per test; shared core auth env.
//! Fixed threshold: exactly 1 agent row always; crash victim restart
//! completes mission; staggered duplicate => loser fails + winner
//! completes; no hung children (guarded kill+wait, Drop fallback);
//! file effect convergent.
//! Expected behavior: HELD.
//! Measurement: agent row counts, mission artifacts, child exits.
//! Pass criterion: all exact. Failure criterion: duplicate agent rows OR
//! restart failure OR hung child.
//! Reproduction protocol: 3 identical runs.
//! Machine exclusion: no other Core on CORE_PIPE during the run.

use serde_json::json;
use space_core::ipc;
use std::path::PathBuf;
use std::time::Duration;

/// Cross-process exclusion for the GLOBAL core pipe + shared result
/// file (see support/global_lock.rs): real spaceos.exe processes bind
/// machine-global names.
#[path = "support/global_lock.rs"]
mod global_lock;

fn tmp_base(tag: &str) -> PathBuf {
    let d = std::env::temp_dir().join("spaceos-x5").join(tag);
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

/// Result file name INSIDE the served root (SPACEOS_RESULT_FILE); the
/// provider gates writes to its effective roots.
const RESULT_FILE_NAME: &str = "v1-result.txt";

struct ChildGuard {
    child: tokio::process::Child,
    name: &'static str,
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        // Best-effort kill on unwind paths so no child outlives the test.
        let _ = self.child.start_kill();
    }
}

impl ChildGuard {
    async fn kill_wait(&mut self) {
        let _ = self.child.kill().await;
        match tokio::time::timeout(Duration::from_secs(15), self.child.wait()).await {
            Ok(Ok(_)) => {}
            other => panic!(
                "X5: {} did not exit: {:?}",
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
    panic!("X5: Core not ready in 30s");
}

async fn wait_agent_row() {
    // Tight cadence (10ms): the kill point must land in the ~45ms window
    // between the agent row appearing and its result-file action; coarse
    // polling lets the victim finish the whole mission first, and the
    // same cadence staggers the duplicate-start race deterministically.
    for _ in 0..300 {
        match ipc::send_request(ipc::CORE_PIPE, "query_objects", json!({})).await {
            Ok(v) => {
                let objs: Vec<space_core::Object> =
                    serde_json::from_value(v["result"].clone()).unwrap_or_default();
                if objs.iter().any(|o| o.id == "agent:SpaceOS Demo Agent") {
                    return;
                }
            }
            _ => {}
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("X5: agent row never appeared");
}

async fn count_agent_rows() -> usize {
    let v = ipc::send_request(ipc::CORE_PIPE, "query_objects", json!({}))
        .await
        .unwrap();
    let objs: Vec<space_core::Object> = serde_json::from_value(v["result"].clone()).unwrap();
    objs.iter()
        .filter(|o| o.id == "agent:SpaceOS Demo Agent")
        .count()
}

async fn run_agent(agent_bin: &PathBuf, base: &PathBuf, token: &str) -> std::process::ExitStatus {
    tokio::process::Command::new(agent_bin)
        .env("SPACEOS_CORE_TOKEN", token)
        .current_dir(base)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn agent")
        .wait()
        .await
        .expect("agent wait")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn x5_crash_restart_and_duplicate_start() {
    let _ipc_lock = global_lock::acquire_global_ipc();
    let base = tmp_base("x5");
    let db = base.join("core.db");
    let root = base.join("root");
    std::fs::create_dir_all(&root).unwrap();
    // The provider gates writes to its served roots (`--root`): point the
    // demo's result file INSIDE the root via the env knob.
    let result_file = root.join(RESULT_FILE_NAME);
    let _ = std::fs::remove_file(&result_file);
    let core_token = "x5-fixed-token";
    std::env::set_var("SPACEOS_CORE_TOKEN", core_token);
    std::env::set_var("SPACEOS_RESULT_FILE", &result_file);
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

    // (a) Crash mid-mission: start agent, kill once its row appears.
    let mut victim = tokio::process::Command::new(&agent_bin)
        .env("SPACEOS_CORE_TOKEN", core_token)
        .current_dir(&base)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn victim");
    wait_agent_row().await;
    victim.kill().await.expect("kill victim");
    let _ = victim.wait().await;
    // Partial mission state exists, but exactly one agent row.
    assert_eq!(
        count_agent_rows().await,
        1,
        "X5: single agent row after crash"
    );

    // Restart: re-registers (new token path) and completes the mission.
    let st = run_agent(&agent_bin, &base, core_token).await;
    assert!(st.success(), "X5: restarted agent completes");
    assert_eq!(count_agent_rows().await, 1, "X5: still single agent row");
    let content = std::fs::read_to_string(&result_file).expect("X5: result file");
    assert_eq!(content, "V1 Result");
    let _ = std::fs::remove_file(&result_file);

    // (b) Duplicate start: staggered so the lockout is deterministic.
    // Established invariant (v17_agent, cited): duplicate start = last
    // writer wins, loser locked out via token rotation. Start dup1, wait
    // until its mission is underway, then start dup2: dup2's registration
    // rotates the token mid-mission, so dup1 MUST fail and dup2 completes.
    let mut d1 = tokio::process::Command::new(&agent_bin)
        .env("SPACEOS_CORE_TOKEN", core_token)
        .current_dir(&base)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn dup1");
    wait_agent_row().await;
    // dup1 is mid-mission now (its row exists, status running).
    let mut d2 = tokio::process::Command::new(&agent_bin)
        .env("SPACEOS_CORE_TOKEN", core_token)
        .current_dir(&base)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn dup2");
    let (s1, s2) = tokio::join!(d1.wait(), d2.wait());
    assert!(!s1.expect("dup1 wait").success(), "X5: loser locked out");
    assert!(s2.expect("dup2 wait").success(), "X5: winner completes");
    assert_eq!(
        count_agent_rows().await,
        1,
        "X5: duplicate start => 1 agent row"
    );
    assert_eq!(
        std::fs::read_to_string(&result_file).expect("X5: result file 2"),
        "V1 Result"
    );
    let _ = std::fs::remove_file(&result_file);

    core.kill_wait().await;
    eprintln!("X5: crash+restart and duplicate start keep single identity, mission converges");
}
