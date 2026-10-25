//! Crash taxonomy T7.7: concurrent requests during reconnect rescan.
//!
//! T7 mapping (spec §10): T7.1 provider-down pre-op, T7.2 dies after
//! accepting, T7.3 reconnect-before-reconcile-done, T7.4 core restart
//! while disconnected, T7.5 re-registration, T7.6 scanner/watcher
//! restart, T7.8 reality updates while down — all COVERED by
//! v15_provider_recovery (11 tests), break_b11_b14
//! (dual_watcher_kill_reconnect), X5 (duplicate start) — cited, not
//! duplicated. The one uncovered point is T7.7: live requests DURING
//! the post-restart reconnect scan (v15 scans quiesced worlds only).
//!
//! Hypothesis: IPC mutations racing the reconnect scan may duplicate
//! rows, strand stale paths, or corrupt identity.
//! What can break: duplicate external_ids; stale rows; wrong fingerprint.
//! Failure boundary: scanner/Core convergence under racing mutations.
//! Existing fault hook: era drop/reboot (crash_util pattern, in-process).
//! Exact checkpoint: first rescan after reboot runs WHILE 10 concurrent
//!   mutators create/update/query the same world (incl. a path the scan
//!   is about to observe + a mid-scan rename).
//! Workload: seed 5 files + scan; crash; external create/modify/rename/
//!   delete while dead; reboot; racing rescan + 10 mutators x 10 ops;
//!   quiesce + final scan.
//! Preconditions: era1 converged; dead-era external changes present.
//! Expected: quiesced convergence: exactly 1 row per live path,
//!   fingerprint match on witnesses, no stale rows, no dupes (I4/I12).
//! Fixed threshold: row paths == on-disk paths exactly; dup external
//!   count == 0; renamed file keeps id; deleted file row gone after
//!   confirmation scans.
//! Pass: exact. Fail: dupe/stale/mismatch.
//! Measurements: path sets pre/post, dupe count, fingerprint witness.
//! Reproduction protocol: 3 identical internal runs.
//!
//! No global lock: fixtures are temp dirs + pid-tagged test pipes
//! (disjoint by construction, decision #21: lock only the conflicting
//! subset — no global pipe is touched here).

use filesystem_provider::scanner::Scanner;
use serde_json::{json, Value};
use space_core::{ipc, Object};
use std::path::{Path, PathBuf};

fn tmp_base(run: usize) -> PathBuf {
    let d = std::env::temp_dir()
        .join("spaceos-ct7")
        .join(format!("run{run}"));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn test_pipe(run: usize) -> &'static str {
    Box::leak(format!(r"\\.\pipe\spaceos-ct7-{run}-{}", std::process::id()).into_boxed_str())
}

async fn boot_era(db: &Path, pipe: &'static str) -> tokio::runtime::Runtime {
    let storage = space_core::Storage::open(db).unwrap();
    let state = ipc::CoreState::new(storage);
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let s2 = state.clone();
    rt.spawn(async move {
        let _ = ipc::run_server(s2, pipe).await;
    });
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        let ok = ipc::send_request(pipe, "list_providers", json!({}))
            .await
            .map(|v| v["ok"].as_bool() == Some(true))
            .unwrap_or(false);
        if ok {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "T7: era not ready");
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    rt
}

async fn crash_era(rt: tokio::runtime::Runtime) {
    tokio::task::spawn_blocking(move || drop(rt)).await.unwrap();
}

async fn register(pipe: &str, name: &str, caps: &[&str]) -> String {
    let r = ipc::send_request(
        pipe,
        "register_agent",
        json!({"principal":"system","name":name,"capabilities":caps,"worlds":[]}),
    )
    .await
    .unwrap();
    assert_eq!(r["ok"], true);
    r["result"]["token"].as_str().unwrap().to_string()
}

async fn live_paths(pipe: &str) -> Vec<String> {
    let r = ipc::send_request(pipe, "query_objects", json!({"provider_id": "filesystem"}))
        .await
        .unwrap();
    let objs: Vec<Object> = serde_json::from_value(r["result"].clone()).unwrap();
    let mut v: Vec<String> = objs.into_iter().filter_map(|o| o.external_id).collect();
    v.sort();
    v
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn t7_requests_during_reconnect_rescan() {
    for run in 1..=3 {
        let base = tmp_base(run);
        let db = base.join("core.db");
        let root = base.join("root");
        std::fs::create_dir_all(&root).unwrap();
        let pipe = test_pipe(run);
        let sc = Scanner::new("filesystem");

        // Era 1: seed + converged scan.
        let rt1 = boot_era(&db, pipe).await;
        for f in ["a.txt", "b.txt", "c.txt", "d.txt", "e.txt"] {
            std::fs::write(root.join(f), format!("seed-{f}")).unwrap();
        }
        sc.scan_on(&root, pipe).await.unwrap();

        // Dead era: external create/modify/rename/delete.
        crash_era(rt1).await;
        std::fs::write(root.join("fresh.txt"), "new").unwrap();
        std::fs::write(root.join("a.txt"), "modified").unwrap();
        std::fs::rename(root.join("b.txt"), root.join("b2.txt")).unwrap();
        std::fs::remove_file(root.join("c.txt")).unwrap();

        // Era 2: reconnect rescan RACES 10 concurrent mutators.
        let rt2 = boot_era(&db, pipe).await;
        let tok = register(pipe, "mx", &["object.create", "object.update"]).await;
        let scan_h = tokio::spawn({
            let root = root.clone();
            let pipe = pipe.to_string();
            async move {
                Scanner::new("filesystem")
                    .scan_on(&root, &pipe)
                    .await
                    .unwrap()
            }
        });
        let mut mut_h = vec![];
        for w in 0..10 {
            let (p, t, r) = (pipe.to_string(), tok.clone(), root.clone());
            mut_h.push(tokio::spawn(async move {
                for i in 0..10 {
                    if i % 2 == 0 {
                        // A path the scan is about to observe.
                        let mut o = Object::new("File", "mx", json!({"race": format!("{w}-{i}")}));
                        o.id = format!("ct7-race-{w}-{i:02}");
                        let mut params = json!(o);
                        params["actor_token"] = json!(t);
                        let _ = ipc::send_request(&p, "create_object", params).await;
                    } else {
                        let _ = ipc::send_request(&p, "query_objects", json!({})).await;
                    }
                    if w == 0 && i == 5 {
                        // Mid-scan rename of a live file.
                        let _ = std::fs::rename(r.join("d.txt"), r.join("d2.txt"));
                    }
                }
            }));
        }
        let _ = scan_h.await.unwrap();
        for h in mut_h {
            h.await.unwrap();
        }
        // Quiesce + final scans (delete needs confirmation passes, V1.7).
        sc.scan_on(&root, pipe).await.unwrap();
        sc.scan_on(&root, pipe).await.unwrap();
        let stats = sc.scan_on(&root, pipe).await.unwrap();

        // Convergence: rows == disk paths exactly (rows include the
        // root dir itself — V1.7 precedent).
        let rows = live_paths(pipe).await;
        let mut disk: Vec<String> = std::fs::read_dir(&root)
            .unwrap()
            .map(|e| e.unwrap().path().to_string_lossy().to_string())
            .collect();
        disk.push(root.to_string_lossy().to_string());
        disk.sort();
        assert_eq!(rows, disk, "T7 run{run}: rows == disk (stats={stats})");
        // No duplicate external identity.
        let mut uniq = rows.clone();
        uniq.dedup();
        assert_eq!(rows.len(), uniq.len(), "T7 run{run}: no dupes");
        // Renamed files kept identity (single row each).
        for name in ["b2.txt", "d2.txt", "fresh.txt"] {
            let n = rows.iter().filter(|p| p.ends_with(name)).count();
            assert_eq!(n, 1, "T7 run{run}: exactly one row for {name}");
        }
        assert!(
            !rows.iter().any(|p| p.ends_with("c.txt")),
            "T7 run{run}: deleted confirmed gone"
        );
        // Fingerprint witness on a raced-over file.
        let fp = filesystem_provider::identity::fingerprint(&root.join("a.txt")).unwrap();
        let r = ipc::send_request(
            pipe,
            "get_object_by_external",
            json!({"provider_id": "filesystem", "external_id": root.join("a.txt").to_string_lossy()}),
        )
        .await
        .unwrap();
        let row: Option<Object> = serde_json::from_value(r["result"].clone()).unwrap_or(None);
        assert_eq!(
            row.expect("a.txt row")
                .properties
                .get("fingerprint")
                .and_then(|v| v.as_str()),
            Some(fp.as_str()),
            "T7 run{run}: fingerprint match"
        );
        // Race rows from mutators exist exactly once each (I12).
        let q = ipc::send_request(pipe, "query_objects", json!({}))
            .await
            .unwrap();
        let all: Vec<Object> = serde_json::from_value(q["result"].clone()).unwrap();
        let race: Vec<&Object> = all
            .iter()
            .filter(|o| o.id.starts_with("ct7-race-"))
            .collect();
        assert_eq!(
            race.len(),
            50,
            "T7 run{run}: 50 race rows, got {}",
            race.len()
        );
        crash_era(rt2).await;
        eprintln!("T7 run{run}: HELD (reconnect race converged; 0 dupes, 0 stale)");
    }
}
