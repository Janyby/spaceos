//! M5 — Shared file conflicts (human + agents, one real file).
//!
//! Hypothesis: concurrent human (direct fs) and agent (Core/Provider)
//! work on one real file may cause silent overwrite, wrong Object
//! identity, or Reality/Core divergence.
//! What may break: duplicate rows for one path, stale fingerprint after
//! quiesce, confirmed-delete of a present file.
//! Failure boundary: Provider/Reality (scan reconciliation, fingerprint
//! evidence); SQLite atomicity vs fs-op atomicity distinguished (fs reads
//! may race writes; convergence required after quiesce, not mid-storm).
//! Why it may break: torn reads, rename races, watcher/scan interleave.
//! Exact workload (isolated temp dir): (a) 20 human rewrites x 5
//! concurrent scans, then quiesce + final scan; (b) human rename between
//! scans + new file at old path.
//! Dataset: temp world dir + temp DB + temp pipe per test.
//! Fixed threshold: 0 errors; exactly 1 row per path; stored fingerprint
//! == actual file fingerprint after quiesce; no duplicates; rename keeps
//! id with 0 duplicates.
//! Expected behavior: HELD (convergence after quiesce).
//! Measurement: row counts, fingerprint equality, duplicate count.
//! Pass criterion: post-quiesce exact convergence in both scenarios.
//! Failure criterion: duplicate identity OR stale fingerprint after
//! quiesce OR present-file deletion.
//! Reproduction protocol: 3 identical runs each.

use filesystem_provider::scanner::Scanner;
use space_core::{ipc, Object, Storage};
use std::path::PathBuf;

fn tmp(name: &str) -> PathBuf {
    let d = std::env::temp_dir().join("spaceos-fm5").join(name);
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

async fn start_core(pipe: &str, db: &PathBuf) {
    let storage = Storage::open(db).unwrap();
    let state = ipc::CoreState::new(storage);
    let s2 = state.clone();
    let p = pipe.to_string();
    tokio::spawn(async move {
        let _ = ipc::run_server(s2, &p).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
}

fn get_by_path(objs: &[Object], path: &str) -> Vec<Object> {
    objs.iter()
        .filter(|o| o.external_id.as_deref() == Some(path))
        .cloned()
        .collect()
}

async fn all_objects(pipe: &str) -> Vec<Object> {
    let r = ipc::send_request(
        pipe,
        "query_objects",
        serde_json::json!({"provider_id": "filesystem"}),
    )
    .await
    .unwrap();
    serde_json::from_value(r["result"].clone()).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn m5_write_storm_converges() {
    let pipe_owned = format!(r"\\.\pipe\spaceos-fm5w-{}", std::process::id());
    let pipe: &'static str = Box::leak(pipe_owned.into_boxed_str());
    let dir = tmp("wstorm");
    let db = dir.join("core.db");
    let world = dir.join("world");
    std::fs::create_dir_all(&world).unwrap();
    start_core(pipe, &db).await;

    let scanner = std::sync::Arc::new(Scanner::new("filesystem"));
    let f = world.join("f.txt");
    std::fs::write(&f, "v0").unwrap();
    scanner.scan_on(&world, pipe).await.unwrap();

    // Human rewrite storm concurrent with scans.
    let fw = f.clone();
    let human = tokio::spawn(async move {
        for i in 1..=20 {
            std::fs::write(&fw, format!("human-{i:02}")).unwrap();
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    });
    for _ in 0..5 {
        scanner.scan_on(&world, pipe).await.unwrap();
    }
    human.await.unwrap();

    // Quiesce + final scan: must converge exactly.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let stats = scanner.scan_on(&world, pipe).await.unwrap();
    let objs = all_objects(pipe).await;
    let rows = get_by_path(&objs, &f.to_string_lossy());
    assert_eq!(
        rows.len(),
        1,
        "M5 exactly one row per path, got {}",
        rows.len()
    );
    let actual_fp = filesystem_provider::identity::fingerprint(&f).expect("readable");
    let stored_fp = rows[0]
        .properties
        .get("fingerprint")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    assert_eq!(
        stored_fp, actual_fp,
        "M5 fingerprint converged after quiesce"
    );
    eprintln!("M5 write-storm: 1 row, fingerprint converged, stats={stats}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn m5_rename_under_load_preserves_identity() {
    let pipe_owned = format!(r"\\.\pipe\spaceos-fm5r-{}", std::process::id());
    let pipe: &'static str = Box::leak(pipe_owned.into_boxed_str());
    let dir = tmp("rename");
    let db = dir.join("core.db");
    let world = dir.join("world");
    std::fs::create_dir_all(&world).unwrap();
    start_core(pipe, &db).await;

    let scanner = std::sync::Arc::new(Scanner::new("filesystem"));
    let f = world.join("f.txt");
    let g = world.join("g.txt");
    std::fs::write(&f, "rename-me").unwrap();
    scanner.scan_on(&world, pipe).await.unwrap();
    let before = all_objects(pipe).await;
    let orig = get_by_path(&before, &f.to_string_lossy());
    assert_eq!(orig.len(), 1);
    let orig_id = orig[0].id.clone();

    // Human renames; scan while the old path is missing (unambiguous:
    // the orphan rule requires the old path to be unobservable, otherwise
    // "rewrite + copy" is indistinguishable from "rename + recreate").
    std::fs::rename(&f, &g).unwrap();
    let s = scanner.scan_on(&world, pipe).await.unwrap();

    let after = all_objects(pipe).await;
    let at_g = get_by_path(&after, &g.to_string_lossy());
    assert_eq!(at_g.len(), 1, "renamed file has exactly one row");
    assert_eq!(at_g[0].id, orig_id, "M5 rename preserves identity");

    // Agent-style new file appears at the old path: fresh identity, no dupes.
    std::fs::write(&f, "fresh-file").unwrap();
    let s2 = scanner.scan_on(&world, pipe).await.unwrap();
    let after2 = all_objects(pipe).await;
    let at_f = get_by_path(&after2, &f.to_string_lossy());
    assert_eq!(at_f.len(), 1, "new file at old path has exactly one row");
    assert_ne!(at_f[0].id, orig_id, "new file is a new identity");
    // No duplicate external_ids anywhere.
    let mut seen = std::collections::HashSet::new();
    for o in &after2 {
        if let Some(ext) = &o.external_id {
            assert!(seen.insert(ext.clone()), "M5 no duplicate path {ext}");
        }
    }
    eprintln!("M5 rename: identity kept + fresh identity minted, stats={s} {s2}");
}
