//! SpaceTrace overhead probe (Slice 3/4): hook emission cost vs commit latency.
//!
//! Compares single hook emission latency against WAL commit latency.
//! Threshold (fixed before): p95 hook overhead < 5% of p50 commit latency.

use serde_json::json;

#[test]
fn hook_overhead_vs_commit_latency() {
    let st = spacetrace::SpaceTrace::new().unwrap();
    spacetrace::init_global_hooks(st.buffer(), st.correlation());
    let hooks = spacetrace::hooks::global_hooks();

    // 1. Hook emission latency (active): start+commit pair.
    let mut hook_lat = Vec::with_capacity(2000);
    for i in 0..2000 {
        let corr = format!("ovh-{i}");
        let t = std::time::Instant::now();
        hooks.core_mutation_start(
            &corr,
            "create_object",
            "ovh-agent",
            Some("ovh-world"),
            json!({"i": i}),
        );
        hooks.core_mutation_commit(&corr, "create_object", "ovh-agent", Some("ovh-world"), 1);
        hook_lat.push(t.elapsed().as_nanos() as i64);
    }
    hook_lat.sort_unstable();
    let hook_p50 = hook_lat[(hook_lat.len() as f64 * 0.50) as usize];
    let hook_p95 = hook_lat[(hook_lat.len() as f64 * 0.95) as usize];
    let hook_p99 = hook_lat[(hook_lat.len() as f64 * 0.99) as usize];
    eprintln!("hook start+commit pair: p50={hook_p50}ns p95={hook_p95}ns p99={hook_p99}ns");

    // 2. WAL single-commit latency baseline (Storage direct, no IPC).
    let dir = std::env::temp_dir().join("spaceos-st-ovh");
    std::fs::create_dir_all(&dir).unwrap();
    let db = dir.join("ovh.db");
    let _ = std::fs::remove_file(&db);
    let s = space_core::Storage::open(&db).unwrap();
    let mut commit_lat = Vec::with_capacity(500);
    for i in 0..500 {
        let mut o = space_core::Object::new("File", "ovh", json!({"i": i}));
        o.id = format!("ovh-obj-{i}");
        let t = std::time::Instant::now();
        s.create_object(&o).unwrap();
        commit_lat.push(t.elapsed().as_nanos() as i64);
    }
    commit_lat.sort_unstable();
    let commit_p50 = commit_lat[(commit_lat.len() as f64 * 0.50) as usize];
    eprintln!("WAL single commit: p50={commit_p50}ns");

    let ratio = hook_p95 as f64 / commit_p50 as f64 * 100.0;
    eprintln!("hook p95 overhead = {ratio:.2}% of commit p50");

    // Threshold: hook pair p95 < 5% of commit p50.
    assert!(
        ratio < 5.0,
        "hook overhead {ratio:.2}% exceeds 5% of commit latency"
    );
}
